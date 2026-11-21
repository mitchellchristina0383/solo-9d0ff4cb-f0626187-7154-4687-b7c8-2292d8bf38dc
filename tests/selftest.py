"""端到端自测：写偏斜（撤班样例）、锁等待、超时回滚与重跑、blocked 未阻塞分支。

用法：
  docker compose up -d --build 之后：
      docker compose exec web python tests/selftest.py
  或对着任意可达的 PostgreSQL：
      PGHOST=... PGPORT=... PGDATABASE=... PGUSER=... PGPASSWORD=... python tests/selftest.py
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "app"))

from engine import run_experiment  # noqa: E402
from presets import PRESETS  # noqa: E402
from script_parser import ScriptError, parse_script  # noqa: E402

FAILURES = []


def conninfo():
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": os.environ.get("PGDATABASE", "postgres"),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", ""),
        "connect_timeout": 10,
    }


def preset(pid):
    return next(p for p in PRESETS if p["id"] == pid)


def run_preset(pid, timeout=6.0):
    p = preset(pid)
    steps = parse_script(p["script"])
    return run_experiment(conninfo(), p["init_sql"], steps, p["check_sql"], timeout)


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"  [{status}] {name}" + (f"  ({detail})" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def check_value(rows, col_name, columns):
    idx = columns.index(col_name)
    return rows[0][idx]


def test_parser():
    print("== 脚本解析器 ==")
    steps = parse_script(preset("lock-wait")["script"])
    check("解析行锁等待样例得到 4 步", len(steps) == 4, f"got {len(steps)}")
    check("第 2 步是 B/blocked", steps[1].session == "B" and steps[1].mode == "blocked")
    check("其余步骤是 complete 模式", all(s.mode == "complete" for s in steps if s.index != 1))
    try:
        parse_script("SELECT 1;")
        check("头部之外的 SQL 报错", False)
    except ScriptError:
        check("头部之外的 SQL 报错", True)
    try:
        parse_script("@A complete\n")
        check("空步骤报错", False)
    except ScriptError:
        check("空步骤报错", True)
    try:
        parse_script("@A turbo\nSELECT 1;")
        check("未知模式报错", False)
    except ScriptError:
        check("未知模式报错", True)


def test_write_skew():
    print("== 两人同时撤班（写偏斜） ==")
    rc, ser = run_preset("shift-withdraw")
    check("RC 轮正常完成", not rc["timed_out"] and not rc["error"], rc.get("error"))
    check("RC 两事务都提交",
          rc["outcomes"]["A"]["outcome"] == "committed"
          and rc["outcomes"]["B"]["outcome"] == "committed",
          str(rc["outcomes"]))
    rc_count = check_value(rc["check"]["rows"], "on_duty_count", rc["check"]["columns"])
    check("RC 撤班后无人值守（on_duty_count = 0）", rc_count == 0, f"got {rc_count}")

    check("SER 轮正常完成", not ser["timed_out"] and not ser["error"], ser.get("error"))
    outcomes = sorted(o["outcome"] for o in ser["outcomes"].values())
    check("SER 恰一个提交、一个中止", outcomes == ["aborted", "committed"], str(ser["outcomes"]))
    has_40001 = any(e["kind"] == "step_error" and e.get("pgcode") == "40001"
                    for e in ser["events"])
    check("SER 时间线出现 40001 序列化失败", has_40001)
    ser_count = check_value(ser["check"]["rows"], "on_duty_count", ser["check"]["columns"])
    check("SER 仍有一人在岗（on_duty_count = 1）", ser_count == 1, f"got {ser_count}")


def test_lock_wait():
    print("== 行锁等待（blocked 模式） ==")
    rc, ser = run_preset("lock-wait")
    check("RC 轮正常完成", not rc["timed_out"] and not rc["error"], rc.get("error"))
    blocked_ev = [e for e in rc["events"] if e["kind"] == "blocked" and e["session"] == "B"]
    check("B 被数据库确认阻塞", len(blocked_ev) == 1)
    check("阻塞者正是会话 A", bool(blocked_ev and blocked_ev[0]["by_other"]))
    resumed = [e for e in rc["events"]
               if e["kind"] == "step_ok" and e["session"] == "B" and e.get("after_blocked")]
    check("A 提交后 B 的 UPDATE 解除阻塞并完成", bool(resumed))
    check("RC 两事务都提交",
          rc["outcomes"]["A"]["outcome"] == "committed"
          and rc["outcomes"]["B"]["outcome"] == "committed",
          str(rc["outcomes"]))
    rc_bal = check_value(rc["check"]["rows"], "balance", rc["check"]["columns"])
    check("RC 余额 = 700（B 在 A 的结果上继续）", rc_bal == 700, f"got {rc_bal}")

    check("SER 轮正常完成", not ser["timed_out"] and not ser["error"], ser.get("error"))
    check("SER 下 A 提交、B 中止",
          ser["outcomes"]["A"]["outcome"] == "committed"
          and ser["outcomes"]["B"]["outcome"] == "aborted",
          str(ser["outcomes"]))
    ser_bal = check_value(ser["check"]["rows"], "balance", ser["check"]["columns"])
    check("SER 余额 = 900（只有 A 生效）", ser_bal == 900, f"got {ser_bal}")


def test_timeout_and_rerun():
    print("== 超时回滚与重跑 ==")
    rc, ser = run_preset("timeout-demo", timeout=3.0)
    for rnd, name in ((rc, "RC"), (ser, "SER")):
        check(f"{name} 轮判定为超时", rnd["timed_out"], str(rnd.get("error")))
        check(f"{name} 两事务都被回滚",
              rnd["outcomes"]["A"]["outcome"] == "rolled_back"
              and rnd["outcomes"]["B"]["outcome"] == "rolled_back",
              str(rnd["outcomes"]))
        bal = check_value(rnd["check"]["rows"], "balance", rnd["check"]["columns"])
        check(f"{name} 余额保持 1000（未提交的全部回滚）", bal == 1000, f"got {bal}")
    cancelled = any(e["kind"] == "step_error" and e.get("pgcode") == "57014"
                    for e in rc["events"])
    check("RC 时间线出现语句被取消（57014）", cancelled)

    # 超时之后立刻重跑另一组实验：仍应从初始数据开始并正常完成
    rc2, _ = run_preset("lock-wait")
    bal2 = check_value(rc2["check"]["rows"], "balance", rc2["check"]["columns"])
    check("超时后重跑仍从初始数据开始（锁等待样例 RC 余额 = 700）",
          not rc2["timed_out"] and bal2 == 700, f"got {bal2}")


def test_no_block_branch():
    print("== blocked 模式但语句未阻塞 ==")
    init = preset("lock-wait")["init_sql"]
    script = """@B blocked
UPDATE accounts SET balance = 0 WHERE id = 999;

@B complete
COMMIT;"""
    steps = parse_script(script)
    rc, _ = run_experiment(conninfo(), init, steps,
                           "SELECT balance FROM accounts WHERE id = 1;", 5.0)
    check("语句直接完成、本轮未超时", not rc["timed_out"] and not rc["error"])
    check("时间线记录“未阻塞”事件",
          any(e["kind"] == "no_block" for e in rc["events"]))
    bal = check_value(rc["check"]["rows"], "balance", rc["check"]["columns"])
    check("未命中任何行，余额不变（1000）", bal == 1000, f"got {bal}")


def main():
    test_parser()
    test_write_skew()
    test_lock_wait()
    test_timeout_and_rerun()
    test_no_block_branch()
    print()
    if FAILURES:
        print(f"共 {len(FAILURES)} 项失败：")
        for name in FAILURES:
            print("  -", name)
        return 1
    print("全部自测通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
