"""并发执行引擎。

每一轮（round）：
  1. 用控制连接执行“初始数据 SQL”，把库重置到相同起点；
  2. 会话 A、B 各开一条连接，按本轮隔离级别 BEGIN；
  3. 按脚本顺序交错执行步骤：
     - complete 模式：等语句执行完毕（直到超时）；
     - blocked  模式：语句在后台执行，轮询 pg_blocking_pids()，
       一旦数据库确认它被另一会话阻塞就推进下一步（语句仍在飞），
       持锁会话的后续步骤（如 COMMIT）不受影响；
  4. 收尾：**先取消所有仍在执行的语句**并等服务器确认结束，再回滚仍未提交的
     事务，最后确认每个会话都已停止。顺序反过来会释放行锁，让仍在等锁的
     多语句批次继续执行到自己的 COMMIT——超时批次会因此被“收尾”提交。
     正常结束（未超时）时先给在飞语句一个宽限窗口自行完成，再取消剩余语句；
  5. 收尾成功后才在控制连接上执行核对查询。清理失败时提示并跳过核对，
     后续轮次也不再启动。

超时：任何一步等待超过本轮时限即抛 RoundTimeout，本轮结束并按第 4 步收尾。
重开一轮总是从第 1 步的初始数据重新开始。
"""
from __future__ import annotations

import threading
import time

import psycopg2
from psycopg2 import extensions

ISOLATION_LEVELS = ("READ COMMITTED", "SERIALIZABLE")

MAX_ROWS = 50            # 单步 SELECT 结果最多带回前端的行数
POLL_INTERVAL = 0.05     # 阻塞确认轮询间隔（秒）
CANCEL_WAIT = 2.0        # 取消语句后等待服务器确认结束的时限（秒）
CLEANUP_GRACE = 2.0      # 正常结束收尾时，等在飞语句自行完成的宽限（秒）
STOP_VERIFY_WAIT = 1.0   # 收尾后确认会话后端真正停止的时限（秒）

# 处于这些状态的后端仍在执行语句或持有未结束的事务
_TXN_ACTIVE_STATES = ("active", "idle in transaction",
                      "idle in transaction (aborted)", "fastpath function call")

CANCEL_PGCODE = "57014"


class RoundTimeout(Exception):
    """本轮超时：结束本轮，由收尾逻辑取消在飞语句并回滚未提交事务。"""


def _normalize(value):
    """把 psycopg2 返回的值转成 JSON 可序列化的形式。"""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def _err_info(exc):
    pgcode = getattr(exc, "pgcode", None)
    diag = getattr(exc, "diag", None)
    msg = getattr(diag, "message_primary", None) if diag else None
    return pgcode, (msg or str(exc).strip())


def query_blocking_pids(conn, pid):
    """在控制连接上查询指定后端的阻塞者 pid 列表；查询失败按“未阻塞”处理。"""
    try:
        cur = conn.cursor()
        cur.execute("SELECT pg_blocking_pids(%s)", (pid,))
        row = cur.fetchone()
        cur.close()
        return list(row[0]) if row and row[0] else []
    except Exception:
        return []


def _backend_rows(ctl, pids):
    """返回 pg_stat_activity 中给定 pid 的 (pid, state)；查询失败返回 None。"""
    pids = list(pids)
    if not pids:
        return []
    if ctl is None:
        return None
    try:
        cur = ctl.cursor()
        cur.execute(
            "SELECT pid, state FROM pg_stat_activity WHERE pid = ANY(%s)", (pids,))
        rows = cur.fetchall()
        cur.close()
        return [(r[0], r[1]) for r in rows]
    except Exception:
        return None


def _wait_until_stopped(ctl, pids, wait=STOP_VERIFY_WAIT):
    """确认这些后端不再执行语句、也没有未结束的事务。"""
    pids = list(pids)
    if not pids:
        return True
    deadline = time.monotonic() + wait
    while True:
        rows = _backend_rows(ctl, pids)
        if rows is None:
            return False
        busy = [pid for pid, state in rows if state in _TXN_ACTIVE_STATES]
        if not busy:
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(POLL_INTERVAL)


def _terminate_backend(ctl, pid, log):
    """强制终止一个会话后端，并确认它已停止；用于取消失败时的兜底。"""
    if ctl is None:
        log("SYS", "cleanup_error", pid=pid,
            detail=f"控制连接不可用，无法终止会话后端 {pid}")
        return False
    try:
        cur = ctl.cursor()
        cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
        row = cur.fetchone()
        cur.close()
        terminated = bool(row and row[0])
    except Exception as exc:
        log("SYS", "cleanup_error", pid=pid,
            detail=f"终止会话后端 {pid} 失败：{exc}")
        return False
    if _wait_until_stopped(ctl, [pid]):
        log("SYS", "terminate_backend", pid=pid,
            detail=f"已强制终止会话后端 pid {pid}（其未提交事务由服务器回滚）"
                   if terminated else f"会话后端 pid {pid} 已停止")
        return True
    log("SYS", "cleanup_error", pid=pid,
        detail=f"会话后端 pid {pid} 仍在执行，无法确认停止")
    return False


class SessionRunner:
    """一个数据库会话：一条连接，同一时刻至多一条在飞语句。"""

    def __init__(self, name, conninfo, isolation, log):
        self.name = name
        self.log = log
        self.conn = psycopg2.connect(**conninfo)
        self.conn.autocommit = True  # 事务边界完全由 BEGIN/COMMIT 语句控制
        self.pid = self.conn.get_backend_pid()
        self.inflight = None   # (box, step)，仍在后台执行的语句
        self.outcome = None    # committed / rolled_back / aborted
        self.outcome_note = ""
        cur = self.conn.cursor()
        cur.execute(f"BEGIN ISOLATION LEVEL {isolation}")
        cur.close()
        log(self.name, "begin", detail=f"BEGIN ISOLATION LEVEL {isolation}", pid=self.pid)

    # ---- 内部 ----

    def _start(self, sql):
        """在工作线程里执行语句，主线程可以继续轮询阻塞状态。"""
        box = {
            "event": threading.Event(),
            "error": None,
            "columns": None,
            "rows": None,
            "status": None,
            "was_in_txn": self.conn.get_transaction_status()
            != extensions.TRANSACTION_STATUS_IDLE,
        }

        def work():
            try:
                cur = self.conn.cursor()
                cur.execute(sql)
                box["status"] = cur.statusmessage
                if cur.description:
                    box["columns"] = [d.name for d in cur.description]
                    box["rows"] = [
                        [_normalize(v) for v in row] for row in cur.fetchmany(MAX_ROWS)
                    ]
                cur.close()
            except Exception as exc:  # 任何错误都要带回时间线
                box["error"] = exc
            finally:
                box["event"].set()

        thread = threading.Thread(target=work, daemon=True)
        thread.start()
        box["thread"] = thread
        return box

    def _collect(self, box, step, after_blocked):
        """语句结束后记录结果，并维护事务结局。"""
        err = box["error"]
        if err is not None:
            pgcode, msg = _err_info(err)
            cancelled = pgcode == CANCEL_PGCODE
            self.log(self.name, "step_error", step=step.index, sql=step.sql,
                     pgcode=pgcode, error=msg, after_blocked=after_blocked,
                     **({"cancelled": True} if cancelled else {}))
            if pgcode in ("40001", "40P01"):  # 序列化失败 / 死锁
                self.outcome = "aborted"
                self.outcome_note = f"{pgcode} {msg}"
            elif cancelled and self.outcome is None:
                # 被引擎取消的语句不会随批次提交；服务器已回滚其事务。
                self.outcome = "rolled_back"
                self.outcome_note = "语句被取消，事务未提交"
            return
        self.log(self.name, "step_ok", step=step.index, sql=step.sql,
                 status=box["status"], columns=box["columns"], rows=box["rows"],
                 after_blocked=after_blocked)
        # 事务随这条语句结束了吗？以服务器返回的命令标签为准
        # （COMMIT 一个已失败的事务时，PG 实际返回 ROLLBACK）。
        if (box["was_in_txn"]
                and self.conn.get_transaction_status() == extensions.TRANSACTION_STATUS_IDLE
                and self.outcome != "aborted"):
            tag = (box["status"] or "").upper()
            if tag == "COMMIT":
                self.outcome = "committed"
            elif tag == "ROLLBACK":
                self.outcome = "rolled_back"

    def _cancel(self):
        try:
            self.conn.cancel()
        except Exception:
            pass

    def _take_inflight(self, box, step):
        self.inflight = None
        self._collect(box, step, after_blocked=bool(box.get("blocked_confirmed")))

    # ---- 对外 ----

    def settle_inflight(self, wait):
        """给在飞语句一段时间自行结束；结束则收集结果并返回 True。"""
        if self.inflight is None:
            return True
        box, step = self.inflight
        if not box["event"].wait(max(wait, 0.0)):
            return False
        self._take_inflight(box, step)
        return True

    def abandon_inflight(self):
        """后端已被强制终止：等执行线程退出，丢弃结果（语句未完成、未提交）。"""
        if self.inflight is None:
            return
        box, step = self.inflight
        box["event"].wait(CANCEL_WAIT)
        self.inflight = None

    def send_cancel(self, reason):
        """对在飞语句发出取消请求（不等结果）；取消会让语句出错、事务中止。"""
        if self.inflight is None:
            return
        box, step = self.inflight
        if box["event"].is_set():  # 已经结束，无需取消
            return
        self.log(self.name, "cancel", step=step.index, sql=step.sql, detail=reason)
        self._cancel()

    def join_inflight(self, deadline):
        """执行同一会话的下一步前，先等上一条在飞语句结束。"""
        if self.inflight is None:
            return
        box, step = self.inflight
        remaining = deadline - time.monotonic()
        if box["event"].wait(max(remaining, 0.0)):
            self._take_inflight(box, step)
            return
        # 留给收尾统一取消：先取消等锁的批次，再取消持锁会话，
        # 避免持锁事务中止释放锁后，等待中的批次继续跑到 COMMIT。
        raise RoundTimeout()

    def run_step(self, step, other, ctl, deadline):
        self.join_inflight(deadline)
        box = self._start(step.sql)
        self.log(self.name, "step_start", step=step.index, sql=step.sql, mode=step.mode)

        if step.mode == "complete":
            remaining = deadline - time.monotonic()
            if not box["event"].wait(max(remaining, 0.0)):
                self.inflight = (box, step)
                raise RoundTimeout()
            self._collect(box, step, after_blocked=False)
            return

        # blocked 模式：等到数据库确认被另一会话阻塞即推进，语句留在后台
        while True:
            if box["event"].wait(POLL_INTERVAL):
                self._collect(box, step, after_blocked=False)
                if box["error"] is None:
                    self.log(self.name, "no_block", step=step.index, sql=step.sql,
                             detail="语句直接完成，未发生阻塞")
                return
            blockers = query_blocking_pids(ctl, self.pid)
            if blockers:
                box["blocked_confirmed"] = True
                self.inflight = (box, step)
                self.log(self.name, "blocked", step=step.index, sql=step.sql,
                         blockers=blockers, by_other=other.pid in blockers,
                         detail="pg_blocking_pids → " + ",".join(map(str, blockers)))
                return
            if time.monotonic() >= deadline:
                self.inflight = (box, step)
                raise RoundTimeout()

    def rollback_if_open(self, timed_out):
        """回滚未提交事务；成功（含本来就空闲）返回 True。"""
        try:
            status = self.conn.get_transaction_status()
        except Exception as exc:
            pgcode, msg = _err_info(exc)
            self.log(self.name, "rollback_error", pgcode=pgcode, error=msg)
            return False
        if status == extensions.TRANSACTION_STATUS_IDLE:
            return True
        try:
            cur = self.conn.cursor()
            cur.execute("ROLLBACK")
            cur.close()
            note = "超时回滚未提交事务" if timed_out else "脚本结束仍有未提交事务，引擎回滚"
            self.log(self.name, "rollback", detail=note)
            if self.outcome is None:
                self.outcome = "rolled_back"
                self.outcome_note = note
            return True
        except Exception as exc:
            pgcode, msg = _err_info(exc)
            self.log(self.name, "rollback_error", pgcode=pgcode, error=msg)
            return False

    def verify_idle(self):
        """确认事务已经结束（空闲），否则视为清理失败。"""
        try:
            status = self.conn.get_transaction_status()
        except Exception as exc:
            pgcode, msg = _err_info(exc)
            self.log(self.name, "cleanup_error", pgcode=pgcode, error=msg)
            return False
        if status != extensions.TRANSACTION_STATUS_IDLE:
            self.log(self.name, "cleanup_error",
                     detail=f"事务状态仍为 {int(status)}，未完成回滚")
            return False
        return True

    def mark_terminated(self, note):
        """连接被强制终止（服务器端事务已回滚）。"""
        if self.outcome is None:
            self.outcome = "rolled_back"
            self.outcome_note = note

    def close(self):
        try:
            self.conn.close()
            return True
        except Exception as exc:
            self.log(self.name, "close_error", error=str(exc))
            return False


def _inflight_is_waiting(session, ctl):
    """该会话的在飞语句当前是否在等别的会话（pg_blocking_pids 非空）。"""
    try:
        return bool(query_blocking_pids(ctl, session.pid))
    except Exception:
        return False


def _cleanup_sessions(ctl, sessions, timed_out, log):
    """收尾：先取消所有在飞语句，再回滚、校验并关闭会话。

    绝不能先回滚持锁会话：那会释放行锁，让仍在等锁的多语句批次继续跑到
    自己的 COMMIT，等于由“收尾”替超时批次提交。正常结束（未超时）时先给
    在飞语句一段宽限，让它按脚本自行完成，再取消剩余语句。
    """
    result = {"ok": True, "timed_out": timed_out, "errors": [],
              "terminated": [], "done": True}

    def fail(msg):
        result["errors"].append(msg)
        log("SYS", "cleanup_error", error=msg)

    # 0) 正常结束：先让在飞语句自行完成（不触碰任何事务）
    if not timed_out:
        grace_deadline = time.monotonic() + CLEANUP_GRACE
        for s in sessions:
            if s.inflight is not None:
                s.settle_inflight(grace_deadline - time.monotonic())

    # 1) 取消所有在飞语句，等服务器确认结束（取消即中止语句，不会提交）。
    #    顺序关键：先把取消请求发给所有“正在等锁”的会话并确认它们已中止，
    #    再去动持锁会话。若先回滚/取消持锁会话，其事务中止、行锁释放，等待中的
    #    多语句批次会立刻继续跑到自己的 COMMIT——等于由收尾替它提交。
    #    若等锁批次取消不生效，立即终止它的后端（服务器回滚其事务）；
    #    绝不为了“继续收尾”而提前释放持锁事务的锁。
    reason = ("超时收尾：取消仍在等锁的批次，禁止其继续提交" if timed_out
              else "脚本结束收尾：取消仍在执行的语句")
    inflight = [s for s in sessions if s.inflight is not None]
    waiting = [s for s in inflight if _inflight_is_waiting(s, ctl)]
    others = [s for s in inflight if s not in waiting]
    for s in waiting:  # 先发出全部取消请求，再统一等待
        s.send_cancel(reason)
    for s in waiting:
        if s.settle_inflight(CANCEL_WAIT):
            continue
        if _terminate_backend(ctl, s.pid, log):
            result["terminated"].append(s.name)
            s.mark_terminated("等锁批次取消未生效，强制终止连接（未提交事务已回滚）")
            s.abandon_inflight()
        else:
            fail(f"会话 {s.name} 的等锁批次无法停止，可能仍在执行")
    for s in others:
        s.send_cancel(reason)
    for s in others:
        s.settle_inflight(CANCEL_WAIT)

    # 2) 仍未收拢的语句（持锁会话或未阻塞语句）：终止后端兜底
    for s in sessions:
        if s.inflight is None or s.name in result["terminated"]:
            continue
        box, step = s.inflight
        log(s.name, "step_error", step=step.index, sql=step.sql,
            error="取消未生效，强制终止连接", after_blocked=True, cancelled=True)
        if _terminate_backend(ctl, s.pid, log):
            result["terminated"].append(s.name)
            s.mark_terminated("取消未生效，强制终止连接（未提交事务已回滚）")
            s.abandon_inflight()
        else:
            fail(f"会话 {s.name} 的语句无法停止，可能仍在执行")

    # 3) 现在没有语句在飞，可以安全回滚未提交事务
    for s in sessions:
        if s.name in result["terminated"]:
            continue
        if not s.rollback_if_open(timed_out):
            fail(f"会话 {s.name} 回滚未提交事务失败")

    # 4) 已有清理失败：为保证会话停止，终止所有仍未停止的后端
    if result["errors"]:
        for s in sessions:
            if s.name in result["terminated"]:
                continue
            if _terminate_backend(ctl, s.pid, log):
                result["terminated"].append(s.name)
                s.mark_terminated("清理失败，强制终止连接（未提交事务已回滚）")
            else:
                fail(f"会话 {s.name} 无法终止，可能仍在执行")

    # 5) 确认所有会话都已停止（事务结束、后端不再执行语句）
    stopped_pids = []
    for s in sessions:
        if s.name not in result["terminated"] and not s.verify_idle():
            fail(f"会话 {s.name} 事务未结束")
        stopped_pids.append(s.pid)
    if not _wait_until_stopped(ctl, stopped_pids):
        fail("仍有会话后端未停止")

    # 6) 关闭连接
    for s in sessions:
        if not s.close() and s.name not in result["terminated"]:
            fail(f"会话 {s.name} 连接关闭失败")
    result["ok"] = not result["errors"]
    if result["ok"]:
        note = "会话已停止，未提交事务已回滚"
        if result["terminated"]:
            note += f"（强制终止：{', '.join(result['terminated'])}）"
        log("SYS", "cleanup_ok", detail=note)
    return result


def _run_check(ctl, check_sql):
    try:
        cur = ctl.cursor()
        cur.execute(check_sql)
        out = {"error": None, "pgcode": None, "columns": None, "rows": None,
               "status": cur.statusmessage}
        if cur.description:
            out["columns"] = [c.name for c in cur.description]
            out["rows"] = [[_normalize(v) for v in row] for row in cur.fetchmany(200)]
        cur.close()
        return out
    except Exception as exc:
        pgcode, msg = _err_info(exc)
        return {"error": msg, "pgcode": pgcode, "columns": None, "rows": None,
                "status": None}


def _new_cleanup():
    return {"ok": True, "timed_out": False, "errors": [], "terminated": [],
            "done": False}


def run_round(conninfo, init_sql, steps, check_sql, isolation, timeout_s):
    """跑一轮：重置 → 交错执行 → 收尾（取消/回滚/校验）→ 核对查询。"""
    events = []
    t0 = time.monotonic()

    def log(session, kind, **kw):
        events.append({"t": round(time.monotonic() - t0, 3),
                       "session": session, "kind": kind, **kw})

    result = {"isolation": isolation, "skipped": False, "timed_out": False,
              "error": None, "events": events, "outcomes": {}, "check": None,
              "check_skipped": False, "cleanup": _new_cleanup(),
              "duration": None}
    deadline = t0 + timeout_s
    ctl = None
    sessions = []
    try:
        try:
            ctl = psycopg2.connect(**conninfo)
            ctl.autocommit = True
            log("SYS", "reset", detail="重置初始数据")
            cur = ctl.cursor()
            cur.execute(init_sql)
            cur.close()

            sess_a = SessionRunner("A", conninfo, isolation, log)
            sessions.append(sess_a)
            sess_b = SessionRunner("B", conninfo, isolation, log)
            sessions.append(sess_b)
            by_name = {"A": sess_a, "B": sess_b}

            try:
                for step in steps:
                    if time.monotonic() >= deadline:
                        raise RoundTimeout()
                    sess = by_name[step.session]
                    other = by_name["B" if step.session == "A" else "A"]
                    sess.run_step(step, other, ctl, deadline)
            except RoundTimeout:
                result["timed_out"] = True
                log("SYS", "timeout",
                    detail=f"超过 {timeout_s:g} 秒未推进，结束本轮：先取消仍在执行的"
                           "语句，再回滚未提交事务")
            except Exception as exc:
                pgcode, msg = _err_info(exc)
                result["error"] = msg
                log("SYS", "round_error", pgcode=pgcode, error=msg)
        except Exception as exc:
            # 连接 / 初始数据 / 建立会话阶段失败
            pgcode, msg = _err_info(exc)
            result["error"] = msg
            log("SYS", "round_error", pgcode=pgcode, error=msg)

        # 收尾：取消在飞语句 → 回滚 → 校验。成功后才允许核对。
        result["cleanup"] = _cleanup_sessions(
            ctl, sessions, result["timed_out"], log)
        if result["cleanup"]["ok"] and ctl is not None:
            log("SYS", "check", detail="执行核对查询")
            result["check"] = _run_check(ctl, check_sql)
        else:
            result["check_skipped"] = True
            log("SYS", "check_skipped",
                detail="清理未完成或连接不可用，跳过核对查询；后续轮次也不会启动")
    finally:
        result["duration"] = round(time.monotonic() - t0, 3)
        for s in sessions:
            result["outcomes"][s.name] = {
                "outcome": s.outcome or "unknown",
                "note": s.outcome_note,
                "pid": s.pid,
            }
        for s in sessions:
            s.close()
        if ctl is not None:
            try:
                ctl.close()
            except Exception:
                pass
    return result


def _skipped_round(isolation, reason):
    return {"isolation": isolation, "skipped": True, "skip_reason": reason,
            "timed_out": False, "error": None, "events": [], "outcomes": {},
            "check": None, "check_skipped": True, "duration": None,
            "cleanup": {"ok": True, "timed_out": False, "errors": [],
                        "terminated": [], "done": False}}


def run_experiment(conninfo, init_sql, steps, check_sql, timeout_s):
    """同一初始数据，分别按 READ COMMITTED 和 SERIALIZABLE 各跑一轮。

    若某一轮清理失败（会话可能仍有残留），不再启动后续轮次，返回带
    skipped 标记的占位结果，页面据此提示。
    """
    rounds = []
    for index, isolation in enumerate(ISOLATION_LEVELS):
        rnd = run_round(conninfo, init_sql, steps, check_sql, isolation, timeout_s)
        rounds.append(rnd)
        if not rnd["cleanup"]["ok"]:
            detail = "；".join(rnd["cleanup"]["errors"]) or "清理未完成"
            reason = f"{isolation} 轮清理失败（{detail}），未启动后续轮次"
            for later in ISOLATION_LEVELS[index + 1:]:
                rounds.append(_skipped_round(later, reason))
            break
    return rounds
