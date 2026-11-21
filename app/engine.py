"""并发执行引擎。

每一轮（round）：
  1. 用控制连接执行“初始数据 SQL”，把库重置到相同起点；
  2. 会话 A、B 各开一条连接，按本轮隔离级别 BEGIN；
  3. 按脚本顺序交错执行步骤：
     - complete 模式：等语句执行完毕（直到超时）；
     - blocked  模式：语句在后台执行，轮询 pg_blocking_pids()，
       一旦数据库确认它被另一会话阻塞就推进下一步（语句仍在飞），
       持锁会话的后续步骤（如 COMMIT）不受影响；
  4. 收尾：先回滚“没有在飞语句”的事务（可能借此解除另一会话的阻塞），
     再等在飞语句结束（等不到就 pg_cancel），最后回滚仍未提交的事务；
  5. 在控制连接上执行核对查询，收集结果。

超时：任何一步等待超过本轮时限即抛 RoundTimeout，本轮结束并按第 4 步回滚。
重开一轮总是从第 1 步的初始数据重新开始。
"""
from __future__ import annotations

import threading
import time

import psycopg2
from psycopg2 import extensions

ISOLATION_LEVELS = ("READ COMMITTED", "SERIALIZABLE")

MAX_ROWS = 50          # 单步 SELECT 结果最多带回前端的行数
POLL_INTERVAL = 0.05   # 阻塞确认轮询间隔（秒）
CLEANUP_GRACE = 2.0    # 收尾时等在飞语句完成的宽限（秒）


class RoundTimeout(Exception):
    """本轮超时：结束本轮，由收尾逻辑回滚未提交事务。"""


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
            self.log(self.name, "step_error", step=step.index, sql=step.sql,
                     pgcode=pgcode, error=msg, after_blocked=after_blocked)
            if pgcode in ("40001", "40P01"):  # 序列化失败 / 死锁
                self.outcome = "aborted"
                self.outcome_note = f"{pgcode} {msg}"
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

    # ---- 对外 ----

    def join_inflight(self, deadline):
        """执行同一会话的下一步前，先等上一条在飞语句结束。"""
        if self.inflight is None:
            return
        box, step = self.inflight
        self.inflight = None
        remaining = deadline - time.monotonic()
        if not box["event"].wait(max(remaining, 0.0)):
            self._cancel()
            box["event"].wait(1.0)
            self._collect(box, step, after_blocked=True)
            raise RoundTimeout()
        self._collect(box, step, after_blocked=True)

    def run_step(self, step, other, ctl, deadline):
        self.join_inflight(deadline)
        box = self._start(step.sql)
        self.log(self.name, "step_start", step=step.index, sql=step.sql, mode=step.mode)

        if step.mode == "complete":
            remaining = deadline - time.monotonic()
            if not box["event"].wait(max(remaining, 0.0)):
                self._cancel()
                box["event"].wait(1.0)
                self._collect(box, step, after_blocked=False)
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
                self.inflight = (box, step)
                self.log(self.name, "blocked", step=step.index, sql=step.sql,
                         blockers=blockers, by_other=other.pid in blockers,
                         detail="pg_blocking_pids → " + ",".join(map(str, blockers)))
                return
            if time.monotonic() >= deadline:
                self._cancel()
                box["event"].wait(1.0)
                self._collect(box, step, after_blocked=False)
                raise RoundTimeout()

    def rollback_if_open(self, timed_out):
        try:
            status = self.conn.get_transaction_status()
        except Exception:
            return
        if status == extensions.TRANSACTION_STATUS_IDLE:
            return
        try:
            cur = self.conn.cursor()
            cur.execute("ROLLBACK")
            cur.close()
            note = "超时回滚未提交事务" if timed_out else "脚本结束仍有未提交事务，引擎回滚"
            self.log(self.name, "rollback", detail=note)
            if self.outcome is None:
                self.outcome = "rolled_back"
                self.outcome_note = note
        except Exception as exc:
            pgcode, msg = _err_info(exc)
            self.log(self.name, "rollback_error", pgcode=pgcode, error=msg)

    def finish_inflight(self, grace):
        if self.inflight is None:
            return
        box, step = self.inflight
        self.inflight = None
        if not box["event"].wait(grace):
            self._cancel()
            box["event"].wait(1.0)
        if box["event"].is_set():
            self._collect(box, step, after_blocked=True)
        else:
            self.log(self.name, "step_error", step=step.index, sql=step.sql,
                     pgcode=None, error="语句无法取消，放弃连接", after_blocked=True)

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


def _run_check(ctl, check_sql):
    try:
        cur = ctl.cursor()
        cur.execute(check_sql)
        out = {"error": None, "pgcode": None, "columns": None, "rows": None,
               "status": cur.statusmessage}
        if cur.description:
            out["columns"] = [d.name for d in cur.description]
            out["rows"] = [[_normalize(v) for v in row] for row in cur.fetchmany(200)]
        cur.close()
        return out
    except Exception as exc:
        pgcode, msg = _err_info(exc)
        return {"error": msg, "pgcode": pgcode, "columns": None, "rows": None,
                "status": None}


def run_round(conninfo, init_sql, steps, check_sql, isolation, timeout_s):
    """跑一轮：重置 → 交错执行 → 收尾回滚 → 核对查询。"""
    events = []
    t0 = time.monotonic()

    def log(session, kind, **kw):
        events.append({"t": round(time.monotonic() - t0, 3),
                       "session": session, "kind": kind, **kw})

    result = {"isolation": isolation, "timed_out": False, "error": None,
              "events": events, "outcomes": {}, "check": None, "duration": None}
    deadline = t0 + timeout_s
    ctl = None
    sessions = []
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
                detail=f"超过 {timeout_s:g} 秒未推进，结束本轮，回滚未提交事务")
        finally:
            # 收尾顺序：先回滚没有在飞语句的事务（可能解除另一会话的阻塞），
            # 再等在飞语句结束（等不到就取消），最后回滚仍未提交的事务。
            grace = 1.0 if result["timed_out"] else CLEANUP_GRACE
            for s in sessions:
                if s.inflight is None:
                    s.rollback_if_open(result["timed_out"])
            for s in sessions:
                s.finish_inflight(grace)
            for s in sessions:
                s.rollback_if_open(result["timed_out"])
            for s in sessions:
                result["outcomes"][s.name] = {
                    "outcome": s.outcome or "unknown",
                    "note": s.outcome_note,
                    "pid": s.pid,
                }
                s.close()

        log("SYS", "check", detail="执行核对查询")
        result["check"] = _run_check(ctl, check_sql)
    except Exception as exc:
        pgcode, msg = _err_info(exc)
        result["error"] = msg
        log("SYS", "round_error", pgcode=pgcode, error=msg)
        for s in sessions:
            s.close()
    finally:
        result["duration"] = round(time.monotonic() - t0, 3)
        if ctl is not None:
            try:
                ctl.close()
            except Exception:
                pass
    return result


def run_experiment(conninfo, init_sql, steps, check_sql, timeout_s):
    """同一初始数据，分别按 READ COMMITTED 和 SERIALIZABLE 各跑一轮。"""
    return [run_round(conninfo, init_sql, steps, check_sql, iso, timeout_s)
            for iso in ISOLATION_LEVELS]
