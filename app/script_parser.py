"""交错脚本解析器。

脚本由若干“步骤”组成，每个步骤以一行头部开始：

    @A complete     -- 会话 A，等语句执行完成再推进（默认模式）
    @B blocked      -- 会话 B，等数据库确认该语句被另一会话阻塞即推进

头部之后、下一个头部之前的所有行是该步骤的 SQL。
事务由引擎统一以 BEGIN ISOLATION LEVEL ... 开启，脚本里用 COMMIT / ROLLBACK 结束。
"""
from __future__ import annotations

import re
from dataclasses import dataclass

HEADER_RE = re.compile(r"^\s*@\s*(A|B)\b[\s:]*([\w一-鿿]*)?\s*$", re.IGNORECASE)

MODE_ALIASES = {
    "": "complete",
    "complete": "complete",
    "done": "complete",
    "完成": "complete",
    "blocked": "blocked",
    "blocking": "blocked",
    "阻塞": "blocked",
}

MAX_STEPS = 200


@dataclass
class Step:
    index: int    # 从 0 开始的步骤序号
    session: str  # 'A' 或 'B'
    mode: str     # 'complete' 或 'blocked'
    sql: str
    line: int     # 头部所在行号（从 1 开始），用于报错定位


class ScriptError(ValueError):
    """脚本格式错误。"""


def parse_script(text: str) -> list[Step]:
    steps: list[Step] = []
    session: str | None = None
    mode = "complete"
    header_line = 0
    buf: list[str] = []

    def flush() -> None:
        nonlocal buf
        if session is None:
            return
        sql = "\n".join(buf).strip()
        buf = []
        if not sql:
            raise ScriptError(f"第 {header_line} 行：@{session} 步骤没有 SQL 语句")
        steps.append(Step(index=len(steps), session=session, mode=mode, sql=sql, line=header_line))

    for lineno, raw in enumerate(text.splitlines(), start=1):
        m = HEADER_RE.match(raw)
        if m and m.group(1):
            # 只有“@A / @B 开头、后面是空或合法模式词”的行才视为头部
            token = (m.group(2) or "").lower()
            if token not in MODE_ALIASES:
                raise ScriptError(
                    f"第 {lineno} 行：未知模式 “{m.group(2)}”，"
                    "可用 complete（等完成）或 blocked（等被阻塞）"
                )
            flush()
            session = m.group(1).upper()
            mode = MODE_ALIASES[token]
            header_line = lineno
            continue
        if session is None:
            if not raw.strip() or raw.strip().startswith("--"):
                continue  # 脚本开头允许空行和 -- 注释
            raise ScriptError(f"第 {lineno} 行：SQL 必须放在 @A / @B 步骤头部之后")
        buf.append(raw)

    flush()
    if not steps:
        raise ScriptError("脚本为空：至少需要一个 @A 或 @B 步骤")
    if len(steps) > MAX_STEPS:
        raise ScriptError(f"步骤过多（{len(steps)} > {MAX_STEPS}）")
    return steps
