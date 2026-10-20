"""Pause → supervisor five-way decision (G1 + wall-clock).

Pure helpers for unit tests. trial-broker wires these into chain routing.
"""
from __future__ import annotations

import re
from typing import Any

PAUSE_LIMIT = "paused_limit"
PAUSE_WALLCLOCK = "paused_wallclock"
PAUSE_KINDS = frozenset({PAUSE_LIMIT, PAUSE_WALLCLOCK})

# Five options (工头侧会话); escalate alone wakes Hub/human.
PAUSE_DECISIONS = frozenset({
    "continue_same",  # keep-open + resume 原会话
    "new_ticket",
    "close",
    "split",
    "escalate",
})

_HARD_CAP_RE = re.compile(r"timeout after \d+s hard cap", re.I)
_IDLE_RE = re.compile(r"timeout after \d+s idle", re.I)
_STEP_HARD_RE = re.compile(
    r"step-budget hard|status\s*=\s*paused_limit|硬阈值",
    re.I,
)


def classify_pause_reason(
    *,
    summary_status: str | None = None,
    log_text: str = "",
) -> str | None:
    """Return paused_limit | paused_wallclock | None."""
    st = str(summary_status or "").strip().lower()
    if st in PAUSE_KINDS:
        return st
    if st == "paused":
        return PAUSE_LIMIT
    section = log_text or ""
    if "pause_kind=paused_wallclock" in section:
        return PAUSE_WALLCLOCK
    if "pause_kind=paused_limit" in section:
        return PAUSE_LIMIT
    # Prefer explicit plugin / summary markers over wall-clock.
    if _STEP_HARD_RE.search(section):
        return PAUSE_LIMIT
    if _HARD_CAP_RE.search(section) or _IDLE_RE.search(section):
        return PAUSE_WALLCLOCK
    return None


def parse_pause_decision(block: dict | None, text: str = "") -> str | None:
    block = block if isinstance(block, dict) else {}
    for key in ("decision", "verdict", "choice", "action"):
        raw = str(block.get(key) or "").strip().lower()
        if raw in PAUSE_DECISIONS:
            return raw
        # allow action=pause_decision with nested decision
        if raw == "pause_decision":
            nested = str(block.get("decision") or "").strip().lower()
            if nested in PAUSE_DECISIONS:
                return nested
    blob = (text or "").lower()
    for opt in PAUSE_DECISIONS:
        if f'"decision": "{opt}"' in blob or f'"decision":"{opt}"' in blob:
            return opt
    return None


def build_pause_pack_body(
    *,
    slice_id: str,
    round_n: int,
    pause_kind: str,
    steps_used: int | None,
    step_budget: int | None,
    elapsed_sec: int | None,
    idle_timeout_sec: int | None,
    hard_timeout_sec: int | None,
    goal_id: str | None,
    pending_pack: str,
    impl_ticket: str,
    notes: str = "",
) -> str:
    """Markdown pack body for supervisor-pause decision."""
    kind = pause_kind if pause_kind in PAUSE_KINDS else PAUSE_LIMIT
    title = "步数硬阈值" if kind == PAUSE_LIMIT else "墙钟到点"
    steps_line = (
        f"- 已用步数: {steps_used} / 预算 {step_budget or '?'} "
        f"(硬阈值约 2×budget)"
        if steps_used is not None
        else f"- 已用步数: 未知 / 预算 {step_budget or '?'}"
    )
    time_line = (
        f"- 已用墙钟: {elapsed_sec}s "
        f"(idle={idle_timeout_sec or '?'}, hard={hard_timeout_sec or '?'})"
        if elapsed_sec is not None
        else f"- 墙钟: idle={idle_timeout_sec or '?'}, hard={hard_timeout_sec or '?'}"
    )
    suggest = (
        "建议 continue_same 并 +0.5×budget 步 / 或适量延长 hard timeout"
        if kind == PAUSE_LIMIT
        else "建议 continue_same 并延长 idle/hard，或 close/split"
    )
    return f"""---
kind: supervisor-pause
slice_id: {slice_id}
round: {round_n}
pause_kind: {kind}
goal: {goal_id or ''}
pending_impl_pack: {pending_pack}
impl_ticket: {impl_ticket}
summary_out: $DSH_HOME/supervisor/thin-state/summaries/{slice_id}-sup-pause-r{round_n}.md
---

# Supervisor pause decision · {slice_id} · r{round_n} · {title}

暂停点，不是失败。禁止机械 closeout / 直接交人。优先原会话继续。

## 已用量
{steps_line}
{time_line}
- 原因: `{kind}`
- 工头 notes: {(notes or '')[:800]}

## 建议
{suggest}

## Options（五选一，工头侧会话）
- **continue_same**: keep-open + resume 同一 impl 会话，追加步数/时长（优先）
- **new_ticket**: 同分支新开 impl 票（冷读，不优先）
- **close**: 按当前 WIP 收口（commit/PR 规则照旧）
- **split**: 拆分剩余工作；本链可 resume
- **escalate**: 真正升级到 Hub/人（仅此选项通知人）

## Instruction
写 summary，机器块：
```json
{{
  "action": "pause_decision",
  "slice": "{slice_id}",
  "decision": "continue_same|new_ticket|close|split|escalate",
  "add_steps": 0,
  "add_timeout_sec": 0,
  "notes": "one line"
}}
```
"""


def supervisor_ticket_name(goal_id: str, kind: str = "pause") -> str:
    """Stable Goal-level supervisor ticket id for resume reuse."""
    gid = re.sub(r"[^a-zA-Z0-9._-]+", "-", str(goal_id or "goal")).strip("-") or "goal"
    return f"supervisor-goal-{gid}"


def should_reuse_supervisor(goal: dict | None) -> tuple[bool, str]:
    """Whether to --resume the Goal-level supervisor ticket."""
    if not isinstance(goal, dict):
        return False, ""
    ticket = str(goal.get("supervisor_reuse_ticket") or "").strip()
    if not ticket:
        return False, ""
    if goal.get("supervisor_reuse_broken"):
        return False, ticket
    return True, ticket
