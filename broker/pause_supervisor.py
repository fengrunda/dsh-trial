"""Pause → supervisor five-way decision (G1 + wall-clock + upstream env).

Pure helpers for unit tests. trial-broker wires these into chain routing.
"""
from __future__ import annotations

import re
from typing import Any

PAUSE_LIMIT = "paused_limit"
PAUSE_WALLCLOCK = "paused_wallclock"
PAUSE_ENV = "paused_env"
PAUSE_KINDS = frozenset({PAUSE_LIMIT, PAUSE_WALLCLOCK, PAUSE_ENV})

# Five options (工头侧会话); escalate alone wakes Hub/human.
PAUSE_DECISIONS = frozenset({
    "continue_same",  # keep-open + resume 原会话
    "new_ticket",
    "close",
    "split",
    "escalate",
})

# Goal 级 supervisor 复用会话常把 pause 包答成触顶三选一。
# inplace = 同一切片有界继续，对应 pause 的 continue_same。
_MAX_ROUNDS_PAUSE_ALIASES = {
    "inplace": "continue_same",
    "split": "split",
    "escalate": "escalate",
}

_PROSE_DECISION_OPTS = (
    "continue_same",
    "new_ticket",
    "close",
    "split",
    "escalate",
    "inplace",
)

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
    """Return paused_limit | paused_wallclock | paused_env | None."""
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
    if "pause_kind=paused_env" in section or "env_blocked(" in section:
        return PAUSE_ENV
    # Prefer explicit plugin / summary markers over wall-clock.
    if _STEP_HARD_RE.search(section):
        return PAUSE_LIMIT
    if _HARD_CAP_RE.search(section) or _IDLE_RE.search(section):
        return PAUSE_WALLCLOCK
    return None


def _canonical_pause_decision(raw: object) -> str | None:
    token = str(raw or "").strip().lower()
    if token in PAUSE_DECISIONS:
        return token
    if token == "inplace":
        return "continue_same"
    return None


def _nonneg_int(value: object, default: int = 0) -> int:
    try:
        n = int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return default
    return n if n > 0 else 0


def pause_decision_name(parsed: object) -> str | None:
    """Decision token from ``parse_pause_decision`` (dict) or a legacy string."""
    if isinstance(parsed, str):
        token = parsed.strip()
        return token or None
    if isinstance(parsed, dict):
        token = str(parsed.get("decision") or "").strip()
        return token or None
    return None


def continue_same_step_budget(base: int, used: int | None, add_steps: int) -> int:
    """Steps left for a continue_same resume of the same impl session.

    ponytail: when ``used`` is unknown, return ``base + add_steps``. That can
    overshoot the 2× hard cap. Upgrade path: pass ``impl_steps`` from the
    round record or the ticket log.
    """
    extra = _nonneg_int(add_steps)
    base_n = max(0, _nonneg_int(base))
    if used is None:
        return max(1, base_n + extra)
    remaining = max(0, base_n - int(used))
    return max(1, remaining + extra)


def parse_pause_decision(block: dict | None, text: str = "") -> dict | None:
    """Five-way pause decision plus the continue_same budget deltas.

    Returns ``{"decision", "add_steps", "add_timeout_sec"}`` or None.
    ``add_*`` are 0 when the block omits them.

    Reused supervisor sessions often answer a pause pack with
    ``action=max_rounds_decision`` and ``decision=inplace|split|escalate``.
    Map that onto continue_same / split / escalate so the chain does not
    stay awaiting_supervisor. Canonical pause_decision + five-way unchanged.
    """
    block = block if isinstance(block, dict) else {}
    decision: str | None = None
    action = str(block.get("action") or "").strip().lower()
    if action == "max_rounds_decision":
        nested = str(block.get("decision") or "").strip().lower()
        mapped = _MAX_ROUNDS_PAUSE_ALIASES.get(nested)
        if mapped:
            decision = mapped
        elif nested in PAUSE_DECISIONS:
            decision = nested

    if decision is None:
        for key in ("decision", "verdict", "choice", "action"):
            raw = str(block.get(key) or "").strip().lower()
            mapped = _canonical_pause_decision(raw)
            if mapped:
                decision = mapped
                break
            # allow action=pause_decision with nested decision
            if raw == "pause_decision":
                nested_dec = _canonical_pause_decision(block.get("decision"))
                if nested_dec:
                    decision = nested_dec
                    break

    if decision is None:
        # Prose tokens, same shape as parse_max_rounds_decision.
        answer = str(block.get("answer") or "").strip().lower()
        for opt in _PROSE_DECISION_OPTS:
            if answer == opt or answer.startswith(opt + " ") or f"decision={opt}" in answer:
                decision = _canonical_pause_decision(opt)
                break
    if decision is None:
        blob = (text or "").lower()
        for opt in _PROSE_DECISION_OPTS:
            if f'"decision": "{opt}"' in blob or f'"decision":"{opt}"' in blob:
                decision = _canonical_pause_decision(opt)
                break
    if not decision:
        return None
    return {
        "decision": decision,
        "add_steps": _nonneg_int(block.get("add_steps")),
        "add_timeout_sec": _nonneg_int(block.get("add_timeout_sec")),
    }


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
    if pause_kind == "orphan_running_no_processing":
        kind = pause_kind
        title = "无活票交接"
        suggest = (
            "看门狗没看到心跳槽、活进程、processing。"
            "五选一；只有 escalate 才升级。"
        )
    else:
        kind = pause_kind if pause_kind in PAUSE_KINDS else PAUSE_LIMIT
        if kind == PAUSE_LIMIT:
            title = "步数硬阈值"
            suggest = "建议 continue_same 并 +0.5×budget 步 / 或适量延长 hard timeout"
        elif kind == PAUSE_ENV:
            title = "上游瞬态故障"
            suggest = "建议 continue_same 续跑原会话；这不是设计被否"
        else:
            title = "墙钟到点"
            suggest = "建议 continue_same 并延长 idle/hard，或 close/split"
    env_line = ""
    if kind == PAUSE_ENV:
        env_line = (
            "这是上游瞬态故障，票内已重试用尽。"
            "工作区可能有未提交改动。不要当成设计被否。\n"
            "continue_same 仍续跑原会话。\n\n"
        )
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

{env_line}## 已用量
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
本包只接受 pause 五选一。禁止 `action=max_rounds_decision`，禁止 `decision=inplace`。
inplace 是触顶三选一的用词，写在本包是错的；原会话继续请用 continue_same。
写 summary，机器块只许这一种：
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
