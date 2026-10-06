"""Mailbox handlers for ask_supervisor + submit_for_review (used by trial-broker)."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

import trial_lib as T

MAILBOX = T.MAILBOX

# routes.json handler name → broker handler key
ROUTE_HANDLERS = {
    "supervisor_answer": "ask_supervisor",
    "gate_review": "submit_for_review",
}


def dispatch_target(ask: dict, routes: dict | list | None = None) -> str | None:
    """Map (to_role, kind) through routes.json to a broker handler key.

    Returns 'ask_supervisor' | 'submit_for_review' | None (no route).
    """
    route = T.find_route(
        str(ask.get("to_role") or ""),
        str(ask.get("kind") or "ask_supervisor"),
        routes,
    )
    if not route:
        return None
    return ROUTE_HANDLERS.get(str(route.get("handler") or ""))


def no_route_answer(ask_id: str) -> dict:
    """Structured answer for an unknown (to_role, kind) with no routes.json entry."""
    return {"ask_id": ask_id, "ok": False, "error": "no route"}


def gate_running_ack(
    ask_id: str,
    gate_ticket: str,
    gate_timeout_sec: int,
    started_at: str,
    gate_log: str | None = None,
) -> dict:
    """Interim ``answers/<ask_id>.json`` body written before the gate runs.

    Deliberately has NO ``verdict`` / ``ok`` / ``answer`` key: an old plugin that
    accepts the first answer-shaped body would otherwise treat this as final.
    The new plugin detects ``status == "gate_running"`` and keeps polling while
    extending its deadline to the gate's own budget.
    """
    body: dict[str, Any] = {
        "ask_id": ask_id,
        "status": "gate_running",
        "gate_ticket": gate_ticket,
        "gate_timeout_sec": int(gate_timeout_sec),
        "started_at": started_at,
        "progress": True,
    }
    if gate_log is not None:
        body["gate_log"] = str(gate_log)
    return body


def decide_and_answer_review(
    *,
    ask: dict,
    gate_block: dict,
    gate_ticket: str,
    limits: dict,
) -> tuple[str, dict]:
    """Build answer payload for submit_for_review. Returns (rework_mode, answer_dict)."""
    verdict = str(gate_block.get("verdict") or "").upper()
    findings = gate_block.get("findings") or []
    unmet = gate_block.get("unmet_acceptance") or []
    if verdict == "PASS":
        return "", {
            "ask_id": ask.get("ask_id"),
            "status": "done",
            "verdict": "PASS",
            "gate_ticket": gate_ticket,
            "findings": findings,
            "unmet_acceptance": unmet,
            "instruction": "PASS — write final summary status=done and stop.",
        }
    usage = int(ask.get("usage_prompt") or 0)
    mode = T.decide_rework_mode(usage_prompt=usage, findings=findings, limits=limits)
    if mode == "inplace":
        instruction = (
            "HOLD inplace — fix the listed findings in THIS ticket, then call submit_for_review again. "
            "Record a finding_resolutions entry for each finding in the summary machine block. "
            "Do not end the ticket with status=done while a P0/P1 is unresolved."
        )
    else:
        instruction = (
            "HOLD fresh — end THIS ticket now with status=done and notes containing rework_fresh "
            "(plus finding_resolutions). Broker will immediately open a NEW impl fix ticket with a "
            "findings-first pack and run the gate again. This is NOT a slice completion; do not treat "
            "the done as a clean PASS."
        )
    return mode, {
        "ask_id": ask.get("ask_id"),
        "status": "done",
        "verdict": "HOLD",
        "rework_mode": mode,
        "mode": mode,
        "gate_ticket": gate_ticket,
        "findings": findings,
        "unmet_acceptance": unmet,
        "instruction": instruction,
        "usage_prompt": usage,
    }
