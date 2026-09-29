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
            "Do not end the ticket yet."
        )
    else:
        instruction = (
            "HOLD fresh — end THIS ticket now with status=done and notes containing rework_fresh; "
            "broker will open a new fix ticket with findings-only pack."
        )
    return mode, {
        "ask_id": ask.get("ask_id"),
        "verdict": "HOLD",
        "rework_mode": mode,
        "mode": mode,
        "gate_ticket": gate_ticket,
        "findings": findings,
        "unmet_acceptance": unmet,
        "instruction": instruction,
        "usage_prompt": usage,
    }
