#!/usr/bin/env python3
"""goal-update raising max_supervisor_tickets must take effect and wake a parked pause.

Incident 2026-10-26 (hub-admin-overview-health-extract-sleep): goal-update only
wrote limits_overrides/limits_effective; the supervisor-cap checks read the stale
top-level value and nothing re-queued the chain parked at paused_limit.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import trial_lib as T  # noqa: E402

spec = importlib.util.spec_from_file_location(
    "trial_broker_goal_update_cap", ROOT / "trial-broker.py"
)
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)


def test_apply_goal_update_syncs_top_level_and_cap_helper():
    goal = {"goal": "g", "max_supervisor_tickets": 8}
    assert T.goal_supervisor_cap(goal) == 8
    T.apply_goal_update(goal, {"max_supervisor_tickets": 12})
    assert goal["max_supervisor_tickets"] == 12
    assert T.goal_supervisor_cap(goal) == 12
    # stale top-level alone must not beat an explicit override
    goal["max_supervisor_tickets"] = 8
    assert T.goal_supervisor_cap(goal) == 12
    assert T.goal_supervisor_cap(None, {"max_supervisor_tickets": 5}) == 5
    assert T.goal_supervisor_cap({}, {}) == 8


def test_load_job_keeps_supervisor_note(tmp_path: Path):
    p = tmp_path / "gu.json"
    p.write_text(json.dumps({
        "type": "goal-update", "goal": "g", "max_supervisor_tickets": 12,
        "supervisor_note": "rebase main + draft PR", "requeue_supervisor": True,
    }))
    job = tb.load_job(p)
    assert job["supervisor_note"] == "rebase main + draft PR"
    assert job["requeue_supervisor"] is True


def _patched(root: Path):
    names = ("goals", "processing", "outbox", "failed", "chains", "packs",
             "summaries", "artifacts", "inbox")
    dirs = {n: root / n for n in names}
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return mock.patch.multiple(
        tb, GOALS=dirs["goals"], PROCESSING=dirs["processing"], OUTBOX=dirs["outbox"],
        FAILED=dirs["failed"], CHAINS=dirs["chains"], PACKS=dirs["packs"],
        SUMMARIES=dirs["summaries"], ARTIFACT_ROOT=dirs["artifacts"], INBOX=dirs["inbox"],
        maybe_offload_gc=lambda **k: None,
        maybe_notify_hub=lambda *a, **k: {"sent": False},
    ), dirs


def _setup(dirs, tmp_path: Path, *, count: int):
    sid, gid = "cap-slice", "cap-goal"
    goal = {
        "goal": gid, "status": "running", "brief": "x", "slices": [sid],
        "cwd": str(tmp_path), "max_supervisor_tickets": 8,
        "supervisor_ticket_count": count, "awaiting_reason": "paused_limit",
        "last_chain_state": "awaiting_supervisor",
    }
    chain = {
        "slice": sid, "type": "chain", "state": "awaiting_supervisor", "goal": gid,
        "pack": f"{sid}.pack.md", "acceptance": ["a"], "profile": "acp-lite",
        "gate_profile": "acp-lite", "cwd": str(tmp_path), "max_rounds": 2,
        "rounds": [{"round": 1, "impl_ticket": "impl-r1", "impl_pack": f"{sid}.pack.md"}],
        "awaiting_reason": "paused_limit", "pending_impl_pack": f"{sid}.pack.md",
    }
    (dirs["goals"] / f"{gid}.json").write_text(json.dumps(goal))
    (dirs["chains"] / f"{sid}.json").write_text(json.dumps(chain))
    return sid, gid


def _gu(dirs, **extra) -> Path:
    p = dirs["inbox"] / "gu.json"
    body = {"type": "goal-update", "id": "gu-1", "goal": "cap-goal",
            "max_supervisor_tickets": 12}
    body.update(extra)
    p.write_text(json.dumps(body))
    return p


def test_goal_update_requeues_parked_pause_with_note(tmp_path: Path):
    patcher, dirs = _patched(tmp_path)
    with patcher:
        sid, gid = _setup(dirs, tmp_path, count=8)
        seen: dict = {}

        def _decide(job, chain, **kw):
            seen.update(kw)
            return "continue_same"

        def _dispatch(job, chain, dest, slice_id):
            seen["note"] = chain.get("pause_supervisor_note")
            return 0

        with mock.patch.object(tb, "auto_supervisor_pause_decision", _decide), \
             mock.patch.object(tb, "route_pause_decision",
                               lambda *a, **k: tb.CONTINUE_SAME_INLINE), \
             mock.patch.object(tb, "_dispatch_continue_same_rounds", _dispatch):
            rc = tb.run_job(_gu(dirs, supervisor_note="rebase main, draft PR"))
        assert rc == 0
        assert seen["pause_kind"] == "paused_limit"
        assert "rebase main" in seen["notes"]
        assert seen["note"] == "rebase main, draft PR"
        g = json.loads((dirs["goals"] / f"{gid}.json").read_text())
        assert g["max_supervisor_tickets"] == 12


def test_goal_update_requeue_skipped_when_cap_still_reached(tmp_path: Path):
    patcher, dirs = _patched(tmp_path)
    with patcher:
        sid, gid = _setup(dirs, tmp_path, count=12)
        with mock.patch.object(tb, "auto_supervisor_pause_decision",
                               side_effect=AssertionError("must not ask")):
            rc = tb.run_job(_gu(dirs))
        assert rc == 0
        ch = json.loads((dirs["chains"] / f"{sid}.json").read_text())
        assert ch["state"] == "awaiting_supervisor"


def test_requeue_stays_parked_when_supervisor_returns_nothing(tmp_path: Path):
    patcher, dirs = _patched(tmp_path)
    with patcher:
        sid, gid = _setup(dirs, tmp_path, count=8)
        with mock.patch.object(tb, "auto_supervisor_pause_decision",
                               lambda *a, **k: None):
            rc = tb.run_job(_gu(dirs))
        assert rc == 0
        ch = json.loads((dirs["chains"] / f"{sid}.json").read_text())
        assert ch["state"] == "awaiting_supervisor"
        assert ch["awaiting_reason"] == "paused_limit"
