#!/usr/bin/env python3
"""goal-update wakes max_rounds parks and closes parked slices on action=done.

Incident 2026-10-26: hub-admin-graph-crosspage-focus parked at max_rounds; a
goal-update raising max_supervisor_tickets did not re-ask the supervisor because
only paused_* parks were woken. hub-admin-overview-health-extract-sleep: Hub
closed manually, but goal-update action=done was rejected on an
awaiting_supervisor slice (accept_slices was also dropped by load_job).
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location(
    "trial_broker_mr_wake", ROOT / "trial-broker.py"
)
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)
T = tb.T


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


def _setup(dirs, tmp_path: Path, *, count: int, reason: str = "max_rounds"):
    sid, gid = "mr-slice", "mr-goal"
    goal = {
        "goal": gid, "status": "running", "brief": "x", "slices": [sid],
        "cwd": str(tmp_path), "max_supervisor_tickets": 8,
        "supervisor_ticket_count": count, "awaiting_reason": reason,
        "last_chain_state": "awaiting_supervisor", "metrics": T.empty_metrics(),
    }
    chain = {
        "slice": sid, "type": "chain", "state": "awaiting_supervisor", "goal": gid,
        "pack": f"{sid}.pack.md", "acceptance": ["a"], "profile": "acp-lite",
        "gate_profile": "acp-lite", "cwd": str(tmp_path), "max_rounds": 2,
        "rounds": [{"round": 2, "impl_ticket": "impl-r2", "impl_pack": f"{sid}.pack.md"}],
        "awaiting_reason": reason, "awaiting_answer_for_round": 2,
        "pending_impl_pack": f"{sid}.pack.md",
        "max_rounds_decision_pending": {
            "round_n": 2, "max_rounds": 2, "reason": "HOLD at max_rounds=2",
            "findings": [{"tier": "P1", "issue": "x"}],
        },
    }
    (dirs["goals"] / f"{gid}.json").write_text(json.dumps(goal))
    (dirs["chains"] / f"{sid}.json").write_text(json.dumps(chain))
    return sid, gid


def _gu(dirs, **extra) -> Path:
    p = dirs["inbox"] / "gu.json"
    body = {"type": "goal-update", "id": "gu-1", "goal": "mr-goal",
            "max_supervisor_tickets": 12}
    body.update(extra)
    p.write_text(json.dumps(body))
    return p


def test_load_job_passes_accept_slices(tmp_path: Path):
    p = tmp_path / "gu.json"
    p.write_text(json.dumps({"type": "goal-update", "goal": "g", "action": "done",
                             "accept_slices": ["s1"], "reason": "r"}))
    job = tb.load_job(p)
    assert job["accept_slices"] == ["s1"]


def test_wakeable_reasons():
    for r in ("paused_limit", "paused_wallclock", "paused_env",
              "max_rounds", "max_rounds_split"):
        assert tb._is_wakeable_park_reason(r)
    assert not tb._is_wakeable_park_reason("max_gate_rounds")
    assert not tb._is_wakeable_park_reason("")


def test_goal_update_wakes_max_rounds_inplace(tmp_path: Path):
    patcher, dirs = _patched(tmp_path)
    with patcher:
        sid, gid = _setup(dirs, tmp_path, count=8)
        seen: dict = {}

        def _decide(job, chain, **kw):
            seen.update(kw)
            return "inplace"

        def _reply(path, job):
            seen["reply"] = job
            return 0

        with mock.patch.object(tb, "auto_supervisor_max_rounds_decision", _decide), \
             mock.patch.object(tb, "run_chain_reply_job", _reply):
            rc = tb.run_job(_gu(dirs, supervisor_note="only fix the one P1"))
        assert rc == 0
        assert seen["round_n"] == 2 and seen["max_rounds"] == 2
        assert "only fix the one P1" in seen["reason"]
        assert seen["findings"][0]["tier"] == "P1"
        assert seen["reply"]["decision"] == "inplace"
        assert seen["reply"]["slice"] == sid
        assert "only fix the one P1" in seen["reply"]["answer"]
        g = json.loads((dirs["goals"] / f"{gid}.json").read_text())
        assert g["max_supervisor_tickets"] == 12


def test_goal_update_max_rounds_skipped_when_cap_still_reached(tmp_path: Path):
    patcher, dirs = _patched(tmp_path)
    with patcher:
        sid, gid = _setup(dirs, tmp_path, count=12)
        with mock.patch.object(tb, "auto_supervisor_max_rounds_decision",
                               side_effect=AssertionError("must not ask")):
            rc = tb.run_job(_gu(dirs))
        assert rc == 0
        ch = json.loads((dirs["chains"] / f"{sid}.json").read_text())
        assert ch["state"] == "awaiting_supervisor"
        assert ch["awaiting_reason"] == "max_rounds"


def test_max_rounds_wake_stays_parked_when_supervisor_returns_nothing(tmp_path: Path):
    patcher, dirs = _patched(tmp_path)
    with patcher:
        sid, gid = _setup(dirs, tmp_path, count=8)
        with mock.patch.object(tb, "auto_supervisor_max_rounds_decision",
                               lambda *a, **k: None), \
             mock.patch.object(tb, "run_chain_reply_job",
                               side_effect=AssertionError("no reply")):
            rc = tb.run_job(_gu(dirs))
        assert rc == 0
        ch = json.loads((dirs["chains"] / f"{sid}.json").read_text())
        assert ch["state"] == "awaiting_supervisor"
        assert ch["awaiting_reason"] == "max_rounds"
        g = json.loads((dirs["goals"] / f"{gid}.json").read_text())
        assert g["awaiting_reason"] == "max_rounds"


def test_close_done_supersedes_parked_when_accept_all(tmp_path: Path):
    patcher, dirs = _patched(tmp_path)
    with patcher:
        sid, gid = _setup(dirs, tmp_path, count=8, reason="paused_limit")
        goal = json.loads((dirs["goals"] / f"{gid}.json").read_text())
        tb._close_goal_as_done(goal, reason="hub closed", accept_slices=None)
        got = json.loads((dirs["goals"] / f"{gid}.json").read_text())
        assert got["status"] == "done"
        ch = json.loads((dirs["chains"] / f"{sid}.json").read_text())
        assert ch["state"] == "superseded"
        assert int(got["metrics"]["slices_completed"]) == 0


def test_close_done_goal_update_accepts_explicit_parked_slice(tmp_path: Path):
    patcher, dirs = _patched(tmp_path)
    with patcher:
        sid, gid = _setup(dirs, tmp_path, count=8)
        with mock.patch.object(tb, "_notify_goal_terminal", lambda g: None):
            rc = tb.run_job(_gu(dirs, action="done", accept_slices=[sid],
                                reason="hub closed", max_supervisor_tickets=None))
        assert rc == 0
        g = json.loads((dirs["goals"] / f"{gid}.json").read_text())
        assert g["status"] == "done"
        assert "awaiting_reason" not in g
        ch = json.loads((dirs["chains"] / f"{sid}.json").read_text())
        assert ch["state"] == "PASS" and ch["ops_close_done"] is True
        assert "awaiting_reason" not in ch


def test_close_done_goal_update_without_accept_supersedes(tmp_path: Path):
    patcher, dirs = _patched(tmp_path)
    with patcher:
        sid, gid = _setup(dirs, tmp_path, count=8, reason="paused_limit")
        with mock.patch.object(tb, "_notify_goal_terminal", lambda g: None):
            rc = tb.run_job(_gu(dirs, action="done", reason="hub closed",
                                max_supervisor_tickets=None))
        assert rc == 0
        g = json.loads((dirs["goals"] / f"{gid}.json").read_text())
        assert g["status"] == "done"
        ch = json.loads((dirs["chains"] / f"{sid}.json").read_text())
        assert ch["state"] == "superseded"
