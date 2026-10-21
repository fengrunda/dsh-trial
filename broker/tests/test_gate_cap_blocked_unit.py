"""Slice 2 — gate-cap blocked stays in the supervisor decision lane.

Covered here:
  a. ``_blocked_from_gate_cap`` recognises chain/goal gate-cap markers;
  b. chain ``summary(status=blocked)`` after a gate-cap HOLD keeps
     ``awaiting_reason=max_gate_rounds``, emits no ``AWAITING_HUB_NOTIFY``
     event, and opens the decision short ticket exactly once;
  c. idempotency: a repeated blocked with ``ticket_opened=True`` spawns no
     second short ticket and still notifies nothing;
  d. ticket-job twin: ``_park_ticket_gate_cap_blocked`` keeps the Goal running
     in the decision lane, and ``handle_ticket_max_rounds_touch(notify_hub=
     False)`` stays silent (``_notify_awaiting_hub`` not called);
  e. control: a plain blocked (no gate-cap marker) still wakes the Hub once.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_gate_cap_blocked", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

SLICE = "s2-slice"


@contextmanager
def _patched_dirs(root: Path):
    names = ("goals", "chains", "packs", "summaries", "artifacts", "processing",
             "outbox", "failed", "inbox", "mailbox")
    dirs = {n: root / n for n in names}
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    with mock.patch.multiple(
        tb,
        GOALS=dirs["goals"], CHAINS=dirs["chains"], PACKS=dirs["packs"],
        SUMMARIES=dirs["summaries"], ARTIFACT_ROOT=dirs["artifacts"],
        PROCESSING=dirs["processing"], OUTBOX=dirs["outbox"], FAILED=dirs["failed"],
        INBOX=dirs["inbox"], MAILBOX=dirs["mailbox"],
        maybe_offload_gc=lambda **k: None,
    ), mock.patch.object(tb.T, "MAILBOX", dirs["mailbox"]):
        yield dirs


def _seed_goal(dirs, *, ticket_opened: bool, awaiting: str = "max_gate_rounds") -> dict:
    goal = {
        "goal": "g-test",
        "status": "running",
        "current_pack": f"{SLICE}.pack.md",
        "awaiting_reason": awaiting,
        "max_gate_rounds_decision_pending": {
            "at": "2026-10-21T00:00:00Z",
            "slice": SLICE,
            "cap": 2,
            "round_n": 3,
            "ticket_opened": ticket_opened,
        },
        "supervisor_events": [],
    }
    (dirs["goals"] / "g-test.json").write_text(json.dumps(goal) + "\n", encoding="utf-8")
    return goal


def _seed_chain(dirs, **overrides) -> dict:
    chain = {
        "slice": SLICE,
        "goal": "g-test",
        "state": "awaiting_supervisor",
        "pack": f"{SLICE}.pack.md",
        "pending_impl_pack": f"{SLICE}.pack.md",
        "rounds": [],
    }
    chain.update(overrides)
    (dirs["chains"] / f"{SLICE}.json").write_text(json.dumps(chain) + "\n", encoding="utf-8")
    return chain


def _decision_recorder(calls: list):
    def _fake(job, chain, **kw):
        calls.append(kw)
        chain.setdefault("supervisor_events", []).append(
            {"kind": "max_rounds_decision", "ticket": f"supervisor-maxgaterounds-{SLICE}-r3"}
        )
        tb._write_chain(chain)
        return "inplace"

    return _fake


# --------------------------------------------------------------------------
# a. Recognition helper
# --------------------------------------------------------------------------
def test_blocked_from_gate_cap_markers():
    assert tb._blocked_from_gate_cap({"awaiting_reason": "max_gate_rounds"}, None) is True
    assert tb._blocked_from_gate_cap({"max_gate_rounds_decision_pending": {}}, None) is True
    assert tb._blocked_from_gate_cap(None, {"awaiting_reason": "max_gate_rounds"}) is True
    assert tb._blocked_from_gate_cap({"awaiting_reason": "blocked"}, {"awaiting_reason": "blocked"}) is False
    assert tb._blocked_from_gate_cap(None, None) is False


# --------------------------------------------------------------------------
# b. chain side: grouped blocked → decision lane, no Hub notify, ticket opened
# --------------------------------------------------------------------------
def test_chain_gate_cap_blocked_stays_in_decision_lane(tmp_path):
    with _patched_dirs(tmp_path) as dirs:
        goal = _seed_goal(dirs, ticket_opened=False)
        chain = _seed_chain(dirs, awaiting_reason="max_gate_rounds", supervisor_events=[])
        calls: list = []
        job = {"goal": "g-test", "cwd": str(tmp_path), "id": "job-1"}
        with mock.patch.object(tb, "_notify_awaiting_hub") as notify, \
                mock.patch.object(tb, "auto_supervisor_max_rounds_decision",
                                  side_effect=_decision_recorder(calls)):
            rc = tb._handle_foreman_blocked(
                job, chain, dirs["processing"] / "job-1.json",
                slice_id=SLICE, round_n=3, pack_for_impl=f"{SLICE}.pack.md",
                goal_obj=goal, block={"status": "blocked", "notes": "gate cap"},
            )

        assert rc == 0
        assert notify.call_count == 0, "gate-cap blocked must not wake the Hub"
        assert len(calls) == 1 and calls[0]["kind"] == "max_gate_rounds"
        assert chain["state"] == "awaiting_supervisor"
        assert chain["awaiting_reason"] == "max_gate_rounds"
        assert chain["gate_cap_blocked"]["round_n"] == 3
        saved = json.loads((dirs["goals"] / "g-test.json").read_text(encoding="utf-8"))
        assert saved["max_gate_rounds_decision_pending"]["ticket_opened"] is True
        assert saved["status"] == "running"


# --------------------------------------------------------------------------
# c. idempotency: no second decision short ticket
# --------------------------------------------------------------------------
def test_chain_gate_cap_blocked_is_idempotent(tmp_path):
    with _patched_dirs(tmp_path) as dirs:
        goal = _seed_goal(dirs, ticket_opened=True)
        chain = _seed_chain(dirs, awaiting_reason="max_gate_rounds", supervisor_events=[])
        calls: list = []
        job = {"goal": "g-test", "cwd": str(tmp_path), "id": "job-2"}
        with mock.patch.object(tb, "_notify_awaiting_hub") as notify, \
                mock.patch.object(tb, "auto_supervisor_max_rounds_decision",
                                  side_effect=_decision_recorder(calls)):
            rc = tb._handle_foreman_blocked(
                job, chain, dirs["processing"] / "job-2.json",
                slice_id=SLICE, round_n=3, pack_for_impl=f"{SLICE}.pack.md",
                goal_obj=goal, block={"status": "blocked"},
            )

        assert rc == 0
        assert calls == [], "a repeated blocked must not open a second ticket"
        assert notify.call_count == 0
        assert chain["awaiting_reason"] == "max_gate_rounds"
        saved = json.loads((dirs["goals"] / "g-test.json").read_text(encoding="utf-8"))
        assert saved["max_gate_rounds_decision_pending"]["ticket_opened"] is True


# --------------------------------------------------------------------------
# d. ticket-job twin
# --------------------------------------------------------------------------
def test_ticket_gate_cap_blocked_stays_running_in_decision_lane(tmp_path):
    with _patched_dirs(tmp_path) as dirs:
        goal = _seed_goal(dirs, ticket_opened=False)
        calls: list = []
        job = {"ticket": "impl-s2", "goal": "g-test", "cwd": str(tmp_path)}
        with mock.patch.object(tb, "_notify_awaiting_hub") as notify, \
                mock.patch.object(tb, "auto_supervisor_max_rounds_decision",
                                  side_effect=_decision_recorder(calls)):
            ok = tb._park_ticket_gate_cap_blocked(
                job, goal, goal_id="g-test", slice_id=SLICE,
                round_n=3, block={"status": "blocked"},
            )

        assert ok is True
        assert notify.call_count == 0
        assert len(calls) == 1 and calls[0]["kind"] == "max_gate_rounds"
        saved = json.loads((dirs["goals"] / "g-test.json").read_text(encoding="utf-8"))
        assert saved["status"] == "running"
        assert saved["awaiting_reason"] == "max_gate_rounds"
        assert saved["max_gate_rounds_decision_pending"]["ticket_opened"] is True


def test_ticket_max_rounds_touch_notify_hub_false_is_silent(tmp_path):
    with _patched_dirs(tmp_path) as dirs:
        goal = _seed_goal(dirs, ticket_opened=True, awaiting="max_rounds")
        job = {"ticket": "impl-s2", "goal": "g-test", "cwd": str(tmp_path),
               "auto_supervisor_answer": None}
        with mock.patch.object(tb, "_notify_awaiting_hub") as notify, \
                mock.patch.object(tb, "auto_supervisor_max_rounds_decision", return_value=None):
            outcome = tb.handle_ticket_max_rounds_touch(
                job, goal_id="g-test", goal_obj=goal, round_n=2, max_rounds=2,
                findings=[], reason="gate cap", handoff={}, slice_id=SLICE,
                pack_for_impl=f"{SLICE}.pack.md", notify_hub=False,
            )
        assert outcome == "parked"
        assert notify.call_count == 0

        with mock.patch.object(tb, "_notify_awaiting_hub") as notify_on, \
                mock.patch.object(tb, "auto_supervisor_max_rounds_decision", return_value=None):
            outcome_on = tb.handle_ticket_max_rounds_touch(
                job, goal_id="g-test", goal_obj=goal, round_n=2, max_rounds=2,
                findings=[], reason="gate cap", handoff={}, slice_id=SLICE,
                pack_for_impl=f"{SLICE}.pack.md",
            )
        assert outcome_on == "parked"
        assert notify_on.call_count == 1, "default notify_hub=True keeps old behaviour"


# --------------------------------------------------------------------------
# e. control: plain blocked still notifies the Hub exactly once
# --------------------------------------------------------------------------
def test_plain_blocked_still_notifies_hub(tmp_path):
    with _patched_dirs(tmp_path) as dirs:
        goal = {"goal": "g-test", "status": "running"}
        (dirs["goals"] / "g-test.json").write_text(json.dumps(goal) + "\n", encoding="utf-8")
        chain = _seed_chain(dirs, state="running", round_n=1, last_verdict="HOLD")
        job = {"goal": "g-test", "cwd": str(tmp_path), "id": "job-3"}
        with mock.patch.object(tb, "_notify_awaiting_hub") as notify:
            rc = tb._handle_foreman_blocked(
                job, chain, dirs["processing"] / "job-3.json",
                slice_id=SLICE, round_n=1, pack_for_impl=f"{SLICE}.pack.md",
                goal_obj=goal, block={"status": "blocked", "notes": "human needed"},
            )

        assert rc == 0
        assert notify.call_count == 1
        assert notify.call_args.kwargs["reason"] == "blocked"
        assert chain["awaiting_reason"] == "blocked"
