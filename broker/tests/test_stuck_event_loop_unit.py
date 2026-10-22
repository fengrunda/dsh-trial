#!/usr/bin/env python3
"""Unit tests: stuck event-loop remediation (gate spawn fail + orphan Goals).

Incident recap
--------------
``_handle_submit_for_review`` wrote the interim ``gate_running`` ack *before*
spawning the gate. When the spawn failed (drain / crash) the interim survived
forever and ``_fail_ask_after_error`` skipped the ask because an answer file
already existed, so the ticket hung on a phantom gate. Similarly a Goal stuck in
planning/running/closeout whose processing job vanished was merely logged
("left alone") and never advanced.

Rules under test:
  a) a BrokerDraining gate spawn clears the gate_running ack to a terminal
     ERROR, archives pending, and re-raises;
  b) ``_fail_ask_after_error`` overwrites an existing gate_running ack instead
     of skipping it;
  c) ``reconcile_orphan_running_goals`` escalates a running Goal with no
     processing job;
  d) the watchdog sweep warns on (dry-run) and clears (real) a stale
     gate_running ack with no live process.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from contextlib import ExitStack
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_bspec = importlib.util.spec_from_file_location("trial_broker_stuck", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(_bspec)
_bspec.loader.exec_module(tb)

_wspec = importlib.util.spec_from_file_location("trial_watchdog_stuck", ROOT / "trial-watchdog.py")
wd = importlib.util.module_from_spec(_wspec)
_wspec.loader.exec_module(wd)


def _patched_dirs(root: Path):
    names = ("goals", "chains", "packs", "summaries", "artifacts", "processing",
             "outbox", "failed", "inbox", "mailbox")
    d = {n: root / n for n in names}
    for p in d.values():
        p.mkdir(parents=True, exist_ok=True)
    stack = ExitStack()
    stack.enter_context(mock.patch.multiple(
        tb,
        GOALS=d["goals"], CHAINS=d["chains"], PACKS=d["packs"], SUMMARIES=d["summaries"],
        ARTIFACT_ROOT=d["artifacts"], PROCESSING=d["processing"], OUTBOX=d["outbox"],
        FAILED=d["failed"], INBOX=d["inbox"], MAILBOX=d["mailbox"],
    ))
    stack.enter_context(mock.patch.object(tb.T, "MAILBOX", d["mailbox"]))
    return stack, d


def _ask(ask_id: str, slice_id: str) -> dict:
    return {
        "ask_id": ask_id,
        "slice": slice_id,
        "summary": "did the thing",
        "changed_files": ["a.py"],
        "base": "",
        "commit": "",
        "usage_prompt": 100,
    }


def _seed_gate_running(mailbox: Path, ask_id: str, *, ticket: str = "gate-trial-x-rev1") -> Path:
    answers = mailbox / "answers"
    answers.mkdir(parents=True, exist_ok=True)
    path = answers / f"{ask_id}.json"
    path.write_text(
        json.dumps(
            {
                "ask_id": ask_id,
                "status": "gate_running",
                "gate_ticket": ticket,
                "started_at": "2020-01-01T00:00:00+00:00",
                "progress": True,
            }
        ),
        encoding="utf-8",
    )
    return path


# --------------------------------------------------------------------------
# a) gate spawn draining clears the interim ack and re-raises
# --------------------------------------------------------------------------
def test_gate_spawn_draining_clears_ack_and_reraises(tmp_path: Path):
    ctx, d = _patched_dirs(tmp_path / "state")
    ask_id = "ask-drain"
    slice_id = "drain"
    (d["packs"] / f"{slice_id}.pack.md").write_text(
        f"---\nslice_id: {slice_id}\n---\n# pack\n", encoding="utf-8"
    )
    pending = d["mailbox"] / "pending" / f"{ask_id}.json"
    pending.parent.mkdir(parents=True, exist_ok=True)
    pending.write_text(json.dumps(_ask(ask_id, slice_id)), encoding="utf-8")

    def boom(**kwargs):
        # run_open_slice raises BrokerDraining when the broker is draining
        raise tb.BrokerDraining("draining: not spawning gate")

    with ctx, mock.patch.object(tb, "run_open_slice", side_effect=boom):
        with pytest.raises(tb.BrokerDraining):
            tb._handle_submit_for_review(
                _ask(ask_id, slice_id), pending, default_profile="acp-lite",
                cwd=str(tmp_path), goal=None,
            )

    answer = json.loads((d["mailbox"] / "answers" / f"{ask_id}.json").read_text())
    assert answer["status"] == "done"
    assert answer["ok"] is False
    assert answer["verdict"] == "ERROR"
    assert "draining" in answer["error"]
    assert not pending.exists(), "pending ask must be archived"
    assert (d["mailbox"] / "archive" / f"{ask_id}.pending.json").is_file()


# --------------------------------------------------------------------------
# b) _fail_ask_after_error overwrites an existing gate_running ack
# --------------------------------------------------------------------------
def test_fail_ask_after_error_overwrites_gate_running(tmp_path: Path):
    ctx, d = _patched_dirs(tmp_path / "state")
    ask_id = "ask-fail"
    with ctx:
        ans = _seed_gate_running(d["mailbox"], ask_id)
        pending = d["mailbox"] / "pending" / f"{ask_id}.json"
        pending.parent.mkdir(parents=True, exist_ok=True)
        pending.write_text(json.dumps(_ask(ask_id, "s")), encoding="utf-8")

        tb._fail_ask_after_error(pending, {"ask_id": ask_id}, RuntimeError("kaboom"))

        body = json.loads(ans.read_text())
        assert body["status"] == "done"
        assert body["ok"] is False
        assert body["verdict"] == "ERROR"
        assert "kaboom" in body["error"]
        assert not pending.exists()
        assert (d["mailbox"] / "archive" / f"{ask_id}.pending.json").is_file()


# --------------------------------------------------------------------------
# c) reconcile_orphan_running_goals escalates a jobless running Goal
# --------------------------------------------------------------------------
def test_reconcile_orphan_running_goal_escalates(tmp_path: Path):
    ctx, d = _patched_dirs(tmp_path / "state")
    goal_id = "orphan-1"
    with ctx:
        (d["goals"] / f"{goal_id}.json").write_text(
            json.dumps({"goal": goal_id, "status": "running"}), encoding="utf-8"
        )
        escalated = tb.reconcile_orphan_running_goals()
        assert escalated == [goal_id]
        goal = json.loads((d["goals"] / f"{goal_id}.json").read_text())
        assert goal["status"] == "escalated"
        assert goal["escalate_reason"] == "orphan_running_no_processing"
        assert "processing job missing" in goal["error"]


def test_collect_resume_items_escalates_instead_of_leaving_alone(tmp_path: Path):
    ctx, d = _patched_dirs(tmp_path / "state")
    goal_id = "orphan-2"
    with ctx:
        (d["goals"] / f"{goal_id}.json").write_text(
            json.dumps({"goal": goal_id, "status": "planning"}), encoding="utf-8"
        )
        items = tb.collect_resume_items()
        assert items == []
        goal = json.loads((d["goals"] / f"{goal_id}.json").read_text())
        assert goal["status"] == "escalated"
        assert goal["escalate_reason"] == "orphan_running_no_processing"


# --------------------------------------------------------------------------
# d) watchdog sweep: dry-run warns, real pass clears and writes ERROR
# --------------------------------------------------------------------------
def _patch_watchdog_dirs(tmp_path: Path):
    dirs = {n: tmp_path / n for n in ("mailbox", "goals", "inbox", "processing", "chains")}
    for p in dirs.values():
        p.mkdir(parents=True, exist_ok=True)
    return mock.patch.multiple(
        wd,
        MAILBOX=dirs["mailbox"],
        GOALS=dirs["goals"],
        INBOX=dirs["inbox"],
        PROCESSING=dirs["processing"],
        CHAINS=dirs["chains"],
        HEARTBEAT=tmp_path / "heartbeat.json",
        OUTBOX=tmp_path / "outbox",
    ), dirs


def test_watchdog_stale_gate_running_dry_run_then_clear(tmp_path: Path):
    patch, d = _patch_watchdog_dirs(tmp_path)
    ask_id = "ask-stale"
    ticket = "gate-trial-stale-rev1"
    now = 1_800_000_000.0
    with patch:
        ans = _seed_gate_running(d["mailbox"], ask_id, ticket=ticket)

        # dry run: event only, nothing is touched
        events = wd.sweep_stale_gate_running_and_orphans(
            now=now, work_pids=[], dry_run=True
        )
        assert len(events) == 1
        assert events[0]["kind"] == wd.STALE_GATE_KIND
        assert events[0]["reason_key"] == f"stale_gate_running:{ask_id}"
        assert json.loads(ans.read_text())["status"] == "gate_running"
        assert not (d["mailbox"] / "archive").exists()

        # real pass: archive the interim and write a terminal ERROR
        events = wd.sweep_stale_gate_running_and_orphans(
            now=now, work_pids=[], dry_run=False
        )
        assert len(events) == 1
        body = json.loads(ans.read_text())
        assert body["status"] == "done"
        assert body["ok"] is False
        assert body["verdict"] == "ERROR"
        assert "stale gate_running" in body["error"]
        archived = d["mailbox"] / "archive" / f"{ask_id}.stale-gate-running.json"
        assert archived.is_file()


def test_watchdog_stale_gate_leaves_live_process_alone(tmp_path: Path):
    patch, d = _patch_watchdog_dirs(tmp_path)
    ask_id = "ask-live"
    ticket = "gate-trial-live-rev1"
    with patch, mock.patch.object(
        wd, "_read_proc_cmdline", return_value=["open-slice", "--ticket", ticket]
    ):
        _seed_gate_running(d["mailbox"], ask_id, ticket=ticket)
        events = wd.sweep_stale_gate_running_and_orphans(
            now=1_800_000_000.0, work_pids=[4242], dry_run=False
        )
        # the pid's cmdline is stubbed to carry the gate ticket
        assert events == []
        assert json.loads((d["mailbox"] / "answers" / f"{ask_id}.json").read_text())[
            "status"
        ] == "gate_running"


def test_watchdog_orphan_goal_hands_off_not_escalated(tmp_path: Path):
    patch, d = _patch_watchdog_dirs(tmp_path)
    goal_id = "watchdog-orphan"
    with patch, mock.patch.object(
        wd, "_read_proc_cmdline", return_value=["open-slice", "--ticket", "gate-x"]
    ):
        goal_path = d["goals"] / f"{goal_id}.json"
        goal_path.write_text(
            json.dumps(
                {"goal": goal_id, "status": "running",
                 "updated_at": "2020-01-01T00:00:00+00:00"}
            ),
            encoding="utf-8",
        )
        events = wd.sweep_stale_gate_running_and_orphans(
            now=1_800_000_000.0, work_pids=[4242], dry_run=True
        )
        assert [e["kind"] for e in events] == [wd.ORPHAN_HANDOFF_KIND]
        assert events[0]["status"] == "handoff"
        assert events[0]["goal"] == goal_id
        assert json.loads(goal_path.read_text())["status"] == "running"

        events = wd.sweep_stale_gate_running_and_orphans(
            now=1_800_000_000.0, work_pids=[4242], dry_run=False
        )
        assert [e["kind"] for e in events] == [wd.ORPHAN_HANDOFF_KIND]
        goal = json.loads(goal_path.read_text())
        assert goal["status"] == "running"
        assert "escalate_reason" not in goal
        assert goal["perceive_handoff"]["handed"] is True
        assert goal["perceive_handoff"]["ticket"] == f"supervisor-goal-{goal_id}"


def test_watchdog_skips_parked_chain(tmp_path: Path):
    patch, d = _patch_watchdog_dirs(tmp_path)
    goal_id = "watchdog-parked"
    sid = f"{goal_id}-s1"
    with patch:
        goal_path = d["goals"] / f"{goal_id}.json"
        goal_path.write_text(
            json.dumps(
                {
                    "goal": goal_id,
                    "status": "running",
                    "slices": [sid],
                    "current_slice": sid,
                    "updated_at": "2020-01-01T00:00:00+00:00",
                }
            ),
            encoding="utf-8",
        )
        (d["chains"] / f"{sid}.json").write_text(
            json.dumps({"slice": sid, "state": "awaiting_supervisor", "goal": goal_id}),
            encoding="utf-8",
        )
        events = wd.sweep_stale_gate_running_and_orphans(
            now=1_800_000_000.0, work_pids=[], dry_run=False
        )
        assert events == []
        assert json.loads(goal_path.read_text())["status"] == "running"


def test_reconcile_orphan_skips_awaiting_supervisor(tmp_path: Path):
    """Awaiting Goals without a processing job must not be orphan-escalated."""
    ctx, d = _patched_dirs(tmp_path / "state")
    goal_id = "orphan-awaiting"
    sid = f"{goal_id}-s1"
    with ctx:
        goal = {
            "goal": goal_id,
            "status": "running",
            "cwd": str(tmp_path / "repo"),
            "slices": [sid],
            "current_slice": sid,
            "last_chain_state": "awaiting_supervisor",
            "awaiting_reason": "pause_continue:paused_wallclock",
        }
        (d["goals"] / f"{goal_id}.json").write_text(
            json.dumps(goal, ensure_ascii=False), encoding="utf-8"
        )
        (d["chains"] / f"{sid}.json").write_text(
            json.dumps(
                {
                    "slice": sid,
                    "state": "awaiting_supervisor",
                    "awaiting_reason": "pause_continue:paused_wallclock",
                    "goal": goal_id,
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        escalated = tb.reconcile_orphan_running_goals()
        assert goal_id not in escalated
        after = json.loads((d["goals"] / f"{goal_id}.json").read_text(encoding="utf-8"))
        assert after["status"] == "running"
        assert after.get("escalate_reason") is None
