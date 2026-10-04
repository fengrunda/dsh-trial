#!/usr/bin/env python3
"""Startup reconcile: a terminal chain must land on a Goal still marked running.

Live bug: the chain (and, for the done case, the close summary) already
finished, but the goal JSON write-back never landed, so the file stayed
``running``. A restart that does not adopt the chain leaves the board dirty
and, worse, may re-queue the Goal.

This is NOT the 1a8fe63 salvage. That path turns a *missing impl summary*
into PASS only when a gate PASS already exists, and only while a chain is
being closed. Startup reconcile:

  * adopts ``chain.state`` that is already terminal;
  * never invents PASS from a missing summary;
  * never flips ``cancelled``;
  * never touches a chain that is still in flight.

Fixtures are trimmed copies of the real thin-state fields (read 2026-10-04):

  * hub-kg-dsh-hub-read-wire-v1: goal status=running, slices s1+s2,
    both chains state=PASS last_verdict=PASS, close summary
    action=goal_done / goal_status=done. Goal file had no close_summary key;
    the file is the conventional summaries/goal-{id}-close.md.
  * hub-kg-reflect-proposals-read-v1: goal status=running, one slice,
    chain state=failed, error="foreman exit 1 without summary",
    last_verdict=null, no gate round, no close summary.

Run:
  python3 -m pytest broker/tests/test_startup_reconcile_unit.py -q
"""
from __future__ import annotations

import importlib.util
import json
import shutil
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = Path(__file__).resolve().parent / "fixtures"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location(
    "trial_broker_reconcile", ROOT / "trial-broker.py"
)
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)


def _patched_dirs(root: Path):
    names = ("goals", "processing", "outbox", "failed", "chains", "packs", "summaries", "artifacts")
    dirs = {n: root / n for n in names}
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return mock.patch.multiple(
        tb,
        GOALS=dirs["goals"],
        PROCESSING=dirs["processing"],
        OUTBOX=dirs["outbox"],
        FAILED=dirs["failed"],
        CHAINS=dirs["chains"],
        PACKS=dirs["packs"],
        SUMMARIES=dirs["summaries"],
        ARTIFACT_ROOT=dirs["artifacts"],
        maybe_offload_gc=lambda **k: None,
    ), dirs


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _put_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _install_wire(dirs: dict) -> dict:
    goal = _load("goal-hub-read-wire-running.json")
    _put_json(dirs["goals"] / f"{goal['goal']}.json", goal)
    for name in ("chain-hub-read-wire-s1.json", "chain-hub-read-wire-s2.json"):
        chain = _load(name)
        _put_json(dirs["chains"] / f"{chain['slice']}.json", chain)
    src = FIXTURES / "goal-hub-read-wire-close.md"
    dest = dirs["summaries"] / f"goal-{goal['goal']}-close.md"
    shutil.copyfile(src, dest)
    return goal


def _install_reflect(dirs: dict) -> dict:
    goal = _load("goal-reflect-proposals-running.json")
    _put_json(dirs["goals"] / f"{goal['goal']}.json", goal)
    chain = _load("chain-reflect-proposals-s1.json")
    _put_json(dirs["chains"] / f"{chain['slice']}.json", chain)
    return goal


def test_running_all_pass_close_goal_done_becomes_done(tmp_path: Path):
    """(a) running + each slice PASS + close action=goal_done → done, no enqueue."""
    patch, dirs = _patched_dirs(tmp_path)
    with patch:
        goal = _install_wire(dirs)
        before_chains = {
            p.name: p.read_text(encoding="utf-8") for p in dirs["chains"].glob("*.json")
        }
        inbox = tmp_path / "inbox"
        inbox.mkdir()
        with mock.patch.object(tb, "INBOX", inbox), mock.patch.object(
            tb, "list_pending", lambda: []
        ), mock.patch.object(tb, "run_chain_rounds") as run_rounds, mock.patch.object(
            tb, "run_goal_job"
        ) as run_goal, mock.patch.object(tb, "run_open_slice") as run_open, mock.patch.object(
            tb, "_slice_has_review_pass"
        ) as salvage:
            settled = tb.reconcile_terminal_goals_at_start()
            run_rounds.assert_not_called()
            run_goal.assert_not_called()
            run_open.assert_not_called()
            salvage.assert_not_called()
            assert list(inbox.iterdir()) == []
        written = json.loads((dirs["goals"] / f"{goal['goal']}.json").read_text(encoding="utf-8"))
        assert written["status"] == "done"
        assert written.get("reconciled_at_start") is True
        assert "goal_done" in str(written.get("reconcile_reason") or "")
        assert settled == [goal["goal"]]
        # chains are adopted, not rewritten
        after = {p.name: p.read_text(encoding="utf-8") for p in dirs["chains"].glob("*.json")}
        assert after == before_chains


def test_running_failed_without_summary_and_no_gate_pass_becomes_failed(tmp_path: Path):
    """(b) running + failed slice, no last_verdict, no gate PASS → failed, never PASS."""
    patch, dirs = _patched_dirs(tmp_path)
    with patch:
        goal = _install_reflect(dirs)
        with mock.patch.object(tb, "_slice_has_review_pass") as salvage:
            settled = tb.reconcile_terminal_goals_at_start()
            salvage.assert_not_called()
        written = json.loads((dirs["goals"] / f"{goal['goal']}.json").read_text(encoding="utf-8"))
        assert written["status"] == "failed"
        assert written["status"] != "done"
        assert written.get("last_chain_state") == "failed"
        assert "without summary" in str(written.get("error") or "")
        assert settled == [goal["goal"]]
        chain = json.loads(
            (dirs["chains"] / "hub-kg-reflect-proposals-read-v1-s1.json").read_text(encoding="utf-8")
        )
        assert chain["state"] == "failed"
        assert chain["state"] != "PASS"
        assert chain.get("last_verdict") in (None, "")


def test_cancelled_goal_stays_cancelled(tmp_path: Path):
    """(c) a goal already cancelled is not flipped, even if its chain is terminal."""
    patch, dirs = _patched_dirs(tmp_path)
    with patch:
        goal = _install_wire(dirs)
        goal["status"] = "cancelled"
        goal["cancel_reason"] = "goal-update"
        _put_json(dirs["goals"] / f"{goal['goal']}.json", goal)
        before = (dirs["goals"] / f"{goal['goal']}.json").read_text(encoding="utf-8")
        settled = tb.reconcile_terminal_goals_at_start()
        after = (dirs["goals"] / f"{goal['goal']}.json").read_text(encoding="utf-8")
        assert settled == []
        assert after == before
        assert json.loads(after)["status"] == "cancelled"


def test_in_flight_running_goal_is_left_alone(tmp_path: Path):
    """(d) planning, or a running goal whose chain is not terminal, is not touched."""
    patch, dirs = _patched_dirs(tmp_path)
    with patch:
        # planning: no slices, status is not running
        planning = {
            "goal": "hub-kg-trust-dial-switch-v1",
            "status": "planning",
            "slices": [],
        }
        _put_json(dirs["goals"] / "hub-kg-trust-dial-switch-v1.json", planning)
        # running, but the only chain is still running
        running = {
            "goal": "fixture-in-flight",
            "status": "running",
            "slices": ["fixture-in-flight-s1"],
        }
        _put_json(dirs["goals"] / "fixture-in-flight.json", running)
        _put_json(
            dirs["chains"] / "fixture-in-flight-s1.json",
            {"slice": "fixture-in-flight-s1", "state": "running", "last_verdict": None},
        )
        # running, chain parked awaiting_supervisor (not terminal)
        waiting = {
            "goal": "fixture-awaiting",
            "status": "running",
            "slices": ["fixture-awaiting-s1"],
        }
        _put_json(dirs["goals"] / "fixture-awaiting.json", waiting)
        _put_json(
            dirs["chains"] / "fixture-awaiting-s1.json",
            {
                "slice": "fixture-awaiting-s1",
                "state": "awaiting_supervisor",
                "last_verdict": None,
            },
        )
        before = {
            p.name: p.read_text(encoding="utf-8") for p in dirs["goals"].glob("*.json")
        }
        settled = tb.reconcile_terminal_goals_at_start()
        after = {p.name: p.read_text(encoding="utf-8") for p in dirs["goals"].glob("*.json")}
        assert settled == []
        assert after == before


def test_missing_summary_without_gate_pass_is_not_salvaged_to_pass(tmp_path: Path):
    """(e) startup reconcile must not turn a non-terminal, no-gate-PASS chain into PASS.

    1a8fe63 covers "summary missing but gate already PASS" on the close path.
    Here the chain is still ``running`` (not yet marked terminal) and there is
    no gate PASS. Reconcile leaves the goal running and does not call salvage.
    """
    patch, dirs = _patched_dirs(tmp_path)
    with patch:
        goal = {
            "goal": "fixture-no-gate",
            "status": "running",
            "slices": ["fixture-no-gate-s1"],
        }
        _put_json(dirs["goals"] / "fixture-no-gate.json", goal)
        chain = {
            "slice": "fixture-no-gate-s1",
            "state": "running",
            "last_verdict": None,
            "error": "foreman exit 1 without summary",
            "rounds": [
                {
                    "round": 1,
                    "impl_exit": 1,
                    "impl_artifacts": {"summary_exists": False},
                }
            ],
        }
        _put_json(dirs["chains"] / "fixture-no-gate-s1.json", chain)
        with mock.patch.object(tb, "_slice_has_review_pass") as salvage:
            settled = tb.reconcile_terminal_goals_at_start()
            salvage.assert_not_called()
        written = json.loads((dirs["goals"] / "fixture-no-gate.json").read_text(encoding="utf-8"))
        chain_after = json.loads(
            (dirs["chains"] / "fixture-no-gate-s1.json").read_text(encoding="utf-8")
        )
        assert settled == []
        assert written["status"] == "running"
        assert chain_after["state"] == "running"
        assert chain_after["state"] != "PASS"


def test_all_pass_without_goal_done_close_is_not_marked_done(tmp_path: Path):
    """All chains PASS but no close action=goal_done: do not invent done."""
    patch, dirs = _patched_dirs(tmp_path)
    with patch:
        goal = _install_wire(dirs)
        (dirs["summaries"] / f"goal-{goal['goal']}-close.md").unlink()
        settled = tb.reconcile_terminal_goals_at_start()
        written = json.loads((dirs["goals"] / f"{goal['goal']}.json").read_text(encoding="utf-8"))
        assert settled == []
        assert written["status"] == "running"
