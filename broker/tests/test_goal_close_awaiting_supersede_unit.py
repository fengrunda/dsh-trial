"""Exit close-out: done clears awaiting markers and supersedes parked siblings.

S3 of Goal dsh-max-rounds-split-closeout-20261024. ``superseded`` is terminal
for Goal close/reconcile but is never PASS and never counts as completed.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location(
    "trial_broker_close_awaiting_supersede", ROOT / "trial-broker.py"
)
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

T = tb.T
GOAL = "engine-entity-resolution-v1"
S1 = f"{GOAL}-s1"
S2 = f"{GOAL}-s2"


def _patched_dirs(root: Path):
    names = (
        "goals",
        "processing",
        "outbox",
        "failed",
        "chains",
        "packs",
        "summaries",
        "artifacts",
        "inbox",
    )
    dirs = {n: root / n for n in names}
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return (
        mock.patch.multiple(
            tb,
            GOALS=dirs["goals"],
            PROCESSING=dirs["processing"],
            OUTBOX=dirs["outbox"],
            FAILED=dirs["failed"],
            CHAINS=dirs["chains"],
            PACKS=dirs["packs"],
            SUMMARIES=dirs["summaries"],
            ARTIFACT_ROOT=dirs["artifacts"],
            INBOX=dirs["inbox"],
            maybe_offload_gc=lambda **k: None,
        ),
        dirs,
    )


def _write_goal(dirs, goal: dict) -> None:
    (dirs["goals"] / f"{goal['goal']}.json").write_text(
        json.dumps(goal, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def _write_chain(dirs, chain: dict) -> None:
    (dirs["chains"] / f"{chain['slice']}.json").write_text(
        json.dumps(chain, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def _read(dirs, sid: str) -> dict:
    return json.loads((dirs["chains"] / f"{sid}.json").read_text(encoding="utf-8"))


def test_supervisor_close_done_clears_awaiting_and_supersedes_parked(tmp_path: Path):
    """(a) done close: Goal not parked; parked sibling superseded; PASS untouched."""
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        _write_goal(
            dirs,
            {
                "goal": GOAL,
                "status": "running",
                "slices": [S1, S2],
                "awaiting_reason": "max_gate_rounds",
                "max_rounds_decision_pending": True,
                "metrics": T.empty_metrics(),
            },
        )
        _write_chain(dirs, {"slice": S1, "type": "chain", "state": "PASS"})
        _write_chain(
            dirs,
            {
                "slice": S2,
                "type": "chain",
                "state": "awaiting_supervisor",
                "awaiting_reason": "pause_split",
                "last_verdict": "HOLD",
                "supervisor_decision": "split",
                "rounds": [],
            },
        )
        chain = {"slice": S1, "state": "PASS", "goal": GOAL, "rounds": []}
        notified: list[str] = []
        with mock.patch.object(
            tb, "build_supervisor_close_pack", return_value="close.pack.md"
        ), mock.patch.object(
            tb,
            "run_supervisor_ticket",
            return_value=(0, {}, dirs["summaries"] / "goal-close.md"),
        ), mock.patch.object(
            tb,
            "_parse_supervisor_summary",
            return_value={"block": {"goal_status": "done"}},
        ), mock.patch.object(
            tb,
            "_notify_goal_terminal",
            side_effect=lambda gid: notified.append(gid),
        ):
            tb.maybe_supervisor_close(
                {"goal": GOAL, "cwd": str(tmp_path), "supervisor_close": True}, chain
            )

        goal = json.loads((dirs["goals"] / f"{GOAL}.json").read_text(encoding="utf-8"))
        assert goal["status"] == "done"
        assert "awaiting_reason" not in goal
        assert not goal.get("max_rounds_decision_pending")
        assert goal.get("superseded_slices") == [S2]
        assert notified == [GOAL]

        parked = _read(dirs, S2)
        assert parked["state"] == "superseded"
        assert parked["superseded_reason"] == "goal_close:done"
        assert parked["superseded_at"]
        assert "awaiting_reason" not in parked
        assert parked["last_verdict"] == "HOLD"
        assert parked["supervisor_decision"] == "split"

        # A slice that already PASSed is not touched and is not superseded.
        assert _read(dirs, S1)["state"] == "PASS"


def test_close_done_supersedes_parked_slice_instead_of_raising(tmp_path: Path):
    """(b) 1 PASS + 1 parked outside accept_slices → no raise, Goal done."""
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        _write_goal(
            dirs,
            {
                "goal": GOAL,
                "status": "escalated",
                "slices": [S1, S2],
                "awaiting_reason": "max_gate_rounds",
                "metrics": T.empty_metrics(),
            },
        )
        _write_chain(dirs, {"slice": S1, "type": "chain", "state": "PASS"})
        _write_chain(
            dirs,
            {
                "slice": S2,
                "type": "chain",
                "state": "awaiting_supervisor",
                "awaiting_reason": "pause_split",
                "rounds": [],
            },
        )
        goal = json.loads((dirs["goals"] / f"{GOAL}.json").read_text(encoding="utf-8"))
        tb._close_goal_as_done(goal, reason="ops ruling", accept_slices=["s1"])

        got = json.loads((dirs["goals"] / f"{GOAL}.json").read_text(encoding="utf-8"))
        assert got["status"] == "done"
        assert "awaiting_reason" not in got
        assert not got.get("max_rounds_decision_pending")
        assert got["close_done_accepted_slices"] == [S2]
        assert int(got["metrics"]["slices_completed"]) == 1  # superseded is not PASS

        parked = _read(dirs, S2)
        assert parked["state"] == "superseded"
        assert parked["superseded_reason"] == "goal_close_done:ops ruling"
        assert parked["salvage_note"] == "parked at close done; superseded"
        assert "awaiting_reason" not in parked

        close_md = Path(got["close_summary"]).read_text(encoding="utf-8")
        assert "action" in close_md and "goal_done" in close_md


def test_reconcile_done_accepts_superseded_sibling(tmp_path: Path):
    """(c) all PASS + superseded + close action=goal_done → reconcile done."""
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        close_path = dirs["summaries"] / f"goal-{GOAL}-close.md"
        close_path.write_text(
            "# close\n\n```json\n"
            + json.dumps({"action": "goal_done", "goal": GOAL, "goal_status": "done"})
            + "\n```\n",
            encoding="utf-8",
        )
        _write_goal(
            dirs,
            {
                "goal": GOAL,
                "status": "running",
                "slices": [S1, S2],
                "close_summary": str(close_path),
                "metrics": T.empty_metrics(),
            },
        )
        _write_chain(dirs, {"slice": S1, "type": "chain", "state": "PASS"})
        _write_chain(
            dirs,
            {
                "slice": S2,
                "type": "chain",
                "state": "superseded",
                "superseded_reason": "goal_close:done",
            },
        )
        goal = json.loads((dirs["goals"] / f"{GOAL}.json").read_text(encoding="utf-8"))
        assert tb.reconcile_running_goal(goal) == "done"
        got = json.loads((dirs["goals"] / f"{GOAL}.json").read_text(encoding="utf-8"))
        assert got["status"] == "done"
        assert int(got["metrics"]["slices_completed"]) == 1
