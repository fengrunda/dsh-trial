"""goal-update action=done + resume-aware terminal notify."""
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
    "trial_broker_close_done", ROOT / "trial-broker.py"
)
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

T = tb.T
GOAL = "engine-entity-resolution-v1"
S1 = f"{GOAL}-s1"
S2 = f"{GOAL}-s2"
S3 = f"{GOAL}-s3"


def _patched_dirs(root: Path):
    names = ("goals", "processing", "outbox", "failed", "chains", "packs", "summaries", "artifacts", "inbox")
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
        INBOX=dirs["inbox"],
        maybe_offload_gc=lambda **k: None,
    ), dirs


def _write_goal(dirs, goal: dict) -> None:
    (dirs["goals"] / f"{goal['goal']}.json").write_text(
        json.dumps(goal, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def _write_chain(dirs, chain: dict) -> None:
    (dirs["chains"] / f"{chain['slice']}.json").write_text(
        json.dumps(chain, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def test_notify_kinds_ignore_pre_resume_events():
    goal = {
        "goal": GOAL,
        "resumed_at": "2026-10-05T16:30:20+08:00",
        "metrics": {
            "notify_events": [
                {
                    "at": "2026-10-05T16:24:49+08:00",
                    "kind": "dsh-trial-goal-failed",
                    "info": {"sent": True},
                }
            ]
        },
    }
    assert tb._notify_kinds_since_resume(goal) == set()
    goal["metrics"]["notify_events"].append(
        {
            "at": "2026-10-05T17:51:58+08:00",
            "kind": "dsh-trial-goal-failed",
            "info": {"sent": True},
        }
    )
    assert tb._notify_kinds_since_resume(goal) == {"dsh-trial-goal-failed"}


def test_close_done_accepts_escalated_s3(tmp_path: Path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        goal = {
            "goal": GOAL,
            "status": "escalated",
            "slices": [S1, S2, S3],
            "notify": "hub",
            "notify_dry_run": True,
            "metrics": T.empty_metrics(),
            "resumed_at": "2026-10-05T16:30:20+08:00",
        }
        goal["metrics"]["notify_events"] = [
            {
                "at": "2026-10-05T16:24:49+08:00",
                "kind": "dsh-trial-goal-failed",
                "info": {"sent": True},
            }
        ]
        goal["metrics"]["slices_completed"] = 2
        _write_goal(dirs, goal)
        for sid, state in ((S1, "PASS"), (S2, "PASS"), (S3, "escalated")):
            _write_chain(
                dirs,
                {
                    "slice": sid,
                    "type": "chain",
                    "state": state,
                    "last_verdict": "HOLD" if state == "escalated" else "PASS",
                    "rounds": [],
                    "goal": GOAL,
                },
            )

        notified = []

        def fake_notify(g, *, kind, ok, extra=None):
            notified.append(kind)
            g.setdefault("metrics", T.empty_metrics()).setdefault("notify_events", []).append(
                {"at": "2026-10-05T18:20:00+08:00", "kind": kind, "info": {"sent": True}}
            )
            tb._write_goal(g)

        with mock.patch.object(tb, "_notify_goal_event", side_effect=fake_notify):
            inbox = dirs["inbox"] / "close.json"
            inbox.write_text(
                json.dumps(
                    {
                        "type": "goal-update",
                        "goal": GOAL,
                        "action": "done",
                        "accept_slices": ["s3"],
                        "reason": "supervisor ruling: PR N/A, code already on main",
                        "id": "close-entity",
                    },
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
            rc = tb.run_goal_update_job(
                inbox,
                json.loads(inbox.read_text(encoding="utf-8")),
            )
        assert rc == 0
        got = json.loads((dirs["goals"] / f"{GOAL}.json").read_text(encoding="utf-8"))
        assert got["status"] == "done"
        assert got["close_done_accepted_slices"] == [S3]
        s3 = json.loads((dirs["chains"] / f"{S3}.json").read_text(encoding="utf-8"))
        assert s3["state"] == "PASS"
        assert "action=done" in s3["salvage_note"]
        close_md = Path(got["close_summary"])
        assert close_md.is_file()
        assert "goal_done" in close_md.read_text(encoding="utf-8")
        assert "dsh-trial-goal-complete" in notified


def test_close_done_rejects_running_slice(tmp_path: Path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        goal = {
            "goal": GOAL,
            "status": "escalated",
            "slices": [S1, S2],
            "metrics": T.empty_metrics(),
        }
        _write_goal(dirs, goal)
        _write_chain(dirs, {"slice": S1, "state": "PASS", "rounds": []})
        _write_chain(dirs, {"slice": S2, "state": "running", "rounds": []})
        try:
            tb._close_goal_as_done(goal, reason="x", accept_slices=["s2"])
            assert False, "expected ValueError"
        except ValueError as e:
            assert "running" in str(e)
