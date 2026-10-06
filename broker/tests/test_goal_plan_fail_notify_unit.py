"""A Goal whose supervisor-plan ticket fails must still wake Hub (failed notify)."""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_plan_fail", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

GOAL = "engine-plan-fail-notify-v1"


def _dirs(root: Path):
    names = ("goals", "processing", "outbox", "failed", "chains", "packs", "summaries", "artifacts", "inbox")
    d = {n: root / n for n in names}
    for p in d.values():
        p.mkdir(parents=True, exist_ok=True)
    patch = mock.patch.multiple(
        tb,
        GOALS=d["goals"], PROCESSING=d["processing"], OUTBOX=d["outbox"], FAILED=d["failed"],
        CHAINS=d["chains"], PACKS=d["packs"], SUMMARIES=d["summaries"], ARTIFACT_ROOT=d["artifacts"],
        INBOX=d["inbox"],
    )
    return patch, d


def _job(tmp: Path) -> tuple[Path, dict]:
    job = {
        "id": f"20261006-000000-goal-{GOAL}", "type": "goal", "goal": GOAL, "brief": "b",
        "cwd": str(tmp), "profile": "acp-lite-trial", "notify": "hub", "notify_dry_run": True,
    }
    path = tmp / "inbox" / f"{job['id']}.json"
    path.write_text(json.dumps(job), encoding="utf-8")
    return path, job


def test_plan_ticket_exit_without_summary_notifies_hub_failed(tmp_path):
    patch, d = _dirs(tmp_path)
    sent: list[str] = []

    def fake_notify(job, out_body, result, *, kind, extra_summary="", payload_extra=None):
        sent.append(kind)
        return {"sent": True, "kind": kind}

    with patch, mock.patch.object(tb, "build_goal_brief_pack", return_value="brief.pack.md"), \
            mock.patch.object(tb, "run_supervisor_ticket",
                              return_value=(1, {}, d["summaries"] / "missing-plan.md")), \
            mock.patch.object(tb, "maybe_notify_hub", side_effect=fake_notify):
        path, job = _job(tmp_path)
        rc = tb.run_goal_job(path, job)
        goal = json.loads((d["goals"] / f"{GOAL}.json").read_text())
    assert rc == 1
    assert goal["status"] == "failed"
    assert sent == ["dsh-trial-goal-failed"]
    assert [e["kind"] for e in goal["metrics"]["notify_events"]] == ["dsh-trial-goal-failed"]


def test_brief_pack_error_notifies_hub_failed(tmp_path):
    patch, d = _dirs(tmp_path)
    sent: list[str] = []
    with patch, mock.patch.object(tb, "build_goal_brief_pack", side_effect=ValueError("bad")), \
            mock.patch.object(tb, "maybe_notify_hub",
                              side_effect=lambda *a, kind, **k: sent.append(kind) or {"sent": True}):
        path, job = _job(tmp_path)
        assert tb.run_goal_job(path, job) == 1
    assert sent == ["dsh-trial-goal-failed"]
