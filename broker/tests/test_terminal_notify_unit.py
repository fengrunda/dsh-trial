"""Addendum ① — every Goal-terminal path must wake Hub.

These tests lock the newly covered return paths (missing pack, startup
reconcile, supervisor close, closeout) plus the unified
``goal`` / ``status`` / ``reason`` / ``suggested_action`` payload.
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

spec = importlib.util.spec_from_file_location("trial_broker_terminal", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)


def _dirs(root: Path):
    names = ("goals", "chains", "packs", "summaries", "artifacts", "processing", "outbox",
             "failed", "inbox", "mailbox")
    d = {n: root / n for n in names}
    for p in d.values():
        p.mkdir(parents=True, exist_ok=True)
    patch = mock.patch.multiple(
        tb,
        GOALS=d["goals"], CHAINS=d["chains"], PACKS=d["packs"], SUMMARIES=d["summaries"],
        ARTIFACT_ROOT=d["artifacts"], PROCESSING=d["processing"], OUTBOX=d["outbox"],
        FAILED=d["failed"], INBOX=d["inbox"], MAILBOX=d["mailbox"],
    )
    return patch, d


def _goal(path: Path, **over) -> dict:
    goal = {"goal": "g1", "status": "failed", "slices": ["g1-s1"], "notify": "hub",
            "supervisor_ticket_count": 0, "metrics": {}}
    goal.update(over)
    path.write_text(json.dumps(goal), encoding="utf-8")
    return goal


# --- payload shape -----------------------------------------------------------

def test_notify_goal_event_carries_reason_and_suggested_action(tmp_path):
    patch, d = _dirs(tmp_path)
    goal = _goal(d["goals"] / "g1.json")
    sent: list[dict] = []

    def fake_notify(job, out_body, result, *, kind, extra_summary="", payload_extra=None):
        sent.append({"kind": kind, "payload_extra": payload_extra, "summary": extra_summary})
        return {"sent": True}

    with patch, mock.patch.object(tb, "maybe_notify_hub", side_effect=fake_notify):
        tb._notify_goal_event(goal, kind="dsh-trial-goal-failed", ok=False, reason="boom")
    assert sent[0]["kind"] == "dsh-trial-goal-failed"
    assert sent[0]["payload_extra"]["reason"] == "boom"
    assert sent[0]["payload_extra"]["suggested_action"]
    assert "reason=boom" in sent[0]["summary"]


# --- terminal dispatch -------------------------------------------------------

def test_notify_goal_terminal_closeout_uses_limit_kind(tmp_path):
    patch, d = _dirs(tmp_path)
    _goal(d["goals"] / "g1.json", status="closeout", closeout_note="hard cap")
    called: list[dict] = []
    with patch, mock.patch.object(tb, "_notify_goal_event",
                                  side_effect=lambda goal, **kw: called.append(kw)):
        tb._notify_goal_terminal("g1")
    assert called and called[0]["kind"] == "dsh-trial-limit"


def test_notify_goal_terminal_done_and_failed(tmp_path):
    patch, d = _dirs(tmp_path)
    _goal(d["goals"] / "g1.json", status="done")
    kinds: list[str] = []
    with patch, mock.patch.object(tb, "_notify_goal_event",
                                  side_effect=lambda goal, **kw: kinds.append(kw["kind"])):
        tb._notify_goal_terminal("g1")
        _goal(d["goals"] / "g1.json", status="escalated")
        tb._notify_goal_terminal("g1")
    assert kinds == ["dsh-trial-goal-complete", "dsh-trial-goal-failed"]


def test_notify_goal_terminal_dedupes():
    # Existing notify event in the same lifecycle suppresses a second send.
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        patch, d = _dirs(root)
        _goal(d["goals"] / "g1.json", status="failed", metrics={
            "notify_events": [{"at": tb._iso(), "kind": "dsh-trial-goal-failed"}]
        })
        calls: list[str] = []
        with patch, mock.patch.object(tb, "_notify_goal_event",
                                      side_effect=lambda goal, **kw: calls.append(kw["kind"])):
            tb._notify_goal_terminal("g1")
        assert calls == []


# --- newly covered return paths ---------------------------------------------

def test_execute_goal_slices_missing_pack_notifies(tmp_path):
    patch, d = _dirs(tmp_path)
    goal_state = {"goal": "g1", "status": "running", "slices": []}
    job = {"goal": "g1", "cwd": str(tmp_path), "profile": "acp-lite"}
    specs = [{"slice": "g1-s1", "pack": "nope.pack.md", "acceptance": []}]
    dest = d["processing"] / "job.json"
    dest.write_text("x", encoding="utf-8")
    notified: list[str] = []
    with patch, mock.patch.object(tb, "_notify_goal_terminal",
                                  side_effect=lambda gid: notified.append(gid)):
        rc = tb._execute_goal_slices(
            goal_state, job, specs, dest, on_slice_fail="stop",
            plan_ticket=None, skip_passed=False,
        )
    assert rc == 1
    assert notified == ["g1"]
    assert json.loads((d["goals"] / "g1.json").read_text())["status"] == "failed"


def test_startup_reconcile_notifies_settled_goal(tmp_path):
    patch, d = _dirs(tmp_path)
    _goal(d["goals"] / "g1.json", status="running")
    (d["chains"] / "g1-s1.json").write_text(
        json.dumps({"slice": "g1-s1", "state": "failed", "error": "boom"}), encoding="utf-8"
    )
    notified: list[str] = []
    with patch, mock.patch.object(tb, "_notify_goal_terminal",
                                  side_effect=lambda gid: notified.append(gid)):
        settled = tb.reconcile_terminal_goals_at_start()
    assert settled == ["g1"]
    assert notified == ["g1"]


def test_maybe_supervisor_close_notifies_on_done(tmp_path):
    patch, d = _dirs(tmp_path)
    job = {"goal": "g1", "cwd": str(tmp_path), "supervisor_close": True}
    chain = {"slice": "g1-s1", "state": "PASS", "goal": "g1", "rounds": []}
    notified: list[str] = []
    with patch, \
            mock.patch.object(tb, "build_supervisor_close_pack", return_value="p.pack.md"), \
            mock.patch.object(tb, "run_supervisor_ticket",
                              return_value=(0, {}, d["summaries"] / "close.md")), \
            mock.patch.object(tb, "_parse_supervisor_summary",
                              return_value={"block": {"goal_status": "done"}}), \
            mock.patch.object(tb, "_write_chain"), \
            mock.patch.object(tb, "_notify_goal_terminal",
                              side_effect=lambda gid: notified.append(gid)):
        tb.maybe_supervisor_close(job, chain)
    assert notified == ["g1"]


# --- run_ticket_job terminal exits ------------------------------------------

def _ticket_job(root: Path) -> tuple[Path, dict]:
    job = {
        "id": "job-ticket-1", "ticket": "impl-trial-1", "pack": "g1-s1.pack.md",
        "profile": "acp-lite", "cwd": str(root), "role": "impl",
        "summary_name": "g1-s1-impl", "goal": "g1", "notify": "hub",
    }
    job_path = root / "job-ticket-1.json"
    job_path.write_text("{}", encoding="utf-8")
    return job_path, job


def test_run_ticket_job_missing_pack_notifies_goal(tmp_path):
    patch, d = _dirs(tmp_path)
    _goal(d["goals"] / "g1.json", status="running")
    job_path, job = _ticket_job(tmp_path)
    notified: list[str] = []
    with patch, \
            mock.patch.object(tb, "_notify_goal_terminal",
                              side_effect=lambda gid: notified.append(gid)):
        rc = tb.run_ticket_job(job_path, job)
    assert rc == 1
    assert notified == ["g1"]
    assert json.loads((d["goals"] / "g1.json").read_text())["status"] == "failed"


def test_run_ticket_job_failed_ticket_notifies_goal(tmp_path):
    patch, d = _dirs(tmp_path)
    _goal(d["goals"] / "g1.json", status="running")
    (d["packs"] / "g1-s1.pack.md").write_text(
        "---\nslice_id: g1-s1\n---\n# intent\n", encoding="utf-8"
    )
    job_path, job = _ticket_job(tmp_path)
    notified: list[str] = []
    with patch, \
            mock.patch.object(tb, "run_open_slice", return_value=1), \
            mock.patch.object(tb, "write_artifacts", return_value={"ticket": "impl-trial-1"}), \
            mock.patch.object(tb, "maybe_offload_gc", return_value=None), \
            mock.patch.object(tb, "_notify_goal_terminal",
                              side_effect=lambda gid: notified.append(gid)):
        rc = tb.run_ticket_job(job_path, job)
    assert rc == 1
    assert notified == ["g1"]
    goal = json.loads((d["goals"] / "g1.json").read_text())
    assert goal["status"] == "failed"
    assert goal["error"]


def test_run_ticket_job_clean_done_notifies_goal(tmp_path):
    patch, d = _dirs(tmp_path)
    _goal(d["goals"] / "g1.json", status="running")
    (d["packs"] / "g1-s1.pack.md").write_text(
        "---\nslice_id: g1-s1\n---\n# intent\n", encoding="utf-8"
    )
    job_path, job = _ticket_job(tmp_path)

    def fake_open_slice(**kwargs):
        sp = d["summaries"] / f"{kwargs['summary_name']}.md"
        sp.parent.mkdir(parents=True, exist_ok=True)
        sp.write_text('```json\n{"status": "done"}\n```\n', encoding="utf-8")
        return 0

    notified: list[str] = []
    with patch, \
            mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), \
            mock.patch.object(tb, "write_artifacts", return_value={"ticket": "impl-trial-1"}), \
            mock.patch.object(tb, "maybe_offload_gc", return_value=None), \
            mock.patch.object(tb, "_notify_goal_terminal",
                              side_effect=lambda gid: notified.append(gid)):
        rc = tb.run_ticket_job(job_path, job)
    assert rc == 0
    assert notified == ["g1"]
    assert json.loads((d["goals"] / "g1.json").read_text())["status"] == "done"


def test_run_ticket_job_non_goal_keeps_ticket_notify(tmp_path):
    patch, d = _dirs(tmp_path)
    (d["packs"] / "x.pack.md").write_text(
        "---\nslice_id: x\n---\n# intent\n", encoding="utf-8"
    )
    job = {"id": "j1", "ticket": "impl-x", "pack": "x.pack.md", "profile": "acp-lite",
           "cwd": str(tmp_path), "role": "impl", "summary_name": "x-impl",
           "notify": "hub"}
    job_path = tmp_path / "j1.json"
    job_path.write_text("{}", encoding="utf-8")

    def fake_open_slice(**kwargs):
        sp = d["summaries"] / f"{kwargs['summary_name']}.md"
        sp.parent.mkdir(parents=True, exist_ok=True)
        sp.write_text('```json\n{"status": "done"}\n```\n', encoding="utf-8")
        return 0

    sent: list[int] = []
    with patch, \
            mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), \
            mock.patch.object(tb, "write_artifacts", return_value={"ticket": "impl-x"}), \
            mock.patch.object(tb, "maybe_offload_gc", return_value=None), \
            mock.patch.object(tb, "maybe_notify_hub",
                              side_effect=lambda *a, **k: sent.append(1) or {"sent": True}), \
            mock.patch.object(tb, "_notify_goal_terminal") as goal_notify:
        rc = tb.run_ticket_job(job_path, job)
    assert rc == 0
    assert sent == [1]
    assert not goal_notify.called
