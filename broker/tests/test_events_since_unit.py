#!/usr/bin/env python3
"""Read-only --events-since cursor, and dry-run event outbox (no ACP)."""
from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location(
    "trial_broker_events_since", ROOT / "trial-broker.py"
)
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)


def _patched_dirs(root: Path):
    names = (
        "goals", "processing", "outbox", "failed", "chains",
        "packs", "summaries", "artifacts", "inbox",
    )
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


def test_next_ticket_split_is_followup_not_old_impl():
    assert tb.next_ticket(tb.trial_event(kind="supervisor", decision="split")) == "split_followup"
    assert tb.next_ticket(tb.trial_event(kind="pause", decision="split")) == "split_followup"
    assert tb.next_ticket(tb.trial_event(kind="supervisor", decision="inplace")) == "inplace_plus"
    assert tb.next_ticket(tb.trial_event(kind="supervisor", decision="escalate")) == "terminal"
    assert tb.next_ticket(tb.trial_event(kind="question", reason="q")) == "answer_same_impl"
    assert tb.next_ticket(tb.trial_event(kind="impl_done", reason="done")) == "gate"
    assert tb.next_ticket(tb.trial_event(kind="gate", verdict="PASS")) == "next_or_close"
    assert tb.next_ticket(tb.trial_event(kind="gate", verdict="HOLD")) == "hold"
    assert tb.next_ticket(tb.trial_event(kind="goal_terminal", decision="done")) == "stop"
    pause_other = tb.next_ticket(tb.trial_event(kind="pause", decision="continue_same"))
    assert pause_other == "continue_same"


def test_pause_split_tries_followup_and_does_not_reopen_impl(tmp_path: Path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher, mock.patch.object(
        tb, "maybe_notify_hub", return_value={"sent": False},
    ), mock.patch.object(
        tb, "emit_split_followup", return_value=True,
    ) as emit, mock.patch.object(
        tb, "run_open_slice",
        side_effect=AssertionError("old impl must not reopen"),
    ):
        rc = tb.route_pause_decision(
            {"goal": "g1", "notify_dry_run": True},
            {"slice": "s1", "goal": "g1", "state": "running", "pack": "s1.pack.md"},
            dirs["processing"] / "job.json",
            decision="split",
            pause_kind="paused_limit",
            round_n=2,
            pack_for_impl="s1.pack.md",
            goal_obj={"goal": "g1"},
        )
    assert rc == 0
    emit.assert_called_once()
    assert emit.call_args.kwargs["source"] == "pause_split"


def test_events_since_init_hides_history_then_only_new(tmp_path: Path, capsys):
    patcher, dirs = _patched_dirs(tmp_path)
    cursor = tmp_path / "cursor.json"
    goal = {"goal": "g-old", "status": "running", "slices": ["s-old"]}
    goal_path = dirs["goals"] / "g-old.json"
    with patcher:
        goal_path.write_text(json.dumps(goal), encoding="utf-8")
        before = goal_path.read_bytes()
        rc = tb.cmd_events_since(mock.Mock(events_since=str(cursor), peek=False))
        first = json.loads(capsys.readouterr().out.strip())
        assert rc == 0
        assert first == {"init": True, "tracked": 1}
        assert "records" not in first
        assert goal_path.read_bytes() == before
        assert list(dirs["inbox"].iterdir()) == []

        new = dirs["outbox"] / "evt.json"
        new.write_text(json.dumps({
            "kind": "impl_done",
            "goal": "g-old",
            "slice": "s-new",
            "decision": "gate",
            "status": "impl_done",
        }), encoding="utf-8")
        rc = tb.cmd_events_since(mock.Mock(events_since=str(cursor), peek=False))
        second = json.loads(capsys.readouterr().out.strip())
        assert rc == 0
        assert second["count"] == 1
        assert second["records"][0]["path"] == str(new)
        assert second["records"][0]["goal"] == "g-old"
        assert second["records"][0]["slice"] == "s-new"
        assert second["records"][0]["status"] == "impl_done"
        assert second["records"][0]["decision"] == "gate"
        assert goal_path.read_bytes() == before

        rc = tb.cmd_events_since(mock.Mock(events_since=str(cursor), peek=False))
        third = json.loads(capsys.readouterr().out.strip())
        assert rc == 0
        assert third == {"count": 0, "records": []}


def test_events_since_peek_does_not_move_cursor(tmp_path: Path, capsys):
    patcher, dirs = _patched_dirs(tmp_path)
    cursor = tmp_path / "cursor.json"
    with patcher:
        (dirs["goals"] / "g.json").write_text(
            json.dumps({"goal": "g", "status": "running"}), encoding="utf-8"
        )
        tb.cmd_events_since(mock.Mock(events_since=str(cursor), peek=False))
        capsys.readouterr()
        frozen = cursor.read_text(encoding="utf-8")
        fresh = dirs["failed"] / "later.json"
        fresh.write_text(json.dumps({
            "goal": "g",
            "slice": "s",
            "state": "failed",
            "last_verdict": "HOLD",
        }), encoding="utf-8")
        tb.cmd_events_since(mock.Mock(events_since=str(cursor), peek=True))
        peeked = json.loads(capsys.readouterr().out.strip())
        assert peeked["count"] == 1
        assert peeked["records"][0]["path"] == str(fresh)
        assert peeked["records"][0]["state"] == "failed"
        assert peeked["records"][0]["last_verdict"] == "HOLD"
        assert cursor.read_text(encoding="utf-8") == frozen
        tb.cmd_events_since(mock.Mock(events_since=str(cursor), peek=False))
        moved = json.loads(capsys.readouterr().out.strip())
        assert moved["count"] == 1
        assert cursor.read_text(encoding="utf-8") != frozen


def test_dry_run_writes_outbox_and_does_not_send(tmp_path: Path):
    patcher, dirs = _patched_dirs(tmp_path)
    calls = []

    def _run(cmd, **kwargs):
        calls.append(list(cmd))
        return subprocess.CompletedProcess(cmd, 0, stdout="dry", stderr="")

    chain = {
        "slice": "s1",
        "goal": "g1",
        "state": "running",
        "supervisor_events": [],
    }
    with patcher, mock.patch.object(tb.subprocess, "run", side_effect=_run):
        action = tb.dispatch_event(
            chain,
            {"goal": "g1", "slice": "s1", "notify_dry_run": True},
            kind="impl_done",
            goal="g1",
            slice_id="s1",
            reason="foreman done",
        )
    assert action == "gate"
    files = list(dirs["outbox"].glob("*.json"))
    assert len(files) == 1
    body = json.loads(files[0].read_text(encoding="utf-8"))
    assert body["kind"] == "impl_done"
    assert body["goal"] == "g1"
    assert body["slice"] == "s1"
    assert body["decision"] == "foreman done"
    saved = json.loads((dirs["chains"] / "s1.json").read_text(encoding="utf-8"))
    event = saved["supervisor_events"][-1]
    assert event["kind"] == "impl_done"
    assert event["reason"] == "foreman done"
    assert "verdict" not in event and "decision" not in event
    assert calls
    cmd = calls[0]
    assert "--dry-run" in cmd
    assert "--kind" in cmd
    assert cmd[cmd.index("--kind") + 1] == "impl_done"
    assert "dsh-trial-complete" not in cmd
