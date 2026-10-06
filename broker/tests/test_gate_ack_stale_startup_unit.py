#!/usr/bin/env python3
"""Unit tests: stale ``gate_running`` acks cleared at broker startup.

Incident: 9cd7224 writes an interim ``MAILBOX/answers/<ask_id>.json`` with
``status="gate_running"`` before the gate spawn, and both
``trial_lib.list_pending_asks`` and ``_handle_pending_ask_file`` skip an ask
whenever ANY answer file exists. If the broker dies/restarts mid-gate (stop.sh
TERMs only the broker pid; the orphan gate cannot write the final answer), the
interim stays forever and the pending ask is never re-dispatched.

Rules under test:
  A) a ``status=gate_running`` interim is renamed to
     ``archive/<ask_id>.stale-gate-running.json`` and reported;
  B) a final PASS answer (``verdict`` present) is untouched;
  C) unreadable / invalid JSON is untouched;
  D) after clearing, ``list_pending_asks`` returns the pending ask again.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from contextlib import ExitStack
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_gateack_stale", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)


def _patched_mailbox(root: Path):
    mailbox = root / "mailbox"
    (mailbox / "answers").mkdir(parents=True, exist_ok=True)
    (mailbox / "pending").mkdir(parents=True, exist_ok=True)
    stack = ExitStack()
    stack.enter_context(mock.patch.object(tb, "MAILBOX", mailbox))
    stack.enter_context(mock.patch.object(tb.T, "MAILBOX", mailbox))
    return stack, mailbox


def _write_pending(mailbox: Path, ask_id: str) -> Path:
    path = mailbox / "pending" / f"{ask_id}.json"
    path.write_text(
        json.dumps({"ask_id": ask_id, "kind": "submit_for_review"}, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return path


def _write_answer(mailbox: Path, ask_id: str, body: dict) -> Path:
    path = mailbox / "answers" / f"{ask_id}.json"
    path.write_text(json.dumps(body, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def test_interim_gate_running_ack_is_cleared_and_archived(tmp_path: Path):
    ctx, mailbox = _patched_mailbox(tmp_path / "state")
    ask_id = "ask-stale-1"
    _write_pending(mailbox, ask_id)
    interim = {
        "ask_id": ask_id,
        "status": "gate_running",
        "gate_ticket": "gate-trial-stale-1-rev1",
        "gate_timeout_sec": 4320,
        "started_at": "2026-01-01T00:00:00Z",
        "progress": True,
    }
    _write_answer(mailbox, ask_id, interim)
    with ctx:
        cleared = tb.clear_stale_gate_running_acks_at_start()
    assert cleared == [ask_id]
    assert not (mailbox / "answers" / f"{ask_id}.json").exists()
    archived = mailbox / "archive" / f"{ask_id}.stale-gate-running.json"
    assert archived.is_file()
    assert json.loads(archived.read_text())["status"] == "gate_running"


def test_final_pass_answer_is_untouched(tmp_path: Path):
    ctx, mailbox = _patched_mailbox(tmp_path / "state")
    ask_id = "ask-final-1"
    final = {
        "ask_id": ask_id,
        "status": "done",
        "verdict": "PASS",
        "ok": True,
        "instruction": "merge",
    }
    ans = _write_answer(mailbox, ask_id, final)
    with ctx:
        cleared = tb.clear_stale_gate_running_acks_at_start()
    assert cleared == []
    assert ans.is_file()
    assert json.loads(ans.read_text())["verdict"] == "PASS"
    assert not (mailbox / "archive" / f"{ask_id}.stale-gate-running.json").exists()


def test_invalid_json_answer_is_untouched(tmp_path: Path):
    ctx, mailbox = _patched_mailbox(tmp_path / "state")
    ask_id = "ask-badjson-1"
    bad = mailbox / "answers" / f"{ask_id}.json"
    bad.write_text("{not valid json", encoding="utf-8")
    with ctx:
        cleared = tb.clear_stale_gate_running_acks_at_start()
    assert cleared == []
    assert bad.is_file()
    assert bad.read_text() == "{not valid json"


def test_pending_is_redispatched_after_clearing(tmp_path: Path):
    ctx, mailbox = _patched_mailbox(tmp_path / "state")
    ask_id = "ask-redispatch-1"
    pending = _write_pending(mailbox, ask_id)
    _write_answer(
        mailbox,
        ask_id,
        {
            "ask_id": ask_id,
            "status": "gate_running",
            "gate_ticket": "gate-trial-redispatch-1-rev1",
            "gate_timeout_sec": 4320,
            "started_at": "2026-01-01T00:00:00Z",
            "progress": True,
        },
    )
    with ctx:
        # The interim answer still suppresses the pending ask (the regression).
        assert tb.T.list_pending_asks(mailbox) == []
        tb.clear_stale_gate_running_acks_at_start()
        # After the sweep the pending ask is visible to the watcher again.
        assert tb.T.list_pending_asks(mailbox) == [pending]


if __name__ == "__main__":
    import tempfile

    failures = 0
    with tempfile.TemporaryDirectory() as td:
        p = Path(td)
        for name, fn in [
            ("test_interim_gate_running_ack_is_cleared_and_archived",
             test_interim_gate_running_ack_is_cleared_and_archived),
            ("test_final_pass_answer_is_untouched",
             test_final_pass_answer_is_untouched),
            ("test_invalid_json_answer_is_untouched",
             test_invalid_json_answer_is_untouched),
            ("test_pending_is_redispatched_after_clearing",
             test_pending_is_redispatched_after_clearing),
        ]:
            try:
                fn(p / name)
            except AssertionError as e:  # pragma: no cover
                failures += 1
                print(f"FAIL {name}: {e}")
    print("ALL GATE-ACK-STALE-STARTUP UNIT OK" if not failures else f"{failures} FAILED")
    sys.exit(1 if failures else 0)
