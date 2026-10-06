#!/usr/bin/env python3
"""Unit tests: gate_running interim ack + final status=done.

Incident: ``_handle_submit_for_review`` wrote ``answers/<ask_id>.json`` only
after the (multi-minute) gate short-ticket finished. The plugin gave up at the
LLM's 180s timeout, the foreman concluded the gate was absent and set blocked —
although the gate was still running.

Rules under test:
  A) before spawning the gate, the broker atomically writes an interim body with
     ``status=gate_running``, the gate ticket, the gate job budget and NO
     verdict/ok/answer;
  B) the final PASS/HOLD answer overwrites the same path with ``status=done``;
  C) the pack-error structured failure also carries ``status=done``;
  D) ``gate_job_timeout_sec`` honours TRIAL_BROKER_JOB_TIMEOUT and defaults.
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

spec = importlib.util.spec_from_file_location("trial_broker_gateack", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)


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
    # trial_lib keeps its own MAILBOX binding used by write_ask_answer; in
    # production both point at the same global mailbox.
    stack.enter_context(mock.patch.object(tb.T, "MAILBOX", d["mailbox"]))
    return stack, d


def _fake_artifacts(job, log_path, summary_path, ec, ticket=None):
    return {
        "ticket": ticket or job.get("ticket"),
        "exit_code": ec,
        "assert_clean": True,
        "composition": None,
        "summary": str(summary_path),
        "summary_exists": summary_path.is_file(),
    }


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


def _verdict_block(verdict: str, findings: list | None = None) -> dict:
    return {
        "block": {
            "verdict": verdict,
            "findings": findings or [],
            "unmet_acceptance": [],
        },
        "text": "",
    }


# --------------------------------------------------------------------------
# D) gate_job_timeout_sec
# --------------------------------------------------------------------------
def test_gate_job_timeout_sec_defaults_and_env(monkeypatch):
    monkeypatch.delenv("TRIAL_BROKER_JOB_TIMEOUT", raising=False)
    assert tb.gate_job_timeout_sec({}) == 4320  # 600 + 3600 + 120
    assert tb.gate_job_timeout_sec(
        {"ask_supervisor_timeout_sec": 600, "prompt_timeout_sec": 3600}
    ) == 4320
    assert tb.gate_job_timeout_sec(
        {"ask_supervisor_timeout_sec": 100, "prompt_timeout_sec": 200}
    ) == 420

    monkeypatch.setenv("TRIAL_BROKER_JOB_TIMEOUT", "123")
    assert tb.gate_job_timeout_sec({}) == 123

    # "0"/empty means "unset" -> fall back to the computed budget
    monkeypatch.setenv("TRIAL_BROKER_JOB_TIMEOUT", "0")
    assert tb.gate_job_timeout_sec({"ask_supervisor_timeout_sec": 600, "prompt_timeout_sec": 3600}) == 4320


# --------------------------------------------------------------------------
# A + B) interim ack before spawn, final PASS overwrites with status=done
# --------------------------------------------------------------------------
def test_submit_for_review_writes_interim_ack_then_final_pass(tmp_path: Path):
    ctx, d = _patched_dirs(tmp_path / "state")
    ask_id = "ask-ack-pass"
    slice_id = "ack-pass"
    gate_ticket = f"gate-trial-{slice_id}-rev1"
    expected_budget = tb.gate_job_timeout_sec(tb.T.load_global_limits())
    (d["packs"] / f"{slice_id}.pack.md").write_text(
        f"---\nslice_id: {slice_id}\n---\n# pack\n", encoding="utf-8"
    )
    seen: dict = {}

    def fake_open_slice(**kwargs):
        assert kwargs["ticket"] == gate_ticket
        ack = json.loads((d["mailbox"] / "answers" / f"{ask_id}.json").read_text())
        assert ack["status"] == "gate_running"
        assert ack["gate_ticket"] == gate_ticket
        assert ack["gate_timeout_sec"] == expected_budget
        assert ack["progress"] is True
        # no final-answer keys: old clients keep waiting
        for key in ("verdict", "ok", "answer"):
            assert key not in ack, f"interim ack leaked final key {key}: {ack}"
        assert ack.get("gate_log")
        seen["ack"] = ack
        return 0

    pending = d["mailbox"] / "pending" / f"{ask_id}.json"
    with ctx, mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), \
            mock.patch.object(tb, "write_artifacts", side_effect=_fake_artifacts), \
            mock.patch.object(tb, "_parse_gate_verdict",
                              return_value=_verdict_block("PASS")):
        tb._handle_submit_for_review(
            _ask(ask_id, slice_id), pending, default_profile="acp-lite",
            cwd=str(tmp_path), goal=None,
        )

    assert seen, "run_open_slice was not called"
    final = json.loads((d["mailbox"] / "answers" / f"{ask_id}.json").read_text())
    assert final["status"] == "done"
    assert final["verdict"] == "PASS"
    assert final["ask_id"] == ask_id


# --------------------------------------------------------------------------
# A + B) HOLD final also overwrites the interim with status=done
# --------------------------------------------------------------------------
def test_hold_final_overwrites_interim_with_status_done(tmp_path: Path):
    ctx, d = _patched_dirs(tmp_path / "state")
    ask_id = "ask-ack-hold"
    slice_id = "ack-hold"
    (d["packs"] / f"{slice_id}.pack.md").write_text(
        f"---\nslice_id: {slice_id}\n---\n# pack\n", encoding="utf-8"
    )
    interim_seen: dict = {}

    def fake_open_slice(**kwargs):
        body = json.loads((d["mailbox"] / "answers" / f"{ask_id}.json").read_text())
        interim_seen.update(body)
        return 0

    hold_findings = [{"id": "F1", "severity": "P0", "detail": "broken"}]
    with ctx, mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), \
            mock.patch.object(tb, "write_artifacts", side_effect=_fake_artifacts), \
            mock.patch.object(tb, "_parse_gate_verdict",
                              return_value=_verdict_block("HOLD", hold_findings)):
        tb._handle_submit_for_review(
            _ask(ask_id, slice_id), d["mailbox"] / "pending" / f"{ask_id}.json",
            default_profile="acp-lite", cwd=str(tmp_path), goal=None,
        )

    assert interim_seen["status"] == "gate_running"
    assert "verdict" not in interim_seen
    final = json.loads((d["mailbox"] / "answers" / f"{ask_id}.json").read_text())
    assert final["status"] == "done"
    assert final["verdict"] == "HOLD"
    assert final["findings"] == hold_findings


# --------------------------------------------------------------------------
# C) pack-error structured failure carries status=done
# --------------------------------------------------------------------------
def test_pack_error_answer_carries_status_done(tmp_path: Path):
    ctx, d = _patched_dirs(tmp_path / "state")
    ask_id = "ask-ack-packer"
    slice_id = "ack-packer"
    with ctx, mock.patch.object(tb, "build_gate_pack", side_effect=ValueError("bad pack")), \
            mock.patch.object(tb, "run_open_slice") as spawn:
        tb._handle_submit_for_review(
            _ask(ask_id, slice_id), d["mailbox"] / "pending" / f"{ask_id}.json",
            default_profile="acp-lite", cwd=str(tmp_path), goal=None,
        )
    assert not spawn.called
    body = json.loads((d["mailbox"] / "answers" / f"{ask_id}.json").read_text())
    assert body["status"] == "done"
    assert body["ok"] is False
    assert body["verdict"] == "HOLD"


if __name__ == "__main__":
    import tempfile

    failures = 0
    with tempfile.TemporaryDirectory() as td:
        p = Path(td)
        for name, fn in [
            ("test_submit_for_review_writes_interim_ack_then_final_pass",
             test_submit_for_review_writes_interim_ack_then_final_pass),
            ("test_hold_final_overwrites_interim_with_status_done",
             test_hold_final_overwrites_interim_with_status_done),
            ("test_pack_error_answer_carries_status_done",
             test_pack_error_answer_carries_status_done),
        ]:
            try:
                fn(p / name)
            except AssertionError as e:  # pragma: no cover
                failures += 1
                print(f"FAIL {name}: {e}")
    print("ALL GATE-RUNNING-ACK UNIT OK" if not failures else f"{failures} FAILED")
    sys.exit(1 if failures else 0)
