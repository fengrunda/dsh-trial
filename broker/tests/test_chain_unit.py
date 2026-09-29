#!/usr/bin/env python3
"""Unit tests for chain HOLD→fix→PASS and chain-reply (mocked open-slice)."""
from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("trial_broker_mod", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)


def _write_summary(path: Path, block: dict, prose: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        prose + "\n\n```json\n" + json.dumps(block, ensure_ascii=False, indent=2) + "\n```\n",
        encoding="utf-8",
    )


def test_pack_builders_and_byte_cap(tmp_cwd: Path):
    slice_id = "unit-hold"
    orig = tb.PACKS / "unit-hold-orig.pack.md"
    orig.write_text("---\nslice_id: unit-hold\n---\n# orig\n" + ("x" * 100), encoding="utf-8")
    gate_name = tb.build_gate_pack(
        slice_id=slice_id,
        round_n=1,
        original_pack_name=orig.name,
        acceptance=["must greet"],
        foreman_block={
            "status": "done",
            "changed_files": ["hello.py"],
            "base": "aaa",
            "commit": "bbb",
            "questions": [],
            "notes": "ok",
        },
        foreman_summary_text="done",
        cwd=tmp_cwd,
    )
    assert (tb.PACKS / gate_name).is_file()
    assert (tb.PACKS / gate_name).stat().st_size <= tb.MAX_PACK_BYTES

    fix_name = tb.build_fix_pack(
        slice_id=slice_id,
        next_round=2,
        original_pack_name=orig.name,
        gate_findings=[{"tier": "P1", "file": "hello.py", "issue": "no docstring", "fix_hint": "add one"}],
        gate_summary_text="HOLD",
    )
    assert (tb.PACKS / fix_name).is_file()
    body = (tb.PACKS / fix_name).read_text()
    assert "Gate findings only" in body

    reply = tb.build_reply_addendum_pack(
        slice_id=slice_id,
        round_n=2,
        prior_pack_name=orig.name,
        answer="Add docstring.",
    )
    assert "Supervisor answer" in (tb.PACKS / reply).read_text()
    print("OK pack builders")


def test_hold_fix_pass_mocked(tmp_cwd: Path):
    slice_id = "unit-chain-h2p"
    chain_path = tb.CHAINS / f"{slice_id}.json"
    if chain_path.exists():
        chain_path.unlink()

    orig = tb.PACKS / f"{slice_id}.pack.md"
    orig.write_text(f"---\nslice_id: {slice_id}\n---\n# do greet\n", encoding="utf-8")

    calls = {"n": 0}

    def fake_open_slice(**kwargs):
        calls["n"] += 1
        ticket = kwargs["ticket"]
        summary = tb.SUMMARIES / f"{kwargs['summary_name']}.md"
        mode = kwargs["prompt_mode"]
        log = kwargs["log_path"]
        log.parent.mkdir(parents=True, exist_ok=True)
        log.write_text(f"fake open-slice {ticket} mode={mode}\n", encoding="utf-8")
        if mode == "foreman":
            round_hint = ticket.rsplit("-r", 1)[-1]
            _write_summary(
                summary,
                {
                    "status": "done",
                    "changed_files": ["hello.py"],
                    "branch": "main",
                    "commit": "deadbeef",
                    "base": "cafebabe",
                    "questions": [],
                    "notes": f"round {round_hint}",
                },
                prose=f"foreman r{round_hint}",
            )
            (tmp_cwd / "hello.py").write_text(
                'def greet(name=None):\n    """Greet."""\n    return f"Hello, {name or \'world\'}!"\n'
                if round_hint == "2"
                else "def greet(name=None):\n    return 'hi'\n",
                encoding="utf-8",
            )
        elif mode == "gate":
            round_hint = ticket.rsplit("-r", 1)[-1]
            if round_hint == "1":
                _write_summary(
                    summary,
                    {
                        "verdict": "HOLD",
                        "findings": [
                            {
                                "tier": "P1",
                                "file": "hello.py",
                                "issue": "missing docstring",
                                "fix_hint": "add one-line docstring",
                            }
                        ],
                    },
                    prose="HOLD missing docstring",
                )
            else:
                _write_summary(
                    summary,
                    {"verdict": "PASS", "findings": []},
                    prose="PASS",
                )
        return 0

    def fake_artifacts(job, log_path, summary_path, ec, ticket=None):
        return {
            "ticket": ticket or job.get("ticket"),
            "exit_code": ec,
            "assert_clean": True,
            "composition": None,
            "summary": str(summary_path),
            "summary_exists": summary_path.is_file(),
        }

    job = {
        "id": "unit-h2p",
        "type": "chain",
        "slice": slice_id,
        "pack": orig.name,
        "acceptance": ["greet with docstring"],
        "profile": "acp-lite",
        "gate_profile": "acp-lite",
        "cwd": str(tmp_cwd),
        "max_rounds": 2,
        "notify": "hub",
        "notify_dry_run": True,
    }

    inbox = tb.INBOX / f"job-unit-{slice_id}.json"
    inbox.write_text(json.dumps(job), encoding="utf-8")

    with mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), mock.patch.object(
        tb, "write_artifacts", side_effect=fake_artifacts
    ), mock.patch.object(
        tb, "maybe_notify_hub", return_value={"sent": True, "rc": 0, "kind": "dsh-trial-chain", "detail": "dry"}
    ):
        ec = tb.run_job(inbox)

    assert ec == 0, ec
    state = json.loads(chain_path.read_text())
    assert state["state"] == "PASS", state
    assert state["last_verdict"] == "PASS"
    assert len(state["rounds"]) == 2
    assert state["rounds"][0]["verdict"] == "HOLD"
    assert state["rounds"][1]["verdict"] == "PASS"
    assert state["rounds"][0].get("fix_pack_next")
    assert calls["n"] == 4
    print("OK HOLD→fix→PASS", state["state"], "rounds", len(state["rounds"]))


def test_chain_reply_mocked(tmp_cwd: Path):
    slice_id = "unit-chain-reply"
    chain_path = tb.CHAINS / f"{slice_id}.json"
    if chain_path.exists():
        chain_path.unlink()

    orig = tb.PACKS / f"{slice_id}.pack.md"
    orig.write_text(f"---\nslice_id: {slice_id}\n---\n# ask first\n", encoding="utf-8")

    phase = {"i": 0}

    def fake_open_slice(**kwargs):
        phase["i"] += 1
        ticket = kwargs["ticket"]
        summary = tb.SUMMARIES / f"{kwargs['summary_name']}.md"
        mode = kwargs["prompt_mode"]
        kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["log_path"].write_text(f"fake {ticket}\n", encoding="utf-8")
        if mode == "foreman":
            if phase["i"] == 1:
                _write_summary(
                    summary,
                    {
                        "status": "question",
                        "changed_files": [],
                        "branch": "main",
                        "commit": "",
                        "base": "",
                        "questions": ["What signature for greet?"],
                        "notes": "need supervisor",
                    },
                )
            else:
                _write_summary(
                    summary,
                    {
                        "status": "done",
                        "changed_files": ["hello.py"],
                        "branch": "main",
                        "commit": "abc",
                        "base": "def",
                        "questions": [],
                        "notes": "applied answer",
                    },
                )
                (tmp_cwd / "hello.py").write_text("def greet(name=None):\n    return 'Hello'\n")
        elif mode == "gate":
            _write_summary(summary, {"verdict": "PASS", "findings": []})
        return 0

    def fake_artifacts(job, log_path, summary_path, ec, ticket=None):
        return {
            "ticket": ticket or job.get("ticket"),
            "exit_code": ec,
            "assert_clean": True,
            "composition": None,
            "summary_exists": summary_path.is_file(),
        }

    job = {
        "id": "unit-reply-1",
        "type": "chain",
        "slice": slice_id,
        "pack": orig.name,
        "acceptance": ["greet exists"],
        "profile": "acp-lite",
        "gate_profile": "acp-lite",
        "cwd": str(tmp_cwd),
        "max_rounds": 3,
        "notify_dry_run": True,
    }
    inbox = tb.INBOX / f"job-unit-{slice_id}.json"
    inbox.write_text(json.dumps(job), encoding="utf-8")

    with mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), mock.patch.object(
        tb, "write_artifacts", side_effect=fake_artifacts
    ), mock.patch.object(tb, "maybe_notify_hub", return_value={"sent": True, "rc": 0}):
        ec1 = tb.run_job(inbox)
    assert ec1 == 0
    state1 = json.loads(chain_path.read_text())
    assert state1["state"] == "awaiting_supervisor", state1

    reply_job = {
        "type": "chain-reply",
        "id": "unit-reply-2",
        "slice": slice_id,
        "answer": "def greet(name=None) -> str",
        "notify_dry_run": True,
    }
    inbox2 = tb.INBOX / f"job-unit-{slice_id}-reply.json"
    inbox2.write_text(json.dumps(reply_job), encoding="utf-8")

    with mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), mock.patch.object(
        tb, "write_artifacts", side_effect=fake_artifacts
    ), mock.patch.object(tb, "maybe_notify_hub", return_value={"sent": True, "rc": 0}):
        ec2 = tb.run_job(inbox2)
    assert ec2 == 0
    state2 = json.loads(chain_path.read_text())
    assert state2["state"] == "PASS", state2
    assert any(r.get("verdict") == "PASS" for r in state2["rounds"])
    print("OK chain-reply → PASS", "rounds", len(state2["rounds"]))


def test_enforce_gate_verdict():
    assert tb.enforce_gate_verdict({
        "verdict": "PASS",
        "findings": [{"tier": "P1", "file": "hello.py", "issue": "no tests", "fix_hint": "add"}],
    })["verdict"] == "HOLD"
    assert tb.enforce_gate_verdict({
        "verdict": "PASS",
        "findings": [{"tier": "P2", "issue": "style"}],
        "unmet_acceptance": [],
    })["verdict"] == "PASS"
    assert tb.enforce_gate_verdict({
        "verdict": "PASS",
        "findings": [],
        "unmet_acceptance": ["unit tests required"],
    })["verdict"] == "HOLD"
    b = tb.enforce_gate_verdict({
        "verdict": "PASS",
        "findings": [{"tier": "P0", "issue": "broken"}],
    })
    assert b.get("verdict_overridden") is True
    print("OK enforce_gate_verdict")


def main():
    tb._ensure_dirs()
    cwd = Path(tempfile.mkdtemp(prefix="dsh-trial-unit-"))
    test_pack_builders_and_byte_cap(cwd)
    test_hold_fix_pass_mocked(cwd)
    test_chain_reply_mocked(cwd)
    test_enforce_gate_verdict()
    print("ALL UNIT OK")


if __name__ == "__main__":
    main()
