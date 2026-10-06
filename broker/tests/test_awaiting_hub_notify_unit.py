"""Fix 1 — a chain parked in ``awaiting_supervisor`` must wake the Hub.

Covers the new ``dsh-trial-awaiting-hub`` event: the helper payload, the goal
``notify_events`` record, and the run_chain_rounds wiring for blocked / parked
question versus the auto-answered question that resumes inline.
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

spec = importlib.util.spec_from_file_location("trial_broker_awaiting", ROOT / "trial-broker.py")
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


def _fake_open_slice(d: dict, status: str, questions=None):
    def fake(**kwargs):
        block = {
            "status": status,
            "changed_files": [],
            "branch": "main",
            "base": "base1",
            "commit": "head1",
            "questions": list(questions or []),
            "notes": "unit",
        }
        sp = d["summaries"] / f"{kwargs['summary_name']}.md"
        sp.parent.mkdir(parents=True, exist_ok=True)
        sp.write_text("```json\n" + json.dumps(block) + "\n```\n", encoding="utf-8")
        kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["log_path"].write_text("unit log\n", encoding="utf-8")
        return 0
    return fake


def _artifacts(*_a, **_k):
    return {"ticket": "impl-x", "composition": None, "assert_clean": True}


def _chain_job(root: Path, **over) -> dict:
    job = {
        "id": "unit-awaiting", "type": "chain", "slice": "unit-await",
        "pack": "orig.pack.md", "acceptance": ["greet"], "profile": "acp-lite",
        "cwd": str(root), "max_rounds": 3, "notify": "hub",
    }
    job.update(over)
    return job


def _chain_state() -> dict:
    return {
        "slice": "unit-await", "state": "running", "rounds": [],
        "acceptance": ["greet"], "max_rounds": 3,
    }


# --- helper unit tests -------------------------------------------------------

def test_notify_awaiting_hub_skips_without_hub_notify(tmp_path):
    patch, _d = _dirs(tmp_path)
    sent: list[dict] = []
    with patch, mock.patch.object(
        tb, "maybe_notify_hub", side_effect=lambda *a, **k: sent.append(k) or {"sent": True}
    ):
        out = tb._notify_awaiting_hub({"slice": "s"}, {"slice": "s"}, reason="blocked", block={})
    assert out is None
    assert sent == []


def test_notify_awaiting_hub_honors_goal_notify_and_records_event(tmp_path):
    patch, d = _dirs(tmp_path)
    goal = {"goal": "g1", "status": "running", "slices": ["g1-s1"], "notify": "hub", "metrics": {}}
    (d["goals"] / "g1.json").write_text(json.dumps(goal), encoding="utf-8")
    sent: list[dict] = []
    chain = {"slice": "g1-s1", "rounds": [{"gate_summary": "/tmp/gate-r1.md"}]}
    with patch, mock.patch.object(
        tb, "maybe_notify_hub", side_effect=lambda *a, **k: sent.append(k) or {"sent": True}
    ):
        info = tb._notify_awaiting_hub(
            {"slice": "g1-s1"},  # job has no notify; goal does
            chain,
            reason="blocked",
            block={"questions": ["which rule?"]},
            goal=goal,
        )
    assert info == {"sent": True}
    assert sent[0]["kind"] == "dsh-trial-awaiting-hub"
    assert sent[0]["payload_extra"]["status"] == "blocked"
    assert sent[0]["payload_extra"]["goal"] == "g1"
    assert sent[0]["payload_extra"]["slice"] == "g1-s1"
    assert sent[0]["payload_extra"]["questions"] == ["which rule?"]
    assert sent[0]["payload_extra"]["suggested_action"] == "chain-reply"
    assert sent[0]["payload_extra"]["last_gate_verdict_path"] == "/tmp/gate-r1.md"
    stored = json.loads((d["goals"] / "g1.json").read_text())
    events = stored["metrics"]["notify_events"]
    assert events and events[-1]["kind"] == "dsh-trial-awaiting-hub"
    assert events[-1]["info"]["reason"] == "blocked"


# --- run_chain_rounds wiring -------------------------------------------------

def test_blocked_path_notifies_awaiting_hub(tmp_path):
    patch, d = _dirs(tmp_path)
    sent: list[dict] = []
    dest = d["processing"] / "job.json"
    dest.write_text("x", encoding="utf-8")
    with patch, \
            mock.patch.object(tb, "run_open_slice", side_effect=_fake_open_slice(d, "blocked", ["why?"])), \
            mock.patch.object(tb, "write_artifacts", side_effect=_artifacts), \
            mock.patch.object(tb, "_terminal_outbox", return_value=d["outbox"] / "o.json"), \
            mock.patch.object(tb, "maybe_notify_hub",
                              side_effect=lambda *a, **k: sent.append(k) or {"sent": True}):
        rc = tb.run_chain_rounds(_chain_job(tmp_path), _chain_state(), dest,
                                 start_round=1, start_pack="orig.pack.md")
    assert rc == 0
    kinds = [c.get("kind") for c in sent]
    assert "dsh-trial-awaiting-hub" in kinds
    call = next(c for c in sent if c["kind"] == "dsh-trial-awaiting-hub")
    assert call["payload_extra"]["status"] == "blocked"
    assert call["payload_extra"]["slice"] == "unit-await"
    assert call["payload_extra"]["questions"] == ["why?"]
    assert call["payload_extra"]["suggested_action"] == "chain-reply"


def test_parked_question_notifies_awaiting_hub(tmp_path):
    patch, d = _dirs(tmp_path)
    sent: list[dict] = []
    dest = d["processing"] / "job.json"
    dest.write_text("x", encoding="utf-8")
    chain = _chain_state()
    with patch, \
            mock.patch.object(tb, "run_open_slice", side_effect=_fake_open_slice(d, "question", ["q?"])), \
            mock.patch.object(tb, "write_artifacts", side_effect=_artifacts), \
            mock.patch.object(tb, "_terminal_outbox", return_value=d["outbox"] / "o.json"), \
            mock.patch.object(tb, "maybe_notify_hub",
                              side_effect=lambda *a, **k: sent.append(k) or {"sent": True}):
        rc = tb.run_chain_rounds(_chain_job(tmp_path, auto_supervisor_answer=False), chain, dest,
                                 start_round=1, start_pack="orig.pack.md")
    assert rc == 0
    assert chain["state"] == "awaiting_supervisor"
    call = next(c for c in sent if c.get("kind") == "dsh-trial-awaiting-hub")
    assert call["payload_extra"]["status"] == "question"


def test_auto_answered_question_does_not_notify_awaiting_hub(tmp_path):
    patch, d = _dirs(tmp_path)
    sent: list[dict] = []
    dest = d["processing"] / "job.json"
    dest.write_text("x", encoding="utf-8")
    chain = _chain_state()
    orig = tb.run_chain_rounds
    with patch, \
            mock.patch.object(tb, "run_open_slice", side_effect=_fake_open_slice(d, "question", ["q?"])), \
            mock.patch.object(tb, "write_artifacts", side_effect=_artifacts), \
            mock.patch.object(tb, "_terminal_outbox", return_value=d["outbox"] / "o.json"), \
            mock.patch.object(tb, "auto_supervisor_answer", return_value="use rule X"), \
            mock.patch.object(tb, "build_reply_addendum_pack", return_value="reply.pack.md"), \
            mock.patch.object(tb, "maybe_notify_hub",
                              side_effect=lambda *a, **k: sent.append(k) or {"sent": True}):
        with mock.patch.object(tb, "run_chain_rounds", return_value=0) as recur:
            rc = orig(_chain_job(tmp_path), chain, dest, start_round=1, start_pack="orig.pack.md")
    assert rc == 0
    assert recur.called
    assert chain["state"] == "running"
    assert not [c for c in sent if c.get("kind") == "dsh-trial-awaiting-hub"]
