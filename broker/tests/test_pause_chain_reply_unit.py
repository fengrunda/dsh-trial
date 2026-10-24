#!/usr/bin/env python3
"""chain-reply on pause_* parks must not need an awaiting_reason remap.

pause_split + decision=split does not reopen impl rN+1. It tries one
follow-up slice; if that emit does not land a pack, the chain stays parked.
max_rounds_split + decision=split does not reopen the old impl either. It
tries the same one-slice follow-up, then the parent is always superseded
(awaiting cleared). A rejected emit does not re-park the parent.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location(
    "trial_broker_pause_chain_reply", ROOT / "trial-broker.py"
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
        maybe_notify_hub=lambda *a, **k: {"sent": False},
    ), dirs


def _write_parked(dirs, tmp_path: Path, *, awaiting_reason: str, goal_reason: str | None = None):
    slice_id = "pause-reply-slice"
    goal_id = "pause-reply-goal"
    goal = {
        "goal": goal_id,
        "status": "running",
        "brief": "x",
        "slices": [slice_id],
        "awaiting_reason": goal_reason if goal_reason is not None else awaiting_reason,
        "last_chain_state": "awaiting_supervisor",
    }
    chain = {
        "slice": slice_id,
        "type": "chain",
        "state": "awaiting_supervisor",
        "goal": goal_id,
        "pack": f"{slice_id}.pack.md",
        "acceptance": ["a"],
        "profile": "acp-lite",
        "gate_profile": "acp-lite",
        "cwd": str(tmp_path),
        "max_rounds": 2,
        "rounds": [
            {"round": 1, "impl_pack": f"{slice_id}.pack.md", "impl_ticket": "impl-r1"},
            {"round": 2, "impl_pack": f"{slice_id}.pack.md", "impl_ticket": "impl-r2"},
        ],
        "awaiting_answer_for_round": 2,
        "awaiting_reason": awaiting_reason,
        "pending_impl_pack": f"{slice_id}.pack.md",
    }
    (dirs["goals"] / f"{goal_id}.json").write_text(
        json.dumps(goal, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    (dirs["chains"] / f"{slice_id}.json").write_text(
        json.dumps(chain, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    (dirs["packs"] / f"{slice_id}.pack.md").write_text(
        f"---\nslice_id: {slice_id}\n---\n# pack\n", encoding="utf-8"
    )
    return slice_id, goal_id


def _reply(dirs, slice_id: str, *, decision: str, answer: str) -> Path:
    path = dirs["inbox"] / "reply.json"
    path.write_text(
        json.dumps(
            {
                "type": "chain-reply",
                "id": "pause-reply",
                "slice": slice_id,
                "answer": answer,
                "decision": decision,
            }
        ),
        encoding="utf-8",
    )
    return path


def _read(dirs, kind: str, name: str) -> dict:
    return json.loads((dirs[kind] / f"{name}.json").read_text(encoding="utf-8"))


def _block_supervisor_emit(dirs):
    """Record the supervisor-plan attempt and do not run ACP."""
    modes: list[str] = []

    def _sup(**kwargs):
        modes.append(str(kwargs.get("prompt_mode") or ""))
        missing = dirs["summaries"] / "split-emit-missing.md"
        return 1, {}, missing

    return modes, _sup


def test_pause_split_chain_reply_does_not_open_impl_round(tmp_path: Path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        slice_id, goal_id = _write_parked(
            dirs, tmp_path, awaiting_reason="pause_split:paused_limit"
        )
        inbox = _reply(
            dirs,
            slice_id,
            decision="split",
            answer="remaining work goes to a new slice",
        )
        modes, _sup = _block_supervisor_emit(dirs)
        launched: list[str] = []

        def _rounds(*args, **kwargs):
            launched.append("run_chain_rounds")
            raise AssertionError("failed emit must not open a slice")

        with mock.patch.object(tb, "run_open_slice", side_effect=AssertionError("no acp")), \
                mock.patch.object(tb, "run_supervisor_ticket", side_effect=_sup), \
                mock.patch.object(tb, "run_chain_rounds", side_effect=_rounds):
            rc = tb.run_job(inbox)

        assert rc == 0
        assert modes == ["supervisor-plan"]
        assert launched == []
        chain = _read(dirs, "chains", slice_id)
        goal = _read(dirs, "goals", goal_id)
        assert chain["state"] == "awaiting_supervisor"
        assert chain["awaiting_reason"] == "pause_split:paused_limit"
        assert chain["supervisor_decision"]["decision"] == "split"
        assert len(chain["rounds"]) == 2
        assert goal["status"] == "running"
        assert goal["last_chain_state"] == "awaiting_supervisor"
        assert goal["awaiting_reason"] == "pause_split:paused_limit"
        assert "max_rounds" not in str(goal["awaiting_reason"])
        assert not list(dirs["packs"].glob("*-reply-r*.pack.md"))
        ask = (dirs["packs"] / f"{slice_id}-split-r2.pack.md").read_text(encoding="utf-8")
        assert "emit_chain" in ask
        assert "不要重写整个 Goal" in ask


def test_max_rounds_split_chain_reply_terminalizes_parent(tmp_path: Path):
    """Do not reopen the old impl. Do try one follow-up. Parent is superseded."""
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        slice_id, goal_id = _write_parked(
            dirs,
            tmp_path,
            awaiting_reason="max_rounds_split",
            goal_reason="max_rounds_split",
        )
        inbox = _reply(
            dirs,
            slice_id,
            decision="split",
            answer="split remaining P0",
        )
        modes, _sup = _block_supervisor_emit(dirs)
        launched: list[str] = []

        def _rounds(*args, **kwargs):
            launched.append("run_chain_rounds")
            raise AssertionError("failed emit must not open a slice")

        with mock.patch.object(tb, "run_open_slice", side_effect=AssertionError("no acp")), \
                mock.patch.object(tb, "run_supervisor_ticket", side_effect=_sup), \
                mock.patch.object(tb, "run_chain_rounds", side_effect=_rounds):
            rc = tb.run_job(inbox)

        assert rc == 0
        assert modes == ["supervisor-plan"]
        assert launched == []
        chain = _read(dirs, "chains", slice_id)
        goal = _read(dirs, "goals", goal_id)
        # Shared closeout: the parent chain never stays awaiting.
        assert chain["state"] == "superseded"
        assert "awaiting_reason" not in chain
        assert "awaiting_reason" not in goal
        assert chain["supervisor_decision"]["decision"] == "split"
        assert chain["supervisor_decision"]["via"] == "chain-reply"
        assert chain["supervisor_decision"]["source"] == "chain-reply"
        assert "do not reopen this impl" in str(chain["supervisor_decision"].get("note") or "")
        assert len(chain["rounds"]) == 2
        assert goal["status"] == "running"
        assert not list(dirs["packs"].glob("*-reply-r*.pack.md"))
        ask = (dirs["packs"] / f"{slice_id}-split-r2.pack.md").read_text(encoding="utf-8")
        assert "split remaining P0" in ask
        assert "emit_chain" in ask


def test_max_rounds_split_chain_reply_launches_one_followup(tmp_path: Path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        slice_id, goal_id = _write_parked(
            dirs,
            tmp_path,
            awaiting_reason="max_rounds_split",
            goal_reason="max_rounds_split",
        )
        inbox = _reply(dirs, slice_id, decision="split", answer="split remaining P0")
        child_id = "follow-slice"
        launched: list[str] = []

        def _sup(**kwargs):
            assert kwargs.get("prompt_mode") == "supervisor-plan"
            summary = dirs["summaries"] / f"{slice_id}-sup-split-r2.md"
            summary.write_text(
                "```json\n"
                + json.dumps({
                    "action": "emit_chain",
                    "slice": child_id,
                    "pack": f"{child_id}.pack.md",
                    "acceptance": ["remainder"],
                })
                + "\n```\n",
                encoding="utf-8",
            )
            (dirs["packs"] / f"{child_id}.pack.md").write_text("# follow\n", encoding="utf-8")
            return 0, {}, summary

        def _rounds(job, chain, dest, **kwargs):
            launched.append(str(job.get("slice")))
            assert kwargs.get("start_pack") == f"{child_id}.pack.md"
            assert str(job.get("slice")) != slice_id
            chain["state"] = "PASS"
            tb._write_chain(chain)
            return 0

        with mock.patch.object(tb, "run_open_slice", side_effect=AssertionError("no acp")), \
                mock.patch.object(tb, "run_supervisor_ticket", side_effect=_sup), \
                mock.patch.object(tb, "run_chain_rounds", side_effect=_rounds):
            rc = tb.run_job(inbox)

        assert rc == 0
        assert launched == [child_id]
        chain = _read(dirs, "chains", slice_id)
        goal = _read(dirs, "goals", goal_id)
        child = _read(dirs, "chains", child_id)
        assert chain["state"] == "superseded"
        assert chain["superseded_reason"] == "max_rounds_split"
        assert len(chain["rounds"]) == 2
        assert "awaiting_reason" not in chain
        assert child["state"] == "PASS"
        assert child_id in goal["slices"]
        assert goal["split_followup"]["slice"] == child_id
        assert not list(dirs["packs"].glob("*-reply-r*.pack.md"))


def test_max_rounds_split_rejects_two_slices(tmp_path: Path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        slice_id, goal_id = _write_parked(
            dirs, tmp_path, awaiting_reason="max_rounds_split", goal_reason="max_rounds_split",
        )
        inbox = _reply(dirs, slice_id, decision="split", answer="too many")

        def _sup(**kwargs):
            summary = dirs["summaries"] / f"{slice_id}-sup-split-r2.md"
            summary.write_text(
                "```json\n"
                + json.dumps({
                    "action": "emit_chains",
                    "slices": [
                        {"slice": "a", "pack": "a.pack.md"},
                        {"slice": "b", "pack": "b.pack.md"},
                    ],
                })
                + "\n```\n",
                encoding="utf-8",
            )
            return 0, {}, summary

        with mock.patch.object(tb, "run_open_slice", side_effect=AssertionError("no acp")), \
                mock.patch.object(tb, "run_supervisor_ticket", side_effect=_sup), \
                mock.patch.object(tb, "run_chain_rounds", side_effect=AssertionError("no launch")):
            rc = tb.run_job(inbox)

        assert rc == 0
        chain = _read(dirs, "chains", slice_id)
        goal = _read(dirs, "goals", goal_id)
        assert chain["state"] == "superseded"
        assert "awaiting_reason" not in chain
        assert len(chain["rounds"]) == 2
        assert "split_followup" not in goal
        assert not (dirs["chains"] / "a.json").is_file()


def test_pause_split_escalate_does_not_open_impl_round(tmp_path: Path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        slice_id, goal_id = _write_parked(
            dirs, tmp_path, awaiting_reason="pause_split:paused_limit"
        )
        inbox = _reply(dirs, slice_id, decision="escalate", answer="need a human")
        with mock.patch.object(tb, "run_open_slice", side_effect=AssertionError("no impl")), \
                mock.patch.object(tb, "_notify_goal_terminal") as notify:
            rc = tb.run_job(inbox)
        assert rc == 1
        chain = _read(dirs, "chains", slice_id)
        goal = _read(dirs, "goals", goal_id)
        assert chain["state"] == "escalated"
        assert chain["supervisor_decision"]["decision"] == "escalate"
        assert len(chain["rounds"]) == 2
        assert goal["status"] == "escalated"
        assert notify.called


def test_pause_continue_inplace_resumes_bounded(tmp_path: Path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        slice_id, goal_id = _write_parked(
            dirs, tmp_path, awaiting_reason="pause_continue:paused_limit"
        )
        inbox = _reply(dirs, slice_id, decision="inplace", answer="keep going")
        seen: dict = {}

        def _rounds(job, chain, dest, **kwargs):
            seen["start_round"] = kwargs.get("start_round")
            seen["start_pack"] = kwargs.get("start_pack")
            seen["answer"] = job.get("answer")
            chain["state"] = "running"
            tb._write_chain(chain)
            return 0

        with mock.patch.object(tb, "run_chain_rounds", side_effect=_rounds), \
                mock.patch.object(tb, "run_open_slice", side_effect=AssertionError("direct open")):
            rc = tb.run_job(inbox)
        assert rc == 0
        assert seen["start_round"] == 2
        chain = _read(dirs, "chains", slice_id)
        goal = _read(dirs, "goals", goal_id)
        assert chain["supervisor_decision"]["decision"] == "continue_same"
        assert chain["pause_resume_mode"] == "continue_same"
        assert chain.get("max_rounds_bonus_used") is not True
        assert int(chain["max_rounds"]) == 2
        assert chain["inline_continue_same"]["impl_ticket"] == "impl-r2"
        assert chain["inline_continue_same"]["round_n"] == 2
        assert seen["start_pack"] == f"{slice_id}.pack.md"
        assert chain.get("awaiting_reason") in (None, "")
        assert goal["status"] == "running"
        assert "awaiting_reason" not in goal


def test_pause_new_ticket_opens_fresh_round(tmp_path: Path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        slice_id, _goal_id = _write_parked(
            dirs, tmp_path, awaiting_reason="pause_new_ticket:paused_wallclock"
        )
        # Seed the paused session id; a fresh ticket must drop it.
        chain_path = dirs["chains"] / f"{slice_id}.json"
        chain = json.loads(chain_path.read_text(encoding="utf-8"))
        chain["pause_impl_ticket"] = "impl-trial-old"
        chain["pause_resume_mode"] = "new_ticket"
        chain_path.write_text(json.dumps(chain), encoding="utf-8")
        inbox = _reply(dirs, slice_id, decision="new_ticket", answer="cold read")
        seen: dict = {}

        def _rounds(job, chain, dest, **kwargs):
            seen["start_round"] = kwargs.get("start_round")
            tb._write_chain(chain)
            return 0

        with mock.patch.object(tb, "run_chain_rounds", side_effect=_rounds):
            rc = tb.run_job(inbox)
        assert rc == 0
        assert seen["start_round"] == 3
        chain = _read(dirs, "chains", slice_id)
        assert chain["supervisor_decision"]["decision"] == "new_ticket"
        assert chain["pause_resume_mode"] == "new_ticket"
        assert "pause_impl_ticket" not in chain
        assert chain.get("max_rounds_bonus_used") is not True


def test_load_job_chain_reply_keeps_continue_same_budget_deltas(tmp_path):
    """PR #21 follow-up: chain-reply must not drop add_steps / add_timeout_sec."""
    job_path = tmp_path / "chain-reply.json"
    job_path.write_text(
        json.dumps(
            {
                "type": "chain-reply",
                "slice": "s1",
                "decision": "continue_same",
                "add_steps": 0,
                "add_timeout_sec": 1800,
                "answer": "continue",
            }
        ),
        encoding="utf-8",
    )
    job = tb.load_job(job_path)
    assert job["type"] == "chain-reply"
    assert job["decision"] == "continue_same"
    assert job["add_steps"] == 0
    assert job["add_timeout_sec"] == 1800


def test_load_job_goal_update_keeps_impl_max_steps(tmp_path):
    job_path = tmp_path / "goal-update.json"
    job_path.write_text(
        json.dumps(
            {
                "type": "goal-update",
                "goal": "g1",
                "impl_max_steps": 80,
                "prompt_idle_timeout_sec": 900,
            }
        ),
        encoding="utf-8",
    )
    job = tb.load_job(job_path)
    assert job["impl_max_steps"] == 80
    assert job["prompt_idle_timeout_sec"] == 900
