#!/usr/bin/env python3
"""chain-reply on pause_* parks must not need an awaiting_reason remap.

pause_split + decision=split stays parked (no impl rN+1).
max_rounds_split + decision=split is unchanged.
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
        opened: list[str] = []

        def _open(**kwargs):
            opened.append(str(kwargs.get("prompt_mode") or kwargs.get("ticket")))
            raise AssertionError("pause_split split must not open an impl round")

        with mock.patch.object(tb, "run_open_slice", side_effect=_open):
            rc = tb.run_job(inbox)

        assert rc == 0
        assert opened == []
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


def test_max_rounds_split_chain_reply_unchanged(tmp_path: Path):
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
        opened: list[str] = []

        def _open(**kwargs):
            opened.append(str(kwargs.get("ticket")))
            raise AssertionError("max_rounds_split must not open an impl round")

        with mock.patch.object(tb, "run_open_slice", side_effect=_open):
            rc = tb.run_job(inbox)

        assert rc == 0
        assert opened == []
        chain = _read(dirs, "chains", slice_id)
        goal = _read(dirs, "goals", goal_id)
        assert chain["state"] == "awaiting_supervisor"
        assert chain["awaiting_reason"] == "max_rounds_split"
        assert chain["supervisor_decision"]["decision"] == "split"
        assert chain["supervisor_decision"]["via"] == "chain-reply"
        assert "Goal resumable" in str(chain["supervisor_decision"].get("note") or "")
        assert len(chain["rounds"]) == 2
        assert goal["status"] == "running"
        assert goal["awaiting_reason"] == "max_rounds_split"
        assert goal["last_chain_state"] == "awaiting_supervisor"
        assert not list(dirs["packs"].glob("*-reply-r*.pack.md"))


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
