#!/usr/bin/env python3
"""max_rounds split closeout: parent chain is superseded, Goal keeps running.

Covers slice S1 of goal dsh-max-rounds-split-closeout-20261024:
  a) planned next slice → parent superseded + s2 registered + inbox chain job
  b) no planned next slice → Goal split_pending, no inbox job
  c) auto_supervisor_answer=False → old park behaviour unchanged
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
    "trial_broker_max_rounds_split_closeout", ROOT / "trial-broker.py"
)
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

G = "split-closeout-goal"
S1 = "split-closeout-s1"
S2 = "split-closeout-s2"


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


def _write_json(path: Path, body: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(body, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_summary(path: Path, block: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("```json\n" + json.dumps(block, ensure_ascii=False, indent=2) + "\n```\n", encoding="utf-8")


def _seed(dirs, tmp_path: Path, *, plan_slices: list[dict] | None) -> tuple[dict, dict]:
    goal = {
        "goal": G,
        "status": "running",
        "brief": "x",
        "slices": [S1],
        "cwd": str(tmp_path),
        "profile": "acp",
        "gate_profile": "acp-gate",
        "max_rounds": 2,
    }
    if plan_slices is not None:
        plan = dirs["summaries"] / f"goal-{G}-plan.md"
        _write_summary(plan, {"action": "emit_chains", "slices": plan_slices})
        goal["plan_summary"] = str(plan)
    chain = {
        "slice": S1,
        "type": "chain",
        "state": "awaiting_supervisor",
        "goal": G,
        "pack": f"{S1}.pack.md",
        "acceptance": ["a"],
        "profile": "acp-lite",
        "gate_profile": "acp-lite",
        "cwd": str(tmp_path),
        "max_rounds": 2,
        "rounds": [
            {"round": 1, "impl_pack": f"{S1}.pack.md", "impl_ticket": "impl-r1"},
            {"round": 2, "impl_pack": f"{S1}.pack.md", "impl_ticket": "impl-r2"},
        ],
        "awaiting_answer_for_round": 2,
        "awaiting_reason": "max_rounds",
        "pending_impl_pack": f"{S1}.pack.md",
    }
    goal["awaiting_reason"] = "max_rounds"
    goal["max_rounds_decision_pending"] = {"at": "x", "round_n": 2}
    _write_json(dirs["goals"] / f"{G}.json", goal)
    _write_json(dirs["chains"] / f"{S1}.json", chain)
    for sid in (S1, S2):
        (dirs["packs"] / f"{sid}.pack.md").write_text(
            f"---\nslice_id: {sid}\n---\n# pack\n", encoding="utf-8"
        )
    return goal, chain


def _run(chain: dict, goal: dict, dest: Path, *, auto=True) -> str:
    job = {
        "id": f"goal-chain-{S1}",
        "type": "chain",
        "slice": S1,
        "pack": f"{S1}.pack.md",
        "goal": G,
        "from_goal": True,
        "auto_supervisor_answer": auto,
        "profile": "acp",
        "gate_profile": "acp-gate",
        "cwd": str(dest),
        "max_rounds": 2,
    }
    with mock.patch.object(tb, "auto_supervisor_max_rounds_decision", return_value="split"), \
         mock.patch.object(tb, "emit_split_followup", return_value=False), \
         mock.patch.object(tb, "_notify_awaiting_hub", lambda *a, **k: None):
        return tb.handle_max_rounds_touch(
            job,
            chain,
            dest,
            round_n=2,
            max_rounds=2,
            findings=["P1: leftover"],
            reason="remaining P0/P1",
            goal_obj=goal,
            slice_id=S1,
            pack_for_impl=f"{S1}.pack.md",
            source="chain",
        )


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_split_with_planned_next_slice_supersedes_parent(tmp_path: Path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        goal, chain = _seed(
            dirs,
            tmp_path,
            plan_slices=[
                {"slice": S1, "pack": f"{S1}.pack.md", "acceptance": ["a"]},
                {"slice": S2, "pack": f"{S2}.pack.md", "acceptance": ["b"]},
            ],
        )
        assert _run(chain, goal, dirs["processing"] / "j.json") == "split"

        parent = _read_json(dirs["chains"] / f"{S1}.json")
        assert parent["state"] == "superseded"
        assert parent["superseded_reason"] == "max_rounds_split"
        assert parent["superseded_by"] == S2
        assert "awaiting_reason" not in parent
        assert parent["supervisor_decision"]["decision"] == "split"
        assert parent["supervisor_decision"]["source"] == "chain"
        assert parent["supervisor_decision"]["next_slice"] == S2

        stored_goal = _read_json(dirs["goals"] / f"{G}.json")
        assert stored_goal["status"] == "running"
        assert "awaiting_reason" not in stored_goal
        assert "max_rounds_decision_pending" not in stored_goal
        assert stored_goal["last_chain_state"] == "running"
        assert S2 in stored_goal["slices"]
        assert stored_goal["next_slice"] == S2
        assert "split_pending" not in stored_goal

        jobs = []
        for p in dirs["inbox"].glob("*.json"):
            body = _read_json(p)
            if body.get("type") == "chain":
                jobs.append(body)
        assert len(jobs) == 1
        job = jobs[0]
        assert job["slice"] == S2
        assert job["goal"] == G
        assert job["from_goal"] is True
        assert job["profile"] == "acp"
        assert job["gate_profile"] == "acp-gate"
        assert job["cwd"] == str(tmp_path)
        assert job["max_rounds"] == 2


def test_split_without_next_slice_records_split_pending(tmp_path: Path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        goal, chain = _seed(
            dirs,
            tmp_path,
            plan_slices=[{"slice": S1, "pack": f"{S1}.pack.md", "acceptance": ["a"]}],
        )
        assert _run(chain, goal, dirs["processing"] / "j.json") == "split"

        parent = _read_json(dirs["chains"] / f"{S1}.json")
        assert parent["state"] == "superseded"
        assert parent["superseded_by"] == ""
        assert "awaiting_reason" not in parent

        stored_goal = _read_json(dirs["goals"] / f"{G}.json")
        assert "awaiting_reason" not in stored_goal
        assert stored_goal["status"] == "running"
        pending = stored_goal["split_pending"]
        assert pending["parent_slice"] == S1
        assert pending["reason"] == "remaining P0/P1"
        assert pending["findings"] == ["P1: leftover"]
        assert pending["at"]

        assert list(dirs["inbox"].glob("*.json")) == []


def test_split_auto_off_keeps_park_behaviour(tmp_path: Path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        goal, chain = _seed(
            dirs,
            tmp_path,
            plan_slices=[{"slice": S1, "pack": f"{S1}.pack.md", "acceptance": ["a"]}],
        )
        with mock.patch.object(tb, "_notify_awaiting_hub", lambda *a, **k: None):
            rc = tb.handle_max_rounds_touch(
                {
                    "id": f"goal-chain-{S1}",
                    "type": "chain",
                    "slice": S1,
                    "pack": f"{S1}.pack.md",
                    "goal": G,
                    "from_goal": True,
                    "auto_supervisor_answer": False,
                },
                chain,
                dirs["processing"] / "j.json",
                round_n=2,
                max_rounds=2,
                findings=["P1: leftover"],
                reason="remaining P0/P1",
                goal_obj=goal,
                slice_id=S1,
                pack_for_impl=f"{S1}.pack.md",
                source="chain",
            )
        assert rc == "parked"

        parent = _read_json(dirs["chains"] / f"{S1}.json")
        assert parent["state"] == "awaiting_supervisor"
        assert parent["awaiting_reason"] == "max_rounds"

        stored_goal = _read_json(dirs["goals"] / f"{G}.json")
        assert stored_goal["awaiting_reason"] == "max_rounds"
        assert stored_goal["last_chain_state"] == "awaiting_supervisor"
        assert "split_pending" not in stored_goal
        assert list(dirs["inbox"].glob("*.json")) == []
