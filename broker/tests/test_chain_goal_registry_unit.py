#!/usr/bin/env python3
"""Chain ↔ Goal registry + late-reply refusal (max-rounds split closeout).

Covers: ``type=chain`` with ``goal=`` registers the slice (from_goal), missing /
terminal Goals are refused instead of opening orphan chains, and a split
chain-reply for an already-closed Goal is an idempotent no-op.
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
    "trial_broker_chain_goal_registry", ROOT / "trial-broker.py"
)
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

_DIRS = (
    "goals",
    "chains",
    "processing",
    "outbox",
    "failed",
    "packs",
    "summaries",
    "artifacts",
    "inbox",
)


def _patched_dirs(root: Path):
    dirs = {n: root / n for n in _DIRS}
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    patcher = mock.patch.multiple(
        tb,
        GOALS=dirs["goals"],
        CHAINS=dirs["chains"],
        PROCESSING=dirs["processing"],
        OUTBOX=dirs["outbox"],
        FAILED=dirs["failed"],
        PACKS=dirs["packs"],
        SUMMARIES=dirs["summaries"],
        ARTIFACT_ROOT=dirs["artifacts"],
        INBOX=dirs["inbox"],
        maybe_offload_gc=lambda **k: None,
    )
    return patcher, dirs


def _chain_job(dirs, tmp_path: Path, *, slice_id: str, goal_id: str | None, tag: str = "j1") -> tuple[Path, dict]:
    (dirs["packs"] / f"{slice_id}.pack.md").write_text(
        f"---\nslice_id: {slice_id}\n---\n# pack\n", encoding="utf-8"
    )
    job = {
        "type": "chain",
        "id": tag,
        "slice": slice_id,
        "pack": f"{slice_id}.pack.md",
        "acceptance": ["done"],
        "profile": "acp-lite",
        "gate_profile": "acp-lite",
        "cwd": str(tmp_path),
        "max_rounds": 2,
    }
    if goal_id is not None:
        job["goal"] = goal_id
    path = dirs["inbox"] / f"{tag}.json"
    path.write_text(json.dumps(job, ensure_ascii=False), encoding="utf-8")
    return path, job


def _write_goal(dirs, goal: dict) -> Path:
    path = dirs["goals"] / f"{goal['goal']}.json"
    path.write_text(json.dumps(goal, ensure_ascii=False) + "\n", encoding="utf-8")
    return path


def _write_chain(dirs, chain: dict) -> Path:
    path = dirs["chains"] / f"{chain['slice']}.json"
    path.write_text(json.dumps(chain, ensure_ascii=False) + "\n", encoding="utf-8")
    return path


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _parked_chain(slice_id: str, goal_id: str, tmp_path: Path) -> dict:
    return {
        "slice": slice_id,
        "type": "chain",
        "state": "awaiting_supervisor",
        "goal": goal_id,
        "from_goal": True,
        "pack": f"{slice_id}.pack.md",
        "acceptance": ["done"],
        "profile": "acp-lite",
        "gate_profile": "acp-lite",
        "cwd": str(tmp_path),
        "max_rounds": 2,
        "awaiting_answer_for_round": 2,
        "awaiting_reason": "max_rounds_split",
        "pending_impl_pack": f"{slice_id}.pack.md",
        "rounds": [{"round": 1}, {"round": 2}],
    }


def _reply(dirs, slice_id: str, *, tag: str = "reply1") -> tuple[Path, dict]:
    job = {
        "type": "chain-reply",
        "id": tag,
        "slice": slice_id,
        "decision": "split",
        "answer": "split remaining P0",
    }
    path = dirs["inbox"] / f"{tag}.json"
    path.write_text(json.dumps(job, ensure_ascii=False), encoding="utf-8")
    return path, job


def test_chain_job_registers_slice_on_goal(tmp_path: Path):
    """a) ``goal=`` puts the slice in goal["slices"] and marks from_goal."""
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        goal_id = "registry-goal"
        slice_id = "registry-slice-s1"
        _write_goal(dirs, {"goal": goal_id, "status": "running", "brief": "x", "slices": []})
        path, job = _chain_job(dirs, tmp_path, slice_id=slice_id, goal_id=goal_id)
        seen: list[str] = []

        def _rounds(job_, chain_, dest, **_kw):
            seen.append(str(chain_.get("state")))
            return 0

        with mock.patch.object(tb, "run_chain_rounds", side_effect=_rounds):
            rc = tb.run_chain_job(path, job)
        assert rc == 0
        assert seen == ["running"]

        goal = _read(dirs["goals"] / f"{goal_id}.json")
        chain = _read(dirs["chains"] / f"{slice_id}.json")
        assert goal["slices"] == [slice_id]
        assert goal["last_chain_state"] == "running"
        assert chain["from_goal"] is True
        assert chain["goal"] == goal_id

        # Re-delivery of the same slice must not duplicate the registry entry.
        path2, job2 = _chain_job(dirs, tmp_path, slice_id=slice_id, goal_id=goal_id, tag="j2")
        with mock.patch.object(tb, "run_chain_rounds", side_effect=_rounds):
            assert tb.run_chain_job(path2, job2) == 0
        goal = _read(dirs["goals"] / f"{goal_id}.json")
        assert goal["slices"] == [slice_id]


def test_standalone_chain_without_goal_stays_unregistered(tmp_path: Path):
    """``goal`` empty or equal to the slice id keeps the standalone shape."""
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        slice_id = "standalone-slice"
        path, job = _chain_job(dirs, tmp_path, slice_id=slice_id, goal_id=slice_id)
        with mock.patch.object(tb, "run_chain_rounds", return_value=0):
            assert tb.run_chain_job(path, job) == 0
        chain = _read(dirs["chains"] / f"{slice_id}.json")
        assert "goal" not in chain
        assert "from_goal" not in chain
        assert not list(dirs["goals"].glob("*.json"))


def test_done_goal_refuses_chain_and_split_reply(tmp_path: Path):
    """b) terminal Goal: chain → FAILED; split reply → rc=0, nothing written."""
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        goal_id = "done-goal"
        slice_id = "done-goal-s1"
        goal_path = _write_goal(
            dirs, {"goal": goal_id, "status": "done", "brief": "x", "slices": []}
        )
        before = goal_path.read_text(encoding="utf-8")

        path, job = _chain_job(dirs, tmp_path, slice_id=slice_id, goal_id=goal_id)
        with mock.patch.object(
            tb, "run_chain_rounds", side_effect=AssertionError("no orphan chain")
        ):
            rc = tb.run_chain_job(path, job)
        assert rc == 1
        assert not (dirs["chains"] / f"{slice_id}.json").exists()
        errors = list(dirs["failed"].glob("*.error.json"))
        assert errors, "rejected chain must be observable in FAILED"
        assert "late chain for terminal goal" in json.dumps(
            [e.read_text(encoding="utf-8") for e in errors]
        )
        assert goal_path.read_text(encoding="utf-8") == before

        # chain-reply split for the same (closed) Goal is a no-op.
        chain_path = _write_chain(dirs, _parked_chain(slice_id, goal_id, tmp_path))
        chain_before = _read(chain_path)
        rpath, rjob = _reply(dirs, slice_id)
        with mock.patch.object(
            tb, "run_open_slice", side_effect=AssertionError("no impl round")
        ):
            rc2 = tb.run_chain_reply_job(rpath, rjob)
        assert rc2 == 0
        assert goal_path.read_text(encoding="utf-8") == before
        chain = _read(chain_path)
        assert chain["state"] == chain_before["state"] == "awaiting_supervisor"
        assert chain["awaiting_reason"] == "max_rounds_split"
        assert "awaiting_reason" not in _read(goal_path)
        assert len(chain["late_chain_reply_ignored"]) == 1
        assert not list(dirs["packs"].glob("*-reply-r*.pack.md"))


def test_late_split_reply_is_idempotent(tmp_path: Path):
    """c) The same late split twice: Goal bytes stable, marker stays length 1."""
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        goal_id = "late-goal"
        slice_id = "late-goal-s1"
        goal_path = _write_goal(
            dirs,
            {
                "goal": goal_id,
                "status": "done",
                "brief": "x",
                "slices": [slice_id],
                "awaiting_reason": "max_rounds_split",
            },
        )
        before = goal_path.read_text(encoding="utf-8")
        chain_path = _write_chain(dirs, _parked_chain(slice_id, goal_id, tmp_path))

        for tag in ("reply1", "reply2"):
            rpath, rjob = _reply(dirs, slice_id, tag=tag)
            with mock.patch.object(
                tb, "run_open_slice", side_effect=AssertionError("no impl round")
            ):
                assert tb.run_chain_reply_job(rpath, rjob) == 0
            assert goal_path.read_text(encoding="utf-8") == before

        chain = _read(chain_path)
        assert len(chain["late_chain_reply_ignored"]) == 1
        assert chain["late_chain_reply_ignored"][0]["decision"] == "split"
        assert chain["state"] == "awaiting_supervisor"
        assert not list(dirs["failed"].glob("*.json"))


def test_late_reply_marker_dedupes_by_decision(tmp_path: Path):
    """Same decision with a different reason keeps a single marker (first reason)."""
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        goal_id = "late-goal"
        slice_id = "late-goal-s1"
        _write_goal(
            dirs,
            {
                "goal": goal_id,
                "status": "done",
                "brief": "x",
                "slices": [slice_id],
            },
        )
        chain_path = _write_chain(dirs, _parked_chain(slice_id, goal_id, tmp_path))
        dest = dirs["processing"] / "late.json"
        dest.write_text("{}", encoding="utf-8")

        for reason in ("max_rounds_split", "goal terminal"):
            assert (
                tb._ignore_late_chain_reply(
                    tb._read_chain(slice_id),
                    dest,
                    slice_id,
                    decision="split",
                    reason=reason,
                )
                == 0
            )

        chain = _read(chain_path)
        assert len(chain["late_chain_reply_ignored"]) == 1
        assert chain["late_chain_reply_ignored"][0]["reason"] == "max_rounds_split"
        assert chain["state"] == "awaiting_supervisor"


def test_missing_goal_refuses_chain(tmp_path: Path):
    """d) Unknown Goal id: refused into FAILED, no orphan chain state."""
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        slice_id = "orphan-slice"
        path, job = _chain_job(dirs, tmp_path, slice_id=slice_id, goal_id="no-such-goal")
        with mock.patch.object(
            tb, "run_chain_rounds", side_effect=AssertionError("no orphan chain")
        ):
            rc = tb.run_chain_job(path, job)
        assert rc == 1
        assert not (dirs["chains"] / f"{slice_id}.json").exists()
        errors = list(dirs["failed"].glob("*.error.json"))
        assert errors
        assert "chain goal missing" in json.dumps(
            [e.read_text(encoding="utf-8") for e in errors]
        )
