#!/usr/bin/env python3
"""Unit tests for the multi-slice Goal loop (mocked open-slice, no model / no dsh spawn).

Run with either:
  python3 -m pytest broker/tests/test_goal_multislice_unit.py -q
  python3 broker/tests/test_goal_multislice_unit.py
"""
from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_multi", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

T = tb.T


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
@contextmanager
def _limits_file(root: Path, data: dict):
    p = root / "limits.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    old = T.LIMITS_PATH
    T.LIMITS_PATH = p
    try:
        yield p
    finally:
        T.LIMITS_PATH = old


@contextmanager
def _patched_dirs(root: Path):
    names = ("goals", "processing", "outbox", "failed", "chains", "packs", "summaries", "artifacts")
    dirs = {n: root / n for n in names}
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    with mock.patch.object(tb, "GOALS", dirs["goals"]), mock.patch.object(
        tb, "PROCESSING", dirs["processing"]
    ), mock.patch.object(tb, "OUTBOX", dirs["outbox"]), mock.patch.object(
        tb, "FAILED", dirs["failed"]
    ), mock.patch.object(tb, "CHAINS", dirs["chains"]), mock.patch.object(
        tb, "PACKS", dirs["packs"]
    ), mock.patch.object(tb, "SUMMARIES", dirs["summaries"]), mock.patch.object(
        tb, "ARTIFACT_ROOT", dirs["artifacts"]
    ), mock.patch.object(tb, "maybe_offload_gc", lambda **k: None):
        yield dirs


def _write_summary(path: Path, block: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "prose\n\n```json\n" + json.dumps(block, ensure_ascii=False, indent=2) + "\n```\n",
        encoding="utf-8",
    )


def _fake_artifacts(job, log_path, summary_path, ec, ticket=None):
    return {
        "ticket": ticket or job.get("ticket"),
        "exit_code": ec,
        "assert_clean": True,
        "composition": None,
        "summary": str(summary_path),
        "summary_exists": summary_path.is_file(),
    }


def _slice_id_from_ticket(ticket: str, prefix: str) -> str:
    return ticket.split(prefix, 1)[-1].rsplit("-r", 1)[0]


def _make_open_slice(plan_block: dict, *, blocked_slices=()):
    """Return (fake_open_slice, calls) simulating plan/foreman/gate/close tickets."""
    calls: list[dict] = []

    def fake_open_slice(**kwargs):
        calls.append(kwargs)
        mode = kwargs["prompt_mode"]
        summary = tb.SUMMARIES / f"{kwargs['summary_name']}.md"
        kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["log_path"].write_text(f"fake {kwargs['ticket']} mode={mode}\n", encoding="utf-8")
        if mode == "supervisor-plan":
            _write_summary(summary, plan_block)
        elif mode == "supervisor-close":
            _write_summary(
                summary,
                {"action": "goal_done", "goal_status": "done", "slices": plan_block.get("slices", [])},
            )
        elif mode == "foreman":
            sid = _slice_id_from_ticket(kwargs["ticket"], "impl-trial-")
            status = "blocked" if sid in blocked_slices else "done"
            _write_summary(
                summary,
                {
                    "status": status,
                    "changed_files": [],
                    "branch": "main",
                    "commit": "",
                    "base": "",
                    "questions": [],
                    "notes": f"slice {sid} {status}",
                },
            )
        elif mode == "gate":
            sid = _slice_id_from_ticket(kwargs["ticket"], "gate-trial-")
            _write_summary(summary, {"verdict": "PASS", "findings": []})
        return 0

    return fake_open_slice, calls


def _goal_job(gid: str, cwd: Path, *, max_slices: int = 2) -> dict:
    return {
        "type": "goal",
        "id": f"job-{gid}",
        "goal": gid,
        "brief": "two small slices",
        "cwd": str(cwd),
        "profile": "acp-lite",
        "gate_profile": "acp-lite",
        "supervisor_profile": "acp-lite",
        "max_slices": max_slices,
        "max_supervisor_tickets": 8,
        "max_rounds": 2,
        "notify": "hub",
        "notify_dry_run": True,
    }


def _run(job: dict, root: Path, fake_open_slice, calls) -> int:
    inbox = root / "job-goal.json"
    inbox.write_text(json.dumps(job), encoding="utf-8")
    with mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), mock.patch.object(
        tb, "write_artifacts", side_effect=_fake_artifacts
    ), mock.patch.object(tb, "maybe_notify_hub", return_value={"sent": True, "rc": 0, "dry_run": True}):
        return tb.run_job(inbox)


# --------------------------------------------------------------------------
# tests
# --------------------------------------------------------------------------
def test_two_slices_all_pass():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        cwd = root / "repo"
        cwd.mkdir()
        with _limits_file(root, {"max_slices": 3, "max_rounds": 2}):
            with _patched_dirs(root / "state") as dirs:
                gid = "unit-ms-pass"
                specs = [
                    {"slice": "unit-ms-s1", "pack": "unit-ms-s1.pack.md", "acceptance": ["a1"]},
                    {"slice": "unit-ms-s2", "pack": "unit-ms-s2.pack.md", "acceptance": ["a2"]},
                ]
                for s in specs:
                    (dirs["packs"] / s["pack"]).write_text(
                        f"---\nslice_id: {s['slice']}\n---\n# {s['slice']}\n", encoding="utf-8"
                    )
                plan = {"action": "emit_chains", "goal": gid, "slices": specs, "goal_status": "running"}
                fake_open_slice, calls = _make_open_slice(plan)

                ec = _run(_goal_job(gid, cwd, max_slices=2), root, fake_open_slice, calls)

                assert ec == 0, ec
                goal = json.loads((dirs["goals"] / f"{gid}.json").read_text())
                assert goal["status"] == "done", goal.get("status")
                assert goal["slices"] == ["unit-ms-s1", "unit-ms-s2"], goal["slices"]
                assert goal["metrics"]["slices_planned"] == 2
                assert goal["metrics"]["slices_completed"] == 2
                for s in specs:
                    chain = json.loads((dirs["chains"] / f"{s['slice']}.json").read_text())
                    assert chain["state"] == "PASS", chain
                modes = [c["prompt_mode"] for c in calls]
                assert modes.count("supervisor-plan") == 1
                assert modes.count("supervisor-close") == 1, modes
                assert modes.count("foreman") == 2
                assert modes.count("gate") == 2
                print("OK two slices all PASS", goal["slices"])


def test_emit_chains_over_max_slices_hits_limit():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        cwd = root / "repo"
        cwd.mkdir()
        with _limits_file(root, {"max_slices": 3, "max_rounds": 2}):
            with _patched_dirs(root / "state") as dirs:
                gid = "unit-ms-limit"
                specs = [
                    {"slice": "unit-ms-l1", "pack": "unit-ms-l1.pack.md", "acceptance": ["a"]},
                    {"slice": "unit-ms-l2", "pack": "unit-ms-l2.pack.md", "acceptance": ["b"]},
                    {"slice": "unit-ms-l3", "pack": "unit-ms-l3.pack.md", "acceptance": ["c"]},
                ]
                plan = {"action": "emit_chains", "goal": gid, "slices": specs, "goal_status": "running"}
                fake_open_slice, calls = _make_open_slice(plan)

                ec = _run(_goal_job(gid, cwd, max_slices=2), root, fake_open_slice, calls)

                assert ec == 1, ec
                goal = json.loads((dirs["goals"] / f"{gid}.json").read_text())
                assert goal["status"] == "escalated", goal.get("status")
                hit = goal.get("limit_hit") or {}
                assert hit.get("limit") == "max_slices", hit
                assert hit.get("limit_value") == 2, hit
                assert hit.get("used") == 3, hit
                assert hit in goal["metrics"]["limit_hits"]
                chain_files = list(dirs["chains"].glob("*.json"))
                assert chain_files == [], chain_files
                assert [c["prompt_mode"] for c in calls] == ["supervisor-plan"]
                print("OK max_slices limit payload", hit.get("used"))


def test_legacy_emit_chain_single_slice():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        cwd = root / "repo"
        cwd.mkdir()
        with _limits_file(root, {"max_slices": 3, "max_rounds": 2}):
            with _patched_dirs(root / "state") as dirs:
                gid = "unit-ms-legacy"
                pack = "unit-ms-legacy.pack.md"
                (dirs["packs"] / pack).write_text("# legacy\n", encoding="utf-8")
                plan = {
                    "action": "emit_chain",
                    "goal": gid,
                    "slice": "unit-ms-legacy",
                    "pack": pack,
                    "acceptance": ["x"],
                    "goal_status": "running",
                }
                fake_open_slice, calls = _make_open_slice(plan)

                ec = _run(_goal_job(gid, cwd, max_slices=1), root, fake_open_slice, calls)

                assert ec == 0, ec
                goal = json.loads((dirs["goals"] / f"{gid}.json").read_text())
                assert goal["status"] == "done", goal.get("status")
                assert goal["slices"] == ["unit-ms-legacy"], goal["slices"]
                assert goal["metrics"]["slices_completed"] == 1
                assert [c["prompt_mode"] for c in calls].count("supervisor-close") == 1
                print("OK legacy emit_chain single slice")


def test_on_slice_fail_stop_default():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        cwd = root / "repo"
        cwd.mkdir()
        with _limits_file(root, {"max_slices": 3, "max_rounds": 2}):
            with _patched_dirs(root / "state") as dirs:
                gid = "unit-ms-stop"
                specs = [
                    {"slice": "unit-ms-t1", "pack": "unit-ms-t1.pack.md", "acceptance": ["a"]},
                    {"slice": "unit-ms-t2", "pack": "unit-ms-t2.pack.md", "acceptance": ["b"]},
                ]
                for s in specs:
                    (dirs["packs"] / s["pack"]).write_text("# slice\n", encoding="utf-8")
                plan = {"action": "emit_chains", "goal": gid, "slices": specs, "goal_status": "running"}
                fake_open_slice, calls = _make_open_slice(plan, blocked_slices={"unit-ms-t1"})

                ec = _run(_goal_job(gid, cwd, max_slices=2), root, fake_open_slice, calls)

                assert ec == 1, ec
                goal = json.loads((dirs["goals"] / f"{gid}.json").read_text())
                assert goal["slices"] == ["unit-ms-t1"], goal["slices"]
                assert goal["metrics"]["slices_completed"] == 0
                assert not (dirs["chains"] / "unit-ms-t2.json").exists()
                modes = [c["prompt_mode"] for c in calls]
                assert "supervisor-close" not in modes, modes
                print("OK on_slice_fail default stop")


def main():
    test_two_slices_all_pass()
    test_emit_chains_over_max_slices_hits_limit()
    test_legacy_emit_chain_single_slice()
    test_on_slice_fail_stop_default()
    print("ALL MULTISLICE UNIT OK")


if __name__ == "__main__":
    main()
