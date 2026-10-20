#!/usr/bin/env python3
"""Unit tests for gate: per_goal (grain resolve + Goal multi-slice defer)."""
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

import gate_effort as GE

spec = importlib.util.spec_from_file_location("trial_broker_pg", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)
T = tb.T


# --------------------------------------------------------------------------
# resolve helpers
# --------------------------------------------------------------------------
def test_default_grain_is_per_goal():
    assert GE.resolve_goal_gate_grain(env={}) == "per_goal"


def test_env_overrides_grain():
    assert GE.resolve_goal_gate_grain(env={"DSH_GATE_GRAIN": "per_slice"}) == "per_slice"
    assert GE.resolve_goal_gate_grain(env={"DSH_GATE_GRAIN": "goal"}) == "per_goal"


def test_limits_and_goal_override():
    assert GE.resolve_goal_gate_grain(limits={"gate_grain": "per_slice"}, env={}) == "per_slice"
    assert (
        GE.resolve_goal_gate_grain(
            goal={"gate": "per_slice"},
            limits={"gate_grain": "per_goal"},
            env={},
        )
        == "per_slice"
    )


def test_pack_per_slice_and_skip_and_high_risk():
    pol, _ = GE.resolve_slice_gate_policy("per_goal", pack_meta={"gate": "per_slice"})
    assert pol == "per_slice"
    pol, _ = GE.resolve_slice_gate_policy("per_goal", pack_meta={"skip_gate": "true"})
    assert pol == "skip"
    pol, _ = GE.resolve_slice_gate_policy("per_goal", pack_meta={"risk": "high"})
    assert pol == "per_slice"
    pol, _ = GE.resolve_slice_gate_policy(
        "per_goal", pack_meta={}, paths=["src/auth/session.py"]
    )
    assert pol == "per_slice"
    pol, why = GE.resolve_slice_gate_policy(
        "per_goal",
        pack_meta={"skip_gate": "true"},
        paths=["src/foo.py"],
        is_goal_bearer=False,
    )
    assert pol == "defer", (pol, why)


def test_bearer_vs_defer():
    pol, _ = GE.resolve_slice_gate_policy("per_goal", is_goal_bearer=False)
    assert pol == "defer"
    pol, _ = GE.resolve_slice_gate_policy("per_goal", is_goal_bearer=True)
    assert pol == "per_goal"
    pol, _ = GE.resolve_slice_gate_policy("per_slice", is_goal_bearer=True)
    assert pol == "per_slice"


def test_standalone_always_per_slice():
    pol, _ = GE.resolve_slice_gate_policy("per_goal", from_goal=False, is_goal_bearer=True)
    assert pol == "per_slice"


# --------------------------------------------------------------------------
# multi-slice Goal integration (mocked open-slice)
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


def _make_open_slice(plan_block: dict):
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
            _write_summary(
                summary,
                {
                    "status": "done",
                    "changed_files": [],
                    "branch": "main",
                    "commit": "abc",
                    "base": "def",
                    "questions": [],
                    "notes": f"slice {sid} done",
                },
            )
        elif mode == "gate":
            _write_summary(summary, {"verdict": "PASS", "findings": [], "gate_mode": "per_goal"})
        return 0

    return fake_open_slice, calls


def _goal_job(gid: str, cwd: Path, **extra) -> dict:
    job = {
        "type": "goal",
        "id": f"job-{gid}",
        "goal": gid,
        "brief": "two small slices",
        "cwd": str(cwd),
        "profile": "acp-lite",
        "gate_profile": "acp-lite",
        "supervisor_profile": "acp-lite",
        "max_slices": 2,
        "max_supervisor_tickets": 8,
        "max_rounds": 2,
        "notify": "hub",
        "notify_dry_run": True,
    }
    job.update(extra)
    return job


def _run(job: dict, root: Path, fake_open_slice):
    inbox = root / "job-goal.json"
    inbox.write_text(json.dumps(job), encoding="utf-8")
    with mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), mock.patch.object(
        tb, "write_artifacts", side_effect=_fake_artifacts
    ), mock.patch.object(tb, "maybe_notify_hub", return_value={"sent": True, "rc": 0, "dry_run": True}):
        return tb.run_job(inbox)


def test_per_goal_two_slices_one_llm_gate():
    """Default per_goal: s1 defer (no LLM), s2 bearer → one gate ticket."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        cwd = root / "repo"
        cwd.mkdir()
        with _limits_file(root, {"max_slices": 3, "max_rounds": 2, "gate_grain": "per_goal"}):
            with _patched_dirs(root / "state") as dirs:
                gid = "unit-pg-merge"
                specs = [
                    {"slice": "unit-pg-s1", "pack": "unit-pg-s1.pack.md", "acceptance": ["a1"]},
                    {"slice": "unit-pg-s2", "pack": "unit-pg-s2.pack.md", "acceptance": ["a2"]},
                ]
                for s in specs:
                    (dirs["packs"] / s["pack"]).write_text(
                        f"---\nslice_id: {s['slice']}\n---\n# {s['slice']}\n", encoding="utf-8"
                    )
                plan = {"action": "emit_chains", "goal": gid, "slices": specs, "goal_status": "running"}
                fake_open_slice, calls = _make_open_slice(plan)
                ec = _run(_goal_job(gid, cwd), root, fake_open_slice)
                assert ec == 0, ec
                modes = [c["prompt_mode"] for c in calls]
                assert modes.count("foreman") == 2, modes
                assert modes.count("gate") == 1, modes  # only bearer
                c1 = json.loads((dirs["chains"] / "unit-pg-s1.json").read_text())
                c2 = json.loads((dirs["chains"] / "unit-pg-s2.json").read_text())
                assert c1["state"] == "PASS" and c1.get("gate_mode") == "defer"
                assert c2["state"] == "PASS"
                assert c2.get("gate_policy") == "per_goal"
                # Goal gate pack written
                packs = list(dirs["packs"].glob("*goal*.pack.md"))
                assert packs, "expected goal-gate pack"
                print("OK per_goal one LLM gate", modes)


def test_per_slice_grain_keeps_two_gates():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        cwd = root / "repo"
        cwd.mkdir()
        with _limits_file(root, {"max_slices": 3, "max_rounds": 2, "gate_grain": "per_slice"}):
            with _patched_dirs(root / "state") as dirs:
                gid = "unit-pg-slice"
                specs = [
                    {"slice": "unit-ps-s1", "pack": "unit-ps-s1.pack.md", "acceptance": ["a"]},
                    {"slice": "unit-ps-s2", "pack": "unit-ps-s2.pack.md", "acceptance": ["b"]},
                ]
                for s in specs:
                    (dirs["packs"] / s["pack"]).write_text(
                        f"---\nslice_id: {s['slice']}\n---\n# {s['slice']}\n", encoding="utf-8"
                    )
                plan = {"action": "emit_chains", "goal": gid, "slices": specs, "goal_status": "running"}
                fake_open_slice, calls = _make_open_slice(plan)
                ec = _run(_goal_job(gid, cwd, gate="per_slice"), root, fake_open_slice)
                assert ec == 0, ec
                modes = [c["prompt_mode"] for c in calls]
                assert modes.count("gate") == 2, modes
                print("OK per_slice two gates")


def test_high_risk_pack_forces_per_slice_amid_per_goal():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        cwd = root / "repo"
        cwd.mkdir()
        with _limits_file(root, {"max_slices": 3, "max_rounds": 2, "gate_grain": "per_goal"}):
            with _patched_dirs(root / "state") as dirs:
                gid = "unit-pg-risk"
                specs = [
                    {"slice": "unit-rk-s1", "pack": "unit-rk-s1.pack.md", "acceptance": ["a"]},
                    {"slice": "unit-rk-s2", "pack": "unit-rk-s2.pack.md", "acceptance": ["b"]},
                ]
                (dirs["packs"] / "unit-rk-s1.pack.md").write_text(
                    "---\nslice_id: unit-rk-s1\nrisk: high\n---\n# s1 auth\n", encoding="utf-8"
                )
                (dirs["packs"] / "unit-rk-s2.pack.md").write_text(
                    "---\nslice_id: unit-rk-s2\n---\n# s2 docs\n", encoding="utf-8"
                )
                plan = {"action": "emit_chains", "goal": gid, "slices": specs, "goal_status": "running"}
                fake_open_slice, calls = _make_open_slice(plan)
                ec = _run(_goal_job(gid, cwd), root, fake_open_slice)
                assert ec == 0, ec
                modes = [c["prompt_mode"] for c in calls]
                # s1 high-risk per_slice gate + s2 bearer per_goal gate
                assert modes.count("gate") == 2, modes
                c1 = json.loads((dirs["chains"] / "unit-rk-s1.json").read_text())
                assert c1.get("gate_policy") == "per_slice"
                c2 = json.loads((dirs["chains"] / "unit-rk-s2.json").read_text())
                assert c2.get("gate_policy") == "per_goal"
                print("OK high-risk per_slice + bearer")


def test_skip_gate_slice_no_llm_and_no_bearer_slot():
    """Closing skip_gate slice alone → synthetic skip, zero LLM gates."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        cwd = root / "repo"
        cwd.mkdir()
        with _limits_file(root, {"max_slices": 3, "max_rounds": 2, "gate_grain": "per_goal"}):
            with _patched_dirs(root / "state") as dirs:
                gid = "unit-pg-skip"
                specs = [
                    {"slice": "unit-sk-s1", "pack": "unit-sk-s1.pack.md", "acceptance": ["pr"]},
                ]
                (dirs["packs"] / "unit-sk-s1.pack.md").write_text(
                    "---\nslice_id: unit-sk-s1\nskip_gate: true\n---\n# open PR only\n",
                    encoding="utf-8",
                )
                plan = {"action": "emit_chains", "goal": gid, "slices": specs, "goal_status": "running"}
                fake_open_slice, calls = _make_open_slice(plan)
                ec = _run(_goal_job(gid, cwd, max_slices=1), root, fake_open_slice)
                assert ec == 0, ec
                modes = [c["prompt_mode"] for c in calls]
                assert modes.count("gate") == 0, modes
                c1 = json.loads((dirs["chains"] / "unit-sk-s1.json").read_text())
                assert c1.get("gate_mode") == "skip"
                print("OK skip_gate synthetic")


def test_plan_policies_helper():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        packs = root / "packs"
        packs.mkdir()
        (packs / "a.pack.md").write_text("---\nslice_id: a\n---\n", encoding="utf-8")
        (packs / "b.pack.md").write_text("---\nslice_id: b\nrisk: high\n---\n", encoding="utf-8")
        (packs / "c.pack.md").write_text("---\nslice_id: c\n---\n", encoding="utf-8")
        with mock.patch.object(tb, "PACKS", packs):
            specs = [
                {"slice": "a", "pack": "a.pack.md"},
                {"slice": "b", "pack": "b.pack.md"},
                {"slice": "c", "pack": "c.pack.md"},
            ]
            pols = tb._plan_goal_gate_policies("per_goal", specs)
            assert pols == ["defer", "per_slice", "per_goal"], pols
            pols2 = tb._plan_goal_gate_policies("per_slice", specs)
            assert pols2 == ["per_slice", "per_slice", "per_slice"], pols2
            print("OK plan helper", pols)


if __name__ == "__main__":
    test_default_grain_is_per_goal()
    test_env_overrides_grain()
    test_limits_and_goal_override()
    test_pack_per_slice_and_skip_and_high_risk()
    test_bearer_vs_defer()
    test_standalone_always_per_slice()
    test_plan_policies_helper()
    test_per_goal_two_slices_one_llm_gate()
    test_per_slice_grain_keeps_two_gates()
    test_high_risk_pack_forces_per_slice_amid_per_goal()
    test_skip_gate_slice_no_llm_and_no_bearer_slot()
    print("ALL GATE PER_GOAL UNIT OK")
