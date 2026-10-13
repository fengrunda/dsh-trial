#!/usr/bin/env python3
"""Unit tests for per-slice fresh ACP sessions + thin-state prior-slices digest.

Run with either:
  python3 -m pytest broker/tests/test_slice_fresh_session_unit.py -q
  python3 broker/tests/test_slice_fresh_session_unit.py
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

spec = importlib.util.spec_from_file_location("trial_broker_fresh", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

T = tb.T


# --------------------------------------------------------------------------
# helpers (mirrored from test_goal_multislice_unit.py)
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


def _make_open_slice(plan_block: dict):
    """Return (fake_open_slice, calls) covering plan/close supervisor tickets."""
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
def test_prior_slices_digest_truncated_and_contains_sid():
    with tempfile.TemporaryDirectory() as td:
        with _patched_dirs(Path(td) / "state") as dirs:
            gid = "unit-fs-digest"
            long_summary = "/supervisor/thin-state/summaries/" + "x" * 200
            ids: list[str] = []
            for i in range(12):
                sid = f"unit-fs-d{i}"
                ids.append(sid)
                tb._write_chain(
                    {
                        "slice": sid,
                        "type": "chain",
                        "state": "PASS",
                        "commit": f"abc{i}",
                        "rounds": [{"summary": f"{long_summary}-{i}.md"}],
                    }
                )
            digest = tb.build_prior_slices_digest(gid, ids)
            assert len(digest) <= 1500, len(digest)
            assert "unit-fs-d0" in digest, digest[:200]
            assert digest.count("\n- ") >= 1
            first = tb.build_prior_slices_digest(gid, ids[:1])
            assert f"- {ids[0]}: state=PASS" in first
            assert f"last_commit=abc0" in first
            assert long_summary in first
            print("OK digest truncation", len(digest))


def test_second_slice_pack_gets_prior_digest_once():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        cwd = root / "repo"
        cwd.mkdir()
        with _limits_file(root, {"max_slices": 3, "max_rounds": 2}):
            with _patched_dirs(root / "state") as dirs:
                gid = "unit-fs-fresh"
                specs = [
                    {"slice": "unit-fs-s1", "pack": "unit-fs-s1.pack.md", "acceptance": ["a1"]},
                    {"slice": "unit-fs-s2", "pack": "unit-fs-s2.pack.md", "acceptance": ["a2"]},
                ]
                for s in specs:
                    (dirs["packs"] / s["pack"]).write_text(
                        f"---\nslice_id: {s['slice']}\n---\n# {s['slice']}\n", encoding="utf-8"
                    )
                plan = {"action": "emit_chains", "goal": gid, "slices": specs, "goal_status": "running"}
                fake_open_slice, calls = _make_open_slice(plan)

                chain_calls: list[str] = []

                def fake_run_chain_rounds(chain_job, chain, dest, **kwargs):
                    chain_calls.append(chain_job["slice"])
                    ch = tb._read_chain(chain_job["slice"]) or dict(chain)
                    ch["state"] = "PASS"
                    ch["rounds"] = [{"summary": f"/summaries/{chain_job['slice']}-impl-r1.md"}]
                    tb._write_chain(ch)
                    return 0

                with mock.patch.object(
                    tb, "run_chain_rounds", side_effect=fake_run_chain_rounds
                ):
                    ec = _run(_goal_job(gid, cwd, max_slices=2), root, fake_open_slice, calls)

                assert ec == 0, ec
                assert chain_calls == ["unit-fs-s1", "unit-fs-s2"], chain_calls
                p1 = (dirs["packs"] / "unit-fs-s1.pack.md").read_text(encoding="utf-8")
                p2 = (dirs["packs"] / "unit-fs-s2.pack.md").read_text(encoding="utf-8")
                # First slice had no prior slices; second got the digest once.
                assert tb.PRIOR_SLICES_HEADING not in p1
                assert p2.count(tb.PRIOR_SLICES_HEADING) == 1, p2
                assert "unit-fs-s1" in p2 and "state=PASS" in p2
                # Idempotency: a re-append call would not duplicate the heading.
                tb._append_prior_slices_digest(dirs["packs"] / "unit-fs-s2.pack.md", gid, ["unit-fs-s1"])
                p2b = (dirs["packs"] / "unit-fs-s2.pack.md").read_text(encoding="utf-8")
                assert p2b.count(tb.PRIOR_SLICES_HEADING) == 1
                goal = json.loads((dirs["goals"] / f"{gid}.json").read_text())
                assert goal["status"] == "done", goal.get("status")
                print("OK fresh-session digest appended once", chain_calls)


def test_open_slice_args_force_new_session():
    script = (ROOT.parent / "open-slice.sh").read_text(encoding="utf-8")
    for line in script.splitlines():
        if line.startswith("ARGS=(--profile"):
            assert "--new" in line, line
            break
    else:
        raise AssertionError("ARGS=(--profile ...) line not found in open-slice.sh")
    print("OK open-slice.sh forces --new per ticket/slice")


if __name__ == "__main__":
    test_prior_slices_digest_truncated_and_contains_sid()
    test_second_slice_pack_gets_prior_digest_once()
    test_open_slice_args_force_new_session()
    print("ALL OK")
