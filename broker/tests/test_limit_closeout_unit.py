#!/usr/bin/env python3
"""Limit-stop closeout: hard cap and max_steps share one path.

Idle timeout and a plain non-zero exit stay on the old failure path.
A clean worktree still fails, but the handoff says there was nothing to commit.
Closeout must not open a second impl ticket and must not mark the slice PASS.
"""
from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_limit_closeout", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)


def _patched_dirs(root: Path):
    names = ("goals", "processing", "outbox", "failed", "chains", "packs", "summaries", "artifacts")
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
        maybe_offload_gc=lambda **k: None,
    ), dirs


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True, text=True)


def _init_repo(repo: Path, branch: str = "feat/slice") -> None:
    repo.mkdir(parents=True, exist_ok=True)
    _git(repo, "init", "-b", branch)
    _git(repo, "config", "user.email", "closeout-test@example.com")
    _git(repo, "config", "user.name", "closeout-test")
    (repo / "src").mkdir()
    (repo / "src" / "keep.py").write_text("VALUE = 1\n", encoding="utf-8")
    (repo / "tests").mkdir()
    (repo / "tests" / "test_ok.py").write_text(
        "def test_ok():\n    assert True\n", encoding="utf-8"
    )
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "base")
    _git(repo, "remote", "add", "origin", "https://github.com/fengrunda/memory-as-training.git")


def _dirty(repo: Path) -> None:
    (repo / "src" / "new_work.py").write_text("VALUE = 2\n", encoding="utf-8")
    cache = repo / "tests" / "__pycache__"
    cache.mkdir(parents=True, exist_ok=True)
    (cache / "test_ok.cpython-312.pyc").write_bytes(b"\x00junk")
    (repo / ".pytest_cache").mkdir(exist_ok=True)
    (repo / ".pytest_cache" / "README.md").write_text("cache\n", encoding="utf-8")


def _write_pack(dirs: dict, slice_id: str, repo: Path) -> None:
    (dirs["packs"] / f"{slice_id}.pack.md").write_text(
        "\n".join(
            [
                "---",
                "kind: slice-pack",
                f"slice: {slice_id}",
                f"repo: {repo}",
                "branch: feat/slice",
                "---",
                "",
                "Run `python3 -m pytest -q`.",
                "用 dsh-trial-pr 向 fengrunda/memory-as-training 开 PR。",
                "",
            ]
        ),
        encoding="utf-8",
    )


def _seed_goal(dirs: dict, goal_id: str, cwd: Path) -> None:
    goal = {
        "goal": goal_id,
        "status": "running",
        "cwd": str(cwd),
        "slices": [],
        "metrics": tb.T.empty_metrics(),
    }
    (dirs["goals"] / f"{goal_id}.json").write_text(
        json.dumps(goal, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def _pr_script(path: Path, log: Path) -> None:
    path.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' \"$*\" >> {log}\n"
        "echo 'https://github.com/fengrunda/memory-as-training/pull/4242'\n",
        encoding="utf-8",
    )
    path.chmod(0o755)


def _run(dirs, repo, slice_id, goal_id, log_body, pr_bin: Path) -> tuple[int, list[str]]:
    calls: list[str] = []

    def fake_open_slice(**kwargs):
        calls.append(kwargs.get("ticket") or "")
        kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["log_path"].write_text(log_body, encoding="utf-8")
        return 1

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
        "id": f"unit-{slice_id}",
        "type": "goal",
        "goal": goal_id,
        "profile": "acp-lite",
        "cwd": str(repo),
        "max_rounds": 2,
    }
    dest = dirs["processing"] / f"job-{slice_id}.json"
    dest.write_text("{}\n", encoding="utf-8")
    goal = json.loads((dirs["goals"] / f"{goal_id}.json").read_text(encoding="utf-8"))
    with mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), mock.patch.object(
        tb, "write_artifacts", side_effect=fake_artifacts
    ), mock.patch.dict(os.environ, {"DSH_TRIAL_PR": str(pr_bin)}):
        rc = tb._execute_goal_slices(
            goal,
            job,
            [{"slice": slice_id, "pack": f"{slice_id}.pack.md", "acceptance": []}],
            dest,
            on_slice_fail="stop",
            plan_ticket="plan-unit",
        )
    return rc, calls


def _load(dirs, goal_id, slice_id):
    goal = json.loads((dirs["goals"] / f"{goal_id}.json").read_text(encoding="utf-8"))
    chain = json.loads((dirs["chains"] / f"{slice_id}.json").read_text(encoding="utf-8"))
    return goal, chain


def _handoff_text(chain) -> str:
    path = Path(chain["handoff_summary"])
    assert path.is_file(), path
    return path.read_text(encoding="utf-8")


HARD = (
    "prompt mentions 步数纪律（软上限）and impl_max_steps before the run.\n"
    "=== run ===\n"
    "ACP session/prompt failed: timeout after 3600s hard cap "
    "(no session/prompt result from agent)\n"
)
STEPS = (
    "=== run ===\n"
    "foreman stopped: max_steps limit hit (impl_max_steps exceeded)\n"
)
IDLE = (
    "impl_max_steps hit would be a false positive if read from the prompt.\n"
    "=== run ===\n"
    "ACP session/prompt failed: timeout after 900s idle "
    "(no ACP stdout from agent)\n"
)
PLAIN = "=== run ===\nforeman crashed: assertion failed\n"


def test_hard_cap_with_changes_closes_out_and_does_not_rerun(tmp_path: Path):
    ctx, dirs = _patched_dirs(tmp_path / "state")
    repo = tmp_path / "repo"
    _init_repo(repo)
    _dirty(repo)
    slice_id = "unit-hard-dirty"
    goal_id = "goal-hard-dirty"
    pr_log = tmp_path / "pr.log"
    pr_bin = tmp_path / "dsh-trial-pr"
    _pr_script(pr_bin, pr_log)
    with ctx:
        _write_pack(dirs, slice_id, repo)
        _seed_goal(dirs, goal_id, repo)
        rc, calls = _run(dirs, repo, slice_id, goal_id, HARD, pr_bin)
        goal, chain = _load(dirs, goal_id, slice_id)
    assert rc != 0
    assert goal["status"] == "closeout", goal
    assert goal["status"] != "failed"
    assert chain["state"] == "closeout"
    assert chain["state"] != "PASS"
    assert chain.get("last_verdict") in (None, "")
    assert len(chain["rounds"]) == 1
    assert calls == [f"impl-trial-{slice_id}-r1"]
    assert not any(c.startswith("gate-") for c in calls)
    text = _handoff_text(chain)
    assert "硬上限" in text
    assert "unit-hard-dirty" in text
    assert "impl-trial-unit-hard-dirty-r1" in text
    assert "src/new_work.py" in text
    assert "python3 -m pytest -q" in text
    assert "https://github.com/fengrunda/memory-as-training/pull/4242" in text
    head = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=repo, capture_output=True, text=True, check=True
    ).stdout.strip()
    assert head == "feat/slice"
    show = subprocess.run(
        ["git", "show", "--name-only", "--format=%s", "HEAD"],
        cwd=repo, capture_output=True, text=True, check=True
    ).stdout
    assert "src/new_work.py" in show
    assert "__pycache__" not in show
    assert ".pytest_cache" not in show
    assert pr_log.is_file()


def test_hard_cap_clean_tree_still_fails(tmp_path: Path):
    ctx, dirs = _patched_dirs(tmp_path / "state")
    repo = tmp_path / "repo"
    _init_repo(repo)
    slice_id = "unit-hard-clean"
    goal_id = "goal-hard-clean"
    pr_bin = tmp_path / "dsh-trial-pr"
    _pr_script(pr_bin, tmp_path / "pr.log")
    with ctx:
        _write_pack(dirs, slice_id, repo)
        _seed_goal(dirs, goal_id, repo)
        _rc, calls = _run(dirs, repo, slice_id, goal_id, HARD, pr_bin)
        goal, chain = _load(dirs, goal_id, slice_id)
    assert goal["status"] == "failed"
    assert chain["state"] == "failed"
    assert "without summary" in chain["error"]
    assert "hard_cap" in chain["error"]
    text = _handoff_text(chain)
    assert "硬上限" in text
    assert "没有可提交改动" in text
    assert calls == [f"impl-trial-{slice_id}-r1"]
    assert not (tmp_path / "pr.log").exists()


def test_plain_exit_does_not_close_out(tmp_path: Path):
    ctx, dirs = _patched_dirs(tmp_path / "state")
    repo = tmp_path / "repo"
    _init_repo(repo)
    _dirty(repo)
    slice_id = "unit-plain"
    goal_id = "goal-plain"
    pr_bin = tmp_path / "dsh-trial-pr"
    _pr_script(pr_bin, tmp_path / "pr.log")
    with ctx:
        _write_pack(dirs, slice_id, repo)
        _seed_goal(dirs, goal_id, repo)
        _rc, calls = _run(dirs, repo, slice_id, goal_id, PLAIN, pr_bin)
        goal, chain = _load(dirs, goal_id, slice_id)
        handoffs = list(dirs["summaries"].glob("*sup-handoff*"))
    assert goal["status"] == "failed"
    assert chain["state"] == "failed"
    assert chain["error"] == "foreman exit 1 without summary"
    assert not chain.get("handoff_summary")
    assert handoffs == []
    assert calls == [f"impl-trial-{slice_id}-r1"]
    dirty = subprocess.run(
        ["git", "status", "--porcelain"], cwd=repo, capture_output=True, text=True, check=True
    ).stdout
    assert "src/new_work.py" in dirty


def test_idle_timeout_does_not_close_out(tmp_path: Path):
    ctx, dirs = _patched_dirs(tmp_path / "state")
    repo = tmp_path / "repo"
    _init_repo(repo)
    _dirty(repo)
    slice_id = "unit-idle"
    goal_id = "goal-idle"
    pr_bin = tmp_path / "dsh-trial-pr"
    _pr_script(pr_bin, tmp_path / "pr.log")
    with ctx:
        _write_pack(dirs, slice_id, repo)
        _seed_goal(dirs, goal_id, repo)
        _rc, calls = _run(dirs, repo, slice_id, goal_id, IDLE, pr_bin)
        goal, chain = _load(dirs, goal_id, slice_id)
        handoffs = list(dirs["summaries"].glob("*sup-handoff*"))
    assert goal["status"] == "failed"
    assert chain["state"] == "failed"
    assert "without summary" in chain["error"]
    assert "hard_cap" not in chain["error"]
    assert not chain.get("handoff_summary")
    assert handoffs == []
    assert calls == [f"impl-trial-{slice_id}-r1"]
    dirty = subprocess.run(
        ["git", "status", "--porcelain"], cwd=repo, capture_output=True, text=True, check=True
    ).stdout
    assert "src/new_work.py" in dirty


def test_max_steps_with_changes_writes_handoff(tmp_path: Path):
    ctx, dirs = _patched_dirs(tmp_path / "state")
    repo = tmp_path / "repo"
    _init_repo(repo)
    _dirty(repo)
    slice_id = "unit-steps-dirty"
    goal_id = "goal-steps-dirty"
    pr_log = tmp_path / "pr.log"
    pr_bin = tmp_path / "dsh-trial-pr"
    _pr_script(pr_bin, pr_log)
    with ctx:
        _write_pack(dirs, slice_id, repo)
        _seed_goal(dirs, goal_id, repo)
        _rc, calls = _run(dirs, repo, slice_id, goal_id, STEPS, pr_bin)
        goal, chain = _load(dirs, goal_id, slice_id)
    assert goal["status"] == "closeout"
    assert chain["state"] == "closeout"
    assert chain["state"] != "PASS"
    assert len(chain["rounds"]) == 1
    assert calls == [f"impl-trial-{slice_id}-r1"]
    text = _handoff_text(chain)
    assert "步数上限" in text
    assert "max_steps" in text
    assert goal_id in text
    assert slice_id in text
    assert f"impl-trial-{slice_id}-r1" in text
    assert "src/new_work.py" in text
    assert "python3 -m pytest -q" in text and "exit=0" in text
    assert "https://github.com/fengrunda/memory-as-training/pull/4242" in text
    assert "不要重跑整片" in text
    assert pr_log.is_file()


def test_max_steps_clean_tree_fails_but_handoff_says_so(tmp_path: Path):
    ctx, dirs = _patched_dirs(tmp_path / "state")
    repo = tmp_path / "repo"
    _init_repo(repo)
    slice_id = "unit-steps-clean"
    goal_id = "goal-steps-clean"
    pr_bin = tmp_path / "dsh-trial-pr"
    _pr_script(pr_bin, tmp_path / "pr.log")
    with ctx:
        _write_pack(dirs, slice_id, repo)
        _seed_goal(dirs, goal_id, repo)
        _rc, calls = _run(dirs, repo, slice_id, goal_id, STEPS, pr_bin)
        goal, chain = _load(dirs, goal_id, slice_id)
    assert goal["status"] == "failed"
    assert chain["state"] == "failed"
    assert chain["state"] != "PASS"
    assert "without summary" in chain["error"]
    assert "max_steps" in chain["error"]
    text = _handoff_text(chain)
    assert "步数上限" in text
    assert "真撞" in text
    assert "没有可提交改动" in text
    assert "不要收成 PASS" in text
    assert calls == [f"impl-trial-{slice_id}-r1"]
    assert not (tmp_path / "pr.log").exists()


def test_limit_kind_ignores_prompt_and_idle():
    assert tb.limit_stop_kind(HARD) == "hard_cap"
    assert tb.limit_stop_kind(STEPS) == "max_steps"
    assert tb.limit_stop_kind(IDLE) is None
    assert tb.limit_stop_kind(PLAIN) is None
    assert tb.limit_stop_kind("timeout after 3600s hard cap") == "hard_cap"
