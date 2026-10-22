#!/usr/bin/env python3
"""cwd conflict opens a worktree instead of leaving the goal queued."""
from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("trial_broker_wt", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)


def _proc(code: int = 0, out: str = "", err: str = "") -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=[], returncode=code, stdout=out, stderr=err)


def test_chain_reply_is_not_relocated(tmp_path: Path):
    path = tmp_path / "reply.json"
    path.write_text("{}", encoding="utf-8")
    job = {"type": "chain-reply", "cwd": str(tmp_path), "goal": "g"}
    assert tb.relocate_goal_to_worktree(job, path) is None


def test_cwd_already_under_prefix_stays(tmp_path: Path, monkeypatch):
    prefix = tmp_path / "wt"
    prefix.mkdir()
    monkeypatch.setenv("DSH_TRIAL_WORKTREE_PREFIXES", str(prefix))
    path = tmp_path / "goal.json"
    path.write_text("{}", encoding="utf-8")
    job = {"type": "goal", "goal": "already", "cwd": str(prefix / "already")}
    assert tb.relocate_goal_to_worktree(job, path) is None


def test_git_success_rewrites_cwd(tmp_path: Path, monkeypatch):
    tb._WORKTREE_RELOCATE_FAILED.clear()
    repo = tmp_path / "repo"
    repo.mkdir()
    prefix = tmp_path / "wt"
    monkeypatch.setenv("DSH_TRIAL_WORKTREE_PREFIXES", str(prefix))
    path = tmp_path / "inbox.json"
    job = {"type": "goal", "goal": "engine-next", "cwd": str(repo)}
    path.write_text(json.dumps(job), encoding="utf-8")

    def fake_run(argv, **kwargs):
        if "rev-parse" in argv and "--is-inside-work-tree" in argv:
            return _proc(0, "true\n")
        if "rev-parse" in argv and "origin/main" in argv:
            return _proc(0, "abc\n")
        if "worktree" in argv and "add" in argv:
            dest = Path(argv[-2])
            dest.mkdir(parents=True)
            return _proc(0)
        return _proc(1, err="unexpected")

    with mock.patch.object(tb.subprocess, "run", side_effect=fake_run):
        moved = tb.relocate_goal_to_worktree(job, path)
    assert moved is not None
    assert moved["cwd"] == str(prefix / "engine-next")
    assert moved["worktree_relocated_from"] == str(repo)
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved["cwd"] == str(prefix / "engine-next")


def test_git_failure_is_not_retried(tmp_path: Path, monkeypatch):
    tb._WORKTREE_RELOCATE_FAILED.clear()
    repo = tmp_path / "repo"
    repo.mkdir()
    monkeypatch.setenv("DSH_TRIAL_WORKTREE_PREFIXES", str(tmp_path / "wt"))
    path = tmp_path / "inbox.json"
    job = {"type": "goal", "goal": "engine-next", "cwd": str(repo)}
    path.write_text("{}", encoding="utf-8")
    calls = {"n": 0}

    def fake_run(argv, **kwargs):
        calls["n"] += 1
        if "worktree" in argv and "add" in argv:
            return _proc(1, err="branch checked out")
        if "--is-inside-work-tree" in argv:
            return _proc(0, "true\n")
        if "origin/main" in argv:
            return _proc(0)

    with mock.patch.object(tb.subprocess, "run", side_effect=fake_run):
        assert tb.relocate_goal_to_worktree(job, path) is None
        first = calls["n"]
        assert tb.relocate_goal_to_worktree(job, path) is None
    assert calls["n"] == first
