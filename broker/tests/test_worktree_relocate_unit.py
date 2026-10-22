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


def _git_ready(argv, *, list_out: str | None = None, add_code: int = 0, origin: int = 0, main: int = 1):
    """Fake ``git -C`` for relocate. ``worktree add`` creates the dest directory."""
    if "--is-inside-work-tree" in argv:
        return _proc(0, "true\n")
    if "worktree" in argv and "list" in argv:
        text = list_out if list_out is not None else "worktree /src\nbranch refs/heads/main\n"
        return _proc(0, text)
    if "rev-parse" in argv and "origin/main" in argv:
        return _proc(origin, "abc\n" if origin == 0 else "", "" if origin == 0 else "missing")
    if "rev-parse" in argv and argv[-1] == "main":
        return _proc(main, "def\n" if main == 0 else "", "" if main == 0 else "missing")
    if "worktree" in argv and "add" in argv:
        if add_code == 0:
            Path(argv[-2]).mkdir(parents=True, exist_ok=True)
        return _proc(add_code, err="" if add_code == 0 else "branch checked out")
    return _proc(1, err="unexpected")


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
        return _git_ready(argv)

    with mock.patch.object(tb.subprocess, "run", side_effect=fake_run):
        moved = tb.relocate_goal_to_worktree(job, path)
    assert moved is not None
    assert moved["cwd"] == str(prefix / "engine-next")
    assert "worktree_relocated_from" not in moved
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved["cwd"] == str(prefix / "engine-next")
    assert saved["goal"] == "engine-next"
    assert "worktree_relocated_from" not in saved


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
        return _git_ready(argv, add_code=1)

    with mock.patch.object(tb.subprocess, "run", side_effect=fake_run):
        assert tb.relocate_goal_to_worktree(job, path) is None
        first = calls["n"]
        assert tb.relocate_goal_to_worktree(job, path) is None
    assert calls["n"] == first
    assert path.is_file()
    assert json.loads(path.read_text(encoding="utf-8")) == {}


def test_existing_path_and_checked_out_branch_get_suffix(tmp_path: Path, monkeypatch):
    tb._WORKTREE_RELOCATE_FAILED.clear()
    repo = tmp_path / "repo"
    repo.mkdir()
    prefix = tmp_path / "wt"
    taken = prefix / "engine-next"
    taken.mkdir(parents=True)
    (taken / "keep").write_text("stay", encoding="utf-8")
    monkeypatch.setenv("DSH_TRIAL_WORKTREE_PREFIXES", str(prefix))
    path = tmp_path / "inbox.json"
    job = {"type": "goal", "goal": "engine-next", "cwd": str(repo), "brief": "keep-me"}
    path.write_text(json.dumps(job), encoding="utf-8")
    adds: list[list[str]] = []

    def fake_run(argv, **kwargs):
        if "worktree" in argv and "add" in argv:
            adds.append(list(argv))
        return _git_ready(
            argv,
            list_out="worktree /other\nbranch refs/heads/feat/engine-next\n",
        )

    with mock.patch.object(tb.subprocess, "run", side_effect=fake_run):
        moved = tb.relocate_goal_to_worktree(job, path)
    assert moved is not None
    assert moved["cwd"] == str(prefix / "engine-next-2")
    assert adds and adds[0][adds[0].index("-b") + 1] == "feat/engine-next-2"
    assert (taken / "keep").read_text(encoding="utf-8") == "stay"
    saved = json.loads(path.read_text(encoding="utf-8"))
    assert saved["brief"] == "keep-me"
    assert saved["cwd"] == str(prefix / "engine-next-2")


def test_start_ref_falls_back_to_main(tmp_path: Path, monkeypatch):
    tb._WORKTREE_RELOCATE_FAILED.clear()
    repo = tmp_path / "repo"
    repo.mkdir()
    prefix = tmp_path / "wt"
    monkeypatch.setenv("DSH_TRIAL_WORKTREE_PREFIXES", str(prefix))
    path = tmp_path / "inbox.json"
    job = {"type": "goal", "goal": "plain", "cwd": str(repo)}
    path.write_text(json.dumps(job), encoding="utf-8")
    adds: list[list[str]] = []

    def fake_run(argv, **kwargs):
        if "worktree" in argv and "add" in argv:
            adds.append(list(argv))
        return _git_ready(argv, origin=1, main=0)

    with mock.patch.object(tb.subprocess, "run", side_effect=fake_run):
        moved = tb.relocate_goal_to_worktree(job, path)
    assert moved is not None
    assert adds and adds[-1][-1] == "main"


def test_explicit_ticket_is_not_relocated(tmp_path: Path, monkeypatch):
    tb._WORKTREE_RELOCATE_FAILED.clear()
    monkeypatch.setenv("DSH_TRIAL_WORKTREE_PREFIXES", str(tmp_path / "wt"))
    path = tmp_path / "ticket.json"
    job = {"type": "ticket", "goal": "g", "cwd": str(tmp_path / "repo")}
    path.write_text(json.dumps(job), encoding="utf-8")

    def fake_run(argv, **kwargs):
        raise AssertionError(argv)

    with mock.patch.object(tb.subprocess, "run", side_effect=fake_run):
        assert tb.relocate_goal_to_worktree(job, path) is None
    assert json.loads(path.read_text(encoding="utf-8"))["cwd"] == str(tmp_path / "repo")
