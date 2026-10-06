"""Fix 3 — gate "New diff" must be a merge-base three-dot diff.

``base...head`` shows only what the branch introduced; two-dot ``base..head``
diffs the two tree tips and reports main-side files once the branch merged
main. These tests lock the subprocess argv, not just the rendered text.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_git_diff", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)


def _result(stdout: str):
    return type("R", (), {"returncode": 0, "stdout": stdout, "stderr": ""})()


def _capture(calls: list, stdout: str = "diff --git a/x b/x\n"):
    def fake_run(cmd, **_kw):
        calls.append(list(cmd))
        return _result(stdout)
    return fake_run


def test_three_dot_when_base_set(tmp_path):
    calls: list[list[str]] = []
    with mock.patch.object(tb.subprocess, "run", side_effect=_capture(calls)):
        out = tb._git_diff(tmp_path, "base1", "head1")
    assert calls[0] == ["git", "diff", "base1...head1"]
    assert "base1..head1" not in " ".join(calls[0])
    assert out.startswith("diff --git")


def test_three_dot_default_head(tmp_path):
    calls: list[list[str]] = []
    with mock.patch.object(tb.subprocess, "run", side_effect=_capture(calls)):
        tb._git_diff(tmp_path, "base1")
    assert calls[0] == ["git", "diff", "base1...HEAD"]


def test_no_base_keeps_head_diff(tmp_path):
    calls: list[list[str]] = []
    with mock.patch.object(tb.subprocess, "run", side_effect=_capture(calls)):
        out = tb._git_diff(tmp_path, None)
    assert calls[0] == ["git", "diff", "HEAD"]
    assert out.startswith("diff --git")


def test_empty_base_keeps_head_diff(tmp_path):
    calls: list[list[str]] = []
    with mock.patch.object(tb.subprocess, "run", side_effect=_capture(calls)):
        tb._git_diff(tmp_path, "")
    assert calls[0] == ["git", "diff", "HEAD"]


def test_empty_diff_falls_back_to_cached(tmp_path):
    calls: list[list[str]] = []
    responses = iter([_result(""), _result("cached diff\n")])

    def fake_run(cmd, **_kw):
        calls.append(list(cmd))
        return next(responses)

    with mock.patch.object(tb.subprocess, "run", side_effect=fake_run):
        out = tb._git_diff(tmp_path, "base1", "head1")
    assert calls[0] == ["git", "diff", "base1...head1"]
    assert calls[1] == ["git", "diff", "--cached"]
    assert out == "cached diff\n"
