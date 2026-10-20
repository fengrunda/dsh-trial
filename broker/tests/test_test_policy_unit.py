#!/usr/bin/env python3
"""Unit tests for broker/pytest_scope.py (three-tier test policy injection)."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

_BROKER_ROOT = Path(__file__).resolve().parent.parent
if str(_BROKER_ROOT) not in sys.path:
    sys.path.insert(0, str(_BROKER_ROOT))

import pytest_scope  # noqa: E402


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _git_repo(root: Path, files: list[str]) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "-C", str(root), "init", "-q"], check=True)
    for name in files:
        _write(root / name, "")
    subprocess.run(["git", "-C", str(root), "add", "-A"], check=True)
    return root


def _pack(tmp_path: Path, frontmatter: str, body: str = "") -> Path:
    return _write(tmp_path / "s.pack.md",
                  f"---\n{frontmatter.rstrip(chr(10))}\n---\n\n{body}\n")


def test_parse_tests_three_styles(tmp_path):
    for idx, value in enumerate(("tests: a/test_a.py, b/test_b.py",
                                 "tests: [a/test_a.py, b/test_b.py]",
                                 "tests: a/test_a.py b/test_b.py")):
        pack = _pack(tmp_path / f"case{idx}", value)
        out = pytest_scope.build_section("foreman", str(pack), str(tmp_path))
        assert "python3 -m pytest -q a/test_a.py" in out
        assert "python3 -m pytest -q b/test_b.py" in out
        assert pytest_scope.HARD_RULE in out


def test_collect_backtick_commands(tmp_path):
    body = (
        "跑 `pytest tests/test_x.py -q`，或 "
        "`cd broker && python3 -m pytest tests/test_y.py -q`。\n"
        "别跑 `pytest -q` 之外的东西。"
    )
    pack = _pack(tmp_path, "tests:\n", body)
    out = pytest_scope.build_section("foreman", str(pack), str(tmp_path))
    assert "`pytest tests/test_x.py -q`" in out
    assert "`cd broker && python3 -m pytest tests/test_y.py -q`" in out


def test_original_pack_fallback(tmp_path):
    _write(tmp_path / "orig.pack.md",
           "---\ntests: src/test_orig.py\n---\n\nbody\n")
    pack = _pack(tmp_path, "original_pack: orig.pack.md\n")
    out = pytest_scope.build_section("gate", str(pack), str(tmp_path))
    assert "python3 -m pytest -q src/test_orig.py" in out
    assert pytest_scope.GATE_NOTE in out


def test_full_suite_batched_by_dir(tmp_path):
    repo = _git_repo(tmp_path / "repo", [
        "pkg_a/test_one.py", "pkg_a/test_two.py",
        "pkg_b/nested/test_three.py", "pkg_c/other_test.py",
    ])
    pack = _pack(tmp_path, "full_suite: true\n")
    out = pytest_scope.build_section("foreman", str(pack), str(repo))
    assert "本 pack 显式要求全量" in out
    assert "- `python3 -m pytest -q pkg_a`" in out
    assert "- `python3 -m pytest -q pkg_b/nested`" in out
    assert "- `python3 -m pytest -q pkg_c`" in out
    assert pytest_scope.HARD_RULE in out


def test_full_suite_dedups_nested_dirs(tmp_path):
    repo = _git_repo(tmp_path / "repo", [
        "test_root.py",
        "pkg_a/test_one.py",
        "pkg_a/nested/test_two.py",
        "pkg_a/nested/test_three.py",
    ])
    pack = _pack(tmp_path, "full_suite: true\n")
    out = pytest_scope.build_section("foreman", str(pack), str(repo))
    # 根目录和 pkg_a 都有后代目录在分组里：只列自身直接文件。
    assert "- `python3 -m pytest -q test_root.py`" in out
    assert "- `python3 -m pytest -q pkg_a/test_one.py`" in out
    assert "- `python3 -m pytest -q pkg_a/nested`" in out
    assert "- `python3 -m pytest -q .`" not in out
    assert "- `python3 -m pytest -q pkg_a`" not in out


def test_block_list_frontmatter(tmp_path):
    fm = "tests:\n  - a/test_a.py\n  - b/test_b.py\ntest_paths:\n  - c/test_c.py\n"
    pack = _pack(tmp_path, fm)
    out = pytest_scope.build_section("foreman", str(pack), str(tmp_path))
    assert "python3 -m pytest -q a/test_a.py" in out
    assert "python3 -m pytest -q b/test_b.py" in out
    assert "python3 -m pytest -q c/test_c.py" in out


def test_gate_fallback_untracked_test_file(tmp_path):
    repo = _git_repo(tmp_path / "repo", ["src/foo.py"])
    diff = (
        "```diff\n"
        "diff --git a/tests/test_new.py b/tests/test_new.py\n"
        "--- a/tests/test_new.py\n+++ b/tests/test_new.py\n"
        "```\n"
    )
    pack = _pack(tmp_path, "", diff)
    out = pytest_scope.build_section("gate", str(pack), str(repo))
    assert "python3 -m pytest -q tests/test_new.py" in out


def test_bare_pytest_not_collected(tmp_path):
    body = (
        "全量是 `pytest -q` 或 `python3 -m pytest -q`，"
        "带 cd 的 `cd broker && pytest -q` 同理。\n"
    )
    pack = _pack(tmp_path, "tests:\n", body)
    out = pytest_scope.build_section("foreman", str(pack), str(tmp_path))
    assert "- `pytest -q`" not in out
    assert "- `python3 -m pytest -q`" not in out
    assert "cd broker && pytest -q" not in out


def test_flag_values_are_not_positional_targets(tmp_path):
    body = (
        "`pytest -q -k smoke` / `python3 -m pytest -q -n auto` / "
        "`pytest -q -p no:cacheprovider` 都不收；"
        "`pytest -q -k smoke tests/test_a.py` 要收。\n"
    )
    pack = _pack(tmp_path, "tests:\n", body)
    out = pytest_scope.build_section("foreman", str(pack), str(tmp_path))
    assert "- `pytest -q -k smoke`" not in out
    assert "- `python3 -m pytest -q -n auto`" not in out
    assert "- `pytest -q -p no:cacheprovider`" not in out
    assert "- `pytest -q -k smoke tests/test_a.py`" in out


def test_deleted_test_file_not_selected(tmp_path):
    repo = _git_repo(tmp_path / "repo", ["src/foo.py", "tests/test_foo.py"])
    (repo / "tests/test_foo.py").unlink()
    diff = (
        "```diff\n"
        "diff --git a/src/foo.py b/src/foo.py\n"
        "--- a/src/foo.py\n+++ b/src/foo.py\n"
        "diff --git a/tests/test_foo.py b/tests/test_foo.py\n"
        "--- a/tests/test_foo.py\n+++ /dev/null\n"
        "```\n"
    )
    pack = _pack(tmp_path, "", diff)
    out = pytest_scope.build_section("gate", str(pack), str(repo))
    assert "test_foo.py" not in out
    assert "无对应测试" in out


def test_goal_block_items_not_merged(tmp_path):
    pack = _pack(tmp_path, "goal: |\n  - x\ntests:\n  - a/test_a.py\n")
    fm, _ = pytest_scope._split_frontmatter(pack.read_text(encoding="utf-8"))
    assert "x" not in fm["goal"]
    assert "a/test_a.py" in fm["tests"]
    out = pytest_scope.build_section("foreman", str(pack), str(tmp_path))
    assert "python3 -m pytest -q a/test_a.py" in out


def test_gate_fallback_maps_diff_to_tests(tmp_path):
    repo = _git_repo(tmp_path / "repo", [
        "src/foo.py", "src/test_bar.py", "tests/test_foo_unit.py",
    ])
    diff = (
        "```diff\n"
        "diff --git a/src/foo.py b/src/foo.py\n"
        "--- a/src/foo.py\n+++ b/src/foo.py\n"
        "diff --git a/src/test_bar.py b/src/test_bar.py\n"
        "--- a/src/test_bar.py\n+++ b/src/test_bar.py\n"
        "```\n"
    )
    pack = _pack(tmp_path, "original_pack: missing.pack.md\n", diff)
    out = pytest_scope.build_section("gate", str(pack), str(repo))
    assert "python3 -m pytest -q tests/test_foo_unit.py src/test_bar.py" in out


def test_gate_fallback_without_tests(tmp_path):
    repo = _git_repo(tmp_path / "repo", ["src/foo.py", "README.md"])
    diff = "```diff\ndiff --git a/src/foo.py b/src/foo.py\n--- a/src/foo.py\n+++ b/src/foo.py\n```\n"
    pack = _pack(tmp_path, "", diff)
    out = pytest_scope.build_section("gate", str(pack), str(repo))
    assert "无对应测试，不跑测试，只读审查" in out
    assert "pytest" not in out


def test_foreman_without_tests_rule(tmp_path):
    pack = _pack(tmp_path, "risk: low\n")
    out = pytest_scope.build_section("foreman", str(pack), str(tmp_path))
    assert "no_targeted_tests" in out
    assert "test_foo*.py" in out


def test_cli_error_prints_empty_exit_zero(tmp_path):
    proc = subprocess.run(
        [sys.executable, str(_BROKER_ROOT / "pytest_scope.py"),
         "--mode", "bogus", str(tmp_path / "nope.md"), str(tmp_path)],
        capture_output=True, text=True,
    )
    assert proc.returncode == 0
    assert proc.stdout.strip() == ""


if __name__ == "__main__":
    sys.exit(subprocess.call([sys.executable, "-m", "pytest", __file__, "-q"]))
