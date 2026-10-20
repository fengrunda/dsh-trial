#!/usr/bin/env python3
"""Unit tests for broker/gate_effort.py (hot-path gate effort tiering)."""
from __future__ import annotations

from pathlib import Path
import subprocess

import pytest

import gate_effort


def test_risk_high_maps_high():
    assert gate_effort.decide("high", ["src/a.py"], 1)[0] == "high"


def test_risk_low_maps_low():
    assert gate_effort.decide("low", ["src/a.py"], 1)[0] == "low"


def test_risk_medium_maps_low_with_note():
    effort, reason = gate_effort.decide("medium", ["src/a.py"], 1)
    assert effort == "low"
    assert "medium" in reason


@pytest.mark.parametrize(
    "path",
    [
        "db/migrations/0001_init.sql",
        "src/Migrate_users.py",
        "src/auth/session.py",
        "lib/AuthToken.go",
        "src/tenant/settings.py",
        "pkg/api/v1/handler.go",
        "docs/openapi.yaml",
        "hooks/webhook_receiver.py",
        "infra/deploy/prod.yml",
        "src/prod/settings.py",
    ],
)
def test_high_risk_paths_without_risk(path):
    effort, _ = gate_effort.decide(None, ["src/safe.py", path], 3)
    assert effort == "high", path


def test_small_safe_diff_is_low():
    assert gate_effort.decide(None, ["src/util.py"], 12)[0] == "low"


def test_large_diff_no_longer_raises_tier():
    assert gate_effort.decide(None, ["src/util.py"], 747)[0] == "low"


def test_risk_label_tenant_is_high():
    effort, reason = gate_effort.decide("tenant,takeover", ["src/util.py"], 3)
    assert effort == "high"
    assert "risk=" in reason


def test_explicit_low_beats_high_risk_path():
    effort, reason = gate_effort.decide("low", ["src/auth/session.py"], 3)
    assert effort == "low"
    assert "risk=low" in reason


def _write_pack(d: Path, name: str, body: str) -> Path:
    p = d / name
    p.write_text(body, encoding="utf-8")
    return p


def _orig_pack(d: Path, risk: str | None = None) -> None:
    front = "---\nslice_id: s1\n"
    if risk:
        front += f"risk: {risk}\n"
    front += "---\n\n# Slice pack\n"
    _write_pack(d, "s1.pack.md", front)


GATE_HEAD = """---
slice_id: s1
kind: gate-pack
round: 2
role: gate
original_pack: s1.pack.md
"""


def _git_repo(tmp_path: Path):
    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(repo), *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout

    git("init", "-q")
    git("config", "user.email", "test@example.com")
    git("config", "user.name", "test")
    (repo / "tracked.txt").write_text("a\n", encoding="utf-8")
    git("add", ".")
    git("commit", "-q", "-m", "base")
    return repo, git, git("rev-parse", "HEAD").strip()


def _closing_pack(d: Path, baseline: str) -> None:
    _write_pack(
        d,
        "s1.pack.md",
        "---\nslice_id: s1\n"
        f"base_for_review: {baseline}\n"
        "---\n\n不要调 `submit_for_review`；直接开 PR。\n",
    )


def test_from_gate_pack_falls_back_to_diff_block(tmp_path):
    _orig_pack(tmp_path)
    gate = _write_pack(
        tmp_path,
        "s1.rev2.pack.md",
        GATE_HEAD
        + "---\n\n## Diff (base...head merge-base; base=deadbeef head=cafebabe; truncated=false)\n"
        + "```diff\n"
        + "diff --git a/src/migrations/0002_x.sql b/src/migrations/0002_x.sql\n"
        + "--- a/src/migrations/0002_x.sql\n"
        + "+++ b/src/migrations/0002_x.sql\n"
        + "+create table t (id int);\n"
        + "```\n",
    )
    effort, reason = gate_effort.from_gate_pack(gate, tmp_path)
    assert effort == "high"
    assert "migrations" in reason


def test_from_gate_pack_uses_original_risk(tmp_path):
    _orig_pack(tmp_path, risk="high")
    gate = _write_pack(
        tmp_path,
        "s1.rev2.pack.md",
        GATE_HEAD
        + "---\n\n## Diff (base...head merge-base; base=deadbeef head=cafebabe; truncated=false)\n"
        + "```diff\n"
        + "diff --git a/src/x.py b/src/x.py\n"
        + "+x = 1\n"
        + "```\n",
    )
    effort, reason = gate_effort.from_gate_pack(gate, tmp_path)
    assert effort == "high"
    assert "risk" in reason


def test_from_gate_pack_small_safe_diff_is_low(tmp_path):
    _orig_pack(tmp_path)
    gate = _write_pack(
        tmp_path,
        "s1.rev2.pack.md",
        GATE_HEAD
        + "---\n\n## Diff (base...head merge-base; base=deadbeef head=cafebabe; truncated=false)\n"
        + "```diff\n"
        + "diff --git a/src/x.py b/src/x.py\n"
        + "+x = 1\n"
        + "-y = 2\n"
        + "```\n",
    )
    effort, reason = gate_effort.from_gate_pack(gate, tmp_path)
    assert effort == "low"


def test_gate_effort_frontmatter_wins(tmp_path):
    _orig_pack(tmp_path, risk="high")
    gate = _write_pack(
        tmp_path,
        "s1.rev2.pack.md",
        GATE_HEAD.replace("role: gate\n", "role: gate\ngate_effort: low\n") + "---\n",
    )
    effort, reason = gate_effort.from_gate_pack(gate, tmp_path)
    assert effort == "low"
    assert reason == "pack gate_effort"


def test_gate_effort_frontmatter_whitelist_normalized(tmp_path):
    for raw, expected in [
        ("low", "low"),
        ("off", "off"),
        ("medium", "low"),
        ("high", "high"),
        ("max", "high"),
    ]:
        gate = _write_pack(
            tmp_path,
            f"s1.rev2.{raw}.pack.md",
            GATE_HEAD.replace("role: gate\n", f"role: gate\ngate_effort: {raw}\n")
            + "---\n",
        )
        effort, reason = gate_effort.from_gate_pack(gate, tmp_path)
        assert effort == expected, raw
        assert reason == "pack gate_effort"


def test_gate_effort_frontmatter_invalid_ignored(tmp_path):
    _orig_pack(tmp_path, risk="high")
    gate = _write_pack(
        tmp_path,
        "s1.rev2.invalid.pack.md",
        GATE_HEAD.replace("role: gate\n", "role: gate\ngate_effort: urgent\n") + "---\n",
    )
    effort, reason = gate_effort.from_gate_pack(gate, tmp_path)
    assert effort == "high"
    assert reason == "risk=high"


def test_header_without_sha_falls_back_to_diff_block(tmp_path):
    _orig_pack(tmp_path)
    gate = _write_pack(
        tmp_path,
        "s1.rev2.pack.md",
        GATE_HEAD
        + "---\n\n## New diff (base...head merge-base; truncated=true)\n"
        + "```diff\n"
        + "diff --git a/src/migrations/0002_x.sql b/src/migrations/0002_x.sql\n"
        + "--- a/src/migrations/0002_x.sql\n"
        + "+++ b/src/migrations/0002_x.sql\n"
        + "+create table t (id int);\n"
        + "```\n",
    )
    effort, reason = gate_effort.from_gate_pack(gate, tmp_path)
    assert effort == "high"
    assert "migrations" in reason


def test_empty_numstat_falls_back_to_diff_block(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(repo), *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout

    git("init", "-q")
    git("config", "user.email", "test@example.com")
    git("config", "user.name", "test")
    (repo / "tracked.txt").write_text("a\n", encoding="utf-8")
    git("add", ".")
    git("commit", "-q", "-m", "base")
    # 未提交改动 + base==head：numstat 成功但输出为空。
    (repo / "tracked.txt").write_text("a\nb\n", encoding="utf-8")
    sha = git("rev-parse", "HEAD").strip()
    _orig_pack(tmp_path)

    gate = _write_pack(
        tmp_path,
        "s1.rev2.pack.md",
        GATE_HEAD
        + f"---\n\n## Diff (base...head merge-base; base={sha} head={sha}; truncated=false)\n"
        + "```diff\n"
        + "diff --git a/src/migrations/0002_x.sql b/src/migrations/0002_x.sql\n"
        + "index 1111111..2222222 100644\n"
        + "--- a/src/migrations/0002_x.sql\n"
        + "+++ b/src/migrations/0002_x.sql\n"
        + "+create table t (id int);\n"
        + "```\n",
    )
    effort, reason = gate_effort.from_gate_pack(gate, repo)
    assert effort == "high"
    assert "migrations" in reason


def test_diff_block_counts_hunk_lines_starting_with_double_dash_or_plus():
    text = (
        "```diff\n"
        "diff --git a/src/x.py b/src/x.py\n"
        "index 1111111..2222222 100644\n"
        "--- a/src/x.py\n"
        "+++ b/src/x.py\n"
        "@@ -1,3 +1,3 @@\n"
        "--- old\n"
        "+++ new\n"
        "-plain\n"
        "+plain\n"
        "```\n"
    )
    paths, changed = gate_effort._diff_block_stats(text)
    assert paths == ["src/x.py"]
    assert changed == 4


def test_classify_skip_closing_pr_only(tmp_path):
    repo, git, sha = _git_repo(tmp_path)
    (repo / ".github" / "workflows").mkdir(parents=True)
    (repo / ".github" / "workflows" / "ci.yml").write_text(
        "name: ci\n", encoding="utf-8"
    )
    git("add", ".")
    git("commit", "-q", "-m", "pr meta only")
    _closing_pack(tmp_path, sha)
    gate = _write_pack(tmp_path, "s1.rev2.pack.md", GATE_HEAD + "---\n")
    c = gate_effort.classify(gate, repo)
    assert c["action"] == "skip"
    assert c["effort"] is None
    assert "closing_slice_pr_only" in c["reason"]
    assert "gate_skip: closing_ok" in c["reason"]


def test_classify_skip_closing_docs_only(tmp_path):
    repo, git, sha = _git_repo(tmp_path)
    (repo / "docs").mkdir()
    (repo / "docs" / "x.md").write_text("doc\n", encoding="utf-8")
    git("add", ".")
    git("commit", "-q", "-m", "docs only")
    _closing_pack(tmp_path, sha)
    gate = _write_pack(tmp_path, "s1.rev2.pack.md", GATE_HEAD + "---\n")
    c = gate_effort.classify(gate, repo)
    assert c["action"] == "skip"
    assert "gate_skip: closing_ok" in c["reason"]


def test_classify_blocks_fake_closing_with_src_change(tmp_path):
    repo, git, sha = _git_repo(tmp_path)
    (repo / "src").mkdir()
    (repo / "src" / "foo.py").write_text("x = 1\n", encoding="utf-8")
    git("add", ".")
    git("commit", "-q", "-m", "fake src")
    _closing_pack(tmp_path, sha)
    gate = _write_pack(tmp_path, "s1.rev2.pack.md", GATE_HEAD + "---\n")
    c = gate_effort.classify(gate, repo)
    assert c["action"] == "review"
    assert "src_change" in c["reason"]
    assert c["effort"] in {"low", "high"}


def test_base_for_review_range_uses_start_not_head(tmp_path):
    """`<old>..HEAD` 必须取左侧起点，否则 diff HEAD...HEAD 恒空 -> 假收尾。"""
    repo, git, sha = _git_repo(tmp_path)
    (repo / "src").mkdir()
    (repo / "src" / "foo.py").write_text("x = 1\n", encoding="utf-8")
    git("add", ".")
    git("commit", "-q", "-m", "fake src")
    _write_pack(
        tmp_path,
        "s1.pack.md",
        "---\nslice_id: s1\n"
        f"base_for_review: {sha}..HEAD\n"
        "---\n\n不要调 `submit_for_review`；直接开 PR。\n",
    )
    gate = _write_pack(tmp_path, "s1.rev2.pack.md", GATE_HEAD + "---\n")
    c = gate_effort.classify(gate, repo)
    assert c["action"] == "review", c
    assert "src_change" in c["reason"]
    assert c["effort"] in {"low", "high"}


def test_last_commit_beats_bad_range_right_side(tmp_path):
    """正文 last_commit 优先于 `..HEAD` 右侧，不能被右侧 HEAD 带偏。"""
    repo, git, sha = _git_repo(tmp_path)
    (repo / "src").mkdir()
    (repo / "src" / "foo.py").write_text("x = 1\n", encoding="utf-8")
    git("add", ".")
    git("commit", "-q", "-m", "fake src")
    _write_pack(
        tmp_path,
        "s1.pack.md",
        "---\nslice_id: s1\n"
        "base_for_review: deadbeef..HEAD\n"
        "---\n\n"
        f"last_commit={sha}\n\n"
        "不要调 `submit_for_review`；直接开 PR。\n",
    )
    gate = _write_pack(tmp_path, "s1.rev2.pack.md", GATE_HEAD + "---\n")
    c = gate_effort.classify(gate, repo)
    assert c["action"] == "review", c
    assert "src_change" in c["reason"]


def test_baseline_equal_head_is_rejected(tmp_path):
    """所有候选都解析为当前 HEAD -> no_baseline，禁止 skip。"""
    repo, git, _ = _git_repo(tmp_path)
    (repo / "src").mkdir()
    (repo / "src" / "foo.py").write_text("x = 1\n", encoding="utf-8")
    git("add", ".")
    git("commit", "-q", "-m", "fake src")
    _write_pack(
        tmp_path,
        "s1.pack.md",
        "---\nslice_id: s1\n"
        "base_for_review: HEAD..HEAD\n"
        "---\n\n不要调 `submit_for_review`；直接开 PR。\n",
    )
    gate = _write_pack(tmp_path, "s1.rev2.pack.md", GATE_HEAD + "---\n")
    c = gate_effort.classify(gate, repo)
    assert c["action"] == "review", c
    assert "no_baseline" in c["reason"]


def test_classify_blocks_fake_closing_with_defaults_change(tmp_path):
    repo, git, sha = _git_repo(tmp_path)
    (repo / "config").mkdir()
    (repo / "config" / "defaults.yaml").write_text("a: 1\n", encoding="utf-8")
    git("add", ".")
    git("commit", "-q", "-m", "fake defaults")
    _closing_pack(tmp_path, sha)
    gate = _write_pack(tmp_path, "s1.rev2.pack.md", GATE_HEAD + "---\n")
    c = gate_effort.classify(gate, repo)
    assert c["action"] == "review"
    assert "src_change" in c["reason"]


def test_classify_skip_gate_frontmatter(tmp_path):
    repo, git, sha = _git_repo(tmp_path)
    (repo / "docs").mkdir()
    (repo / "docs" / "x.md").write_text("doc\n", encoding="utf-8")
    git("add", ".")
    git("commit", "-q", "-m", "docs only")
    _orig_pack(tmp_path)
    gate = _write_pack(
        tmp_path,
        "s1.rev2.pack.md",
        GATE_HEAD.replace(
            "role: gate\n",
            f"role: gate\nskip_gate: true\nbase_for_review: {sha}\n",
        )
        + "---\n",
    )
    c = gate_effort.classify(gate, repo)
    assert c["action"] == "skip"
    assert "frontmatter" in c["reason"]


def test_classify_review_keeps_effort(tmp_path):
    _orig_pack(tmp_path)
    gate = _write_pack(tmp_path, "s1.rev2.pack.md", GATE_HEAD + "---\n")
    c = gate_effort.classify(gate, tmp_path)
    assert c["action"] == "review"
    assert c["effort"] == "low"


def test_cli_classify_prints_three_fields(tmp_path, capsys):
    _orig_pack(tmp_path)
    gate = _write_pack(tmp_path, "s1.rev2.pack.md", GATE_HEAD + "---\n")
    rc = gate_effort.main(["gate_effort.py", "--classify", str(gate), str(tmp_path)])
    out = capsys.readouterr().out.rstrip("\n")
    assert rc == 0
    assert out.split("\t")[:2] == ["review", "low"]


def test_cli_bad_input_prints_empty_and_exits_zero(capsys):
    rc = gate_effort.main(["gate_effort.py", "/nonexistent/pack.md", "/tmp"])
    out = capsys.readouterr().out
    assert rc == 0
    assert out == "\n"
