#!/usr/bin/env python3
"""Unit tests for broker/gate_effort.py (hot-path gate effort tiering)."""
from __future__ import annotations

from pathlib import Path

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


def test_large_diff_is_high():
    assert gate_effort.decide(None, ["src/util.py"], 401)[0] == "high"


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


def test_cli_bad_input_prints_empty_and_exits_zero(capsys):
    rc = gate_effort.main(["gate_effort.py", "/nonexistent/pack.md", "/tmp"])
    out = capsys.readouterr().out
    assert rc == 0
    assert out == "\n"
