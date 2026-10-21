#!/usr/bin/env python3
"""Gate review caliber: one frozen 5-line block, one soft step cap.

Regression lock for dsh-gate-ab-20261021-s2:

  a. ``GATE_PRINCIPLES_BLOCK`` is the single authority; ``open-slice.sh``
     (``PROMPT_MODE=gate``) must render it byte-for-byte;
  b. ``build_gate_pack`` and ``build_delta_gate_pack`` both inject it right
     before ``## Done when`` — so delta/rev packs cannot lose the caliber;
  c. the block carries the four keywords the gate relies on
     (``P0–P1`` / ``P2`` / ``只读复核`` / ``paused_limit``) plus the soft cap 28;
  d. ``role_step_budget("gate", ...)`` resolves gate_max_steps -> role table ->
     28 without raising on bad values, and ``_spawn_env("gate")`` exports
     ``DSH_GATE_MAX_STEPS`` for the plugin hint (temp limits only).
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location("trial_broker_gate_caliber", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tb)

T = tb.T
REPO = ROOT.parent
KEYWORDS = ("P0–P1", "P2", "只读复核", "paused_limit")


def _open_slice_block() -> str:
    """The gate principles block exactly as open-slice.sh renders it."""
    src = (REPO / "open-slice.sh").read_text(encoding="utf-8")
    start = src.index("## 审核原则")
    tail = "交监理五选一。"
    end = src.index(tail, start) + len(tail)
    return src[start:end]


def _gate_pack_body(tmp_path: Path) -> str:
    (tmp_path / "s1.pack.md").write_text("# pack\n", encoding="utf-8")
    with mock.patch.object(tb, "PACKS", tmp_path), \
            mock.patch.object(tb, "_git_rev", return_value="deadbeef"), \
            mock.patch.object(tb, "_git_diff", return_value="+x\n"):
        name = tb.build_gate_pack(
            slice_id="s1",
            round_n=1,
            original_pack_name="s1.pack.md",
            acceptance=["do the thing"],
            foreman_block={"base": "deadbeef", "commit": "cafebabe"},
            foreman_summary_text="did it",
            cwd=tmp_path,
        )
        return (tmp_path / name).read_text(encoding="utf-8")


def _delta_pack_body(tmp_path: Path) -> str:
    (tmp_path / "s1.pack.md").write_text("# pack\n", encoding="utf-8")
    with mock.patch.object(tb, "PACKS", tmp_path), \
            mock.patch.object(tb, "_git_rev", return_value="deadbeef"), \
            mock.patch.object(tb, "_git_diff", return_value="+x\n"):
        name = tb.build_delta_gate_pack(
            slice_id="s1",
            round_n=2,
            original_pack_name="s1.pack.md",
            acceptance=[],
            prior_findings=[{"tier": "P1", "issue": "x"}],
            foreman_block={"base": "deadbeef", "commit": "cafebabe"},
            foreman_summary_text="fixed x",
            cwd=tmp_path,
            label="rev2",
        )
        return (tmp_path / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# a. one frozen block, rendered identically by both injection paths
# ---------------------------------------------------------------------------
def test_block_is_the_five_line_caliber():
    lines = tb.GATE_PRINCIPLES_BLOCK.strip().splitlines()
    assert lines[0] == "## 审核原则"
    assert len(lines) == 6, lines  # heading + 5 rules
    for kw in KEYWORDS:
        assert kw in tb.GATE_PRINCIPLES_BLOCK, kw
    assert tb.GATE_SOFT_MAX_STEPS == 28
    assert f"约 {tb.GATE_SOFT_MAX_STEPS} 步" in tb.GATE_PRINCIPLES_BLOCK


def test_open_slice_renders_the_same_block():
    assert _open_slice_block() == tb.GATE_PRINCIPLES_BLOCK.strip()


# ---------------------------------------------------------------------------
# b. both pack builders inject it before ## Done when
# ---------------------------------------------------------------------------
def test_build_gate_pack_carries_caliber(tmp_path):
    body = _gate_pack_body(tmp_path)
    for kw in KEYWORDS:
        assert kw in body, kw
    assert "## 审核原则" in body
    assert body.index("## 审核原则") < body.index("## Done when")
    assert "28" in body


def test_build_delta_gate_pack_carries_caliber(tmp_path):
    body = _delta_pack_body(tmp_path)
    for kw in KEYWORDS:
        assert kw in body, kw
    assert "## 审核原则" in body
    assert body.index("## 审核原则") < body.index("## Done when")


def test_gate_pack_pairs_soft_cap_with_paused_limit(tmp_path):
    body = _gate_pack_body(tmp_path)
    line = next(l for l in body.splitlines() if "paused_limit" in l)
    assert "28" in line
    # the caliber never becomes a hard kill
    assert "硬" not in line


# ---------------------------------------------------------------------------
# c. gate step budget: defaults, override precedence, bad values
# ---------------------------------------------------------------------------
def test_gate_budget_default_is_28():
    assert T.DEFAULT_STEP_BUDGET_BY_ROLE["gate"] == 28
    assert T.role_step_budget("gate", {}) == 28


def test_gate_max_steps_override_and_precedence():
    assert T.role_step_budget("gate", {"gate_max_steps": 22}) == 22
    assert T.role_step_budget("gate", {"step_budget_by_role": {"gate": 40}}) == 40
    assert (
        T.role_step_budget(
            "gate", {"gate_max_steps": 22, "step_budget_by_role": {"gate": 40}}
        )
        == 22
    )


@pytest.mark.parametrize("bad", ["x", -1, 0, None, "", 0.5])
def test_gate_bad_values_fall_back_without_raising(bad):
    assert T.role_step_budget("gate", {"gate_max_steps": bad}) == 28
    assert (
        T.role_step_budget(
            "gate", {"gate_max_steps": bad, "step_budget_by_role": {"gate": bad}}
        )
        == 28
    )


def test_gate_budget_reads_only_temp_limits(monkeypatch, tmp_path):
    path = tmp_path / "limits.json"
    path.write_text(json.dumps({"gate_max_steps": 22}), encoding="utf-8")
    monkeypatch.setattr(T, "LIMITS_PATH", path)
    assert T.role_step_budget("gate") == 22


# ---------------------------------------------------------------------------
# d. spawned gate env exposes the soft cap under both names
# ---------------------------------------------------------------------------
def test_spawn_env_gate_exports_soft_cap(monkeypatch, tmp_path):
    path = tmp_path / "limits.json"
    path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(T, "LIMITS_PATH", path)
    env = tb._spawn_env("gate")
    assert env["DSH_STEP_BUDGET"] == "28"
    assert env["DSH_GATE_MAX_STEPS"] == "28"
