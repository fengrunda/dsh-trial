#!/usr/bin/env python3
"""Gate review caliber: evidence check (TDD + run + design), one soft step cap.

Regression lock (2026-11-06 口径，取代「只核 Done-when / 设计对齐 / P0–P1」):

  a. ``GATE_PRINCIPLES_BLOCK`` is the single authority; ``open-slice.sh``
     (``PROMPT_MODE=gate``) must render it byte-for-byte;
  b. ``build_gate_pack`` and ``build_delta_gate_pack`` both inject it right
     before ``## Done when`` — so delta/rev packs cannot lose the caliber;
  c. the block carries TDD 证据 / 实证运行 / 猜测性风险不判 HOLD / 禁止兜底 /
     测试日志落盘，并保留 paused_limit 与软上限 28;
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
KEYWORDS = (
    "TDD 证据",
    "实证运行",
    "猜测性风险",
    "不判 HOLD",
    "禁止要求",
    "兜底",
    "摘要 + 失败用例",
    "日志落盘",
    "paused_limit",
    "env_blocked",
)
OLD_CALIBER = ("设计对齐 / P0–P1", "只读复核")


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
def test_block_is_the_evidence_caliber():
    lines = tb.GATE_PRINCIPLES_BLOCK.strip().splitlines()
    assert lines[0] == "## 审核原则"
    assert "## 测试口径" in tb.GATE_PRINCIPLES_BLOCK
    for kw in KEYWORDS:
        assert kw in tb.GATE_PRINCIPLES_BLOCK, kw
    assert "猜测性风险" in tb.GATE_PRINCIPLES_BLOCK and "不判 HOLD" in tb.GATE_PRINCIPLES_BLOCK
    assert "禁止要求" in tb.GATE_PRINCIPLES_BLOCK and "兜底" in tb.GATE_PRINCIPLES_BLOCK
    assert "摘要 + 失败用例" in tb.GATE_PRINCIPLES_BLOCK and "日志落盘" in tb.GATE_PRINCIPLES_BLOCK
    for old in OLD_CALIBER:
        assert old not in tb.GATE_PRINCIPLES_BLOCK, old
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
    # paused_limit summaries must carry used_steps/remaining (r2 P1)
    assert "used_steps" in body and "remaining" in body


def test_build_delta_gate_pack_carries_caliber(tmp_path):
    body = _delta_pack_body(tmp_path)
    for kw in KEYWORDS:
        assert kw in body, kw
    assert "## 审核原则" in body
    assert body.index("## 审核原则") < body.index("## Done when")
    assert "used_steps" in body and "remaining" in body


def test_gate_pack_pairs_soft_cap_with_paused_limit(tmp_path):
    body = _gate_pack_body(tmp_path)
    line = next(l for l in body.splitlines() if "paused_limit" in l)
    assert "28" in line
    # the caliber never becomes a hard kill
    assert "硬" not in line


# ---------------------------------------------------------------------------
# b2. near-maxPackBytes packs keep the tail (caliber + Done when) — r2 P1
# ---------------------------------------------------------------------------
def _assert_tail_survived(body: str) -> None:
    assert len(body.encode("utf-8")) <= tb.MAX_PACK_BYTES
    assert tb.GATE_PRINCIPLES_BLOCK in body
    assert "## Done when" in body
    assert body.index("## 审核原则") < body.index("## Done when")


def test_oversize_gate_pack_keeps_caliber_and_done_when(tmp_path):
    (tmp_path / "s1.pack.md").write_text("# pack\n" + "+pad\n" * 2000, encoding="utf-8")
    with mock.patch.object(tb, "PACKS", tmp_path), \
            mock.patch.object(tb, "_git_rev", return_value="deadbeef"), \
            mock.patch.object(tb, "_git_diff", return_value="+x\n" * 4000):
        name = tb.build_gate_pack(
            slice_id="s1",
            round_n=1,
            original_pack_name="s1.pack.md",
            acceptance=["do the thing"],
            foreman_block={"base": "deadbeef", "commit": "cafebabe"},
            foreman_summary_text="did it",
            cwd=tmp_path,
        )
        _assert_tail_survived((tmp_path / name).read_text(encoding="utf-8"))


def test_oversize_delta_pack_keeps_caliber_and_done_when(tmp_path):
    findings = [
        {"tier": "P1", "file": "a.py", "issue": "x" * 4000, "fix_hint": "y" * 4000}
        for _ in range(4)
    ]
    (tmp_path / "s1.pack.md").write_text("# pack\n", encoding="utf-8")
    with mock.patch.object(tb, "PACKS", tmp_path), \
            mock.patch.object(tb, "_git_rev", return_value="deadbeef"), \
            mock.patch.object(tb, "_git_diff", return_value="+x\n" * 4000):
        name = tb.build_delta_gate_pack(
            slice_id="s1",
            round_n=2,
            original_pack_name="s1.pack.md",
            acceptance=[],
            prior_findings=findings,
            foreman_block={"base": "deadbeef", "commit": "cafebabe"},
            foreman_summary_text="did it",
            cwd=tmp_path,
            label="rev2",
        )
        _assert_tail_survived((tmp_path / name).read_text(encoding="utf-8"))


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
