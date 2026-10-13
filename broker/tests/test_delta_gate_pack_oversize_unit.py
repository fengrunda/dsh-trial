"""impl-delta-gate-pack-oversize — delta gate pack graded truncation + regressions.

Covered here:
  a. ``build_delta_gate_pack`` never raises on an oversize diff: the pack is
     written, stays within ``MAX_PACK_BYTES``, carries the truncation marker and
     still keeps the ``Diff --stat`` section (file list);
  b. ``decide_impl_completion`` treats ``goal["gate_pack_error"]`` as a clean
     done (the chain runs the regular broker gate, no fix ticket) even with stale
     P1 findings and a ``rework_fresh`` handoff;
  c. an empty-findings HOLD (``last_gate_verdict_empty_hold``) does not inherit
     the previous round's findings;
  d. a successfully built pack / a real HOLD-PASS accounting round clears a
     stale ``goal["gate_pack_error"]`` so the guard cannot stay stuck on
     ``clean_done`` for the rest of the ticket.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location(
    "trial_broker_delta_pack_oversize", ROOT / "trial-broker.py"
)
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)


# --------------------------------------------------------------------------
# a. oversize diff is graded-truncated, not raised
# --------------------------------------------------------------------------
def test_oversize_diff_is_truncated_not_raised(tmp_path):
    huge = ("+" + "x" * 98 + "\n") * 600  # ~60KB of diff body
    assert len(huge.encode("utf-8")) > 50_000
    with mock.patch.object(tb, "_git_diff", return_value=huge), \
            mock.patch.object(tb, "PACKS", tmp_path):
        name = tb.build_delta_gate_pack(
            slice_id="s1",
            round_n=2,
            original_pack_name="s1.pack.md",
            acceptance=[],
            prior_findings=[{"tier": "P1", "issue": "x"}],
            foreman_block={"base": "deadbeef", "commit": "cafebabe"},
            foreman_summary_text="notes " * 100,
            cwd=tmp_path,
            label="rev2",
        )
        body = (tmp_path / name).read_text(encoding="utf-8")

    assert len(body.encode("utf-8")) <= tb.MAX_PACK_BYTES
    assert "[delta pack truncated]" in body
    assert "Diff --stat" in body


def test_graded_truncation_path_runs_when_precap_disabled(tmp_path):
    """With the diff pre-cap lifted, the body itself is oversize and the graded
    diff truncation must kick in — still no raise, still within budget, and the
    Diff --stat / file-list section survives."""
    huge = ("+" + "x" * 98 + "\n") * 600
    with mock.patch.object(tb, "_git_diff", return_value=huge), \
            mock.patch.object(tb, "MAX_DIFF_BYTES", 10_000_000), \
            mock.patch.object(tb, "PACKS", tmp_path):
        name = tb.build_delta_gate_pack(
            slice_id="s1",
            round_n=2,
            original_pack_name="s1.pack.md",
            acceptance=[],
            prior_findings=[{"tier": "P1", "issue": "x"}],
            foreman_block={"base": "deadbeef", "commit": "cafebabe"},
            foreman_summary_text="notes " * 100,
            cwd=tmp_path,
            label="rev2",
        )
        body = (tmp_path / name).read_text(encoding="utf-8")

    assert len(body.encode("utf-8")) <= tb.MAX_PACK_BYTES
    assert "[delta pack truncated]" in body


def test_huge_findings_truncated_to_200_chars(tmp_path):
    """Step (c): when even a fully-shrunk diff cannot fit the pack, every
    fix_hint/detail is capped at 200 chars and the pack still fits."""
    huge = ("+" + "x" * 98 + "\n") * 600
    findings = [
        {"tier": "P1", "issue": f"f{i}", "fix_hint": "y" * 5000, "detail": "z" * 5000}
        for i in range(40)
    ]
    with mock.patch.object(tb, "_git_diff", return_value=huge), \
            mock.patch.object(tb, "MAX_DIFF_BYTES", 10_000_000), \
            mock.patch.object(tb, "PACKS", tmp_path):
        name = tb.build_delta_gate_pack(
            slice_id="s1",
            round_n=2,
            original_pack_name="s1.pack.md",
            acceptance=[],
            prior_findings=findings,
            foreman_block={"base": "deadbeef", "commit": "cafebabe"},
            foreman_summary_text="notes " * 100,
            cwd=tmp_path,
            label="rev2",
        )
        body = (tmp_path / name).read_text(encoding="utf-8")

    assert len(body.encode("utf-8")) <= tb.MAX_PACK_BYTES
    assert "[delta pack truncated]" in body
    # The pathological case (40 findings × 200-char hints + details) is still
    # over budget after step (c); the safety net must prevent any raise.


# --------------------------------------------------------------------------
# b. gate pack build error → clean done, broker gate reviews
# --------------------------------------------------------------------------
def test_decide_impl_completion_gate_pack_error_is_clean_done():
    goal = {
        "gate_pack_error": "pack too big",
        "last_gate_findings": [{"tier": "P1", "issue": "stale"}],
        "status": "done",
    }
    block = {"status": "done", "notes": "rework_fresh handoff"}
    d = tb.decide_impl_completion(block, goal)
    assert d["action"] == "clean_done"
    assert d["findings"] == []
    assert d["mode"] == ""


# --------------------------------------------------------------------------
# c. empty HOLD does not inherit prior findings
# --------------------------------------------------------------------------
def test_decide_impl_completion_empty_hold_does_not_inherit_findings():
    goal = {
        "last_gate_verdict_empty_hold": True,
        "last_gate_findings": [{"tier": "P1", "issue": "stale"}],
    }
    d = tb.decide_impl_completion({"status": "done"}, goal)
    assert d["action"] == "clean_done"
    assert d["findings"] == []


# --------------------------------------------------------------------------
# d. a real gate round (HOLD/PASS accounting) clears the stale pack error
# --------------------------------------------------------------------------
def test_gate_pack_error_cleared_after_hold_accounting():
    goal = {
        "gate_pack_error": "no such dir",
        "last_gate_findings": [{"tier": "P1", "issue": "stale"}],
    }
    # the HOLD/PASS accounting path clears it before recording the round
    tb._clear_gate_pack_error(goal)
    assert "gate_pack_error" not in goal
    # so decide_impl_completion no longer takes the pack-error shortcut
    d = tb.decide_impl_completion({"status": "done"}, goal)
    assert d["reason"] != "gate pack build error; broker gate will review"


def test_clear_gate_pack_error_is_noop_without_goal():
    tb._clear_gate_pack_error(None)  # no active chain goal must not raise
