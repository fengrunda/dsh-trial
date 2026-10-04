#!/usr/bin/env python3
"""Regression tests: a gate PASS must never be turned into a false failure.

Real bug (Menu-4 r3): a mid-ticket ``submit_for_review`` gate PASSed the slice,
then the foreman exited non-zero **without** writing a disk impl summary.
``run_chain_rounds`` marked the chain ``failed`` with
``foreman exit 1 without summary`` and ``run_goal_job`` copied that into
``goal.status = failed`` — a false fail of a slice that was already proven done.

Rules under test:
  A) missing summary + gate PASS evidence  -> chain PASS (salvage), rc 0
     (non-zero exit, and exit 0 with the summary file absent)
  B) missing summary + NO PASS evidence    -> chain failed, error mentions
                                              "without summary", rc != 0
     (including exit 0: do not invent blocked / awaiting_supervisor)
  C) final-round gate missing/empty summary (timeout) + prior gate PASS
     evidence + impl summary present        -> chain PASS (salvage), rc 0
  D) final-round gate missing/empty summary + NO PASS evidence
                                            -> still escalated, rc != 0
  E) real HOLD gate summary (findings/unmet acceptance) at final round
                                            -> still escalated, rc != 0

Run with either:
  python3 -m pytest broker/tests/test_gate_pass_no_false_fail_unit.py -q
  python3 broker/tests/test_gate_pass_no_false_fail_unit.py
"""
from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_gatepass", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

T = tb.T


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
def _patched_dirs(root: Path):
    """Patch every broker state dir onto ``root`` (mirrors goal multislice tests)."""
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


def _write_summary(path: Path, block: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "prose\n\n```json\n" + json.dumps(block, ensure_ascii=False, indent=2) + "\n```\n",
        encoding="utf-8",
    )


def _fake_artifacts(job, log_path, summary_path, ec, ticket=None):
    return {
        "ticket": ticket or job.get("ticket"),
        "exit_code": ec,
        "assert_clean": True,
        "composition": None,
        "summary": str(summary_path),
        "summary_exists": summary_path.is_file(),
    }


def _chain_job(slice_id: str, cwd: Path, *, goal: str | None = None) -> dict:
    job = {
        "id": f"unit-gp-{slice_id}",
        "type": "chain",
        "slice": slice_id,
        "pack": f"{slice_id}.pack.md",
        "acceptance": ["do the thing"],
        "profile": "acp-lite",
        "gate_profile": "acp-lite",
        "cwd": str(cwd),
        "max_rounds": 1,
        "notify": "hub",
        "notify_dry_run": True,
    }
    if goal:
        job["goal"] = goal
        job["from_goal"] = True
        job["defer_supervisor_close"] = True
    return job


def _seed_goal(dirs: dict, goal_id: str, slice_id: str, *, gate_verdict: str | None) -> dict:
    """Write a goal state to disk; optionally with an inline gate PASS/HOLD row."""
    goal = {
        "goal": goal_id,
        "status": "running",
        "slices": [slice_id],
        "current_slice": slice_id,
        "metrics": T.empty_metrics(),
    }
    if gate_verdict:
        T.record_ticket_metric(
            goal,
            {
                "role": "gate",
                "ticket": f"gate-{slice_id}-r1",
                "slice": slice_id,
                "kind": "submit_for_review",
                "verdict": gate_verdict,
                "exit": 0,
            },
        )
    (dirs["goals"] / f"{goal_id}.json").write_text(
        json.dumps(goal, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return goal


def _run_chain(job: dict, root: Path, fake_open_slice) -> int:
    inbox = tb.INBOX / f"{job['id']}.json"
    inbox.parent.mkdir(parents=True, exist_ok=True)
    inbox.write_text(json.dumps(job), encoding="utf-8")
    with mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), mock.patch.object(
        tb, "write_artifacts", side_effect=_fake_artifacts
    ), mock.patch.object(
        tb, "maybe_notify_hub", return_value={"sent": True, "rc": 0, "kind": "dsh-trial-chain", "detail": "dry"}
    ):
        return tb.run_job(inbox)


# --------------------------------------------------------------------------
# A) missing summary + gate PASS evidence -> salvage PASS
# --------------------------------------------------------------------------
def test_missing_summary_with_submit_for_review_pass_is_salvaged(tmp_path: Path):
    """The Menu-4 r3 case: foreman ec=1, no summary, but gate already PASSed."""
    slice_id = "unit-gp-salvage"
    goal_id = "unit-gp-goal"
    ctx, dirs = _patched_dirs(tmp_path / "state")
    with ctx:
        (dirs["packs"] / f"{slice_id}.pack.md").write_text(
            f"---\nslice_id: {slice_id}\n---\n# salvage me\n", encoding="utf-8"
        )
        # inline gate PASS recorded in the goal metrics, exactly like
        # _handle_submit_for_review does before the foreman dies.
        _seed_goal(dirs, goal_id, slice_id, gate_verdict="PASS")

        def fake_open_slice(**kwargs):
            kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
            kwargs["log_path"].write_text("boom\n", encoding="utf-8")
            return 1  # foreman exits non-zero and writes NO summary

        ec = _run_chain(_chain_job(slice_id, tmp_path, goal=goal_id), tmp_path, fake_open_slice)

        chain = json.loads((dirs["chains"] / f"{slice_id}.json").read_text())
        assert ec == 0, f"salvage should return 0, got {ec}"
        assert chain["state"] == "PASS", chain
        assert not chain.get("error"), chain
        assert "salvage" in str(chain.get("salvage_note") or ""), chain
        assert chain["last_verdict"] == "PASS", chain

        # the goal must not be marked failed by the missing-summary branch
        goal_after = json.loads((dirs["goals"] / f"{goal_id}.json").read_text())
        assert goal_after["status"] != "failed", goal_after.get("status")
        print("OK salvage: missing summary + gate PASS ->", chain["state"])


def test_salvage_writes_synthetic_impl_summary(tmp_path: Path):
    """Salvage leaves a readable status=done summary instead of a hole."""
    slice_id = "unit-gp-synth"
    goal_id = "unit-gp-goal-synth"
    ctx, dirs = _patched_dirs(tmp_path / "state")
    with ctx:
        (dirs["packs"] / f"{slice_id}.pack.md").write_text(
            f"---\nslice_id: {slice_id}\n---\n# synth\n", encoding="utf-8"
        )
        _seed_goal(dirs, goal_id, slice_id, gate_verdict="PASS")
        seen: dict = {}

        def fake_open_slice(**kwargs):
            kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
            kwargs["log_path"].write_text("boom\n", encoding="utf-8")
            if kwargs["prompt_mode"] == "foreman":
                seen["impl_summary"] = tb.SUMMARIES / f"{kwargs['summary_name']}.md"
            return 1

        ec = _run_chain(_chain_job(slice_id, tmp_path, goal=goal_id), tmp_path, fake_open_slice)
        assert ec == 0, ec
        p = seen.get("impl_summary")
        assert p is not None and p.is_file(), f"synthetic summary missing: {p}"
        text = p.read_text(encoding="utf-8")
        assert '"status": "done"' in text or '"status":"done"' in text, text
        print("OK salvage wrote synthetic summary", p.name)


def test_helper_sees_inline_pass_and_disk_gate_pass(tmp_path: Path):
    """Both evidence sources are recognised (metrics row and gate summary file)."""
    slice_id = "unit-gp-evidence"
    ctx, dirs = _patched_dirs(tmp_path / "state")
    with ctx:
        goal = {"goal": "unit-gp-g", "metrics": T.empty_metrics()}
        T.record_ticket_metric(
            goal,
            {
                "role": "gate",
                "ticket": f"gate-{slice_id}-r1",
                "slice": slice_id,
                "kind": "submit_for_review",
                "verdict": "PASS",
                "exit": 0,
            },
        )
        ok, reason = tb._slice_has_review_pass(goal, slice_id, reload=True)
        assert ok is True, "helper must see the submit_for_review PASS row"
        assert "submit_for_review" in reason, reason

        _write_summary(dirs["summaries"] / f"{slice_id}-gate-rev1.md", {"verdict": "PASS", "findings": []})
        ok2, reason2 = tb._slice_has_review_pass(None, slice_id, reload=True)
        assert ok2 is True, "helper must see the on-disk gate PASS summary"
        assert "gate summary" in reason2, reason2
        print("OK helper sees inline metric + disk gate summary")


# --------------------------------------------------------------------------
# B) missing summary + NO PASS evidence -> still failed
# --------------------------------------------------------------------------

def test_exit0_missing_summary_with_prior_pass_closes_pass(tmp_path: Path):
    """Exit 0, summary path missing, prior gate PASS → PASS, not blocked."""
    slice_id = "unit-gp-exit0-pass"
    goal_id = "unit-gp-exit0-pass-goal"
    ctx, dirs = _patched_dirs(tmp_path / "state")
    with ctx:
        (dirs["packs"] / f"{slice_id}.pack.md").write_text(
            f"---\nslice_id: {slice_id}\n---\n# exit0 salvage\n", encoding="utf-8"
        )
        _seed_goal(dirs, goal_id, slice_id, gate_verdict="PASS")

        def fake_open_slice(**kwargs):
            kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
            kwargs["log_path"].write_text("ok but summary elsewhere\n", encoding="utf-8")
            return 0  # exit 0, no summary at the broker path

        ec = _run_chain(_chain_job(slice_id, tmp_path, goal=goal_id), tmp_path, fake_open_slice)
        chain = json.loads((dirs["chains"] / f"{slice_id}.json").read_text())
        assert ec == 0, f"exit-0 salvage should return 0, got {ec}"
        assert chain["state"] == "PASS", chain
        assert chain["state"] != "awaiting_supervisor", chain
        assert chain.get("awaiting_reason") != "blocked", chain
        assert not chain.get("error"), chain
        assert "salvage" in str(chain.get("salvage_note") or ""), chain
        assert chain["last_verdict"] == "PASS", chain
        print("OK exit0 missing summary + gate PASS ->", chain["state"])


def test_exit0_missing_summary_without_pass_is_not_pass(tmp_path: Path):
    """Exit 0, summary missing, no gate PASS → failed, never PASS or blocked."""
    slice_id = "unit-gp-exit0-nopass"
    goal_id = "unit-gp-exit0-nopass-goal"
    ctx, dirs = _patched_dirs(tmp_path / "state")
    with ctx:
        (dirs["packs"] / f"{slice_id}.pack.md").write_text(
            f"---\nslice_id: {slice_id}\n---\n# exit0 no pass\n", encoding="utf-8"
        )
        _seed_goal(dirs, goal_id, slice_id, gate_verdict=None)

        def fake_open_slice(**kwargs):
            kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
            kwargs["log_path"].write_text("ok but no summary\n", encoding="utf-8")
            return 0

        ec = _run_chain(_chain_job(slice_id, tmp_path, goal=goal_id), tmp_path, fake_open_slice)
        chain = json.loads((dirs["chains"] / f"{slice_id}.json").read_text())
        assert chain["state"] != "PASS", chain
        assert chain["state"] == "failed", chain
        assert chain["state"] != "awaiting_supervisor", chain
        assert "without summary" in str(chain.get("error") or ""), chain
        assert not chain.get("salvage_note"), chain
        assert ec != 0, f"no-PASS missing summary must not return 0 (got {ec})"
        print("OK exit0 missing summary + no PASS ->", chain["state"])


def test_missing_summary_without_pass_still_fails(tmp_path: Path):
    """No PASS evidence anywhere: the failure must be preserved, not masked."""
    slice_id = "unit-gp-real-fail"
    goal_id = "unit-gp-goal-fail"
    ctx, dirs = _patched_dirs(tmp_path / "state")
    with ctx:
        (dirs["packs"] / f"{slice_id}.pack.md").write_text(
            f"---\nslice_id: {slice_id}\n---\n# really broken\n", encoding="utf-8"
        )
        _seed_goal(dirs, goal_id, slice_id, gate_verdict=None)

        def fake_open_slice(**kwargs):
            kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
            kwargs["log_path"].write_text("boom\n", encoding="utf-8")
            return 1  # no summary, no gate PASS

        ec = _run_chain(_chain_job(slice_id, tmp_path, goal=goal_id), tmp_path, fake_open_slice)

        chain = json.loads((dirs["chains"] / f"{slice_id}.json").read_text())
        assert chain["state"] == "failed", chain
        assert "without summary" in str(chain.get("error") or ""), chain
        assert not chain.get("salvage_note"), chain
        assert ec != 0, f"a real failure must not return 0 (got {ec})"
        print("OK no-PASS failure preserved:", chain["state"], chain["error"])


def test_hold_gate_row_does_not_salvage(tmp_path: Path):
    """A HOLD gate row is not PASS evidence: the failure must stand."""
    slice_id = "unit-gp-hold-run"
    goal_id = "unit-gp-goal-hold"
    ctx, dirs = _patched_dirs(tmp_path / "state")
    with ctx:
        (dirs["packs"] / f"{slice_id}.pack.md").write_text(
            f"---\nslice_id: {slice_id}\n---\n# held\n", encoding="utf-8"
        )
        _seed_goal(dirs, goal_id, slice_id, gate_verdict="HOLD")

        def fake_open_slice(**kwargs):
            kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
            kwargs["log_path"].write_text("boom\n", encoding="utf-8")
            return 1

        ec = _run_chain(_chain_job(slice_id, tmp_path, goal=goal_id), tmp_path, fake_open_slice)
        chain = json.loads((dirs["chains"] / f"{slice_id}.json").read_text())
        assert chain["state"] == "failed", chain
        assert "without summary" in str(chain.get("error") or ""), chain
        assert ec != 0, ec
        print("OK HOLD gate row did not salvage")


def test_hold_verdict_is_not_pass_evidence(tmp_path: Path):
    """A HOLD gate summary must never be mistaken for PASS evidence."""
    slice_id = "unit-gp-hold"
    ctx, dirs = _patched_dirs(tmp_path / "state")
    with ctx:
        _write_summary(
            dirs["summaries"] / f"{slice_id}-gate-rev1.md",
            {"verdict": "HOLD", "findings": [{"tier": "P1", "file": "a.py", "issue": "x", "fix_hint": "y"}]},
        )
        ok, reason = tb._slice_has_review_pass(None, slice_id, reload=True)
        assert ok is False, f"HOLD must not count as PASS evidence ({reason})"

        goal = {"goal": "g", "metrics": T.empty_metrics()}
        T.record_ticket_metric(
            goal,
            {"role": "gate", "ticket": "gate-x", "slice": slice_id, "kind": "submit_for_review", "verdict": "HOLD"},
        )
        ok2, _ = tb._slice_has_review_pass(goal, slice_id)
        assert ok2 is False, "a HOLD submit_for_review must not be PASS evidence"
        print("OK HOLD is not PASS evidence")


def test_helper_is_slice_scoped(tmp_path: Path):
    """A PASS on another slice must not salvage this one."""
    ctx, dirs = _patched_dirs(tmp_path / "state")
    with ctx:
        goal = {"goal": "g", "metrics": T.empty_metrics()}
        T.record_ticket_metric(
            goal,
            {"role": "gate", "ticket": "gate-s1", "slice": "slice-one", "kind": "submit_for_review", "verdict": "PASS"},
        )
        ok, _ = tb._slice_has_review_pass(goal, "slice-two")
        assert ok is False, "PASS must be scoped to the same slice"
        ok2, _ = tb._slice_has_review_pass(goal, "slice-one")
        assert ok2 is True
        ok3, _ = tb._slice_has_review_pass(goal, "")
        assert ok3 is False, "empty slice id must not match everything"
        print("OK helper is slice scoped")


# --------------------------------------------------------------------------
# C) final-round gate missing/empty summary + prior PASS -> salvage PASS
# --------------------------------------------------------------------------
def test_last_round_gate_missing_summary_with_prior_pass_is_salvaged(tmp_path: Path):
    """Final-round gate wrote no summary (timeout); prior gate PASS exists."""
    slice_id = "unit-gp-gatesalv"
    goal_id = "unit-gp-gatesalv-goal"
    ctx, dirs = _patched_dirs(tmp_path / "state")
    with ctx:
        (dirs["packs"] / f"{slice_id}.pack.md").write_text(
            f"---\nslice_id: {slice_id}\n---\n# gate salvage me\n", encoding="utf-8"
        )
        # prior PASS evidence: inline gate PASS row recorded in goal metrics
        _seed_goal(dirs, goal_id, slice_id, gate_verdict="PASS")

        def fake_open_slice(**kwargs):
            kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
            kwargs["log_path"].write_text("ran\n", encoding="utf-8")
            if kwargs["prompt_mode"] == "foreman":
                _write_summary(
                    tb.SUMMARIES / f"{kwargs['summary_name']}.md",
                    {"status": "done", "verdict": "PASS"},
                )
                return 0
            # gate: writes NO summary (timeout / crash) and exits non-zero
            return 1

        ec = _run_chain(_chain_job(slice_id, tmp_path, goal=goal_id), tmp_path, fake_open_slice)
        chain = json.loads((dirs["chains"] / f"{slice_id}.json").read_text())
        assert ec == 0, f"gate-salvage should return 0, got {ec}"
        assert chain["state"] == "PASS", chain
        assert chain["last_verdict"] == "PASS", chain
        assert not chain.get("error"), chain
        note = str(chain.get("salvage_note") or "")
        assert "last-round gate missing summary" in note, chain
        assert "adopting prior PASS" in note, chain
        last_round = (chain.get("rounds") or [{}])[-1]
        assert last_round.get("salvaged_missing_gate_summary") is True, chain
        # never synthesize/overwrite a disk gate summary
        assert not (tb.SUMMARIES / f"{slice_id}-gate-r1.md").exists()
        print("OK gate-salvage: missing gate summary + prior PASS ->", chain["state"])


def test_last_round_gate_missing_summary_without_pass_still_escalates(tmp_path: Path):
    """Missing final-round gate summary but NO PASS evidence -> escalated."""
    slice_id = "unit-gp-gateesc"
    goal_id = "unit-gp-gateesc-goal"
    ctx, dirs = _patched_dirs(tmp_path / "state")
    with ctx:
        (dirs["packs"] / f"{slice_id}.pack.md").write_text(
            f"---\nslice_id: {slice_id}\n---\n# gate escalate me\n", encoding="utf-8"
        )
        # NO PASS evidence in goal metrics
        _seed_goal(dirs, goal_id, slice_id, gate_verdict=None)

        def fake_open_slice(**kwargs):
            kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
            kwargs["log_path"].write_text("ran\n", encoding="utf-8")
            if kwargs["prompt_mode"] == "foreman":
                _write_summary(
                    tb.SUMMARIES / f"{kwargs['summary_name']}.md",
                    {"status": "done", "verdict": "PASS"},
                )
                return 0
            # gate: writes NO summary (timeout / crash) and exits non-zero
            return 1

        ec = _run_chain(_chain_job(slice_id, tmp_path, goal=goal_id), tmp_path, fake_open_slice)
        chain = json.loads((dirs["chains"] / f"{slice_id}.json").read_text())
        assert ec != 0, ec
        assert chain["state"] == "escalated", chain
        assert "last-round gate missing summary" not in str(chain.get("salvage_note") or ""), chain
        print("OK gate-salvage: missing gate summary + no PASS ->", chain["state"])


def test_last_round_gate_hold_with_findings_still_escalates(tmp_path: Path):
    """A real HOLD gate summary (findings present) must never be salvaged."""
    slice_id = "unit-gp-gatehold"
    goal_id = "unit-gp-gatehold-goal"
    ctx, dirs = _patched_dirs(tmp_path / "state")
    with ctx:
        (dirs["packs"] / f"{slice_id}.pack.md").write_text(
            f"---\nslice_id: {slice_id}\n---\n# gate hold me\n", encoding="utf-8"
        )
        # prior PASS evidence exists, but the real HOLD must win
        _seed_goal(dirs, goal_id, slice_id, gate_verdict="PASS")

        def fake_open_slice(**kwargs):
            kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
            kwargs["log_path"].write_text("ran\n", encoding="utf-8")
            if kwargs["prompt_mode"] == "foreman":
                _write_summary(
                    tb.SUMMARIES / f"{kwargs['summary_name']}.md",
                    {"status": "done", "verdict": "PASS"},
                )
                return 0
            # gate: real HOLD with blocking findings
            _write_summary(
                tb.SUMMARIES / f"{kwargs['summary_name']}.md",
                {"verdict": "HOLD", "findings": [{"severity": "P1", "desc": "broken"}]},
            )
            return 0

        ec = _run_chain(_chain_job(slice_id, tmp_path, goal=goal_id), tmp_path, fake_open_slice)
        chain = json.loads((dirs["chains"] / f"{slice_id}.json").read_text())
        assert ec != 0, ec
        assert chain["state"] == "escalated", chain
        assert "last-round gate missing summary" not in str(chain.get("salvage_note") or ""), chain
        print("OK gate-salvage: real HOLD with findings ->", chain["state"])


def main():
    tb._ensure_dirs()
    with tempfile.TemporaryDirectory() as td:
        p = Path(td)
        test_missing_summary_with_submit_for_review_pass_is_salvaged(p / "a")
        test_exit0_missing_summary_with_prior_pass_closes_pass(p / "a2")
        test_exit0_missing_summary_without_pass_is_not_pass(p / "a3")
        test_salvage_writes_synthetic_impl_summary(p / "b")
        test_helper_sees_inline_pass_and_disk_gate_pass(p / "c")
        test_missing_summary_without_pass_still_fails(p / "d")
        test_hold_gate_row_does_not_salvage(p / "e")
        test_hold_verdict_is_not_pass_evidence(p / "f")
        test_helper_is_slice_scoped(p / "g")
        test_last_round_gate_missing_summary_with_prior_pass_is_salvaged(p / "h")
        test_last_round_gate_missing_summary_without_pass_still_escalates(p / "i")
        test_last_round_gate_hold_with_findings_still_escalates(p / "j")
    print("ALL GATE-PASS-NO-FALSE-FAIL UNIT OK")


if __name__ == "__main__":
    main()
