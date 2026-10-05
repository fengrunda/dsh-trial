#!/usr/bin/env python3
"""Short metric slice names must salvage, and a failed goal must continue.

Incident fields (thin-state, 2026-10-05), copied into fixtures — tests do not
read the live supervisor directory:

* metrics row: slice="s1", ticket="gate-trial-s1-rev1",
  kind="submit_for_review", verdict="PASS"
* chain slice: "engine-memory-extract-embed-v1-s1"
* chain.error: "foreman exit 0 without summary"
* round impl_exit=0, summary_exists=false, last_verdict empty
* goal status failed; goal-update resume previously ignored failed

A different goal's "s1" must not match. Missing summary without PASS stays
failed. Resume of the false fail runs the next planned slice and does not
re-open s1. A real failure cannot resume into PASS.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location(
    "trial_broker_slice_alias", ROOT / "trial-broker.py"
)
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

T = tb.T

# Real incident ids. Matching must work for these AND for any other goal.
GOAL = "engine-memory-extract-embed-v1"
SLICE_S1 = "engine-memory-extract-embed-v1-s1"
SLICE_S2 = "engine-memory-extract-embed-v1-s2"
OTHER_GOAL = "other-goal"
OTHER_S1 = "other-goal-s1"


def _patched_dirs(root: Path):
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


def _pass_row(*, slice_name: str, verdict: str = "PASS") -> dict:
    """Shape of the real gate-trial-s1-rev1 metrics row."""
    return {
        "role": "gate",
        "ticket": "gate-trial-s1-rev1",
        "slice": slice_name,
        "exit": 0,
        "kind": "submit_for_review",
        "verdict": verdict,
        "rework_mode": None,
    }


def _goal_with_rows(goal_id: str, rows: list[dict], **extra) -> dict:
    metrics = T.empty_metrics()
    metrics["tickets"] = rows
    metrics["slices_planned"] = extra.pop("slices_planned", 2)
    goal = {
        "goal": goal_id,
        "status": extra.pop("status", "running"),
        "slices": extra.pop("slices", [f"{goal_id}-s1"]),
        "metrics": metrics,
        "profile": "acp",
        "supervisor_profile": "acp-lite",
        "cwd": extra.pop("cwd", "/tmp/unit-cwd"),
        "max_rounds": 2,
        "max_slices": 2,
        "notify": "hub",
        "notify_dry_run": True,
    }
    goal.update(extra)
    return goal


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def test_short_slice_matches_full_name_and_exact_and_not_other_goal():
    goal = _goal_with_rows(GOAL, [_pass_row(slice_name="s1")])
    ok, reason = tb._slice_has_review_pass(goal, SLICE_S1)
    assert ok, reason
    assert "submit_for_review PASS" in reason

    exact = _goal_with_rows(GOAL, [_pass_row(slice_name=SLICE_S1)])
    ok_exact, _ = tb._slice_has_review_pass(exact, SLICE_S1)
    assert ok_exact

    # Another goal's s1, and a bare suffix, must not hit this chain.
    assert tb._slice_has_review_pass(goal, OTHER_S1)[0] is False
    assert tb._slice_has_review_pass(goal, "unrelated-s1")[0] is False
    assert tb._slice_has_review_pass(goal, f"{GOAL}-s1-extra")[0] is False

    # Same rule on a different goal id — not hardcoded to the incident goal.
    other = _goal_with_rows(OTHER_GOAL, [_pass_row(slice_name="s1")])
    assert tb._slice_has_review_pass(other, OTHER_S1)[0] is True
    assert tb._slice_has_review_pass(other, SLICE_S1)[0] is False

    # A shared short gate filename is not evidence for every s1.
    hold = _goal_with_rows(GOAL, [_pass_row(slice_name="s1", verdict="HOLD")])
    assert tb._slice_has_review_pass(hold, SLICE_S1)[0] is False


def _fake_artifacts(job, log_path, summary_path, ec, ticket=None):
    return {
        "ticket": ticket or job.get("ticket"),
        "exit_code": ec,
        "assert_clean": True,
        "composition": None,
        "summary": str(summary_path),
        "summary_exists": summary_path.is_file(),
    }


def _run_exit0_no_summary(tmp_path: Path, *, with_pass: bool, verdict: str = "PASS") -> tuple[int, dict]:
    ctx, dirs = _patched_dirs(tmp_path / "state")
    slice_id = SLICE_S1
    with ctx:
        (dirs["packs"] / f"{slice_id}.pack.md").write_text(
            f"---\nslice_id: {slice_id}\n---\n# s1\n", encoding="utf-8"
        )
        rows = [_pass_row(slice_name="s1", verdict=verdict)] if with_pass else []
        goal = _goal_with_rows(GOAL, rows, cwd=str(tmp_path))
        _write_json(dirs["goals"] / f"{GOAL}.json", goal)

        def fake_open_slice(**kwargs):
            kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
            kwargs["log_path"].write_text("exit 0, no summary\n", encoding="utf-8")
            return 0

        job = {
            "id": "unit-alias-chain",
            "type": "chain",
            "slice": slice_id,
            "pack": f"{slice_id}.pack.md",
            "acceptance": ["create_app wires extractor"],
            "profile": "acp",
            "gate_profile": "acp",
            "cwd": str(tmp_path),
            "max_rounds": 1,
            "goal": GOAL,
            "from_goal": True,
            "defer_supervisor_close": True,
            "notify_dry_run": True,
        }
        inbox = tmp_path / "job.json"
        _write_json(inbox, job)
        with mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), mock.patch.object(
            tb, "write_artifacts", side_effect=_fake_artifacts
        ), mock.patch.object(tb, "maybe_notify_hub", return_value={"sent": False, "rc": 0}):
            rc = tb.run_job(inbox)
        chain = json.loads((dirs["chains"] / f"{slice_id}.json").read_text(encoding="utf-8"))
        return rc, chain


def test_exit0_missing_summary_with_short_pass_salvages(tmp_path: Path):
    rc, chain = _run_exit0_no_summary(tmp_path, with_pass=True)
    assert rc == 0, rc
    assert chain["state"] == "PASS", chain
    assert chain.get("error") in ("", None), chain
    assert "salvage" in str(chain.get("salvage_note") or "")
    assert chain["last_verdict"] == "PASS"


def test_exit0_missing_summary_without_pass_stays_failed(tmp_path: Path):
    rc, chain = _run_exit0_no_summary(tmp_path, with_pass=False)
    assert rc != 0
    assert chain["state"] == "failed", chain
    assert chain["error"] == "foreman exit 0 without summary"
    assert chain.get("state") != "PASS"


def test_exit0_missing_summary_with_short_hold_stays_failed(tmp_path: Path):
    rc, chain = _run_exit0_no_summary(tmp_path, with_pass=True, verdict="HOLD")
    assert rc != 0
    assert chain["state"] == "failed", chain
    assert "without summary" in chain["error"]


def _plan_text() -> str:
    block = {
        "action": "emit_chains",
        "goal": GOAL,
        "slices": [
            {
                "slice": SLICE_S1,
                "pack": f"{SLICE_S1}.pack.md",
                "acceptance": ["create_app wires extractor"],
            },
            {
                "slice": SLICE_S2,
                "pack": f"{SLICE_S2}.pack.md",
                "acceptance": ["ingest extracts into the graph and opens a PR"],
            },
        ],
        "goal_status": "running",
    }
    return "# plan\n\n```json\n" + json.dumps(block, ensure_ascii=False, indent=2) + "\n```\n"


def _failed_chain() -> dict:
    return {
        "slice": SLICE_S1,
        "type": "chain",
        "state": "failed",
        "pack": f"{SLICE_S1}.pack.md",
        "acceptance": ["create_app wires extractor"],
        "profile": "acp",
        "gate_profile": "acp",
        "cwd": "/tmp/unit-cwd",
        "max_rounds": 2,
        "rounds": [
            {
                "round": 1,
                "impl_ticket": f"impl-trial-{SLICE_S1}-r1",
                "impl_exit": 0,
                "impl_summary": f"/tmp/summaries/{SLICE_S1}-impl-r1.md",
                "impl_artifacts": {"summary_exists": False, "exit_code": 0},
                "foreman_status": "blocked",
                "foreman_block": {"status": "blocked", "commit": "ad8950a"},
            }
        ],
        "last_verdict": None,
        "goal": GOAL,
        "from_goal": True,
        "error": "foreman exit 0 without summary",
    }


def _seed_failed_goal(dirs: dict, tmp_path: Path, *, with_pass: bool, error: str | None = None) -> None:
    plan = dirs["summaries"] / f"goal-{GOAL}-plan.md"
    plan.write_text(_plan_text(), encoding="utf-8")
    rows = [_pass_row(slice_name="s1")] if with_pass else []
    goal = _goal_with_rows(
        GOAL,
        rows,
        status="failed",
        cwd=str(tmp_path),
        slices=[SLICE_S1],
        plan_summary=str(plan),
        last_chain_state="failed",
    )
    goal["metrics"]["slices_completed"] = 0
    _write_json(dirs["goals"] / f"{GOAL}.json", goal)
    chain = _failed_chain()
    if error is not None:
        chain["error"] = error
        chain["rounds"][0]["impl_exit"] = 1
    _write_json(dirs["chains"] / f"{SLICE_S1}.json", chain)
    (dirs["packs"] / f"{SLICE_S2}.pack.md").write_text(
        f"---\nslice_id: {SLICE_S2}\n---\n# s2\n", encoding="utf-8"
    )


def _open_slice_recorder(calls: list[str]):
    def fake_open_slice(**kwargs):
        calls.append(kwargs["ticket"])
        mode = kwargs["prompt_mode"]
        summary = tb.SUMMARIES / f"{kwargs['summary_name']}.md"
        kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["log_path"].write_text("ok\n", encoding="utf-8")
        if mode == "foreman":
            summary.parent.mkdir(parents=True, exist_ok=True)
            summary.write_text(
                "```json\n"
                + json.dumps({"status": "done", "changed_files": [], "commit": "abc", "base": "def"})
                + "\n```\n",
                encoding="utf-8",
            )
        elif mode == "gate":
            summary.parent.mkdir(parents=True, exist_ok=True)
            summary.write_text(
                "```json\n" + json.dumps({"verdict": "PASS", "findings": []}) + "\n```\n",
                encoding="utf-8",
            )
        elif mode == "supervisor-close":
            summary.parent.mkdir(parents=True, exist_ok=True)
            summary.write_text(
                "```json\n" + json.dumps({"action": "goal_done", "goal_status": "done"}) + "\n```\n",
                encoding="utf-8",
            )
        return 0

    return fake_open_slice


def test_failed_goal_resume_runs_s2_not_s1(tmp_path: Path):
    ctx, dirs = _patched_dirs(tmp_path / "state")
    with ctx:
        _seed_failed_goal(dirs, tmp_path, with_pass=True)
        calls: list[str] = []
        inbox = tmp_path / "resume.json"
        _write_json(
            inbox,
            {"type": "goal-update", "goal": GOAL, "resume": True, "id": "resume-false-fail"},
        )
        with mock.patch.object(tb, "run_open_slice", side_effect=_open_slice_recorder(calls)), mock.patch.object(
            tb, "write_artifacts", side_effect=_fake_artifacts
        ), mock.patch.object(tb, "maybe_notify_hub", return_value={"sent": False, "rc": 0}):
            rc = tb.run_job(inbox)
        assert rc == 0, rc
        s1 = json.loads((dirs["chains"] / f"{SLICE_S1}.json").read_text(encoding="utf-8"))
        assert s1["state"] == "PASS", s1.get("state")
        assert len(s1["rounds"]) == 1, s1["rounds"]
        assert "salvaged on resume" in str(s1.get("salvage_note") or "")
        s2 = json.loads((dirs["chains"] / f"{SLICE_S2}.json").read_text(encoding="utf-8"))
        assert s2["state"] == "PASS", s2
        assert not any(SLICE_S1 in t and t.startswith("impl-trial-") for t in calls), calls
        assert any(t.startswith(f"impl-trial-{SLICE_S2}") for t in calls), calls
        goal = json.loads((dirs["goals"] / f"{GOAL}.json").read_text(encoding="utf-8"))
        assert goal["status"] == "done", goal.get("status")
        assert SLICE_S1 in goal["slices"] and SLICE_S2 in goal["slices"]


def test_true_failure_resume_does_not_pass(tmp_path: Path):
    ctx, dirs = _patched_dirs(tmp_path / "state")
    with ctx:
        _seed_failed_goal(
            dirs,
            tmp_path,
            with_pass=False,
            error="foreman exit 1 without summary",
        )
        calls: list[str] = []
        inbox = tmp_path / "resume-real.json"
        _write_json(
            inbox,
            {"type": "goal-update", "goal": GOAL, "resume": True, "id": "resume-real-fail"},
        )
        with mock.patch.object(tb, "run_open_slice", side_effect=_open_slice_recorder(calls)), mock.patch.object(
            tb, "write_artifacts", side_effect=_fake_artifacts
        ), mock.patch.object(tb, "maybe_notify_hub", return_value={"sent": False, "rc": 0}):
            rc = tb.run_job(inbox)
        assert rc != 0
        assert calls == []
        assert not (dirs["chains"] / f"{SLICE_S2}.json").exists()
        s1 = json.loads((dirs["chains"] / f"{SLICE_S1}.json").read_text(encoding="utf-8"))
        assert s1["state"] == "failed"
        goal = json.loads((dirs["goals"] / f"{GOAL}.json").read_text(encoding="utf-8"))
        assert goal["status"] == "failed", goal.get("status")
        assert goal.get("resume_rejected")


def test_shared_short_gate_file_is_not_pass_evidence(tmp_path: Path):
    """s1-gate-rev1.md is an unscoped name. It must not salvage another chain."""
    ctx, dirs = _patched_dirs(tmp_path / "state")
    with ctx:
        (dirs["summaries"] / "s1-gate-rev1.md").write_text(
            "```json\n" + json.dumps({"verdict": "PASS", "findings": []}) + "\n```\n",
            encoding="utf-8",
        )
        goal = _goal_with_rows(OTHER_GOAL, [])
        # Full chain id must not inherit an unscoped s1-gate-rev1.md.
        assert tb._slice_has_review_pass(goal, OTHER_S1)[0] is False
        assert tb._slice_has_review_pass(goal, SLICE_S1)[0] is False


# --------------------------------------------------------------------------
# Summary path + literal "slice" default (2026-10-05 entity-resolution)
# --------------------------------------------------------------------------

def test_infer_slice_from_impl_and_gate_tickets():
    assert (
        tb._infer_slice_from_ticket("impl-trial-engine-entity-resolution-v1-s1-r1")
        == "engine-entity-resolution-v1-s1"
    )
    assert (
        tb._infer_slice_from_ticket("gate-trial-engine-entity-resolution-v1-s1-rev1")
        == "engine-entity-resolution-v1-s1"
    )
    # Broken default must not be treated as a real slice id.
    assert tb._infer_slice_from_ticket("gate-trial-slice-rev1") is None
    assert tb._infer_slice_from_ticket("") is None


def test_resolve_ask_slice_id_prefers_ticket_over_literal_slice():
    ask = {
        "slice": None,
        "ticket": "impl-trial-engine-entity-resolution-v1-s1-r1",
        "from": {
            "ticket": "impl-trial-engine-entity-resolution-v1-s1-r1",
            "role": "impl",
            "slice": None,
        },
    }
    goal = {"goal": "engine-entity-resolution-v1", "current_slice": None}
    assert (
        tb._resolve_ask_slice_id(ask, goal)
        == "engine-entity-resolution-v1-s1"
    )
    # Explicit real slice wins.
    ask2 = dict(ask, slice="engine-entity-resolution-v1-s2")
    assert tb._resolve_ask_slice_id(ask2, goal) == "engine-entity-resolution-v1-s2"
    # current_slice on goal wins over ticket when ask.slice empty.
    goal2 = {
        "goal": "engine-entity-resolution-v1",
        "current_slice": "engine-entity-resolution-v1-s2",
    }
    ask3 = {"slice": None, "from": {}, "ticket": ""}
    assert tb._resolve_ask_slice_id(ask3, goal2) == "engine-entity-resolution-v1-s2"


def test_adopt_misplaced_summary_moves_from_glued_path(tmp_path: Path, monkeypatch):
    canonical = tmp_path / "canonical" / "summaries"
    canonical.mkdir(parents=True)
    expected = canonical / "engine-entity-resolution-v1-s1-impl-r1.md"
    glued = tmp_path / "home" / ".dsh-supervisor" / "thin-state" / "summaries"
    glued.mkdir(parents=True)
    wrong = glued / expected.name
    wrong.write_text("# real summary\n\n```json\n{\"status\": \"done\"}\n```\n", encoding="utf-8")

    monkeypatch.setattr(tb, "_misplaced_summary_roots", lambda: [glued])
    assert expected.is_file() is False
    assert tb._adopt_misplaced_summary(expected) is True
    assert expected.is_file()
    assert wrong.is_file() is False
    assert "status" in expected.read_text(encoding="utf-8")
    # Second call is a no-op success.
    assert tb._adopt_misplaced_summary(expected) is True


def test_exit0_misplaced_summary_is_adopted_before_missing_check(tmp_path: Path, monkeypatch):
    """Foreman wrote under glued path; broker must adopt then see the file."""
    ctx, dirs = _patched_dirs(tmp_path / "state")
    slice_id = SLICE_S1
    glued = tmp_path / "glued-summaries"
    glued.mkdir()
    body = (
        "# s1 impl\n\n```json\n"
        + json.dumps(
            {
                "status": "done",
                "changed_files": ["a.py"],
                "branch": "main",
                "commit": "abc",
                "base": "def",
                "questions": [],
                "notes": "ok",
            },
            ensure_ascii=False,
        )
        + "\n```\n"
    )
    with ctx:
        (dirs["packs"] / f"{slice_id}.pack.md").write_text(
            f"---\nslice_id: {slice_id}\n---\n# s1\n", encoding="utf-8"
        )
        goal = _goal_with_rows(GOAL, [], cwd=str(tmp_path))
        _write_json(dirs["goals"] / f"{GOAL}.json", goal)
        monkeypatch.setattr(tb, "_misplaced_summary_roots", lambda: [glued])

        def fake_open_slice(**kwargs):
            kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
            kwargs["log_path"].write_text("ok\n", encoding="utf-8")
            # Write ONLY to the wrong dir — canonical still empty at exit.
            name = f"{kwargs['summary_name']}.md"
            (glued / name).write_text(body, encoding="utf-8")
            return 0

        # Avoid running a real second gate: salvage path is not taken because
        # after adopt the summary exists; patch gate open-slice to PASS quickly.
        gate_calls = {"n": 0}

        def fake_open_slice_all(**kwargs):
            role = kwargs.get("role")
            kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
            kwargs["log_path"].write_text(f"role={role}\n", encoding="utf-8")
            name = f"{kwargs['summary_name']}.md"
            if role == "impl":
                (glued / name).write_text(body, encoding="utf-8")
            else:
                gate_calls["n"] += 1
                dest = dirs["summaries"] / name
                dest.write_text(
                    "gate\n\n```json\n"
                    + json.dumps(
                        {"verdict": "PASS", "unmet_acceptance": [], "findings": []},
                        ensure_ascii=False,
                    )
                    + "\n```\n",
                    encoding="utf-8",
                )
            return 0

        job = {
            "id": "unit-adopt-chain",
            "type": "chain",
            "slice": slice_id,
            "pack": f"{slice_id}.pack.md",
            "acceptance": ["create_app wires extractor"],
            "profile": "acp",
            "gate_profile": "acp",
            "cwd": str(tmp_path),
            "max_rounds": 1,
            "goal": GOAL,
            "from_goal": True,
            "defer_supervisor_close": True,
            "notify_dry_run": True,
        }
        inbox = tmp_path / "job.json"
        _write_json(inbox, job)
        with mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice_all), mock.patch.object(
            tb, "write_artifacts", side_effect=_fake_artifacts
        ), mock.patch.object(tb, "maybe_notify_hub", return_value={"sent": False, "rc": 0}):
            rc = tb.run_job(inbox)
        chain = json.loads((dirs["chains"] / f"{slice_id}.json").read_text(encoding="utf-8"))
        canonical = dirs["summaries"] / f"{slice_id}-impl-r1.md"
        assert canonical.is_file(), "summary should have been adopted to canonical"
        assert (glued / f"{slice_id}-impl-r1.md").is_file() is False
        assert rc == 0, (rc, chain)
        assert chain["state"] == "PASS", chain
        assert gate_calls["n"] >= 1


def test_review_pass_via_ask_archive_ties_impl_ticket(tmp_path: Path, monkeypatch):
    mailbox = tmp_path / "mailbox"
    arch = mailbox / "archive"
    answers = mailbox / "answers"
    arch.mkdir(parents=True)
    answers.mkdir(parents=True)
    ask_id = "submit-for-review-unit-archive-1"
    pend = {
        "ask_id": ask_id,
        "kind": "submit_for_review",
        "slice": None,
        "from": {
            "ticket": f"impl-trial-{SLICE_S1}-r1",
            "role": "impl",
            "slice": None,
        },
        "ticket": f"impl-trial-{SLICE_S1}-r1",
    }
    (arch / f"{ask_id}.pending.json").write_text(
        json.dumps(pend, ensure_ascii=False), encoding="utf-8"
    )
    (answers / f"{ask_id}.json").write_text(
        json.dumps({"ask_id": ask_id, "verdict": "PASS"}, ensure_ascii=False),
        encoding="utf-8",
    )
    monkeypatch.setattr(tb, "MAILBOX", mailbox)
    ok, reason = tb._review_pass_via_ask_archive(SLICE_S1)
    assert ok, reason
    assert ask_id in reason
    # Different slice must not match.
    assert tb._review_pass_via_ask_archive(SLICE_S2)[0] is False


def test_slice_has_review_pass_recovers_literal_slice_via_archive(tmp_path: Path, monkeypatch):
    """Metrics row slice='slice' alone must not match; archive recovery does."""
    mailbox = tmp_path / "mailbox"
    arch = mailbox / "archive"
    answers = mailbox / "answers"
    arch.mkdir(parents=True)
    answers.mkdir(parents=True)
    ask_id = "submit-for-review-unit-literal-1"
    (arch / f"{ask_id}.pending.json").write_text(
        json.dumps(
            {
                "ask_id": ask_id,
                "kind": "submit_for_review",
                "slice": None,
                "from": {"ticket": f"impl-trial-{SLICE_S1}-r1", "role": "impl"},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    (answers / f"{ask_id}.json").write_text(
        json.dumps({"ask_id": ask_id, "verdict": "PASS"}, ensure_ascii=False),
        encoding="utf-8",
    )
    monkeypatch.setattr(tb, "MAILBOX", mailbox)
    # Metrics used the broken default — must NOT match via _slice_names_same.
    goal = _goal_with_rows(GOAL, [_pass_row(slice_name="slice")])
    ok, reason = tb._slice_has_review_pass(goal, SLICE_S1)
    assert ok, reason
    assert "ask archive" in reason
    # Other goal slice still false.
    assert tb._slice_has_review_pass(goal, OTHER_S1)[0] is False
