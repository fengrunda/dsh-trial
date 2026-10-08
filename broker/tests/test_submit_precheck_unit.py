"""F2 — ``submit_for_review`` must not open a gate for an unwritten summary.

The incident: the impl agent called ``submit_for_review`` *before* writing its
impl summary, so the gate reviewed a submission with no summary at all and the
round was counted anyway.

Covered here (process completeness only — content is never judged):
  1. summary file missing (path from ``goal.current_impl_summary``) → rejected
     with ``ok=False`` / ``precheck=summary_missing`` / ``rework_mode=inplace``,
     no gate spawn, no ``review_seq`` bump, no gate pack, ask archived;
  2. summary present but whitespace only (path from ``ask.summary_path``) → same;
  3. path inferred from ``from.ticket`` trailing ``-r<N>`` → same;
  4. summary present and non-empty → the gate opens as usual (one spawn,
     ``review_seq`` + 1);
  5. no expected path resolvable (no summary_path, no goal field, no ``-r<N>``)
     → never intercepted, gate opens as usual;
  6. ``run_chain_rounds`` publishes ``current_impl_summary`` onto the goal when
     it starts an impl ticket, and ``run_ticket_job`` does the same.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_submit_precheck", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)


# --------------------------------------------------------------------------
# fixtures
# --------------------------------------------------------------------------
@contextmanager
def _patched_dirs(root: Path):
    names = ("goals", "chains", "packs", "summaries", "artifacts", "processing",
             "outbox", "failed", "inbox", "mailbox")
    dirs = {n: root / n for n in names}
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    with mock.patch.multiple(
        tb,
        GOALS=dirs["goals"], CHAINS=dirs["chains"], PACKS=dirs["packs"],
        SUMMARIES=dirs["summaries"], ARTIFACT_ROOT=dirs["artifacts"],
        PROCESSING=dirs["processing"], OUTBOX=dirs["outbox"], FAILED=dirs["failed"],
        INBOX=dirs["inbox"], MAILBOX=dirs["mailbox"],
        maybe_offload_gc=lambda **k: None,
    ), mock.patch.object(tb.T, "MAILBOX", dirs["mailbox"]):
        yield dirs


def _write_summary(path: Path, block: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "prose\n\n```json\n" + json.dumps(block, ensure_ascii=False) + "\n```\n",
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


def _verdict_block(verdict: str) -> dict:
    return {"block": {"verdict": verdict, "findings": [], "unmet_acceptance": []}, "text": ""}


def _ask(ask_id: str, slice_id: str, *, ticket: str | None = None,
         summary_path: str | None = None) -> dict:
    ask = {
        "ask_id": ask_id,
        "slice": slice_id,
        "summary": "did the thing",
        "changed_files": ["a.py"],
        "base": "",
        "commit": "",
        "usage_prompt": 100,
    }
    if ticket:
        ask["from"] = {"ticket": ticket}
    if summary_path:
        ask["summary_path"] = summary_path
    return ask


def _goal(slice_id: str, *, review_seq: int = 0, impl_summary: str | None = None) -> dict:
    goal = {
        "goal": f"unit-f2-{slice_id}-goal",
        "status": "running",
        "slice": slice_id,
        "review_seq": review_seq,
        "current_slice": slice_id,
        "current_pack": f"{slice_id}.pack.md",
        "current_acceptance": ["do the thing"],
    }
    if impl_summary is not None:
        goal["current_impl_summary"] = impl_summary
    return goal


def _drive_review(dirs: dict, *, ask: dict, cwd: Path, goal: dict | None = None) -> list:
    """Call the real submit handler with the gate spawn mocked; return spawn kwargs."""
    calls: list = []

    def fake_open_slice(**kwargs):
        calls.append(kwargs)
        kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["log_path"].write_text("gate log\n", encoding="utf-8")
        return 0

    ask_id = ask["ask_id"]
    pending = dirs["mailbox"] / "pending" / f"{ask_id}.json"
    pending.parent.mkdir(parents=True, exist_ok=True)
    pending.write_text("{}", encoding="utf-8")
    with mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), \
            mock.patch.object(tb, "write_artifacts", side_effect=_fake_artifacts), \
            mock.patch.object(tb, "_parse_gate_verdict", return_value=_verdict_block("PASS")):
        tb._handle_submit_for_review(
            ask, pending, default_profile="acp-lite", cwd=str(cwd), goal=goal,
        )
    return calls


def _answer(dirs: dict, ask_id: str) -> dict:
    return json.loads((dirs["mailbox"] / "answers" / f"{ask_id}.json").read_text(encoding="utf-8"))


def _gate_packs(dirs: dict) -> list:
    return sorted(p.name for p in dirs["packs"].glob("*gate*"))


# --------------------------------------------------------------------------
# 1) missing summary (goal-published path) → rejected, no round, no gate
# --------------------------------------------------------------------------
def test_missing_summary_rejected_before_gate(tmp_path: Path):
    slice_id = "unit-f2-missing"
    with _patched_dirs(tmp_path / "state") as dirs:
        expected = dirs["summaries"] / f"{slice_id}-impl-r1.md"
        goal = _goal(slice_id, review_seq=3, impl_summary=str(expected))
        calls = _drive_review(
            dirs, ask=_ask("f2-missing", slice_id, ticket=f"impl-trial-{slice_id}-r1"),
            cwd=tmp_path, goal=goal,
        )

        ans = _answer(dirs, "f2-missing")
        assert ans["ok"] is False
        assert ans["precheck"] == "summary_missing"
        assert ans["rework_mode"] == "inplace"
        assert ans["mode"] == "inplace"
        assert ans["verdict"] == "HOLD"
        assert ans["status"] == "done"
        assert ans["summary_path"] == str(expected)
        assert str(expected) in ans["instruction"]
        assert "不计轮次" in ans["instruction"]
        # no gate was spawned and no round was spent
        assert calls == []
        assert goal["review_seq"] == 3
        assert _gate_packs(dirs) == []
        # the goal was never rewritten by the rejection
        assert not (dirs["goals"] / f"{goal['goal']}.json").is_file()
        # ask archived exactly like the normal path
        assert (dirs["mailbox"] / "archive" / "f2-missing.pending.json").is_file()
        assert not (dirs["mailbox"] / "pending" / "f2-missing.json").is_file()


# --------------------------------------------------------------------------
# 2) whitespace-only summary (ask.summary_path) → rejected
# --------------------------------------------------------------------------
def test_whitespace_only_summary_rejected(tmp_path: Path):
    slice_id = "unit-f2-blank"
    with _patched_dirs(tmp_path / "state") as dirs:
        expected = dirs["summaries"] / f"{slice_id}-impl-r1.md"
        expected.parent.mkdir(parents=True, exist_ok=True)
        expected.write_text("   \n\n\t\n", encoding="utf-8")
        goal = _goal(slice_id, review_seq=1)
        calls = _drive_review(
            dirs, ask=_ask("f2-blank", slice_id, summary_path=str(expected)),
            cwd=tmp_path, goal=goal,
        )

        ans = _answer(dirs, "f2-blank")
        assert ans["ok"] is False and ans["precheck"] == "summary_missing"
        assert ans["rework_mode"] == "inplace"
        assert calls == []
        assert goal["review_seq"] == 1
        assert _gate_packs(dirs) == []


# --------------------------------------------------------------------------
# 3) path inferred from from.ticket trailing -r<N> → rejected
# --------------------------------------------------------------------------
def test_missing_summary_inferred_from_ticket_round(tmp_path: Path):
    slice_id = "unit-f2-infer"
    with _patched_dirs(tmp_path / "state") as dirs:
        goal = _goal(slice_id, review_seq=2)
        calls = _drive_review(
            dirs, ask=_ask("f2-infer", slice_id, ticket=f"impl-trial-{slice_id}-r2"),
            cwd=tmp_path, goal=goal,
        )

        ans = _answer(dirs, "f2-infer")
        assert ans["ok"] is False and ans["precheck"] == "summary_missing"
        assert ans["summary_path"] == str(dirs["summaries"] / f"{slice_id}-impl-r2.md")
        assert str(dirs["summaries"] / f"{slice_id}-impl-r2.md") in ans["instruction"]
        assert calls == []
        assert goal["review_seq"] == 2


# --------------------------------------------------------------------------
# 4) summary present and non-empty → gate opens as usual
# --------------------------------------------------------------------------
def test_present_summary_opens_gate(tmp_path: Path):
    slice_id = "unit-f2-ok"
    with _patched_dirs(tmp_path / "state") as dirs:
        expected = dirs["summaries"] / f"{slice_id}-impl-r1.md"
        _write_summary(expected, {"status": "done", "changed_files": ["a.py"]})
        goal = _goal(slice_id, review_seq=1, impl_summary=str(expected))
        calls = _drive_review(
            dirs, ask=_ask("f2-ok", slice_id, ticket=f"impl-trial-{slice_id}-r1"),
            cwd=tmp_path, goal=goal,
        )

        assert len(calls) == 1 and calls[0]["role"] == "gate"
        assert goal["review_seq"] == 2
        ans = _answer(dirs, "f2-ok")
        assert ans["verdict"] == "PASS"
        assert "precheck" not in ans


# --------------------------------------------------------------------------
# 5) no resolvable expected path → never intercepted
# --------------------------------------------------------------------------
def test_unknown_expected_path_is_not_rejected(tmp_path: Path):
    slice_id = "unit-f2-unknown"
    with _patched_dirs(tmp_path / "state") as dirs:
        goal = _goal(slice_id, review_seq=0)
        calls = _drive_review(dirs, ask=_ask("f2-unknown", slice_id), cwd=tmp_path, goal=goal)

        assert len(calls) == 1 and calls[0]["role"] == "gate"
        assert goal["review_seq"] == 1
        assert _answer(dirs, "f2-unknown")["verdict"] == "PASS"


# --------------------------------------------------------------------------
# 6) the expected summary path is published onto the goal when impl starts
# --------------------------------------------------------------------------
def _chain_job(slice_id: str, cwd: Path, goal_id: str) -> dict:
    return {
        "id": f"unit-f2-{slice_id}", "type": "chain", "slice": slice_id,
        "pack": f"{slice_id}.pack.md", "acceptance": ["do the thing"],
        "profile": "acp-lite", "gate_profile": "acp-lite", "cwd": str(cwd),
        "max_rounds": 1, "goal": goal_id,
    }


def test_chain_rounds_publishes_current_impl_summary(tmp_path: Path):
    slice_id = "unit-f2-chain"
    with _patched_dirs(tmp_path / "state") as dirs:
        pack = dirs["packs"] / f"{slice_id}.pack.md"
        pack.write_text(f"---\nslice_id: {slice_id}\n---\n# pack\n", encoding="utf-8")
        goal_id = f"unit-f2-{slice_id}-goal"
        goal = _goal(slice_id, review_seq=0)
        goal["goal"] = goal_id
        (dirs["goals"] / f"{goal_id}.json").write_text(json.dumps(goal), encoding="utf-8")

        job = _chain_job(slice_id, tmp_path, goal_id)
        chain = tb._new_chain_state(job)
        dest = dirs["processing"] / f"{slice_id}.json"

        def fake_open_slice(**kwargs):
            kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
            kwargs["log_path"].write_text("unit log\n", encoding="utf-8")
            summary = dirs["summaries"] / f"{kwargs['summary_name']}.md"
            if kwargs.get("role") == "gate":
                _write_summary(summary, {"verdict": "PASS", "findings": [], "unmet_acceptance": []})
            else:
                _write_summary(summary, {
                    "status": "done", "changed_files": ["a.py"], "base": "b",
                    "commit": "c", "questions": [], "notes": "ok",
                })
            return 0

        with mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), \
                mock.patch.object(tb, "write_artifacts", side_effect=_fake_artifacts), \
                mock.patch.object(tb, "maybe_notify_hub", return_value={"sent": True}):
            tb.run_chain_rounds(job, chain, dest, start_round=1, start_pack=pack.name)

        written = json.loads((dirs["goals"] / f"{goal_id}.json").read_text(encoding="utf-8"))
        assert written["current_impl_summary"] == str(dirs["summaries"] / f"{slice_id}-impl-r1.md")


def test_ticket_job_publishes_current_impl_summary(tmp_path: Path):
    """A plain impl ticket job publishes the same field before it spawns."""
    slice_id = "unit-f2-ticket"
    with _patched_dirs(tmp_path / "state") as dirs:
        pack_name = f"{slice_id}.pack.md"
        (dirs["packs"] / pack_name).write_text(
            f"---\nslice_id: {slice_id}\n---\n# pack\n", encoding="utf-8")
        goal_id = f"unit-f2-{slice_id}-goal"
        goal = _goal(slice_id, review_seq=0)
        goal["goal"] = goal_id
        (dirs["goals"] / f"{goal_id}.json").write_text(json.dumps(goal), encoding="utf-8")

        job_path = dirs["inbox"] / "ticket.json"
        job = {
            "ticket": f"impl-trial-{slice_id}-r1", "pack": pack_name, "slice": slice_id,
            "cwd": str(tmp_path), "profile": "acp-lite", "goal": goal_id, "max_rounds": 1,
        }
        job_path.write_text(json.dumps(job), encoding="utf-8")

        snapshots: list = []
        real_write_goal = tb._write_goal

        def spy_write_goal(state):
            snapshots.append(dict(state))
            return real_write_goal(state)

        class _Stop(Exception):
            pass

        def fake_open_slice(**kwargs):
            raise _Stop

        with mock.patch.object(tb, "_write_goal", side_effect=spy_write_goal), \
                mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice):
            with pytest.raises(_Stop):
                tb.run_ticket_job(job_path, job)

        # the published path must be exactly the summary the ticket was told to
        # write (base_summary comes from the pack name on round 1)
        assert any(
            s.get("current_impl_summary") == str(dirs["summaries"] / f"{slice_id}.md")
            for s in snapshots
        ), snapshots
