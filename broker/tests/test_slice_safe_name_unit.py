"""T2 — a slice *title* is never a path, and a failed gate pack spends no round.

The incident: the orphan ask ``t3c-gate-1`` carried a slice *title* containing
``/`` (``status/report 从 broker 心跳显示并发（slots/queued）``). The broker used
it verbatim as a slice id, so ``build_gate_pack`` tried to write
``packs/status/report ...-gate-rev1.pack.md`` into a non-existent sub-directory
and raised ``FileNotFoundError`` — which the old ``except ValueError`` did not
catch. The ask stayed pending, the watcher retried every 2 s and bumped
``goal["review_seq"]`` on each retry (r30 → r37; the next real ticket became
r38).

Covered here:
  1. ``_safe_slice_id`` — path separators/control chars/whitespace neutralised,
     runs collapsed, ``-._`` trimmed, 80-char cap, empty → ``slice``,
     idempotent, ordinary ids untouched;
  2. the real ask shape drives ``_handle_submit_for_review``: the pack lands
     *directly* under ``PACKS`` and the gate ticket contains no ``/``;
  3. a pack build raising ``OSError`` changes no ``review_seq``, writes a
     structured ``ok=False`` HOLD answer and archives the pending ask — and a
     second pass still does not bump;
  4. the happy path still spends exactly one round (``review_seq`` + 1);
  5. ``_review_key`` normalises ``None`` and ``""`` to the same goal id.
"""
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

spec = importlib.util.spec_from_file_location("trial_broker_slice_safe_name", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)


# The exact orphan-ask title from the incident.
TITLE = "status/report 从 broker 心跳显示并发（slots/queued）"
GOAL_ID = "t3c-status-report-heartbeat-concurrency"


# --------------------------------------------------------------------------
# fixtures (same shape as test_submit_precheck_unit.py)
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


def _goal(*, review_seq: int = 4) -> dict:
    return {
        "goal": GOAL_ID,
        "status": "running",
        "slice": TITLE,
        "review_seq": review_seq,
        "current_slice": TITLE,
        "current_pack": f"{GOAL_ID}.pack.md",
        "current_acceptance": ["do the thing"],
    }


def _ask(ask_id: str) -> dict:
    """Today's real orphan-ask shape: title in slice/from.slice, empty commit."""
    return {
        "ask_id": ask_id,
        "kind": "submit_for_review",
        "slice": TITLE,
        "from": {"slice": TITLE},
        "summary": "did the thing",
        "changed_files": ["broker/trial-broker.py"],
        "base": "",
        "commit": "",
        "usage_prompt": 100,
    }


def _pending(dirs: dict, ask_id: str) -> Path:
    p = dirs["mailbox"] / "pending" / f"{ask_id}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("{}", encoding="utf-8")
    return p


def _answer(dirs: dict, ask_id: str) -> dict:
    return json.loads((dirs["mailbox"] / "answers" / f"{ask_id}.json").read_text(encoding="utf-8"))


def _drive(dirs: dict, *, ask: dict, cwd: Path, goal: dict) -> list:
    """Call the real submit handler with the gate spawn mocked; return spawn kwargs."""
    calls: list = []

    def fake_open_slice(**kwargs):
        calls.append(kwargs)
        kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["log_path"].write_text("gate log\n", encoding="utf-8")
        return 0

    with mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), \
            mock.patch.object(tb, "write_artifacts", side_effect=_fake_artifacts), \
            mock.patch.object(tb, "_parse_gate_verdict", return_value=_verdict_block("PASS")):
        tb._handle_submit_for_review(
            ask, _pending(dirs, ask["ask_id"]), default_profile="acp-lite", cwd=str(cwd), goal=goal,
        )
    return calls


# --------------------------------------------------------------------------
# 1) _safe_slice_id
# --------------------------------------------------------------------------
def test_safe_slice_id_neutralises_path_separators():
    got = tb._safe_slice_id(TITLE)
    for bad in ("/", "\\", ":", "\x00"):
        assert bad not in got
    assert got == "status-report-从-broker-心跳显示并发（slots-queued）"
    assert "从" in got and "心跳显示并发" in got  # unicode survives


def test_safe_slice_id_is_idempotent_and_keeps_ordinary_ids():
    assert tb._safe_slice_id(tb._safe_slice_id(TITLE)) == tb._safe_slice_id(TITLE)
    for good in ("s1", "hub-logic-gate-g1-s0-adr-v1-r2-s2", "engine-proposal-persistence-s1"):
        assert tb._safe_slice_id(good) == good


def test_safe_slice_id_empty_and_truncation():
    assert tb._safe_slice_id("") == "slice"
    assert tb._safe_slice_id("   ") == "slice"
    assert tb._safe_slice_id(None) == "slice"
    assert tb._safe_slice_id("///") == "slice"
    long_raw = "x" * 500
    assert len(tb._safe_slice_id(long_raw)) == 80
    # a cut that would leave a trailing separator is trimmed, so it stays idempotent
    cut = tb._safe_slice_id("a" * 79 + "-b")
    assert tb._safe_slice_id(cut) == cut


def test_safe_slice_id_collapses_runs_and_trims_edges():
    assert tb._safe_slice_id("a//b :: c") == "a-b-c"
    assert tb._safe_slice_id("..a..") == "a"
    assert tb._safe_slice_id("--a--") == "a"


# --------------------------------------------------------------------------
# 2) the real ask shape: pack lands directly under PACKS, ticket has no "/"
#    (and 4) the happy path still spends exactly one round)
# --------------------------------------------------------------------------
def test_handler_writes_pack_directly_under_packs(tmp_path: Path):
    with _patched_dirs(tmp_path / "state") as dirs:
        goal = _goal(review_seq=4)
        calls = _drive(dirs, ask=_ask("t3c-gate-1"), cwd=tmp_path, goal=goal)

        packs = sorted(dirs["packs"].glob("*gate*"))
        assert len(packs) == 1
        assert packs[0].parent == dirs["packs"]           # no sub-directory
        assert "/" not in packs[0].name
        assert all(p.is_file() for p in dirs["packs"].iterdir())

        assert len(calls) == 1 and calls[0]["role"] == "gate"
        assert "/" not in calls[0]["ticket"]
        assert "/" not in calls[0]["pack_name"]
        assert calls[0]["ticket"] == "gate-trial-status-report-从-broker-心跳显示并发（slots-queued）-rev5"

        # 4) one round spent, answer written as usual
        assert goal["review_seq"] == 5
        ans = _answer(dirs, "t3c-gate-1")
        assert ans["verdict"] == "PASS"
        assert "precheck" not in ans


# --------------------------------------------------------------------------
# 3) pack build fails → no round, structured HOLD, pending archived, no repeat
# --------------------------------------------------------------------------
def test_pack_build_oserror_spends_no_round_and_archives(tmp_path: Path):
    with _patched_dirs(tmp_path / "state") as dirs:
        goal = _goal(review_seq=4)
        ask = _ask("t3c-gate-1")
        archive = dirs["mailbox"] / "archive" / "t3c-gate-1.pending.json"

        with mock.patch.object(tb, "build_gate_pack", side_effect=FileNotFoundError("no such dir")):
            # first pass
            tb._handle_submit_for_review(
                ask, _pending(dirs, "t3c-gate-1"),
                default_profile="acp-lite", cwd=str(tmp_path), goal=goal,
            )
            assert goal["review_seq"] == 4              # unchanged
            ans = _answer(dirs, "t3c-gate-1")
            assert ans["ok"] is False
            assert ans["verdict"] == "HOLD"
            assert ans["status"] == "done"
            assert "no such dir" in ans["error"]
            assert archive.is_file()                     # archived → watcher stops
            assert (dirs["goals"] / f"{GOAL_ID}.json").is_file() is False

            # second pass (the watcher's 2 s retry) — still no bump
            tb._handle_submit_for_review(
                ask, archive, default_profile="acp-lite", cwd=str(tmp_path), goal=goal,
            )
            assert goal["review_seq"] == 4
            assert _answer(dirs, "t3c-gate-1")["verdict"] == "HOLD"

        assert list(dirs["packs"].glob("*gate*")) == []


def test_pack_build_oserror_does_not_spawn_gate(tmp_path: Path):
    with _patched_dirs(tmp_path / "state") as dirs:
        goal = _goal(review_seq=7)
        with mock.patch.object(tb, "build_gate_pack", side_effect=OSError("disk full")), \
                mock.patch.object(tb, "run_open_slice") as spawn:
            tb._handle_submit_for_review(
                _ask("t3c-gate-1"), _pending(dirs, "t3c-gate-1"),
                default_profile="acp-lite", cwd=str(tmp_path), goal=goal,
            )
        spawn.assert_not_called()
        assert goal["review_seq"] == 7


# --------------------------------------------------------------------------
# 5) _review_key: None and "" are the same (unknown) goal
# --------------------------------------------------------------------------
def test_review_key_normalises_missing_goal_id(tmp_path: Path):
    assert tb._review_key(None, "s1", tmp_path, "fp") == tb._review_key("", "s1", tmp_path, "fp")
