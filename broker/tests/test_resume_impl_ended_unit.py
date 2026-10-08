"""T3 — a resumed chain round must not treat a bare impl summary as "finished".

The incident: the impl foreman writes its summary *before* ``submit_for_review``
(94c6621). ``run_chain_rounds``' resume branch saw ``impl_summary.is_file()`` and
jumped straight into the gate even when the impl ticket was still running (or had
died mid-flight), so the gate reviewed a half-written round.

Covered here:
  1. ``_impl_ticket_state`` → ended / running / dead, and an *old* run's
     ``=== exit=`` line before the last ``=== open-slice `` header proves nothing;
  2. resume with summary + no exit + no live process → summary set aside with an
     ``.unfinished-`` suffix and the impl ticket re-spawned before any gate;
  3. resume with summary + closing exit line → reused, impl not re-opened;
  4. resume while running, then ended → reused, impl not re-opened.

Run: python3 -m pytest broker/tests/test_resume_impl_ended_unit.py -q
"""

from __future__ import annotations

import importlib.util
import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_resume_ended", ROOT / "trial-broker.py")
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


def _chain_job(slice_id: str, cwd: Path) -> dict:
    return {
        "id": f"unit-t3-{slice_id}", "type": "chain", "slice": slice_id,
        "pack": f"{slice_id}.pack.md", "acceptance": ["do the thing"],
        "profile": "acp-lite", "gate_profile": "acp-lite", "cwd": str(cwd),
        "max_rounds": 1, "goal": None,
    }


def _write_impl_log(dirs: dict, ticket: str, *, text: str) -> Path:
    log = dirs["artifacts"] / f"{ticket}.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    log.write_text(text, encoding="utf-8")
    return log


# --------------------------------------------------------------------------
# 1) _impl_ticket_state
# --------------------------------------------------------------------------
def test_impl_ticket_state_exit_after_last_header_is_ended(tmp_path):
    with _patched_dirs(tmp_path) as dirs:
        log = _write_impl_log(
            dirs, "impl-trial-s1-r1",
            text="=== open-slice 20260101-000000 ===\nwork\n=== exit=0 ===\n",
        )
        with mock.patch.object(tb, "_live_ticket_pids", return_value=[]) as live:
            assert tb._impl_ticket_state("impl-trial-s1-r1", log) == "ended"
        live.assert_not_called()


def test_impl_ticket_state_header_only_with_live_pid_is_running(tmp_path):
    with _patched_dirs(tmp_path) as dirs:
        log = _write_impl_log(
            dirs, "impl-trial-s1-r1",
            text="=== open-slice 20260101-000000 ===\nstill working\n",
        )
        with mock.patch.object(tb, "_live_ticket_pids", return_value=[123]):
            assert tb._impl_ticket_state("impl-trial-s1-r1", log) == "running"


def test_impl_ticket_state_without_process_is_dead(tmp_path):
    with _patched_dirs(tmp_path) as dirs:
        log = _write_impl_log(
            dirs, "impl-trial-s1-r1",
            text="=== open-slice 20260101-000000 ===\nmid-flight, no exit\n",
        )
        with mock.patch.object(tb, "_live_ticket_pids", return_value=[]):
            assert tb._impl_ticket_state("impl-trial-s1-r1", log) == "dead"


def test_impl_ticket_state_old_exit_before_last_header_is_dead(tmp_path):
    """An earlier run's exit line must not vouch for the current run."""
    with _patched_dirs(tmp_path) as dirs:
        log = _write_impl_log(
            dirs, "impl-trial-s1-r1",
            text=("=== open-slice 20260101-000000 ===\nold\n=== exit=0 ===\n"
                  "=== open-slice 20260101-010000 ===\nnew run, no exit yet\n"),
        )
        with mock.patch.object(tb, "_live_ticket_pids", return_value=[]):
            assert tb._impl_ticket_state("impl-trial-s1-r1", log) == "dead"


def test_live_ticket_pids_matches_ticket_exactly():
    ps_out = (
        "  123 /usr/bin/bash /repo/open-slice.sh --ticket impl-trial-s1-r1 --x\n"
        "  124 /usr/bin/bash /repo/open-slice.sh --ticket impl-trial-s1-r10 --x\n"
        "  125 /usr/bin/python3 /repo/dsh-acp-ask.py --ticket=impl-trial-s1-r1\n"
        "  126 /usr/bin/bash /repo/other.sh --ticket impl-trial-s1-r1\n"
    )
    proc = mock.Mock(stdout=ps_out)
    with mock.patch.object(tb.subprocess, "run", return_value=proc):
        assert tb._live_ticket_pids("impl-trial-s1-r1") == [123, 125]


# --------------------------------------------------------------------------
# 2-4) resume behaviour of run_chain_rounds
# --------------------------------------------------------------------------
def _drive_chain(dirs: dict, root: Path, slice_id: str) -> tuple[int, dict, list]:
    """Run run_chain_rounds (resume) with both foreman and gate spawns faked."""
    pack = dirs["packs"] / f"{slice_id}.pack.md"
    pack.write_text(f"---\nslice_id: {slice_id}\n---\n# pack\n", encoding="utf-8")
    job = _chain_job(slice_id, root)
    chain = tb._new_chain_state(job)
    dest = dirs["processing"] / f"{slice_id}.json"
    calls: list = []

    def fake_open_slice(**kwargs):
        calls.append(kwargs)
        kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["log_path"].write_text(
            f"=== open-slice 20260101-020000 ===\nrun\n=== exit=0 ===\n",
            encoding="utf-8",
        )
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
        rc = tb.run_chain_rounds(job, chain, dest, start_round=1,
                                 start_pack=pack.name, resume=True)
    return rc, chain, calls


def _seed_round1_summary(dirs: dict, slice_id: str) -> Path:
    summary = dirs["summaries"] / f"{slice_id}-impl-r1.md"
    _write_summary(summary, {
        "status": "done", "changed_files": ["a.py"], "base": "b",
        "commit": "c", "questions": [], "notes": "ok",
    })
    return summary


def test_resume_dead_impl_sets_summary_aside_and_reopens(tmp_path):
    slice_id = "res-s1"
    with _patched_dirs(tmp_path) as dirs:
        summary = _seed_round1_summary(dirs, slice_id)
        _write_impl_log(dirs, f"impl-trial-{slice_id}-r1",
                        text="=== open-slice 20260101-000000 ===\nmid-flight\n")
        with mock.patch.object(tb, "_live_ticket_pids", return_value=[]):
            rc, chain, calls = _drive_chain(dirs, tmp_path, slice_id)

        assert rc == 0
        # impl re-opened; the gate never ran before that spawn.
        impl_calls = [c for c in calls if c.get("role") == "impl"]
        assert [c["ticket"] for c in impl_calls] == [f"impl-trial-{slice_id}-r1"]
        assert calls[0].get("role") == "impl"
        # the stale summary was set aside, the fresh one is canonical
        assert summary.is_file()
        aside = list(dirs["summaries"].glob(f"{slice_id}-impl-r1.md.unfinished-*"))
        assert len(aside) == 1
        assert chain["rounds"][0]["impl_ticket"] == f"impl-trial-{slice_id}-r1"


def test_resume_ended_impl_is_reused(tmp_path):
    slice_id = "res-s2"
    with _patched_dirs(tmp_path) as dirs:
        summary = _seed_round1_summary(dirs, slice_id)
        _write_impl_log(
            dirs, f"impl-trial-{slice_id}-r1",
            text="=== open-slice 20260101-000000 ===\nwork\n=== exit=0 ===\n",
        )
        with mock.patch.object(tb, "_live_ticket_pids", return_value=[]):
            rc, chain, calls = _drive_chain(dirs, tmp_path, slice_id)

        assert rc == 0
        assert [c for c in calls if c.get("role") == "impl"] == []
        assert summary.is_file()
        assert list(dirs["summaries"].glob(f"{slice_id}-impl-r1.md.unfinished-*")) == []
        assert chain["rounds"][0]["impl_ticket"] == f"impl-trial-{slice_id}-r1"


def test_resume_running_impl_waits_then_reuses(tmp_path):
    slice_id = "res-s3"
    with _patched_dirs(tmp_path) as dirs:
        summary = _seed_round1_summary(dirs, slice_id)
        _write_impl_log(dirs, f"impl-trial-{slice_id}-r1",
                        text="=== open-slice 20260101-000000 ===\nwork\n")
        with mock.patch.object(tb, "_impl_ticket_state",
                               side_effect=["running", "ended"]) as state, \
                mock.patch.object(tb, "RESUME_IMPL_WAIT_POLL_SEC", 0):
            rc, chain, calls = _drive_chain(dirs, tmp_path, slice_id)

        assert rc == 0
        assert state.call_count >= 2
        assert [c for c in calls if c.get("role") == "impl"] == []
        assert summary.is_file()
        assert list(dirs["summaries"].glob(f"{slice_id}-impl-r1.md.unfinished-*")) == []
