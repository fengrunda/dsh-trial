#!/usr/bin/env python3
"""T2b unit tests: broker restart → resume of non-terminal Goals.

Every ticket is mocked: ``run_open_slice`` (impl / gate) and
``run_supervisor_ticket`` (plan / close) are replaced by recorders that only
write the summary file they would have produced, so nothing real is spawned and
no ticket is ever paid for. State lives under a tmp dir (``_patched``) and
conftest's guard fails any test that would resolve a real ``~/.dsh`` path.

The chain/goal fixtures are seeded on disk exactly as a crashed broker leaves
them, then ``resume_goal_job`` is called as the scheduler would.

Run: python3 -m pytest broker/tests/test_conc_restart_resume_unit.py -q
"""
from __future__ import annotations

import importlib.util
import json
import os
import shutil
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_resume", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

T = tb.T


# ---------------------------------------------------------------------------
# fixtures / helpers (shared with tests/test_conc_scheduler_unit.py)
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _clean_module_state():
    tb.clear_drain()
    with tb._ACTIVE_GOALS_LOCK:
        tb.ACTIVE_GOALS.clear()
    yield
    tb.clear_drain()
    with tb._ACTIVE_GOALS_LOCK:
        tb.ACTIVE_GOALS.clear()


@contextmanager
def _patched(root: Path):
    names = (
        "inbox", "processing", "goals", "chains", "summaries", "packs", "outbox",
        "failed", "artifacts", "broker", "mailbox", "metrics",
    )
    dirs = {n: root / n for n in names}
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    with mock.patch.object(tb, "INBOX", dirs["inbox"]), mock.patch.object(
        tb, "PROCESSING", dirs["processing"]
    ), mock.patch.object(tb, "GOALS", dirs["goals"]), mock.patch.object(
        tb, "CHAINS", dirs["chains"]
    ), mock.patch.object(tb, "SUMMARIES", dirs["summaries"]), mock.patch.object(
        tb, "PACKS", dirs["packs"]
    ), mock.patch.object(tb, "OUTBOX", dirs["outbox"]), mock.patch.object(
        tb, "FAILED", dirs["failed"]
    ), mock.patch.object(tb, "ARTIFACT_ROOT", dirs["artifacts"]), mock.patch.object(
        tb, "STATE_DIR", dirs["broker"]
    ), mock.patch.object(tb, "HEARTBEAT", dirs["broker"] / "trial-broker.heartbeat.json"), mock.patch.object(
        tb, "MAILBOX", dirs["mailbox"]
    ), mock.patch.object(
        tb, "maybe_offload_gc", lambda **k: None
    ):
        yield dirs


@contextmanager
def _limits(root: Path, data: dict | None):
    """Point trial_lib at a limits.json and clear the env override."""
    path = root / "limits.json"
    if data is None:
        if path.exists():
            path.unlink()
    else:
        path.write_text(json.dumps(data), encoding="utf-8")
    old = T.LIMITS_PATH
    T.LIMITS_PATH = path
    env = {T.MAX_CONCURRENT_GOALS_ENV: ""}
    with mock.patch.dict(os.environ, env):
        os.environ.pop(T.MAX_CONCURRENT_GOALS_ENV, None)
        try:
            yield path
        finally:
            T.LIMITS_PATH = old


def _write_job(inbox: Path, name: str, **fields) -> Path:
    body = {
        "id": name,
        "type": "goal",
        "goal": fields.pop("goal", name.replace(".json", "")),
        "brief": "do the thing",
        "cwd": str(Path.cwd()),
        "profile": "acp-lite",
    }
    body.update(fields)
    p = inbox / name
    p.write_text(json.dumps(body, ensure_ascii=False), encoding="utf-8")
    return p


def _wait_for(pred, timeout: float = 5.0) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return True
        time.sleep(0.01)
    return False


def _heartbeat() -> dict:
    try:
        return json.loads(tb.HEARTBEAT.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _queued() -> list[dict]:
    return list(_heartbeat().get("queued") or [])


def _queued_reason(name: str) -> str | None:
    for item in _queued():
        if item.get("job") == name:
            return str(item.get("reason") or "")
    return None


def _slot_goals() -> list[str]:
    return sorted(str(row.get("goal")) for row in (_heartbeat().get("slots") or []))


class _Ctl:
    """Deterministic stand-in for ``run_job``: records, blocks, releases."""

    def __init__(self):
        self._lock = threading.RLock()
        self.order: list[str] = []
        self.finished: list[str] = []
        self.quarantined: list[str] = []
        self._started: dict[str, threading.Event] = {}
        self._gates: dict[str, threading.Event] = {}
        self.on_run = None  # optional callable(name, job) for side effects

    def _ev(self, store: dict, name: str) -> threading.Event:
        with self._lock:
            return store.setdefault(name, threading.Event())

    def started_ev(self, name: str) -> threading.Event:
        return self._ev(self._started, name)

    def gate(self, name: str) -> threading.Event:
        return self._ev(self._gates, name)

    def release(self, name: str) -> None:
        self.gate(name).set()

    def release_all(self) -> None:
        with self._lock:
            gates = list(self._gates.values())
        for ev in gates:
            ev.set()

    def wait_started(self, n: int, timeout: float = 5.0) -> bool:
        return _wait_for(lambda: len(self.order) >= n, timeout)

    def run_job(self, path: Path) -> int:
        name = path.name
        # Mirror the real broker: the job file moves to PROCESSING first.
        dest = tb.PROCESSING / name
        if path.exists() and path != dest:
            shutil.move(str(path), str(dest))
        try:
            job = json.loads(dest.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            job = {}
        gid = job.get("goal")
        owns = job.get("type") == "goal" and gid
        if owns:
            tb.register_active_goal(
                str(gid), slot=tb.current_slot(), cwd=job.get("cwd"),
                ticket=job.get("ticket"), phase="running",
            )
        try:
            if self.on_run is not None:
                self.on_run(name, job)
            with self._lock:
                self.order.append(name)
            self.started_ev(name).set()
            self.gate(name).wait(timeout=10)
            with self._lock:
                self.finished.append(name)
        finally:
            if owns:
                tb.unregister_active_goal(str(gid))
        return 0


class _Sched:
    """Run ``scheduler_loop`` in a thread and stop it deterministically."""

    def __init__(self, interval: float = 0.05, resume_items=()):
        self.stop = threading.Event()
        self.rc: int | None = None
        self._items = list(resume_items)
        self._interval = interval
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        self.rc = tb.scheduler_loop(self._interval, self._items, stop_event=self.stop)

    def __enter__(self) -> "_Sched":
        self.thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self.stop.set()
        self.thread.join(timeout=5)


# ---------------------------------------------------------------------------
# resume-specific helpers
# ---------------------------------------------------------------------------
def _write_summary(path: Path, block: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "prose\n\n```json\n" + json.dumps(block, ensure_ascii=False, indent=2) + "\n```\n",
        encoding="utf-8",
    )


def _plan_block(gid: str, slices: list[dict]) -> dict:
    return {"action": "emit_chains", "goal": gid, "slices": slices, "goal_status": "running"}


def _foreman_block() -> dict:
    return {
        "status": "done",
        "changed_files": [],
        "branch": "main",
        "commit": "c0ffee",
        "base": "beef",
        "questions": [],
        "notes": "impl done",
    }


def _gate_block(verdict: str = "PASS") -> dict:
    return {"verdict": verdict, "findings": [], "unmet_acceptance": []}


def _goal_job(gid: str, cwd: Path, *, max_rounds: int = 2) -> dict:
    return {
        "type": "goal",
        "id": f"job-{gid}",
        "goal": gid,
        "brief": "do the thing",
        "cwd": str(cwd),
        "profile": "acp-lite",
        "gate_profile": "acp-lite",
        "supervisor_profile": "acp-lite",
        "max_slices": 2,
        "max_supervisor_tickets": 8,
        "max_rounds": max_rounds,
        "notify": "hub",
        "notify_dry_run": True,
    }


def _seed_goal(
    dirs: dict,
    gid: str,
    cwd: Path,
    *,
    status: str = "running",
    slices: list[str] | None = None,
    plan_summary: Path | None = None,
    max_rounds: int = 2,
    created_at: str = "2024-01-01T00:00:00Z",
) -> dict:
    state = {
        "goal": gid,
        "status": status,
        "brief": "do the thing",
        "cwd": str(cwd),
        "profile": "acp-lite",
        "gate_profile": "acp-lite",
        "supervisor_profile": "acp-lite",
        "max_slices": 2,
        "max_supervisor_tickets": 8,
        "max_rounds": max_rounds,
        "slices": list(slices or []),
        "supervisor_ticket_count": 0,
        "supervisor_tickets": [],
        "created_at": created_at,
        "source_job": f"job-{gid}",
        "notify": "hub",
        "notify_dry_run": True,
    }
    if plan_summary is not None:
        state["plan_summary"] = str(plan_summary)
    (dirs["goals"] / f"{gid}.json").write_text(
        json.dumps(state, ensure_ascii=False), encoding="utf-8"
    )
    return state


def _seed_processing_job(
    dirs: dict, gid: str, cwd: Path, *, max_rounds: int = 2, name: str | None = None
) -> tuple[Path, dict]:
    job = _goal_job(gid, cwd, max_rounds=max_rounds)
    p = dirs["processing"] / (name or f"20240101T000000-{gid}.json")
    p.write_text(json.dumps(job, ensure_ascii=False), encoding="utf-8")
    return p, job


def _round_rec(
    sid: str,
    round_n: int,
    pack: str,
    *,
    impl_summary: Path | None = None,
    gate: bool = False,
    gate_pack: str | None = None,
    gate_summary: Path | None = None,
) -> dict:
    impl_ticket = f"impl-trial-{sid}-r{round_n}"
    rec = {
        "round": round_n,
        "impl_ticket": impl_ticket,
        "impl_pack": pack,
        "impl_exit": 0,
        "impl_summary": str(impl_summary or ""),
        "impl_artifacts": {
            "ticket": impl_ticket,
            "exit_code": 0,
            "assert_clean": True,
            "composition": None,
            "summary": str(impl_summary or ""),
            "summary_exists": True,
        },
        "impl_peak": None,
        "impl_steps": None,
        "foreman_status": "done",
        "foreman_block": _foreman_block(),
    }
    if gate:
        gate_ticket = f"gate-trial-{sid}-r{round_n}"
        rec.update(
            {
                "gate_ticket": gate_ticket,
                "gate_pack": gate_pack or pack,
                "gate_exit": 0,
                "gate_summary": str(gate_summary or ""),
                "verdict": "PASS",
                "gate_artifacts": {
                    "ticket": gate_ticket,
                    "exit_code": 0,
                    "assert_clean": True,
                    "composition": None,
                    "summary": str(gate_summary or ""),
                    "summary_exists": True,
                },
                "gate_peak": None,
                "gate_steps": None,
                "gate_block": _gate_block("PASS"),
            }
        )
    return rec


def _seed_chain(
    dirs: dict,
    gid: str,
    sid: str,
    cwd: Path,
    pack: str,
    rounds: list[dict],
    *,
    state: str = "running",
    max_rounds: int = 2,
) -> dict:
    chain = {
        "slice": sid,
        "type": "chain",
        "state": state,
        "pack": pack,
        "acceptance": [],
        "profile": "acp-lite",
        "gate_profile": "acp-lite",
        "cwd": str(cwd),
        "max_rounds": max_rounds,
        "notify": "hub",
        "notify_dry_run": True,
        "rounds": rounds,
        "last_verdict": None,
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z",
        "awaiting_answer_for_round": None,
        "goal": gid,
        "from_goal": True,
        "defer_supervisor_close": True,
    }
    (dirs["chains"] / f"{sid}.json").write_text(
        json.dumps(chain, ensure_ascii=False), encoding="utf-8"
    )
    return chain


class _Tickets:
    """Counts the tickets the resume path would open; opens none of them."""

    def __init__(self, *, plan_slices: list[dict] | None = None, gate_verdict: str = "PASS"):
        self.calls: list[str] = []
        self.plan_slices = list(plan_slices or [])
        self.gate_verdict = gate_verdict

    def count(self, ticket: str) -> int:
        return self.calls.count(ticket)

    def open_slice(self, **kwargs) -> int:
        ticket = str(kwargs["ticket"])
        mode = str(kwargs["prompt_mode"])
        self.calls.append(ticket)
        log_path = Path(kwargs["log_path"])
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text(f"fake {ticket} mode={mode}\n", encoding="utf-8")
        summary = tb.SUMMARIES / f"{kwargs['summary_name']}.md"
        if mode == "foreman":
            _write_summary(summary, _foreman_block())
        elif mode == "gate":
            _write_summary(summary, _gate_block(self.gate_verdict))
        return 0

    def supervisor_ticket(self, **kwargs):
        ticket = str(kwargs["ticket"])
        mode = str(kwargs["prompt_mode"])
        self.calls.append(ticket)
        summary = tb.SUMMARIES / f"{kwargs['summary_name']}.md"
        if mode == "supervisor-plan":
            goal = kwargs.get("goal") if isinstance(kwargs.get("goal"), dict) else {}
            _write_summary(
                summary, _plan_block(str(goal.get("goal") or ""), self.plan_slices)
            )
        elif mode == "supervisor-close":
            _write_summary(summary, {"action": "goal_done", "goal_status": "done"})
        return 0, {
            "ticket": ticket,
            "exit_code": 0,
            "assert_clean": True,
            "composition": None,
            "summary": str(summary),
            "summary_exists": summary.is_file(),
        }, summary


@contextmanager
def _mocked_tickets(tickets: _Tickets):
    def _fake_artifacts(job, log_path, summary_path, ec, ticket=None):
        return {
            "ticket": ticket or job.get("ticket"),
            "exit_code": ec,
            "assert_clean": True,
            "composition": None,
            "summary": str(summary_path),
            "summary_exists": summary_path.is_file(),
        }

    with mock.patch.object(tb, "run_open_slice", side_effect=tickets.open_slice), mock.patch.object(
        tb, "run_supervisor_ticket", side_effect=tickets.supervisor_ticket
    ), mock.patch.object(tb, "write_artifacts", side_effect=_fake_artifacts), mock.patch.object(
        tb, "maybe_notify_hub", return_value={"sent": True, "rc": 0, "dry_run": True}
    ):
        yield tickets


def _resume(proc: Path, job: dict, tickets: _Tickets) -> int:
    with _mocked_tickets(tickets):
        return tb.resume_goal_job(proc, job)


# ---------------------------------------------------------------------------
# 1. collect_resume_items: order, legacy scan, skips
# ---------------------------------------------------------------------------
def test_collect_resume_items_orders_and_skips(tmp_path):
    with _patched(tmp_path / "state") as dirs:
        cwd_a = tmp_path / "repo-a"
        cwd_b = tmp_path / "repo-b"
        cwd_a.mkdir()
        cwd_b.mkdir()

        # A: modern state carries the owning processing job.
        proc_a, _job_a = _seed_processing_job(dirs, "unit-res-a", cwd_a, name="proc-a.json")
        state_a = _seed_goal(
            dirs, "unit-res-a", cwd_a, created_at="2024-01-01T00:00:00Z"
        )
        state_a["processing_job"] = str(proc_a)
        (dirs["goals"] / "unit-res-a.json").write_text(
            json.dumps(state_a, ensure_ascii=False), encoding="utf-8"
        )

        # B: legacy state without ``processing_job`` → found by content scan.
        proc_b, _job_b = _seed_processing_job(dirs, "unit-res-b", cwd_b, name="proc-b.json")
        _seed_goal(dirs, "unit-res-b", cwd_b, created_at="2024-01-02T00:00:00Z")

        # C: running but its processing job is gone → left alone.
        _seed_goal(dirs, "unit-res-c", cwd_b, created_at="2024-01-03T00:00:00Z")

        # D/E: terminal goals are never resumed.
        _seed_goal(dirs, "unit-res-d", cwd_a, status="done", created_at="2023-12-01T00:00:00Z")
        _seed_goal(dirs, "unit-res-e", cwd_a, status="failed", created_at="2023-12-02T00:00:00Z")

        items = tb.collect_resume_items()

        assert [it["goal"] for it in items] == ["unit-res-a", "unit-res-b"], items
        assert [it["path"] for it in items] == [proc_a, proc_b]
        assert all(it["runner"] is tb.resume_goal_job for it in items)
        assert [it["label"] for it in items] == ["resume-unit-res-a", "resume-unit-res-b"]
        assert all(it["goal_ids"] == {it["goal"]} for it in items)
        assert items[0]["job"]["goal"] == "unit-res-a"


# ---------------------------------------------------------------------------
# 2. plan summary already on disk → no second plan ticket
# ---------------------------------------------------------------------------
def test_resume_skips_plan_when_summary_exists(tmp_path):
    gid, sid, pack = "unit-resume-plan", "unit-resume-plan-s1", "unit-resume-plan-s1.pack.md"
    with _patched(tmp_path / "state") as dirs, _limits(
        tmp_path, {"max_slices": 2, "max_rounds": 2}
    ):
        cwd = tmp_path / "repo"
        cwd.mkdir()
        plan_path = dirs["summaries"] / f"goal-{gid}-plan.md"
        slices = [{"slice": sid, "pack": pack, "acceptance": []}]
        _write_summary(plan_path, _plan_block(gid, slices))
        (dirs["packs"] / pack).write_text("# pack\n", encoding="utf-8")
        _seed_goal(dirs, gid, cwd, status="planning", plan_summary=plan_path)
        proc, job = _seed_processing_job(dirs, gid, cwd)

        tickets = _Tickets(plan_slices=slices)
        rc = _resume(proc, job, tickets)

        assert rc == 0
        assert tickets.count(f"supervisor-plan-{gid}") == 0, tickets.calls
        assert tickets.count(f"impl-trial-{sid}-r1") == 1, tickets.calls
        assert tickets.count(f"gate-trial-{sid}-r1") == 1, tickets.calls
        assert tb._read_goal(gid)["status"] == "done"
        assert tb._read_chain(sid)["state"] == "PASS"


# ---------------------------------------------------------------------------
# 3. impl r1 summary already on disk → skip impl, run gate r1 once
# ---------------------------------------------------------------------------
def test_resume_reuses_impl_summary_and_runs_gate_once(tmp_path):
    gid, sid, pack = "unit-resume-impl", "unit-resume-impl-s1", "unit-resume-impl-s1.pack.md"
    with _patched(tmp_path / "state") as dirs, _limits(
        tmp_path, {"max_slices": 2, "max_rounds": 2}
    ):
        cwd = tmp_path / "repo"
        cwd.mkdir()
        plan_path = dirs["summaries"] / f"goal-{gid}-plan.md"
        _write_summary(plan_path, _plan_block(gid, [{"slice": sid, "pack": pack, "acceptance": []}]))
        (dirs["packs"] / pack).write_text("# pack\n", encoding="utf-8")
        impl_summary = dirs["summaries"] / f"{sid}-impl-r1.md"
        _write_summary(impl_summary, _foreman_block())
        _seed_goal(dirs, gid, cwd, status="running", slices=[sid], plan_summary=plan_path)
        proc, job = _seed_processing_job(dirs, gid, cwd)
        _seed_chain(
            dirs, gid, sid, cwd, pack,
            [_round_rec(sid, 1, pack, impl_summary=impl_summary)],
        )

        tickets = _Tickets()
        rc = _resume(proc, job, tickets)

        assert rc == 0
        assert tickets.count(f"impl-trial-{sid}-r1") == 0, tickets.calls
        assert tickets.count(f"gate-trial-{sid}-r1") == 1, tickets.calls
        assert tickets.count(f"supervisor-close-{gid}") == 1, tickets.calls
        assert tickets.count(f"supervisor-plan-{gid}") == 0, tickets.calls
        chain = tb._read_chain(sid)
        assert chain["state"] == "PASS"
        assert len(chain["rounds"]) == 1, "resume must not append a duplicate round"
        assert tb._read_goal(gid)["status"] == "done"


# ---------------------------------------------------------------------------
# 4. gate r1 PASS recorded, close still missing → only the close ticket opens
# ---------------------------------------------------------------------------
def test_resume_reuses_gate_pass_and_closes_goal_once(tmp_path):
    gid, sid, pack = "unit-resume-gate", "unit-resume-gate-s1", "unit-resume-gate-s1.pack.md"
    with _patched(tmp_path / "state") as dirs, _limits(
        tmp_path, {"max_slices": 2, "max_rounds": 1}
    ):
        cwd = tmp_path / "repo"
        cwd.mkdir()
        plan_path = dirs["summaries"] / f"goal-{gid}-plan.md"
        _write_summary(plan_path, _plan_block(gid, [{"slice": sid, "pack": pack, "acceptance": []}]))
        (dirs["packs"] / pack).write_text("# pack\n", encoding="utf-8")
        impl_summary = dirs["summaries"] / f"{sid}-impl-r1.md"
        gate_summary = dirs["summaries"] / f"{sid}-gate-r1.md"
        _write_summary(impl_summary, _foreman_block())
        _write_summary(gate_summary, _gate_block("PASS"))
        _seed_goal(
            dirs, gid, cwd, status="running", slices=[sid], plan_summary=plan_path, max_rounds=1
        )
        proc, job = _seed_processing_job(dirs, gid, cwd, max_rounds=1)
        _seed_chain(
            dirs, gid, sid, cwd, pack,
            [_round_rec(sid, 1, pack, impl_summary=impl_summary, gate=True, gate_summary=gate_summary)],
            max_rounds=1,
        )

        tickets = _Tickets()
        rc = _resume(proc, job, tickets)

        assert rc == 0
        assert tickets.count(f"impl-trial-{sid}-r1") == 0, tickets.calls
        assert tickets.count(f"gate-trial-{sid}-r1") == 0, tickets.calls
        assert tickets.count(f"supervisor-close-{gid}") == 1, tickets.calls
        assert tickets.count(f"supervisor-plan-{gid}") == 0, tickets.calls
        goal = tb._read_goal(gid)
        assert goal["status"] == "done"
        assert goal["last_chain_state"] == "PASS"
        assert tb._read_chain(sid)["state"] == "PASS"


# ---------------------------------------------------------------------------
# 5. awaiting_supervisor chain stays parked (no tickets, no state loss)
# ---------------------------------------------------------------------------
def test_resume_leaves_awaiting_supervisor_chain_parked(tmp_path):
    gid, sid, pack = "unit-resume-ask", "unit-resume-ask-s1", "unit-resume-ask-s1.pack.md"
    with _patched(tmp_path / "state") as dirs, _limits(
        tmp_path, {"max_slices": 2, "max_rounds": 2}
    ):
        cwd = tmp_path / "repo"
        cwd.mkdir()
        plan_path = dirs["summaries"] / f"goal-{gid}-plan.md"
        _write_summary(plan_path, _plan_block(gid, [{"slice": sid, "pack": pack, "acceptance": []}]))
        (dirs["packs"] / pack).write_text("# pack\n", encoding="utf-8")
        impl_summary = dirs["summaries"] / f"{sid}-impl-r1.md"
        _write_summary(impl_summary, _foreman_block())
        _seed_goal(dirs, gid, cwd, status="running", slices=[sid], plan_summary=plan_path)
        proc, job = _seed_processing_job(dirs, gid, cwd)
        _seed_chain(
            dirs, gid, sid, cwd, pack,
            [_round_rec(sid, 1, pack, impl_summary=impl_summary)],
            state="awaiting_supervisor",
        )

        tickets = _Tickets()
        _resume(proc, job, tickets)

        assert tickets.calls == [], tickets.calls
        assert tb._read_goal(gid)["status"] == "running"
        assert tb._read_chain(sid)["state"] == "awaiting_supervisor"
        assert proc.is_file(), "the owning processing job must survive the restart"
        assert tb.PROCESSING.joinpath(proc.name).is_file()


# ---------------------------------------------------------------------------
# 6. scheduler: resume items take the slots before the inbox
# ---------------------------------------------------------------------------
def test_scheduler_resume_items_take_slots_before_inbox(tmp_path):
    with _patched(tmp_path / "state") as dirs, _limits(tmp_path, {"max_concurrent_goals": 2}):
        cwd_a, cwd_b, cwd_c = (tmp_path / n for n in ("repo-a", "repo-b", "repo-c"))
        for d in (cwd_a, cwd_b, cwd_c):
            d.mkdir()

        _seed_goal(dirs, "unit-slot-a", cwd_a, created_at="2024-01-01T00:00:00Z")
        _seed_goal(dirs, "unit-slot-b", cwd_b, created_at="2024-01-02T00:00:00Z")
        proc_a, _ = _seed_processing_job(dirs, "unit-slot-a", cwd_a, name="resume-a.json")
        proc_b, _ = _seed_processing_job(dirs, "unit-slot-b", cwd_b, name="resume-b.json")

        items = tb.collect_resume_items()
        assert [it["goal"] for it in items] == ["unit-slot-a", "unit-slot-b"], items

        ctl = _Ctl()
        # The resume *selection* is real; the ticket work is the blocking stand-in.
        for it in items:
            it["runner"] = ctl.run_job

        _write_job(dirs["inbox"], "third.json", goal="unit-slot-c", cwd=str(cwd_c))

        with mock.patch.object(tb, "run_job", ctl.run_job), _Sched(
            interval=0.05, resume_items=items
        ) as sched:
            assert ctl.wait_started(2), ctl.order
            assert _wait_for(
                lambda: _queued_reason("third.json") == "max_concurrent_goals"
            ), _queued()
            # Both resume Goals own slots; the inbox Goal is still parked.
            # (The slot snapshot trails the worker threads by one beat.)
            assert _wait_for(
                lambda: _slot_goals() == ["unit-slot-a", "unit-slot-b"]
            ), _heartbeat()
            assert not ctl.started_ev("third.json").is_set()
            assert "third.json" not in ctl.order

            ctl.release(proc_a.name)
            assert ctl.wait_started(3), ctl.order
            assert set(ctl.order[:2]) == {proc_a.name, proc_b.name}, ctl.order
            assert ctl.order[2] == "third.json", ctl.order
            assert _wait_for(lambda: "unit-slot-c" in _slot_goals()), _heartbeat()

            ctl.release_all()

        assert sched.rc == 0
        assert sorted(ctl.finished) == ["resume-a.json", "resume-b.json", "third.json"]
