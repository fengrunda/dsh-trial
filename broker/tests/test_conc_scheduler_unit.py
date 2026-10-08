#!/usr/bin/env python3
"""T2 scheduler unit tests: FIFO dispatch, slot cap, cwd conflicts, drain.

All model/subprocess work is mocked: ``run_job`` is replaced by a runner that
blocks on a ``threading.Event`` per job, so the tests never sleep for real and
never touch the real ``~/.dsh`` tree (conftest points DSH_HOME at a tmp dir).

Run: python3 -m pytest broker/tests/test_conc_scheduler_unit.py -q
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

spec = importlib.util.spec_from_file_location("trial_broker_sched", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

T = tb.T


# ---------------------------------------------------------------------------
# fixtures / helpers
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


def _write_mtimes(paths, base: float = 1_700_000_000.0) -> None:
    """Force an explicit enqueue order (FIFO is mtime then name)."""
    for i, p in enumerate(paths):
        os.utime(p, (base + i, base + i))


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
        if path.exists():
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

    def failing_run_job(self, fail_name: str):
        def _run(path: Path) -> int:
            if path.name == fail_name:
                # The real broker moves the job out of INBOX before working.
                dest = tb.PROCESSING / path.name
                if path.exists():
                    shutil.move(str(path), str(dest))
                raise RuntimeError("boom")
            return self.run_job(path)
        return _run


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
# 1. concurrency cap
# ---------------------------------------------------------------------------
def test_limit_two_third_job_queues_then_runs(tmp_path):
    with _patched(tmp_path / "state") as dirs, _limits(tmp_path, {"max_concurrent_goals": 2}):
        ctl = _Ctl()
        paths = [
            _write_job(dirs["inbox"], f"job{i}.json", cwd=str(tmp_path / f"repo{i}"))
            for i in range(3)
        ]
        _write_mtimes(paths)
        with mock.patch.object(tb, "run_job", ctl.run_job), _Sched() as sched:
            assert ctl.wait_started(2), ctl.order
            assert _wait_for(lambda: _queued_reason("job2.json") == "max_concurrent_goals")
            assert "job2.json" not in ctl.order
            snap = _heartbeat()
            assert snap["max_concurrent_goals"] == 2
            assert _wait_for(lambda: len(_heartbeat().get("slots") or []) == 2), _heartbeat()
            ctl.release("job0.json")
            assert ctl.wait_started(3), ctl.order
        assert ctl.order == ["job0.json", "job1.json", "job2.json"] or ctl.order[2] == "job2.json"


def test_limit_lowered_to_one_stops_new_dispatch(tmp_path):
    with _patched(tmp_path / "state") as dirs, _limits(tmp_path, {"max_concurrent_goals": 2}):
        ctl = _Ctl()
        paths = [
            _write_job(dirs["inbox"], f"job{i}.json", cwd=str(tmp_path / f"repo{i}"))
            for i in range(3)
        ]
        _write_mtimes(paths)
        with mock.patch.object(tb, "run_job", ctl.run_job), _Sched() as sched:
            assert ctl.wait_started(2)
            assert _wait_for(lambda: _queued_reason("job2.json") == "max_concurrent_goals")
            # Operator narrows the cap without a restart.
            (tmp_path / "limits.json").write_text(
                json.dumps({"max_concurrent_goals": 1}), encoding="utf-8"
            )
            ctl.release("job0.json")
            assert ctl.wait_started(1) is not None
            assert _wait_for(lambda: "job0.json" in ctl.finished)
            # One slot free, cap is now 1 → job2 must stay queued.
            time.sleep(0.3)
            assert "job2.json" not in ctl.order, ctl.order
            assert _queued_reason("job2.json") == "max_concurrent_goals"
            # Free the last slot: with nothing running, one job may go.
            ctl.release("job1.json")
            assert _wait_for(lambda: "job2.json" in ctl.order), ctl.order
        assert sched.rc == 0


# ---------------------------------------------------------------------------
# 2. cwd conflict keys
# ---------------------------------------------------------------------------
def test_same_cwd_serializes_fifo(tmp_path):
    with _patched(tmp_path / "state") as dirs, _limits(tmp_path, {"max_concurrent_goals": 3}):
        ctl = _Ctl()
        repo = tmp_path / "repo"
        repo.mkdir()
        paths = [
            _write_job(dirs["inbox"], f"job{i}.json", cwd=str(repo)) for i in range(3)
        ]
        _write_mtimes(paths)
        with mock.patch.object(tb, "run_job", ctl.run_job), _Sched() as sched:
            assert ctl.wait_started(1)
            assert _wait_for(lambda: _queued_reason("job1.json", ) == "cwd-conflict:job0.json")
            assert ctl.order == ["job0.json"]
            ctl.release("job0.json")
            assert _wait_for(lambda: len(ctl.order) == 2)
            assert ctl.order == ["job0.json", "job1.json"]
            ctl.release("job1.json")
            assert _wait_for(lambda: len(ctl.order) == 3)
            assert ctl.order == ["job0.json", "job1.json", "job2.json"]


def test_symlinked_and_nested_cwd_conflict(tmp_path):
    with _patched(tmp_path / "state") as dirs, _limits(tmp_path, {"max_concurrent_goals": 3}):
        ctl = _Ctl()
        repo = tmp_path / "repo"
        (repo / "sub").mkdir(parents=True)
        link = tmp_path / "repo-link"
        link.symlink_to(repo)
        paths = [
            _write_job(dirs["inbox"], "a.json", cwd=str(repo)),
            _write_job(dirs["inbox"], "b.json", cwd=str(link)),
            _write_job(dirs["inbox"], "c.json", cwd=str(repo / "sub")),
            _write_job(dirs["inbox"], "d.json", cwd=str(tmp_path / "elsewhere")),
        ]
        _write_mtimes(paths)
        with mock.patch.object(tb, "run_job", ctl.run_job), _Sched() as sched:
            assert ctl.wait_started(2)
            assert set(ctl.order) == {"a.json", "d.json"}
            assert _queued_reason("b.json") == "cwd-conflict:a.json"
            assert _queued_reason("c.json") == "cwd-conflict:a.json"


def test_brief_worktree_conflicts_but_readonly_reference_does_not(tmp_path):
    with _patched(tmp_path / "state") as dirs, _limits(tmp_path, {"max_concurrent_goals": 2}):
        ctl = _Ctl()
        wt = Path("/workspace/dsh-wt/conc-sched-x")
        paths = [
            _write_job(dirs["inbox"], "owner.json", cwd=str(wt), brief="own this worktree"),
            _write_job(
                dirs["inbox"], "reader.json", cwd=str(tmp_path / "other"),
                brief=f"read {wt}/docs/readme.md then edit your own repo",
            ),
            _write_job(
                dirs["inbox"], "unrelated.json", cwd=str(tmp_path / "other2"),
                brief="only a read-only reference: /workspace/dsh-trial-other/src",
            ),
        ]
        _write_mtimes(paths)
        with mock.patch.object(tb, "run_job", ctl.run_job), _Sched() as sched:
            assert ctl.wait_started(2)
            assert set(ctl.order) == {"owner.json", "unrelated.json"}
            assert _queued_reason("reader.json") == "cwd-conflict:owner.json"


def test_different_cwds_run_in_parallel(tmp_path):
    with _patched(tmp_path / "state") as dirs, _limits(tmp_path, {"max_concurrent_goals": 2}):
        ctl = _Ctl()
        paths = [
            _write_job(dirs["inbox"], "a.json", cwd=str(tmp_path / "ra")),
            _write_job(dirs["inbox"], "b.json", cwd=str(tmp_path / "rb")),
        ]
        _write_mtimes(paths)
        with mock.patch.object(tb, "run_job", ctl.run_job), _Sched() as sched:
            assert ctl.wait_started(2)
            assert ctl.finished == []
            assert _wait_for(lambda: len(_heartbeat().get("slots") or []) == 2)


# ---------------------------------------------------------------------------
# 3. fairness / goal ownership
# ---------------------------------------------------------------------------
def test_blocked_job_is_not_starved(tmp_path):
    with _patched(tmp_path / "state") as dirs, _limits(tmp_path, {"max_concurrent_goals": 2}):
        ctl = _Ctl()
        repo = tmp_path / "repo"
        repo.mkdir()
        paths = [
            _write_job(dirs["inbox"], "a.json", cwd=str(repo)),
            _write_job(dirs["inbox"], "b.json", cwd=str(repo)),
            _write_job(dirs["inbox"], "c.json", cwd=str(tmp_path / "free")),
        ]
        _write_mtimes(paths)
        with mock.patch.object(tb, "run_job", ctl.run_job), _Sched() as sched:
            # a starts, b is blocked, c takes the free slot (b does not block it).
            assert ctl.wait_started(2)
            assert ctl.order == ["a.json", "c.json"]
            assert _queued_reason("b.json") == "cwd-conflict:a.json"
            ctl.release("a.json")
            assert _wait_for(lambda: "b.json" in ctl.order)
            assert ctl.order == ["a.json", "c.json", "b.json"]


def test_goal_update_waits_for_running_goal(tmp_path):
    with _patched(tmp_path / "state") as dirs, _limits(tmp_path, {"max_concurrent_goals": 3}):
        ctl = _Ctl()
        paths = [
            _write_job(dirs["inbox"], "goal.json", goal="g-wait", cwd=str(tmp_path / "g")),
            _write_job(
                dirs["inbox"], "update.json", type="goal-update", goal="g-wait",
                cwd=str(tmp_path / "u"), action="widen",
            ),
        ]
        _write_mtimes(paths)
        with mock.patch.object(tb, "run_job", ctl.run_job), _Sched() as sched:
            assert ctl.wait_started(1)
            assert _wait_for(lambda: _queued_reason("update.json") == "goal-busy:g-wait")
            assert ctl.order == ["goal.json"]
            ctl.release("goal.json")
            assert _wait_for(lambda: "update.json" in ctl.order)


# ---------------------------------------------------------------------------
# 4. failure isolation / per-goal accounting
# ---------------------------------------------------------------------------
def test_worker_exception_is_isolated(tmp_path):
    with _patched(tmp_path / "state") as dirs, _limits(tmp_path, {"max_concurrent_goals": 2}):
        ctl = _Ctl()
        paths = [
            _write_job(dirs["inbox"], "bad.json", cwd=str(tmp_path / "bad")),
            _write_job(dirs["inbox"], "good.json", cwd=str(tmp_path / "good")),
        ]
        _write_mtimes(paths)

        def _quarantine(path, exc):
            ctl.quarantined.append(path.name)

        with mock.patch.object(tb, "run_job", ctl.failing_run_job("bad.json")), mock.patch.object(
            tb, "_quarantine_unexpected", _quarantine
        ), _Sched() as sched:
            assert _wait_for(lambda: "good.json" in ctl.order), ctl.order
            assert _wait_for(lambda: ctl.quarantined == ["bad.json"]), ctl.quarantined
            ctl.release("good.json")
            assert _wait_for(lambda: "good.json" in ctl.finished), ctl.finished
            # The loop is still alive: a new job is dispatched after the failure.
            _write_job(dirs["inbox"], "later.json", cwd=str(tmp_path / "later"))
            assert _wait_for(lambda: "later.json" in ctl.order), ctl.order
            ctl.release("later.json")


def test_per_goal_notify_and_metrics_are_isolated(tmp_path):
    with _patched(tmp_path / "state") as dirs, _limits(tmp_path, {"max_concurrent_goals": 2}):
        ctl = _Ctl()
        calls: list[tuple[str, int]] = []
        lock = threading.Lock()

        def _note(goal_id):
            with lock:
                calls.append((str(goal_id), tb.current_slot()))

        def _on_run(name, job):
            gid = str(job.get("goal"))
            state = {
                "goal": gid,
                "status": "done",
                "cwd": job.get("cwd"),
                "slices": [],
                "supervisor_ticket_count": 0,
                "supervisor_tickets": [],
                "metrics": T.empty_metrics(),
            }
            state["metrics"]["tickets"].append({"ticket": f"t-{gid}", "role": "impl"})
            tb._write_goal(state)
            tb._notify_goal_terminal(gid)

        ctl.on_run = _on_run
        paths = [
            _write_job(dirs["inbox"], "g1.json", goal="goal-one", cwd=str(tmp_path / "p1")),
            _write_job(dirs["inbox"], "g2.json", goal="goal-two", cwd=str(tmp_path / "p2")),
        ]
        _write_mtimes(paths)
        with mock.patch.object(tb, "run_job", ctl.run_job), mock.patch.object(
            tb, "_notify_goal_terminal", _note
        ), _Sched() as sched:
            assert ctl.wait_started(2)
            assert _wait_for(lambda: len(calls) == 2), calls
        assert sorted(c[0] for c in calls) == ["goal-one", "goal-two"]
        assert {c[1] for c in calls} == {1, 2}
        one = json.loads((dirs["goals"] / "goal-one.json").read_text())
        two = json.loads((dirs["goals"] / "goal-two.json").read_text())
        assert [t["ticket"] for t in one["metrics"]["tickets"]] == ["t-goal-one"]
        assert [t["ticket"] for t in two["metrics"]["tickets"]] == ["t-goal-two"]


# ---------------------------------------------------------------------------
# 5. heartbeat
# ---------------------------------------------------------------------------
def test_heartbeat_carries_slots_and_queued(tmp_path):
    with _patched(tmp_path / "state") as dirs, _limits(tmp_path, {"max_concurrent_goals": 1}):
        ctl = _Ctl()
        repo = tmp_path / "repo"
        repo.mkdir()
        paths = [
            _write_job(dirs["inbox"], "run.json", goal="g-hb", cwd=str(repo)),
            _write_job(dirs["inbox"], "wait.json", goal="g-hb2", cwd=str(repo)),
        ]
        _write_mtimes(paths)
        with mock.patch.object(tb, "run_job", ctl.run_job), _Sched() as sched:
            assert ctl.wait_started(1)
            assert _wait_for(lambda: bool(_queued())), _heartbeat()
            assert _wait_for(lambda: len(_heartbeat().get("slots") or []) == 1), _heartbeat()
            snap = _heartbeat()
            assert snap["pending"] >= 1
            assert snap["max_concurrent_goals"] == 1
            assert [s["slot"] for s in snap["slots"]] == [1]
            assert snap["slots"][0]["phase"] == "running"
            assert _queued()[0]["job"] == "wait.json"
            assert _queued()[0]["reason"]
            assert snap["drain"] is False


# ---------------------------------------------------------------------------
# 6. drain
# ---------------------------------------------------------------------------
def test_run_open_slice_refuses_while_draining(tmp_path):
    with _patched(tmp_path / "state") as dirs:
        tb.request_drain()
        with pytest.raises(tb.BrokerDraining):
            tb.run_open_slice(
                ticket="impl-trial-drain-s1-r1",
                pack_name="p.pack.md",
                profile="acp-lite",
                cwd=str(tmp_path),
                role="impl",
                summary_name="drain-s1-impl-r1",
                prompt_mode="foreman",
                log_path=dirs["artifacts"] / "x.log",
            )
        assert tb.draining() is True
        # Second request reports "already draining" (the second signal exits).
        assert tb.request_drain() is False


def test_drain_keeps_goal_running_and_processing_file(tmp_path):
    with _patched(tmp_path / "state") as dirs:
        path = _write_job(dirs["inbox"], "goal.json", goal="g-drain", cwd=str(tmp_path / "repo"))
        job = tb.load_job(path)
        tb.request_drain()

        def _raise(**kwargs):
            raise tb.BrokerDraining("draining: test")

        with mock.patch.object(tb, "run_supervisor_ticket", _raise):
            with pytest.raises(tb.BrokerDraining):
                tb.run_goal_job(path, job)

        goal = json.loads((dirs["goals"] / "g-drain.json").read_text())
        assert goal["status"] == "planning"
        assert goal.get("error") is None
        assert list(dirs["processing"].glob("*.json")), "processing job must stay for the next start"
        assert goal["processing_job"].startswith(str(dirs["processing"]))
        assert not list(dirs["failed"].glob("*.json"))
        assert not list(dirs["outbox"].glob("*.json"))


def test_drain_stops_new_dispatch_and_exits_when_workers_finish(tmp_path):
    with _patched(tmp_path / "state") as dirs, _limits(tmp_path, {"max_concurrent_goals": 1}):
        ctl = _Ctl()
        paths = [
            _write_job(dirs["inbox"], "a.json", cwd=str(tmp_path / "ra")),
            _write_job(dirs["inbox"], "b.json", cwd=str(tmp_path / "rb")),
        ]
        _write_mtimes(paths)
        with mock.patch.object(tb, "run_job", ctl.run_job), _Sched() as sched:
            assert ctl.wait_started(1)
            tb.request_drain()
            # No new job may start while draining …
            time.sleep(0.2)
            assert ctl.order == ["a.json"], ctl.order
            assert sched.rc is None
            # … and the loop exits 0 once the in-flight job is done, without
            # ever starting the queued one.
            ctl.release_all()
            assert _wait_for(lambda: sched.rc == 0), sched.rc
        assert ctl.order == ["a.json"]
        assert tb.draining() is True


# ---------------------------------------------------------------------------
# 7. --once keeps the old serial behaviour
# ---------------------------------------------------------------------------
def test_once_processes_one_job_and_exits(tmp_path):
    with _patched(tmp_path / "state") as dirs, _limits(tmp_path, {"max_concurrent_goals": 2}):
        ctl = _Ctl()
        ctl.release_all()
        paths = [
            _write_job(dirs["inbox"], "a.json", cwd=str(tmp_path / "ra")),
            _write_job(dirs["inbox"], "b.json", cwd=str(tmp_path / "rb")),
        ]
        _write_mtimes(paths)
        with mock.patch.object(tb, "run_job", ctl.run_job), mock.patch.object(
            tb, "collect_resume_items", lambda: []
        ):
            rc = tb.cmd_run(_args(once=True))
        assert rc == 0
        assert ctl.order == ["a.json"]
        assert (dirs["processing"] / "a.json").is_file()
        assert (dirs["inbox"] / "b.json").is_file()


def _args(*, once: bool = False, poll: int = 20):
    return type("A", (), {"once": once, "poll": poll})()
