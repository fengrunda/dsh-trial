"""Concurrent-Goal watchdog: per-Goal ``goal_no_ticket`` + slot-aware heartbeat.

The new broker publishes ``slots`` (one row per running Goal) and stamps
``DSH_TRIAL_GOAL`` into every work process env, so the watchdog must decide
liveness *per Goal*: Goal A's live worker must never excuse Goal B. All checks
are tmp-isolated with injected clocks, pids and heartbeats.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_watchdog_conc_mod", ROOT / "trial-watchdog.py")
wd = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wd)

HB_TIMEOUT = 300.0
GOAL_TIMEOUT = 600.0
BROKER_PID = 4242


@pytest.fixture(autouse=True)
def _pin_env(monkeypatch):
    """Deterministic knobs: no ambient env may shift the thresholds."""
    monkeypatch.setenv("TRIAL_WATCHDOG_HEARTBEAT_SEC", str(int(HB_TIMEOUT)))
    monkeypatch.setenv("TRIAL_WATCHDOG_GOAL_STALL_SEC", str(int(GOAL_TIMEOUT)))


def _patch_state(tmp_path: Path):
    dirs = {n: tmp_path / n for n in ("goals", "inbox", "processing")}
    for p in dirs.values():
        p.mkdir(parents=True, exist_ok=True)
    return mock.patch.multiple(
        wd,
        STATE_DIR=tmp_path,
        PIDFILE=tmp_path / "trial-broker.pid",
        HEARTBEAT=tmp_path / "trial-broker.heartbeat.json",
        STATE_FILE=tmp_path / "trial-watchdog.state.json",
        GOALS=dirs["goals"],
        INBOX=dirs["inbox"],
        PROCESSING=dirs["processing"],
    ), dirs


def _iso(when: float) -> str:
    return datetime.fromtimestamp(when, tz=timezone.utc).isoformat()


def _goal(gid: str, now: float, *, age: float = GOAL_TIMEOUT + 120.0, status: str = "running") -> None:
    # Filename deliberately differs from the goal id: only the JSON ``goal``
    # field may drive attribution (no accidental filename/​id substring match).
    body = {"goal": gid, "status": status, "updated_at": _iso(now - age)}
    name = gid.replace("-", "_") + ".json"
    (wd.GOALS / name).write_text(json.dumps(body), encoding="utf-8")


def _heartbeat_file(now: float, age: float, doc: dict) -> None:
    """Heartbeat JSON whose mtime is ``age`` seconds before ``now``."""
    wd.HEARTBEAT.write_text(json.dumps(doc), encoding="utf-8")
    ts = now - age
    os.utime(wd.HEARTBEAT, (ts, ts))


def _new_hb(*goals: str) -> dict:
    return {
        "pid": BROKER_PID,
        "slots": [{"slot": i, "goal": g, "phase": "impl"} for i, g in enumerate(goals)],
        "max_concurrent_goals": 2,
        "queued": [],
    }


def _keys(events) -> list[str]:
    return [str(e.get("reason_key") or "") for e in events]


# --- collect_stall_events now takes the heartbeat / pid map as parameters -----

def test_collect_stall_events_uses_injected_heartbeat_not_the_file(tmp_path):
    """Injected ``heartbeat``/``goal_pids`` must win over any live read."""
    patch, _ = _patch_state(tmp_path)
    now = 10_000.0
    with patch:
        _heartbeat_file(now, age=5.0, doc={"slots": [{"goal": "ON_DISK"}]})
        wd.PIDFILE.write_text(str(BROKER_PID), encoding="utf-8")
        with mock.patch.object(
            wd, "work_pid_goals", side_effect=AssertionError("must not probe /proc")
        ):
            events = wd.collect_stall_events(
                now,
                broker_pids=[BROKER_PID],
                work_pids=[222],
                heartbeat={"slots": []},  # fresh file beat would excuse B; injected one must not
                goal_pids={"g-7f3a1c": [222]},
            )
    assert _keys(events) == []  # fresh on-disk beat is ignored; injected hb has no slots


# --- (A/B) one Goal's worker must not excuse another Goal ---------------------

def test_owned_pid_excuses_only_its_own_goal(tmp_path):
    patch, _ = _patch_state(tmp_path)
    now = 10_000.0
    with patch:
        # Only Goal A is in the broker's slots; B has no slot and no owned pid.
        hb = _new_hb("g-7f3a1c")
        _heartbeat_file(now, age=HB_TIMEOUT - 60.0, doc=hb)
        _goal("g-7f3a1c", now)
        _goal("g-9b2e4d", now)
        events = wd.collect_stall_events(
            now,
            broker_pids=[BROKER_PID],
            work_pids=[222],
            heartbeat=hb,
            goal_pids={"g-7f3a1c": [222]},
        )
    assert _keys(events) == ["goal_no_ticket:g-9b2e4d"]
    assert events[0]["goal"] == "g-9b2e4d"


# --- (B fresh slots) a published slot beats a missing pid ---------------------

def test_fresh_slots_excuse_goal_without_pid(tmp_path):
    patch, _ = _patch_state(tmp_path)
    now = 10_000.0
    with patch:
        hb = _new_hb("g-7f3a1c", "g-9b2e4d")
        _heartbeat_file(now, age=5.0, doc=hb)
        _goal("g-7f3a1c", now)
        _goal("g-9b2e4d", now)
        events = wd.collect_stall_events(
            now, broker_pids=[BROKER_PID], work_pids=[222], heartbeat=hb, goal_pids={"g-7f3a1c": [222]}
        )
    assert _keys(events) == []


def test_stale_slots_do_not_excuse_goal_without_pid(tmp_path):
    """The slots key alone is not enough — the beat carrying it must be fresh."""
    patch, _ = _patch_state(tmp_path)
    now = 10_000.0
    with patch:
        hb = _new_hb("g-7f3a1c", "g-9b2e4d")
        _heartbeat_file(now, age=HB_TIMEOUT + 100.0, doc=hb)
        _goal("g-7f3a1c", now)
        _goal("g-9b2e4d", now)
        events = wd.collect_stall_events(
            now, broker_pids=[BROKER_PID], work_pids=[222], heartbeat=hb, goal_pids={"g-7f3a1c": [222]}
        )
    assert _keys(events) == ["heartbeat_stale", "goal_no_ticket:g-9b2e4d"]


# --- (B inbox) a queued/processing job for that Goal counts as a ticket -------

def test_goal_with_pending_job_in_inbox_is_not_stalled(tmp_path):
    patch, dirs = _patch_state(tmp_path)
    now = 10_000.0
    with patch:
        hb = _new_hb("g-7f3a1c")
        _heartbeat_file(now, age=HB_TIMEOUT - 60.0, doc=hb)
        _goal("g-7f3a1c", now)
        _goal("g-9b2e4d", now)
        (dirs["inbox"] / "b.json").write_text(
            json.dumps({"type": "goal", "goal": "g-9b2e4d", "brief": "x"}), encoding="utf-8"
        )
        events = wd.collect_stall_events(
            now, broker_pids=[BROKER_PID], work_pids=[222], heartbeat=hb, goal_pids={"g-7f3a1c": [222]}
        )
    assert _keys(events) == []


def test_goal_with_pending_job_in_processing_is_not_stalled(tmp_path):
    patch, dirs = _patch_state(tmp_path)
    now = 10_000.0
    with patch:
        hb = _new_hb("g-7f3a1c")
        _heartbeat_file(now, age=HB_TIMEOUT - 60.0, doc=hb)
        _goal("g-7f3a1c", now)
        _goal("g-9b2e4d", now)
        (dirs["processing"] / "b.json").write_text(
            json.dumps({"type": "goal", "goal": "g-9b2e4d", "brief": "x"}), encoding="utf-8"
        )
        events = wd.collect_stall_events(
            now, broker_pids=[BROKER_PID], work_pids=[222], heartbeat=hb, goal_pids={"g-7f3a1c": [222]}
        )
    assert _keys(events) == []


def test_pending_goal_ids_reads_json_goal_field(tmp_path):
    patch, dirs = _patch_state(tmp_path)
    with patch:
        (dirs["inbox"] / "b.json").write_text(json.dumps({"goal": "g-9b2e4d"}), encoding="utf-8")
        (dirs["processing"] / "c.json").write_text(json.dumps({"goal": "C"}), encoding="utf-8")
        (dirs["inbox"] / "broken.json").write_text("{not json", encoding="utf-8")
        (dirs["inbox"] / "notagoal.txt").write_text("g-9b2e4d", encoding="utf-8")
        assert wd._pending_goal_ids() == {"g-9b2e4d", "C"}
        # The legacy boolean helper survives for other callers.
        assert wd._pending_jobs() is True


def test_unattributed_pending_job_keeps_goal_from_being_reported(tmp_path):
    """A job file nobody can attribute to a Goal is still "some work pending"."""
    patch, dirs = _patch_state(tmp_path)
    now = 10_000.0
    with patch:
        hb = _new_hb("g-7f3a1c")
        _heartbeat_file(now, age=HB_TIMEOUT - 60.0, doc=hb)
        _goal("g-9b2e4d", now)
        (dirs["processing"] / "live-job.json").write_text("{}", encoding="utf-8")
        events = wd.collect_stall_events(
            now, broker_pids=[BROKER_PID], work_pids=[222], heartbeat=hb, goal_pids={"g-7f3a1c": [222]}
        )
    assert _keys(events) == []


# --- legacy broker: no `slots` key keeps the old "*" exemption ----------------

def test_legacy_heartbeat_with_unowned_pid_excuses_goal(tmp_path):
    patch, _ = _patch_state(tmp_path)
    now = 10_000.0
    with patch:
        legacy = {"pid": BROKER_PID, "pending": 1}
        _heartbeat_file(now, age=HB_TIMEOUT - 60.0, doc=legacy)
        _goal("g-9b2e4d", now)
        events = wd.collect_stall_events(
            now, broker_pids=[BROKER_PID], work_pids=[222], heartbeat=legacy, goal_pids={"*": [222]}
        )
    assert _keys(events) == []


def test_legacy_heartbeat_without_any_pid_still_reports_goal(tmp_path):
    patch, _ = _patch_state(tmp_path)
    now = 10_000.0
    with patch:
        legacy = {"pid": BROKER_PID, "pending": 0}
        _heartbeat_file(now, age=HB_TIMEOUT - 60.0, doc=legacy)
        _goal("g-9b2e4d", now)
        events = wd.collect_stall_events(
            now, broker_pids=[BROKER_PID], work_pids=[], heartbeat=legacy, goal_pids={}
        )
    assert _keys(events) == ["goal_no_ticket:g-9b2e4d"]


# --- heartbeat rule: with `slots`, a live work pid no longer excuses staleness -

def test_stale_slot_heartbeat_reports_even_with_live_work_pid(tmp_path):
    patch, _ = _patch_state(tmp_path)
    now = 10_000.0
    with patch:
        hb = _new_hb("g-7f3a1c")
        _heartbeat_file(now, age=HB_TIMEOUT + 100.0, doc=hb)
        _goal("g-7f3a1c", now, age=60.0)  # well inside the Goal timeout
        events = wd.collect_stall_events(
            now, broker_pids=[BROKER_PID], work_pids=[222], heartbeat=hb, goal_pids={"g-7f3a1c": [222]}
        )
    assert _keys(events) == ["heartbeat_stale"]


def test_legacy_stale_heartbeat_with_live_work_pid_is_excused(tmp_path):
    patch, _ = _patch_state(tmp_path)
    now = 10_000.0
    with patch:
        legacy = {"pid": BROKER_PID}
        _heartbeat_file(now, age=HB_TIMEOUT + 100.0, doc=legacy)
        _goal("g-7f3a1c", now, age=60.0)
        events = wd.collect_stall_events(
            now, broker_pids=[BROKER_PID], work_pids=[222], heartbeat=legacy, goal_pids={"*": [222]}
        )
    assert _keys(events) == []


def test_missing_heartbeat_reports_no_heartbeat(tmp_path):
    patch, _ = _patch_state(tmp_path)
    now = 10_000.0
    with patch:
        events = wd.collect_stall_events(
            now, broker_pids=[BROKER_PID], work_pids=[], heartbeat={}, goal_pids={}
        )
        assert _keys(events) == ["no_heartbeat"]
        excused = wd.collect_stall_events(
            now, broker_pids=[BROKER_PID], work_pids=[1], heartbeat={}, goal_pids={"*": [1]}
        )
    assert _keys(excused) == []  # legacy rule: a live worker still excuses it


def test_read_heartbeat_tolerates_garbage(tmp_path):
    with mock.patch.object(wd, "HEARTBEAT", tmp_path / "missing.json"):
        assert wd.read_heartbeat() == {}
    p = tmp_path / "broken.json"
    p.write_text("[1, 2]", encoding="utf-8")
    with mock.patch.object(wd, "HEARTBEAT", p):
        assert wd.read_heartbeat() == {}
    p.write_text(json.dumps({"slots": [], "queued": []}), encoding="utf-8")
    with mock.patch.object(wd, "HEARTBEAT", p):
        assert wd.read_heartbeat()["slots"] == []


# --- work pid -> Goal attribution --------------------------------------------

def test_proc_goal_id_prefers_env_then_cmdline_ticket(tmp_path):
    patch, _ = _patch_state(tmp_path)
    pid = os.getpid()
    with patch:
        # 1. DSH_TRIAL_GOAL in the work process env wins outright.
        assert wd._proc_goal_id(pid, [], b"PATH=/bin\0DSH_TRIAL_GOAL=g-env99\0") == "g-env99"
        # 2. No env for this pid: the cmdline ``--ticket`` is matched against the
        #    running Goal ids (substring either way).
        env = b"PATH=/bin\0"
        with mock.patch.object(wd, "_arg_value", return_value="ticket for g-abc123"):
            assert wd._proc_goal_id(pid, ["g-abc123"], env) == "g-abc123"
        with mock.patch.object(wd, "_arg_value", return_value="g-abc123"):
            assert wd._proc_goal_id(pid, ["g-abc123"], env) == "g-abc123"
        # 3. Unrelated or missing ticket: unattributable.
        with mock.patch.object(wd, "_arg_value", return_value=None):
            assert wd._proc_goal_id(pid, ["g-abc123"], env) == "*"
        with mock.patch.object(wd, "_arg_value", return_value="something-else"):
            assert wd._proc_goal_id(pid, ["g-abc123"], env) == "*"
        with mock.patch.object(wd, "_arg_value", return_value=""):
            assert wd._proc_goal_id(pid, ["g-abc123"], env) == "*"


def test_work_pid_goals_groups_unattributable_pids_under_star(tmp_path):
    patch, _ = _patch_state(tmp_path)
    with patch:
        with mock.patch.object(wd, "_proc_goal_id", side_effect=["g-7f3a1c", "g-7f3a1c", "*"]):
            assert wd.work_pid_goals([1, 2, 3]) == {"g-7f3a1c": [1, 2], "*": [3]}


# --- freeze / grace rounds still drop clock-based stalls ----------------------

def test_freeze_round_still_suppresses_goal_no_ticket_and_stale_heartbeat(tmp_path):
    patch, _ = _patch_state(tmp_path)
    now = 100_000.0
    with patch:
        hb = _new_hb("g-9b2e4d")
        _heartbeat_file(now, age=HB_TIMEOUT + 500.0, doc=hb)
        _goal("g-9b2e4d", now)
        wd.STATE_FILE.write_text(
            json.dumps({"wall": now - 5.0, "mono": now - 5.0, "boot": now, "interval": 60.0}),
            encoding="utf-8",
        )
        seen: dict = {}
        real_collect = wd.collect_stall_events

        def _spy(*args, **kwargs):
            seen.update(kwargs)
            return real_collect(*args, **kwargs)

        with mock.patch.object(wd, "collect_stall_events", side_effect=_spy), \
                mock.patch.object(wd, "collect_freeze_events", return_value=[{"reason_key": "resumed-after-freeze"}]), \
                mock.patch.object(wd, "notify_event", return_value={"sent": True}), \
                mock.patch.object(wd, "list_broker_pids", return_value=[BROKER_PID]), \
                mock.patch.object(wd, "list_work_pids", return_value=[222]):
            result = wd.run_once(
                now=now, now_mono=now, dedupe=wd.Deduper(cooldown=0), dry_run=True
            )

    assert result["grace_active"] is True
    assert result["freeze"] is True
    assert _keys(result["events"]) == ["resumed-after-freeze"]
    # run_once now surfaces the slot/goal-pid view it judged with.
    assert result["slots"] == hb["slots"]
    assert result["goal_pids"] == {"*": [222]}  # real probe: pytest pid has no env


def test_run_once_reports_slots_and_goal_pids(tmp_path):
    patch, _ = _patch_state(tmp_path)
    now = 100_000.0
    with patch:
        hb = _new_hb("g-7f3a1c")
        _heartbeat_file(now, age=5.0, doc=hb)
        _goal("g-7f3a1c", now, age=60.0)
        with mock.patch.object(wd, "notify_event", return_value={"sent": True}), \
                mock.patch.object(wd, "list_broker_pids", return_value=[BROKER_PID]), \
                mock.patch.object(wd, "list_work_pids", return_value=[]):
            result = wd.run_once(
                now=now, now_mono=now, dedupe=wd.Deduper(cooldown=0), dry_run=True
            )
    assert _keys(result["events"]) == []
    assert result["slots"] == hb["slots"]
    assert result["goal_pids"] == {}


# --- the freeze classifier keeps covering the new reason keys -----------------

def test_clock_stall_classifier_covers_goal_no_ticket_and_stale_heartbeat():
    assert wd.is_clock_related_stall({"reason_key": "goal_no_ticket:g-9b2e4d"}) is True
    assert wd.is_clock_related_stall({"reason_key": "heartbeat_stale"}) is True
    assert wd.is_clock_related_stall({"reason_key": "no_broker_process"}) is False
