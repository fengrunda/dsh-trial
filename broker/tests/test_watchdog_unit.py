"""Addendum ②/③ — standalone watchdog checks, dedup and freeze detection."""
from __future__ import annotations

import importlib.util
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_watchdog_mod", ROOT / "trial-watchdog.py")
wd = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wd)

import trial_lib as T  # noqa: E402


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


# --- stall checks ------------------------------------------------------------

def test_no_broker_process_stalls(tmp_path):
    patch, _ = _patch_state(tmp_path)
    with patch:
        events = wd.collect_stall_events(time.time(), broker_pids=[], work_pids=[])
    keys = {e["reason_key"] for e in events}
    assert "no_broker_process" in keys


def test_heartbeat_missing_stalls(tmp_path):
    patch, _ = _patch_state(tmp_path)
    with patch:
        events = wd.collect_stall_events(time.time(), broker_pids=[111], work_pids=[])
    assert {e["reason_key"] for e in events} == {"no_heartbeat"}


def test_heartbeat_stale_stalls_but_fresh_does_not(tmp_path):
    patch, _ = _patch_state(tmp_path)
    with patch:
        hb = wd.HEARTBEAT
        hb.write_text("{}", encoding="utf-8")
        old = time.time() - 10000
        os.utime(hb, (old, old))
        stale = wd.collect_stall_events(time.time(), broker_pids=[111], work_pids=[])
        # fresh heartbeat: no heartbeat event.
        hb.write_text("{}", encoding="utf-8")
        fresh = wd.collect_stall_events(time.time(), broker_pids=[111], work_pids=[])
    assert "heartbeat_stale" in {e["reason_key"] for e in stale}
    assert "heartbeat_stale" not in {e["reason_key"] for e in fresh}


def test_live_work_process_suppresses_stale_heartbeat(tmp_path):
    patch, _ = _patch_state(tmp_path)
    with patch:
        hb = wd.HEARTBEAT
        hb.write_text("{}", encoding="utf-8")
        old = time.time() - 10000
        os.utime(hb, (old, old))
        events = wd.collect_stall_events(time.time(), broker_pids=[111], work_pids=[222])
    assert "heartbeat_stale" not in {e["reason_key"] for e in events}


def test_goal_without_ticket_stalls(tmp_path):
    patch, dirs = _patch_state(tmp_path)
    goal = {
        "goal": "g1",
        "status": "running",
        "updated_at": (datetime.now() - timedelta(hours=2)).isoformat(),
    }
    (dirs["goals"] / "g1.json").write_text(json.dumps(goal), encoding="utf-8")
    with patch:
        hb = wd.HEARTBEAT
        hb.write_text("{}", encoding="utf-8")
        events = wd.collect_stall_events(time.time(), broker_pids=[111], work_pids=[])
    assert any(e["reason_key"] == "goal_no_ticket:g1" for e in events)


def test_goal_with_open_ticket_does_not_stall(tmp_path):
    patch, dirs = _patch_state(tmp_path)
    goal = {
        "goal": "g1",
        "status": "running",
        "updated_at": (datetime.now() - timedelta(hours=2)).isoformat(),
    }
    (dirs["goals"] / "g1.json").write_text(json.dumps(goal), encoding="utf-8")
    (dirs["processing"] / "live-job.json").write_text("{}", encoding="utf-8")
    with patch:
        hb = wd.HEARTBEAT
        hb.write_text("{}", encoding="utf-8")
        events = wd.collect_stall_events(time.time(), broker_pids=[111], work_pids=[])
    assert not any(e["reason_key"].startswith("goal_no_ticket") for e in events)


# --- notify + dedup ----------------------------------------------------------

def test_run_once_calls_notify(tmp_path):
    patch, _ = _patch_state(tmp_path)
    event = {"kind": "dsh-trial-stalled", "status": "stalled", "goal": "*",
             "reason": "dead", "suggested_action": "restart", "reason_key": "dead"}
    with patch, mock.patch.object(wd, "collect_stall_events", return_value=[event]), \
            mock.patch.object(wd, "collect_freeze_events", return_value=[]), \
            mock.patch.object(wd, "collect_session_idle_events", return_value=([], {})), \
            mock.patch.object(wd, "list_broker_pids", return_value=[]), \
            mock.patch.object(wd, "list_work_pids", return_value=[]), \
            mock.patch.object(wd, "notify_event", return_value={"sent": True}) as m:
        result = wd.run_once(now=1000, dedupe=wd.Deduper(cooldown=900))
    assert m.call_count == 1
    assert result["sent"] and result["sent"][0]["reason_key"] == "dead"


def test_run_once_dedupes_same_reason(tmp_path):
    patch, _ = _patch_state(tmp_path)
    event = {"kind": "dsh-trial-stalled", "status": "stalled", "goal": "*",
             "reason": "dead", "suggested_action": "restart", "reason_key": "dead"}
    dedupe = wd.Deduper(cooldown=900)
    with patch, mock.patch.object(wd, "collect_stall_events", return_value=[event]), \
            mock.patch.object(wd, "collect_freeze_events", return_value=[]), \
            mock.patch.object(wd, "collect_session_idle_events", return_value=([], {})), \
            mock.patch.object(wd, "list_broker_pids", return_value=[]), \
            mock.patch.object(wd, "list_work_pids", return_value=[]), \
            mock.patch.object(wd, "notify_event", return_value={"sent": True}) as m:
        wd.run_once(now=1000, dedupe=dedupe)
        second = wd.run_once(now=1001, dedupe=dedupe)
    assert m.call_count == 1
    assert second["suppressed"] == ["dead"]


# --- freeze detection --------------------------------------------------------

def test_freeze_detected_once(tmp_path):
    state = tmp_path / "watchdog.state.json"
    # First run establishes the baseline.
    assert wd.collect_freeze_events(1000.0, 100.0, state_path=state) == []
    # Wall jumped 590s while monotonic advanced 10s.
    events = wd.collect_freeze_events(1600.0, 110.0, state_path=state)
    assert len(events) == 1
    assert events[0]["status"] == "resumed-after-freeze"
    assert events[0]["goal"] == "*"
    assert "9.8" in events[0]["reason"] or "9.7" in events[0]["reason"]
    # Immediately after, clocks agree again: no repeat.
    assert wd.collect_freeze_events(1605.0, 115.0, state_path=state) == []


def test_freeze_helper_threshold():
    assert T.detect_resumed_after_freeze(0, 0, 600, 10, threshold_sec=300) == 590.0
    assert T.detect_resumed_after_freeze(0, 0, 100, 90, threshold_sec=300) is None
    # Clock moving backwards is not a freeze.
    assert T.detect_resumed_after_freeze(1000, 100, 900, 110, threshold_sec=300) is None


# --- list_work_pids: only this broker's own work processes -------------------

def _patch_ownership(work_pids, broker_pids, descendants, guarded=()):
    """Fake ps / broker-pid / descendant / environ lookups for list_work_pids."""
    guarded = set(guarded)
    return mock.patch.multiple(
        wd,
        _ps_pids=mock.Mock(return_value=list(work_pids)),
        list_broker_pids=mock.Mock(return_value=list(broker_pids)),
        _descendant_pids=mock.Mock(
            side_effect=lambda pid: list(descendants.get(pid, []))
        ),
        _proc_env_has=mock.Mock(
            side_effect=lambda pid, key, value, environ=None: (
                key == "DSH_TRIAL_GUARD" and value == "1" and pid in guarded
            )
        ),
    )


def test_list_work_pids_keeps_broker_descendants():
    with _patch_ownership(
        work_pids=[101, 202, 303],
        broker_pids=[10],
        descendants={10: [101, 202]},
    ):
        assert wd.list_work_pids() == [101, 202]


def test_list_work_pids_drops_foreign_unguarded_pid():
    # 303 is another agent's dsh-acp-ask: no broker ancestry, no guard.
    with _patch_ownership(
        work_pids=[101, 303],
        broker_pids=[10],
        descendants={10: [101]},
        guarded=[],
    ):
        assert wd.list_work_pids() == [101]


def test_list_work_pids_keeps_guarded_orphan():
    # 404 was reparented to init after a broker crash but kept the guard env.
    with _patch_ownership(
        work_pids=[101, 404],
        broker_pids=[10],
        descendants={10: [101]},
        guarded=[404],
    ):
        assert wd.list_work_pids() == [101, 404]


def test_list_work_pids_without_broker_keeps_only_guarded():
    with _patch_ownership(
        work_pids=[202, 404, 505],
        broker_pids=[],
        descendants={},
        guarded=[404],
    ):
        assert wd.list_work_pids() == [404]


def test_proc_env_has_matches_exact_chunk():
    env = b"PATH=/bin\0DSH_TRIAL_GUARD=1\0DSH_TRIAL_GOAL=g1"
    assert wd._proc_env_has(1, "DSH_TRIAL_GUARD", "1", environ=env) is True
    # Wrong value, prefix look-alike and embedded occurrence all fail.
    assert wd._proc_env_has(1, "DSH_TRIAL_GUARD", "0", environ=env) is False
    assert wd._proc_env_has(1, "DSH_TRIAL_GUARD", "1", environ=b"DSH_TRIAL_GUARD=10\0") is False
    assert wd._proc_env_has(1, "DSH_TRIAL_GUARD", "1", environ=b"X=DSH_TRIAL_GUARD=1\0") is False


def test_proc_env_has_unreadable_pid_is_false():
    # A pid that cannot exist: /proc lookup fails, so no match is claimed.
    assert wd._proc_env_has(2**31 - 1, "DSH_TRIAL_GUARD", "1") is False
