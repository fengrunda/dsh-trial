"""Idle-ask kill guard for trial-watchdog (impl-watchdog-kill-idle-ask).

Covers: the enable switch (default on, env off), matching a ``session_idle:``
key back to process pids, the SIGTERM->SIGKILL reaper, and the ``run_once``
wiring (dry-run never kills, enabled kills the idle ticket tree).

Fully mock-isolated: no real process is ever signaled and no state file is
written (``write_watchdog_state`` is patched out).
"""
from __future__ import annotations

import contextlib
import importlib.util
import signal
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_watchdog_mod", ROOT / "trial-watchdog.py")
wd = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wd)


# --- enable switch ----------------------------------------------------------

def test_kill_idle_ask_enabled_by_default(monkeypatch):
    monkeypatch.delenv("TRIAL_WATCHDOG_KILL_IDLE_ASK", raising=False)
    assert wd.kill_idle_ask_enabled() is True


def test_kill_idle_ask_env_off(monkeypatch):
    for value in ("0", "false", "False", "off", "OFF", "no", " no "):
        monkeypatch.setenv("TRIAL_WATCHDOG_KILL_IDLE_ASK", value)
        assert wd.kill_idle_ask_enabled() is False, value


def test_kill_idle_ask_env_on_values(monkeypatch):
    for value in ("1", "true", "yes", "on", "anything", ""):
        monkeypatch.setenv("TRIAL_WATCHDOG_KILL_IDLE_ASK", value)
        assert wd.kill_idle_ask_enabled() is True, value


# --- key -> pids ------------------------------------------------------------

def test_pids_for_pid_key_includes_descendants(monkeypatch):
    monkeypatch.setattr(wd, "_descendant_pids", lambda pid, **k: [pid + 1])
    assert wd.pids_for_session_idle_key("session_idle:pid:123", [123, 9]) == [123, 124]


def test_pids_for_pid_key_unknown_pid_is_empty(monkeypatch):
    monkeypatch.setattr(wd, "_descendant_pids", lambda pid, **k: [pid + 1])
    assert wd.pids_for_session_idle_key("pid:777", [123]) == []


def test_pids_for_ticket_key_matches_cmdline(monkeypatch):
    cmds = {
        10: ["open-slice.sh", "--ticket", "impl-x"],
        11: ["dsh-acp-ask.py", "--ticket", "other"],
        12: ["dsh-acp-ask.py", "--goal", "impl-x"],
    }
    monkeypatch.setattr(wd, "_read_proc_cmdline", lambda pid: cmds.get(pid, []))
    monkeypatch.setattr(wd, "_descendant_pids", lambda pid, **k: [99] if pid == 10 else [])
    assert wd.pids_for_session_idle_key("session_idle:impl-x", [10, 11, 12]) == [10, 99, 12]


def test_pids_for_fallback_and_empty_keys_are_empty():
    assert wd.pids_for_session_idle_key("session_idle:fallback:abc", [1, 2]) == []
    assert wd.pids_for_session_idle_key("", [1, 2]) == []
    assert wd.pids_for_session_idle_key("session_idle:pid:notanint", [1]) == []


# --- SIGTERM then SIGKILL ---------------------------------------------------

def test_kill_idle_ask_pids_sigterm_then_sigkill():
    calls: list[tuple[int, int]] = []

    def killfn(pid, sig):
        calls.append((pid, sig))

    out = wd.kill_idle_ask_pids([5, 5, 6], killfn=killfn)
    assert out == [5, 6]
    assert (5, signal.SIGTERM) in calls
    assert (6, signal.SIGTERM) in calls
    assert (5, signal.SIGKILL) in calls
    assert (6, signal.SIGKILL) in calls


def test_kill_idle_ask_pids_swallows_kill_errors():
    def killfn(pid, sig):
        raise ProcessLookupError(pid)

    assert wd.kill_idle_ask_pids([123], killfn=killfn) == []


# --- run_once wiring --------------------------------------------------------

def _idle_event(key: str = "impl-x") -> dict:
    return {
        "kind": wd.STALLED_KIND,
        "status": "stalled",
        "goal": "impl-x",
        "reason": "live ticket but session/output idle 2000s",
        "reason_key": f"session_idle:{key}",
        "suggested_action": "疑似 ACP 挂起",
    }


def _base_patches(work_pids=(123,)):
    return [
        mock.patch.object(wd, "list_broker_pids", return_value=[]),
        mock.patch.object(wd, "list_work_pids", return_value=list(work_pids)),
        mock.patch.object(wd, "read_watchdog_state", return_value={}),
        mock.patch.object(wd, "write_watchdog_state"),
        mock.patch.object(wd, "boottime_now", return_value=0.0),
        mock.patch.object(wd, "collect_freeze_events", return_value=[]),
        mock.patch.object(wd, "collect_stall_events", return_value=[]),
        mock.patch.object(wd, "read_heartbeat", return_value={}),
        mock.patch.object(wd, "work_pid_goals", return_value={}),
        mock.patch.object(wd, "notify_event", return_value={"sent": False}),
    ]


def _run_once(patches, event, **kwargs):
    with contextlib.ExitStack() as stack:
        for patch in patches:
            stack.enter_context(patch)
        stack.enter_context(
            mock.patch.object(wd, "collect_session_idle_events", return_value=([event], {}))
        )
        return wd.run_once(now=1000.0, now_mono=100.0, dedupe=wd.Deduper(cooldown=0), **kwargs)


def test_run_once_dry_run_does_not_kill(monkeypatch):
    monkeypatch.delenv("TRIAL_WATCHDOG_KILL_IDLE_ASK", raising=False)
    event = _idle_event()
    with mock.patch.object(wd, "pids_for_session_idle_key", return_value=[123]) as pf, \
            mock.patch.object(wd, "kill_idle_ask_pids") as kf:
        _run_once(_base_patches(), event, dry_run=True)
    pf.assert_not_called()
    kf.assert_not_called()
    assert event["killed_pids"] == []


def test_run_once_enabled_kills_idle_pids(monkeypatch):
    monkeypatch.delenv("TRIAL_WATCHDOG_KILL_IDLE_ASK", raising=False)
    event = _idle_event()
    with mock.patch.object(wd, "pids_for_session_idle_key", return_value=[123, 124]) as pf, \
            mock.patch.object(wd, "kill_idle_ask_pids", return_value=[123, 124]) as kf:
        _run_once(_base_patches(), event)
    pf.assert_called_once_with("session_idle:impl-x", [123])
    kf.assert_called_once_with([123, 124])
    assert event["killed_pids"] == [123, 124]
    assert "123,124" in event["suggested_action"]


def test_run_once_disabled_does_not_kill(monkeypatch):
    monkeypatch.setenv("TRIAL_WATCHDOG_KILL_IDLE_ASK", "0")
    event = _idle_event()
    with mock.patch.object(wd, "pids_for_session_idle_key", return_value=[123]) as pf, \
            mock.patch.object(wd, "kill_idle_ask_pids") as kf:
        _run_once(_base_patches(), event)
    pf.assert_not_called()
    kf.assert_not_called()
    assert event["killed_pids"] == []
    assert "已杀僵住进程" not in event["suggested_action"]
