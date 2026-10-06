"""Session-idle stall detection + dry-run-never-writes-state (A1/A2).

Fully tmp-isolated: state/heartbeat/goals and the ACP homes root are patched to
``tmp_path`` and discovery is stubbed, so no real ``~/.dsh`` or live ticket
``/proc`` entry is ever touched.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_watchdog_mod", ROOT / "trial-watchdog.py")
wd = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wd)


def _patch_state(tmp_path: Path):
    dirs = {n: tmp_path / n for n in ("goals", "inbox", "processing")}
    for p in dirs.values():
        p.mkdir(parents=True, exist_ok=True)
    (tmp_path / "logs").mkdir(parents=True, exist_ok=True)
    (tmp_path / "homes").mkdir(parents=True, exist_ok=True)
    return mock.patch.multiple(
        wd,
        STATE_DIR=tmp_path,
        PIDFILE=tmp_path / "trial-broker.pid",
        HEARTBEAT=tmp_path / "trial-broker.heartbeat.json",
        STATE_FILE=tmp_path / "trial-watchdog.state.json",
        GOALS=dirs["goals"],
        INBOX=dirs["inbox"],
        PROCESSING=dirs["processing"],
        HOMES_ROOT=tmp_path / "homes",
        OPEN_SLICE_LOG_DIR=tmp_path / "logs",
    ), dirs


def _seed_state(path: Path) -> bytes:
    doc = {
        "wall": 111.0,
        "mono": 222.0,
        "at": "2020-01-01T00:00:00+00:00",
        "session_idle": {
            "impl-x": {"sig": "1:1", "since_mono": 5.0, "since_at": "2020-01-01T00:00:00+00:00"}
        },
    }
    path.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path.read_bytes()


# --- A1: dry-run never writes state -----------------------------------------

def test_run_once_dry_run_never_writes_state(tmp_path):
    patch, _ = _patch_state(tmp_path)
    with patch:
        state = wd.STATE_FILE
        before = _seed_state(state)
        before_mtime = state.stat().st_mtime_ns
        with mock.patch.object(wd, "list_broker_pids", return_value=[]), \
                mock.patch.object(wd, "list_work_pids", return_value=[]), \
                mock.patch.object(wd, "notify_event", return_value={"sent": False}):
            wd.run_once(now=999.0, now_mono=999.0, dry_run=True,
                        dedupe=wd.Deduper(cooldown=0))
        assert state.read_bytes() == before
        assert state.stat().st_mtime_ns == before_mtime


def test_main_once_dry_run_never_writes_state(tmp_path, capsys):
    patch, _ = _patch_state(tmp_path)
    with patch:
        state = wd.STATE_FILE
        before = _seed_state(state)
        before_mtime = state.stat().st_mtime_ns
        with mock.patch.object(sys, "argv", ["trial-watchdog.py", "--once", "--dry-run"]), \
                mock.patch.object(wd, "list_broker_pids", return_value=[]), \
                mock.patch.object(wd, "list_work_pids", return_value=[]), \
                mock.patch.object(wd, "notify_event", return_value={"sent": False}):
            rc = wd.main()
        assert rc == 0
        assert state.read_bytes() == before
        assert state.stat().st_mtime_ns == before_mtime
    printed = json.loads(capsys.readouterr().out)
    assert printed["events"] and all(
        e["notify"] == {"sent": False} for e in printed["sent"]
    )


def test_freeze_write_preserves_session_idle(tmp_path):
    state = tmp_path / "watchdog.state.json"
    _seed_state(state)
    wd.collect_freeze_events(2000.0, 200.0, state_path=state)
    doc = json.loads(state.read_text(encoding="utf-8"))
    assert doc["wall"] == 2000.0
    assert doc["mono"] == 200.0
    assert doc["session_idle"]["impl-x"]["sig"] == "1:1"
    assert doc["at"]


def test_run_once_writes_freeze_and_session_idle(tmp_path):
    patch, _ = _patch_state(tmp_path)
    with patch:
        log = wd.OPEN_SLICE_LOG_DIR / "impl-x.log"
        log.write_text("hello", encoding="utf-8")
        wd.STATE_FILE.write_text(json.dumps({
            "wall": 1.0, "mono": 1.0,
            "session_idle": {"impl-x": {"sig": "stale", "since_mono": 0.0, "since_at": "x"}},
        }) + "\n", encoding="utf-8")
        with mock.patch.object(wd, "list_broker_pids", return_value=[]), \
                mock.patch.object(wd, "list_work_pids", return_value=[123]), \
                mock.patch.object(wd, "discover_session_outputs",
                                  return_value={"impl-x": [log]}), \
                mock.patch.object(wd, "collect_stall_events", return_value=[]), \
                mock.patch.object(wd, "collect_freeze_events", return_value=[]), \
                mock.patch.object(wd, "notify_event", return_value={"sent": False}):
            wd.run_once(now=5000.0, now_mono=50.0, dedupe=wd.Deduper(cooldown=0))
        doc = json.loads(wd.STATE_FILE.read_text(encoding="utf-8"))
    assert doc["wall"] == 5000.0
    assert doc["mono"] == 50.0
    # sig changed from the seeded "stale" -> baseline reset to the new now_mono,
    # while the freeze fields survive the session-idle write.
    assert doc["session_idle"]["impl-x"]["since_mono"] == 50.0


# --- A2: monotonic session-idle accounting -----------------------------------

def _stub_outputs(path: Path):
    return mock.patch.object(wd, "discover_session_outputs",
                             return_value={"impl-x": [path]})


def test_session_idle_event_after_threshold(tmp_path, monkeypatch):
    monkeypatch.setenv("TRIAL_WATCHDOG_SESSION_IDLE_SEC", "60")
    patch, _ = _patch_state(tmp_path)
    with patch:
        log = wd.OPEN_SLICE_LOG_DIR / "impl-x.log"
        log.write_text("hello", encoding="utf-8")
        with _stub_outputs(log):
            events, state = wd.collect_session_idle_events([123], 0.0, {})
            assert events == []
            assert state["impl-x"]["since_mono"] == 0.0
            events, state = wd.collect_session_idle_events([123], 61.0, state)
    assert len(events) == 1
    assert events[0]["reason_key"] == "session_idle:impl-x"
    assert events[0]["goal"] == "impl-x"
    assert "idle" in events[0]["reason"]
    assert "61" in events[0]["reason"] and "60" in events[0]["reason"]


def test_session_idle_signature_change_resets(tmp_path, monkeypatch):
    monkeypatch.setenv("TRIAL_WATCHDOG_SESSION_IDLE_SEC", "60")
    patch, _ = _patch_state(tmp_path)
    with patch:
        log = wd.OPEN_SLICE_LOG_DIR / "impl-x.log"
        log.write_text("a", encoding="utf-8")
        with _stub_outputs(log):
            _, state = wd.collect_session_idle_events([1], 0.0, {})
            log.write_text("bb", encoding="utf-8")  # signature changes
            events, state = wd.collect_session_idle_events([1], 100.0, state)
    assert events == []
    assert state["impl-x"]["since_mono"] == 100.0


def test_session_idle_below_threshold_no_event(tmp_path, monkeypatch):
    monkeypatch.setenv("TRIAL_WATCHDOG_SESSION_IDLE_SEC", "60")
    patch, _ = _patch_state(tmp_path)
    with patch:
        log = wd.OPEN_SLICE_LOG_DIR / "impl-x.log"
        log.write_text("hello", encoding="utf-8")
        with _stub_outputs(log):
            _, state = wd.collect_session_idle_events([1], 0.0, {})
            events, state = wd.collect_session_idle_events([1], 30.0, state)
    assert events == []
    assert state["impl-x"]["since_mono"] == 0.0


def test_session_idle_ignores_wall_clock_jump(tmp_path, monkeypatch):
    monkeypatch.setenv("TRIAL_WATCHDOG_SESSION_IDLE_SEC", "60")
    patch, _ = _patch_state(tmp_path)
    with patch:
        log = wd.OPEN_SLICE_LOG_DIR / "impl-x.log"
        log.write_text("hello", encoding="utf-8")
        with _stub_outputs(log):
            sig = wd.output_signature([log])
            assert sig is not None
            # Wall clock +10000s but monotonic advanced only 10s.
            events, _ = wd.collect_session_idle_events(
                [1], 10.0, {"impl-x": {"sig": sig, "since_mono": 0.0, "since_at": "x"}}
            )
    assert events == []


def test_run_once_session_idle_ignores_wall_clock_jump(tmp_path, monkeypatch):
    monkeypatch.setenv("TRIAL_WATCHDOG_SESSION_IDLE_SEC", "60")
    patch, _ = _patch_state(tmp_path)
    with patch:
        log = wd.OPEN_SLICE_LOG_DIR / "impl-x.log"
        log.write_text("hello", encoding="utf-8")
        sig = wd.output_signature([log])
        wd.STATE_FILE.write_text(json.dumps({
            "wall": 0.0, "mono": 0.0,
            "session_idle": {"impl-x": {"sig": sig, "since_mono": 0.0, "since_at": "x"}},
        }) + "\n", encoding="utf-8")
        with mock.patch.object(wd, "list_broker_pids", return_value=[1]), \
                mock.patch.object(wd, "list_work_pids", return_value=[1]), \
                mock.patch.object(wd, "discover_session_outputs",
                                  return_value={"impl-x": [log]}), \
                mock.patch.object(wd, "collect_stall_events", return_value=[]), \
                mock.patch.object(wd, "collect_freeze_events", return_value=[]), \
                mock.patch.object(wd, "notify_event", return_value={"sent": False}):
            result = wd.run_once(now=10000.0, now_mono=10.0, dedupe=wd.Deduper(cooldown=0))
    assert not any(str(e.get("reason_key", "")).startswith("session_idle")
                   for e in result["events"])


def test_no_work_pids_clears_session_idle_state():
    events, state = wd.collect_session_idle_events(
        [], 100.0, {"impl-x": {"sig": "1:1", "since_mono": 0.0}}
    )
    assert events == []
    assert state == {}


# --- discovery / cmdline parsing ---------------------------------------------

def test_discover_cmdline_ticket_uses_default_log(tmp_path):
    patch, _ = _patch_state(tmp_path)
    with patch:
        log = wd.OPEN_SLICE_LOG_DIR / "impl-x.log"
        log.write_text("x", encoding="utf-8")
        argv = ["open-slice.sh", "--ticket", "impl-x", "--pack", "p"]
        with mock.patch.object(wd, "_read_proc_cmdline", return_value=argv), \
                mock.patch.object(wd, "_descendant_pids", return_value=[]), \
                mock.patch.object(wd, "_fd_targets", return_value=[]):
            out = wd.discover_session_outputs([123])
    assert list(out) == ["impl-x"]
    assert out["impl-x"] == [log]


def test_discover_cmdline_explicit_log_wins(tmp_path):
    patch, _ = _patch_state(tmp_path)
    with patch:
        xlog = tmp_path / "x.log"
        xlog.write_text("x", encoding="utf-8")
        argv = ["open-slice.sh", "--ticket", "impl-x", "--log", str(xlog)]
        with mock.patch.object(wd, "_read_proc_cmdline", return_value=argv), \
                mock.patch.object(wd, "_descendant_pids", return_value=[]), \
                mock.patch.object(wd, "_fd_targets", return_value=[]):
            out = wd.discover_session_outputs([123])
    assert out["impl-x"] == [xlog]


def test_discover_fd_session_artifact(tmp_path):
    patch, _ = _patch_state(tmp_path)
    with patch:
        sess = tmp_path / "sessions" / "abc.json"
        sess.parent.mkdir(parents=True, exist_ok=True)
        sess.write_text("{}", encoding="utf-8")
        argv = ["dsh-acp-ask.py", "--ticket", "impl-x"]
        with mock.patch.object(wd, "_read_proc_cmdline", return_value=argv), \
                mock.patch.object(wd, "_descendant_pids", return_value=[]), \
                mock.patch.object(wd, "_fd_targets", return_value=[str(sess)]):
            out = wd.discover_session_outputs([123])
    assert sess in out["impl-x"]


def test_discover_fallback_newest_session(tmp_path):
    patch, _ = _patch_state(tmp_path)
    with patch:
        sessions = wd.HOMES_ROOT / "impl" / wd.SESSION_PROJCACHE_SESSIONS
        sessions.mkdir(parents=True, exist_ok=True)
        old = sessions / "old.json"
        new = sessions / "new.json"
        old.write_text("{}", encoding="utf-8")
        new.write_text("{}", encoding="utf-8")
        os.utime(old, (1000, 1000))
        os.utime(new, (2000, 2000))
        # --ticket without --log => default log path does not exist => fallback.
        argv = ["dsh-acp-ask.py", "--ticket", "impl-x"]
        with mock.patch.object(wd, "_read_proc_cmdline", return_value=argv), \
                mock.patch.object(wd, "_descendant_pids", return_value=[]), \
                mock.patch.object(wd, "_fd_targets", return_value=[]):
            out = wd.discover_session_outputs([123])
    assert out == {"fallback:sessions": [new]}


def test_output_signature_none_when_missing(tmp_path):
    assert wd.output_signature([tmp_path / "nope.log"]) is None
