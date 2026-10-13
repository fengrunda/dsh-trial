"""Freeze recovery must not double-report a wall-clock stall (impl-wd-freeze-grace).

A host freeze makes the wall clock jump while the monotonic clock barely moves,
so the same round that emits ``dsh-trial-resumed-after-freeze`` used to also
emit ``dsh-trial-stalled`` (heartbeat_stale) even though the broker renewed its
heartbeat seconds later. These tests pin the three freeze criteria and the
clock-stall grace window, all tmp-isolated with injected clocks.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_watchdog_freeze_mod", ROOT / "trial-watchdog.py")
wd = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wd)

FREEZE_SEC = 8.7 * 3600  # 31320 s — the 10/8 host freeze
BOOT_OFFSET = 400.0  # boottime tracks the monotonic clock in these tests
_AUTO = object()


@pytest.fixture(autouse=True)
def _pin_env(monkeypatch):
    """Deterministic knobs: no ambient env may shift the thresholds."""
    monkeypatch.setenv("DSH_TRIAL_FREEZE_JUMP_SEC", "300")
    monkeypatch.setenv("TRIAL_WATCHDOG_HEARTBEAT_SEC", "300")
    monkeypatch.setenv("TRIAL_WATCHDOG_FREEZE_GRACE_SEC", "120")


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


def _seed(state: Path, wall: float, mono: float, *, boot: float | None | object = _AUTO,
          interval: float | None = 60.0, loop_pid: int | None = None, **extra) -> None:
    doc = {
        "wall": wall,
        "mono": mono,
        "at": "2020-01-01T00:00:00+00:00",
        "loop_pid": os.getpid() if loop_pid is None else loop_pid,
    }
    if boot is _AUTO:
        boot = mono + BOOT_OFFSET  # boottime and monotonic agree before the freeze
    if boot is not None:
        doc["boot"] = boot
    if interval is not None:
        doc["interval"] = interval
    doc.update(extra)
    state.write_text(json.dumps(doc, ensure_ascii=False) + "\n", encoding="utf-8")


def _heartbeat(now: float, age: float) -> None:
    """Heartbeat file whose mtime is ``age`` seconds before ``now``."""
    hb = wd.HEARTBEAT
    hb.write_text("{}", encoding="utf-8")
    ts = now - age
    os.utime(hb, (ts, ts))


def _boot_for(mono: float) -> float:
    return mono + BOOT_OFFSET


def _run(now: float, now_mono: float, *, interval: float | None = 60.0,
         boot: float | None | object = _AUTO, dry_run: bool = False,
         broker_pids: list[int] | None = None) -> dict:
    """run_once with the process probes and the notifier stubbed out."""
    patches = [
        mock.patch.object(wd, "list_broker_pids", return_value=[4242] if broker_pids is None else broker_pids),
        mock.patch.object(wd, "list_work_pids", return_value=[]),
        mock.patch.object(wd, "notify_event", return_value={"sent": True}),
    ]
    if boot is not _AUTO:
        patches.append(mock.patch.object(wd, "boottime_now", return_value=boot))
    for p in patches:
        p.start()
    try:
        return wd.run_once(
            now=now, now_mono=now_mono, dedupe=wd.Deduper(cooldown=0),
            expected_interval=interval, dry_run=dry_run,
        )
    finally:
        for p in patches:
            p.stop()


def _keys(result: dict) -> list[str]:
    return [str(e.get("reason_key") or "") for e in result["events"]]


# --- (a) freeze round with a heartbeat the broker already renewed -------------

def test_freeze_wall_jump_with_fresh_heartbeat_only_reports_freeze(tmp_path):
    patch, _ = _patch_state(tmp_path)
    with patch:
        _seed(wd.STATE_FILE, 1000.0, 100.0)
        now, now_mono = 1000.0 + FREEZE_SEC, 160.0  # wall +8.7h, monotonic +60s
        _heartbeat(now, age=5.0)  # broker renewed 2s after the freeze ended
        result = _run(now, now_mono, boot=_boot_for(now_mono))

    assert result["freeze"] is True
    assert result["grace_active"] is True
    assert _keys(result) == ["resumed-after-freeze"]
    assert not any(k == "heartbeat_stale" for k in _keys(result))
    freeze = result["events"][0]
    assert freeze["kind"] == wd.FREEZE_KIND
    assert freeze["status"] == "resumed-after-freeze"
    assert freeze["goal"] == "*"
    assert freeze["criterion"] == "wall-mono"
    assert freeze["criteria"] == ["wall-mono"]
    # The drift is what the wall clock gained on the monotonic clock: 8.7h − 60s.
    assert freeze["jump_sec"] == pytest.approx(FREEZE_SEC - 60.0)
    assert "521.0 min" in freeze["reason"] and "wall-mono" in freeze["reason"]
    assert f"{wd.freeze_grace_sec(60.0):.0f}s grace" in freeze["reason"]


# --- (a2) the freeze round still holds the stale pre-freeze heartbeat --------

def test_freeze_round_stale_heartbeat_is_graced(tmp_path):
    patch, _ = _patch_state(tmp_path)
    with patch:
        state = wd.STATE_FILE
        _seed(state, 1000.0, 100.0)
        # Heartbeat last written 5s before the freeze began, never renewed since.
        _heartbeat(1000.0, age=5.0)
        now, now_mono = 1000.0 + FREEZE_SEC, 160.0
        # Sanity: that heartbeat is far past the stall threshold, so the grace
        # window (not freshness) is what keeps this round quiet.
        assert wd.heartbeat_age(now=now) > wd.heartbeat_timeout()
        first = _run(now, now_mono, boot=_boot_for(now_mono))
        assert first["freeze"] is True
        assert _keys(first) == ["resumed-after-freeze"]

        # Next round, still inside the grace window: the broker is back.
        now2, now_mono2 = now + 60.0, now_mono + 60.0
        _heartbeat(now2, age=5.0)
        second = _run(now2, now_mono2, boot=_boot_for(now_mono2))

        doc = json.loads(state.read_text(encoding="utf-8"))

    assert second["freeze"] is False
    assert second["grace_active"] is True
    assert _keys(second) == []
    assert doc["freeze_grace_until_mono"] == pytest.approx(
        now_mono + wd.freeze_grace_sec(60.0)
    )
    assert doc["loop_pid"] == os.getpid()
    assert doc["interval"] == pytest.approx(60.0)


# --- (b) past the grace window a heartbeat that never came back still stalls --

def test_heartbeat_stale_after_grace_window(tmp_path):
    patch, _ = _patch_state(tmp_path)
    with patch:
        _seed(wd.STATE_FILE, 1000.0, 100.0)
        _heartbeat(1000.0, age=5.0)
        freeze_wall, freeze_mono = 1000.0 + FREEZE_SEC, 160.0
        assert _run(freeze_wall, freeze_mono, boot=_boot_for(freeze_mono))["freeze"]

        # Walk past the grace window (floored at the loop interval and twice
        # the heartbeat timeout) in loop-sized steps: the never-recovered
        # heartbeat then surfaces as exactly one merged freeze_stalled instead
        # of a bare heartbeat_stale.
        grace = wd.freeze_grace_sec(60.0)
        now, now_mono = freeze_wall, freeze_mono
        result = None
        while now_mono < freeze_mono + grace:
            now += 60.0
            now_mono += 60.0
            result = _run(now, now_mono, boot=_boot_for(now_mono))

    assert result["freeze"] is False
    assert result["grace_active"] is False
    assert "heartbeat_stale" not in _keys(result)
    assert _keys(result).count("freeze_stalled") == 1
    stalled = next(e for e in result["events"] if e["reason_key"] == "freeze_stalled")
    assert stalled["kind"] == wd.STALLED_KIND


# --- (c) no clock jump: an expired heartbeat stalls exactly as before ---------

def test_heartbeat_stale_without_any_jump(tmp_path):
    patch, _ = _patch_state(tmp_path)
    with patch:
        _seed(wd.STATE_FILE, 1000.0, 100.0)
        _heartbeat(600.0, age=0.0)  # already 400s old at the baseline
        result = _run(1200.0, 300.0, boot=_boot_for(300.0))

    assert result["freeze"] is False
    assert result["grace_active"] is False
    assert "heartbeat_stale" in _keys(result)


# --- (d) one-round gap: same loop pid vs. a restarted watchdog ---------------

def test_loop_interval_criterion_freezes():
    # Same watchdog pid as the previous round, and the round took 800s while the
    # loop asked for 60s: 740s of that gap is unaccounted for.
    state = {"wall": 1000.0, "mono": 100.0, "boot": 500.0, "loop_pid": 4242}
    events = wd.collect_freeze_events(
        1000.0, 900.0, state=state, persist=False, boottime=500.0,
        loop_pid=4242, expected_interval=60.0,
    )
    assert len(events) == 1
    assert events[0]["criterion"] == "loop-interval"
    assert events[0]["criteria"] == ["loop-interval"]
    assert events[0]["jump_sec"] == pytest.approx(740.0)


def test_loop_interval_criterion_skipped_after_restart():
    state = {"wall": 1000.0, "mono": 100.0, "boot": 500.0, "loop_pid": 4242}
    # Same mono gap, but the previous round belonged to another watchdog pid
    # (our own downtime is not a freeze).
    assert wd.collect_freeze_events(
        1000.0, 900.0, state=state, persist=False, boottime=500.0,
        loop_pid=9999, expected_interval=60.0,
    ) == []
    # And without a loop interval (--once) the criterion is not armed at all.
    assert wd.collect_freeze_events(
        1000.0, 900.0, state=state, persist=False, boottime=500.0,
        loop_pid=4242, expected_interval=None,
    ) == []


# --- (e) dry-run freeze still writes nothing ---------------------------------

def test_dry_run_freeze_never_writes_state(tmp_path):
    patch, _ = _patch_state(tmp_path)
    with patch:
        state = wd.STATE_FILE
        _seed(state, 1000.0, 100.0)
        before = state.read_bytes()
        before_mtime = state.stat().st_mtime_ns
        _heartbeat(1000.0, age=-395.0)
        now, now_mono = 1000.0 + FREEZE_SEC, 160.0
        result = _run(now, now_mono, boot=_boot_for(now_mono), dry_run=True)

        assert state.read_bytes() == before
        assert state.stat().st_mtime_ns == before_mtime

    assert result["freeze"] is True
    assert result["grace_active"] is True
    assert _keys(result) == ["resumed-after-freeze"]


# --- grace scope: process death and wall-clock goal ages --------------------

def test_grace_keeps_no_broker_process_but_drops_heartbeat_stall(tmp_path):
    patch, _ = _patch_state(tmp_path)
    with patch:
        _seed(wd.STATE_FILE, 1000.0, 100.0, freeze_grace_until_mono=400.0)
        _heartbeat(1000.0, age=900.0)
        # A dead broker is reported even inside the grace window ...
        dead = _run(1100.0, 300.0, boot=_boot_for(300.0), broker_pids=[])
        # ... while the stale heartbeat of a live broker is not.
        alive = _run(1100.0, 300.0, boot=_boot_for(300.0), broker_pids=[4242])

    assert _keys(dead) == ["no_broker_process"]
    assert _keys(alive) == []
    assert dead["grace_active"] is True and dead["freeze"] is False
    assert alive["grace_active"] is True


def test_grace_drops_goal_no_ticket_then_reports_it(tmp_path):
    patch, dirs = _patch_state(tmp_path)
    goal = {
        "goal": "g1",
        "status": "running",
        # 1100s old on the injected wall clock of the first round.
        "updated_at": datetime.fromtimestamp(0, timezone.utc).isoformat(),
    }
    (dirs["goals"] / "g1.json").write_text(json.dumps(goal), encoding="utf-8")
    with patch:
        _seed(wd.STATE_FILE, 1000.0, 100.0, freeze_grace_until_mono=400.0)
        _heartbeat(1100.0, age=5.0)
        graced = _run(1100.0, 300.0, boot=_boot_for(300.0))
        _heartbeat(1500.0, age=5.0)
        after = _run(1500.0, 500.0, boot=_boot_for(500.0))

    assert "goal_no_ticket:g1" not in _keys(graced)
    assert "goal_no_ticket:g1" in _keys(after)


# --- criterion (b): boottime vs monotonic, and platforms without the clock ---

def test_boottime_criterion_fires_without_wall_jump():
    state = {"wall": 1000.0, "mono": 100.0, "boot": 500.0}
    events = wd.collect_freeze_events(
        1100.0, 200.0, state=state, persist=False, boottime=1000.0,
    )
    assert len(events) == 1
    assert events[0]["criterion"] == "boottime-mono"
    assert events[0]["jump_sec"] == pytest.approx(400.0)


def test_boottime_criterion_skipped_without_the_clock():
    state = {"wall": 1000.0, "mono": 100.0, "boot": 500.0}
    with mock.patch.object(wd, "boottime_now", return_value=None):
        # Auto-detection reports "no CLOCK_BOOTTIME": criterion (b) is skipped
        # while the (aligned) wall clock raises nothing either.
        assert wd.collect_freeze_events(
            1100.0, 200.0, state=state, persist=False, loop_pid=1,
        ) == []


# --- R2: a deadline left over from a previous boot must not hold grace -------

def test_stale_grace_deadline_after_mono_reset_is_ignored(tmp_path):
    """Host rebooted (monotonic ≈ 50) while the deadline says 67000 sec."""
    patch, _ = _patch_state(tmp_path)
    with patch:
        state = wd.STATE_FILE
        # No wall/mono baseline survives the reboot either: only the deadline.
        state.write_text(
            json.dumps({"freeze_grace_until_mono": 67000.0}) + "\n", encoding="utf-8"
        )
        _heartbeat(1000.0, age=900.0)
        result = _run(1000.0, 50.0, boot=_boot_for(50.0))

        doc = json.loads(state.read_text(encoding="utf-8"))

    assert result["freeze"] is False
    assert result["grace_active"] is False
    assert "heartbeat_stale" in _keys(result)
    # The unusable deadline is dropped instead of being persisted again.
    assert "freeze_grace_until_mono" not in doc
    assert doc["mono"] == pytest.approx(50.0)


def test_grace_deadline_bound_and_tolerance():
    state = {"freeze_grace_until_mono": 67000.0}
    # Beyond the window ⇒ ignored, so a real stall can still be reported.
    assert wd.grace_is_active(50.0, False, state, 120.0) is False
    # Inside the window ⇒ honoured as before.
    assert wd.grace_is_active(66900.0, False, state, 120.0) is True
    # Exactly at / past the deadline ⇒ expired.
    assert wd.grace_is_active(67000.0, False, state, 120.0) is False
    assert wd.grace_is_active(67001.0, False, state, 120.0) is False
    # 1s of tolerance on the upper bound, then it is rejected.
    assert wd.grace_is_active(66879.0, False, state, 120.0) is True
    assert wd.grace_is_active(66878.0, False, state, 120.0) is False
    # The freeze round itself never needs the persisted value.
    assert wd.grace_is_active(50.0, True, state, 120.0) is True
    # Unbounded legacy call (no grace_sec) still trusts the stored deadline.
    assert wd.grace_is_active(50.0, False, state) is True


# --- (f) pending freeze: heartbeat recovers inside the grace window ----------

def test_pending_freeze_heartbeat_recovers_sends_nothing(tmp_path):
    """Wall jumps 1h; the broker renews its heartbeat by the very next round.

    The freeze round parks the event in ``pending_freeze`` instead of sending
    it, and the recovery round clears the pending verdict with a log line
    only — nothing is ever sent.
    """
    patch, _ = _patch_state(tmp_path)
    with patch:
        state = wd.STATE_FILE
        _seed(state, 1000.0, 100.0)
        _heartbeat(1000.0, age=5.0)  # stale the moment the freeze ends
        freeze_wall, freeze_mono = 1000.0 + 3600.0, 160.0
        first = _run(freeze_wall, freeze_mono, boot=_boot_for(freeze_mono))
        assert first["freeze"] is True
        assert first["sent"] == []
        doc = json.loads(state.read_text(encoding="utf-8"))
        assert doc["pending_freeze"]["grace_until"] == pytest.approx(
            freeze_mono + wd.freeze_grace_sec(60.0)
        )

        # Next round, still inside the grace window: heartbeat is back.
        now, now_mono = freeze_wall + 60.0, freeze_mono + 60.0
        _heartbeat(now, age=5.0)
        second = _run(now, now_mono, boot=_boot_for(now_mono))
        doc = json.loads(state.read_text(encoding="utf-8"))

    assert second["freeze"] is False
    assert second["grace_active"] is True
    assert second["sent"] == []
    assert "freeze_stalled" not in _keys(second)
    # Recovery only logs: the pending verdict is cleared, never sent.
    assert "pending_freeze" not in doc


# --- (g) pending freeze: heartbeat never recovers -> one merged stalled ------

def test_pending_freeze_never_recovers_merges_exactly_one_stalled(tmp_path):
    """Wall jumps 1h and the heartbeat never renews: after the grace window
    the pending freeze merges into exactly one freeze_stalled alert."""
    patch, _ = _patch_state(tmp_path)
    with patch:
        state = wd.STATE_FILE
        _seed(state, 1000.0, 100.0)
        _heartbeat(1000.0, age=5.0)  # never touched again
        freeze_wall, freeze_mono = 1000.0 + 3600.0, 160.0
        first = _run(freeze_wall, freeze_mono, boot=_boot_for(freeze_mono))
        assert first["freeze"] is True
        assert first["sent"] == []

        grace = wd.freeze_grace_sec(60.0)

        # Inside the grace window: pending is kept, nothing is sent.
        mid = _run(
            freeze_wall + 60.0, freeze_mono + 60.0,
            boot=_boot_for(freeze_mono + 60.0),
        )
        assert mid["sent"] == []
        assert "freeze_stalled" not in _keys(mid)
        doc = json.loads(state.read_text(encoding="utf-8"))
        assert "pending_freeze" in doc

        # Past the grace window, heartbeat still stale: exactly one stalled.
        # Walk in loop-sized steps so the loop-interval freeze criterion does
        # not fire on the fast-forwarded monotonic clock.
        now, now_mono = freeze_wall + 60.0, freeze_mono + 60.0
        last = mid
        doc = {}
        while now_mono < freeze_mono + grace:
            now += 60.0
            now_mono += 60.0
            last = _run(now, now_mono, boot=_boot_for(now_mono))
            doc = json.loads(state.read_text(encoding="utf-8"))
            if now_mono < freeze_mono + grace:
                assert last["sent"] == []
                assert "freeze_stalled" not in _keys(last)
                assert "pending_freeze" in doc

    assert last["freeze"] is False
    assert last["grace_active"] is False
    assert "heartbeat_stale" not in _keys(last)
    stalled_sent = [e for e in last["sent"] if e.get("reason_key") == "freeze_stalled"]
    assert len(stalled_sent) == 1
    assert stalled_sent[0]["kind"] == wd.STALLED_KIND
    assert stalled_sent[0]["status"] == "stalled"
    assert stalled_sent[0]["jump_sec"] == pytest.approx(3600.0 - 60.0)
    # The pending verdict is consumed by the merged alert.
    assert "pending_freeze" not in doc
