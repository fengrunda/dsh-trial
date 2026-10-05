#!/usr/bin/env python3
"""Prompt timeout is idle-reset plus a hard cap measured on time.monotonic().

The deadlines must follow process uptime, not the wall clock: a host suspend
or NTP jump must not trip the hard/idle timer early.
"""
from __future__ import annotations

import importlib.util
import json
import queue
import sys
import tempfile
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_mod", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

ask_spec = importlib.util.spec_from_file_location("dsh_acp_ask_mod", ROOT / "dsh-acp-ask.py")
ask = importlib.util.module_from_spec(ask_spec)
ask_spec.loader.exec_module(ask)

T = tb.T


class _FakeProc:
    def poll(self):
        return None


class _FakeClient:
    def __init__(self):
        self.q = queue.Queue()
        self.p = _FakeProc()
        self._timeout_kind = None

    wait_for = ask.AcpClient.wait_for


def test_prompt_timeout_defaults_are_idle_plus_hard_cap():
    idle, hard = ask.prompt_timeouts_from_env({})
    assert idle == 900
    assert hard == 3600
    assert T.DEFAULT_LIMITS["prompt_idle_timeout_sec"] == 900
    assert T.DEFAULT_LIMITS["prompt_timeout_sec"] == 3600
    assert T.DEFAULT_LIMITS["prompt_idle_timeout_sec"] > T.DEFAULT_LIMITS["ask_supervisor_timeout_sec"]


def test_hard_cap_is_never_shorter_than_idle():
    idle, hard = ask.prompt_timeouts_from_env(
        {"DSH_ACP_PROMPT_IDLE_TIMEOUT": "1000", "DSH_ACP_PROMPT_TIMEOUT": "100"}
    )
    assert idle == 1000
    assert hard == 1000
    assert ask.bump_idle_deadline(10.0, 5.0, 12.0) == 12.0


def test_legacy_1800_wall_without_idle_key_becomes_hard_cap():
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "limits.json"
        path.write_text(json.dumps({"prompt_timeout_sec": 1800, "max_slices": 3}), encoding="utf-8")
        old = T.LIMITS_PATH
        T.LIMITS_PATH = path
        try:
            loaded = T.load_global_limits()
        finally:
            T.LIMITS_PATH = old
    assert loaded["prompt_timeout_sec"] == 3600
    assert loaded["prompt_idle_timeout_sec"] == 900
    assert loaded["max_slices"] == 3


def test_explicit_idle_keeps_short_hard_cap():
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "limits.json"
        path.write_text(
            json.dumps({"prompt_timeout_sec": 1800, "prompt_idle_timeout_sec": 300}),
            encoding="utf-8",
        )
        old = T.LIMITS_PATH
        T.LIMITS_PATH = path
        try:
            loaded = T.load_global_limits()
        finally:
            T.LIMITS_PATH = old
    assert loaded["prompt_timeout_sec"] == 1800
    assert loaded["prompt_idle_timeout_sec"] == 300


def test_spawn_env_points_at_repo_ask_and_sets_both_timeouts():
    env = tb._spawn_env(
        "impl",
        ticket="impl-unit",
        prompt_timeout_sec=3600,
        prompt_idle_timeout_sec=900,
    )
    assert env["DSH_ACP_PROMPT_TIMEOUT"] == "3600"
    assert env["DSH_ACP_PROMPT_IDLE_TIMEOUT"] == "900"
    assert env["DSH_ACP_ASK"] == str(ROOT / "dsh-acp-ask.py")
    assert env["DSH_ACP_ASK"] != str(Path.home() / ".dsh" / "bin" / "dsh-acp-ask.py")


def test_stdout_progress_resets_idle_timer():
    client = _FakeClient()

    def later():
        time.sleep(0.35)
        client.q.put(("out", json.dumps({"method": "session/update", "params": {}})))
        time.sleep(0.35)
        client.q.put(("out", json.dumps({"id": 7, "result": {}})))

    threading.Thread(target=later, daemon=True).start()
    started = time.time()
    msg, _errs = client.wait_for(lambda m: m.get("id") == 7, timeout=3, idle_timeout=0.5)
    elapsed = time.time() - started
    assert msg is not None and msg["id"] == 7
    assert elapsed >= 0.6


def test_stderr_does_not_reset_idle_timer():
    client = _FakeClient()

    def noise():
        for _ in range(8):
            client.q.put(("err", "banner"))
            time.sleep(0.1)

    threading.Thread(target=noise, daemon=True).start()
    started = time.time()
    msg, _errs = client.wait_for(lambda m: False, timeout=5, idle_timeout=0.45)
    elapsed = time.time() - started
    assert msg is None
    assert client._timeout_kind == "idle"
    assert elapsed < 1.3


def test_hard_cap_fires_while_stdout_keeps_coming():
    client = _FakeClient()
    stop = threading.Event()

    def pump():
        while not stop.is_set():
            client.q.put(("out", json.dumps({"method": "session/update", "params": {}})))
            time.sleep(0.05)

    threading.Thread(target=pump, daemon=True).start()
    try:
        started = time.time()
        msg, _errs = client.wait_for(lambda m: False, timeout=0.7, idle_timeout=0.3)
        elapsed = time.time() - started
    finally:
        stop.set()
    assert msg is None
    assert client._timeout_kind == "hard"
    assert elapsed < 1.6


class _WallJump:
    """Proxy for ``time``: real monotonic (no hangs), scripted wall clock.

    The wall clock starts near zero and can leap forward at a chosen step,
    modelling a host suspend / NTP jump *during* the wait.
    """

    def __init__(self, *, jump_at: int | None = None, jump_to: float = 3600.0):
        self.jump_at = jump_at
        self.jump_to = jump_to
        self.clock_calls = 0

    def time(self) -> float:
        self.clock_calls += 1
        if self.jump_at is not None and self.clock_calls >= self.jump_at:
            return self.jump_to
        return float(self.clock_calls)

    def __getattr__(self, name):
        # Everything else (sleep, monotonic, ...) stays the real clock.
        return getattr(time, name)


def test_hard_cap_times_out_on_monotonic_not_wall(monkeypatch):
    """A far-future wall clock must not make the hard cap fire immediately."""
    real_sleep = time.sleep
    fake = _WallJump()
    fake.jump_at = 1
    fake.jump_to = 1_700_000_000.0
    client = _FakeClient()
    monkeypatch.setattr(ask, "time", fake)

    def later():
        real_sleep(0.3)
        client.q.put(("out", json.dumps({"id": 7, "result": {}})))

    threading.Thread(target=later, daemon=True).start()
    started = time.monotonic()
    msg, _errs = client.wait_for(lambda m: m.get("id") == 7, timeout=1.0, idle_timeout=None)
    elapsed = time.monotonic() - started
    assert msg is not None and msg["id"] == 7
    assert client._timeout_kind is None
    assert elapsed < 0.8


def test_wall_clock_jump_does_not_trip_hard_cap(monkeypatch):
    """Host suspend mid-wait: wall clock leaps, monotonic hard cap survives."""
    real_sleep = time.sleep
    fake = _WallJump(jump_at=30, jump_to=1_700_000_000.0)
    client = _FakeClient()
    monkeypatch.setattr(ask, "time", fake)

    def later():
        real_sleep(0.12)
        client.q.put(("out", json.dumps({"id": 7, "result": {}})))

    threading.Thread(target=later, daemon=True).start()
    started = time.monotonic()
    msg, _errs = client.wait_for(lambda m: m.get("id") == 7, timeout=1.0, idle_timeout=None)
    elapsed = time.monotonic() - started
    assert msg is not None and msg["id"] == 7
    assert client._timeout_kind is None
    assert elapsed < 0.8


def test_wall_clock_jump_does_not_trip_idle(monkeypatch):
    """A wall-clock leap during progress must not expire the idle timer."""
    real_sleep = time.sleep
    fake = _WallJump(jump_at=30, jump_to=2_000_000_000.0)
    client = _FakeClient()
    monkeypatch.setattr(ask, "time", fake)

    def later():
        real_sleep(0.12)
        # Progress: with monotonic this is the only thing that resets idle.
        client.q.put(("out", json.dumps({"method": "session/update", "params": {}})))
        real_sleep(0.13)
        client.q.put(("out", json.dumps({"id": 9, "result": {}})))

    threading.Thread(target=later, daemon=True).start()
    msg, _errs = client.wait_for(lambda m: m.get("id") == 9, timeout=5, idle_timeout=0.4)
    assert msg is not None and msg["id"] == 9
    assert client._timeout_kind is None


def test_hard_deadline_uses_monotonic_source():
    """Guard: wait_for must not read the wall clock for its deadlines."""
    src = (ROOT / "dsh-acp-ask.py").read_text(encoding="utf-8")
    start = src.index("def wait_for(")
    end = src.index("def initialize(", start)
    body = src[start:end]
    assert "time.monotonic()" in body
    assert "time.time()" not in body

