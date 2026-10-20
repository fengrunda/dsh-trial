#!/usr/bin/env python3
"""Unit tests: pause classify, decision parse, pack body, supervisor reuse."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("pause_supervisor", ROOT / "pause_supervisor.py")
ps = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ps)


def test_classify_paused_limit_from_status():
    assert ps.classify_pause_reason(summary_status="paused_limit") == "paused_limit"


def test_classify_paused_wallclock_from_status():
    assert ps.classify_pause_reason(summary_status="paused_wallclock") == "paused_wallclock"


def test_classify_hard_cap_log_is_wallclock():
    log = "=== run ===\nACP session/prompt failed: timeout after 3600s hard cap\n"
    assert ps.classify_pause_reason(log_text=log) == "paused_wallclock"


def test_classify_idle_log_is_wallclock():
    log = "=== run ===\ntimeout after 900s idle (no ACP stdout)\n"
    assert ps.classify_pause_reason(log_text=log) == "paused_wallclock"


def test_classify_step_hard_plugin_is_limit():
    log = "=== run ===\n[step-budget] 已到硬阈值（60/30）\n"
    assert ps.classify_pause_reason(log_text=log) == "paused_limit"


def test_classify_none_when_clean():
    assert ps.classify_pause_reason(summary_status="done", log_text="=== run ===\nok\n") is None


def test_parse_pause_decision_five_ways():
    for d in ("continue_same", "new_ticket", "close", "split", "escalate"):
        assert ps.parse_pause_decision({"decision": d}) == d
    assert ps.parse_pause_decision({"action": "pause_decision", "decision": "close"}) == "close"
    assert (
        ps.parse_pause_decision(None, text='{"decision": "continue_same"}') == "continue_same"
    )


def test_build_pause_pack_mentions_five_options():
    body = ps.build_pause_pack_body(
        slice_id="s1",
        round_n=1,
        pause_kind="paused_limit",
        steps_used=60,
        step_budget=30,
        elapsed_sec=1200,
        idle_timeout_sec=900,
        hard_timeout_sec=3600,
        goal_id="g1",
        pending_pack="s1.pack.md",
        impl_ticket="impl-s1",
        notes="still working on X",
    )
    assert "continue_same" in body and "escalate" in body
    assert "paused_limit" in body
    assert "60" in body


def test_supervisor_reuse_same_goal():
    ok, ticket = ps.should_reuse_supervisor(
        {"supervisor_reuse_ticket": "supervisor-goal-g1"}
    )
    assert ok and ticket == "supervisor-goal-g1"
    ok2, _ = ps.should_reuse_supervisor(
        {"supervisor_reuse_ticket": "supervisor-goal-g1", "supervisor_reuse_broken": True}
    )
    assert not ok2
    assert ps.supervisor_ticket_name("engine-k2c!") == "supervisor-goal-engine-k2c"


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("ok", name)
    print("ALL OK")
