#!/usr/bin/env python3
"""T6: per-role reasoning effort / step budget defaults, overrides, spawn env.

Covers:
  * ``role_reasoning_effort`` / ``role_step_budget`` defaults + unknown roles
  * limits.json style overrides, including bad values (non-int, negative)
  * ``_spawn_env`` wiring of DSH_REASONING_EFFORT / DSH_STEP_BUDGET
"""

import importlib.util
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location("trial_broker_role_effort", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tb)

T = tb.T


# ---------------------------------------------------------------------------
# defaults / unknown role
# ---------------------------------------------------------------------------
def test_defaults_by_role():
    assert T.role_reasoning_effort("supervisor", {}) == "low"
    assert T.role_step_budget("supervisor", {}) == 30
    assert T.role_reasoning_effort("impl", {}) == "low"
    assert T.role_step_budget("impl", {}) == 80
    assert T.role_reasoning_effort("gate", {}) == "high"
    assert T.role_step_budget("gate", {}) == 60


def test_unknown_role_gets_nothing():
    for role in ("foreman", "nope", "", None):
        assert T.role_reasoning_effort(role, {}) is None
        assert T.role_step_budget(role, {}) == 0


def test_defaults_match_module_tables():
    assert T.DEFAULT_REASONING_EFFORT_BY_ROLE == {
        "supervisor": "low",
        "impl": "low",
        "gate": "high",
    }
    assert T.DEFAULT_STEP_BUDGET_BY_ROLE == {"supervisor": 30, "impl": 80, "gate": 60}


# ---------------------------------------------------------------------------
# limits overrides
# ---------------------------------------------------------------------------
def test_limits_override_only_named_role_and_key():
    limits = {
        "reasoning_effort_by_role": {"impl": "low"},
        "step_budget_by_role": {"gate": 40},
    }
    assert T.role_reasoning_effort("impl", limits) == "low"
    assert T.role_reasoning_effort("gate", limits) == "high"  # untouched default
    assert T.role_reasoning_effort("supervisor", limits) == "low"
    assert T.role_step_budget("gate", limits) == 40
    assert T.role_step_budget("impl", limits) == 80  # untouched default
    assert T.role_step_budget("supervisor", limits) == 30


def test_bad_step_budget_values_fall_back_to_zero():
    for bad in ("x", -5, -5.0, 0, None, "", 0.5):
        limits = {"step_budget_by_role": {"impl": bad}}
        assert T.role_step_budget("impl", limits) == 0, bad
    # a non-dict table is ignored entirely
    assert T.role_step_budget("impl", {"step_budget_by_role": "nope"}) == 80
    assert T.role_reasoning_effort("impl", {"reasoning_effort_by_role": 7}) == "low"


def test_blank_effort_override_yields_none():
    assert T.role_reasoning_effort("impl", {"reasoning_effort_by_role": {"impl": "  "}}) is None


# ---------------------------------------------------------------------------
# _spawn_env wiring
# ---------------------------------------------------------------------------
def test_spawn_env_gate_sets_high_and_60(monkeypatch):
    monkeypatch.setattr(T, "load_global_limits", lambda: {})
    env = tb._spawn_env("gate")
    assert env["DSH_ROLE"] == "gate"
    assert env["DSH_REASONING_EFFORT"] == "high"
    assert env["DSH_STEP_BUDGET"] == "60"


def test_spawn_env_impl_sets_low_and_80(monkeypatch):
    monkeypatch.setattr(T, "load_global_limits", lambda: {})
    env = tb._spawn_env("impl", ticket="impl-t6-r1")
    assert env["DSH_TICKET"] == "impl-t6-r1"
    assert env["DSH_REASONING_EFFORT"] == "low"
    assert env["DSH_STEP_BUDGET"] == "80"


def test_spawn_env_honours_limits_override(monkeypatch):
    monkeypatch.setattr(
        T,
        "load_global_limits",
        lambda: {"reasoning_effort_by_role": {"impl": "low"}, "step_budget_by_role": {"impl": 12}},
    )
    env = tb._spawn_env("impl")
    assert env["DSH_REASONING_EFFORT"] == "low"
    assert env["DSH_STEP_BUDGET"] == "12"


def test_spawn_env_unknown_role_clears_inherited(monkeypatch):
    monkeypatch.setattr(T, "load_global_limits", lambda: {})
    monkeypatch.setenv("DSH_REASONING_EFFORT", "max")
    monkeypatch.setenv("DSH_STEP_BUDGET", "999")
    env = tb._spawn_env("foreman")
    assert "DSH_REASONING_EFFORT" not in env
    assert "DSH_STEP_BUDGET" not in env
    # parent process env is untouched (copy, not mutation)
    assert os.environ["DSH_STEP_BUDGET"] == "999"


def test_spawn_env_supervisor_defaults(monkeypatch):
    monkeypatch.setattr(T, "load_global_limits", lambda: {})
    env = tb._spawn_env("supervisor")
    assert env["DSH_REASONING_EFFORT"] == "low"
    assert env["DSH_STEP_BUDGET"] == "30"


def test_load_global_limits_never_raises_on_missing_file(monkeypatch, tmp_path):
    # real loader stays total (implicit fallback table in _spawn_env)
    monkeypatch.setattr(T, "LIMITS_PATH", tmp_path / "absent.json")
    assert T.role_reasoning_effort("gate") == "high"
    assert T.role_step_budget("gate") == 60


# ---------------------------------------------------------------------------
# load_global_limits: per-role tables survive the DEFAULT_LIMITS allow-list
# ---------------------------------------------------------------------------
def test_load_global_limits_keeps_role_tables(monkeypatch, tmp_path):
    path = tmp_path / "limits.json"
    path.write_text(
        json.dumps(
            {
                "reasoning_effort_by_role": {"impl": "high"},
                "step_budget_by_role": {"impl": 12},
                "some_unknown_key": "still dropped",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(T, "LIMITS_PATH", path)
    limits = T.load_global_limits()
    assert limits["reasoning_effort_by_role"] == {"impl": "high"}
    assert limits["step_budget_by_role"] == {"impl": 12}
    assert "some_unknown_key" not in limits
    assert T.role_reasoning_effort("impl", limits) == "high"
    assert T.role_step_budget("impl", limits) == 12


def test_load_global_limits_drops_non_dict_role_tables(monkeypatch, tmp_path):
    path = tmp_path / "limits.json"
    path.write_text(
        json.dumps({"reasoning_effort_by_role": 7, "step_budget_by_role": "nope"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(T, "LIMITS_PATH", path)
    limits = T.load_global_limits()
    assert "reasoning_effort_by_role" not in limits
    assert "step_budget_by_role" not in limits
    assert T.role_reasoning_effort("impl", limits) == "low"
    assert T.role_step_budget("impl", limits) == 80
