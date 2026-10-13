#!/usr/bin/env python3
"""impl-tokopt-b: eager-offload env passthrough (``_spawn_env``).

The broker forwards the hard character threshold to every spawned agent as
``DSH_OFFLOAD_THRESHOLD_CHARS`` and pins full-text offload artifacts under the
ticket's artifacts dir via ``DSH_OFFLOAD_DIR`` (so offload text lands in the
ticket's artifacts instead of the shared role home).

Run: python3 -m pytest broker/tests/test_offload_env_unit.py -q
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location("trial_broker_offload_env", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tb)

T = tb.T


def test_default_limits_has_offload_threshold_chars():
    assert T.DEFAULT_LIMITS["offload_threshold_chars"] == 4000


def test_spawn_env_sets_threshold_and_ticket_offload_dir():
    env = tb._spawn_env(
        "impl",
        ticket="impl-tokopt-b",
        offload_threshold_chars=4000,
    )
    assert env["DSH_OFFLOAD_THRESHOLD_CHARS"] == "4000"
    assert env["DSH_OFFLOAD_DIR"] == str(tb.ARTIFACT_ROOT / "offload" / "impl-tokopt-b")


def test_spawn_env_zero_threshold_means_off():
    env = tb._spawn_env("impl", ticket="t-zero", offload_threshold_chars=0)
    assert env["DSH_OFFLOAD_THRESHOLD_CHARS"] == "0"
    assert env["DSH_OFFLOAD_DIR"].endswith("/offload/t-zero")


def test_spawn_env_without_ticket_leaves_offload_dir_unset():
    env = tb._spawn_env("gate", offload_threshold_chars=1234)
    assert env["DSH_OFFLOAD_THRESHOLD_CHARS"] == "1234"
    assert "DSH_OFFLOAD_DIR" not in env


def test_spawn_env_without_threshold_leaves_key_unset():
    env = tb._spawn_env("impl", ticket="impl-none")
    assert "DSH_OFFLOAD_THRESHOLD_CHARS" not in env
    # Ticket still pins the offload dir even with no threshold override.
    assert env["DSH_OFFLOAD_DIR"] == str(tb.ARTIFACT_ROOT / "offload" / "impl-none")
