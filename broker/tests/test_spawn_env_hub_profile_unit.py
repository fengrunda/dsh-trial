"""Unit tests for hub profile -> HUB_* injection in ``_spawn_env`` (hub-mount-b s2).

Locks the fail-closed contract:
- profile lookup by ``hub_profile`` id and by unique reverse ``hub_bank``,
- nothing given -> the four HUB_* keys are absent (even if inherited dirty),
- unknown id / malformed entry / ambiguous bank / missing or broken registry
  -> no injection, no raise,
- only the ``env_file`` path is injected, never its content (token).

The registry path is always redirected to a tmp file via ``DSH_HUB_READ_PROFILES``
(and ``DSH_HUB_INTEGRATIONS_DIR``), so the real box registry is never touched.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_spawn_env_hub", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

HUB_KEYS = ("HUB_BANK", "HUB_SCENE", "HUB_READ_ENV", "HUB_WIRE_SRC")

GOOD_ENTRY = {
    "bank": "dsh-dev",
    "scene": "scene-a",
    "env_file": "/home/box/.dsh/profiles/acp-hub-read-dsh-dev/engine.env",
    "wire_src": "/workspace/knowledge-hub/src",
    "default_browse_kind": "design-doc",
}


def _registry(tmp_path: Path, data) -> Path:
    """Write a registry file (raw text) and point the env at it."""
    p = tmp_path / "hub-read-profiles.json"
    p.write_text(data if isinstance(data, str) else json.dumps(data), encoding="utf-8")
    return p


@pytest.fixture(autouse=True)
def _isolate_registry(monkeypatch, tmp_path):
    """Never let a test fall through to the real box registry."""
    monkeypatch.setenv("DSH_HUB_INTEGRATIONS_DIR", str(tmp_path / "absent"))
    monkeypatch.delenv("DSH_HUB_READ_PROFILES", raising=False)
    for k in HUB_KEYS + ("HUB_PYTHON",):
        monkeypatch.delenv(k, raising=False)


def _env(tmp_path: Path, monkeypatch, data, **kwargs) -> dict:
    p = _registry(tmp_path, data)
    monkeypatch.setenv("DSH_HUB_READ_PROFILES", str(p))
    return tb._spawn_env("impl", ticket="t-hub", **kwargs)


# --- T1: explicit hub_profile id -------------------------------------------
def test_t1_profile_id_injects_four_keys(tmp_path, monkeypatch):
    env = _env(tmp_path, monkeypatch, {"dsh-dev": GOOD_ENTRY}, hub_profile="dsh-dev")
    assert env["HUB_BANK"] == "dsh-dev"
    assert env["HUB_SCENE"] == "scene-a"
    assert env["HUB_READ_ENV"] == GOOD_ENTRY["env_file"]
    assert env["HUB_WIRE_SRC"] == GOOD_ENTRY["wire_src"]
    # python is optional: absent field -> no default python3
    assert "HUB_PYTHON" not in env


# --- T2: reverse lookup by unique bank -------------------------------------
def test_t2_bank_only_reverse_lookup(tmp_path, monkeypatch):
    env = _env(tmp_path, monkeypatch, {"dsh-dev": GOOD_ENTRY}, hub_bank="dsh-dev")
    assert env["HUB_BANK"] == "dsh-dev"
    assert env["HUB_SCENE"] == "scene-a"
    assert env["HUB_READ_ENV"] == GOOD_ENTRY["env_file"]
    assert env["HUB_WIRE_SRC"] == GOOD_ENTRY["wire_src"]


# --- T3: nothing given -> absent, inherited dirt removed --------------------
def test_t3_no_binding_clears_inherited_hub_env(tmp_path, monkeypatch):
    monkeypatch.setenv("HUB_BANK", "dirty-bank")
    monkeypatch.setenv("HUB_SCENE", "dirty-scene")
    monkeypatch.setenv("HUB_READ_ENV", "/dirty/engine.env")
    monkeypatch.setenv("HUB_WIRE_SRC", "/dirty/src")
    env = _env(tmp_path, monkeypatch, {"dsh-dev": GOOD_ENTRY})
    for k in HUB_KEYS:
        assert k not in env


# --- T4: unknown profile -> no injection, no raise -------------------------
def test_t4_unknown_profile_fails_closed_without_raising(tmp_path, monkeypatch, capsys):
    env = _env(tmp_path, monkeypatch, {"dsh-dev": GOOD_ENTRY}, hub_profile="nope")
    for k in HUB_KEYS:
        assert k not in env
    assert "fail-closed" in capsys.readouterr().out


# --- T5: malformed entry / ambiguous bank ----------------------------------
@pytest.mark.parametrize(
    "data,kwargs",
    [
        ({"p1": {"scene": "s", "env_file": "/e", "wire_src": "/w"}}, {"hub_profile": "p1"}),
        ({"p1": {"bank": "b", "env_file": "/e", "wire_src": "/w"}}, {"hub_profile": "p1"}),
        ({"p1": {"bank": "b", "scene": "s", "wire_src": "/w"}}, {"hub_profile": "p1"}),
        (
            {
                "p1": {"bank": "b", "scene": "s", "env_file": "/e", "wire_src": "/w"},
                "p2": {"bank": "b", "scene": "s2", "env_file": "/e2", "wire_src": "/w2"},
            },
            {"hub_bank": "b"},
        ),
    ],
)
def test_t5_malformed_or_ambiguous_fails_closed(tmp_path, monkeypatch, data, kwargs):
    env = _env(tmp_path, monkeypatch, data, **kwargs)
    for k in HUB_KEYS:
        assert k not in env


# --- T6: missing / broken registry -----------------------------------------
def test_t6_missing_registry_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setenv("DSH_HUB_READ_PROFILES", str(tmp_path / "nope.json"))
    env = tb._spawn_env("impl", ticket="t-hub", hub_profile="dsh-dev")
    for k in HUB_KEYS:
        assert k not in env


def test_t6_broken_json_fails_closed(tmp_path, monkeypatch):
    env = _env(tmp_path, monkeypatch, "{not json", hub_profile="dsh-dev")
    for k in HUB_KEYS:
        assert k not in env


# --- T7: never leak env_file content ---------------------------------------
def test_t7_env_file_content_is_never_injected(tmp_path, monkeypatch):
    secret = "SUPERSECRETTOKEN123"
    env_file = tmp_path / "engine.env"
    env_file.write_text(f"KHUB_TOKEN={secret}\n", encoding="utf-8")
    entry = dict(GOOD_ENTRY, env_file=str(env_file))
    env = _env(tmp_path, monkeypatch, {"dsh-dev": entry}, hub_profile="dsh-dev")
    injected = {k: env[k] for k in HUB_KEYS}
    assert injected["HUB_READ_ENV"] == str(env_file)
    for value in injected.values():
        assert secret not in value
    assert secret not in "".join(env.values())
