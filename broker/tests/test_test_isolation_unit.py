#!/usr/bin/env python3
"""Self-checks for the unit-test isolation guard (impl-tests-tmp-home-guard).

These tests assert that importing the broker modules inside the suite lands all
state under a temporary DSH_HOME, and that the conftest guard actually catches a
module attribute pointing back at the real ``~/.dsh``.
"""
from __future__ import annotations

import importlib.util
import os
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import conftest  # noqa: E402  (loaded by pytest from this directory)

_REAL_DSH_HOME = (Path.home() / ".dsh").resolve()
_REAL_DSH_HOMES = (Path.home() / ".dsh-homes").resolve()

_ENV_KEYS = (
    "DSH_HOME",
    "TRIAL_BROKER_DIR",
    "TRIAL_BROKER_HEARTBEAT",
    "TRIAL_WATCHDOG_STATE",
    "DSH_TRIAL_MAILBOX",
    "TRIAL_ARTIFACT_ROOT",
    "DSH_HOMES_ROOT",
    "TRIAL_WATCHDOG_LOG_DIR",
)


def _under(path: Path, root: Path) -> bool:
    path = path.resolve()
    return path == root or root in path.parents


def test_env_dsh_home_is_tmp_not_real():
    raw = os.environ.get("DSH_HOME")
    assert raw, "conftest must export a DSH_HOME"
    dsh_home = Path(raw).resolve()

    assert not _under(dsh_home, _REAL_DSH_HOME), f"DSH_HOME points at real ~/.dsh: {dsh_home}"
    assert not _under(dsh_home, _REAL_DSH_HOMES), f"DSH_HOME points at real ~/.dsh-homes: {dsh_home}"
    assert dsh_home.is_dir(), dsh_home
    assert "dsh-trial-test-home-" in dsh_home.name


def test_all_env_state_roots_live_under_tmp_home():
    dsh_home = Path(os.environ["DSH_HOME"]).resolve()
    for key in _ENV_KEYS:
        raw = os.environ.get(key)
        assert raw, f"{key} must be exported by conftest"
        path = Path(raw).resolve()
        assert _under(path, dsh_home), f"{key}={path} escapes tmp DSH_HOME {dsh_home}"


def test_tmp_home_has_trial_layout():
    dsh_home = Path(os.environ["DSH_HOME"])
    for name in ("inbox", "outbox", "processing", "failed"):
        assert (dsh_home / "supervisor" / "trial" / name).is_dir(), name
    for name in ("goals", "chains", "packs", "summaries", "mailbox", "metrics"):
        assert (dsh_home / "supervisor" / "thin-state" / name).is_dir(), name


def test_broker_module_paths_are_tmp_home():
    spec = importlib.util.spec_from_file_location(
        "trial_broker_isolation_probe", ROOT / "trial-broker.py"
    )
    tb = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tb)

    dsh_home = Path(os.environ["DSH_HOME"]).resolve()
    for attr in ("INBOX", "OUTBOX", "PACKS"):
        value = Path(getattr(tb, attr)).resolve()
        assert _under(value, dsh_home), f"tb.{attr}={value} escapes {dsh_home}"
    assert not _under(Path(tb.INBOX).resolve(), _REAL_DSH_HOME)
    assert not _under(Path(tb.PACKS).resolve(), _REAL_DSH_HOME)


def test_guard_flags_module_attribute_pointing_at_real_home():
    fake = types.SimpleNamespace(
        INBOX=_REAL_DSH_HOME / "supervisor" / "trial" / "outbox",
        PACKS=Path(os.environ["DSH_HOME"]) / "supervisor" / "thin-state" / "packs",
    )
    violations = conftest._real_dsh_home_violations({"fake_real_home_mod": fake})

    assert ("fake_real_home_mod", "INBOX", str(fake.INBOX)) in violations
    assert all(name == "fake_real_home_mod" for name, _attr, _path in violations)
    assert {attr for _m, attr, _p in violations} == {"INBOX"}


def test_guard_accepts_tmp_paths_and_ignores_non_path_values():
    dsh_home = Path(os.environ["DSH_HOME"])
    fake = types.SimpleNamespace(
        INBOX=dsh_home / "supervisor" / "trial" / "inbox",
        STATE_DIR=dsh_home / "broker-dsh-trial",
        LIMITS_PATH=str(_REAL_DSH_HOME / "supervisor" / "trial" / "limits.json"),
        HOMES_ROOT=dsh_home / "dsh-homes",
    )
    assert conftest._real_dsh_home_violations({"fake_tmp_mod": fake}) == []


if __name__ == "__main__":  # pragma: no cover - manual driver
    for _name, _fn in sorted(globals().items()):
        if _name.startswith("test_") and callable(_fn):
            _fn()
            print("OK", _name)
    print("ALL ISOLATION OK")
