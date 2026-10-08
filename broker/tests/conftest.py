"""Shared pytest fixtures for broker-dsh-trial unit tests.

Isolation contract (impl-tests-tmp-home-guard)
----------------------------------------------
Every unit-test module imports ``trial-broker.py`` / ``trial_lib.py`` /
``trial-watchdog.py`` at module scope, and those modules freeze ``DSH_HOME``
derived constants (INBOX/OUTBOX/PROCESSING/FAILED/PACKS/SUMMARIES/CHAINS/GOALS/
MAILBOX/METRICS_JSONL/STATE_DIR/...) at import time. Pointing ``DSH_HOME`` at the
real ``~/.dsh`` therefore made the suite write real inbox/outbox/chain/pack
files and race the live broker.

So, **before any test module is imported**, this conftest provisions a fresh
session-scoped temp DSH_HOME (with the real ``supervisor/trial`` layout) and
re-points every DSH_*/TRIAL_* environment variable at it. The real
``routes.json`` / ``limits.json`` are copied in (read-only source: they are never
written back) so behaviour matches production limits/routes.

The autouse ``_no_real_dsh_home_paths`` guard is the safety net: it scans every
imported module for broker state attributes that still resolve under the real
``~/.dsh`` / ``~/.dsh-homes`` and fails the offending test.

``tmp_cwd`` is a thin alias over pytest's built-in ``tmp_path`` so the same tests
can also be driven without pytest.
"""
from __future__ import annotations

import atexit
import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest

_BROKER_ROOT = Path(__file__).resolve().parent.parent
if str(_BROKER_ROOT) not in sys.path:
    sys.path.insert(0, str(_BROKER_ROOT))

# Real homes computed from Path.home() -- deliberately NOT from the environment,
# because the point of this module is to stop trusting DSH_HOME in tests.
_REAL_DSH_HOME = (Path.home() / ".dsh").resolve()
_REAL_DSH_HOMES = (Path.home() / ".dsh-homes").resolve()

# Broker/plugin state attributes that must never point into a real home.
_GUARDED_ATTRS = frozenset(
    {
        "DSH_HOME",
        "TRIAL_SUP",
        "INBOX",
        "OUTBOX",
        "PROCESSING",
        "FAILED",
        "PACKS",
        "SUMMARIES",
        "CHAINS",
        "GOALS",
        "MAILBOX",
        "METRICS_JSONL",
        "STATE_DIR",
        "HEARTBEAT",
        "STATE_FILE",
        "LIMITS_PATH",
        "ROUTES_PATH",
        "HOMES_ROOT",
        # T1 slot-isolated role homes: default is <HOMES_ROOT>/trial-slots, so
        # an unpatched constant pointing at the real ~/.dsh-homes is a bug.
        "SLOT_HOMES_ROOT",
    }
)

_TRIAL_SUB_DIRS = ("inbox", "outbox", "processing", "failed")
_THIN_STATE_SUB_DIRS = ("goals", "chains", "packs", "summaries", "mailbox", "metrics")


def _provision_tmp_home() -> Path:
    """Create a throwaway DSH_HOME carrying the real trial supervisor layout."""
    tmp = Path(tempfile.mkdtemp(prefix="dsh-trial-test-home-"))
    sup = tmp / "supervisor"
    (sup / "trial").mkdir(parents=True, exist_ok=True)
    for name in _TRIAL_SUB_DIRS:
        (sup / "trial" / name).mkdir(parents=True, exist_ok=True)
    for name in _THIN_STATE_SUB_DIRS:
        (sup / "thin-state" / name).mkdir(parents=True, exist_ok=True)
    (tmp / "broker-dsh-trial").mkdir(parents=True, exist_ok=True)
    (tmp / "trial-broker-artifacts").mkdir(parents=True, exist_ok=True)
    (tmp / "dsh-homes").mkdir(parents=True, exist_ok=True)
    (tmp / "watchdog-logs").mkdir(parents=True, exist_ok=True)
    return tmp


def _copy_real_trial_config(tmp: Path) -> None:
    """Copy routes.json/limits.json from the real home. Missing/failed: skip."""

    def _copy(name: str) -> None:
        src = _REAL_DSH_HOME / "supervisor" / "trial" / name
        dst = tmp / "supervisor" / "trial" / name
        try:
            if src.is_file():
                shutil.copyfile(src, dst)
                dst.chmod(0o444)  # read-only snapshot; tests must not mutate it
        except OSError:
            pass

    _copy("routes.json")
    _copy("limits.json")


_TMP_HOME = _provision_tmp_home()
_copy_real_trial_config(_TMP_HOME)

# Environment overrides: every test-visible state root lives under _TMP_HOME.
_ENV_OVERRIDES = {
    "DSH_HOME": _TMP_HOME,
    "TRIAL_BROKER_DIR": _TMP_HOME / "broker-dsh-trial",
    "TRIAL_BROKER_HEARTBEAT": _TMP_HOME / "broker-dsh-trial" / "trial-broker.heartbeat.json",
    "TRIAL_WATCHDOG_STATE": _TMP_HOME / "broker-dsh-trial" / "trial-watchdog.state.json",
    "DSH_TRIAL_MAILBOX": _TMP_HOME / "supervisor" / "thin-state" / "mailbox",
    "TRIAL_ARTIFACT_ROOT": _TMP_HOME / "trial-broker-artifacts",
    "DSH_HOMES_ROOT": _TMP_HOME / "dsh-homes",
    "TRIAL_WATCHDOG_LOG_DIR": _TMP_HOME / "watchdog-logs",
}
for _key, _path in _ENV_OVERRIDES.items():
    os.environ[_key] = str(_path)


@atexit.register
def _cleanup_tmp_home() -> None:  # pragma: no cover - best effort teardown
    shutil.rmtree(_TMP_HOME, ignore_errors=True)


# ---------------------------------------------------------------------------
# real-home guard
# ---------------------------------------------------------------------------
def _real_dsh_home_violations(modules=None) -> list[tuple[str, str, str]]:
    """Return ``(module, attr, path)`` for state paths inside a real DSH home.

    ``modules`` defaults to ``sys.modules``; pass a mapping/iterable of
    ``(name, module)`` pairs to check synthetic modules (used by unit tests).
    """
    items = sys.modules.items() if modules is None else (
        modules.items() if hasattr(modules, "items") else modules
    )
    violations: list[tuple[str, str, str]] = []
    for mod_name, mod in list(items):
        if mod is None:
            continue
        for attr in _GUARDED_ATTRS:
            try:
                value = getattr(mod, attr, None)
            except Exception:
                continue
            if not isinstance(value, Path):
                continue
            try:
                resolved = value.resolve()
            except OSError:
                resolved = Path(os.path.abspath(str(value)))
            for real_home in (_REAL_DSH_HOME, _REAL_DSH_HOMES):
                if resolved == real_home or real_home in resolved.parents:
                    violations.append((mod_name, attr, str(value)))
                    break
    return violations


@pytest.fixture(autouse=True)
def _no_real_dsh_home_paths():
    """Fail if any imported module keeps state paths under the real DSH homes."""

    def _check(stage: str) -> None:
        bad = _real_dsh_home_violations()
        if bad:
            detail = "\n".join(f"  - {m}.{a} -> {p}" for m, a, p in sorted(bad))
            pytest.fail(
                f"{stage}: module state points into the real DSH home "
                f"(tests must use a tmp DSH_HOME / mock.patch the module dirs):\n{detail}"
            )

    _check("before test")
    yield
    _check("after test")


@pytest.fixture
def tmp_cwd(tmp_path: Path) -> Path:
    return tmp_path
