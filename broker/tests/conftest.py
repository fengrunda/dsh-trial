"""Shared pytest fixtures for broker-dsh-trial unit tests.

The chain tests take a real ``Path`` scratch working directory (they shell out
to git only best-effort). ``tmp_cwd`` is a thin alias over pytest's built-in
``tmp_path`` so the same tests can also be driven without pytest.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

_BROKER_ROOT = Path(__file__).resolve().parent.parent
if str(_BROKER_ROOT) not in sys.path:
    sys.path.insert(0, str(_BROKER_ROOT))

_CANONICAL_DSH_HOME = Path(os.environ.get("DSH_HOME_CANONICAL") or (Path.home() / ".dsh"))


def _use_trial_supervisor_home() -> None:
    """Keep DSH_HOME on a home that actually has supervisor/trial state.

    The agent harness exports an agent-scoped ``DSH_HOME`` (e.g.
    ``~/.dsh-homes/<agent>``) with no ``supervisor/trial/routes.json``; the
    broker and its unit tests expect the trial supervisor home. An explicit
    DSH_HOME that already carries trial state is left untouched.
    """
    current = os.environ.get("DSH_HOME")
    if current and (Path(current) / "supervisor" / "trial" / "routes.json").is_file():
        return
    os.environ["DSH_HOME"] = str(_CANONICAL_DSH_HOME)


_use_trial_supervisor_home()


@pytest.fixture
def tmp_cwd(tmp_path: Path) -> Path:
    return tmp_path
