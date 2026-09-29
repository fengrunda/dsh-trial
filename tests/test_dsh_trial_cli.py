"""CLI wrappers: dsh-trial start|stop|broker-status call broker/start.sh etc."""
from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_CLI = _REPO / "bin" / "dsh-trial"


def _run(env: dict, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(_CLI), *args],
        capture_output=True,
        text=True,
        env=env,
    )


def test_help_lists_start_stop_broker_status() -> None:
    cp = _run(os.environ.copy(), "-h")
    assert cp.returncode == 0, cp.stderr
    out = cp.stdout
    assert "start" in out
    assert "stop" in out
    assert "broker-status" in out
    assert "status" in out
    assert "goals" in out


def test_start_stop_wrap_scripts(tmp_path: Path) -> None:
    broker = tmp_path / "broker"
    broker.mkdir()
    (broker / "trial_lib.py").write_text("# stub for path discovery\n", encoding="utf-8")
    start = broker / "start.sh"
    stop = broker / "stop.sh"
    start.write_text("#!/bin/sh\necho START-WRAP \"$@\"\n", encoding="utf-8")
    stop.write_text("#!/bin/sh\necho STOP-WRAP \"$@\"\n", encoding="utf-8")
    start.chmod(start.stat().st_mode | stat.S_IEXEC)
    stop.chmod(stop.stat().st_mode | stat.S_IEXEC)

    env = {**os.environ, "DSH_TRIAL_ROOT": str(tmp_path)}
    cp = _run(env, "start", "--once")
    assert cp.returncode == 0, cp.stderr + cp.stdout
    assert "START-WRAP --once" in cp.stdout

    cp2 = _run(env, "stop")
    assert cp2.returncode == 0, cp2.stderr + cp2.stdout
    assert "STOP-WRAP" in cp2.stdout


def test_missing_start_script(tmp_path: Path) -> None:
    broker = tmp_path / "broker"
    broker.mkdir()
    (broker / "trial_lib.py").write_text("# stub\n", encoding="utf-8")
    env = {**os.environ, "DSH_TRIAL_ROOT": str(tmp_path)}
    cp = _run(env, "start")
    assert cp.returncode == 2
    assert "missing" in cp.stderr


if __name__ == "__main__":
    import tempfile

    test_help_lists_start_stop_broker_status()
    with tempfile.TemporaryDirectory(prefix="dsh-trial-cli-") as td:
        test_start_stop_wrap_scripts(Path(td))
    with tempfile.TemporaryDirectory(prefix="dsh-trial-cli-") as td:
        test_missing_start_script(Path(td))
    print("OK dsh-trial CLI start/stop")
