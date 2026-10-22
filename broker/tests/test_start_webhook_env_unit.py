"""Unit tests: broker start scripts source $DSH_HOME/watchdog-webhook.env.

Both `start.sh` and `watchdog-start.sh` support `--once` and then
`exec python3 <script> --once`, so putting a fake `python3` in front of PATH
lets us observe the environment handed to the child process without ever
starting a resident broker/watchdog. Pure pytest + subprocess: no network, no
background processes, and the real `~/.dsh` is never read.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
START_SH = REPO_ROOT / "broker" / "start.sh"
WATCHDOG_START_SH = REPO_ROOT / "broker" / "watchdog-start.sh"

SCRIPTS = [START_SH, WATCHDOG_START_SH]
SCRIPT_IDS = [p.name for p in SCRIPTS]

WEBHOOK_URL = "https://example.invalid/hook-TEST"
WEBHOOK_KEY = "test-key-not-real"

FAKE_PYTHON = """#!/usr/bin/env bash
set -uo pipefail
{
  echo "URL=${DSH_KHUB_COMPLETE_WEBHOOK_URL:-}"
  echo "KEY=${DSH_KHUB_COMPLETE_WEBHOOK_KEY:-}"
} >>"${DSH_TEST_ENV_DUMP:?DSH_TEST_ENV_DUMP unset}"
exit 0
"""


def _write_webhook_env(home: Path) -> None:
    (home / "watchdog-webhook.env").write_text(
        f"DSH_KHUB_COMPLETE_WEBHOOK_URL={WEBHOOK_URL}\n"
        f"DSH_KHUB_COMPLETE_WEBHOOK_KEY={WEBHOOK_KEY}\n",
        encoding="utf-8",
    )


def _make_tmp_bin(tmp_path: Path) -> Path:
    tmp_bin = tmp_path / "bin"
    tmp_bin.mkdir()
    fake = tmp_bin / "python3"
    fake.write_text(FAKE_PYTHON, encoding="utf-8")
    fake.chmod(0o755)
    return tmp_bin


def _run_once(script: Path, tmp_path: Path, *, with_env_file: bool):
    tmp_home = tmp_path / "dsh-home"
    tmp_home.mkdir()
    if with_env_file:
        _write_webhook_env(tmp_home)

    tmp_bin = _make_tmp_bin(tmp_path)
    dump = tmp_path / "env-dump.txt"

    env = {k: v for k, v in os.environ.items()}
    # Host may already export the real webhook values; drop them so the
    # "missing file -> empty" case cannot be a false positive.
    env.pop("DSH_KHUB_COMPLETE_WEBHOOK_URL", None)
    env.pop("DSH_KHUB_COMPLETE_WEBHOOK_KEY", None)
    env.update(
        {
            "DSH_HOME": str(tmp_home),
            "TRIAL_BROKER_DIR": str(tmp_home / "broker-dsh-trial"),
            "TRIAL_ARTIFACT_ROOT": str(tmp_path / "art"),
            "TRIAL_WATCHDOG_ENABLE": "0",
            "DSH_TEST_ENV_DUMP": str(dump),
            "PATH": str(tmp_bin) + os.pathsep + os.environ["PATH"],
        }
    )

    proc = subprocess.run(
        ["bash", str(script), "--once"],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return proc, dump


@pytest.mark.parametrize("script", SCRIPTS, ids=SCRIPT_IDS)
def test_bash_syntax_ok(script: Path) -> None:
    proc = subprocess.run(
        ["bash", "-n", str(script)],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stderr


@pytest.mark.parametrize("script", SCRIPTS, ids=SCRIPT_IDS)
def test_env_file_loaded_into_child(script: Path, tmp_path: Path) -> None:
    proc, dump = _run_once(script, tmp_path, with_env_file=True)

    assert proc.returncode == 0, proc.stderr
    dumped = dump.read_text(encoding="utf-8")
    assert f"URL={WEBHOOK_URL}" in dumped
    assert f"KEY={WEBHOOK_KEY}" in dumped
    # Never echo the secret back to the terminal/log.
    assert WEBHOOK_KEY not in proc.stdout
    assert WEBHOOK_KEY not in proc.stderr


@pytest.mark.parametrize("script", SCRIPTS, ids=SCRIPT_IDS)
def test_missing_env_file_does_not_fail(script: Path, tmp_path: Path) -> None:
    proc, dump = _run_once(script, tmp_path, with_env_file=False)

    assert proc.returncode == 0, proc.stderr
    dumped = dump.read_text(encoding="utf-8")
    assert "URL=" in dumped
    assert "KEY=" in dumped
    assert f"URL={WEBHOOK_URL}" not in dumped
    assert f"KEY={WEBHOOK_KEY}" not in dumped
    for line in dumped.splitlines():
        if line.startswith("URL=") or line.startswith("KEY="):
            assert line.endswith("="), f"unexpected value leaked: {line!r}"


@pytest.mark.parametrize("script", SCRIPTS, ids=SCRIPT_IDS)
def test_source_block_shape(script: Path) -> None:
    text = script.read_text(encoding="utf-8")

    assert "set -x" not in text
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if "DSH_KHUB_COMPLETE_WEBHOOK_KEY" in stripped:
            assert "echo" not in stripped and "printf" not in stripped, stripped

    # The block must sit after the load-env.sh guard closes and before
    # `export DSH_HOME` (load-env first, webhook overrides afterwards).
    anchor = text.index("load-env.sh")
    tail = text[anchor:]
    fi_end = tail.index("\nfi\n") + len("\nfi\n")
    body = tail[fi_end:]

    pos_file_test = body.index('[[ -f "$DSH_HOME/watchdog-webhook.env" ]]')
    pos_set_a = body.index("set -a")
    pos_source = body.index('source "$DSH_HOME/watchdog-webhook.env"')
    pos_set_plus_a = body.index("set +a")
    pos_export_home = tail.index("export DSH_HOME")

    assert pos_file_test < pos_set_a < pos_source < pos_set_plus_a
    assert fi_end + pos_set_plus_a < pos_export_home
    # Purpose comment for the block.
    assert "watchdog-webhook" in body.splitlines()[0] or any(
        "webhook" in ln for ln in body.splitlines()[:3]
    )
