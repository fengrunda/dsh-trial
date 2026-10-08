"""CLI wrappers: dsh-trial start|stop|broker-status call broker/start.sh etc."""
from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_CLI = _REPO / "bin" / "dsh-trial"
_HEARTBEAT_NAME = "trial-broker.heartbeat.json"

# One heartbeat doc reused by the status/report tests (wall is fixed so the
# age assertion in the trial_lib test is deterministic).
_HB_WALL = 100.0
_HB_DOC = {
    "pid": 4242,
    "at": "2026-02-01T10:00:00+08:00",
    "wall": _HB_WALL,
    "mono": 1.0,
    "pending": 1,
    "slots": [
        {
            "slot": 1,
            "goal": "g-a",
            "cwd": "/tmp/a",
            "ticket": "T-a",
            "phase": "impl",
            "started_at": "2026-02-01T10:00:00+08:00",
        }
    ],
    "max_concurrent_goals": 2,
    "queued": [{"job": "job-c.json", "reason": "max_concurrent_goals"}],
}


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


# ---------------------------------------------------------------------------
# T3c: status/report show broker concurrency (heartbeat slots / queued)
# ---------------------------------------------------------------------------
def _goal_env(tmp_path: Path, heartbeat: dict | None = _HB_DOC) -> dict:
    """Env with a tmp DSH home (2 goals) + tmp broker state dir (1 heartbeat)."""
    home = tmp_path / "dshhome"
    goals = home / "supervisor" / "thin-state" / "goals"
    goals.mkdir(parents=True, exist_ok=True)
    (goals / "g-a.json").write_text(
        json.dumps(
            {
                "goal": "g-a",
                "status": "running",
                "slices": ["s1"],
                "supervisor_ticket_count": 2,
                "metrics": {"prompt_token_total": 1234},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (goals / "g-c.json").write_text(
        json.dumps({"goal": "g-c", "status": "queued"}) + "\n", encoding="utf-8"
    )
    state = tmp_path / "state"
    state.mkdir(parents=True, exist_ok=True)
    if heartbeat is not None:
        (state / _HEARTBEAT_NAME).write_text(
            json.dumps(heartbeat) + "\n", encoding="utf-8"
        )
    env = {**os.environ, "DSH_HOME": str(home), "TRIAL_BROKER_DIR": str(state)}
    # Ambient checkout/override would shadow the tmp state dir; drop both.
    env.pop("DSH_TRIAL_ROOT", None)
    env.pop("TRIAL_BROKER_HEARTBEAT", None)
    return env


def test_status_text_shows_broker_slots(tmp_path: Path) -> None:
    cp = _run(_goal_env(tmp_path), "status")
    assert cp.returncode == 0, cp.stderr
    out = cp.stdout
    assert "broker pid=4242" in out
    assert "max_concurrent_goals=2" in out
    assert "slots_busy=1/2" in out
    assert (
        "slot=1 goal=g-a phase=impl ticket=T-a cwd=/tmp/a "
        "since=2026-02-01T10:00:00+08:00" in out
    )
    assert "queued name=job-c.json reason=max_concurrent_goals" in out
    running = [ln for ln in out.splitlines() if ln.startswith("- g-a:")]
    assert running and running[0].endswith("slot=1"), out
    queued_goal = [ln for ln in out.splitlines() if ln.startswith("- g-c:")]
    assert queued_goal and not queued_goal[0].endswith("slot=1"), out


def test_status_json_has_broker_and_goal_slots(tmp_path: Path) -> None:
    cp = _run(_goal_env(tmp_path), "status", "--json")
    assert cp.returncode == 0, cp.stderr
    data = json.loads(cp.stdout)
    assert data["broker"]["pid"] == 4242
    assert data["broker"]["max_concurrent_goals"] == 2
    assert data["broker"]["slots"][0]["goal"] == "g-a"
    assert data["broker"]["slots"][0]["slot"] == 1
    assert data["broker"]["queued"][0]["name"] == "job-c.json"
    assert isinstance(data["broker"]["heartbeat_age"], (int, float))
    rows = {r["goal"]: r for r in data["goals"]}
    assert rows["g-a"]["slot"] == 1
    assert rows["g-c"]["slot"] is None


def test_report_json_has_concurrency(tmp_path: Path) -> None:
    cp = _run(_goal_env(tmp_path), "report", "--json")
    assert cp.returncode == 0, cp.stderr
    rep = json.loads(cp.stdout)
    conc = rep["concurrency"]
    assert conc["max_concurrent_goals"] == 2
    assert [s["goal"] for s in conc["slots"]] == ["g-a"]
    assert conc["running_goals"] == [{"goal": "g-a", "slot": 1, "phase": "impl"}]
    assert rep["heartbeat"]["pid"] == 4242
    assert rep["heartbeat"]["queued"][0]["name"] == "job-c.json"


def test_status_and_report_without_heartbeat(tmp_path: Path) -> None:
    env = _goal_env(tmp_path, heartbeat=None)
    cp = _run(env, "status")
    assert cp.returncode == 0, cp.stderr
    assert "slots_busy=0/2" in cp.stdout
    assert "slot=" not in cp.stdout
    assert "queued count=0" in cp.stdout

    rp = _run(env, "report")
    assert rp.returncode == 0, rp.stderr
    assert "### 并发" in rp.stdout
    assert "无心跳" in rp.stdout


def _trial_lib():
    if str(_REPO / "broker") not in sys.path:
        sys.path.insert(0, str(_REPO / "broker"))
    import trial_lib

    return trial_lib


def test_trial_lib_heartbeat_helpers(tmp_path: Path) -> None:
    lib = _trial_lib()
    hb_file = tmp_path / "hb.json"
    hb_file.write_text(json.dumps(_HB_DOC), encoding="utf-8")
    hb = lib.read_broker_heartbeat(hb_file)
    assert hb["pid"] == 4242
    assert lib.read_broker_heartbeat(tmp_path / "missing.json") == {}
    (tmp_path / "garbage.json").write_text("{not json", encoding="utf-8")
    assert lib.read_broker_heartbeat(tmp_path / "garbage.json") == {}
    (tmp_path / "list.json").write_text("[1, 2]", encoding="utf-8")
    assert lib.read_broker_heartbeat(tmp_path / "list.json") == {}

    summary = lib.heartbeat_summary(hb, now=_HB_WALL + 7)
    assert summary["pid"] == 4242
    assert summary["heartbeat_age"] == 7.0
    assert summary["max_concurrent_goals"] == 2
    assert [s["goal"] for s in summary["slots"]] == ["g-a"]
    assert summary["queued"][0]["name"] == "job-c.json"

    # Legacy beat without the T1 concurrency keys degrades to empty, not Error.
    legacy = lib.heartbeat_summary({"pid": 1, "wall": _HB_WALL}, now=_HB_WALL + 3)
    assert legacy["slots"] == [] and legacy["queued"] == []
    assert legacy["max_concurrent_goals"] is None
    assert legacy["heartbeat_age"] == 3.0
    assert lib.heartbeat_summary({})["heartbeat_age"] is None


if __name__ == "__main__":
    import tempfile

    test_help_lists_start_stop_broker_status()
    with tempfile.TemporaryDirectory(prefix="dsh-trial-cli-") as td:
        test_start_stop_wrap_scripts(Path(td))
    with tempfile.TemporaryDirectory(prefix="dsh-trial-cli-") as td:
        test_missing_start_script(Path(td))
    for _name in (
        test_status_text_shows_broker_slots,
        test_status_json_has_broker_and_goal_slots,
        test_report_json_has_concurrency,
        test_status_and_report_without_heartbeat,
        test_trial_lib_heartbeat_helpers,
    ):
        with tempfile.TemporaryDirectory(prefix="dsh-trial-cli-") as td:
            _name(Path(td))
    print("OK dsh-trial CLI start/stop + status/report concurrency")
