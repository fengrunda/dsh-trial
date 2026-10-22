"""Watchdog orphan scan perceives; it does not write status=escalated."""
from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_wspec = importlib.util.spec_from_file_location(
    "trial_watchdog_perceive", ROOT / "trial-watchdog.py"
)
wd = importlib.util.module_from_spec(_wspec)
_wspec.loader.exec_module(wd)

_bspec = importlib.util.spec_from_file_location(
    "trial_broker_perceive", ROOT / "trial-broker.py"
)
tb = importlib.util.module_from_spec(_bspec)
_bspec.loader.exec_module(tb)


def _patch(tmp_path: Path):
    dirs = {n: tmp_path / n for n in ("goals", "inbox", "processing", "chains", "mailbox", "outbox")}
    for p in dirs.values():
        p.mkdir(parents=True, exist_ok=True)
    return mock.patch.multiple(
        wd,
        GOALS=dirs["goals"],
        INBOX=dirs["inbox"],
        PROCESSING=dirs["processing"],
        CHAINS=dirs["chains"],
        MAILBOX=dirs["mailbox"],
        OUTBOX=dirs["outbox"],
        HEARTBEAT=tmp_path / "heartbeat.json",
    ), dirs


def _old_goal(dirs: dict, goal_id: str) -> Path:
    path = dirs["goals"] / f"{goal_id}.json"
    path.write_text(
        json.dumps(
            {
                "goal": goal_id,
                "status": "running",
                "updated_at": "2020-01-01T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )
    return path


def test_fresh_heartbeat_slot_blocks_handoff_when_ticket_empty(tmp_path: Path):
    patch, dirs = _patch(tmp_path)
    goal_id = "goal-slot"
    now = time.time()
    with patch:
        goal_path = _old_goal(dirs, goal_id)
        wd.HEARTBEAT.write_text(
            json.dumps({"slots": [{"slot": 1, "goal": goal_id, "ticket": ""}]}),
            encoding="utf-8",
        )
        events = wd.sweep_stale_gate_running_and_orphans(
            now=now, work_pids=[], dry_run=False
        )
        assert events == []
        goal = json.loads(goal_path.read_text(encoding="utf-8"))
        assert goal["status"] == "running"
        assert "escalate_reason" not in goal
        assert "perceive_handoff" not in goal

        old = now - 10_000
        os.utime(wd.HEARTBEAT, (old, old))
        events = wd.sweep_stale_gate_running_and_orphans(
            now=now, work_pids=[], dry_run=False
        )
        assert [e["kind"] for e in events] == [wd.ORPHAN_HANDOFF_KIND]
        goal = json.loads(goal_path.read_text(encoding="utf-8"))
        assert goal["status"] == "running"
        assert "escalate_reason" not in goal


def test_mac_child_process_blocks_handoff(tmp_path: Path):
    if sys.platform == "darwin":
        child = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            env={**os.environ, "DSH_TRIAL_GUARD": "1"},
        )
        try:
            seen = False
            for _ in range(20):
                if child.pid in wd._proc_children(os.getpid()):
                    seen = True
                    break
                time.sleep(0.05)
            assert seen, "ps child list did not include the spawned pid"
            assert wd._proc_env_has(child.pid, "DSH_TRIAL_GUARD", "1") is True
        finally:
            child.kill()
            child.wait(timeout=5)

    patch, dirs = _patch(tmp_path)
    goal_id = "goal-mac-child"
    ticket = f"impl-trial-{goal_id}-s2-r1"
    with patch, mock.patch.object(wd.sys, "platform", "darwin"), mock.patch.object(
        wd, "_ppid_map", return_value={4242: 10}
    ), mock.patch.object(wd, "list_broker_pids", return_value=[10]), mock.patch.object(
        wd, "_ps_pids", return_value=[4242]
    ), mock.patch.object(
        wd,
        "_read_proc_cmdline",
        return_value=["/bin/open-slice.sh", "--ticket", ticket],
    ):
        assert wd.list_work_pids() == [4242]
        goal_path = _old_goal(dirs, goal_id)
        events = wd.sweep_stale_gate_running_and_orphans(now=time.time(), dry_run=False)
    goal = json.loads(goal_path.read_text(encoding="utf-8"))
    assert events == []
    assert goal["status"] == "running"
    assert "escalate_reason" not in goal
    assert "perceive_handoff" not in goal


def test_no_signals_hands_to_supervisor_not_escalated(tmp_path: Path):
    patch, dirs = _patch(tmp_path)
    goal_id = "goal-none"
    with patch:
        goal_path = _old_goal(dirs, goal_id)
        events = wd.sweep_stale_gate_running_and_orphans(
            now=time.time(), work_pids=[], dry_run=False
        )
    assert [e["kind"] for e in events] == [wd.ORPHAN_HANDOFF_KIND]
    assert events[0]["status"] == "handoff"
    assert events[0]["supervisor_ticket"] == f"supervisor-goal-{goal_id}"
    goal = json.loads(goal_path.read_text(encoding="utf-8"))
    assert goal["status"] == "running"
    assert "escalate_reason" not in goal
    assert goal["perceive_handoff"]["handed"] is True
    assert goal["perceive_handoff"]["opened"] is False
    bodies = [
        json.loads(p.read_text(encoding="utf-8"))
        for p in sorted(dirs["outbox"].glob("*.json"))
    ]
    assert len(bodies) == 1
    assert bodies[0]["kind"] == wd.ORPHAN_HANDOFF_KIND
    assert bodies[0]["status"] == "handoff"
    assert bodies[0]["status"] != "escalated"

    with mock.patch.object(tb, "GOALS", dirs["goals"]), mock.patch.object(
        tb, "auto_supervisor_pause_decision", return_value="escalate"
    ) as pause, mock.patch.object(
        tb, "route_pause_decision", return_value=1
    ), mock.patch.object(tb, "_notify_goal_event"):
        items = tb.collect_perceive_handoff_items()
        assert len(items) == 1
        assert items[0]["runner"] is tb.run_perceive_handoff
        rc = tb.run_perceive_handoff(items[0]["path"], items[0]["job"])
    assert rc == 1
    assert pause.call_args.kwargs["pause_kind"] == "orphan_running_no_processing"
    reuse = tb._supervisor_reuse_kwargs({"goal": goal_id})
    assert reuse["keep_open"] is True
    assert reuse["reuse_ticket"] == f"supervisor-goal-{goal_id}"
    after = json.loads(goal_path.read_text(encoding="utf-8"))
    assert after["status"] == "escalated"
    assert after["escalate_reason"] == "supervisor_pause:orphan_running_no_processing"
