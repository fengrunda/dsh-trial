#!/usr/bin/env python3
"""Standalone watchdog for broker-dsh-trial (independent of the broker process).

Runs even when ``trial-broker.py`` is dead or frozen and POSTs the same Hub
webhook the broker uses (``khub-dsh-complete-notify.py``). Three stall checks:

1. broker process missing (pidfile dead/missing and no ``trial-broker.py``)
2. heartbeat file missing/stale (broker writes
   ``$TRIAL_BROKER_DIR/trial-broker.heartbeat.json`` every loop) while no
   ``open-slice.sh`` / ``dsh-acp-ask.py`` work process is alive
3. a Goal is still ``running``/``planning``/``closeout`` in thin-state but there
   is no live ticket (processing + inbox empty and no work process) past the
   goal-stall threshold

Plus freeze detection: when the wall clock jumps forward relative to the
monotonic clock by more than the threshold, emit
``dsh-trial-resumed-after-freeze`` once.

Dedup: the same stall key is not re-sent within ``--cooldown`` seconds.

Env knobs (all optional):
  TRIAL_BROKER_DIR              state dir (default $DSH_HOME/broker-dsh-trial)
  TRIAL_BROKER_HEARTBEAT        heartbeat file path
  TRIAL_WATCHDOG_HEARTBEAT_SEC  stale heartbeat threshold (default 300)
  TRIAL_WATCHDOG_GOAL_STALL_SEC goal thin-state stall threshold (default 600)
  TRIAL_WATCHDOG_COOLDOWN_SEC   per-reason dedup cooldown (default 900)
  DSH_TRIAL_FREEZE_JUMP_SEC     wall-clock jump threshold (default 300)
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import trial_lib as T  # noqa: E402

DSH_HOME = Path(os.environ.get("DSH_HOME") or (Path.home() / ".dsh"))
STATE_DIR = Path(os.environ.get("TRIAL_BROKER_DIR") or (DSH_HOME / "broker-dsh-trial"))
TRIAL_SUP = DSH_HOME / "supervisor" / "trial"
INBOX = TRIAL_SUP / "inbox"
PROCESSING = TRIAL_SUP / "processing"
GOALS = DSH_HOME / "supervisor" / "thin-state" / "goals"
NOTIFY = DSH_HOME / "bin" / "khub-dsh-complete-notify.py"
PIDFILE = STATE_DIR / "trial-broker.pid"
HEARTBEAT = Path(
    os.environ.get("TRIAL_BROKER_HEARTBEAT")
    or (STATE_DIR / "trial-broker.heartbeat.json")
)
STATE_FILE = Path(
    os.environ.get("TRIAL_WATCHDOG_STATE") or (STATE_DIR / "trial-watchdog.state.json")
)

RUNNING_GOAL_STATES = ("running", "planning", "closeout")
STALLED_KIND = "dsh-trial-stalled"
FREEZE_KIND = "dsh-trial-resumed-after-freeze"


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name) or default)
    except (TypeError, ValueError):
        return float(default)


def heartbeat_timeout() -> float:
    return _env_float("TRIAL_WATCHDOG_HEARTBEAT_SEC", 300)


def goal_stall_timeout() -> float:
    return _env_float("TRIAL_WATCHDOG_GOAL_STALL_SEC", 600)


def cooldown_sec() -> float:
    return _env_float("TRIAL_WATCHDOG_COOLDOWN_SEC", 900)


def read_pidfile(pidfile: Path | None = None) -> int | None:
    path = pidfile or PIDFILE
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except (OSError, ValueError):
        return False


def _ps_pids(needles: tuple[str, ...]) -> list[int]:
    """PIDs whose command line contains any needle (best-effort, no pgrep)."""
    try:
        cp = subprocess.run(
            ["ps", "-eo", "pid=,args="],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return []
    out: list[int] = []
    me = os.getpid()
    for line in (cp.stdout or "").splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        try:
            pid = int(parts[0])
        except ValueError:
            continue
        if pid == me:
            continue
        args = parts[1]
        if any(n in args for n in needles):
            out.append(pid)
    return out


def list_broker_pids() -> list[int]:
    # A `--status` invocation is a short-lived query, not the resident loop.
    return [p for p in _ps_pids(("trial-broker.py",))]


def list_work_pids() -> list[int]:
    return _ps_pids(("open-slice.sh", "dsh-acp-ask.py"))


def heartbeat_age(path: Path | None = None, now: float | None = None) -> float | None:
    p = path or HEARTBEAT
    try:
        st = p.stat()
    except OSError:
        return None
    return max(0.0, (time.time() if now is None else now) - st.st_mtime)


def _pending_jobs() -> bool:
    for d in (INBOX, PROCESSING):
        try:
            if any(d.iterdir()):
                return True
        except OSError:
            continue
    return False


def _read_goals(goals_dir: Path | None = None) -> list[dict]:
    root = goals_dir or GOALS
    out: list[dict] = []
    try:
        paths = sorted(root.glob("*.json"))
    except OSError:
        return out
    for p in paths:
        try:
            doc = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(doc, dict):
            out.append(doc)
    return out


def _goal_age_sec(goal: dict, now: float) -> float | None:
    for key in ("updated_at", "created_at"):
        raw = str(goal.get(key) or "").strip()
        if not raw:
            continue
        try:
            from datetime import datetime

            dt = datetime.fromisoformat(raw)
            return max(0.0, now - dt.timestamp())
        except (TypeError, ValueError):
            continue
    return None


def _stalled_event(reason_key: str, goal: str, reason: str, suggested: str) -> dict:
    return {
        "kind": STALLED_KIND,
        "status": "stalled",
        "goal": goal,
        "reason": reason,
        "suggested_action": suggested,
        "reason_key": reason_key,
    }


def collect_stall_events(
    now: float | None = None,
    *,
    broker_pids: list[int] | None = None,
    work_pids: list[int] | None = None,
) -> list[dict]:
    """All currently-detected stall events (no notify, no dedup)."""
    now = time.time() if now is None else now
    broker_pids = list_broker_pids() if broker_pids is None else broker_pids
    work_pids = list_work_pids() if work_pids is None else work_pids
    pid = read_pidfile()
    live = bool(broker_pids) or pid_alive(pid)
    events: list[dict] = []

    if not live:
        events.append(_stalled_event(
            "no_broker_process",
            "*",
            "trial-broker.py not running (no live pid / pidfile)",
            "重启 trial broker（broker/start.sh）并检查 processing",
        ))
    else:
        age = heartbeat_age(HEARTBEAT, now)
        if age is None:
            if not work_pids:
                events.append(_stalled_event(
                    "no_heartbeat",
                    "*",
                    "broker heartbeat file missing",
                    "检查 trial broker 主循环是否启动 / 重启 broker",
                ))
        elif age > heartbeat_timeout() and not work_pids:
            events.append(_stalled_event(
                "heartbeat_stale",
                "*",
                f"broker heartbeat stale {age:.0f}s > {heartbeat_timeout():.0f}s "
                "and no live open-slice/ask process",
                "重启 trial broker；检查 processing / 是否有僵死票",
            ))

    # Condition 3: thin-state still active but no live ticket.
    goals = _read_goals()
    busy = _pending_jobs() or bool(work_pids)
    if not busy:
        for g in goals:
            status = str(g.get("status") or "").lower()
            if status not in RUNNING_GOAL_STATES:
                continue
            age = _goal_age_sec(g, now)
            if age is None or age <= goal_stall_timeout():
                continue
            gid = str(g.get("goal") or "?")
            events.append(_stalled_event(
                f"goal_no_ticket:{gid}",
                gid,
                f"thin-state goal status={status} for {age:.0f}s with no live ticket",
                "检查 processing / 是否需重启 broker 或 salvage",
            ))
    return events


def read_watchdog_state(path: Path | None = None) -> dict:
    p = path or STATE_FILE
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
        return doc if isinstance(doc, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def write_watchdog_state(state: dict, path: Path | None = None) -> None:
    p = path or STATE_FILE
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except OSError:
        pass


def collect_freeze_events(
    now_wall: float | None = None,
    now_mono: float | None = None,
    *,
    state_path: Path | None = None,
    persist: bool = True,
) -> list[dict]:
    """Emit once when the wall clock jumped forward vs the monotonic clock."""
    now_wall = time.time() if now_wall is None else now_wall
    now_mono = time.monotonic() if now_mono is None else now_mono
    prev = read_watchdog_state(state_path)
    prev_wall = prev.get("wall")
    prev_mono = prev.get("mono")
    if persist:
        write_watchdog_state(
            {"wall": now_wall, "mono": now_mono, "at": T.now_iso()}, state_path
        )
    if prev_wall is None or prev_mono is None:
        return []
    drift = T.detect_resumed_after_freeze(prev_wall, prev_mono, now_wall, now_mono)
    if drift is None:
        return []
    return [{
        "kind": FREEZE_KIND,
        "status": "resumed-after-freeze",
        "goal": "*",
        "reason": f"wall clock jumped {drift / 60.0:.1f} min vs monotonic clock",
        "suggested_action": "核对 running Goal / 是否需 salvage；检查系统是否刚从冻住恢复",
        "reason_key": "resumed-after-freeze",
    }]


def notify_event(event: dict, *, dry_run: bool = False) -> dict:
    kind = str(event.get("kind") or STALLED_KIND)
    summary = (
        f"trial-watchdog status={event.get('status')} goal={event.get('goal')} "
        f"reason={event.get('reason')} suggested_action={event.get('suggested_action')}"
    )
    payload = {
        "source": "dsh",
        "kind": kind,
        "status": event.get("status"),
        "goal": event.get("goal"),
        "ticket": "trial-watchdog",
        "summary": summary,
        "reason": event.get("reason"),
        "suggested_action": event.get("suggested_action"),
        "host": "box",
        "at": T.now_iso(),
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    cmd = [
        sys.executable, str(NOTIFY),
        "--json-body", json.dumps(payload, ensure_ascii=False),
        "--source", "dsh",
        "--kind", kind,
    ]
    if dry_run:
        cmd.append("--dry-run")
    try:
        cp = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return {"sent": cp.returncode == 0, "rc": cp.returncode, "kind": kind,
                "detail": (cp.stderr or cp.stdout or "")[-300:]}
    except (OSError, subprocess.SubprocessError) as e:
        return {"sent": False, "error": str(e), "kind": kind}


class Deduper:
    """Suppress the same stall key for a cooldown window (in-memory)."""

    def __init__(self, cooldown: float | None = None):
        self.cooldown = cooldown if cooldown is not None else cooldown_sec()
        self._last: dict[str, float] = {}

    def should_send(self, key: str, now: float | None = None) -> bool:
        now = time.time() if now is None else now
        last = self._last.get(key)
        if last is not None and (now - last) < self.cooldown:
            return False
        self._last[key] = now
        return True


def run_once(
    now: float | None = None,
    *,
    dry_run: bool = False,
    dedupe: Deduper | None = None,
    persist_freeze: bool = True,
) -> dict:
    """Evaluate all checks, dedup, notify. Returns a structured result."""
    now = time.time() if now is None else now
    dedupe = dedupe if dedupe is not None else Deduper()
    events = collect_stall_events(now)
    events += collect_freeze_events(now, time.monotonic(), persist=persist_freeze)
    sent: list[dict] = []
    suppressed: list[str] = []
    for ev in events:
        key = str(ev.get("reason_key") or ev.get("reason") or "")
        if not dedupe.should_send(key, now):
            suppressed.append(key)
            continue
        info = notify_event(ev, dry_run=dry_run)
        ev = dict(ev)
        ev["notify"] = info
        sent.append(ev)
    return {"at": T.now_iso(), "events": events, "sent": sent, "suppressed": suppressed}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--once", action="store_true", help="run one check then exit")
    ap.add_argument("--loop", action="store_true", help="run forever (default)")
    ap.add_argument("--interval", type=float, default=60.0, help="loop interval seconds")
    ap.add_argument("--dry-run", action="store_true", help="print payload, do not POST")
    args = ap.parse_args()

    if args.once:
        result = run_once(dry_run=args.dry_run, persist_freeze=True)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    interval = max(5.0, float(args.interval))
    print(f"[trial-watchdog] loop every {interval:.0f}s state_dir={STATE_DIR}", flush=True)
    dedupe = Deduper()
    while True:
        try:
            run_once(dry_run=args.dry_run, dedupe=dedupe, persist_freeze=True)
        except Exception as e:  # noqa: BLE001 — a watchdog must not die
            print(f"[trial-watchdog] check error: {e}", file=sys.stderr, flush=True)
        time.sleep(interval)


if __name__ == "__main__":
    raise SystemExit(main())
