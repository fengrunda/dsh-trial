#!/usr/bin/env python3
"""Standalone watchdog for broker-dsh-trial (independent of the broker process).

Runs even when ``trial-broker.py`` is dead or frozen and POSTs the same Hub
webhook the broker uses (``khub-dsh-complete-notify.py``). Four stall checks:

1. broker process missing (pidfile dead/missing and no ``trial-broker.py``)
2. heartbeat file missing/stale (broker writes
   ``$TRIAL_BROKER_DIR/trial-broker.heartbeat.json`` every loop) while no
   ``open-slice.sh`` / ``dsh-acp-ask.py`` work process is alive
3. a Goal is still ``running``/``planning``/``closeout`` in thin-state but there
   is no live ticket (processing + inbox empty and no work process) past the
   goal-stall threshold
4. a live ticket (``open-slice.sh`` / ``dsh-acp-ask.py`` pid present) whose ACP
   session/output file has not changed for ``TRIAL_WATCHDOG_SESSION_IDLE_SEC``
   — idle is measured purely from the monotonic clock, so a wall-clock jump
   (freeze/resume/NTP step) can never raise a false session-idle alarm

Plus freeze detection: when the wall clock jumps forward relative to the
monotonic clock by more than the threshold, emit
``dsh-trial-resumed-after-freeze`` once.

Dedup: the same stall key is not re-sent within ``--cooldown`` seconds.

``--dry-run`` never writes state: neither the freeze baseline nor the
session-idle accounting is persisted on a dry-run pass.

Env knobs (all optional):
  TRIAL_BROKER_DIR              state dir (default $DSH_HOME/broker-dsh-trial)
  TRIAL_BROKER_HEARTBEAT        heartbeat file path
  TRIAL_WATCHDOG_HEARTBEAT_SEC  stale heartbeat threshold (default 300)
  TRIAL_WATCHDOG_GOAL_STALL_SEC goal thin-state stall threshold (default 600)
  TRIAL_WATCHDOG_COOLDOWN_SEC   per-reason dedup cooldown (default 900)
  DSH_TRIAL_FREEZE_JUMP_SEC     wall-clock jump threshold (default 300)
  TRIAL_WATCHDOG_SESSION_IDLE_SEC  live-ticket session idle threshold (default 1800)
  TRIAL_WATCHDOG_LOG_DIR        open-slice default log dir (default /workspace/tmp)
  DSH_HOMES_ROOT                ACP homes root for the session fallback
                                (default ~/.dsh-homes)
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
OPEN_SLICE_LOG_DIR = Path(
    os.environ.get("TRIAL_WATCHDOG_LOG_DIR") or "/workspace/tmp"
)
HOMES_ROOT = Path(
    os.environ.get("DSH_HOMES_ROOT") or (Path.home() / ".dsh-homes")
)
SESSION_PROJCACHE_SESSIONS = Path("storages") / "session_projcache" / "sessions"

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


def session_idle_timeout() -> float:
    return _env_float("TRIAL_WATCHDOG_SESSION_IDLE_SEC", 1800)


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


def _read_proc_cmdline(pid: int) -> list[str]:
    """NUL-split argv of a live pid (empty list when it vanished)."""
    try:
        raw = Path(f"/proc/{int(pid)}/cmdline").read_bytes()
    except (OSError, ValueError):
        return []
    return [part for part in raw.decode("utf-8", "replace").split("\0") if part]


def _arg_value(argv: list[str], flag: str) -> str | None:
    for i, arg in enumerate(argv):
        if arg == flag and i + 1 < len(argv):
            return argv[i + 1]
        if arg.startswith(flag + "="):
            return arg.split("=", 1)[1]
    return None


def _proc_children(pid: int) -> list[int]:
    out: list[int] = []
    try:
        tasks = list(Path(f"/proc/{int(pid)}/task").iterdir())
    except (OSError, ValueError):
        return out
    for task in tasks:
        try:
            raw = (task / "children").read_text(encoding="utf-8")
        except OSError:
            continue
        for token in raw.split():
            try:
                out.append(int(token))
            except ValueError:
                continue
    return out


def _descendant_pids(pid: int, *, limit: int = 64) -> list[int]:
    """Best-effort descendant pids of ``pid`` (empty when unknown)."""
    out: list[int] = []
    stack = [pid]
    while stack and len(out) < limit:
        cur = stack.pop()
        for child in _proc_children(cur):
            if child != pid and child not in out:
                out.append(child)
                stack.append(child)
    return out


def _fd_targets(pid: int) -> list[str]:
    """readlink targets of every open fd for ``pid`` (best-effort)."""
    out: list[str] = []
    try:
        entries = list(Path(f"/proc/{int(pid)}/fd").iterdir())
    except (OSError, ValueError):
        return out
    for entry in entries:
        try:
            out.append(os.readlink(entry))
        except OSError:
            continue
    return out


def _is_session_artifact(target: str) -> bool:
    return target.endswith(".log") or (
        "/sessions/" in target and target.endswith(".json")
    )


def _path_exists(path: Path) -> bool:
    try:
        return path.exists()
    except OSError:
        return False


def _newest_session_json() -> Path | None:
    """Newest (mtime) session json under the ACP homes, or None."""
    best: Path | None = None
    best_mtime: int | None = None
    for sub in ("impl", "gate", "supervisor"):
        root = HOMES_ROOT / sub / SESSION_PROJCACHE_SESSIONS
        try:
            entries = list(root.glob("*.json"))
        except OSError:
            continue
        for path in entries:
            try:
                mtime = path.stat().st_mtime_ns
            except OSError:
                continue
            if best_mtime is None or mtime > best_mtime:
                best_mtime, best = mtime, path
    return best


def discover_session_outputs(work_pids: list[int]) -> dict[str, list[Path]]:
    """Best-effort map of live ticket -> candidate session/output files.

    Priority: cmdline ``--log`` / ``--ticket`` (open-slice default log), then
    open fds of the pid and its descendants (``*/sessions/*.json`` / ``*.log``),
    then the newest session json under ``HOMES_ROOT`` when nothing exists yet.
    All OS errors are swallowed; a vanished pid contributes nothing.
    """
    per_pid: list[tuple[str, list[Path]]] = []
    for pid in work_pids:
        argv = _read_proc_cmdline(pid)
        log_arg = _arg_value(argv, "--log")
        ticket = _arg_value(argv, "--ticket")
        goal = _arg_value(argv, "--goal")
        key = ticket or goal or f"pid:{pid}"
        cands: list[Path] = []
        if log_arg:
            cands.append(Path(log_arg))
        if ticket and not log_arg:
            cands.append(OPEN_SLICE_LOG_DIR / f"{ticket}.log")
        for proc in [pid, *_descendant_pids(pid)]:
            for target in _fd_targets(proc):
                if _is_session_artifact(target):
                    cands.append(Path(target))
        uniq: list[Path] = []
        seen: set[str] = set()
        for cand in cands:
            if str(cand) not in seen:
                seen.add(str(cand))
                uniq.append(cand)
        per_pid.append((key, uniq))

    if not any(_path_exists(p) for _, cands in per_pid for p in cands):
        newest = _newest_session_json()
        if newest is not None:
            return {"fallback:sessions": [newest]}

    result: dict[str, list[Path]] = {}
    for key, cands in per_pid:
        bucket = result.setdefault(key, [])
        known = {str(c) for c in bucket}
        for cand in cands:
            if str(cand) not in known:
                bucket.append(cand)
                known.add(str(cand))
    return result


def output_signature(paths: list[Path] | None) -> str | None:
    """``max_mtime_ns:total_size`` over existing files, or None if none exist."""
    max_mtime: int | None = None
    total = 0
    for path in paths or []:
        try:
            st = path.stat()
        except OSError:
            continue
        max_mtime = st.st_mtime_ns if max_mtime is None else max(max_mtime, st.st_mtime_ns)
        total += st.st_size
    if max_mtime is None:
        return None
    return f"{max_mtime}:{total}"


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
    """Read-modify-write ``state`` so freeze and session-idle keys coexist."""
    p = path or STATE_FILE
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        doc = read_watchdog_state(p)
        doc.update(state)
        p.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
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


def collect_session_idle_events(
    work_pids: list[int],
    now_mono: float,
    state: dict | None = None,
) -> tuple[list[dict], dict]:
    """Detect live tickets whose ACP session/output has gone idle.

    Idle is ``now_mono - since_mono`` for an unchanged output signature; a new
    key, a changed signature or a monotonic regression resets the baseline.
    Returns ``(events, new_session_idle_state)``; discovery is injectable via
    :func:`discover_session_outputs`.
    """
    state = dict(state) if isinstance(state, dict) else {}
    if not work_pids:
        return [], {}
    outputs = discover_session_outputs(work_pids)
    thr = session_idle_timeout()
    pids_txt = ",".join(str(p) for p in sorted(work_pids))
    events: list[dict] = []
    new_state: dict[str, dict] = {}
    for key, paths in outputs.items():
        sig = output_signature(paths)
        if sig is None:
            # No output artifact yet: nothing measurable to account for.
            continue
        prev = state.get(key)
        prev = prev if isinstance(prev, dict) else {}
        prev_sig = prev.get("sig")
        prev_since = prev.get("since_mono")
        reset = (
            prev_sig != sig
            or not isinstance(prev_since, (int, float))
            or float(now_mono) < float(prev_since)
        )
        if reset:
            new_state[key] = {
                "sig": sig,
                "since_mono": float(now_mono),
                "since_at": T.now_iso(),
            }
            continue
        idle = float(now_mono) - float(prev_since)
        if idle > thr:
            goal = "*" if key.startswith(("pid:", "fallback:")) else key
            events.append(_stalled_event(
                f"session_idle:{key}",
                goal,
                f"live ticket but session/output idle {idle:.0f}s > {thr:.0f}s "
                f"(pids={pids_txt})",
                "疑似 ACP 挂起：检查对应 dsh-acp-ask / ACP 会话是否 hung，必要时 salvage 该票",
            ))
        new_state[key] = {
            "sig": sig,
            "since_mono": float(prev_since),
            "since_at": prev.get("since_at") or T.now_iso(),
        }
    return events, new_state


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
    persist: bool | None = None,
    now_mono: float | None = None,
) -> dict:
    """Evaluate all checks, dedup, notify. Returns a structured result.

    ``persist`` overrides the legacy ``persist_freeze`` switch (which is kept
    for back-compat); either way ``dry_run=True`` forces a read-only pass so no
    state is ever written.
    """
    now = time.time() if now is None else now
    now_mono = time.monotonic() if now_mono is None else now_mono
    dedupe = dedupe if dedupe is not None else Deduper()
    persist = (persist_freeze if persist is None else persist) and not dry_run

    broker_pids = list_broker_pids()
    work_pids = list_work_pids()
    prior = read_watchdog_state()

    events = collect_stall_events(now, broker_pids=broker_pids, work_pids=work_pids)
    events += collect_freeze_events(now, now_mono, persist=False)
    idle_events, idle_state = collect_session_idle_events(
        work_pids, now_mono, prior.get("session_idle") or {}
    )
    events += idle_events

    if persist:
        doc = dict(prior)
        doc["wall"] = now
        doc["mono"] = now_mono
        doc["at"] = T.now_iso()
        doc["session_idle"] = idle_state
        write_watchdog_state(doc)

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
        result = run_once(dry_run=args.dry_run, persist_freeze=not args.dry_run)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0

    interval = max(5.0, float(args.interval))
    print(f"[trial-watchdog] loop every {interval:.0f}s state_dir={STATE_DIR}", flush=True)
    dedupe = Deduper()
    while True:
        try:
            run_once(
                dry_run=args.dry_run,
                dedupe=dedupe,
                persist_freeze=not args.dry_run,
            )
        except Exception as e:  # noqa: BLE001 — a watchdog must not die
            print(f"[trial-watchdog] check error: {e}", file=sys.stderr, flush=True)
        time.sleep(interval)


if __name__ == "__main__":
    raise SystemExit(main())
