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

Plus freeze detection: emit ``dsh-trial-resumed-after-freeze`` once when any of
three criteria fires in a round —

  a. wall − monotonic drift ≥ ``DSH_TRIAL_FREEZE_JUMP_SEC`` (default 300)
  b. boottime − monotonic drift ≥ the same threshold (skipped when the platform
     has no ``CLOCK_BOOTTIME``)
  c. a single round took ``expected_interval + threshold`` monotonic seconds
     while the previous round belonged to the same loop pid (``--loop`` only, so
     a watchdog restart never mistakes its own downtime for a freeze)

The freeze round and the ``TRIAL_WATCHDOG_FREEZE_GRACE_SEC`` seconds after it
(monotonic deadline persisted as ``freeze_grace_until_mono``) discard the
clock-based stalls — ``heartbeat_stale`` / ``no_heartbeat`` /
``goal_no_ticket:*`` — because a wall-clock jump makes all of them lie.
``no_broker_process`` and the already-monotonic ``session_idle:*`` still report.
Past the grace window the old rules apply unchanged, so a heartbeat the broker
really stopped renewing still stalls.

Note on scope: ``ensure-up`` restart and wallclock freeze recovery only handle a
*dead* process or a jumped clock — neither notices "process alive, ACP stream
dead". That case is exactly check 4: on a ``session_idle:*`` stall the watchdog
now reaps the stuck ticket tree (unless ``TRIAL_WATCHDOG_KILL_IDLE_ASK`` is off)
so the broker can collect the ticket instead of hanging forever.

Dedup: the same stall key is not re-sent within ``--cooldown`` seconds.

``--dry-run`` never writes state: neither the freeze baseline nor the
session-idle accounting is persisted on a dry-run pass.

Env knobs (all optional):
  TRIAL_BROKER_DIR              state dir (default $DSH_HOME/broker-dsh-trial)
  TRIAL_BROKER_HEARTBEAT        heartbeat file path
  TRIAL_WATCHDOG_HEARTBEAT_SEC  stale heartbeat threshold (default 300)
  TRIAL_WATCHDOG_GOAL_STALL_SEC goal thin-state stall threshold (default 600)
  TRIAL_WATCHDOG_COOLDOWN_SEC   per-reason dedup cooldown (default 900)
  DSH_TRIAL_FREEZE_JUMP_SEC     wall/boottime jump threshold (default 300)
  TRIAL_WATCHDOG_FREEZE_GRACE_SEC  post-freeze grace for clock-based stalls
                                (default 120, never below --interval)
  TRIAL_WATCHDOG_SESSION_IDLE_SEC  live-ticket session idle threshold (default 1800)
  TRIAL_WATCHDOG_KILL_IDLE_ASK  kill the stuck ticket tree on a session_idle
                                stall (default on; 0/false/off/no disables)
  TRIAL_WATCHDOG_LOG_DIR        open-slice default log dir (default /workspace/tmp)
  DSH_HOMES_ROOT                ACP homes root for the session fallback
                                (default ~/.dsh-homes)
"""
from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pause_supervisor as PS  # noqa: E402
import trial_lib as T  # noqa: E402

DSH_HOME = Path(os.environ.get("DSH_HOME") or (Path.home() / ".dsh"))
STATE_DIR = Path(os.environ.get("TRIAL_BROKER_DIR") or (DSH_HOME / "broker-dsh-trial"))
TRIAL_SUP = DSH_HOME / "supervisor" / "trial"
INBOX = TRIAL_SUP / "inbox"
PROCESSING = TRIAL_SUP / "processing"
OUTBOX = TRIAL_SUP / "outbox"
GOALS = DSH_HOME / "supervisor" / "thin-state" / "goals"
CHAINS = DSH_HOME / "supervisor" / "thin-state" / "chains"
_PARKED_CHAIN_STATES = frozenset({
    "awaiting_supervisor",
    "paused_limit",
    "paused_wallclock",
    "paused",
})
MAILBOX = T.MAILBOX
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
# T1/T2 slot-isolated role homes (mirrors trial-broker.py's SLOT_HOMES_ROOT):
# ``<HOMES_ROOT>/trial-slots/slot-<n>/{impl,gate,supervisor}``.
SLOT_HOMES_ROOT = Path(
    os.environ.get("DSH_TRIAL_SLOT_HOMES_ROOT") or (HOMES_ROOT / "trial-slots")
)
SESSION_PROJCACHE_SESSIONS = Path("storages") / "session_projcache" / "sessions"
_ROLE_HOMES = ("impl", "gate", "supervisor")

# open-slice logs carry a small header (``=== run ===``, ``DSH_HOME=``,
# ``sessionId=``) written once at start; the rest of the file stays static until
# the ACP prompt ends, so only the header needs sniffing.
_LOG_SNIFF_BYTES = 64 * 1024
_SESSION_ID_RE = re.compile(r"^sessionId=([0-9A-Za-z-]+)\s*$", re.MULTILINE)
_DSH_HOME_RE = re.compile(r"^DSH_HOME=(\S+)", re.MULTILINE)

RUNNING_GOAL_STATES = ("running", "planning", "closeout")
STALLED_KIND = "dsh-trial-stalled"
FREEZE_KIND = "dsh-trial-resumed-after-freeze"
STALE_GATE_KIND = "dsh-trial-stale-gate-running"
# Transitional outbox kind. Not a terminal notify and not ``status=escalated``.
ORPHAN_HANDOFF_KIND = "orphan_supervisor_handoff"
ORPHAN_ESCALATE_REASON = "orphan_running_no_processing"


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


def stale_gate_sec() -> float:
    """Age after which a ``gate_running`` ack with no process is stale."""
    return _env_float("TRIAL_WATCHDOG_STALE_GATE_SEC", 180)


def cooldown_sec() -> float:
    return _env_float("TRIAL_WATCHDOG_COOLDOWN_SEC", 900)


# Grace between the SIGTERM and the SIGKILL when reaping a stuck ticket tree.
KILL_IDLE_ASK_GRACE_SEC = 1.0

_KILL_IDLE_ASK_OFF = frozenset({"0", "false", "off", "no"})


def kill_idle_ask_enabled() -> bool:
    """Whether a ``session_idle:*`` stall may reap the stuck ticket tree.

    On by default; ``TRIAL_WATCHDOG_KILL_IDLE_ASK`` set to ``0``/``false``/
    ``off``/``no`` turns it off. An unset or empty value keeps it on.
    """
    raw = (os.environ.get("TRIAL_WATCHDOG_KILL_IDLE_ASK") or "").strip().lower()
    return raw not in _KILL_IDLE_ASK_OFF


FREEZE_GRACE_DEFAULT_SEC = 120

# Stalls whose verdict depends on the wall clock (heartbeat mtime, goal
# thin-state timestamps) and therefore lie for a whole freeze recovery window.
CLOCK_STALL_KEYS = frozenset({"heartbeat_stale", "no_heartbeat"})
CLOCK_STALL_KEY_PREFIXES = ("goal_no_ticket:",)

# ``boottime=None`` must be distinguishable from "detect it yourself".
_AUTO = object()


def freeze_grace_sec(expected_interval: float | None = None) -> float:
    """Grace after a detected freeze, floored at the loop interval and at
    twice the heartbeat timeout (a broker that just froze needs at least two
    missed-heartbeat beats of slack before a stall verdict is trustworthy)."""
    grace = _env_float("TRIAL_WATCHDOG_FREEZE_GRACE_SEC", FREEZE_GRACE_DEFAULT_SEC)
    floor = 2.0 * heartbeat_timeout()
    if expected_interval is not None:
        floor = max(floor, float(expected_interval))
    return max(0.0, grace, floor)


def boottime_now() -> float | None:
    """``CLOCK_BOOTTIME`` seconds, or ``None`` when the platform lacks it."""
    clock = getattr(time, "CLOCK_BOOTTIME", None)
    if clock is None:
        return None
    try:
        return float(time.clock_gettime(clock))
    except (OSError, ValueError, AttributeError):
        return None


def is_clock_related_stall(event: dict) -> bool:
    """True for stall reasons invalidated by a wall-clock jump (freeze grace)."""
    key = str(event.get("reason_key") or "")
    if key in CLOCK_STALL_KEYS:
        return True
    return any(key.startswith(prefix) for prefix in CLOCK_STALL_KEY_PREFIXES)


def persisted_grace_deadline(
    now_mono: float,
    state: dict | None = None,
    grace_sec: float | None = None,
) -> float | None:
    """The still-trusted persisted grace deadline, or ``None`` when unusable.

    Rejected: missing/expired values, and values further ahead than
    ``grace_sec`` (+1s tolerance). The upper bound matters because the monotonic
    clock restarts near zero on reboot — a deadline left over from the previous
    boot (e.g. 67000) would otherwise hold the grace window open for hours and
    swallow a genuinely stale heartbeat.
    """
    doc = state if isinstance(state, dict) else {}
    until = doc.get("freeze_grace_until_mono")
    if not isinstance(until, (int, float)):
        return None
    until = float(until)
    if float(now_mono) >= until:
        return None
    if grace_sec is not None and until - float(now_mono) > float(grace_sec) + 1.0:
        return None
    return until


def grace_is_active(
    now_mono: float,
    freeze: bool,
    state: dict | None = None,
    grace_sec: float | None = None,
) -> bool:
    """The freeze round itself, plus every round before the persisted deadline.

    ``grace_sec`` bounds how far ahead that deadline may sit (a stale deadline
    from a previous boot is ignored); ``None`` keeps the old unbounded check.
    """
    if freeze:
        return True
    return persisted_grace_deadline(now_mono, state, grace_sec) is not None


def _freeze_state_fields(
    now_wall: float,
    now_mono: float,
    boot: float | None,
    loop_pid: int,
    expected_interval: float | None,
    grace_until: float | None,
) -> dict:
    """Baseline (+ loop identity and grace deadline) persisted every round."""
    fields: dict = {
        "wall": float(now_wall),
        "mono": float(now_mono),
        "loop_pid": int(loop_pid),
        "at": T.now_iso(),
    }
    if boot is not None:
        fields["boot"] = float(boot)
    if expected_interval is not None:
        fields["interval"] = float(expected_interval)
    if grace_until is not None:
        fields["freeze_grace_until_mono"] = float(grace_until)
    return fields


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


def _proc_env_has(pid: int, key: str, value: str, environ: bytes | None = None) -> bool:
    """True when ``pid``'s environment contains ``key=value``.

    ``environ`` (the raw NUL-delimited block) is injectable for tests; a
    missing or unreadable environ block counts as "no match".
    Darwin reads the env tail of ``KERN_PROCARGS2``; Linux reads
    ``/proc/<pid>/environ``.
    """
    raw = environ if environ is not None else _read_proc_environ(pid)
    if not raw:
        return False
    try:
        return f"{key}={value}" in raw.decode("utf-8", "replace").split("\0")
    except (UnicodeDecodeError, ValueError):
        return False


def list_work_pids() -> list[int]:
    """Work pids belonging to *this* broker, not every look-alike on the box.

    A candidate qualifies when it is a descendant of a broker pid, or when it
    carries ``DSH_TRIAL_GUARD=1`` — the guard covers children orphaned onto
    init after the broker died.  Foreign ``dsh-acp-ask.py`` / ``open-slice.sh``
    invocations started by other agents carry neither and are dropped.
    """
    owned: set[int] = set()
    for pid in list_broker_pids():
        owned.update(_descendant_pids(pid))
    out: list[int] = []
    for pid in _ps_pids(("open-slice.sh", "dsh-acp-ask.py")):
        if pid in owned or _proc_env_has(pid, "DSH_TRIAL_GUARD", "1"):
            out.append(pid)
    return out


def _split_kern_procargs2(raw: bytes) -> tuple[list[str], bytes]:
    """``(argv, env tail)`` from a macOS ``KERN_PROCARGS2`` blob.

    Layout: native ``argc``, NUL-terminated executable path, NUL padding,
    then ``argc`` NUL-terminated arguments. The environment block follows
    the arguments and is returned unparsed (NUL-delimited ``KEY=value``).
    """
    if len(raw) < 4:
        return [], b""
    argc = int.from_bytes(raw[:4], sys.byteorder)
    if argc <= 0:
        return [], b""
    rest = raw[4:]
    nul = rest.find(b"\0")
    if nul < 0:
        return [], b""
    rest = rest[nul + 1 :]
    while rest[:1] == b"\0":
        rest = rest[1:]
    argv: list[str] = []
    while rest and len(argv) < argc:
        nul = rest.find(b"\0")
        if nul < 0:
            break
        argv.append(rest[:nul].decode("utf-8", "replace"))
        rest = rest[nul + 1 :]
    return argv, rest


def _parse_kern_procargs2(raw: bytes) -> list[str]:
    """argv from a macOS ``KERN_PROCARGS2`` blob. Env tail is ignored here."""
    argv, _env = _split_kern_procargs2(raw)
    return [part for part in argv if part]


def _kern_procargs2_environ(raw: bytes) -> bytes:
    """NUL-delimited environment tail of a ``KERN_PROCARGS2`` blob."""
    _argv, env = _split_kern_procargs2(raw)
    return env


def _read_proc_environ(pid: int) -> bytes | None:
    """NUL-delimited environ, or None when the pid is unreadable.

    Darwin has no ``/proc``. The env block is the tail of the same
    ``sysctl(KERN_PROCARGS2)`` blob already used for argv.
    """
    if sys.platform == "darwin":
        raw = _darwin_procargs2_bytes(pid)
        if not raw:
            return None
        return _kern_procargs2_environ(raw)
    try:
        return Path(f"/proc/{int(pid)}/environ").read_bytes()
    except (OSError, ValueError):
        return None


def _darwin_procargs2_bytes(pid: int) -> bytes | None:
    """Raw ``KERN_PROCARGS2`` blob, or None when the pid is unreadable.

    ``ps -o args=`` stops at the terminal width, so a long ``--ticket`` can
    lose the goal id. This sysctl returns the full vector (up to ``ARG_MAX``).
    """
    try:
        import ctypes
        import ctypes.util

        libc_name = ctypes.util.find_library("c")
        if not libc_name:
            return None
        libc = ctypes.CDLL(libc_name, use_errno=True)
        argmax = ctypes.c_int()
        size = ctypes.c_size_t(ctypes.sizeof(argmax))
        # CTL_KERN=1, KERN_ARGMAX=8
        mib_max = (ctypes.c_int * 2)(1, 8)
        if libc.sysctl(mib_max, 2, ctypes.byref(argmax), ctypes.byref(size), None, 0) != 0:
            return None
        if argmax.value <= 0:
            return None
        buf = ctypes.create_string_buffer(argmax.value)
        size = ctypes.c_size_t(argmax.value)
        # KERN_PROCARGS2=49
        mib = (ctypes.c_int * 3)(1, 49, int(pid))
        if libc.sysctl(mib, 3, buf, ctypes.byref(size), None, 0) != 0:
            return None
        n = int(size.value)
        if n <= 0:
            return None
        return buf.raw[:n]
    except (OSError, ValueError, AttributeError):
        return None


def _read_darwin_cmdline(pid: int) -> list[str]:
    raw = _darwin_procargs2_bytes(pid)
    if not raw:
        return []
    return _parse_kern_procargs2(raw)


def _read_proc_cmdline(pid: int) -> list[str]:
    """NUL-split argv of a live pid (empty list when it vanished).

    Linux reads ``/proc/<pid>/cmdline``. Darwin has no ``/proc``; argv comes
    from ``sysctl(KERN_PROCARGS2)`` so a goal id inside ``--ticket`` still
    matches.
    """
    if sys.platform == "darwin":
        return _read_darwin_cmdline(pid)
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


def read_heartbeat(path: Path | None = None) -> dict:
    """Parsed broker heartbeat JSON; ``{}`` when missing or unreadable.

    An unreadable heartbeat is *not* an error here: the age-based checks own
    liveness, so callers just see "no slots published".
    """
    p = path or HEARTBEAT
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return doc if isinstance(doc, dict) else {}


def _known_goal_ids() -> list[str]:
    out: list[str] = []
    for g in _read_goals():
        gid = str(g.get("goal") or "")
        if gid and gid not in out:
            out.append(gid)
    return out


def _proc_goal_id(pid: int, known: list[str], environ: bytes | None = None) -> str:
    """Goal a work pid belongs to: env first, cmdline ``--ticket`` as fallback.

    ``environ`` (the raw NUL-delimited block) is injectable for tests.
    """
    raw = environ if environ is not None else (_read_proc_environ(pid) or b"")
    try:
        for chunk in raw.decode("utf-8", "replace").split("\0"):
            if chunk.startswith("DSH_TRIAL_GOAL="):
                gid = chunk.split("=", 1)[1].strip()
                if gid:
                    return gid
    except (UnicodeDecodeError, ValueError):
        pass
    ticket = _arg_value(_read_proc_cmdline(pid), "--ticket") or ""
    if ticket:
        for gid in known:
            if gid and (gid in ticket or ticket in gid):
                return gid
    return "*"


def work_pid_goals(
    pids: list[int] | None = None, *, now: float | None = None
) -> dict[str, list[int]]:
    """``goal id -> [pid]`` for live work processes; unattributable pids -> ``"*"``."""
    pids = list_work_pids() if pids is None else list(pids)
    known = _known_goal_ids()
    _ = now  # accepted for symmetry with the other collectors
    out: dict[str, list[int]] = {}
    for pid in pids:
        out.setdefault(_proc_goal_id(pid, known), []).append(pid)
    return out


def _ppid_map() -> dict[int, int]:
    """``pid -> ppid`` from ``ps``. Empty when ps cannot be read.

    macOS has no ``/proc/<pid>/task/*/children``. One ``ps`` pass is the
    process table the orphan scan walks for broker descendants.
    """
    try:
        cp = subprocess.run(
            ["ps", "-ax", "-o", "pid=,ppid="],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return {}
    out: dict[int, int] = {}
    for line in (cp.stdout or "").splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        try:
            out[int(parts[0])] = int(parts[1])
        except ValueError:
            continue
    return out


def _proc_children(pid: int, ppid_map: dict[int, int] | None = None) -> list[int]:
    """Direct child pids. Darwin uses ``ps``; Linux reads ``/proc``."""
    if sys.platform == "darwin":
        mapping = _ppid_map() if ppid_map is None else ppid_map
        parent = int(pid)
        return [child for child, ppid in mapping.items() if ppid == parent]
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
    ppid_map = _ppid_map() if sys.platform == "darwin" else None
    out: list[int] = []
    stack = [pid]
    while stack and len(out) < limit:
        cur = stack.pop()
        for child in _proc_children(cur, ppid_map):
            if child != pid and child not in out:
                out.append(child)
                stack.append(child)
    return out


def pids_for_session_idle_key(key: str, work_pids: list[int]) -> list[int]:
    """Work pids (plus descendants) owning a ``session_idle:<key>`` event.

    ``key`` may carry the ``session_idle:`` prefix. A ``pid:N`` key maps to that
    pid only when it is a known work pid; a ticket/goal key maps to every work
    pid whose cmdline ``--ticket`` (or ``--goal``) equals it; a ``fallback:`` key
    is not attributable to any process and yields ``[]``. Every seed's
    descendants are included so the whole stuck tree is covered.
    """
    raw = str(key or "")
    if raw.startswith("session_idle:"):
        raw = raw.split(":", 1)[1]
    if not raw or raw.startswith("fallback:"):
        return []
    work = [int(p) for p in work_pids]
    seeds: list[int] = []
    if raw.startswith("pid:"):
        try:
            pid = int(raw.split(":", 1)[1])
        except (IndexError, ValueError):
            return []
        if pid in work:
            seeds = [pid]
    else:
        for pid in work:
            argv = _read_proc_cmdline(pid)
            if _arg_value(argv, "--ticket") == raw or _arg_value(argv, "--goal") == raw:
                seeds.append(pid)
    out: list[int] = []
    for pid in seeds:
        for cand in [pid, *_descendant_pids(pid)]:
            if cand not in out:
                out.append(cand)
    return out


def kill_idle_ask_pids(
    pids: list[int],
    killfn=None,
    *,
    grace_sec: float = KILL_IDLE_ASK_GRACE_SEC,
) -> list[int]:
    """SIGTERM the pids, wait briefly, then SIGKILL; return the signaled pids.

    ``killfn(pid, sig)`` is injectable for tests; with an injected function the
    graceful wait is skipped (there is nothing real to wait on) and SIGKILL is
    attempted unconditionally. Already-dead pids and permission errors are
    swallowed, so the caller always gets the list of pids that got a SIGTERM.
    """
    kill = os.kill if killfn is None else killfn
    signaled: list[int] = []
    for pid in pids:
        try:
            ipid = int(pid)
        except (TypeError, ValueError):
            continue
        if ipid in signaled:
            continue
        try:
            kill(ipid, signal.SIGTERM)
        except (OSError, ValueError):
            continue
        signaled.append(ipid)
    if killfn is None and signaled and grace_sec > 0:
        deadline = time.monotonic() + float(grace_sec)
        while time.monotonic() < deadline:
            if not any(pid_alive(p) for p in signaled):
                break
            time.sleep(0.05)
    for pid in signaled:
        try:
            kill(pid, signal.SIGKILL)
        except (OSError, ValueError):
            pass
    return signaled


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


def session_search_roots(*, exists: bool = True) -> list[Path]:
    """Every candidate ACP role home, legacy first then slot-isolated.

    ``HOMES_ROOT/{impl,gate,supervisor}`` followed by
    ``SLOT_HOMES_ROOT/slot-*/{impl,gate,supervisor}`` ordered by slot number.
    With ``exists`` only directories that are really there are returned (the
    legacy behaviour these call sites relied on); ``exists=False`` returns the
    candidates regardless, which is what a log header without ``DSH_HOME=``
    needs. Best effort: unreadable roots are skipped.
    """
    roots = [HOMES_ROOT / role for role in _ROLE_HOMES]

    def _slot_num(path: Path) -> int:
        try:
            return int(path.name.split("-", 1)[1])
        except (IndexError, ValueError):
            return 1 << 30

    slots: list[Path] = []
    try:
        entries = list(SLOT_HOMES_ROOT.glob("slot-*"))
    except OSError:
        entries = []
    for slot_root in sorted(entries, key=_slot_num):
        slots.extend(slot_root / role for role in _ROLE_HOMES)
    roots.extend(slots)

    if not exists:
        return roots
    out: list[Path] = []
    for root in roots:
        try:
            if root.is_dir():
                out.append(root)
        except OSError:
            continue
    return out


def _newest_session_json() -> Path | None:
    """Newest (mtime) session json under the ACP homes, or None."""
    best: Path | None = None
    best_mtime: int | None = None
    for home in session_search_roots():
        root = home / SESSION_PROJCACHE_SESSIONS
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


def _session_json_from_log(log_path: Path) -> list[Path]:
    """ACP session json path(s) an open-slice log header points at.

    Reads at most the first ~64 KiB (any OS error → ``[]``). ``sessionId=``
    lines are collected distinct with the newest last (an ask may run several
    sessions); ``DSH_HOME=`` selects the role home, otherwise every candidate
    role home from :func:`session_search_roots` (legacy ``HOMES_ROOT`` roles and
    slot-isolated ones) that holds the session json is kept. A log
    without a ``sessionId=`` line (or without a readable header) yields ``[]``.
    """
    try:
        with open(log_path, "rb") as fh:
            raw = fh.read(_LOG_SNIFF_BYTES)
        text = raw.decode("utf-8", "replace")
    except OSError:
        return []

    ids: list[str] = []
    for match in _SESSION_ID_RE.finditer(text):
        sid = match.group(1)
        if sid in ids:
            ids.remove(sid)  # re-seen => newer occurrence, move to the end
        ids.append(sid)
    if not ids:
        return []

    home_match = _DSH_HOME_RE.search(text)
    if home_match:
        base = Path(home_match.group(1))
        candidates = [
            base / SESSION_PROJCACHE_SESSIONS / f"{sid}.json" for sid in ids
        ]
    else:
        candidates = []
        for sid in ids:
            for home in session_search_roots(exists=False):
                path = home / SESSION_PROJCACHE_SESSIONS / f"{sid}.json"
                if _path_exists(path):
                    candidates.append(path)

    out: list[Path] = []
    seen: set[str] = set()
    for cand in candidates:
        if str(cand) not in seen:
            seen.add(str(cand))
            out.append(cand)
    return out


def discover_session_outputs(work_pids: list[int]) -> dict[str, list[Path]]:
    """Best-effort map of live ticket -> candidate session/output files.

    Priority: cmdline ``--log`` / ``--ticket`` (open-slice default log), then
    the ACP session json each log header points at (a static log is normal for
    a long prompt, the session json is rewritten while the agent works), then
    open fds of the pid and its descendants (``*/sessions/*.json`` / ``*.log``),
    then the newest session json under the searched ACP homes (legacy
    ``HOMES_ROOT`` roles + slot homes) as a per-key fallback
    (only for a key that has a log but no resolvable session json) or as the
    global fallback when nothing exists yet.
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
        # Follow the session json the log header names (dedup, order kept).
        for log in [c for c in list(uniq) if str(c).endswith(".log")]:
            for sess in _session_json_from_log(log):
                if str(sess) not in seen:
                    seen.add(str(sess))
                    uniq.append(sess)
        # Per-key fallback: a blank/unreadable log header (no resolved session
        # json) still tracks the newest ACP session while it is written.
        has_json = any(str(c).endswith(".json") for c in uniq)
        log_exists = any(
            str(c).endswith(".log") and _path_exists(c) for c in uniq
        )
        if not has_json and log_exists:
            newest = _newest_session_json()
            if newest is not None and str(newest) not in seen:
                seen.add(str(newest))
                uniq.append(newest)
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


def _pending_goals_and_unattributed() -> tuple[set[str], bool]:
    """``(goal ids, had_unattributed_job)`` for inbox/processing job JSON.

    A non-goal file (broken JSON, non-dict, empty/``?`` goal) is not evidence
    *against* any single Goal — it is exactly the legacy "there is some pending
    work, no per-Goal claim" case, so it is reported separately and keeps the
    conservative old behaviour.
    """
    goals: set[str] = set()
    unattributed = False
    for d in (INBOX, PROCESSING):
        try:
            files = list(d.glob("*.json"))
        except OSError:
            continue
        for p in files:
            try:
                doc = json.loads(p.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                unattributed = True
                continue
            gid = str(doc.get("goal") or "").strip() if isinstance(doc, dict) else ""
            if gid and gid != "?":
                goals.add(gid)
            else:
                unattributed = True
    return goals, unattributed


def _pending_goal_ids() -> set[str]:
    """Goal ids carried by inbox/processing job JSON (non-goal files skipped)."""
    return _pending_goals_and_unattributed()[0]


def _running_goal_ids(now: float | None = None) -> list[str]:
    out: list[str] = []
    for g in _read_goals():
        status = str(g.get("status") or "").lower()
        if status not in RUNNING_GOAL_STATES:
            continue
        gid = str(g.get("goal") or "")
        if gid and gid not in out:
            out.append(gid)
    return out


def _heartbeat_goal_ids(heartbeat: dict | None) -> set[str]:
    out: set[str] = set()
    for row in (heartbeat or {}).get("slots") or []:
        if not isinstance(row, dict):
            continue
        gid = str(row.get("goal") or "").strip()
        if gid:
            out.add(gid)
    return out


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


def _iso_age_sec(raw: str, now: float) -> float | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        from datetime import datetime

        return max(0.0, now - datetime.fromisoformat(text).timestamp())
    except (TypeError, ValueError):
        return None


def _work_pid_has_token(pids: list[int], token: str) -> bool:
    """True when any work pid's argv contains ``token`` (e.g. a gate ticket)."""
    if not token:
        return False
    for pid in pids:
        if any(token in arg for arg in _read_proc_cmdline(pid)):
            return True
    return False


def _write_goal_doc(path: Path, goal: dict) -> None:
    goal["updated_at"] = T.now_iso()
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    tmp.write_text(json.dumps(goal, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _goal_slice_ids(goal: dict) -> list[str]:
    ids: list[str] = []
    for raw in goal.get("slices") or []:
        if isinstance(raw, str) and raw.strip():
            ids.append(raw.strip())
        elif isinstance(raw, dict):
            sid = str(raw.get("slice") or raw.get("id") or "").strip()
            if sid:
                ids.append(sid)
    cur = str(goal.get("current_slice") or "").strip()
    if cur and cur not in ids:
        ids.append(cur)
    return ids


def _chain_state(slice_id: str) -> str:
    path = CHAINS / f"{slice_id}.json"
    if not path.is_file():
        return ""
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, UnicodeDecodeError):
        return ""
    if not isinstance(body, dict):
        return ""
    return str(body.get("state") or "").strip()


def _goal_parked_slice(goal: dict) -> str:
    """Slice id when this Goal is waiting on a supervisor pause, else ''."""
    for sid in _goal_slice_ids(goal):
        if _chain_state(sid) in _PARKED_CHAIN_STATES:
            return sid
    cur = str(goal.get("current_slice") or "").strip()
    last = str(goal.get("last_chain_state") or "").strip()
    if cur and last in _PARKED_CHAIN_STATES:
        return cur
    return ""


def _goal_has_job(goal: dict, goal_id: str) -> bool:
    """True when the Goal still owns a processing/inbox job (or declared path)."""
    raw = str(goal.get("processing_job") or "").strip()
    if raw:
        cand = Path(raw)
        if not cand.is_absolute():
            cand = PROCESSING / cand
        if cand.is_file():
            return True
    for root in (PROCESSING, INBOX):
        if not root.is_dir():
            continue
        for cand in sorted(root.glob("*.json")):
            if cand.name.endswith(".error.json"):
                continue
            try:
                data = json.loads(cand.read_text(encoding="utf-8"))
            except (OSError, ValueError, UnicodeDecodeError):
                continue
            if isinstance(data, dict) and str(data.get("goal") or "") == goal_id:
                return True
    return False


def _clear_stale_gate_running(
    ask_path: Path, body: dict, threshold: float
) -> str:
    """Archive the interim ack and write a terminal ERROR in its place.

    Mirrors the broker's own ``clear_stale_gate_running_acks_at_start`` rename
    (``*.stale-gate-running.json``), then leaves a *final* answer so the client
    stops polling instead of waiting on a gate that no longer has a process.
    """
    ask_id = str(body.get("ask_id") or ask_path.stem)
    arch = MAILBOX / "archive"
    try:
        arch.mkdir(parents=True, exist_ok=True)
        ask_path.rename(arch / f"{ask_id}.stale-gate-running.json")
    except OSError:
        try:
            ask_path.unlink()
        except OSError:
            pass
    new = {
        "ask_id": ask_id,
        "status": "done",
        "ok": False,
        "verdict": "ERROR",
        "rework_mode": "",
        "degrade": "close_ticket",
        "error": f"stale gate_running: no process for >{threshold:.0f}s",
        "instruction": "gate 进程消失：保持摘要 status=done 并停止，broker 兜底 gate",
    }
    tmp = ask_path.with_name(f".{ask_path.name}.tmp-{os.getpid()}")
    try:
        tmp.write_text(
            json.dumps(new, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        os.replace(tmp, ask_path)
    except OSError:
        pass
    return ask_id


def _goal_already_handed(goal: dict) -> bool:
    """True once this Goal was handed to the supervisor session."""
    hand = goal.get("perceive_handoff")
    return isinstance(hand, dict) and bool(hand.get("handed"))


def _write_handoff_outbox(body: dict) -> None:
    """One transitional outbox JSON. Same directory ``--events-since`` reads."""
    try:
        OUTBOX.mkdir(parents=True, exist_ok=True)
        path = OUTBOX / f"event-{body.get('kind')}-{time.time_ns()}.json"
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(body, ensure_ascii=False) + "\n", encoding="utf-8")
        os.replace(tmp, path)
    except OSError:
        pass


def _hand_orphan_to_supervisor(
    path: Path,
    goal: dict,
    goal_id: str,
    reason: str,
    *,
    dry_run: bool,
) -> dict:
    """Record a supervisor handoff. Does not write ``status=escalated``.

    The broker loop opens ``supervisor-goal-<id>`` (keep_open, five-way) from
    ``perceive_handoff``. The watchdog only perceives and writes the outbox fact.
    """
    slice_id = str(goal.get("current_slice") or "").strip()
    if not slice_id:
        slices = _goal_slice_ids(goal)
        slice_id = slices[-1] if slices else ""
    ticket = PS.supervisor_ticket_name(goal_id)
    event = {
        "kind": ORPHAN_HANDOFF_KIND,
        "status": "handoff",
        "goal": goal_id,
        "slice": slice_id,
        "reason": reason,
        "suggested_action": (
            f"交给监理会话 {ticket}（keep_open 五选一）；看门狗不写 escalated"
        ),
        "reason_key": f"orphan_supervisor_handoff:{goal_id}",
        "supervisor_ticket": ticket,
    }
    if dry_run:
        return event
    _write_handoff_outbox(
        {
            "kind": ORPHAN_HANDOFF_KIND,
            "goal": goal_id,
            "slice": slice_id,
            "decision": "",
            "reason": reason,
            "status": "handoff",
            "supervisor_ticket": ticket,
            "at": T.now_iso(),
        }
    )
    goal["perceive_handoff"] = {
        "handed": True,
        "opened": False,
        "reason": ORPHAN_ESCALATE_REASON,
        "ticket": ticket,
        "slice": slice_id,
        "at": T.now_iso(),
    }
    try:
        _write_goal_doc(path, goal)
    except OSError:
        pass
    return event


def sweep_stale_gate_running_and_orphans(
    now: float | None = None,
    *,
    work_pids: list[int] | None = None,
    dry_run: bool = False,
) -> list[dict]:
    """Light consistency sweep: phantom gate acks + orphaned non-terminal Goals.

    A ``gate_running`` ack older than ``TRIAL_WATCHDOG_STALE_GATE_SEC`` whose
    gate ticket has no live process can never be finalized. A Goal in
    planning/running/closeout with no processing job, no live work pid, and no
    fresh heartbeat slot is handed to the existing Goal supervisor session.
    The watchdog does not write ``status=escalated``. Never kills a live
    process: a matching work pid means the gate/Goal is left alone.
    """
    now = time.time() if now is None else now
    work_pids = list_work_pids() if work_pids is None else list(work_pids)
    threshold = stale_gate_sec()
    events: list[dict] = []

    answers = MAILBOX / "answers"
    if answers.is_dir():
        for path in sorted(answers.glob("*.json")):
            try:
                body = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError, UnicodeDecodeError):
                continue
            if not isinstance(body, dict):
                continue
            if str(body.get("status") or "") != "gate_running":
                continue
            if body.get("verdict") is not None:
                continue
            age = _iso_age_sec(str(body.get("started_at") or ""), now)
            if age is None:
                try:
                    age = max(0.0, now - path.stat().st_mtime)
                except OSError:
                    age = None
            if age is None or age <= threshold:
                continue
            gate_ticket = str(body.get("gate_ticket") or "")
            if _work_pid_has_token(work_pids, gate_ticket):
                continue
            ask_id = str(body.get("ask_id") or path.stem)
            if not dry_run:
                ask_id = _clear_stale_gate_running(path, body, threshold)
            events.append(
                {
                    "kind": STALE_GATE_KIND,
                    "status": "stalled",
                    "goal": "",
                    "reason": (
                        f"stale gate_running: no process for >{threshold:.0f}s "
                        f"ask={ask_id} ticket={gate_ticket or '(none)'}"
                    ),
                    "suggested_action": (
                        "gate 进程已消失：清掉 interim ack，broker 兜底 gate"
                    ),
                    "reason_key": f"stale_gate_running:{ask_id}",
                }
            )

    hb_age = heartbeat_age(HEARTBEAT, now)
    hb_fresh = hb_age is not None and hb_age <= heartbeat_timeout()
    hb_goals = _heartbeat_goal_ids(read_heartbeat()) if hb_fresh else set()

    if GOALS.is_dir():
        for path in sorted(GOALS.glob("*.json")):
            try:
                goal = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError, UnicodeDecodeError):
                continue
            if not isinstance(goal, dict):
                continue
            status = str(goal.get("status") or "")
            if status not in RUNNING_GOAL_STATES:
                continue
            goal_id = str(goal.get("goal") or path.stem)
            if _goal_has_job(goal, goal_id):
                continue
            if _work_pid_has_token(work_pids, goal_id):
                continue
            age = _goal_age_sec(goal, now)
            if age is not None and age <= threshold:
                continue
            parked = _goal_parked_slice(goal)
            if parked:
                continue
            if _goal_already_handed(goal):
                continue
            # Empty ``ticket`` still means the Goal thread holds the slot
            # (between slices, or while a ticket name is being swapped).
            if goal_id in hb_goals:
                continue
            reason = (
                f"orphan running goal status={status}: no processing job / live "
                f"process / heartbeat slot for >{threshold:.0f}s"
            )
            events.append(
                _hand_orphan_to_supervisor(
                    path, goal, goal_id, reason, dry_run=dry_run
                )
            )
    return events


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
    heartbeat: dict | None = None,
    goal_pids: dict | None = None,
) -> list[dict]:
    """All currently-detected stall events (no notify, no dedup).

    ``heartbeat`` / ``goal_pids`` default to a live read; inject them in tests.
    """
    now = time.time() if now is None else now
    broker_pids = list_broker_pids() if broker_pids is None else broker_pids
    work_pids = list_work_pids() if work_pids is None else work_pids
    hb = read_heartbeat() if heartbeat is None else heartbeat
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
        # A slots-publishing broker owns per-Goal liveness in the heartbeat: a
        # live work process no longer excuses a stale beat (it may belong to a
        # different Goal). Legacy heartbeats keep the old work-pid exemption.
        if age is not None and "slots" in hb:
            stale = age > heartbeat_timeout()
        else:
            stale = age is not None and age > heartbeat_timeout() and not work_pids
        if age is None:
            if not work_pids:
                events.append(_stalled_event(
                    "no_heartbeat",
                    "*",
                    "broker heartbeat file missing",
                    "检查 trial broker 主循环是否启动 / 重启 broker",
                ))
        elif stale:
            events.append(_stalled_event(
                "heartbeat_stale",
                "*",
                f"broker heartbeat stale {age:.0f}s > {heartbeat_timeout():.0f}s "
                + (
                    "with live work process(es) not excusing it"
                    if work_pids
                    else "and no live open-slice/ask process"
                ),
                "重启 trial broker；检查 processing / 是否有僵死票",
            ))

    # Condition 3: a running Goal with no live ticket that belongs to it.
    # Evaluated per Goal: one Goal's worker must never excuse another's.
    gmap = work_pid_goals(work_pids, now=now) if goal_pids is None else goal_pids
    unowned = bool(gmap.get("*"))
    pending, pending_unattributed = _pending_goals_and_unattributed()
    has_slots = "slots" in hb
    hb_age = heartbeat_age(HEARTBEAT, now)
    hb_goals = _heartbeat_goal_ids(hb)
    hb_fresh = hb_age is not None and hb_age <= heartbeat_timeout()
    for g in _read_goals():
        status = str(g.get("status") or "").lower()
        if status not in RUNNING_GOAL_STATES:
            continue
        age = _goal_age_sec(g, now)
        if age is None or age <= goal_stall_timeout():
            continue
        gid = str(g.get("goal") or "?")
        if gmap.get(gid):
            continue  # a live work pid carries this Goal's env/cmdline
        if gid in hb_goals and hb_fresh:
            continue  # published in the broker's slots on a fresh beat
        if gid in pending:
            continue  # a job for this Goal is queued/processing
        if pending_unattributed:
            continue  # a job nobody could attribute to a Goal is pending
        if unowned and not has_slots:
            continue  # legacy heartbeat: an unattributable worker may be it
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


def write_watchdog_state(
    state: dict,
    path: Path | None = None,
    *,
    drop: tuple[str, ...] = (),
) -> None:
    """Read-modify-write ``state`` so freeze and session-idle keys coexist.

    ``drop`` removes keys that must not survive the merge (an unusable
    ``freeze_grace_until_mono``).
    """
    p = path or STATE_FILE
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        doc = read_watchdog_state(p)
        doc.update(state)
        for key in drop:
            doc.pop(key, None)
        p.write_text(json.dumps(doc, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except OSError:
        pass


def collect_freeze_events(
    now_wall: float | None = None,
    now_mono: float | None = None,
    *,
    state_path: Path | None = None,
    persist: bool = True,
    boottime: float | None | object = _AUTO,
    expected_interval: float | None = None,
    loop_pid: int | None = None,
    state: dict | None = None,
    grace_sec: float | None = None,
) -> list[dict]:
    """Emit at most one event when any freeze criterion fired this round.

    ``expected_interval`` (the ``--loop`` interval) enables the one-round-gap
    criterion; it is only trusted while ``prev.loop_pid`` equals the current pid,
    so a watchdog restart does not read its own downtime as a freeze.
    ``boottime`` defaults to auto-detection via :func:`boottime_now`.
    ``state`` lets ``run_once`` share its single read of the state file.

    ``persist`` writes the baseline (plus the grace deadline on a freeze round);
    ``run_once`` passes ``persist=False`` and owns that write.
    """
    now_wall = time.time() if now_wall is None else now_wall
    now_mono = time.monotonic() if now_mono is None else now_mono
    boot = boottime_now() if boottime is _AUTO else boottime
    pid = os.getpid() if loop_pid is None else int(loop_pid)
    prev = read_watchdog_state(state_path) if state is None else state
    prev = prev if isinstance(prev, dict) else {}
    if grace_sec is None:
        grace_sec = freeze_grace_sec(expected_interval)

    threshold = T.freeze_jump_threshold()
    prev_wall = prev.get("wall")
    prev_mono = prev.get("mono")
    hits: dict[str, float] = {}

    # (a) wall clock ran ahead of the monotonic clock.
    if isinstance(prev_wall, (int, float)) and isinstance(prev_mono, (int, float)):
        drift = T.wall_clock_drift_sec(prev_wall, prev_mono, now_wall, now_mono)
        if drift >= threshold:
            hits["wall-mono"] = drift

    # (b) boottime ran ahead of the monotonic clock (skipped without the clock).
    prev_boot = prev.get("boot")
    if (
        boot is not None
        and isinstance(prev_boot, (int, float))
        and isinstance(prev_mono, (int, float))
    ):
        drift_boot = T.wall_clock_drift_sec(prev_boot, prev_mono, boot, now_mono)
        if drift_boot >= threshold:
            hits["boottime-mono"] = drift_boot

    # (c) this round simply took far longer than the loop asked for, while the
    # previous round was ours (a restarted watchdog starts a fresh baseline).
    if (
        expected_interval is not None
        and prev.get("loop_pid") == pid
        and isinstance(prev_mono, (int, float))
    ):
        gap = (float(now_mono) - float(prev_mono)) - float(expected_interval)
        if gap >= threshold:
            hits["loop-interval"] = gap

    freeze = bool(hits)
    grace_until = float(now_mono) + float(grace_sec) if freeze else None
    if persist:
        write_watchdog_state(
            _freeze_state_fields(
                now_wall, now_mono, boot, pid, expected_interval, grace_until
            ),
            state_path,
            drop=() if grace_until is not None else ("freeze_grace_until_mono",),
        )
    if not freeze:
        return []

    jump = max(hits.values())
    primary = next(name for name, value in hits.items() if value == jump)
    criteria = ",".join(sorted(hits))
    return [{
        "kind": FREEZE_KIND,
        "status": "resumed-after-freeze",
        "goal": "*",
        "reason": (
            f"clock jump {jump / 60.0:.1f} min ({criteria}) vs monotonic clock; "
            f"clock-based stall checks granted {float(grace_sec):.0f}s grace"
        ),
        "suggested_action": "核对 running Goal / 是否需 salvage；检查系统是否刚从冻住恢复",
        "reason_key": "resumed-after-freeze",
        "criterion": primary,
        "criteria": sorted(hits),
        "jump_sec": jump,
        "grace_sec": float(grace_sec),
    }]


def collect_session_idle_events(
    work_pids: list[int],
    now_mono: float,
    state: dict | None = None,
    goal_pids: dict[str, list[int]] | None = None,
) -> tuple[list[dict], dict]:
    """Detect live tickets whose ACP session/output has gone idle.

    Idle is ``now_mono - since_mono`` for an unchanged output signature; a new
    key, a changed signature or a monotonic regression resets the baseline.
    Returns ``(events, new_session_idle_state)``; discovery is injectable via
    :func:`discover_session_outputs`. A ``pid:``/``fallback:`` key is owned by
    the Goal its work pid is attributed to (``goal_pids``, computed when not
    injected, mirroring :func:`collect_stall_events`); an unattributable or
    unknown pid keeps the legacy ``"*"``/key rule.
    """
    state = dict(state) if isinstance(state, dict) else {}
    if not work_pids:
        return [], {}
    outputs = discover_session_outputs(work_pids)
    pid_goals = work_pid_goals(work_pids) if goal_pids is None else goal_pids

    def owner_goal(key: str) -> str:
        """Goal a discovered key belongs to; legacy key rule when unknown."""
        if key.startswith("pid:"):
            try:
                pid = int(key.split(":", 1)[1])
            except (IndexError, ValueError):
                pid = -1
            for gid, pids in pid_goals.items():
                if pid != -1 and pid in pids and gid and gid != "*":
                    return gid
            return "*"
        if key.startswith("fallback:"):
            return "*"
        return key

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
            goal = owner_goal(key)
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
    expected_interval: float | None = None,
) -> dict:
    """Evaluate all checks, dedup, notify. Returns a structured result.

    ``expected_interval`` is the loop interval (``--loop`` only); it arms the
    one-round-gap freeze criterion and the grace floor. ``persist`` overrides the
    legacy ``persist_freeze`` switch (which is kept for back-compat); either way
    ``dry_run=True`` forces a read-only pass so no state is ever written.
    """
    now = time.time() if now is None else now
    now_mono = time.monotonic() if now_mono is None else now_mono
    dedupe = dedupe if dedupe is not None else Deduper()
    persist = (persist_freeze if persist is None else persist) and not dry_run

    broker_pids = list_broker_pids()
    work_pids = list_work_pids()
    prior = read_watchdog_state()
    boot = boottime_now()

    # Freeze first: its verdict decides whether the clock-based stalls below are
    # trustworthy this round (and for the following grace window).
    freeze_events = collect_freeze_events(
        now,
        now_mono,
        persist=False,
        boottime=boot,
        expected_interval=expected_interval,
        state=prior,
    )
    freeze = bool(freeze_events)
    grace_sec = freeze_grace_sec(expected_interval)
    if freeze:
        grace_until = float(now_mono) + float(
            freeze_events[0].get("grace_sec") or grace_sec
        )
    else:
        # A deadline from a previous boot (monotonic clock reset) is unusable
        # and must neither arm the grace window nor be persisted again.
        grace_until = persisted_grace_deadline(now_mono, prior, grace_sec)
    grace_active = grace_is_active(now_mono, freeze, prior, grace_sec)

    events = collect_stall_events(now, broker_pids=broker_pids, work_pids=work_pids)
    heartbeat = read_heartbeat()
    goal_pids = work_pid_goals(work_pids, now=now)
    if grace_active:
        events = [e for e in events if not is_clock_related_stall(e)]

    # Freeze events are never sent in the round that detects them: the wall
    # clock just lied, so park them in ``pending_freeze`` and decide next
    # round, once the grace window has shown whether the broker recovered.
    prior_pending = prior.get("pending_freeze")
    prior_pending = prior_pending if isinstance(prior_pending, dict) else None
    pending_freeze: dict | None = None
    pending_drop: tuple[str, ...] = ("pending_freeze",)
    freeze_stalled: dict | None = None
    if freeze:
        pending_freeze = {
            "detected_wall": float(now),
            "detected_mono": float(now_mono),
            "grace_until": float(grace_until) if grace_until is not None else None,
            "jump_sec": float(freeze_events[0].get("jump_sec") or 0.0),
            "criteria": list(freeze_events[0].get("criteria") or []),
        }
        pending_drop = ()
    elif prior_pending is not None:
        try:
            until = float(prior_pending.get("grace_until"))
        except (TypeError, ValueError):
            until = None
        age = heartbeat_age(now=now)
        if age is not None and age <= heartbeat_timeout():
            # The heartbeat came back: the broker survived the freeze. Drop the
            # pending verdict with a log line only — nothing is ever sent.
            print(
                "[trial-watchdog] freeze pending 清除：心跳已恢复，仅记录日志",
                file=sys.stderr, flush=True,
            )
        elif until is not None and now_mono < until:
            # Grace still running and the heartbeat has not recovered: keep
            # the pending verdict for a later round.
            pending_freeze = prior_pending
            pending_drop = ()
        else:
            # Grace over and the heartbeat never came back: merge every
            # freeze-suppressed clock stall into exactly one stalled alert.
            detected_mono = float(prior_pending.get("detected_mono") or now_mono)
            freeze_stalled = {
                "kind": STALLED_KIND,
                "status": "stalled",
                "goal": "*",
                "reason": (
                    f"freeze 宽限已过（{(until or 0.0) - detected_mono:.0f}s）心跳仍未恢复；"
                    f"冻结检测于 {float(prior_pending.get('detected_wall') or now):.0f} "
                    f"(wall)，合并冻结期间抑制的心跳停滞为一条 stalled"
                ),
                "suggested_action": "检查 broker 进程是否存活，必要时 salvage",
                "reason_key": "freeze_stalled",
                "criteria": list(prior_pending.get("criteria") or []),
                "jump_sec": float(prior_pending.get("jump_sec") or 0.0),
            }
            events = [e for e in events if not is_clock_related_stall(e)]

    events += freeze_events
    if freeze_stalled is not None:
        events.append(freeze_stalled)
    idle_events, idle_state = collect_session_idle_events(
        work_pids, now_mono, prior.get("session_idle") or {}, goal_pids
    )
    kill_enabled = kill_idle_ask_enabled() and not dry_run
    for ev in idle_events:
        # A live-but-hung ticket never recovers on its own; reap its process
        # tree so the broker can close the slice (never on a dry-run pass).
        ev_pids = (
            pids_for_session_idle_key(str(ev.get("reason_key") or ""), work_pids)
            if kill_enabled
            else []
        )
        killed = kill_idle_ask_pids(ev_pids) if (kill_enabled and ev_pids) else []
        ev["killed_pids"] = killed
        if killed:
            ev["suggested_action"] = (
                f"{ev.get('suggested_action') or ''}"
                f"；watchdog 已杀僵住进程 pids={','.join(str(p) for p in killed)}"
            )
    events += idle_events

    # Light consistency sweep: a phantom gate_running ack or an orphaned
    # non-terminal Goal would otherwise stall forever. Dry runs only warn.
    events += sweep_stale_gate_running_and_orphans(
        now, work_pids=work_pids, dry_run=dry_run
    )

    if persist:
        doc = dict(prior)
        doc.update(
            _freeze_state_fields(
                now, now_mono, boot, os.getpid(), expected_interval, grace_until
            )
        )
        doc["session_idle"] = idle_state
        if pending_freeze is not None:
            doc["pending_freeze"] = pending_freeze
        drop = list(pending_drop)
        if grace_until is None:
            drop.append("freeze_grace_until_mono")
        write_watchdog_state(doc, drop=tuple(drop))

    sent: list[dict] = []
    suppressed: list[str] = []
    for ev in events:
        key = str(ev.get("reason_key") or ev.get("reason") or "")
        if freeze and any(ev is fe for fe in freeze_events):
            # Parked in pending_freeze this round; the verdict (or silence)
            # arrives in a later round once the grace window has elapsed.
            suppressed.append(key)
            continue
        if str(ev.get("kind") or "") == ORPHAN_HANDOFF_KIND:
            # Outbox already has the transitional fact. Do not webhook it
            # as a terminal notify; only the supervisor may escalate.
            continue
        if not dedupe.should_send(key, now):
            suppressed.append(key)
            continue
        info = notify_event(ev, dry_run=dry_run)
        ev = dict(ev)
        ev["notify"] = info
        sent.append(ev)
    return {
        "at": T.now_iso(),
        "events": events,
        "sent": sent,
        "suppressed": suppressed,
        "freeze": freeze,
        "grace_active": grace_active,
        "slots": list(heartbeat.get("slots") or []),
        "goal_pids": goal_pids,
    }


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
                expected_interval=interval,
            )
        except Exception as e:  # noqa: BLE001 — a watchdog must not die
            print(f"[trial-watchdog] check error: {e}", file=sys.stderr, flush=True)
        time.sleep(interval)


if __name__ == "__main__":
    raise SystemExit(main())
