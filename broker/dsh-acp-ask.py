#!/usr/bin/env python3
"""ACP client for local `dsh --profile <name>` (default ``acp``).

Profile selection
-----------------
``--profile acp`` (default): Hub / engine / intel work — includes MCP
(codegraph, serena, serena-hub).
``--profile acp-lite``: plugin / scaffold / context trials — lean; no MCP
insert blocks. Spill-policy, tool-fs, deepseek-flash, team-rooms kept.

Modes
-----
One-shot (compat)::
  dsh-acp-ask.py [cwd] <prompt...>

Multi-round on one connection (ticket-in-process)::
  dsh-acp-ask.py --multi --cwd <abs> --prompt 'r1' --prompt 'r2'
  dsh-acp-ask.py --multi --cwd <abs> --prompts-file rounds.txt
  # rounds.txt / stdin: prompts separated by a line that is exactly ===

Ticket across process restarts (session/list + session/resume)::
  dsh-acp-ask.py --ticket NAME --cwd <abs> --prompt 'r1' --keep-open
  dsh-acp-ask.py --ticket NAME --resume --prompt 'r2 delta'
  dsh-acp-ask.py --ticket NAME --close
  dsh-acp-ask.py --ticket NAME --list   # ACP session/list for ticket cwd

Role / DSH_HOME (parallel bots)
-------------------------------
Explicit ``DSH_HOME`` always wins.
Else ``--role gate|impl|supervisor`` or ``DSH_ROLE``.
Else infer from ``--ticket`` prefix: ``gate-*`` / ``review-*`` → gate;
``impl-*`` / ``fix-*`` / ``dev-*`` → impl; ``supervisor-*`` / ``sup-*`` → supervisor.
Maps to ``$DSH_HOMES_ROOT/{role}`` (default ``/home/box/.dsh-homes`` on box,
``~/.dsh-homes`` elsewhere). Ensures sessions/storages dirs exist.
One-shot without role/ticket stays on ``~/.dsh`` (or existing DSH_HOME).

Env
---
DSH_BIN, DSH_HOME, DSH_ROLE, DSH_HOMES_ROOT, DSH_PERMISSION_MODE,
DSH_ACP_PROMPT_IDLE_TIMEOUT (default 900): reset while ACP stdout shows
progress. DSH_ACP_PROMPT_TIMEOUT (default 3600): hard cap on process
uptime (time.monotonic) for one session/prompt so a true hang cannot hold
the broker forever and a host suspend / wall-clock jump cannot trip it.
Loads $DSH_HOME/.env into the child without overwriting existing env.

Vendored into dsh-trial so the trial broker can set DSH_ACP_ASK here.
Do not overwrite ~/.dsh/bin/dsh-acp-ask.py while a ticket is running it.
"""
from __future__ import annotations

import argparse
import json
import os
import queue
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

DEFAULT_DSH = os.environ.get("DSH_BIN") or (
    "/home/box/.local/bin/dsh"
    if Path("/home/box/.local/bin/dsh").exists()
    else "/Users/fengrunda/.local/bin/dsh"
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _dsh_home() -> Path:
    return Path(os.environ.get("DSH_HOME") or (Path.home() / ".dsh"))


def _homes_root() -> Path:
    if os.environ.get("DSH_HOMES_ROOT"):
        return Path(os.environ["DSH_HOMES_ROOT"])
    if Path("/home/box/.dsh-homes").is_dir():
        return Path("/home/box/.dsh-homes")
    return Path.home() / ".dsh-homes"


def _infer_role(role: str | None, ticket: str | None) -> str | None:
    allowed = ("gate", "impl", "supervisor")
    if role:
        r = role.strip().lower()
        if r not in allowed:
            raise ValueError(f"role must be gate|impl|supervisor, got {role!r}")
        return r
    env_role = (os.environ.get("DSH_ROLE") or "").strip().lower()
    if env_role in allowed:
        return env_role
    if not ticket:
        return None
    name = ticket.strip().lower()
    for pref, r in (
        ("gate-", "gate"),
        ("gate_", "gate"),
        ("review-", "gate"),
        ("review_", "gate"),
        ("pr-gate-", "gate"),
        ("impl-", "impl"),
        ("impl_", "impl"),
        ("fix-", "impl"),
        ("fix_", "impl"),
        ("dev-", "impl"),
        ("dev_", "impl"),
        ("supervisor-", "supervisor"),
        ("supervisor_", "supervisor"),
        ("sup-", "supervisor"),
        ("sup_", "supervisor"),
    ):
        if name.startswith(pref):
            return r
    return None


def _ensure_role_home(role: str) -> Path:
    """Create/repair a per-role DSH_HOME (isolated sessions/storages)."""
    home = _homes_root() / role
    home.mkdir(parents=True, exist_ok=True)
    (home / "profiles").mkdir(exist_ok=True)
    (home / "sessions").mkdir(exist_ok=True)
    (home / "storages").mkdir(exist_ok=True)
    (home / "acp-tickets").mkdir(exist_ok=True)
    base = Path.home() / ".dsh"
    if Path("/home/box/.dsh").is_dir():
        base = Path("/home/box/.dsh")
    for name in ("settings.yaml", ".env"):
        src = base / name
        dst = home / name
        if src.exists() and not dst.exists():
            try:
                dst.symlink_to(src)
            except OSError:
                pass
    for pname in ("acp", "acp-lite", "acp-lite-offload"):
        dst = home / "profiles" / pname
        src = base / "profiles" / pname
        if src.exists() and not dst.exists():
            try:
                dst.symlink_to(src)
            except OSError:
                pass
    # thin-state pack bridge (shared packs/summaries)
    ts = home / "supervisor" / "thin-state"
    ts.mkdir(parents=True, exist_ok=True)
    shared_ts = base / "supervisor" / "thin-state"
    for name in ("packs", "summaries", "chains", "goals", "index.json"):
        src = shared_ts / name
        dst = ts / name
        if src.exists() and not dst.exists() and not dst.is_symlink():
            try:
                dst.symlink_to(src)
            except OSError:
                pass
    return home



def _ticket_path(name: str) -> Path:
    safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in name)
    return _dsh_home() / "acp-tickets" / f"{safe}.json"


def _load_ticket(name: str) -> dict[str, Any] | None:
    p = _ticket_path(name)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except json.JSONDecodeError:
        return None


def _save_ticket(name: str, data: dict[str, Any]) -> None:
    p = _ticket_path(name)
    p.parent.mkdir(parents=True, exist_ok=True)
    data = dict(data)
    data["updatedAt"] = _now()
    p.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
    p.chmod(0o600)


def _clear_ticket(name: str) -> None:
    p = _ticket_path(name)
    if p.exists():
        p.unlink()


def _build_env() -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("DSH_PERMISSION_MODE", "danger-full-access")
    env.setdefault("DSH_HOME", str(_dsh_home()))
    extras = []
    if Path("/home/box/.local/node/current/bin").exists():
        extras.append("/home/box/.local/node/current/bin")
    if Path("/home/box/.local/bin").exists():
        extras.append("/home/box/.local/bin")
    if extras:
        env["PATH"] = ":".join(extras) + ":" + env.get("PATH", "")
    envf = Path(env["DSH_HOME"]) / ".env"
    if envf.exists():
        for line in envf.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            v = v.strip().strip('"').strip("'")
            if k and v and k not in env:
                env[k] = v
    return env


def _split_prompts_blob(blob: str) -> list[str]:
    parts: list[str] = []
    cur: list[str] = []
    for line in blob.splitlines():
        if line.strip() == "===":
            text = "\n".join(cur).strip()
            if text:
                parts.append(text)
            cur = []
        else:
            cur.append(line)
    text = "\n".join(cur).strip()
    if text:
        parts.append(text)
    return parts



def prompt_timeouts_from_env(env: dict[str, str] | None = None) -> tuple[int, int]:
    """Return (idle_seconds, hard_cap_seconds) for one session/prompt.

    Idle resets when ACP stdout shows progress. The hard cap is absolute
    from prompt start on the monotonic clock (process uptime). idle <= 0
    disables the idle timer (hard cap only).
    The hard cap is never shorter than the idle window.
    """
    src = env if env is not None else os.environ
    idle = int(src.get("DSH_ACP_PROMPT_IDLE_TIMEOUT", "900"))
    hard = int(src.get("DSH_ACP_PROMPT_TIMEOUT", "3600"))
    if idle <= 0:
        idle = hard
    if hard < idle:
        hard = idle
    return idle, hard


def bump_idle_deadline(now: float, idle_timeout: float, hard_deadline: float) -> float:
    """Next idle deadline after progress. Never past the hard cap."""
    return min(now + idle_timeout, hard_deadline)


class AcpClient:
    def __init__(self, dsh_bin: str, env: dict[str, str], profile: str = "acp"):
        self.env = env
        self.profile = profile or "acp"
        self.p = subprocess.Popen(
            [dsh_bin, "--profile", self.profile],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        )
        self.q: queue.Queue = queue.Queue()
        self._rid = 0
        threading.Thread(target=self._reader, args=(self.p.stdout, "out"), daemon=True).start()
        threading.Thread(target=self._reader, args=(self.p.stderr, "err"), daemon=True).start()

    def _reader(self, stream, tag: str) -> None:
        for line in stream:
            self.q.put((tag, line.rstrip("\n")))
        self.q.put((tag, None))

    def _next_id(self) -> int:
        self._rid += 1
        return self._rid

    def send(self, obj: dict[str, Any]) -> None:
        assert self.p.stdin is not None
        self.p.stdin.write(json.dumps(obj, ensure_ascii=False) + "\n")
        self.p.stdin.flush()

    def wait_for(
        self,
        pred: Callable[[dict[str, Any]], bool],
        timeout: float,
        collect_chunks: list[str] | None = None,
        idle_timeout: float | None = None,
    ) -> tuple[dict[str, Any] | None, list[str]]:
        """Wait until pred(msg) or a deadline.

        timeout is a hard cap measured on time.monotonic() (process uptime)
        from entry, so a host suspend or wall-clock jump cannot trip it.
        idle_timeout, when set, resets on each ACP stdout line (progress)
        but never past the hard deadline. stderr does not count as progress.
        On expiry, self._timeout_kind is "idle" or "hard".
        """
        errs: list[str] = []
        self._timeout_kind = None
        hard_deadline = time.monotonic() + timeout
        idle_deadline = (
            time.monotonic() + idle_timeout if idle_timeout else hard_deadline
        )
        while True:
            now = time.monotonic()
            if now >= hard_deadline:
                self._timeout_kind = "hard"
                return None, errs
            if idle_timeout and now >= idle_deadline:
                self._timeout_kind = "idle"
                return None, errs
            slice_wait = hard_deadline - now
            if idle_timeout:
                slice_wait = min(slice_wait, idle_deadline - now)
            slice_wait = min(0.5, max(0.0, slice_wait))
            try:
                tag, line = self.q.get(timeout=slice_wait)
            except queue.Empty:
                code = self.p.poll()
                if code is not None:
                    errs.append(f"dsh-acp process exited early code={code}")
                    break
                continue
            if line is None:
                continue
            if tag == "out" and idle_timeout:
                idle_deadline = bump_idle_deadline(time.monotonic(), idle_timeout, hard_deadline)
            if tag == "err":
                errs.append(line)
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if collect_chunks is not None and msg.get("method") == "session/update":
                upd = (msg.get("params") or {}).get("update") or {}
                if upd.get("sessionUpdate") == "agent_message_chunk":
                    c = upd.get("content") or {}
                    if isinstance(c, dict):
                        collect_chunks.append(c.get("text") or "")
            if pred(msg):
                return msg, errs
        return None, errs

    def initialize(self) -> None:
        rid = self._next_id()
        self.send(
            {
                "jsonrpc": "2.0",
                "id": rid,
                "method": "initialize",
                "params": {
                    "protocolVersion": 1,
                    "clientCapabilities": {},
                    "clientInfo": {"name": "dsh-acp-ask", "version": "0.2"},
                },
            }
        )
        msg, errs = self.wait_for(lambda m: m.get("id") == rid, timeout=60)
        if not msg or "error" in msg:
            raise RuntimeError(f"ACP initialize failed: {msg or errs[:5]}")
        self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def session_new(self, cwd: str, mcp_servers: list | None = None) -> str:
        Path(cwd).mkdir(parents=True, exist_ok=True)
        rid = self._next_id()
        self.send(
            {
                "jsonrpc": "2.0",
                "id": rid,
                "method": "session/new",
                "params": {"cwd": cwd, "mcpServers": mcp_servers or []},
            }
        )
        msg, errs = self.wait_for(lambda m: m.get("id") == rid, timeout=90)
        if not msg or "error" in msg:
            raise RuntimeError(f"ACP session/new failed: {msg or errs[:8]}")
        return msg["result"]["sessionId"]

    def session_list(self, cwd: str | None = None) -> dict[str, Any]:
        rid = self._next_id()
        params: dict[str, Any] = {}
        if cwd:
            params["cwd"] = cwd
        self.send({"jsonrpc": "2.0", "id": rid, "method": "session/list", "params": params})
        msg, errs = self.wait_for(lambda m: m.get("id") == rid, timeout=60)
        if not msg or "error" in msg:
            raise RuntimeError(f"ACP session/list failed: {msg or errs[:8]}")
        return msg.get("result") or {}

    def session_resume(
        self, session_id: str, cwd: str, mcp_servers: list | None = None
    ) -> None:
        Path(cwd).mkdir(parents=True, exist_ok=True)
        rid = self._next_id()
        self.send(
            {
                "jsonrpc": "2.0",
                "id": rid,
                "method": "session/resume",
                "params": {
                    "sessionId": session_id,
                    "cwd": cwd,
                    "mcpServers": mcp_servers or [],
                },
            }
        )
        msg, errs = self.wait_for(lambda m: m.get("id") == rid, timeout=90)
        if not msg or "error" in msg:
            raise RuntimeError(f"ACP session/resume failed: {msg or errs[:8]}")

    def session_prompt(self, session_id: str, prompt: str) -> str:
        chunks: list[str] = []
        rid = self._next_id()
        idle, hard = prompt_timeouts_from_env()

        def done(m: dict[str, Any]) -> bool:
            return m.get("id") == rid

        self.send(
            {
                "jsonrpc": "2.0",
                "id": rid,
                "method": "session/prompt",
                "params": {
                    "sessionId": session_id,
                    "prompt": [{"type": "text", "text": prompt}],
                },
            }
        )
        msg, errs = self.wait_for(
            done, timeout=hard, collect_chunks=chunks, idle_timeout=idle
        )
        if msg is None:
            # No JSON-RPC reply within the deadline. The collected stderr is usually
            # unrelated agent/harness noise (serena/codegraph banners), so never
            # present it as a root cause here. Surface either a real early process
            # exit or a plain timeout instead.
            early = [e for e in errs if "exited early" in e]
            if early:
                raise RuntimeError(
                    f"ACP session/prompt failed: dsh-acp process exited early "
                    f"before replying (timeout after {hard}s): {early[0]}"
                )
            kind = getattr(self, "_timeout_kind", None) or "hard"
            if kind == "idle":
                raise RuntimeError(
                    f"ACP session/prompt failed: timeout after {idle}s idle "
                    f"(no ACP stdout from agent for session {session_id})"
                )
            raise RuntimeError(
                f"ACP session/prompt failed: timeout after {hard}s hard cap "
                f"(no session/prompt result from agent for session {session_id})"
            )
        if "error" in msg:
            raise RuntimeError(f"ACP session/prompt failed: {msg}")
        return "".join(chunks).strip()

    def session_close(self, session_id: str) -> None:
        rid = self._next_id()
        self.send(
            {
                "jsonrpc": "2.0",
                "id": rid,
                "method": "session/close",
                "params": {"sessionId": session_id},
            }
        )
        self.wait_for(lambda m: m.get("id") == rid, timeout=30)

    def close_proc(self) -> None:
        try:
            if self.p.stdin:
                self.p.stdin.close()
        except Exception:
            pass
        self.p.terminate()
        try:
            self.p.wait(timeout=5)
        except Exception:
            self.p.kill()


def _parse_legacy_oneshot(argv: list[str]) -> tuple[str, str] | None:
    """Return (cwd, prompt) if argv looks like legacy [cwd] prompt... without flags."""
    if not argv or any(a.startswith("-") for a in argv):
        return None
    if len(argv) >= 2 and (
        argv[0].startswith("/") or argv[0] in (".", "..") or Path(argv[0]).exists()
    ):
        return str(Path(argv[0]).resolve()), " ".join(argv[1:])
    return str(Path.cwd().resolve()), " ".join(argv)


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)

    legacy = _parse_legacy_oneshot(argv)
    if legacy is not None:
        cwd, prompt = legacy
        if "DSH_HOME" not in os.environ:
            role = _infer_role(None, None)
            if role:
                os.environ["DSH_HOME"] = str(_ensure_role_home(role))
                os.environ["DSH_ROLE"] = role
                print(f"DSH_HOME={os.environ['DSH_HOME']} role={role}", file=sys.stderr)
        return _run_prompts(cwd=cwd, prompts=[prompt], ticket=None, resume=False, keep_open=False, force_new=False)

    ap = argparse.ArgumentParser(
        prog="dsh-acp-ask.py",
        description="ACP ask helper: one-shot, multi-round, or resumable ticket sessions",
    )
    ap.add_argument("--cwd", help="absolute workspace cwd")
    ap.add_argument("--ticket", help="named ticket; meta under $DSH_HOME/acp-tickets/")
    ap.add_argument("--resume", action="store_true", help="force session/resume from ticket meta")
    ap.add_argument("--new", action="store_true", help="force session/new (ignore existing ticket)")
    ap.add_argument("--keep-open", action="store_true", help="do not session/close; save ticket for later resume")
    ap.add_argument("--close", action="store_true", help="resume ticket session only to close it")
    ap.add_argument("--list", action="store_true", help="session/list (optionally filtered by --cwd / ticket cwd)")
    ap.add_argument("--multi", action="store_true", help="run multiple prompts on one connection")
    ap.add_argument("--prompt", action="append", default=[], help="prompt text (repeatable)")
    ap.add_argument("--prompt-file", dest="prompt_files", action="append", default=[], help="read one prompt from file")
    ap.add_argument("--prompts-file", help="multi prompts separated by === lines")
    ap.add_argument("--print-session-id", action="store_true", help="print sessionId to stderr")
    ap.add_argument("--final", action="store_true", help="after prompts, session/close and clear ticket")
    ap.add_argument("--role", choices=["gate", "impl", "supervisor"], help="parallel isolation role → DSH_HOME")
    ap.add_argument(
        "--profile",
        default="acp",
        help="dsh profile name (default: acp; use acp-lite for plugin/scaffold/context trials)",
    )
    args = ap.parse_args(argv)

    # Role → DSH_HOME before building child env / reading tickets
    dsh_home_was_set = "DSH_HOME" in os.environ
    try:
        role = _infer_role(args.role, args.ticket)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 2
    if role and not dsh_home_was_set:
        home = _ensure_role_home(role)
        os.environ["DSH_HOME"] = str(home)
        os.environ["DSH_ROLE"] = role
        print(f"DSH_HOME={home} role={role}", file=sys.stderr)
    elif role and dsh_home_was_set:
        print(f"DSH_HOME={os.environ['DSH_HOME']} role={role} (explicit home kept)", file=sys.stderr)
    elif args.role is None and args.ticket and role is None:
        print("note: ticket has no gate-/impl- prefix; using default DSH_HOME (set --role for parallel)", file=sys.stderr)

    env = _build_env()
    dsh_bin = env.get("DSH_BIN") or DEFAULT_DSH
    profile = (args.profile or "acp").strip() or "acp"

    # Resolve prompts
    prompts: list[str] = []
    for p in args.prompt:
        if p.strip():
            prompts.append(p)
    for f in args.prompt_files:
        prompts.append(Path(f).read_text())
    if args.prompts_file:
        prompts.extend(_split_prompts_blob(Path(args.prompts_file).read_text()))
    if args.multi and not prompts and not args.close and not args.list:
        blob = sys.stdin.read()
        prompts.extend(_split_prompts_blob(blob))

    ticket_meta = _load_ticket(args.ticket) if args.ticket else None
    cwd = args.cwd
    if not cwd and ticket_meta and ticket_meta.get("cwd"):
        cwd = ticket_meta["cwd"]
    if cwd:
        cwd = str(Path(cwd).resolve())

    if args.list:
        if not cwd and not args.ticket:
            # list all under default persistence — still need a client
            cwd = None
        client = AcpClient(dsh_bin, env, profile=profile)
        try:
            client.initialize()
            result = client.session_list(cwd)
            print(json.dumps(result, indent=2, ensure_ascii=False))
            return 0
        except Exception as e:
            print(str(e), file=sys.stderr)
            return 1
        finally:
            client.close_proc()

    if args.close:
        if not args.ticket:
            print("--close requires --ticket", file=sys.stderr)
            return 2
        if not ticket_meta or not ticket_meta.get("sessionId"):
            print(f"no ticket meta for {args.ticket}", file=sys.stderr)
            return 1
        if not cwd:
            print("--close needs ticket cwd or --cwd", file=sys.stderr)
            return 2
        client = AcpClient(dsh_bin, env, profile=profile)
        try:
            client.initialize()
            client.session_resume(
                ticket_meta["sessionId"],
                cwd,
                ticket_meta.get("mcpServers") or [],
            )
            client.session_close(ticket_meta["sessionId"])
            _clear_ticket(args.ticket)
            print(f"closed {args.ticket}", file=sys.stderr)
            return 0
        except Exception as e:
            print(str(e), file=sys.stderr)
            return 1
        finally:
            client.close_proc()

    if not prompts:
        print("need a prompt (positional, --prompt, --prompt-file, or --prompts-file)", file=sys.stderr)
        return 2
    if not cwd:
        print("need --cwd (or ticket with saved cwd)", file=sys.stderr)
        return 2

    want_resume = bool(args.resume)
    force_new = bool(args.new)
    if args.ticket and ticket_meta and ticket_meta.get("sessionId") and not force_new:
        # auto-resume for follow-up rounds unless --new
        want_resume = True
    if args.ticket and force_new:
        want_resume = False

    # Policy: no ticket → close after prompts; ticket → keep open until --close / --final
    keep_open = bool(args.ticket) and not bool(getattr(args, "final", False)) and os.environ.get("DSH_ACP_FINAL") != "1"
    if args.keep_open:
        keep_open = True

    return _run_prompts(
        cwd=cwd,
        prompts=prompts,
        ticket=args.ticket,
        resume=want_resume,
        keep_open=keep_open,
        force_new=force_new,
        print_session_id=args.print_session_id,
        mcp_servers=(ticket_meta or {}).get("mcpServers") or [],
        session_id=(ticket_meta or {}).get("sessionId") if want_resume and not force_new else None,
        dsh_bin=dsh_bin,
        env=env,
        profile=profile,
    )


def _run_prompts(
    *,
    cwd: str,
    prompts: list[str],
    ticket: str | None,
    resume: bool,
    keep_open: bool,
    force_new: bool,
    print_session_id: bool = False,
    mcp_servers: list | None = None,
    session_id: str | None = None,
    dsh_bin: str | None = None,
    env: dict[str, str] | None = None,
    profile: str = "acp",
) -> int:
    env = env or _build_env()
    dsh_bin = dsh_bin or env.get("DSH_BIN") or DEFAULT_DSH
    mcp_servers = mcp_servers or []
    profile = profile or "acp"
    client = AcpClient(dsh_bin, env, profile=profile)
    try:
        client.initialize()
        sid = session_id
        if resume and sid and not force_new:
            # Prefer resume; if resume fails, fall back to list+match then new
            try:
                client.session_resume(sid, cwd, mcp_servers)
            except RuntimeError as e:
                print(f"resume failed ({e}); trying session/list", file=sys.stderr)
                listed = client.session_list(cwd)
                sessions = listed.get("sessions") or []
                match = next((s for s in sessions if s.get("sessionId") == sid), None)
                if not match and sessions:
                    # take most recently updated for this cwd
                    match = sessions[0]
                    sid = match.get("sessionId")
                if match and sid:
                    client.session_resume(sid, cwd, mcp_servers)
                else:
                    sid = client.session_new(cwd, mcp_servers)
        else:
            sid = client.session_new(cwd, mcp_servers)

        if print_session_id or ticket:
            print(f"sessionId={sid}", file=sys.stderr)

        if ticket:
            _save_ticket(
                ticket,
                {
                    "sessionId": sid,
                    "cwd": cwd,
                    "mcpServers": mcp_servers,
                    "ticket": ticket,
                    "role": os.environ.get("DSH_ROLE"),
                    "dshHome": os.environ.get("DSH_HOME"),
                    "profile": profile,
                },
            )

        outputs: list[str] = []
        for i, prompt in enumerate(prompts, 1):
            if len(prompts) > 1:
                print(f"--- round {i}/{len(prompts)} ---", file=sys.stderr)
            text = client.session_prompt(sid, prompt)
            outputs.append(text)
            if len(prompts) > 1:
                print(f"===== ROUND {i} =====")
            print(text)

        if not keep_open:
            client.session_close(sid)
            if ticket:
                _clear_ticket(ticket)
        return 0
    except Exception as e:
        print(str(e), file=sys.stderr)
        return 1
    finally:
        client.close_proc()


if __name__ == "__main__":
    raise SystemExit(main())
