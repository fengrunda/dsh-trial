#!/usr/bin/env python3
"""Thin supervisor for broker-dsh-trial (Route C · option C).

Polls ~/.dsh/supervisor/trial/inbox for slice jobs, spawns open-slice.sh
--final (medium ACP ticket via dsh-acp-ask). Executors NEVER join rooms.
Does NOT talk to broker-khub-prod / dsh-acp-broker / team-rooms dispatch.

Job formats
-----------
Single ticket JSON (*.json)::
  {
    "id": "optional-slug",
    "ticket": "impl-…",
    "pack": "name.pack.md",
    "profile": "acp" | "acp-lite",
    "cwd": "/tmp/dsh-trial-example-workdir",
    "role": "impl" | "gate",
    "summary_name": "optional",
    "notify": "hub",
    "notify_dry_run": true
  }

Chain (supervisor → foreman → gate over files)::
  {
    "type": "chain",
    "slice": "scratch-hello",
    "pack": "scratch-hello.pack.md",
    "acceptance": "text or list",
    "profile": "acp-lite",
    "cwd": "/tmp/dsh-trial-example-workdir",
    "max_rounds": 2,
    "gate_profile": "acp-lite",
    "notify": "hub",
    "notify_dry_run": true
  }

Chain reply (resume after awaiting_supervisor)::
  {
    "type": "chain-reply",
    "slice": "scratch-hello",
    "answer": "supervisor answer text"
  }

Goal (dsh supervisor plans slices; Hub only drops Goal)::
  {
    "type": "goal",
    "goal": "intra-comms-proto",
    "brief": "natural language goal…",
    "cwd": "/tmp/dsh-trial-example-workdir",
    "profile": "acp-lite",
    "supervisor_profile": "acp-lite",
    "max_slices": 3,
    "max_supervisor_tickets": 8,
    "max_rounds": 2,
    "on_slice_fail": "stop",          # stop (default) | continue
    "notify": "hub",
    "notify_dry_run": true
  }

The supervisor plan summary may emit one slice (legacy ``action=emit_chain``
with top-level slice/pack/acceptance) or many (``action=emit_chains`` with a
``slices`` list). The broker runs each slice as its own chain, stops on the
first failed/escalated slice unless ``on_slice_fail=continue``, and runs the
supervisor close once after every slice PASSes.

An impl that exits without a summary because it hit the ACP hard cap
(``timeout after Ns hard cap``) or the step cap (``max_steps`` / 步数上限)
is not marked failed when the slice worktree already has committable
changes. The broker closeout commits those changes, runs the tests named
in the slice pack, opens a PR through ``dsh-trial-pr`` when the pack asks
for one, and writes a supervisor handoff under thin-state summaries. A
clean worktree still fails. An idle timeout is not a closeout.

``type:goal-update`` with ``resume: true`` on a goal already ``failed``
continues only when that failure is a missing impl summary and the slice
already has gate PASS. A metrics row may name the slice ``s1`` while the
chain slice is ``{goal}-s1``; those are the same slice for this goal only.
Slices already PASS are not re-opened. A real failure without gate PASS
is rejected and is not turned into PASS.

Usage
-----
  trial-broker.py --once
  trial-broker.py --poll 20
  trial-broker.py --status
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
import threading
import trial_lib as T
import trial_mailbox as TM

TZ = ZoneInfo("Asia/Shanghai")

DSH_HOME = Path(os.environ.get("DSH_HOME") or (Path.home() / ".dsh"))
_REPO_ROOT = Path(__file__).resolve().parent.parent
TRIAL_SUP = DSH_HOME / "supervisor" / "trial"
INBOX = TRIAL_SUP / "inbox"
OUTBOX = TRIAL_SUP / "outbox"
PROCESSING = TRIAL_SUP / "processing"
FAILED = TRIAL_SUP / "failed"
STATE_DIR = Path(os.environ.get("TRIAL_BROKER_DIR") or (DSH_HOME / "broker-dsh-trial"))
ARTIFACT_ROOT = Path(
    os.environ.get("TRIAL_ARTIFACT_ROOT")
    or (Path(os.environ.get("TMPDIR") or "/tmp") / "trial-broker")
)
OPEN_SLICE = Path(
    os.environ.get("DSH_TRIAL_OPEN_SLICE") or (_REPO_ROOT / "open-slice.sh")
)
COMPOSE = DSH_HOME / "bin" / "khub-acp-ctx-composition.py"
ASSERT = Path(
    os.environ.get("DSH_TRIAL_ASSERT") or (_REPO_ROOT / "assert-no-room-inject.py")
)
PACKS = DSH_HOME / "supervisor" / "thin-state" / "packs"
SUMMARIES = DSH_HOME / "supervisor" / "thin-state" / "summaries"
CHAINS = DSH_HOME / "supervisor" / "thin-state" / "chains"
GOALS = DSH_HOME / "supervisor" / "thin-state" / "goals"
# 与 trial_lib.MAILBOX 完全一致（含 DSH_TRIAL_MAILBOX env 覆盖；插件侧用 $DSH_HOME 全局 mailbox）
MAILBOX = T.MAILBOX
HOMES_ROOT = Path(os.environ.get("DSH_HOMES_ROOT") or (Path.home() / ".dsh-homes"))
NOTIFY = DSH_HOME / "bin" / "khub-dsh-complete-notify.py"
HEARTBEAT = Path(
    os.environ.get("TRIAL_BROKER_HEARTBEAT")
    or (STATE_DIR / "trial-broker.heartbeat.json")
)
OFFLOAD_GC = DSH_HOME / "bin" / "dsh-offload-gc.py"
_LAST_OFFLOAD_GC_TS = 0.0

# Pack / diff caps (align with dsh-design-pack default 12288)
MAX_PACK_BYTES = int(os.environ.get("TRIAL_MAX_PACK_BYTES", "12288"))
MAX_DIFF_BYTES = int(os.environ.get("TRIAL_MAX_DIFF_BYTES", "6144"))

PROD_BROKER = DSH_HOME / "broker-khub-prod"
PROD_RESTART = DSH_HOME / "bin" / "khub-broker-restart.sh"

FOREMAN_STATUS_DONE = "done"
FOREMAN_STATUS_BLOCKED = "blocked"
FOREMAN_STATUS_QUESTION = "question"
VERDICT_PASS = "PASS"
VERDICT_HOLD = "HOLD"


def _now() -> datetime:
    return datetime.now(TZ)


def _iso() -> str:
    return _now().isoformat(timespec="seconds")


def _stamp() -> str:
    return _now().strftime("%Y%m%d-%H%M%S")


def _ensure_dirs() -> None:
    for d in (INBOX, OUTBOX, PROCESSING, FAILED, STATE_DIR, ARTIFACT_ROOT, CHAINS, PACKS, SUMMARIES, GOALS, MAILBOX):
        d.mkdir(parents=True, exist_ok=True)


def _write_heartbeat(pending: int = 0, *, extra: dict | None = None) -> None:
    """Persist loop liveness for the independent watchdog (Addendum ②).

    Path: ``$TRIAL_BROKER_DIR/trial-broker.heartbeat.json`` (or
    ``$TRIAL_BROKER_HEARTBEAT``). Never raises.
    """
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "pid": os.getpid(),
            "at": _iso(),
            "wall": time.time(),
            "mono": time.monotonic(),
            "pending": int(pending),
        }
        if extra:
            payload.update(extra)
        HEARTBEAT.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    except OSError:
        pass


def _redact_env(env: dict[str, str]) -> dict[str, str]:
    bad = re.compile(r"(KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL)", re.I)
    return {k: ("***" if bad.search(k) else v) for k, v in env.items()}


def _parse_frontmatter(text: str) -> dict:
    if not text.startswith("---"):
        return {}
    parts = text.split("---", 2)
    if len(parts) < 3:
        return {}
    meta: dict = {}
    for line in parts[1].splitlines():
        line = line.strip()
        if not line or line.startswith("#") or ":" not in line:
            continue
        k, v = line.split(":", 1)
        meta[k.strip()] = v.strip().strip("\"'")
    return meta


def _extract_fenced_json(text: str) -> dict | None:
    """Pull first ```json ... ``` or bare {...} object from summary text."""
    if not text:
        return None
    m = re.search(r"```(?:json|JSON)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    if m:
        try:
            obj = json.loads(m.group(1))
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            pass
    # YAML-ish front matter already handled elsewhere; try last {...} block
    m2 = re.search(r"(\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\})", text, re.DOTALL)
    if m2:
        try:
            obj = json.loads(m2.group(1))
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            pass
    return None


def _acceptance_text(acceptance) -> str:
    if acceptance is None:
        return ""
    if isinstance(acceptance, list):
        return "\n".join(f"- {x}" for x in acceptance)
    return str(acceptance).strip()


# --- Fix 2: gate acceptance must not require an already-open PR -------------
# The gate decides code correctness; an open/unmerged PR is a post-PASS
# delivery artifact (impl / dsh-trial-pr / supervisor-close). When a Hub brief
# mistakenly lists PR evidence as gate acceptance, the gate could HOLD forever:
# impl is forbidden from opening a PR before gate PASS. Strip those items from
# the gate pack (never from the caller's own list) so it cannot deadlock.
_GATE_ACCEPTANCE_PR_MARKERS = (
    "dsh-trial-pr",
    "已开 pr",
    "pr 已开",
    "开 pr",
    "pull request",
    "unmerged pr",
    "未合并",
    "pr url",
)
GATE_ACCEPTANCE_PR_FILTER_NOTE = "（已剔除 PR 证据项；PR 由 close/impl 在 gate PASS 后核对）"


def _is_gate_acceptance_pr_item(item) -> bool:
    """Heuristic: does this acceptance entry demand an open/unmerged PR?"""
    low = str(item or "").lower()
    if not low:
        return False
    if "github.com/" in low and "/pull/" in low:
        return True
    return any(marker in low for marker in _GATE_ACCEPTANCE_PR_MARKERS)


def filter_gate_acceptance(acceptance):
    """Return a copy of acceptance with PR-open evidence stripped (Fix 2).

    Preserves the input shape as far as practical: lists lose matching items,
    a multi-line string loses matching lines, a matching single-line string
    becomes ``""``. The caller's object is never mutated.
    """
    if acceptance is None:
        return acceptance
    if isinstance(acceptance, list):
        return [x for x in acceptance if not _is_gate_acceptance_pr_item(x)]
    if isinstance(acceptance, tuple):
        return tuple(x for x in acceptance if not _is_gate_acceptance_pr_item(x))
    if isinstance(acceptance, str):
        if "\n" in acceptance:
            return "\n".join(
                ln for ln in acceptance.splitlines()
                if not _is_gate_acceptance_pr_item(ln)
            )
        return "" if _is_gate_acceptance_pr_item(acceptance) else acceptance
    return "" if _is_gate_acceptance_pr_item(acceptance) else acceptance


def _gate_acceptance_text(acceptance) -> tuple[str, bool]:
    """Filter + render gate acceptance; report whether anything was stripped."""
    filtered = filter_gate_acceptance(acceptance)
    try:
        stripped_any = filtered != acceptance
    except Exception:  # pragma: no cover - exotic non-comparable input
        stripped_any = False
    text = _acceptance_text(filtered)
    if stripped_any:
        text = f"{GATE_ACCEPTANCE_PR_FILTER_NOTE}\n{text}".strip()
    return text, stripped_any


def _truncate_bytes(s: str, cap: int, note: str = "\n\n…[truncated]\n") -> tuple[str, bool]:
    raw = s.encode("utf-8")
    if len(raw) <= cap:
        return s, False
    # leave room for note
    note_b = note.encode("utf-8")
    keep = max(0, cap - len(note_b))
    cut = raw[:keep].decode("utf-8", errors="ignore")
    return cut + note, True


def _enforce_pack_bytes(path: Path, cap: int = MAX_PACK_BYTES) -> None:
    data = path.read_bytes()
    if len(data) > cap:
        raise ValueError(
            f"pack {path.name} is {len(data)} bytes > maxPackBytes={cap}"
        )


def _job_cwd(data: dict, path: Path) -> str:
    """Return the job's cwd, warning and falling back to the broker's own
    cwd when the job omits it (never silently default to /workspace)."""
    raw = data.get("cwd")
    s = str(raw).strip() if raw is not None else ""
    if not s:
        cwd = os.getcwd()
        print(
            f"[trial-broker] job {path} missing cwd; defaulting to broker cwd {cwd}",
            flush=True,
        )
        return cwd
    return str(raw)


def load_job(path: Path) -> dict:
    raw = path.read_text(encoding="utf-8")
    if path.suffix == ".json":
        data = json.loads(raw)
    else:
        data = _parse_frontmatter(raw)
    if not isinstance(data, dict):
        raise ValueError(f"job must be object: {path}")

    jtype = str(data.get("type") or "ticket").strip().lower()
    base = {
        "id": data.get("id") or path.stem,
        "type": jtype,
        "source": str(path),
        "loaded_at": _iso(),
        "notify": data.get("notify"),
        "notify_dry_run": data.get("notify_dry_run"),
        "notify_prefix": data.get("notify_prefix"),
        "goal": data.get("goal"),
    }

    if jtype == "chain":
        slice_id = data.get("slice")
        pack = data.get("pack")
        if not slice_id or not pack:
            raise ValueError(f"chain job missing slice/pack: {path}")
        profile = str(data.get("profile") or "acp")
        if profile not in ("acp", "acp-lite", "acp-trial", "acp-lite-trial"):
            raise ValueError(f"profile must be acp|acp-lite|acp-trial|acp-lite-trial, got {profile!r}")
        gate_profile = str(data.get("gate_profile") or profile)
        if gate_profile not in ("acp", "acp-lite", "acp-trial", "acp-lite-trial"):
            raise ValueError(f"gate_profile must be acp|acp-lite|*-trial, got {gate_profile!r}")
        max_rounds = int(data.get("max_rounds") or 2)
        if max_rounds < 1 or max_rounds > 8:
            raise ValueError(f"max_rounds out of range: {max_rounds}")
        return {
            **base,
            "slice": str(slice_id),
            "pack": str(pack),
            "acceptance": data.get("acceptance") or "",
            "profile": profile,
            "gate_profile": gate_profile,
            "cwd": _job_cwd(data, path),
            "max_rounds": max_rounds,
        }

    if jtype == "chain-reply":
        slice_id = data.get("slice")
        answer = data.get("answer")
        if not slice_id or answer is None or str(answer).strip() == "":
            raise ValueError(f"chain-reply missing slice/answer: {path}")
        return {
            **base,
            "slice": str(slice_id),
            "answer": str(answer),
        }

    if jtype == "goal-update":
        goal_id = data.get("goal") or data.get("id")
        if not goal_id:
            raise ValueError(f"goal-update missing goal: {path}")
        return {
            **base,
            "goal": str(goal_id),
            "cancel": bool(data.get("cancel") or str(data.get("action") or "").lower() in ("cancel", "abort")),
            "reason": data.get("reason"),
            "max_slices": data.get("max_slices"),
            "max_supervisor_tickets": data.get("max_supervisor_tickets"),
            "max_rounds": data.get("max_rounds"),
            "ask_supervisor_timeout_sec": data.get("ask_supervisor_timeout_sec"),
            "ask_supervisor_poll_ms": data.get("ask_supervisor_poll_ms"),
            "prompt_timeout_sec": data.get("prompt_timeout_sec"),
            "action": data.get("action"),
            "resume": bool(data.get("resume")),
        }

    if jtype == "goal":
        goal_id = data.get("goal") or data.get("id")
        brief = data.get("brief") or data.get("goal_text") or data.get("prompt")
        if not goal_id or not brief:
            raise ValueError(f"goal job missing goal/brief: {path}")
        profile = str(data.get("profile") or "acp-lite")
        if profile not in ("acp", "acp-lite", "acp-trial", "acp-lite-trial"):
            raise ValueError(f"profile must be acp|acp-lite|acp-trial|acp-lite-trial, got {profile!r}")
        sup_profile = str(data.get("supervisor_profile") or "acp-lite")
        if sup_profile not in ("acp", "acp-lite", "acp-trial", "acp-lite-trial"):
            raise ValueError(f"supervisor_profile must be acp|acp-lite|*-trial, got {sup_profile!r}")
        max_slices = int(data.get("max_slices") or 1)
        max_sup = int(data.get("max_supervisor_tickets") or 8)
        max_rounds = int(data.get("max_rounds") or 2)
        return {
            **base,
            "goal": str(goal_id),
            "brief": str(brief),
            "profile": profile,
            "supervisor_profile": sup_profile,
            "gate_profile": str(data.get("gate_profile") or profile),
            "cwd": _job_cwd(data, path),
            "max_slices": max_slices,
            "max_supervisor_tickets": max_sup,
            "max_rounds": max_rounds,
            "ask_supervisor_timeout_sec": data.get("ask_supervisor_timeout_sec"),
            "ask_supervisor_poll_ms": data.get("ask_supervisor_poll_ms"),
            "prompt_timeout_sec": data.get("prompt_timeout_sec"),
            "suggested_slice": data.get("suggested_slice"),
            "suggested_pack": data.get("suggested_pack"),
            "acceptance_hint": data.get("acceptance_hint"),
            "auto_supervisor_answer": data.get("auto_supervisor_answer", True),
            "on_slice_fail": data.get("on_slice_fail"),  # stop (default) | continue
            "slices_spec": data.get("slices_spec"),  # optional explicit multi-slice list
        }

    # default single ticket
    ticket = data.get("ticket")
    pack = data.get("pack")
    if not ticket or not pack:
        raise ValueError(f"job missing ticket/pack: {path}")
    job = {
        **base,
        "ticket": str(ticket),
        "pack": str(pack),
        "profile": str(data.get("profile") or "acp"),
        "cwd": _job_cwd(data, path),
        "role": data.get("role"),
        "summary_name": data.get("summary_name") or data.get("summary"),
        # Single-ticket job owns its own findings-first rework budget.
        "max_rounds": int(data.get("max_rounds") or 2),
    }
    if job["profile"] not in ("acp", "acp-lite", "acp-trial", "acp-lite-trial"):
        raise ValueError(f"profile must be acp|acp-lite|*-trial, got {job['profile']!r}")
    return job


def list_pending() -> list[Path]:
    files = []
    for p in sorted(INBOX.iterdir()):
        if p.name.startswith(".") or p.name == "README.md":
            continue
        if p.suffix in (".json", ".md") and p.is_file():
            files.append(p)
    return files


def quarantine_inbox_reject(path: Path, reason: str, *, raw: dict | None = None) -> dict:
    """Quarantine one bad inbox file so a single reject never kills the broker.

    Best-effort only: moves ``path`` under ``FAILED``, writes a small
    ``*.error.json`` sidecar (never the full raw body — it may carry secrets),
    logs, and wakes Hub with ``kind=dsh-trial-inbox-reject``. Never re-raises.
    """
    _ensure_dirs()
    name = path.name
    dest = FAILED / name
    if dest.exists():
        # Collision: stamp-prefix so the rejected file is never lost/overwritten.
        dest = FAILED / f"{_stamp()}-{name}"
        n = 1
        while dest.exists():
            dest = FAILED / f"{_stamp()}-{n}-{name}"
            n += 1
    error_path = FAILED / f"{dest.stem}.error.json"

    payload: dict = {"error": reason, "original_name": name, "at": _iso()}
    if isinstance(raw, dict):
        for key in ("goal", "id", "type"):
            val = raw.get(key)
            if val is not None:
                payload[key] = val

    moved = False
    try:
        if path.exists():
            shutil.move(str(path), str(dest))
            moved = True
    except OSError as e:
        print(f"[trial-broker] quarantine move failed file={name} err={e}", flush=True)

    try:
        error_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    except OSError as e:
        print(f"[trial-broker] quarantine error.json write failed file={name} err={e}", flush=True)

    print(f"[trial-broker] quarantine inbox reject file={name} reason={reason} → failed/", flush=True)

    job = {
        "id": str((raw or {}).get("id") or path.stem),
        "goal": (raw or {}).get("goal"),
        "notify": (raw or {}).get("notify") or "hub",
        "notify_dry_run": (raw or {}).get("notify_dry_run"),
    }
    notify_result: dict | None = None
    try:
        notify_result = maybe_notify_hub(
            job,
            {"status": "failed"},
            {"ticket": job["id"]},
            kind="dsh-trial-inbox-reject",
            extra_summary=f"inbox reject file={name} reason={reason}",
            payload_extra={
                "reason": reason,
                "original_name": name,
                "suggested_action": "fix inbox JSON (e.g. add type:goal / ticket+pack) and re-drop",
            },
        )
        if not (isinstance(notify_result, dict) and notify_result.get("sent")):
            print(
                f"[trial-broker] quarantine notify not sent kind=dsh-trial-inbox-reject file={name}",
                flush=True,
            )
    except Exception as e:  # noqa: BLE001 - intake guard must never re-raise
        notify_result = {"sent": False, "error": type(e).__name__}
        print(
            f"[trial-broker] quarantine notify failed kind=dsh-trial-inbox-reject "
            f"file={name} err={type(e).__name__}",
            flush=True,
        )

    return {
        "dest": str(dest) if moved else None,
        "error_path": str(error_path),
        "original_name": name,
        "notify": notify_result,
    }


def _quarantine_unexpected(path: Path, exc: Exception) -> None:
    """Quarantine after an unexpected ``run_job`` blow-up. Never re-raises."""
    reason = f"unexpected: {exc}"
    try:
        target: Path | None = path
        if not path.exists() and PROCESSING.is_dir():
            target = next(iter(sorted(PROCESSING.glob(f"*{path.name}"))), None)
        if target is None:
            print(f"[trial-broker] unexpected reject left no file path={path.name}", flush=True)
            return
        quarantine_inbox_reject(target, reason)
    except Exception as e:  # noqa: BLE001 - intake guard must never re-raise
        print(
            f"[trial-broker] unexpected reject quarantine failed file={path.name} err={e}",
            flush=True,
        )


def find_session(ticket: str, role: str | None) -> Path | None:
    candidates: list[Path] = []
    role_homes: list[Path] = []
    if role:
        role_homes.append(HOMES_ROOT / role)
    for r in ("impl", "gate", "supervisor"):
        h = HOMES_ROOT / r
        if h not in role_homes:
            role_homes.append(h)
    role_homes.append(DSH_HOME)

    for home in role_homes:
        meta = home / "acp-tickets" / f"{ticket}.json"
        if meta.is_file():
            try:
                m = json.loads(meta.read_text())
                sid = m.get("sessionId")
                if sid:
                    for root in (home / "sessions",):
                        if not root.is_dir():
                            continue
                        for hit in root.rglob(f"*{sid}*"):
                            if hit.is_dir():
                                for name in (
                                    "session.v3.jsonl.zstd",
                                    "session.v3.jsonl",
                                ):
                                    f = hit / name
                                    if f.is_file():
                                        candidates.append(f)
                            elif hit.name.startswith("session.v3"):
                                candidates.append(hit)
            except (OSError, json.JSONDecodeError):
                pass
        sessions = home / "sessions"
        if sessions.is_dir():
            for f in sessions.rglob("session.v3.jsonl.zstd"):
                candidates.append(f)
            for f in sessions.rglob("session.v3.jsonl"):
                if not str(f).endswith(".zstd"):
                    candidates.append(f)

    scored: list[tuple[float, Path]] = []
    for f in candidates:
        try:
            mtime = f.stat().st_mtime
        except OSError:
            continue
        bonus = 0.0
        text_hint = str(f)
        if ticket.replace("-", "")[:12] in text_hint.replace("-", ""):
            bonus = 1e9
        scored.append((mtime + bonus, f))
    if not scored:
        return None
    scored.sort(key=lambda x: x[0], reverse=True)
    seen = set()
    now = time.time()
    for score, f in scored:
        rp = f.resolve()
        if rp in seen:
            continue
        seen.add(rp)
        try:
            if now - f.stat().st_mtime < 7200 or score > 1e9:
                return f
        except OSError:
            continue
    return scored[0][1] if scored else None


def find_session_from_log(log_path: Path) -> Path | None:
    if not log_path.is_file():
        return None
    text = log_path.read_text(errors="replace")
    m = re.search(r"sessionId=([0-9a-fA-F-]{36})", text)
    if not m:
        return None
    sid = m.group(1)
    for home in (HOMES_ROOT / "impl", HOMES_ROOT / "gate", HOMES_ROOT / "supervisor", DSH_HOME):
        sessions = home / "sessions"
        if not sessions.is_dir():
            continue
        for d in sessions.rglob(sid):
            if d.is_dir():
                for name in ("session.v3.jsonl.zstd", "session.v3.jsonl"):
                    f = d / name
                    if f.is_file():
                        return f
        for f in sessions.rglob(f"*{sid}*"):
            if f.is_file() and "session.v3" in f.name:
                return f
    return None


def write_artifacts(job: dict, log_path: Path, summary_path: Path, ec: int, ticket: str | None = None) -> dict:
    ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
    ticket = ticket or job.get("ticket") or "unknown"
    art_dir = ARTIFACT_ROOT / ticket
    art_dir.mkdir(parents=True, exist_ok=True)
    result = {
        "ticket": ticket,
        "job_id": job.get("id"),
        "exit_code": ec,
        "log": str(log_path),
        "summary": str(summary_path),
        "summary_exists": summary_path.is_file(),
        "composition": None,
        "assert": None,
        "session": None,
        "assert_clean": None,
        "at": _iso(),
    }
    session = find_session_from_log(log_path) or find_session(
        ticket, job.get("role")
    )
    if session:
        result["session"] = str(session)
        comp_csv = art_dir / "composition.csv"
        comp_md = art_dir / "composition.md"
        assert_json = art_dir / "assert.json"
        try:
            cp = subprocess.run(
                [
                    sys.executable,
                    str(COMPOSE),
                    str(session),
                    "--csv",
                    str(comp_csv),
                    "--md",
                    str(comp_md),
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=120,
            )
            if cp.returncode == 0 and comp_csv.is_file():
                result["composition"] = str(comp_csv)
                if comp_md.is_file():
                    result["composition_md"] = str(comp_md)
            else:
                result["composition_error"] = (
                    f"rc={cp.returncode} " + (cp.stderr or cp.stdout or "")[-500:]
                )
        except (OSError, subprocess.SubprocessError) as e:
            result["composition_error"] = str(e)
        try:
            r = subprocess.run(
                [sys.executable, str(ASSERT), str(session)],
                capture_output=True,
                text=True,
                timeout=120,
            )
            assert_json.write_text(r.stdout or r.stderr or "")
            result["assert"] = str(assert_json)
            try:
                parsed = json.loads(r.stdout or "{}")
                result["assert_clean"] = bool(parsed.get("clean"))
            except json.JSONDecodeError:
                result["assert_clean"] = r.returncode == 0
        except (OSError, subprocess.SubprocessError) as e:
            result["assert_error"] = str(e)
    report = art_dir / "broker-result.json"
    report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    result["report"] = str(report)
    return result


def _spawn_env(
    role: str | None,
    *,
    ticket: str | None = None,
    prompt_timeout_sec: int | None = None,
    prompt_idle_timeout_sec: int | None = None,
) -> dict[str, str]:
    env = os.environ.copy()
    if role:
        env.pop("DSH_HOME", None)
        env["DSH_ROLE"] = str(role)
    if "NEWAPICY1" not in env and "DEEPSEEK_API_KEY" not in env and "NEWAPICY1_API_KEY" not in env:
        print(
            "[trial-broker] note: source ~/.dsh/load-env.sh before start for API",
            flush=True,
        )
    env.setdefault("DSH_PERMISSION_MODE", "danger-full-access")
    # Enforce dsh-trial-git-guard (~/.dsh/bin/git): no bare push/commit on main.
    env["DSH_TRIAL_GUARD"] = "1"
    env["PATH"] = f"{DSH_HOME / 'bin'}:{Path.home() / '.local/bin'}:{env.get('PATH', '')}"
    # ask_supervisor sync wait needs long ACP prompt budget (tool timeoutMs ~ timeoutSec+60s).
    # DSH_ACP_PROMPT_TIMEOUT is the hard cap. DSH_ACP_PROMPT_IDLE_TIMEOUT resets
    # while ACP stdout shows progress (see broker/dsh-acp-ask.py).
    lim = T.load_global_limits()
    pt = int(prompt_timeout_sec or lim.get("prompt_timeout_sec") or T.DEFAULT_LIMITS["prompt_timeout_sec"])
    idle = int(
        prompt_idle_timeout_sec
        or lim.get("prompt_idle_timeout_sec")
        or T.DEFAULT_LIMITS["prompt_idle_timeout_sec"]
    )
    if idle <= 0:
        idle = pt
    if pt < idle:
        pt = idle
    env["DSH_ACP_PROMPT_TIMEOUT"] = str(pt)
    env["DSH_ACP_PROMPT_IDLE_TIMEOUT"] = str(idle)
    ask = Path(__file__).resolve().parent / "dsh-acp-ask.py"
    if ask.is_file():
        env["DSH_ACP_ASK"] = str(ask)
    if ticket:
        env["DSH_TICKET"] = str(ticket)
    env.pop("NEW_API_KEY", None)
    _ = _redact_env(env)
    return env


def _handle_pending_ask_file(path: Path, *, default_profile: str, cwd: str, goal: dict | None) -> None:
    """Wake supervisor-answer or gate short-ticket for mailbox pending files."""
    try:
        ask = T.load_ask(path)
    except (OSError, json.JSONDecodeError) as e:
        print(f"[trial-broker] bad ask file {path}: {e}", flush=True)
        return
    ask_id = str(ask.get("ask_id") or path.stem)
    ans_path = MAILBOX / "answers" / f"{ask_id}.json"
    if ans_path.is_file():
        return
    # routes.json lookup: (to_role, kind) → handler
    target = TM.dispatch_target(ask)
    if target is None and not T.ROUTES_PATH.is_file():
        # routes.json absent → legacy kind fallback for the two built-in kinds
        k = str(ask.get("kind") or "ask_supervisor")
        if k == "submit_for_review":
            target = "submit_for_review"
        elif k == "ask_supervisor":
            target = "ask_supervisor"
    if target == "submit_for_review":
        _handle_submit_for_review(ask, path, default_profile=default_profile, cwd=cwd, goal=goal)
        return
    if target == "ask_supervisor":
        _handle_ask_supervisor(ask, path, default_profile=default_profile, cwd=cwd, goal=goal)
        return
    # unknown (to_role, kind): structured no-route answer
    (MAILBOX / "answers").mkdir(parents=True, exist_ok=True)
    (MAILBOX / "answers" / f"{ask_id}.json").write_text(
        json.dumps(TM.no_route_answer(ask_id), ensure_ascii=False, indent=2) + "\n"
    )
    print(f"[trial-broker] no route for ask {ask_id} kind={ask.get('kind')!r}", flush=True)


def _handle_ask_supervisor(ask: dict, path: Path, *, default_profile: str, cwd: str, goal: dict | None) -> None:
    t0 = time.monotonic()  # 处理耗时 → wait_ms / by_kind.wait_ms_total
    ask_id = str(ask.get("ask_id") or path.stem)
    if goal:
        lim = T.resolve_limits(goal=goal)
        used = int(goal.get("supervisor_ticket_count") or 0)
        cap = int(lim.get("max_supervisor_tickets") or 8)
        if used >= cap:
            hit = T.limit_hit_payload(
                which="max_supervisor_tickets",
                limit=cap,
                used=used,
                slice_id=ask.get("slice"),
                step="ask_supervisor",
                goal_id=str(goal.get("goal")),
                suggestion=f"投 type:goal-update 提高 max_supervisor_tickets（当前 {cap}）",
            )
            goal["limit_hit"] = hit
            goal.setdefault("metrics", T.empty_metrics())["limit_hits"].append(hit)
            goal["status"] = "escalated"
            _write_goal(goal)
            T.write_ask_answer(
                ask_id,
                f"[LIMIT] max_supervisor_tickets={cap} used={used}. Degrade to status=question.",
                supervisor_ticket="(limit)",
            )
            _notify_goal_event(goal, kind="dsh-trial-limit", ok=False, extra=hit)
            print(f"[trial-broker] ask {ask_id} hit supervisor ticket cap", flush=True)
            return

    questions = ask.get("questions") or []
    slice_id = _resolve_ask_slice_id(
        ask, goal, fallback=str((goal or {}).get("goal") or "ask")
    )
    round_n = int((goal or {}).get("ask_seq") or 0) + 1
    if goal is not None:
        goal["ask_seq"] = round_n
        _write_goal(goal)
    ask_pack = build_supervisor_ask_pack(
        slice_id=slice_id,
        round_n=round_n,
        questions=questions if isinstance(questions, list) else [str(questions)],
        foreman_notes=str(ask.get("context") or ""),
        pending_pack=str(ask.get("pack") or ""),
        goal_id=(goal or {}).get("goal") if goal else ask.get("goal"),
    )
    ticket = f"supervisor-answer-{slice_id}-ask{round_n}"
    summary_name = f"{slice_id}-sup-ask{round_n}"
    raw_prof = (goal or {}).get("supervisor_profile") if goal else default_profile
    profile = T.map_profile(raw_prof or "acp-lite")
    ec, art, summary_path = run_supervisor_ticket(
        ticket=ticket,
        pack_name=ask_pack,
        profile=profile,
        cwd=cwd,
        summary_name=summary_name,
        prompt_mode="supervisor-answer",
        goal=goal,
    )
    parsed = _parse_supervisor_summary(summary_path)
    answer = str(parsed["block"].get("answer") or "").strip() or (parsed["text"] or "")[:1500]
    if not answer:
        answer = "(empty supervisor answer)"
    T.write_ask_answer(ask_id, answer, supervisor_ticket=ticket)
    if goal is not None:
        m = goal.setdefault("metrics", T.empty_metrics())
        m["ask_count"] = int(m.get("ask_count") or 0) + 1
        m["ask_sync_ok"] = int(m.get("ask_sync_ok") or 0) + 1
        peak, steps = _peak_prompt(art.get("composition"))
        wait_ms = int((time.monotonic() - t0) * 1000)
        timed_out = ec == 124  # run_open_slice timeout exit
        if timed_out:
            m["ask_timeout"] = int(m.get("ask_timeout") or 0) + 1
        T.record_ticket_metric(goal, {
            "role": "supervisor",
            "ticket": ticket,
            "slice": slice_id,
            "exit": ec,
            "peak_prompt": peak,
            "steps": steps,
            "tool_res": _tool_res_from_art(art),
            "assert_clean": art.get("assert_clean"),
            "kind": "ask_supervisor",
            "compat_kind": "ask_sync",
            "wait_ms": wait_ms,
            "timeout": timed_out,
        })
        _write_goal(goal)
    print(f"[trial-broker] answered ask {ask_id} via {ticket}", flush=True)


def _tool_res_from_art(art: dict | None) -> str | None:
    if not art:
        return None
    md = art.get("composition_md")
    if md and Path(md).is_file():
        import re
        text = Path(md).read_text(errors="replace")
        m = re.search(r"tool[_ ]?res[^0-9%]*([0-9.]+)\s*%", text, re.I)
        if m:
            return m.group(1) + "%"
    return None


def _notify_goal_event(
    goal: dict,
    *,
    kind: str,
    ok: bool,
    extra: dict | None = None,
    reason: str | None = None,
    suggested_action: str | None = None,
) -> None:
    """Notify Hub only for Goal complete / escalate / fail / limit (Decision D).

    Every goal-terminal event carries ``goal`` / ``status`` / ``reason`` /
    ``suggested_action`` (structured json-body) so the webhook routine and
    ``khub-dsh-complete-notify.py`` can act without scraping the summary.
    """
    notify = str(goal.get("notify") or "").strip().lower()
    if notify not in ("hub", "khub", "true", "1", "yes"):
        return
    job = {
        "notify": goal.get("notify"),
        "notify_dry_run": goal.get("notify_dry_run"),
        "notify_prefix": goal.get("notify_prefix"),
        "goal": goal.get("goal"),
        "slice": (goal.get("slices") or [None])[-1],
        "profile": goal.get("profile"),
    }
    status = goal.get("status") or ("completed" if ok else "failed")
    out_body = {
        "status": status,
        "goal": goal.get("goal"),
        "limit_hit": goal.get("limit_hit"),
        "extra": extra,
    }
    result = {"ticket": f"goal-{goal.get('goal')}", "composition": None, "assert_clean": True}
    reason = reason or (
        str(goal.get("error") or goal.get("escalate_reason") or "").strip()
        or (extra or {}).get("reason")
        or ""
    )
    if not suggested_action:
        suggested_action = {
            "done": "none",
            "escalated": "提高 limits 或人工处理",
            "cancelled": "无需动作",
            "failed": "检查 failed/ 并决定 salvage / 重投",
            "closeout": "核对 handoff 后人工收口",
        }.get(str(status), "检查 goal 状态")
    summary = (
        f"dsh-trial-goal goal={goal.get('goal')} status={status} kind={kind} "
        f"slices={goal.get('slices')} sup_tickets={goal.get('supervisor_ticket_count')} "
        f"tokens={(goal.get('metrics') or {}).get('prompt_token_total')} "
        f"limit_hit={goal.get('limit_hit')}"
    )
    if reason:
        summary += f" reason={reason}"
    if extra and extra.get("suggestion"):
        summary += f" suggestion={extra.get('suggestion')}"
    summary += f" suggested_action={suggested_action}"
    prefix = str(goal.get("notify_prefix") or "").strip()
    if prefix:
        summary = f"{prefix}{summary}"
    payload_extra = {"reason": reason, "suggested_action": suggested_action}
    if isinstance(extra, dict):
        payload_extra["extra"] = extra
    info = maybe_notify_hub(
        job, out_body, result, kind=kind, extra_summary=summary,
        payload_extra=payload_extra,
    )
    goal.setdefault("metrics", T.empty_metrics()).setdefault("notify_events", []).append({
        "at": _iso(), "kind": kind, "info": info,
    })
    _write_goal(goal)


AWAITING_HUB_NOTIFY_KIND = "dsh-trial-awaiting-hub"


def _notify_awaiting_hub(
    job: dict | None,
    chain: dict | None,
    *,
    reason: str,
    block: dict | None = None,
    goal: dict | None = None,
) -> dict | None:
    """Explicit Hub wake when a chain parks in ``awaiting_supervisor`` (Fix 1).

    Decision D keeps the chain's own terminal outbox at ``notify=False`` and
    ``_notify_goal_terminal`` ignores ``awaiting_*`` states, so a parked chain
    used to leave the Hub blind until something else settled the goal. This
    sends the dedicated ``dsh-trial-awaiting-hub`` event so the Hub webhook can
    push a ``chain-reply``. Never raises.
    """
    try:
        notify = ""
        for src in (job, goal, chain):
            if isinstance(src, dict) and src.get("notify"):
                notify = str(src.get("notify")).strip().lower()
                if notify:
                    break
        if notify not in ("hub", "khub", "true", "1", "yes"):
            return None

        job = job if isinstance(job, dict) else {}
        chain = chain if isinstance(chain, dict) else {}
        block = block if isinstance(block, dict) else {}
        goal = goal if isinstance(goal, dict) else None

        slice_id = str(
            job.get("slice") or chain.get("slice") or (goal or {}).get("current_slice") or ""
        )
        goal_id = str(
            job.get("goal") or chain.get("goal") or (goal or {}).get("goal") or ""
        )
        reason = str(reason or "blocked")
        questions = list(block.get("questions") or [])

        # Best-effort last gate verdict/summary path for the Hub to read.
        gate_path = ""
        if goal:
            gate_path = str(
                goal.get("last_gate_verdict_path")
                or goal.get("last_gate_summary_path")
                or ""
            )
        for rd in reversed(chain.get("rounds") or []):
            if not isinstance(rd, dict):
                continue
            gate_path = gate_path or str(
                rd.get("gate_summary") or rd.get("gate_verdict_path") or ""
            )
            if gate_path:
                break

        suggested_action = "chain-reply"
        summary = (
            f"dsh-trial-awaiting-hub goal={goal_id or '-'} slice={slice_id or '-'} "
            f"reason={reason} questions={len(questions)} "
            f"suggested_action={suggested_action}"
        )
        prefix = str(job.get("notify_prefix") or "").strip()
        if prefix:
            summary = f"{prefix}{summary}"

        payload_extra = {
            "goal": goal_id,
            "slice": slice_id,
            "status": reason,
            "reason": reason,
            "questions": questions,
            "suggested_action": suggested_action,
        }
        if gate_path:
            payload_extra["last_gate_verdict_path"] = gate_path

        notify_job = dict(job)
        notify_job.setdefault("slice", slice_id)
        if goal_id:
            notify_job.setdefault("goal", goal_id)
        out_body = {
            "status": reason,
            "chain_state": "awaiting_supervisor",
            "goal": goal_id,
            "slice": slice_id,
        }
        result = {
            "ticket": f"awaiting-{slice_id or goal_id or 'chain'}",
            "composition": None,
            "assert_clean": True,
        }
        info = maybe_notify_hub(
            notify_job,
            out_body,
            result,
            kind=AWAITING_HUB_NOTIFY_KIND,
            extra_summary=summary,
            payload_extra=payload_extra,
        )
        if goal and goal.get("goal"):
            goal.setdefault("metrics", T.empty_metrics()).setdefault(
                "notify_events", []
            ).append({
                "at": _iso(),
                "kind": AWAITING_HUB_NOTIFY_KIND,
                "info": {
                    "reason": reason,
                    "slice": slice_id,
                    "sent": (info or {}).get("sent"),
                },
            })
            _write_goal(goal)
        return info
    except Exception as e:  # never block the chain on a notify failure
        print(f"[trial-broker] awaiting-hub notify failed: {e}", flush=True)
        return None


def _atomic_write_json(path: Path, body: dict) -> None:
    """Write JSON via tmp file + os.replace so a reader never sees a partial body.

    Used for the interim ``gate_running`` ack and the final gate answer written to
    the same ``answers/<ask_id>.json`` path.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp-{os.getpid()}-{threading.get_ident()}")
    tmp.write_text(json.dumps(body, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _handle_submit_for_review(ask: dict, path: Path, *, default_profile: str, cwd: str, goal: dict | None) -> None:
    """Open delta gate short-ticket; write PASS/HOLD(+rework_mode) answer for submit_for_review."""
    t0 = time.monotonic()  # 处理耗时 → wait_ms / by_kind.wait_ms_total
    ask_id = str(ask.get("ask_id") or path.stem)
    slice_id = _resolve_ask_slice_id(ask, goal, fallback="slice")
    lim = T.resolve_limits(goal=goal) if goal else T.load_global_limits()
    round_n = int(ask.get("round") or (goal or {}).get("review_seq") or 1)
    if goal is not None:
        goal["review_seq"] = int(goal.get("review_seq") or 0) + 1
        round_n = goal["review_seq"]
        _write_goal(goal)

    # Build a gate pack: delta-only when prior findings exist on goal/chain
    prior_findings = []
    if goal:
        prior_findings = list(goal.get("last_gate_findings") or [])
    original_pack = str((goal or {}).get("current_pack") or ask.get("pack") or f"{slice_id}.pack.md")
    acceptance = (goal or {}).get("current_acceptance") or []
    foreman_block = {
        "status": "done",
        "changed_files": ask.get("changed_files") or [],
        "base": ask.get("base") or "",
        "commit": ask.get("commit") or "",
        "notes": ask.get("summary") or "",
        "questions": [],
    }
    cwd_path = Path(cwd)
    try:
        if prior_findings:
            # delta-only re-review pack
            gate_pack = build_delta_gate_pack(
                slice_id=slice_id,
                round_n=round_n,
                original_pack_name=original_pack,
                acceptance=acceptance,
                prior_findings=prior_findings,
                foreman_block=foreman_block,
                foreman_summary_text=str(ask.get("summary") or ""),
                cwd=cwd_path,
            )
        else:
            gate_pack = build_gate_pack(
                slice_id=slice_id,
                round_n=round_n,
                original_pack_name=original_pack,
                acceptance=acceptance,
                foreman_block=foreman_block,
                foreman_summary_text=str(ask.get("summary") or ""),
                cwd=cwd_path,
            )
    except ValueError as e:
        T.write_ask_answer(ask_id, f"[error] gate pack: {e}", supervisor_ticket="(error)")
        # overwrite with structured fail
        (MAILBOX / "answers" / f"{ask_id}.json").write_text(
            json.dumps({"ask_id": ask_id, "status": "done", "ok": False, "error": str(e), "verdict": "HOLD",
                        "rework_mode": "fresh", "instruction": "pack error — end ticket"}, ensure_ascii=False, indent=2) + "\n"
        )
        return

    gate_ticket = f"gate-trial-{slice_id}-rev{round_n}"
    gate_summary_name = f"{slice_id}-gate-rev{round_n}"
    gate_log = ARTIFACT_ROOT / f"{gate_ticket}.log"
    # Interim ack written BEFORE the (possibly multi-minute) gate spawn, so the
    # plugin learns a gate is running and how long to wait. It carries no
    # verdict/ok/answer so old clients keep polling. The final verdict below
    # atomically overwrites this same path.
    _atomic_write_json(
        MAILBOX / "answers" / f"{ask_id}.json",
        TM.gate_running_ack(
            ask_id,
            gate_ticket,
            gate_job_timeout_sec(lim),
            T.now_iso(),
            gate_log=gate_log,
        ),
    )
    profile = T.map_profile((goal or {}).get("gate_profile") or (goal or {}).get("profile") or default_profile)
    gec = run_open_slice(
        ticket=gate_ticket,
        pack_name=gate_pack,
        profile=profile,
        cwd=cwd,
        role="gate",
        summary_name=gate_summary_name,
        prompt_mode="gate",
        log_path=gate_log,
        goal=goal,
        watch_mailbox=False,
    )
    gate_summary = SUMMARIES / f"{gate_summary_name}.md"
    _adopt_misplaced_summary(gate_summary)
    gate_art = write_artifacts(
        {"ticket": gate_ticket, "role": "gate", "profile": profile, "cwd": cwd},
        gate_log, gate_summary, gec, ticket=gate_ticket,
    )
    gparsed = _parse_gate_verdict(gate_summary)
    gblock = enforce_gate_verdict(gparsed["block"])
    mode, answer = TM.decide_and_answer_review(
        ask=ask, gate_block=gblock, gate_ticket=gate_ticket, limits=lim,
    )
    (MAILBOX / "answers").mkdir(parents=True, exist_ok=True)
    _atomic_write_json(MAILBOX / "answers" / f"{ask_id}.json", answer)
    # archive pending
    try:
        arch = MAILBOX / "archive"
        arch.mkdir(parents=True, exist_ok=True)
        if path.is_file():
            path.rename(arch / f"{ask_id}.pending.json")
    except OSError:
        pass

    if goal is not None:
        m = goal.setdefault("metrics", T.empty_metrics())
        m["review_submit_count"] = int(m.get("review_submit_count") or 0) + 1
        if gblock.get("verdict") == VERDICT_HOLD:
            m["hold_count"] = int(m.get("hold_count") or 0) + 1
            if mode:
                m["rework_rounds"] = int(m.get("rework_rounds") or 0) + 1
                key = f"rework_{mode}"
                bucket = m.setdefault(key, {"count": 0, "steps_sum": 0, "prompt_token_total": 0, "pass_after": 0})
                bucket["count"] = int(bucket.get("count") or 0) + 1
                # steps/tokens for the gate ticket itself recorded below; inplace segment filled later by impl
                goal["pending_rework_mode"] = mode
                goal["pending_rework_ask_id"] = ask_id
                goal["last_gate_findings"] = gblock.get("findings") or []
        else:
            # PASS after rework?
            prm = goal.pop("pending_rework_mode", None)
            if prm:
                bucket = m.setdefault(f"rework_{prm}", {"count": 0, "steps_sum": 0, "prompt_token_total": 0, "pass_after": 0})
                bucket["pass_after"] = int(bucket.get("pass_after") or 0) + 1
            goal["last_gate_findings"] = []
        peak, steps = _peak_prompt(gate_art.get("composition"))
        T.record_ticket_metric(goal, {
            "role": "gate",
            "ticket": gate_ticket,
            "slice": slice_id,
            "exit": gec,
            "peak_prompt": peak,
            "steps": steps,
            "tool_res": _tool_res_from_art(gate_art),
            "assert_clean": gate_art.get("assert_clean"),
            "kind": "submit_for_review",
            "verdict": gblock.get("verdict"),
            "rework_mode": mode or None,
            "wait_ms": int((time.monotonic() - t0) * 1000),
        })
        _write_goal(goal)
    print(
        f"[trial-broker] review {ask_id} verdict={answer.get('verdict')} mode={mode or '-'} via {gate_ticket}",
        flush=True,
    )


def build_delta_gate_pack(
    *,
    slice_id: str,
    round_n: int,
    original_pack_name: str,
    acceptance,
    prior_findings: list,
    foreman_block: dict,
    foreman_summary_text: str,
    cwd: Path,
) -> str:
    """Delta-only gate pack: prior findings + new diff + acceptance. No full original pack body."""
    base = (foreman_block or {}).get("base") or ""
    commit = (foreman_block or {}).get("commit") or _git_rev(cwd)
    if not base:
        base = _git_rev(cwd, "HEAD~1") or ""
    diff = _git_diff(cwd, base if base else None, commit or "HEAD")
    diff, _ = _truncate_bytes(diff, MAX_DIFF_BYTES)
    findings_json = json.dumps(prior_findings or [], ensure_ascii=False, indent=2)
    acc_txt, _acc_stripped = _gate_acceptance_text(acceptance)
    name = f"{slice_id}-gate-delta-r{round_n}.pack.md"
    path = PACKS / name
    body = f"""---
slice_id: {slice_id}
kind: gate-delta
round: {round_n}
role: gate
original_pack: {Path(original_pack_name).name}
summary_out: $DSH_HOME/supervisor/thin-state/summaries/{slice_id}-gate-rev{round_n}.md
---

# Delta gate · {slice_id} · rev{round_n}

## Acceptance (still apply)
{acc_txt}

## Prior findings only (re-check these; do not full-file review)
```json
{findings_json}
```

## New diff (base...head · merge-base)
```diff
{diff}
```

## Foreman notes
{(foreman_summary_text or '')[:1500]}

## Done when
- Delta-only: verify prior findings fixed; note any new P0/P1.
- Write verdict PASS|HOLD with machine json (findings/unmet_acceptance).
- No room_*; do not modify business src.
"""
    path.write_text(body, encoding="utf-8")
    _enforce_pack_bytes(path)
    return name


def gate_job_timeout_sec(lim: dict | None = None) -> int:
    """Wall-clock budget for one open-slice gate job (seconds).

    ``TRIAL_BROKER_JOB_TIMEOUT`` env override wins if set; otherwise it is
    ``ask_supervisor_timeout_sec + prompt_timeout_sec + 120`` (defaults
    600 + 3600 + 120 = 4320). The plugin must wait **at least** this long
    (``gateTimeoutSec``) before declaring a gate answer missing: the broker only
    writes the final ``answers/<ask_id>.json`` after the gate short-ticket
    exits.
    """
    lim = lim or {}
    ask_to = int(lim.get("ask_supervisor_timeout_sec") or 600)
    hard = int(lim.get("prompt_timeout_sec") or T.DEFAULT_LIMITS["prompt_timeout_sec"])
    return int(os.environ.get("TRIAL_BROKER_JOB_TIMEOUT") or 0) or (ask_to + hard + 120)


def run_open_slice(
    *,
    ticket: str,
    pack_name: str,
    profile: str,
    cwd: str,
    role: str,
    summary_name: str,
    prompt_mode: str,
    log_path: Path,
    goal: dict | None = None,
    watch_mailbox: bool | None = None,
) -> int:
    profile = T.map_profile(profile)
    cmd = [
        str(OPEN_SLICE),
        "--ticket", ticket,
        "--pack", pack_name,
        "--profile", profile,
        "--cwd", cwd,
        "--role", role,
        "--summary-name", summary_name,
        "--prompt-mode", prompt_mode,
        "--final",
        "--log", str(log_path),
    ]
    lim = T.resolve_limits(goal=goal) if goal else T.load_global_limits()
    env = _spawn_env(
        role,
        ticket=ticket,
        prompt_timeout_sec=int(lim.get("prompt_timeout_sec") or T.DEFAULT_LIMITS["prompt_timeout_sec"]),
        prompt_idle_timeout_sec=int(
            lim.get("prompt_idle_timeout_sec") or T.DEFAULT_LIMITS["prompt_idle_timeout_sec"]
        ),
    )
    # Job timeout must exceed the prompt hard cap. A fixed 2400s backstop
    # used to cut the slice before a still-working agent finished.
    # Shared with the gate ack so the plugin's wait matches this budget.
    job_timeout = gate_job_timeout_sec(lim)
    print(
        f"[trial-broker] spawn open-slice ticket={ticket} pack={pack_name} "
        f"profile={profile} role={role} mode={prompt_mode} job_timeout={job_timeout}",
        flush=True,
    )
    if watch_mailbox is None:
        watch_mailbox = role == "impl"
    stop = threading.Event()
    watcher = None
    if watch_mailbox:
        # 超时对齐：watcher 与插件共用同一 MAILBOX（T.MAILBOX，可读 DSH_TRIAL_MAILBOX）。
        # 插件侧同步等待超时须 ≥ 工具 timeoutSec ≤ ask_supervisor_timeout_sec；
        # ACP prompt：空闲 DSH_ACP_PROMPT_IDLE_TIMEOUT（默认 900，须 > ask 等待）有进展就重置；
        # 硬上限 DSH_ACP_PROMPT_TIMEOUT / prompt_timeout_sec（默认 3600）到点仍收口。
        def _watch():
            while not stop.is_set():
                _write_heartbeat(1)
                for ap in T.list_pending_asks(MAILBOX):
                    try:
                        _handle_pending_ask_file(
                            ap,
                            default_profile=str((goal or {}).get("supervisor_profile") or "acp-lite"),
                            cwd=cwd,
                            goal=goal,
                        )
                    except Exception as e:  # noqa: BLE001
                        print(f"[trial-broker] mailbox watch error: {e}", flush=True)
                stop.wait(2.0)
        watcher = threading.Thread(target=_watch, name="mailbox-watch", daemon=True)
        watcher.start()
    try:
        r = subprocess.run(cmd, env=env, timeout=job_timeout)
        return r.returncode
    except subprocess.TimeoutExpired:
        print("[trial-broker] FAIL timeout", flush=True)
        return 124
    except OSError as e:
        print(f"[trial-broker] FAIL spawn: {e}", flush=True)
        return 127
    finally:
        stop.set()
        if watcher is not None:
            watcher.join(timeout=5)


def _git_rev(cwd: Path, rev: str = "HEAD") -> str:
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--short", rev],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=15,
        )
        return (r.stdout or "").strip() if r.returncode == 0 else ""
    except (OSError, subprocess.SubprocessError):
        return ""


def _git_diff(cwd: Path, base: str | None, head: str = "HEAD") -> str:
    """Diff merge-base(base, head)...head; if no base, use HEAD vs worktree/last commit.

    Fix 3: use three-dot ``base...head`` when a base is set. Two-dot
    ``base..head`` is a plain tree-to-tree diff at those two commits, so when
    ``base`` is an older tip and ``head`` has since merged main, it reports
    main-side files as if this branch changed them. Three-dot diffs from the
    merge base, i.e. only what this branch actually introduced — which is what
    the gate "New diff" must show.
    """
    try:
        if base:
            cmd = ["git", "diff", f"{base}...{head}"]
        else:
            # unstaged + staged vs HEAD
            cmd = ["git", "diff", "HEAD"]
        r = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True, timeout=30)
        diff = r.stdout or ""
        if not diff.strip():
            r2 = subprocess.run(
                ["git", "diff", "--cached"],
                cwd=str(cwd),
                capture_output=True,
                text=True,
                timeout=30,
            )
            diff = r2.stdout or ""
        if not diff.strip() and base:
            r3 = subprocess.run(
                ["git", "show", "--stat", "--patch", head],
                cwd=str(cwd),
                capture_output=True,
                text=True,
                timeout=30,
            )
            diff = r3.stdout or ""
        return diff
    except (OSError, subprocess.SubprocessError) as e:
        return f"(git diff failed: {e})"


def _read_chain(slice_id: str) -> dict | None:
    p = CHAINS / f"{slice_id}.json"
    if not p.is_file():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _write_chain(state: dict) -> Path:
    CHAINS.mkdir(parents=True, exist_ok=True)
    slice_id = state["slice"]
    p = CHAINS / f"{slice_id}.json"
    state["updated_at"] = _iso()
    p.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n")
    return p


def _new_chain_state(job: dict) -> dict:
    return {
        "slice": job["slice"],
        "type": "chain",
        "state": "running",
        "pack": job["pack"],
        "acceptance": job.get("acceptance"),
        "profile": job["profile"],
        "gate_profile": job["gate_profile"],
        "cwd": job["cwd"],
        "max_rounds": job["max_rounds"],
        "notify": job.get("notify"),
        "notify_dry_run": job.get("notify_dry_run"),
        "notify_prefix": job.get("notify_prefix"),
        "rounds": [],
        "last_verdict": None,
        "created_at": _iso(),
        "updated_at": _iso(),
        "awaiting_answer_for_round": None,
    }


def build_gate_pack(
    *,
    slice_id: str,
    round_n: int,
    original_pack_name: str,
    acceptance,
    foreman_block: dict,
    foreman_summary_text: str,
    cwd: Path,
) -> str:
    """Write packs/<slice>-gate-r<N>.pack.md; return basename."""
    orig_path = PACKS / Path(original_pack_name).name
    orig_body = orig_path.read_text(encoding="utf-8") if orig_path.is_file() else "(missing original pack)"
    # Prefer machine base/commit for diff
    base = (foreman_block or {}).get("base") or ""
    commit = (foreman_block or {}).get("commit") or _git_rev(cwd)
    if not base:
        # try parent of HEAD
        base = _git_rev(cwd, "HEAD~1") or ""
    diff = _git_diff(cwd, base if base else None, commit or "HEAD")
    diff, truncated = _truncate_bytes(diff, MAX_DIFF_BYTES)
    acc, _acc_stripped = _gate_acceptance_text(acceptance)
    # Keep original pack reference + truncated contents
    orig_cap = max(1024, MAX_PACK_BYTES // 3)
    orig_trunc, orig_was_trunc = _truncate_bytes(orig_body, orig_cap)
    fm_json = json.dumps(foreman_block or {}, ensure_ascii=False, indent=2)
    name = f"{slice_id}-gate-r{round_n}.pack.md"
    path = PACKS / name
    body = f"""---
slice_id: {slice_id}
kind: gate-pack
round: {round_n}
role: gate
original_pack: {Path(original_pack_name).name}
summary_out: $DSH_HOME/supervisor/thin-state/summaries/{slice_id}-gate-r{round_n}.md
max_bytes_hint: {MAX_PACK_BYTES}
---

# Gate pack · {slice_id} · round {round_n}

## Original pack reference
- name: `{Path(original_pack_name).name}`
- truncated: {str(orig_was_trunc).lower()}

### Original pack contents
{orig_trunc}

## Acceptance
{acc or '(none provided)'}

## Foreman structured summary
```json
{fm_json}
```

### Foreman summary text (excerpt)
{foreman_summary_text[:2000]}

## Diff (base...head merge-base; base={base or 'HEAD'} head={commit or 'HEAD'}; truncated={str(truncated).lower()})
```diff
{diff}
```

## Done when
- Write verdict to summary_out with machine json block (verdict PASS|HOLD + findings).
- Delta-only review; no room_*; do not modify business src.
"""
    path.write_text(body, encoding="utf-8")
    # If over cap, shrink diff section further
    for _ in range(4):
        try:
            _enforce_pack_bytes(path)
            break
        except ValueError:
            # re-truncate more aggressively
            cur = path.read_text(encoding="utf-8")
            # cut from end of diff fence
            if len(cur.encode()) > MAX_PACK_BYTES:
                keep = MAX_PACK_BYTES - 80
                cut, _ = _truncate_bytes(cur, keep)
                path.write_text(cut + "\n\n…[pack truncated to maxPackBytes]\n", encoding="utf-8")
    _enforce_pack_bytes(path)
    return name


# --- Gate findings are the primary task in fix / rework packs (Change ①) ---
BLOCKING_FINDING_TIERS = ("P0", "P1")
RESOLUTION_TERMINAL_STATUSES = ("fixed", "deferred", "wontfix", "resolved")

_FIX_PACK_DONE_WHEN = """## Done when
- Fix every P0/P1 finding above; keep changes minimal.
- For EACH finding write a `finding_resolutions` entry in the summary machine block:
  `{"finding": "…or tier+issue", "change": "file:line or path+symbol", "status": "fixed|deferred|wontfix", "reason": "required when not fixed"}`.
- Unfixed P0/P1: use status=blocked/question, or keep reworking inplace and call submit_for_review again. Never status=done with an unresolved P0/P1.
- Write the foreman structured summary machine block.
- No room_*.
"""


def _finding_tier(f) -> str:
    if not isinstance(f, dict):
        return ""
    return str(
        f.get("tier") or f.get("severity") or f.get("priority") or ""
    ).strip().upper()


def pending_blocking_findings(findings) -> list:
    """P0/P1 findings only; P2 nits never block completion."""
    return [
        f for f in (findings or [])
        if isinstance(f, dict) and _finding_tier(f) in BLOCKING_FINDING_TIERS
    ]


def summary_finding_resolutions(block: dict) -> list:
    """The machine block ``finding_resolutions`` list (tolerates nested ``machine``)."""
    block = block or {}
    raw = block.get("finding_resolutions")
    if raw is None and isinstance(block.get("machine"), dict):
        raw = block["machine"].get("finding_resolutions")
    if not isinstance(raw, list):
        return []
    return [r for r in raw if isinstance(r, dict)]


def summary_claims_rework_fresh(block: dict) -> bool:
    """Whether the foreman explicitly handed off to a fresh fix ticket."""
    return "rework_fresh" in str((block or {}).get("notes") or "")


def _resolutions_cover_findings(resolutions: list, findings: list) -> bool:
    if not findings:
        return True
    if len(resolutions) < len(findings):
        return False
    statuses = [str(r.get("status") or "").strip().lower() for r in resolutions]
    return all(s in RESOLUTION_TERMINAL_STATUSES for s in statuses)


def decide_impl_completion(block: dict, goal: dict | None) -> dict:
    """Guard a ``done`` impl summary that still carries unresolved P0/P1 findings.

    Change ②: a mid-ticket ``submit_for_review`` HOLD stores findings on the
    goal (``last_gate_findings``). If the foreman then ends the ticket with
    status=done but neither claims a fresh handoff (``rework_fresh``) nor maps
    every blocking finding in ``finding_resolutions``, it is NOT a clean
    completion. The broker must open a findings-first fix ticket (fresh) or
    escalate when no round remains — never close as a clean PASS.

    Returns ``{"action": "clean_done"|"open_fix", "findings": [...], "reason": str, "mode": str}``.
    """
    goal = goal or {}
    findings = pending_blocking_findings(goal.get("last_gate_findings"))
    mode = str(goal.get("pending_rework_mode") or "")
    if not findings:
        return {
            "action": "clean_done",
            "findings": [],
            "reason": "no pending P0/P1 findings",
            "mode": mode,
        }
    status = str((block or {}).get("status") or "").strip().lower()
    if status != "done":
        # blocked / question have their own paths; nothing to guard.
        return {
            "action": "clean_done",
            "findings": findings,
            "reason": f"status={status or 'unknown'} is not done",
            "mode": mode,
        }
    if summary_claims_rework_fresh(block) or mode == "fresh":
        return {
            "action": "open_fix",
            "findings": findings,
            "reason": "fresh rework handoff on a done ticket",
            "mode": mode or "fresh",
        }
    if _resolutions_cover_findings(summary_finding_resolutions(block), findings):
        return {
            "action": "clean_done",
            "findings": findings,
            "reason": "finding_resolutions cover every blocking finding",
            "mode": mode,
        }
    return {
        "action": "open_fix",
        "findings": findings,
        "reason": (
            "status=done with pending P0/P1 findings and no "
            "finding_resolutions/rework_fresh handoff"
        ),
        "mode": "fresh",
    }


def _gate_findings_section(findings: list, gate_summary_text: str = "") -> str:
    findings_json = json.dumps(findings or [], ensure_ascii=False, indent=2)
    section = (
        "## Gate findings (primary task)\n"
        "```json\n"
        f"{findings_json}\n"
        "```\n"
    )
    if gate_summary_text:
        section += (
            "\n### Gate notes (excerpt)\n"
            f"{gate_summary_text[:1500]}\n"
        )
    return section


def _write_pack_capped(path: Path, body: str) -> None:
    path.write_text(body, encoding="utf-8")
    for _ in range(4):
        try:
            _enforce_pack_bytes(path)
            return
        except ValueError:
            cur = path.read_text(encoding="utf-8")
            cut, _ = _truncate_bytes(cur, MAX_PACK_BYTES - 80)
            path.write_text(cut + "\n\n…[pack truncated to maxPackBytes]\n", encoding="utf-8")
    _enforce_pack_bytes(path)


def build_fix_pack(
    *,
    slice_id: str,
    next_round: int,
    original_pack_name: str,
    gate_findings: list,
    gate_summary_text: str,
) -> str:
    """Write packs/<slice>-fix-r<N>.pack.md — findings first, original as appendix.

    Body order is deliberate (Change ①): Gate findings (primary) → Done when
    (per-finding ``finding_resolutions`` mapping) → Original pack abridged as an
    appendix. The original pack must never precede the findings and steal the
    primary-task slot.
    """
    orig_path = PACKS / Path(original_pack_name).name
    orig_body = orig_path.read_text(encoding="utf-8") if orig_path.is_file() else "(missing)"
    orig_trunc, _ = _truncate_bytes(orig_body, max(1024, MAX_PACK_BYTES // 3))
    name = f"{slice_id}-fix-r{next_round}.pack.md"
    path = PACKS / name
    body = f"""---
slice_id: {slice_id}
kind: fix-pack
round: {next_round}
role: impl
original_pack: {Path(original_pack_name).name}
summary_out: $DSH_HOME/supervisor/thin-state/summaries/{slice_id}-impl-r{next_round}.md
max_bytes_hint: {MAX_PACK_BYTES}
---

# Fix pack · {slice_id} · round {next_round} (gate findings first)

{_gate_findings_section(gate_findings, gate_summary_text)}
{_FIX_PACK_DONE_WHEN}
## Original pack (appendix, abridged)
{orig_trunc}
"""
    _write_pack_capped(path, body)
    return name


def build_reply_addendum_pack(
    *,
    slice_id: str,
    round_n: int,
    prior_pack_name: str,
    answer: str,
    gate_findings: list | None = None,
) -> str:
    """Write packs/<slice>-reply-r<N>.pack.md for a continuing impl ticket.

    When ``gate_findings`` is non-empty the gate findings are the primary task
    and the supervisor answer is only an appendix (Change ①). Without findings
    the supervisor answer drives continued implementation; the prior pack is
    always an appendix, never the primary task.
    """
    prior = PACKS / Path(prior_pack_name).name
    prior_body = prior.read_text(encoding="utf-8") if prior.is_file() else ""
    prior_trunc, _ = _truncate_bytes(prior_body, max(1024, MAX_PACK_BYTES // 3))
    findings = [f for f in (gate_findings or []) if isinstance(f, dict)]
    name = f"{slice_id}-reply-r{round_n}.pack.md"
    path = PACKS / name
    if findings:
        body = f"""---
slice_id: {slice_id}
kind: reply-addendum
round: {round_n}
role: impl
prior_pack: {Path(prior_pack_name).name}
has_gate_findings: true
summary_out: $DSH_HOME/supervisor/thin-state/summaries/{slice_id}-impl-r{round_n}.md
max_bytes_hint: {MAX_PACK_BYTES}
---

# Gate findings + supervisor reply · {slice_id} · round {round_n}

{_gate_findings_section(findings)}
{_FIX_PACK_DONE_WHEN}
## Supervisor answer (appendix)
{answer.strip()}

## Prior pack (appendix, abridged)
{prior_trunc}
"""
    else:
        body = f"""---
slice_id: {slice_id}
kind: reply-addendum
round: {round_n}
role: impl
prior_pack: {Path(prior_pack_name).name}
has_gate_findings: false
summary_out: $DSH_HOME/supervisor/thin-state/summaries/{slice_id}-impl-r{round_n}.md
max_bytes_hint: {MAX_PACK_BYTES}
---

# Supervisor reply addendum · {slice_id} · round {round_n}

## Supervisor answer
{answer.strip()}

## Done when
- Incorporate the supervisor answer; continue implementation.
- Write the foreman structured summary machine block.
- No room_*.

## Prior pack (appendix, abridged)
{prior_trunc}
"""
    _write_pack_capped(path, body)
    return name


def _peak_prompt(csv_path):
    try:
        import csv as _csv
        if not csv_path or not Path(csv_path).is_file():
            return (None, 0)
        vals = [int(float(r.get("usage_prompt") or 0)) for r in _csv.DictReader(open(csv_path))]
        return (max(vals), len(vals)) if vals else (None, 0)
    except Exception:
        return (None, 0)


def maybe_notify_hub(
    job,
    out_body,
    result,
    *,
    kind: str = "dsh-trial-complete",
    extra_summary: str = "",
    payload_extra: dict | None = None,
):
    ticket = result.get("ticket") or job.get("ticket") or job.get("slice") or job.get("id")
    peak, steps = _peak_prompt(result.get("composition") or "")
    summary = extra_summary or (
        f"trial-broker {out_body.get('status')} ticket={ticket} profile={job.get('profile')} "
        f"exit={out_body.get('exit_code')} assert_clean={result.get('assert_clean')} "
        f"peak_prompt={peak} steps={steps} summary={out_body.get('summary_path')}"
    )
    prefix = str(job.get("notify_prefix") or "").strip()
    if prefix:
        summary = f"{prefix}{summary}"
    cmd = [
        sys.executable, str(NOTIFY),
        "--ticket", str(ticket),
        "--status", "completed" if out_body.get("status") in ("ok", "PASS", "pass") else (
            "failed" if out_body.get("status") in ("failed", "escalated") else str(out_body.get("status") or "completed")
        ),
        "--summary", summary,
        "--source", "dsh",
        "--kind", kind,
    ]
    if job.get("goal") or job.get("slice"):
        cmd += ["--goal", str(job.get("goal") or job.get("slice"))]
    if str(job.get("notify_dry_run") or "").lower() in ("true", "1", "yes"):
        cmd.append("--dry-run")
    # Prefer rich payload for chain via --json-body when kind is chain
    if kind == "dsh-trial-chain":
        payload = {
            "source": "dsh",
            "kind": kind,
            "status": out_body.get("status"),
            "slice": job.get("slice") or out_body.get("slice"),
            "state": out_body.get("chain_state") or out_body.get("status"),
            "rounds": out_body.get("rounds_completed"),
            "last_verdict": out_body.get("last_verdict"),
            "summary_paths": out_body.get("summary_paths"),
            "chain_path": out_body.get("chain_path"),
            "ticket": str(ticket),
            "host": "box",
            "at": _iso(),
            "summary": summary,
        }
        payload = {k: v for k, v in payload.items() if v is not None}
        cmd = [
            sys.executable, str(NOTIFY),
            "--json-body", json.dumps(payload, ensure_ascii=False),
            "--source", "dsh",
            "--kind", kind,
        ]
        if str(job.get("notify_dry_run") or "").lower() in ("true", "1", "yes"):
            cmd.append("--dry-run")
    elif payload_extra:
        # Goal-terminal events: structured goal/status/reason/suggested_action.
        payload = {
            "source": "dsh",
            "kind": kind,
            "status": out_body.get("status"),
            "goal": job.get("goal") or out_body.get("goal"),
            "ticket": str(ticket),
            "summary": summary,
            "host": "box",
            "at": _iso(),
        }
        payload.update(payload_extra)
        payload = {k: v for k, v in payload.items() if v is not None}
        cmd = [
            sys.executable, str(NOTIFY),
            "--json-body", json.dumps(payload, ensure_ascii=False),
            "--source", "dsh",
            "--kind", kind,
        ]
        if str(job.get("notify_dry_run") or "").lower() in ("true", "1", "yes"):
            cmd.append("--dry-run")
    try:
        cp = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        return {
            "sent": cp.returncode == 0,
            "rc": cp.returncode,
            "detail": (cp.stderr or cp.stdout or "")[-500:],
            "kind": kind,
        }
    except (OSError, subprocess.SubprocessError) as e:
        return {"sent": False, "error": str(e), "kind": kind}


def _parse_foreman_summary(summary_path: Path) -> dict:
    text = summary_path.read_text(encoding="utf-8") if summary_path.is_file() else ""
    block = _extract_fenced_json(text) or {}
    status = str(block.get("status") or "").strip().lower()
    if status not in (FOREMAN_STATUS_DONE, FOREMAN_STATUS_BLOCKED, FOREMAN_STATUS_QUESTION):
        # heuristic fallback
        low = text.lower()
        if "blocked" in low:
            status = FOREMAN_STATUS_BLOCKED
        elif "question" in low:
            status = FOREMAN_STATUS_QUESTION
        elif summary_path.is_file():
            status = FOREMAN_STATUS_DONE
        else:
            status = FOREMAN_STATUS_BLOCKED
        block["status"] = status
    else:
        block["status"] = status
    return {"block": block, "text": text}


def _parse_gate_verdict(summary_path: Path) -> dict:
    text = summary_path.read_text(encoding="utf-8") if summary_path.is_file() else ""
    block = _extract_fenced_json(text) or {}
    verdict = str(block.get("verdict") or "").strip().upper()
    if verdict not in (VERDICT_PASS, VERDICT_HOLD):
        low = text.upper()
        if "PASS" in low and "HOLD" not in low.split("PASS", 1)[0][-20:]:
            # crude
            if re.search(r"\bHOLD\b", text, re.I):
                verdict = VERDICT_HOLD
            elif re.search(r"\bPASS\b", text, re.I):
                verdict = VERDICT_PASS
            else:
                verdict = VERDICT_HOLD
        elif re.search(r"\bHOLD\b", text, re.I):
            verdict = VERDICT_HOLD
        elif re.search(r"\bPASS\b", text, re.I):
            verdict = VERDICT_PASS
        else:
            verdict = VERDICT_HOLD
        block["verdict"] = verdict
    else:
        block["verdict"] = verdict
    if "findings" not in block or not isinstance(block.get("findings"), list):
        block["findings"] = block.get("findings") if isinstance(block.get("findings"), list) else []
    return {"block": block, "text": text}



def enforce_gate_verdict(block: dict) -> dict:
    """Hard policy: any P0/P1 or unmet_acceptance ⇒ HOLD; P2-only may PASS.

    Overrides a machine-block that claims PASS while listing blocking findings.
    """
    block = dict(block or {})
    findings = block.get("findings") if isinstance(block.get("findings"), list) else []
    unmet = block.get("unmet_acceptance") if isinstance(block.get("unmet_acceptance"), list) else []
    # also accept string unmet
    if isinstance(block.get("unmet_acceptance"), str) and block.get("unmet_acceptance").strip():
        unmet = [block["unmet_acceptance"].strip()]
    blocking = []
    for f in findings:
        if not isinstance(f, dict):
            continue
        tier = str(f.get("tier") or "").strip().upper()
        if tier in ("P0", "P1"):
            blocking.append(f)
    forced = False
    reason = []
    if blocking:
        forced = True
        reason.append(f"P0/P1 findings={len(blocking)}")
    if unmet:
        forced = True
        reason.append(f"unmet_acceptance={len(unmet)}")
    verdict = str(block.get("verdict") or "").strip().upper()
    if forced:
        if verdict != VERDICT_HOLD:
            block["verdict_original"] = verdict or None
            block["verdict_overridden"] = True
            block["verdict_override_reason"] = "; ".join(reason)
        block["verdict"] = VERDICT_HOLD
    else:
        # no blockers: keep PASS/HOLD as declared; default PASS if empty findings
        if verdict not in (VERDICT_PASS, VERDICT_HOLD):
            block["verdict"] = VERDICT_PASS
        else:
            block["verdict"] = verdict
    block["findings"] = findings
    block["unmet_acceptance"] = unmet
    return block


def maybe_offload_gc(*, force: bool = False) -> dict | None:
    """Run offload GC at most once per hour (or force after a job)."""
    global _LAST_OFFLOAD_GC_TS
    now = time.time()
    if not force and (now - _LAST_OFFLOAD_GC_TS) < 3600:
        return None
    if not OFFLOAD_GC.is_file():
        return {"skipped": True, "reason": "missing gc script"}
    try:
        cp = subprocess.run(
            [sys.executable, str(OFFLOAD_GC), "--days", os.environ.get("DSH_OFFLOAD_GC_DAYS", "3")],
            capture_output=True,
            text=True,
            timeout=60,
        )
        _LAST_OFFLOAD_GC_TS = now
        return {
            "rc": cp.returncode,
            "detail": (cp.stdout or cp.stderr or "")[-400:],
            "at": _iso(),
        }
    except (OSError, subprocess.SubprocessError) as e:
        return {"error": str(e)}


def _terminal_outbox(job: dict, chain: dict, dest: Path, *, ok: bool, notify: bool | None = None) -> Path:
    rounds = chain.get("rounds") or []
    summary_paths = []
    for rd in rounds:
        for k in ("impl_summary", "gate_summary"):
            if rd.get(k):
                summary_paths.append(rd[k])
    out_body = {
        "status": chain.get("state"),
        "chain_state": chain.get("state"),
        "slice": chain.get("slice"),
        "job": {k: job.get(k) for k in job if k != "answer"},
        "rounds_completed": len(rounds),
        "last_verdict": chain.get("last_verdict"),
        "summary_paths": summary_paths,
        "chain_path": str(CHAINS / f"{chain['slice']}.json"),
        "finished_at": _iso(),
        "rooms": False,
        "prod_broker_touched": False,
        "tickets": [
            {
                "ticket": rd.get("impl_ticket"),
                "role": "impl",
                "exit": rd.get("impl_exit"),
                "assert_clean": rd.get("impl_assert_clean"),
                "peak_usage_prompt": rd.get("impl_peak"),
                "steps": rd.get("impl_steps"),
            }
            for rd in rounds
        ] + [
            {
                "ticket": rd.get("gate_ticket"),
                "role": "gate",
                "exit": rd.get("gate_exit"),
                "assert_clean": rd.get("gate_assert_clean"),
                "peak_usage_prompt": rd.get("gate_peak"),
                "steps": rd.get("gate_steps"),
            }
            for rd in rounds if rd.get("gate_ticket")
        ],
    }
    # Decision D: single chain never notifies Hub. Only Goal complete / escalate / fail.
    do_notify = False if notify is None else bool(notify)
    out_body["notify_skipped_reason"] = None if do_notify else "chain_no_hub_notify"
    if do_notify:
        last_art = {}
        for rd in reversed(rounds):
            if rd.get("gate_artifacts"):
                last_art = rd["gate_artifacts"]
                break
            if rd.get("impl_artifacts"):
                last_art = rd["impl_artifacts"]
                break
        summary = (
            f"dsh-trial-chain slice={chain.get('slice')} state={chain.get('state')} "
            f"rounds={len(rounds)} last_verdict={chain.get('last_verdict')} "
            f"chain={out_body['chain_path']}"
        )
        prefix = str(job.get("notify_prefix") or "").strip()
        if prefix:
            summary = f"{prefix}{summary}"
        out_body["notify"] = maybe_notify_hub(
            job, out_body, last_art or {"ticket": f"chain-{chain.get('slice')}"},
            kind="dsh-trial-goal",
            extra_summary=summary,
        )
    out_name = f"{_stamp()}-chain-{chain['slice']}.json"
    out_path = OUTBOX / out_name
    out_path.write_text(json.dumps(out_body, ensure_ascii=False, indent=2) + "\n")
    maybe_offload_gc(force=True)
    target_dir = OUTBOX if ok else FAILED
    if dest.exists():
        shutil.move(str(dest), str(target_dir / dest.name))
    return out_path



def _read_goal(goal_id: str) -> dict | None:
    path = GOALS / f"{goal_id}.json"
    if not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _write_goal(state: dict) -> Path:
    GOALS.mkdir(parents=True, exist_ok=True)
    goal_id = state["goal"]
    path = GOALS / f"{goal_id}.json"
    state["updated_at"] = _iso()
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n")
    return path



def _infer_slice_from_ticket(ticket: str) -> str | None:
    """Pull a real slice id out of impl/gate/supervisor ticket names.

    Patterns (group 1 = slice):
      impl-trial-{slice}-r{N}
      impl-{slice}-r{N}
      gate-trial-{slice}-rev{N}
      gate-{slice}-rev{N} / gate-trial-{slice}-r{N}
      supervisor-answer-{slice}-ask{N} / -r{N}
    The broken default literal ``slice`` is rejected so callers fall through.
    """
    t = str(ticket or "").strip()
    if not t:
        return None
    patterns = (
        r"^impl-trial-(.+)-r\d+$",
        r"^impl-(.+)-r\d+$",
        r"^gate-trial-(.+)-rev\d+$",
        r"^gate-trial-(.+)-r\d+$",
        r"^supervisor-answer-(.+)-ask\d+$",
        r"^supervisor-answer-(.+)-r\d+$",
    )
    for pat in patterns:
        m = re.match(pat, t)
        if not m:
            continue
        got = m.group(1).strip()
        if got and got != "slice":
            return got
    return None


def _resolve_ask_slice_id(
    ask: dict, goal: dict | None, *, fallback: str = "slice"
) -> str:
    """Resolve the chain slice for a mailbox ask.

    Prefer explicit ask/from/goal.current_slice. When the agent omits slice
    (common on submit_for_review), infer from ``from.ticket`` /
    ``ask.ticket`` (``impl-trial-{slice}-rN``). Never keep the placeholder
    literal ``slice`` when a real id is available — that placeholder made
    gate tickets ``gate-trial-slice-revN`` and broke salvage matching.
    """
    frm = ask.get("from") if isinstance(ask.get("from"), dict) else {}
    candidates = [
        ask.get("slice"),
        frm.get("slice"),
        (goal or {}).get("current_slice") if isinstance(goal, dict) else None,
    ]
    for raw in candidates:
        s = str(raw or "").strip()
        if s and s != "slice":
            return s
    for ticket in (ask.get("ticket"), frm.get("ticket")):
        inferred = _infer_slice_from_ticket(str(ticket or ""))
        if inferred:
            return inferred
    for raw in candidates:
        s = str(raw or "").strip()
        if s:
            return s
    fb = str(fallback or "").strip()
    return fb or "slice"


def _misplaced_summary_roots() -> list[Path]:
    """Dirs where agents have historically written summaries by mistake.

    The recurring shape is ``~/.dsh-supervisor/thin-state/summaries/`` —
    the model glues ``.dsh`` + ``supervisor`` and drops the ``/supervisor/``
    segment from the canonical ``~/.dsh/supervisor/thin-state/summaries/``.
    Canonical SUMMARIES (under DSH_HOME) remains the single source of truth.
    """
    home = Path.home()
    return [
        home / ".dsh-supervisor" / "thin-state" / "summaries",
    ]


def _adopt_misplaced_summary(expected: Path) -> bool:
    """Move a same-basename summary from a known wrong dir into ``expected``.

    Returns True when ``expected`` exists after this call. No-op when it
    already exists. Does not invent content — only relocates a real file.
    """
    if expected.is_file():
        return True
    name = expected.name
    if not name:
        return False
    for root in _misplaced_summary_roots():
        cand = root / name
        if not cand.is_file():
            continue
        try:
            expected.parent.mkdir(parents=True, exist_ok=True)
            try:
                cand.replace(expected)
            except OSError:
                expected.write_bytes(cand.read_bytes())
                try:
                    cand.unlink()
                except OSError:
                    pass
            print(
                f"[trial-broker] adopted misplaced summary {cand} → {expected}",
                flush=True,
            )
            return expected.is_file()
        except OSError as e:
            print(f"[trial-broker] adopt summary failed from {cand}: {e}", flush=True)
    return expected.is_file()


def _review_pass_via_ask_archive(slice_id: str) -> tuple[bool, str]:
    """Recover submit_for_review PASS when metrics recorded slice as ``slice``.

    Ties the gate answer back to this chain via the archived pending ask's
    ``from.ticket`` (``impl-trial-{slice}-rN``), which still embeds the real
    slice id even when ask.slice was null.
    """
    slice_id = str(slice_id or "").strip()
    if not slice_id:
        return (False, "")
    arch = MAILBOX / "archive"
    answers = MAILBOX / "answers"
    if not arch.is_dir() or not answers.is_dir():
        return (False, "")
    try:
        pendings = sorted(arch.glob("submit-for-review-*.pending.json"), reverse=True)
    except OSError:
        return (False, "")
    for pend in pendings:
        try:
            ask = json.loads(pend.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError):
            continue
        if not isinstance(ask, dict):
            continue
        frm = ask.get("from") if isinstance(ask.get("from"), dict) else {}
        ticket = str(frm.get("ticket") or ask.get("ticket") or "")
        # Real impl tickets embed the full slice id.
        if slice_id not in ticket:
            continue
        ask_id = str(ask.get("ask_id") or "").strip()
        if not ask_id:
            # pending name: submit-for-review-….pending.json
            ask_id = pend.name[: -len(".pending.json")] if pend.name.endswith(".pending.json") else pend.stem
        ans_path = answers / f"{ask_id}.json"
        if not ans_path.is_file():
            continue
        try:
            ans = json.loads(ans_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError):
            continue
        if not isinstance(ans, dict):
            continue
        if str(ans.get("verdict") or "").strip().upper() == VERDICT_PASS:
            return (True, f"submit_for_review PASS (ask archive {ask_id})")
    return (False, "")


def _slice_names_same(recorded: str, chain_slice: str, goal_id: str) -> bool:
    """True when a metrics ``row.slice`` names the same slice as the chain.

    Exact equality always matches. The other real shape is a
    ``submit_for_review`` row whose ``slice`` is the short name the agent
    sent (``s1``) while the chain slice is ``{goal}-s1``. That matches only
    when ``chain_slice == f"{goal_id}-{recorded}"`` (or the reverse). A bare
    ``s1`` does not match another goal's ``other-goal-s1``, and a suffix
    check is not used.
    """
    recorded = str(recorded or "").strip()
    chain_slice = str(chain_slice or "").strip()
    if not recorded or not chain_slice:
        return False
    if recorded == chain_slice:
        return True
    goal_id = str(goal_id or "").strip()
    if not goal_id:
        return False
    prefix = goal_id + "-"
    if chain_slice.startswith(prefix) and recorded == chain_slice[len(prefix):]:
        return True
    if recorded.startswith(prefix) and chain_slice == recorded[len(prefix):]:
        return True
    return False


def _slice_has_review_pass(
    goal: dict | None, slice_id: str, *, reload: bool = False
) -> tuple[bool, str]:
    """True when this slice already has a PASS verdict independent of impl summary.

    Covers two real paths:
      * an inline (mid-ticket) ``submit_for_review`` gate that returned PASS —
        recorded as a ``goal["metrics"]["tickets"]`` row with
        ``kind in ("submit_for_review", "gate")`` and ``verdict == "PASS"``.
        ``row.slice`` may be the full chain slice or the short name under
        this goal (``s1`` vs ``{goal}-s1``). Another goal's ``s1`` does not
        match. Disk gate summaries are still looked up by the full slice id
        only — a shared ``s1-gate-rev*.md`` name is not evidence;
      * a gate summary file on disk whose parsed verdict is PASS.

    ``reload=True`` re-reads the goal from disk first, so mailbox-watcher
    metric writes (which happen in another code path) are visible to a
    long-running chain. Reload-friendly: callers may pass either a live goal
    dict or one freshly loaded from ``GOALS``.
    """
    slice_id = str(slice_id or "")
    if not slice_id:
        return (False, "")
    if reload and isinstance(goal, dict) and goal.get("goal"):
        goal = _read_goal(str(goal["goal"])) or goal
    if isinstance(goal, dict):
        rows = ((goal.get("metrics") or {}).get("tickets")) or []
        for row in rows:
            if not isinstance(row, dict):
                continue
            goal_id = str(goal.get("goal") or "")
            if not _slice_names_same(str(row.get("slice") or ""), slice_id, goal_id):
                continue
            kind = str(row.get("kind") or "")
            verdict = str(row.get("verdict") or "").strip().upper()
            if kind == "submit_for_review" and verdict == VERDICT_PASS:
                return (True, f"submit_for_review PASS ({row.get('ticket') or 'inline gate'})")
            if kind == "gate" and verdict == VERDICT_PASS:
                return (True, f"gate PASS ({row.get('ticket') or 'gate'})")
    # Disk gate summaries: {slice}-gate-rev*.md / {slice}-gate-r*.md
    try:
        candidates = sorted(SUMMARIES.glob(f"{slice_id}-gate-rev*.md"))
        candidates += sorted(SUMMARIES.glob(f"{slice_id}-gate-r*.md"))
    except OSError:
        candidates = []
    for cand in candidates:
        try:
            parsed = _parse_gate_verdict(cand)
            block = enforce_gate_verdict(parsed["block"])
        except Exception:  # noqa: BLE001 — unreadable summary is simply no evidence
            continue
        if str(block.get("verdict") or "").strip().upper() == VERDICT_PASS:
            return (True, f"gate summary PASS ({cand.name})")
    # Metrics may have recorded the literal default "slice" when the agent
    # omitted ask.slice. Recover PASS by tying the archived pending ask's
    # from.ticket (impl-trial-{slice}-rN) to this chain slice.
    ok_arch, reason_arch = _review_pass_via_ask_archive(slice_id)
    if ok_arch:
        return (True, reason_arch)
    return (False, "")


def _bump_supervisor_tickets(goal: dict | None, ticket: str) -> dict | None:
    if not goal:
        return None
    tickets = list(goal.get("supervisor_tickets") or [])
    tickets.append({"ticket": ticket, "at": _iso()})
    goal["supervisor_tickets"] = tickets
    goal["supervisor_ticket_count"] = len(tickets)
    _write_goal(goal)
    return goal


def _parse_supervisor_summary(summary_path: Path) -> dict:
    text = summary_path.read_text(encoding="utf-8") if summary_path.is_file() else ""
    block = _extract_fenced_json(text) or {}
    action = str(block.get("action") or "").strip()
    return {"block": block, "text": text, "action": action}


# Chain states that mean the slice already finished. Anything else
# (running, awaiting_supervisor, empty) is still in flight.
_CHAIN_TERMINAL_STATES = frozenset({"PASS", "failed", "escalated", "cancelled"})


def _goal_slice_ids(goal: dict) -> list[str]:
    ids: list[str] = []
    for raw in goal.get("slices") or []:
        if isinstance(raw, str) and raw.strip():
            ids.append(raw.strip())
        elif isinstance(raw, dict):
            sid = str(raw.get("slice") or raw.get("id") or "").strip()
            if sid:
                ids.append(sid)
    return ids


def _close_summary_says_done(goal: dict) -> bool:
    """True when the supervisor close summary already recorded goal_done.

    Reads the file named by ``goal["close_summary"]`` when present, otherwise
    the conventional ``summaries/goal-{id}-close.md``. Only an explicit
    ``action=goal_done`` (or ``goal_status=done``) counts. Missing summary,
    missing file, or a close that does not say done is not evidence of PASS.
    """
    path_s = str(goal.get("close_summary") or "").strip()
    path = Path(path_s) if path_s else None
    if path is None or not path.is_file():
        gid = str(goal.get("goal") or "").strip()
        if not gid:
            return False
        path = SUMMARIES / f"goal-{gid}-close.md"
    if not path.is_file():
        return False
    try:
        parsed = _parse_supervisor_summary(path)
    except OSError:
        return False
    block = parsed.get("block") or {}
    action = str(parsed.get("action") or "").strip()
    gstatus = str(block.get("goal_status") or "").strip().lower()
    return action == "goal_done" or gstatus == "done"


def reconcile_running_goal(goal: dict) -> str | None:
    """Fold a terminal chain back onto a Goal still stuck at ``running``.

    Call only at process start. Returns the new status (``done`` or
    ``failed``) when this Goal was reconciled, else ``None``.

    Rules (chain JSON is the source of truth; this does not re-run salvage):

    * status other than ``running`` (cancelled, done, failed, planning, …)
      is left untouched.
    * no slices, or any slice whose chain is missing / not yet terminal
      (``running``, ``awaiting_supervisor``, …) → leave it.
    * every slice chain ``state == PASS``, and the close summary says
      ``action=goal_done`` (or ``goal_status=done``) → ``done``.
    * any slice chain already ``failed`` / ``escalated`` / ``cancelled``,
      including ``error`` like ``foreman exit 1 without summary`` with
      ``last_verdict`` empty and no gate PASS → that chain state.
      A missing summary is **not** turned into PASS here. 1a8fe63 salvage
      stays on the live close path only.
    """
    if not isinstance(goal, dict):
        return None
    if str(goal.get("status") or "") != "running":
        return None
    slice_ids = _goal_slice_ids(goal)
    if not slice_ids:
        return None
    chains: list[dict] = []
    for sid in slice_ids:
        chain = _read_chain(sid)
        if not isinstance(chain, dict):
            return None
        state = str(chain.get("state") or "")
        if state not in _CHAIN_TERMINAL_STATES:
            return None
        chains.append(chain)
    if any(str(c.get("state") or "") != "PASS" for c in chains):
        # First non-PASS terminal state wins; never invent PASS.
        bad = next(c for c in chains if str(c.get("state") or "") != "PASS")
        new_status = str(bad.get("state") or "failed")
        goal["status"] = new_status
        goal["last_chain_state"] = new_status
        err = str(bad.get("error") or "").strip()
        if err:
            goal["error"] = err
        goal["reconciled_at_start"] = True
        goal["reconcile_reason"] = (
            f"chain {bad.get('slice')} already {new_status}"
            + (f" ({err})" if err else "")
        )
        _write_goal(goal)
        print(
            f"[trial-broker] startup reconcile {goal.get('goal')} "
            f"running → {new_status} ({goal['reconcile_reason']})",
            flush=True,
        )
        return new_status
    if not _close_summary_says_done(goal):
        # All chains PASS but close never recorded goal_done. Do not invent
        # done, and do not salvage a missing summary into PASS.
        return None
    goal["status"] = "done"
    goal["last_chain_state"] = "PASS"
    goal["reconciled_at_start"] = True
    goal["reconcile_reason"] = "all slice chains PASS and close action=goal_done"
    _sync_slices_completed(goal)
    _write_goal(goal)
    print(
        f"[trial-broker] startup reconcile {goal.get('goal')} running → done "
        f"({goal['reconcile_reason']})",
        flush=True,
    )
    return "done"


def reconcile_terminal_goals_at_start() -> list[str]:
    """At process start, settle Goals left ``running`` after a terminal chain.

    Does not enqueue, open a slice, or resume. Cancelled goals stay cancelled.
    """
    settled: list[str] = []
    if not GOALS.is_dir():
        return settled
    for path in sorted(GOALS.glob("*.json")):
        try:
            goal = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(goal, dict):
            continue
        if str(goal.get("status") or "") != "running":
            continue
        new_status = reconcile_running_goal(goal)
        if new_status:
            gid = str(goal.get("goal") or path.stem)
            settled.append(gid)
            # Reconcile is a terminal transition too: never leave Hub blind.
            _notify_goal_terminal(gid)
    if settled:
        print(
            f"[trial-broker] startup reconcile settled {len(settled)} goal(s): "
            + ", ".join(settled),
            flush=True,
        )
    return settled


def clear_stale_gate_running_acks_at_start() -> list[str]:
    """At process start, drop interim ``gate_running`` acks orphaned by a crash.

    ``_handle_submit_for_review`` writes ``MAILBOX/answers/<ask_id>.json`` with
    ``status="gate_running"`` before spawning the gate. If this broker dies or
    restarts mid-gate, the orphan gate cannot write the final answer, so the
    interim would keep ``list_pending_asks`` / ``_handle_pending_ask_file``
    skipping the still-present ``pending/<ask_id>.json`` forever.

    We are at startup, so any such interim ack cannot have been written by this
    process: rename it aside so the watcher re-dispatches the pending ask (a
    still-polling client sees the next interim ack with the new gate rev and
    keeps waiting). Final answers (``verdict`` / ``status=done`` / ``answer``)
    and unreadable / non-dict files are left untouched.
    """
    cleared: list[str] = []
    answers = MAILBOX / "answers"
    if not answers.is_dir():
        return cleared
    for path in sorted(answers.glob("*.json")):
        try:
            body = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError):
            continue
        if not isinstance(body, dict):
            continue
        if str(body.get("status") or "") != "gate_running":
            continue
        if body.get("verdict") is not None:
            continue
        ask_id = str(body.get("ask_id") or path.stem)
        ticket = str(body.get("gate_ticket") or "")
        dest = MAILBOX / "archive" / f"{ask_id}.stale-gate-running.json"
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            path.rename(dest)
        except OSError:
            try:
                path.unlink()
            except OSError:
                continue
        cleared.append(ask_id)
        print(
            f"[trial-broker] stale gate_running ack cleared ask={ask_id} "
            f"ticket={ticket}",
            flush=True,
        )
    return cleared


def build_goal_brief_pack(job: dict) -> str:
    goal_id = job["goal"]
    slice_hint = job.get("suggested_slice") or f"{goal_id}-s1"
    pack_hint = job.get("suggested_pack") or f"{slice_hint}.pack.md"
    accept_hint = job.get("acceptance_hint") or []
    if isinstance(accept_hint, str):
        accept_hint = [accept_hint]
    accept_json = json.dumps(accept_hint, ensure_ascii=False, indent=2)
    name = f"goal-{goal_id}-brief.pack.md"
    path = PACKS / name
    body = f"""---
kind: goal-brief
goal: {goal_id}
role: supervisor
suggested_slice: {slice_hint}
suggested_pack: {pack_hint}
max_slices: {job.get('max_slices') or 1}
cwd: {job.get('cwd')}
summary_out: $DSH_HOME/supervisor/thin-state/summaries/goal-{goal_id}-plan.md
---

# Goal brief · {goal_id}

## Brief
{job['brief']}

## Constraints
- Write at most {job.get('max_slices') or 1} slice pack(s) under packs/ (one pack per slice).
- If more than one slice, emit `action=emit_chains` with a `slices` list; a single slice may keep the legacy `emit_chain` fields.
- Leave **one** deliberate ambiguity in exactly one slice pack so foreman must ask (status=question).
- 禁止把「已开 PR / PR URL / dsh-trial-pr 证据 / 未合并 PR」写进 **gate** acceptance；PR 仅在 gate PASS 之后由 impl/`dsh-trial-pr` 或 supervisor-close 核对。
- Do not modify business src yourself.
- acceptance_hint (optional guidance for your acceptance list):
```json
{accept_json}
```

## Goal state file
Create/update `$DSH_HOME/supervisor/thin-state/goals/{goal_id}.json` with status planning→running after emit.
"""
    path.write_text(body, encoding="utf-8")
    _enforce_pack_bytes(path)
    return name


def build_supervisor_ask_pack(
    *,
    slice_id: str,
    round_n: int,
    questions: list,
    foreman_notes: str,
    pending_pack: str,
    goal_id: str | None,
) -> str:
    name = f"{slice_id}-ask-r{round_n}.pack.md"
    path = PACKS / name
    q_json = json.dumps(questions or [], ensure_ascii=False, indent=2)
    body = f"""---
kind: supervisor-ask
slice_id: {slice_id}
round: {round_n}
goal: {goal_id or ''}
pending_impl_pack: {pending_pack}
summary_out: $DSH_HOME/supervisor/thin-state/summaries/{slice_id}-sup-answer-r{round_n}.md
---

# Supervisor ask · {slice_id} · r{round_n}

## Foreman questions
```json
{q_json}
```

## Foreman notes
{(foreman_notes or '')[:2000]}

## Instruction
Answer with a concrete rule the foreman can implement without further clarification.
Machine block must be action=chain_reply with non-empty answer.
"""
    path.write_text(body, encoding="utf-8")
    _enforce_pack_bytes(path)
    return name


def build_supervisor_close_pack(
    *,
    goal_id: str,
    slice_id: str,
    chain: dict,
    slices: list | None = None,
) -> str:
    name = f"goal-{goal_id}-close.pack.md"
    path = PACKS / name
    chain_excerpt = {
        "slice": slice_id,
        "slices": list(slices or [slice_id]),
        "state": chain.get("state"),
        "last_verdict": chain.get("last_verdict"),
        "rounds": len(chain.get("rounds") or []),
        "supervisor_answers": chain.get("last_supervisor_answer"),
        "handoff_summary": chain.get("handoff_summary") or "",
        "closeout": chain.get("closeout") or None,
    }
    body = f"""---
kind: supervisor-close
goal: {goal_id}
slice: {slice_id}
summary_out: $DSH_HOME/supervisor/thin-state/summaries/goal-{goal_id}-close.md
---

# Supervisor close · {goal_id}

## Chain terminal excerpt
```json
{json.dumps(chain_excerpt, ensure_ascii=False, indent=2)}
```

## Instruction
If handoff_summary is set, read that file first. It is the limit-stop handoff
(hard cap or max_steps). Do not re-open the impl slice.
If state/verdict is PASS: set goals/{goal_id}.json status=done and emit action=goal_done.
If state is closeout: do not mark the goal failed and do not emit goal_done.
Otherwise set failed/escalated accordingly.
"""
    path.write_text(body, encoding="utf-8")
    _enforce_pack_bytes(path)
    return name


def run_supervisor_ticket(
    *,
    ticket: str,
    pack_name: str,
    profile: str,
    cwd: str,
    summary_name: str,
    prompt_mode: str,
    goal: dict | None = None,
) -> tuple[int, dict, Path]:
    """Open one supervisor short ticket; return (exit, artifacts, summary_path)."""
    log_path = ARTIFACT_ROOT / f"{ticket}.log"
    ec = run_open_slice(
        ticket=ticket,
        pack_name=pack_name,
        profile=profile,
        cwd=cwd,
        role="supervisor",
        summary_name=summary_name,
        prompt_mode=prompt_mode,
        log_path=log_path,
        goal=goal,
        watch_mailbox=False,
    )
    summary_path = SUMMARIES / f"{summary_name}.md"
    art = write_artifacts(
        {"ticket": ticket, "role": "supervisor", "profile": profile, "cwd": cwd},
        log_path,
        summary_path,
        ec,
        ticket=ticket,
    )
    _bump_supervisor_tickets(goal, ticket)
    return ec, art, summary_path


def auto_supervisor_answer(job: dict, chain: dict, *, round_n: int, block: dict) -> str | None:
    """Wake dsh supervisor to answer foreman question; return answer text or None."""
    goal_id = job.get("goal") or chain.get("goal")
    goal = _read_goal(goal_id) if goal_id else None
    max_sup = int((goal or {}).get("max_supervisor_tickets") or job.get("max_supervisor_tickets") or 8)
    if goal and int(goal.get("supervisor_ticket_count") or 0) >= max_sup:
        print(f"[trial-broker] supervisor ticket cap {max_sup} reached", flush=True)
        return None
    questions = block.get("questions") or []
    if isinstance(questions, str):
        questions = [questions]
    ask_pack = build_supervisor_ask_pack(
        slice_id=chain["slice"],
        round_n=round_n,
        questions=questions,
        foreman_notes=str(block.get("notes") or ""),
        pending_pack=str(chain.get("pending_impl_pack") or chain.get("pack") or ""),
        goal_id=goal_id,
    )
    ticket = f"supervisor-answer-{chain['slice']}-r{round_n}"
    summary_name = f"{chain['slice']}-sup-answer-r{round_n}"
    profile = str(
        job.get("supervisor_profile")
        or (goal or {}).get("supervisor_profile")
        or "acp-lite"
    )
    ec, art, summary_path = run_supervisor_ticket(
        ticket=ticket,
        pack_name=ask_pack,
        profile=profile,
        cwd=str(job.get("cwd") or chain.get("cwd") or "/workspace"),
        summary_name=summary_name,
        prompt_mode="supervisor-answer",
        goal=goal,
    )
    chain.setdefault("supervisor_events", []).append({
        "kind": "answer",
        "ticket": ticket,
        "exit": ec,
        "summary": str(summary_path),
        "assert_clean": art.get("assert_clean"),
        "peak": _peak_prompt(art.get("composition"))[0],
        "steps": _peak_prompt(art.get("composition"))[1],
        "artifacts": art,
    })
    _write_chain(chain)
    parsed = _parse_supervisor_summary(summary_path)
    answer = str(parsed["block"].get("answer") or "").strip()
    if not answer:
        # fallback: whole summary truncated
        answer = (parsed["text"] or "").strip()[:1500]
    if not answer:
        print("[trial-broker] supervisor answer empty", flush=True)
        return None
    print(f"[trial-broker] supervisor answered via {ticket}", flush=True)
    return answer


def _resolve_parent_goal_id(job: dict, chain: dict) -> str | None:
    """Real parent Goal id for a chain job, or None for a standalone chain.

    Chain JSON is the SSOT: Hub may send ``type:chain-reply`` with only
    ``slice`` + ``answer`` (no ``goal``). Falling back to the slice id would
    create a pseudo ``goals/<slice>.json``, run ``supervisor-close-<slice>``
    and skip the parent Goal's Hub notify (engine-obs-llm-empty-groups-v1).
    """
    if not isinstance(job, dict):
        job = {}
    if not isinstance(chain, dict):
        chain = {}
    slice_id = str(chain.get("slice") or job.get("slice") or "")
    for raw in (chain.get("goal"), job.get("goal")):
        cand = str(raw or "").strip()
        if not cand:
            continue
        if cand != slice_id:
            return cand
        # candidate == slice: only a real Goal (run_goal_job wrote ``brief``)
        # may be settled; never invent slice-as-goal.
        existing = _read_goal(cand)
        if isinstance(existing, dict) and "brief" in existing:
            return cand
    return None


def _sync_slices_completed(goal: dict, extra_pass: str | None = None) -> int:
    """Idempotently set ``metrics.slices_completed`` to the PASS-chain count.

    ``extra_pass`` (the slice whose chain-reply just PASSed) counts as PASS
    even if its chain read fails. Never lowers an already higher count, and
    never writes the Goal (callers persist).
    """
    ids: list[str] = []
    for sid in _goal_slice_ids(goal):
        if sid not in ids:
            ids.append(sid)
    extra = str(extra_pass or "").strip()
    n_pass = 0
    for sid in ids:
        if extra and sid == extra:
            n_pass += 1
        elif str((_read_chain(sid) or {}).get("state") or "") == VERDICT_PASS:
            n_pass += 1
    if extra and extra not in ids:
        n_pass += 1
    metrics = goal.get("metrics")
    if not isinstance(metrics, dict):
        metrics = T.empty_metrics()
        goal["metrics"] = metrics
    metrics["slices_completed"] = max(int(metrics.get("slices_completed") or 0), n_pass)
    return int(metrics["slices_completed"])


def maybe_supervisor_close(job: dict, chain: dict) -> None:
    goal_id = _resolve_parent_goal_id(job, chain)
    if not goal_id:
        return
    if not job.get("supervisor_close", True) and not chain.get("from_goal"):
        # still close if goal file exists from goal job
        if not (GOALS / f"{goal_id}.json").is_file():
            return
    goal = _read_goal(goal_id) or {
        "goal": goal_id,
        "status": "running",
        "slices": [chain.get("slice")],
        "supervisor_ticket_count": 0,
        "supervisor_tickets": [],
    }
    close_pack = build_supervisor_close_pack(
        goal_id=goal_id,
        slice_id=chain["slice"],
        chain=chain,
        slices=goal.get("slices"),
    )
    ticket = f"supervisor-close-{goal_id}"
    profile = str(job.get("supervisor_profile") or goal.get("supervisor_profile") or "acp-lite")
    ec, art, summary_path = run_supervisor_ticket(
        ticket=ticket,
        pack_name=close_pack,
        profile=profile,
        cwd=str(job.get("cwd") or chain.get("cwd") or "/workspace"),
        summary_name=f"goal-{goal_id}-close",
        prompt_mode="supervisor-close",
        goal=goal,
    )
    chain.setdefault("supervisor_events", []).append({
        "kind": "close",
        "ticket": ticket,
        "exit": ec,
        "summary": str(summary_path),
        "assert_clean": art.get("assert_clean"),
        "peak": _peak_prompt(art.get("composition"))[0],
        "steps": _peak_prompt(art.get("composition"))[1],
        "artifacts": art,
    })
    _write_chain(chain)
    parsed = _parse_supervisor_summary(summary_path)
    gstatus = str(parsed["block"].get("goal_status") or "").strip() or (
        "done" if chain.get("state") == "PASS" else str(chain.get("state") or "failed")
    )
    # The close ticket (and its metric bump) may have rewritten the parent.
    goal = _read_goal(goal_id) or goal
    goal["status"] = gstatus
    goal["close_summary"] = str(summary_path)
    goal["last_chain_state"] = chain.get("state")
    if str(chain.get("state") or "") == VERDICT_PASS:
        _sync_slices_completed(goal, extra_pass=str(chain.get("slice") or ""))
    _write_goal(goal)
    print(f"[trial-broker] supervisor close {ticket} goal_status={gstatus}", flush=True)
    # The close is a terminal transition when it says done/failed/escalated.
    if str(gstatus).lower() in ("done", "failed", "escalated", "cancelled", "closeout"):
        _notify_goal_terminal(str(goal_id))



# Limit-stop closeout. Hard cap and max_steps share this path. Idle timeout
# is intentionally not a kind: "timeout after Ns idle" must stay a plain fail.
_HARD_CAP_RE = re.compile(r"timeout after \d+s hard cap")
_STEP_LIMIT_RE = re.compile(
    r"(?i)(?:"
    r"(?:impl_)?max_steps\b[^\n]{0,80}\b(?:hit|exceed\w*|reached|limit)\b|"
    r"\b(?:hit|exceed\w*|reached)\b[^\n]{0,80}\b(?:impl_)?max_steps\b|"
    r"\bstep limit\b[^\n]{0,40}\b(?:hit|exceed\w*|reached)\b|"
    r"步数上限"
    r")"
)
_TEST_BACKTICK_RE = re.compile(
    r"`((?:python3? -m )?pytest\b[^`]*|npm test[^`]*|pnpm(?: run)? test[^`]*|yarn test[^`]*)`"
)
_JUNK_DIRS = {
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    ".hypothesis",
    "node_modules",
    ".venv",
    "venv",
    ".smoke",
}
_JUNK_NAMES = {".DS_Store"}
_JUNK_SUFFIXES = {".pyc", ".pyo"}
_LIMIT_KIND_ZH = {"hard_cap": "硬上限", "max_steps": "步数上限"}


def _run_section(log_text: str) -> str:
    """ACP failure text. The prompt echoed above ``=== run ===`` is not a hit."""
    if "=== run ===" in (log_text or ""):
        return log_text.rsplit("=== run ===", 1)[-1]
    return log_text or ""


def limit_stop_kind(log_text: str) -> str | None:
    """``hard_cap`` or ``max_steps`` when the impl log says that limit stopped it.

    Idle timeout is not a limit stop. Prompt text before ``=== run ===`` is
    ignored so the soft step-discipline paragraph cannot trip this.
    """
    section = _run_section(log_text)
    if _HARD_CAP_RE.search(section):
        return "hard_cap"
    if _STEP_LIMIT_RE.search(section):
        return "max_steps"
    return None


def _is_junk_path(path: str) -> bool:
    parts = Path(path).parts
    if any(part in _JUNK_DIRS for part in parts):
        return True
    name = parts[-1] if parts else path
    if name in _JUNK_NAMES:
        return True
    return Path(name).suffix in _JUNK_SUFFIXES


def _parse_porcelain_z(blob: str) -> list[tuple[str, str]]:
    if not blob:
        return []
    parts = blob.split("\0")
    out: list[tuple[str, str]] = []
    i = 0
    while i < len(parts):
        rec = parts[i]
        if not rec:
            i += 1
            continue
        if len(rec) < 4:
            i += 1
            continue
        xy = rec[:2]
        path = rec[3:]
        i += 1
        if "R" in xy or "C" in xy:
            if i < len(parts) and parts[i] != "":
                path = parts[i]
                i += 1
        out.append((xy, path))
    return out


def _git_dirty_paths(cwd: Path) -> tuple[list[str], str]:
    """Non-junk uncommitted paths (tracked and untracked). Error string on failure."""
    try:
        r = subprocess.run(
            ["git", "status", "--porcelain=v1", "-uall", "-z"],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as e:
        return [], f"git status failed: {e}"
    if r.returncode != 0:
        err = (r.stderr or r.stdout or "git status failed").strip()
        return [], err[-400:]
    paths: list[str] = []
    for _xy, path in _parse_porcelain_z(r.stdout or ""):
        if not path or _is_junk_path(path):
            continue
        if path not in paths:
            paths.append(path)
    return paths, ""


def _head_branch(cwd: Path) -> str:
    try:
        r = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    if r.returncode != 0:
        return ""
    return (r.stdout or "").strip()


def _branch_token(raw: str) -> str:
    raw = (raw or "").strip().strip("\"'")
    if not raw:
        return ""
    return re.split(r"[\s（(]", raw, maxsplit=1)[0].strip()


def _origin_repo(cwd: Path) -> str:
    try:
        r = subprocess.run(
            ["git", "remote", "get-url", "origin"],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    url = (r.stdout or "").strip()
    if r.returncode != 0 or not url:
        return ""
    m = re.search(r"github\.com[:/]+([^/]+)/([^/\s]+)", url)
    if not m:
        return ""
    repo = f"{m.group(1)}/{m.group(2)}"
    if repo.endswith(".git"):
        repo = repo[: -len(".git")]
    return repo


def _slice_pack_text(pack_name: str) -> str:
    if not pack_name:
        return ""
    path = PACKS / Path(pack_name).name
    try:
        return path.read_text(encoding="utf-8") if path.is_file() else ""
    except OSError:
        return ""


def _allowed_test_argv(argv: list[str]) -> bool:
    if not argv:
        return False
    if any(tok in arg for arg in argv for tok in (";", "|", "&", "$", "`", ">", "<")):
        return False
    head = argv[0]
    if head == "pytest":
        return True
    if head in {"npm", "pnpm", "yarn"} and "test" in argv[1:]:
        return True
    if head in {"python", "python3"} and len(argv) >= 3 and argv[1] == "-m" and argv[2] == "pytest":
        return True
    return False


def _test_commands_from_pack(pack_text: str) -> list[str]:
    """Commands the slice pack already names. Never invent a test file."""
    meta = _parse_frontmatter(pack_text)
    found: list[str] = []
    for key in ("test_cmd", "test_command", "test", "tests"):
        raw = str(meta.get(key) or "").strip()
        if raw and raw.lower() not in {"true", "false", "yes", "no"}:
            found.append(raw)
    for m in _TEST_BACKTICK_RE.finditer(pack_text or ""):
        cmd = m.group(1).strip()
        if cmd and cmd not in found:
            found.append(cmd)
    kept: list[str] = []
    for cmd in found:
        try:
            argv = shlex.split(cmd)
        except ValueError:
            continue
        if _allowed_test_argv(argv) and cmd not in kept:
            kept.append(cmd)
    return kept


def _pack_wants_pr(pack_text: str) -> bool:
    meta = _parse_frontmatter(pack_text)
    flag = str(meta.get("pr") or meta.get("open_pr") or "").strip().lower()
    if flag in {"1", "true", "yes", "required"}:
        return True
    body = pack_text or ""
    if "dsh-trial-pr" in body or "开 PR" in body or "PR 已开" in body:
        return True
    return False


def _pack_pr_repo(pack_text: str, cwd: Path) -> str:
    meta = _parse_frontmatter(pack_text)
    named = str(meta.get("pr_repo") or meta.get("github_repo") or "").strip()
    if named:
        return named
    m = re.search(r"fengrunda/(?:knowledge-hub|memory-as-training)", pack_text or "")
    if m:
        return m.group(0)
    return _origin_repo(cwd)


def _commit_paths(cwd: Path, paths: list[str]) -> tuple[str, str]:
    """Commit ``paths`` only. Does not edit file contents. Returns (sha, error)."""
    if not paths:
        return "", "no paths"
    try:
        added = subprocess.run(
            ["git", "add", "--", *paths],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError) as e:
        return "", f"git add failed: {e}"
    if added.returncode != 0:
        return "", (added.stderr or added.stdout or "git add failed").strip()[-400:]
    msg = (
        "chore: supervisor closeout of work left by a limit stop\n\n"
        "Commit only files already in the worktree. No extra code edits.\n"
    )
    commit_argv = ["commit", "-m", msg]
    prefix = ["git"]
    try:
        email = subprocess.run(
            ["git", "config", "--get", "user.email"],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        email = None
    if email is None or email.returncode != 0 or not (email.stdout or "").strip():
        prefix = [
            "git",
            "-c",
            "user.email=dsh-trial-broker@localhost",
            "-c",
            "user.name=dsh-trial-broker",
        ]
    try:
        committed = subprocess.run(
            [*prefix, *commit_argv],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError) as e:
        return "", f"git commit failed: {e}"
    if committed.returncode != 0:
        return "", (committed.stderr or committed.stdout or "git commit failed").strip()[-400:]
    return _git_rev(cwd), ""


def _run_slice_tests(cwd: Path, commands: list[str]) -> list[dict]:
    results: list[dict] = []
    for cmd in commands:
        argv = shlex.split(cmd)
        if not _allowed_test_argv(argv):
            results.append({"cmd": cmd, "exit": 127, "error": "refused test command"})
            break
        try:
            r = subprocess.run(
                argv, cwd=str(cwd), capture_output=True, text=True, timeout=600
            )
            tail = ((r.stdout or "") + "\n" + (r.stderr or "")).strip()[-800:]
            results.append({"cmd": cmd, "exit": r.returncode, "tail": tail})
        except (OSError, subprocess.SubprocessError) as e:
            results.append({"cmd": cmd, "exit": 127, "error": str(e)})
            break
        if r.returncode != 0:
            break
    return results


def _trial_pr_bin() -> Path:
    env = os.environ.get("DSH_TRIAL_PR", "").strip()
    if env:
        return Path(env)
    for cand in (DSH_HOME / "bin" / "dsh-trial-pr", _REPO_ROOT / "bin" / "dsh-trial-pr"):
        if cand.is_file():
            return cand
    return _REPO_ROOT / "bin" / "dsh-trial-pr"


def _open_slice_pr(*, cwd: Path, repo: str, branch: str, title: str, body: str) -> dict:
    """Open a PR through the existing dsh-trial-pr wrapper. Never pushes main."""
    if branch in {"", "HEAD", "main", "master"}:
        return {"opened": False, "url": "", "error": f"refusing PR from branch {branch or '(empty)'}"}
    if not repo:
        return {"opened": False, "url": "", "error": "no allowlisted repo from pack or origin"}
    pr_bin = _trial_pr_bin()
    if not pr_bin.is_file():
        return {"opened": False, "url": "", "error": f"dsh-trial-pr missing: {pr_bin}"}
    try:
        r = subprocess.run(
            [
                str(pr_bin),
                "push-and-pr",
                "--repo",
                repo,
                "--cwd",
                str(cwd),
                "--branch",
                branch,
                "--title",
                title,
                "--body",
                body,
            ],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=180,
        )
    except (OSError, subprocess.SubprocessError) as e:
        return {"opened": False, "url": "", "error": f"dsh-trial-pr failed: {e}"}
    out = ((r.stdout or "") + "\n" + (r.stderr or "")).strip()
    url = ""
    for line in reversed(out.splitlines()):
        line = line.strip()
        if line.startswith("https://") or line.startswith("http://"):
            url = line
            break
    if r.returncode != 0:
        return {"opened": False, "url": url, "error": out[-500:] or f"dsh-trial-pr exit {r.returncode}"}
    return {"opened": bool(url) or r.returncode == 0, "url": url, "error": ""}


def _handoff_next(kind: str, *, dirty: bool, committed: str, tests: list[dict], pr: dict, blocked: str) -> str:
    label = _LIMIT_KIND_ZH.get(kind, kind)
    if not dirty:
        return (
            f"这是真撞{label}，工作区没有可提交改动。按失败处理，不要收成 PASS，"
            "不要为了捞改动重跑整片。"
        )
    if blocked:
        return (
            f"撞上{label}，工作区有改动但收口停住：{blocked} "
            "不要重跑整片，不要丢掉已有文件。监理在当前分支上接着处理。"
        )
    tests_ok = bool(tests) and all(int(row.get("exit") or 0) == 0 for row in tests)
    if not tests:
        return (
            f"已提交 {committed or '(无)'}。切片 pack 没有写明可执行的测试命令，"
            "所以没有跑测试，也没有开 PR。不要假装整片 PASS，不要重跑整片。"
            "监理补跑 pack 里的测试后再决定 PR。"
        )
    if not tests_ok:
        return (
            f"已提交 {committed or '(无)'} 保留。测试没过，PR 没开，整片不是 PASS。"
            "不要重跑整片，不要丢掉这个提交。监理在该提交上修到测试过。"
        )
    if pr.get("opened"):
        return (
            f"已提交 {committed}，测试已过，PR 已开（{pr.get('url') or '见上'}）。"
            "不要重跑整片，不要改已提交的文件。监理核对交接后收口。"
        )
    if pr.get("error"):
        return (
            f"已提交 {committed} 保留，测试已过，但 PR 没开成：{pr.get('error')} "
            "整片不是 PASS。不要重跑整片。监理用 dsh-trial-pr 补开 PR。"
        )
    return (
        f"已提交 {committed}，测试已过。切片没有要求 PR。不要重跑整片。"
        "监理核对交接后收口，不要把整片标成 gate PASS。"
    )


def write_limit_handoff(
    *,
    goal_id: str,
    slice_id: str,
    ticket: str,
    kind: str,
    round_n: int,
    exit_code: int,
    head_before: str,
    commit: str,
    committed_paths: list[str],
    dirty_before: list[str],
    uncommitted_after: list[str],
    tests: list[dict],
    pr: dict,
    blocked: str,
    branch: str,
) -> Path:
    """Handoff the supervisor already reads: thin-state summaries."""
    SUMMARIES.mkdir(parents=True, exist_ok=True)
    path = SUMMARIES / f"{slice_id}-sup-handoff-r{round_n}.md"
    label = _LIMIT_KIND_ZH.get(kind, kind)
    tests_ran = bool(tests)
    pr_opened = bool(pr.get("opened"))
    next_step = _handoff_next(
        kind,
        dirty=bool(dirty_before),
        committed=commit,
        tests=tests,
        pr=pr,
        blocked=blocked,
    )
    block = {
        "action": "supervisor_handoff",
        "limit": kind,
        "goal": goal_id,
        "slice": slice_id,
        "ticket": ticket,
        "exit": exit_code,
        "summary": False,
        "gate": False,
        "slice_pass": False,
        "branch": branch,
        "head_before": head_before,
        "commit": commit,
        "committed_paths": committed_paths,
        "dirty_before": dirty_before,
        "uncommitted_after": uncommitted_after,
        "tests_ran": tests_ran,
        "tests": [{k: row.get(k) for k in ("cmd", "exit", "error")} for row in tests],
        "pr_required_opened": pr_opened,
        "pr_url": pr.get("url") or "",
        "pr_error": pr.get("error") or "",
        "blocked": blocked,
        "next": next_step,
    }
    dirty_txt = "\n".join(f"- `{p}`" for p in dirty_before) or "- （没有可提交改动）"
    left_txt = "\n".join(f"- `{p}`" for p in uncommitted_after) or "- （没有）"
    committed_txt = "\n".join(f"- `{p}`" for p in committed_paths) or "- （这次没有新提交）"
    if tests_ran:
        test_txt = "\n".join(
            f"- `{row.get('cmd')}` exit={row.get('exit')}"
            + (f" error={row.get('error')}" if row.get("error") else "")
            for row in tests
        )
    else:
        test_txt = "- 没跑"
    if pr_opened:
        pr_txt = f"已开 {pr.get('url') or '(wrapper exit 0, url not parsed)'}"
    elif pr.get("error"):
        pr_txt = f"没开。原因：{pr.get('error')}"
    elif not dirty_before or blocked or not tests_ran or any(int(r.get("exit") or 1) != 0 for r in tests):
        pr_txt = "没开。"
    else:
        pr_txt = "没开。切片没有要求 PR。"
    body = f"""---
kind: supervisor-handoff
goal: {goal_id}
slice: {slice_id}
ticket: {ticket}
limit: {kind}
round: {round_n}
---

# 监理交接 · {slice_id}

不要重跑整片 impl。下面是撞上限时工作区的收口结果。

## 哪一票
- Goal: `{goal_id or "(none)"}`
- 切片: `{slice_id}`
- 票: `{ticket}`
- 撞上: {label}（`{kind}`）
- impl exit: {exit_code}
- impl summary: 无
- gate: 无
- 分支: `{branch or "(unknown)"}`
- 撞上限前 HEAD: `{head_before or "(none)"}`

## 已有提交
- 收口提交: `{commit or "（没有新提交）"}`
{committed_txt}

## 撞上限时工作区里的未提交文件
{dirty_txt}

## 收口后还剩的未提交文件
{left_txt}

## 测试
{test_txt}

## PR
{pr_txt}

## 监理接下来
{next_step}

```json
{json.dumps(block, ensure_ascii=False, indent=2)}
```
"""
    path.write_text(body, encoding="utf-8")
    return path


def limit_stop_closeout(
    *,
    job: dict,
    chain: dict,
    cwd: Path,
    slice_id: str,
    round_n: int,
    ticket: str,
    exit_code: int,
    log_path: Path,
    head_before: str,
) -> dict | None:
    """Shared closeout for a hard cap or a step limit.

    Returns None when this exit is not a limit stop (ordinary failure, or an
    idle timeout). Otherwise returns ``action=closeout`` when there was
    something to commit, or ``action=fail`` when the worktree had no
    committable change. Both write a supervisor handoff. Never opens another
    impl ticket and never checks out a different branch.
    """
    try:
        log_text = log_path.read_text(encoding="utf-8", errors="replace") if log_path.is_file() else ""
    except OSError:
        log_text = ""
    kind = limit_stop_kind(log_text)
    if not kind:
        return None
    if not cwd.is_dir():
        return {
            "action": "fail",
            "kind": kind,
            "error": f"foreman exit {exit_code} without summary; {kind}; cwd missing",
            "handoff_summary": "",
        }
    dirty, status_err = _git_dirty_paths(cwd)
    pack_text = _slice_pack_text(str(job.get("pack") or chain.get("pack") or ""))
    branch = _head_branch(cwd)
    wanted = _branch_token(str(_parse_frontmatter(pack_text).get("branch") or ""))
    goal_id = str(job.get("goal") or chain.get("goal") or "")
    blocked = ""
    commit = ""
    committed_paths: list[str] = []
    tests: list[dict] = []
    pr: dict = {"opened": False, "url": "", "error": ""}
    if status_err and not dirty:
        blocked = status_err
    elif not dirty:
        blocked = ""
    else:
        if branch in {"main", "master", "HEAD", ""}:
            blocked = f"refusing to commit on {branch or 'detached HEAD'}"
        elif wanted and branch != wanted:
            blocked = (
                f"refusing checkout: HEAD is {branch}, slice pack branch is {wanted}"
            )
        else:
            commit, commit_err = _commit_paths(cwd, dirty)
            if commit_err:
                blocked = commit_err
            else:
                committed_paths = list(dirty)
                commands = _test_commands_from_pack(pack_text)
                tests = _run_slice_tests(cwd, commands)
                tests_ok = bool(tests) and all(int(row.get("exit") or 0) == 0 for row in tests)
                if tests_ok and _pack_wants_pr(pack_text):
                    repo = _pack_pr_repo(pack_text, cwd)
                    pr = _open_slice_pr(
                        cwd=cwd,
                        repo=repo,
                        branch=branch,
                        title=f"closeout: {slice_id} after {kind}",
                        body=(
                            f"Supervisor closeout after {kind}. "
                            f"Commit {commit}. Slice tests passed. "
                            "Not a gate PASS. Do not merge from this ticket."
                        ),
                    )
                    if not pr.get("opened"):
                        blocked = pr.get("error") or "PR was not opened"
    uncommitted, _ = _git_dirty_paths(cwd)
    # A commit is kept even when tests or PR fail. ``blocked`` is only the
    # reason we refused to commit (protected branch, checkout mismatch).
    handoff_blocked = "" if commit else blocked
    handoff = write_limit_handoff(
        goal_id=goal_id,
        slice_id=slice_id,
        ticket=ticket,
        kind=kind,
        round_n=round_n,
        exit_code=exit_code,
        head_before=head_before,
        commit=commit,
        committed_paths=committed_paths,
        dirty_before=dirty,
        uncommitted_after=uncommitted,
        tests=tests,
        pr=pr,
        blocked=handoff_blocked,
        branch=branch,
    )
    record = {
        "kind": kind,
        "commit": commit,
        "committed_paths": committed_paths,
        "dirty_before": dirty,
        "uncommitted_after": uncommitted,
        "tests": tests,
        "pr_url": pr.get("url") or "",
        "pr_opened": bool(pr.get("opened")),
        "pr_error": pr.get("error") or "",
        "blocked": "" if commit else blocked,
        "handoff_summary": str(handoff),
        "slice_pass": False,
    }
    if dirty and (commit or blocked):
        note = _handoff_next(
            kind,
            dirty=True,
            committed=commit,
            tests=tests,
            pr=pr,
            blocked="" if commit else blocked,
        )
        return {"action": "closeout", "error": "", "note": note, **record}
    err = f"foreman exit {exit_code} without summary; {kind}; no committable changes"
    if blocked and not dirty:
        err = f"foreman exit {exit_code} without summary; {kind}; {blocked}"
    return {"action": "fail", "error": err, "note": "", **record}


def run_chain_rounds(job: dict, chain: dict, dest: Path, *, start_round: int, start_pack: str) -> int:
    """Run foreman→gate from start_round using start_pack for first foreman."""
    cwd = Path(job["cwd"])
    slice_id = job["slice"]
    max_rounds = int(job["max_rounds"])
    pack_for_impl = start_pack
    original_pack = job["pack"] if job.get("pack") else chain.get("pack")

    # Goal-scoped chain: load goal so limits + mailbox watcher are goal-aware.
    goal_id = job.get("goal") or chain.get("goal")
    goal_obj = _read_goal(str(goal_id)) if goal_id else None

    for round_n in range(start_round, max_rounds + 1):
        impl_ticket = f"impl-trial-{slice_id}-r{round_n}"
        impl_summary_name = f"{slice_id}-impl-r{round_n}"
        impl_summary = SUMMARIES / f"{impl_summary_name}.md"
        impl_log = ARTIFACT_ROOT / f"{impl_ticket}.log"

        # Capture base sha before foreman (best-effort)
        base_before = _git_rev(cwd)

        # Publish current slice onto the goal so mailbox submit_for_review /
        # ask_supervisor resolve the real id when the agent omits slice.
        if goal_obj is not None:
            goal_obj["current_slice"] = slice_id
            goal_obj["current_pack"] = Path(pack_for_impl).name
            goal_obj["current_acceptance"] = list(
                job.get("acceptance") or chain.get("acceptance") or []
            )
            _write_goal(goal_obj)

        ec = run_open_slice(
            ticket=impl_ticket,
            pack_name=Path(pack_for_impl).name,
            profile=job["profile"],
            cwd=str(cwd),
            role="impl",
            summary_name=impl_summary_name,
            prompt_mode="foreman",
            log_path=impl_log,
            goal=goal_obj,
            watch_mailbox=True,
        )
        # Agents sometimes write the summary under ~/.dsh-supervisor/...
        # (glued path). Adopt into canonical SUMMARIES before missing-check.
        _adopt_misplaced_summary(impl_summary)
        impl_art = write_artifacts(
            {**job, "ticket": impl_ticket, "role": "impl"},
            impl_log, impl_summary, ec, ticket=impl_ticket,
        )
        peak, steps = _peak_prompt(impl_art.get("composition"))
        if goal_obj is not None:
            # chain 路径记账（prompt_token_total 默认 peak×steps）→ dsh-trial report 非 0
            T.record_ticket_metric(goal_obj, {
                "role": "impl",
                "ticket": impl_ticket,
                "slice": slice_id,
                "exit": ec,
                "peak_prompt": peak,
                "steps": steps,
                "tool_res": _tool_res_from_art(impl_art),
                "assert_clean": impl_art.get("assert_clean"),
                "kind": "impl",
            })
            _write_goal(goal_obj)
        parsed = _parse_foreman_summary(impl_summary)
        block = parsed["block"]
        # Fill base if agent omitted
        if not block.get("base"):
            block["base"] = base_before
        if not block.get("commit"):
            block["commit"] = _git_rev(cwd)

        round_rec = {
            "round": round_n,
            "impl_ticket": impl_ticket,
            "impl_pack": Path(pack_for_impl).name,
            "impl_exit": ec,
            "impl_summary": str(impl_summary),
            "impl_assert_clean": impl_art.get("assert_clean"),
            "impl_peak": peak,
            "impl_steps": steps,
            "impl_artifacts": impl_art,
            "foreman_status": block.get("status"),
            "foreman_block": block,
        }
        chain["rounds"].append(round_rec)
        _write_chain(chain)

        if not impl_summary.is_file():
            # Salvage: a mid-ticket submit_for_review gate may already have
            # PASSed this slice, and the foreman then left no disk impl
            # summary — non-zero exit, or exit 0 with the file written off
            # the summaries dir. A missing file is not status=blocked (the
            # parser default) and must not sit in awaiting_supervisor.
            # Closing PASS is allowed only when this slice already has gate
            # PASS evidence (Menu-4 r3, and the exit-0 wrong-path case).
            # No prior PASS still fails. A real HOLD is on the gate path.
            # Re-read the goal so mailbox-watcher PASS metrics are visible.
            salvage_goal = goal_obj
            if goal_id:
                salvage_goal = _read_goal(str(goal_id)) or goal_obj
            salvaged, salvage_reason = _slice_has_review_pass(
                salvage_goal, slice_id, reload=True
            )
            if salvaged:
                block["status"] = FOREMAN_STATUS_DONE
                round_rec["foreman_status"] = FOREMAN_STATUS_DONE
                round_rec["foreman_block"] = block
                round_rec["salvaged_missing_summary"] = True
                round_rec["salvage_reason"] = salvage_reason
                chain["rounds"][-1] = round_rec
                # Synthetic summary so later tools still see status=done.
                try:
                    impl_summary.parent.mkdir(parents=True, exist_ok=True)
                    impl_summary.write_text(
                        f"# {impl_summary_name} (broker salvage)\n\n"
                        f"<!-- broker salvage: foreman exit {ec} without summary, "
                        f"but slice already had {salvage_reason} -->\n\n"
                        "```json\n"
                        + json.dumps(block, ensure_ascii=False, indent=2)
                        + "\n```\n",
                        encoding="utf-8",
                    )
                except OSError:
                    pass
                chain["state"] = VERDICT_PASS
                chain["salvage_note"] = (
                    f"salvaged: missing impl summary after gate PASS ({salvage_reason})"
                )
                chain["error"] = ""
                chain["last_verdict"] = VERDICT_PASS
                _write_chain(chain)
                print(
                    f"[trial-broker] chain {slice_id} SALVAGE PASS r{round_n} "
                    f"(foreman exit {ec}, no summary; {salvage_reason})",
                    flush=True,
                )
                if (job.get("from_goal") or job.get("goal")) and not job.get(
                    "defer_supervisor_close"
                ):
                    try:
                        maybe_supervisor_close(job, chain)
                    except Exception as e:  # noqa: BLE001 — close best-effort
                        print(f"[trial-broker] supervisor close error: {e}", flush=True)
                out = _terminal_outbox(job, chain, dest, ok=True)
                print(
                    f"[trial-broker] chain {slice_id} PASS (salvaged) outbox={out.name}",
                    flush=True,
                )
                return 0
            # Hard cap and max_steps share one closeout. Idle timeout and a
            # plain non-zero exit still fail below. No second impl ticket.
            closed = None
            if ec != 0:
                closed = limit_stop_closeout(
                    job=job,
                    chain=chain,
                    cwd=cwd,
                    slice_id=slice_id,
                    round_n=round_n,
                    ticket=impl_ticket,
                    exit_code=ec,
                    log_path=impl_log,
                    head_before=base_before,
                )
            if closed and closed.get("action") == "closeout":
                round_rec["limit_closeout"] = {
                    "kind": closed.get("kind"),
                    "commit": closed.get("commit"),
                    "handoff_summary": closed.get("handoff_summary"),
                }
                chain["rounds"][-1] = round_rec
                chain["state"] = "closeout"
                chain["error"] = ""
                chain["last_verdict"] = None
                chain["closeout"] = {
                    k: closed.get(k)
                    for k in (
                        "kind",
                        "commit",
                        "committed_paths",
                        "pr_url",
                        "pr_opened",
                        "pr_error",
                        "blocked",
                        "slice_pass",
                    )
                }
                chain["closeout_note"] = closed.get("note") or ""
                chain["handoff_summary"] = closed.get("handoff_summary") or ""
                _write_chain(chain)
                print(
                    f"[trial-broker] chain {slice_id} CLOSEOUT r{round_n} "
                    f"kind={closed.get('kind')} commit={closed.get('commit') or '-'} "
                    f"handoff={closed.get('handoff_summary')}",
                    flush=True,
                )
                out = _terminal_outbox(job, chain, dest, ok=True)
                print(
                    f"[trial-broker] chain {slice_id} closeout outbox={out.name}",
                    flush=True,
                )
                return 2
            chain["state"] = "failed"
            if closed and closed.get("action") == "fail":
                chain["error"] = str(closed.get("error") or f"foreman exit {ec} without summary")
                chain["handoff_summary"] = closed.get("handoff_summary") or ""
                chain["limit_kind"] = closed.get("kind") or ""
            else:
                chain["error"] = f"foreman exit {ec} without summary"
            _write_chain(chain)
            _terminal_outbox(job, chain, dest, ok=False)
            return ec or 1

        status = block.get("status")

        # Change ② — mid-ticket gate HOLD left unresolved P0/P1. A foreman
        # `done` that neither maps finding_resolutions nor hands off to a fresh
        # fix ticket must NOT be treated as a clean round: open the findings-first
        # fix ticket now (真·fresh), or escalate when no round remains. Never PASS.
        completion = decide_impl_completion(block, goal_obj)
        if completion["action"] == "open_fix":
            blocking = completion["findings"]
            chain["rework_handoff"] = {
                "at": _iso(),
                "mode": completion.get("mode") or "fresh",
                "reason": completion["reason"],
                "findings": blocking,
                "impl_summary": str(impl_summary),
            }
            round_rec["rework_handoff"] = dict(chain["rework_handoff"])
            if round_n >= max_rounds:
                chain["state"] = "escalated"
                chain["escalate_reason"] = (
                    "unresolved P0/P1 at max_rounds after rework handoff: "
                    + completion["reason"]
                )
                if goal_obj is not None:
                    goal_obj = _read_goal(str(goal_id)) or goal_obj
                    goal_obj.pop("pending_rework_mode", None)
                    goal_obj.pop("pending_rework_ask_id", None)
                    goal_obj["last_chain_state"] = "escalated"
                    goal_obj["status"] = "escalated"
                    goal_obj["error"] = chain["escalate_reason"]
                    _write_goal(goal_obj)
                chain["rounds"][-1] = round_rec
                _write_chain(chain)
                print(
                    f"[trial-broker] chain {slice_id} ESCALATED r{round_n} "
                    f"unresolved P0/P1 ({completion['reason']})",
                    flush=True,
                )
                out = _terminal_outbox(job, chain, dest, ok=False)
                if goal_id:
                    _notify_goal_terminal(str(goal_id))
                return 1
            next_round = round_n + 1
            try:
                fresh_fix_pack = build_fix_pack(
                    slice_id=slice_id,
                    next_round=next_round,
                    original_pack_name=original_pack,
                    gate_findings=blocking,
                    gate_summary_text="",
                )
            except ValueError as e:
                chain["state"] = "failed"
                chain["error"] = f"fresh fix pack build: {e}"
                _write_chain(chain)
                _terminal_outbox(job, chain, dest, ok=False)
                return 1
            round_rec["fresh_rework_fix_pack"] = fresh_fix_pack
            chain["rounds"][-1] = round_rec
            pack_for_impl = fresh_fix_pack
            if goal_obj is not None:
                goal_obj = _read_goal(str(goal_id)) or goal_obj
                goal_obj.pop("pending_rework_mode", None)
                goal_obj.pop("pending_rework_ask_id", None)
                _write_goal(goal_obj)
            _write_chain(chain)
            print(
                f"[trial-broker] chain {slice_id} fresh rework r{round_n} → "
                f"fix ticket r{next_round} pack={fresh_fix_pack}",
                flush=True,
            )
            continue

        # Decision: HOLD never wakes supervisor (handled below via fix pack).
        # Blocked → awaiting without auto-wake. Question → auto supervisor answer
        # (fallback when ask_supervisor mid-ticket timed out / unused).
        if status == FOREMAN_STATUS_BLOCKED:
            chain["state"] = "awaiting_supervisor"
            chain["awaiting_answer_for_round"] = round_n
            chain["awaiting_reason"] = status
            chain["pending_impl_pack"] = Path(pack_for_impl).name
            _write_chain(chain)
            # Fix 1: Decision D keeps the chain outbox notify=False, but a
            # parked chain must still wake the Hub so it can push chain-reply.
            _notify_awaiting_hub(job, chain, reason="blocked", block=block, goal=goal_obj)
            out = _terminal_outbox(job, chain, dest, ok=True, notify=False)
            print(
                f"[trial-broker] chain {slice_id} → awaiting_supervisor "
                f"(blocked; hub notified via {AWAITING_HUB_NOTIFY_KIND}) "
                f"outbox={out.name}",
                flush=True,
            )
            return 0
        if status == FOREMAN_STATUS_QUESTION:
            chain["state"] = "awaiting_supervisor"
            chain["awaiting_answer_for_round"] = round_n
            chain["awaiting_reason"] = status
            chain["pending_impl_pack"] = Path(pack_for_impl).name
            _write_chain(chain)
            auto = job.get("auto_supervisor_answer")
            if auto is None:
                auto = True
            if auto:
                answer = auto_supervisor_answer(
                    job, chain, round_n=round_n, block=block
                )
                if answer:
                    # resume inline via reply pack (same as chain-reply)
                    prior_round = round_n
                    next_round = prior_round + 1
                    max_rounds_local = int(chain.get("max_rounds") or max_rounds)
                    if next_round > max_rounds_local:
                        max_rounds_local = next_round
                        chain["max_rounds"] = max_rounds_local
                        max_rounds = max_rounds_local
                    try:
                        reply_pack = build_reply_addendum_pack(
                            slice_id=slice_id,
                            round_n=next_round,
                            prior_pack_name=Path(pack_for_impl).name,
                            answer=answer,
                            gate_findings=list(
                                (goal_obj or {}).get("last_gate_findings") or []
                            ),
                        )
                    except ValueError as e:
                        chain["state"] = "failed"
                        chain["error"] = f"auto reply pack: {e}"
                        _write_chain(chain)
                        _terminal_outbox(job, chain, dest, ok=False)
                        return 1
                    chain["state"] = "running"
                    chain["awaiting_answer_for_round"] = None
                    chain["last_supervisor_answer"] = answer[:2000]
                    chain["reply_pack"] = reply_pack
                    _write_chain(chain)
                    pack_for_impl = reply_pack
                    print(
                        f"[trial-broker] auto supervisor answer → continue r{next_round} "
                        f"pack={reply_pack}",
                        flush=True,
                    )
                    return run_chain_rounds(
                        job, chain, dest,
                        start_round=next_round,
                        start_pack=reply_pack,
                    )
            # Fix 1: question parked (auto-answer off / failed) — wake the Hub
            # too. The auto-answered path above resumes and never reaches here.
            _notify_awaiting_hub(job, chain, reason="question", block=block, goal=goal_obj)
            out = _terminal_outbox(job, chain, dest, ok=True)
            print(
                f"[trial-broker] chain {slice_id} → awaiting_supervisor "
                f"({status}; hub notified via {AWAITING_HUB_NOTIFY_KIND}) outbox={out.name}",
                flush=True,
            )
            return 0

        # Gate
        gate_ticket = f"gate-trial-{slice_id}-r{round_n}"
        gate_summary_name = f"{slice_id}-gate-r{round_n}"
        gate_summary = SUMMARIES / f"{gate_summary_name}.md"
        gate_log = ARTIFACT_ROOT / f"{gate_ticket}.log"
        try:
            gate_pack = build_gate_pack(
                slice_id=slice_id,
                round_n=round_n,
                original_pack_name=original_pack,
                acceptance=job.get("acceptance") or chain.get("acceptance"),
                foreman_block=block,
                foreman_summary_text=parsed["text"],
                cwd=cwd,
            )
        except ValueError as e:
            chain["state"] = "failed"
            chain["error"] = f"gate pack build: {e}"
            _write_chain(chain)
            _terminal_outbox(job, chain, dest, ok=False)
            return 1

        round_rec["gate_pack"] = gate_pack
        gec = run_open_slice(
            ticket=gate_ticket,
            pack_name=gate_pack,
            profile=job.get("gate_profile") or job["profile"],
            cwd=str(cwd),
            role="gate",
            summary_name=gate_summary_name,
            prompt_mode="gate",
            log_path=gate_log,
            goal=goal_obj,
            watch_mailbox=False,
        )
        _adopt_misplaced_summary(gate_summary)
        gate_art = write_artifacts(
            {**job, "ticket": gate_ticket, "role": "gate"},
            gate_log, gate_summary, gec, ticket=gate_ticket,
        )
        gpeak, gsteps = _peak_prompt(gate_art.get("composition"))
        gparsed = _parse_gate_verdict(gate_summary)
        gblock = enforce_gate_verdict(gparsed["block"])
        gparsed["block"] = gblock
        if goal_obj is not None:
            T.record_ticket_metric(goal_obj, {
                "role": "gate",
                "ticket": gate_ticket,
                "slice": slice_id,
                "exit": gec,
                "peak_prompt": gpeak,
                "steps": gsteps,
                "tool_res": _tool_res_from_art(gate_art),
                "assert_clean": gate_art.get("assert_clean"),
                "kind": "gate",
                "verdict": gblock.get("verdict"),
            })
            _write_goal(goal_obj)
        # rewrite summary note if overridden (append; do not wipe agent prose)
        if gblock.get("verdict_overridden") and gate_summary.is_file():
            try:
                note = (
                    f"\n\n<!-- broker enforce_gate_verdict: HOLD "
                    f"({gblock.get('verdict_override_reason')}) "
                    f"original={gblock.get('verdict_original')} -->\n"
                )
                with gate_summary.open("a", encoding="utf-8") as fh:
                    fh.write(note)
            except OSError:
                pass
        verdict = gblock.get("verdict")
        round_rec.update({
            "gate_ticket": gate_ticket,
            "gate_exit": gec,
            "gate_summary": str(gate_summary),
            "gate_assert_clean": gate_art.get("assert_clean"),
            "gate_peak": gpeak,
            "gate_steps": gsteps,
            "gate_artifacts": gate_art,
            "verdict": verdict,
            "gate_block": gblock,
        })
        chain["last_verdict"] = verdict
        _write_chain(chain)

        if verdict == VERDICT_PASS:
            chain["state"] = "PASS"
            _write_chain(chain)
            if (job.get("from_goal") or job.get("goal")) and not job.get("defer_supervisor_close"):
                try:
                    maybe_supervisor_close(job, chain)
                except Exception as e:  # noqa: BLE001 — close best-effort
                    print(f"[trial-broker] supervisor close error: {e}", flush=True)
            out = _terminal_outbox(job, chain, dest, ok=True)
            print(f"[trial-broker] chain {slice_id} PASS outbox={out.name}", flush=True)
            return 0

        # HOLD
        if round_n >= max_rounds:
            # Salvage: the final-round gate timed out or wrote no usable
            # summary (file absent / empty text → parse defaults to an empty
            # HOLD with no findings and no unmet_acceptance). If this slice
            # already has PASS evidence and the impl summary exists, closing
            # failed here would falsely fail the whole Goal — adopt the prior
            # PASS instead. A real HOLD (findings / unmet_acceptance) or a
            # round without PASS evidence still escalates below.
            gate_summary_missing = (
                verdict == VERDICT_HOLD
                and not (gblock.get("findings") or [])
                and not (gblock.get("unmet_acceptance") or [])
                and (not gate_summary.is_file() or not gparsed["text"].strip())
            )
            if gate_summary_missing and impl_summary.is_file():
                salvage_goal = goal_obj
                if goal_id:
                    salvage_goal = _read_goal(str(goal_id)) or goal_obj
                salvaged, salvage_reason = _slice_has_review_pass(
                    salvage_goal, slice_id, reload=True
                )
                if salvaged:
                    round_rec["salvaged_missing_gate_summary"] = True
                    round_rec["salvage_reason"] = salvage_reason
                    round_rec["verdict"] = VERDICT_PASS
                    chain["rounds"][-1] = round_rec
                    chain["state"] = VERDICT_PASS
                    chain["salvage_note"] = (
                        "salvaged: last-round gate missing summary; adopting prior PASS "
                        f"({salvage_reason})"
                    )
                    chain["error"] = ""
                    chain["last_verdict"] = VERDICT_PASS
                    _write_chain(chain)
                    print(
                        f"[trial-broker] chain {slice_id} SALVAGE PASS r{round_n} "
                        f"(gate missing summary; {salvage_reason})",
                        flush=True,
                    )
                    if (job.get("from_goal") or job.get("goal")) and not job.get(
                        "defer_supervisor_close"
                    ):
                        try:
                            maybe_supervisor_close(job, chain)
                        except Exception as e:  # noqa: BLE001 — close best-effort
                            print(f"[trial-broker] supervisor close error: {e}", flush=True)
                    out = _terminal_outbox(job, chain, dest, ok=True)
                    print(f"[trial-broker] chain {slice_id} PASS outbox={out.name}", flush=True)
                    return 0
            chain["state"] = "escalated"
            chain["escalate_reason"] = f"HOLD at max_rounds={max_rounds}"
            _write_chain(chain)
            out = _terminal_outbox(job, chain, dest, ok=False)
            print(f"[trial-broker] chain {slice_id} escalated outbox={out.name}", flush=True)
            return 1

        next_round = round_n + 1
        try:
            fix_pack = build_fix_pack(
                slice_id=slice_id,
                next_round=next_round,
                original_pack_name=original_pack,
                gate_findings=gblock.get("findings") or [],
                gate_summary_text=gparsed["text"],
            )
        except ValueError as e:
            chain["state"] = "failed"
            chain["error"] = f"fix pack build: {e}"
            _write_chain(chain)
            _terminal_outbox(job, chain, dest, ok=False)
            return 1
        round_rec["fix_pack_next"] = fix_pack
        pack_for_impl = fix_pack
        _write_chain(chain)
        print(
            f"[trial-broker] chain {slice_id} HOLD r{round_n} → fix pack {fix_pack}",
            flush=True,
        )

    chain["state"] = "escalated"
    chain["escalate_reason"] = "exhausted rounds"
    _write_chain(chain)
    _terminal_outbox(job, chain, dest, ok=False)
    return 1



def _normalize_goal_slices(block: dict, job: dict, goal_id: str) -> list[dict]:
    """Plan machine block → ordered slice specs (new emit_chains + legacy emit_chain).

    New: ``action=emit_chains`` with a non-empty ``slices`` list of
    ``{slice, pack, acceptance}`` objects. Legacy: ``action=emit_chain`` with
    top-level slice/pack/acceptance (wrapped as a one-element list). Job hints
    are the final fallback so a malformed plan still yields a runnable slice.
    """
    raw = block.get("slices")
    specs: list[dict] = []
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            pack_name = Path(str(item.get("pack") or "")).name
            slice_id = str(item.get("slice") or item.get("id") or "").strip()
            if not slice_id and pack_name:
                slice_id = pack_name[: -len(".pack.md")] if pack_name.endswith(".pack.md") else pack_name
            if not slice_id:
                continue
            acc = item.get("acceptance") or job.get("acceptance_hint") or []
            if isinstance(acc, str):
                acc = [acc]
            specs.append({
                "slice": slice_id,
                "pack": Path(pack_name or f"{slice_id}.pack.md").name,
                "acceptance": acc,
            })
    if not specs:
        slice_id = str(block.get("slice") or job.get("suggested_slice") or f"{goal_id}-s1")
        pack_name = Path(
            str(block.get("pack") or job.get("suggested_pack") or f"{slice_id}.pack.md")
        ).name
        acc = block.get("acceptance") or job.get("acceptance_hint") or []
        if isinstance(acc, str):
            acc = [acc]
        specs.append({"slice": slice_id, "pack": pack_name, "acceptance": acc})
    seen: set[str] = set()
    out: list[dict] = []
    for spec in specs:
        if spec["slice"] in seen:
            continue
        seen.add(spec["slice"])
        out.append(spec)
    return out


def _execute_goal_slices(
    goal_state: dict,
    job: dict,
    slice_specs: list[dict],
    dest: Path,
    *,
    on_slice_fail: str,
    plan_ticket: str | None,
    skip_passed: bool = False,
) -> int:
    """Run planned slices in order and close the goal when every slice PASSes.

    ``skip_passed=False`` is the fresh goal path (always opens each slice).
    ``skip_passed=True`` is the resume path after a salvageable false failure:
    a chain already in PASS is not re-opened, so a salvaged s1 is not rewritten
    and the next slice still runs.
    """
    goal_id = str(job.get("goal") or goal_state.get("goal") or "")
    if on_slice_fail not in ("continue", "stop"):
        on_slice_fail = "stop"

    all_pass = True
    rc = 0
    last_chain: dict | None = None
    last_chain_job: dict | None = None
    for spec in slice_specs:
        slice_id = str(spec["slice"])
        if skip_passed:
            existing = _read_chain(slice_id)
            if isinstance(existing, dict) and str(existing.get("state") or "") == VERDICT_PASS:
                last_chain = existing
                last_chain_job = {
                    "id": job.get("id") or f"goal-chain-{slice_id}",
                    "type": "chain",
                    "slice": slice_id,
                    "pack": Path(str(spec.get("pack") or existing.get("pack") or f"{slice_id}.pack.md")).name,
                    "acceptance": spec.get("acceptance") or existing.get("acceptance") or [],
                    "profile": job.get("profile") or existing.get("profile") or "acp",
                    "gate_profile": job.get("gate_profile") or job.get("profile") or existing.get("gate_profile") or "acp",
                    "supervisor_profile": job.get("supervisor_profile") or "acp-lite",
                    "cwd": job.get("cwd") or existing.get("cwd") or "/workspace",
                    "max_rounds": int(job.get("max_rounds") or existing.get("max_rounds") or 2),
                    "notify": job.get("notify"),
                    "notify_dry_run": job.get("notify_dry_run"),
                    "notify_prefix": job.get("notify_prefix"),
                    "goal": goal_id,
                    "from_goal": True,
                    "auto_supervisor_answer": job.get("auto_supervisor_answer", True),
                    "max_supervisor_tickets": job.get("max_supervisor_tickets") or 8,
                    "defer_supervisor_close": True,
                }
                print(
                    f"[trial-broker] goal {goal_id} skip already-PASS slice={slice_id}",
                    flush=True,
                )
                continue
        pack_name = Path(spec["pack"]).name
        pack_abs = PACKS / pack_name
        if not pack_abs.is_file():
            goal_state["status"] = "failed"
            goal_state["error"] = f"supervisor did not write pack {pack_name}"
            _write_goal(goal_state)
            if dest.exists():
                shutil.move(str(dest), str(FAILED / dest.name))
            print(f"[trial-broker] FAIL missing pack {pack_abs}", flush=True)
            # Terminal: wake Hub like the other goal-level failures.
            _notify_goal_terminal(goal_id)
            return 1

        goal_state["slices"] = list(dict.fromkeys(list(goal_state.get("slices") or []) + [slice_id]))
        _write_goal(goal_state)

        chain_job = {
            "id": job.get("id") or f"goal-chain-{slice_id}",
            "type": "chain",
            "slice": slice_id,
            "pack": pack_name,
            "acceptance": spec.get("acceptance") or [],
            "profile": job["profile"],
            "gate_profile": job.get("gate_profile") or job["profile"],
            "supervisor_profile": job.get("supervisor_profile") or "acp-lite",
            "cwd": job["cwd"],
            "max_rounds": int(job.get("max_rounds") or 2),
            "notify": job.get("notify"),
            "notify_dry_run": job.get("notify_dry_run"),
            "notify_prefix": job.get("notify_prefix"),
            "goal": goal_id,
            "from_goal": True,
            "auto_supervisor_answer": job.get("auto_supervisor_answer", True),
            "max_supervisor_tickets": job.get("max_supervisor_tickets") or 8,
            # close the Goal once, after every slice is PASS (never per slice)
            "defer_supervisor_close": True,
        }
        chain = _new_chain_state(chain_job)
        chain["from_goal"] = True
        chain["goal"] = goal_id
        # Persist the goal-scoped knobs so a later chain-reply can restore them
        # when Hub omits them from its reply job.
        chain["supervisor_profile"] = chain_job.get("supervisor_profile")
        chain["auto_supervisor_answer"] = chain_job.get("auto_supervisor_answer")
        chain["max_supervisor_tickets"] = chain_job.get("max_supervisor_tickets")
        chain["defer_supervisor_close"] = True
        chain["plan_ticket"] = plan_ticket
        _write_chain(chain)
        print(
            f"[trial-broker] goal {goal_id} → chain slice={slice_id} pack={pack_name}",
            flush=True,
        )
        rc = run_chain_rounds(chain_job, chain, dest, start_round=1, start_pack=pack_name)
        final_chain = _read_chain(slice_id) or chain
        last_chain = final_chain
        last_chain_job = chain_job
        state = str(final_chain.get("state") or "")
        if state == "PASS":
            # Reload so chain-side record_ticket_metric writes are not wiped by stale goal_state.
            goal_state = _read_goal(goal_id) or goal_state
            m = goal_state.setdefault("metrics", T.empty_metrics())
            m["slices_completed"] = int(m.get("slices_completed") or 0) + 1
            _write_goal(goal_state)
            continue
        if state == "closeout":
            # Limit stop with work in the tree. Not failed, not a new impl, not PASS.
            goal_state = _read_goal(goal_id) or goal_state
            goal_state["status"] = "closeout"
            goal_state["last_chain_state"] = "closeout"
            goal_state["error"] = ""
            if final_chain.get("handoff_summary"):
                goal_state["handoff_summary"] = final_chain["handoff_summary"]
            if final_chain.get("closeout_note"):
                goal_state["closeout_note"] = final_chain["closeout_note"]
            _write_goal(goal_state)
            all_pass = False
            print(
                f"[trial-broker] slice {slice_id} closeout; not marking goal failed",
                flush=True,
            )
            break
        goal_state = _read_goal(goal_id) or goal_state
        # Belt-and-suspenders: never fail a slice solely because the impl
        # summary was missing when a gate/submit_for_review PASS already
        # proved this slice done. run_chain_rounds salvages this case itself;
        # this covers chains written before that fix landed.
        if state == "failed" and "without summary" in str(final_chain.get("error") or ""):
            salvaged, salvage_reason = _slice_has_review_pass(
                goal_state, slice_id, reload=True
            )
            if salvaged:
                print(
                    f"[trial-broker] slice {slice_id} salvage PASS at goal level "
                    f"({salvage_reason}); ignoring missing-summary failure",
                    flush=True,
                )
                final_chain["state"] = VERDICT_PASS
                final_chain["salvage_note"] = (
                    f"salvaged at goal level: missing impl summary after "
                    f"gate PASS ({salvage_reason})"
                )
                final_chain["error"] = ""
                _write_chain(final_chain)
                state = VERDICT_PASS
                goal_state = _read_goal(goal_id) or goal_state
                m = goal_state.setdefault("metrics", T.empty_metrics())
                m["slices_completed"] = int(m.get("slices_completed") or 0) + 1
                _write_goal(goal_state)
                continue
        all_pass = False
        goal_state["last_chain_state"] = state
        if state in ("failed", "escalated", "cancelled"):
            goal_state["status"] = state
        if final_chain.get("handoff_summary"):
            goal_state["handoff_summary"] = final_chain["handoff_summary"]
        _write_goal(goal_state)
        if on_slice_fail != "continue":
            print(
                f"[trial-broker] slice {slice_id} {state or 'not-PASS'}; on_slice_fail=stop",
                flush=True,
            )
            break
        print(
            f"[trial-broker] slice {slice_id} {state or 'not-PASS'}; on_slice_fail=continue",
            flush=True,
        )

    # One supervisor close for the whole Goal, only when every slice PASSed.
    if all_pass and last_chain is not None and last_chain_job is not None:
        try:
            maybe_supervisor_close(last_chain_job, last_chain)
        except Exception as e:  # noqa: BLE001 — close best-effort
            print(f"[trial-broker] supervisor close error: {e}", flush=True)
    # Goal-level notify only (single chain never notifies Hub).
    _notify_goal_terminal(goal_id)
    if all_pass or (len(slice_specs) == 1 and rc == 0):
        return 0
    return 1



def run_goal_job(path: Path, job: dict) -> int:
    """Hub drops Goal → dsh supervisor plans pack → inline chain (+ auto answer/close)."""
    dest = PROCESSING / f"{_stamp()}-{path.name}"
    shutil.move(str(path), str(dest))
    goal_id = job["goal"]
    print(f"[trial-broker] processing GOAL {dest.name} goal={goal_id}", flush=True)

    goal_state = {
        "goal": goal_id,
        "status": "planning",
        "brief": job["brief"][:4000],
        "cwd": job["cwd"],
        "profile": job["profile"],
        "supervisor_profile": job.get("supervisor_profile") or "acp-lite",
        "max_slices": job.get("max_slices") or 1,
        "max_supervisor_tickets": job.get("max_supervisor_tickets") or 8,
        "max_rounds": job.get("max_rounds") or 2,
        "slices": [],
        "supervisor_ticket_count": 0,
        "supervisor_tickets": [],
        "created_at": _iso(),
        "source_job": job.get("id"),
        "notify": job.get("notify"),
        "notify_dry_run": job.get("notify_dry_run"),
        "notify_prefix": job.get("notify_prefix"),
    }
    _write_goal(goal_state)

    try:
        brief_pack = build_goal_brief_pack(job)
    except ValueError as e:
        goal_state["status"] = "failed"
        goal_state["error"] = f"brief pack: {e}"
        _write_goal(goal_state)
        shutil.move(str(dest), str(FAILED / dest.name))
        _notify_goal_terminal(goal_id)
        return 1

    plan_ticket = f"supervisor-plan-{goal_id}"
    ec, art, summary_path = run_supervisor_ticket(
        ticket=plan_ticket,
        pack_name=brief_pack,
        profile=str(job.get("supervisor_profile") or "acp-lite"),
        cwd=str(job["cwd"]),
        summary_name=f"goal-{goal_id}-plan",
        prompt_mode="supervisor-plan",
        goal=goal_state,
    )
    goal_state = _read_goal(goal_id) or goal_state
    parsed = _parse_supervisor_summary(summary_path)
    block = parsed["block"]
    if ec != 0 and not summary_path.is_file():
        goal_state["status"] = "failed"
        goal_state["error"] = f"plan ticket exit {ec}"
        _write_goal(goal_state)
        (FAILED / f"{dest.stem}.error.json").write_text(
            json.dumps({"error": goal_state["error"], "job": job, "at": _iso()}, ensure_ascii=False, indent=2)
        )
        shutil.move(str(dest), str(FAILED / dest.name))
        # Plan-ticket failure is terminal too: wake Hub like slice failures do.
        _notify_goal_terminal(goal_id)
        return ec or 1

    action = str(block.get("action") or "")
    if action not in ("emit_chain", "emit_chains"):
        # soft: try to recover fields anyway
        print(f"[trial-broker] WARN plan action={action!r}, trying field recovery", flush=True)

    slice_specs = _normalize_goal_slices(block, job, goal_id)

    # Enforce max_slices (goal-update overrides > job > global default).
    limits = T.resolve_limits(job=job, goal=goal_state)
    max_slices = int(limits.get("max_slices") or 1)
    if len(slice_specs) > max_slices:
        hit = T.limit_hit_payload(
            which="max_slices",
            limit=max_slices,
            used=len(slice_specs),
            slice_id=slice_specs[0]["slice"] if slice_specs else None,
            step="emit_chains",
            goal_id=goal_id,
            suggestion=f"投 type:goal-update 提高 max_slices（当前 {max_slices}）",
        )
        goal_state["limit_hit"] = hit
        goal_state.setdefault("metrics", T.empty_metrics())["limit_hits"].append(hit)
        goal_state["status"] = "escalated"
        goal_state["plan_summary"] = str(summary_path)
        _write_goal(goal_state)
        _notify_goal_event(goal_state, kind="dsh-trial-limit", ok=False, extra=hit)
        (FAILED / f"{dest.stem}.error.json").write_text(
            json.dumps(
                {"error": "max_slices exceeded", "limit_hit": hit, "at": _iso()},
                ensure_ascii=False,
                indent=2,
            ) + "\n"
        )
        shutil.move(str(dest), str(FAILED / dest.name))
        print(
            f"[trial-broker] FAIL goal {goal_id} planned {len(slice_specs)} slices "
            f"> max_slices={max_slices}",
            flush=True,
        )
        return 1

    goal_state["status"] = "running"
    goal_state["plan_summary"] = str(summary_path)
    goal_state["plan_artifacts"] = art
    goal_state.setdefault("metrics", T.empty_metrics())["slices_planned"] = len(slice_specs)
    _write_goal(goal_state)

    on_slice_fail = str(block.get("on_slice_fail") or job.get("on_slice_fail") or "stop").strip().lower()
    return _execute_goal_slices(
        goal_state,
        job,
        slice_specs,
        dest,
        on_slice_fail=on_slice_fail,
        plan_ticket=plan_ticket,
        skip_passed=False,
    )


def run_chain_job(path: Path, job: dict) -> int:
    dest = PROCESSING / f"{_stamp()}-{path.name}"
    shutil.move(str(path), str(dest))
    print(f"[trial-broker] processing CHAIN {dest.name} slice={job['slice']}", flush=True)

    pack_name = Path(job["pack"]).name
    pack_abs = PACKS / pack_name
    if not pack_abs.is_file():
        err = f"pack missing: {pack_abs}"
        print(f"[trial-broker] FAIL {err}", flush=True)
        (FAILED / f"{dest.stem}.error.json").write_text(
            json.dumps({"error": err, "job": job, "at": _iso()}, ensure_ascii=False, indent=2)
        )
        shutil.move(str(dest), str(FAILED / dest.name))
        return 1

    existing = _read_chain(job["slice"])
    if existing and existing.get("state") == "running":
        print(f"[trial-broker] WARN overwriting running chain state for {job['slice']}", flush=True)

    chain = _new_chain_state(job)
    _write_chain(chain)
    return run_chain_rounds(job, chain, dest, start_round=1, start_pack=pack_name)


def run_chain_reply_job(path: Path, job: dict) -> int:
    dest = PROCESSING / f"{_stamp()}-{path.name}"
    shutil.move(str(path), str(dest))
    slice_id = job["slice"]
    print(f"[trial-broker] processing CHAIN-REPLY {dest.name} slice={slice_id}", flush=True)

    chain = _read_chain(slice_id)
    if not chain:
        err = f"no chain state for slice={slice_id}"
        print(f"[trial-broker] FAIL {err}", flush=True)
        (FAILED / f"{dest.stem}.error.json").write_text(
            json.dumps({"error": err, "job": job, "at": _iso()}, ensure_ascii=False, indent=2)
        )
        shutil.move(str(dest), str(FAILED / dest.name))
        return 1
    if chain.get("state") != "awaiting_supervisor":
        err = f"chain state is {chain.get('state')}, expected awaiting_supervisor"
        print(f"[trial-broker] FAIL {err}", flush=True)
        (FAILED / f"{dest.stem}.error.json").write_text(
            json.dumps({"error": err, "job": job, "at": _iso()}, ensure_ascii=False, indent=2)
        )
        shutil.move(str(dest), str(FAILED / dest.name))
        return 1

    prior_round = int(chain.get("awaiting_answer_for_round") or len(chain.get("rounds") or []) or 1)
    next_round = prior_round + 1
    max_rounds = int(chain.get("max_rounds") or 2)
    if next_round > max_rounds:
        # allow one resume round by bumping max if supervisor replied
        max_rounds = next_round
        chain["max_rounds"] = max_rounds

    prior_pack = chain.get("pending_impl_pack") or chain.get("pack")
    if chain.get("rounds"):
        last = chain["rounds"][-1]
        prior_pack = last.get("impl_pack") or prior_pack

    parent_goal_id = _resolve_parent_goal_id(job, chain)
    parent_goal = _read_goal(parent_goal_id) if parent_goal_id else None

    try:
        reply_gate_findings: list = []
        if parent_goal_id:
            reply_goal = _read_goal(parent_goal_id)
            reply_gate_findings = list((reply_goal or {}).get("last_gate_findings") or [])
        reply_pack = build_reply_addendum_pack(
            slice_id=slice_id,
            round_n=next_round,
            prior_pack_name=prior_pack,
            answer=job["answer"],
            gate_findings=reply_gate_findings,
        )
    except ValueError as e:
        chain["state"] = "failed"
        chain["error"] = f"reply pack: {e}"
        _write_chain(chain)
        shutil.move(str(dest), str(FAILED / dest.name))
        return 1

    chain["state"] = "running"
    chain["awaiting_answer_for_round"] = None
    chain["last_supervisor_answer"] = job["answer"][:2000]
    chain["reply_pack"] = reply_pack
    _write_chain(chain)

    # Reconstruct job fields from chain for resume
    resume_job = {
        "id": job.get("id") or f"chain-reply-{slice_id}",
        "type": "chain",
        "slice": slice_id,
        "pack": chain.get("pack"),
        "acceptance": chain.get("acceptance"),
        "profile": chain.get("profile") or "acp",
        "gate_profile": chain.get("gate_profile") or chain.get("profile") or "acp",
        "cwd": chain.get("cwd") or "/workspace",
        "max_rounds": max_rounds,
        "notify": chain.get("notify") or job.get("notify"),
        "notify_dry_run": chain.get("notify_dry_run") if chain.get("notify_dry_run") is not None else job.get("notify_dry_run"),
        "notify_prefix": chain.get("notify_prefix") or job.get("notify_prefix"),
        "goal": parent_goal_id,
    }
    if parent_goal_id:
        resume_job["from_goal"] = True
        if chain.get("supervisor_profile") is not None:
            resume_job["supervisor_profile"] = chain.get("supervisor_profile")
        elif parent_goal and parent_goal.get("supervisor_profile") is not None:
            resume_job["supervisor_profile"] = parent_goal.get("supervisor_profile")
        if chain.get("auto_supervisor_answer") is not None:
            resume_job["auto_supervisor_answer"] = chain.get("auto_supervisor_answer")
        if chain.get("max_supervisor_tickets") is not None:
            resume_job["max_supervisor_tickets"] = chain.get("max_supervisor_tickets")
        elif parent_goal and parent_goal.get("max_supervisor_tickets") is not None:
            resume_job["max_supervisor_tickets"] = parent_goal.get("max_supervisor_tickets")
        # Close the parent exactly once, after _settle checks all slices.
        resume_job["defer_supervisor_close"] = True
    rc = run_chain_rounds(
        resume_job, chain, dest, start_round=next_round, start_pack=reply_pack
    )
    if parent_goal_id:
        return _settle_parent_goal_after_resume(
            parent_goal_id, resume_job, slice_id, dest, rc
        )
    return rc


def _settle_parent_goal_after_resume(
    goal_id: str, job: dict, slice_id: str, dest: Path, rc: int
) -> int:
    """Fold a resumed chain's terminal state onto its parent Goal + notify.

    A chain-reply / goal-update resume must never leave the parent stuck at
    ``awaiting_supervisor`` with zero completed slices and no Hub notify: the
    Hub only learns about completion from the parent Goal terminal event.
    """
    chain = _read_chain(slice_id) or {}
    state = str(chain.get("state") or "")
    goal = _read_goal(goal_id)
    if goal is None:
        return rc

    if state == VERDICT_PASS:
        goal["last_chain_state"] = VERDICT_PASS
        _sync_slices_completed(goal, extra_pass=slice_id)
        _write_goal(goal)
        specs = _slice_specs_from_plan(goal)
        ids = [str(s.get("slice")) for s in specs] if specs else _goal_slice_ids(goal)
        seen: list[str] = []
        for sid in ids:
            if sid and sid not in seen:
                seen.append(sid)
        pending = [
            sid
            for sid in seen
            if str((_read_chain(sid) or {}).get("state") or "") != VERDICT_PASS
        ]
        if pending:
            if all(_read_chain(sid) is None for sid in pending):
                # Planned slices exist but none has started: continue the Goal
                # without re-running the slice that just PASSed.
                print(
                    f"[trial-broker] goal {goal_id} resume PASS {slice_id}; "
                    f"continuing planned slices {pending}",
                    flush=True,
                )
                return _execute_goal_slices(
                    goal,
                    _resume_job_from_goal(goal),
                    specs,
                    dest,
                    on_slice_fail="stop",
                    plan_ticket=chain.get("plan_ticket"),
                    skip_passed=True,
                )
            print(
                f"[trial-broker] goal {goal_id} resume PASS {slice_id} but "
                f"pending slices {pending} already have non-PASS chains; "
                "not auto re-running",
                flush=True,
            )
            _notify_goal_terminal(goal_id)
            return rc
        try:
            maybe_supervisor_close(job, chain)
        except Exception as e:  # noqa: BLE001 — close best-effort
            print(f"[trial-broker] supervisor close error: {e}", flush=True)
        # Dedupe-safe belt-and-braces: maybe_supervisor_close already notifies
        # when its close says done/failed/escalated.
        _notify_goal_terminal(goal_id)
        return rc

    if state in ("failed", "escalated", "cancelled", "closeout"):
        goal = _read_goal(goal_id) or goal
        goal["status"] = state
        goal["last_chain_state"] = state
        if state == "failed":
            goal["error"] = str(chain.get("error") or goal.get("error") or "")
        if state == "escalated":
            goal["escalate_reason"] = str(
                chain.get("escalate_reason") or goal.get("escalate_reason") or ""
            )
        if chain.get("handoff_summary"):
            goal["handoff_summary"] = chain["handoff_summary"]
        if chain.get("closeout_note"):
            goal["closeout_note"] = chain["closeout_note"]
        _write_goal(goal)
        _notify_goal_terminal(goal_id)
        return rc

    # awaiting_supervisor again / running: the awaiting-hub notify already
    # fired inside run_chain_rounds; just keep the parent's chain state fresh.
    goal["last_chain_state"] = state
    _write_goal(goal)
    return rc


def _ticket_goal_terminal(goal_id: str | None, status: str, *, reason: str = "") -> None:
    """Settle a standalone ticket job's mapped Goal and wake Hub exactly once.

    ``run_ticket_job`` is not chain-owned, so a Goal mapped onto a single
    ticket must update its own status (done / failed / escalated) and notify
    with the same ``goal`` / ``status`` / ``reason`` / ``suggested_action``
    payload the chain terminal paths use. ``_notify_goal_terminal`` dedupes and
    owns the payload, so callers only persist the status transition.
    """
    if not goal_id:
        return
    goal = _read_goal(goal_id)
    if goal is not None:
        goal["status"] = status
        if status == "done":
            goal["last_chain_state"] = VERDICT_PASS
            goal["error"] = ""
        else:
            goal["last_chain_state"] = status
            if status == "escalated":
                goal["escalate_reason"] = reason or str(goal.get("escalate_reason") or "")
            else:
                goal["error"] = reason or str(goal.get("error") or "")
            goal.pop("pending_rework_mode", None)
            goal.pop("pending_rework_ask_id", None)
        _write_goal(goal)
    _notify_goal_terminal(goal_id)


def run_ticket_job(path: Path, job: dict) -> int:
    dest = PROCESSING / f"{_stamp()}-{path.name}"
    shutil.move(str(path), str(dest))
    print(f"[trial-broker] processing {dest.name} ticket={job['ticket']}", flush=True)

    goal_id = str(job.get("goal") or "").strip()
    goal_obj = _read_goal(goal_id) if goal_id else None

    pack_name = Path(job["pack"]).name
    pack_abs = PACKS / pack_name
    if not pack_abs.is_file():
        err = f"pack missing: {pack_abs}"
        print(f"[trial-broker] FAIL {err}", flush=True)
        fail_meta = FAILED / f"{dest.stem}.error.json"
        fail_meta.write_text(
            json.dumps({"error": err, "job": job, "at": _iso()}, ensure_ascii=False, indent=2)
        )
        shutil.move(str(dest), str(FAILED / dest.name))
        # Missing pack is terminal for a mapped Goal: never leave Hub blind.
        _ticket_goal_terminal(goal_id, "failed", reason=err)
        return 1

    role = job.get("role")
    if not role:
        t = job["ticket"]
        if t.startswith("gate-") or t.startswith("review-"):
            role = "gate"
        else:
            role = "impl"
    base_summary = str(job.get("summary_name") or pack_name.replace(".pack.md", ""))
    slice_id = str(job.get("slice") or Path(pack_name).name.replace(".pack.md", "") or "ticket")
    original_pack = pack_name
    max_rounds = int(job.get("max_rounds") or (goal_obj or {}).get("max_rounds") or 2)
    if max_rounds < 1:
        max_rounds = 1

    pack_for_impl = pack_name
    round_n = 1
    rounds: list[dict] = []
    status = "failed"
    reason = ""
    ec = 1
    summary_path = SUMMARIES / f"{base_summary}.md"
    result: dict = {}

    while True:
        ticket = job["ticket"] if round_n == 1 else f"{job['ticket']}-fix-r{round_n}"
        summary_name = base_summary if round_n == 1 else f"{slice_id}-impl-r{round_n}"
        log_path = ARTIFACT_ROOT / f"{ticket}.log"
        round_summary = SUMMARIES / f"{summary_name}.md"
        ec = run_open_slice(
            ticket=ticket,
            pack_name=pack_for_impl,
            profile=job["profile"],
            cwd=job["cwd"],
            role=str(role),
            summary_name=str(summary_name),
            prompt_mode="baseline" if round_n == 1 else "foreman",
            log_path=log_path,
            goal=goal_obj,
        )
        result = write_artifacts(
            {**job, "ticket": ticket, "role": role},
            log_path,
            round_summary,
            ec,
            ticket=ticket,
        )
        summary_path = round_summary
        # The mailbox watcher may have stored fresh gate findings on the Goal
        # while the ticket was running; reload before judging completion.
        if goal_id:
            goal_obj = _read_goal(goal_id) or goal_obj
        parsed = _parse_foreman_summary(round_summary)
        block = parsed.get("block") or {}
        # The guard is about impl completion: gate/review tickets keep their
        # historical treatment and are never turned into an impl fix ticket.
        if str(role) == "impl":
            completion = decide_impl_completion(block, goal_obj)
        else:
            completion = {
                "action": "clean_done",
                "findings": [],
                "reason": f"non-impl ticket role={role}",
                "mode": "",
            }
        round_rec = {
            "round": round_n,
            "ticket": ticket,
            "pack": pack_for_impl,
            "exit": ec,
            "summary": str(round_summary),
            "status": block.get("status"),
            "artifacts": result,
        }
        rounds.append(round_rec)

        if completion["action"] != "open_fix":
            # Clean completion only when the ticket really produced a summary.
            if ec == 0 and round_summary.is_file():
                status = "ok"
                reason = ""
                _ticket_goal_terminal(goal_id, "done")
                break
            status = "failed"
            reason = str(
                block.get("error")
                or f"exit={ec} summary_exists={round_summary.is_file()}"
            )
            _ticket_goal_terminal(goal_id, "failed", reason=reason)
            break

        # Change ② — status=done (or a rework_fresh handoff) with pending P0/P1
        # and no finding_resolutions must never be a silent PASS. Open the
        # findings-first fix ticket now, or escalate when no round remains.
        blocking = completion["findings"]
        handoff = {
            "at": _iso(),
            "mode": completion.get("mode") or "fresh",
            "reason": completion["reason"],
            "findings": blocking,
            "impl_summary": str(round_summary),
        }
        round_rec["rework_handoff"] = handoff
        if round_n >= max_rounds:
            reason = (
                "unresolved P0/P1 at max_rounds after rework handoff: "
                + completion["reason"]
            )
            status = "escalated"
            if goal_obj is not None:
                goal_obj = _read_goal(goal_id) or goal_obj
                goal_obj["last_rework_handoff"] = handoff
                _write_goal(goal_obj)
            print(
                f"[trial-broker] ticket {job['ticket']} ESCALATED r{round_n} "
                f"unresolved P0/P1 ({completion['reason']})",
                flush=True,
            )
            _ticket_goal_terminal(goal_id, "escalated", reason=reason)
            break
        try:
            fresh_fix_pack = build_fix_pack(
                slice_id=slice_id,
                next_round=round_n + 1,
                original_pack_name=original_pack,
                gate_findings=blocking,
                gate_summary_text="",
            )
        except ValueError as e:
            reason = f"fresh fix pack build: {e}"
            status = "failed"
            _ticket_goal_terminal(goal_id, "failed", reason=reason)
            break
        round_rec["fresh_rework_fix_pack"] = fresh_fix_pack
        pack_for_impl = fresh_fix_pack
        if goal_obj is not None:
            goal_obj = _read_goal(goal_id) or goal_obj
            goal_obj["last_rework_handoff"] = handoff
            goal_obj.pop("pending_rework_mode", None)
            goal_obj.pop("pending_rework_ask_id", None)
            _write_goal(goal_obj)
        print(
            f"[trial-broker] ticket {job['ticket']} fresh rework r{round_n} → "
            f"fix ticket r{round_n + 1} pack={fresh_fix_pack}",
            flush=True,
        )
        round_n += 1

    out_name = f"{_stamp()}-{job['id']}.json"
    out_path = OUTBOX / out_name
    out_body = {
        "status": status,
        "exit_code": ec,
        "job": job,
        "summary_path": str(summary_path),
        "summary_exists": summary_path.is_file(),
        "artifacts": result,
        "rounds": rounds,
        "rounds_completed": len(rounds),
        "finished_at": _iso(),
        "rooms": False,
        "prod_broker_touched": False,
    }
    if reason:
        out_body["reason"] = reason
    notify = str(job.get("notify") or "").strip().lower()
    if notify in ("hub", "khub", "true", "1", "yes"):
        if goal_id:
            # Decision D: a mapped Goal terminal is the single Hub wake.
            out_body["notify_skipped_reason"] = "goal_terminal_notify"
        else:
            out_body["notify"] = maybe_notify_hub(job, out_body, result)
    out_path.write_text(json.dumps(out_body, ensure_ascii=False, indent=2) + "\n")
    maybe_offload_gc(force=True)

    if status == "ok":
        shutil.move(str(dest), str(OUTBOX / dest.name))
        print(
            f"[trial-broker] OK outbox={out_path.name} summary={summary_path} assert_clean={result.get('assert_clean')}",
            flush=True,
        )
        return 0

    shutil.move(str(dest), str(FAILED / dest.name))
    print(
        f"[trial-broker] FAIL status={status} ec={ec} "
        f"summary_exists={summary_path.is_file()} → failed/",
        flush=True,
    )
    return 1 if status == "escalated" else (ec or 1)


def _notify_kinds_since_resume(goal: dict) -> set[str]:
    """Terminal notify kinds already sent in the *current* Goal lifecycle.

    A Goal that failed, was resumed (salvage), then escalated again must still
    wake Hub. Events recorded *before* ``resumed_at`` belong to the previous
    lifecycle and must not suppress the new terminal notify.
    """
    resumed_at = str(goal.get("resumed_at") or "").strip()
    out: set[str] = set()
    for e in ((goal.get("metrics") or {}).get("notify_events") or []):
        if not isinstance(e, dict):
            continue
        kind = e.get("kind")
        if not kind:
            continue
        at = str(e.get("at") or "").strip()
        if resumed_at and at and at < resumed_at:
            continue
        out.add(str(kind))
    return out


def _notify_goal_terminal(goal_id: str) -> None:
    """Notify Hub once for a terminal Goal event (complete / escalated / failed / limit)."""
    goal = _read_goal(goal_id)
    if not goal:
        return
    notified = _notify_kinds_since_resume(goal)
    hit = goal.get("limit_hit")
    status = str(goal.get("status") or "").lower()
    if isinstance(hit, dict) and "dsh-trial-limit" not in notified:
        _notify_goal_event(goal, kind="dsh-trial-limit", ok=False, extra=hit)
        return
    if status == "done" and "dsh-trial-goal-complete" not in notified:
        _notify_goal_event(goal, kind="dsh-trial-goal-complete", ok=True)
    elif status == "closeout" and "dsh-trial-limit" not in notified:
        # Limit-stop closeout (hard cap / max_steps) without a limit_hit dict.
        _notify_goal_event(
            goal,
            kind="dsh-trial-limit",
            ok=False,
            extra={"kind": "limit_closeout", "suggestion": "核对 handoff 后人工收口"},
            reason=str(goal.get("closeout_note") or goal.get("error") or "limit closeout"),
            suggested_action="核对 handoff 后人工收口 / 决定是否续投",
        )
    elif status in ("escalated", "failed", "cancelled") and "dsh-trial-goal-failed" not in notified:
        _notify_goal_event(
            goal,
            kind="dsh-trial-goal-failed",
            ok=False,
            reason=str(
                goal.get("escalate_reason") or goal.get("error") or "goal failed"
            ),
        )


def _slice_specs_from_plan(goal: dict) -> list[dict]:
    """Slice specs from the goal's plan summary. Empty when the plan has none.

    Does not invent ``{goal}-s1`` from a missing block. Callers that need the
    next slice (s2 after a salvaged s1) must read the plan the supervisor
    already emitted.
    """
    path_s = str(goal.get("plan_summary") or "").strip()
    if not path_s:
        return []
    path = Path(path_s)
    if not path.is_file():
        return []
    try:
        parsed = _parse_supervisor_summary(path)
    except OSError:
        return []
    block = parsed.get("block") or {}
    if not isinstance(block, dict):
        return []
    raw = block.get("slices")
    has_list = isinstance(raw, list) and any(isinstance(item, dict) for item in raw)
    has_one = bool(str(block.get("slice") or "").strip())
    if not has_list and not has_one:
        return []
    return _normalize_goal_slices(block, {}, str(goal.get("goal") or ""))


def _classify_failed_goal_resume(goal: dict) -> dict:
    """Whether a failed goal may continue without re-running a PASS slice.

    Allowed only when every non-PASS chain is a missing-summary failure that
    already has gate PASS, and at least one such chain exists. A later slice
    with no chain is the next work. Any real failure (no gate PASS, or any
    other terminal state) refuses the whole resume so it cannot become PASS.
    """
    specs = _slice_specs_from_plan(goal)
    if not specs:
        return {
            "ok": False,
            "reason": "no planned slices in plan summary",
            "salvage": [],
            "specs": [],
        }
    salvage: list[tuple[str, str]] = []
    opened = False
    for spec in specs:
        sid = str(spec["slice"])
        chain = _read_chain(sid)
        if not isinstance(chain, dict):
            opened = True
            continue
        state = str(chain.get("state") or "")
        if state == VERDICT_PASS:
            if opened:
                return {
                    "ok": False,
                    "reason": f"slice {sid} is PASS after an unstarted slice",
                    "salvage": [],
                    "specs": specs,
                }
            continue
        err = str(chain.get("error") or "")
        if state == "failed" and "without summary" in err and not opened:
            ok, reason = _slice_has_review_pass(goal, sid, reload=False)
            if not ok:
                return {
                    "ok": False,
                    "reason": f"slice {sid} missing summary without gate PASS",
                    "salvage": [],
                    "specs": specs,
                }
            salvage.append((sid, reason))
            continue
        return {
            "ok": False,
            "reason": (
                f"slice {sid} state={state or 'unknown'} "
                "is not a salvageable false fail"
            ),
            "salvage": [],
            "specs": specs,
        }
    if not salvage:
        return {
            "ok": False,
            "reason": "no salvageable false-fail slice",
            "salvage": [],
            "specs": specs,
        }
    return {"ok": True, "reason": "", "salvage": salvage, "specs": specs}


def _apply_summary_salvage(goal: dict, slice_id: str, reason: str) -> None:
    """Mark a missing-summary chain PASS. Does not open a ticket or add a round."""
    chain = _read_chain(slice_id)
    if not isinstance(chain, dict):
        return
    chain["state"] = VERDICT_PASS
    chain["error"] = ""
    chain["last_verdict"] = VERDICT_PASS
    chain["salvage_note"] = (
        f"salvaged on resume: missing impl summary after gate PASS ({reason})"
    )
    _write_chain(chain)
    metrics = goal.setdefault("metrics", T.empty_metrics())
    metrics["slices_completed"] = int(metrics.get("slices_completed") or 0) + 1
    _write_goal(goal)


def _resume_job_from_goal(goal: dict) -> dict:
    goal_id = str(goal.get("goal") or "")
    return {
        "id": goal.get("source_job") or f"resume-{goal_id}",
        "type": "goal",
        "goal": goal_id,
        "profile": goal.get("profile") or "acp",
        "gate_profile": goal.get("gate_profile") or goal.get("profile") or "acp",
        "supervisor_profile": goal.get("supervisor_profile") or "acp-lite",
        "cwd": goal.get("cwd") or "/workspace",
        "max_rounds": int(goal.get("max_rounds") or 2),
        "max_supervisor_tickets": goal.get("max_supervisor_tickets") or 8,
        "notify": goal.get("notify"),
        "notify_dry_run": goal.get("notify_dry_run"),
        "notify_prefix": goal.get("notify_prefix"),
        "auto_supervisor_answer": True,
        "on_slice_fail": "stop",
    }


def _continue_salvaged_goal(goal: dict, specs: list[dict], dest: Path) -> int:
    """Run the plan again with skip_passed, so salvaged PASS slices stay closed."""
    plan_ticket = None
    for sid in _goal_slice_ids(goal):
        chain = _read_chain(sid)
        if isinstance(chain, dict) and chain.get("plan_ticket"):
            plan_ticket = str(chain.get("plan_ticket"))
            break
    return _execute_goal_slices(
        goal,
        _resume_job_from_goal(goal),
        specs,
        dest,
        on_slice_fail="stop",
        plan_ticket=plan_ticket,
        skip_passed=True,
    )



def _close_goal_as_done(
    goal: dict,
    *,
    reason: str,
    accept_slices: list[str] | None = None,
) -> None:
    """Close a Goal as done after supervisor/ops ruling (no re-run, no empty PR).

    Marks listed (or all non-PASS terminal) escalated/failed slices as PASS with
    an ops salvage note, writes ``goal-{id}-close.md`` with ``action=goal_done``,
    sets goal status=done, and leaves notify to the caller.
    """
    goal_id = str(goal.get("goal") or "").strip()
    if not goal_id:
        raise ValueError("close_done: missing goal id")
    slice_ids = _goal_slice_ids(goal)
    if not slice_ids:
        raise ValueError(f"close_done: goal {goal_id} has no slices")

    accept_set = {str(s).strip() for s in (accept_slices or []) if str(s).strip()}

    def _matched(sid: str) -> bool:
        if not accept_set:
            return True
        if sid in accept_set:
            return True
        # allow short aliases: "s3" matches "...-s3"
        for a in accept_set:
            if sid.endswith(f"-{a}") or sid.endswith(a):
                return True
        return False

    accepted: list[str] = []
    for sid in slice_ids:
        chain = _read_chain(sid)
        if not isinstance(chain, dict):
            raise ValueError(f"close_done: missing chain for slice {sid}")
        state = str(chain.get("state") or "")
        if state == VERDICT_PASS:
            continue
        if state not in ("escalated", "failed", "cancelled"):
            raise ValueError(
                f"close_done: slice {sid} state={state or 'unknown'} is not terminal"
            )
        if not _matched(sid):
            raise ValueError(
                f"close_done: slice {sid} state={state} not in accept_slices={sorted(accept_set)}"
            )
        chain["state"] = VERDICT_PASS
        chain["last_verdict"] = VERDICT_PASS
        chain["error"] = ""
        chain["ops_close_done"] = True
        chain["salvage_note"] = (
            f"closed as PASS by goal-update action=done ({reason})"
        )
        _write_chain(chain)
        accepted.append(sid)
        metrics = goal.setdefault("metrics", T.empty_metrics())
        metrics["slices_completed"] = int(metrics.get("slices_completed") or 0) + 1

    # All slices must now be PASS.
    for sid in slice_ids:
        chain = _read_chain(sid)
        if not isinstance(chain, dict) or str(chain.get("state") or "") != VERDICT_PASS:
            st = (chain or {}).get("state") if isinstance(chain, dict) else "missing"
            raise ValueError(f"close_done: slice {sid} still {st} after accept")

    close_path = SUMMARIES / f"goal-{goal_id}-close.md"
    body = (
        f"# Supervisor/ops close · {goal_id}\n\n"
        f"Closed as **done** via goal-update `action=done`.\n\n"
        f"- Reason: {reason}\n"
        f"- Accepted slices (ops PASS): {accepted or '(none — already PASS)'}\n"
        f"- All slices: {slice_ids}\n"
        f"- No empty PR / empty commit created.\n\n"
        f"```json\n"
        + json.dumps(
            {
                "action": "goal_done",
                "goal": goal_id,
                "goal_status": "done",
                "report": reason,
                "slices": slice_ids,
                "accepted_slices": accepted,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n```\n"
    )
    close_path.write_text(body, encoding="utf-8")

    goal["status"] = "done"
    goal["last_chain_state"] = VERDICT_PASS
    goal["close_summary"] = str(close_path)
    goal["close_done_at"] = _iso()
    goal["close_done_reason"] = reason
    goal["close_done_accepted_slices"] = accepted
    goal["updated_at"] = _iso()
    # Clear residual escalate markers so status/report stay coherent.
    goal.pop("escalate_reason", None)
    _sync_slices_completed(goal)
    _write_goal(goal)


def run_goal_update_job(path: Path, job: dict) -> int:
    """Apply a type:goal-update onto goal state; optionally resume.

    A failed goal resumes only when the failure is a salvageable missing
    summary with gate PASS. That slice is not re-run; the next planned
    slice is. Other failed goals are rejected.
    """
    dest = PROCESSING / f"{_stamp()}-{path.name}"
    shutil.move(str(path), str(dest))
    goal_id = str(job.get("goal") or job.get("id"))
    print(f"[trial-broker] processing GOAL-UPDATE {dest.name} goal={goal_id}", flush=True)

    goal = _read_goal(goal_id)
    if goal is None:
        err = f"no goal state for goal={goal_id}"
        print(f"[trial-broker] FAIL {err}", flush=True)
        (FAILED / f"{dest.stem}.error.json").write_text(
            json.dumps({"error": err, "job": job, "at": _iso()}, ensure_ascii=False, indent=2) + "\n"
        )
        shutil.move(str(dest), str(FAILED / dest.name))
        return 1

    update = {k: job.get(k) for k in T.DEFAULT_LIMITS if job.get(k) is not None}
    update["cancel"] = job.get("cancel")
    update["action"] = job.get("action")
    update["reason"] = job.get("reason")
    T.apply_goal_update(goal, update)

    # If a raise resolves a recorded limit hit, write that down explicitly.
    hit = goal.get("limit_hit")
    bumped = set(goal.get("last_update_fields") or [])
    if isinstance(hit, dict) and hit.get("limit") in bumped:
        resolved = dict(hit)
        resolved["resolved_by"] = "goal-update"
        resolved["resolved_at"] = _iso()
        goal["limit_hit_resolved"] = resolved
        goal.pop("limit_hit", None)
    goal["last_goal_update_job"] = job.get("id")
    # a goal-update job may override where/how terminal notify goes (respect dry-run)
    for _k in ("notify", "notify_dry_run", "notify_prefix"):
        if job.get(_k) is not None:
            goal[_k] = job[_k]
    _write_goal(goal)

    if str(goal.get("status") or "") == "cancelled":
        _notify_goal_terminal(goal_id)
        shutil.move(str(dest), str(OUTBOX / dest.name))
        print(f"[trial-broker] goal {goal_id} cancelled via goal-update", flush=True)
        return 0

    action = str(job.get("action") or "").strip().lower()
    if action in ("done", "close_done", "goal_done"):
        reason = str(
            job.get("reason")
            or job.get("close_reason")
            or "goal-update action=done"
        ).strip()
        raw_accept = job.get("accept_slices") or job.get("slices") or []
        if isinstance(raw_accept, str):
            accept_slices = [raw_accept]
        elif isinstance(raw_accept, list):
            accept_slices = [str(x) for x in raw_accept]
        else:
            accept_slices = []
        try:
            _close_goal_as_done(goal, reason=reason, accept_slices=accept_slices or None)
        except ValueError as e:
            goal = _read_goal(goal_id) or goal
            goal["close_done_rejected"] = str(e)
            _write_goal(goal)
            if dest.exists():
                shutil.move(str(dest), str(FAILED / dest.name))
            print(f"[trial-broker] goal {goal_id} close_done rejected: {e}", flush=True)
            return 1
        _notify_goal_terminal(goal_id)
        if dest.exists():
            shutil.move(str(dest), str(OUTBOX / dest.name))
        print(
            f"[trial-broker] goal {goal_id} closed done via goal-update "
            f"accepted={(_read_goal(goal_id) or {}).get('close_done_accepted_slices')}",
            flush=True,
        )
        return 0

    if job.get("resume") and str(goal.get("status") or "") in (
        "paused", "running", "awaiting_supervisor", "escalated",
    ):
        goal["status"] = "running"
        goal["resumed_at"] = _iso()
        _write_goal(goal)
        return _resume_goal_chain(goal)

    # failed is not a normal resume. Only a missing-summary false fail that
    # already has gate PASS may continue, and only onto slices that are not
    # already PASS. A real failure is rejected.
    if job.get("resume") and str(goal.get("status") or "") == "failed":
        plan = _classify_failed_goal_resume(goal)
        if not plan["ok"]:
            goal["resume_rejected"] = plan["reason"]
            _write_goal(goal)
            if dest.exists():
                shutil.move(str(dest), str(FAILED / dest.name))
            print(
                f"[trial-broker] goal {goal_id} resume rejected: {plan['reason']}",
                flush=True,
            )
            return 1
        for sid, reason in plan["salvage"]:
            goal = _read_goal(goal_id) or goal
            _apply_summary_salvage(goal, sid, reason)
        goal = _read_goal(goal_id) or goal
        goal["status"] = "running"
        goal["resumed_at"] = _iso()
        goal["resume_reason"] = (
            "salvaged missing-summary false fail with gate PASS; "
            "continuing remaining slices without re-running PASS slices"
        )
        _write_goal(goal)
        print(
            f"[trial-broker] goal {goal_id} resume after false-fail salvage "
            f"slices={[sid for sid, _reason in plan['salvage']]}",
            flush=True,
        )
        return _continue_salvaged_goal(goal, plan["specs"], dest)

    shutil.move(str(dest), str(OUTBOX / dest.name))
    print(
        f"[trial-broker] goal {goal_id} updated fields={goal.get('last_update_fields')} "
        f"limits={goal.get('limits_effective')}",
        flush=True,
    )
    return 0


def _resume_goal_chain(goal: dict) -> int:
    """Resume the last slice chain after a goal-update (best-effort)."""
    goal_id = str(goal.get("goal"))
    slices = goal.get("slices") or []
    if not slices:
        print(f"[trial-broker] goal {goal_id} resume: no slices", flush=True)
        return 0
    slice_id = str(slices[-1])
    chain = _read_chain(slice_id)
    if not chain:
        print(f"[trial-broker] goal {goal_id} resume: no chain for {slice_id}", flush=True)
        return 0
    dest = PROCESSING / f"{_stamp()}-resume-{slice_id}.json"
    rounds = chain.get("rounds") or []
    start_round = max(1, int(chain.get("awaiting_answer_for_round") or len(rounds) or 1))
    start_pack = (
        chain.get("reply_pack")
        or chain.get("pending_impl_pack")
        or Path(chain.get("pack") or f"{slice_id}.pack.md").name
    )
    resume_job = {
        "id": f"resume-{slice_id}",
        "type": "chain",
        "slice": slice_id,
        "pack": Path(chain.get("pack") or f"{slice_id}.pack.md").name,
        "acceptance": chain.get("acceptance"),
        "profile": goal.get("profile") or chain.get("profile") or "acp-lite",
        "gate_profile": chain.get("gate_profile") or goal.get("gate_profile") or goal.get("profile") or "acp-lite",
        "cwd": goal.get("cwd") or chain.get("cwd") or "/workspace",
        "max_rounds": int(chain.get("max_rounds") or goal.get("max_rounds") or 2),
        "notify": goal.get("notify"),
        "notify_dry_run": goal.get("notify_dry_run"),
        "notify_prefix": goal.get("notify_prefix"),
        "goal": goal_id,
        "from_goal": True,
        "auto_supervisor_answer": True,
        "defer_supervisor_close": True,
    }
    chain["state"] = "running"
    _write_chain(chain)
    print(
        f"[trial-broker] goal {goal_id} resume chain {slice_id} r{start_round} pack={start_pack}",
        flush=True,
    )
    rc = run_chain_rounds(
        resume_job, chain, dest, start_round=start_round, start_pack=start_pack
    )
    return _settle_parent_goal_after_resume(goal_id, resume_job, slice_id, dest, rc)


def run_job(path: Path) -> int:
    maybe_offload_gc(force=False)
    try:
        job = load_job(path)
    except (ValueError, json.JSONDecodeError, OSError, UnicodeDecodeError) as e:
        # A single malformed inbox file must never take the broker down.
        raw = None
        try:
            if path.suffix == ".json" and path.is_file():
                parsed = json.loads(path.read_text(encoding="utf-8"))
                if isinstance(parsed, dict):
                    raw = parsed
        except (OSError, ValueError, UnicodeDecodeError):
            raw = None
        quarantine_inbox_reject(path, str(e), raw=raw)
        return 1
    jtype = job.get("type") or "ticket"
    if jtype == "goal":
        return run_goal_job(path, job)
    if jtype == "goal-update":
        return run_goal_update_job(path, job)
    if jtype == "chain":
        return run_chain_job(path, job)
    if jtype == "chain-reply":
        return run_chain_reply_job(path, job)
    return run_ticket_job(path, job)


def cmd_status(_: argparse.Namespace) -> int:
    _ensure_dirs()
    pending = [p.name for p in list_pending()]
    out = [n for n in sorted(p.name for p in OUTBOX.iterdir()) if n != "README.md"][:12]
    failed = [p.name for p in FAILED.iterdir() if p.name != "README.md"][:8]
    chains = [p.name for p in CHAINS.iterdir() if p.suffix == ".json"][:12] if CHAINS.is_dir() else []
    goals = [p.name for p in GOALS.iterdir() if p.suffix == ".json"][:12] if GOALS.is_dir() else []
    pidfile = STATE_DIR / "trial-broker.pid"
    pid_alive = False
    pid = None
    if pidfile.is_file():
        try:
            pid = int(pidfile.read_text().strip())
            os.kill(pid, 0)
            pid_alive = True
        except (ValueError, OSError, ProcessLookupError):
            pid_alive = False
    prod_sock = PROD_BROKER / "broker.sock"
    print(f"state_dir={STATE_DIR}")
    print(f"inbox={INBOX} pending={len(pending)} {pending[:8]}")
    print(f"outbox={OUTBOX} recent={out[:8]}")
    print(f"failed={FAILED} {failed}")
    print(f"chains={CHAINS} {chains}")
    print(f"goals={GOALS} {goals}")
    print(f"pidfile={pidfile} pid={pid} alive={pid_alive}")
    print(f"prod_broker_dir={PROD_BROKER} sock_exists={prod_sock.exists()} (coexist OK; we never call it)")
    print(f"open_slice={OPEN_SLICE} exists={OPEN_SLICE.is_file()}")
    print(f"artifacts={ARTIFACT_ROOT}")
    print(f"max_pack_bytes={MAX_PACK_BYTES} max_diff_bytes={MAX_DIFF_BYTES}")
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    _ensure_dirs()
    # Fold terminal chains back onto Goals still marked running. Never
    # resumes them and never touches cancelled / in-flight goals.
    reconcile_terminal_goals_at_start()
    # Drop interim gate_running acks orphaned by a previous crash/restart so
    # their pending asks are re-dispatched instead of being skipped forever.
    try:
        clear_stale_gate_running_acks_at_start()
    except Exception as e:  # never crash startup
        print(f"[trial-broker] stale gate_running ack sweep failed: {e}", flush=True)
    _write_heartbeat(0)
    if not OPEN_SLICE.is_file():
        print(f"missing {OPEN_SLICE}", file=sys.stderr)
        return 2
    if PROD_RESTART.resolve() == Path(sys.argv[0]).resolve():
        print("refusing prod restart path", file=sys.stderr)
        return 2

    if args.once:
        pending = list_pending()
        _write_heartbeat(len(pending))
        if not pending:
            print("[trial-broker] inbox empty", flush=True)
            return 0
        try:
            rc = run_job(pending[0])
        except Exception as e:  # noqa: BLE001 - one bad inbox file must not exit the process
            print(f"[trial-broker] UNEXPECTED run_job error: {e}", flush=True)
            _quarantine_unexpected(pending[0], e)
            rc = 1
        _write_heartbeat(0)
        return rc

    interval = max(5, int(args.poll))
    print(f"[trial-broker] polling every {interval}s inbox={INBOX}", flush=True)
    while True:
        pending = list_pending()
        # Heartbeat for the independent watchdog. The mailbox watcher also
        # refreshes it during long impl tickets; subprocess liveness covers the
        # rest so a healthy long job is not mistaken for a stalled loop.
        _write_heartbeat(len(pending))
        if pending:
            try:
                run_job(pending[0])
            except Exception as e:  # noqa: BLE001 - keep the poll loop alive
                print(f"[trial-broker] UNEXPECTED run_job error: {e}", flush=True)
                _quarantine_unexpected(pending[0], e)
            _write_heartbeat(0)
        else:
            maybe_offload_gc(force=False)
            time.sleep(interval)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--once", action="store_true", help="process one inbox job then exit")
    ap.add_argument("--poll", type=int, default=20, help="poll interval seconds (default 20)")
    ap.add_argument("--status", action="store_true", help="print inbox/outbox/pid status")
    args = ap.parse_args()
    if args.status:
        return cmd_status(args)
    return cmd_run(args)


if __name__ == "__main__":
    raise SystemExit(main())
