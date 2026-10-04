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


def _spawn_env(role: str | None, *, ticket: str | None = None, prompt_timeout_sec: int | None = None) -> dict[str, str]:
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
    env["PATH"] = f"{DSH_HOME / 'bin'}:{Path.home() / '.local/bin'}:{env.get('PATH', '')}"
    # ask_supervisor sync wait needs long ACP prompt budget (tool timeoutMs ~ timeoutSec+60s)
    pt = int(prompt_timeout_sec or T.load_global_limits().get("prompt_timeout_sec") or 1800)
    env["DSH_ACP_PROMPT_TIMEOUT"] = str(pt)
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
    slice_id = str(ask.get("slice") or (goal or {}).get("goal") or "ask")
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


def _notify_goal_event(goal: dict, *, kind: str, ok: bool, extra: dict | None = None) -> None:
    """Notify Hub only for Goal complete / escalate / fail / limit (Decision D)."""
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
    summary = (
        f"dsh-trial-goal goal={goal.get('goal')} status={status} kind={kind} "
        f"slices={goal.get('slices')} sup_tickets={goal.get('supervisor_ticket_count')} "
        f"tokens={(goal.get('metrics') or {}).get('prompt_token_total')} "
        f"limit_hit={goal.get('limit_hit')}"
    )
    if extra and extra.get("suggestion"):
        summary += f" suggestion={extra.get('suggestion')}"
    prefix = str(goal.get("notify_prefix") or "").strip()
    if prefix:
        summary = f"{prefix}{summary}"
    info = maybe_notify_hub(job, out_body, result, kind=kind, extra_summary=summary)
    goal.setdefault("metrics", T.empty_metrics()).setdefault("notify_events", []).append({
        "at": _iso(), "kind": kind, "info": info,
    })
    _write_goal(goal)


def _handle_submit_for_review(ask: dict, path: Path, *, default_profile: str, cwd: str, goal: dict | None) -> None:
    """Open delta gate short-ticket; write PASS/HOLD(+rework_mode) answer for submit_for_review."""
    t0 = time.monotonic()  # 处理耗时 → wait_ms / by_kind.wait_ms_total
    ask_id = str(ask.get("ask_id") or path.stem)
    slice_id = str(ask.get("slice") or (goal or {}).get("current_slice") or "slice")
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
            json.dumps({"ask_id": ask_id, "ok": False, "error": str(e), "verdict": "HOLD",
                        "rework_mode": "fresh", "instruction": "pack error — end ticket"}, ensure_ascii=False, indent=2) + "\n"
        )
        return

    gate_ticket = f"gate-trial-{slice_id}-rev{round_n}"
    gate_summary_name = f"{slice_id}-gate-rev{round_n}"
    gate_log = ARTIFACT_ROOT / f"{gate_ticket}.log"
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
    (MAILBOX / "answers" / f"{ask_id}.json").write_text(
        json.dumps(answer, ensure_ascii=False, indent=2) + "\n"
    )
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
    if isinstance(acceptance, list):
        acc_txt = "\n".join(f"- {a}" for a in acceptance)
    else:
        acc_txt = str(acceptance or "")
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

## New diff
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
        prompt_timeout_sec=int(lim.get("prompt_timeout_sec") or 1800),
    )
    # Job timeout must exceed ask wait + work
    ask_to = int(lim.get("ask_supervisor_timeout_sec") or 600)
    job_timeout = int(os.environ.get("TRIAL_BROKER_JOB_TIMEOUT") or (ask_to + int(lim.get("prompt_timeout_sec") or 1800) + 120))
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
        # ACP prompt 预算 DSH_ACP_PROMPT_TIMEOUT / prompt_timeout_sec（默认 1800）须 ≥ timeoutSec + 余量。
        def _watch():
            while not stop.is_set():
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
    """Diff base..head; if no base, use HEAD vs worktree / last commit."""
    try:
        if base:
            cmd = ["git", "diff", f"{base}..{head}"]
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
    acc = _acceptance_text(acceptance)
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

## Diff (base={base or 'HEAD'} .. head={commit or 'HEAD'}; truncated={str(truncated).lower()})
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


def build_fix_pack(
    *,
    slice_id: str,
    next_round: int,
    original_pack_name: str,
    gate_findings: list,
    gate_summary_text: str,
) -> str:
    """Write packs/<slice>-fix-r<N+1>.pack.md = original + gate findings only."""
    orig_path = PACKS / Path(original_pack_name).name
    orig_body = orig_path.read_text(encoding="utf-8") if orig_path.is_file() else "(missing)"
    orig_trunc, _ = _truncate_bytes(orig_body, max(1024, MAX_PACK_BYTES // 2))
    findings_json = json.dumps(gate_findings or [], ensure_ascii=False, indent=2)
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

# Fix pack · {slice_id} · round {next_round}

## Original pack (authoritative intent)
{orig_trunc}

## Gate findings only (no transcripts)
```json
{findings_json}
```

### Gate notes (excerpt)
{(gate_summary_text or '')[:1500]}

## Done when
- Address P0/P1 findings; keep changes minimal.
- Write foreman structured summary (status/changed_files/base/commit/questions/notes).
- No room_*.
"""
    path.write_text(body, encoding="utf-8")
    for _ in range(4):
        try:
            _enforce_pack_bytes(path)
            break
        except ValueError:
            cur = path.read_text(encoding="utf-8")
            cut, _ = _truncate_bytes(cur, MAX_PACK_BYTES - 80)
            path.write_text(cut + "\n\n…[pack truncated to maxPackBytes]\n", encoding="utf-8")
    _enforce_pack_bytes(path)
    return name


def build_reply_addendum_pack(
    *,
    slice_id: str,
    round_n: int,
    prior_pack_name: str,
    answer: str,
) -> str:
    prior = PACKS / Path(prior_pack_name).name
    prior_body = prior.read_text(encoding="utf-8") if prior.is_file() else ""
    prior_trunc, _ = _truncate_bytes(prior_body, max(1024, MAX_PACK_BYTES // 2))
    name = f"{slice_id}-reply-r{round_n}.pack.md"
    path = PACKS / name
    body = f"""---
slice_id: {slice_id}
kind: reply-addendum
round: {round_n}
role: impl
prior_pack: {Path(prior_pack_name).name}
summary_out: $DSH_HOME/supervisor/thin-state/summaries/{slice_id}-impl-r{round_n}.md
max_bytes_hint: {MAX_PACK_BYTES}
---

# Supervisor reply addendum · {slice_id} · round {round_n}

## Supervisor answer
{answer.strip()}

## Prior pack (context)
{prior_trunc}

## Done when
- Incorporate supervisor answer; continue implementation.
- Write foreman structured summary machine block.
- No room_*.
"""
    path.write_text(body, encoding="utf-8")
    _enforce_pack_bytes(path)
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


def maybe_notify_hub(job, out_body, result, *, kind: str = "dsh-trial-complete", extra_summary: str = ""):
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


def _slice_has_review_pass(
    goal: dict | None, slice_id: str, *, reload: bool = False
) -> tuple[bool, str]:
    """True when this slice already has a PASS verdict independent of impl summary.

    Covers two real paths:
      * an inline (mid-ticket) ``submit_for_review`` gate that returned PASS —
        recorded as a ``goal["metrics"]["tickets"]`` row with
        ``kind in ("submit_for_review", "gate")`` and ``verdict == "PASS"``;
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
            if str(row.get("slice") or "") != slice_id:
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
If state/verdict is PASS: set goals/{goal_id}.json status=done and emit action=goal_done.
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


def maybe_supervisor_close(job: dict, chain: dict) -> None:
    goal_id = job.get("goal") or chain.get("goal")
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
    goal["status"] = gstatus
    goal["close_summary"] = str(summary_path)
    goal["last_chain_state"] = chain.get("state")
    _write_goal(goal)
    print(f"[trial-broker] supervisor close {ticket} goal_status={gstatus}", flush=True)



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
            chain["state"] = "failed"
            chain["error"] = f"foreman exit {ec} without summary"
            _write_chain(chain)
            _terminal_outbox(job, chain, dest, ok=False)
            return ec or 1

        status = block.get("status")
        # Decision: HOLD never wakes supervisor (handled below via fix pack).
        # Blocked → awaiting without auto-wake. Question → auto supervisor answer
        # (fallback when ask_supervisor mid-ticket timed out / unused).
        if status == FOREMAN_STATUS_BLOCKED:
            chain["state"] = "awaiting_supervisor"
            chain["awaiting_answer_for_round"] = round_n
            chain["awaiting_reason"] = status
            chain["pending_impl_pack"] = Path(pack_for_impl).name
            _write_chain(chain)
            out = _terminal_outbox(job, chain, dest, ok=True, notify=False)
            print(
                f"[trial-broker] chain {slice_id} → awaiting_supervisor (blocked, no auto-wake) "
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
            out = _terminal_outbox(job, chain, dest, ok=True)
            print(
                f"[trial-broker] chain {slice_id} → awaiting_supervisor "
                f"({status}) outbox={out.name}",
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
    if on_slice_fail not in ("continue", "stop"):
        on_slice_fail = "stop"

    all_pass = True
    rc = 0
    last_chain: dict | None = None
    last_chain_job: dict | None = None
    for spec in slice_specs:
        slice_id = str(spec["slice"])
        pack_name = Path(spec["pack"]).name
        pack_abs = PACKS / pack_name
        if not pack_abs.is_file():
            goal_state["status"] = "failed"
            goal_state["error"] = f"supervisor did not write pack {pack_name}"
            _write_goal(goal_state)
            if dest.exists():
                shutil.move(str(dest), str(FAILED / dest.name))
            print(f"[trial-broker] FAIL missing pack {pack_abs}", flush=True)
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

    try:
        reply_pack = build_reply_addendum_pack(
            slice_id=slice_id,
            round_n=next_round,
            prior_pack_name=prior_pack,
            answer=job["answer"],
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
        "goal": job.get("goal") or slice_id,
    }
    return run_chain_rounds(
        resume_job, chain, dest, start_round=next_round, start_pack=reply_pack
    )


def run_ticket_job(path: Path, job: dict) -> int:
    dest = PROCESSING / f"{_stamp()}-{path.name}"
    shutil.move(str(path), str(dest))
    print(f"[trial-broker] processing {dest.name} ticket={job['ticket']}", flush=True)

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
        return 1

    log_path = ARTIFACT_ROOT / f"{job['ticket']}.log"
    role = job.get("role")
    if not role:
        t = job["ticket"]
        if t.startswith("gate-") or t.startswith("review-"):
            role = "gate"
        else:
            role = "impl"
    summary_name = job.get("summary_name") or pack_name.replace(".pack.md", "")
    ec = run_open_slice(
        ticket=job["ticket"],
        pack_name=pack_name,
        profile=job["profile"],
        cwd=job["cwd"],
        role=str(role),
        summary_name=str(summary_name),
        prompt_mode="baseline",
        log_path=log_path,
    )

    summary_path = SUMMARIES / f"{summary_name}.md"
    result = write_artifacts({**job, "role": role}, log_path, summary_path, ec)

    out_name = f"{_stamp()}-{job['id']}.json"
    out_path = OUTBOX / out_name
    out_body = {
        "status": "ok" if ec == 0 and summary_path.is_file() else "failed",
        "exit_code": ec,
        "job": job,
        "summary_path": str(summary_path),
        "summary_exists": summary_path.is_file(),
        "artifacts": result,
        "finished_at": _iso(),
        "rooms": False,
        "prod_broker_touched": False,
    }
    notify = str(job.get("notify") or "").strip().lower()
    if notify in ("hub", "khub", "true", "1", "yes"):
        out_body["notify"] = maybe_notify_hub(job, out_body, result)
    out_path.write_text(json.dumps(out_body, ensure_ascii=False, indent=2) + "\n")
    maybe_offload_gc(force=True)

    if ec == 0 and summary_path.is_file():
        shutil.move(str(dest), str(OUTBOX / dest.name))
        print(
            f"[trial-broker] OK outbox={out_path.name} summary={summary_path} assert_clean={result.get('assert_clean')}",
            flush=True,
        )
        return 0

    shutil.move(str(dest), str(FAILED / dest.name))
    print(f"[trial-broker] FAIL ec={ec} summary_exists={summary_path.is_file()} → failed/", flush=True)
    return ec or 1


def _notify_goal_terminal(goal_id: str) -> None:
    """Notify Hub once for a terminal Goal event (complete / escalated / failed / limit)."""
    goal = _read_goal(goal_id)
    if not goal:
        return
    notified = {
        e.get("kind")
        for e in ((goal.get("metrics") or {}).get("notify_events") or [])
        if isinstance(e, dict)
    }
    hit = goal.get("limit_hit")
    status = str(goal.get("status") or "").lower()
    if isinstance(hit, dict) and "dsh-trial-limit" not in notified:
        _notify_goal_event(goal, kind="dsh-trial-limit", ok=False, extra=hit)
        return
    if status == "done" and "dsh-trial-goal-complete" not in notified:
        _notify_goal_event(goal, kind="dsh-trial-goal-complete", ok=True)
    elif status in ("escalated", "failed", "cancelled") and "dsh-trial-goal-failed" not in notified:
        _notify_goal_event(goal, kind="dsh-trial-goal-failed", ok=False)


def run_goal_update_job(path: Path, job: dict) -> int:
    """Apply a type:goal-update onto goal state; optionally resume the last chain."""
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

    if job.get("resume") and str(goal.get("status") or "") in (
        "paused", "running", "awaiting_supervisor", "escalated",
    ):
        goal["status"] = "running"
        goal["resumed_at"] = _iso()
        _write_goal(goal)
        return _resume_goal_chain(goal)

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
    }
    chain["state"] = "running"
    _write_chain(chain)
    print(
        f"[trial-broker] goal {goal_id} resume chain {slice_id} r{start_round} pack={start_pack}",
        flush=True,
    )
    return run_chain_rounds(resume_job, chain, dest, start_round=start_round, start_pack=start_pack)


def run_job(path: Path) -> int:
    maybe_offload_gc(force=False)
    job = load_job(path)
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
    if not OPEN_SLICE.is_file():
        print(f"missing {OPEN_SLICE}", file=sys.stderr)
        return 2
    if PROD_RESTART.resolve() == Path(sys.argv[0]).resolve():
        print("refusing prod restart path", file=sys.stderr)
        return 2

    if args.once:
        pending = list_pending()
        if not pending:
            print("[trial-broker] inbox empty", flush=True)
            return 0
        return run_job(pending[0])

    interval = max(5, int(args.poll))
    print(f"[trial-broker] polling every {interval}s inbox={INBOX}", flush=True)
    while True:
        pending = list_pending()
        if pending:
            run_job(pending[0])
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
