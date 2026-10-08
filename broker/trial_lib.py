"""Shared helpers for broker-dsh-trial: limits, metrics, mailbox, profile map."""
from __future__ import annotations

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Shanghai")

DSH_HOME = Path(os.environ.get("DSH_HOME") or (Path.home() / ".dsh"))
TRIAL_SUP = DSH_HOME / "supervisor" / "trial"
LIMITS_PATH = TRIAL_SUP / "limits.json"
ROUTES_PATH = TRIAL_SUP / "routes.json"
GOALS = DSH_HOME / "supervisor" / "thin-state" / "goals"
# 插件侧固定全局 mailbox：允许 DSH_TRIAL_MAILBOX 覆盖（绝对规范化，resolve 不依赖目录已存在）
MAILBOX = (
    Path(os.environ["DSH_TRIAL_MAILBOX"]).expanduser().resolve()
    if os.environ.get("DSH_TRIAL_MAILBOX")
    else DSH_HOME / "supervisor" / "thin-state" / "mailbox"
)
METRICS_JSONL = DSH_HOME / "supervisor" / "thin-state" / "metrics" / "goals.jsonl"
CHAINS = DSH_HOME / "supervisor" / "thin-state" / "chains"

DEFAULT_LIMITS = {
    "max_slices": 3,
    "max_supervisor_tickets": 8,
    "max_rounds": 2,
    "ask_supervisor_timeout_sec": 600,
    "ask_supervisor_poll_ms": 2000,
    # Hard wall clock for one session/prompt. Idle resets on ACP stdout;
    # this cap still reaps a hung agent so it cannot hold the broker forever.
    "prompt_timeout_sec": 3600,
    # No ACP stdout for this long ends the prompt even if the hard cap remains.
    # Above ask_supervisor_timeout_sec so a full supervisor wait is not a hang.
    "prompt_idle_timeout_sec": 900,
    "inplace_rework_max_prompt": 20000,
    "inplace_rework_max_findings": 3,
    # Soft reference for impl/foreman prompt discipline (default 30).
    # NOT a hard kill: no reliable mid-ticket hook yet; do not bare-kill.
    "impl_max_steps": 30,
    # How many Goal jobs the broker may run at once. T1 only publishes the
    # resolution helper (heartbeat + tests); the T2 scheduler consumes it.
    "max_concurrent_goals": 2,
}

# Product profiles → trial profiles (ask_supervisor installed only here)
PROFILE_MAP = {
    "acp": "acp-trial",
    "acp-lite": "acp-lite-trial",
    "acp-trial": "acp-trial",
    "acp-lite-trial": "acp-lite-trial",
}


def now_iso() -> str:
    return datetime.now(TZ).isoformat(timespec="seconds")


# Wall-clock jump (freeze / suspend / NTP step) detection. Monotonic time does
# not advance while suspended; wall time does, so a positive drift between the
# two clocks is a resume-after-freeze (Addendum ③). Default N = 5 minutes.
FREEZE_JUMP_DEFAULT_SEC = 300
FREEZE_JUMP_ENV = "DSH_TRIAL_FREEZE_JUMP_SEC"


def freeze_jump_threshold() -> float:
    try:
        return float(os.environ.get(FREEZE_JUMP_ENV) or FREEZE_JUMP_DEFAULT_SEC)
    except (TypeError, ValueError):
        return float(FREEZE_JUMP_DEFAULT_SEC)


def wall_clock_drift_sec(prev_wall, prev_mono, now_wall, now_mono) -> float:
    """(wall elapsed) − (monotonic elapsed). Positive ⇒ wall jumped forward."""
    return float(now_wall) - float(prev_wall) - (float(now_mono) - float(prev_mono))


def detect_resumed_after_freeze(
    prev_wall,
    prev_mono,
    now_wall,
    now_mono,
    *,
    threshold_sec: float | None = None,
) -> float | None:
    """Return the forward wall-clock jump in seconds, or ``None`` when normal."""
    threshold = freeze_jump_threshold() if threshold_sec is None else float(threshold_sec)
    drift = wall_clock_drift_sec(prev_wall, prev_mono, now_wall, now_mono)
    if drift >= threshold:
        return drift
    return None


def load_global_limits() -> dict:
    if LIMITS_PATH.is_file():
        try:
            data = json.loads(LIMITS_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                out = dict(DEFAULT_LIMITS)
                out.update({k: data[k] for k in DEFAULT_LIMITS if k in data})
                return _migrate_legacy_prompt_wall(data, out)
        except (OSError, json.JSONDecodeError):
            pass
    return dict(DEFAULT_LIMITS)


def save_global_limits(limits: dict) -> Path:
    TRIAL_SUP.mkdir(parents=True, exist_ok=True)
    body = dict(DEFAULT_LIMITS)
    body.update({k: limits[k] for k in DEFAULT_LIMITS if k in limits})
    body["updated_at"] = now_iso()
    LIMITS_PATH.write_text(json.dumps(body, ensure_ascii=False, indent=2) + "\n")
    return LIMITS_PATH


# T2 scheduler knob: how many Goal jobs may run concurrently. Env wins over
# limits.json so an operator can widen/narrow a running box without editing it.
MAX_CONCURRENT_GOALS_ENV = "TRIAL_BROKER_MAX_CONCURRENT_GOALS"


def max_concurrent_goals(limits: dict | None = None) -> int:
    """Resolve the concurrency cap: env > limits.json > DEFAULT_LIMITS (2).

    Anything unparsable or non-positive falls back to the default, so a typo can
    never wedge the broker into "run nothing".
    """
    default = int(DEFAULT_LIMITS["max_concurrent_goals"])
    raw = os.environ.get(MAX_CONCURRENT_GOALS_ENV)
    if raw is None or str(raw).strip() == "":
        if limits is None:
            limits = load_global_limits()
        raw = (limits or {}).get("max_concurrent_goals")
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


# ---------------------------------------------------------------------------
# T3c: broker heartbeat — status/report 展示并发（slots / queued）
# ---------------------------------------------------------------------------
HEARTBEAT_NAME = "trial-broker.heartbeat.json"


def _heartbeat_candidates() -> list[Path]:
    """Candidate heartbeat files, highest priority first.

    Explicit env first (``TRIAL_BROKER_HEARTBEAT`` = exact file, as the broker
    itself resolves it; ``TRIAL_BROKER_DIR`` = the daemon state dir), then the
    ``bin/dsh-trial:broker_dir`` order (``DSH_TRIAL_ROOT`` checkout → this
    ``broker/`` → legacy box default). The daemon's own default state dir
    (``$DSH_HOME/broker-dsh-trial``, see ``broker/start.sh``) comes last so a
    checkout-local heartbeat always wins over unrelated box state.
    """
    rows: list[Path] = []
    if os.environ.get("TRIAL_BROKER_HEARTBEAT"):
        rows.append(Path(os.environ["TRIAL_BROKER_HEARTBEAT"]).expanduser())
    if os.environ.get("TRIAL_BROKER_DIR"):
        rows.append(Path(os.environ["TRIAL_BROKER_DIR"]).expanduser() / HEARTBEAT_NAME)
    if os.environ.get("DSH_TRIAL_ROOT"):
        rows.append(Path(os.environ["DSH_TRIAL_ROOT"]) / "broker" / HEARTBEAT_NAME)
    rows.append(Path(__file__).resolve().parent / HEARTBEAT_NAME)
    rows.append(Path("/workspace/dsh-trial/broker") / HEARTBEAT_NAME)
    rows.append(DSH_HOME / "broker-dsh-trial" / HEARTBEAT_NAME)
    return rows


def heartbeat_path(path: Path | None = None) -> Path:
    """Resolved heartbeat file: explicit ``path`` wins, else first one on disk."""
    if path is not None:
        return Path(path)
    rows = _heartbeat_candidates()
    for p in rows:
        if p.is_file():
            return p
    return rows[0]  # nothing written yet: report "no heartbeat" at top priority


def read_broker_heartbeat(path: Path | None = None) -> dict:
    """Parsed broker heartbeat JSON; ``{}`` when missing/unreadable/garbage.

    Shape (broker ``_write_heartbeat``): ``pid``, ``at``/``wall``, ``slots``
    (``{slot,goal,cwd,ticket,phase,started_at}``), ``max_concurrent_goals``,
    ``queued`` (``{job,reason}``). Older beats lack the concurrency keys — that
    is not an error, callers just see empty ``slots``/``queued``.
    """
    p = heartbeat_path(path)
    try:
        doc = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return doc if isinstance(doc, dict) else {}


def _heartbeat_wall(hb: dict) -> float | None:
    """Epoch seconds of a beat: ``wall``/``ts``/``time`` first, then ISO ``at``."""
    for key in ("wall", "ts", "time"):
        try:
            return float(hb.get(key))
        except (TypeError, ValueError):
            continue
    at = hb.get("at")
    if isinstance(at, str) and at.strip():
        try:
            return datetime.fromisoformat(at.strip()).timestamp()
        except ValueError:
            return None
    return None


def heartbeat_summary(hb: dict | None, now: float | None = None) -> dict:
    """Normalised view of a heartbeat for status/report.

    Returns ``{pid, heartbeat_age, max_concurrent_goals, slots, queued}``.
    An empty/legacy heartbeat yields ``None``/``[]`` values (not an error), so
    callers can print "无心跳" instead of raising.
    """
    doc = hb if isinstance(hb, dict) else {}
    slots = [s for s in (doc.get("slots") or []) if isinstance(s, dict)]
    queued: list[dict] = []
    for item in doc.get("queued") or []:
        if isinstance(item, dict):
            row = dict(item)
            row["name"] = str(item.get("name") or item.get("job") or item.get("label") or "")
            row["reason"] = str(item.get("reason") or "")
            queued.append(row)
        else:
            queued.append({"name": str(item), "reason": ""})

    def _num(key: str) -> float | None:
        try:
            return float(doc.get(key))
        except (TypeError, ValueError):
            return None

    wall = _heartbeat_wall(doc)
    age = None if wall is None else max(0.0, (time.time() if now is None else now) - wall)
    cap = _num("max_concurrent_goals")
    pid = _num("pid")
    return {
        "pid": int(pid) if pid is not None else None,
        "heartbeat_age": age,
        "max_concurrent_goals": int(cap) if cap is not None else None,
        "slots": slots,
        "queued": queued,
    }


# Box limits.json written before idle timeouts pinned this whole-prompt wall.
LEGACY_PROMPT_WALL_SEC = 1800


def _migrate_legacy_prompt_wall(file_data: dict, out: dict) -> dict:
    """1800 with no idle key is the old wall clock, not an intentional hard cap.

    Set prompt_idle_timeout_sec in limits.json to keep a shorter hard cap.
    """
    if "prompt_idle_timeout_sec" in file_data:
        return out
    try:
        pinned = int(file_data.get("prompt_timeout_sec"))
    except (TypeError, ValueError):
        return out
    if pinned == LEGACY_PROMPT_WALL_SEC:
        out["prompt_timeout_sec"] = DEFAULT_LIMITS["prompt_timeout_sec"]
    return out


def load_routes(path: Path | None = None) -> dict:
    """Load ~/.dsh/supervisor/trial/routes.json (to_role+kind → handler)."""
    p = path or ROUTES_PATH
    if p.is_file():
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("routes"), list):
                return data
        except (OSError, json.JSONDecodeError):
            pass
    return {"version": 1, "routes": []}


def find_route(to_role: str | None, kind: str | None, routes: dict | list | None = None) -> dict | None:
    """Return the route entry matching (to_role, kind). Empty to_role matches by kind only."""
    if routes is None:
        routes = load_routes()
    items = routes.get("routes") if isinstance(routes, dict) else routes
    kind_s = str(kind or "").strip()
    role_s = str(to_role or "").strip()
    for r in items or []:
        if not isinstance(r, dict):
            continue
        if str(r.get("kind") or "") != kind_s:
            continue
        if role_s and str(r.get("to_role") or "") != role_s:
            continue
        return r
    return None


def map_profile(name: str | None) -> str:
    n = str(name or "acp-lite")
    return PROFILE_MAP.get(n, n)


def resolve_limits(job: dict | None = None, goal: dict | None = None) -> dict:
    """Priority: goal.limits_effective (after updates) > job overrides > global defaults.

    Returns dict with values plus `_sources` map field→default|job|goal|update.
    """
    base = load_global_limits()
    sources = {k: "default" for k in DEFAULT_LIMITS}
    effective = dict(base)

    job = job or {}
    for k in DEFAULT_LIMITS:
        if k in job and job[k] is not None:
            effective[k] = job[k]
            sources[k] = "job"

    if goal:
        # explicit overrides stored on goal
        go = goal.get("limits_overrides") or {}
        for k in DEFAULT_LIMITS:
            if k in go and go[k] is not None:
                effective[k] = go[k]
                sources[k] = goal.get("limits_override_via") or "goal"
        # after goal-update, limits_effective is authoritative if present
        le = goal.get("limits_effective")
        if isinstance(le, dict):
            for k in DEFAULT_LIMITS:
                if k in le and le[k] is not None:
                    effective[k] = le[k]
                    sources[k] = (goal.get("limits_sources") or {}).get(k) or sources[k]

    # coerce ints
    for k in DEFAULT_LIMITS:
        try:
            effective[k] = type(DEFAULT_LIMITS[k])(effective[k])
        except (TypeError, ValueError):
            effective[k] = DEFAULT_LIMITS[k]
            sources[k] = "default"
    effective["_sources"] = sources
    return effective


def apply_goal_update(goal: dict, update: dict) -> dict:
    """Apply type:goal-update fields onto goal. Supports cancel + limit bumps."""
    if update.get("cancel") or str(update.get("action") or "").lower() in ("cancel", "abort"):
        goal["status"] = "cancelled"
        goal["cancel_reason"] = update.get("reason") or "goal-update cancel"
        goal["cancelled_at"] = now_iso()
        return goal

    overrides = dict(goal.get("limits_overrides") or {})
    sources = dict(goal.get("limits_sources") or {})
    changed = []
    for k in DEFAULT_LIMITS:
        if k in update and update[k] is not None:
            overrides[k] = type(DEFAULT_LIMITS[k])(update[k])
            sources[k] = "update"
            changed.append(k)
    goal["limits_overrides"] = overrides
    goal["limits_sources"] = sources
    # recompute effective from global + job snapshot + overrides
    job_snap = goal.get("job_limits_snapshot") or {}
    merged_job = {**job_snap, **overrides}
    eff = resolve_limits(merged_job, None)
    # force override sources
    for k in changed:
        eff[k] = overrides[k]
        eff["_sources"][k] = "update"
    goal["limits_effective"] = {k: eff[k] for k in DEFAULT_LIMITS}
    goal["limits_sources"] = eff["_sources"]
    goal["last_update_at"] = now_iso()
    goal["last_update_fields"] = changed
    goal.setdefault("updates", []).append({
        "at": now_iso(),
        "fields": changed,
        "payload": {k: update[k] for k in changed},
    })
    return goal


def empty_metrics() -> dict:
    return {
        "slices_planned": 0,
        "slices_completed": 0,
        "tickets": [],
        "by_role": {
            "supervisor": {"count": 0, "peak_sum": 0, "steps_sum": 0, "prompt_token_total": 0},
            "impl": {"count": 0, "peak_sum": 0, "steps_sum": 0, "prompt_token_total": 0},
            "gate": {"count": 0, "peak_sum": 0, "steps_sum": 0, "prompt_token_total": 0},
        },
        "by_kind": {},
        "rework_rounds": 0,
        "hold_count": 0,
        "ask_count": 0,
        "ask_wait_ms_total": 0,
        "ask_sync_ok": 0,
        "ask_timeout": 0,
        "review_submit_count": 0,
        "rework_inplace": {"count": 0, "steps_sum": 0, "prompt_token_total": 0, "pass_after": 0},
        "rework_fresh": {"count": 0, "steps_sum": 0, "prompt_token_total": 0, "pass_after": 0},
        "limit_hits": [],
        "phase_timings": {},
        "prompt_token_total": 0,
        "notify_events": [],
    }


def decide_rework_mode(
    *,
    usage_prompt: int,
    findings: list,
    limits: dict,
) -> str:
    """Return 'inplace' or 'fresh' for HOLD after submit_for_review."""
    max_prompt = int(limits.get("inplace_rework_max_prompt") or 20000)
    max_findings = int(limits.get("inplace_rework_max_findings") or 3)
    findings = findings or []
    has_p0 = any(str((f or {}).get("tier") or "").upper() == "P0" for f in findings)
    n = len(findings)
    if has_p0:
        return "fresh"
    if usage_prompt < max_prompt and n <= max_findings:
        return "inplace"
    return "fresh"


def record_ticket_metric(goal: dict, row: dict) -> dict:
    """row: role, ticket, slice, exit, peak_prompt, steps, tool_res, assert_clean, prompt_token_total, duration_ms"""
    m = goal.setdefault("metrics", empty_metrics())
    peak = int(row.get("peak_prompt") or 0)
    steps = int(row.get("steps") or 0)
    # user-caring metric: peak * steps as proxy when cumulative unavailable
    # 策略（默认）：未显式给 prompt_token_total 时一律记 peak×steps，保证 report 各角色行非 0
    ptt = int(row.get("prompt_token_total") or (peak * steps if peak and steps else 0))
    row = dict(row)
    row["prompt_token_total"] = ptt
    row["at"] = now_iso()
    m["tickets"].append(row)
    role = str(row.get("role") or "impl")
    br = m["by_role"].setdefault(role, {"count": 0, "peak_sum": 0, "steps_sum": 0, "prompt_token_total": 0})
    br["count"] += 1
    br["peak_sum"] += peak
    br["steps_sum"] += steps
    br["prompt_token_total"] += ptt
    m["prompt_token_total"] = int(m.get("prompt_token_total") or 0) + ptt

    # per-kind rollup: count / wait_ms / timeouts / wake prompt tokens
    kind = str(row.get("kind") or role)
    bk = m.setdefault("by_kind", {}).setdefault(
        kind,
        {"count": 0, "wait_ms_total": 0, "timeouts": 0, "wake_prompt_token_total": 0},
    )
    bk["count"] = int(bk.get("count") or 0) + 1
    bk["wait_ms_total"] = int(bk.get("wait_ms_total") or 0) + int(row.get("wait_ms") or 0)
    if row.get("timeout") or row.get("timed_out"):
        bk["timeouts"] = int(bk.get("timeouts") or 0) + 1
    bk["wake_prompt_token_total"] = int(bk.get("wake_prompt_token_total") or 0) + int(
        row.get("wake_prompt_token_total") or 0
    )
    return goal


def append_metrics_jsonl(goal: dict) -> None:
    METRICS_JSONL.parent.mkdir(parents=True, exist_ok=True)
    line = {
        "at": now_iso(),
        "goal": goal.get("goal"),
        "status": goal.get("status"),
        "metrics": goal.get("metrics"),
        "limits_effective": goal.get("limits_effective"),
        "limit_hit": goal.get("limit_hit"),
        "slices": goal.get("slices"),
        "supervisor_ticket_count": goal.get("supervisor_ticket_count"),
    }
    with METRICS_JSONL.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(line, ensure_ascii=False) + "\n")


def limit_hit_payload(
    *,
    which: str,
    limit: int,
    used: int,
    slice_id: str | None,
    step: str,
    goal_id: str,
    suggestion: str,
) -> dict:
    return {
        "limit": which,
        "limit_value": limit,
        "used": used,
        "slice": slice_id,
        "step": step,
        "goal": goal_id,
        "suggestion": suggestion,
        "at": now_iso(),
    }


def list_pending_asks(mailbox: Path | None = None) -> list[Path]:
    root = mailbox or MAILBOX
    pending = root / "pending"
    if not pending.is_dir():
        return []
    out = []
    for p in sorted(pending.glob("*.json")):
        # skip if answer already exists
        ans = root / "answers" / p.name
        if ans.is_file():
            continue
        out.append(p)
    return out


def write_ask_answer(ask_id: str, answer: str, *, supervisor_ticket: str, mailbox: Path | None = None) -> Path:
    root = mailbox or MAILBOX
    (root / "answers").mkdir(parents=True, exist_ok=True)
    path = root / "answers" / f"{ask_id}.json"
    body = {
        "ask_id": ask_id,
        "answer": answer,
        "supervisor_ticket": supervisor_ticket,
        "answered_at": now_iso(),
    }
    path.write_text(json.dumps(body, ensure_ascii=False, indent=2) + "\n")
    # archive pending
    pending = root / "pending" / f"{ask_id}.json"
    arch = root / "archive"
    arch.mkdir(parents=True, exist_ok=True)
    if pending.is_file():
        try:
            pending.rename(arch / f"{ask_id}.pending.json")
        except OSError:
            pass
    return path


def load_ask(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def aggregate_report(goals_dir: Path | None = None, since: str | None = None) -> dict:
    gdir = goals_dir or GOALS
    goals = []
    if gdir.is_dir():
        for p in sorted(gdir.glob("*.json")):
            try:
                g = json.loads(p.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if since:
                created = str(g.get("created_at") or "")
                if created and created < since:
                    continue
            goals.append(g)

    n = len(goals)
    hold = sum(int((g.get("metrics") or {}).get("hold_count") or 0) for g in goals)
    rework = sum(int((g.get("metrics") or {}).get("rework_rounds") or 0) for g in goals)
    asks = sum(int((g.get("metrics") or {}).get("ask_count") or 0) for g in goals)
    limit_hits = []
    for g in goals:
        for h in (g.get("metrics") or {}).get("limit_hits") or []:
            limit_hits.append({"goal": g.get("goal"), **h})
        if g.get("limit_hit"):
            limit_hits.append({"goal": g.get("goal"), **(g["limit_hit"] if isinstance(g["limit_hit"], dict) else {"limit": g["limit_hit"]})})

    def role_tokens(role: str) -> int:
        return sum(int(((g.get("metrics") or {}).get("by_role") or {}).get(role, {}).get("prompt_token_total") or 0) for g in goals)

    def role_count(role: str) -> int:
        return sum(int(((g.get("metrics") or {}).get("by_role") or {}).get(role, {}).get("count") or 0) for g in goals)

    total_tokens = sum(int((g.get("metrics") or {}).get("prompt_token_total") or 0) for g in goals)
    sup_tokens = role_tokens("supervisor")
    total_tickets = sum(role_count(r) for r in ("supervisor", "impl", "gate"))
    sup_tickets = role_count("supervisor")

    by_kind: dict[str, dict] = {}
    for g in goals:
        for k, v in (((g.get("metrics") or {}).get("by_kind")) or {}).items():
            if not isinstance(v, dict):
                continue
            agg = by_kind.setdefault(
                str(k),
                {"count": 0, "wait_ms_total": 0, "timeouts": 0, "wake_prompt_token_total": 0},
            )
            for f in ("count", "wait_ms_total", "timeouts", "wake_prompt_token_total"):
                agg[f] += int(v.get(f) or 0)

    done = sum(1 for g in goals if g.get("status") == "done")
    failed = sum(1 for g in goals if g.get("status") in ("failed", "escalated", "cancelled"))
    # 打回率 ≈ goals with at least one HOLD / goals that ran gate
    with_hold = sum(1 for g in goals if int((g.get("metrics") or {}).get("hold_count") or 0) > 0)
    gated = sum(
        1
        for g in goals
        if int(((g.get("metrics") or {}).get("by_role") or {}).get("gate", {}).get("count") or 0) > 0
    )

    per_goal = []
    for g in goals:
        m = g.get("metrics") or {}
        per_goal.append({
            "goal": g.get("goal"),
            "status": g.get("status"),
            "slices": len(g.get("slices") or []),
            "hold_count": m.get("hold_count") or 0,
            "rework_rounds": m.get("rework_rounds") or 0,
            "ask_count": m.get("ask_count") or 0,
            "supervisor_tickets": g.get("supervisor_ticket_count") or 0,
            "prompt_token_total": m.get("prompt_token_total") or 0,
            "limit_hit": g.get("limit_hit"),
        })

    def rework_agg(mode: str) -> dict:
        count = steps = tokens = pas = 0
        for g in goals:
            r = ((g.get("metrics") or {}).get(f"rework_{mode}") or {})
            count += int(r.get("count") or 0)
            steps += int(r.get("steps_sum") or 0)
            tokens += int(r.get("prompt_token_total") or 0)
            pas += int(r.get("pass_after") or 0)
        return {
            "count": count,
            "avg_steps": (steps / count) if count else 0.0,
            "avg_prompt_token_total": (tokens / count) if count else 0.0,
            "pass_rate": (pas / count) if count else 0.0,
        }

    # T3c: concurrency snapshot from the broker heartbeat. `concurrency` carries
    # the scheduling view; the full beat (pid/age/queued) travels as `heartbeat`.
    # A missing heartbeat (broker down / legacy beat) leaves slots empty.
    hb_summary = heartbeat_summary(read_broker_heartbeat())
    concurrency = {
        "max_concurrent_goals": hb_summary["max_concurrent_goals"],
        "slots": hb_summary["slots"],
        "running_goals": [
            {"goal": s.get("goal"), "slot": s.get("slot"), "phase": s.get("phase")}
            for s in hb_summary["slots"]
        ],
    }

    return {
        "goals": n,
        "done": done,
        "failed_or_escalated": failed,
        "hold_rate": (with_hold / gated) if gated else 0.0,
        "avg_rework_rounds": (rework / n) if n else 0.0,
        "ask_count": asks,
        "supervisor_ticket_share": (sup_tickets / total_tickets) if total_tickets else 0.0,
        "supervisor_token_share": (sup_tokens / total_tokens) if total_tokens else 0.0,
        "prompt_token_total": total_tokens,
        "limit_hit_count": len(limit_hits),
        "limit_hits": limit_hits,
        "rework_inplace": rework_agg("inplace"),
        "rework_fresh": rework_agg("fresh"),
        "by_kind": by_kind,
        "per_goal": per_goal,
        "concurrency": concurrency,
        "heartbeat": hb_summary,
        "generated_at": now_iso(),
    }


def format_report_zh(rep: dict) -> str:
    lines = [
        f"## dsh-trial 汇总报告（{rep.get('generated_at')}）",
        "",
        f"- Goal 数：{rep['goals']}（done={rep['done']} / failed|escalated|cancelled={rep['failed_or_escalated']}）",
        f"- 打回率（有 HOLD 的 Goal / 有 gate 票的 Goal）：{rep['hold_rate']:.1%}",
        f"- 平均返工轮数：{rep['avg_rework_rounds']:.2f}",
        f"- 提问次数合计：{rep['ask_count']}",
        f"- 监理票占比：{rep['supervisor_ticket_share']:.1%}",
        f"- 监理 token 占比（prompt×步数累计）：{rep['supervisor_token_share']:.1%}",
        f"- 全部 Goal 总 prompt token：{rep['prompt_token_total']}",
        f"- 撞上限次数：{rep['limit_hit_count']}",
        f"- 返工 inplace：次数={rep.get('rework_inplace',{}).get('count',0)} "
        f"平均步数={rep.get('rework_inplace',{}).get('avg_steps',0):.1f} "
        f"平均总token={rep.get('rework_inplace',{}).get('avg_prompt_token_total',0):.0f} "
        f"通过率={rep.get('rework_inplace',{}).get('pass_rate',0):.1%}",
        f"- 返工 fresh：次数={rep.get('rework_fresh',{}).get('count',0)} "
        f"平均步数={rep.get('rework_fresh',{}).get('avg_steps',0):.1f} "
        f"平均总token={rep.get('rework_fresh',{}).get('avg_prompt_token_total',0):.0f} "
        f"通过率={rep.get('rework_fresh',{}).get('pass_rate',0):.1%}",
    ]
    conc = rep.get("concurrency") or {}
    beat = rep.get("heartbeat") or {}
    lines += ["", "### 并发"]
    if beat.get("pid") is None and not conc.get("slots"):
        lines.append("- 无心跳（broker 未运行或心跳不可读）")
    else:
        age = beat.get("heartbeat_age")
        age_s = f"{age:.1f}s" if isinstance(age, (int, float)) else "-"
        cap = conc.get("max_concurrent_goals")
        lines.append(
            f"- broker pid={beat.get('pid')} heartbeat_age={age_s} "
            f"max_concurrent_goals={cap} slots_busy={len(conc.get('slots') or [])}/{cap}"
        )
        for s in conc.get("slots") or []:
            lines.append(
                f"- slot={s.get('slot')} goal={s.get('goal')} phase={s.get('phase')} "
                f"ticket={s.get('ticket')} cwd={s.get('cwd')} since={s.get('started_at')}"
            )
        for q in beat.get("queued") or []:
            lines.append(f"- queued name={q.get('name')} reason={q.get('reason')}")
    lines += [
        "",
        "| Goal | 状态 | 切片 | HOLD | 返工 | 提问 | 监理票 | 总 token | 撞上限 |",
        "|------|------|-----:|-----:|-----:|-----:|-------:|---------:|--------|",
    ]
    for g in rep.get("per_goal") or []:
        hit = g.get("limit_hit")
        hit_s = hit.get("limit") if isinstance(hit, dict) else (hit or "")
        lines.append(
            f"| {g.get('goal')} | {g.get('status')} | {g.get('slices')} | {g.get('hold_count')} | "
            f"{g.get('rework_rounds')} | {g.get('ask_count')} | {g.get('supervisor_tickets')} | "
            f"{g.get('prompt_token_total')} | {hit_s} |"
        )
    if rep.get("by_kind"):
        lines += ["", "### by_kind（按消息 kind）"]
        for k, v in sorted(rep["by_kind"].items()):
            lines.append(
                f"- `{k}`: count={v.get('count')} wait_ms={v.get('wait_ms_total')} "
                f"timeouts={v.get('timeouts')} wake_prompt_tokens={v.get('wake_prompt_token_total')}"
            )
    if rep.get("limit_hits"):
        lines += ["", "### 撞上限明细"]
        for h in rep["limit_hits"]:
            lines.append(f"- `{h.get('goal')}`: {h}")
    return "\n".join(lines) + "\n"
