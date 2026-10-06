#!/usr/bin/env python3
"""POST a Hermes/dsh completion notice to Knowledge Hub 开发's webhook routine.

Auth: Authorization Bearer from env DSH_KHUB_COMPLETE_WEBHOOK_KEY, or
/home/box/sand-data/box-secrets.json card.DSH_KHUB_COMPLETE_WEBHOOK_KEY.
URL: --url, env DSH_KHUB_COMPLETE_WEBHOOK_URL, or config file
(default /home/box/.dsh/supervisor/khub-complete-webhook.json).

Never prints the secret.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_CONFIG = Path("/home/box/.dsh/supervisor/khub-complete-webhook.json")
BOX_SECRETS = Path("/home/box/sand-data/box-secrets.json")

# Patchable indirections so unit tests never touch the real clock or sleep.
_sleep = time.sleep
_monotonic = time.monotonic

DEFAULT_NOTIFY_RETRIES = 3
DEFAULT_NOTIFY_BACKOFF_SEC = 1.0
DEFAULT_NOTIFY_TOTAL_SEC = 25.0
MIN_ATTEMPT_BUDGET_SEC = 1.0


def _load_url(args: argparse.Namespace) -> str:
    if args.url:
        return args.url.strip()
    env = (os.environ.get("DSH_KHUB_COMPLETE_WEBHOOK_URL") or "").strip()
    if env:
        return env
    cfg_path = Path(args.config)
    if cfg_path.is_file():
        doc = json.loads(cfg_path.read_text())
        url = (doc.get("url") or "").strip()
        if url:
            return url
    raise SystemExit(
        f"missing webhook url (pass --url, set DSH_KHUB_COMPLETE_WEBHOOK_URL, "
        f"or write {cfg_path})"
    )


def _load_key() -> str:
    env = (os.environ.get("DSH_KHUB_COMPLETE_WEBHOOK_KEY") or "").strip()
    if env:
        return env
    if BOX_SECRETS.is_file():
        try:
            card = json.loads(BOX_SECRETS.read_text()).get("card") or {}
            key = (card.get("DSH_KHUB_COMPLETE_WEBHOOK_KEY") or "").strip()
            if key:
                return key
        except (OSError, json.JSONDecodeError) as e:
            raise SystemExit(f"failed reading box-secrets: {e}") from e
    raise SystemExit(
        "missing DSH_KHUB_COMPLETE_WEBHOOK_KEY "
        "(env or box-secrets card.DSH_KHUB_COMPLETE_WEBHOOK_KEY)"
    )


def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def notify_log_path() -> Path:
    """Resolve the jsonl send-log path at call time (env wins over ~/.dsh)."""
    env = (os.environ.get("DSH_KHUB_NOTIFY_LOG") or "").strip()
    if env:
        return Path(env).expanduser()
    return Path.home() / ".dsh" / "logs" / "khub-complete-notify.jsonl"


def append_send_log(rec: dict) -> None:
    """Append one jsonl record; a logging failure must never break the send."""
    try:
        path = notify_log_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except OSError as e:
        print(f"warn: notify log write failed: {e}", file=sys.stderr)


def _payload_meta(payload: dict) -> dict:
    """Audit fields taken from the payload that is actually sent."""
    return {
        "kind": payload.get("kind"),
        "goal": payload.get("goal"),
        "status": payload.get("status"),
        "ticket": payload.get("ticket"),
    }


def _log_rec(meta: dict, event: str, http, attempt: int, ok: bool, error, **extra) -> dict:
    rec = {"at": datetime.now(timezone.utc).isoformat(), "event": event}
    rec.update(meta)
    rec.update({"http": http, "attempt": attempt, "ok": ok, "error": error})
    rec.update(extra)
    return rec


def _attempt_timeout(timeout: float, remaining: float) -> float:
    """urlopen timeout bounded by the remaining total budget."""
    per = min(timeout, remaining)
    return per if per > 0 else timeout


def send_with_retry(url: str, data: bytes, headers: dict, timeout: float, meta: dict) -> int:
    """POST ``data`` with bounded retries + jsonl audit log. Returns 0/1."""
    attempts_max = _env_int("DSH_KHUB_NOTIFY_RETRIES", DEFAULT_NOTIFY_RETRIES)
    if attempts_max < 1:
        attempts_max = 1
    backoff = _env_float("DSH_KHUB_NOTIFY_BACKOFF_SEC", DEFAULT_NOTIFY_BACKOFF_SEC)
    if backoff < 0:
        backoff = 0.0
    budget = _env_float("DSH_KHUB_NOTIFY_TOTAL_SEC", DEFAULT_NOTIFY_TOTAL_SEC)

    start = _monotonic()
    attempt = 0
    last_http = None
    last_error = None
    while attempt < attempts_max:
        remaining = budget - (_monotonic() - start)
        if remaining < MIN_ATTEMPT_BUDGET_SEC:
            error = last_error or f"budget exhausted after {attempt} attempt(s)"
            append_send_log(_log_rec(meta, "final", last_http, attempt, False, error,
                                     attempts=attempt, retries=max(attempt - 1, 0),
                                     retryable_exhausted=False))
            print(f"error {error}", file=sys.stderr)
            return 1
        attempt += 1
        per_timeout = _attempt_timeout(timeout, remaining)
        try:
            req = urllib.request.Request(url, data=data, method="POST", headers=headers)
            with urllib.request.urlopen(req, timeout=per_timeout) as resp:
                body = resp.read(4096).decode("utf-8", errors="replace")
                http = getattr(resp, "status", None)
            append_send_log(_log_rec(meta, "attempt", http, attempt, True, None))
            append_send_log(_log_rec(meta, "final", http, attempt, True, None,
                                     attempts=attempt, retries=attempt - 1,
                                     retryable_exhausted=False))
            print(f"ok http={http} bytes={len(body)}")
            if body.strip():
                print(body[:500])
            return 0
        except urllib.error.HTTPError as e:
            http = e.code
            reason = getattr(e, "reason", "")
            last_http = http
            last_error = f"HTTPError {http} {reason}".strip()
            retryable = http in (408, 425, 429) or 500 <= http <= 599
            append_send_log(_log_rec(meta, "attempt", http, attempt, False, last_error))
            if not retryable:
                append_send_log(_log_rec(meta, "final", http, attempt, False, last_error,
                                         attempts=attempt, retries=attempt - 1,
                                         retryable_exhausted=False))
                print(f"error http={http} {reason}", file=sys.stderr)
                try:
                    err_body = e.read(1024).decode("utf-8", errors="replace")
                except Exception:
                    err_body = ""
                if err_body.strip():
                    print(err_body[:500], file=sys.stderr)
                return 1
        except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as e:
            reason = getattr(e, "reason", e)
            last_http = None
            last_error = f"URLError {reason}".strip()
            append_send_log(_log_rec(meta, "attempt", None, attempt, False, last_error))

        if attempt >= attempts_max:
            break
        remaining = budget - (_monotonic() - start)
        if remaining < MIN_ATTEMPT_BUDGET_SEC:
            error = f"budget exhausted after {attempt} attempt(s)"
            append_send_log(_log_rec(meta, "final", last_http, attempt, False, error,
                                     attempts=attempt, retries=attempt - 1,
                                     retryable_exhausted=False))
            print(f"error {error}", file=sys.stderr)
            return 1
        _sleep(backoff * (2 ** (attempt - 1)))

    error = last_error or "request failed"
    append_send_log(_log_rec(meta, "final", last_http, attempt, False, error,
                             attempts=attempt, retries=max(attempt - 1, 0),
                             retryable_exhausted=True))
    print(f"error {error}", file=sys.stderr)
    return 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--url", help="webhook URL override")
    ap.add_argument("--config", default=str(DEFAULT_CONFIG), help="JSON with {url}")
    ap.add_argument("--ticket", default="", help="dsh ACP ticket name")
    ap.add_argument("--room", default="", help="team-rooms room id or name")
    ap.add_argument("--status", default="completed", help="completed|failed|test|…")
    ap.add_argument("--summary", default="", help="short human summary")
    ap.add_argument("--goal", default="", help="optional goal / slice id")
    ap.add_argument("--source", default="hermes", help="payload source (default hermes; legacy dsh ok)")
    ap.add_argument("--kind", default="hermes_complete", help="payload kind")
    ap.add_argument("--json-body", default="", help="raw JSON object string (replaces built payload)")
    ap.add_argument("--dry-run", action="store_true", help="print payload only (no secret)")
    ap.add_argument("--timeout", type=float, default=30.0)
    args = ap.parse_args()

    if args.json_body:
        payload = json.loads(args.json_body)
        if not isinstance(payload, dict):
            raise SystemExit("--json-body must be a JSON object")
    else:
        payload = {
            "source": args.source,
            "kind": args.kind,
            "status": args.status,
            "ticket": args.ticket or None,
            "room": args.room or None,
            "goal": args.goal or None,
            "summary": args.summary or None,
            "at": datetime.now(timezone.utc).isoformat(),
            "host": "box",
        }
        # drop nulls for cleaner body
        payload = {k: v for k, v in payload.items() if v is not None}

    url = _load_url(args)

    if args.dry_run:
        print(json.dumps({"url": url, "payload": payload}, ensure_ascii=False, indent=2))
        append_send_log(_log_rec(_payload_meta(payload), "dry_run", None, 0, True, None))
        return 0

    key = _load_key()
    # Accept either raw token or already "Bearer …"
    auth = key if key.lower().startswith("bearer ") else f"Bearer {key}"

    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Authorization": auth,
        "User-Agent": "khub-dsh-complete-notify/0.1",
    }
    return send_with_retry(url, data, headers, args.timeout, _payload_meta(payload))


if __name__ == "__main__":
    raise SystemExit(main())
