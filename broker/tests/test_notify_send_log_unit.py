"""Unit tests for ``bin/khub-dsh-complete-notify.py`` send log + retry + budget.

Fully mocked/tmp-isolated: ``urllib.request.urlopen`` is patched (no network and
no real Hub POST) and the module's ``_sleep`` / ``_monotonic`` hooks are patched
(no real waiting). Every log lands in ``tmp_path``.
"""
from __future__ import annotations

import importlib.util
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

# ROOT is the broker/ dir (same convention as the other broker unit tests).
ROOT = Path(__file__).resolve().parent.parent
MODULE_PATH = ROOT.parent / "bin" / "khub-dsh-complete-notify.py"

spec = importlib.util.spec_from_file_location("khub_complete_notify_mod", MODULE_PATH)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

WEBHOOK_URL = "https://example.invalid/hook"
WEBHOOK_KEY = "sk-test-SECRET-123"


class FakeResp:
    """Minimal urlopen context-manager response."""

    def __init__(self, status: int = 200, body: bytes = b"{}"):
        self.status = status
        self._body = body

    def read(self, n: int = -1) -> bytes:
        return self._body

    def __enter__(self) -> "FakeResp":
        return self

    def __exit__(self, *exc) -> bool:
        return False


class FakeUrlopen:
    """Returns/raises queued results in order and records every call."""

    def __init__(self, results):
        self.results = list(results)
        self.calls = []

    def __call__(self, req, timeout=None):
        self.calls.append({"req": req, "timeout": timeout})
        item = self.results.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


def _http_error(code: int, reason: str) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(WEBHOOK_URL, code, reason, None, None)


def _setup(monkeypatch, tmp_path) -> Path:
    log = tmp_path / "n.jsonl"
    monkeypatch.setenv("DSH_KHUB_NOTIFY_LOG", str(log))
    monkeypatch.setenv("DSH_KHUB_COMPLETE_WEBHOOK_URL", WEBHOOK_URL)
    monkeypatch.setenv("DSH_KHUB_COMPLETE_WEBHOOK_KEY", WEBHOOK_KEY)
    for name in (
        "DSH_KHUB_NOTIFY_RETRIES",
        "DSH_KHUB_NOTIFY_BACKOFF_SEC",
        "DSH_KHUB_NOTIFY_TOTAL_SEC",
    ):
        monkeypatch.delenv(name, raising=False)
    return log


def _install(monkeypatch, results) -> FakeUrlopen:
    fake = FakeUrlopen(results)
    monkeypatch.setattr(urllib.request, "urlopen", fake)
    return fake


def _no_urlopen(monkeypatch) -> None:
    def boom(*a, **k):
        raise AssertionError("urlopen must not be called")

    monkeypatch.setattr(urllib.request, "urlopen", boom)


def _patch_sleep(monkeypatch) -> list:
    slept: list = []
    monkeypatch.setattr(mod, "_sleep", lambda s: slept.append(s))
    return slept


def _run(monkeypatch, argv) -> int:
    monkeypatch.setattr(sys, "argv", ["khub-dsh-complete-notify.py", *argv])
    return mod.main()


def _read_log(path: Path) -> list:
    if not path.exists():
        return []
    return [json.loads(ln) for ln in path.read_text().splitlines() if ln.strip()]


def test_first_attempt_urlerror_then_success(tmp_path, monkeypatch):
    log = _setup(monkeypatch, tmp_path)
    slept = _patch_sleep(monkeypatch)
    fake = _install(monkeypatch, [urllib.error.URLError("boom"), FakeResp(200)])

    assert _run(monkeypatch, ["--ticket", "t-1"]) == 0

    lines = _read_log(log)
    attempts = [r for r in lines if r["event"] == "attempt"]
    assert len(attempts) == 2
    assert attempts[0]["ok"] is False
    assert attempts[0]["http"] is None
    assert "URLError" in attempts[0]["error"]
    assert attempts[1]["ok"] is True
    assert attempts[1]["http"] == 200
    final = [r for r in lines if r["event"] == "final"][-1]
    assert final["ok"] is True
    assert final["attempts"] == 2
    assert final["retries"] == 1
    assert len(slept) == 1
    assert len(fake.calls) == 2


def test_503_retries_exhausted(tmp_path, monkeypatch):
    log = _setup(monkeypatch, tmp_path)
    slept = _patch_sleep(monkeypatch)
    _install(monkeypatch, [_http_error(503, "Service Unavailable") for _ in range(3)])

    assert _run(monkeypatch, []) != 0

    lines = _read_log(log)
    attempts = [r for r in lines if r["event"] == "attempt"]
    assert len(attempts) == 3
    assert all(r["http"] == 503 for r in attempts)
    final = [r for r in lines if r["event"] == "final"][-1]
    assert final["ok"] is False
    assert final["retries"] == 2
    assert final["retryable_exhausted"] is True
    assert len(slept) == 2


def test_400_no_retry(tmp_path, monkeypatch):
    log = _setup(monkeypatch, tmp_path)
    slept = _patch_sleep(monkeypatch)
    fake = _install(monkeypatch, [_http_error(400, "Bad Request")])

    assert _run(monkeypatch, []) != 0

    assert len(fake.calls) == 1
    lines = _read_log(log)
    attempts = [r for r in lines if r["event"] == "attempt"]
    assert len(attempts) == 1
    assert attempts[0]["http"] == 400
    final = [r for r in lines if r["event"] == "final"][-1]
    assert final["ok"] is False
    assert final["retries"] == 0
    assert slept == []


def test_retries_env_one_single_attempt(tmp_path, monkeypatch):
    log = _setup(monkeypatch, tmp_path)
    monkeypatch.setenv("DSH_KHUB_NOTIFY_RETRIES", "1")
    slept = _patch_sleep(monkeypatch)
    fake = _install(
        monkeypatch,
        [urllib.error.URLError("boom"), urllib.error.URLError("boom")],
    )

    assert _run(monkeypatch, []) != 0

    assert len(fake.calls) == 1
    lines = _read_log(log)
    assert len([r for r in lines if r["event"] == "attempt"]) == 1
    assert slept == []


def test_dry_run_logs_once_without_post(tmp_path, monkeypatch):
    log = _setup(monkeypatch, tmp_path)
    _no_urlopen(monkeypatch)

    def _boom_key():
        raise AssertionError("_load_key must not be called during --dry-run")

    monkeypatch.setattr(mod, "_load_key", _boom_key)

    assert _run(monkeypatch, ["--dry-run"]) == 0

    lines = _read_log(log)
    assert len(lines) == 1
    assert lines[0]["event"] == "dry_run"
    assert lines[0]["http"] is None
    assert lines[0]["ok"] is True
    assert lines[0]["attempt"] == 0


def test_json_body_meta_carried_into_log(tmp_path, monkeypatch):
    log = _setup(monkeypatch, tmp_path)
    _patch_sleep(monkeypatch)
    _install(monkeypatch, [FakeResp(200)])
    body = json.dumps(
        {
            "kind": "dsh-trial-resumed-after-freeze",
            "goal": "*",
            "status": "resumed-after-freeze",
        }
    )

    assert _run(monkeypatch, ["--json-body", body]) == 0

    lines = _read_log(log)
    assert lines
    for rec in lines:
        assert rec["kind"] == "dsh-trial-resumed-after-freeze"
        assert rec["goal"] == "*"
        assert rec["status"] == "resumed-after-freeze"


def test_no_secrets_in_log(tmp_path, monkeypatch):
    log = _setup(monkeypatch, tmp_path)
    monkeypatch.setenv("DSH_KHUB_NOTIFY_RETRIES", "2")
    _patch_sleep(monkeypatch)
    _install(
        monkeypatch,
        [urllib.error.URLError("boom"), _http_error(500, "Server Error")],
    )

    assert _run(monkeypatch, ["--ticket", "secret-check"]) != 0

    text = log.read_text()
    for needle in ("sk-test-SECRET-123", "Bearer", "Authorization"):
        assert needle not in text


def test_budget_stops_early(tmp_path, monkeypatch):
    log = _setup(monkeypatch, tmp_path)
    monkeypatch.setenv("DSH_KHUB_NOTIFY_TOTAL_SEC", "2.0")
    slept = _patch_sleep(monkeypatch)
    clock = {"t": 100.0}
    monkeypatch.setattr(mod, "_monotonic", lambda: clock["t"])

    def slow_urlopen(req, timeout=None):
        clock["t"] += 5.0
        raise urllib.error.URLError("timed out")

    monkeypatch.setattr(urllib.request, "urlopen", slow_urlopen)

    assert _run(monkeypatch, []) != 0

    lines = _read_log(log)
    assert len([r for r in lines if r["event"] == "attempt"]) == 1
    final = [r for r in lines if r["event"] == "final"][-1]
    assert final["ok"] is False
    assert "budget" in (final["error"] or "").lower()
    assert slept == []


def test_log_dir_auto_created(tmp_path, monkeypatch):
    nested = tmp_path / "a" / "b" / "n.jsonl"
    monkeypatch.setenv("DSH_KHUB_NOTIFY_LOG", str(nested))
    monkeypatch.setenv("DSH_KHUB_COMPLETE_WEBHOOK_URL", WEBHOOK_URL)
    monkeypatch.setenv("DSH_KHUB_COMPLETE_WEBHOOK_KEY", WEBHOOK_KEY)
    _patch_sleep(monkeypatch)
    _install(monkeypatch, [FakeResp(200)])

    assert _run(monkeypatch, []) == 0

    assert nested.exists()
    events = [json.loads(ln)["event"] for ln in nested.read_text().splitlines() if ln.strip()]
    assert events == ["attempt", "final"]
