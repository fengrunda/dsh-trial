"""Unit tests for the completion-notify board-refresh hook.

``bin/khub-dsh-complete-notify.py`` must fire a *detached* board refresh on every
non-dry-run notification (success or failure of the webhook alike), and must be
skippable/isolation-safe: ``DSH_BOARD_REFRESH=0``, any pytest run
(``PYTEST_CURRENT_TEST``) or a missing/non-executable refresh script means no
spawn at all.

``subprocess.Popen`` is always patched here (or points at a tmp stub) so the real
``khub-board-refresh`` is never executed, and no real network call happens.
"""
from __future__ import annotations

import importlib.util
import re
import subprocess
import sys
from pathlib import Path

# ROOT is the broker/ dir (same convention as the other broker unit tests).
ROOT = Path(__file__).resolve().parent.parent
MODULE_PATH = ROOT.parent / "bin" / "khub-dsh-complete-notify.py"

spec = importlib.util.spec_from_file_location("khub_complete_notify_refresh_mod", MODULE_PATH)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

REASON_RE = re.compile(r"^dsh-notify:[^:]*:.*$")


class PopenSpy:
    """Records every ``Popen`` call; optionally raises instead of spawning."""

    def __init__(self, exc: BaseException | None = None):
        self.calls: list[dict] = []
        self.exc = exc

    def __call__(self, *a, **k):
        self.calls.append({"args": a, "kwargs": k})
        if self.exc is not None:
            raise self.exc
        return None

    @property
    def argv(self) -> list[str]:
        return list(self.calls[-1]["args"][0])

    @property
    def kwargs(self) -> dict:
        return self.calls[-1]["kwargs"]


def _enable_spawn(monkeypatch) -> None:
    """Remove both skip switches so a spawn is actually attempted."""
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("DSH_BOARD_REFRESH", raising=False)


def _fake_bin(tmp_path: Path, *, executable: bool = True) -> Path:
    """Land a stub script in tmp_path so nothing real is ever executed."""
    path = tmp_path / "khub-board-refresh-stub"
    path.write_text("#!/bin/sh\nexit 0\n")
    path.chmod(0o755 if executable else 0o644)
    return path


def _use_bin(monkeypatch, tmp_path: Path, *, executable: bool = True) -> Path:
    path = _fake_bin(tmp_path, executable=executable)
    monkeypatch.setenv("DSH_BOARD_REFRESH_BIN", str(path))
    return path


def _spy(monkeypatch, exc: BaseException | None = None) -> PopenSpy:
    spy = PopenSpy(exc)
    monkeypatch.setattr(subprocess, "Popen", spy)
    return spy


# ---------------------------------------------------------------------------
# skip conditions
# ---------------------------------------------------------------------------
def test_skip_when_dsh_board_refresh_is_zero(monkeypatch):
    monkeypatch.setenv("DSH_BOARD_REFRESH", "0")
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    spy = _spy(monkeypatch)

    assert mod._maybe_refresh_board({"kind": "hermes_complete"}) is False

    assert spy.calls == []


def test_skip_under_pytest_by_default(monkeypatch):
    """Inside the suite PYTEST_CURRENT_TEST is present -- so: no spawn."""
    monkeypatch.delenv("DSH_BOARD_REFRESH", raising=False)
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "tests/test_notify_board_refresh_unit.py::x")
    spy = _spy(monkeypatch)

    assert mod._maybe_refresh_board({"kind": "hermes_complete"}) is False

    assert spy.calls == []


def test_skip_when_bin_missing(monkeypatch, tmp_path):
    _enable_spawn(monkeypatch)
    monkeypatch.setenv("DSH_BOARD_REFRESH_BIN", str(tmp_path / "nope-khub-board-refresh"))
    spy = _spy(monkeypatch)

    assert mod._maybe_refresh_board({"kind": "hermes_complete"}) is False

    assert spy.calls == []


def test_skip_when_bin_not_executable(monkeypatch, tmp_path):
    _enable_spawn(monkeypatch)
    _use_bin(monkeypatch, tmp_path, executable=False)
    spy = _spy(monkeypatch)

    assert mod._maybe_refresh_board({"kind": "hermes_complete"}) is False

    assert spy.calls == []


# ---------------------------------------------------------------------------
# the spawn itself
# ---------------------------------------------------------------------------
def test_spawns_with_expected_argv_and_detached_kwargs(monkeypatch, tmp_path):
    _enable_spawn(monkeypatch)
    bin_path = _use_bin(monkeypatch, tmp_path)
    spy = _spy(monkeypatch)
    payload = {"kind": "hermes_complete", "goal": "g-42", "ticket": "t-7"}

    assert mod._maybe_refresh_board(payload) is True

    reason = "dsh-notify:hermes_complete:g-42"
    assert spy.argv == [str(bin_path), "--delay", "20", "--reason", reason]
    assert spy.calls[-1]["args"][0][2] == "20"
    assert REASON_RE.match(reason)
    assert len(reason) <= 120
    assert spy.kwargs["stdin"] is subprocess.DEVNULL
    assert spy.kwargs["stdout"] is subprocess.DEVNULL
    assert spy.kwargs["stderr"] is subprocess.DEVNULL
    assert spy.kwargs["start_new_session"] is True
    assert spy.kwargs["close_fds"] is True


def test_uses_payload_meta_dict_and_falls_back_to_ticket(monkeypatch, tmp_path):
    _enable_spawn(monkeypatch)
    _use_bin(monkeypatch, tmp_path)
    spy = _spy(monkeypatch)
    meta = mod._payload_meta({"kind": "hermes_failed", "ticket": "t-9"})

    assert mod._maybe_refresh_board(meta) is True

    assert spy.argv[4] == "dsh-notify:hermes_failed:t-9"


def test_reason_fields_default_to_dash(monkeypatch, tmp_path):
    _enable_spawn(monkeypatch)
    _use_bin(monkeypatch, tmp_path)
    spy = _spy(monkeypatch)

    assert mod._maybe_refresh_board({}) is True

    assert spy.argv[4] == "dsh-notify:-:-"


def test_reason_is_truncated_to_120_chars(monkeypatch, tmp_path):
    _enable_spawn(monkeypatch)
    _use_bin(monkeypatch, tmp_path)
    spy = _spy(monkeypatch)
    payload = {"kind": "hermes_complete", "goal": "g" * 300, "ticket": "t" * 300}

    assert mod._maybe_refresh_board(payload) is True

    reason = spy.argv[4]
    assert reason.startswith("dsh-notify:hermes_complete:")
    assert len(reason) == 120


def test_popen_oserror_is_swallowed(monkeypatch, tmp_path):
    _enable_spawn(monkeypatch)
    _use_bin(monkeypatch, tmp_path)
    spy = _spy(monkeypatch, exc=OSError("exec format error"))

    assert mod._maybe_refresh_board({"kind": "hermes_complete"}) is False

    assert len(spy.calls) == 1


# ---------------------------------------------------------------------------
# main() integration: dry-run never refreshes, otherwise refresh precedes the key
# ---------------------------------------------------------------------------
def test_main_dry_run_does_not_refresh(monkeypatch, tmp_path, capsys):
    _enable_spawn(monkeypatch)
    calls: list = []
    monkeypatch.setattr(mod, "_maybe_refresh_board", lambda payload: calls.append(payload) or True)
    monkeypatch.setattr(mod, "append_send_log", lambda rec: None)
    monkeypatch.setenv("DSH_KHUB_NOTIFY_LOG", str(tmp_path / "n.jsonl"))
    monkeypatch.setenv("DSH_KHUB_COMPLETE_WEBHOOK_URL", "https://example.invalid/hook")
    monkeypatch.setattr(sys, "argv", ["khub-dsh-complete-notify.py", "--dry-run", "--ticket", "t-1"])

    assert mod.main() == 0

    assert calls == []
    assert '"payload"' in capsys.readouterr().out


def test_main_refreshes_before_loading_key(monkeypatch, tmp_path):
    _enable_spawn(monkeypatch)
    order: list = []
    monkeypatch.setattr(mod, "_maybe_refresh_board", lambda payload: order.append("refresh") or True)
    monkeypatch.setattr(mod, "_load_key", lambda: order.append("key") or "sk-test")
    monkeypatch.setattr(
        mod, "send_with_retry",
        lambda url, data, headers, timeout, meta: order.append("send") or 0,
    )
    monkeypatch.setenv("DSH_KHUB_NOTIFY_LOG", str(tmp_path / "n.jsonl"))
    monkeypatch.setenv("DSH_KHUB_COMPLETE_WEBHOOK_URL", "https://example.invalid/hook")
    monkeypatch.setattr(
        sys, "argv",
        ["khub-dsh-complete-notify.py", "--ticket", "t-1", "--goal", "g-42"],
    )

    assert mod.main() == 0

    assert order == ["refresh", "key", "send"]
