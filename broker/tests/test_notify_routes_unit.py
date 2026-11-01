"""Goal notify routes: hub stays on the legacy webhook; excel uses notify-routes.json.

Unknown route names are skipped (no hub fallback). A missing routes file makes
``--route excel`` fail with an explicit error and does not POST.
"""
from __future__ import annotations

import importlib.util
import json
import sys
import urllib.request
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_notify_routes", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

NOTIFY_PATH = ROOT.parent / "bin" / "khub-dsh-complete-notify.py"
nspec = importlib.util.spec_from_file_location("khub_notify_routes_mod", NOTIFY_PATH)
mod = importlib.util.module_from_spec(nspec)
nspec.loader.exec_module(mod)

EXCEL_URL = "https://example.invalid/excel"
HUB_URL = "https://example.invalid/hub"
EXCEL_KEY = "excel-test-key"
HUB_KEY = "hub-test-key"


def _dirs(root: Path):
    names = ("goals", "chains", "packs", "summaries", "artifacts", "processing", "outbox",
             "failed", "inbox", "mailbox")
    d = {n: root / n for n in names}
    for p in d.values():
        p.mkdir(parents=True, exist_ok=True)
    patch = mock.patch.multiple(
        tb,
        GOALS=d["goals"], CHAINS=d["chains"], PACKS=d["packs"], SUMMARIES=d["summaries"],
        ARTIFACT_ROOT=d["artifacts"], PROCESSING=d["processing"], OUTBOX=d["outbox"],
        FAILED=d["failed"], INBOX=d["inbox"], MAILBOX=d["mailbox"],
    )
    return patch, d


def _routes_file(path: Path, *, url: str = EXCEL_URL) -> Path:
    path.write_text(json.dumps({
        "version": 1,
        "routes": {
            "excel": {
                "url_env": "PI_CB_EXCEL_URL",
                "key_env": "PI_CB_EXCEL_KEY",
                "url": url,
                "note": "example only",
            }
        },
    }), encoding="utf-8")
    return path


def _goal(**over) -> dict:
    goal = {
        "goal": "g1", "status": "failed", "slices": ["g1-s1"],
        "notify": "hub", "metrics": {},
    }
    goal.update(over)
    return goal


class _FakeResp:
    def __init__(self, status: int = 200, body: bytes = b"{}"):
        self.status = status
        self._body = body

    def read(self, n: int = -1) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> bool:
        return False


class _FakeUrlopen:
    def __init__(self, results):
        self.results = list(results)
        self.calls = []

    def __call__(self, req, timeout=None):
        self.calls.append({"req": req, "timeout": timeout})
        item = self.results.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


def _run_notify(monkeypatch, argv) -> int:
    monkeypatch.setattr(sys, "argv", ["khub-dsh-complete-notify.py", *argv])
    return mod.main()


def _isolate_notify_home(monkeypatch, tmp_path) -> Path:
    log = tmp_path / "notify.jsonl"
    monkeypatch.setenv("DSH_KHUB_NOTIFY_LOG", str(log))
    monkeypatch.setattr(mod, "BOX_SECRETS", tmp_path / "no-box-secrets.json")
    monkeypatch.delenv("PI_CB_EXCEL_URL", raising=False)
    monkeypatch.delenv("PI_CB_EXCEL_KEY", raising=False)
    return log


# --- broker: route selection -------------------------------------------------

def test_hub_aliases_and_disabled_do_not_open_routes_file(monkeypatch, tmp_path):
    missing = tmp_path / "missing-routes.json"
    monkeypatch.setenv("DSH_NOTIFY_ROUTES", str(missing))
    assert tb._expand_notify_targets("hub") == ["hub"]
    assert tb._expand_notify_targets("KHUB") == ["hub"]
    assert tb._expand_notify_targets("yes") == ["hub"]
    assert tb._expand_notify_targets(True) == ["hub"]
    assert tb._expand_notify_targets(1) == ["hub"]
    assert tb._expand_notify_targets(["hub", "true", "yes"]) == ["hub"]
    assert tb._expand_notify_targets(None) == []
    assert tb._expand_notify_targets(False) == []
    assert tb._expand_notify_targets("false") == []
    assert tb._expand_notify_targets("no") == []
    assert tb._expand_notify_targets("") == []
    assert not missing.exists()


def test_unknown_route_skipped_no_hub_fallback(tmp_path, monkeypatch, capsys):
    routes = _routes_file(tmp_path / "notify-routes.json")
    monkeypatch.setenv("DSH_NOTIFY_ROUTES", str(routes))
    patch, _d = _dirs(tmp_path)
    goal = _goal(notify="nope")
    calls: list = []
    with patch, mock.patch.object(
        tb, "maybe_notify_hub",
        side_effect=lambda *a, **k: calls.append(k) or {"sent": True},
    ):
        tb._notify_goal_event(goal, kind="dsh-trial-goal-failed", ok=False, reason="boom")
    assert calls == []
    logged = capsys.readouterr().out
    assert "nope" in logged
    assert "unknown" in logged
    assert "not falling back to hub" in logged
    assert goal["metrics"].get("notify_events") in (None, [])


def test_unknown_in_list_skips_only_that_name(tmp_path, monkeypatch, capsys):
    routes = _routes_file(tmp_path / "notify-routes.json")
    monkeypatch.setenv("DSH_NOTIFY_ROUTES", str(routes))
    patch, d = _dirs(tmp_path)
    goal = _goal(notify=["hub", "nope", "excel"])
    (d["goals"] / "g1.json").write_text("{}", encoding="utf-8")
    calls: list = []
    with patch, mock.patch.object(
        tb, "maybe_notify_hub",
        side_effect=lambda *a, **k: calls.append(dict(k)) or {"sent": True, "kind": k.get("kind")},
    ):
        tb._notify_goal_event(goal, kind="dsh-trial-goal-failed", ok=False, reason="boom")
    assert "route" not in calls[0]
    assert calls[1]["route"] == "excel"
    assert "nope" in capsys.readouterr().out
    stored = json.loads((d["goals"] / "g1.json").read_text())
    events = stored["metrics"]["notify_events"]
    assert [e["kind"] for e in events] == ["dsh-trial-goal-failed", "dsh-trial-goal-failed"]
    assert "route" not in events[0]
    assert events[1]["route"] == "excel"
    assert events[0]["info"]["sent"] is True


def test_missing_routes_file_still_attempts_excel(tmp_path, monkeypatch):
    monkeypatch.setenv("DSH_NOTIFY_ROUTES", str(tmp_path / "missing-routes.json"))
    patch, _d = _dirs(tmp_path)
    goal = _goal(notify="excel")
    calls: list = []
    with patch, mock.patch.object(
        tb, "maybe_notify_hub",
        side_effect=lambda *a, **k: calls.append(k) or {"sent": False, "detail": "routes file missing"},
    ):
        tb._notify_goal_event(goal, kind="dsh-trial-goal-failed", ok=False)
    assert len(calls) == 1
    assert calls[0]["route"] == "excel"
    assert calls[0]["kind"] == "dsh-trial-goal-failed"


def test_hub_maybe_notify_argv_unchanged(monkeypatch):
    captured: dict = {}

    def fake_run(cmd, capture_output, text, timeout):
        captured["cmd"] = list(cmd)
        return mock.Mock(returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr(tb.subprocess, "run", fake_run)
    job = {"goal": "g1", "slice": "s1", "notify_dry_run": "true"}
    info = tb.maybe_notify_hub(
        job,
        {"status": "done"},
        {"ticket": "goal-g1"},
        kind="dsh-trial-goal-complete",
        extra_summary="sum",
        payload_extra={"reason": "r", "suggested_action": "none"},
    )
    assert info["sent"] is True
    cmd = captured["cmd"]
    assert cmd[:2] == [sys.executable, str(tb.NOTIFY)]
    assert "--route" not in cmd
    assert "--routes-config" not in cmd
    assert "--dry-run" in cmd
    assert cmd[cmd.index("--kind") + 1] == "dsh-trial-goal-complete"
    body = json.loads(cmd[cmd.index("--json-body") + 1])
    assert body["source"] == "dsh"
    assert body["kind"] == "dsh-trial-goal-complete"
    assert body["status"] == "done"
    assert body["goal"] == "g1"
    assert body["reason"] == "r"
    assert body["suggested_action"] == "none"
    assert "route" not in body

    tb.maybe_notify_hub(
        job, {"status": "done"}, {"ticket": "goal-g1"},
        kind="dsh-trial-goal-complete", route="hub",
        payload_extra={"reason": "r", "suggested_action": "none"},
    )
    assert "--route" not in captured["cmd"]

    tb.maybe_notify_hub(
        job, {"status": "done"}, {"ticket": "goal-g1"},
        kind="dsh-trial-goal-complete", route="excel",
        payload_extra={"reason": "r", "suggested_action": "none"},
    )
    routed = captured["cmd"]
    assert routed[routed.index("--route") + 1] == "excel"
    assert "--routes-config" not in routed


def test_awaiting_hub_excel_passes_route(tmp_path, monkeypatch):
    routes = _routes_file(tmp_path / "notify-routes.json")
    monkeypatch.setenv("DSH_NOTIFY_ROUTES", str(routes))
    patch, d = _dirs(tmp_path)
    goal = _goal(notify="excel", status="running")
    (d["goals"] / "g1.json").write_text(json.dumps(goal), encoding="utf-8")
    calls: list = []
    with patch, mock.patch.object(
        tb, "maybe_notify_hub",
        side_effect=lambda *a, **k: calls.append(k) or {"sent": True},
    ):
        info = tb._notify_awaiting_hub(
            {"slice": "g1-s1"},
            {"slice": "g1-s1", "rounds": []},
            reason="blocked",
            block={"questions": ["q"]},
            goal=goal,
        )
    assert info == {"sent": True}
    assert calls[0]["route"] == "excel"
    assert calls[0]["kind"] == "dsh-trial-awaiting-hub"
    stored = json.loads((d["goals"] / "g1.json").read_text())
    event = stored["metrics"]["notify_events"][-1]
    assert event["route"] == "excel"
    assert event["info"]["sent"] is True


# --- notify script -----------------------------------------------------------

def test_hub_dry_run_ignores_routes_file(monkeypatch, tmp_path, capsys):
    log = _isolate_notify_home(monkeypatch, tmp_path)
    bad = tmp_path / "broken-routes.json"
    bad.write_text("{", encoding="utf-8")
    monkeypatch.setenv("DSH_NOTIFY_ROUTES", str(bad))
    monkeypatch.setenv("DSH_KHUB_COMPLETE_WEBHOOK_URL", HUB_URL)

    def boom(*_a, **_k):
        raise AssertionError("named-route loader must not run for hub")

    monkeypatch.setattr(mod, "_route_spec", boom)
    monkeypatch.setattr(urllib.request, "urlopen", boom)
    assert _run_notify(monkeypatch, ["--dry-run", "--ticket", "t-1", "--kind", "dsh-trial-goal-complete"]) == 0
    out = capsys.readouterr().out
    assert HUB_URL in out
    assert EXCEL_URL not in out
    rec = json.loads(log.read_text().splitlines()[-1])
    assert rec["event"] == "dry_run"
    assert rec["kind"] == "dsh-trial-goal-complete"
    assert "route" not in rec


def test_excel_dry_run_does_not_post(monkeypatch, tmp_path, capsys):
    log = _isolate_notify_home(monkeypatch, tmp_path)
    routes = _routes_file(tmp_path / "notify-routes.json")
    monkeypatch.setenv("DSH_NOTIFY_ROUTES", str(routes))
    monkeypatch.setenv("DSH_KHUB_COMPLETE_WEBHOOK_URL", "https://hub.example.invalid/hook")
    monkeypatch.setenv("DSH_KHUB_COMPLETE_WEBHOOK_KEY", HUB_KEY)
    monkeypatch.setenv("PI_CB_EXCEL_URL", "https://example.invalid/from-env")

    def boom(*_a, **_k):
        raise AssertionError("must not POST or load the hub webhook")

    monkeypatch.setattr(urllib.request, "urlopen", boom)
    monkeypatch.setattr(mod, "_load_url", boom)
    monkeypatch.setattr(mod, "_load_key", boom)
    monkeypatch.setattr(mod, "_maybe_refresh_board", boom)
    rc = _run_notify(monkeypatch, [
        "--route", "excel", "--dry-run",
        "--kind", "dsh-trial-goal-complete",
        "--json-body", json.dumps({
            "source": "dsh",
            "kind": "dsh-trial-goal-complete",
            "status": "done",
            "goal": "g1",
            "reason": "ok",
            "suggested_action": "none",
        }),
    ])
    assert rc == 0
    out = capsys.readouterr().out
    assert "https://example.invalid/from-env" in out
    assert "hub.example.invalid" not in out
    assert HUB_KEY not in out
    payload = json.loads(out)["payload"]
    assert payload["kind"] == "dsh-trial-goal-complete"
    assert payload["suggested_action"] == "none"
    rec = json.loads(log.read_text().splitlines()[-1])
    assert rec["route"] == "excel"
    assert rec["event"] == "dry_run"
    assert HUB_KEY not in log.read_text()


def test_excel_url_falls_back_to_file_when_env_unset(monkeypatch, tmp_path, capsys):
    _isolate_notify_home(monkeypatch, tmp_path)
    routes = _routes_file(tmp_path / "notify-routes.json", url=EXCEL_URL)
    monkeypatch.setenv("DSH_NOTIFY_ROUTES", str(routes))
    monkeypatch.setattr(urllib.request, "urlopen", lambda *a, **k: (_ for _ in ()).throw(AssertionError("post")))
    assert _run_notify(monkeypatch, ["--route", "excel", "--dry-run", "--ticket", "t"]) == 0
    assert EXCEL_URL in capsys.readouterr().out


def test_excel_missing_routes_file_errors(monkeypatch, tmp_path):
    _isolate_notify_home(monkeypatch, tmp_path)
    missing = tmp_path / "missing-routes.json"
    monkeypatch.setenv("DSH_NOTIFY_ROUTES", str(missing))
    monkeypatch.setenv("DSH_KHUB_COMPLETE_WEBHOOK_URL", HUB_URL)
    monkeypatch.setenv("DSH_KHUB_COMPLETE_WEBHOOK_KEY", HUB_KEY)

    def boom(*_a, **_k):
        raise AssertionError("hub webhook must not be used when the routes file is missing")

    monkeypatch.setattr(urllib.request, "urlopen", boom)
    monkeypatch.setattr(mod, "_load_url", boom)
    monkeypatch.setattr(mod, "_load_key", boom)
    with pytest.raises(SystemExit, match="routes file missing") as exc:
        _run_notify(monkeypatch, ["--route", "excel", "--kind", "dsh-trial-goal-failed", "--goal", "g1"])
    text = str(exc.value)
    assert "excel" in text
    assert str(missing) in text
    assert "refusing to fall back to hub webhook" in text
    assert HUB_KEY not in text
    assert HUB_URL not in text


def test_excel_post_uses_route_key_not_hub(monkeypatch, tmp_path, capsys):
    log = _isolate_notify_home(monkeypatch, tmp_path)
    routes = _routes_file(tmp_path / "notify-routes.json", url="https://example.invalid/file-url")
    monkeypatch.setenv("DSH_NOTIFY_ROUTES", str(routes))
    monkeypatch.setenv("PI_CB_EXCEL_URL", EXCEL_URL)
    monkeypatch.setenv("PI_CB_EXCEL_KEY", EXCEL_KEY)
    monkeypatch.setenv("DSH_KHUB_COMPLETE_WEBHOOK_URL", HUB_URL)
    monkeypatch.setenv("DSH_KHUB_COMPLETE_WEBHOOK_KEY", HUB_KEY)
    monkeypatch.setattr(mod, "_load_url", lambda *a, **k: (_ for _ in ()).throw(AssertionError("hub url")))
    monkeypatch.setattr(mod, "_load_key", lambda *a, **k: (_ for _ in ()).throw(AssertionError("hub key")))
    monkeypatch.setattr(mod, "_maybe_refresh_board", lambda *a, **k: (_ for _ in ()).throw(AssertionError("board")))
    fake = _FakeUrlopen([_FakeResp(200, b"{}")])
    monkeypatch.setattr(urllib.request, "urlopen", fake)
    assert _run_notify(monkeypatch, [
        "--route", "excel", "--kind", "dsh-trial-goal-failed", "--status", "failed", "--goal", "g1",
    ]) == 0
    assert len(fake.calls) == 1
    req = fake.calls[0]["req"]
    assert req.full_url == EXCEL_URL
    assert req.get_header("Authorization") == f"Bearer {EXCEL_KEY}"
    blob = capsys.readouterr().out + capsys.readouterr().err + log.read_text()
    assert EXCEL_KEY not in blob
    assert HUB_KEY not in blob
    assert HUB_URL not in blob


def test_excel_key_from_box_secrets_not_logged(monkeypatch, tmp_path, capsys):
    log = _isolate_notify_home(monkeypatch, tmp_path)
    secret = "box-card-excel-secret"
    secrets = tmp_path / "box-secrets.json"
    secrets.write_text(json.dumps({"card": {"PI_CB_EXCEL_KEY": secret}}), encoding="utf-8")
    monkeypatch.setattr(mod, "BOX_SECRETS", secrets)
    routes = _routes_file(tmp_path / "notify-routes.json")
    monkeypatch.setenv("DSH_NOTIFY_ROUTES", str(routes))
    monkeypatch.setenv("PI_CB_EXCEL_URL", EXCEL_URL)
    monkeypatch.delenv("PI_CB_EXCEL_KEY", raising=False)
    fake = _FakeUrlopen([_FakeResp(200, b"{}")])
    monkeypatch.setattr(urllib.request, "urlopen", fake)
    monkeypatch.setattr(mod, "_maybe_refresh_board", lambda *_a, **_k: False)
    assert _run_notify(monkeypatch, ["--route", "excel", "--ticket", "t-1"]) == 0
    assert fake.calls[0]["req"].get_header("Authorization") == f"Bearer {secret}"
    blob = capsys.readouterr().out + capsys.readouterr().err + log.read_text()
    assert secret not in blob


def test_broker_excel_dry_run_script_does_not_need_hub_webhook(tmp_path, monkeypatch):
    routes = _routes_file(tmp_path / "notify-routes.json")
    monkeypatch.setenv("DSH_NOTIFY_ROUTES", str(routes))
    monkeypatch.setenv("DSH_KHUB_NOTIFY_LOG", str(tmp_path / "n.jsonl"))
    monkeypatch.delenv("DSH_KHUB_COMPLETE_WEBHOOK_URL", raising=False)
    monkeypatch.delenv("DSH_KHUB_COMPLETE_WEBHOOK_KEY", raising=False)
    monkeypatch.delenv("PI_CB_EXCEL_KEY", raising=False)
    monkeypatch.setattr(tb, "NOTIFY", NOTIFY_PATH)
    info = tb.maybe_notify_hub(
        {"goal": "g1", "notify_dry_run": True},
        {"status": "done"},
        {"ticket": "goal-g1"},
        kind="dsh-trial-goal-complete",
        extra_summary="sum",
        payload_extra={"reason": "r", "suggested_action": "none"},
        route="excel",
    )
    assert info["sent"] is True
    assert EXCEL_URL in info["detail"]
    assert "khub-complete-webhook" not in info["detail"]


def test_dispatch_event_routes_explicit_notify_and_keeps_unset_on_hub(tmp_path, monkeypatch, capsys):
    routes = _routes_file(tmp_path / "notify-routes.json")
    monkeypatch.setenv("DSH_NOTIFY_ROUTES", str(routes))
    patch, _d = _dirs(tmp_path)
    calls: list = []

    def _capture(*_a, **k):
        calls.append(dict(k))
        return {"sent": True}

    with patch, mock.patch.object(tb, "maybe_notify_hub", side_effect=_capture):
        tb.dispatch_event(
            None, {"notify": "excel", "notify_dry_run": True},
            kind="goal_terminal", goal="g1", slice_id="s1", decision="done",
        )
        tb.dispatch_event(
            None, {"notify": "hub"},
            kind="goal_terminal", goal="g1", slice_id="s1", decision="done",
        )
        tb.dispatch_event(
            None, {},
            kind="gate", goal="g1", slice_id="s1", verdict="PASS",
        )
        tb.dispatch_event(
            None, {"notify": "nope"},
            kind="goal_terminal", goal="g1", slice_id="s1", decision="done",
        )
    assert [c.get("route") for c in calls] == ["excel", None, None]
    assert "route" not in calls[1]
    assert "route" not in calls[2]
    assert calls[0]["kind"] == "goal_terminal"
    assert calls[1]["kind"] == "goal_terminal"
    assert calls[2]["kind"] == "gate"
    logged = capsys.readouterr().out
    assert "nope" in logged
    assert "not falling back to hub" in logged


def test_quarantine_default_stays_hub_and_excel_passes_route(tmp_path, monkeypatch):
    routes = _routes_file(tmp_path / "notify-routes.json")
    monkeypatch.setenv("DSH_NOTIFY_ROUTES", str(routes))
    patch, d = _dirs(tmp_path)
    calls: list = []

    def _capture(*a, **k):
        calls.append((a, dict(k)))
        return {"sent": True, "kind": k.get("kind")}

    hub_file = d["inbox"] / "hub.json"
    hub_file.write_text("{}", encoding="utf-8")
    excel_file = d["inbox"] / "excel.json"
    excel_file.write_text("{}", encoding="utf-8")
    with patch, mock.patch.object(tb, "maybe_notify_hub", side_effect=_capture):
        hub_info = tb.quarantine_inbox_reject(hub_file, "no ticket", raw={"goal": "g1"})
        excel_info = tb.quarantine_inbox_reject(
            excel_file, "no ticket", raw={"goal": "g1", "notify": "excel"},
        )
    assert hub_info["notify"]["sent"] is True
    assert excel_info["notify"]["sent"] is True
    hub_args, hub_kw = calls[0]
    excel_args, excel_kw = calls[1]
    assert "route" not in hub_kw
    assert hub_args[0]["notify"] == "hub"
    assert hub_kw["kind"] == "dsh-trial-inbox-reject"
    assert excel_kw["route"] == "excel"
    assert excel_kw["kind"] == "dsh-trial-inbox-reject"


def test_broker_excel_missing_routes_script_error(tmp_path, monkeypatch):
    missing = tmp_path / "missing-routes.json"
    monkeypatch.setenv("DSH_NOTIFY_ROUTES", str(missing))
    monkeypatch.setenv("DSH_KHUB_NOTIFY_LOG", str(tmp_path / "n.jsonl"))
    monkeypatch.setenv("DSH_KHUB_COMPLETE_WEBHOOK_URL", HUB_URL)
    monkeypatch.setenv("DSH_KHUB_COMPLETE_WEBHOOK_KEY", HUB_KEY)
    monkeypatch.setattr(tb, "NOTIFY", NOTIFY_PATH)
    info = tb.maybe_notify_hub(
        {"goal": "g1"},
        {"status": "failed"},
        {"ticket": "goal-g1"},
        kind="dsh-trial-goal-failed",
        payload_extra={"reason": "r", "suggested_action": "check"},
        route="excel",
    )
    assert info["sent"] is False
    assert info["rc"] != 0
    assert "routes file missing" in info["detail"]
    assert "refusing to fall back to hub webhook" in info["detail"]
    assert "excel" in info["detail"]
    assert HUB_KEY not in info["detail"]
    assert HUB_URL not in info["detail"]
