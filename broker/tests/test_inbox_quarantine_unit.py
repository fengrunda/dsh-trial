"""Inbox quarantine hardening — one bad inbox file must never kill the broker.

Locks the intake guard added for the Hub drop that omitted ``type:goal``:
``load_job`` still raises, but ``run_job`` quarantines + notifies instead of
re-raising, and ``cmd_run`` survives an unexpected ``run_job`` blow-up.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_quarantine", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

REJECT_KIND = "dsh-trial-inbox-reject"


def _dirs(root: Path):
    names = ("goals", "chains", "packs", "summaries", "artifacts", "processing", "outbox",
             "failed", "inbox", "mailbox", "state")
    d = {n: root / n for n in names}
    for p in d.values():
        p.mkdir(parents=True, exist_ok=True)
    patch = mock.patch.multiple(
        tb,
        GOALS=d["goals"], CHAINS=d["chains"], PACKS=d["packs"], SUMMARIES=d["summaries"],
        ARTIFACT_ROOT=d["artifacts"], PROCESSING=d["processing"], OUTBOX=d["outbox"],
        FAILED=d["failed"], INBOX=d["inbox"], MAILBOX=d["mailbox"],
        STATE_DIR=d["state"], HEARTBEAT=d["state"] / "trial-broker.heartbeat.json",
    )
    return patch, d


def _capture(calls: list[dict]):
    def fake_notify(job, out_body, result, *, kind, extra_summary="", payload_extra=None):
        calls.append({
            "job": job, "out_body": out_body, "result": result, "kind": kind,
            "summary": extra_summary, "payload_extra": payload_extra,
        })
        return {"sent": True}

    return fake_notify


# --- (a) missing type + no ticket/pack ---------------------------------------

def test_run_job_missing_type_quarantines_and_notifies(tmp_path):
    patch, d = _dirs(tmp_path)
    bad = d["inbox"] / "hub-drop.json"
    bad.write_text(json.dumps({"goal": "g1", "brief": "b"}), encoding="utf-8")
    calls: list[dict] = []

    with patch, \
            mock.patch.object(tb, "maybe_offload_gc", return_value=None), \
            mock.patch.object(tb, "maybe_notify_hub", side_effect=_capture(calls)):
        rc = tb.run_job(bad)

    assert rc == 1
    assert not bad.exists()
    assert (d["failed"] / "hub-drop.json").is_file()

    err = json.loads((d["failed"] / "hub-drop.error.json").read_text(encoding="utf-8"))
    assert "ticket/pack" in err["error"]
    assert err["original_name"] == "hub-drop.json"
    assert err["goal"] == "g1"
    assert "at" in err
    # No full raw body / secrets dumped.
    assert "brief" not in err

    assert len(calls) == 1
    assert calls[0]["kind"] == REJECT_KIND
    assert calls[0]["out_body"]["status"] == "failed"
    assert calls[0]["result"]["ticket"] == "hub-drop"
    assert calls[0]["job"]["notify"] == "hub"
    assert calls[0]["payload_extra"]["suggested_action"]
    assert "inbox reject" in calls[0]["summary"]


# --- (b) invalid JSON --------------------------------------------------------

def test_run_job_invalid_json_quarantines_and_notifies(tmp_path):
    patch, d = _dirs(tmp_path)
    bad = d["inbox"] / "broken.json"
    bad.write_text('{"goal": "g1", "brief": ', encoding="utf-8")
    calls: list[dict] = []

    with patch, \
            mock.patch.object(tb, "maybe_offload_gc", return_value=None), \
            mock.patch.object(tb, "maybe_notify_hub", side_effect=_capture(calls)):
        rc = tb.run_job(bad)

    assert rc == 1
    assert not bad.exists()
    assert (d["failed"] / "broken.json").is_file()

    err = json.loads((d["failed"] / "broken.error.json").read_text(encoding="utf-8"))
    assert err["error"]
    assert err["original_name"] == "broken.json"

    assert len(calls) == 1
    assert calls[0]["kind"] == REJECT_KIND


# --- (c) valid minimal goal is NOT quarantined -------------------------------

def test_valid_goal_not_quarantined(tmp_path):
    patch, d = _dirs(tmp_path)
    good = d["inbox"] / "good-goal.json"
    good.write_text(
        json.dumps({"type": "goal", "goal": "g1", "brief": "do it"}), encoding="utf-8"
    )

    with patch, \
            mock.patch.object(tb, "maybe_offload_gc", return_value=None), \
            mock.patch.object(tb, "run_goal_job", return_value=0) as run_goal, \
            mock.patch.object(tb, "quarantine_inbox_reject") as quarantine:
        rc = tb.run_job(good)

    assert rc == 0
    assert run_goal.called
    assert not quarantine.called
    assert good.is_file()
    assert list(d["failed"].iterdir()) == []


# --- quarantine helper directly: collision + notify failure never raise -------

def test_quarantine_helper_collision_and_notify_failure(tmp_path):
    patch, d = _dirs(tmp_path)
    first = d["inbox"] / "dup.json"
    first.write_text("{}", encoding="utf-8")
    # Pre-existing reject with the same basename forces a stamp-prefixed dest.
    (d["failed"] / "dup.json").write_text("{}", encoding="utf-8")

    with patch, \
            mock.patch.object(tb, "maybe_notify_hub", side_effect=RuntimeError("webhook down")):
        info = tb.quarantine_inbox_reject(first, "boom", raw={"id": "i1", "type": "ticket"})

    assert info["dest"] is not None
    assert Path(info["dest"]).name.endswith("-dup.json")
    assert Path(info["dest"]).is_file()
    assert info["notify"]["sent"] is False
    assert Path(info["error_path"]).is_file()
    err = json.loads(Path(info["error_path"]).read_text(encoding="utf-8"))
    assert err["error"] == "boom"
    assert err["id"] == "i1"


# --- (d) cmd_run --once survives an unexpected run_job error ------------------

def test_cmd_run_once_survives_unexpected_run_job_error(tmp_path):
    patch, d = _dirs(tmp_path)
    bad = d["inbox"] / "boom.json"
    bad.write_text(json.dumps({"ticket": "x", "pack": "p"}), encoding="utf-8")
    args = argparse.Namespace(once=True, poll=20)

    with patch, \
            mock.patch.object(tb, "reconcile_terminal_goals_at_start", return_value=None), \
            mock.patch.object(tb, "run_job", side_effect=RuntimeError("kaboom")), \
            mock.patch.object(tb, "maybe_notify_hub", return_value={"sent": True}):
        rc = tb.cmd_run(args)

    assert rc == 1
    assert not bad.exists()
    assert (d["failed"] / "boom.json").is_file()


# --- (d') poll loop continues after an unexpected run_job error ---------------

def test_cmd_run_poll_survives_unexpected_run_job_error(tmp_path):
    patch, d = _dirs(tmp_path)
    bad = d["inbox"] / "boom2.json"
    bad.write_text(json.dumps({"ticket": "x", "pack": "p"}), encoding="utf-8")
    args = argparse.Namespace(once=False, poll=20)
    seen = {"n": 0}

    def fake_pending():
        seen["n"] += 1
        if seen["n"] == 1:
            return [bad]
        raise StopIteration  # bounded escape from the otherwise infinite loop

    with patch, \
            mock.patch.object(tb, "reconcile_terminal_goals_at_start", return_value=None), \
            mock.patch.object(tb, "list_pending", side_effect=fake_pending), \
            mock.patch.object(tb, "run_job", side_effect=RuntimeError("kaboom")), \
            mock.patch.object(tb, "maybe_offload_gc", return_value=None), \
            mock.patch.object(tb, "maybe_notify_hub", return_value={"sent": True}):
        with pytest.raises(StopIteration):
            tb.cmd_run(args)

    assert seen["n"] == 2  # loop reached a second poll after the failure
    assert not bad.exists()
    assert (d["failed"] / "boom2.json").is_file()
