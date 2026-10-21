#!/usr/bin/env python3
"""Broker wiring: paused_* routes to supervisor decision, not closeout."""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_pause", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
# Avoid executing full module side effects on import paths that need dirs —
# trial-broker is heavy; load carefully.
spec.loader.exec_module(tb)


def _patched_dirs(root: Path):
    names = ("goals", "processing", "outbox", "failed", "chains", "packs", "summaries", "artifacts", "inbox")
    dirs = {n: root / n for n in names}
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return mock.patch.multiple(
        tb,
        GOALS=dirs["goals"],
        PROCESSING=dirs["processing"],
        OUTBOX=dirs["outbox"],
        FAILED=dirs["failed"],
        CHAINS=dirs["chains"],
        PACKS=dirs["packs"],
        SUMMARIES=dirs["summaries"],
        ARTIFACT_ROOT=dirs["artifacts"],
        INBOX=dirs["inbox"],
        maybe_offload_gc=lambda **k: None,
    ), dirs


def test_route_pause_continue_same_parks_awaiting(tmp_path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        job = {"goal": "g1", "cwd": str(tmp_path), "profile": "acp-lite"}
        chain = {"slice": "s1", "goal": "g1", "pack": "s1.pack.md", "rounds": []}
        dest = dirs["outbox"]
        with mock.patch.object(tb, "_notify_awaiting_hub"), mock.patch.object(
            tb, "_terminal_outbox", return_value=Path("x.json")
        ) as term, mock.patch.object(tb, "_write_chain"):
            rc = tb.route_pause_decision(
                job,
                chain,
                dest,
                decision="continue_same",
                pause_kind="paused_limit",
                round_n=1,
                pack_for_impl="s1.pack.md",
                goal_obj={"goal": "g1"},
                impl_ticket="impl-s1",
            )
        assert rc == tb.CONTINUE_SAME_INLINE
        assert chain["state"] == "running"
        assert chain["pause_resume_mode"] == "continue_same"
        assert chain["pause_impl_ticket"] == "impl-s1"
        assert chain["inline_continue_same"]["round_n"] == 1
        assert chain.get("awaiting_reason") in (None, "")
        assert "max_rounds_bonus_used" not in chain
        term.assert_not_called()


def test_route_pause_escalate_notifies(tmp_path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        job = {"goal": "g1", "cwd": str(tmp_path)}
        chain = {"slice": "s1", "goal": "g1", "pack": "s1.pack.md"}
        notified = {}

        def _notify(*a, **k):
            notified["reason"] = k.get("reason")

        with mock.patch.object(tb, "_notify_awaiting_hub", side_effect=_notify), mock.patch.object(
            tb, "_terminal_outbox", return_value=Path("x.json")
        ), mock.patch.object(tb, "_write_chain"):
            rc = tb.route_pause_decision(
                job,
                chain,
                dirs["outbox"],
                decision="escalate",
                pause_kind="paused_wallclock",
                round_n=1,
                pack_for_impl="s1.pack.md",
                goal_obj={"goal": "g1"},
            )
        assert rc == 1
        assert chain["state"] == "escalated"
        assert "pause_escalate" in str(notified.get("reason"))


def test_classify_wallclock_marker_line():
    assert (
        tb.PS.classify_pause_reason(log_text="pause_kind=paused_wallclock reason=idle\n")
        == "paused_wallclock"
    )


def test_supervisor_reuse_kwargs_same_goal():
    goal = {"goal": "g1", "supervisor_reuse_ticket": "supervisor-goal-g1"}
    kw = tb._supervisor_reuse_kwargs(goal)
    assert kw["keep_open"] is True
    assert kw["resume"] is True
    assert kw["reuse_ticket"] == "supervisor-goal-g1"


def test_auto_supervisor_pause_writes_pack_and_parses(tmp_path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        goal = {"goal": "g1", "supervisor_ticket_count": 0, "max_supervisor_tickets": 8}
        (dirs["goals"] / "g1.json").write_text(json.dumps(goal), encoding="utf-8")
        job = {"goal": "g1", "cwd": str(tmp_path), "profile": "acp-lite"}
        chain = {"slice": "s1", "goal": "g1", "pack": "s1.pack.md", "supervisor_events": []}

        def fake_run_supervisor_ticket(**kwargs):
            assert kwargs.get("keep_open") is True
            summary = dirs["summaries"] / "s1-sup-pause-r1.md"
            summary.write_text(
                '```json\n{"action":"pause_decision","decision":"close","notes":"ok"}\n```\n',
                encoding="utf-8",
            )
            return 0, {"assert_clean": True}, summary

        with mock.patch.object(tb, "run_supervisor_ticket", side_effect=fake_run_supervisor_ticket), mock.patch.object(
            tb, "_read_goal", return_value=goal
        ), mock.patch.object(tb, "_write_goal"), mock.patch.object(tb, "_write_chain"), mock.patch.object(
            tb, "_bump_supervisor_tickets"
        ), mock.patch.object(tb, "T") as T:
            T.resolve_limits.return_value = {"impl_max_steps": 30, "prompt_idle_timeout_sec": 900, "prompt_timeout_sec": 3600}
            T.load_global_limits.return_value = T.resolve_limits.return_value
            T.DEFAULT_LIMITS = {"impl_max_steps": 30}
            decision = tb.auto_supervisor_pause_decision(
                job,
                chain,
                round_n=1,
                pause_kind="paused_limit",
                goal=goal,
                steps_used=60,
                impl_ticket="impl-s1",
            )
        assert decision == "close"
        pack = dirs["packs"] / "s1-pause-r1.pack.md"
        assert pack.is_file()
        text = pack.read_text(encoding="utf-8")
        assert "continue_same" in text
        assert "禁止 `action=max_rounds_decision`" in text


def test_auto_supervisor_pause_accepts_max_rounds_inplace_alias(tmp_path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        goal = {"goal": "g1", "supervisor_ticket_count": 0, "max_supervisor_tickets": 8}
        job = {"goal": "g1", "cwd": str(tmp_path), "profile": "acp-lite"}
        chain = {"slice": "s1", "goal": "g1", "pack": "s1.pack.md", "supervisor_events": []}

        def fake_run_supervisor_ticket(**kwargs):
            summary = dirs["summaries"] / "s1-sup-pause-r1.md"
            summary.write_text(
                "\n".join(
                    [
                        "```json",
                        json.dumps(
                            {
                                "action": "max_rounds_decision",
                                "slice": "s1",
                                "decision": "inplace",
                                "notes": "reuse session answered with touch schema",
                            }
                        ),
                        "```",
                        "",
                    ]
                ),
                encoding="utf-8",
            )
            return 0, {"assert_clean": True}, summary

        with mock.patch.object(tb, "run_supervisor_ticket", side_effect=fake_run_supervisor_ticket), mock.patch.object(
            tb, "_read_goal", return_value=goal
        ), mock.patch.object(tb, "_write_goal"), mock.patch.object(tb, "_write_chain"), mock.patch.object(
            tb, "_bump_supervisor_tickets"
        ), mock.patch.object(tb, "T") as T:
            T.resolve_limits.return_value = {
                "impl_max_steps": 30,
                "prompt_idle_timeout_sec": 900,
                "prompt_timeout_sec": 3600,
            }
            T.load_global_limits.return_value = T.resolve_limits.return_value
            T.DEFAULT_LIMITS = {"impl_max_steps": 30}
            decision = tb.auto_supervisor_pause_decision(
                job,
                chain,
                round_n=1,
                pause_kind="paused_limit",
                goal=goal,
                steps_used=60,
                impl_ticket="impl-s1",
            )
        assert decision == "continue_same"


if __name__ == "__main__":
    import tempfile
    with tempfile.TemporaryDirectory() as td:
        test_route_pause_continue_same_parks_awaiting(Path(td))
    print("smoke ok")
