"""chain-reply settles the real parent Goal and always notifies terminal.

Regression for engine-obs-llm-empty-groups-v1: Hub sent ``type:chain-reply``
with only ``slice`` + ``answer``, the broker used the slice id as the Goal,
wrote a pseudo ``goals/<slice>.json`` and never woke Hub for the real parent.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location(
    "trial_broker_chain_reply_parent", ROOT / "trial-broker.py"
)
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

T = tb.T
G = "engine-obs-llm-empty-groups-v1"
S = f"{G}-s1"
S2 = f"{G}-s2"


def _patched_dirs(root: Path):
    names = (
        "goals", "processing", "outbox", "failed", "chains",
        "packs", "summaries", "artifacts", "inbox", "mailbox",
    )
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
        MAILBOX=dirs["mailbox"],
        maybe_offload_gc=lambda **k: None,
    ), dirs


def _write_goal(dirs, goal: dict) -> None:
    (dirs["goals"] / f"{goal['goal']}.json").write_text(
        json.dumps(goal, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def _write_chain(dirs, chain: dict) -> None:
    (dirs["chains"] / f"{chain['slice']}.json").write_text(
        json.dumps(chain, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def _read_goal_file(dirs, goal_id: str) -> dict:
    return json.loads((dirs["goals"] / f"{goal_id}.json").read_text(encoding="utf-8"))


def _read_chain_file(dirs, slice_id: str) -> dict:
    return json.loads((dirs["chains"] / f"{slice_id}.json").read_text(encoding="utf-8"))


def _write_summary(path: Path, block: dict, prose: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    head = f"{prose}\n\n" if prose else ""
    path.write_text(
        head + "```json\n" + json.dumps(block, ensure_ascii=False, indent=2) + "\n```\n",
        encoding="utf-8",
    )


def _base_goal(slices: list[str] | None = None) -> dict:
    return {
        "goal": G,
        "status": "running",
        "brief": "x",
        "notify": "hub",
        "notify_dry_run": True,
        "slices": list(slices or [S]),
        "metrics": T.empty_metrics(),
        "supervisor_ticket_count": 0,
        "supervisor_tickets": [],
    }


def _base_chain(cwd: Path, *, state: str = "awaiting_supervisor") -> dict:
    return {
        "slice": S,
        "type": "chain",
        "state": state,
        "goal": G,
        "from_goal": True,
        "pack": f"{S}.pack.md",
        "acceptance": ["a"],
        "profile": "acp-lite",
        "gate_profile": "acp-lite",
        "cwd": str(cwd),
        "max_rounds": 2,
        "rounds": [{"round": 1, "impl_pack": f"{S}.pack.md"}],
        "awaiting_answer_for_round": 1,
        "supervisor_profile": "acp-lite",
        "auto_supervisor_answer": True,
        "max_supervisor_tickets": 8,
        "defer_supervisor_close": True,
    }


def _fixture(dirs, tmp_path: Path) -> tuple[dict, dict]:
    goal = _base_goal()
    chain = _base_chain(tmp_path)
    _write_goal(dirs, goal)
    _write_chain(dirs, chain)
    (dirs["packs"] / f"{S}.pack.md").write_text(
        f"---\nslice_id: {S}\n---\n# dummy pack\n", encoding="utf-8"
    )
    return goal, chain


class _NotifyRecorder:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def __call__(self, job, out_body, result, **kwargs):
        self.calls.append({"job": job, "out_body": out_body, "result": result, **kwargs})
        return {"sent": True, "rc": 0}


def _fake_artifacts(job, log_path, summary_path, ec, ticket=None):
    return {
        "ticket": ticket or job.get("ticket"),
        "exit_code": ec,
        "assert_clean": True,
        "composition": None,
        "summary_exists": summary_path.is_file(),
    }


def _fake_open_slice(**kwargs):
    summary = tb.SUMMARIES / f"{kwargs['summary_name']}.md"
    kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
    kwargs["log_path"].write_text("fake log\n", encoding="utf-8")
    mode = kwargs.get("prompt_mode")
    if mode == "foreman":
        _write_summary(
            summary,
            {
                "status": "done",
                "changed_files": [],
                "branch": "main",
                "commit": "",
                "base": "",
                "questions": [],
                "notes": "done",
            },
        )
    elif mode == "gate":
        _write_summary(summary, {"verdict": "PASS", "findings": []})
    elif mode == "supervisor-close":
        goal_arg = kwargs.get("goal")
        gid = goal_arg.get("goal") if isinstance(goal_arg, dict) else G
        _write_summary(
            summary,
            {"action": "goal_done", "goal": gid, "goal_status": "done"},
        )
    return 0


def _goal_notify_kinds(dirs, goal_id: str) -> list[str]:
    goal = _read_goal_file(dirs, goal_id)
    return [
        str(e.get("kind"))
        for e in ((goal.get("metrics") or {}).get("notify_events") or [])
        if isinstance(e, dict)
    ]


def test_chain_reply_without_goal_closes_real_parent(tmp_path: Path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        _, chain = _fixture(dirs, tmp_path)
        rec = _NotifyRecorder()
        inbox = dirs["inbox"] / "reply.json"
        inbox.write_text(
            json.dumps(
                {
                    "type": "chain-reply",
                    "id": "unit-parent-reply",
                    "slice": S,
                    "answer": "use the plan",
                    "notify_dry_run": True,
                }
            ),
            encoding="utf-8",
        )
        with mock.patch.object(tb, "run_open_slice", side_effect=_fake_open_slice), \
                mock.patch.object(tb, "write_artifacts", side_effect=_fake_artifacts), \
                mock.patch.object(tb, "maybe_notify_hub", side_effect=rec):
            rc = tb.run_job(inbox)

        assert rc == 0
        goal = _read_goal_file(dirs, G)
        assert goal["status"] == "done"
        assert goal["last_chain_state"] == "PASS"
        assert int(goal["metrics"]["slices_completed"]) >= 1
        assert goal.get("close_summary")
        # No pseudo goals/<slice>.json was ever created.
        assert not (dirs["goals"] / f"{S}.json").exists()
        # The close ticket targeted the parent Goal, never the slice.
        events = _read_chain_file(dirs, S).get("supervisor_events") or []
        close_tickets = [e.get("ticket") for e in events if e.get("kind") == "close"]
        assert close_tickets == [f"supervisor-close-{G}"]
        assert f"supervisor-close-{S}" not in close_tickets
        # Hub got exactly the parent Goal terminal event, with goal=G.
        assert ("dsh-trial-goal-complete", G) in [
            (c.get("kind"), (c.get("job") or {}).get("goal")) for c in rec.calls
        ]
        assert not any((c.get("job") or {}).get("goal") == S for c in rec.calls)
        assert "dsh-trial-goal-complete" in _goal_notify_kinds(dirs, G)


def test_resolve_parent_goal_id_prefers_chain_goal(tmp_path: Path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        _write_goal(dirs, _base_goal())
        # chain goal (SSOT) wins over a job goal that equals the slice id.
        assert tb._resolve_parent_goal_id(
            {"goal": S, "slice": S}, {"goal": G, "slice": S}
        ) == G
        # job goal == slice with no real goal file → None (never invent).
        assert tb._resolve_parent_goal_id({"goal": S, "slice": S}, {"slice": S}) is None
        # standalone chain with no goal anywhere → None.
        assert tb._resolve_parent_goal_id({"slice": S}, {"slice": S, "state": "running"}) is None
        # A real Goal file named like the slice (brief from run_goal_job) is OK.
        real = _base_goal()
        real["goal"] = S
        _write_goal(dirs, real)
        assert tb._resolve_parent_goal_id({"goal": S}, {"slice": S}) == S


def test_maybe_supervisor_close_settles_parent_not_slice(tmp_path: Path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        goal = _base_goal()
        _write_goal(dirs, goal)
        chain = _base_chain(tmp_path, state="PASS")
        _write_chain(dirs, chain)
        rec = _NotifyRecorder()
        with mock.patch.object(tb, "run_open_slice", side_effect=_fake_open_slice), \
                mock.patch.object(tb, "write_artifacts", side_effect=_fake_artifacts), \
                mock.patch.object(tb, "maybe_notify_hub", side_effect=rec):
            tb.maybe_supervisor_close(
                {"goal": S, "slice": S, "cwd": str(tmp_path)}, chain
            )

        got = _read_goal_file(dirs, G)
        assert got["status"] == "done"
        assert got["last_chain_state"] == "PASS"
        assert int(got["metrics"]["slices_completed"]) == 1
        assert not (dirs["goals"] / f"{S}.json").exists()


def test_sync_slices_completed_idempotent(tmp_path: Path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        goal = _base_goal()
        _write_chain(dirs, _base_chain(tmp_path, state="PASS"))
        assert tb._sync_slices_completed(goal) == 1
        assert tb._sync_slices_completed(goal) == 1
        assert int(goal["metrics"]["slices_completed"]) == 1


def test_resume_pass_continues_planned_slice_without_closing(tmp_path: Path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        goal, chain = _fixture(dirs, tmp_path)
        plan_path = dirs["summaries"] / f"goal-{G}-plan.md"
        _write_summary(
            plan_path,
            {
                "action": "emit_chains",
                "slices": [
                    {"slice": S, "pack": f"{S}.pack.md", "acceptance": ["a"]},
                    {"slice": S2, "pack": f"{S2}.pack.md", "acceptance": ["a"]},
                ],
            },
        )
        goal["plan_summary"] = str(plan_path)
        _write_goal(dirs, goal)

        def fake_rounds(job, chain_state, dest, *, start_round, start_pack):
            chain_state["state"] = "PASS"
            tb._write_chain(chain_state)
            return 0

        inbox = dirs["inbox"] / "reply.json"
        inbox.write_text(
            json.dumps(
                {
                    "type": "chain-reply",
                    "id": "unit-plan-reply",
                    "slice": S,
                    "answer": "go on",
                    "notify_dry_run": True,
                }
            ),
            encoding="utf-8",
        )
        exec_mock = mock.Mock(return_value=0)
        close_mock = mock.Mock(return_value=None)
        with mock.patch.object(tb, "run_chain_rounds", side_effect=fake_rounds), \
                mock.patch.object(tb, "_execute_goal_slices", exec_mock), \
                mock.patch.object(tb, "maybe_supervisor_close", close_mock):
            rc = tb.run_job(inbox)

        assert rc == 0
        close_mock.assert_not_called()
        assert exec_mock.call_count == 1
        args, kwargs = exec_mock.call_args
        assert kwargs.get("skip_passed") is True
        assert kwargs.get("on_slice_fail") == "stop"
        assert [s["slice"] for s in args[2]] == [S, S2]
        got = _read_goal_file(dirs, G)
        assert got["last_chain_state"] == "PASS"
        assert int(got["metrics"]["slices_completed"]) >= 1


def test_resume_escalated_marks_parent_and_notifies_failure(tmp_path: Path):
    patcher, dirs = _patched_dirs(tmp_path)
    with patcher:
        _fixture(dirs, tmp_path)
        rec = _NotifyRecorder()

        def fake_rounds(job, chain_state, dest, *, start_round, start_pack):
            chain_state["state"] = "escalated"
            chain_state["escalate_reason"] = "HOLD at max_rounds=2"
            tb._write_chain(chain_state)
            return 1

        inbox = dirs["inbox"] / "reply.json"
        inbox.write_text(
            json.dumps(
                {
                    "type": "chain-reply",
                    "id": "unit-escalate-reply",
                    "slice": S,
                    "answer": "still stuck",
                    "notify_dry_run": True,
                }
            ),
            encoding="utf-8",
        )
        with mock.patch.object(tb, "run_chain_rounds", side_effect=fake_rounds), \
                mock.patch.object(tb, "maybe_notify_hub", side_effect=rec):
            rc = tb.run_job(inbox)

        assert rc == 1
        got = _read_goal_file(dirs, G)
        assert got["status"] == "escalated"
        assert got["last_chain_state"] == "escalated"
        assert got.get("escalate_reason") == "HOLD at max_rounds=2"
        assert ("dsh-trial-goal-failed", G) in [
            (c.get("kind"), (c.get("job") or {}).get("goal")) for c in rec.calls
        ]
        assert not (dirs["goals"] / f"{S}.json").exists()
        assert "dsh-trial-goal-failed" in _goal_notify_kinds(dirs, G)
