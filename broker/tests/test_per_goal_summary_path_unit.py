#!/usr/bin/env python3
"""per_goal gate summary path: write where the broker reads, and don't invent HOLD.

The incident: gate_grain=per_goal wrote ``summaries/<slice>-gate-goal-rN.md``
while the chain read ``summaries/<slice>-gate-rN.md``. The missing file parsed
as HOLD with empty findings, so a PASS body became another impl round.

New packs declare the broker path (``-gate-rN.md``). An already written
``-gate-goal-rN.md`` is still read. A missing machine block is ``question``.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from contextlib import contextmanager, nullcontext
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location(
    "trial_broker_per_goal_path", ROOT / "trial-broker.py"
)
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)


@contextmanager
def _patched_dirs(root: Path):
    names = (
        "goals", "chains", "packs", "summaries", "artifacts", "processing",
        "outbox", "failed", "inbox", "mailbox",
    )
    dirs = {n: root / n for n in names}
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    with mock.patch.multiple(
        tb,
        GOALS=dirs["goals"], CHAINS=dirs["chains"], PACKS=dirs["packs"],
        SUMMARIES=dirs["summaries"], ARTIFACT_ROOT=dirs["artifacts"],
        PROCESSING=dirs["processing"], OUTBOX=dirs["outbox"], FAILED=dirs["failed"],
        INBOX=dirs["inbox"], MAILBOX=dirs["mailbox"],
        maybe_offload_gc=lambda **k: None,
    ), mock.patch.object(tb.T, "MAILBOX", dirs["mailbox"]):
        yield dirs


def _write_summary(path: Path, block: dict, prose: str = "prose") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        prose + "\n\n```json\n" + json.dumps(block, ensure_ascii=False) + "\n```\n",
        encoding="utf-8",
    )


def _summary_out_name(pack: Path) -> str:
    for line in pack.read_text(encoding="utf-8").splitlines():
        if line.startswith("summary_out:"):
            return Path(line.split(":", 1)[1].strip()).name
    raise AssertionError(f"no summary_out in {pack}")


def _fake_artifacts(job, log_path, summary_path, ec, ticket=None):
    return {
        "ticket": ticket or job.get("ticket"),
        "exit_code": ec,
        "assert_clean": True,
        "composition": None,
        "summary": str(summary_path),
        "summary_exists": summary_path.is_file(),
    }


def test_new_goal_pack_writes_broker_summary_path(tmp_path: Path):
    slice_id = "unit-path-s1"
    with _patched_dirs(tmp_path / "state") as dirs:
        name = tb.build_goal_gate_pack(
            goal_id="unit-path-goal",
            bearer_slice_id=slice_id,
            round_n=1,
            deferred_slice_ids=[],
            slice_specs=[],
            bearer_pack_name=f"{slice_id}.pack.md",
            bearer_block={"status": "done", "base": "", "commit": ""},
            bearer_summary_text="done",
            cwd=tmp_path,
            label="goal-r1",
            summary_name=f"{slice_id}-gate-r1",
        )
        pack = dirs["packs"] / name
        assert pack.name == f"{slice_id}-gate-goal-r1.pack.md"
        assert _summary_out_name(pack) == f"{slice_id}-gate-r1.md"
        assert f"{slice_id}-gate-goal-r1.md" not in pack.read_text(encoding="utf-8")


def test_broker_reads_existing_goal_summary(tmp_path: Path):
    slice_id = "unit-path-alias"
    with _patched_dirs(tmp_path / "state") as dirs:
        expected = dirs["summaries"] / f"{slice_id}-gate-r1.md"
        written = dirs["summaries"] / f"{slice_id}-gate-goal-r1.md"
        _write_summary(written, {"verdict": "PASS", "findings": [], "unmet_acceptance": []})
        # A newer unrelated review must not win over the same-round alias.
        newer = dirs["summaries"] / f"{slice_id}-gate-rev9.md"
        _write_summary(newer, {"verdict": "HOLD", "findings": [{"tier": "P1", "issue": "stale"}]})
        found = tb._resolve_written_gate_summary(expected, slice_id)
        assert found == written
        parsed = tb._parse_gate_verdict(found)
        block = tb.enforce_gate_verdict(parsed["block"])
        assert block["verdict"] == "PASS"
        assert block["findings"] == []


def test_missing_machine_block_is_question_not_hold(tmp_path: Path):
    missing = tmp_path / "no-such-gate.md"
    parsed = tb._parse_gate_verdict(missing)
    block = tb.enforce_gate_verdict(parsed["block"])
    assert block["verdict"] == "question"
    assert block["machine_block_missing"] is True
    assert block["findings"] == []
    assert "summary path missing" in block["notes"]
    assert str(missing) in block["notes"]
    assert block["verdict"] != "HOLD"

    prose = tmp_path / "prose-pass.md"
    prose.write_text(
        "模板 token verdict PASS|HOLD 不计入。\n本轮结论 PASS。0 P0 / 0 P1，findings 为空。\n",
        encoding="utf-8",
    )
    prose_block = tb.enforce_gate_verdict(tb._parse_gate_verdict(prose)["block"])
    assert prose_block["verdict"] == "PASS", prose_block

    ambiguous = tmp_path / "no-block.md"
    ambiguous.write_text("看过 diff，没有机器块。\n", encoding="utf-8")
    amb = tb.enforce_gate_verdict(tb._parse_gate_verdict(ambiguous)["block"])
    assert amb["verdict"] == "question"
    assert "machine block missing" in amb["notes"]

    mode, answer = tb.TM.decide_and_answer_review(
        ask={"ask_id": "a1", "usage_prompt": 10},
        gate_block=block,
        gate_ticket="gate-t",
        limits={},
    )
    assert mode == ""
    assert answer["verdict"] == "question"
    assert answer["verdict"] != "HOLD"
    assert "summary path missing" in answer["instruction"]


class _NoSupervisor:
    pass


def _drive(
    dirs: dict,
    root: Path,
    slice_id: str,
    fake_open_slice,
    *,
    max_rounds: int = 2,
    goal_id: str = "",
    supervisor=_NoSupervisor,
):
    pack = dirs["packs"] / f"{slice_id}.pack.md"
    pack.write_text(f"---\nslice_id: {slice_id}\n---\n# pack\n", encoding="utf-8")
    job = {
        "id": f"unit-path-{slice_id}",
        "type": "chain",
        "slice": slice_id,
        "pack": pack.name,
        "acceptance": ["do the thing"],
        "profile": "acp-lite",
        "gate_profile": "acp-lite",
        "cwd": str(root),
        "max_rounds": max_rounds,
        "gate_policy": "per_goal",
    }
    if goal_id:
        job["goal"] = goal_id
        (dirs["goals"] / f"{goal_id}.json").write_text(
            json.dumps({
                "goal": goal_id,
                "status": "running",
                "slices": [slice_id],
                "current_slice": slice_id,
            }),
            encoding="utf-8",
        )
    chain = tb._new_chain_state(job)
    if goal_id:
        chain["goal"] = goal_id
    dest = dirs["processing"] / f"{slice_id}.json"
    calls: list = []

    def _wrap(**kwargs):
        calls.append(kwargs)
        return fake_open_slice(**kwargs)

    supervisor_patch = (
        nullcontext()
        if supervisor is _NoSupervisor
        else mock.patch.object(
            tb, "auto_supervisor_machine_block_decision", return_value=supervisor,
        )
    )
    with mock.patch.object(tb, "run_open_slice", side_effect=_wrap), \
            mock.patch.object(tb, "write_artifacts", side_effect=_fake_artifacts), \
            mock.patch.object(tb, "maybe_notify_hub", return_value={"sent": True}), \
            supervisor_patch as sup:
        rc = tb.run_chain_rounds(
            job, chain, dest, start_round=1, start_pack=pack.name,
        )
    return rc, chain, calls, sup


def _impl_summary(block_status: str = "done"):
    return {
        "status": block_status,
        "changed_files": ["docs/x.md"],
        "base": "b",
        "commit": "c",
        "questions": [],
        "notes": "ok",
    }


def _event_bodies(dirs: dict) -> list[dict]:
    bodies = []
    for path in sorted(dirs["outbox"].glob("*.json")):
        body = json.loads(path.read_text(encoding="utf-8"))
        if body.get("kind"):
            bodies.append(body)
    return bodies


def test_chain_reads_legacy_goal_summary_as_pass(tmp_path: Path):
    slice_id = "unit-path-chain"
    root = tmp_path / "repo"
    root.mkdir()

    def fake_open_slice(**kwargs):
        kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["log_path"].write_text("unit\n", encoding="utf-8")
        if kwargs.get("role") == "gate":
            # Old landing path. The spawn is told to use the broker path.
            _write_summary(
                tb.SUMMARIES / f"{slice_id}-gate-goal-r1.md",
                {"verdict": "PASS", "findings": [], "unmet_acceptance": []},
                prose="PASS。0 P0 / 0 P1。",
            )
            return 0
        _write_summary(
            tb.SUMMARIES / f"{kwargs['summary_name']}.md",
            {"status": "done", "changed_files": ["docs/x.md"], "base": "b",
             "commit": "c", "questions": [], "notes": "ok"},
        )
        return 0

    with _patched_dirs(tmp_path / "state") as dirs:
        rc, chain, calls, _sup = _drive(dirs, root, slice_id, fake_open_slice)
        gate = [c for c in calls if c.get("role") == "gate"]
        assert len(gate) == 1, calls
        assert gate[0]["summary_name"] == f"{slice_id}-gate-r1"
        pack = dirs["packs"] / gate[0]["pack_name"]
        assert _summary_out_name(pack) == f"{slice_id}-gate-r1.md"
        assert rc == 0, chain
        assert chain["state"] == "PASS", chain
        assert chain["last_verdict"] == "PASS", chain
        assert len([c for c in calls if c.get("role") != "gate"]) == 1


def test_missing_gate_summary_does_not_open_another_impl(tmp_path: Path):
    slice_id = "unit-path-missing"
    root = tmp_path / "repo"
    root.mkdir()

    def fake_open_slice(**kwargs):
        kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["log_path"].write_text("unit\n", encoding="utf-8")
        if kwargs.get("role") == "gate":
            return 0
        _write_summary(
            tb.SUMMARIES / f"{kwargs['summary_name']}.md",
            _impl_summary(),
        )
        return 0

    with _patched_dirs(tmp_path / "state") as dirs:
        rc, chain, calls, sup = _drive(
            dirs, root, slice_id, fake_open_slice, max_rounds=2, supervisor="question",
        )
        assert rc == 0, chain
        assert chain["state"] == "awaiting_supervisor", chain
        assert chain["last_verdict"] == "question", chain
        assert chain["awaiting_reason"] == "summary_path_missing", chain
        assert "summary path missing" in str(chain.get("error") or "")
        assert chain["last_verdict"] != "HOLD"
        assert chain["last_verdict"] != "PASS"
        impls = [c for c in calls if c.get("role") != "gate"]
        assert len(impls) == 1, calls
        assert sup.call_count == 1
        last = chain["rounds"][-1]
        assert last["verdict"] == "question"
        assert not (last.get("gate_block") or {}).get("findings")
        kinds = [b["kind"] for b in _event_bodies(dirs)]
        assert "machine_block_handoff" in kinds
        assert "dsh-trial-complete" not in kinds
        assert "goal_terminal" not in kinds


def test_parse_machine_block_decision_rejects_impl_choices():
    assert tb.parse_machine_block_decision({"decision": "rerun_gate"}) == "rerun_gate"
    assert tb.parse_machine_block_decision({"decision": "question"}) == "question"
    assert tb.parse_machine_block_decision({"decision": "escalate"}) == "escalate"
    assert tb.parse_machine_block_decision({"decision": "continue_same"}) is None
    assert tb.parse_machine_block_decision({"decision": "new_ticket"}) is None
    assert tb.parse_machine_block_decision(None, "") is None


def _unreadable_gate(slice_id: str):
    gates = {"n": 0}

    def fake_open_slice(**kwargs):
        kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["log_path"].write_text("unit\n", encoding="utf-8")
        if kwargs.get("role") == "gate":
            gates["n"] += 1
            path = tb.SUMMARIES / f"{kwargs['summary_name']}.md"
            path.parent.mkdir(parents=True, exist_ok=True)
            if gates["n"] == 1:
                path.write_text("看过 diff，没有机器块。\n", encoding="utf-8")
            else:
                _write_summary(
                    path,
                    {"verdict": "PASS", "findings": [], "unmet_acceptance": []},
                    prose="PASS。0 P0 / 0 P1。",
                )
            return 0
        _write_summary(tb.SUMMARIES / f"{kwargs['summary_name']}.md", _impl_summary())
        return 0

    return fake_open_slice


def test_unreadable_gate_summary_asks_supervisor_not_pass(tmp_path: Path):
    slice_id = "unit-path-noblock"
    root = tmp_path / "repo"
    root.mkdir()
    with _patched_dirs(tmp_path / "state") as dirs:
        rc, chain, calls, sup = _drive(
            dirs, root, slice_id, _unreadable_gate(slice_id),
            max_rounds=2, supervisor="question",
        )
        assert rc == 0, chain
        assert chain["state"] == "awaiting_supervisor", chain
        assert chain["awaiting_reason"] == "machine_block_missing", chain
        assert chain["last_verdict"] == "question"
        assert chain["last_verdict"] != "PASS"
        assert len([c for c in calls if c.get("role") == "impl"]) == 1
        assert len([c for c in calls if c.get("role") == "gate"]) == 1
        assert sup.call_count == 1
        assert sup.call_args.kwargs["reason"] == "machine_block_missing"
        handoff = [
            b for b in _event_bodies(dirs) if b["kind"] == "machine_block_handoff"
        ]
        assert len(handoff) == 1
        assert handoff[0]["decision"] == "question"
        assert handoff[0]["kind"] not in ("PASS", "escalated", "dsh-trial-complete")


def test_unreadable_gate_summary_rerun_gate_does_not_open_impl(tmp_path: Path):
    slice_id = "unit-path-rerun"
    root = tmp_path / "repo"
    root.mkdir()
    with _patched_dirs(tmp_path / "state") as dirs:
        rc, chain, calls, sup = _drive(
            dirs, root, slice_id, _unreadable_gate(slice_id),
            max_rounds=2, supervisor="rerun_gate",
        )
        assert rc == 0, chain
        assert chain["state"] == "PASS", chain
        assert chain["last_verdict"] == "PASS"
        assert len([c for c in calls if c.get("role") == "impl"]) == 1, calls
        assert len([c for c in calls if c.get("role") == "gate"]) == 2, calls
        assert sup.call_count == 1
        kinds = [b["kind"] for b in _event_bodies(dirs)]
        assert "machine_block_handoff" in kinds


def test_unreadable_gate_summary_escalate_is_not_pass(tmp_path: Path):
    slice_id = "unit-path-esc"
    root = tmp_path / "repo"
    root.mkdir()
    goal_id = "unit-path-esc-goal"
    with _patched_dirs(tmp_path / "state") as dirs:
        rc, chain, calls, sup = _drive(
            dirs, root, slice_id, _unreadable_gate(slice_id),
            max_rounds=2, goal_id=goal_id, supervisor="escalate",
        )
        assert rc == 1, chain
        assert chain["state"] == "escalated", chain
        assert chain["last_verdict"] == "question"
        assert chain["last_verdict"] != "PASS"
        assert chain["escalate_reason"] == "machine_block_missing"
        goal = json.loads((dirs["goals"] / f"{goal_id}.json").read_text())
        assert goal["status"] == "escalated"
        assert goal["escalate_reason"] == "machine_block_missing"
        assert len([c for c in calls if c.get("role") == "impl"]) == 1
        assert sup.call_count == 1
        kinds = [b["kind"] for b in _event_bodies(dirs)]
        assert "machine_block_handoff" in kinds
        assert "goal_terminal" in kinds


def test_machine_block_supervisor_reuses_goal_session(tmp_path: Path):
    slice_id = "unit-path-sup"
    goal_id = "unit-path-sup-goal"
    with _patched_dirs(tmp_path / "state") as dirs:
        goal = {"goal": goal_id, "status": "running", "current_slice": slice_id}
        (dirs["goals"] / f"{goal_id}.json").write_text(json.dumps(goal), encoding="utf-8")
        job = {"goal": goal_id, "slice": slice_id, "cwd": str(tmp_path), "profile": "acp-lite"}
        chain = {"slice": slice_id, "goal": goal_id, "state": "running", "rounds": []}

        def fake_run(**kwargs):
            assert kwargs["keep_open"] is True
            assert kwargs["ticket"] == f"supervisor-goal-{goal_id}"
            assert kwargs["prompt_mode"] == "supervisor-pause"
            path = dirs["summaries"] / f"{kwargs['summary_name']}.md"
            path.write_text(
                '```json\n{"action": "machine_block_decision", "decision": "rerun_gate"}\n```\n',
                encoding="utf-8",
            )
            return 0, {}, path

        with mock.patch.object(tb, "run_supervisor_ticket", side_effect=fake_run):
            decision = tb.auto_supervisor_machine_block_decision(
                job, chain, round_n=1, reason="machine_block_missing", goal=goal,
            )
        assert decision == "rerun_gate"
