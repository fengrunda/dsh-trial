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
from contextlib import contextmanager
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


def _drive(dirs: dict, root: Path, slice_id: str, fake_open_slice, *, max_rounds: int = 2):
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
    chain = tb._new_chain_state(job)
    dest = dirs["processing"] / f"{slice_id}.json"
    calls: list = []

    def _wrap(**kwargs):
        calls.append(kwargs)
        return fake_open_slice(**kwargs)

    with mock.patch.object(tb, "run_open_slice", side_effect=_wrap), \
            mock.patch.object(tb, "write_artifacts", side_effect=_fake_artifacts), \
            mock.patch.object(tb, "maybe_notify_hub", return_value={"sent": True}):
        rc = tb.run_chain_rounds(
            job, chain, dest, start_round=1, start_pack=pack.name,
        )
    return rc, chain, calls


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
        rc, chain, calls = _drive(dirs, root, slice_id, fake_open_slice)
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
            {"status": "done", "changed_files": ["docs/x.md"], "base": "b",
             "commit": "c", "questions": [], "notes": "ok"},
        )
        return 0

    with _patched_dirs(tmp_path / "state") as dirs:
        rc, chain, calls = _drive(dirs, root, slice_id, fake_open_slice, max_rounds=2)
        assert rc == 0, chain
        assert chain["state"] == "awaiting_supervisor", chain
        assert chain["last_verdict"] == "question", chain
        assert chain["awaiting_reason"] == "summary_path_missing", chain
        assert "summary path missing" in str(chain.get("error") or "")
        assert chain["last_verdict"] != "HOLD"
        impls = [c for c in calls if c.get("role") != "gate"]
        assert len(impls) == 1, calls
        last = chain["rounds"][-1]
        assert last["verdict"] == "question"
        assert not (last.get("gate_block") or {}).get("findings")
