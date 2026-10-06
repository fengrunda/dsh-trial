"""Change ①/② — findings-first rework packs and fresh rework routing.

Locks:
* fix / reply-with-findings packs put Gate findings before the original pack
  and before the supervisor answer;
* ``decide_impl_completion`` refuses to treat ``done`` with pending P0/P1 and no
  ``finding_resolutions`` / ``rework_fresh`` handoff as a clean completion;
* ``run_chain_rounds`` really opens a findings-first fix ticket on that guard
  (and escalates instead of PASSing when no round remains);
* ``decide_and_answer_review`` fresh instruction matches "broker opens a new
  fix ticket";
* ``open-slice.sh`` foreman prompt requires ``finding_resolutions``.
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

spec = importlib.util.spec_from_file_location("trial_broker_findings", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

import trial_mailbox as TM  # noqa: E402

OPEN_SLICE_SH = ROOT.parent / "open-slice.sh"


def _write_summary(path: Path, block: dict, prose: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        prose + "\n\n```json\n" + json.dumps(block, ensure_ascii=False, indent=2) + "\n```\n",
        encoding="utf-8",
    )


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


# --- Change ①: pack ordering -------------------------------------------------

def test_fix_pack_findings_before_original(tmp_path):
    with mock.patch.object(tb, "PACKS", tmp_path):
        orig = tmp_path / "s.pack.md"
        orig.write_text("---\nslice_id: s\n---\n# Original intent\n", encoding="utf-8")
        name = tb.build_fix_pack(
            slice_id="s",
            next_round=2,
            original_pack_name=orig.name,
            gate_findings=[{"tier": "P1", "issue": "missing docstring"}],
            gate_summary_text="HOLD",
        )
        body = (tmp_path / name).read_text(encoding="utf-8")
    assert "Gate findings (primary task)" in body
    assert "## Done when" in body
    assert body.index("Gate findings (primary task)") < body.index("Original pack (appendix")
    assert "finding_resolutions" in body


def test_reply_pack_with_findings_puts_findings_first(tmp_path):
    with mock.patch.object(tb, "PACKS", tmp_path):
        orig = tmp_path / "s.pack.md"
        orig.write_text("# prior\n", encoding="utf-8")
        name = tb.build_reply_addendum_pack(
            slice_id="s",
            round_n=3,
            prior_pack_name=orig.name,
            answer="Please also add tests.",
            gate_findings=[{"tier": "P1", "issue": "no tests"}],
        )
        body = (tmp_path / name).read_text(encoding="utf-8")
        no_findings = tb.build_reply_addendum_pack(
            slice_id="s",
            round_n=4,
            prior_pack_name=orig.name,
            answer="Keep going.",
        )
        no_body = (tmp_path / no_findings).read_text(encoding="utf-8")
    assert "Gate findings (primary task)" in body
    assert body.index("Gate findings (primary task)") < body.index("Supervisor answer (appendix")
    assert "Supervisor answer (appendix)" in body
    assert "Prior pack (appendix" in body
    # No findings: supervisor answer is the driver, prior pack is appendix.
    assert "Supervisor answer" in no_body
    assert "Prior pack (appendix" in no_body
    assert "Gate findings" not in no_body


# --- Change ②: done + pending P1 guard --------------------------------------

def test_decide_impl_completion_blocks_dirty_done():
    goal = {"last_gate_findings": [{"tier": "P1", "issue": "missing docstring"}]}
    block = {"status": "done", "notes": "looks good"}
    d = tb.decide_impl_completion(block, goal)
    assert d["action"] == "open_fix"
    assert d["mode"] == "fresh"
    assert len(d["findings"]) == 1


def test_decide_impl_completion_accepts_resolutions():
    goal = {"last_gate_findings": [{"tier": "P1", "issue": "missing docstring"}]}
    block = {
        "status": "done",
        "finding_resolutions": [
            {"finding": "missing docstring", "change": "a.py:1", "status": "fixed"}
        ],
    }
    assert tb.decide_impl_completion(block, goal)["action"] == "clean_done"


def test_decide_impl_completion_accepts_rework_fresh_handoff():
    goal = {"last_gate_findings": [{"tier": "P1", "issue": "x"}]}
    block = {"status": "done", "notes": "handoff rework_fresh"}
    d = tb.decide_impl_completion(block, goal)
    assert d["action"] == "open_fix"
    assert d["reason"] == "fresh rework handoff on a done ticket"


def test_decide_impl_completion_ignores_p2_and_no_findings():
    assert tb.decide_impl_completion(
        {"status": "done"}, {"last_gate_findings": [{"tier": "P2", "issue": "nit"}]}
    )["action"] == "clean_done"
    assert tb.decide_impl_completion({"status": "done"}, {})["action"] == "clean_done"


def test_decide_impl_completion_defers_one_short():
    # A resolution list shorter than the blocking findings is not enough.
    goal = {"last_gate_findings": [
        {"tier": "P1", "issue": "a"}, {"tier": "P1", "issue": "b"},
    ]}
    block = {
        "status": "done",
        "finding_resolutions": [{"finding": "a", "change": "a.py:1", "status": "fixed"}],
    }
    assert tb.decide_impl_completion(block, goal)["action"] == "open_fix"


# --- Change ②: run_chain_rounds opens a real fresh fix ticket ----------------

def _run_chain(tmp_path, *, max_rounds, round1_block, round2_block=None):
    patch, d = _dirs(tmp_path)
    slice_id = "fresh-slice"
    goal_id = "fresh-goal"
    orig = d["packs"] / f"{slice_id}.pack.md"
    orig.write_text(f"---\nslice_id: {slice_id}\n---\n# intent\n", encoding="utf-8")

    goal = {
        "goal": goal_id,
        "status": "running",
        "slices": [slice_id],
        "profile": "acp-lite",
        "max_rounds": max_rounds,
        "last_gate_findings": [{"tier": "P1", "file": "a.py", "issue": "missing docstring"}],
    }
    (d["goals"] / f"{goal_id}.json").write_text(json.dumps(goal), encoding="utf-8")

    modes: list[str] = []

    def fake_open_slice(**kwargs):
        modes.append(kwargs["prompt_mode"])
        summary = d["summaries"] / f"{kwargs['summary_name']}.md"
        kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["log_path"].write_text("log\n", encoding="utf-8")
        if kwargs["prompt_mode"] == "foreman":
            rnd = kwargs["ticket"].rsplit("-r", 1)[-1]
            _write_summary(summary, round1_block if rnd == "1" else (round2_block or {}))
        elif kwargs["prompt_mode"] == "gate":
            _write_summary(summary, {"verdict": "PASS", "findings": []})
        return 0

    def fake_artifacts(job, log_path, summary_path, ec, ticket=None):
        return {"ticket": ticket or job.get("ticket"), "exit_code": ec, "assert_clean": True,
                "composition": None, "summary_exists": summary_path.is_file()}

    job = {
        "id": "j-fresh", "type": "chain", "slice": slice_id, "pack": orig.name,
        "acceptance": ["x"], "profile": "acp-lite", "gate_profile": "acp-lite",
        "cwd": str(tmp_path), "max_rounds": max_rounds, "goal": goal_id,
        "defer_supervisor_close": True,
    }
    chain = tb._new_chain_state(job)
    chain["goal"] = goal_id
    tb._write_chain(chain)
    dest = d["processing"] / "job-fresh.json"
    dest.write_text("x", encoding="utf-8")

    with patch, mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), \
            mock.patch.object(tb, "write_artifacts", side_effect=fake_artifacts), \
            mock.patch.object(tb, "maybe_notify_hub", return_value={"sent": False}), \
            mock.patch.object(tb, "_notify_goal_terminal"):
        rc = tb.run_chain_rounds(job, chain, dest, start_round=1, start_pack=orig.name)
    state = json.loads((d["chains"] / f"{slice_id}.json").read_text(encoding="utf-8"))
    return rc, state, modes


def test_chain_opens_fresh_fix_ticket_on_dirty_done(tmp_path):
    rc, state, modes = _run_chain(
        tmp_path,
        max_rounds=2,
        round1_block={"status": "done", "changed_files": ["a.py"], "notes": "no handoff"},
        round2_block={
            "status": "done", "changed_files": ["a.py"],
            "finding_resolutions": [
                {"finding": "missing docstring", "change": "a.py:1", "status": "fixed"}
            ],
        },
    )
    assert rc == 0, rc
    # Round 1 done must NOT have been gated: only one gate call, on round 2.
    assert modes == ["foreman", "foreman", "gate"], modes
    assert state["rounds"][0].get("fresh_rework_fix_pack")
    assert state["state"] == "PASS"


def test_chain_escalates_dirty_done_at_max_rounds(tmp_path):
    rc, state, modes = _run_chain(
        tmp_path,
        max_rounds=1,
        round1_block={"status": "done", "changed_files": ["a.py"], "notes": "no handoff"},
    )
    assert rc == 1, rc
    assert state["state"] == "escalated"
    assert "unresolved P0/P1" in state["escalate_reason"]
    assert modes == ["foreman"], modes


# --- Change ②: mailbox instruction + foreman prompt -------------------------

def test_mailbox_fresh_instruction_matches_new_fix_ticket():
    _, answer = TM.decide_and_answer_review(
        ask={"ask_id": "a1", "usage_prompt": 999999},
        gate_block={"verdict": "HOLD", "findings": [{"tier": "P1", "issue": "x"}]},
        gate_ticket="gate-1",
        limits={},
    )
    assert answer["rework_mode"] == "fresh"
    assert "NEW impl fix ticket" in answer["instruction"]
    assert "not" in answer["instruction"].lower()


def test_open_slice_foreman_prompt_requires_finding_resolutions():
    text = OPEN_SLICE_SH.read_text(encoding="utf-8")
    assert "finding_resolutions" in text
    assert "rework_fresh" in text
    # fresh wording must agree with the broker opening a new fix ticket.
    assert "另开" in text
