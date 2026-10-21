"""impl-tokopt-a — incremental (last-reviewed-SHA) delta gate + max_gate_rounds cap.

Covered here:
  a. ``DEFAULT_LIMITS`` carries ``max_gate_rounds == 2``;
  b. ``build_delta_gate_pack(base_sha=...)`` diffs 上次已审 SHA...HEAD and the
     pack carries the 禁止通读 ban (and the 上轮 FAIL 清单 heading);
  c. once ``gate_rounds_by_slice[slice]`` reaches the cap,
     ``_handle_submit_for_review`` spawns no gate (``run_open_slice`` not
     called), parks the Goal at ``awaiting_supervisor`` and lets the
     supervisor decide inplace(+1) | split | escalate.
"""
from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_gate_incr", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)


# --------------------------------------------------------------------------
# fixtures (mirrors test_gate_dedup_unit.py)
# --------------------------------------------------------------------------
@contextmanager
def _patched_dirs(root: Path):
    names = ("goals", "chains", "packs", "summaries", "artifacts", "processing",
             "outbox", "failed", "inbox", "mailbox")
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


def _git(cwd: Path, *args: str):
    return subprocess.run(["git", *args], cwd=str(cwd), capture_output=True, text=True)


def _make_repo(root: Path) -> Path:
    repo = root / "repo"
    repo.mkdir(parents=True, exist_ok=True)
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "t@t")
    _git(repo, "config", "user.name", "t")
    (repo / "a.txt").write_text("base\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "base")
    base_sha = _git(repo, "rev-parse", "HEAD").stdout.strip()
    (repo / "b.txt").write_text("new work\n", encoding="utf-8")
    _git(repo, "add", ".")
    _git(repo, "commit", "-q", "-m", "more")
    head_sha = _git(repo, "rev-parse", "HEAD").stdout.strip()
    return repo, base_sha, head_sha


# --------------------------------------------------------------------------
# a. limit present
# --------------------------------------------------------------------------
def test_default_limits_has_max_gate_rounds():
    assert tb.T.DEFAULT_LIMITS.get("max_gate_rounds") == 2


# --------------------------------------------------------------------------
# b. base_sha drives the delta diff + ban line in the pack
# --------------------------------------------------------------------------
def test_delta_pack_uses_base_sha(tmp_path):
    repo, base_sha, head_sha = _make_repo(tmp_path)
    with _patched_dirs(tmp_path / "state"):
        name = tb.build_delta_gate_pack(
            slice_id="s1",
            round_n=2,
            original_pack_name="s1.pack.md",
            acceptance=[],
            prior_findings=[{"tier": "P1", "issue": "x"}],
            foreman_block={"base": "", "commit": head_sha},
            foreman_summary_text="s",
            cwd=repo,
            label="rev2",
            base_sha=base_sha,
        )
        body = (tb.PACKS / name).read_text(encoding="utf-8")
    # diff is base...HEAD: sees b.txt (new work), and never a.txt's base line
    assert "new work" in body
    assert "diff --git a/b.txt" in body
    # 上轮 FAIL 清单 heading + 禁止通读 ban present
    assert "上轮 FAIL 清单" in body
    assert "禁止通读全文件/原 pack/全仓" in body
    # base actually honored: the pack's diff base is the last reviewed SHA,
    # which predates b.txt — so a.txt content shows no change from base
    diff_section = body.split("```diff", 1)[1]
    assert "a.txt" not in diff_section


# --------------------------------------------------------------------------
# c. round cap: no gate spawn, park at awaiting_supervisor, supervisor decides
# --------------------------------------------------------------------------
def _cap_ask(tmp_path: Path, name: str = "ask-cap") -> tuple[dict, Path]:
    summary = tmp_path / "impl-summary.md"
    summary.write_text("done\n", encoding="utf-8")
    ask_path = tmp_path / "ask.json"
    ask_path.write_text("{}", encoding="utf-8")
    ask = {
        "ask_id": name,
        "slice": "s1",
        "summary_path": str(summary),
        "changed_files": [],
    }
    return ask, ask_path


def _run_cap(tmp_path, goal, decision=None, *, name="ask-cap", chain=None):
    ask, ask_path = _cap_ask(tmp_path, name)
    with _patched_dirs(tmp_path / "state") as dirs:
        if chain is not None:
            (dirs["chains"] / f"{chain['slice']}.json").write_text(
                json.dumps(chain), encoding="utf-8"
            )
        with mock.patch.object(
            tb, "run_open_slice",
            side_effect=AssertionError("run_open_slice must not be called"),
        ), mock.patch.object(
            tb, "auto_supervisor_max_rounds_decision", return_value=decision,
        ) as auto, mock.patch.object(tb, "_notify_awaiting_hub") as notify:
            tb._handle_submit_for_review(
                ask, ask_path, default_profile="default",
                cwd=str(tmp_path), goal=goal,
            )
        ans = json.loads(
            (tb.MAILBOX / "answers" / f"{name}.json").read_text(encoding="utf-8")
        )
    return ans, auto, notify


def test_gate_round_cap_parks_awaiting_supervisor(tmp_path):
    goal = {
        "goal": "g1",
        "slice": "s1",
        "gate_rounds_by_slice": {"s1": 2},
        "review_seq": 2,
    }
    ans, auto, notify = _run_cap(tmp_path, goal)

    assert ans["status"] == "done"
    assert ans["ok"] is False
    assert ans["verdict"] == "HOLD"
    assert ans["rework_mode"] == "supervisor"
    assert ans.get("escalate") is False
    assert ans.get("awaiting_reason") == "max_gate_rounds"
    assert "max_gate_rounds=2" in ans["instruction"]

    # parked, not escalated: no hard-escalate bookkeeping, no round spent
    assert goal.get("status") != "PASS"
    assert goal["last_chain_state"] == "awaiting_supervisor"
    assert goal["awaiting_reason"] == "max_gate_rounds"
    assert "escalate_reason" not in goal
    assert "gate_escalated" not in goal
    assert goal["gate_rounds_by_slice"] == {"s1": 2}
    assert goal["max_gate_rounds_decision_pending"]["ticket_opened"] is False

    # decision was attempted once, for the gate-cap kind; parked => no Hub wake
    auto.assert_called_once()
    assert auto.call_args.kwargs["kind"] == "max_gate_rounds"
    assert auto.call_args.kwargs["max_rounds"] == 2
    notify.assert_not_called()


def test_gate_round_cap_syncs_existing_chain(tmp_path):
    goal = {
        "goal": "g1",
        "slice": "s1",
        "gate_rounds_by_slice": {"s1": 2},
        "review_seq": 2,
    }
    chain = {"slice": "s1", "goal": "g1", "state": "running", "pack": "s1.pack.md"}
    _run_cap(tmp_path, goal, chain=chain)
    saved = json.loads(
        (tmp_path / "state" / "chains" / "s1.json").read_text(encoding="utf-8")
    )
    assert saved["state"] == "awaiting_supervisor"
    assert saved["awaiting_reason"] == "max_gate_rounds"
    assert saved["awaiting_answer_for_round"] == 3


def test_gate_round_cap_inplace_grants_one_bonus_round(tmp_path):
    goal = {
        "goal": "g1",
        "slice": "s1",
        "gate_rounds_by_slice": {"s1": 2},
        "review_seq": 2,
    }
    ans, _, notify = _run_cap(tmp_path, goal, "inplace", name="ask-inplace")

    assert goal["max_gate_rounds_by_slice"]["s1"] == 3
    assert goal["max_gate_rounds_bonus_used"]["s1"] is True
    assert goal["status"] == "running"
    assert "awaiting_reason" not in goal
    assert "max_gate_rounds_decision_pending" not in goal
    assert ans["rework_mode"] == "inplace"
    assert ans.get("escalate") is False
    assert "awaiting_reason" not in ans
    assert "3" in ans["instruction"]
    notify.assert_not_called()


def test_gate_round_cap_second_inplace_escalates(tmp_path):
    goal = {
        "goal": "g1",
        "slice": "s1",
        "gate_rounds_by_slice": {"s1": 3},
        "review_seq": 3,
        "max_gate_rounds_by_slice": {"s1": 3},
        "max_gate_rounds_bonus_used": {"s1": True},
    }
    ans, _, notify = _run_cap(tmp_path, goal, "inplace", name="ask-inplace2")

    assert goal["status"] == "escalated"
    assert goal["gate_escalated"] is True
    assert goal["escalate_reason"] == "max_gate_rounds=3 reached slice=s1 (supervisor)"
    assert goal["max_gate_rounds_by_slice"]["s1"] == 3
    assert ans["rework_mode"] == "escalate"
    assert ans.get("escalate") is True
    notify.assert_called_once()


def test_gate_round_cap_split_stays_resumable(tmp_path):
    goal = {
        "goal": "g1",
        "slice": "s1",
        "gate_rounds_by_slice": {"s1": 2},
        "review_seq": 2,
    }
    ans, _, notify = _run_cap(tmp_path, goal, "split", name="ask-split")

    assert goal["status"] == "running"
    assert goal["awaiting_reason"] == "max_gate_rounds_split"
    assert goal["supervisor_decision"]["decision"] == "split"
    assert ans["rework_mode"] == "split"
    assert ans.get("escalate") is False
    assert ans.get("awaiting_reason") == "max_gate_rounds_split"
    assert "escalate_reason" not in goal
    notify.assert_not_called()


def test_gate_round_cap_escalate_notifies_once(tmp_path):
    goal = {
        "goal": "g1",
        "slice": "s1",
        "gate_rounds_by_slice": {"s1": 2},
        "review_seq": 2,
    }
    ans, _, notify = _run_cap(tmp_path, goal, "escalate", name="ask-esc")

    assert goal["status"] == "escalated"
    assert goal["gate_escalated"] is True
    assert goal["escalate_reason"] == "max_gate_rounds=2 reached slice=s1 (supervisor)"
    assert ans["rework_mode"] == "escalate"
    assert ans.get("escalate") is True
    notify.assert_called_once()
    assert notify.call_args.kwargs["reason"] == "max_gate_rounds"


def test_gate_cap_reached_honours_slice_override():
    goal = {
        "goal": "g1",
        "gate_rounds_by_slice": {"s1": 2},
        "max_gate_rounds_by_slice": {"s1": 3},
    }
    assert tb._gate_cap_reached(goal, "s1", {"max_gate_rounds": 2}) == (False, 3)
    assert tb._gate_cap_reached(goal, "s1", {"max_gate_rounds": 1}) == (False, 3)
    assert tb._gate_cap_reached(None, "s1", {"max_gate_rounds": 2}) == (False, 2)


def test_build_supervisor_pack_gate_kind(tmp_path):
    with _patched_dirs(tmp_path / "state"):
        name = tb.build_supervisor_max_rounds_pack(
            slice_id="s1", round_n=2, max_rounds=3, findings=[],
            reason="cap", goal_id="g1", pending_pack="s1.pack.md",
            bonus_used=False, kind="max_gate_rounds",
        )
        body = (tb.PACKS / name).read_text(encoding="utf-8")
    assert name == "s1-maxrounds-r2.pack.md"
    assert "kind: supervisor-max-gate-rounds" in body
    assert "max_gate_rounds: 3" in body
    assert "Supervisor max_gate_rounds decision" in body
    assert "sup-maxgaterounds-r2" in body
