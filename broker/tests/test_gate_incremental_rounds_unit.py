"""impl-tokopt-a — incremental (last-reviewed-SHA) delta gate + max_gate_rounds cap.

Covered here:
  a. ``DEFAULT_LIMITS`` carries ``max_gate_rounds == 2``;
  b. ``build_delta_gate_pack(base_sha=...)`` diffs 上次已审 SHA...HEAD and the
     pack carries the 禁止通读 ban (and the 上轮 FAIL 清单 heading);
  c. once ``gate_rounds_by_slice[slice]`` reaches the cap,
     ``_handle_submit_for_review`` spawns no gate (``run_open_slice`` not
     called) and answers HOLD + escalate.
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
# c. round cap: no gate spawn, HOLD + escalate answer
# --------------------------------------------------------------------------
def test_gate_round_cap_escalates_without_spawning(tmp_path):
    summary = tmp_path / "impl-summary.md"
    summary.write_text("done\n", encoding="utf-8")
    ask_path = tmp_path / "ask.json"
    ask_path.write_text("{}", encoding="utf-8")
    goal = {
        "goal": "g1",
        "slice": "s1",
        "gate_rounds_by_slice": {"s1": 2},
        "review_seq": 2,
    }
    ask = {
        "ask_id": "ask-cap",
        "slice": "s1",
        "summary_path": str(summary),
        "changed_files": [],
    }
    with _patched_dirs(tmp_path / "state"):
        with mock.patch.object(
            tb, "run_open_slice",
            side_effect=AssertionError("run_open_slice must not be called"),
        ):
            tb._handle_submit_for_review(
                ask, ask_path, default_profile="default",
                cwd=str(tmp_path), goal=goal,
            )
        ans = json.loads(
            (tb.MAILBOX / "answers" / "ask-cap.json").read_text(encoding="utf-8")
        )
    assert ans["status"] == "done"
    assert ans["ok"] is False
    assert ans["verdict"] == "HOLD"
    assert ans.get("escalate") is True
    assert ans.get("rework_mode") == "escalate"
    assert "max_gate_rounds=2" in ans["instruction"]
    # goal flagged for the supervisor, and PASS must never be set
    assert goal.get("gate_escalated") is True
    assert "max_gate_rounds=2 reached slice=s1" == goal.get("escalate_reason")
    assert goal.get("status") != "PASS"
