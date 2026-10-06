"""Fix 2 — gate acceptance must never require an open/unmerged PR.

Locks ``filter_gate_acceptance`` and the goal-brief constraint that forbids
putting PR-open evidence into gate acceptance (impl cannot open a PR before the
gate PASSes, so such an item would HOLD forever).
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_gate_pr", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)


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


# --- filter ------------------------------------------------------------------

def test_filter_drops_pr_item_keeps_normal():
    out = tb.filter_gate_acceptance(["已开 PR 且 URL 附上", "must greet the user"])
    assert out == ["must greet the user"]


def test_filter_matches_all_pr_heuristics():
    items = [
        "dsh-trial-pr 证据齐全",
        "PR 已开",
        "开 PR 后核对",
        "pull request is open",
        "见 https://github.com/fengrunda/dsh-trial/pull/12",
        "unmerged PR blocks merge",
        "未合并的 PR 需要关闭",
        "PR URL: https://example.com/pr/1",
        "普通验收项 A",
    ]
    out = tb.filter_gate_acceptance(items)
    assert out == ["普通验收项 A"]


def test_filter_does_not_mutate_caller_list():
    original = ["已开 PR", "keep me"]
    out = tb.filter_gate_acceptance(original)
    assert original == ["已开 PR", "keep me"]
    assert out == ["keep me"]


def test_filter_string_shapes():
    assert tb.filter_gate_acceptance("PR 已开") == ""
    assert tb.filter_gate_acceptance("must greet") == "must greet"
    multiline = "已开 PR\nmust greet\n未合并 PR"
    assert tb.filter_gate_acceptance(multiline) == "must greet"
    assert tb.filter_gate_acceptance(None) is None


# --- brief constraint --------------------------------------------------------

def test_goal_brief_forbids_pr_in_gate_acceptance(tmp_path):
    patch, d = _dirs(tmp_path)
    job = {
        "goal": "g1", "brief": "do the thing", "cwd": str(tmp_path),
        "max_slices": 1, "acceptance_hint": ["code works"],
    }
    with patch:
        name = tb.build_goal_brief_pack(job)
    body = (d["packs"] / name).read_text(encoding="utf-8")
    assert "禁止把「已开 PR / PR URL / dsh-trial-pr 证据 / 未合并 PR」写进 **gate** acceptance" in body


# --- gate pack rendering -----------------------------------------------------

def test_gate_pack_strips_pr_acceptance_and_notes_it(tmp_path):
    patch, d = _dirs(tmp_path)
    (d["packs"] / "orig.pack.md").write_text(
        "---\nslice_id: s1\n---\n# orig\n", encoding="utf-8"
    )
    with patch, mock.patch.object(tb, "_git_diff", return_value="diff --git a/x b/x\n"):
        name = tb.build_gate_pack(
            slice_id="s1", round_n=1, original_pack_name="orig.pack.md",
            acceptance=["已开 PR 并附 URL", "must greet"],
            foreman_block={"status": "done", "base": "b", "commit": "h", "questions": []},
            foreman_summary_text="done", cwd=tmp_path,
        )
    body = (d["packs"] / name).read_text(encoding="utf-8")
    assert "must greet" in body
    assert "已开 PR 并附 URL" not in body
    assert tb.GATE_ACCEPTANCE_PR_FILTER_NOTE in body


def test_delta_gate_pack_strips_pr_acceptance(tmp_path):
    patch, d = _dirs(tmp_path)
    (d["packs"] / "orig.pack.md").write_text(
        "---\nslice_id: s1\n---\n# orig\n", encoding="utf-8"
    )
    with patch, mock.patch.object(tb, "_git_diff", return_value="diff --git a/x b/x\n"):
        name = tb.build_delta_gate_pack(
            slice_id="s1", round_n=2, original_pack_name="orig.pack.md",
            acceptance=["dsh-trial-pr 已开", "must greet"],
            prior_findings=[{"tier": "P1", "issue": "x"}],
            foreman_block={"status": "done", "base": "b", "commit": "h", "questions": []},
            foreman_summary_text="done", cwd=tmp_path,
        )
    body = (d["packs"] / name).read_text(encoding="utf-8")
    assert "must greet" in body
    assert "dsh-trial-pr 已开" not in body
    assert tb.GATE_ACCEPTANCE_PR_FILTER_NOTE in body
