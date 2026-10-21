"""per_goal — a mid-Goal ``submit_for_review`` must not open the LLM gate.

The incident: with ``gate_grain=per_goal`` every slice submit still spawned a
full ``prompt_mode="gate"`` session, so a 5-slice Goal paid 5 reviews instead
of one aggregated bearer review.

Covered here (routing only — the verdict is never re-judged):
  1. per_goal + non-bearer + ordinary paths → ``defer``: 0 gate spawns,
     synthetic PASS answer carrying ``gate_policy``/``reason``, no
     ``review_seq`` bump, one ``gate_deferred`` record;
  2. per_goal + ``risk: high`` pack → ``per_slice``: gate spawns once;
  3. pack frontmatter ``gate: per_slice`` → ``per_slice``: gate spawns once;
  4. ``gate_grain=per_slice`` on the goal → ``per_slice``: gate spawns once;
  5. ``skip_gate: true`` + a ``src/`` diff → never skipped (defer/per_slice),
     so the gate is not elided;
  6. ``skip_gate: true`` + no src diff → ``skip``: synthetic PASS, 0 spawns.
"""
from __future__ import annotations

import importlib.util
import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_submit_defer", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)


# --------------------------------------------------------------------------
# fixtures
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


def _write_pack(path: Path, frontmatter: dict, body: str = "# slice\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = ["---"] + [f"{k}: {v}" for k, v in frontmatter.items()] + ["---", ""]
    path.write_text("\n".join(lines) + body, encoding="utf-8")
    return path


def _write_summary(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "prose\n\n```json\n" + json.dumps({"status": "done"}, ensure_ascii=False) + "\n```\n",
        encoding="utf-8",
    )
    return path


def _fake_artifacts(job, log_path, summary_path, ec, ticket=None):
    return {
        "ticket": ticket or job.get("ticket"),
        "exit_code": ec,
        "assert_clean": True,
        "composition": None,
        "summary": str(summary_path),
        "summary_exists": summary_path.is_file(),
    }


def _verdict_block(verdict: str) -> dict:
    return {"block": {"verdict": verdict, "findings": [], "unmet_acceptance": []}, "text": ""}


def _goal(slice_id: str, *, pack: str, summary: str, pending: list[str] | None = None,
          review_seq: int = 0, **extra) -> dict:
    goal = {
        "goal": f"unit-defer-{slice_id}",
        "status": "running",
        "slice": slice_id,
        "current_slice": slice_id,
        "current_pack": pack,
        "current_acceptance": ["do the thing"],
        "current_impl_summary": summary,
        "review_seq": review_seq,
        "gate_rounds_by_slice": {},
        "pending_slices": list(pending if pending is not None else []),
    }
    goal.update(extra)
    return goal


def _ask(ask_id: str, slice_id: str, *, paths: list[str] | None = None) -> dict:
    return {
        "ask_id": ask_id,
        "slice": slice_id,
        "summary": "did the thing",
        "changed_files": list(paths if paths is not None else ["broker/gate_effort.py"]),
        "base": "",
        "commit": "",
        "usage_prompt": 100,
    }


def _drive(dirs: dict, *, ask: dict, cwd: Path, goal: dict | None) -> list:
    """Call the real submit handler with the gate spawn mocked; return spawn kwargs."""
    calls: list = []

    def fake_open_slice(**kwargs):
        calls.append(kwargs)
        kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["log_path"].write_text("gate log\n", encoding="utf-8")
        return 0

    ask_id = ask["ask_id"]
    pending = dirs["mailbox"] / "pending" / f"{ask_id}.json"
    pending.parent.mkdir(parents=True, exist_ok=True)
    pending.write_text("{}", encoding="utf-8")
    with mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), \
            mock.patch.object(tb, "write_artifacts", side_effect=_fake_artifacts), \
            mock.patch.object(tb, "_parse_gate_verdict", return_value=_verdict_block("PASS")):
        tb._handle_submit_for_review(
            ask, pending, default_profile="acp-lite", cwd=str(cwd), goal=goal,
        )
    return calls


def _answer(dirs: dict, ask_id: str) -> dict:
    return json.loads((dirs["mailbox"] / "answers" / f"{ask_id}.json").read_text(encoding="utf-8"))


# --------------------------------------------------------------------------
# 1) per_goal + non-bearer → defer, no gate, no round
# --------------------------------------------------------------------------
def test_per_goal_non_bearer_defers_with_synthetic_pass(tmp_path: Path):
    slice_id = "unit-defer-a"
    with _patched_dirs(tmp_path / "state") as dirs:
        _write_pack(dirs["packs"] / f"{slice_id}.pack.md", {"slice": slice_id, "risk": "low"})
        summary = _write_summary(dirs["summaries"] / f"{slice_id}-impl-r1.md")
        goal = _goal(slice_id, pack=f"{slice_id}.pack.md", summary=str(summary),
                     pending=[slice_id, "unit-defer-b"])

        calls = _drive(dirs, ask=_ask("ask-defer-a", slice_id), cwd=tmp_path, goal=goal)

        assert calls == [], "per_goal non-bearer must not spawn a gate session"
        ans = _answer(dirs, "ask-defer-a")
        assert ans["verdict"] == "PASS" and ans["synthetic"] is True
        assert ans["gate_policy"] == "defer"
        assert ans["reason"] == "per_goal_defer"
        assert ans["findings"] == []
        assert goal.get("review_seq") in (0, None)
        assert not goal.get("gate_rounds_by_slice")
        assert not goal.get("last_reviewed_sha")
        assert len(goal["gate_deferred"]) == 1
        assert goal["gate_deferred"][0]["slice"] == slice_id
        assert goal["gate_pending"] is True
        assert (dirs["mailbox"] / "archive" / "ask-defer-a.pending.json").is_file()


# --------------------------------------------------------------------------
# 2) high-risk pack → per_slice, gate spawns
# --------------------------------------------------------------------------
def test_high_risk_pack_still_opens_gate(tmp_path: Path):
    slice_id = "unit-defer-high"
    with _patched_dirs(tmp_path / "state") as dirs:
        _write_pack(dirs["packs"] / f"{slice_id}.pack.md", {"slice": slice_id, "risk": "high"})
        summary = _write_summary(dirs["summaries"] / f"{slice_id}-impl-r1.md")
        goal = _goal(slice_id, pack=f"{slice_id}.pack.md", summary=str(summary),
                     pending=[slice_id, "unit-defer-b"])

        calls = _drive(dirs, ask=_ask("ask-defer-high", slice_id), cwd=tmp_path, goal=goal)

        assert len(calls) == 1
        assert goal.get("review_seq") == 1
        assert "gate_deferred" not in goal
        assert _answer(dirs, "ask-defer-high")["verdict"] == "PASS"


# --------------------------------------------------------------------------
# 3) pack gate: per_slice → per_slice, gate spawns
# --------------------------------------------------------------------------
def test_pack_per_slice_override_still_opens_gate(tmp_path: Path):
    slice_id = "unit-defer-packslice"
    with _patched_dirs(tmp_path / "state") as dirs:
        _write_pack(dirs["packs"] / f"{slice_id}.pack.md",
                    {"slice": slice_id, "risk": "low", "gate": "per_slice"})
        summary = _write_summary(dirs["summaries"] / f"{slice_id}-impl-r1.md")
        goal = _goal(slice_id, pack=f"{slice_id}.pack.md", summary=str(summary),
                     pending=[slice_id, "unit-defer-b"])

        calls = _drive(dirs, ask=_ask("ask-defer-packslice", slice_id), cwd=tmp_path, goal=goal)

        assert len(calls) == 1
        assert "gate_deferred" not in goal


# --------------------------------------------------------------------------
# 4) goal gate_grain=per_slice → per_slice, gate spawns
# --------------------------------------------------------------------------
def test_goal_grain_per_slice_still_opens_gate(tmp_path: Path):
    slice_id = "unit-defer-grainslice"
    with _patched_dirs(tmp_path / "state") as dirs:
        _write_pack(dirs["packs"] / f"{slice_id}.pack.md", {"slice": slice_id, "risk": "low"})
        summary = _write_summary(dirs["summaries"] / f"{slice_id}-impl-r1.md")
        goal = _goal(slice_id, pack=f"{slice_id}.pack.md", summary=str(summary),
                     pending=[slice_id, "unit-defer-b"], gate_grain="per_slice")

        calls = _drive(dirs, ask=_ask("ask-defer-grainslice", slice_id), cwd=tmp_path, goal=goal)

        assert len(calls) == 1
        assert "gate_deferred" not in goal


# --------------------------------------------------------------------------
# 5) skip_gate + src diff → still gated (never skipped)
# --------------------------------------------------------------------------
def test_skip_gate_with_src_diff_is_not_skipped(tmp_path: Path):
    slice_id = "unit-defer-skipsrc"
    with _patched_dirs(tmp_path / "state") as dirs:
        _write_pack(dirs["packs"] / f"{slice_id}.pack.md",
                    {"slice": slice_id, "risk": "low", "skip_gate": "true"})
        summary = _write_summary(dirs["summaries"] / f"{slice_id}-impl-r1.md")
        goal = _goal(slice_id, pack=f"{slice_id}.pack.md", summary=str(summary),
                     pending=[slice_id, "unit-defer-b"])

        policy, reason = tb._resolve_mid_slice_gate_policy(
            _ask("ask-defer-skipsrc", slice_id, paths=["src/gate/policy.py"]), goal,
        )
        assert policy in ("defer", "per_slice"), (policy, reason)
        assert policy != "skip"

        # non-bearer + per_goal → the src diff downgrades skip to defer only, so
        # the *bearer* still reviews it; make it the bearer to prove a gate runs.
        goal["pending_slices"] = [slice_id]
        calls = _drive(dirs, ask=_ask("ask-defer-skipsrc", slice_id,
                                      paths=["src/gate/policy.py"]),
                       cwd=tmp_path, goal=goal)
        assert len(calls) == 1, "a skip_gate pack with src diff must still be gated"


# --------------------------------------------------------------------------
# 6) skip_gate + no src diff → skip, synthetic PASS, no spawn
# --------------------------------------------------------------------------
def test_skip_gate_without_src_diff_is_skipped(tmp_path: Path):
    slice_id = "unit-defer-skipdocs"
    with _patched_dirs(tmp_path / "state") as dirs:
        _write_pack(dirs["packs"] / f"{slice_id}.pack.md",
                    {"slice": slice_id, "risk": "low", "skip_gate": "true"})
        summary = _write_summary(dirs["summaries"] / f"{slice_id}-impl-r1.md")
        goal = _goal(slice_id, pack=f"{slice_id}.pack.md", summary=str(summary),
                     pending=[slice_id])

        calls = _drive(dirs, ask=_ask("ask-defer-skipdocs", slice_id, paths=["docs/README.md"]),
                       cwd=tmp_path, goal=goal)

        assert calls == []
        ans = _answer(dirs, "ask-defer-skipdocs")
        assert ans["gate_policy"] == "skip"
        assert ans["synthetic"] is True and ans["verdict"] == "PASS"
        assert goal["gate_deferred"][0]["policy"] == "skip"
