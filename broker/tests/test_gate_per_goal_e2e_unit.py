#!/usr/bin/env python3
"""E2E lock: a per_goal Goal spawns exactly ONE LLM gate.

s1 defers mid-slice submits, s2 wires the caliber + soft-cap into every gate
spawn. This file drives the *real* handlers across modules
(``_handle_submit_for_review`` → synthetic PASS, then the Goal bearer gate)
and asserts the aggregate: 2 deferrals, 1 gate session, ``review_seq`` +1.

All state lives under a temporary ``TB_*`` tree — the real limits.json and
mailbox are never read.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import gate_effort as GE  # noqa: E402

spec = importlib.util.spec_from_file_location("trial_broker_pge2e", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)
T = tb.T


# --------------------------------------------------------------------------
# fixtures (all state is temporary)
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
    ), mock.patch.object(T, "LIMITS_PATH", root / "limits.json"):
        (root / "limits.json").write_text(
            json.dumps({"gate_grain": "per_goal", "max_rounds": 2, "max_slices": 3}),
            encoding="utf-8",
        )
        yield dirs


def _write_summary(path: Path, block: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "prose\n\n```json\n" + json.dumps(block, ensure_ascii=False, indent=2) + "\n```\n",
        encoding="utf-8",
    )


def _fake_artifacts(job, log_path, summary_path, ec, ticket=None):
    return {
        "ticket": ticket or job.get("ticket"),
        "exit_code": ec,
        "assert_clean": True,
        "composition": None,
        "summary": str(summary_path),
        "summary_exists": summary_path.is_file(),
    }


def _pack(dirs: dict, name: str, *, frontmatter: str = "") -> str:
    (dirs["packs"] / name).write_text(
        f"---\nslice_id: {name.split('.')[0]}\n{frontmatter}---\n# pack {name}\n",
        encoding="utf-8",
    )
    return name


def _impl_summary(dirs: dict, slice_id: str) -> str:
    path = dirs["summaries"] / f"{slice_id}-impl-r1.md"
    _write_summary(path, {"status": "done", "changed_files": ["a.py"],
                          "branch": "feat/x", "commit": "abc", "base": "def",
                          "questions": [], "notes": "ok"})
    return str(path)


def _goal(dirs: dict, gid: str, *, cwd: Path, pack: str, slice_id: str,
          pending: list[str] | None = None) -> dict:
    goal = {
        "goal": gid,
        "status": "running",
        "slice": slice_id,
        "current_slice": slice_id,
        "current_pack": pack,
        "current_acceptance": ["do the thing"],
        "review_seq": 0,
        "gate_rounds_by_slice": {},
        "pending_slices": list(pending or []),
    }
    (dirs["goals"] / f"{gid}.json").write_text(json.dumps(goal), encoding="utf-8")
    return goal


def _reload_goal(dirs: dict, gid: str) -> dict:
    return json.loads((dirs["goals"] / f"{gid}.json").read_text(encoding="utf-8"))


def _ask(ask_id: str, slice_id: str, *, paths: list[str] | None = None,
         risk: str | None = None) -> dict:
    ask = {
        "ask_id": ask_id,
        "slice": slice_id,
        "summary": f"{slice_id} did the thing",
        "changed_files": paths if paths is not None else ["docs/x.md"],
        "base": "",
        "commit": "",
        "usage_prompt": 1000,
        "from": {"ticket": f"impl-trial-{slice_id}-r1"},
    }
    if risk:
        ask["risk"] = risk
    return ask


def _drive(dirs: dict, *, ask: dict, cwd: Path, goal: dict) -> list:
    """Run the real submit_for_review handler; gate spawn is faked and counted."""
    calls: list = []

    def fake_open_slice(**kwargs):
        calls.append(kwargs)
        kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["log_path"].write_text("gate log\n", encoding="utf-8")
        _write_summary(
            dirs["summaries"] / f"{kwargs['summary_name']}.md",
            {"verdict": "PASS", "findings": [], "unmet_acceptance": []},
        )
        return 0

    ask_id = str(ask["ask_id"])
    pending = dirs["mailbox"] / "pending" / f"{ask_id}.json"
    pending.parent.mkdir(parents=True, exist_ok=True)
    pending.write_text("{}", encoding="utf-8")
    with mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), \
            mock.patch.object(tb, "write_artifacts", side_effect=_fake_artifacts), \
            mock.patch.object(tb, "_parse_gate_verdict",
                              return_value={"block": {"verdict": "PASS", "findings": []},
                                            "text": "PASS"}):
        tb._handle_submit_for_review(
            ask, pending, default_profile="acp-lite", cwd=str(cwd), goal=goal,
        )
    return calls


def _answer(dirs: dict, ask_id: str) -> dict:
    return json.loads((dirs["mailbox"] / "answers" / f"{ask_id}.json").read_text(encoding="utf-8"))


def _gate_calls(calls: list) -> list:
    return [c for c in calls if c.get("role") == "gate"]


def _setup(tmp_path: Path):
    root = tmp_path
    cwd = root / "repo"
    cwd.mkdir(exist_ok=True)
    return root, cwd


# --------------------------------------------------------------------------
# 1) happy path: 2 defers + exactly one bearer gate, review_seq +1
# --------------------------------------------------------------------------
def test_per_goal_two_slices_one_gate_e2e(tmp_path: Path):
    root, cwd = _setup(tmp_path)
    gid = "unit-e2e-happy"
    s1, s2 = "unit-e2e-happy-s1", "unit-e2e-happy-s2"
    with _patched_dirs(root / "state") as dirs:
        p1 = _pack(dirs, f"{s1}.pack.md")
        p2 = _pack(dirs, f"{s2}.pack.md")
        # both slices are still pending → every mid-slice submit is a non-bearer
        _goal(dirs, gid, cwd=cwd, pack=p1, slice_id=s1, pending=[s1, s2])

        calls: list = []
        for sid, pack in ((s1, p1), (s2, p2)):
            goal = _reload_goal(dirs, gid)
            goal["current_slice"], goal["current_pack"] = sid, pack
            goal["current_impl_summary"] = _impl_summary(dirs, sid)
            (dirs["goals"] / f"{gid}.json").write_text(json.dumps(goal), encoding="utf-8")
            calls += _drive(dirs, ask=_ask(f"e2e-{sid}", sid), cwd=cwd,
                            goal=_reload_goal(dirs, gid))

        assert [c.get("role") for c in calls] == [], "mid-slice submits must not spawn"
        for sid in (s1, s2):
            a = _answer(dirs, f"e2e-{sid}")
            assert a["synthetic"] is True, a
            assert a["gate_policy"] == GE.GATE_POLICY_DEFER, a
            assert a["verdict"] == "PASS", a

        goal = _reload_goal(dirs, gid)
        assert len(goal.get("gate_deferred") or []) == 2, goal.get("gate_deferred")
        assert goal.get("review_seq") in (0, None), goal.get("review_seq")

        # Goal-end bearer gate: s2 is the last pending slice.
        goal = _reload_goal(dirs, gid)
        goal.update(current_slice=s2, current_pack=p2, pending_slices=[s2],
                    current_impl_summary=_impl_summary(dirs, s2))
        (dirs["goals"] / f"{gid}.json").write_text(json.dumps(goal), encoding="utf-8")
        calls = _drive(dirs, ask=_ask(f"e2e-{s2}-bear", s2), cwd=cwd,
                       goal=_reload_goal(dirs, gid))

        assert len(_gate_calls(calls)) == 1, [c.get("ticket") for c in calls]
        bearer = _answer(dirs, f"e2e-{s2}-bear")
        assert bearer.get("synthetic") is not True, bearer
        assert bearer["verdict"] == "PASS", bearer
        goal = _reload_goal(dirs, gid)
        assert goal["review_seq"] == 1, goal.get("review_seq")      # only the bearer
        assert len(goal.get("gate_deferred") or []) == 2, goal.get("gate_deferred")


# --------------------------------------------------------------------------
# 2) high-risk slice still opens its own gate (total 2: s1 + bearer)
# --------------------------------------------------------------------------
def test_per_goal_high_risk_slice_opens_own_gate(tmp_path: Path):
    root, cwd = _setup(tmp_path)
    gid = "unit-e2e-risk"
    s1, s2 = "unit-e2e-risk-s1", "unit-e2e-risk-s2"
    with _patched_dirs(root / "state") as dirs:
        p1 = _pack(dirs, f"{s1}.pack.md", frontmatter="risk: high\n")
        p2 = _pack(dirs, f"{s2}.pack.md")
        g0 = _goal(dirs, gid, cwd=cwd, pack=p1, slice_id=s1, pending=[s1, s2])
        g0["current_impl_summary"] = _impl_summary(dirs, s1)
        (dirs["goals"] / f"{gid}.json").write_text(json.dumps(g0), encoding="utf-8")

        calls = _drive(dirs, ask=_ask(f"e2e-{s1}", s1, paths=["src/core.py"]),
                       cwd=cwd, goal=_reload_goal(dirs, gid))
        assert len(_gate_calls(calls)) == 1, "high-risk slice must still be reviewed"
        a1 = _answer(dirs, f"e2e-{s1}")
        assert a1.get("synthetic") is not True, a1
        assert _reload_goal(dirs, gid)["review_seq"] == 1

        goal = _reload_goal(dirs, gid)
        goal.update(current_slice=s2, current_pack=p2,
                    current_impl_summary=_impl_summary(dirs, s2))
        (dirs["goals"] / f"{gid}.json").write_text(json.dumps(goal), encoding="utf-8")
        calls = _drive(dirs, ask=_ask(f"e2e-{s2}", s2), cwd=cwd,
                       goal=_reload_goal(dirs, gid))
        assert _gate_calls(calls) == [], "s2 is still a non-bearer → defer"
        a2 = _answer(dirs, f"e2e-{s2}")
        assert a2["synthetic"] is True and a2["gate_policy"] == GE.GATE_POLICY_DEFER, a2

        # bearer gate on top → two gate sessions for the whole Goal
        goal = _reload_goal(dirs, gid)
        goal["pending_slices"] = [s2]
        (dirs["goals"] / f"{gid}.json").write_text(json.dumps(goal), encoding="utf-8")
        calls = _drive(dirs, ask=_ask(f"e2e-{s2}-bear", s2), cwd=cwd,
                       goal=_reload_goal(dirs, gid))
        assert len(_gate_calls(calls)) == 1
        assert _reload_goal(dirs, gid)["review_seq"] == 2


# --------------------------------------------------------------------------
# 3) skip_gate + src/ diff must not buy a free pass
# --------------------------------------------------------------------------
def test_skip_gate_with_src_never_skips_e2e(tmp_path: Path):
    root, cwd = _setup(tmp_path)
    gid = "unit-e2e-skip"
    s1 = "unit-e2e-skip-s1"
    s2 = "unit-e2e-skip-s2"
    with _patched_dirs(root / "state") as dirs:
        p1 = _pack(dirs, f"{s1}.pack.md", frontmatter="skip_gate: true\n")
        _pack(dirs, f"{s2}.pack.md")
        g0 = _goal(dirs, gid, cwd=cwd, pack=p1, slice_id=s1, pending=[s1, s2])
        g0["current_impl_summary"] = _impl_summary(dirs, s1)
        (dirs["goals"] / f"{gid}.json").write_text(json.dumps(g0), encoding="utf-8")

        _drive(dirs, ask=_ask(f"e2e-{s1}", s1, paths=["src/app/main.py"]),
               cwd=cwd, goal=_reload_goal(dirs, gid))
        a1 = _answer(dirs, f"e2e-{s1}")
        assert a1.get("gate_policy") != GE.GATE_POLICY_SKIP, a1
        assert a1.get("synthetic") is not True or a1["gate_policy"] == GE.GATE_POLICY_DEFER, a1
        # the src/ diff is deferred onto the bearer, never silently skipped
        deferred = _reload_goal(dirs, gid).get("gate_deferred") or []
        assert len(deferred) == 1 and deferred[0]["policy"] == GE.GATE_POLICY_DEFER, deferred


# --------------------------------------------------------------------------
# 4) s2 wiring is live on the spawn path: 28 steps + paused_limit in the pack
# --------------------------------------------------------------------------
def test_gate_spawn_carries_soft_cap_and_caliber(tmp_path: Path):
    root, cwd = _setup(tmp_path)
    with _patched_dirs(root / "state") as dirs:
        env = tb._spawn_env("gate")
        assert env.get("DSH_STEP_BUDGET") == "28", env.get("DSH_STEP_BUDGET")
        assert env.get("DSH_GATE_MAX_STEPS") == "28", env.get("DSH_GATE_MAX_STEPS")
        # no leakage into non-gate roles
        assert tb._spawn_env("impl").get("DSH_GATE_MAX_STEPS") is None

        gid, sid = "unit-e2e-cap", "unit-e2e-cap-s1"
        pack = _pack(dirs, f"{sid}.pack.md")
        g0 = _goal(dirs, gid, cwd=cwd, pack=pack, slice_id=sid, pending=[sid])
        g0["current_impl_summary"] = _impl_summary(dirs, sid)
        (dirs["goals"] / f"{gid}.json").write_text(json.dumps(g0), encoding="utf-8")

        calls = _drive(dirs, ask=_ask("e2e-cap", sid), cwd=cwd,
                       goal=_reload_goal(dirs, gid))
        gates = _gate_calls(calls)
        assert len(gates) == 1, calls
        text = (tb.PACKS / str(gates[0]["pack_name"])).read_text(encoding="utf-8")
        assert "paused_limit" in text, "gate pack must teach paused_limit"
        assert "28" in text, "gate pack must state the soft cap"
