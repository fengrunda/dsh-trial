#!/usr/bin/env python3
"""Unit tests: limits priority, routes dispatch, goal-update, limit payload, by_kind, HOLD no-wake.

No model / no dsh spawn. Run with either:
  python3 -m pytest broker/tests/test_limits_routes_unit.py -q
  python3 broker/tests/test_limits_routes_unit.py
"""
from __future__ import annotations

import importlib.util
import json
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_mod", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

T = tb.T
TM = tb.TM


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
@contextmanager
def _limits_file(root: Path, data: dict):
    p = root / "limits.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    old = T.LIMITS_PATH
    T.LIMITS_PATH = p
    try:
        yield p
    finally:
        T.LIMITS_PATH = old


@contextmanager
def _patched_dirs(root: Path):
    names = ("goals", "processing", "outbox", "failed", "chains", "packs", "summaries", "artifacts")
    dirs = {n: root / n for n in names}
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    with mock.patch.object(tb, "GOALS", dirs["goals"]), mock.patch.object(
        tb, "PROCESSING", dirs["processing"]
    ), mock.patch.object(tb, "OUTBOX", dirs["outbox"]), mock.patch.object(
        tb, "FAILED", dirs["failed"]
    ), mock.patch.object(tb, "CHAINS", dirs["chains"]), mock.patch.object(
        tb, "PACKS", dirs["packs"]
    ), mock.patch.object(tb, "SUMMARIES", dirs["summaries"]), mock.patch.object(
        tb, "ARTIFACT_ROOT", dirs["artifacts"]
    ), mock.patch.object(tb, "maybe_offload_gc", lambda **k: None):
        yield dirs


def _write_summary(path: Path, block: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "prose\n\n```json\n" + json.dumps(block, ensure_ascii=False) + "\n```\n",
        encoding="utf-8",
    )


# --------------------------------------------------------------------------
# tests
# --------------------------------------------------------------------------
def test_limits_priority():
    with tempfile.TemporaryDirectory() as td:
        with _limits_file(Path(td), {"max_slices": 5, "max_rounds": 3}):
            base = T.resolve_limits()
            assert base["max_slices"] == 5
            assert base["_sources"]["max_slices"] == "default"

            job = {"max_slices": 4}
            r = T.resolve_limits(job=job)
            assert r["max_slices"] == 4 and r["_sources"]["max_slices"] == "job"

            goal = {"limits_overrides": {"max_slices": 2}, "limits_override_via": "goal"}
            r = T.resolve_limits(job=job, goal=goal)
            assert r["max_slices"] == 2 and r["_sources"]["max_slices"] == "goal"

            goal2 = {
                "limits_effective": {"max_slices": 1},
                "limits_sources": {"max_slices": "update"},
            }
            r = T.resolve_limits(job=job, goal=goal2)
            assert r["max_slices"] == 1 and r["_sources"]["max_slices"] == "update"
    print("OK limits priority")


def test_routes_lookup():
    routes = T.load_routes()
    assert routes.get("routes"), "routes.json should be present"

    r1 = T.find_route("supervisor", "ask_supervisor", routes)
    assert r1 and r1.get("handler") == "supervisor_answer", r1
    r2 = T.find_route("gate", "submit_for_review", routes)
    assert r2 and r2.get("handler") == "gate_review", r2
    assert T.find_route("supervisor", "nope", routes) is None
    # empty to_role → kind-only match
    assert T.find_route("", "ask_supervisor", routes) is not None

    custom = {"routes": [{"to_role": "x", "kind": "y", "handler": "gate_review"}]}
    assert T.find_route("x", "y", custom)["handler"] == "gate_review"
    assert T.find_route("z", "y", custom) is None
    with tempfile.TemporaryDirectory() as td:
        assert T.load_routes(Path(td) / "missing.json") == {"version": 1, "routes": []}
    print("OK routes lookup")


def test_dispatch_and_no_route():
    routes = T.load_routes()
    assert TM.dispatch_target({"to_role": "supervisor", "kind": "ask_supervisor"}, routes) == "ask_supervisor"
    # kind-only ask (no to_role) still routes
    assert TM.dispatch_target({"kind": "submit_for_review"}, routes) == "submit_for_review"
    assert TM.dispatch_target({"to_role": "gate", "kind": "made_up"}, routes) is None
    assert TM.no_route_answer("ask-1") == {"ask_id": "ask-1", "ok": False, "error": "no route"}
    print("OK route dispatch")


def test_goal_update_apply():
    with tempfile.TemporaryDirectory() as td:
        with _limits_file(Path(td), {"max_slices": 3}):
            g: dict = {}
            T.apply_goal_update(g, {"max_slices": 9})
            assert g["limits_effective"]["max_slices"] == 9
            assert g["limits_sources"]["max_slices"] == "update"
            assert "max_slices" in g["last_update_fields"]

            g2: dict = {}
            T.apply_goal_update(g2, {"cancel": True, "reason": "nope"})
            assert g2["status"] == "cancelled"
            assert g2["cancel_reason"] == "nope"
    print("OK apply_goal_update")


def test_run_goal_update_job():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        with _limits_file(root, {"max_slices": 3, "max_rounds": 2}):
            with _patched_dirs(root / "state") as dirs:
                gid = "unit-goal"
                (dirs["goals"] / f"{gid}.json").write_text(
                    json.dumps(
                        {
                            "goal": gid,
                            "status": "escalated",
                            "slices": [],
                            "limits_overrides": {},
                            "limit_hit": {"limit": "max_slices", "limit_value": 3, "used": 3},
                        }
                    ),
                    encoding="utf-8",
                )
                jp = root / "job.json"
                jp.write_text(json.dumps({"type": "goal-update", "goal": gid, "max_slices": 7}), encoding="utf-8")
                job = tb.load_job(jp)
                assert job["type"] == "goal-update", job
                rc = tb.run_goal_update_job(jp, job)
                assert rc == 0, rc
                out = json.loads((dirs["goals"] / f"{gid}.json").read_text(encoding="utf-8"))
                assert out["limits_effective"]["max_slices"] == 7, out.get("limits_effective")
                assert out["limit_hit_resolved"]["resolved_by"] == "goal-update"
                assert out.get("limit_hit") is None
                assert any(p.name.endswith("job.json") for p in dirs["outbox"].iterdir())

                # cancel path
                gid2 = "unit-goal-cancel"
                (dirs["goals"] / f"{gid2}.json").write_text(
                    json.dumps({"goal": gid2, "status": "running", "slices": []}), encoding="utf-8"
                )
                jp2 = root / "job2.json"
                jp2.write_text(
                    json.dumps({"type": "goal-update", "goal": gid2, "cancel": True, "reason": "stop"}),
                    encoding="utf-8",
                )
                rc2 = tb.run_goal_update_job(jp2, tb.load_job(jp2))
                assert rc2 == 0, rc2
                out2 = json.loads((dirs["goals"] / f"{gid2}.json").read_text(encoding="utf-8"))
                assert out2["status"] == "cancelled", out2

                # missing goal state → failed
                jp3 = root / "job3.json"
                jp3.write_text(json.dumps({"type": "goal-update", "goal": "nope"}), encoding="utf-8")
                rc3 = tb.run_goal_update_job(jp3, tb.load_job(jp3))
                assert rc3 == 1, rc3
    print("OK run_goal_update_job")


def test_limit_hit_payload_and_rework():
    hit = T.limit_hit_payload(
        which="max_slices",
        limit=3,
        used=3,
        slice_id="s1",
        step="open_slice",
        goal_id="g1",
        suggestion="raise max_slices",
    )
    assert hit["limit"] == "max_slices" and hit["used"] == 3
    assert hit["goal"] == "g1" and hit["suggestion"] == "raise max_slices" and hit["at"]

    limits = {"inplace_rework_max_prompt": 20000, "inplace_rework_max_findings": 3}
    assert T.decide_rework_mode(usage_prompt=100, findings=[{"tier": "P1"}], limits=limits) == "inplace"
    assert T.decide_rework_mode(usage_prompt=100, findings=[{"tier": "P0"}], limits=limits) == "fresh"
    assert T.decide_rework_mode(usage_prompt=10**9, findings=[], limits=limits) == "fresh"

    mode, ans = TM.decide_and_answer_review(
        ask={"ask_id": "a1", "usage_prompt": 10},
        gate_block={"verdict": "HOLD", "findings": [{"tier": "P1"}]},
        gate_ticket="g1",
        limits=limits,
    )
    assert mode == "inplace" and ans["verdict"] == "HOLD" and ans["rework_mode"] == "inplace"

    mode2, ans2 = TM.decide_and_answer_review(
        ask={"ask_id": "a2"},
        gate_block={"verdict": "PASS", "findings": []},
        gate_ticket="g2",
        limits=limits,
    )
    assert mode2 == "" and ans2["verdict"] == "PASS"
    print("OK limit payload + rework mode")


def test_by_kind_report():
    g1 = {"goal": "g1", "status": "done", "slices": [], "metrics": T.empty_metrics()}
    T.record_ticket_metric(
        g1, {"role": "supervisor", "kind": "ask_supervisor", "wait_ms": 100, "wake_prompt_token_total": 50}
    )
    T.record_ticket_metric(
        g1,
        {"role": "gate", "kind": "submit_for_review", "wait_ms": 200, "timeout": True, "wake_prompt_token_total": 30},
    )
    T.record_ticket_metric(g1, {"role": "impl", "kind": "ask_supervisor", "wait_ms": 50})

    bk = g1["metrics"]["by_kind"]
    assert bk["ask_supervisor"] == {
        "count": 2,
        "wait_ms_total": 150,
        "timeouts": 0,
        "wake_prompt_token_total": 50,
    }, bk
    assert bk["submit_for_review"]["count"] == 1
    assert bk["submit_for_review"]["timeouts"] == 1
    assert bk["submit_for_review"]["wake_prompt_token_total"] == 30

    with tempfile.TemporaryDirectory() as td:
        gdir = Path(td) / "goals"
        gdir.mkdir()
        (gdir / "g1.json").write_text(json.dumps(g1), encoding="utf-8")
        rep = T.aggregate_report(goals_dir=gdir)
        assert rep["by_kind"]["ask_supervisor"]["count"] == 2
        assert rep["by_kind"]["submit_for_review"]["wake_prompt_token_total"] == 30
        text = T.format_report_zh(rep)
        assert "by_kind" in text
    print("OK by_kind report")


def test_pending_ask_route_and_no_route():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        pending = root / "pending"
        pending.mkdir(parents=True)
        (root / "answers").mkdir()
        old_mailbox = tb.MAILBOX
        tb.MAILBOX = root
        try:
            p1 = pending / "a1.json"
            p1.write_text(
                json.dumps({"ask_id": "a1", "to_role": "supervisor", "kind": "ask_supervisor"}),
                encoding="utf-8",
            )
            with mock.patch.object(tb, "_handle_ask_supervisor") as m1, mock.patch.object(
                tb, "_handle_submit_for_review"
            ) as m2:
                tb._handle_pending_ask_file(p1, default_profile="acp-lite", cwd=str(root), goal=None)
                assert m1.call_count == 1 and m2.call_count == 0

            p2 = pending / "a2.json"
            p2.write_text(
                json.dumps({"ask_id": "a2", "to_role": "gate", "kind": "submit_for_review"}),
                encoding="utf-8",
            )
            with mock.patch.object(tb, "_handle_ask_supervisor") as m1, mock.patch.object(
                tb, "_handle_submit_for_review"
            ) as m2:
                tb._handle_pending_ask_file(p2, default_profile="acp-lite", cwd=str(root), goal=None)
                assert m2.call_count == 1 and m1.call_count == 0

            p3 = pending / "a3.json"
            p3.write_text(
                json.dumps({"ask_id": "a3", "to_role": "gate", "kind": "made_up"}),
                encoding="utf-8",
            )
            tb._handle_pending_ask_file(p3, default_profile="acp-lite", cwd=str(root), goal=None)
            ans = json.loads((root / "answers" / "a3.json").read_text(encoding="utf-8"))
            assert ans == {"ask_id": "a3", "ok": False, "error": "no route"}, ans
        finally:
            tb.MAILBOX = old_mailbox
    print("OK pending ask route + no-route")


def test_hold_does_not_wake_supervisor():
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        with _patched_dirs(root / "state") as dirs:
            slice_id = "unit-hold-nowake"
            pack = dirs["packs"] / f"{slice_id}.pack.md"
            pack.write_text(f"---\nslice_id: {slice_id}\n---\n# unit\n", encoding="utf-8")
            calls = {"auto": 0}

            def fake_open_slice(**kwargs):
                summary = dirs["summaries"] / f"{kwargs['summary_name']}.md"
                mode = kwargs["prompt_mode"]
                log = kwargs["log_path"]
                log.parent.mkdir(parents=True, exist_ok=True)
                log.write_text("fake open-slice\n", encoding="utf-8")
                if mode == "foreman":
                    _write_summary(
                        summary,
                        {
                            "status": "done",
                            "changed_files": ["a.py"],
                            "base": "b",
                            "commit": "c",
                            "questions": [],
                        },
                    )
                else:
                    _write_summary(
                        summary,
                        {"verdict": "HOLD", "findings": [{"tier": "P1", "file": "a.py", "issue": "x"}]},
                    )
                return 0

            def fake_artifacts(job, log_path, summary_path, ec, ticket=None):
                return {
                    "ticket": ticket or job.get("ticket"),
                    "exit_code": ec,
                    "assert_clean": True,
                    "composition": None,
                    "summary": str(summary_path),
                }

            def must_not_wake(*a, **k):
                calls["auto"] += 1
                raise AssertionError("auto_supervisor_answer must NOT be called on HOLD")

            job = {
                "id": "nw",
                "type": "chain",
                "slice": slice_id,
                "pack": pack.name,
                "acceptance": [],
                "profile": "acp-lite",
                "gate_profile": "acp-lite",
                "cwd": str(root),
                "max_rounds": 1,
                "goal": None,
            }
            chain = tb._new_chain_state(job)
            chain["rounds"] = []
            dest = dirs["processing"] / f"{slice_id}.json"

            with mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), mock.patch.object(
                tb, "write_artifacts", side_effect=fake_artifacts
            ), mock.patch.object(
                tb, "auto_supervisor_answer", side_effect=must_not_wake
            ), mock.patch.object(
                tb, "build_gate_pack", return_value=pack.name
            ):
                rc = tb.run_chain_rounds(job, chain, dest, start_round=1, start_pack=pack.name)

            assert calls["auto"] == 0
            assert rc == 1, rc
            state = json.loads((dirs["chains"] / f"{slice_id}.json").read_text(encoding="utf-8"))
            assert state["state"] == "escalated", state
    print("OK HOLD no-wake")


def main() -> int:
    test_limits_priority()
    test_routes_lookup()
    test_dispatch_and_no_route()
    test_goal_update_apply()
    test_run_goal_update_job()
    test_limit_hit_payload_and_rework()
    test_by_kind_report()
    test_pending_ask_route_and_no_route()
    test_hold_does_not_wake_supervisor()
    print("ALL LIMITS/ROUTES UNIT OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
