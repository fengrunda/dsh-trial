"""Unit tests: Pi-aligned cache hit rate on Goal metrics + report filter."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_BROKER = Path(__file__).resolve().parents[1]
if str(_BROKER) not in sys.path:
    sys.path.insert(0, str(_BROKER))

import trial_lib as T  # noqa: E402


def test_cache_hit_rate_formula():
    assert T.cache_hit_rate(10, 90) == pytest.approx(0.9)
    assert T.cache_hit_rate(100, 50) == pytest.approx(1 / 3)
    assert T.cache_hit_rate(0, 0) is None
    assert T.cache_hit_rate(None, None) is None
    # output is excluded from denom: only input+cacheRead
    assert T.cache_hit_rate(10, 90) == pytest.approx(90 / 100)


def test_cache_hit_low_threshold():
    assert T.cache_hit_low(0.8999) is True
    assert T.cache_hit_low(0.9) is False
    assert T.cache_hit_low(None) is False
    note = T.cache_hit_note(0.5)
    assert "CACHE_HIT_LOW" in note
    assert T.cache_hit_note(0.95) == ""


def test_summarize_session_usage(tmp_path: Path):
    sess = tmp_path / "session.v3.jsonl"
    events = [
        {
            "type": "assistant/message",
            "data": {
                "usage": {
                    "inputTokens": 100,
                    "cacheReadTokens": 900,
                    "outputTokens": 50,
                }
            },
        },
        {
            "type": "assistant/message",
            "data": {
                "usage": {
                    "inputTokens": 200,
                    "cacheReadTokens": 800,
                    "outputTokens": 30,
                }
            },
        },
        {"type": "user/message", "data": {}},
    ]
    sess.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")
    u = T.summarize_session_usage(sess)
    assert u["input"] == 300
    assert u["cacheRead"] == 1700
    assert u["output"] == 80
    assert u["llm_calls"] == 2
    assert u["cache_hit_rate"] == pytest.approx(round(1700 / 2000, 4))
    assert u["cache_hit_low"] is True  # 1700/2000 = 0.85 < 0.9
    assert "CACHE_HIT_LOW" in u["cache_hit_note"]


def test_record_ticket_metric_rolls_up_cache():
    g = {"goal": "g-cache", "status": "active", "metrics": T.empty_metrics()}
    T.record_ticket_metric(
        g,
        {
            "role": "impl",
            "ticket": "t1",
            "peak_prompt": 100,
            "steps": 2,
            "input": 100,
            "cacheRead": 900,
            "output": 40,
        },
    )
    m = g["metrics"]
    assert m["prompt_token_total"] == 200  # peak×steps unchanged
    assert m["input"] == 100
    assert m["cacheRead"] == 900
    assert m["output"] == 40
    assert m["cache_hit_rate"] == pytest.approx(0.9)
    assert m["cache_hit_low"] is False
    row = m["tickets"][0]
    assert row["cache_hit_rate"] == pytest.approx(0.9)

    T.record_ticket_metric(
        g,
        {
            "role": "gate",
            "ticket": "t2",
            "peak_prompt": 50,
            "steps": 1,
            "input": 400,
            "cacheRead": 100,
            "output": 10,
        },
    )
    m = g["metrics"]
    assert m["input"] == 500
    assert m["cacheRead"] == 1000
    # 1000/(500+1000)=2/3 < 0.9
    assert m["cache_hit_low"] is True
    assert "CACHE_HIT_LOW" in m["cache_hit_note"]


def test_record_ticket_metric_from_session(tmp_path: Path):
    sess = tmp_path / "s.jsonl"
    sess.write_text(
        json.dumps(
            {
                "type": "assistant/message",
                "data": {
                    "usage": {
                        "inputTokens": 50,
                        "cacheReadTokens": 50,
                        "outputTokens": 5,
                    }
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    g = {"goal": "g-sess", "metrics": T.empty_metrics()}
    T.record_ticket_metric(
        g,
        {"role": "impl", "ticket": "t", "peak_prompt": 10, "steps": 1, "session": str(sess)},
    )
    m = g["metrics"]
    assert m["input"] == 50
    assert m["cacheRead"] == 50
    assert m["cache_hit_rate"] == pytest.approx(0.5)
    assert m["cache_hit_low"] is True


def test_aggregate_report_flags_and_filter(tmp_path: Path, monkeypatch):
    goals = tmp_path / "goals"
    goals.mkdir()
    low = {
        "goal": "low-g",
        "status": "done",
        "slices": ["s1"],
        "supervisor_ticket_count": 1,
        "created_at": "2026-10-21T00:00:00+08:00",
        "metrics": {
            **T.empty_metrics(),
            "prompt_token_total": 1000,
            "input": 200,
            "cacheRead": 100,
            "output": 20,
            "tickets": [
                {
                    "role": "impl",
                    "ticket": "impl-low",
                    "kind": "impl",
                    "input": 200,
                    "cacheRead": 100,
                    "output": 20,
                    "session": "/tmp/fake-session.jsonl",
                    "prompt_token_total": 1000,
                }
            ],
        },
    }
    T.finish_cache_metrics(low["metrics"])
    for t in low["metrics"]["tickets"]:
        rate = T.cache_hit_rate(t["input"], t["cacheRead"])
        t["cache_hit_rate"] = None if rate is None else round(rate, 4)
        t["cache_hit_low"] = T.cache_hit_low(rate)
        t["cache_hit_note"] = T.cache_hit_note(rate)

    ok = {
        "goal": "ok-g",
        "status": "done",
        "slices": ["s1"],
        "supervisor_ticket_count": 0,
        "created_at": "2026-10-21T00:00:00+08:00",
        "metrics": {
            **T.empty_metrics(),
            "prompt_token_total": 500,
            "input": 50,
            "cacheRead": 950,
            "output": 10,
            "tickets": [
                {
                    "role": "impl",
                    "ticket": "impl-ok",
                    "kind": "impl",
                    "input": 50,
                    "cacheRead": 950,
                    "output": 10,
                    "prompt_token_total": 500,
                }
            ],
        },
    }
    T.finish_cache_metrics(ok["metrics"])
    for t in ok["metrics"]["tickets"]:
        rate = T.cache_hit_rate(t["input"], t["cacheRead"])
        t["cache_hit_rate"] = None if rate is None else round(rate, 4)
        t["cache_hit_low"] = T.cache_hit_low(rate)
        t["cache_hit_note"] = T.cache_hit_note(rate)

    (goals / "low-g.json").write_text(json.dumps(low) + "\n", encoding="utf-8")
    (goals / "ok-g.json").write_text(json.dumps(ok) + "\n", encoding="utf-8")

    # Avoid slow session enrich probing missing paths
    monkeypatch.setattr(
        T,
        "enrich_goal_cache_metrics",
        lambda g, **kw: T.finish_cache_metrics(g.setdefault("metrics", T.empty_metrics())) or g,
    )

    rep = T.aggregate_report(goals_dir=goals)
    assert rep["prompt_token_total"] == 1500
    assert "low-g" in rep["cache_hit_low_goals"]
    assert "ok-g" not in rep["cache_hit_low_goals"]
    assert any(t["ticket"] == "impl-low" for t in rep["tickets_cache_hit_low"])

    text = T.format_report_zh(rep)
    assert "CACHE_HIT_LOW" in text or "缓存命中率 < 90%" in text
    assert "impl-low" in text
    assert "low-g" in text

    filtered = T.filter_report_cache_hit_low(rep)
    assert filtered["filter"] == "cache_hit_low"
    assert [g["goal"] for g in filtered["per_goal"]] == ["low-g"]
    assert filtered["goals"] == 1


def test_cli_report_cache_hit_low_flag(tmp_path: Path, monkeypatch):
    """dsh-trial report --cache-hit-low --json only keeps low Goals."""
    import os
    import subprocess

    home = tmp_path / "dshhome"
    goals = home / "supervisor" / "thin-state" / "goals"
    goals.mkdir(parents=True)
    g = {
        "goal": "only-low",
        "status": "done",
        "slices": [],
        "metrics": {
            "prompt_token_total": 10,
            "input": 80,
            "cacheRead": 20,
            "output": 1,
            "hold_count": 0,
            "rework_rounds": 0,
            "ask_count": 0,
            "tickets": [],
            "by_role": {},
            "by_kind": {},
        },
    }
    T.finish_cache_metrics(g["metrics"])
    (goals / "only-low.json").write_text(json.dumps(g) + "\n", encoding="utf-8")
    ok = {
        "goal": "only-ok",
        "status": "done",
        "slices": [],
        "metrics": {
            "prompt_token_total": 10,
            "input": 5,
            "cacheRead": 95,
            "output": 1,
            "hold_count": 0,
            "rework_rounds": 0,
            "ask_count": 0,
            "tickets": [],
            "by_role": {},
            "by_kind": {},
        },
    }
    T.finish_cache_metrics(ok["metrics"])
    (goals / "only-ok.json").write_text(json.dumps(ok) + "\n", encoding="utf-8")

    state = tmp_path / "state"
    state.mkdir()
    env = {
        **os.environ,
        "DSH_HOME": str(home),
        "TRIAL_BROKER_DIR": str(state),
        "DSH_TRIAL_ROOT": str(_BROKER.parent),
    }
    env.pop("TRIAL_BROKER_HEARTBEAT", None)
    cli = _BROKER.parent / "bin" / "dsh-trial"
    cp = subprocess.run(
        [sys.executable, str(cli), "report", "--json", "--cache-hit-low"],
        capture_output=True,
        text=True,
        env=env,
    )
    assert cp.returncode == 0, cp.stderr + cp.stdout
    rep = json.loads(cp.stdout)
    assert rep.get("filter") == "cache_hit_low"
    names = [g["goal"] for g in rep["per_goal"]]
    assert names == ["only-low"]
