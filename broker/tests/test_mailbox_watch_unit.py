#!/usr/bin/env python3
"""Unit tests: DSH_TRIAL_MAILBOX env override, mailbox watch answer flow, metrics accounting.

No model / no dsh spawn. Run with either:
  python3 -m pytest broker/tests/test_mailbox_watch_unit.py -q
  python3 broker/tests/test_mailbox_watch_unit.py
"""
from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_mod", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)

T = tb.T


# --------------------------------------------------------------------------
# DSH_TRIAL_MAILBOX env override
# --------------------------------------------------------------------------
def test_trial_mailbox_env_override(tmp_path: Path):
    mb = tmp_path / "custom" / "mailbox"
    with mock.patch.dict(os.environ, {"DSH_TRIAL_MAILBOX": str(mb)}):
        spec = importlib.util.spec_from_file_location("trial_lib_env", ROOT / "trial_lib.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    assert mod.MAILBOX == mb.resolve(), mod.MAILBOX
    # 未设置 env 时回落默认 supervisor/thin-state/mailbox
    env = {k: v for k, v in os.environ.items() if k != "DSH_TRIAL_MAILBOX"}
    with mock.patch.dict(os.environ, env, clear=True):
        spec2 = importlib.util.spec_from_file_location("trial_lib_def", ROOT / "trial_lib.py")
        mod2 = importlib.util.module_from_spec(spec2)
        spec2.loader.exec_module(mod2)
    assert str(mod2.MAILBOX).endswith(str(Path("supervisor") / "thin-state" / "mailbox"))
    print("OK DSH_TRIAL_MAILBOX env override")


# --------------------------------------------------------------------------
# 假 pending → handler 写答案 → 插件 pollAnswer 拿到
# --------------------------------------------------------------------------
def test_pending_ask_watch_and_answer(tmp_path: Path):
    root = tmp_path / "mailbox"
    (root / "pending").mkdir(parents=True)
    (root / "answers").mkdir()
    ask = {
        "ask_id": "w1",
        "to_role": "supervisor",
        "kind": "ask_supervisor",
        "questions": ["继续吗？"],
        "slice": "unit-watch",
    }
    p = root / "pending" / "w1.json"
    p.write_text(json.dumps(ask, ensure_ascii=False), encoding="utf-8")

    # broker watcher 扫描同一 mailbox 的 pending
    assert [x.name for x in T.list_pending_asks(root)] == ["w1.json"]

    def fake_handler(ask_, path, *, default_profile, cwd, goal):
        # 模拟 _handle_ask_supervisor 处理后回写答案（archives pending）
        T.write_ask_answer("w1", "答案是 42", supervisor_ticket="sup-t1", mailbox=root)

    old = tb.MAILBOX
    tb.MAILBOX = root
    try:
        with mock.patch.object(tb, "_handle_ask_supervisor", side_effect=fake_handler):
            tb._handle_pending_ask_file(p, default_profile="acp-lite", cwd=str(tmp_path), goal=None)
    finally:
        tb.MAILBOX = old

    # 插件侧 pollAnswer / askSupervisor(短 timeout) 读到的答案
    ans = json.loads((root / "answers" / "w1.json").read_text(encoding="utf-8"))
    assert ans["answer"] == "答案是 42"
    assert ans["supervisor_ticket"] == "sup-t1"
    # 答案已写 → pending 不再列出（且 pending 已归档）
    assert T.list_pending_asks(root) == []
    assert (root / "archive" / "w1.pending.json").is_file()
    print("OK pending ask watch + answer flow")


# --------------------------------------------------------------------------
# record_ticket_metric：默认 prompt_token_total = peak×steps；超时进 by_kind.timeouts
# --------------------------------------------------------------------------
def test_record_ticket_metric_peak_times_steps():
    g = {"goal": "unit-mb", "status": "active", "metrics": T.empty_metrics()}
    T.record_ticket_metric(g, {
        "role": "impl",
        "ticket": "impl-trial-unit-r1",
        "slice": "unit",
        "exit": 0,
        "peak_prompt": 120,
        "steps": 4,
        "tool_res": "1.0%",
        "assert_clean": True,
        "kind": "impl",
    })
    m = g["metrics"]
    assert len(m["tickets"]) == 1
    row = m["tickets"][0]
    assert row["prompt_token_total"] == 480, row  # peak×steps 默认策略
    assert m["prompt_token_total"] == 480
    assert m["by_role"]["impl"]["count"] == 1
    assert m["by_kind"]["impl"]["count"] == 1

    T.record_ticket_metric(g, {
        "role": "supervisor",
        "kind": "ask_supervisor",
        "wait_ms": 100,
        "timeout": True,
    })
    bk = m["by_kind"]["ask_supervisor"]
    assert bk["timeouts"] == 1 and bk["wait_ms_total"] == 100, bk
    print("OK record_ticket_metric peak*steps + timeout by_kind")


# --------------------------------------------------------------------------
# _handle_ask_supervisor：timeout 路径递增 ask_timeout / by_kind.timeouts，kind=ask_supervisor
# --------------------------------------------------------------------------
def test_ask_handler_timeout_accounting(tmp_path: Path):
    root = tmp_path / "mailbox"
    (root / "pending").mkdir(parents=True)
    (root / "answers").mkdir()
    summary = tmp_path / "sup.md"
    summary.write_text("timeout, no answer\n", encoding="utf-8")

    goal = {
        "goal": "unit-ask-to",
        "status": "active",
        "metrics": T.empty_metrics(),
        "ask_seq": 0,
        "supervisor_ticket_count": 0,
    }
    ask = {
        "ask_id": "to1",
        "to_role": "supervisor",
        "kind": "ask_supervisor",
        "questions": ["q"],
        "slice": "unit",
    }
    p = root / "pending" / "to1.json"
    p.write_text(json.dumps(ask), encoding="utf-8")

    old_mb = tb.MAILBOX
    old_tmb = T.MAILBOX
    tb.MAILBOX = root
    T.MAILBOX = root
    try:
        with mock.patch.object(tb, "build_supervisor_ask_pack", return_value="unit-ask.pack.md"), \
             mock.patch.object(tb, "run_supervisor_ticket", return_value=(124, {"composition": None, "assert_clean": True}, summary)), \
             mock.patch.object(tb, "_write_goal"):
            tb._handle_ask_supervisor(ask, p, default_profile="acp-lite", cwd=str(tmp_path), goal=goal)
    finally:
        tb.MAILBOX = old_mb
        T.MAILBOX = old_tmb

    m = goal["metrics"]
    assert m["ask_timeout"] == 1, m
    assert m["by_kind"]["ask_supervisor"]["timeouts"] == 1
    row = m["tickets"][-1]
    assert row["kind"] == "ask_supervisor"
    assert row["compat_kind"] == "ask_sync"
    assert row["timeout"] is True
    assert isinstance(row.get("wait_ms"), int)
    print("OK ask handler timeout accounting")


# --------------------------------------------------------------------------
# T2b: orphan ask belonging to another Goal must not be handled by our watcher
# --------------------------------------------------------------------------
class _MailboxEnv:
    """Point tb.MAILBOX / T.MAILBOX at a throwaway mailbox for one test."""

    def __init__(self, root: Path):
        self.root = root

    def __enter__(self) -> Path:
        self.old = (tb.MAILBOX, T.MAILBOX)
        tb.MAILBOX = self.root
        T.MAILBOX = self.root
        return self.root

    def __exit__(self, *exc) -> bool:
        tb.MAILBOX, T.MAILBOX = self.old
        return False


class _OnlyActiveGoal:
    """Exactly one active Goal (``gid``) for the duration of the block."""

    def __init__(self, gid: str):
        self.gid = gid

    def __enter__(self) -> str:
        with tb._ACTIVE_GOALS_LOCK:
            self.prev = dict(tb.ACTIVE_GOALS)
            tb.ACTIVE_GOALS.clear()
        tb.register_active_goal(self.gid, ticket="t-g1", slices=["unit-watch"])
        return self.gid

    def __exit__(self, *exc) -> bool:
        tb.unregister_active_goal(self.gid)
        with tb._ACTIVE_GOALS_LOCK:
            tb.ACTIVE_GOALS.clear()
            tb.ACTIVE_GOALS.update(self.prev)
        return False


def _goal(gid: str = "G1") -> dict:
    return {
        "goal": gid,
        "status": "active",
        "ticket": "t-g1",
        "slices": ["unit-watch"],
        "current_slice": "unit-watch",
    }


def test_ask_goal_id_reads_goal_and_from_goal():
    assert tb._ask_goal_id({"goal": " G2 "}) == "G2"
    assert tb._ask_goal_id({"from": {"goal": "G2"}}) == "G2"
    assert tb._ask_goal_id({"from": {"role": "impl"}}) == ""
    assert tb._ask_goal_id({}) == ""
    assert tb._ask_goal_id("not-a-dict") == ""


def test_watch_should_handle_ask_orphan_owned_by_other_goal():
    with _OnlyActiveGoal("G1"):
        goal = _goal("G1")
        orphan_g2 = {
            "ask_id": "t3c-gate-1",
            "to_role": "supervisor",
            "kind": "ask_supervisor",
            "questions": ["q"],
            "goal": "G2",  # 非活跃 Goal：本 watcher 不得接手
        }
        assert tb.ask_disposition(orphan_g2, ticket="t-g1", goal=goal) == "orphan"
        assert tb.watch_should_handle_ask(orphan_g2, ticket="t-g1", goal=goal) is False
        # from.goal 同样识别
        nested = dict(orphan_g2, goal=None, **{"from": {"goal": "G2"}})
        nested.pop("goal")
        assert tb._ask_goal_id(nested) == "G2"
        assert tb.watch_should_handle_ask(nested, ticket="t-g1", goal=goal) is False

        # 未声明 goal 的 orphan 保持旧行为（单活跃 Goal → 处理）
        anon = {k: v for k, v in orphan_g2.items() if k != "goal"}
        assert tb.watch_should_handle_ask(anon, ticket="t-g1", goal=goal) is True

        # 本 Goal 自己的 ask → 处理
        mine = dict(orphan_g2, goal="G1", ticket="t-g1")
        assert tb.ask_disposition(mine, ticket="t-g1", goal=goal) == "mine"
        assert tb.watch_should_handle_ask(mine, ticket="t-g1", goal=goal) is True
        # 无 ticket/slice 但声明 goal=G1 的 ask 仍走 orphan 旧行为
        own_gid_only = dict(orphan_g2, goal="G1")
        assert tb.watch_should_handle_ask(own_gid_only, ticket="t-g1", goal=goal) is True


def test_fail_ask_after_error_writes_answer_and_archives(tmp_path: Path):
    root = tmp_path / "mailbox"
    (root / "pending").mkdir(parents=True)
    (root / "answers").mkdir()
    ask = {"ask_id": "boom1", "to_role": "supervisor", "kind": "ask_supervisor"}
    p = root / "pending" / "boom1.json"
    p.write_text(json.dumps(ask), encoding="utf-8")

    with _MailboxEnv(root):
        with mock.patch.object(tb, "print") as mp:
            tb._fail_ask_after_error(p, ask, RuntimeError("handler blew up"))
            # 第二次调用：答复已存在 → 不覆盖，日志也只打一条
            tb._fail_ask_after_error(p, ask, RuntimeError("handler blew up"))
        lines = [
            ca.args[0]
            for ca in mp.call_args_list
            if ca.args and "mailbox watch error" in str(ca.args[0])
        ]
        assert len(lines) == 1

        ans = json.loads((root / "answers" / "boom1.json").read_text(encoding="utf-8"))
        assert ans["ask_id"] == "boom1"
        assert ans["ok"] is False
        assert ans["status"] == "done"
        assert ans["verdict"] == "HOLD"
        assert ans["rework_mode"] == "fresh"
        assert "handler blew up" in ans["error"]
        assert "end ticket" in ans["instruction"]
        assert not p.exists()
        assert (root / "archive" / "boom1.pending.json").is_file()

        # 已有答复不被覆盖
        (root / "answers" / "boom1.json").write_text(json.dumps({"ok": True}), encoding="utf-8")
        tb._fail_ask_after_error(p, ask, RuntimeError("again"))
        assert json.loads((root / "answers" / "boom1.json").read_text(encoding="utf-8")) == {"ok": True}


def test_watch_except_calls_fail_ask_after_error():
    """`_watch` 的 except 必须把异常交给 _fail_ask_after_error（不再无限重试）。"""
    import re

    src = (ROOT / "trial-broker.py").read_text(encoding="utf-8")
    m = re.search(r"\n        def _watch\(\):(.*?)\n        watcher = threading\.Thread", src, re.S)
    assert m, "watcher loop not found"
    body = m.group(1)
    assert "_fail_ask_after_error(ap, ask, e)" in body
    assert "except Exception as e:" in body
    # except 分支里 _handle_pending_ask_file 的裸 print 重试已被替换
    assert 'mailbox watch error: {e}", flush=True)' not in body


if __name__ == "__main__":
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        test_trial_mailbox_env_override(base / "env")
        test_pending_ask_watch_and_answer(base / "watch")
        test_record_ticket_metric_peak_times_steps()
        test_ask_handler_timeout_accounting(base / "askto")
        test_ask_goal_id_reads_goal_and_from_goal()
        test_watch_should_handle_ask_orphan_owned_by_other_goal()
        test_fail_ask_after_error_writes_answer_and_archives(base / "fail-ask")
        test_watch_except_calls_fail_ask_after_error()
    print("ALL OK")
