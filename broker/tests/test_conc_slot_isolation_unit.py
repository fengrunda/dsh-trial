#!/usr/bin/env python3
"""T1 concurrency prerequisites: per-Goal slot isolation (no scheduler yet).

Covers the pieces that let several Goals run side by side safely:
  * ``ensure_slot_homes`` layout + idempotency + never clobbering real files
  * ``_spawn_env`` slot wiring (DSH_HOMES_ROOT / DSH_TRIAL_SLOT / DSH_TRIAL_GOAL)
  * ticket→session lookup across slot homes
  * mailbox ask ownership (``ask_belongs_to`` / ``watch_should_handle_ask``)
  * two concurrent watchers never steal each other's ask
  * goal-tagged log prefix, heartbeat ``slots``, atomic goal/chain writes
  * slice-id collision rename (+ pack copy) and the unchanged no-conflict path

Run: python3 -m pytest broker/tests/test_conc_slot_isolation_unit.py -q
"""
from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_spec = importlib.util.spec_from_file_location("trial_broker_conc_slot", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(tb)

T = tb.T


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def _clear_registry() -> None:
    for rec in tb.active_goals_snapshot():
        tb.unregister_active_goal(str(rec.get("goal") or ""))


@pytest.fixture(autouse=True)
def _fresh_registry():
    _clear_registry()
    yield
    _clear_registry()


def _make_dsh_home(root: Path) -> Path:
    """Minimal $DSH_HOME lookalike: env files, profiles/, canonical thin-state."""
    dsh = root / "dsh"
    (dsh / "profiles").mkdir(parents=True, exist_ok=True)
    for name in ("acp-trial", "acp-lite-trial"):
        (dsh / "profiles" / name).mkdir(exist_ok=True)
    for name in ("settings.yaml", ".env", "load-env.sh"):
        (dsh / name).write_text(f"# {name}\n", encoding="utf-8")
    thin = dsh / "supervisor" / "thin-state"
    for name in ("packs", "summaries", "chains", "goals"):
        (thin / name).mkdir(parents=True, exist_ok=True)
    (thin / "index.json").write_text("{}\n", encoding="utf-8")
    return dsh


def _patch_homes(homes: Path, dsh: Path):
    return mock.patch.multiple(
        tb,
        DSH_HOME=dsh,
        HOMES_ROOT=homes,
        SLOT_HOMES_ROOT=homes / "trial-slots",
    )


# ---------------------------------------------------------------------------
# 1. slot homes
# ---------------------------------------------------------------------------
def test_ensure_slot_homes_layout_and_idempotent(tmp_path):
    dsh = _make_dsh_home(tmp_path)
    homes = tmp_path / "dsh-homes"
    template_impl = homes / "impl" / "profiles"
    template_impl.mkdir(parents=True)
    (template_impl / "acp").symlink_to(dsh / "profiles" / "acp-trial")
    (template_impl / "node_modules").mkdir()

    slot_root = homes / "trial-slots" / "slot-2"
    # Pre-existing real content in the slot home must survive untouched.
    (slot_root / "gate").mkdir(parents=True)
    (slot_root / "gate" / "load-env.sh").write_text("KEEP\n", encoding="utf-8")
    (slot_root / "gate" / "sessions").mkdir()
    (slot_root / "gate" / "sessions" / "keep.txt").write_text("k\n", encoding="utf-8")

    canonical_ts = dsh / "supervisor" / "thin-state"
    with _patch_homes(homes, dsh):
        root = tb.ensure_slot_homes(2)
        assert root == slot_root

        for role in ("impl", "gate", "supervisor"):
            home = root / role
            for name in ("settings.yaml", ".env", "load-env.sh"):
                link = home / name
                if role == "gate" and name == "load-env.sh":
                    assert not link.is_symlink()
                    assert link.read_text(encoding="utf-8") == "KEEP\n"
                    continue
                assert link.is_symlink(), f"{link} should be a symlink"
                assert link.resolve() == (dsh / name).resolve()
            for name in ("sessions", "storages", "acp-tickets", "offload"):
                d = home / name
                assert d.is_dir() and not d.is_symlink(), f"{d} must be a real per-slot dir"
            thin = home / "supervisor" / "thin-state"
            for name in ("packs", "summaries", "chains", "goals", "index.json"):
                link = thin / name
                assert link.is_symlink(), f"{link} should be a symlink"
                assert link.resolve() == (canonical_ts / name).resolve()

        assert (root / "gate" / "sessions" / "keep.txt").read_text(encoding="utf-8") == "k\n"

        # profiles: template role home when present (node_modules linked as a dir),
        # else $DSH_HOME/profiles.
        impl_profiles = root / "impl" / "profiles"
        assert (impl_profiles / "acp").is_symlink()
        assert (impl_profiles / "acp").resolve() == (dsh / "profiles" / "acp-trial").resolve()
        assert (impl_profiles / "node_modules").is_symlink()
        assert (impl_profiles / "node_modules").resolve() == (template_impl / "node_modules").resolve()
        sup_profiles = root / "supervisor" / "profiles"
        assert (sup_profiles / "acp-lite-trial").is_symlink()
        assert (sup_profiles / "acp-lite-trial").resolve() == (
            dsh / "profiles" / "acp-lite-trial"
        ).resolve()

        # Idempotent: a second call reuses the very same symlink inodes.
        watched = [root / "impl" / "settings.yaml", impl_profiles / "acp", impl_profiles / "node_modules"]
        inodes = [os.lstat(p).st_ino for p in watched]
        entries = sorted(p.name for p in impl_profiles.iterdir())
        assert tb.ensure_slot_homes(2) == root
        assert [os.lstat(p).st_ino for p in watched] == inodes
        assert sorted(p.name for p in impl_profiles.iterdir()) == entries

        # Distinct slot numbers get distinct roots.
        slot_one = tb.ensure_slot_homes(1)
        assert slot_one == homes / "trial-slots" / "slot-1"
        assert (slot_one / "impl" / "settings.yaml").is_symlink()
        assert (slot_one / "impl" / "settings.yaml").resolve() == (dsh / "settings.yaml").resolve()


# ---------------------------------------------------------------------------
# 2. spawn env
# ---------------------------------------------------------------------------
def test_spawn_env_slot_and_goal(tmp_path):
    homes = tmp_path / "dsh-homes"
    dsh = _make_dsh_home(tmp_path)
    with _patch_homes(homes, dsh):
        env = tb._spawn_env("impl", ticket="impl-trial-s1-r1", slot=3, goal="goal-env-1")
        assert env["DSH_HOMES_ROOT"] == str(homes / "trial-slots" / "slot-3")
        assert env["DSH_TRIAL_SLOT"] == "3"
        assert env["DSH_TRIAL_GOAL"] == "goal-env-1"
        assert env["DSH_TICKET"] == "impl-trial-s1-r1"
        assert env["DSH_ROLE"] == "impl"

        # No slot anywhere: old behaviour, DSH_HOMES_ROOT is merely passed through.
        legacy = tb._spawn_env("impl")
        assert legacy["DSH_HOMES_ROOT"] == os.environ["DSH_HOMES_ROOT"]
        assert "DSH_TRIAL_SLOT" not in legacy
        assert "DSH_TRIAL_GOAL" not in legacy

        # Thread context supplies both slot and goal when not passed explicitly.
        with tb.goal_context("goal-env-2", 5):
            ctx_env = tb._spawn_env("gate")
            assert ctx_env["DSH_HOMES_ROOT"] == str(homes / "trial-slots" / "slot-5")
            assert ctx_env["DSH_TRIAL_SLOT"] == "5"
            assert ctx_env["DSH_TRIAL_GOAL"] == "goal-env-2"

        # Explicit goal wins over the context goal.
        with tb.goal_context("goal-env-3", 5):
            assert tb._spawn_env("gate", goal="goal-env-4")["DSH_TRIAL_GOAL"] == "goal-env-4"


# ---------------------------------------------------------------------------
# 3. ticket → session lookup across slot homes
# ---------------------------------------------------------------------------
def test_find_session_searches_slot_homes(tmp_path):
    homes = tmp_path / "dsh-homes"
    dsh = _make_dsh_home(tmp_path)
    slot_root = homes / "trial-slots" / "slot-1"

    sid_impl = "11111111-2222-3333-4444-555555555555"
    impl_session = slot_root / "impl" / "sessions" / sid_impl / "session.v3.jsonl"
    impl_session.parent.mkdir(parents=True)
    impl_session.write_text("{}\n", encoding="utf-8")

    sid_gate = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    gate_home = slot_root / "gate"
    (gate_home / "acp-tickets").mkdir(parents=True)
    (gate_home / "acp-tickets" / "gate-trial-s1-rev1.json").write_text(
        json.dumps({"sessionId": sid_gate}), encoding="utf-8"
    )
    gate_session = gate_home / "sessions" / sid_gate / "session.v3.jsonl.zstd"
    gate_session.parent.mkdir(parents=True)
    gate_session.write_text("{}", encoding="utf-8")
    # find_session ranks candidates by mtime; make the gate session the newest so
    # the assertion is deterministic on coarse-granularity filesystems.
    newer = impl_session.stat().st_mtime + 10
    os.utime(gate_session, (newer, newer))

    with _patch_homes(homes, dsh):
        assert tb.all_role_homes("impl")[0] == homes / "impl"
        assert slot_root / "impl" in tb.all_role_homes()
        # A session living only under a slot home is found.
        assert tb.find_session("gate-trial-s1-rev1", "gate") == gate_session
        os.utime(gate_session, (newer - 20, newer - 20))
        assert tb.find_session("impl-trial-s1-r1", "impl") == impl_session


# ---------------------------------------------------------------------------
# 4. ask ownership
# ---------------------------------------------------------------------------
def _goal(gid: str, slices: list[str], current: str | None = None) -> dict:
    return {"goal": gid, "slices": [{"slice": s} for s in slices], "current_slice": current or ""}


def test_ask_belongs_to_branches():
    goal = _goal("goal-a", ["s-a", "s-b"], current="s-b")

    # own ticket
    assert tb.ask_belongs_to({"ticket": "ticket-own", "slice": "whatever"}, ticket="ticket-own", goal=goal)
    # same goal, other slice (exact slice field, and via ticket name)
    assert tb.ask_belongs_to({"ticket": "impl-trial-s-a-r1", "slice": "s-a"}, ticket="ticket-own", goal=goal)
    assert tb.ask_belongs_to({"ticket": "gate-trial-s-b-rev1"}, ticket="ticket-own", goal=goal)
    assert tb.ask_belongs_to({"ticket": "x", "slice": "s-b"}, ticket="ticket-own", goal=goal)
    # another goal's ask
    assert not tb.ask_belongs_to({"ticket": "impl-trial-s-x-r1", "slice": "s-x"}, ticket="ticket-own", goal=goal)
    assert not tb.ask_belongs_to({"ticket": "ticket-other", "slice": "s-x"}, ticket="ticket-own", goal=goal)
    # token boundaries: slice s-a must not match s-a10
    narrow = _goal("goal-a", ["s-a"])
    assert not tb.ask_belongs_to({"ticket": "gate-trial-s-a10-rev1"}, ticket="ticket-own", goal=narrow)
    # no goal and no matching ticket → not ours
    assert not tb.ask_belongs_to({"ticket": "other"}, ticket="ticket-own", goal=None)
    # goal id inside the ask ticket (supervisor-plan-<goal>)
    assert tb.ask_belongs_to({"ticket": "supervisor-plan-goal-a"}, ticket="ticket-own", goal=goal)


def test_watch_should_handle_ask_orphan_and_other_goal():
    mine = _goal("goal-mine", ["s-mine"], current="s-mine")
    other = _goal("goal-other", ["s-other"], current="s-other")

    # No Goal at all (legacy): everything is handled, as before.
    assert tb.watch_should_handle_ask({"ticket": "anything"}, ticket="t-legacy", goal=None)

    tb.register_active_goal("goal-mine", ticket="t-mine", slices=["s-mine"], current_slice="s-mine")
    # Single active Goal: an orphan keeps the old permissive behaviour.
    assert tb.watch_should_handle_ask({"ticket": "mystery", "slice": "nope"}, ticket="t-mine", goal=mine)

    tb.register_active_goal("goal-other", ticket="t-other", slices=["s-other"], current_slice="s-other")
    # Two active Goals: the orphan is left to its owner.
    assert not tb.watch_should_handle_ask({"ticket": "mystery", "slice": "nope"}, ticket="t-mine", goal=mine)
    # Another Goal's ask is never handled here…
    other_ask = {"ticket": "impl-trial-s-other-r1", "slice": "s-other"}
    assert not tb.watch_should_handle_ask(other_ask, ticket="t-mine", goal=mine)
    assert tb.ask_disposition(other_ask, ticket="t-mine", goal=mine) == "other"
    # …but its owner still handles it.
    assert tb.watch_should_handle_ask(other_ask, ticket="t-other", goal=other)
    assert tb.ask_disposition(other_ask, ticket="t-other", goal=other) == "mine"
    assert tb.active_goal_count() == 2


# ---------------------------------------------------------------------------
# 5. two watchers, two goals, one shared mailbox
# ---------------------------------------------------------------------------
def test_two_goal_watchers_only_handle_their_own_ask(tmp_path):
    mailbox = tmp_path / "mailbox"
    (mailbox / "pending").mkdir(parents=True)
    (mailbox / "answers").mkdir()
    asks = {
        "ask-a": {"ask_id": "ask-a", "ticket": "t-mine", "slice": "s-mine", "kind": "ask_supervisor"},
        "ask-b": {"ask_id": "ask-b", "ticket": "t-other", "slice": "s-other", "kind": "ask_supervisor"},
    }
    for ask_id, body in asks.items():
        (mailbox / "pending" / f"{ask_id}.json").write_text(json.dumps(body), encoding="utf-8")

    goals = {"ask-a": _goal("goal-mine", ["s-mine"], "s-mine"), "ask-b": _goal("goal-other", ["s-other"], "s-other")}
    tickets = {"goal-mine": "t-mine", "goal-other": "t-other"}
    tb.register_active_goal("goal-mine", slot=1, ticket="t-mine", slices=["s-mine"], current_slice="s-mine")
    tb.register_active_goal("goal-other", slot=2, ticket="t-other", slices=["s-other"], current_slice="s-other")

    handled: dict[str, list[str]] = {}
    seen = {ask_id: threading.Event() for ask_id in asks}
    lock = threading.Lock()

    def fake_handle(path, *, default_profile, cwd, goal):
        ask_id = path.stem
        with lock:
            handled.setdefault(ask_id, []).append(tb.current_goal_id() or "-")
        seen[ask_id].set()

    def fake_run(cmd, env=None, timeout=None):
        deadline = time.time() + 15
        for ev in seen.values():
            ev.wait(max(0.0, deadline - time.time()))
        time.sleep(0.3)
        return subprocess.CompletedProcess(cmd, 0)

    homes = tmp_path / "dsh-homes"
    dsh = _make_dsh_home(tmp_path)
    errs: list[BaseException] = []

    def worker(ask_id: str):
        goal = goals[ask_id]
        gid = str(goal["goal"])
        try:
            with tb.goal_context(gid, 1 if gid == "goal-mine" else 2):
                tb.run_open_slice(
                    ticket=tickets[gid],
                    pack_name=f"{gid}.pack.md",
                    profile="acp-lite",
                    cwd=str(tmp_path),
                    role="impl",
                    summary_name=f"{gid}-impl",
                    prompt_mode="foreman",
                    log_path=tmp_path / f"{tickets[gid]}.log",
                    goal=goal,
                    watch_mailbox=True,
                )
        except BaseException as e:  # pragma: no cover - surfaced below
            errs.append(e)

    with _patch_homes(homes, dsh), mock.patch.multiple(
        tb,
        MAILBOX=mailbox,
        ARTIFACT_ROOT=tmp_path / "artifacts",
        _handle_pending_ask_file=fake_handle,
    ), mock.patch.object(tb.subprocess, "run", side_effect=fake_run):
        threads = [threading.Thread(target=worker, args=(ask_id,)) for ask_id in asks]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

    assert not errs, errs
    assert sorted(handled) == ["ask-a", "ask-b"], handled
    # Each ask was only ever handled by its own Goal's watcher.
    assert set(handled["ask-a"]) == {"goal-mine"}, handled
    assert set(handled["ask-b"]) == {"goal-other"}, handled


# ---------------------------------------------------------------------------
# 6. logging context
# ---------------------------------------------------------------------------
def test_log_lines_carry_goal_and_slot(capsys):
    with tb.goal_context("goal-log-1", 2):
        tb.print("[trial-broker] spawn open-slice ticket=x")
    out = capsys.readouterr().out
    assert "[trial-broker][goal=goal-log-1][slot=2] spawn open-slice ticket=x" in out

    tb.print("[trial-broker] idle")
    assert "[trial-broker][goal=-][slot=-] idle" in capsys.readouterr().out

    tb.print("plain line")
    assert "[trial-broker][goal=-][slot=-] plain line" in capsys.readouterr().out

    # file= output is never rewritten (pid files / stderr diagnostics).
    tb.print("to-stderr", file=sys.stderr)
    captured = capsys.readouterr()
    assert captured.err.strip() == "to-stderr"
    assert "[goal=" not in captured.err


# ---------------------------------------------------------------------------
# 7. heartbeat
# ---------------------------------------------------------------------------
def test_heartbeat_slots_and_concurrent_writers(tmp_path):
    state = tmp_path / "state"
    hb = state / "trial-broker.heartbeat.json"
    with mock.patch.multiple(tb, STATE_DIR=state, HEARTBEAT=hb):
        tb.register_active_goal("goal-hb-1", slot=1, cwd="/w1", ticket="t1", phase="running")
        tb.register_active_goal("goal-hb-2", slot=2, cwd="/w2", ticket="t2", phase="planning")
        tb._write_heartbeat(3)

        body = json.loads(hb.read_text(encoding="utf-8"))
        assert body["pending"] == 3
        assert set(body) >= {"pid", "at", "wall", "mono", "pending", "slots", "max_concurrent_goals"}
        assert body["max_concurrent_goals"] == T.max_concurrent_goals()
        slots = {s["goal"]: s for s in body["slots"]}
        assert set(slots) == {"goal-hb-1", "goal-hb-2"}
        assert slots["goal-hb-1"]["slot"] == 1
        assert slots["goal-hb-1"]["phase"] == "running"
        assert slots["goal-hb-2"]["started_at"]

        def writer(i: int) -> None:
            for _ in range(25):
                tb._write_heartbeat(i)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        body = json.loads(hb.read_text(encoding="utf-8"))  # still valid JSON
        assert {s["goal"] for s in body["slots"]} == {"goal-hb-1", "goal-hb-2"}
        # Atomic write: no tmp debris left behind.
        assert [p.name for p in state.iterdir()] == ["trial-broker.heartbeat.json"]


def test_max_concurrent_goals_resolution(tmp_path, monkeypatch):
    limits = tmp_path / "limits.json"
    limits.write_text(json.dumps({"max_concurrent_goals": 4}), encoding="utf-8")
    monkeypatch.delenv(T.MAX_CONCURRENT_GOALS_ENV, raising=False)
    with mock.patch.object(T, "LIMITS_PATH", limits):
        assert T.max_concurrent_goals() == 4
        monkeypatch.setenv(T.MAX_CONCURRENT_GOALS_ENV, "7")
        assert T.max_concurrent_goals() == 7  # env wins
        monkeypatch.setenv(T.MAX_CONCURRENT_GOALS_ENV, "0")
        assert T.max_concurrent_goals() == int(T.DEFAULT_LIMITS["max_concurrent_goals"])
        monkeypatch.setenv(T.MAX_CONCURRENT_GOALS_ENV, "nonsense")
        assert T.max_concurrent_goals() == int(T.DEFAULT_LIMITS["max_concurrent_goals"])

    empty = tmp_path / "no-limits.json"
    with mock.patch.object(T, "LIMITS_PATH", empty):
        monkeypatch.setenv(T.MAX_CONCURRENT_GOALS_ENV, "3")
        assert T.max_concurrent_goals() == 3


# ---------------------------------------------------------------------------
# 8. atomic state writes
# ---------------------------------------------------------------------------
def test_goal_and_chain_writes_are_atomic(tmp_path):
    goals = tmp_path / "goals"
    chains = tmp_path / "chains"
    with mock.patch.multiple(tb, GOALS=goals, CHAINS=chains):
        goal_path = tb._write_goal({"goal": "goal-atomic", "status": "running"})
        raw = goal_path.read_text(encoding="utf-8")
        assert raw.endswith("\n")
        assert '  "goal": "goal-atomic"' in raw  # indent=2 kept
        assert json.loads(raw)["updated_at"]
        assert [p.name for p in goals.iterdir()] == ["goal-atomic.json"]

        chain_path = tb._write_chain({"slice": "s-atomic", "state": "running"})
        raw = chain_path.read_text(encoding="utf-8")
        assert raw.endswith("\n")
        assert json.loads(raw)["updated_at"]
        assert [p.name for p in chains.iterdir()] == ["s-atomic.json"]


# ---------------------------------------------------------------------------
# 9. slice-id collision rename
# ---------------------------------------------------------------------------
def test_normalize_goal_slices_renames_collision_and_copies_pack(tmp_path):
    chains = tmp_path / "chains"
    packs = tmp_path / "packs"
    chains.mkdir(parents=True)
    packs.mkdir(parents=True)
    (chains / "s1.json").write_text(
        json.dumps({"slice": "s1", "goal": "goal-other", "state": "running"}), encoding="utf-8"
    )
    (packs / "s1.pack.md").write_text("# s1 pack\n", encoding="utf-8")
    (packs / "s2.pack.md").write_text("# s2 pack\n", encoding="utf-8")

    block = {
        "action": "emit_chains",
        "slices": [
            {"slice": "s1", "pack": "s1.pack.md", "acceptance": ["a"]},
            {"slice": "s2", "pack": "s2.pack.md", "acceptance": ["b"]},
        ],
    }
    goal_state = {"goal": "goal-mine"}
    with mock.patch.multiple(tb, CHAINS=chains, PACKS=packs):
        specs = tb._normalize_goal_slices(block, {}, "goal-mine", goal_state)

    assert [s["slice"] for s in specs] == ["goal-mine--s1", "s2"]
    assert specs[0]["pack"] == "goal-mine--s1.pack.md"
    assert specs[0]["acceptance"] == ["a"]
    # Pack copied under the new name, original untouched.
    assert (packs / "goal-mine--s1.pack.md").read_text(encoding="utf-8") == "# s1 pack\n"
    assert (packs / "s1.pack.md").is_file()
    assert goal_state["slice_renames"][0]["from"] == "s1"
    assert goal_state["slice_renames"][0]["to"] == "goal-mine--s1"

    # No conflict → nothing renamed, nothing recorded.
    clean_state = {"goal": "goal-mine"}
    with mock.patch.multiple(tb, CHAINS=chains, PACKS=packs):
        clean = tb._normalize_goal_slices(
            {"action": "emit_chains", "slices": [{"slice": "s2", "pack": "s2.pack.md"}]},
            {},
            "goal-mine",
            clean_state,
        )
    assert [s["slice"] for s in clean] == ["s2"]
    assert clean[0]["pack"] == "s2.pack.md"
    assert "slice_renames" not in clean_state

    # A slice owned by a live Goal (no chain file yet) collides too.
    tb.register_active_goal("goal-other", slot=1, ticket="t-other", slices=["s3"])
    live_state = {"goal": "goal-mine"}
    (packs / "s3.pack.md").write_text("# s3 pack\n", encoding="utf-8")
    with mock.patch.multiple(tb, CHAINS=chains, PACKS=packs):
        live = tb._normalize_goal_slices(
            {"slices": [{"slice": "s3", "pack": "s3.pack.md"}]}, {}, "goal-mine", live_state
        )
    assert [s["slice"] for s in live] == ["goal-mine--s3"]
    assert (packs / "goal-mine--s3.pack.md").read_text(encoding="utf-8") == "# s3 pack\n"

    # A chain already owned by *this* Goal is not a collision.
    (chains / "s4.json").write_text(
        json.dumps({"slice": "s4", "goal": "goal-mine", "state": "PASS"}), encoding="utf-8"
    )
    mine_state = {"goal": "goal-mine"}
    with mock.patch.multiple(tb, CHAINS=chains, PACKS=packs):
        mine = tb._normalize_goal_slices(
            {"slices": [{"slice": "s4", "pack": "s4.pack.md"}]}, {}, "goal-mine", mine_state
        )
    assert [s["slice"] for s in mine] == ["s4"]
    assert "slice_renames" not in mine_state
