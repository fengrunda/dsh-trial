"""T1 — one gate per impl content.

The incident: an impl ticket called ``submit_for_review`` mid-ticket (gate
``rev<N>`` PASSed) and then its chain retired the *same* workspace through the
chain gate (``r<N>``), paying for a second review of identical code.

Covered here (content identity only — the verdict itself is never re-judged):
  a. ``_impl_fingerprint`` — stable per content, tracks committed + uncommitted
     + untracked changes, leaves the real index alone, ``""`` outside a repo;
  b. the review ledger — roundtrip, only PASS/HOLD recorded, empty fingerprint
     and a vanished summary are misses, key includes goal + slice + realpath;
  c. review-path PASS → the chain gate for the same content does not spawn and
     the round carries ``gate_dedup_of``;
  d. changed content / another goal / another slice → the gate spawns as before.
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

spec = importlib.util.spec_from_file_location("trial_broker_gate_dedup", ROOT / "trial-broker.py")
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


def _git(cwd: Path, *args: str):
    return subprocess.run(["git", *args], cwd=str(cwd), capture_output=True, text=True)


def _make_repo(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    rc = _git(root, "init", "-q")
    assert rc.returncode == 0, rc.stderr
    _git(root, "config", "user.email", "unit@example.com")
    _git(root, "config", "user.name", "unit")
    _git(root, "config", "commit.gpgsign", "false")
    (root / "a.py").write_text("x = 1\n", encoding="utf-8")
    assert _git(root, "add", "-A").returncode == 0
    rc = _git(root, "commit", "-qm", "init")
    assert rc.returncode == 0, rc.stderr
    return root


def _write_block(path: Path, block: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "prose\n\n```json\n" + json.dumps(block, ensure_ascii=False) + "\n```\n",
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


def _seed_review(dirs: dict, *, repo: Path, goal_id, slice_id, verdict="PASS",
                 ticket=None) -> tuple[str, Path]:
    """Write a gate summary + ledger record exactly like the review path would."""
    ticket = ticket or f"gate-trial-{slice_id}-rev1"
    summary = _write_block(
        dirs["summaries"] / f"{slice_id}-gate-rev1.md",
        {"verdict": verdict, "findings": [], "unmet_acceptance": []},
    )
    fp = tb._impl_fingerprint(repo)
    tb._record_review(goal_id, slice_id, repo, fp, verdict=verdict,
                      gate_ticket=ticket, gate_summary=summary)
    return fp, summary


# --------------------------------------------------------------------------
# a) _impl_fingerprint
# --------------------------------------------------------------------------
def test_impl_fingerprint_tracks_worktree_content(tmp_path: Path):
    repo = _make_repo(tmp_path / "repo")

    fp1 = tb._impl_fingerprint(repo)
    assert fp1, "a committed repo must fingerprint non-empty"
    assert tb._impl_fingerprint(repo) == fp1, "same content → same fingerprint"

    (repo / "a.py").write_text("x = 2\n", encoding="utf-8")  # tracked, uncommitted
    fp2 = tb._impl_fingerprint(repo)
    assert fp2 != fp1, "uncommitted edit must change the fingerprint"

    (repo / "new.txt").write_text("n\n", encoding="utf-8")  # untracked
    fp3 = tb._impl_fingerprint(repo)
    assert fp3 not in (fp1, fp2), "untracked file must change the fingerprint"

    # the repository's real index was never touched
    assert _git(repo, "diff", "--cached", "--name-only").stdout.strip() == ""
    assert "?? new.txt" in _git(repo, "status", "--porcelain").stdout


def test_impl_fingerprint_empty_outside_a_repo(tmp_path: Path):
    plain = tmp_path / "plain"
    plain.mkdir()
    assert tb._impl_fingerprint(plain) == ""
    assert tb._impl_fingerprint(tmp_path / "missing") == ""


# --------------------------------------------------------------------------
# b) review ledger
# --------------------------------------------------------------------------
def test_review_ledger_roundtrip_and_guards(tmp_path: Path):
    repo = _make_repo(tmp_path / "repo")
    slice_id = "unit-t1-ledger"
    with _patched_dirs(tmp_path / "state") as dirs:
        assert tb._review_ledger_path() == dirs["summaries"].parent / "review-ledger.json"
        fp = tb._impl_fingerprint(repo)
        assert tb._lookup_review("g", slice_id, repo, fp) is None

        summary = _write_block(dirs["summaries"] / f"{slice_id}-gate-rev1.md",
                               {"verdict": "PASS", "findings": []})
        tb._record_review("g", slice_id, repo, fp, verdict="pass",
                          gate_ticket=f"gate-trial-{slice_id}-rev1", gate_summary=summary)

        rec = tb._lookup_review("g", slice_id, repo, fp)
        assert rec is not None
        assert rec["verdict"] == "PASS" and rec["gate_ticket"] == f"gate-trial-{slice_id}-rev1"

        # key includes goal, slice and the real cwd → no cross-talk
        assert tb._lookup_review("other-goal", slice_id, repo, fp) is None
        assert tb._lookup_review("g", "other-slice", repo, fp) is None
        assert tb._lookup_review("g", slice_id, tmp_path / "elsewhere", fp) is None
        # ... and a different content fingerprint too
        (repo / "a.py").write_text("x = 9\n", encoding="utf-8")
        assert tb._lookup_review("g", slice_id, repo, tb._impl_fingerprint(repo)) is None

        # only PASS/HOLD are recorded, and an empty fingerprint is never recorded
        tb._record_review("g", "noverdict", repo, fp, verdict="MAYBE",
                          gate_ticket="t", gate_summary=summary)
        assert tb._lookup_review("g", "noverdict", repo, fp) is None
        tb._record_review("g", "nofp", repo, "", verdict="PASS",
                          gate_ticket="t", gate_summary=summary)
        assert tb._lookup_review("g", "nofp", repo, "") is None
        body = json.loads(tb._review_ledger_path().read_text(encoding="utf-8"))
        assert list(body["reviews"]) == [tb._review_key("g", slice_id, repo, fp)]

        # a record whose summary file vanished is a miss (evidence gone)
        tb._record_review("g", "gone", repo, fp, verdict="HOLD",
                          gate_ticket="gate-trial-gone-rev1", gate_summary=summary)
        assert tb._lookup_review("g", "gone", repo, fp) is not None
        summary.unlink()
        assert tb._lookup_review("g", "gone", repo, fp) is None


# --------------------------------------------------------------------------
# chain driver (foreman + gate spawns faked)
# --------------------------------------------------------------------------
def _goal(slice_id: str, goal_id: str, *, review_seq: int = 0,
          impl_summary: str | None = None) -> dict:
    goal = {
        "goal": goal_id, "status": "running", "slice": slice_id,
        "review_seq": review_seq, "current_slice": slice_id,
        "current_pack": f"{slice_id}.pack.md",
        "current_acceptance": ["do the thing"],
    }
    if impl_summary:
        goal["current_impl_summary"] = impl_summary
    return goal


def _write_goal_file(dirs: dict, goal: dict) -> None:
    (dirs["goals"] / f"{goal['goal']}.json").write_text(
        json.dumps(goal), encoding="utf-8")


def _drive_review(dirs: dict, *, repo: Path, slice_id: str, goal: dict) -> list:
    """Run the real submit_for_review handler with the gate spawn faked."""
    calls: list = []

    def fake_open_slice(**kwargs):
        calls.append(kwargs)
        kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["log_path"].write_text("gate log\n", encoding="utf-8")
        _write_block(dirs["summaries"] / f"{kwargs['summary_name']}.md",
                     {"verdict": "PASS", "findings": [], "unmet_acceptance": []})
        return 0

    ask_id = f"t1-{slice_id}"
    ask = {
        "ask_id": ask_id, "slice": slice_id, "summary": "did the thing",
        "changed_files": ["a.py"], "base": "", "commit": "", "usage_prompt": 100,
        "from": {"ticket": f"impl-trial-{slice_id}-r1"},
    }
    pending = dirs["mailbox"] / "pending" / f"{ask_id}.json"
    pending.parent.mkdir(parents=True, exist_ok=True)
    pending.write_text("{}", encoding="utf-8")
    with mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), \
            mock.patch.object(tb, "write_artifacts", side_effect=_fake_artifacts):
        tb._handle_submit_for_review(ask, pending, default_profile="acp-lite",
                                     cwd=str(repo), goal=goal)
    return calls


def _drive_chain(dirs: dict, *, repo: Path, slice_id: str, goal_id,
                 gate_verdict: str = "PASS") -> tuple[int, dict, list]:
    pack = dirs["packs"] / f"{slice_id}.pack.md"
    pack.write_text(f"---\nslice_id: {slice_id}\n---\n# pack\n", encoding="utf-8")
    job = {
        "id": f"unit-t1-{slice_id}", "type": "chain", "slice": slice_id,
        "pack": f"{slice_id}.pack.md", "acceptance": ["do the thing"],
        "profile": "acp-lite", "gate_profile": "acp-lite", "cwd": str(repo),
        "max_rounds": 1, "goal": goal_id,
    }
    chain = tb._new_chain_state(job)
    dest = dirs["processing"] / f"{slice_id}.json"
    calls: list = []

    def fake_open_slice(**kwargs):
        calls.append(kwargs)
        kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["log_path"].write_text("unit log\n", encoding="utf-8")
        summary = dirs["summaries"] / f"{kwargs['summary_name']}.md"
        if kwargs.get("role") == "gate":
            _write_block(summary, {"verdict": gate_verdict, "findings": [],
                                   "unmet_acceptance": []})
        else:
            _write_block(summary, {
                "status": "done", "changed_files": ["a.py"], "base": "b",
                "commit": "c", "questions": [], "notes": "ok",
            })
        return 0

    with mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), \
            mock.patch.object(tb, "write_artifacts", side_effect=_fake_artifacts), \
            mock.patch.object(tb, "maybe_notify_hub", return_value={"sent": True}):
        rc = tb.run_chain_rounds(job, chain, dest, start_round=1, start_pack=pack.name)
    return rc, chain, calls


def _gate_calls(calls: list) -> list:
    return [c for c in calls if c.get("role") == "gate"]


# --------------------------------------------------------------------------
# c) review-path PASS → chain gate for the same content does not spawn
# --------------------------------------------------------------------------
def test_chain_gate_dedups_review_path_pass(tmp_path: Path):
    repo = _make_repo(tmp_path / "repo")
    slice_id = "unit-t1-dedup"
    goal_id = f"unit-t1-{slice_id}-goal"
    with _patched_dirs(tmp_path / "state") as dirs:
        impl_summary = _write_block(dirs["summaries"] / f"{slice_id}-impl-r1.md",
                                    {"status": "done", "changed_files": ["a.py"]})
        goal = _goal(slice_id, goal_id, impl_summary=str(impl_summary))
        _write_goal_file(dirs, goal)

        review_calls = _drive_review(dirs, repo=repo, slice_id=slice_id, goal=goal)
        assert _gate_calls(review_calls), "the mid-ticket review must still spawn its gate"
        review_ticket = f"gate-trial-{slice_id}-rev1"
        assert _gate_calls(review_calls)[0]["ticket"] == review_ticket

        # the review path recorded the verdict for this exact tree
        fp = tb._impl_fingerprint(repo)
        rec = tb._lookup_review(goal_id, slice_id, repo, fp)
        assert rec is not None and rec["verdict"] == "PASS"
        assert rec["gate_ticket"] == review_ticket

        rc, chain, calls = _drive_chain(dirs, repo=repo, slice_id=slice_id,
                                        goal_id=goal_id, gate_verdict="HOLD")
        assert rc == 0
        assert _gate_calls(calls) == [], "chain gate must not re-review identical content"
        assert not (dirs["summaries"] / f"{slice_id}-gate-r1.md").exists()
        assert sorted(p.name for p in dirs["packs"].glob(f"{slice_id}-gate-r1*")) == []

        round_rec = chain["rounds"][-1]
        assert round_rec["verdict"] == "PASS"
        assert round_rec["gate_dedup_of"] == review_ticket
        assert round_rec["gate_ticket"] == review_ticket
        assert round_rec["gate_summary"] == str(dirs["summaries"] / f"{slice_id}-gate-rev1.md")
        assert round_rec["gate_exit"] == 0
        assert round_rec["gate_peak"] is None and round_rec["gate_steps"] is None
        assert round_rec["gate_artifacts"] == {"dedup_of": review_ticket}
        assert chain["last_verdict"] == "PASS"


# --------------------------------------------------------------------------
# d) changed content → spawn as before
# --------------------------------------------------------------------------
def test_chain_gate_spawns_when_content_changed(tmp_path: Path):
    repo = _make_repo(tmp_path / "repo")
    slice_id = "unit-t1-changed"
    goal_id = f"unit-t1-{slice_id}-goal"
    with _patched_dirs(tmp_path / "state") as dirs:
        _seed_review(dirs, repo=repo, goal_id=goal_id, slice_id=slice_id)
        (repo / "a.py").write_text("x = 42\n", encoding="utf-8")  # new content

        rc, chain, calls = _drive_chain(dirs, repo=repo, slice_id=slice_id,
                                        goal_id=goal_id, gate_verdict="PASS")
        gate = _gate_calls(calls)
        assert len(gate) == 1, "a changed tree must still be gated"
        assert gate[0]["ticket"] == f"gate-trial-{slice_id}-r1"
        round_rec = chain["rounds"][-1]
        assert "gate_dedup_of" not in round_rec
        assert round_rec["verdict"] == "PASS"
        assert rc == 0


# --------------------------------------------------------------------------
# d) other goal / other slice → no cross-talk
# --------------------------------------------------------------------------
def test_chain_gate_spawns_for_other_goal_and_slice(tmp_path: Path):
    repo = _make_repo(tmp_path / "repo")
    slice_id = "unit-t1-keys"
    goal_id = f"unit-t1-{slice_id}-goal"
    with _patched_dirs(tmp_path / "state") as dirs:
        _seed_review(dirs, repo=repo, goal_id="some-other-goal", slice_id=slice_id)
        rc, chain, calls = _drive_chain(dirs, repo=repo, slice_id=slice_id,
                                        goal_id=goal_id, gate_verdict="PASS")
        assert len(_gate_calls(calls)) == 1
        assert "gate_dedup_of" not in chain["rounds"][-1]
        assert rc == 0

    with _patched_dirs(tmp_path / "state2") as dirs:
        _seed_review(dirs, repo=repo, goal_id=goal_id, slice_id="some-other-slice")
        rc, chain, calls = _drive_chain(dirs, repo=repo, slice_id=slice_id,
                                        goal_id=goal_id, gate_verdict="PASS")
        assert len(_gate_calls(calls)) == 1
        assert "gate_dedup_of" not in chain["rounds"][-1]
        assert rc == 0
