"""F1 — a gate review must never overwrite an earlier gate verdict.

The incident: a mid-ticket ``submit_for_review`` with no prior findings built
its pack with ``build_gate_pack(round_n=1)``, so it rewrote the chain round's
own ``<slice>-gate-r1.pack.md`` whose ``summary_out`` still pointed at
``<slice>-gate-r1.md``; the gate agent wrote both ``-gate-rev1.md`` and
``-gate-r1.md`` and round 1's conclusion (plus ``rounds[0].gate_summary``) was
lost.

Covered here:
  1. rev1 (no prior findings) gets ``-gate-rev1.pack.md`` / ``-gate-rev1.md``
     and leaves the chain round-1 pack/summary bytes untouched;
  2. a rev with prior findings gets ``-gate-delta-rev<N>.pack.md`` with
     ``summary_out`` = ``-gate-rev<N>.md``;
  3. an already used ``-gate-rev1.md`` is bumped to ``-gate-rev1-2``;
  4. a non-resume chain round whose ``-gate-r1.md`` already carries a verdict
     uses ``-gate-r1-2`` and records it in ``round_rec``;
  5. a resume reuses the recorded name (a bump is never re-bumped).
"""
from __future__ import annotations

import importlib.util
import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_gate_naming", ROOT / "trial-broker.py")
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


def _write_summary(path: Path, block: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "prose\n\n```json\n" + json.dumps(block, ensure_ascii=False) + "\n```\n",
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


def _ask(ask_id: str, slice_id: str) -> dict:
    return {
        "ask_id": ask_id,
        "slice": slice_id,
        "summary": "did the thing",
        "changed_files": ["a.py"],
        "base": "",
        "commit": "",
        "usage_prompt": 100,
    }


def _verdict_block(verdict: str, findings: list | None = None) -> dict:
    return {"block": {"verdict": verdict, "findings": findings or [], "unmet_acceptance": []}, "text": ""}


def _pack_frontmatter_summary(pack: Path) -> str:
    """The ``summary_out`` basename declared by a written gate pack."""
    for line in pack.read_text(encoding="utf-8").splitlines():
        if line.startswith("summary_out:"):
            return Path(line.split(":", 1)[1].strip()).name
    raise AssertionError(f"no summary_out in {pack}")


def _drive_review(dirs: dict, *, ask_id: str, slice_id: str, cwd: Path, goal=None) -> dict:
    """Call the real submit_for_review handler with the gate spawn mocked."""
    seen: dict = {}

    def fake_open_slice(**kwargs):
        seen.update(kwargs)
        kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["log_path"].write_text("gate log\n", encoding="utf-8")
        return 0

    pending = dirs["mailbox"] / "pending" / f"{ask_id}.json"
    pending.parent.mkdir(parents=True, exist_ok=True)
    pending.write_text("{}", encoding="utf-8")
    with mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), \
            mock.patch.object(tb, "write_artifacts", side_effect=_fake_artifacts), \
            mock.patch.object(tb, "_parse_gate_verdict", return_value=_verdict_block("PASS")):
        tb._handle_submit_for_review(
            _ask(ask_id, slice_id), pending, default_profile="acp-lite",
            cwd=str(cwd), goal=goal,
        )
    return seen


def _chain_job(slice_id: str, cwd: Path, *, max_rounds: int = 1) -> dict:
    return {
        "id": f"unit-f1-{slice_id}", "type": "chain", "slice": slice_id,
        "pack": f"{slice_id}.pack.md", "acceptance": ["do the thing"],
        "profile": "acp-lite", "gate_profile": "acp-lite", "cwd": str(cwd),
        "max_rounds": max_rounds, "goal": None,
    }


def _drive_chain(dirs: dict, root: Path, slice_id: str, *, resume: bool = False,
                 rounds: list | None = None) -> tuple[int, dict, list]:
    """Run run_chain_rounds with both foreman and gate spawns faked."""
    pack = dirs["packs"] / f"{slice_id}.pack.md"
    pack.write_text(f"---\nslice_id: {slice_id}\n---\n# pack\n", encoding="utf-8")
    job = _chain_job(slice_id, root)
    chain = tb._new_chain_state(job)
    chain["rounds"] = list(rounds or [])
    dest = dirs["processing"] / f"{slice_id}.json"
    calls: list = []

    def fake_open_slice(**kwargs):
        calls.append(kwargs)
        kwargs["log_path"].parent.mkdir(parents=True, exist_ok=True)
        kwargs["log_path"].write_text("unit log\n", encoding="utf-8")
        summary = dirs["summaries"] / f"{kwargs['summary_name']}.md"
        if kwargs.get("role") == "gate":
            _write_summary(summary, {"verdict": "PASS", "findings": [], "unmet_acceptance": []})
        else:
            _write_summary(summary, {
                "status": "done", "changed_files": ["a.py"], "base": "b",
                "commit": "c", "questions": [], "notes": "ok",
            })
        return 0

    with mock.patch.object(tb, "run_open_slice", side_effect=fake_open_slice), \
            mock.patch.object(tb, "write_artifacts", side_effect=_fake_artifacts), \
            mock.patch.object(tb, "maybe_notify_hub", return_value={"sent": True}):
        rc = tb.run_chain_rounds(job, chain, dest, start_round=1,
                                 start_pack=pack.name, resume=resume)
    return rc, chain, calls


def _gate_calls(calls: list) -> list:
    return [c for c in calls if c.get("role") == "gate"]


# --------------------------------------------------------------------------
# 1) rev1 gets its own names, chain round 1 is left alone
# --------------------------------------------------------------------------
def test_rev1_review_names_are_own_and_round1_untouched(tmp_path: Path):
    slice_id = "unit-f1-rev1"
    with _patched_dirs(tmp_path / "state") as dirs:
        pack_r1 = dirs["packs"] / f"{slice_id}-gate-r1.pack.md"
        sum_r1 = dirs["summaries"] / f"{slice_id}-gate-r1.md"
        pack_r1.write_text("---\nslice_id: x\n---\n# chain round 1 pack\n", encoding="utf-8")
        sum_r1.write_text("ROUND-1 VERDICT\n", encoding="utf-8")
        before = (pack_r1.read_bytes(), sum_r1.read_bytes())

        seen = _drive_review(dirs, ask_id="f1-rev1", slice_id=slice_id, cwd=tmp_path)

        assert seen["pack_name"] == f"{slice_id}-gate-rev1.pack.md", seen["pack_name"]
        assert seen["summary_name"] == f"{slice_id}-gate-rev1", seen["summary_name"]
        # pack and summary must agree on the file the gate writes
        assert _pack_frontmatter_summary(dirs["packs"] / seen["pack_name"]) == f"{slice_id}-gate-rev1.md"
        # the chain round-1 evidence is untouched, byte for byte
        assert (pack_r1.read_bytes(), sum_r1.read_bytes()) == before


# --------------------------------------------------------------------------
# 2) rev with prior findings → delta pack, still its own summary
# --------------------------------------------------------------------------
def test_rev_with_prior_findings_uses_delta_pack(tmp_path: Path):
    slice_id = "unit-f1-delta"
    goal = {
        "goal": "unit-f1-delta-goal", "status": "running", "slice": slice_id,
        "review_seq": 1, "current_pack": f"{slice_id}.pack.md",
        "current_acceptance": ["do the thing"],
        "last_gate_findings": [{"tier": "P1", "file": "a.py", "issue": "still wrong"}],
    }
    with _patched_dirs(tmp_path / "state") as dirs:
        (dirs["packs"] / f"{slice_id}.pack.md").write_text(
            f"---\nslice_id: {slice_id}\n---\n# orig\n", encoding="utf-8")
        seen = _drive_review(dirs, ask_id="f1-delta", slice_id=slice_id,
                             cwd=tmp_path, goal=goal)

        # review_seq 1 → this review is rev2
        assert seen["pack_name"] == f"{slice_id}-gate-delta-rev2.pack.md", seen["pack_name"]
        assert seen["summary_name"] == f"{slice_id}-gate-rev2", seen["summary_name"]
        assert _pack_frontmatter_summary(dirs["packs"] / seen["pack_name"]) == f"{slice_id}-gate-rev2.md"
        assert json.loads((dirs["goals"] / "unit-f1-delta-goal.json").read_text())["review_seq"] == 2


# --------------------------------------------------------------------------
# 3) an existing rev verdict is bumped, never overwritten
# --------------------------------------------------------------------------
def test_existing_rev_summary_is_bumped(tmp_path: Path):
    slice_id = "unit-f1-bump"
    with _patched_dirs(tmp_path / "state") as dirs:
        sum_rev1 = dirs["summaries"] / f"{slice_id}-gate-rev1.md"
        sum_rev1.write_text("FIRST REVIEW VERDICT\n", encoding="utf-8")

        seen = _drive_review(dirs, ask_id="f1-bump", slice_id=slice_id, cwd=tmp_path)

        assert seen["pack_name"] == f"{slice_id}-gate-rev1-2.pack.md", seen["pack_name"]
        assert seen["summary_name"] == f"{slice_id}-gate-rev1-2", seen["summary_name"]
        assert _pack_frontmatter_summary(dirs["packs"] / seen["pack_name"]) == f"{slice_id}-gate-rev1-2.md"
        assert sum_rev1.read_text(encoding="utf-8") == "FIRST REVIEW VERDICT\n"
        assert not (dirs["summaries"] / f"{slice_id}-gate-r1.md").exists()


# --------------------------------------------------------------------------
# 4) non-resume chain round with a verdict on disk → -gate-r1-2
# --------------------------------------------------------------------------
def test_chain_round_bumps_when_verdict_exists(tmp_path: Path):
    slice_id = "unit-f1-chain"
    with _patched_dirs(tmp_path / "state") as dirs:
        sum_r1 = dirs["summaries"] / f"{slice_id}-gate-r1.md"
        sum_r1.write_text("OLD ROUND-1 VERDICT\n", encoding="utf-8")

        rc, chain, calls = _drive_chain(dirs, tmp_path, slice_id)

        assert rc == 0, rc
        assert chain["state"] == "PASS", chain.get("state")
        gate = _gate_calls(calls)
        assert len(gate) == 1, calls
        assert gate[0]["pack_name"] == f"{slice_id}-gate-r1-2.pack.md", gate[0]["pack_name"]
        assert gate[0]["summary_name"] == f"{slice_id}-gate-r1-2", gate[0]["summary_name"]
        rec = chain["rounds"][0]
        assert rec["gate_pack"] == f"{slice_id}-gate-r1-2.pack.md", rec["gate_pack"]
        assert Path(rec["gate_summary"]).name == f"{slice_id}-gate-r1-2.md", rec["gate_summary"]
        assert Path(rec["gate_summary"]).is_file()
        # the earlier verdict survives, and no -3/-gate-r1 overwrite happened
        assert sum_r1.read_text(encoding="utf-8") == "OLD ROUND-1 VERDICT\n"
        assert not (dirs["packs"] / f"{slice_id}-gate-r1-3.pack.md").exists()


# --------------------------------------------------------------------------
# 5) resume reuses the recorded (possibly bumped) name — never re-bumps
# --------------------------------------------------------------------------
def test_chain_resume_reuses_recorded_gate_names(tmp_path: Path):
    slice_id = "unit-f1-resume"
    with _patched_dirs(tmp_path / "state") as dirs:
        impl_summary = dirs["summaries"] / f"{slice_id}-impl-r1.md"
        _write_summary(impl_summary, {
            "status": "done", "changed_files": ["a.py"], "base": "b",
            "commit": "c", "questions": [], "notes": "ok",
        })
        sum_r1 = dirs["summaries"] / f"{slice_id}-gate-r1.md"
        sum_r1.write_text("OLD ROUND-1 VERDICT\n", encoding="utf-8")
        recorded = dirs["summaries"] / f"{slice_id}-gate-r1-2.md"
        _write_summary(recorded, {"verdict": "PASS", "findings": [], "unmet_acceptance": []})
        rec = {
            "round": 1, "impl_ticket": f"impl-trial-{slice_id}-r1",
            "impl_pack": f"{slice_id}.pack.md", "impl_exit": 0,
            "impl_summary": str(impl_summary), "impl_artifacts": {"assert_clean": True},
            "foreman_status": "done",
            "foreman_block": {"status": "done", "base": "b", "commit": "c", "questions": []},
            "gate_pack": f"{slice_id}-gate-r1-2.pack.md", "gate_exit": 0,
            "gate_summary": str(recorded), "gate_artifacts": {"assert_clean": True},
            "gate_peak": 1, "gate_steps": 1,
        }

        rc, chain, calls = _drive_chain(dirs, tmp_path, slice_id, resume=True, rounds=[rec])

        assert rc == 0, rc
        assert chain["state"] == "PASS", chain.get("state")
        # the gate ticket is not re-opened, and no fresh -3 name is invented
        assert _gate_calls(calls) == [], calls
        assert chain["rounds"][0]["gate_summary"] == str(recorded)
        assert sum_r1.read_text(encoding="utf-8") == "OLD ROUND-1 VERDICT\n"
        assert not (dirs["packs"] / f"{slice_id}-gate-r1-3.pack.md").exists()
        assert not (dirs["summaries"] / f"{slice_id}-gate-r1-3.md").exists()


# --------------------------------------------------------------------------
# helper contract: names, tags and pack→summary mapping stay symmetric
# --------------------------------------------------------------------------
def test_review_name_helpers_are_symmetric(tmp_path: Path):
    slice_id = "unit-f1-helper"
    with _patched_dirs(tmp_path / "state"):
        for delta in (False, True):
            pack_name, summary_name = tb._unique_review_names(slice_id, "rev1", delta=delta)
            assert tb._gate_summary_name_for_pack(pack_name) == summary_name
            assert tb._gate_review_tag(slice_id, summary_name) == "rev1"
            # free name → the plain label
            assert summary_name == f"{slice_id}-gate-rev1"
        (tb.SUMMARIES / f"{slice_id}-gate-rev1.md").write_text("taken\n", encoding="utf-8")
        pack_name, summary_name = tb._unique_review_names(slice_id, "rev1")
        assert (pack_name, summary_name) == (
            f"{slice_id}-gate-rev1-2.pack.md", f"{slice_id}-gate-rev1-2"
        )
        assert tb._gate_review_tag(slice_id, summary_name) == "rev1-2"
        # a pack whose declared summary already has a verdict also counts as taken
        stale = tb.PACKS / f"{slice_id}-gate-rev2.pack.md"
        stale.write_text(
            "---\nslice_id: x\nsummary_out: /x/summaries/"
            f"{slice_id}-gate-rev2.md\n---\n# stale\n",
            encoding="utf-8",
        )
        (tb.SUMMARIES / f"{slice_id}-gate-rev2.md").write_text("old\n", encoding="utf-8")
        assert tb._unique_review_names(slice_id, "rev2")[1] == f"{slice_id}-gate-rev2-2"
