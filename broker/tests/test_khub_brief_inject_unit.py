"""Unit tests for khub context injection into the goal brief pack.

Locks ``_khub_goal_context`` + the ``build_goal_brief_pack`` wiring:
- normal injection (heading present, keyword plumbing, byte cap),
- opt-out via ``khub_inject: False`` (no subprocess, no section),
- graceful degradation on subprocess timeout/error,
- over-long stdout truncation to <= 2500 bytes.
"""
from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

spec = importlib.util.spec_from_file_location("trial_broker_khub", ROOT / "trial-broker.py")
tb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tb)


HEADING = "## khub 上下文"
CAP = tb.KHUB_GOAL_CONTEXT_MAX_BYTES


def _dirs(root: Path):
    names = ("goals", "chains", "packs", "summaries", "artifacts", "processing",
             "outbox", "failed", "inbox", "mailbox")
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


def _job(**extra):
    job = {
        "goal": "g1",
        "brief": "do the thing\nmore detail",
        "cwd": "/tmp",
        "max_slices": 1,
        "acceptance_hint": ["code works"],
    }
    job.update(extra)
    return job


def _khub_section(body: str) -> bytes | None:
    if HEADING not in body:
        return None
    tail = body.split(HEADING, 1)[1]
    # Section runs until the next top-level heading.
    for marker in ("\n## ", "\n# "):
        if marker in tail:
            tail = tail.split(marker, 1)[0]
    return (HEADING + tail).encode("utf-8")


def _ok(stdout: str) -> mock.Mock:
    return mock.Mock(returncode=0, stdout=stdout, stderr="")


# --- normal ------------------------------------------------------------------

def test_injects_khub_section_and_passes_keywords(tmp_path):
    patch, d = _dirs(tmp_path)
    job = _job(khub_keywords=["alpha", "beta", "gamma"])
    fake = mock.Mock(return_value=_ok("- concept: alpha\n- concept: beta\n"))
    with patch, mock.patch.object(tb.subprocess, "run", fake):
        name = tb.build_goal_brief_pack(job)
    body = (d["packs"] / name).read_text(encoding="utf-8")

    assert HEADING in body
    sec = _khub_section(body)
    assert sec is not None and len(sec) <= CAP

    assert fake.call_count == 1
    argv = fake.call_args[0][0]
    assert argv[argv.index("--keywords") + 1] == "alpha,beta,gamma"
    assert argv[argv.index("--k") + 1] == "8"
    assert argv[argv.index("--max-bytes") + 1] == str(CAP)
    assert fake.call_args.kwargs.get("timeout") == 10

    # section sits between Brief and Constraints
    assert body.index("## Brief") < body.index(HEADING) < body.index("## Constraints")
    assert "hub_resolve" in body


# --- opt out -----------------------------------------------------------------

def test_khub_inject_false_skips_subprocess_and_section(tmp_path):
    patch, d = _dirs(tmp_path)
    job = _job(khub_inject=False, khub_keywords=["alpha"])
    fake = mock.Mock(return_value=_ok("should not be used"))
    with patch, mock.patch.object(tb.subprocess, "run", fake):
        name = tb.build_goal_brief_pack(job)
    body = (d["packs"] / name).read_text(encoding="utf-8")

    fake.assert_not_called()
    assert HEADING not in body


# --- degrade -----------------------------------------------------------------

def test_timeout_still_builds_pack_with_unavailable_note(tmp_path):
    patch, d = _dirs(tmp_path)
    fake = mock.Mock(side_effect=subprocess.TimeoutExpired(cmd="khub", timeout=10))
    with patch, mock.patch.object(tb.subprocess, "run", fake):
        name = tb.build_goal_brief_pack(_job())
    body = (d["packs"] / name).read_text(encoding="utf-8")

    assert HEADING in body
    assert "khub 上下文不可用" in body


def test_subprocess_error_still_builds_pack_with_unavailable_note(tmp_path):
    patch, d = _dirs(tmp_path)
    fake = mock.Mock(side_effect=RuntimeError("boom\nsecond line"))
    with patch, mock.patch.object(tb.subprocess, "run", fake):
        name = tb.build_goal_brief_pack(_job())
    body = (d["packs"] / name).read_text(encoding="utf-8")

    assert "khub 上下文不可用" in body
    assert "boom" in body
    # reason is collapsed to one line
    assert "boom second line" in body


def test_nonzero_exit_reports_unavailable(tmp_path):
    patch, d = _dirs(tmp_path)
    fake = mock.Mock(return_value=mock.Mock(returncode=2, stdout="partial", stderr="x"))
    with patch, mock.patch.object(tb.subprocess, "run", fake):
        name = tb.build_goal_brief_pack(_job())
    body = (d["packs"] / name).read_text(encoding="utf-8")
    assert "khub 上下文不可用" in body


# --- truncation --------------------------------------------------------------

def test_overlong_stdout_truncated_to_cap(tmp_path):
    patch, d = _dirs(tmp_path)
    huge = "- " + ("概念" * 5000)  # multi-byte, well over the cap
    fake = mock.Mock(return_value=_ok(huge))
    with patch, mock.patch.object(tb.subprocess, "run", fake):
        name = tb.build_goal_brief_pack(_job())
    body = (d["packs"] / name).read_text(encoding="utf-8")

    sec = _khub_section(body)
    assert sec is not None
    assert len(sec) <= CAP
    assert HEADING in body
