#!/usr/bin/env python3
"""Per-ticket reasoning effort + step budget passthrough for the ACP ask layer.

dsh-acp-ask.py must set ``reasoning_effort`` on the fresh/resumed session
(off|low|high|max; medium→high, xhigh→max) and export DSH_STEP_BUDGET to the
dsh child process, while a rejected option never sinks the ticket.
"""
from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent  # .../broker
REPO = ROOT.parent
OPEN_SLICE = REPO / "open-slice.sh"

spec = importlib.util.spec_from_file_location("dsh_acp_ask_effort_mod", ROOT / "dsh-acp-ask.py")
ask = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ask)


class _FakeClient:
    """Records session/set_config_option calls; optionally fails them."""

    def __init__(self, error: str | None = None):
        self.calls: list[tuple[str, str, str]] = []
        self.error = error

    def session_set_config_option(self, session_id: str, config_id: str, value: str) -> None:
        self.calls.append((session_id, config_id, value))
        if self.error:
            raise RuntimeError(self.error)


# --- normalize_effort -------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("low", "low"),
        ("LOW", "low"),
        ("  High  ", "high"),
        ("medium", "high"),
        ("Medium", "high"),
        ("xhigh", "max"),
        ("XHIGH", "max"),
        ("none", "off"),
        ("off", "off"),
        ("max", "max"),
        ("", None),
        ("   ", None),
        (None, None),
    ],
)
def test_normalize_effort_maps_aliases_and_case(raw, expected):
    assert ask.normalize_effort(raw) == expected


@pytest.mark.parametrize("raw", ["bogus", "minimal", "medium-high", "1", "none2"])
def test_normalize_effort_rejects_unknown(raw):
    with pytest.raises(ValueError):
        ask.normalize_effort(raw)


# --- apply_session_options --------------------------------------------------


def test_apply_session_options_sets_effort(capsys):
    client = _FakeClient()
    ask.apply_session_options(client, "sid-1", "low")
    assert client.calls == [("sid-1", "reasoning_effort", "low")]
    assert "reasoning_effort=low" in capsys.readouterr().err


def test_apply_session_options_noop_without_effort(capsys):
    client = _FakeClient()
    ask.apply_session_options(client, "sid-1", None)
    ask.apply_session_options(client, "sid-1", "")
    assert client.calls == []
    assert capsys.readouterr().err == ""


def test_apply_session_options_warns_but_does_not_raise(capsys):
    client = _FakeClient(error="unknown reasoning effort: medium")
    ask.apply_session_options(client, "sid-1", "low")  # must not raise
    err = capsys.readouterr().err
    assert "warning" in err
    assert "reasoning_effort=low" in err


# --- main() session-creation path -------------------------------------------


class _FakeAcpClient:
    def __init__(self, error: str | None = None):
        self.sid = "sid-main"
        self.calls: list[tuple[str, str, str]] = []
        self.prompts: list[str] = []
        self.error = error

    def initialize(self) -> None:
        pass

    def session_new(self, cwd, mcp_servers=None) -> str:
        return self.sid

    def session_resume(self, session_id, cwd, mcp_servers=None) -> None:
        pass

    def session_set_config_option(self, session_id: str, config_id: str, value: str) -> None:
        self.calls.append((session_id, config_id, value))
        if self.error:
            raise RuntimeError(self.error)

    def session_prompt(self, session_id: str, prompt: str) -> str:
        self.prompts.append(prompt)
        return "ok"

    def session_close(self, session_id: str) -> None:
        pass

    def close_proc(self) -> None:
        pass


def _run_main(monkeypatch, tmp_path, argv_extra, error=None):
    fake = _FakeAcpClient(error=error)
    seen = []
    real = ask.apply_session_options

    def spy(client, session_id, effort):
        seen.append((session_id, effort))
        return real(client, session_id, effort)

    monkeypatch.setattr(ask, "AcpClient", lambda *a, **k: fake)
    monkeypatch.setattr(ask, "_build_env", lambda: {"DSH_BIN": "dsh-stub"})
    monkeypatch.setattr(ask, "apply_session_options", spy)
    rc = ask.main(
        ["--cwd", str(tmp_path), "--new", "--prompt", "hi"] + list(argv_extra)
    )
    return rc, fake, seen


def test_main_applies_effort_on_new_session(monkeypatch, tmp_path, capsys):
    rc, fake, seen = _run_main(monkeypatch, tmp_path, ["--reasoning-effort", "low"])
    assert rc == 0
    assert seen == [("sid-main", "low")]
    assert fake.calls == [("sid-main", "reasoning_effort", "low")]
    assert "reasoning_effort=low" in capsys.readouterr().err


def test_main_effort_alias_and_env_default(monkeypatch, tmp_path):
    monkeypatch.setenv("DSH_REASONING_EFFORT", "xhigh")
    rc, fake, seen = _run_main(monkeypatch, tmp_path, [])
    assert rc == 0
    assert seen == [("sid-main", "max")]
    assert fake.calls == [("sid-main", "reasoning_effort", "max")]


def test_main_without_effort_does_not_set_option(monkeypatch, tmp_path):
    monkeypatch.delenv("DSH_REASONING_EFFORT", raising=False)
    rc, fake, seen = _run_main(monkeypatch, tmp_path, [])
    assert rc == 0
    assert seen == [("sid-main", None)]
    assert fake.calls == []


def test_main_invalid_effort_exits_2(monkeypatch, tmp_path, capsys):
    monkeypatch.delenv("DSH_REASONING_EFFORT", raising=False)
    rc, _fake, seen = _run_main(monkeypatch, tmp_path, ["--reasoning-effort", "bogus"])
    assert rc == 2
    assert seen == []
    assert "reasoning-effort" in capsys.readouterr().err


def test_main_survives_rejected_effort(monkeypatch, tmp_path, capsys):
    rc, fake, _seen = _run_main(
        monkeypatch, tmp_path, ["--reasoning-effort", "high"], error="boom"
    )
    assert rc == 0
    assert fake.prompts == ["hi"]  # the ticket still ran
    assert "warning" in capsys.readouterr().err


# --- step budget ------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [("30", 30), (" 7 ", 7), ("0", 0), ("-5", 0), ("abc", 0), ("", 0), (None, 0), (12, 12)],
)
def test_parse_step_budget(raw, expected):
    assert ask.parse_step_budget(raw) == expected


def _run_prompts_with_env(monkeypatch, tmp_path, step_budget, env_extra=None):
    captured = {}

    class _Client:
        def __init__(self, dsh_bin, env, profile="acp"):
            captured["env"] = dict(env)

        def initialize(self):
            pass

        def session_new(self, cwd, mcp_servers=None):
            return "sid-1"

        def session_prompt(self, sid, prompt):
            return "ok"

        def session_close(self, sid):
            pass

        def close_proc(self):
            pass

    monkeypatch.setattr(ask, "AcpClient", _Client)
    monkeypatch.delenv("DSH_STEP_BUDGET", raising=False)
    monkeypatch.delenv("DSH_REASONING_EFFORT", raising=False)
    for k, v in (env_extra or {}).items():
        monkeypatch.setenv(k, v)
    rc = ask._run_prompts(
        cwd=str(tmp_path),
        prompts=["hi"],
        ticket=None,
        resume=False,
        keep_open=False,
        force_new=True,
        env={"DSH_BIN": "dsh-stub"},
        step_budget=step_budget,
    )
    assert rc == 0
    return captured["env"]


def test_step_budget_exported_to_child_env(monkeypatch, tmp_path, capsys):
    env = _run_prompts_with_env(monkeypatch, tmp_path, 30)
    assert env["DSH_STEP_BUDGET"] == "30"
    assert "step_budget=30" in capsys.readouterr().err


def test_step_budget_unset_or_zero_not_exported(monkeypatch, tmp_path):
    assert "DSH_STEP_BUDGET" not in _run_prompts_with_env(monkeypatch, tmp_path, 0)
    assert "DSH_STEP_BUDGET" not in _run_prompts_with_env(monkeypatch, tmp_path, None)


def test_step_budget_env_fallback(monkeypatch, tmp_path):
    env = _run_prompts_with_env(
        monkeypatch, tmp_path, 0, env_extra={"DSH_STEP_BUDGET": "12"}
    )
    assert env["DSH_STEP_BUDGET"] == "12"


def test_step_budget_empty_env_not_exported(monkeypatch, tmp_path):
    env = _run_prompts_with_env(
        monkeypatch, tmp_path, 0, env_extra={"DSH_STEP_BUDGET": ""}
    )
    assert "DSH_STEP_BUDGET" not in env


def test_main_forwards_step_budget(monkeypatch, tmp_path):
    captured = {}

    class _Client(_FakeAcpClient):
        def __init__(self, dsh_bin, env, profile="acp"):
            super().__init__()
            captured["env"] = dict(env)

    monkeypatch.setattr(ask, "AcpClient", _Client)
    monkeypatch.setattr(ask, "_build_env", lambda: {"DSH_BIN": "dsh-stub"})
    monkeypatch.delenv("DSH_STEP_BUDGET", raising=False)
    rc = ask.main(["--cwd", str(tmp_path), "--new", "--prompt", "hi", "--step-budget", "9"])
    assert rc == 0
    assert captured["env"]["DSH_STEP_BUDGET"] == "9"


def test_main_rejects_non_positive_step_budget(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(ask, "AcpClient", lambda *a, **k: _FakeAcpClient())
    monkeypatch.setattr(ask, "_build_env", lambda: {"DSH_BIN": "dsh-stub"})
    rc = ask.main(["--cwd", str(tmp_path), "--new", "--prompt", "hi", "--step-budget", "0"])
    assert rc == 2
    assert "step-budget" in capsys.readouterr().err


# --- open-slice.sh ----------------------------------------------------------

BASH = "bash"


def test_open_slice_syntax_ok():
    r = subprocess.run([BASH, "-n", str(OPEN_SLICE)], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_open_slice_forwards_flags_to_ask():
    src = OPEN_SLICE.read_text(encoding="utf-8")
    assert 'ARGS+=(--reasoning-effort "$REASONING_EFFORT")' in src
    assert 'ARGS+=(--step-budget "$STEP_BUDGET")' in src
    # only non-empty values are appended: ask.py re-reads the env otherwise
    assert '[[ -n "$REASONING_EFFORT" ]] && ARGS+=' in src
    assert '[[ -n "$STEP_BUDGET" ]] && ARGS+=' in src


def test_open_slice_accepts_new_flags(tmp_path):
    """--print-prompt exits before ARGS assembly, so assert flag acceptance."""
    pack = tmp_path / "packs" / "p.md"
    pack.parent.mkdir(parents=True)
    pack.write_text("# pack\n", encoding="utf-8")
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": str(tmp_path),
        "DSH_HOME": str(tmp_path / "dsh"),
        "DSH_HOMES_ROOT": str(tmp_path / "homes"),
        "DSH_TRIAL_THIN_STATE": str(tmp_path / "thin"),
        "LC_ALL": "C.UTF-8",
    }
    base = [
        BASH, str(OPEN_SLICE),
        "--ticket", "impl-demo",
        "--pack", str(pack),
        "--cwd", str(tmp_path),
        "--print-prompt",
    ]
    plain = subprocess.run(base, capture_output=True, text=True, env=env, timeout=120)
    with_flags = subprocess.run(
        base + ["--reasoning-effort", "high", "--step-budget", "12"],
        capture_output=True, text=True, env=env, timeout=120,
    )
    assert plain.returncode == 0, plain.stderr
    assert with_flags.returncode == 0, with_flags.stderr
    assert with_flags.stdout == plain.stdout


def test_open_slice_unknown_arg_still_fails(tmp_path):
    r = subprocess.run(
        [BASH, str(OPEN_SLICE), "--ticket", "impl-demo", "--nope"],
        capture_output=True, text=True, timeout=120,
    )
    assert r.returncode == 2
    assert "Unknown arg" in r.stderr
