#!/usr/bin/env python3
"""open-slice.sh prompt templates must be literal (no backtick execution).

Regression lock for impl-open-slice-heredoc-literal.  The six ``build_prompt_*``
templates used unquoted ``cat <<PROMPT`` heredocs, so every backticked snippet
inside the prompt body was *executed* while the prompt was built:

* ``git push`` / ``gh pr merge`` / ``gh`` ran for real;
* ``dsh-trial-pr push-and-pr --repo <a|b> --cwd <repo>`` ran (its ``> --cwd``
  redirect even created a stray ``--cwd`` file in the cwd);
* the command output was pasted into the prompt and the backticked text itself
  was lost.

The templates are now ``cat <<'PROMPT'`` bodies with ``@@NAME@@`` markers
rendered in bash, plus a ``--print-prompt`` switch that prints the prompt and
exits without any filesystem write and without calling ask.
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent  # .../broker
OPEN_SLICE = ROOT.parent / "open-slice.sh"

MODES = (
    "baseline",
    "foreman",
    "gate",
    "supervisor-plan",
    "supervisor-answer",
    "supervisor-close",
)
JSON_MODES = ("foreman", "gate", "supervisor-plan", "supervisor-answer", "supervisor-close")
GIT_PUSH_MODES = ("foreman", "supervisor-close")
# The controlled-PR wrapper: backticks, `<...>`, `|` and `>` must stay literal.
PR_LINE = (
    "dsh-trial-pr push-and-pr --repo "
    "<fengrunda/knowledge-hub|fengrunda/memory-as-training> --cwd <repo>"
)
FAKE_TOOLS = ("git", "gh", "dsh-trial-pr", "push", "create-pr")
PACK_NAME = "demo.pack.md"
TICKET = "impl-demo"


def _bash() -> str:
    return shutil.which("bash") or "/bin/bash"


class Sandbox:
    """tmp-only DSH environment plus recording fake CLIs."""

    def __init__(self, tmp_path: Path) -> None:
        self.root = tmp_path
        self.home = tmp_path / "home"
        self.homes = tmp_path / "homes"
        self.work = tmp_path / "work"
        self.bin_dir = tmp_path / "bin"
        self.thin = tmp_path / "thin"
        for d in (self.home, self.homes, self.work, self.bin_dir, self.thin / "packs"):
            d.mkdir(parents=True, exist_ok=True)

        self.pack = self.thin / "packs" / PACK_NAME
        self.pack.write_text("# Demo pack\n\nDone when: nothing to do.\n", encoding="utf-8")

        # Every fake CLI appends one line here; a correct build records nothing.
        self.calls = tmp_path / "calls.txt"
        self.calls.write_text("", encoding="utf-8")
        for tool in FAKE_TOOLS:
            p = self.bin_dir / tool
            p.write_text(
                "#!/usr/bin/env bash\n"
                f'printf "%s %s\\n" "$(basename "$0")" "$*" >> "{self.calls}"\n'
                "exit 0\n",
                encoding="utf-8",
            )
            p.chmod(0o755)

        # A dummy ask that records its own invocation instead of doing anything.
        self.ask_marker = tmp_path / "ask_calls.txt"
        self.ask_marker.write_text("", encoding="utf-8")
        self.ask = tmp_path / "dummy-ask.py"
        self.ask.write_text(
            "#!/usr/bin/env python3\n"
            "import pathlib, sys\n"
            f"pathlib.Path({str(self.ask_marker)!r}).open('a').write(' '.join(sys.argv[1:]) + '\\n')\n",
            encoding="utf-8",
        )
        self.ask.chmod(0o755)

    @property
    def env(self) -> dict:
        return {
            "PATH": f"{self.bin_dir}:/usr/bin:/bin",
            "HOME": str(self.home),
            "DSH_HOME": str(self.home),
            "DSH_HOMES_ROOT": str(self.homes),
            "DSH_TRIAL_THIN_STATE": str(self.thin),
            "DSH_ACP_ASK": str(self.ask),
            "LC_ALL": "C.UTF-8",
        }

    @property
    def summary_out(self) -> str:
        return f"{self.thin}/summaries/demo.md"

    def run_print_prompt(
        self, mode: str, cwd: str | None = None, pack: Path | None = None
    ) -> subprocess.CompletedProcess:
        argv = [
            _bash(), str(OPEN_SLICE),
            "--ticket", TICKET,
            "--pack", str(pack if pack is not None else self.pack),
            "--cwd", cwd if cwd is not None else str(self.work),
            "--prompt-mode", mode,
            "--print-prompt",
        ]
        return subprocess.run(
            argv, cwd=str(self.work), env=self.env,
            capture_output=True, text=True, encoding="utf-8", timeout=180,
        )

    def extra_pack(self, name: str) -> Path:
        p = self.thin / "packs" / name
        p.write_text("# Demo pack\n\nDone when: nothing to do.\n", encoding="utf-8")
        return p

    def fake_calls(self) -> list[str]:
        return self.calls.read_text(encoding="utf-8").splitlines()

    def ask_calls(self) -> list[str]:
        return self.ask_marker.read_text(encoding="utf-8").splitlines()

    def snapshot(self) -> list:
        out = []
        for p in sorted(self.root.rglob("*")):
            rel = str(p.relative_to(self.root))
            out.append((rel, p.is_dir(), 0 if p.is_dir() else p.stat().st_size))
        return out


@pytest.fixture()
def sb(tmp_path: Path) -> Sandbox:
    return Sandbox(tmp_path)


@pytest.mark.parametrize("mode", MODES)
def test_print_prompt_is_literal_and_side_effect_free(sb: Sandbox, mode: str):
    before = sb.snapshot()
    proc = sb.run_print_prompt(mode)
    assert proc.returncode == 0, proc.stderr
    assert proc.stderr == ""
    prompt = proc.stdout
    assert prompt.strip()

    # No backtick (or redirect) was executed: no fake git/gh/dsh-trial-pr/push/
    # create-pr call, and ask was never invoked.
    assert sb.fake_calls() == []
    assert sb.ask_calls() == []

    # Nothing on disk changed -- in particular no stray `--cwd` file.
    assert sb.snapshot() == before
    assert not (sb.work / "--cwd").exists()
    assert list(sb.work.iterdir()) == []

    # Templates stay literal: no unrendered marker survives.
    assert "@@" not in prompt

    # Substitution still happened for the real variables.
    assert PACK_NAME in prompt
    assert sb.summary_out in prompt


@pytest.mark.parametrize("mode", MODES)
def test_print_prompt_keeps_backtick_snippets(sb: Sandbox, mode: str):
    prompt = sb.run_print_prompt(mode).stdout
    if mode in JSON_MODES:
        assert "```json" in prompt
    if mode in GIT_PUSH_MODES:
        # A backticked snippet must survive as text, never as command output.
        assert "`git push`" in prompt


@pytest.mark.parametrize("mode", ("foreman", "supervisor-close"))
def test_controlled_pr_line_is_literal(sb: Sandbox, mode: str):
    proc = sb.run_print_prompt(mode)
    assert proc.returncode == 0, proc.stderr
    assert PR_LINE in proc.stdout
    # ... and the redirect target of the old bug never appears as a file.
    assert not (sb.work / "--cwd").exists()
    assert sb.fake_calls() == []


def test_foreman_prompt_interpolates_cwd(sb: Sandbox):
    prompt = sb.run_print_prompt("foreman").stdout
    assert f"（cwd={sb.work}）" in prompt
    assert "@@CWD@@" not in prompt


@pytest.mark.parametrize("mode", MODES)
def test_special_chars_in_cwd_are_inserted_literally(sb: Sandbox, mode: str):
    value = "/tmp/a&b $(touch x)"
    before = sb.snapshot()
    proc = sb.run_print_prompt(mode, cwd=value)
    assert proc.returncode == 0, proc.stderr
    prompt = proc.stdout
    if mode == "foreman":
        # `&` must not be re-read as "the matched text" by bash pattern
        # substitution, and the value must never be command-substituted.
        assert f"（cwd={value}）" in prompt
        # Nothing was executed: no fake CLI call, no `touch x`.
        assert not (sb.work / "x").exists()
    assert "@@" not in prompt
    assert sb.fake_calls() == []
    assert sb.ask_calls() == []
    assert sb.snapshot() == before


def test_supervisor_plan_expands_dsh_home_paths(sb: Sandbox):
    """``$DSH_HOME`` in supervisor-plan must arrive expanded, never literal.

    Regression lock for impl-open-slice-heredoc-literal-r2.  With the old
    *unquoted* heredoc these two thin-state paths were expanded while the
    prompt was built, so the supervisor saw the broker's real ``DSH_HOME``.
    Once the heredoc became literal, a bare ``$DSH_HOME`` would instead be
    expanded by the supervisor ACP process itself -- whose ``DSH_HOME`` is the
    *role* home, i.e. the wrong directory for packs/ and goals/.
    """
    proc = sb.run_print_prompt("supervisor-plan")
    assert proc.returncode == 0, proc.stderr
    prompt = proc.stdout
    dsh_home = str(sb.home)
    assert f"{dsh_home}/supervisor/thin-state/packs/" in prompt
    assert f"{dsh_home}/supervisor/thin-state/goals/" in prompt
    # The variable itself must be gone: no shell-dependent text survives.
    assert "$DSH_HOME" not in prompt
    assert "${DSH_HOME}" not in prompt


@pytest.mark.parametrize("mode", MODES)
def test_no_unrendered_placeholder_in_any_mode(sb: Sandbox, mode: str):
    proc = sb.run_print_prompt(mode)
    assert proc.returncode == 0, proc.stderr
    assert proc.stdout.strip()
    assert "@@" not in proc.stdout


@pytest.mark.parametrize("mode", MODES)
def test_special_chars_in_pack_basename_are_inserted_literally(sb: Sandbox, mode: str):
    name = "a&b $(touch x).pack.md"
    pack = sb.extra_pack(name)
    before = sb.snapshot()
    proc = sb.run_print_prompt(mode, pack=pack)
    assert proc.returncode == 0, proc.stderr
    assert name in proc.stdout
    assert "@@" not in proc.stdout
    assert sb.fake_calls() == []
    assert sb.snapshot() == before
    assert not (sb.work / "x").exists()
