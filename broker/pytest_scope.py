#!/usr/bin/env python3
"""测试口径注入（纯标准库，可 import，也可 CLI）。

规则：工头/gate 只跑本片新写测试 + 直接受影响测试文件（L1），绿了就停；
全量回归只在合入前由监理按批次跑一次。只有 pack 显式 `full_suite: true`
才按目录分批全量。broker 每张票重新执行 open-slice.sh，因此放在这里即可热生效。

CLI: python3 pytest_scope.py --mode foreman|gate <pack_path> <cwd>
任何异常都打印空并 exit 0 —— 失败=不注入，保持原 prompt。
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

HEADING = "## 测试口径（L1 定点；全量仅合入前）"
HARD_RULE = (
    "只跑本片新写测试 + 直接受影响测试文件（L1），绿了就停，**不要**自行升级到全仓/全量"
    "（全量回归只在合入前由监理按批次跑一次）。"
)
GATE_NOTE = (
    "不重跑工头已跑绿的测试，只核记录；记录缺失时才补跑一次定点测试。"
    "只看增量；复审只核上轮未过项。"
    "猜测性风险无红测或可复现步骤只记 note，不判 HOLD。"
)
LOG_DISCIPLINE = (
    "测试只回传摘要 + 失败用例；完整日志落盘"
    "（如 `<artifact或cwd>/.dsh-test-logs/<ticket>-<n>.log` 或现有 artifact 目录），"
    "对话/摘要里只留路径。例如 `pytest -q ... > log 2>&1; tail -n 30 log`，或 `-rf` 只列失败。"
)

_BACKTICK_RE = re.compile(r"`([^`\n]+)`")
_DIFF_BLOCK_RE = re.compile(r"```diff\s*\n(.*?)```", re.DOTALL)
_DIFF_HEADER_RE = re.compile(r"^diff --git a/(\S+) b/(\S+)", re.MULTILINE)
_TRUTHY = {"true", "yes", "1", "on"}


def _split_frontmatter(text: str) -> tuple[dict[str, str], str]:
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}, text
    fm: dict[str, str] = {}
    last_key = ""
    for idx in range(1, len(lines)):
        line = lines[idx]
        if line.strip() == "---":
            return fm, "\n".join(lines[idx + 1:])
        stripped = line.strip()
        if last_key in ("tests", "test_paths") and stripped.startswith("- "):
            item = stripped[2:].strip().strip("'\"")
            if item:
                fm[last_key] = f"{fm.get(last_key, '')} {item}".strip()
            continue
        if ":" in line and not line.lstrip().startswith("#"):
            key, _, value = line.partition(":")
            last_key = key.strip().lower()
            fm[last_key] = value.strip().strip("'\"")
    return {}, text


def _parse_list(value: str) -> list[str]:
    raw = value.strip()
    if raw.startswith("[") and raw.endswith("]"):
        raw = raw[1:-1]
    items = re.split(r"[,\s]+", raw)
    out: list[str] = []
    for item in items:
        item = item.strip().strip("'\"")
        if not item:
            continue
        if "/" in item or item.endswith(".py") or "::" in item:
            if item not in out:
                out.append(item)
    return out


def _is_test_file(path: str) -> bool:
    name = Path(path).name
    return (name.startswith("test_") and name.endswith(".py")) or name.endswith("_test.py")


def _git_ls_files(cwd: str) -> list[str]:
    try:
        out = subprocess.run(
            ["git", "-C", cwd, "ls-files"],
            capture_output=True, text=True, timeout=20, check=False,
        )
    except Exception:
        return []
    if out.returncode != 0:
        return []
    return [ln.strip() for ln in out.stdout.splitlines() if ln.strip()]


_VALUE_FLAGS = {
    "-k", "-m", "-p", "-n", "-c", "-o", "-W",
    "--rootdir", "--deselect", "--ignore", "--maxfail",
    "--tb", "--durations",
}


def _looks_like_path(tok: str, cwd: str) -> bool:
    if "/" in tok or tok.endswith(".py") or "::" in tok:
        return True
    try:
        return bool(cwd) and (Path(cwd) / tok).is_dir()
    except OSError:
        return False


def _collect_commands(body: str, cwd: str = "") -> list[str]:
    cmds: list[str] = []
    for match in _BACKTICK_RE.finditer(body):
        cmd = match.group(1).strip()
        if not cmd:
            continue
        probe = re.sub(r"^cd\s+\S+\s*&&\s*", "", cmd)
        tokens = probe.split()
        if tokens[:3] == ["python3", "-m", "pytest"]:
            rest = tokens[3:]
        elif tokens[:1] == ["pytest"]:
            rest = tokens[1:]
        else:
            continue
        # 跳过 flag 及其分离式取值，剩下像路径的位置参数才算定点目标；
        # 裸 pytest（只有 flag、无 path/目录/:: 目标）是全量，不收。
        positional: list[str] = []
        skip_next = False
        for tok in rest:
            if skip_next:
                skip_next = False
                continue
            if tok == "--":
                continue
            if tok.startswith("-"):
                if tok in _VALUE_FLAGS:
                    skip_next = True
                continue
            positional.append(tok)
        if not any(_looks_like_path(tok, cwd) for tok in positional):
            continue
        if cmd not in cmds:
            cmds.append(cmd)
    return cmds


def _diff_changed_files(body: str) -> list[str]:
    files: list[str] = []
    for block in _DIFF_BLOCK_RE.findall(body):
        for segment in re.split(r"(?=^diff --git )", block, flags=re.MULTILINE):
            match = _DIFF_HEADER_RE.search(segment)
            if not match:
                continue
            if re.search(r"^\+\+\+ /dev/null\s*$", segment, re.MULTILINE):
                continue  # 删除的文件不算改动
            path = match.group(2)
            if path not in files:
                files.append(path)
    return files


def _exists(cwd: str, path: str) -> bool:
    if not cwd:
        return True
    try:
        return (Path(cwd) / path).is_file()
    except OSError:
        return False


def _map_tests(changed: list[str], tracked: list[str], cwd: str = "") -> list[str]:
    found: list[str] = []
    for path in changed:
        if _is_test_file(path):
            # diff 里直接出现的新增/修改测试文件（含尚未 tracked 的）照收；
            # 删除项已由 +++ /dev/null 过滤。
            if path not in found:
                found.append(path)
            continue
        if not path.endswith(".py"):
            continue
        stem = Path(path).stem
        for cand in sorted(tracked):
            name = Path(cand).name
            if name == f"test_{stem}.py" or name.startswith(f"test_{stem}_") \
                    or name == f"{stem}_test.py":
                if _exists(cwd, cand) and cand not in found:
                    found.append(cand)
    return found


def _cmd_line(cmds: list[str]) -> list[str]:
    return [f"- `{c}`" for c in cmds]


def _section(lines: list[str]) -> str:
    if LOG_DISCIPLINE not in lines:
        lines = [*lines, LOG_DISCIPLINE]
    return "\n".join(lines).strip() + "\n"


def _fallback_gate(pack_text: str, cwd: str) -> str:
    changed = _diff_changed_files(pack_text)
    tests = _map_tests(changed, _git_ls_files(cwd), cwd)
    if not tests:
        return _section([
            HEADING, "",
            "无对应测试，不跑测试。缺 TDD 证据按审核原则记 HOLD，不要用静态猜测挑刺。",
            "",
            HARD_RULE,
            GATE_NOTE,
        ])
    return _section([
        HEADING, "",
        "适用的定点命令：",
        *_cmd_line([f"python3 -m pytest -q {' '.join(tests)}"]),
        "",
        HARD_RULE,
        GATE_NOTE,
    ])


def _fallback_foreman() -> str:
    return _section([
        HEADING, "",
        "规则（本 pack 未列 tests，也没有定点命令）：",
        "- 按 TDD：先为 Done when 写/改测试并跑红，再改实现到绿；只跑本片新写测试 + "
        "直接受影响测试文件（`python3 -m pytest -q <files>`）。",
        "- 改动的源文件 `foo.py` 用 `git ls-files` 找对应的 "
        "`test_foo*.py` / `foo_test.py`（仓内任意目录）。",
        "- 找不到可挂靠的测试文件就新建；不要在摘要 notes 写 `no_targeted_tests` 跳过。"
        "不得为过审加没有测试撑着的防御性兜底分支。",
        "",
        HARD_RULE,
    ])


def build_section(mode: str, pack_path: str, cwd: str) -> str:
    text = Path(pack_path).read_text(encoding="utf-8", errors="replace")
    fm, body = _split_frontmatter(text)
    orig = fm.get("original_pack", "")
    if orig:
        orig_path = Path(pack_path).parent / orig
        if orig_path.is_file():
            orig_text = orig_path.read_text(encoding="utf-8", errors="replace")
            orig_fm, _ = _split_frontmatter(orig_text)
            for key in ("tests", "test_paths", "full_suite"):
                if not fm.get(key) and orig_fm.get(key):
                    fm[key] = orig_fm[key]

    tests = _parse_list(fm.get("tests", "")) + _parse_list(fm.get("test_paths", ""))
    tests = list(dict.fromkeys(tests))
    cmds = _collect_commands(body, cwd)
    for path in tests:
        cmd = f"python3 -m pytest -q {path}"
        if cmd not in cmds:
            cmds.append(cmd)

    if fm.get("full_suite", "").strip().lower() in _TRUTHY:
        by_dir: dict[str, list[str]] = {}
        for path in _git_ls_files(cwd):
            if _is_test_file(path):
                by_dir.setdefault(str(Path(path).parent), [])
                if path not in by_dir[str(Path(path).parent)]:
                    by_dir[str(Path(path).parent)].append(path)
        dirs = sorted(by_dir)
        if not dirs:
            return _section([HEADING, "", "无测试文件，不跑测试。", "", HARD_RULE])
        batches: list[str] = []
        for d in dirs:
            # 目录 d 的后代目录也在分组里时，`pytest d` 会递归收集到它们，
            # 因此 d 这一批只列它自身直接包含的测试文件，避免重复跑。
            has_child = any(
                o != d and (d == "." or o.startswith(d + "/")) for o in dirs
            )
            if has_child:
                files = sorted(by_dir[d])
                batches.append(f"python3 -m pytest -q {' '.join(files)}")
            else:
                batches.append(f"python3 -m pytest -q {d}")
        return _section([
            HEADING, "",
            "本 pack 显式要求全量：仅合入前由监理按下列目录分批跑（逐批记录结果，"
            "某批失败不阻断后续批次）：",
            *_cmd_line(batches),
            "",
            HARD_RULE,
        ])

    if cmds:
        lines = [HEADING, "", "适用的定点命令（逐条运行并记录结果）：", *_cmd_line(cmds), ""]
        lines.append(HARD_RULE)
        if mode == "gate":
            lines.append(GATE_NOTE)
        return _section(lines)

    return _fallback_gate(text, cwd) if mode == "gate" else _fallback_foreman()


def main(argv: list[str]) -> int:
    try:
        if len(argv) < 4 or argv[1] != "--mode":
            raise ValueError("usage: pytest_scope.py --mode foreman|gate <pack> <cwd>")
        mode = argv[2]
        if mode not in ("foreman", "gate"):
            raise ValueError(f"bad mode: {mode}")
        section = build_section(mode, argv[3], argv[4] if len(argv) > 4 else ".")
        sys.stdout.write(section)
    except Exception:
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
