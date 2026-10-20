#!/usr/bin/env python3
"""Gate 推理档位热分级（纯标准库，可 import，也可 CLI）。

broker 每张票都会重新执行 open-slice.sh，因此分级逻辑放在这里即可热生效，
无需重启 broker。约定：默认低档，只有生产写入、鉴权/租户、对外接口、
迁移/部署类改动（或大 diff）才用 high。

事实：dsh-acp-ask.py 的 normalize_effort 把 medium 映射为 high（适配器只认
off/low/high/max），所以实际只有 low / high 两档有意义；medium risk 取 low。

CLI: python3 gate_effort.py <gate_pack> <cwd>  # 打印 "<effort>\\t<reason>"
任何异常都打印空行并 exit 0 —— 失败=不覆盖，保持原有 env。
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

HIGH_RISK_RE = re.compile(
    r"migrat|(^|/)auth|tenant|(^|/)api/|openapi|webhook|deploy|(^|/)prod",
    re.IGNORECASE,
)
DIFF_HEADER_RE = re.compile(
    r"^##\s*Diff\b.*?base=([^\s;)]+).*?head=([^\s;)]+)",
    re.MULTILINE,
)
FRONTMATTER_RE = re.compile(r"\A---\s*\n(.*?)\n---\s*(\n|$)", re.DOTALL)


def _parse_frontmatter(text: str) -> dict[str, str]:
    m = FRONTMATTER_RE.match(text)
    if not m:
        return {}
    out: dict[str, str] = {}
    for line in m.group(1).splitlines():
        if ":" in line and not line[:1].isspace():
            key, _, val = line.partition(":")
            out[key.strip()] = val.strip()
    return out


def decide(
    risk: str | None,
    paths: list[str],
    changed_lines: int,
    *,
    big_lines: int = 400,
) -> tuple[str, str]:
    """按 risk / 路径 / 改动行数决定档位。"""
    r = (risk or "").strip().lower()
    if r == "high":
        return "high", "risk=high"
    if r == "low":
        return "low", "risk=low"
    if r == "medium":
        return "low", "risk=medium->low (adapter has no medium)"
    for p in paths:
        if HIGH_RISK_RE.search(p or ""):
            return "high", f"high-risk path={p}"
    if changed_lines > big_lines:
        return "high", f"changed_lines={changed_lines}>{big_lines}"
    return "low", f"small diff ({changed_lines} lines)"


def _diff_block_stats(text: str) -> tuple[list[str], int]:
    """回退：从 ```diff 块里统计 diff --git 路径与 +/- 行数。"""
    paths: list[str] = []
    changed = 0
    in_block = False
    for line in text.splitlines():
        if line.strip().startswith("```"):
            if in_block:
                in_block = False
                continue
            if line.strip().lower().startswith("```diff"):
                in_block = True
                continue
        if not in_block:
            continue
        if line.startswith("diff --git "):
            m = re.match(r"diff --git a/(\S+) b/(\S+)", line)
            if m:
                paths.append(m.group(2))
        elif line.startswith("+++") or line.startswith("---"):
            continue
        elif line.startswith("+"):
            changed += 1
        elif line.startswith("-"):
            changed += 1
    return paths, changed


def _git_diff_stats(cwd: str, base: str, head: str) -> tuple[list[str], int]:
    proc = subprocess.run(
        ["git", "-C", cwd, "diff", "--numstat", f"{base}...{head}"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"git diff failed: {proc.returncode}")
    paths: list[str] = []
    changed = 0
    for line in proc.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        added, deleted, path = parts[0], parts[1], parts[2]
        paths.append(path)
        for val in (added, deleted):
            try:
                changed += int(val)
            except ValueError:
                pass  # 二进制文件是 "-"
    return paths, changed


def from_gate_pack(pack_path: str | Path, cwd: str | Path) -> tuple[str, str]:
    """读 gate pack → 原 pack risk / diff → 档位。"""
    pack = Path(pack_path)
    text = pack.read_text(encoding="utf-8", errors="replace")
    meta = _parse_frontmatter(text)

    explicit = (meta.get("gate_effort") or "").strip().lower()
    if explicit:
        return explicit, "pack gate_effort"

    risk: str | None = None
    orig_name = (meta.get("original_pack") or "").strip()
    if orig_name:
        orig = pack.parent / Path(orig_name).name
        try:
            risk = _parse_frontmatter(
                orig.read_text(encoding="utf-8", errors="replace")
            ).get("risk")
        except OSError:
            risk = None

    paths: list[str] = []
    changed = 0
    m = DIFF_HEADER_RE.search(text)
    if m:
        try:
            paths, changed = _git_diff_stats(str(cwd), m.group(1), m.group(2))
        except Exception:
            paths, changed = _diff_block_stats(text)
    else:
        paths, changed = _diff_block_stats(text)

    return decide(risk, paths, changed)


def main(argv: list[str]) -> int:
    try:
        if len(argv) < 3:
            raise ValueError("usage: gate_effort.py <gate_pack> <cwd>")
        effort, reason = from_gate_pack(argv[1], argv[2])
        print(f"{effort}\t{reason}")
    except Exception:
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
