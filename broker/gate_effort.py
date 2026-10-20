#!/usr/bin/env python3
"""Gate 推理档位热分级（纯标准库，可 import，也可 CLI）。

broker 每张票都会重新执行 open-slice.sh，因此分级逻辑放在这里即可热生效，
无需重启 broker。约定：默认低档，只有生产写入、鉴权/租户、对外接口、
迁移/部署类改动才用 high；改动行数本身不再抬档。

事实：dsh-acp-ask.py 的 normalize_effort 把 medium 映射为 high（适配器只认
off/low/high/max），所以实际只有 low / high 两档有意义；medium risk 取 low。

收尾片（只开 PR、无新业务 commit）可在 classify() 里判为 skip，由 open-slice.sh
直接写 PASS 摘要、不调用 gate。

CLI: python3 gate_effort.py <gate_pack> <cwd>  # 打印 "<effort>\\t<reason>"
     python3 gate_effort.py --classify <gate_pack> <cwd>
     # 打印 "<action>\\t<effort|empty>\\t<reason>"
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
    big_lines: int = 400,  # 保留兼容参数；行数不再单独抬档
) -> tuple[str, str]:
    """按显式 risk / risk 标签 / 改动路径决定档位。

    显式 low/medium/high 优先；标签型 risk（如 tenant,takeover）与路径共用
    同一套高风险正则。大 diff 不再抬 high。
    """
    r = (risk or "").strip().lower()
    if r == "high":
        return "high", "risk=high"
    if r == "low":
        return "low", "risk=low"
    if r == "medium":
        return "low", "risk=medium->low (adapter has no medium)"
    for label in (x.strip() for x in r.split(",")):
        if label and HIGH_RISK_RE.search(label):
            return "high", f"high-risk risk={label}"
    for p in paths:
        if HIGH_RISK_RE.search(p or ""):
            return "high", f"high-risk path={p}"
    return "low", f"no high-risk signal ({changed_lines} lines; size not tiered)"


_DIFF_META_PREFIXES = (
    "index ",
    "new file mode ",
    "deleted file mode ",
    "old mode ",
    "new mode ",
    "similarity index ",
    "rename from ",
    "rename to ",
    "Binary files ",
)


def _diff_block_stats(text: str) -> tuple[list[str], int]:
    """回退：从 ```diff 块里统计 diff --git 路径与 +/- 行数。"""
    paths: list[str] = []
    changed = 0
    in_block = False
    in_header = False  # 刚过 diff --git，仍可能处于文件头
    seen_minus = False
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
            in_header = True
            seen_minus = False
            continue
        if in_header:
            if not seen_minus and (
                line.startswith("--- a/") or line.startswith("--- /dev/null")
            ):
                seen_minus = True
                continue
            if seen_minus and (
                line.startswith("+++ b/") or line.startswith("+++ /dev/null")
            ):
                in_header = False
                seen_minus = False
                continue
            if not seen_minus and line.startswith(_DIFF_META_PREFIXES):
                continue
            # 不是文件头：结束头部状态，按普通 diff 行处理。
            in_header = False
            seen_minus = False
        if line.startswith("+"):
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
    if not proc.stdout.strip():
        # base==head / 改动未提交等：空输出不能当 0 行，回退去数 diff 块。
        raise RuntimeError("git diff produced empty numstat")
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


def _orig_pack_text(pack: Path, meta: dict[str, str]) -> str | None:
    orig_name = (meta.get("original_pack") or "").strip()
    if not orig_name:
        return None
    try:
        return (pack.parent / Path(orig_name).name).read_text(
            encoding="utf-8", errors="replace"
        )
    except OSError:
        return None


def _pack_effort(
    pack: Path, text: str, meta: dict[str, str], cwd: str | Path
) -> tuple[str, str]:
    explicit = (meta.get("gate_effort") or "").strip().lower()
    normalized = {
        "low": "low",
        "off": "off",
        "medium": "low",
        "high": "high",
        "max": "high",
    }.get(explicit)
    if normalized:
        return normalized, "pack gate_effort"

    risk: str | None = None
    orig_text = _orig_pack_text(pack, meta)
    if orig_text is not None:
        risk = _parse_frontmatter(orig_text).get("risk")

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


def _frontmatter_skip(meta: dict[str, str]) -> bool:
    return (
        (meta.get("skip_gate") or "").strip().lower() in {"1", "true", "yes", "on"}
        or (meta.get("gate") or "").strip().lower() == "skip"
        or (meta.get("review") or "").strip().lower() == "skip"
    )


def _closing_signal(text: str | None) -> str | None:
    """收尾片强信号：明确不要走 gate，只要开 PR。"""
    if not text:
        return None
    no_submit = re.search(r"不要.*submit_for_review", text)
    pr_only = re.search(r"开\s*PR|dsh-trial-pr|push-and-pr", text, re.IGNORECASE)
    if no_submit and pr_only:
        return "closing_slice_pr_only"
    if re.search(r"supervisor-close", text, re.IGNORECASE) and re.search(
        r"核对|不是\s*gate", text
    ):
        return "supervisor_close_check"
    return None


def classify(pack_path: str | Path, cwd: str | Path) -> dict:
    """返回 {action: skip|review, effort: low|high|None, reason: str}。"""
    pack = Path(pack_path)
    text = pack.read_text(encoding="utf-8", errors="replace")
    meta = _parse_frontmatter(text)
    orig_text = _orig_pack_text(pack, meta)

    if _frontmatter_skip(meta) or _frontmatter_skip(_parse_frontmatter(orig_text or "")):
        return {"action": "skip", "effort": None, "reason": "frontmatter_skip"}
    reason = _closing_signal(orig_text if orig_text is not None else text)
    if reason:
        return {"action": "skip", "effort": None, "reason": reason}
    effort, reason = _pack_effort(pack, text, meta, cwd)
    return {"action": "review", "effort": effort, "reason": reason}


def from_gate_pack(pack_path: str | Path, cwd: str | Path) -> tuple[str, str]:
    """读 gate pack → 原 pack risk / diff → 档位。"""
    pack = Path(pack_path)
    text = pack.read_text(encoding="utf-8", errors="replace")
    return _pack_effort(pack, text, _parse_frontmatter(text), cwd)


def main(argv: list[str]) -> int:
    try:
        if len(argv) >= 2 and argv[1] == "--classify":
            if len(argv) < 4:
                raise ValueError("usage: gate_effort.py --classify <gate_pack> <cwd>")
            c = classify(argv[2], argv[3])
            print(f"{c['action']}\t{c['effort'] or ''}\t{c['reason']}")
        else:
            if len(argv) < 3:
                raise ValueError("usage: gate_effort.py <gate_pack> <cwd>")
            effort, reason = from_gate_pack(argv[1], argv[2])
            print(f"{effort}\t{reason}")
    except Exception:
        print()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
