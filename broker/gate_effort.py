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

# 收尾免审 diff 门禁：业务实质路径 / 允许的文档与 PR 元数据路径。
BLOCK_PATH_RE = re.compile(
    r"(^|/)src/|settings|defaults|openapi|swagger|runbook", re.IGNORECASE
)
ALLOW_PATH_RE = re.compile(
    r"^(docs/|\.github/)|evidence|\.md$|CHANGELOG|(^|/)PR(?:[/_.-]|$)",
    re.IGNORECASE,
)
BASELINE_KEYS = ("prior_head", "last_gate_head", "gate_pass_sha", "base_for_review")


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


def _git(cwd: str | Path, *args: str) -> tuple[bool, str]:
    try:
        proc = subprocess.run(
            ["git", "-C", str(cwd), *args],
            capture_output=True,
            text=True,
            timeout=30,
        )
    except Exception:
        return False, ""
    return proc.returncode == 0, proc.stdout


def _meta_baseline(
    meta: dict[str, str], keys: tuple[str, ...] = BASELINE_KEYS
) -> str | None:
    """取元数据里的基线候选；range 一律取**左侧**（起点），绝不取右侧 HEAD。"""
    for key in keys:
        val = (meta.get(key) or "").strip().strip("`'\"")
        if key == "base_for_review" and ".." in val:
            # `old..HEAD` / `old...HEAD` 的右侧只是声称的终点，不是基线。
            val = val.split("..", 1)[0].strip()
        if not val or val.lower() == "head":
            # 字面 HEAD 候选无效，留给下一个候选。
            continue
        return val
    return None


def _resolve_baseline(
    meta: dict[str, str], orig_text: str | None, text: str, cwd: str | Path
) -> str | None:
    """按优先级取第一个能解析且不等于当前 HEAD 的基线；全失败返回 None。"""
    sources = (meta, _parse_frontmatter(orig_text or ""), _parse_frontmatter(text))
    candidates: list[str] = []
    # 1) 上一轮 gate 记录的真实 HEAD。
    for fm in sources:
        val = _meta_baseline(fm, ("prior_head", "last_gate_head", "gate_pass_sha"))
        if val:
            candidates.append(val)
    # 2) 正文 last_commit=。
    for body in (orig_text or "", text):
        m = re.search(r"last_commit=([0-9a-f]{7,40})", body, re.IGNORECASE)
        if m:
            candidates.append(m.group(1))
            break
    # 3) base_for_review（range 已取左侧；字面 HEAD 已在上游被丢弃）。
    for fm in sources:
        val = _meta_baseline(fm, ("base_for_review",))
        if val:
            candidates.append(val)
    # 4) Diff 行只提供 base 作为对比起点（head 仅是本片声称的 HEAD）。
    m = DIFF_HEADER_RE.search(text)
    if m:
        candidates.append(m.group(1))
    ok, head_out = _git(cwd, "rev-parse", "HEAD")
    head = head_out.strip() if ok else ""
    seen: set[str] = set()
    for cand in candidates:
        if not cand or cand in seen:
            continue
        seen.add(cand)
        ok, out = _git(cwd, "rev-parse", "--verify", f"{cand}^{{commit}}")
        if not ok or not out.strip():
            continue
        resolved = out.strip()
        if head and resolved == head:
            # 解析结果等于当前 HEAD：自基线 diff 恒空，会制造假收尾，弃用。
            continue
        return resolved
    return None


def _closing_diff_gate(
    meta: dict[str, str], orig_text: str | None, text: str, cwd: str | Path
) -> tuple[bool, str]:
    """收尾候选的第二道门：相对基线不得含 src/契约/defaults 等实质变更。"""
    baseline = _resolve_baseline(meta, orig_text, text, cwd)
    if not baseline:
        return False, "gate_skip_blocked: no_baseline"
    ok, head_out = _git(cwd, "rev-parse", "HEAD")
    if not ok or not head_out.strip():
        return False, "gate_skip_blocked: no_baseline"
    head = head_out.strip()
    ok, out = _git(cwd, "diff", "--name-status", f"{baseline}...{head}")
    if not ok:
        return False, "gate_skip_blocked: no_baseline"
    for line in out.splitlines():
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        status, path = parts[0][:1].upper(), parts[-1].strip()
        if not path or status == "D":  # 删除不算新增业务实质
            continue
        if BLOCK_PATH_RE.search(path):
            return False, f"gate_skip_blocked: src_change={path} (baseline={baseline})"
        if not ALLOW_PATH_RE.search(path):
            return False, f"gate_skip_blocked: non_doc_change={path} (baseline={baseline})"
    return True, f"gate_skip: closing_ok (baseline={baseline} no_src)"


def classify(pack_path: str | Path, cwd: str | Path) -> dict:
    """返回 {action: skip|review, effort: low|high|None, reason: str}。"""
    pack = Path(pack_path)
    text = pack.read_text(encoding="utf-8", errors="replace")
    meta = _parse_frontmatter(text)
    orig_text = _orig_pack_text(pack, meta)

    trigger: str | None = None
    if _frontmatter_skip(meta) or _frontmatter_skip(_parse_frontmatter(orig_text or "")):
        trigger = "frontmatter_skip"
    else:
        trigger = _closing_signal(orig_text if orig_text is not None else text)
    if trigger:
        ok, gate_reason = _closing_diff_gate(meta, orig_text, text, cwd)
        if ok:
            return {"action": "skip", "effort": None, "reason": f"{gate_reason}; {trigger}"}
        effort, _ = _pack_effort(pack, text, meta, cwd)
        return {
            "action": "review",
            "effort": effort,
            "reason": f"{gate_reason}; {trigger}",
        }
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
