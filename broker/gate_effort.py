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

import os
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
    """按优先级取第一个能解析的基线；全失败返回 None。

    候选解析结果等于当前 HEAD 时**保留**：真收尾片 gate PASS 后无新 commit，
    此时基线..HEAD 为空 diff，正是 skip 条件。只禁止字面 HEAD 候选与 range 右侧。
    """
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
    seen: set[str] = set()
    for cand in candidates:
        if not cand or cand in seen:
            continue
        seen.add(cand)
        ok, out = _git(cwd, "rev-parse", "--verify", f"{cand}^{{commit}}")
        if not ok or not out.strip():
            continue
        return out.strip()
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


# ---------------------------------------------------------------------------
# Gate grain (per_goal vs per_slice) — used by trial-broker chain/goal loop.
# Default per_goal (Runda 2026-10-20). Scheme C (script PASS) is out of scope.
# ---------------------------------------------------------------------------
GATE_GRAIN_PER_GOAL = "per_goal"
GATE_GRAIN_PER_SLICE = "per_slice"
GATE_POLICY_DEFER = "defer"
GATE_POLICY_SKIP = "skip"
GATE_POLICY_PER_GOAL = "per_goal"
GATE_POLICY_PER_SLICE = "per_slice"
_GRAIN_ALIASES = {
    "per_goal": GATE_GRAIN_PER_GOAL,
    "goal": GATE_GRAIN_PER_GOAL,
    "merge": GATE_GRAIN_PER_GOAL,
    "per_slice": GATE_GRAIN_PER_SLICE,
    "slice": GATE_GRAIN_PER_SLICE,
}


def _norm_grain(raw: str | None) -> str | None:
    if raw is None:
        return None
    key = str(raw).strip().lower()
    if not key:
        return None
    return _GRAIN_ALIASES.get(key)


def resolve_goal_gate_grain(
    *,
    goal: dict | None = None,
    job: dict | None = None,
    limits: dict | None = None,
    env: dict | None = None,
) -> str:
    """Goal-level default grain.

    Precedence (first hit wins): env DSH_GATE_GRAIN → job.gate/gate_grain →
    goal.gate/gate_grain → limits.gate_grain/gate → **per_goal**.
    """
    env = env if env is not None else os.environ
    for raw in (
        (env or {}).get("DSH_GATE_GRAIN"),
        (job or {}).get("gate_grain"),
        (job or {}).get("gate"),
        (goal or {}).get("gate_grain"),
        (goal or {}).get("gate"),
        (limits or {}).get("gate_grain"),
        (limits or {}).get("gate"),
    ):
        g = _norm_grain(raw if isinstance(raw, str) else None)
        if g:
            return g
    return GATE_GRAIN_PER_GOAL


def pack_gate_override(meta: dict[str, str] | None, text: str | None = None) -> str | None:
    """Pack-level gate override: per_slice / per_goal / skip / None."""
    meta = meta or {}
    if _frontmatter_skip(meta):
        return GATE_POLICY_SKIP
    g = _norm_grain(meta.get("gate"))
    if g:
        return g
    if text and _closing_signal(text):
        return GATE_POLICY_SKIP
    return None


def is_high_risk_slice(
    *,
    risk: str | None = None,
    paths: list[str] | None = None,
    meta: dict[str, str] | None = None,
) -> tuple[bool, str]:
    """True when pack risk / tags / paths hit HIGH_RISK_RE (same as effort high)."""
    meta = meta or {}
    r = (risk if risk is not None else meta.get("risk") or "").strip()
    effort, reason = decide(r or None, list(paths or []), 0)
    if effort == "high":
        return True, reason
    return False, reason


def changed_paths_look_src(paths: list[str] | None) -> bool:
    """True if any path would fail the closing diff gate (src/settings/…)."""
    for p in paths or []:
        if not p:
            continue
        if BLOCK_PATH_RE.search(p) and not ALLOW_PATH_RE.search(p):
            return True
    return False


def resolve_slice_gate_policy(
    goal_grain: str,
    *,
    pack_meta: dict[str, str] | None = None,
    pack_text: str | None = None,
    risk: str | None = None,
    paths: list[str] | None = None,
    is_goal_bearer: bool = False,
    from_goal: bool = True,
) -> tuple[str, str]:
    """Per-slice policy for the chain gate step.

    Returns ``(policy, reason)`` where policy is one of:
      per_slice — open LLM gate for this slice alone
      per_goal  — this slice is the Goal gate bearer (aggregated pack)
      defer     — impl done → synthetic PASS; wait for Goal gate
      skip      — closing / skip_gate → synthetic PASS; no Goal gate slot

    Standalone chains (not from_goal) always per_slice.
    """
    if not from_goal:
        return GATE_POLICY_PER_SLICE, "standalone_chain"

    meta = pack_meta or {}
    override = pack_gate_override(meta, pack_text)
    if override == GATE_POLICY_SKIP:
        # Fake closing with src changes must still be reviewed.
        if changed_paths_look_src(paths):
            if goal_grain == GATE_GRAIN_PER_GOAL and not is_goal_bearer:
                return GATE_POLICY_DEFER, "skip_blocked_src→defer"
            return GATE_POLICY_PER_SLICE, "skip_blocked_src→per_slice"
        return GATE_POLICY_SKIP, "pack_skip"
    if override == GATE_GRAIN_PER_SLICE:
        return GATE_POLICY_PER_SLICE, "pack_gate=per_slice"

    high, why = is_high_risk_slice(risk=risk, paths=paths, meta=meta)
    if high:
        return GATE_POLICY_PER_SLICE, f"high_risk:{why}"

    grain = goal_grain if goal_grain in (GATE_GRAIN_PER_GOAL, GATE_GRAIN_PER_SLICE) else GATE_GRAIN_PER_GOAL
    if grain == GATE_GRAIN_PER_SLICE:
        return GATE_POLICY_PER_SLICE, "goal_grain=per_slice"
    if is_goal_bearer:
        return GATE_POLICY_PER_GOAL, "goal_bearer"
    return GATE_POLICY_DEFER, "per_goal_defer"


