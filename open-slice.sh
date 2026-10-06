#!/usr/bin/env bash
# open-slice.sh — Route C mid-ticket open-with-pack (ACP one-shot).
# Executors must NOT join rooms. Pack is SSOT via design_pack_read.
set -euo pipefail

DSH_HOME="${DSH_HOME:-$HOME/.dsh}"
DSH_HOMES_ROOT="${DSH_HOMES_ROOT:-$HOME/.dsh-homes}"
THIN_STATE="${DSH_TRIAL_THIN_STATE:-$DSH_HOME/supervisor/thin-state}"

usage() {
  cat <<'USAGE'
Usage:
  open-slice.sh --ticket NAME --pack REL_OR_ABS [--profile acp|acp-lite]
                [--cwd ABS] [--role gate|impl|supervisor] [--final] [--keep-open]
                [--log PATH] [--summary-name NAME]
                [--prompt-mode baseline|foreman|gate|supervisor-plan|supervisor-answer|supervisor-close] [--prompt-file PATH]

Defaults:
  --profile acp
  --cwd /workspace
  --role inferred from ticket prefix (impl-* → impl, gate-* → gate, supervisor-* → supervisor)
  --final (session/close after prompts)
  --prompt-mode baseline
  --log /workspace/tmp/<ticket>.log

Pack:
  Relative paths resolve under $DSH_HOME/supervisor/thin-state/packs/
  (ask uses design_pack_read; do not bash-cat the pack into the prompt).

prompt-mode:
  baseline  — original smoke/echo slice prompt
  foreman   — impl ticket: design_pack_read + do work + structured summary block
  gate      — gate ticket: design_pack_read + tiered delta review + verdict file
  supervisor-plan   — dsh 监理：读 Goal brief pack，写 slice pack + emit_chain 机器块
  supervisor-answer — dsh 监理：读问题 pack，写 chain_reply 机器块
  supervisor-close  — dsh 监理：读收尾 pack，写 goal_done 机器块
  ( --prompt-file overrides the built-in template entirely )
USAGE
}

TICKET=""
PACK=""
PROFILE="acp"
CWD="/workspace"
ROLE=""
FINAL=1
KEEP_OPEN=0
LOG=""
SUMMARY_NAME=""
PROMPT_MODE="baseline"
PROMPT_FILE=""
ASK_BIN="${DSH_ACP_ASK:-$DSH_HOME/bin/dsh-acp-ask.py}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ticket) TICKET="${2:-}"; shift 2 ;;
    --pack) PACK="${2:-}"; shift 2 ;;
    --profile) PROFILE="${2:-}"; shift 2 ;;
    --cwd) CWD="${2:-}"; shift 2 ;;
    --role) ROLE="${2:-}"; shift 2 ;;
    --final) FINAL=1; KEEP_OPEN=0; shift ;;
    --keep-open) KEEP_OPEN=1; FINAL=0; shift ;;
    --log) LOG="${2:-}"; shift 2 ;;
    --summary-name) SUMMARY_NAME="${2:-}"; shift 2 ;;
    --prompt-mode) PROMPT_MODE="${2:-}"; shift 2 ;;
    --prompt-file) PROMPT_FILE="${2:-}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

[[ -n "$TICKET" && -n "$PACK" ]] || { usage; exit 2; }
[[ -x "$ASK_BIN" || -f "$ASK_BIN" ]] || { echo "missing ask: $ASK_BIN" >&2; exit 2; }

PACK_BASENAME="$(basename "$PACK")"
if [[ "$PACK" = /* ]]; then
  PACK_ABS="$PACK"
else
  PACK_ABS="$THIN_STATE/packs/$PACK_BASENAME"
fi
[[ -f "$PACK_ABS" ]] || { echo "pack not found: $PACK_ABS" >&2; exit 2; }

LOG="${LOG:-/workspace/tmp/${TICKET}.log}"
SUMMARY_NAME="${SUMMARY_NAME:-${PACK_BASENAME%.pack.md}}"
SUMMARY_OUT="$THIN_STATE/summaries/${SUMMARY_NAME}.md"
mkdir -p "$(dirname "$LOG")" "$THIN_STATE/summaries"

ensure_role_pack_bridge() {
  local role="$1"
  local home="$DSH_HOMES_ROOT/$role"
  [[ -d "$home" ]] || return 0
  mkdir -p "$home/supervisor/thin-state"
  if [[ ! -e "$home/supervisor/thin-state/packs" ]]; then
    ln -s "$THIN_STATE/packs" "$home/supervisor/thin-state/packs"
  fi
  if [[ ! -e "$home/supervisor/thin-state/summaries" ]]; then
    ln -s "$THIN_STATE/summaries" "$home/supervisor/thin-state/summaries"
  fi
  if [[ ! -e "$home/supervisor/thin-state/index.json" ]]; then
    ln -s "$THIN_STATE/index.json" "$home/supervisor/thin-state/index.json"
  fi
}

INF_ROLE="$ROLE"
if [[ -z "$INF_ROLE" ]]; then
  case "$TICKET" in
    gate-*|gate_*|review-*|review_*|pr-gate-*) INF_ROLE=gate ;;
    supervisor-*|supervisor_*|sup-*|sup_*) INF_ROLE=supervisor ;;
    impl-*|impl_*|fix-*|fix_*|dev-*|dev_*) INF_ROLE=impl ;;
  esac
fi
[[ -n "$INF_ROLE" ]] && ensure_role_pack_bridge "$INF_ROLE"

build_prompt_baseline() {
  cat <<PROMPT
你是 Route C 中票执行器（基线片）。执行票 **禁止 join 任何 team-room**，禁止一切 room_*（含 room_post / room_read / room_join / room_list）。

本 slice 的唯一权威任务包在 design-pack 工具可读路径下；pack 正文只允许经 design_pack_read 进入上下文。

强制步骤（最多约 8 步，完成后停止）：
1. 调用工具 design_pack_read，path 使用相对名：「${PACK_BASENAME}」。
2. 用一句话确认：slice_id + pack bytes（工具返回的字节数）。
3. 严格按 pack 的 Done when 执行（需要读文件时只用 read 工具，禁止 bash）。
4. 把关票摘要写入：「${SUMMARY_OUT}」
   至少含：status / used_tool=design_pack_read / room_joined=false / blockers。
5. 写完摘要后停止；不要继续探索。

硬禁（基线片）：
- 禁止对 pack 路径做任何 bash（含 cat/sed/head/find/wc/ls/stat/grep/rg）；禁止 bash 探查 packs/ 或 thin-state/packs。
- 禁止 serena / codegraph / mcp-* / 其它 MCP，除非本 pack 明确要求（本基线片不需要）。
- 禁止不必要的 glob / sessions_list / 其它探索工具；只做 Done when 要求的最少工具。

Done when: design_pack_read 成功 + pack 要求的产物落盘 + summary 落盘 + 未进房。
PROMPT
}

build_prompt_foreman() {
  cat <<PROMPT
你是 Route C 工头中票（foreman / impl）。**禁止 join 任何 team-room**，禁止一切 room_*（含 room_post / room_read / room_join / room_list）。

唯一权威任务：经 design_pack_read 读取 pack「${PACK_BASENAME}」，按 pack 执行改动（cwd=${CWD}）。

## 票中沟通（role-bridge 桥；本票只允许以下两个工具）
- 「ask_supervisor」：**仅当**实现歧义真实影响 Done-when（缺参/冲突/不可验证）时找监理澄清；文案风格等可合理默认则不要问。必传 \`questions\`（非空字符串数组，可附 \`context\`）。**允许**。
- 「submit_for_review」：改动做完后提交 gate 审查。必传 \`usage_prompt\` 与 \`summary\`，可附 \`changed_files\` / \`base\` / \`commit\`。**允许**。
- 除上述两个薄封装外：**禁止** send_to_role 直连、room_*、join。
- **默认仍禁**裸 `git push` / `gh`（含 `gh pr create|merge`）。
- **仅当** pack / Goal Done-when 要求对 main 开 PR，且 gate 已 PASS（或 pack 明确允许开 PR）时：**必须**用受控包装：
  `dsh-trial-pr push-and-pr --repo <fengrunda/knowledge-hub|fengrunda/memory-as-training> --cwd <repo> --branch <feature> --title "…" --body "…"`
  （或分步 `push` / `create-pr`）。仍禁 force-push、push 到 main/master、`gh pr merge`、其他 remote/仓。

## usage_prompt（submit_for_review 必填）
- 用**最近一次已知**的 prompt token 用量（会话 usage / composition 里的 usage_prompt）。
- 若确实未知：按本票上下文规模**估一个合理值**（如 8000–20000），并在摘要 notes 明确写「usage_prompt 为估算」。禁止传 0/负数。

## 工具步数纪律（软上限；保质量优先于穷尽探索）
- **建议上限约 25 步**工具轮（design_pack_read / read / edit / bash / ask / submit 均计）。limits 字段 `impl_max_steps` 默认 30（软参考，broker **不**裸杀）。
- 接近上限（约 20+）：停止无效探索；收工写 summary（done/blocked/question）或 ask_supervisor 把剩余拆下一片；禁止重复 glob/无目标 bash/反复读同一大文件。
- 大工具输出已由 eager-offload 裁剪：优先 offset/limit 读所需片段，不要为「看全量」空转。

## 强制步骤：
1. design_pack_read path=「${PACK_BASENAME}」。
2. 按 pack 的 Done when / Steps 做最少必要改动（可用 edit/write/bash 做 git 与文件；禁止 room_*）。
3. **仅当**歧义真实影响 Done-when：调 \`ask_supervisor\`；**超时/失败**（timed_out / degrade）时不要干等，写 summary status=question，并把同样的问题放进 questions。
4. 改完调 \`submit_for_review\`（务必带 usage_prompt）：
   - 返回 PASS → 写 summary status=done 并停止。
   - 返回 HOLD + rework_mode=inplace → **本票内**按 findings 逐条修完，再 \`submit_for_review\` 一次（不要提前关票）。
   - 返回 HOLD + rework_mode=fresh → 本票写 status=done，notes 必须含 \`rework_fresh\`；**broker 会立刻另开一张 findings-first 修复票并重跑 gate**，这不是 slice 完成。
   - 工具不可用/超时（degrade=close_ticket）→ 视为普通完成，写 status=done，交给 broker 兜底 gate。
5. 写关票摘要到：「${SUMMARY_OUT}」
   摘要必须短，并在文末含 **一个** 机器可读 fenced json 块，键固定：
\`\`\`json
{
  "status": "done|blocked|question",
  "changed_files": ["相对路径…"],
  "branch": "当前分支或空",
  "commit": "HEAD sha 短或空",
  "base": "diff 基 sha 或空（改动前）",
  "questions": ["若 status=question 时的问题"],
  "notes": "一两句；fresh 交接必须含 rework_fresh",
  "finding_resolutions": [
    {"finding": "…或 tier+issue 摘要", "change": "file:line 或路径+符号", "status": "fixed|deferred|wontfix", "reason": "未改时必填"}
  ]
}
\`\`\`
   **有 gate findings 的 pack（fix/rework/reply）**：必须逐条写 \`finding_resolutions\`；
   未修 P0/P1 时只能写 status=blocked/question，或继续 inplace 再 \`submit_for_review\`，**禁止** status=done 假装收口。
6. status=done 表示你认为可交 gate；blocked/question 则不要假装完成。
   有未解决 P0/P1 却写 done：broker 不会当干净完成，会另开修复票或降级，白费一轮。
7. 写完摘要后停止。

硬禁：room_* / join / send_to_role 直连；裸 `git push` / 裸 `gh`；force-push；push main/master；`gh pr merge`；其他仓；不要把整仓灌进摘要；不要打印 API key / GH_TOKEN。
若 Done-when 要求 PR：gate PASS（或 pack 允许）后经 `dsh-trial-pr` 开 PR，stdout 的 PR URL 写入 summary notes。
Done when: design_pack_read +（按 pack 完成或明确 blocked/question）+（PASS 后的 done / HOLD-inplace 修完并复提 / HOLD-fresh 的 done+rework_fresh+finding_resolutions / ask 超时的 question）+（若要求 PR 则已用 dsh-trial-pr 开出或确认已有 open PR）+ summary 含机器块 + 有 findings 时逐条 finding_resolutions + room_joined=false。
PROMPT
}

build_prompt_gate() {
  cat <<PROMPT
你是 Route C 设计审查中票（gate / design-gate 风格）。**禁止 join 任何 team-room**，禁止一切 room_*。
本票只读审查：对照 pack 内 acceptance + 工头摘要 + diff，做 **delta-only / 分层** 审查。不要改业务源码。

## 裁决硬规则（必须遵守；broker 会二次强制）
- **任一 acceptance 条目未满足** → verdict 必须 **HOLD**（写入 unmet_acceptance）。
- **任一 P0 或 P1 finding** → verdict 必须 **HOLD**。
- **仅有 P2 nits、且全部 acceptance 满足** → **PASS**（P2 仍写入 findings）。
- 禁止「口头 PASS 但 findings 含 P0/P1」；禁止把缺测/缺边界当成 P2。

强制步骤：
1. design_pack_read path=「${PACK_BASENAME}」（gate pack：原 pack 引用、acceptance、工头摘要、diff）。
2. 只审 delta；逐条对照 acceptance，列出 unmet_acceptance。
   若本 pack 是 **delta / fix pack**（含 prior_findings 或前轮 findings）：
   **只复审** ① 上轮 prior findings 是否已修，② 新 diff 是否引入回归；**不要**重审整仓或重复提出已修项。
3. Findings 分层：P0 阻塞 / P1 应修（含缺单测、缺 acceptance 要求的边界）/ P2 nits。
4. 把审查结论写入：「${SUMMARY_OUT}」
   必须含短文 + **一个** 机器可读 fenced json：
\`\`\`json
{
  "verdict": "PASS|HOLD",
  "unmet_acceptance": ["未满足的 acceptance 原文或摘要"],
  "findings": [
    {"tier": "P0|P1|P2", "file": "路径或-", "issue": "问题", "fix_hint": "改法提示"}
  ]
}
\`\`\`
5. 写完 verdict 文件后停止；不要 Approve/gh；不要改 src。

硬禁：room_*；改业务仓；push / gh / dsh-trial-pr；打印 API key。
Done when: design_pack_read + verdict 文件落盘含机器块 + room_joined=false。
PROMPT
}

build_prompt_supervisor_plan() {
  cat <<PROMPT
你是 dsh 试用**监理**短票（supervisor-plan）。**禁止 join 任何 team-room**，禁止 room_*。不要改业务 src。

读 design_pack_read path=「${PACK_BASENAME}」（Goal brief）。然后：
1. 拆 **1..max_slices** 个最小 slice（max_slices 见 brief pack frontmatter，缺省 1）；每个 slice 写一个有界 pack（相对名见 brief 的 suggested_pack，或自定 <slice>.pack.md）。
2. Pack 必须经文件写入：$DSH_HOME/supervisor/thin-state/packs/<name>.pack.md（可用 write/edit；禁止 bash cat 大段灌上下文）。
3. Pack 的 Steps/Done when 须可执行、可验收；**不要**故意留歧义。仅当实现歧义**真实影响** Done-when（缺参/冲突/不可验证）时，才要求工头用 ask_supervisor；文案风格等无关紧要的细节自行合理默认并写进 pack。
4. 更新或创建 Goal 状态文件：$DSH_HOME/supervisor/thin-state/goals/<goal_id>.json （goal_id 见 brief）。
5. 关票摘要写到：「${SUMMARY_OUT}」，文末 **一个** fenced json（多 slice 用 emit_chains）：
\`\`\`json
{
  "action": "emit_chains",
  "goal": "<goal_id>",
  "slices": [
    {"slice": "<slice_id>", "pack": "<name>.pack.md", "acceptance": ["可验证条目…"]}
  ],
  "goal_status": "running",
  "notes": "一两句"
}
\`\`\`
（单 slice 仍兼容旧版 `action=emit_chain` + 顶层 slice/pack/acceptance；broker 两种都接受。）
硬禁：room_*；push / gh / dsh-trial-pr；打印 API key；不要自己开 impl/gate。
Done when: pack 落盘 + summary 机器块 emit_chains + room_joined=false。
PROMPT
}

build_prompt_supervisor_answer() {
  cat <<PROMPT
你是 dsh 试用**监理**短票（supervisor-answer）。**禁止 join 任何 team-room**，禁止 room_*。不要改业务 src。

读 design_pack_read path=「${PACK_BASENAME}」（含工头问题）。给出**简明可执行**裁决（一两段内），写入摘要：「${SUMMARY_OUT}」，文末：
\`\`\`json
{
  "action": "chain_reply",
  "slice": "<slice_id>",
  "answer": "给工头的明确规则/取值",
  "goal_status": "running",
  "notes": "可选"
}
\`\`\`
硬禁：room_*；push / gh / dsh-trial-pr；空泛回复；打印 API key。
Done when: summary 含 chain_reply + answer 非空。
PROMPT
}

build_prompt_supervisor_close() {
  cat <<PROMPT
你是 dsh 试用**监理**短票（supervisor-close）。**禁止 join 任何 team-room**，禁止 room_*。

读 design_pack_read path=「${PACK_BASENAME}」（chain 终态摘要）。若 PASS：把 Goal 状态文件标为 done；写摘要：「${SUMMARY_OUT}」，文末：
\`\`\`json
{
  "action": "goal_done",
  "goal": "<goal_id>",
  "goal_status": "done",
  "report": "Goal 完成一两句",
  "slices": ["<slice_id>"],
  "pr_url": "若 Done-when 要求 PR 则填 URL，否则空"
}
\`\`\`
若 Goal Done-when 要求对 main 开 PR 且 impl 未开：可用受控包装补开（仍禁改业务 src）：
`dsh-trial-pr push-and-pr --repo <fengrunda/knowledge-hub|fengrunda/memory-as-training> --cwd <repo> --branch <feature> --title "…" --body "…"`
仍禁 force / merge / 其他仓 / 裸 `git push` / 裸 `gh`。PR URL 写入 report 与机器块 pr_url。
若失败/escalated：goal_status 用 failed 或 escalated，action 仍可读 goal_done 或 goal_report。
硬禁：room_*；改业务 src；裸 push/gh；force/merge；打印 API key / GH_TOKEN。
Done when: goals JSON 已更新 +（若要求 PR 则已有 open PR URL）+ summary 机器块。
PROMPT
}

if [[ -n "$PROMPT_FILE" ]]; then
  [[ -f "$PROMPT_FILE" ]] || { echo "prompt-file missing: $PROMPT_FILE" >&2; exit 2; }
  PROMPT="$(cat "$PROMPT_FILE")"
else
  case "$PROMPT_MODE" in
    baseline) PROMPT="$(build_prompt_baseline)" ;;
    foreman)  PROMPT="$(build_prompt_foreman)" ;;
    gate)     PROMPT="$(build_prompt_gate)" ;;
    supervisor-plan)   PROMPT="$(build_prompt_supervisor_plan)" ;;
    supervisor-answer) PROMPT="$(build_prompt_supervisor_answer)" ;;
    supervisor-close)  PROMPT="$(build_prompt_supervisor_close)" ;;
    *) echo "unknown --prompt-mode: $PROMPT_MODE" >&2; exit 2 ;;
  esac
fi

ARGS=(--profile "$PROFILE" --ticket "$TICKET" --cwd "$CWD" --prompt "$PROMPT" --new)
[[ -n "$ROLE" ]] && ARGS+=(--role "$ROLE")
if [[ "$KEEP_OPEN" -eq 1 ]]; then
  ARGS+=(--keep-open)
else
  ARGS+=(--final)
fi

{
  echo "=== open-slice $(date '+%Y-%m-%d %H:%M:%S %Z') ==="
  echo "ticket=$TICKET profile=$PROFILE cwd=$CWD pack=$PACK_ABS summary=$SUMMARY_OUT mode=${PROMPT_MODE} role=${ROLE:-$INF_ROLE}"
  echo "ask=$ASK_BIN"
  echo "=== prompt ==="
  echo "$PROMPT"
  echo "=== run ==="
  ec=0
  python3 "$ASK_BIN" "${ARGS[@]}" || ec=$?
  echo "=== exit=$ec ==="
  # Agents sometimes glue ~/.dsh + supervisor → ~/.dsh-supervisor/thin-state/summaries.
  # Canonical path is $THIN_STATE/summaries (usually ~/.dsh/supervisor/thin-state/summaries).
  if [[ ! -f "$SUMMARY_OUT" ]]; then
    _wrong="$HOME/.dsh-supervisor/thin-state/summaries/$(basename "$SUMMARY_OUT")"
    if [[ -f "$_wrong" ]]; then
      mkdir -p "$(dirname "$SUMMARY_OUT")"
      mv -f "$_wrong" "$SUMMARY_OUT"
      echo "adopted misplaced summary $_wrong → $SUMMARY_OUT"
    fi
  fi
  exit $ec
} 2>&1 | tee "$LOG"
