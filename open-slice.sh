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
                [--cwd ABS] [--role gate|impl|supervisor] [--final] [--keep-open] [--resume]
                [--log PATH] [--summary-name NAME]
                [--prompt-mode baseline|foreman|gate|supervisor-plan|supervisor-answer|supervisor-close|supervisor-max-rounds|supervisor-pause] [--prompt-file PATH]
                [--reasoning-effort off|low|high|max] [--step-budget N]
                [--print-prompt]

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
  supervisor-max-rounds — dsh 监理：max_rounds 触顶三选一 inplace|split|escalate
  ( --prompt-file overrides the built-in template entirely )

print-prompt:
  Build the prompt, print it to stdout and exit 0 - no mkdir, no role-bridge,
  no log tee, no ask call.
USAGE
}

TICKET=""
PACK=""
PROFILE="acp"
CWD="/workspace"
ROLE=""
FINAL=1
KEEP_OPEN=0
RESUME=0
LOG=""
SUMMARY_NAME=""
PROMPT_MODE="baseline"
PROMPT_FILE=""
PRINT_PROMPT=0
REASONING_EFFORT=""
STEP_BUDGET=""
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
    --resume) RESUME=1; shift ;;
    --log) LOG="${2:-}"; shift 2 ;;
    --summary-name) SUMMARY_NAME="${2:-}"; shift 2 ;;
    --prompt-mode) PROMPT_MODE="${2:-}"; shift 2 ;;
    --prompt-file) PROMPT_FILE="${2:-}"; shift 2 ;;
    --reasoning-effort) REASONING_EFFORT="${2:-}"; shift 2 ;;
    --step-budget) STEP_BUDGET="${2:-}"; shift 2 ;;
    --print-prompt) PRINT_PROMPT=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

[[ -n "$TICKET" && -n "$PACK" ]] || { usage; exit 2; }

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

# Render a literal prompt template: substitute @@NAME@@ markers in bash.
# The replacement is quoted so bash 5.2 patsub_replacement does not treat
# "&" (or backslashes) inside a value as special.
_render_prompt() {
  local _text="$1"; shift
  local _ph _val
  while [[ $# -ge 2 ]]; do
    _ph="$1"; _val="$2"; shift 2
    _text="${_text//"$_ph"/"$_val"}"
  done
  printf '%s\n' "$_text"
}

build_prompt_baseline() {
  local _tpl
  _tpl="$(cat <<'PROMPT'
你是 Route C 中票执行器（基线片）。执行票 **禁止 join 任何 team-room**，禁止一切 room_*（含 room_post / room_read / room_join / room_list）。

本 slice 的唯一权威任务包在 design-pack 工具可读路径下；pack 正文只允许经 design_pack_read 进入上下文。

强制步骤（最多约 8 步，完成后停止）：
1. 调用工具 design_pack_read，path 使用相对名：「@@PACK_BASENAME@@」。
2. 用一句话确认：slice_id + pack bytes（工具返回的字节数）。
3. 严格按 pack 的 Done when 执行（需要读文件时只用 read 工具，禁止 bash）。
4. 把关票摘要写入：「@@SUMMARY_OUT@@」
   至少含：status / used_tool=design_pack_read / room_joined=false / blockers。
5. 写完摘要后停止；不要继续探索。

硬禁（基线片）：
- 禁止对 pack 路径做任何 bash（含 cat/sed/head/find/wc/ls/stat/grep/rg）；禁止 bash 探查 packs/ 或 thin-state/packs。
- 禁止 serena / codegraph / mcp-* / 其它 MCP，除非本 pack 明确要求（本基线片不需要）。
- 禁止不必要的 glob / sessions_list / 其它探索工具；只做 Done when 要求的最少工具。

Done when: design_pack_read 成功 + pack 要求的产物落盘 + summary 落盘 + 未进房。
PROMPT
)"
  _render_prompt "$_tpl" \
    '@@PACK_BASENAME@@' "$PACK_BASENAME" \
    '@@SUMMARY_OUT@@' "$SUMMARY_OUT"
}

build_prompt_foreman() {
  local _tpl
  _tpl="$(cat <<'PROMPT'
你是 Route C 工头中票（foreman / impl）。**禁止 join 任何 team-room**，禁止一切 room_*（含 room_post / room_read / room_join / room_list）。

唯一权威任务：经 design_pack_read 读取 pack「@@PACK_BASENAME@@」，按 pack 执行改动（cwd=@@CWD@@）。

## 票中沟通（role-bridge 桥；本票只允许以下两个工具）
- 「ask_supervisor」：**仅当**实现歧义真实影响 Done-when（缺参/冲突/不可验证）时找监理澄清；文案风格等可合理默认则不要问。必传 `questions`（非空字符串数组，可附 `context`）。**允许**。
- 「submit_for_review」：改动做完后提交 gate 审查。必传 `usage_prompt` 与 `summary`，可附 `changed_files` / `base` / `commit`。提交前 summary 必须已写到「@@SUMMARY_OUT@@」且非空（未落盘/为空会被 broker 直接退回，不启动 gate、不计轮次）。**允许**。
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
- 若收到 `[step-budget] …硬阈值` 或工具被拦截：立即写 summary，`status=paused_limit`，notes 写清已用步数与剩余工作；**不要**继续探索。墙钟超时同理可用 `paused_wallclock`。broker 会交监理五选一（继续/新开/收尾/拆分/升级），不会机械 closeout。
- 大工具输出已由 eager-offload 裁剪：优先 offset/limit 读所需片段，不要为「看全量」空转。

## 强制步骤：
1. design_pack_read path=「@@PACK_BASENAME@@」。
2. 按 pack 的 Done when / Steps 做最少必要改动（可用 edit/write/bash 做 git 与文件；禁止 room_*）。
3. **仅当**歧义真实影响 Done-when：调 `ask_supervisor`；**超时/失败**（timed_out / degrade）时不要干等，写 summary status=question，并把同样的问题放进 questions。
4. 改完**先**把关票摘要（含机器块，status=done）按第 5 步格式写到「@@SUMMARY_OUT@@」，**再**调 `submit_for_review`（务必带 usage_prompt；summary 未落盘/为空会被 broker 直接退回，不启动 gate、不计轮次）：
   - 返回 PASS → 确认摘要为最终版后停止。
   - 返回 HOLD + rework_mode=inplace → **本票内**按 findings 逐条修完，更新摘要（逐条 finding_resolutions）后再 `submit_for_review` 一次（不要提前关票）。
   - 返回 HOLD + rework_mode=fresh → 更新摘要 status=done、notes 含 `rework_fresh` 后停止；**broker 会立刻另开一张 findings-first 修复票并重跑 gate**，这不是 slice 完成。
   - 工具不可用/超时（degrade=close_ticket）→ 摘要已在，status=done，停止，交给 broker 兜底 gate。
5. 摘要格式（第 4 步写入「@@SUMMARY_OUT@@」的内容；HOLD 时更新同一文件）
   摘要必须短，并在文末含 **一个** 机器可读 fenced json 块，键固定：
```json
{
  "status": "done|blocked|question|paused_limit|paused_wallclock",
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
```
   **有 gate findings 的 pack（fix/rework/reply）**：必须逐条写 `finding_resolutions`；
   未修 P0/P1 时只能写 status=blocked/question，或继续 inplace 再 `submit_for_review`，**禁止** status=done 假装收口。
6. status=done 表示你认为可交 gate；blocked/question 则不要假装完成。
   有未解决 P0/P1 却写 done：broker 不会当干净完成，会另开修复票或降级，白费一轮。
7. 确认摘要已是最终版（与本次 gate 结果一致）后停止。

硬禁：room_* / join / send_to_role 直连；裸 `git push` / 裸 `gh`；force-push；push main/master；`gh pr merge`；其他仓；不要把整仓灌进摘要；不要打印 API key / GH_TOKEN。
若 Done-when 要求 PR：gate PASS（或 pack 允许）后经 `dsh-trial-pr` 开 PR，stdout 的 PR URL 写入 summary notes。
Done when: design_pack_read +（按 pack 完成或明确 blocked/question）+（PASS 后的 done / HOLD-inplace 修完并复提 / HOLD-fresh 的 done+rework_fresh+finding_resolutions / ask 超时的 question）+（若要求 PR 则已用 dsh-trial-pr 开出或确认已有 open PR）+ summary 含机器块 + 有 findings 时逐条 finding_resolutions + room_joined=false。
PROMPT
)"
  _render_prompt "$_tpl" \
    '@@CWD@@' "$CWD" \
    '@@PACK_BASENAME@@' "$PACK_BASENAME" \
    '@@SUMMARY_OUT@@' "$SUMMARY_OUT"
}

build_prompt_gate() {
  local _tpl
  _tpl="$(cat <<'PROMPT'
你是 Route C 设计审查中票（gate / design-gate 风格）。**禁止 join 任何 team-room**，禁止一切 room_*。
本票只读审查：对照 pack 内 acceptance + 工头摘要 + diff，做 **delta-only / 分层** 审查。不要改业务源码。

## 裁决硬规则（必须遵守；broker 会二次强制）
- **任一 acceptance 条目未满足** → verdict 必须 **HOLD**（写入 unmet_acceptance）。
- **任一 P0 或 P1 finding** → verdict 必须 **HOLD**。
- **仅有 P2 nits、且全部 acceptance 满足** → **PASS**（P2 仍写入 findings）。
- 禁止「口头 PASS 但 findings 含 P0/P1」；禁止把缺测/缺边界当成 P2。

强制步骤：
1. design_pack_read path=「@@PACK_BASENAME@@」（gate pack：原 pack 引用、acceptance、工头摘要、diff）。
2. 只审 delta；逐条对照 acceptance，列出 unmet_acceptance。
   若本 pack 是 **delta / fix pack**（含 prior_findings 或前轮 findings）：
   **只复审** ① 上轮 prior findings 是否已修，② 新 diff 是否引入回归；**不要**重审整仓或重复提出已修项。
3. Findings 分层：P0 阻塞 / P1 应修（含缺单测、缺 acceptance 要求的边界）/ P2 nits。
4. 把审查结论写入：「@@SUMMARY_OUT@@」
   必须含短文 + **一个** 机器可读 fenced json：
```json
{
  "verdict": "PASS|HOLD",
  "unmet_acceptance": ["未满足的 acceptance 原文或摘要"],
  "findings": [
    {"tier": "P0|P1|P2", "file": "路径或-", "issue": "问题", "fix_hint": "改法提示"}
  ]
}
```
   若到约 28 步仍未定论（见「审核原则」末条），机器块改用
   `{"status": "paused_limit", "used_steps": <已用步数>, "remaining": "<剩余待判一句话>", "findings": [...]}`：
   **status=paused_limit 时 `used_steps` 与 `remaining` 两键必填**，勿写空 HOLD、勿留空 verdict。
5. 写完 verdict 文件后停止；不要 Approve/gh；不要改 src。

硬禁：room_*；改业务仓；push / gh / dsh-trial-pr；打印 API key。
Done when: design_pack_read + verdict 文件落盘含机器块 + room_joined=false。
PROMPT
)"
  _render_prompt "$_tpl" \
    '@@PACK_BASENAME@@' "$PACK_BASENAME" \
    '@@SUMMARY_OUT@@' "$SUMMARY_OUT"
}

build_prompt_supervisor_plan() {
  local _tpl
  _tpl="$(cat <<'PROMPT'
你是 dsh 试用**监理**短票（supervisor-plan）。**禁止 join 任何 team-room**，禁止 room_*。不要改业务 src。

读 design_pack_read path=「@@PACK_BASENAME@@」（Goal brief）。然后：
1. 拆 **1..max_slices** 个最小 slice（max_slices 见 brief pack frontmatter，缺省 1）；每个 slice 写一个有界 pack（相对名见 brief 的 suggested_pack，或自定 <slice>.pack.md）。
2. Pack 必须经文件写入：@@DSH_HOME@@/supervisor/thin-state/packs/<name>.pack.md（可用 write/edit；禁止 bash cat 大段灌上下文）。
3. Pack 的 Steps/Done when 须可执行、可验收；**不要**故意留歧义。仅当实现歧义**真实影响** Done-when（缺参/冲突/不可验证）时，才要求工头用 ask_supervisor；文案风格等无关紧要的细节自行合理默认并写进 pack。
   - Pack frontmatter 写 `tests:`（定点测试 path 列表，如 `tests/test_x.py`，逗号或 YAML 数组均可）；只有跨模块改动或切 tip 前才写 `full_suite: true`（按目录分批全量），日常 slice 不要写。
4. 更新或创建 Goal 状态文件：@@DSH_HOME@@/supervisor/thin-state/goals/<goal_id>.json （goal_id 见 brief）。
5. 关票摘要写到：「@@SUMMARY_OUT@@」，文末 **一个** fenced json（多 slice 用 emit_chains）：
```json
{
  "action": "emit_chains",
  "goal": "<goal_id>",
  "slices": [
    {"slice": "<slice_id>", "pack": "<name>.pack.md", "acceptance": ["可验证条目…"]}
  ],
  "goal_status": "running",
  "notes": "一两句"
}
```
（单 slice 仍兼容旧版 `action=emit_chain` + 顶层 slice/pack/acceptance；broker 两种都接受。）
硬禁：room_*；push / gh / dsh-trial-pr；打印 API key；不要自己开 impl/gate。
Done when: pack 落盘 + summary 机器块 emit_chains + room_joined=false。
PROMPT
)"
  _render_prompt "$_tpl" \
    '@@PACK_BASENAME@@' "$PACK_BASENAME" \
    '@@SUMMARY_OUT@@' "$SUMMARY_OUT" \
    '@@DSH_HOME@@' "$DSH_HOME"
}

build_prompt_supervisor_answer() {
  local _tpl
  _tpl="$(cat <<'PROMPT'
你是 dsh 试用**监理**短票（supervisor-answer）。**禁止 join 任何 team-room**，禁止 room_*。不要改业务 src。

读 design_pack_read path=「@@PACK_BASENAME@@」（含工头问题）。给出**简明可执行**裁决（一两段内），写入摘要：「@@SUMMARY_OUT@@」，文末：
```json
{
  "action": "chain_reply",
  "slice": "<slice_id>",
  "answer": "给工头的明确规则/取值",
  "goal_status": "running",
  "notes": "可选"
}
```
硬禁：room_*；push / gh / dsh-trial-pr；空泛回复；打印 API key。
Done when: summary 含 chain_reply + answer 非空。
PROMPT
)"
  _render_prompt "$_tpl" \
    '@@PACK_BASENAME@@' "$PACK_BASENAME" \
    '@@SUMMARY_OUT@@' "$SUMMARY_OUT"
}

build_prompt_supervisor_close() {
  local _tpl
  _tpl="$(cat <<'PROMPT'
你是 dsh 试用**监理**短票（supervisor-close）。**禁止 join 任何 team-room**，禁止 room_*。

读 design_pack_read path=「@@PACK_BASENAME@@」（chain 终态摘要）。若 PASS：把 Goal 状态文件标为 done；写摘要：「@@SUMMARY_OUT@@」，文末：
```json
{
  "action": "goal_done",
  "goal": "<goal_id>",
  "goal_status": "done",
  "report": "Goal 完成一两句",
  "slices": ["<slice_id>"],
  "pr_url": "若 Done-when 要求 PR 则填 URL，否则空"
}
```
若 Goal Done-when 要求对 main 开 PR 且 impl 未开：可用受控包装补开（仍禁改业务 src）：
`dsh-trial-pr push-and-pr --repo <fengrunda/knowledge-hub|fengrunda/memory-as-training> --cwd <repo> --branch <feature> --title "…" --body "…"`
仍禁 force / merge / 其他仓 / 裸 `git push` / 裸 `gh`。PR URL 写入 report 与机器块 pr_url。
若失败/escalated：goal_status 用 failed 或 escalated，action 仍可读 goal_done 或 goal_report。
硬禁：room_*；改业务 src；裸 push/gh；force/merge；打印 API key / GH_TOKEN。
Done when: goals JSON 已更新 +（若要求 PR 则已有 open PR URL）+ summary 机器块。
PROMPT
)"
  _render_prompt "$_tpl" \
    '@@PACK_BASENAME@@' "$PACK_BASENAME" \
    '@@SUMMARY_OUT@@' "$SUMMARY_OUT"
}

build_prompt_supervisor_max_rounds() {
  local _tpl
  _tpl="$(cat <<'PROMPT'
你是 dsh 试用**监理**短票（supervisor-max-rounds）。**禁止 join 任何 team-room**，禁止 room_*。不要改业务 src。

读 design_pack_read path=「@@PACK_BASENAME@@」（max_rounds 触顶决策包）。在 inplace / split / escalate 中**三选一**（有界；返工≠hang，勿写 orch.nudge）。
- inplace：同切片有界 +1（若 pack 写 bonus_used=true 则不可选 inplace，改 escalate 或 split）
- split：结束本链决策，剩余 P0/P1 建议新 slice；Goal 可 resume
- escalate：真升级

写入摘要：「@@SUMMARY_OUT@@」，文末：
```json
{
  "action": "max_rounds_decision",
  "slice": "<slice_id>",
  "decision": "inplace|split|escalate",
  "notes": "一两句理由"
}
```
硬禁：room_*；push / gh / dsh-trial-pr；空泛回复；打印 API key；盲抬全局 max_rounds。
Done when: summary 含 max_rounds_decision + decision 为三选一之一。
PROMPT
)"
  _render_prompt "$_tpl" \
    '@@PACK_BASENAME@@' "$PACK_BASENAME" \
    '@@SUMMARY_OUT@@' "$SUMMARY_OUT"
}

build_prompt_supervisor_pause() {
  local _tpl
  _tpl="$(cat <<'PROMPT'
你是 dsh 试用**监理**短票（supervisor-pause）。**禁止 join 任何 team-room**，禁止 room_*。不要改业务 src。

读 design_pack_read path=「@@PACK_BASENAME@@」。这是墙钟或步数暂停，不是 max_rounds 触顶。
在 continue_same / new_ticket / close / split / escalate 中**五选一**。
- continue_same：原 impl 会话续跑（优先）。写 add_steps 与 add_timeout_sec（秒，可加在现有墙钟上）
- new_ticket：同分支新开 impl 票
- close：按当前 WIP 收口
- split：剩余工作拆走，本链保持暂停
- escalate：升级

禁止写成 max_rounds 的三选一（不要 action=max_rounds_decision，不要 decision=inplace）。

写入摘要：「@@SUMMARY_OUT@@」，文末：
```json
{
  "action": "pause_decision",
  "slice": "<slice_id>",
  "decision": "continue_same|new_ticket|close|split|escalate",
  "add_steps": 0,
  "add_timeout_sec": 0,
  "notes": "一两句"
}
```
硬禁：room_*；push / gh / dsh-trial-pr；空泛回复；打印 API key。
Done when: summary 含 pause_decision，decision 为五选一之一。
PROMPT
)"
  _render_prompt "$_tpl" \
    '@@PACK_BASENAME@@' "$PACK_BASENAME" \
    '@@SUMMARY_OUT@@' "$SUMMARY_OUT"
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
    supervisor-max-rounds) PROMPT="$(build_prompt_supervisor_max_rounds)" ;;
    supervisor-pause) PROMPT="$(build_prompt_supervisor_pause)" ;;
    *) echo "unknown --prompt-mode: $PROMPT_MODE" >&2; exit 2 ;;
  esac
fi

# 三层测试口径热注入：foreman/gate 只跑 pack 列出的定点测试，全量交 CI。
# 逻辑在 broker/pytest_scope.py，每张票重新执行本脚本即热生效；DSH_TEST_POLICY=0 可关闭。
# --print-prompt 是纯 dry-run（不落盘、不跑子进程），故跳过本段以免 git ls-files 探针。
if [[ ( "$PROMPT_MODE" == "foreman" || "$PROMPT_MODE" == "gate" ) \
      && "${DSH_TEST_POLICY:-1}" != "0" && "$PRINT_PROMPT" -eq 0 ]]; then
  _test_policy="$(python3 "$(dirname "$0")/broker/pytest_scope.py" --mode "$PROMPT_MODE" "$PACK_ABS" "$CWD" 2>/dev/null || true)"
  if [[ -n "$_test_policy" ]]; then
    PROMPT="$PROMPT"$'\n\n'"$_test_policy"
  fi
fi

# 审核口径唯一权威 5 条（dsh-gate-ab-20261021-s2）：与 broker GATE_PRINCIPLES_BLOCK
# 逐字一致（tests/test_gate_prompt_caliber_unit.py 锁住）。一次定稿，勿逐轮改措辞。
# 只补注意力/轮次口径，不重写人格；DSH_GATE_PRINCIPLES=0 可关。
if [[ "$PROMPT_MODE" == "gate" && "${DSH_GATE_PRINCIPLES:-1}" != "0" ]]; then
  PROMPT="$PROMPT"$'\n\n''## 审核原则
- 只核三件事：pack Done-when / 设计对齐 / P0–P1；其余细节不审。
- 只有 P2 时判 PASS，P2 写进 findings 备注，不得因 P2 HOLD。
- 工头摘要已有同命令结果则只读复核，不重跑。
- 全量回归交 CI；定点测试缺证据才补跑一次。
- 到约 28 步仍未定论：summary 写 status=paused_limit（勿空 HOLD），交监理五选一。'
fi

if [[ "$PRINT_PROMPT" -eq 1 ]]; then
  # Print-only: build the prompt but touch nothing on disk and never call ask.
  printf '%s\n' "$PROMPT"
  exit 0
fi

[[ -x "$ASK_BIN" || -f "$ASK_BIN" ]] || { echo "missing ask: $ASK_BIN" >&2; exit 2; }

mkdir -p "$(dirname "$LOG")" "$THIN_STATE/summaries"

INF_ROLE="$ROLE"
if [[ -z "$INF_ROLE" ]]; then
  case "$TICKET" in
    gate-*|gate_*|review-*|review_*|pr-gate-*) INF_ROLE=gate ;;
    supervisor-*|supervisor_*|sup-*|sup_*) INF_ROLE=supervisor ;;
    impl-*|impl_*|fix-*|fix_*|dev-*|dev_*) INF_ROLE=impl ;;
  esac
fi
[[ -n "$INF_ROLE" ]] && ensure_role_pack_bridge "$INF_ROLE"

# Gate 分流与推理档位热分级：收尾片可免审，其余默认低档，只有显式风险/高风险
# 路径才 high（行数不再抬档）。逻辑在 broker/gate_effort.py，每张票重新执行
# open-slice.sh 即热生效；DSH_GATE_TIERING=0 全部关闭，DSH_GATE_SKIP=0 只关免审。
GATE_TIER_NOTE=""
GATE_SKIP=""
if [[ ( "$INF_ROLE" == "gate" || "${ROLE:-}" == "gate" ) \
      && "${DSH_GATE_TIERING:-1}" != "0" ]]; then
  _cls_out="$(python3 "$(dirname "$0")/broker/gate_effort.py" --classify "$PACK_ABS" "$CWD" 2>/dev/null || true)"
  if [[ -n "$_cls_out" ]]; then
    _cls_action="${_cls_out%%$'\t'*}"
    _cls_rest="${_cls_out#*$'\t'}"
    _cls_effort="${_cls_rest%%$'\t'*}"
    _cls_reason=""
    [[ "$_cls_rest" == *$'\t'* ]] && _cls_reason="${_cls_rest#*$'\t'}"
    if [[ "$_cls_action" == "skip" && "${DSH_GATE_SKIP:-1}" != "0" ]]; then
      GATE_SKIP="$_cls_reason"
    elif [[ "$_cls_action" == "skip" ]]; then
      # 显式关闭免审：退化为 review + low，继续走 ask。
      [[ -z "$REASONING_EFFORT" ]] && REASONING_EFFORT="low"
      GATE_TIER_NOTE="gate_tier=low (skip disabled: $_cls_reason)"
    elif [[ "$_cls_action" == "review" && -z "$REASONING_EFFORT" && -n "$_cls_effort" ]]; then
      REASONING_EFFORT="$_cls_effort"
      GATE_TIER_NOTE="gate_tier=$_cls_effort ($_cls_reason)"
    fi
  fi
fi

if [[ -n "$GATE_SKIP" ]]; then
  mkdir -p "$(dirname "$SUMMARY_OUT")"
  cat > "$SUMMARY_OUT" <<EOF
# Gate 免审（收尾片）

本片按 gate_skip 规则免审：只开 PR，无新业务 commit，不启动 gate 会话。

\`\`\`json
{
  "verdict": "PASS",
  "findings": [],
  "unmet_acceptance": [],
  "notes": "gate_skip: $GATE_SKIP",
  "skip_reason": "$GATE_SKIP"
}
\`\`\`
EOF
  echo "gate_skip=$GATE_SKIP"
  echo "summary=$SUMMARY_OUT"
  exit 0
fi

# 默认每票新会话；监理同 Goal 复用时传 --resume（勿加 --new）。
ARGS=(--profile "$PROFILE" --ticket "$TICKET" --cwd "$CWD" --prompt "$PROMPT")
if [[ "$RESUME" -eq 1 ]]; then
  ARGS+=(--resume)
else
  ARGS+=(--new)
fi
[[ -n "$ROLE" ]] && ARGS+=(--role "$ROLE")
# Non-empty only: when unset, dsh-acp-ask.py falls back to DSH_REASONING_EFFORT /
# DSH_STEP_BUDGET from the environment itself.
[[ -n "$REASONING_EFFORT" ]] && ARGS+=(--reasoning-effort "$REASONING_EFFORT")
[[ -n "$STEP_BUDGET" ]] && ARGS+=(--step-budget "$STEP_BUDGET")
if [[ "$KEEP_OPEN" -eq 1 ]]; then
  ARGS+=(--keep-open)
else
  ARGS+=(--final)
fi

{
  echo "=== open-slice $(date '+%Y-%m-%d %H:%M:%S %Z') ==="
  echo "ticket=$TICKET profile=$PROFILE cwd=$CWD pack=$PACK_ABS summary=$SUMMARY_OUT mode=${PROMPT_MODE} role=${ROLE:-$INF_ROLE}"
  if [[ -n "$GATE_TIER_NOTE" ]]; then
    echo "$GATE_TIER_NOTE"
  fi
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
