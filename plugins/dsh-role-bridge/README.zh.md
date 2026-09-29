# dsh-role-bridge（角色桥 · 可复用票中沟通）

- 状态：`0.1.0-dev`，**仅 trial profile**（`acp-lite-trial` / `acp-trial`）
- 依赖：箱上常驻 `broker-dsh-trial`（dsh **不能**原生唤醒对端 ACP 票）

## 1. 为什么需要 broker

ACP 一票一进程；`dsh-acp-ask.py` 退出后对端不会被工具调用自动叫醒。本插件只负责：

1. 把结构化消息写入 `thin-state/mailbox/pending/<id>.json`
2. （可选）轮询 `mailbox/answers/<id>.json` 直到有答复或超时

**开对端短票、拼上下文包、写答案文件** 全是 broker 按 **路由表** 做的。插件与 broker 必须成对使用。

## 2. 消息格式

`pending/<id>.json`：

```json
{
  "id": "ask-…",
  "kind": "ask_supervisor",
  "to_role": "supervisor",
  "from": { "ticket": "impl-…", "role": "impl", "goal": "…", "slice": "…" },
  "payload": { "questions": ["…"], "context": "…" },
  "reply_to": null,
  "wait": true,
  "timeout_sec": 600,
  "status": "pending",
  "created_at": "…"
}
```

`answers/<id>.json`：路由处理器自定义，但至少含可识别终态字段（如 `answer` / `verdict`）。

## 3. 工具

| 工具 | 类型 | 说明 |
|------|------|------|
| `send_to_role` | **原语** | `to_role` + `kind` + `payload` + `wait` + `timeout` |
| `ask_supervisor` | 薄封装 | → `send_to_role(supervisor, ask_supervisor, {questions,context})` |
| `submit_for_review` | 薄封装 | → `send_to_role(gate, submit_for_review, {summary,usage_prompt,…})` |

配置（`cordis.patch.yml`）：

- `wrappers`: 启用哪些薄封装（默认两个都开）
- `enableSendToRole`: 是否暴露原语（默认 true）
- `timeoutSec` / `pollMs` / `mailboxRoot`
- `inplaceReworkMaxPrompt` / `inplaceReworkMaxFindings`（给 broker 路由决策用的声明；真正门槛以 `limits.json` / routes 为准）

### 超时（务必读）

- 工具声明 `timeoutMs = (timeoutSec+60)*1000`，由 `@deepseek-ai/dsh-tool-call-timeout-policy` 协作取消。
- 外层 ACP prompt 须 `DSH_ACP_PROMPT_TIMEOUT ≥ timeoutSec + 余量`（trial 默认 1800s）。
- 超时返回 `degrade` 提示（ask→`question` 关票；review→`close_ticket`）。

## 4. 路由表

路径：`~/.dsh/supervisor/trial/routes.json`

按 `(to_role, kind)` 决定：开什么 profile / 角色家 / prompt-mode、带哪些上下文、答案怎么写、套用哪些上限键。

```json
{
  "routes": [
    {
      "to_role": "supervisor",
      "kind": "ask_supervisor",
      "handler": "supervisor_answer",
      "profile_from": "goal.supervisor_profile",
      "role_home": "supervisor",
      "prompt_mode": "supervisor-answer",
      "context": ["questions", "goal_brief"],
      "limits": { "count_as": "supervisor_ticket", "timeout_key": "ask_supervisor_timeout_sec" }
    },
    {
      "to_role": "gate",
      "kind": "submit_for_review",
      "handler": "gate_review",
      "profile_from": "goal.gate_profile",
      "role_home": "gate",
      "prompt_mode": "gate",
      "context": ["acceptance", "diff", "prior_findings", "foreman_summary"],
      "limits": {
        "timeout_key": "ask_supervisor_timeout_sec",
        "inplace_prompt_key": "inplace_rework_max_prompt",
        "inplace_findings_key": "inplace_rework_max_findings"
      }
    }
  ]
}
```

## 5. 怎么加一种新 kind

1. **插件**：若需要专用 schema，加一个薄封装函数（固定 `to_role`/`kind`/payload 形状）；或直接让模型调 `send_to_role`。
2. **路由**：在 `routes.json` 加一条 `(to_role, kind)`，写明 handler 名与 context。
3. **broker**：实现对应 handler（开短票 → 解析摘要 → `write_ask_answer`），并在 mailbox watcher 里按 `kind` 查表分发。
4. **指标**：`metrics.by_kind[<kind>]` 自动记账（次数 / 等待 / 超时 / 被唤醒票 token）。
5. **不要** fork 一整份 mailbox 实现。

## 6. 安装（trial only）

```bash
# 已用独立 profile，避免污染产品 symlink 的 acp/acp-lite
dsh plugin --profile acp-lite-trial add -w /workspace/dsh-plugins/dsh-role-bridge
dsh --profile acp-lite-trial --dump-config | grep -A6 role-bridge
```

**理由**：产品角色家 `~/.dsh-homes/{impl,gate}/profiles/acp` → 共用 `~/.dsh/profiles/acp`。若把桥装进本体，产品四票会看见 `ask_supervisor`/`submit_for_review` 并可能误写 trial mailbox。

## 7. 与旧包关系

`/workspace/dsh-plugins/dsh-ask-supervisor` 为前期原型，**请改用本包**。trial profile 应只挂 `dsh-role-bridge`。
