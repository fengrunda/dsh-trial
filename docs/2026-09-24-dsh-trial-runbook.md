> **路径说明**：文中 `/home/box/.dsh`、`/workspace/dsh-trial` 等为箱上历史默认；复用时请改为 `$DSH_HOME` / `$DSH_TRIAL_ROOT` / 环境变量（见仓库根 README「Known box paths」）。

# dsh 试用 Runbook（Route C · 方案 C）

- 日期：2026-09-24（Asia/Shanghai）
- 目标：dsh 优化后跑**真实试用**，再与 Hermes 对比；本 runbook 只覆盖 **执行票不进房 + 文件桥/pack/中票**。
- 上游：`/workspace/docs/2026-09-24-dsh-room-bypass-p1-plan.md`（选 C）

---

## 0. 硬约束

1. **永不**重启 / mutate `broker-khub-prod` 或产品四票（`khub-supervisor` / `hub-foreman` / `engine-foreman` / `design-gate`）。
2. **执行票禁止加入任何房间**（含 `khub-dev`、将来的 `khub-trial`）。派活不走 `room_post`→执行票 inbox。
3. 不 fork `dsh-team-rooms`；不把 `injectRoomBrief: false` 宣称为旁路完成。
4. 不打印 / 不落盘 API key。

---

## 1. Profile 矩阵

| profile | design-pack | MCP（serena / codegraph / serena-hub） | team-rooms 包 | 用途 |
|---------|:-----------:|:-------------------------------------:|:-------------:|------|
| **`acp`** | ✅（产品路径） | ✅ | 已装（执行票仍**不 join**） | Hub / 引擎 / intel 真实感试用 |
| **`acp-lite`** | ✅ | ❌（有意省略） | 已装（不 join） | 插件 / 旁路形态沙箱、快速烟雾 |
| `dp-design-pack` | ✅ | ❌ | 无 | pack 单测（headless） |
| `web` | 视安装 | 视配置 | 已装 | 人类看板；非本试用主路径 |

- 角色家目录：`~/.dsh-homes/{impl,gate}/profiles/acp` → **符号链接**到 `~/.dsh/profiles/acp`（装插件一次即可）。
- 备份：`~/.dsh/backups/acp-design-pack-*`。

**选用建议**：烟雾 / 插件回归优先 `acp-lite`；宣称「Hub 路径可用」必须在 **`acp`** 上跑通 `design_pack_read`。

---

## 2. Pack 位置与约定

| 项 | 路径 |
|----|------|
| pack 根 | `/home/box/.dsh/supervisor/thin-state/packs/` |
| 索引 | `/home/box/.dsh/supervisor/thin-state/index.json` |
| 关票摘要 | `/home/box/.dsh/supervisor/thin-state/summaries/<slice>.md` |
| 角色桥 | `open-slice.sh` 会把上述 packs/summaries **symlink** 进 `~/.dsh-homes/{role}/supervisor/thin-state/`（因 `--role` 时 `DSH_HOME` 切到角色家，plugin 的 `packRoot` 相对该家） |

Pack 相对名示例：`trial-slice-1.pack.md` → 工具 `design_pack_read` 的 `path` 参数用该相对名。  
**禁止**用 bash `cat` 把整包灌进 prompt（避免制造 sticky 正文税）。

---

## 3. 开 / 关命令

### 开票（推荐包装）

```bash
/workspace/dsh-trial/open-slice.sh \
  --ticket impl-<slice>-YYYYMMDD \
  --pack <name>.pack.md \
  --profile acp \
  --final \
  --log /workspace/tmp/<ticket>.log
```

底层：`dsh-acp-ask.py --profile … --ticket … --prompt … --final`  
Prompt 纪律：必须 `design_pack_read` → 写 summary → **不** `room_*` / 非必要 MCP。

### 关票

- `--final` 已 `session/close` 并清 ticket meta。
- 摘要必须落在 `thin-state/summaries/`。
- Goal 约定时才调 `~/.dsh/bin/khub-dsh-complete-notify.py`（试用烟雾默认 **不**调）。

### 纯插件烟雾

把 `--profile acp-lite` 即可；Hub 主路径验收仍以 `acp` 为准。

---

## 4. Composition + 房注入断言清单

每张试用票结束后：

```bash
# 1) 定位 session（ticket meta 或 sessions 树）
META=~/.dsh-homes/impl/acp-tickets/<ticket>.json   # 或 gate
# 或：find ~/.dsh-homes/impl/sessions -name 'session.v3.jsonl*' -newer …

# 2) composition
python3 ~/.dsh/bin/khub-acp-ctx-composition.py <session.jsonl.zstd> \
  -o /workspace/tmp/<ticket>-composition.csv

# 3) 房注入断言（必须 clean）
python3 /workspace/dsh-trial/assert-no-room-inject.py <session.jsonl.zstd>
# exit 0 = 无 [team-room …] / plugin relay；exit 1 = 有房 bus 灌入
```

断言启发式（与 `dsh-team-rooms` 实现一致；箱上 `PLUGIN='dsh-background-agents'`）：

- 正文以 `[team-room ` 开头；或
- `source.plugin ∈ {dsh-team-rooms, dsh-background-agents}` 且 `form=relay`；或
- 同上 plugin + `form=notice` 且正文像 room brief。

**通过标准**：`clean=true`；composition 的 peak `usage_prompt` 与 tools_n 记入报告（相对 fat 房票应低一个数量级）。

---

## 5. 执行票不得进房（显式）

| 角色 | 房间 | 文件桥 |
|------|------|--------|
| 执行票（工头/审查中票） | **禁止 join**；禁止依赖 room catch-up | pack + summary + inbox/outbox |
| 监理（可选） | 可弱用房间作人类可读总线 | 薄状态索引为主 |
| 人类 | web 看板可选 | — |

若有人手动把执行票 join 进房：该票 composition / assert **应失败**；作废重开，不要在污染史上继续 tick。

---

## 6. 与 Hermes 对比窗口（前置）

在宣称可比前，P1 计划 §6 清单须齐：design-pack 在 `acp`、执行侧无房灌入、中票闭环、composition 入库、完成回执纪律、隔离未污产品四票。  
本 runbook 只保证脚手架；多日试用票集与 Hermes 对照表另文。

---

## 7. 连续试用：broker-dsh-trial（常驻薄 broker）

产品四票 **保持冻结**。真实连续试用用隔离进程：

| 项 | 路径 |
|----|------|
| 脚手架 | `/workspace/dsh-trial/broker/` |
| 运行态 | `/home/box/.dsh/broker-dsh-trial/`（pidfile + log） |
| inbox / outbox | `/home/box/.dsh/supervisor/trial/{inbox,outbox}/` |
| 产物 | `/workspace/tmp/trial-broker/<ticket>/` |

```bash
source /home/box/.dsh/load-env.sh
export DSH_PERMISSION_MODE=danger-full-access

# 投放
cp /workspace/dsh-trial/broker/examples/job-baseline.json \
  /home/box/.dsh/supervisor/trial/inbox/

# 单次（烟雾）或常驻
/workspace/dsh-trial/broker/start.sh --once
# /workspace/dsh-trial/broker/start.sh          # poll 20s
# /workspace/dsh-trial/broker/stop.sh

python3 /workspace/dsh-trial/broker/trial-broker.py --status
```

行为：读 inbox → `open-slice.sh --final` → 成功则归档 outbox + 确认 `thin-state/summaries/` + 写 composition/assert。  
**不** join 房间、**不**调用 `khub-broker-restart.sh`、可与 `broker-khub-prod` 共存。

详：`/workspace/dsh-trial/broker/README.md`；Hub 交接：`/workspace/docs/2026-09-24-hub-bot-dsh-trial-handoff.md`。

---

## 7.1 Eager-offload（正式 profile）

`acp` / `acp-lite` 已装 `dsh-eager-offload`（`inlineMaxBytes=4096`，覆盖 read）。  
备份：`~/.dsh/backups/<profile>-pre-offload-*`。GC：`~/.dsh/bin/dsh-offload-gc.py`（broker 定时调用）。  
验收：`/workspace/docs/2026-09-24-dsh-trial-acceptance.md`。

---

## 8. Slice chain（文件桥替代旧房间）

监理（Hub Bot，**不是** dsh 票）向 trial inbox 丢 `type: "chain"`；broker 编排工头中票 → gate pack → gate 中票，全程 **无房间**。

| 项 | 路径 / 约定 |
|----|-------------|
| 状态 | `~/.dsh/supervisor/thin-state/chains/<slice>.json` |
| 工头票 | `impl-trial-<slice>-r<N>` · role=impl · `--prompt-mode foreman` |
| Gate 票 | `gate-trial-<slice>-r<N>` · role=gate · `--prompt-mode gate` |
| Gate pack | `packs/<slice>-gate-r<N>.pack.md`（原 pack + acceptance + 工头块 + diff） |
| Fix pack | `packs/<slice>-fix-r<N+1>.pack.md`（原 pack + findings，无 transcript） |
| Gate 摘要 | `summaries/<slice>-gate-r<N>.md` · 机器块 `verdict` PASS\|HOLD |
| 续答 | inbox `type: "chain-reply"` + `slice` + `answer` |
| 终态 | PASS / escalated / awaiting_supervisor / failed |
| 通知 | `notify: "hub"` → kind `dsh-trial-chain`（可用 `notify_dry_run`） |

示例：`/workspace/dsh-trial/broker/examples/job-chain-scratch.json`  
烟雾仓：`/workspace/dsh-trial/scratch-repo`（勿 push）。  
单测（mock）：`python3 /workspace/dsh-trial/broker/tests/test_chain_unit.py`

---

## 9. 回滚


```bash
# 仅卸 acp 上的 design-pack（示例）
dsh plugin --profile acp remove -w dsh-design-pack
# 或从备份恢复 package.json / cordis*.yml
# ~/.dsh/backups/acp-design-pack-<ts>/
```

**不要**为回滚去碰 `broker-khub-prod`。

---

*文档版本：2026-09-24-d · 执行面：箱上试用脚手架 + broker-dsh-trial*


---

## 8. 角色桥与可观测（补充）

- trial profile：`acp-lite-trial` / `acp-trial`（含 `dsh-role-bridge`）。
- 路由：`~/.dsh/supervisor/trial/routes.json`；上限：`…/limits.json`。
- CLI：`dsh-trial status|report|limits`。
- 工头工具：`ask_supervisor` / `submit_for_review`（禁止执行票进房）。
