> **路径说明**：文中 `/home/box/.dsh`、`/workspace/dsh-trial` 等为箱上历史默认；复用时请改为 `$DSH_HOME` / `$DSH_TRIAL_ROOT` / 环境变量（见仓库根 README「Known box paths」）。

# dsh-trial token 体积优化（2026-09-29，Asia/Shanghai）

目标：在**不缩短 Done-when / 不拆更碎短票**前提下，压 trial 峰值与 `tool_res` 占比。只动隔离 trial；**未**碰 `broker-khub-prod`、产品四票、产品 `acp`/`acp-lite`、Hermes、房间。

备份后缀：`*.bak-20260929-tokenopt`。

---

## 1. 优先：工具输出 / eager-offload（已开 + trial 更紧）

### 状态

| 项 | 值 |
|----|----|
| 插件路径 | `/workspace/dsh-plugins/dsh-eager-offload`（profile `link:` → `node_modules/dsh-eager-offload`） |
| 挂载 profile | **acp-lite-trial**、**acp-trial**（bundle + `cordis.patch.yml` `- id: eager-offload`） |
| dump-config | 可见 `name: dsh-eager-offload`；改 patch 后新 session 生效 |
| 产品 acp / acp-lite | **未改**（仍 4096 / 1536 / 1024 / bash 3072） |

### Trial 阈值（本轮收紧，低于产品）

| key | 产品 | **trial 现** |
|-----|------|-------------|
| `inlineMaxBytes` | 4096 | **2560** |
| `previewHeadBytes` | 1536 | **1024** |
| `previewTailBytes` | 1024 | **768** |
| `offloadReadMaxInlineBytes` | 16384 | **8192** |
| `toolOverrides.bash` | 3072 | **2048** |
| `toolOverrides.read` | 4096 | **2560** |
| `spill-policy.maxInlineBytes` | 12288 | **8192**（非 read；spill 仍跳过 read） |
| `tool-fs.readMaxBytes` | 24576 | **16384** |

GC：`~/.dsh/bin/dsh-offload-gc.py`（broker 已按小时/每 job 调）。

### 对 composition `tool_res` 的预期

- **入史前裁剪**：超 cap 的 plain-text tool_result → 落盘 + head/tail 预览（预览合计约 ≤2.5KiB / bash ≤2KiB），ACP 每步重送全史时只放大预览而非全文。
- **相对本轮改前（trial=产品 4096）**：
  - 触发更早：原 2.5–4KiB 中带全文现也会 offload。
  - 已超限条目的模型侧体积：约 **~2.5–4KiB 预览 → ~1.8–2.5KiB**（再减 ~30–40%）。
  - 典型大 `bash`/`read`（十几–几十 KiB）：单条对后续每步的贡献从「全文」降到预览量级（约 **5–20×** 视原文大小）；总 peak 降幅取决于大输出占比。
- **不替代**：短小工具结果仍全文；模型若反复 `read` offload 路径仍可能抬 `tool_res`（插件对 offload 根有原地截断，但需 offset/limit 纪律——已写入 foreman prompt）。

---

## 2. 清歧义烟测文案

`open-slice.sh` `supervisor-plan`：删除「故意在 pack 留歧义好让工头 question」。

改为：pack 可执行可验收；**仅当**实现歧义**真实影响** Done-when 才要求工头 `ask_supervisor`；无关紧要细节合理默认。

Foreman / ask 文案同步收紧（同口径）。

---

## 3. impl 步数纪律（软硬结合 → 目前仅软）

| 层 | 做法 |
|----|------|
| Prompt（软） | foreman：建议 **~25** 工具轮；近 20+ 收工写 summary 或 question 拆片；禁无效探索 |
| `limits.json` / `DEFAULT_LIMITS` | 新增 **`impl_max_steps`: 30**（`dsh-trial limits` 可 show/set） |
| Broker 硬钩子 | **无**可靠 mid-ticket 步数钩子 → **未**做裸杀；撞顶应优雅 summary，不要半吊子 kill |

后续若要硬约束：需在 ACP ask / composition 步计数处优雅注入「收工」prompt 或 `session/close`，并区分 inplace rework；落地前保持软字段。

---

## 4. 温和 compact（方案；本轮不实施半成品）

### 现成能力

- 全局 `agent-presets.default: box-tight`（`thresholdRatio: 0.55` + pruner 4096/2048/512）。
- ACP **无** `/compact` slash；自动 compaction 靠压力比；改 preset **不**重装已组装旧 session。
- 社区 `dsh-compaction-instant`：**明确先不上**（见既有 context 文档）。

### 为何未在 trial patch 强开「35–40k」

- `compaction-basic` 有效配置在 **agent preset 隔离 realm**；profile `cordis.patch` 顶层覆盖不一定打进会话 realm（dump 宿主层仍见默认 pruner）。
- 绝对 35–40k 依赖模型 context window（kimi-for-coding 未在本机钉死）；乱调 `thresholdRatio` 可能过早 LLM 摘要、伤质量。
- 短票主因是 **tool_res 累乘**，eager-offload 杠杆更大。

### 可执行接法（批准后再做）

1. **Trial-only preset**（推荐）：复制 `box-tight` → `box-trial-tight`，把 `thresholdRatio` 调到使触发约 **35–40k prompt**（先用 composition/`usage_prompt` 标定窗口，再反推 ratio）；仅 trial profile / ask 显式选用。可回滚：改回 `box-tight`。
2. **风险**：过早摘要丢细节 → gate 误 HOLD / 重复读文件抬步数；旧 sticky session 需轮换才吃到新 preset。
3. **不要**：手写半成品裁史插件塞进产品；不要 alias 换 `dsh-compaction-basic` 进正式 bundle。
4. **观测**：对比同 Goal 改前后 `composition` 的 `tool_res` 占比与 `usage_prompt` peak / steps。

---

## 5. 改动文件清单

| 路径 | 变更 |
|------|------|
| `~/.dsh/profiles/acp-lite-trial/cordis.patch.yml` | trial-tight offload/spill/fs |
| `~/.dsh/profiles/acp-trial/cordis.patch.yml` | 同上 |
| `/workspace/dsh-trial/open-slice.sh` | 清歧义 + foreman 步数纪律 |
| `~/.dsh/supervisor/trial/limits.json` | `impl_max_steps: 30` |
| `/workspace/dsh-trial/broker/trial_lib.py` | `DEFAULT_LIMITS.impl_max_steps` |
| `/workspace/docs/drafts/dsh-trial-SKILL.md` | 一行指针（不 duplicate） |

未改：产品四票、`broker-khub-prod`、产品 cordis、Hermes、房间、Done-when 粒度。

---

## 6. 残余风险

- 过紧 offload → 模型少看上下文误改；用 offset/limit / ask 缓解；可把 trial 阈值回调向产品。
- Soft step 上限靠模型自觉，长 inplace rework 仍可能超 25。
- Compact 未提前：长票 peak 仍可能在 offload 之后爬升到 box-tight 触发点。
- 示例 job（如 `job-goal-medium-bridge.json`）若仍「故意留歧义」，那是 job brief 烟雾，不是 open-slice 默认指令。
