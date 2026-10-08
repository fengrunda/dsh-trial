# dsh-eager-offload

**Eager** tool-output offload for dsh：在 compaction 压力之前就把过大的 plain-text tool_result 外置到磁盘，模型侧只留 head/tail 预览 + 路径 + 字节数。

- 官方 `dsh-spill` / spill-policy（箱上 `maxInlineBytes=12288`）**跳过 `read`**
- `dsh-compaction-tool-result-pruner` **只在压力后**动手
- ACP 每步重送全史 → tool_result 字节会乘进后续每一步

本插件挂 `tools/post-execute`（与 spill-policy 同一 waterfall），默认阈值更低（`inlineMaxBytes=4096`），**覆盖 `read`**，并对 offload 路径上的再次 `read` 做原地截断（不写第二个文件），打断 read→offload→read 环。

## 状态

- `0.1.0-dev`，开发树：`/workspace/dsh-plugins/dsh-eager-offload/`（box 上 trial profile 软链安装的就是它；本目录自 2026-10-08 起与其同步入库）
- 试验 profile：`acp-lite-offload`（从 `acp-lite` 克隆；**未**改 `acp` / `acp-lite`）
- **未**装进产品 broker / 产品四票
- `ageMask` 已废弃（no-op）；`contextClear` 已实现 **T2（压缩联动清旧 tool_result）**；**T3（折叠超大 write/edit 步骤）待做**

## Hook API（实测）

与 `@deepseek-ai/dsh-spill-policy` 相同：

```js
ctx.on('tools/post-execute', async (exec, result, next) => {
  const decision = await next()
  // decision.kind === 'accept' 且无 value 替换、无 exec.parent 时
  // 可返回 { kind:'accept', content:[{type:'text', text }] }
}, { prepend: true })
```

要点：

| 字段 | 用途 |
|------|------|
| `exec.name` | 工具名（含 `read` / `bash` …） |
| `exec.arguments` | 解析后参数（`read` 用 `file_path`） |
| `exec.callId` | 写入 notice |
| `exec.agent.session.header.id` | session 作用域目录 |
| `exec.parent` | 有则跳过（嵌套 PTC sub-call） |
| `decision.content` / `result.content` | plain-text blocks；非 text 不碰 |

只导出 **named** `apply` / `inject` / `name`（不要 `export default apply`，会弄坏 Cordis inject）。

## 配置默认

| key | 默认 | 说明 |
|-----|------|------|
| `offloadRoot` | `offload` | 相对 `$DSH_HOME` → `~/.dsh/offload` |
| `inlineMaxBytes` | `4096` | 超限则整段落盘 + 预览 |
| `previewHeadBytes` | `1536` | 预览头 |
| `previewTailBytes` | `1024` | 预览尾 |
| `offloadReadMaxInlineBytes` | `16384` | `read` 目标已在 offload 根下：允许更大内联；再超则**原地截断** |
| `ageMaskEnabled` | `false` | **DEPRECATED（no-op）**。仅为兼容旧 profile 而接受；产品 profile 保持 `false`。见下节 |
| `ageMaskKeepRecentN` | `8` | **DEPRECATED（no-op）**。仅为兼容旧 profile 而接受，不再生效 |
| `contextClear.*` | 关闭 | 压缩联动的旧工具结果清理（T2 已实现；T3 折叠 write/edit 待做）；见下节 |
| `excludeTools` | `[]` | 完全跳过的工具名 |
| `toolOverrides.<name>.inlineMaxBytes` | — | 单工具覆盖 |

## ageMask（已废弃）

> ⚠️ **已知无效（2026-10-08 实测）**：ageMask（`agent/pre-step` 路径）在 dsh 0.1.5-rc.2 及 0.2.x（核对至 0.2.1-alpha.1）上**不生效**——该钩子的 `messages` 只是本步从 inbox 取出的新 user 消息（返回值会被追加进会话），拿不到完整历史，发给模型的请求只由 `session.deriveMessages()` 从持久会话派生。

因此插件**不再注册** `agent/pre-step` 监听：`ageMaskEnabled` / `ageMaskKeepRecentN` 仍被接受并校验（旧 profile 照常加载），但 `ageMaskEnabled: true` 只在 mount 时 `logger.warn` 一次。`maskOldToolResults`、`formatAgeMaskPlaceholder` 等导出与其单测保留，仅供参考，不再被调用。替代方案见下节。

## contextClear（压缩联动清旧；T2 已实现，T3 待做）

目的：处理体积 offload 管不到的「很多条都不大、但堆满压缩保留尾部」的旧 tool_result。

- **仅压缩联动**：只在 `compaction-basic` 刚做完摘要压缩的那个 pre-step 里动作，经**持久** `surfaceOp replace` 清理压缩保留尾部中的旧工具结果（不是每步重算，也不改易失 `messages`）。
- **默认关**：`contextClear.enabled` 默认 `false`；不配置即完全关闭。关闭时**完全不注册** `agent/pre-step`。
- **产品 `acp` / `acp-lite` 不配置 `contextClear`，即默认关闭**；需要时用试验 profile 显式开启。
- **T2（已实现，本分支）**：清旧 tool_result。开启且 `mode==='compaction-coupled'` 时注册 `{prepend:true}` 的 `agent/pre-step` 监听；`await next()` 返回且 `decision.kind==='enter'` 后运行 `clearAfterCompaction`，随后原样返回 decision。
- **T3（待做）**：`collapseWriteSteps` 折叠超大 write/edit 步骤；目前键被接受/校验但行为尚未落地。

T2 触发与清理规则（实现见 `lib/context-clear.js`）：

1. 从**持久日志**找最新一条 `compaction/summary`；仅当其后（seq 更大）**没有** `step/start`，且其紧邻的 checkpoint `user/message`（`source = {kind:'plugin', plugin:'compact'}`）仍在 surface 上，才动手。这一步重启安全、无需内存状态；溢出重试/手动压缩发生在 pre-step 之外，**不在本票范围**。
2. 候选 = checkpoint 之后的 surface `tool/result`；排除 surface 上最新 `keepRecentResults` 条、`isError`、非纯文本、UTF-8 字节 < `minResultBytes`、以及已带 `[dsh-eager-offload:context-cleared]` 标记的结果。
3. 每条按官方 pruner 的方式追加 `compaction/prune`（经 `tokenMeter.estimateMessage` 计价）+ `tool/result`（`surfaceOp:{op:'replace'}`, `sourceEventSeqs:[seq]`）；只改 `content`，遵守 dsh-session 的 tool/result 替换约束。候选列表来自追加前的 surface 快照。
4. 占位符首行固定为 `[dsh-eager-offload:context-cleared] <toolName> result (<bytes> bytes) cleared after compaction; full text at <path> — re-read it with the read tool (use offset/limit for large files).`，随后是 head/tail 预览；文本以非零 `[exit code: N]` 结尾时用 `failTailBytes` 作尾部。无时间戳/随机数，且必须严格小于原文，否则跳过。
5. 路径优先复用文本中已有的 offload 路径（`offloadedPathFromText`）；否则按 callId 确定性命名落盘到会话 offload 目录；落盘失败则跳过该条（不清）。
6. `tokenMeter` 缺失时 mount 后只 `logger.warn` 一次并跳过；任何异常都被监听器吞掉并 warn，绝不影响该 step。

**重读与缓存**：占位符只留 head/tail + 路径，模型需要全文时用 `read` 工具（大文件带 offset/limit）重读该路径。每次压缩本身已重建 surface（`replaceGeneration` 变化）→ 请求前缀缓存本就已重置一次；T2 的省略不会引入额外的缓存失效点。

| key | 默认 | 说明 |
|-----|------|------|
| `contextClear.enabled` | `false` | 总开关 |
| `contextClear.mode` | `compaction-coupled` | 仅允许 `'off'` \| `'compaction-coupled'`；其他值 mount 报错 |
| `contextClear.keepRecentResults` | `8` | 压缩保留尾部中最近 N 条结果保持全文；更早的才可能被清 |
| `contextClear.minResultBytes` | `1200` | 小于此 UTF-8 字节数的结果不清 |
| `contextClear.placeholder.headBytes` | `160` | 占位符头部字节 |
| `contextClear.placeholder.tailBytes` | `240` | 占位符尾部字节 |
| `contextClear.placeholder.failTailBytes` | `600` | 命令失败（末尾非零 `[exit code: N]`）时的尾部字节 |
| `contextClear.collapseWriteSteps.enabled` | `true` | （T3 待做）是否同时折叠超大的 write/edit 步骤 |
| `contextClear.collapseWriteSteps.tools` | `['write','edit']` | （T3 待做）参与折叠的工具名 |
| `contextClear.collapseWriteSteps.minArgChars` | `1200` | （T3 待做）参数小于此字符数不折叠 |

未知键（任意层级）一律在 mount 时抛错；返回的 `contextClear` 对象深冻结。

`mounted` 日志形如：`contextClear=off` 或 `contextClear=on(compaction-coupled,keep=8)`；清理后在触发的 pre-step 记 `context-clear: cleared=N bytes A->B`。

## 自测（无模型）

```bash
cd /workspace/dsh-plugins/dsh-eager-offload
npm run check && npm test
```

## 装进试验 profile

见父代理报告；装进正式 `acp` / `acp-lite` 的命令**只列不跑**。

## 文件

| 路径 | 说明 |
|------|------|
| `lib/index.js` | Cordis `apply`：挂 `tools/post-execute`；`contextClear` 开启时另挂 `{prepend:true}` 的 `agent/pre-step` |
| `lib/context-clear.js` | T2 压缩联动清理：触发守卫、候选过滤、占位符、`compaction/prune` + `tool/result` 替换 |
| `lib/offload.js` | 纯逻辑：预览、落盘、loop 防护、`contextClear` 配置校验（T2 复用） |
| `cordis.patch.yml` | bundle insert |
| `schemas/config.schema.json` | 配置 schema |
| `examples/config.example.yml` | 示例 patch 片段 |
| `test/offload.test.mjs` | 单元测试 |
| `test/context-clear.test.mjs` | T2 集成测试（真实 `Session` / `TokenMeter`；不可解析时 `test.skip`） |

## 与 spill 叠乘

- spill-policy 仍可先把超 12KiB 的非-`read` 结果换成 spill 预览
- 本插件在 `next()` 之后看到最终投影：若仍 > `inlineMaxBytes`（含 spill 预览、或 spill 跳过的 `read`），再 eager offload
- 替换结果始终 ≤ cap → 单次 post-execute 不会自激振荡
