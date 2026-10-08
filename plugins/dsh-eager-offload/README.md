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
| `ageMaskEnabled` | `false` | 开启「按年龄清旧 tool_result」（见下节；**当前 dsh 版本上无效**）。产品 profile 保持 `false` |
| `ageMaskKeepRecentN` | `8` | 保留最近 N 条 tool_result 全文；更早的换占位符 |
| `excludeTools` | `[]` | 完全跳过的工具名 |
| `toolOverrides.<name>.inlineMaxBytes` | — | 单工具覆盖 |

落盘路径：`$DSH_HOME/offload/<sha256(sessionId)[0:12]>/<id>-<tool>.txt`（目录 `0700`，文件 `0600`）。

Notice 含标记 `dsh-eager-offload:`，提示模型用 `read` + `offset`/`limit` 按需取回。

## 按年龄清旧 tool_result（`ageMaskEnabled`）

> ⚠️ **已知无效（2026-10-08 实测）**：ageMask（`agent/pre-step` 路径）在 dsh 0.1.5-rc.2 及 0.2.x（核对至 0.2.1-alpha.1）上**不生效**——该钩子的 `messages` 只是本步从 inbox 取出的新 user 消息（返回值会被追加进会话），拿不到完整历史，发给模型的请求只由 `session.deriveMessages()` 从持久会话派生。开着也是 no-op（masked=0）；待 `contextClear`（压缩联动、经 surfaceOp replace 持久改写）替代。下文为原设计说明，仅供参考。

体积 offload 之外的**第二个、独立**杠杆：体积 offload 只管「单条太大」，管不到「很多条都不大但堆满历史」。开启后每条预发送历史里只保留**最近 N 条** tool_result 全文，更早的替换成一行占位符。

**选钩理由**：`tools/post-execute` 每次只看到**当次新增的一条**结果，天然无法判断「第几条 / 有多旧」；`agent/pre-step` 是每次模型请求前、唯一拿到**完整 `messages` 数组**的 waterfall 组合点（`dsh-compaction-basic`、`dsh-repeat-tool-reminder` 同用此钩）。ACP 每步重送全史，所以在这里裁剪即可压住后续每一步。

```js
ctx.on('agent/pre-step', async ({ agent, messages }, next) => {
  const decision = await next()            // { kind:'enter', messages }
  return { ...decision, messages: masked } // 只改本请求，不动 durable session
}, { prepend: true })
```

行为要点：

- **只改易失的 `messages`**，不写 durable session surface（与官方 `dsh-compaction-tool-result-pruner` 的 surfaceOp 重写不同）→ 重放 / UI 仍是全文
- message 深冻结 → 一律**重建新对象**，绝不原地 mutate
- 已有 offload 路径（文本含 `dsh-eager-offload:` + `path=`）的旧条目**复用该路径**，绝不二次落盘
- 没有路径的旧条目**惰性落盘一次**再由占位符引用；落盘失败降级为纯占位符，`try/catch` 不阻断本步
- `isError` 结果**不清**（保留诊断）；含非 text 块（图片/文件）的条目跳过
- 与体积 offload **互不干扰**：产品阈值 `inlineMaxBytes` / 预览头尾 / `offloadReadMaxInlineBytes` 原地截断语义完全不变

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
| `lib/index.js` | Cordis `apply`：挂 `tools/post-execute` + `agent/pre-step` |
| `lib/offload.js` | 纯逻辑：预览、落盘、loop 防护、按年龄清旧 |
| `cordis.patch.yml` | bundle insert |
| `schemas/config.schema.json` | 配置 schema |
| `examples/config.example.yml` | 示例 patch 片段 |
| `test/offload.test.mjs` | 单元测试 |

## 与 spill 叠乘

- spill-policy 仍可先把超 12KiB 的非-`read` 结果换成 spill 预览
- 本插件在 `next()` 之后看到最终投影：若仍 > `inlineMaxBytes`（含 spill 预览、或 spill 跳过的 `read`），再 eager offload
- 替换结果始终 ≤ cap → 单次 post-execute 不会自激振荡
