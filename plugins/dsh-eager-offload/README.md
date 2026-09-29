# dsh-eager-offload

**Eager** tool-output offload for dsh：在 compaction 压力之前就把过大的 plain-text tool_result 外置到磁盘，模型侧只留 head/tail 预览 + 路径 + 字节数。

- 官方 `dsh-spill` / spill-policy（箱上 `maxInlineBytes=12288`）**跳过 `read`**
- `dsh-compaction-tool-result-pruner` **只在压力后**动手
- ACP 每步重送全史 → tool_result 字节会乘进后续每一步

本插件挂 `tools/post-execute`（与 spill-policy 同一 waterfall），默认阈值更低（`inlineMaxBytes=4096`），**覆盖 `read`**，并对 offload 路径上的再次 `read` 做原地截断（不写第二个文件），打断 read→offload→read 环。

## 状态

- `0.1.0-dev`，开发树：`/workspace/dsh-plugins/dsh-eager-offload/`
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
| `excludeTools` | `[]` | 完全跳过的工具名 |
| `toolOverrides.<name>.inlineMaxBytes` | — | 单工具覆盖 |

落盘路径：`$DSH_HOME/offload/<sha256(sessionId)[0:12]>/<id>-<tool>.txt`（目录 `0700`，文件 `0600`）。

Notice 含标记 `dsh-eager-offload:`，提示模型用 `read` + `offset`/`limit` 按需取回。

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
| `lib/index.js` | Cordis `apply`：挂 `tools/post-execute` |
| `lib/offload.js` | 纯逻辑：预览、落盘、loop 防护 |
| `cordis.patch.yml` | bundle insert |
| `schemas/config.schema.json` | 配置 schema |
| `examples/config.example.yml` | 示例 patch 片段 |
| `test/offload.test.mjs` | 单元测试 |

## 与 spill 叠乘

- spill-policy 仍可先把超 12KiB 的非-`read` 结果换成 spill 预览
- 本插件在 `next()` 之后看到最终投影：若仍 > `inlineMaxBytes`（含 spill 预览、或 spill 跳过的 `read`），再 eager offload
- 替换结果始终 ≤ cap → 单次 post-execute 不会自激振荡
