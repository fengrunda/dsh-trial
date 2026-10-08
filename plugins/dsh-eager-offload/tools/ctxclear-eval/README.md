# ctxclear-eval — contextClear 离线重放评估

纯 Node（>=22，用 `zlib.zstdDecompressSync`）、ESM、零第三方依赖、只读会话日志：把
`contextClear`（压缩联动的 T2/T3 清理）放到真实 dsh 会话历史上重放，和「不清理」基线
以及「实际日志」对比 token 与费用。

原型：`/tmp/tokaudit/agemask-sim/`（A/B/C 等其它策略、推理清空、重读敏感性仍在原型里，未入库）。
本工具只保留 BASE（不清 + T/R 压缩）与 contextClear 两条策略。

## 运行

```bash
# 默认会话源：/home/box/.dsh-homes/{gate,supervisor,impl}/sessions/**/session.v3.jsonl.zstd
node tools/ctxclear-eval/eval.mjs \
  --date 2026-10-08 --window 09:00-10:00 --window 14:00-15:00 --tz +08:00

# 其它输入
node tools/ctxclear-eval/eval.mjs --sessions-glob '/path/**/session.v3.jsonl.zstd'
node tools/ctxclear-eval/eval.mjs --files a/session.v3.jsonl.zstd,b/session.v3.jsonl.zstd
node tools/ctxclear-eval/eval.mjs --since 2026-10-08T01:00:00Z --until 2026-10-08T02:00:00Z
node tools/ctxclear-eval/eval.mjs --threshold 60000 --retain 20000 --keep 8 --min-result-bytes 1200 --no-collapse --json
```

选项：`--sessions-glob`（可重复）、`--files`、`--since/--until`、`--window`（可重复）+
`--date` + `--tz`、`--threshold`、`--retain`、`--keep`、`--min-result-bytes`、
`--no-collapse`、`--prices`、`--json`。不带任何窗口参数则统计所有会话的所有请求。

## 模型与假设（沿用原型）

- **会话筛选**：只把 `provider=deepseek-official` 且未中断的 assistant 步当作请求；整会话重放，
  只把落在窗口内的请求计入合计。窗口内 951 次请求来自 22 个会话 / 扫描 679 个文件。
- **token 归属**：每步 prompt 增量 `ΔP = P(t+1) − P(t)` 为日志真实值，拆成上一步 assistant
  回放（≈ `outputTokens`，含推理）与本次新增 tool_result / user 消息（按字符比例）。`sys+tools`
  ≈ `(system+tools 字符)/4.16`。
- **前缀缓存**：每次请求在输入末尾与输出末尾各持久化一个前缀单元；改动第 i 条使结束位置 > i
  的单元失效；命中按 128 token 向下取整；压缩后仅 `sys+tools` 命中；首次请求用实际命中值。
  校准：不清不压缩的模拟未命中 0.630M vs 实际 0.655M，费用 $1.012 vs 实际 $1.019。
- **压缩（compaction-basic 语义）**：pre-step 测量 ≥ T → 内置 pruner（>4096 字符 → 头 2048/尾 512）
  → 仍超则摘要：保留尾部 ≥ R 且从 assistant 边界切，摘要调用 = 缓存前缀 + 553 miss，
  输出 = `clamp(0.102×被摘要区, 1500, 8192)`。压缩后只有 `sys+tools` 命中。
- **价格**：deepseek-flash 官方。高峰 = UTC 周一至周五 01:00–04:00 与 06:00–10:00
  （中国法定节假日忽略，不查节假日表），高峰 $/M：命中 0.006 / 未命中 0.30 / 输出 1.20；
  低峰全部减半。每个请求按自身时间戳判峰/谷。`--prices` 可覆盖。

## contextClear（T2/T3）语义

只在**刚做完摘要**的那个 pre-step、只对**摘要之后的保留尾部**执行，不改任何更早前缀。

- **T2**：尾部里除最新 `keep` 条 tool_result 外，plain-text、非 `isError`、
  字节数 ≥ `min-result-bytes` 的结果换成占位符（≈ 头 160B + 尾 240B + 路径行；
  失败类（`[exit code: 非0]` / `FAIL|Error|Traceback|error:|failed`）尾 600B）。
  占位符 token 按字节/4 估：普通 ≈ 130 tok，失败类 ≈ 220 tok。
- **T3**：尾部里「全部工具调用都是 write/edit 且至少一个参数 ≥ 1200 字符」、
  且其**所有**结果都早于最新 `keep` 条的 assistant 步，整步（assistant 的推理/文本/参数 +
  它的结果）折叠为一条 ≈ 80 token 的 user 说明。混合工具（如 bash+write）的步不折叠。
  另可加 `--clear-reasoning`（配合 `--min-reasoning-chars`，默认 600）开启 (b)：
  不限于 write/edit，任意工具步只要 assistant 推理 ≥ 该下限也整步折叠；默认关闭。
- 「清理条数」= T2 替换的结果数 + T3 折叠的步数（表内括号给出 `T2=`、`T3=` 拆分）。

## 真实数据结果

2026-10-08，窗口 `09:00-10:00` 与 `14:00-15:00`（`+08:00`），22 个会话 / 951 次窗口内请求，
T=60k / R=20k / keep=8 / min-result-bytes=1200，T3 开启。Δ 相对 BASE。

| 策略 | prompt合计(M) | 未命中(M) | 输出(M) | 总token(M) | 费用$ 合计 (分窗口) | 压缩次数 | 清理条数 | Δ总token | Δ费用 |
|---|---|---|---|---|---|---|---|---|---|
| 实际日志 | 41.43 | 0.66 | 0.48 | 41.91 | 1.019 (0.363 / 0.656) | 0 | 0 | +22.9% | -7.5% |
| 模拟:不清+不压缩(校准) | 41.43 | 0.63 | 0.48 | 41.91 | 1.012 (0.361 / 0.651) | 0 | 0 | +22.9% | -8.1% |
| BASE 不清+60k/20k压缩 | 33.59 | 0.95 | 0.52 | 34.11 | 1.101 (0.395 / 0.706) | 11 | 0 | +0.0% | +0.0% |
| contextClear(压缩联动) | 33.04 | 0.87 | 0.51 | 33.56 | 1.070 (0.388 / 0.682) | 10 | 72 (T2=52, T3=20) | -1.6% | -2.9% |
| contextClear + clearReasoning | 32.61 | 0.83 | 0.51 | 33.13 | 1.056 (0.384 / 0.672) | 10 | 88 (T2=36, T3=52) | -2.9% | -4.1% |

校准行与 BASE 行和原型完全一致（未命中 0.63M、费用 $1.012；BASE 34.11M / $1.101 / 11 次压缩），
说明移植正确。

**与原型偏差**：原型 D1(尾清 K8)+write/edit 参数占位为总 token −2.0% / 费用 −4.0%；本工具
contextClear 为 −1.6% / −2.9%，即清理更少（72 vs 96 条）。原因是严格按 T2/T3 语义：

1. T2 要求字节数 ≥ `min-result-bytes`(1200)，原型尾清只要求 token 大于占位符（≈110），
   门槛更高、可清条目更少（这是主因）；
2. T3 要求整步工具调用**全部**是 write/edit，原型「只替参数」对任意步内的 write/edit 参数都生效，
   混合步也覆盖；
3. T3 单步删得更多（连 assistant 推理/文本/参数一起折叠），但只作用于「结果全早于最新 keep 条」
   的 write-only 步，覆盖面更窄。

净效果是清理量下降、未命中降幅变小，于是费用改善落在原型「纯尾清」( −0.9%) 与「尾清+参数」
(−4.0%) 之间，方向一致。另：contextClear 少触发 1 次压缩（10 vs 11）。

## 测试

```bash
node --test tools/ctxclear-eval/test/*.test.mjs   # 或 npm test（同时跑插件测试与这里的测试）
```

覆盖：缓存 128 取整与前缀单元失效、高峰/低峰判定（含 UTC 边界）、T2/T3 只改摘要之后的条目
（keep 保护、混合工具不折叠、失败类占位符更大）、以及无压缩会话上 BASE 与 contextClear 结果完全相同。
