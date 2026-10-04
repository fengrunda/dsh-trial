# broker-dsh-trial — 隔离试用常驻薄 broker

日期：2026-09-24（Asia/Shanghai）  
设计：P1 方案 **C**（执行票不进房 + 文件桥 / design-pack / 中票）  
版本注记：支持 **slice chain**（supervisor → foreman → gate 全文件通信）

## 与产品 broker（`broker-khub-prod`）的区别

| | **broker-dsh-trial（本目录）** | **broker-khub-prod** |
|--|-------------------------------|----------------------|
| 进程 | 薄 Python 轮询器（本仓库 `trial-broker.py`） | `dsh-acp-broker` 常驻 + **四票同进程** |
| 派活 | `supervisor/trial/inbox` → `open-slice.sh --final` | 房间 `khub-dev` + supervisor inbox + live followup |
| 执行票 | **禁止 join 任何房间**；一 slice 一中票 / chain 多中票 | 工头/监理常驻，房间 sticky |
| 状态目录 | `~/.dsh/broker-dsh-trial` + `~/.dsh/supervisor/trial/` | `~/.dsh/broker-khub-prod` + `~/.dsh/supervisor/` |
| 启停 | 本目录 `start.sh` / `stop.sh` | `khub-broker-restart.sh` / `dsh-acp-broker` |

**三句话差异：**

1. **本试用 broker 从不 resume / 启动产品四票**，也不调用 `khub-broker-restart.sh`。  
2. **派活只走 trial inbox 文件 + open-slice**，执行票上下文无房间 bus 灌入。  
3. **可与产品 broker 共存**（prod sock 在也不停 prod）；启停只动自己的 pidfile。

## 目录

| 路径 | 作用 |
|------|------|
| `./` | 本脚手架（脚本 + README + examples） |
| `$DSH_HOME/broker-dsh-trial/` | 运行态：`trial-broker.pid` / `trial-broker.log` |
| `$DSH_HOME/supervisor/trial/inbox/` | 投放任务（单票 / chain / chain-reply） |
| `…/outbox/` | 成功结果 + 原始 job 归档 |
| `…/failed/` | 失败 job |
| `/workspace/tmp/trial-broker/` | composition / assert / 每票 log |
| `~/.dsh/supervisor/thin-state/chains/<slice>.json` | chain 状态机 |

Pack 仍在共享：`~/.dsh/supervisor/thin-state/packs/`（票名用 `impl-trial-*` / `gate-trial-*` 隔离）。  
Pack 字节上限默认 **12288**（与 `dsh-design-pack` maxPackBytes 对齐）；diff 截断默认 6144。

人工 **不要**把本目录当 UX：Mac 路径是装 `plugins/dsh-trial-desk` → `dsh-trial start` → `dsh web`。desk ≠ 本 broker；web 只投 inbox / 读状态。短票仍 `--final`。无房间。**永不**碰 `broker-khub-prod`。

## 启动 / 停止

```bash
source $DSH_HOME/load-env.sh   # Official DeepSeek；勿把 key 打进日志
export DSH_PERMISSION_MODE=danger-full-access

# 推荐包装（与 ./start.sh 相同进程）
dsh-trial start
dsh-trial start --once
dsh-trial broker-status
dsh-trial stop

./start.sh
# 或：TRIAL_BROKER_POLL=30 ./start.sh
./start.sh --once
python3 ./trial-broker.py --status
./stop.sh
```

## 投放任务

### 单票（原行为）

见 `examples/job-baseline.json`：`ticket` / `pack` / `profile` / `cwd` / 可选 `role` / `summary_name` / `notify`。

### Chain（supervisor → foreman → gate）

见 `examples/job-chain-scratch.json`：

```json
{
  "type": "chain",
  "slice": "scratch-hello",
  "pack": "scratch-hello.pack.md",
  "acceptance": ["hello.py exposes greet(name)…", "docstring", "no room_*"],
  "profile": "acp-lite",
  "gate_profile": "acp-lite",
  "cwd": "../scratch-repo",
  "max_rounds": 2,
  "notify": "hub",
  "notify_dry_run": true
}
```

流程：

1. 开 `impl-trial-<slice>-r<N>`（role=impl，prompt-mode=foreman）→ 结构化 summary。  
2. 工头 `done` → broker 写 `packs/<slice>-gate-r<N>.pack.md`（原 pack + acceptance + 工头块 + diff）→ 开 `gate-trial-<slice>-r<N>`。  
3. Gate 写 `summaries/<slice>-gate-r<N>.md`（verdict PASS|HOLD + findings）。  
4. HOLD 且 round < max → 写 fix pack（原 pack + findings）→ 下一轮工头；满轮 HOLD → `escalated`。  
5. 工头 `blocked`/`question` → `awaiting_supervisor`；监理丢 `type: chain-reply`（见 `examples/job-chain-reply.json`）续跑。  
6. 终态写 `thin-state/chains/<slice>.json` + outbox；`notify: hub` 时 kind=`dsh-trial-chain`。

### Chain-reply

```json
{
  "type": "chain-reply",
  "slice": "scratch-hello",
  "answer": "Use def greet(name: str | None = None) -> str; …"
}
```

## 纪律

1. **永不**对 `broker-khub-prod` 跑 restart / resume / prompt。  
2. 执行票 **禁止** `room_*` / join；本 broker 也不做房间 ensure。  
3. 不打印 API key；完成 webhook 仅 job 显式 `notify: hub` 时才调。  
4. 角色间消息 **只**经 bounded pack/summary 文件，不经房间。

## Eager-offload 与 GC

正式 profile `acp` / `acp-lite` 已装 `dsh-eager-offload`（备份见 `~/.dsh/backups/*-pre-offload-*`）。  
落盘：`$DSH_HOME/offload/`。清理：

```bash
python3 ~/.dsh/bin/dsh-offload-gc.py --days 3 --dry-run
python3 ~/.dsh/bin/dsh-offload-gc.py --days 3
```

trial-broker 在**每 job 结束**与**空闲轮询（≥1h）**时调用 GC（`DSH_OFFLOAD_GC_DAYS` 可改默认天数）。

## Mailbox 与超时对齐

- broker 与插件共用同一全局 mailbox：`DSH_TRIAL_MAILBOX` 可覆盖默认 `~/.dsh/supervisor/thin-state/mailbox`（`trial_lib.MAILBOX` 绝对规范化，`trial-broker.py` 直接取 `T.MAILBOX`）。
- 插件侧同步等待（`ask_supervisor` / `submit_for_review`）超时须 ≥ 工具 `timeoutSec`，且 `timeoutSec ≤ ask_supervisor_timeout_sec`（默认 600s）。
- ACP `session/prompt` 是空闲超时加硬上限，不是整段墙钟。`prompt_idle_timeout_sec` / `DSH_ACP_PROMPT_IDLE_TIMEOUT`（默认 900）在 ACP stdout 有进展时重置，且须大于 `ask_supervisor_timeout_sec`（600），避免监理等待被当成挂起。`prompt_timeout_sec` / `DSH_ACP_PROMPT_TIMEOUT`（默认 3600）是硬上限，真卡死不会永远占着 broker。仓库里的 `broker/dsh-acp-ask.py` 实现该语义；broker 用 `DSH_ACP_ASK` 指向它。旧的 limits.json 若只写了 `prompt_timeout_sec=1800` 且没有 idle 键，读取时视为过时墙钟，改用 3600 硬上限。
- impl 票期间 broker 有 watcher 扫同一 `MAILBOX/pending` 并即时回写 `answers/`。

## Gate 硬规则

- 任一 acceptance 未满足，或 findings 含 **P0/P1** ⇒ **HOLD**（即便模型写 PASS，broker `enforce_gate_verdict` 会覆盖）。  
- 仅 P2 ⇒ 允许 PASS。  
- 机器块建议含 `unmet_acceptance` 列表。

## 验收

见 `/workspace/docs/2026-09-24-dsh-trial-acceptance.md`。
