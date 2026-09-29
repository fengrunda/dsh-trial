# dsh-trial-desk（人工入口 · 只写 inbox / 只读状态）

- 状态：`0.1.0-dev`
- 装进 **web-facing** profile（`web` 或你用来跑 `dsh web` 的 `acp-lite`）
- **desk ≠ broker**：本插件只把 Goal / chain / chain-reply JSON 丢进 `$DSH_HOME/supervisor/trial/inbox/`，并读 thin-state + broker pid。**从不** `open-slice`、从不进房、从不调 Hub webhook、**永不**碰 `broker-khub-prod`。

短票仍由 trial broker 以 `open-slice.sh --final` 执行（ACP 一票一进程，非 sticky）。

## 1. 人怎么用

```
装 desk ──▶ dsh-trial start（后台 daemon）──▶ dsh web（唯一人工 UX）──▶ desk 工具
                    │
                    └─ poll inbox → open-slice --final → thin-state
```

1. `dsh plugin --profile web add -w <checkout>/plugins/dsh-trial-desk`
2. `dsh-trial start`（包装 `broker/start.sh`；不是 desk 自己起的编排）
3. `dsh web` 会话里调用下面的工具
4. `dsh-trial stop` 停 daemon；`dsh-trial broker-status` 看 pid/inbox；`dsh-trial status` 仍是 goal 列表

## 2. 工具

| 工具 | 作用 |
|------|------|
| `trial_drop_job` | 把 Goal / chain（或 ticket / goal-update）JSON 写入 inbox。`dry_run=true` 只校验不落盘 |
| `trial_validate_job` | 同上校验（不写）。规则对齐 `broker/trial-broker.py` `load_job` |
| `trial_chain_reply` | 写 `type: chain-reply`（`slice` + `answer`），形状同 `broker/examples/job-chain-reply.json` |
| `trial_status` | 只读：broker 是否活着、inbox pending、recent chains / summaries / mailbox pending / goals |

Job 形状 **不要自造**：抄 `broker/examples/job-goal-*.json`、`job-chain-*.json`。`cwd` 由人填工作树路径；desk **不会**默认 `/workspace`。

含 `room_*` / `join_room` 等键的 JSON 会被拒绝。

## 3. 路径（`$DSH_HOME` / env，无箱绝对路径默认）

| 用途 | 默认 |
|------|------|
| inbox | `$DSH_TRIAL_INBOX` 或 `$DSH_HOME/supervisor/trial/inbox` |
| thin-state | `$DSH_TRIAL_THIN_STATE` 或 `$DSH_HOME/supervisor/thin-state` |
| mailbox | `$DSH_TRIAL_MAILBOX` 或 `<thin-state>/mailbox` |
| broker pid | `$TRIAL_BROKER_DIR` 或 `$DSH_HOME/broker-dsh-trial` |
| `$DSH_HOME` | 配置 `dshHome` → `DSH_HOME` → `DSH_GLOBAL_HOME` → `~/.dsh` |

配置见 `cordis.patch.yml` / `schemas/config.schema.json`。无密钥。

## 4. 安装

```bash
# 建议：dsh web 用的 profile（web 或 acp-lite）
dsh plugin --profile web add -w "$(pwd)/plugins/dsh-trial-desk"
# 等价：dsh plugin add -w …/plugins/dsh-trial-desk   （当前默认 profile）

dsh --profile web --dump-config | grep -A8 trial-desk
```

**不要**把 desk 当成 broker 的替代；impl/gate 短票 profile（`acp-lite-trial`）继续只挂 `dsh-role-bridge`。desk 给人/监理会话用。

## 5. 自测（无模型）

```bash
cd plugins/dsh-trial-desk
npm run check && npm test
```

## 文件

| 路径 | 说明 |
|------|------|
| `lib/index.js` | Cordis `apply`：注册四个工具 |
| `lib/desk.js` | 校验、inbox 写入、status 读取 |
| `cordis.patch.yml` | bundle insert |
| `test/desk.test.mjs` | 单元测试（含 broker examples 形状） |
