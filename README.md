# dsh-trial

**短票 trial broker** for DeepSeek Harness（dsh）ACP：mailbox + role-bridge，**不是**常驻多会话 sticky broker。

> Public scaffold for reusing the box trial stack. 箱上默认路径请用 `$DSH_HOME` / `$DSH_TRIAL_ROOT` 覆盖（见 [Known box paths](#known-box-paths)）。

## 与 `dsh-acp-broker` 的差异

| | **本仓 `dsh-trial`** | [`dsh-acp-broker`](https://github.com/fengrunda/dsh-acp-broker) |
|--|---------------------|----------------------------------------------------------------|
| 形态 | 薄短票 trial broker：inbox JSON → `open-slice.sh` → outbox | 常驻多会话 ACP broker（命名 session 保活） |
| 会话 | **非 sticky**：每票 `--final` 一次 ACP；靠 thin-state / mailbox 文件桥接 | 进程内保持多个 named session |
| 通信 | `dsh-role-bridge` 写 mailbox；本 broker poll / 路由唤醒 | 可选 `dsh.bundle` 工具/技能；产品向 |
| 隔离 | 独立 `$DSH_HOME/broker-dsh-trial`；**绝不**碰 `broker-khub-prod` | 产品侧常驻编排 |
| 典型用途 | 试用 / Goal·chain·gate 烟雾 / 插件联调 | 长期会话与工具面 |

**有 broker吗？有。** 人的入口不是它。Mac / 人工只走 `dsh web` + desk 插件；broker 是后台 daemon（[`broker/trial-broker.py`](broker/trial-broker.py)，`dsh-trial start` 包装 `start.sh` / `stop.sh`）。**desk ≠ broker**；web 只是 UX。短票仍 `--final`。无房间。**永不**碰 `broker-khub-prod`。

## 仓库内容

```
broker/           trial-broker.py · trial_lib.py · trial_mailbox.py · start/stop · tests · examples
open-slice.sh     拼 prompt → dsh-acp-ask.py --final
assert-no-room-inject.py
bin/dsh-trial     start / stop / broker-status / status|goals / report / limits
bin/dsh-trial-pr  受控 push + gh pr（allowlist 可配）
plugins/          dsh-trial-desk · dsh-role-bridge · dsh-eager-offload（无 node_modules）
examples/         limits.json · routes.json 模板（无租户密钥）
docs/             runbook / reuse-publish / token-opt / gh-pr 笔记
```

## 依赖

- Python 3.10+
- dsh CLI + `dsh-acp-ask.py`（通常在 `$DSH_HOME/bin`）
- 可选：`gh`（仅 `dsh-trial-pr`）
- 插件：Node `^22.19 || >=24`，peer `@deepseek-ai/cordis`

## 安装概要

```bash
git clone https://github.com/fengrunda/dsh-trial.git
cd dsh-trial
export DSH_HOME="${DSH_HOME:-$HOME/.dsh}"
export DSH_TRIAL_ROOT="$(pwd)"   # 供 bin/dsh-trial 找到 broker/

# 配置模板
mkdir -p "$DSH_HOME/supervisor/trial" "$DSH_HOME/supervisor/thin-state"/{packs,summaries,mailbox,chains,goals,metrics}
cp examples/limits.json examples/routes.json "$DSH_HOME/supervisor/trial/"

# CLI（可选）
ln -sf "$(pwd)/bin/dsh-trial" "$DSH_HOME/bin/dsh-trial"
ln -sf "$(pwd)/bin/dsh-trial-pr" "$DSH_HOME/bin/dsh-trial-pr"

# 人工 UX：desk → web-facing profile（dsh web 的唯一入口；不是 broker）
dsh plugin --profile web add -w "$(pwd)/plugins/dsh-trial-desk"
# 若 `dsh web` 用的是 acp-lite：dsh plugin --profile acp-lite add -w "$(pwd)/plugins/dsh-trial-desk"

# 短票 trial profile（工头/监理票中桥；勿装进产品 acp / acp-lite 本体）
dsh plugin --profile acp-lite-trial add -w "$(pwd)/plugins/dsh-role-bridge"
dsh plugin --profile acp-lite-trial add -w "$(pwd)/plugins/dsh-eager-offload"
```

环境变量由 `$DSH_HOME/load-env.sh` 或进程环境注入（**不要**把 API key / `GH_TOKEN` 写进仓或打进日志）。

## Mac / 人工路径（web 是唯一入口）

desk 只写 inbox、只读 thin-state；**不会**自己开短票。broker 必须作为后台 daemon 先起来。短票 `--final` 行为不变。无房间。

```bash
# 1) desk → 给人用的 web profile（结构上：dsh plugin add -w …/plugins/dsh-trial-desk）
dsh plugin --profile web add -w "$(pwd)/plugins/dsh-trial-desk"

# 2) 后台 daemon（包装 broker/start.sh；绝不碰 broker-khub-prod）
dsh-trial start
# 等价：./broker/start.sh
# 单次：dsh-trial start --once

# 3) 唯一人工 UX
dsh web
# 会话工具：trial_drop_job / trial_validate_job / trial_chain_reply / trial_status
# Job JSON 与 broker/examples/job-goal-*.json、job-chain-*.json、job-chain-reply.json 相同

# 4) 观察
dsh-trial broker-status    # pid / inbox / chains（trial-broker.py --status）
dsh-trial status           # goals 列表（兼容旧调用）
dsh-trial goals            # status 的别名
dsh-trial stop             # 包装 broker/stop.sh
```

不要用 CLI 当第二个人工投放面（可以继续把 JSON 拷进 inbox，但推荐只走 desk）。**不要**把 desk 装进 impl/gate 短票 profile 当 broker 用。

## 启动（daemon）

```bash
dsh-trial start              # 常驻 poll（默认 20s；TRIAL_BROKER_POLL 可改）
dsh-trial start --once       # 单次消费 inbox
dsh-trial stop
dsh-trial broker-status
dsh-trial status             # goals
dsh-trial limits show
```

底层仍是 `./broker/start.sh` / `./broker/stop.sh` / `python3 ./broker/trial-broker.py --status`。  
Job 形态：单票 / `chain` / `chain-reply` / `goal`（详见 `broker/README.md` 与 `broker/trial-broker.py` 文档字符串）。

## 与插件的关系

```
人 ── dsh web + dsh-trial-desk ──inbox──▶ trial-broker daemon ──open-slice --final──▶ ACP 短票
                                         │
                                         ├─ poll thin-state/mailbox ◀── dsh-role-bridge
                                         └─ routes.json 决定 (to_role, kind) → handler

dsh-eager-offload：tools/post-execute 提前 spill 大工具结果（可与产品 profile 共用；role-bridge 仅 trial）。
```

- **`dsh-trial-desk`**：人/监理会话；只写 inbox、只读状态。**不是** broker。
- **`dsh-role-bridge`**：票中发往 supervisor/gate；**必须**有本 broker 的 mailbox watcher + `routes.json`。
- **`dsh-eager-offload`**：相对独立；默认 root `$DSH_HOME/offload`。
- 更完整说明：[`plugins/README.md`](plugins/README.md)。

## `dsh-trial-pr` allowlist

默认 **空**。使用前设置：

```bash
export DSH_TRIAL_PR_ALLOWLIST="owner/repo"
# 可选默认仓
export DSH_TRIAL_PR_DEFAULT_REPO="owner/repo"
dsh-trial-pr push-and-pr --repo owner/repo --title "…"
```

也可编辑 `bin/dsh-trial-pr` 里的 `ALLOWLIST=(...)`（示例注释保留 `fengrunda/knowledge-hub`，**不是**唯一硬绑）。

禁止：force-push、`main`/`master` 源分支、merge。

## Known box paths

下列在箱上曾硬编码；本仓已尽量改为 `$DSH_HOME` / 相对路径。仍可能作为 **legacy fallback** 或文档历史出现：

| 路径 / 变量 | 说明 |
|-------------|------|
| `/workspace/dsh-trial/broker` | `bin/dsh-trial` 的第三候选 `sys.path`（优先 `DSH_TRIAL_ROOT` 与仓内 `../broker`） |
| `/workspace/dsh-plugins/…` | 箱上插件开发树；发布副本在本仓 `plugins/` |
| `$HOME/.dsh`（箱上即 `/home/box/.dsh`） | 默认 `DSH_HOME` |
| `$HOME/.dsh-homes/<role>` | 角色家；可用 `DSH_HOMES_ROOT` |
| `$DSH_HOME/broker-khub-prod` | **只读探测共存**；本 broker 永不启停产品 |
| `$TMPDIR/trial-broker` | 产物根；可用 `TRIAL_ARTIFACT_ROOT` |
| `docs/*.md` 内绝对路径 | 历史 runbook；文首有路径说明 |

推荐覆盖：`DSH_HOME`、`DSH_TRIAL_ROOT`、`DSH_TRIAL_OPEN_SLICE`、`DSH_TRIAL_ASSERT`、`DSH_TRIAL_MAILBOX`、`DSH_TRIAL_THIN_STATE`、`DSH_GLOBAL_HOME`、`DSH_HOMES_ROOT`、`TRIAL_BROKER_DIR`、`TRIAL_ARTIFACT_ROOT`、`DSH_ACP_ASK`。

## 文档

- [broker/README.md](broker/README.md) — broker 脚手架
- [docs/2026-09-24-dsh-trial-runbook.md](docs/2026-09-24-dsh-trial-runbook.md)
- [docs/2026-09-29-dsh-trial-reuse-publish.md](docs/2026-09-29-dsh-trial-reuse-publish.md)
- [docs/2026-09-29-dsh-trial-token-opt.md](docs/2026-09-29-dsh-trial-token-opt.md)
- [docs/2026-09-29-dsh-trial-gh-pr.md](docs/2026-09-29-dsh-trial-gh-pr.md)

## License

MIT（与 [`dsh-acp-broker`](https://github.com/fengrunda/dsh-acp-broker) 一致）。
