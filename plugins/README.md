# plugins/

本仓库内嵌 box 上实际安装的 dsh 插件源码（不含 `node_modules`）：

| 包 | 路径 | 作用 |
|----|------|------|
| `dsh-trial-desk` | `plugins/dsh-trial-desk` | **人工入口**：Goal / chain / chain-reply 写入 trial inbox；读 broker/thin-state。装进 `web`（或 `dsh web` 用的 `acp-lite`）。desk ≠ broker |
| `dsh-role-bridge` | `plugins/dsh-role-bridge` | 票中角色桥：`send_to_role` / `ask_supervisor` / `submit_for_review`（写/poll mailbox） |
| `dsh-eager-offload` | `plugins/dsh-eager-offload` | 工具结果超限时提前 spill 到 `$DSH_HOME/offload`（已同步 box 开发树实际安装版；其中 ageMask 在当前 dsh 上无效） |
| `dsh-design-pack` | `plugins/dsh-design-pack` | `design_pack_read`：按 pack 名读设计切片（acp / acp-lite / 各 trial / acp-hub-read / dp-design-pack 都装） |
| `dsh-hub-read` | `plugins/dsh-hub-read` | Hub 只读工具（仅 `acp-hub-read`） |

## 安装（本地路径）

```bash
# 人：web-facing profile（dsh web 的唯一 UX）
dsh plugin --profile web add -w "$(pwd)/plugins/dsh-trial-desk"
# 结构上等价：dsh plugin add -w "$(pwd)/plugins/dsh-trial-desk"

# 短票：单独 trial profile，勿装进产品 acp / acp-lite 本体
dsh plugin --profile acp-lite-trial add -w "$(pwd)/plugins/dsh-role-bridge"
dsh plugin --profile acp-lite-trial add -w "$(pwd)/plugins/dsh-eager-offload"
```

也可指向开发树（箱上历史路径，仅参考）：

- `/workspace/dsh-plugins/dsh-role-bridge`
- `/workspace/dsh-plugins/dsh-eager-offload`

详细说明见各包 `README.md` / `README.zh.md`。`dsh-role-bridge` 需要本仓 `broker/trial-broker.py` 的 mailbox watcher + `routes.json` 才能唤醒对端。`dsh-trial-desk` 只写 inbox，从不 open-slice / 进房 / Hub webhook / `broker-khub-prod`。

## 已废弃（不入库）

- `dsh-ask-supervisor`（`/workspace/dsh-plugins/dsh-ask-supervisor`）：早期原型，已被 `dsh-role-bridge` 的 `ask_supervisor` 取代；2026-10-08 核对无任何 profile 安装/引用。不入库、暂不删除。

## 固定插件 worktree（上线用安装源）

- 路径：`/workspace/dsh-trial-plugins`——本仓的 **detached worktree**，固定在 tag `plugins-pin-<YYYYMMDD><x>` 上（`git -C /workspace/dsh-trial-plugins describe --tags` 查当前 pin）。
- 用途：profile 软链（`~/.dsh/profiles/<p>/node_modules/<plugin>`）指向 `/workspace/dsh-trial-plugins/plugins/<plugin>`，**不要**指向会随合并变化的运行工作区 `/workspace/dsh-trial`，也不要再指向 `/workspace/dsh-plugins/` 开发树。
- 升级：在主仓打新 tag → `git -C /workspace/dsh-trial-plugins checkout --detach <new-tag>` → 重启使用该插件的 dsh 进程（profile `patchReload: startup`）。回滚：checkout 回旧 tag 再重启。
- 2026-10-08：已建好；目前只有 `web` 的 `dsh-trial-desk` 指向它，其余 profile 仍指 `/workspace/dsh-plugins/` 开发树，待上线窗口统一切换。
