# plugins/

本仓库内嵌三份 **trial 相关** dsh 插件源码（不含 `node_modules`）：

| 包 | 路径 | 作用 |
|----|------|------|
| `dsh-trial-desk` | `plugins/dsh-trial-desk` | **人工入口**：Goal / chain / chain-reply 写入 trial inbox；读 broker/thin-state。装进 `web`（或 `dsh web` 用的 `acp-lite`）。desk ≠ broker |
| `dsh-role-bridge` | `plugins/dsh-role-bridge` | 票中角色桥：`send_to_role` / `ask_supervisor` / `submit_for_review`（写/poll mailbox） |
| `dsh-eager-offload` | `plugins/dsh-eager-offload` | 工具结果超限时提前 spill 到 `$DSH_HOME/offload`（已同步 box 开发树实际安装版；其中 ageMask 在当前 dsh 上无效） |

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
