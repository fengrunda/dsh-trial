> **路径说明**：文中 `/home/box/.dsh`、`/workspace/dsh-trial` 等为箱上历史默认；复用时请改为 `$DSH_HOME` / `$DSH_TRIAL_ROOT` / 环境变量（见仓库根 README「Known box paths」）。

# dsh-trial 受控 GitHub 开 PR（2026-09-29）

## 落点
| 项 | 路径 |
|----|------|
| 包装脚本 | `~/.dsh/bin/dsh-trial-pr` |
| open-slice 硬禁/允许 | `/workspace/dsh-trial/open-slice.sh`（backup `*.bak-20260929-ghpr`） |
| Skill | `/home/box/agent-data/workflows/dsh-trial/SKILL.md`（id `dsh-trial`） |

## 安全边界
- **Allowlist**：`fengrunda/knowledge-hub`、`fengrunda/memory-as-training`（校验 `--repo` 与 `git remote get-url origin`，https / `git@` 均归一成 `owner/repo`）。环境变量 `DSH_TRIAL_PR_ALLOWLIST` 若设置则整表替换，不追加。
- **允许**：feature 分支 `git push -u origin <branch>` + `gh pr create --base main`（经包装）。
- **禁止**：force-push、push/PR 自 main/master、`gh pr merge`、删仓、其他 remote/仓、裸 `git push`/`gh`（执行票默认）、打印 `GH_TOKEN`/API key。
- **凭据**：Cursor 注入或 `~/.dsh/.env` 的 `GH_TOKEN`/`GITHUB_TOKEN`（经 `load-env.sh` 或包装脚本自读）；git 走 `gh auth git-credential`；与 NEWAPICY1 隔离；勿另造 `NEW_API_KEY`；勿把 token 写入文件/prompt/summary。
- **角色**：impl/foreman、supervisor-close 可调包装；supervisor-plan / supervisor-answer / gate 仍禁 push。
- **2026-10-03**：allowlist 增加 `fengrunda/memory-as-training`。仍禁止 force、main/master、merge，以及其他未列入的仓。

## 用法
```bash
dsh-trial-pr push-and-pr \
  --repo fengrunda/knowledge-hub \
  --cwd /path/to/knowledge-hub \
  --branch feat/example \
  --title "feat: …" \
  --body "…"
# 或分步：push | create-pr
# 已有同 head→base 的 open PR 时 create-pr 幂等打印 URL 并 exit 0
```

## 烟测建议（无真实 PR）
- 错误仓 / 在 main 上 / `--force` → 非零退出
- `which dsh-trial-pr`；`gh auth status` 可见 fengrunda
