# Gate 改动交接（per_goal 单一 gate + 口径/软顶）· 2026-10-21

分支 `feat/gate-ab-20261021`（3 个 commit：`df30cf5` s1、`dd521ac`+`9f49c1a` s2、s3）。不改业务行为。

## 1. 五项改动（一句话各一条）
1. `_handle_submit_for_review` 先看 `resolve_slice_gate_policy`：per_goal 的片中（非 bearer）submit 落 `defer` → 合成 PASS（`synthetic: true`），不开 gate、不动 `review_seq`。
2. 链末 gate 读 `job.gate_policy`；`per_goal` 时用 `build_goal_gate_pack` 聚合成**一张** Goal gate pack（带 `gate_deferred_slices`），`_synthetic_gate_pass` 是合成 PASS 的唯一出口。
3. Gate 口径 5 条（手里有什么判什么 / 不重判业务 / Done-when 逐条 / P2 措辞不默认 HOLD / 禁通读全仓）注入 gate prompt 与 gate pack 两处。
4. gate 角色步数软顶 28（`limits.json` 的 `gate_max_steps`，可覆写）→ 子进程 `DSH_STEP_BUDGET=28` + `DSH_GATE_MAX_STEPS=28`；`paused_limit` 机器块必须带 `used_steps`/`remaining`；口径尾巴受 `maxPackBytes` 约束（超了保尾、不涨包）。
5. 本片只加锁：`broker/tests/test_gate_per_goal_e2e_unit.py`（2 片 Goal：2 次 defer + 恰好 1 次 LLM gate）+ 本文件。

## 2. 热更 vs 需空窗重启
- **热更（下一次 spawn 即生效，无需重启 broker）**：`limits.json` 的 `gate_max_steps` / `gate_grain`（每次 `load_global_limits()` 现读文件）；纯 Python 模块里的常量与纯函数（`gate_effort.py` / `trial_lib` 的 `DEFAULT_STEP_BUDGET_BY_ROLE`、`resolve_slice_gate_policy`、pack 构造器）——只要新值来自文件或纯函数入参，下一次 spawn 用的就是它。
- **需空窗重启 broker**：`open-slice.sh` 里被 broker 进程持有/缓存的 prompt 与 gate 口径文本、`trial-broker.py` 中已 import 进进程的对象（`_handle_submit_for_review`、`_synthetic_gate_pass`、`_spawn_env` 等函数体改动）。线上 Hub S3 在跑，重启前先排空：等当前 ticket 结束、Goal 进 `paused`/无 `gate_running` 再 `bin/` 里按 runbook 重启；**本片不动进程**。
- 判断口诀：改**数据/文件**→热更；改**prompt 文本或已 import 的代码**→重启。

## 3. S2 类 2 片 Goal 的预期事件序列（`gate_grain=per_goal`，均非高风险）
1. s1 foreman 片中 `submit_for_review` → answer `synthetic: true`、`gate_policy: "defer"`，0 次 gate 进程，`review_seq` 不变；`goal.gate_deferred` +1。
2. s2 foreman 片中 submit → 同上（第 2 条 defer）。
3. s2 链末 bearer：`gate_policy=per_goal` → 生成 `goal-*.pack.md` → **1 次** `role=gate` 会话（steps 软顶 28），`review_seq` +1；VERDICT PASS 后 Goal 关闭。
- 合计：foreman 2 次，gate **1** 次（旧行为：每片各一次满 gate，37/57 步 ×2）。
- 例外：pack `risk: high` 或 diff 命中高风险路径 → 该片按 `per_slice` 开 gate；`skip_gate: true` + `src/` 改动不得免审（降级为 defer/per_slice）。

## 4. 回滚
- `git revert --no-edit 9f49c1a dd521ac df30cf5`（逆序 revert 本分支 3 个 commit）→ 回到按片开 gate 的旧行为；**无数据迁移**，`gate_deferred`/`gate_policy`/`gate_rounds_by_slice` 都是 goal JSON 的可选键，旧代码忽略即可；已写出的 `goal-*.pack.md` 与合成 PASS 摘要只是历史文件。
- 只想止血不改代码：`limits.json` 里 `gate_grain` 设回 `per_slice`（热更生效），再按上面重启让 prompt 文本也回来。
- 回滚后确认：`grep -c 'gate-defer\|synthetic' broker/trial-broker.py` 归零；跑 `broker/tests` 全绿。
