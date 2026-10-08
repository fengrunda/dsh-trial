# dsh-step-budget-reminder

步数预算的**软提醒**插件：只提醒，不停。

## 用途

给根 agent 的循环加一道"该收尾了"的提示，不打断、不 veto、不停止循环：

- 步数到**预算**时，注入第一条提醒；
- 步数到 **1.5 倍（向上取整）** 时，注入第二条；
- 之后不再重复（每个阈值只投递一次）。

提醒文案固定：

| 时机 | 文案 |
| --- | --- |
| 到预算 | `[step-budget] 已到步数预算（{steps}/{budget}）。请收尾：跑相关测试、写 summary、提交；做不完就在 summary 里写清剩余工作。` |
| 到 1.5 倍 | `[step-budget] 已超预算 1.5 倍（{steps}/{budget}）。请立即收尾：跑相关测试、写 summary、提交；做不完就写清剩余工作。` |

## 投递方式（为什么不会破坏缓存前缀）

提醒**追加在最新位置**：计数越过阈值时先记一条 pending，等本次循环的下一步
`tools/post-execute` 把它**附加到该步 `additionalContexts` 的末尾**
（`[...下游的, 我们的]`，block 分支同样带上）。历史消息一律不改写，已缓存的
prompt 前缀保持稳定。

消息以 `{kind:'plugin', plugin:'step-budget-reminder', form:'notice', summary}`
为 source —— 这个标签是必需的，缺了会被渲染成用户 prompt。

## 预算来源

优先级从高到低：

1. `config.budget`（正整数）；
2. 环境变量 `DSH_STEP_BUDGET`（apply 时读取，正整数）；
3. 无。

三者都没有（或值为 `0`/非数字）时插件**完全不动作**：listener 仍注册，但既不计数
也不注入。`config.secondFactor` 默认 `1.5`。

预算由上游决定（本插件不自己发明预算），预期来源：

- open-slice 的 `--step-budget` 迭代开关；
- broker 按角色给的默认预算；
- 部署时直接设 `DSH_STEP_BUDGET`。

## 只对根 agent 生效

`agent/pre-step` 逐 agent 计数（WeakMap）。只有**根 agent** 被计数 —— 即
`dsh-agent` 的 `Agent` 上没有活父引用（`parentAgent` 缺省）。被委派的子 agent
不计数、不提醒。

## 配置

```yaml
- insert:
    - id: step-budget-reminder
      name: dsh-step-budget-reminder
      config:
        budget: 30        # 显式预算，优先于 DSH_STEP_BUDGET
        secondFactor: 1.5 # 第二条提醒的倍数
```

## 测试

```sh
node --check lib/budget.js && node --check lib/index.js
node --test "test/**/*.test.mjs"
# 注意：本机 Node 22.19 把 `node --test test`（目录参数）当模块解析而报
# MODULE_NOT_FOUND（dsh-role-bridge 同样如此），所以用 glob 或显式文件路径。
```
