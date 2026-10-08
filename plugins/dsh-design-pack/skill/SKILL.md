---
name: design-pack
description: >-
  中票开票用有界设计包：先读 pack，再按需跳读对照路径；
  办完写 summary 回薄监理索引；不靠房间 catch-up 补语境。
---

# design-pack

配合脚手架插件 `dsh-design-pack`（`design_pack_read`）与目录约定
`~/.dsh/supervisor/thin-state/packs/`。

## 开票

1. 父/桥给出 `slice_id` 与 pack 路径（或首 prompt 已贴包正文）。
2. 若只有路径：调用 `design_pack_read`（或 `read` 该文件）；**超限则拆 slice，勿硬灌**。
3. 只打开 pack 里的 `contrast_paths`；禁止整仓当上下文。

## 执行中

- 进度写入 thin-state / outbox，**不要**把长协作聊天依赖成 `user/message` 史。
- 大工具输出靠既有 spill/pruner；勿把整段结果贴回 pack。

## 关票

1. 按 `summary_out` 写 ≤30 行摘要：状态、产物路径、阻塞、下一步。
2. 更新 `thin-state/index.json` 对应条目为 `done` / `blocked`。
3. Goal 写明时才跑 `khub-dsh-complete-notify.py`。
4. **close** session；禁止常驻肥票「接着聊」。

## 硬禁

- 改 Knowledge Hub 业务 `src/`（除非 Goal 明文且本角色允许）。
- 把本 skill 当房间总线替代品却继续 sticky 四票密环。
- 未批准就 `dsh plugin add` 进产品 broker profile。
