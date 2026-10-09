# dsh运维 交接（2026-10-08 16:45 Asia/Shanghai）

## 现状
- 代码：/workspace/dsh-trial（fengrunda/dsh-trial），main = 9b6cef9，已快进到工作区。
- 进程：trial-broker pid 771374（`broker/trial-broker.py --poll 20`），trial-watchdog pid 771419（`--loop --interval 60`）；pid 文件 ~/.dsh/broker-dsh-trial/trial-broker.pid / trial-watchdog.pid。DSH_HOME=/home/box/.dsh。
- 状态 ~/.dsh/supervisor/{trial,thin-state}；配置 ~/.dsh/supervisor/trial/limits.json；日志 ~/.dsh/broker-dsh-trial/trial-broker.log；role homes ~/.dsh-homes/{supervisor,impl,gate}。
- 并发：max_concurrent_goals=2，同 cwd 串行。线上两 Goal 并发尚未实测。
- 模型：所有 ACP profile 与 agent-default-model 均为官方 deepseek-official/deepseek-flash（DEEPSEEK_API_KEY），不走 newapi。
- 推理强度（limits.json reasoning_effort_by_role，默认同）：supervisor=low，impl=low，gate=high。DeepSeek 只有 low/high/max，medium=high。运维票用 `--reasoning-effort` 或 DSH_REASONING_EFFORT。
- 步数软提醒（插件 dsh-step-budget-reminder，不硬停）：supervisor 30 / impl 80 / gate 60（step_budget_by_role）。
- acp-trial / acp-lite-trial：压缩 thresholdRatio 0.06（≈60k）retain 20k；精简工具（38→21 / 37→20）；contextClear {enabled, compaction-coupled, keepRecentResults 8, clearReasoning on}。插件软链指 pin worktree /workspace/dsh-trial-plugins（tag plugins-pin-20261008d = 79a9070，已 push）。产品 profile acp / acp-lite / acp-lite-offload / acp-hub-read 未改（压缩阈值实际约 150k）。
- broker 今日修复：gate 去重账本 thin-state/review-ledger.json；标题安全化与 review_seq；续跑判断票是否真结束；watchdog 只计 broker 子进程；gate pack 独立命名；submit_for_review 前置 summary 检查；看板刷新钩子 ~/.dsh/bin/khub-dsh-complete-notify.py。
- 巡检 routine「dsh trial 底座」已暂停。

## 回退
- 推理强度：改 limits.json（备份 limits.json.bak-20261008-low）。
- contextClear：profile 里 enabled:false，或软链指回 plugins-pin-20261008c，或还原 cordis.patch.yml / package.json 的 .bak-20261008-ctxclear / -reasoning / -pre-compact60k。

## 待办 / 待观察
1. Hub 下一个真实 Goal 上核对：gate 每轮只派一次；票日志 reasoning_effort 为 low/low/high；压缩时有 contextClear 生效记录；两 Goal 并发。
2. review 路径同一份代码重复提交仍会再审（账本只用于 chain 路径去重）。
3. token 记账：监理 plan/close 未计入；指标用 peak×steps，应改实际累加。
4. live 引擎切 d0a466c：等用户解冻；重启须带 /home/box/.local/state/memagent/graphiti-dashscope.env 三项。
5. FYI：gate idle 超时、watchdog 误报 stalled。
6. 遗留 worktree：wt-conc / wt-dedup / wt-limits / wt-notify 已合入，可删。mailbox 孤儿 ask t3c-gate-1 待归档；hub-logic goal review_seq 已到 55。
7. web profile 的 desk 已修好（555578a），dsh web 当前未运行。

## 规矩
- 底座冻结几天：只修阻塞 Hub 的故障，其他优化攒着，等真实数据后一次做。
- 底座代码改动交给 Cursor 云端代理（grok-4.7，reasoning_effort medium，fast false），出 PR；Grok 只写 Goal、看报告和测试结果，不读 diff。合入后由箱上 commit/push、空闲时重启、验证。
- 不清 Redis/DB；key 在 /home/box/.dsh/.env（`source /home/box/.dsh/load-env.sh`），不打印、不找用户要；profile 改动需用户批准；profile patch 里不要重声明 llm-pi-ai.providers。
- 对 Hub 用中文；只报里程碑。

## 今日用量结论
全天约 110M token / ¥19.7（官方高峰价）。约 61% token、56% 费用来自底座运维票，其余是 Hub Goal（约 40M / ¥8.4）。费用里输出加推理占 55%，未命中占 24%，命中只占 21%。审计数据在 /tmp/tokaudit/attrib-1008/。

## dsh-i-have-adhd 试装（2026-10-09）
- 来源：github.com/yongshuai0314/dsh-i-have-adhd @ 4dd11e9e80921739f6cddc1df08f3ca204c223fa（v1.0.2，MIT；概念源自 ayghri/i-have-adhd，THIRD-PARTY-NOTICES 已保留原 MIT 文本）。本地 /workspace/dsh-plugins-3p/dsh-i-have-adhd。
- 审查：通过。无 dependencies、无 scripts（无 install/postinstall/prepare），无网络/子进程/eval/动态 require；只写 $DSH_HOME/dsh-i-have-adhd/always-on 标志文件。patch 只 insert 插件本身，不碰 llm-pi-ai.providers。注入文本是纯输出风格规则（约 570 token），没有可疑指令。
- 安装：只装进新 profile ~/.dsh/profiles/acp-lite-trial-adhd（从 acp-lite-trial 复制，package.json 加 link 依赖和 bundle）。用 pnpm 10（/usr/bin/pnpm）`install --ignore-scripts --config.node-linker=isolated`。不要用 ~/.local/bin 的 pnpm 12：它按 hoisted 重装 peers，session/prompt 会报 -32603 "reading 'prepare'"。`--dump-config` 跟 acp-lite-trial 比只多插件这一项。默认关。其他 profile、broker、limits.json 都没动。
- 缺陷：三个开关工具（adhd_on/off/status）在当前 dsh 上注册失败（output schema type:'json' 不被接受），所以会话里没有开关。目前唯一的开法是建标志文件 ~/.dsh/dsh-i-have-adhd/always-on，删掉即关。这个文件在共享的 DSH_HOME 下，对所有装了该插件的 profile 都生效（--role 会换 DSH_HOME）。
- 实测（deepseek-flash，low，同一汇报票，3 步，0 报错）：每步 prompt token（未命中+命中）：基线 acp-lite-trial 5632/6076/6767，总 19086；关 5632/5907/6575，总 18685；开 6205/6330/7014，总 20136。
  - 关：第 1 步跟基线完全一样，没有注入，每步零额外 token。
  - 开：每步多约 573 token，3 步总量多约 1.45k（约 +8%）。第 1 步因为区段插在 persona 后面、改了前缀，缓存全部未命中（0/6205）；后续步正常命中。
- 效果：关的时候第 1 行是「「待办 / 待观察」5 行总结：」，接 1–5 编号，没有客套话。开的时候第 1 行是加粗结论「待办 7 条…最高优先级是第 1 条」，编号到 7 条，超了 5 行的要求；结尾多了「下一步（2 分钟内可做）…」和一句「想问一句吗：要不要我…分类？」。所以开了以后更长，还会盖过用户的格式要求。
- 建议：默认关。只给汇报/摘要类角色试用（例如监理的 close/汇报），不给 impl 和 gate。插件没修好工具注册之前，不建议推到其他 profile。
- 回退：删掉 ~/.dsh/profiles/acp-lite-trial-adhd 和 ~/.dsh/dsh-i-have-adhd/ 即可，其他 profile 没受影响。
