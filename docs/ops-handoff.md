# dsh运维 交接（2026-10-12 14:52 Asia/Shanghai）

## 2026-10-21 max_gate_rounds 触顶与 max_rounds 同轨
- gate_rounds_by_slice 触顶不再机械 escalate：Goal 停 awaiting_supervisor（awaiting_reason=max_gate_rounds），
  自动开监理决策短票 inplace(+1 有界) / split / escalate；工头随后写的 summary(status=blocked) 同样留在该决策道（不 notify Hub、不重开票）。
- 只有真 escalate 才 notify Hub；parked/split 不留 dsh-trial-awaiting-hub 事件。
- inplace 只抬本 slice 帽（goal.max_gate_rounds_by_slice），全局 DEFAULT_LIMITS.max_gate_rounds 不动。

## 现状
- 代码：/workspace/dsh-trial（fengrunda/dsh-trial），main 见本提交；parent 185976c。本提交：newapicy1 adapter 修复笔记 + Hub salvage。
- 进程：trial-broker pid 1470231（`broker/trial-broker.py --poll 20`），trial-watchdog pid 1470279（`--loop --interval 60`）；pid 文件 ~/.dsh/broker-dsh-trial/trial-broker.pid / trial-watchdog.pid。DSH_HOME=/home/box/.dsh。
- 状态 ~/.dsh/supervisor/{trial,thin-state}；配置 ~/.dsh/supervisor/trial/limits.json；日志 ~/.dsh/broker-dsh-trial/trial-broker.log；role homes ~/.dsh-homes/{supervisor,impl,gate}。
- 并发：max_concurrent_goals=2，同 cwd 串行。线上两 Goal 并发尚未实测。
- 模型：trial（acp-trial / acp-lite-trial）= newapicy1/kimi-for-coding（NEWAPICY1_API_KEY）；产品 acp / acp-lite / acp-hub-read / settings agent-default-model 仍为 deepseek-official/deepseek-flash。
- 推理强度（limits.json reasoning_effort_by_role，默认同）：supervisor=high，impl=high，gate=high（Runda 2026-10-21）。DeepSeek 只有 low/high/max，medium=high。运维票用 `--reasoning-effort` 或 DSH_REASONING_EFFORT。
- 步数软提醒（插件 dsh-step-budget-reminder，不硬停）：supervisor 30 / impl 80 / gate 28（step_budget_by_role；gate 另可用 limits `gate_max_steps` 覆盖，到点写 status=paused_limit 交监理五选一）。gate 摘要写 `status=paused_limit` 时**必须**带 `used_steps` 与 `remaining` 两键（勿空 HOLD）。
- acp-trial / acp-lite-trial：压缩 thresholdRatio 0.15 / retainRatio 0.05（kimi ~39k）；精简工具（38→21 / 37→20）；contextClear {enabled, compaction-coupled, keepRecentResults 8, clearReasoning on}。插件软链指 pin worktree /workspace/dsh-trial-plugins（tag plugins-pin-20261008d = 79a9070，已 push）。产品 profile acp / acp-lite / acp-lite-offload / acp-hub-read 未改（压缩阈值实际约 150k）。
- broker 今日修复：gate 去重账本 thin-state/review-ledger.json；标题安全化与 review_seq；续跑判断票是否真结束；watchdog 只计 broker 子进程；gate pack 独立命名；submit_for_review 前置 summary 检查；看板刷新钩子 ~/.dsh/bin/khub-dsh-complete-notify.py。
- 巡检 routine「dsh trial 底座」**已恢复**（工作日 10:35 / 14:35 / 18:35 Asia/Shanghai）。观察焦点：质量链路（gate 单派 / reasoning high·high·high / contextClear / 双 Goal 并发）+ token。


## 运维笔记（2026-10-12 15:00 Asia/Shanghai）
- **hub-read 回指 dsh-dev**：`~/.dsh/profiles/acp-hub-read/engine.env` TENANT/USER→`dsh-dev`，token 模式 `platform-test-dsh-dev`，BASE_URL 仍 `http://127.0.0.1:8000`。备份 `engine.env.bak-20261012-145929-host-jinyinhua-qa`。
- **trial 挂只读 Hub**：`acp-lite-trial` / `acp-trial` 整 profile 挂 `dsh-hub-read`（`hub_browse`/`hub_resolve`；neighborhood 关；无角色级挂载，impl 亦可见但只读）。软链 pin `dsh-trial-plugins`；`engineEnvFile` 共用 hub-read 的 engine.env。备份 `package.json`/`cordis.patch.yml` 的 `*.bak-20261012-145929-prehub`。未重声明完整 providers。
- **冒烟**：dsh-dev `hub_browse` → 概念非空（例 ABox 等，count≈46）。design-docs@2026-10-04.1 concept+relation 在册，边 25/25 齐全 → **未**重跑 publish。产品 broker 未动。

## 运维笔记（2026-10-12 14:58 Asia/Shanghai）
- **Hub·引擎开发租户评估**：建议继续用 `dsh-dev`（design-docs@2026-10-04.1 concept+relation published/active，35/11/25 边齐全；token 模式 `platform-test-dsh-dev` 可用）。未新建 `hub-engine-dev`。`host-jinyinhua-qa` 未动（无 design-docs 混灌）。restore 优先 `publish_design_pack.py`；勿盲跑完整 `khub-engine-restore-packs`（其 jinyinhua 支路会 409）。产品 broker 未动。

## 运维笔记（2026-10-12 14:52 Asia/Shanghai）
- **根因**：切回 `newapicy1/kimi-for-coding` 后 ACP `session/new` 报 `no adapter registered for provider "newapicy1"`。`~/.dsh/settings.yaml` 虽有 newapicy1，但 ACP 自定义路由不会像 deepseek-official 那样被 sdk 硬编码兜底；trial patch 未在 cordis 声明 `llm-pi-ai.providers.newapicy1` → adapter 未注册。切片未开。
- **修复**（仅 trial，未动产品 profile/broker）：acp-lite-trial / acp-trial 的 `cordis.patch.yml` **最小**插入 `llm-pi-ai.providers.newapicy1`（仅该路由，勿整段重声明含 deepseek 的 providers，以免旧坑 inactive context）。备份 `*.bak-20261012-145006-prenewapi`。
- **验证**：`dsh-acp-ask --profile acp-lite-trial` session/new + 极短票 → OK；`--role supervisor` → OK-SUP。provider+model=`newapicy1/kimi-for-coding`。无需重启 trial-broker（每票新 ACP 读 patch）。
- **Hub salvage**：`hub-kg-my-memory-admin-v1` 已重投 inbox → processing（planning）；新 plan 票 sessionId 已出（仅 warning：kimi 无 reasoning_effort=low）。failed 留 salvage-note。

## 运维笔记（2026-10-12 14:47 Asia/Shanghai）
- **轻量基线（只读）**：broker 1470231 / watchdog 1470279 LIVE；heartbeat slots=[] pending=0 max=2；无 running Goal；近日志无 429/新 ERROR（重启后干净）。Goals 74：done46 / failed13 / escalated7 / cancelled8；最近 `hub-logic-gate-g1-s0-adr-v1-r2` done PASS（10-08）。`review-ledger.json` 尚不存在。历史：mailbox watch ENOENT×53（不安全标题 pack）、同 Goal gate spawn×6（含 rev38）。

## 运维笔记（2026-10-12 14:42 Asia/Shanghai）
- **trial 模型切回 kimi**：acp-lite-trial / acp-trial 的 acp + agent-default-model：`deepseek-official/deepseek-flash` → `newapicy1/kimi-for-coding`。`--dump-config` 已核。**事后补丁**：须最小声明 `llm-pi-ai.providers.newapicy1`（见 14:52 笔记）。NEWAPICY1_API_KEY 仍由 `load-env.sh` 注入，未写入文件。
- **settings.yaml**：`newapicy1.models` 恢复 `kimi-for-coding`（+ highspeed）；全局 `agent-default-model` 仍为 deepseek-official/deepseek-flash（产品 profile 未动）。备份：`settings.yaml.bak-20261012-144139-prekimi`、trial patch `*.bak-20261012-144139-prekimi`。
- **压缩**：去掉 deepseek 专用 modelPolicies(0.06)；kimi 走 thresholdRatio 0.15 / retainRatio 0.05（~39k @ ~262k）。
- **kimi 探测**：newapicy1 `kimi-for-coding` chat completions temperature=1 → HTTP 200。
- **trial-broker 重启**（空闲 slots=0）：旧 pid 771374/771419 → 新 **1470231 / 1470279**。产品 broker 未动。
- **watchdog resumed-after-freeze**：今日 10 次（CST 07:20–14:37），间隔约 17–98 min，符合宿主休眠/时钟跳变误报；broker heartbeat 正常、slots_busy=0、无 running Goal、无 dsh-acp-ask 僵尸。**可忽略**。
- **salvage**：归档孤儿 `t3c-gate-1` + thin-state pending notify/question + impl home medium-bridge 残留 pending → `mailbox/archive/*.orphan-pending.json`。未清 Redis/DB。

## 回退
- 模型回 deepseek：还原 trial cordis.patch.yml.bak-20261012-144139-prekimi（或手改 provider/model）；settings 可保留 kimi 模型条目。
- 推理强度：改 limits.json（备份 limits.json.bak-20261008-low）。
- contextClear：profile 里 enabled:false，或软链指回 plugins-pin-20261008c，或还原 cordis.patch.yml / package.json 的 .bak-20261008-ctxclear / -reasoning / -pre-compact60k。

## 待办 / 待观察
1. Hub 下一个真实 Goal 上核对：gate 每轮只派一次；票日志 reasoning_effort 为 high/high/high；压缩时有 contextClear 生效记录；两 Goal 并发。
2. review 路径同一份代码重复提交仍会再审（账本只用于 chain 路径去重）。
3. token 记账：监理 plan/close 未计入；指标用 peak×steps，应改实际累加。
4. live 引擎已切 5327988（2026-10-12 12:13 CST）：旧 9c114ca2c1f1ed0aea6acd926e66a151cbf2d63f → 新 5327988598f9edcdb77f1799ec6c3803a7548e2b（= origin/main tip，#163 个人版主体记忆 s1）；health `{"status":"ok","redis":"pong"}`；127.0.0.1:8000 pid 1459702；经 khub-engine-with-llm-env。租户仍 host-jinyinhua-qa；未 reseed / 未动 GraphStore 数据目录。
5. FYI：gate idle 超时、watchdog 误报 stalled。
6. 遗留 worktree：wt-conc / wt-dedup / wt-limits / wt-notify 已合入，可删。mailbox 孤儿 t3c-gate-1 已归档（2026-10-12）；hub-logic goal review_seq 已到 55。
7. web profile 的 desk 已修好（555578a），dsh web 当前未运行。

## gate_grain=per_goal（2026-10-20）
- 默认 Goal 末次一张 LLM gate（汇总各片 summary + 一次 diff）；`pack gate: per_slice` / `risk: high` / 高风险路径仍 per_slice。
- 与 `skip_gate` + diff 门禁兼容（收尾免审不占 Goal gate）。
- limits 键 `gate_grain`；env `DSH_GATE_GRAIN`；Goal/job `gate` 可覆盖。方案 C 本周不做。
- **需合 PR + 重启 broker** 才进热路径（与 PR#11 awaiting guard 同空窗）。

## 规矩
- 底座冻结几天：只修阻塞 Hub 的故障，其他优化攒着，等真实数据后一次做。
- 底座代码改动交给 Cursor 云端代理（grok-4.7，reasoning_effort medium，fast false），出 PR；Grok 只写 Goal、看报告和测试结果，不读 diff。合入后由箱上 commit/push、空闲时重启、验证。
- 不清 Redis/DB；key 在 /home/box/.dsh/.env（`source /home/box/.dsh/load-env.sh`），不打印、不找用户要；profile 改动需用户批准；trial patch 可最小声明 `llm-pi-ai.providers.newapicy1`（ACP 必需）；勿整段重声明与 settings 重叠的完整 providers（曾致 inactive context）。
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
- Runda 拍板（经 Grok Bot 2026-10-10）：默认关；开关工具修好前不扩到其他 profile；现有 acp-lite-trial-adhd 留作回退参考，不推。

## 宿主默认租户切换（2026-10-10 19:32 Asia/Shanghai）
> 已于 2026-10-12 15:00 回指 `dsh-dev`（见上方笔记）；下列为当时切到 host-jinyinhua-qa 的记录。
- Hub/读侧宿主闸门默认租户已切到 `host-jinyinhua-qa`（先切环境，G3 另开）。
- 改动文件：`~/.dsh/profiles/acp-hub-read/engine.env`
  - 变量：`KNOWLEDGE_HUB_ENGINE_TENANT_ID` / `KNOWLEDGE_HUB_ENGINE_USER_ID` → `host-jinyinhua-qa`
  - 变量：`KNOWLEDGE_HUB_ENGINE_TOKEN` → 平台测试令牌模式 `platform-test-<tenant>`（指向 host-jinyinhua-qa）
  - `KNOWLEDGE_HUB_ENGINE_BASE_URL` 仍为 `http://127.0.0.1:8000`
  - 备份：`engine.env.bak-20261010-host-jinyinhua-qa`
- 扫描：其它 profile / llm.env / khub 启动脚本无额外「宿主默认租户」硬编码需改；`khub-engine-restore-packs` 的 dsh-dev 设计包入口保留；产品 wire `DEV_SCOPE_PLACEHOLDER`/`HUB_BANK=dsh-dev` 仍为设计仓占位（G3 再开）。
- 验证：health ok+redis pong；引擎 tip 9c114ca，:8000 pid 1430091；租户仍 host-jinyinhua-qa；design-docs@2026-10-04.1 published/present；未 reseed / 未动 GraphStore 数据目录 / 未降版本。

