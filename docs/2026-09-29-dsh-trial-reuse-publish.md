> **路径说明**：文中 `/home/box/.dsh`、`/workspace/dsh-trial` 等为箱上历史默认；复用时请改为 `$DSH_HOME` / `$DSH_TRIAL_ROOT` / 环境变量（见仓库根 README「Known box paths」）。

# broker-dsh-trial 套件：给他人复用的发行形态调研

日期：2026-09-29（Asia/Shanghai）  
范围：只读箱上现状 + 公开上游文档；**未** publish / 建仓 / push / 改产品四票。  
对象：`broker-dsh-trial` + `dsh-role-bridge` + `dsh-eager-offload` + open-slice / thin-state（含 design-pack）。

---

## 1. 箱上现状：插件与注册形态

### 1.1 开发树

| 包 | 路径 | 版本 | `dsh.bundle` | 备注 |
|----|------|------|:------------:|------|
| `dsh-role-bridge` | `/workspace/dsh-plugins/dsh-role-bridge` | `0.1.0-dev` | 有 | trial-only；需 broker 路由唤醒对端 |
| `dsh-eager-offload` | `/workspace/dsh-plugins/dsh-eager-offload` | `0.1.0-dev` | 有 | 已挂产品 `acp`/`acp-lite`（link）与 trial |
| `dsh-design-pack` | `/workspace/dsh-plugins/dsh-design-pack` | `0.1.1-dev` | 有 | 同上 |
| `dsh-ask-supervisor` | `/workspace/dsh-plugins/dsh-ask-supervisor` | `0.1.0-dev` | 有 | **原型，文档标明改用 role-bridge** |

共性：`type: module`、`main`→`lib/`、`files` 含 `cordis.patch.yml`、`peerDependencies: @deepseek-ai/cordis`、`private: true`、`license: UNLICENSED`。  
**无** `publish` / `prepublish` / hub / registry 脚本；无 monorepo 根 `package.json` 发布流水线。

### 1.2 Profile 挂载（`~/.dsh/profiles`）

| Profile | bundles（本套相关） | 依赖形态 |
|---------|---------------------|----------|
| `acp` / `acp-lite` | `dsh-design-pack`, `dsh-eager-offload` | `link:/workspace/dsh-plugins/…` |
| `acp-trial` / `acp-lite-trial` | 上两项 + **`dsh-role-bridge`** | 同上 |
| `dp-design-pack` | 仅 `dsh-design-pack` | 同上 |
| `acp-lite-offload` | design-pack + eager-offload（试验阈值） | 同上 |

`dsh plugin --profile <name> add -w <path>` 即官方「本地 checkout → pnpm link + 追加 `dsh.profile.bundles`」路径；箱上已用该形态。

### 1.3 编排面（非 cordis 插件）

| 组件 | 位置 | 作用 |
|------|------|------|
| `broker-dsh-trial` | `/workspace/dsh-trial/broker/` | 薄 Python：inbox→open-slice→outbox；chain / mailbox watcher |
| `open-slice.sh` | `/workspace/dsh-trial/open-slice.sh` | 拼 prompt → `dsh-acp-ask.py --final` |
| `dsh-trial` CLI | `~/.dsh/bin/dsh-trial` | status / report / limits（`sys.path` 指到 `/workspace/dsh-trial/broker`） |
| `dsh-trial-pr` | `~/.dsh/bin/dsh-trial-pr` | 受控 push+PR；**allowlist 硬绑** `fengrunda/knowledge-hub` |
| thin-state | `~/.dsh/supervisor/thin-state/{packs,summaries,mailbox,chains,goals}` | 文件桥状态 |
| limits / routes | `~/.dsh/supervisor/trial/{limits.json,routes.json}` | 全局上限与 `(to_role,kind)` 路由表 |
| Hub notify | `~/.dsh/bin/khub-dsh-complete-notify.py` | job 显式 `notify: hub` 时调用 |

运行态：`~/.dsh/broker-dsh-trial/`；产物默认 `/workspace/tmp/trial-broker/`。

### 1.4 强绑定（复用前必须参数化或剥离）

- **箱绝对路径**：`/home/box/.dsh`、`/home/box/.dsh-homes`、`/workspace/dsh-trial`、`/workspace/tmp/…` 散落在 `open-slice.sh`、`trial-broker.py`、`dsh-trial` CLI、`dsh-role-bridge/lib/mailbox.js`（`GLOBAL_THIN = '/home/box/.dsh/supervisor/thin-state'`）。
- **租户 / 产品**：`dsh-trial-pr` allowlist；prompt 文案里的 `fengrunda/knowledge-hub`；Hub webhook / `khub-*` 脚本名与 kind（`dsh-trial-chain` 等）。
- **模型中继**：trial profile 的 `cordis.patch.yml` 含 `newapicy1` baseURL / `apiKeyEnv`（属环境配置，**不应**打进对外包）。
- **产品隔离纪律**：永不碰 `broker-khub-prod` / 产品四票；role-bridge **禁止**装进产品 `acp`/`acp-lite`（角色家 symlink 会共享工具面）。

---

## 2. 上游：安装与发布渠道

依据官方 `deepseek-harness` 文档 [`docs/user/develop/basic/publish.md`](https://github.com/deepseek-ai/deepseek-harness/blob/master/docs/user/develop/basic/publish.md)（2026 仍有效）及社区索引站。

### 2.1 概念

| 概念 | 含义 | 清单键 |
|------|------|--------|
| **bundle** | 可发布的 npm 包 + 配置层 | `dsh.bundle.patch` → `cordis.patch.yml` |
| **profile** | `$DSH_HOME/profiles/<name>` 合成目录 | `dsh.profile.bundles` |

`dsh plugin --profile <name> …` = 在 profile 目录里 **转发 pnpm**；CLI **没有**官方 `search` / 官方私有 registry。

### 2.2 官方认可的分发方式

1. **本地路径**：`dsh plugin --profile demo add ./hello-plugin`（或箱上常用的 `-w` link）。
2. **GitHub 源码**：`dsh plugin --profile demo add github:you/hello-plugin`  
   - 拉的是源码；TS 需作者 `prepare` 构建；pnpm≥10 要用户 `allowBuilds`。  
   - 建议 pin commit：`github:you/repo#<sha>`。
3. **npm 预构建**：`pnpm publish` 时带上 `lib/` → `dsh plugin --profile demo add <name>`（**无需** allowBuilds）。
4. **tarball**：`pnpm pack` → `dsh plugin --profile demo add ./pkg-0.1.0.tgz`（同样无需 allowBuilds；适合内部分发 / 试点）。

必备字段：`main`/`exports`、`type: module`、`files` 含入口与 `cordis.patch.yml`、`dsh.bundle.patch`、版本号。缺 `dsh.bundle` → 装上但不生效。

### 2.3 「dsh hub」实际指什么（社区，非 DeepSeek 官方仓）

| 名称 | 角色 | 与本套匹配度 |
|------|------|--------------|
| **官方 CLI** | 无目录；只认 npm/GitHub/本地 | 插件档直接匹配 |
| **dshmp.com** 等索引 | npm 带 keyword `dsh-plugin` 后日更收录 | **仅独立 bundle** 适合上架 |
| **dsh-plugin / dsh-plugin-shop / dshmarketplace** | 社区市场 UI / 目录管道 | 同上；编排 Python 不在其模型内 |
| 旧笔记中的 PerryLink catalog URL | 第三方目录源 | 可选，非前置 |

结论：**没有单一「官方 dsh hub」门禁**；插件走 npm keyword 或 GitHub topic 即可被社区索引。**broker / open-slice / thin-state 编排不适合当 cordis hub 插件**，应走 GitHub 模板仓。

Mac 侧（`/Users/fengrunda`）：有 `~/.dsh/DSH-USAGE-PACK.md`、`~/.agents/skills/dsh-cli`、hermes-agent 树；**无**额外官方 publish 手册，与箱结论一致。

---

## 3. 可复用边界对照

| 组件 | 已是插件？ | 可独立复用？ | 边界说明 |
|------|:----------:|:------------:|----------|
| `dsh-eager-offload` | 是 | **高** | 纯 `tools/post-execute`；相对 `$DSH_HOME/offload`；配 `dsh-offload-gc.py` 更完整 |
| `dsh-design-pack` | 是 | **高** | 只读 `design_pack_read`；`packRoot` 相对 thin-state；开票拼装仍靠外部 |
| `dsh-role-bridge` | 是 | **中（成对）** | 只写/poll mailbox；**必须**有实现 routes 的 watcher（本套即 trial-broker）；mailbox 现硬绑 `/home/box/...` |
| `dsh-ask-supervisor` | 是（遗留） | 不推荐再发 | 由 role-bridge 取代 |
| `broker` + `open-slice` + limits + routes + mailbox 约定 | 否 | **模板仓** | 强路径 / Hub / PR allowlist 属租户适配层 |
| `dsh-trial` / `dsh-trial-pr` / `khub-*` notify | 否 | 条件复用 | CLI 可泛化；PR allowlist 与 Hub notify **默认不含**对外最小包 |
| thin-state 目录约定 | 约定 | 文档化即可 | `packs/` / `summaries/` / `mailbox/` / `chains/` / `goals/` |

分层记忆：**插件 = 票内能力；broker = 票间调度。** ACP 一票一进程，插件无法单独「叫醒」对端。

---

## 4. 推荐发行形态（三档）

> 本调研 **不执行** publish/建仓；下列为给 Runda 拍板的产品化菜单。

### 档 A — 最小插件包（hub / npm 友好）

**含什么**

- 独立 npm 包（或 monorepo packages）：至少 `dsh-eager-offload`、`dsh-design-pack`；可选拆出「纯 mailbox SDK」后再发 `dsh-role-bridge`。
- 每包：`lib/` + `cordis.patch.yml` + README + 测试；`keywords: ["dsh","dsh-plugin",…]`；去掉 `private`、选开源许可证。
- 安装示例：`dsh plugin --profile <you> add dsh-eager-offload@x`（或 `./xxx.tgz` / `github:…#sha`）。

**不含什么**

- trial-broker、open-slice、Hub notify、`dsh-trial-pr`、knowledge-hub allowlist、箱路径、newapicy1 patch、产品四票配置。

**安装概要**

1. 自备 dsh ≥ 与 peer 对齐的版本 + pnpm。  
2. `dsh plugin --profile <name> add <pkg>`。  
3. `dsh --profile <name> --dump-config` 确认层。  
4. role-bridge 档若发布：文档写明「需自备 mailbox watcher / 兼容 broker」。

**安全注意**

- offload 落盘 `0600`；勿把 offload 目录当共享机密仓。  
- design-pack 拒绝 `..` / 绝对路径穿越——保持测试覆盖。  
- 开源前 scrub：mailbox 硬编码、示例 pack 中的箱路径 / 密钥名。

**与 dsh hub**

- **匹配**：符合 bundle 规范；带 `dsh-plugin` keyword 可被 dshmp / shop 日更收录。  
- role-bridge 单独上架时须在描述写清 **broker 依赖**，避免「装了不能问监理」差评。

---

### 档 B — 编排模板仓（推荐对外「整套复用」主路径）

**含什么**

- GitHub template：`broker/`（trial-broker + trial_lib + mailbox + examples）+ `open-slice.sh`（路径改为 `$DSH_HOME` / 可配置前缀）+ `assert-no-room-inject.py` + 示例 `limits.json` / `routes.json` + 示例 packs。  
- 子模块或 `file:`/`github:` 引用档 A 插件（或 README 写 `dsh plugin add`）。  
- 可选：精简 `dsh-trial` CLI（不硬编码 `/workspace/...`）。  
- README：Route C 纪律（执行票不进房）、与产品 broker 共存说明、环境变量表。

**不含什么**

- 默认关闭 Hub notify；不含租户 webhook URL。  
- 不含绑死 `fengrunda/knowledge-hub` 的 PR 工具（或改为空 allowlist + 用户自填）。  
- 不含 `~/.dsh/.env`、API key、relay baseURL、真实 session / scratch 业务仓。

**安装概要**

1. `gh repo create` from template → clone。  
2. `export DSH_HOME=…`；可选 `DSH_TRIAL_MAILBOX`、`DSH_HOMES_ROOT`。  
3. 建隔离 profile（如 `acp-lite-trial`），`dsh plugin add` 三插件。  
4. 复制 `limits.json` / `routes.json` 到 `$DSH_HOME/supervisor/trial/`。  
5. `./broker/start.sh --once` 跑 examples 烟雾。

**安全注意**

- 模板默认 `notify_dry_run: true`；`DSH_PERMISSION_MODE` 由用户自担。  
- 文档强调：勿把 trial 工具装进与产品四票共享的 profile。  
- Git install 须 pin SHA + 谨慎 `allowBuilds`。

**与 dsh hub**

- **不匹配为单插件**：整仓是编排；可在 hub 插件描述里「See companion template repo」。  
- 插件仍可单独上架；模板用 GitHub Topics（如 `dsh`、`deepseek-harness`）发现。

---

### 档 C — 完整试装包（内部 / 信任圈）

**含什么**

- 档 B + 预置 profile 片段（无密钥）+ `dsh-acp-ask.py` 约定文档 + offload GC + composition/assert 脚本副本或链接 +（可选）Hub notify **适配器接口** + 可配置 PR allowlist。  
- 一键 `install.sh`（检测 dsh/pnpm、建 profile、link 插件、写 trial 目录）；附验收清单（对照箱上 `2026-09-24-dsh-trial-acceptance.md`）。

**不含什么**

- 真实 `.env`、GH_TOKEN、产品 `broker-khub-prod`、knowledge-hub 业务代码、箱上 sessions / 备份。  
- 不自动 restart 任何产品 broker。

**安装概要**

1. 信任机：解压或 clone → 填 `.env.example`。  
2. `./install.sh --profile acp-lite-trial`。  
3. 跑 chain 烟雾 + `assert-no-room-inject`。  
4. 再按需打开 notify / PR allowlist。

**安全注意**

- 分发渠道限私有仓或签名 tarball；审计 `allowBuilds` 与 `danger-full-access`。  
- 开源公开前必须完整 scrub（见 §5）。

**与 dsh hub**

- **不匹配**：属运维脚手架；勿整包当 hub 插件上传。

---

## 5. 缺口与开源前 scrub

### 5.1 缺官方 hub 时的替代

| 阶段 | 做法 |
|------|------|
| 试点 | 各插件 `pnpm pack` → 内部分发 `.tgz` + `dsh plugin add ./…tgz` |
| 公开插件 | npm publish + `keywords: ["dsh","dsh-plugin"]`；可选 GitHub topic 同名 |
| 编排 | GitHub **Template repository** + 中英文 README；插件用 git submodule 或 npm 依赖 |
| 发现 | 社区 dshmp / shop 自动收；无需人工「提交表」 |

### 5.2 开源 / 外发前必须 scrub（勿打印密钥；此处只列类别）

| 类别 | 示例位置 / 动作 |
|------|-----------------|
| 密钥与 token | 排除 `~/.dsh/.env*`、任何 `*token*` bak、日志；文档只写变量名 |
| 箱绝对路径 | `mailbox.js` 的 `/home/box/...` → `$HOME/.dsh` 或 `DSH_TRIAL_MAILBOX`；`open-slice` / broker / CLI 中 `/workspace`、`/home/box` |
| 租户标识 | `fengrunda/knowledge-hub` allowlist 与 prompt 示例 → 占位 `OWNER/REPO` |
| 中继 / 模型 | trial `cordis.patch.yml` 的第三方 `baseURL`、专用 provider 块 → 不进模板；用户自配 |
| Hub 耦合 | `khub-dsh-complete-notify.py` 调用改为可选 hook；默认 dry-run / no-op |
| 许可证与 private | 去掉 `private: true` / `UNLICENSED` 或明确保留专有并改走私有 registry |
| 示例与测试 | `examples/*.pack.md`、测试断言里的 `/home/box` 期望值 |
| bak / scratch | 不打包 `*.bak-*`、`scratch-repo` 真实 git 史、session jsonl |

### 5.3 发布前工程债（档 A/B 共用）

1. **mailbox 可移植**：去掉 `GLOBAL_THIN` 硬编码；默认 `path.join(os.homedir(), '.dsh', 'supervisor', 'thin-state', 'mailbox')`，并与 broker `DSH_TRIAL_MAILBOX` 对齐。  
2. **role-bridge 与 broker 版本契约**：routes schema / pending JSON 字段做 semver 或 `protocol_version`。  
3. **许可证与 npm scope**：选 MIT/Apache 或 `@org/` 私有 scope。  
4. **文档**：英文最小 README + 中文详述；标明「不进产品四票」。

---

## 6. 建议下一步（需用户拍板）

1. **先定档**：只发档 A（eager-offload + design-pack）做社区试水，还是直接做档 B 模板仓（插件仍先 `pnpm pack`、暂不 npm）？  
2. **role-bridge**：是否等 mailbox 去 `/home/box` 硬编码并写出「最小兼容 broker 接口」后再进公开档，还是仅随档 B 私有分发？  
3. **许可与名义**：开源（选许可证 + npm 名）还是组织内私有 registry / 仅 GitHub private template？

---

*调研执行面：共享 Linux 箱 `/workspace/dsh-plugins`、`/workspace/dsh-trial`、`~/.dsh`；公开网官方 publish 文档与社区 hub；Mac 只读确认无额外官方发行手册。未执行任何 publish/建仓/push。*
