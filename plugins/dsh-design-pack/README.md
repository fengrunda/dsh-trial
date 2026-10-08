# dsh-design-pack

有界**设计包**读取插件：服务 Route C「中票开票带包」路径。
一票一 slice；开票方直接读 pack，不靠房间 catch-up 或监理肥史补语境。

- **状态**：`0.1.1-dev`，仅 `/workspace/dsh-plugins/` 开发树 + 一个 dev profile（`dp-design-pack`）
- **未**安装进 `~/.dsh/profiles/{acp,web,sdk,headless}` / `broker-khub-prod`（产品四票）
- **未**重启产品 broker；**不**写 `user/message`，**不**自动 webhook
- packRoot 约定：`$DSH_HOME/supervisor/thin-state/packs/`

## 做什么

- Cordis 插件：注册只读工具 `design_pack_read`
- 按**相对路径**从 `packRoot` 读 Markdown pack（绝对路径、`..` 穿越、symlink 逃逸均拒绝）
- 超过 `maxPackBytes`（默认 `12288`）**整包拒绝**并回报字节数，**不**截断正文进史
- **不**把 pack 自动写成 sticky `user/message`（开票 prompt 拼装仍由 broker/桥 + skill）

配置（`cordis.patch.yml` 的 insert config）：

| key | 默认 | 说明 |
|-----|------|------|
| `packRoot` | `packs` | 绝对路径，或相对 `$DSH_HOME/supervisor/thin-state` |
| `maxPackBytes` | `12288` | 正整数 UTF-8 字节上限；超出即拒绝 |

## 不做什么

- 不 fork `dsh-team-rooms`、不改 ACP 全量重送
- 不替换 spill/compaction
- 不自动 webhook（仍用现有 `khub-dsh-complete-notify.py`）
- 不装进产品 profile、不重启产品 broker

## 自测（无模型、无网络）

```bash
cd /workspace/dsh-plugins/dsh-design-pack
node --check lib/pack.js && node --check lib/index.js
node --test test/pack.test.mjs      # 13 pass
```

覆盖：相对路径读、边界字节、超限拒绝且正文不外泄、缺失/目录/绝对/穿越/symlink、
真实 `@deepseek-ai/dsh-tools` schema 检查、`apply` 注册、box 上示例 pack 实读。

## 试装到 dev profile（勿动产品四票）

`<dev>` 用一次性名字，例如 `dp-design-pack`：

```bash
# 1) 建 dev profile（模板初始化后 --dump-config 直接退出，不 boot、不连模型）
dsh --profile dp-design-pack --from-default-profile headless --dump-config >/dev/null

# 2) 装本包（link 到工作树，改码即生效）
dsh plugin --profile dp-design-pack add -w /workspace/dsh-plugins/dsh-design-pack

# 3) 确认 bundle 合成（应出现 `- id: design-pack`）
dsh --profile dp-design-pack --dump-config | grep -A2 -i design-pack

# 4) 跑一次真实执行（headless app + 模型；会消耗一次调用）
dsh --profile dp-design-pack "调用 design_pack_read 读取 slice-design-pack-mvp.pack.md，只回字节数"
```

**箱上注意**：corepack 的 `lastKnownGood` 指向 pnpm `12.5.1`，而该版本是 `bin/pnpm.mjs`
（corepack 0.34 找 `bin/pnpm.cjs`）→ 直接 `dsh plugin` 会报 pnpm 缺失。绕过：
用本机已缓存的 9.15.9，例如把 `pnpm` shim 到
`node ~/.cache/node/corepack/v1/pnpm/9.15.9/bin/pnpm.cjs`，再执行上面命令；
另因 profile 是 `packages: [.]` 的单包 workspace，pnpm 9 需 `-w`。

回滚（dev profile）：

```bash
dsh plugin --profile dp-design-pack remove -w dsh-design-pack   # 或
rm -rf ~/.dsh/profiles/dp-design-pack
```

产品回滚（若曾被误加）：从对应 profile `package.json` 的 `dependencies` 去掉本包，
并确认 `dsh.profile.bundles` 不再含 `dsh-design-pack` —— 但**本 slice 未这么做**。

## 文件

| 路径 | 说明 |
|------|------|
| `lib/pack.js` | 纯核心：路径约束、大小拒绝、工具定义（无 dsh-tools 依赖） |
| `lib/index.js` | Cordis `apply`：校验 config 并 `ctx.tools.register` |
| `test/pack.test.mjs` | 自测（node:test，13 例） |
| `cordis.patch.yml` | bundle insert（默认 config） |
| `schemas/design-pack.schema.json` | pack front-matter 字段约定 |
| `examples/slice.pack.md` | 示例 pack |
| `skill/SKILL.md` | 开/关票纪律 |

**为何不用 `defineTool`**：`file:`/`link:` 试装时包的 realpath 在 profile `node_modules`
之外，裸 import `@deepseek-ai/dsh-tools` 会解析失败。注册表接受纯对象，只需
name/description/parameters/output/execute；参数校验在 `lib/pack.js` 内完成，
schema 仍通过真实注册表同款 `assertObjectJsonSchema` / `assertSupportedJsonSchema` 自测。

详见：`/workspace/docs/2026-09-24-dsh-route-c-plugin-gap.md`
