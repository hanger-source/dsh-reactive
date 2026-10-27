# 六包发行

根目录是 private workspace。以下六包由 Changesets fixed 组同步版本，内部运行依赖和 peer 依赖使用同一个精确版本：

| 包 | 源码目录 |
| --- | --- |
| `@hanger-source/dsh-reactive` | `packages/core` |
| `@hanger-source/dsh-reactive-dsh` | `packages/dsh` |
| `@hanger-source/dsh-reactive-client` | `packages/client` |
| `@hanger-source/dsh-reactive-remote` | `packages/remote` |
| `@hanger-source/dsh-reactive-native-ui` | `packages/native-ui` |
| `@hanger-source/dsh-reactive-skills` | `packages/skills` |

源码仓库为 [hanger-source/dsh-reactive](https://github.com/hanger-source/dsh-reactive)，作者为 hanger。当前六包源码版本为 `0.2.0`，不表示公共 npm 已发布。提交或推送 GitHub 与 npm 发行分别授权；只有明确要求 npm 发布时才执行本页的发布命令。

发行使用 Node.js 24.18 或更高的 24.x。目标 registry 为 `https://registry.npmjs.org`，配置保存在 `scripts/release.config.json` 和各包 `publishConfig`，scoped 包显式使用 `access: public`。源码构建、CI 和候选 tgz 消费不需要发布凭据；发布认证沿正式 npm 用户配置读取，脚本不读取、打印或复制 token。`UNLICENSED` 表示当前未授予开源许可，公开源码或 npm 可见性不改变该许可事实。

Changesets 负责 SemVer、预发布递增、依赖更新与 CHANGELOG；发行脚本负责来源、产物和 registry 边界。固定版本组和预发布使用[官方 fixed 机制](https://changesets.dev/guide/fixed-packages)与[预发布模式](https://changesets.dev/guide/prereleases)。

## 记录与版本化

在提交功能时同时记录用户可感知的变化：

```sh
npm run changeset -- --minor @hanger-source/dsh-reactive --message "说明实际新增能力与行为边界"
```

fixed 组会统一提升六包。修复使用 `--patch`，不兼容变化按正式 SemVer 选择相应变更类型，不手工编写版本递增算法；实际版本以 Changesets plan 和生成的 CHANGELOG 为准。

变更说明提交后，在干净源码 checkout 中应用版本：

```sh
npm run release -- plan
npm run release -- version --channel stable
```

需要候选版时选择 `version --channel next`，使用标准 `changeset pre enter next` 与 `changeset version`。下一次候选继续同一预发布模式；正式版本用 `version --channel stable` 执行 `pre exit` 后再 version。脚本统一更新锁，但不 commit。检查六包版本、内部依赖、各包 CHANGELOG、Skill optional peers 与锁后，运行本批相称的检查并提交版本变更。

`pack:workspaces` 仅验证六个 workspace 能被 npm 打包，跳过 lifecycle；它不是带源码追溯的发行候选，也不表示 registry 已发布。GitHub Actions 另将实际 tgz 安装到仓外消费者严格检查公开类型，仍不执行 npm 发布。

## 准备可审阅候选

使用版本提交的完整 SHA。`prepare` 要求当前 HEAD 正是该 SHA 且 Git clean；其他聊天的未提交文件不能被忽略或混入版本提交。存在并发改动时，用该提交的独立干净 checkout，按其锁执行 `npm ci` 后运行发行命令。下列 `VERSION` 必须替换为该提交中六包的精确版本；当前源码为 `0.2.0`，候选版则需选择 `next` 渠道与对应预发布版本。

```sh
npm run release -- prepare --channel stable --commit FULL_SHA --tag dsh-reactive@VERSION --out /absolute/release-candidate
npm run release -- verify-artifacts --manifest /absolute/release-candidate/release-manifest.json
```

prepare 只执行一次总 build，然后 npm pack（`--ignore-scripts`）。对每个最终 tgz 运行 publint，不再次触发 prepack/build。每个包包含 `release.json`，记录真实源码 commit、拟用 tag、channel、registry、六包版本和锁摘要；外部 manifest 保存各 tgz 的 SHA-512 完整性。打包 staging 不修改源码树，也不在包内写入自身 tarball hash。

候选验证将 tgz 安装到仓外物理目录，分成后台 React 19 和原生 UI React 18 消费者，以 `skipLibCheck:false` 编译并加载正式入口。Skill 通过真实 `getDeveloperSkill()` 和 `dsh-reactive-skill` CLI 定位安装内容，核对版本、入口与五个 references。这个阶段只证明候选产物可消费，不称为已发布。

## 显式 tag 与发布

候选通过后创建 annotated Git tag；命令不 push：

```sh
npm run release -- tag --channel stable --commit FULL_SHA --tag dsh-reactive@VERSION
npm run release -- publish --manifest /absolute/release-candidate/release-manifest.json
```

publish 再核 clean、HEAD、真实 tag 对应的 commit、版本、CHANGELOG、依赖闭包、锁摘要、候选验证和 tgz 完整性。它只向公共 npm 发布已准备的 tgz，显式使用选定的 `--tag stable` 或 `--tag next` 和 `--access public`；根 workspace 不发布。脚本不提供私有 access 选项，也不自动推送 GitHub。

候选版必须使用 `-next.N` 版本与 `next` 渠道；发布前保存各包 dist-tags，候选发行不得改变已有 `latest` 或 `stable`。未知变化硬失败并保留回执，不自动删除或改写其他标签。已受理的部分发行是 registry 事实，不能通过删除现场或修改报告将其改称成功。

同一版本不可覆盖。部分发行继续时只接受 registry 上完整性与本次候选完全相同的版本；不使用 `--force`、忽略依赖或切 registry。CLI 受理未知、认证失败或实际写权限不足都会保留失败现场，需按 registry 事实处理。

正式版本只允许无 prerelease 的 SemVer，默认显式写 `stable`。只有发布命令另外给出 `--also-latest`，才在 registry 安装验证通过后把 `latest` 移到该正式版本。候选版本不能使用此选项。

## Registry 安装验收

publish 完成前会从指定 registry 按 `name@version` 重新安装六包；也可以独立重核：

```sh
npm run release -- verify --manifest /absolute/release-candidate/release-manifest.json
```

该验证不接受 tgz 替代 registry。它比对实际 registry 元数据、dist integrity、dist-tags、安装锁中的 registry tarball 位置和完整性，再运行两个仓外消费者。安装后的 `release.json` 必须对应同一 commit/tag/六包版本；Skill 的实际 API、CLI 与包内 references 必须可用。发布成功以此报告和 registry 受理事实为准。

`candidate-verification.json`、`registry-verification.json` 与 `publication.json` 保存在候选目录，分别记录候选消费、registry 消费和渠道治理。所有 Git push 都是独立的显式操作；脚本不推送分支或标签。

## 使用配套开发 Skill

六包发行后，选择 registry 上实际存在的精确版本安装配套 Skill；以下 `VERSION` 必须替换为该版本，不能从源码版本推定包已可安装。

```sh
npm install --save-dev @hanger-source/dsh-reactive-skills@VERSION
npx --no-install dsh-reactive-skill
```

也可以调用 `getDeveloperSkill()`，取得当前安装版本的 Skill 入口和 references 路径。CLI 与 API 只定位包内内容，不注册全局 Agent 配置，也不安装运行时业务 Skill。源码开发时先完成工作区 build，再使用同一 CLI 读取该源码版本生成的内容；跨版本指导不能替代应用实际安装的公开合同。
