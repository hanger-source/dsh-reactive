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

源码仓库为 [hanger-source/dsh-reactive](https://github.com/hanger-source/dsh-reactive)，作者为 hanger。源码版本和 GitHub 提交不代表公共 npm 已发行；发布以 registry 受理和安装验收为准。普通分支推送运行日常 CI，推送 annotated 发行 tag 会运行独立的 npm 发行工作流。

发行使用 Node.js 24.18 或更高的 24.x。目标 registry 为 `https://registry.npmjs.org`，配置保存在 `scripts/release.config.json` 和各包 `publishConfig`，scoped 包显式使用 `access: public`。源码构建、CI 和候选 tgz 消费不需要发布凭据。GitHub 发行使用 npm Trusted Publishing 与 provenance，不配置 `NPM_TOKEN`；publish job 单独取得 `id-token: write`。`UNLICENSED` 表示当前未授予开源许可，公开源码或 npm 可见性不改变该许可事实。

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

`pack:workspaces` 仅验证六个 workspace 能被 npm 打包，跳过 lifecycle；它不是带源码追溯的发行候选，也不表示 registry 已发布。日常 CI 将实际 tgz 安装到仓外消费者，严格编译并实际加载公开能力；发行工作流则额外检查来源与 receipt，并执行 registry 发布和安装验收。

## 准备可审阅候选

使用版本提交的完整 SHA。`prepare` 要求当前 HEAD 正是该 SHA 且 Git clean；其他聊天的未提交文件不能被忽略或混入版本提交。存在并发改动时，用该提交的独立干净 checkout，按其锁执行 `npm ci` 后运行发行命令。下列 `VERSION` 必须替换为该提交中六包的精确版本；候选版选择 `next` 渠道与对应预发布版本。

```sh
npm run release -- prepare --channel stable --commit FULL_SHA --tag dsh-reactive@VERSION --out /absolute/release-candidate
npm run release -- verify-artifacts --manifest /absolute/release-candidate/release-manifest.json
```

prepare 只执行一次总 build，然后 npm pack（`--ignore-scripts`）。对每个最终 tgz 运行 publint，不再次触发 prepack/build。每个包包含 `release.json`，记录真实源码 commit、拟用 tag、channel、registry、六包版本和锁摘要；外部 manifest 保存各 tgz 的 SHA-512 完整性。打包 staging 不修改源码树，也不在包内写入自身 tarball hash。

候选验证将 tgz 安装到仓外物理目录，分成后台 React 19 和原生 UI React 18 消费者，以 `skipLibCheck:false` 编译并加载正式入口。Skill 通过真实 `getDeveloperSkill()` 和 `dsh-reactive-skill` CLI 定位安装内容，核对版本、入口与五个 references。这个阶段只证明候选产物可消费，不称为已发布。

## GitHub CI 发布

六个 npm 包分别将以下配置登记为 Trusted Publisher：GitHub owner `hanger-source`、repository `dsh-reactive`、workflow file `release.yml`，不设置额外 environment。发行需要允许 `npm publish` 和 `npm dist-tag`，分别用于发布产物与确认稳定版的默认安装渠道。这些是 npm 侧的信任配置，不是 GitHub Actions Secret。

版本与对应变更已提交并推送至 main 后，创建并推送 annotated tag：

```sh
npm run release -- tag --channel stable --commit FULL_SHA --tag dsh-reactive@VERSION
git push origin dsh-reactive@VERSION
```

`.github/workflows/release.yml` 只接受 main 上的 annotated tag 与对应六包版本。prepare job 在无发布权限的 runner 一次构建产物，完成包边界、核心/Gateway 行为、公开用法和仓外候选消费检查；publish job 下载已验证 tgz，不再次构建，使用 OIDC 发布并重新从 registry 安装。稳定版同时设置 `stable` 和 `latest`，候选版只写 `next`。成功后创建 GitHub Release，附上六包 tgz、来源 manifest 和 registry 验收报告。

同一 tag 的运行需要重试时可重新运行原工作流，或显式指定已有 tag：

```sh
gh workflow run release.yml -f tag=dsh-reactive@VERSION
```

publish 再核 clean、HEAD、真实 tag 对应的 commit、版本、CHANGELOG、依赖闭包、锁摘要、候选验证和 tgz 完整性。它只向公共 npm 发布已准备的 tgz；根 workspace 不发布。失败现场保存 publication 与 registry 验收回执。GitHub Release 已存在时不会覆盖附件或改写已发布内容。

候选版必须使用 `-next.N` 版本与 `next` 渠道；发布前保存各包 dist-tags，候选发行不得改变已有 `latest` 或 `stable`。未知变化硬失败并保留回执，不自动删除或改写其他标签。已受理的部分发行是 registry 事实，不能通过删除现场或修改报告将其改称成功。

同一版本不可覆盖。部分发行继续时只接受 registry 上完整性与本次候选完全相同的版本；不使用 `--force`、忽略依赖或切 registry。CLI 受理未知、认证失败或实际写权限不足都会保留失败现场，需按 registry 事实处理。

正式版本只允许无 prerelease 的 SemVer；CI 显式传入 `--also-latest`，在 registry 安装验证通过后把 `latest` 移到该正式版本。候选版本不能使用此选项。

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
