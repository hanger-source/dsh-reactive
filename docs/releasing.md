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

框架优先以补丁和小版本完成增量建设，每一批同时落到相关公共合同、实现、直接消费者和版本指导。缺陷修复使用补丁版本，现有模型能够自然组合的新能力使用小版本。只有能力建设开始要求改变公共模型、状态 owner、生命周期责任或依赖方向，而继续叠加会造成重复状态、双轨执行或持续机械装配时，才进入大版本调整。版本升级由实际合同变化决定，不按改动文件数、示例数量或预定时间触发，也不为了维持版本号保留已退出的旧链路。

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

`pack:workspaces` 仅验证六个 workspace 能被 npm 打包，跳过 lifecycle；它不是带源码追溯的发行候选，也不表示 registry 已发布。日常 CI 将实际 tgz 安装到仓外消费者，严格编译并实际加载公开能力；原生 UI 的物理安装同时按 browser 条件完整打包纯合同、SDK 与可选 UI，核验无外部导入、后台 renderer 或另一份 core。日常 CI 与发行验收共用这些消费者和唯一编译配置。发行工作流额外检查来源与 receipt，并执行 registry 发布和安装验收。

## 准备可审阅候选

使用版本提交的完整 SHA。`prepare` 要求当前 HEAD 正是该 SHA 且 Git clean；其他聊天的未提交文件不能被忽略或混入版本提交。存在并发改动时，用该提交的独立干净 checkout，按其锁执行 `npm ci` 后运行发行命令。下列 `VERSION` 必须替换为该提交中六包的精确版本；候选版选择 `next` 渠道与对应预发布版本。

```sh
npm run release -- prepare --channel stable --commit FULL_SHA --tag dsh-reactive@VERSION --out /absolute/release-candidate
npm run release -- verify-artifacts --manifest /absolute/release-candidate/release-manifest.json
```

prepare 只执行一次总 build，然后 npm pack（`--ignore-scripts`）。对每个最终 tgz 运行 publint，不再次触发 prepack/build。每个包包含 `release.json`，记录真实源码 commit、拟用 tag、channel、registry、六包版本和锁摘要；外部 manifest 保存各 tgz 的 SHA-512 完整性。打包 staging 不修改源码树，也不在包内写入自身 tarball hash。

候选验证将 tgz 安装到仓外物理目录，分别验证默认后台 React 19、选择全部可选原生域的 React 19、原生 UI React 18 消费者，以 `skipLibCheck:false` 编译并运行同一份消费者。原生域所需的 optional peers 从实际宿主 tgz 的 package.json 取得，不借工作区 node_modules 补齐；Jobs、MCP、Approval、Clock、Schedule、Team、Files 子入口均实际加载。日常 CI 与发行沿用相同环境选择和消费者源码，后台默认安装仍独立验证。Skill 通过真实 `getDeveloperSkill()` 和 `dsh-reactive-skill` CLI 定位安装内容，核对版本、入口与五个 references。这个阶段只证明候选产物可消费，不称为已发布。

## GitHub CI 发布

六个 npm 包分别将以下配置登记为 Trusted Publisher：GitHub owner `hanger-source`、repository `dsh-reactive`、workflow file `release.yml`，不设置额外 environment。发行需要允许 `npm publish` 和 `npm dist-tag`，分别用于发布产物与确认稳定版的默认安装渠道。这些是 npm 侧的信任配置，不是 GitHub Actions Secret。

批量配置发布信任时，使用同一 npm 登录会话和网络出口，在 `npm trust` 写入请求的验证页选择接下来五分钟不再验证，然后连续完成各包配置。该窗口适用于 `npm publish` 与 `npm trust`，不适用于 `npm stage approve`；暂存批准要求逐份证明人工在场。直接发行由已配置的 OIDC 执行，不再逐包请求人工验证。

新包尚无包级设置时，使用官方 staged publishing 完成首次发行。提供同一仓库内已完成 prepare、候选验收通过的发行 run ID：

```sh
gh workflow run release.yml -f tag=dsh-reactive@VERSION -f registration_run=VERIFIED_RUN_ID
```

登记 job 核验原 run 的仓库、工作流、源码提交、prepare 结果及候选完整性，不重新构建或改包。它输出 npm 官方网页登录地址，在临时 runner 中建立会话，将原始 tgz 提交 staging 并附 provenance；结束时撤销该登录会话。不要把凭据写入 Actions Secrets 或聊天。下载 `npm-registration-receipt` 中的 `staging.json`，核对六包版本和完整性后，以 `npm stage approve STAGE_ID` 完成各包 2FA 批准。登记 job 成功仅表示已暂存，尚未代表正式发行完成。

暂存会占用该版本，不能再以 `npm publish` 覆盖。若保留未批准的候选并改走直接 OIDC 发行，先为占位包配置发布信任，再通过 Changesets 生成后续版本；已有发行 tag 和候选产物保持不变。

npm 拒绝新包唯一的暂存记录时，也会撤销其 `0.0.0-stage` 占位包；不要把“先暂存正式版本、再拒绝、再直接发布同版本”当作登记流程。首次批准后为实际包配置上述 OIDC 信任，再重跑原发行 run 的失败 job。publish 会核验已经受理版本的完整性，完成 registry 消费、dist-tags 和 GitHub Release。后续版本直接由 OIDC 发布，不需要首次登记的网页登录步骤。

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

npm 受理后，新版本可能因扫描和分发延迟暂时不可见。脚本先提交整批缺失版本，再核对实际 registry，让各包扫描和分发同时进行。发布命令成功返回后，单包最多等待十五分钟，每五秒重新读取该版本；发现不同完整性或源码提交时立即失败，不把冲突当作传播延迟。超时保留现场，待实际版本可见后重试同一候选的发布 job，已受理版本只核验而不覆盖。

正式版本只允许无 prerelease 的 SemVer；CI 显式传入 `--also-latest`，在 registry 安装验证通过后把 `latest` 移到该正式版本。标签写入成功后同样最多等待十五分钟、每五秒读取一次；期间只允许观察到写入前的标签或目标版本，出现其他版本立即失败。候选版本不能使用此选项。

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
