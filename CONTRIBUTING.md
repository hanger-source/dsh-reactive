# 参与开发

使用 Node.js 24.18.0 或更高的 24.x。项目使用 npm workspaces、提交的 `package-lock.json` 和公开 registry `https://registry.npmjs.org`。从 [GitHub 仓库](https://github.com/hanger-source/dsh-reactive) 获取源码；开发构建和 CI 不需要 npm 发布凭据。不要将凭据写入项目、示例或问题记录。

```sh
npm ci
npm run build
npm run typecheck:examples
npm run check:boundaries
npm run check:packages
npm run test:release
```

先阅读 [README](README.md) 和[架构与责任](docs/architecture.md)。源码和发行包按相同边界组织：

| 路径 | 责任 |
| --- | --- |
| `packages/core` | 资源、反应、Action、Operation、参与、Surface、程序及响应式互操作 |
| `packages/dsh` | 原生宿主、执行来源、输入受理、稳定准备、持久关联及可选原生能力 |
| `packages/client` | 纯传输合同、官方生成的客户端 contribution、类型化远程 SDK |
| `packages/remote` | 已认证 Gateway 宿主，复用同一应用及其准入 |
| `packages/native-ui` | 可选原生 UI 的观察、调用、slot 和节点贡献 |
| `packages/skills` | 面向应用开发者的版本化 Skill 与 references |
| `examples` | 消费正式 exports 的完整应用与分域用法 |
| `test`、`packages/remote/test` | 核心行为与真实进程、持久化、远程边界的试验 |
| `scripts` | 工作区构建、唯一模块身份的测试装配与发行工具 |

根目录是私有工作区，不发布。各包在自己的 `package.json` 中声明 exports、依赖和构建命令；跨包代码只导入正式包入口。内部相对路径、测试路径和构建产物路径不构成公共 API。新增出口必须同时让仓外 strict TypeScript 消费者能够安装和编译。

`check:boundaries` 使用 dependency-cruiser 核验依赖方向、各包自己的 npm 声明和运行时代码不借用 devDependencies。包内相对导入不能越出本包；正式公共入口保留原模块身份。声明文件需要的类型包也是发行依赖，不能依赖根工作区或其他依赖恰好将它安装出来。

仓外消费者也使用相同规则检查源码和编译后入口，依赖按消费者自己的 package.json 核验。后台 RxJS 与可选原生域依赖分别来自实际 core、宿主 tgz 的声明，CI 打包消费和正式候选验收共用环境及入口清单。修改消费代码时同步其直接依赖；TypeScript 和 Node 能解析提升后的间接包不代表依赖声明成立。

core 的根入口和客户端保持无 React、无原生宿主装配；后台组件实现通过 `/program` 加载。dsh 不依赖 Gateway、客户端或 UI。remote 依赖 dsh 和纯 client 合同；native-ui 依赖 client 和官方原生 UI 接点，不反向加载 dsh 或 Gateway。可选原生域使用独立入口与显式 peer，不能让未选择该能力的用户被迫安装它。

构建任务由各包 `package.json` 的 `wireit` 声明，根 `npm run build` 聚合六包任务；`npm run build --workspace <包名>` 也会先完成所需合同、宿主声明与生成贡献。Wireit 根据同一任务图调度、共享前置任务并检查输入及产物，无变化时跳过，不要求调用者记住构建顺序。TypeScript 项目引用保留编译边界和声明身份，各 `build:compile` 只编译自己的项目，不递归重建依赖。

core 是共享合同的唯一编译来源；client 的 wire 合同、错误与 transport 由 `build:contracts` 单独产出，完整 SDK 引用这些声明。根 `generate:remote` 是官方 Typert 跨包生成的唯一任务，依赖 dsh 和 client 合同的已编译声明，拥有 remote 的 Host contribution 与 client 的 Remote contribution；包内 `generate` 只引用这项任务。client 编译完成后才能生成浏览器 bundle；native-ui 只需 client 的模块与声明，不要求先打浏览器 bundle。开发 Skill 独立从模板和当前包版本生成。

每项任务的 `output` 同时声明其产物所有权。编译前按这份清单把旧产物移入 `.trash`，再由 TypeScript 完整生成本项目，避免删除或改名的源码留下旧模块；client 合同、SDK、生成贡献和浏览器 bundle 的输出清单互斥。构建缓存由 Wireit 管理，`lib`、构建信息与资产均可重新生成；缓存和构建工具只属于开发工作区，不进入运行时依赖。发行准备使用同一个根构建，打包时不重复运行 prepack。

`docs` 和根 `examples` 是说明及示例的唯一源。core 构建将选定资产复制入包；不要手工编辑其生成副本。客户端 `lib/typert.remote-client.*` 由官方 Typert 工具从远程宿主合同生成，修改服务合同后运行根构建，不手改生成内容。`lib`、`.tmp`、打包文件与本地运行数据不提交。

## 修改和验证

先确认事实 owner、执行身份、生命周期及失败边界，再修改完整调用链。新增业务场景应主要组合已有能力，不应持续修改中央宿主。业务权限和正式完成条件归业务 owner；dsh 拥有模型循环、Goal、原生输入和持久日志。不要另建镜像状态，也不要修改共享上游源码来掩盖公开接点缺失。

一批生产代码和直接消费者齐备后，运行相应编译与依赖边界检查，再选择真实行为验证：

```sh
npm run test:core
npm run test:remote
```

这两个入口会完成自身需要的构建。已经完成构建时可以直接运行其脚本中对应的 Node test 命令，避免重复全量编译。测试要能在现实的错误执行、权限、并发、持久化或连接边界下失败；不为字段映射、普通文档措辞或通过数量增加测试。

真实模型试验显式选择独立数据目录、provider 和预算，保留精确工作区、会话、输入、调用、操作及日志。模型调用、原生服务试验、包安装和外部业务验收分别说明证据，互不替代。

[日常 GitHub CI](.github/workflows/ci.yml) 使用公开 npm 依赖，完成构建、边界检查、核心与 Gateway 试验、示例严格编译，并将实际 tgz 安装到仓外的默认后台、可选原生能力与原生 UI 消费者，以 `skipLibCheck:false` 编译并执行。原生 UI 使用独立的 React 与 ReactDOM 18；同一物理安装中的纯合同、SDK、可选 UI 入口及正式 SlotRenderer 消费者按浏览器条件打包，拒绝未打包的外部依赖、后台 renderer、重复 React 或另一份 core。真实 DOM 行为通过这些产物启动浏览器试验，见[发行与消费验证](docs/releasing.md)。CI 保留六包产物供审阅。独立的 [npm 发行工作流](.github/workflows/release.yml) 对 main 上的 annotated 发行 tag 准备、验证并发布同一份产物，使用 GitHub OIDC Trusted Publishing，随后从 npm registry 重新安装验收。

保留并发修改和有效日志。只暂存本批文件，完成一批有相称验证的变化后本地提交；不要混入其他应用、凭据、临时探针或运行数据。

## 公共 API 与版本

公共合同、运行语义、exports、依赖闭包和开发 Skill 都属于发行接口。变更这些内容时运行 `npm run changeset`，写明用户可观察的行为及升级影响。六个发行包使用同一版本；包间依赖与开发指导适用版本一并更新。破坏性变化提供明确迁移用法，不在生产代码中维持默认兼容分支。

阶段提交记录完整工作批次，版本准备统一消费已经收齐的 Changesets；完成一个局部批次不要求立即升版。同一设计持续出现归属、生命周期或消费边界补丁时，先共同重整相关入口、owner 和直接消费者，再确定发行边界。大版本表达确实不兼容的公共模型变化，不能用版本数字代替框架建设进度。

源码版本不能据此声称公共 npm 已发布。GitHub 提交或推送与 npm 发行是独立操作，后者需要明确授权。

候选版使用 `next`；正式版使用单独的稳定发行流程。版本、构建、产物、registry 安装和 tag 按 `npm run release -- --help` 给出的流程执行。打包准备不代表已经发布，发布成功也不代表外部业务验收。发布需要明确授权，不将预发布加入默认安装渠道。

## 反馈与授权

在 [GitHub 项目](https://github.com/hanger-source/dsh-reactive) 提交 Issue 或 Pull Request。问题请提供确切包版本、Node/dsh 版本、使用入口、最小可复现调用及脱敏错误；涉及恢复和归属时附相关业务身份与日志顺序，勿附密钥或业务隐私正文。

项目由 hanger 维护，包的 `license: UNLICENSED` 表示当前没有授予开源许可证。公开可读或可下载不等于授予使用、修改或再分发许可；许可变更须由权利人明确决定。
