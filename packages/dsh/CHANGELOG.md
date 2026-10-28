# @hanger-source/dsh-reactive-dsh

## 4.0.0

### Major Changes

- 2fd5838: ProgramSessionClient 表达人工和服务共同的会话读取、输入、Surface 与 Operation 能力，HumanProgramSessionClient 单独表达问答、前台认领及 Goal 控制。host.service(actorId) 返回绑定固定身份的应用视图，服务可创建、打开和恢复会话、编辑自己的输入及上传文件，逐调用不能改写 actor。

  Gateway 通过必填 participant(principal) 签发角色及 actor，替换只返回字符串的 actor 配置；输入、Action、Operation 与上传使用同一受信端口，Invocation 关联同时核验角色与 actor，服务器拒绝服务执行人工控制。新增服务 SDK 工厂，与人工 SDK 复用同一套正式连接、资源与恢复机制。固定 rc.3 的 answer 传输不授予 human 权限，none 连接不伪装为具有完整恢复能力。角色错配错误保留宿主已确认的会话身份。开发指导及仓外消费者同步此合同，迁移见 docs/migration-participant-ports.md。
- 6847a95: 根会话与集合成员通过 module(mount) 借出按局部 Surface 编程的 SDK，保留 source/selected 合同、原始资源对象、参数类型和本地/远程调用时序；模块不再要求消费函数知道根路径。挂载与父选择共同限制可见性，投射不创建缓存、业务 owner 或控制权限。

  ProgramSessionClient 统一权威请求核对，HumanProgramSessionClient 提供暂停和取消；控制回执与请求状态类型归核心合同，client 重导出同一类型。调用级 signal 及句柄退出只结束本次等待，已经进入原生控制的工作继续完成。服务身份可读请求状态但不能取得人工控制。远程 journal 保留正式 carrier 的专有合同。

  本地 cancel 现在返回 ProgramCancelResult，而非 Promise<void>；自定义 SurfaceClient 和 ProgramSessionClient 实现需提供同一模块与会话能力。迁移方式见 docs/migration-sdk-modules.md。同版本开发 Skill、本地与远程直接消费者、实际 Gateway 和三环境包消费入口同步此合同。

  dsh 发行声明排除已标记 internal 的构造及装配入口，公开观察类型直接来自核心合同；本地 SDK 不再因内部宿主类型牵入持久投影 Registry 的实现声明。运行时装配继续使用同一实现，跨包内核合同保持独立出口。

### Minor Changes

- 337f349: 应用模块通过 ApplicationAssembly.native 与 operation 一次选择原生能力及恢复 owner，不再要求外围重列安装和恢复清单。独立 Runtime 与嵌入 mountApplication 共用应用安装 Fiber；嵌入宿主显式提供能力安装 home，factory 返回后封闭装配，同一实际对象只登记一次，业务 owner 保留原执行责任。

  Host 的业务排空与安装退出分别由原 owner 负责，公开 dispose 与父作用域退出都按排空、观察装配、原生能力的顺序等待实际清理，退出后撤下应用监听。启动失败释放已经选择的能力并保留原始失败，借入服务及同宿主其他应用继续使用。同版本开发指导与仓外消费入口同步此合同。

### Patch Changes

- 0b94998: 会话域统一签发精确访问并裁决成员和代际，事实投影只借用只读查询。原生 Program 声明安装使用实际 Agent 下的正式服务依赖 Fiber；Context、Tool、Skill、人工参与和 Operation 续接共用同一安装作用域，Program 失败与 Agent 关闭均撤销声明并等待实际异步清理，已受理业务仍由原 owner 结算。

  冷读取在求值前核对预期 Program 合同；并发恢复等待同一次原生取得与子恢复，失败只回滚本次取得的句柄，保留已有冷图及其他会话。构造、恢复和退出保留原始失败并汇总真实清理错误。同版本开发指导同步这些归属与失败语义。
- Updated dependencies [2fd5838]
- Updated dependencies [6847a95]
- Updated dependencies [f8d99d1]
  - @hanger-source/dsh-reactive@4.0.0

## 3.0.0

### Minor Changes

- dc676c6: Workspace提供直接的resource与reaction绑定入口，复用既有参数身份、Resource、Reaction和Scope；本地application.workspace(id)与组件props.workspace取得同一对象，在没有Session时也可构造、读取和显式运行。工作区编程合同及内核装配归core/workspaces，公共类型从根入口与/workspaces导出。跨环境、会话共享，同实现与目标在不同工作区隔离。正式工作区登记删除或宿主退出结束其owner，撤销旧资源与排队关系，保留已受理业务；清理重入不能复活已经删除的owner。共享连接在来源工厂执行前登记实际订阅，工厂内同步退出立即关闭它；直接释放来源后结束观察，使teardown失败返回Scope。删除后的清理失败保留到宿主退出汇总，其他工作区仍完整释放。同步开发指导和仓外消费入口。

### Patch Changes

- Updated dependencies [1c9e7ee]
- Updated dependencies [dc676c6]
  - @hanger-source/dsh-reactive@3.0.0

## 2.1.0

### Minor Changes

- 5b81e34: 新增/subagents公共子入口，以createNativeSubagents接入宿主已有的正式子Agent服务。forSession(source).catalog提供同源资源，保留one-shot、continuable和external持久目录身份；readDescendants使用原生递归查询并保留分支诊断。父会话目录及实际Agent代际变化撤下旧有效性，冷读不加载Agent、创建Goal或投递消息；读取取消立即结算，SDK退出不停止原生子执行。NativeChildWork继续专用于带独立Goal的子程序委托，不把普通发现转换为Operation或复制执行引擎。公共exports、仓外消费者和同版本开发指导同步覆盖该入口。
- d502e72: 公开集合成员与Action目录的同源资源：collection.instances与session.actionManifest可供普通读取、Promise、RxJS和hooks直接消费；对应值流继续借用同一查询，不复制状态、建立额外缓存或打开额外连接。本地、远程、服务参与端口及嵌套集合共用公共合同；嵌套目录随父模块撤下报告error，恢复后继续更新，SDK退出撤销借出读侧。目录只描述当前声明，不授予执行权限。同步仓外类型消费者和版本化开发指导。

### Patch Changes

- Updated dependencies [d502e72]
  - @hanger-source/dsh-reactive@2.1.0

## 2.0.0

### Major Changes

- 581b301: 统一资源有效性与原生依赖的生命周期：owner撤销先交付原reason的error Observation再完成，迟到观察只取得撤销而不回放旧业务值，values$在自身owner退出时正常完成。父Scope撤销立即传播到后代，读取拒绝保留同一原因，实际清理仍由Scope树汇总。九个原生能力按实际配置声明正式Fiber服务依赖，provider卸载结束旧能力及其读侧，不随替换服务重定向，并释放本实例安装范围。Jobs未选工具时明确贡献空目录且不依赖ToolRuntime。更新同版本Skill及迁移说明，见docs/migration-resource-ownership.md。

### Patch Changes

- Updated dependencies [581b301]
  - @hanger-source/dsh-reactive@2.0.0

## 1.1.0

### Patch Changes

- 66a1d07: abortable 接受外部 AbortSignal，以原取消原因结束本次观察与 Promise 等待，忽略不配合取消的迟到结果。客户端等待复用同一互操作；Schedule 只读查询、冷 Team 投影和显式 Files 读取不再被迟到的原生回执阻塞。Files 保留实际 I/O 的退出等待；原生历史读取继续传递查询信号，不取消已受理业务或其他消费者。
- 7b3ef97: 原生能力退出加入外层已经开始的 Fiber 清理，观察释放失败保留到实际资源静止后报告。Runtime、Application、Preset 与动态 Skill 在撤销前确定同一个退出回执，避免同步清理重入启动第二次退出；Skill provider 的退休等待真实 Fiber。应用安装沿同一正式插件就绪检查核验并回收失败范围。
- Updated dependencies [66a1d07]
- Updated dependencies [b4af2f5]
  - @hanger-source/dsh-reactive@1.1.0

## 1.0.0

### Minor Changes

- 60f19d6: 本地与远程 SDK 共用 core 的 ProgramSessionClient 和 SurfaceClient 消费合同，根会话与集合成员可直接供同一个模块使用。保留各端的调用时序、权限与资源 owner，不增加代理状态；远程会话公开同一纯 Surface，本地人工观察直接来自原 human 资源。

### Patch Changes

- Updated dependencies [dd50bb1]
- Updated dependencies [60f19d6]
  - @hanger-source/dsh-reactive@1.0.0

## 0.20.2

### Patch Changes

- Updated dependencies [9aca405]
  - @hanger-source/dsh-reactive@0.20.2

## 0.20.1

### Patch Changes

- @hanger-source/dsh-reactive@0.20.1

## 0.20.0

### Minor Changes

- fdab096: Action 以名称与正整数版本声明跨进程合同身份，新定义省略 version 时为 1。远程解析与业务受理前拒绝不匹配合同，原 Invocation 回执、原生展示与子程序委托恢复保留原版本，不再用当前同名合同解释历史结果。

  迁移时必须同步更新宿主与客户端。自建 wire 调用须提供 contract；原生 manifest 增加 actionVersion，reactiveAction metadata 格式升至 version 2 并增加 actionVersion，委托与子会话记录也要求 actionVersion。缺少这些字段的旧记录不能直接恢复；保留相应旧版本消费者，或由业务明确核验历史合同后进行数据迁移，框架不自动猜测版本。

### Patch Changes

- Updated dependencies [fdab096]
  - @hanger-source/dsh-reactive@0.20.0

## 0.19.2

### Patch Changes

- @hanger-source/dsh-reactive@0.19.2

## 0.19.1

### Patch Changes

- Updated dependencies [84f447f]
  - @hanger-source/dsh-reactive@0.19.1

## 0.19.0

### Minor Changes

- f690a47: Jobs 与 MCP 的 toolNames、MCP 的 resourceToolNames 提供正式 ResourceReader，普通函数、Promise、Observable 与 hooks 共用实际工具注册事实。已有值流直接取自资源，最后消费者退出只释放查询，能力退出结束旧读取，原生连接、执行及准入仍由原 owner 负责。

  NativeTools 接受显式空名称集合并贡献零个声明，使异步准备或全部工具撤销能直接组合；无效名称、重复项和准入配置继续拒绝。

### Patch Changes

- Updated dependencies [f690a47]
  - @hanger-source/dsh-reactive@0.19.0

## 0.18.2

### Patch Changes

- 65812d1: Skill 步骤准备在声明、原生 catalog 或本会话工作目录变化时取消旧查询并重新准备，避免不配合取消的 provider 阻塞当前集合。共享 provider 和其他会话保持原有寿命，迟到正文不写回，当前读取错误继续传播。
- @hanger-source/dsh-reactive@0.18.2

## 0.18.1

### Patch Changes

- Updated dependencies [48fbba0]
  - @hanger-source/dsh-reactive@0.18.1

## 0.18.0

### Minor Changes

- f13c00f: 原生 Approval 的会话对象提供同源 policy 资源，read、peek、观察、hooks 与跨能力组合共用正式会话政策，policy$ 直接使用其值流。会话切面读取保留 pending/error，原生代际和政策日志变化撤下旧 ready 并重新读取，错误可随正式通知恢复。冷读不恢复 Agent，政策读取不产生批准或 grant，审计事件与显式 setPolicy 继续由原生 owner 管理。仓外原生消费者与同版本开发指导同步覆盖政策组合。

### Patch Changes

- @hanger-source/dsh-reactive@0.18.0

## 0.17.2

### Patch Changes

- @hanger-source/dsh-reactive@0.17.2

## 0.17.1

### Patch Changes

- Updated dependencies [d158683]
  - @hanger-source/dsh-reactive@0.17.1

## 0.17.0

### Patch Changes

- Updated dependencies [db574d1]
  - @hanger-source/dsh-reactive@0.17.0

## 0.16.0

### Minor Changes

- 469c5c5: 提供真实 Agent 作用域的原生 Tool 目录资源，沿正式注册表处理注册、撤销、遮蔽、restriction 与会话代际变化；只暴露纯 schema 数据，执行继续经过原生 guard 和 Program 政策。Tool 与 Skill 共用 Agent 资源读取接点，统一冷会话 pending、读取取消和旧代际隔离。仓外消费者验证 Tool/Skill 资源组合，版本化开发指导明确目录与实际执行准入的边界。

### Patch Changes

- @hanger-source/dsh-reactive@0.16.0

## 0.15.0

### Minor Changes

- cb08a3b: 提供运行时 Skill 的会话目录与固定名称正文资源，复用宿主原生 Registry、真实 Agent 层级、工作目录和变更通知。普通读取、RxJS 与 hooks 共用正式 Resource；保留不完整发现与调用政策，支持显式失效、读取错误恢复、冷会话等待及真实代际切换，观察退出不撤销 Skill 注册或启动模型。仓外消费验证包含 skills 公共入口，同版本开发指导说明其 owner 与使用方式。

### Patch Changes

- @hanger-source/dsh-reactive@0.15.0

## 0.14.0

### Patch Changes

- Updated dependencies [8dc0fb9]
  - @hanger-source/dsh-reactive@0.14.0

## 0.13.0

### Patch Changes

- @hanger-source/dsh-reactive@0.13.0

## 0.12.1

### Patch Changes

- @hanger-source/dsh-reactive@0.12.1

## 0.12.0

### Patch Changes

- @hanger-source/dsh-reactive@0.12.0

## 0.11.1

### Patch Changes

- Updated dependencies [9fb4b1a]
- Updated dependencies [be26221]
  - @hanger-source/dsh-reactive@0.11.1

## 0.11.0

### Minor Changes

- 19c9a36: Program、本地与远程 SDK 提供 progress 资源及 Request.state，统一当前读取、Observable 与 hooks 的消费，沿用原请求关联、原生活跃前缀和会话连接。原生 UI 按需借入相同读侧，绑定本身不额外启动进展连接；单次读取取消和 UI 释放不停止原生工作。createRequestHandle 的 progress 参数改为资源对象并显式接收读侧 owner；框架入口自动完成这些装配。

### Patch Changes

- Updated dependencies [19c9a36]
  - @hanger-source/dsh-reactive@0.11.0

## 0.10.0

### Minor Changes

- 8060a41: Program、本地和远程 SDK 提供同源 human 资源；question(callId) 返回固定原问题的可复用对象，将状态观察、回答和前台等待放在同一对象上。问题状态保留 active、foreground、settled 原事实，不建立新的阶段、计时器或批准权。可选原生 UI 完整保留 pending/error，问题控制沿原 UI generation 与 SDK 准入。服务端口的人工回答和 Goal 控制在运行时明确拒绝，绕过类型声明也不能借用人工身份。

### Patch Changes

- Updated dependencies [8060a41]
  - @hanger-source/dsh-reactive@0.10.0

## 0.9.1

### Patch Changes

- f6050e9: 为 dsh 直接使用的 LLM/附件合同和原生 UI 的公开 Session 类型补齐正式依赖，避免借用工作区或传递安装。依赖边界检查同时核验声明、运行时 devDependency 与跨包相对导入。日常 CI 和发行验收共用仓外编译配置，并从实际安装包按 browser 条件完整打包纯合同、SDK 与可选 UI，检查外部导入、后台 renderer 和 core 模块身份。
- @hanger-source/dsh-reactive@0.9.1

## 0.9.0

### Patch Changes

- Updated dependencies [7a31aa8]
  - @hanger-source/dsh-reactive@0.9.0

## 0.8.0

### Minor Changes

- 7451ffd: Jobs、Schedule、Team 的会话 SDK 和固定任务提供 state 资源，Job 的 output 提供非消费输出资源；函数、Promise、RxJS、hooks 和公开 View 共用正式资源机制。Schedule 与 Team 的原生变化先撤销旧 ready，异步读取失败作为可恢复的 error，后续通知继续读取。最后资源消费者退出取消读取且不停止已受理原生执行；各域的控制、单次权威查询和持久性责任保持独立。

### Patch Changes

- @hanger-source/dsh-reactive@0.8.0

## 0.7.0

### Minor Changes

- 424b75d: 本地、远程及动态集合提供稳定的 viewResource 读侧对象，普通函数、Promise、Observable、程序 hooks 与可选原生 UI 共用现有 View 状态和 SDK 读取寿命。ResourceReader 统一已有快照的读取、取消与终止结算，不建立新缓存或来源连接。自定义 SurfaceCollectionTransport 以 viewResource 返回已有读侧，取代分开的 observeView/readView 传输方法。

### Patch Changes

- Updated dependencies [424b75d]
  - @hanger-source/dsh-reactive@0.7.0

## 0.6.0

### Minor Changes

- 4596edd: Files 的文本页、元数据与目录提供同一正式 Resource 实例，支持普通 read、共享观察和程序 hooks。首次观察在原生 watch ready 后读取，同路径查询共享 watch，失效取消旧读，查询退出释放原生监听；目录变化不按目录版本过滤。文件范围直接读取已核验会话 header，不再为文件 I/O 读取或冲刷会话日志，关闭 Agent 不终止仍有效来源的文件观察。独立字节缓冲区与异步 unwatch 保留原边界，产物沿既有资源 Surface 对本地和远程公开，开发 Skill 与仓外类型消费者同步。

### Patch Changes

- @hanger-source/dsh-reactive@0.6.0

## 0.5.0

### Minor Changes

- 339cacf: Team 提供经原任务板核验的固定任务对象 task(id)，state$、read 和显式 update 共用原生投影与 CAS。冷 Lead 可读取、绑定，删除后原对象读侧返回 undefined，写入仍要求实际活跃成员，调用参数不能改指其他任务。Team 投影观察接入实际 Agent 关闭、恢复，取消在途旧切面，不复制任务状态、不创建成员或启动模型；开发 Skill 同步组合与生命周期语义。

  日常 CI 与发行增加可选原生域的仓外消费者，共用环境选择，从实际宿主 tgz 的 optional peer 声明安装所选依赖，严格编译并实际加载七个原生子入口；默认后台与原生 UI 的独立安装验证继续保留。

### Patch Changes

- @hanger-source/dsh-reactive@0.5.0

## 0.4.0

### Patch Changes

- baaa98b: 原生长期切面和日志观察跟随实际 Agent 的关闭与恢复，取消旧 owner 的在途读取并接续当前原生日志，避免迟到切面覆盖新现场或重复交付事件。原生挂载只保留一份可观察引用，输入、人工等待与 Operation 共用该事实；观察不自动恢复会话或启动模型，真正读取错误保持原语义。开发 Skill 同步说明观察寿命。
- Updated dependencies [923671d]
  - @hanger-source/dsh-reactive@0.4.0

## 0.3.0

### Minor Changes

- 1448bec: 原生 Jobs、Schedule、Team、Files、Approval 和 Clock 接受宿主核验的 Program conversation，普通函数与业务 hooks 共用会话能力实例。原生观察延迟到首次订阅，Program 失败或退出只释放其来源观察，不取消已受理工作。根会话在 Program 构造前持久完成工作区归属，创建 ACK 前不签发外部句柄；本地与远程 open/resume 保留同一创建引用，沿原入口完成登记。恢复继续核验成员撤销；开发 Skill 同步说明来源与生命周期。

### Patch Changes

- Updated dependencies [f713b74]
- Updated dependencies [e4273f8]
  - @hanger-source/dsh-reactive@0.3.0

## 0.2.2

### Patch Changes

- Updated dependencies [5f53764]
  - @hanger-source/dsh-reactive@0.2.2

## 0.2.1

### Patch Changes

- ed8c8dc: 明确六包 TypeScript 构建依赖与客户端合同的唯一产物来源，严格编译并执行仓外消费者；以 GitHub OIDC 发布可追溯的同步版本及开发 Skill，并从 npm registry 安装验收。
- Updated dependencies [ed8c8dc]
- Updated dependencies [871403f]
  - @hanger-source/dsh-reactive@0.2.1

## 0.2.0

首次公开源码。

- 提供原生 dsh 宿主、多工作区与会话、统一输入和文件上传、工具来源准入、人工问询及可靠 Operation 等待。
- 提供可选 Jobs、MCP、Approval、时间、现存 Schedule 管理、Team 任务板和工作区文件 SDK。
- Schedule 创建、Team spawn/send 与原生 Controller 输入前门仍受 dsh rc.3 公开接点限制。
