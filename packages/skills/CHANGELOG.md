# @hanger-source/dsh-reactive-skills

此文件保留源码版本准备记录；条目和版本号本身不表示 npm 已发布或公共 API 已稳定。

## 0.21.0

### Minor Changes

- 00f0503: 将借用能力与拥有作用域的权限分开。Resource、Reaction、Value、Workspace 和 Conversation 的公开对象不再返回所属 Scope；工作区和会话的实际内核记录显式拥有 Scope，继续负责资源、程序、观察和失败清理。公共 WorkspaceContext 类型退出，业务使用 Workspace；内核构造改为显式传入由调用者拥有的 Scope。

  Team.task、Schedule.task 与 Jobs.job 同步绑定固定来源和目标，不因构造对象读取领域存储；存在性、成员关系和权限继续由真实读取、观察及原生控制边界裁决。Schedule 修改和删除在取消或 SDK 退出时及时结束消费者等待，原生已受理的事务继续结算。

  同版本开发 Skill、迁移说明和仓外消费者一起更新。Skill 构建复用工作区归档机制撤下过期生成文件，避免模板退出后仍将旧 API 指导装入产物。
- 9c5d4b0: 完整模块可以各自贡献相同的会话级 Skill 依赖与原生工具入口，无需把共同依赖搬到应用外围。Skill 按当前声明集合共享，规范化后的同目录只安装一次；只在最后一个贡献退出时撤下依赖，模块重排不重启同一准备。

  原生工具保持其真实全局名称。相同工具仅在 enabled 相同且 admit 为同一函数引用时共享；不同参与条件或准入政策明确拒绝，不自动扩大、合并权限或重命名工具。异步准入后继续核对当前政策，移除一项相同贡献不撤销其他模块仍持有的入口。
- 73e4c02: Workspace 的 resource 与 reaction 直接接受与 Program 相同的局部业务选项，由实际工作区持有既有资源和反应实例。每次局部构造保留独立身份，显式共享返回对象即可在普通函数、Promise、RxJS 和 Program 中复用；正式实现与参数入口继续稳定绑定。构造不读取或执行，Session 退出不结束工作区实例，登记撤销停止读取与未受理队列，已受理业务仍由原 owner 结算。

  Team 创建与更新在调用取消或 SDK 退出时及时结束回执等待，预取消不进入原生事务；已进入的原生 journal 与 CAS 继续持久化，不以取消表示回滚或自动重试。更新同版本开发指导及仓外消费者。

### Patch Changes

- 81d05bc: 输入提交、队列编辑和上传的消费者等待与实际受理分开。并发提交共享同一请求事务，每个调用者可以独立取消等待；共享登记只随实际事务结算退出。Program 来源与句柄退出结束各自等待，已受理输入和编辑仍由原生 owner 完成持久回执，迟到结算失败保留在原会话错误链。

  本地与远程入口统一及时结束回执及 Blob 转换等待，转换取消后不启动上传；保留执行前取消、身份和正文冲突核验。远程提交与编辑只在请求已启动且回执无法确认时报告受理未知，明确业务拒绝和本地格式错误仍保留原错误，不自动重试或撤回已受理工作。
- Updated dependencies [00f0503]
- Updated dependencies [81d05bc]
- Updated dependencies [9c5d4b0]
- Updated dependencies [73e4c02]
  - @hanger-source/dsh-reactive@0.21.0
  - @hanger-source/dsh-reactive-dsh@0.21.0
  - @hanger-source/dsh-reactive-client@0.21.0
  - @hanger-source/dsh-reactive-native-ui@0.21.0
  - @hanger-source/dsh-reactive-remote@0.21.0

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
- f8d99d1: ResourceSource 统一表达资源对象、业务 hook 返回的观察对象和原 Observation 流。资源算子、shareResource、useResource、资源视图及 Context 直接消费同一来源，不要求外围逐层拆开 observations$；普通值视图继续保持值流语义。

  useObservable、useShared、useRelation 可直接接收已有 Observable，现场构造来源继续使用 factory 与显式依赖。useResource 按实际选中的来源决定处理，factory 选择已有资源对象仍借用其原观察流及 owner；自建流继续由程序持有查询与共享。公开视图按原观察流身份保留连接，资源对象与其流之间切换不重新查询。同步公共消费者与同版本开发指导，不增加业务状态、执行权或生命周期。

### Patch Changes

- 0b94998: 会话域统一签发精确访问并裁决成员和代际，事实投影只借用只读查询。原生 Program 声明安装使用实际 Agent 下的正式服务依赖 Fiber；Context、Tool、Skill、人工参与和 Operation 续接共用同一安装作用域，Program 失败与 Agent 关闭均撤销声明并等待实际异步清理，已受理业务仍由原 owner 结算。

  冷读取在求值前核对预期 Program 合同；并发恢复等待同一次原生取得与子恢复，失败只回滚本次取得的句柄，保留已有冷图及其他会话。构造、恢复和退出保留原始失败并汇总真实清理错误。同版本开发指导同步这些归属与失败语义。
- Updated dependencies [337f349]
- Updated dependencies [2fd5838]
- Updated dependencies [6847a95]
- Updated dependencies [f8d99d1]
- Updated dependencies [0b94998]
  - @hanger-source/dsh-reactive-dsh@4.0.0
  - @hanger-source/dsh-reactive@4.0.0
  - @hanger-source/dsh-reactive-client@4.0.0
  - @hanger-source/dsh-reactive-remote@4.0.0
  - @hanger-source/dsh-reactive-native-ui@4.0.0

## 3.0.0

### Minor Changes

- 1c9e7ee: 显式owner的ResourceReader将来源正常完成与资源有效性结束分开：有限来源立即释放真实连接，资源观察和values$继续接受owner撤销，不让活跃的外部hook、Context或视图永久保留旧ready。空完成或pending完成交付EmptyError以结算读取，不重复查询或添加状态缓存。远程Invocation.activity仍与SDK共用同一资源，原生流在终态释放，业务完成改用result或按returned/failed筛选；不再用lastValueFrom(activity$)推断业务完成。未指定owner的本地不可变事实及普通Observable保留原来源的有限语义。更新本地/远程及浏览器直接消费者、原生UI试验和同版本开发指导，迁移见docs/migration-resource-completion.md。
- dc676c6: Workspace提供直接的resource与reaction绑定入口，复用既有参数身份、Resource、Reaction和Scope；本地application.workspace(id)与组件props.workspace取得同一对象，在没有Session时也可构造、读取和显式运行。工作区编程合同及内核装配归core/workspaces，公共类型从根入口与/workspaces导出。跨环境、会话共享，同实现与目标在不同工作区隔离。正式工作区登记删除或宿主退出结束其owner，撤销旧资源与排队关系，保留已受理业务；清理重入不能复活已经删除的owner。共享连接在来源工厂执行前登记实际订阅，工厂内同步退出立即关闭它；直接释放来源后结束观察，使teardown失败返回Scope。删除后的清理失败保留到宿主退出汇总，其他工作区仍完整释放。同步开发指导和仓外消费入口。

### Patch Changes

- Updated dependencies [1c9e7ee]
- Updated dependencies [dc676c6]
  - @hanger-source/dsh-reactive@3.0.0
  - @hanger-source/dsh-reactive-client@3.0.0
  - @hanger-source/dsh-reactive-dsh@3.0.0
  - @hanger-source/dsh-reactive-native-ui@3.0.0
  - @hanger-source/dsh-reactive-remote@3.0.0

## 2.1.0

### Minor Changes

- 5b81e34: 新增/subagents公共子入口，以createNativeSubagents接入宿主已有的正式子Agent服务。forSession(source).catalog提供同源资源，保留one-shot、continuable和external持久目录身份；readDescendants使用原生递归查询并保留分支诊断。父会话目录及实际Agent代际变化撤下旧有效性，冷读不加载Agent、创建Goal或投递消息；读取取消立即结算，SDK退出不停止原生子执行。NativeChildWork继续专用于带独立Goal的子程序委托，不把普通发现转换为Operation或复制执行引擎。公共exports、仓外消费者和同版本开发指导同步覆盖该入口。
- d502e72: 公开集合成员与Action目录的同源资源：collection.instances与session.actionManifest可供普通读取、Promise、RxJS和hooks直接消费；对应值流继续借用同一查询，不复制状态、建立额外缓存或打开额外连接。本地、远程、服务参与端口及嵌套集合共用公共合同；嵌套目录随父模块撤下报告error，恢复后继续更新，SDK退出撤销借出读侧。目录只描述当前声明，不授予执行权限。同步仓外类型消费者和版本化开发指导。

### Patch Changes

- Updated dependencies [5b81e34]
- Updated dependencies [d502e72]
  - @hanger-source/dsh-reactive-dsh@2.1.0
  - @hanger-source/dsh-reactive@2.1.0
  - @hanger-source/dsh-reactive-client@2.1.0
  - @hanger-source/dsh-reactive-remote@2.1.0
  - @hanger-source/dsh-reactive-native-ui@2.1.0

## 2.0.0

### Minor Changes

- 581b301: 统一资源有效性与原生依赖的生命周期：owner撤销先交付原reason的error Observation再完成，迟到观察只取得撤销而不回放旧业务值，values$在自身owner退出时正常完成。父Scope撤销立即传播到后代，读取拒绝保留同一原因，实际清理仍由Scope树汇总。九个原生能力按实际配置声明正式Fiber服务依赖，provider卸载结束旧能力及其读侧，不随替换服务重定向，并释放本实例安装范围。Jobs未选工具时明确贡献空目录且不依赖ToolRuntime。更新同版本Skill及迁移说明，见docs/migration-resource-ownership.md。

### Patch Changes

- Updated dependencies [581b301]
  - @hanger-source/dsh-reactive@2.0.0
  - @hanger-source/dsh-reactive-dsh@2.0.0
  - @hanger-source/dsh-reactive-client@2.0.0
  - @hanger-source/dsh-reactive-native-ui@2.0.0
  - @hanger-source/dsh-reactive-remote@2.0.0

## 1.1.0

### Minor Changes

- 66a1d07: abortable 接受外部 AbortSignal，以原取消原因结束本次观察与 Promise 等待，忽略不配合取消的迟到结果。客户端等待复用同一互操作；Schedule 只读查询、冷 Team 投影和显式 Files 读取不再被迟到的原生回执阻塞。Files 保留实际 I/O 的退出等待；原生历史读取继续传递查询信号，不取消已受理业务或其他消费者。
- b4af2f5: combineResources 和 switchResource 直接组合 ResourceReadable 对象与 Observation 流；公开 ResourceSource 表达同一来源的两种消费入口。保持领域类型、原观察流、共享与 owner，不额外读取、解析或取得业务执行所有权。

### Patch Changes

- 7b3ef97: 原生能力退出加入外层已经开始的 Fiber 清理，观察释放失败保留到实际资源静止后报告。Runtime、Application、Preset 与动态 Skill 在撤销前确定同一个退出回执，避免同步清理重入启动第二次退出；Skill provider 的退休等待真实 Fiber。应用安装沿同一正式插件就绪检查核验并回收失败范围。
- Updated dependencies [66a1d07]
- Updated dependencies [7b3ef97]
- Updated dependencies [b4af2f5]
  - @hanger-source/dsh-reactive@1.1.0
  - @hanger-source/dsh-reactive-dsh@1.1.0
  - @hanger-source/dsh-reactive-client@1.1.0
  - @hanger-source/dsh-reactive-native-ui@1.1.0
  - @hanger-source/dsh-reactive-remote@1.1.0

## 1.0.0

### Major Changes

- bdd5279: SDK、Session及其远程资源统一由正式dsh Fiber/effect持有构造与异步释放，替换各域独立的退出和退休队列。bindProgramClient现在返回Promise并等待作用域安装，直接消费者需要await；ProgramJournal通过session.openEvents取得。取消立即撤销使用权，退出等待实际清理静止并汇总失败，已受理业务继续遵循原owner。迁移方式见docs/migration-sdk-scopes.md。

### Minor Changes

- dd50bb1: 局部 Action 可在同一个 implementAction 选项对象中声明合同与实现，也可由 useAction 的 factory 同时构造实现和明确目标。沿用现有绑定、宿主准入、Invocation 与 Operation owner，不引入额外状态或执行生命周期；共享合同入口继续保持独立。
- 60f19d6: 本地与远程 SDK 共用 core 的 ProgramSessionClient 和 SurfaceClient 消费合同，根会话与集合成员可直接供同一个模块使用。保留各端的调用时序、权限与资源 owner，不增加代理状态；远程会话公开同一纯 Surface，本地人工观察直接来自原 human 资源。

### Patch Changes

- Updated dependencies [dd50bb1]
- Updated dependencies [bdd5279]
- Updated dependencies [60f19d6]
  - @hanger-source/dsh-reactive@1.0.0
  - @hanger-source/dsh-reactive-client@1.0.0
  - @hanger-source/dsh-reactive-native-ui@1.0.0
  - @hanger-source/dsh-reactive-dsh@1.0.0
  - @hanger-source/dsh-reactive-remote@1.0.0

## 0.20.2

### Patch Changes

- 9aca405: useShared 与 factory 形式的 useResource 将共享连接和终止缓存绑定到 Program 寿命。Program 失败或退出后，既有观察完成，之前捕获的流不再重放旧值或错误，也不重新打开尚未读取的查询；完成清理期间的重入订阅遵循同一边界。直接借入的 ResourceReadable 保留原对象与原 owner。
- Updated dependencies [9aca405]
  - @hanger-source/dsh-reactive@0.20.2
  - @hanger-source/dsh-reactive-client@0.20.2
  - @hanger-source/dsh-reactive-dsh@0.20.2
  - @hanger-source/dsh-reactive-native-ui@0.20.2
  - @hanger-source/dsh-reactive-remote@0.20.2

## 0.20.1

### Patch Changes

- bdf0417: 远程 Operation 观察使用原生 baseline/delta 流。事件连接恢复时，Operation、View、人工资源与执行进展统一失效并通过原生 restart 重新取得 baseline，避免查询流仍存活却只清空快照后永久 pending。Session release 与客户端 dispose 等待自己拥有的 Operation 原生流完成异步清理，包括已退订但尚未退出的查询；状态解析和共享仍归同一 core ResourceReader。

  Session 显式查询与已受理 Invocation 的 Operation 观察保持不同寿命，原 Session 退出不会撤销后者。客户端退出也等待 Invocation 活动流真正退出，而不只发出取消。清理等待只证明本地原生迭代器和回调静止，不增加远端关闭 ACK 或取消业务操作。
- Updated dependencies [bdf0417]
  - @hanger-source/dsh-reactive-client@0.20.1
  - @hanger-source/dsh-reactive-native-ui@0.20.1
  - @hanger-source/dsh-reactive-remote@0.20.1
  - @hanger-source/dsh-reactive@0.20.1
  - @hanger-source/dsh-reactive-dsh@0.20.1

## 0.20.0

### Minor Changes

- fdab096: Action 以名称与正整数版本声明跨进程合同身份，新定义省略 version 时为 1。远程解析与业务受理前拒绝不匹配合同，原 Invocation 回执、原生展示与子程序委托恢复保留原版本，不再用当前同名合同解释历史结果。

  迁移时必须同步更新宿主与客户端。自建 wire 调用须提供 contract；原生 manifest 增加 actionVersion，reactiveAction metadata 格式升至 version 2 并增加 actionVersion，委托与子会话记录也要求 actionVersion。缺少这些字段的旧记录不能直接恢复；保留相应旧版本消费者，或由业务明确核验历史合同后进行数据迁移，框架不自动猜测版本。

### Patch Changes

- Updated dependencies [fdab096]
  - @hanger-source/dsh-reactive@0.20.0
  - @hanger-source/dsh-reactive-dsh@0.20.0
  - @hanger-source/dsh-reactive-client@0.20.0
  - @hanger-source/dsh-reactive-remote@0.20.0
  - @hanger-source/dsh-reactive-native-ui@0.20.0

## 0.19.2

### Patch Changes

- ebf227d: 远程 View 与 human 读侧只在实际消费者读取或订阅时连接，共享正式资源快照，最后消费者退出取消查询并撤下旧值。会话释放等待退休查询完成，不再在打开会话时隐式请求全部 View 与人工观察权限。原生 Gateway 仍负责传输代际和恢复，业务执行与 Invocation 的寿命保持独立。
- Updated dependencies [ebf227d]
  - @hanger-source/dsh-reactive-client@0.19.2
  - @hanger-source/dsh-reactive-native-ui@0.19.2
  - @hanger-source/dsh-reactive-remote@0.19.2
  - @hanger-source/dsh-reactive@0.19.2
  - @hanger-source/dsh-reactive-dsh@0.19.2

## 0.19.1

### Patch Changes

- 84f447f: NativeTools 保留原生注册名中的大小写与连字符，使正式 MCP client 已生成的公开身份可以直接声明；不改名、不重新映射服务器工具。重复项、无效字符、长度和保留执行入口的限制继续生效。
- Updated dependencies [84f447f]
  - @hanger-source/dsh-reactive@0.19.1
  - @hanger-source/dsh-reactive-client@0.19.1
  - @hanger-source/dsh-reactive-dsh@0.19.1
  - @hanger-source/dsh-reactive-native-ui@0.19.1
  - @hanger-source/dsh-reactive-remote@0.19.1

## 0.19.0

### Minor Changes

- f690a47: Jobs 与 MCP 的 toolNames、MCP 的 resourceToolNames 提供正式 ResourceReader，普通函数、Promise、Observable 与 hooks 共用实际工具注册事实。已有值流直接取自资源，最后消费者退出只释放查询，能力退出结束旧读取，原生连接、执行及准入仍由原 owner 负责。

  NativeTools 接受显式空名称集合并贡献零个声明，使异步准备或全部工具撤销能直接组合；无效名称、重复项和准入配置继续拒绝。

### Patch Changes

- Updated dependencies [f690a47]
  - @hanger-source/dsh-reactive@0.19.0
  - @hanger-source/dsh-reactive-dsh@0.19.0
  - @hanger-source/dsh-reactive-client@0.19.0
  - @hanger-source/dsh-reactive-native-ui@0.19.0
  - @hanger-source/dsh-reactive-remote@0.19.0

## 0.18.2

### Patch Changes

- 65812d1: Skill 步骤准备在声明、原生 catalog 或本会话工作目录变化时取消旧查询并重新准备，避免不配合取消的 provider 阻塞当前集合。共享 provider 和其他会话保持原有寿命，迟到正文不写回，当前读取错误继续传播。
- Updated dependencies [65812d1]
  - @hanger-source/dsh-reactive-dsh@0.18.2
  - @hanger-source/dsh-reactive-remote@0.18.2
  - @hanger-source/dsh-reactive-client@0.18.2
  - @hanger-source/dsh-reactive@0.18.2
  - @hanger-source/dsh-reactive-native-ui@0.18.2

## 0.18.1

### Patch Changes

- Updated dependencies [48fbba0]
  - @hanger-source/dsh-reactive@0.18.1
  - @hanger-source/dsh-reactive-client@0.18.1
  - @hanger-source/dsh-reactive-dsh@0.18.1
  - @hanger-source/dsh-reactive-native-ui@0.18.1
  - @hanger-source/dsh-reactive-remote@0.18.1

## 0.18.0

### Patch Changes

- Updated dependencies [f13c00f]
  - @hanger-source/dsh-reactive-dsh@0.18.0
  - @hanger-source/dsh-reactive-remote@0.18.0
  - @hanger-source/dsh-reactive-client@0.18.0
  - @hanger-source/dsh-reactive@0.18.0
  - @hanger-source/dsh-reactive-native-ui@0.18.0

## 0.17.2

### Patch Changes

- Updated dependencies [d458636]
  - @hanger-source/dsh-reactive-client@0.17.2
  - @hanger-source/dsh-reactive-remote@0.17.2
  - @hanger-source/dsh-reactive-native-ui@0.17.2
  - @hanger-source/dsh-reactive@0.17.2
  - @hanger-source/dsh-reactive-dsh@0.17.2

## 0.17.1

### Patch Changes

- Updated dependencies [d158683]
  - @hanger-source/dsh-reactive@0.17.1
  - @hanger-source/dsh-reactive-client@0.17.1
  - @hanger-source/dsh-reactive-dsh@0.17.1
  - @hanger-source/dsh-reactive-native-ui@0.17.1
  - @hanger-source/dsh-reactive-remote@0.17.1

## 0.17.0

### Patch Changes

- Updated dependencies [db574d1]
  - @hanger-source/dsh-reactive@0.17.0
  - @hanger-source/dsh-reactive-client@0.17.0
  - @hanger-source/dsh-reactive-dsh@0.17.0
  - @hanger-source/dsh-reactive-native-ui@0.17.0
  - @hanger-source/dsh-reactive-remote@0.17.0

## 0.16.0

### Patch Changes

- Updated dependencies [469c5c5]
  - @hanger-source/dsh-reactive-dsh@0.16.0
  - @hanger-source/dsh-reactive-remote@0.16.0
  - @hanger-source/dsh-reactive-client@0.16.0
  - @hanger-source/dsh-reactive@0.16.0
  - @hanger-source/dsh-reactive-native-ui@0.16.0

## 0.15.0

### Patch Changes

- Updated dependencies [cb08a3b]
  - @hanger-source/dsh-reactive-dsh@0.15.0
  - @hanger-source/dsh-reactive-remote@0.15.0
  - @hanger-source/dsh-reactive-client@0.15.0
  - @hanger-source/dsh-reactive@0.15.0
  - @hanger-source/dsh-reactive-native-ui@0.15.0

## 0.14.0

### Minor Changes

- 8dc0fb9: Invocation 新增同源 activity 资源，普通读取、Observable、hooks 与原生 UI 共用同一份调用事实；activity$ 直接引用 values$，业务终态后有限结束，晚读取保留终态，result 继续独立表达业务完成。本地调用事实不借用已退出组件或执行取消的寿命，远程读侧仍由 SDK 拥有；未终结活动的连接断开立即失效，重连与单次读取取消均不重做业务，已确认终态不被流清理错误撤销。ResourceReader 支持省略借用选项而直接沿用来源寿命，问答和请求工厂保留明确 owner；开发 Skill/reference 与实际包消费者同步更新。

### Patch Changes

- Updated dependencies [8dc0fb9]
  - @hanger-source/dsh-reactive@0.14.0
  - @hanger-source/dsh-reactive-client@0.14.0
  - @hanger-source/dsh-reactive-native-ui@0.14.0
  - @hanger-source/dsh-reactive-dsh@0.14.0
  - @hanger-source/dsh-reactive-remote@0.14.0

## 0.13.0

### Minor Changes

- 2a3e750: 新增 retainNativeSession，以明确的应用 Context 持有原生 SessionReference、独立 Program SDK 和 UI 观察。取得时刷新官方目录并核验真实代际；失败、取消和 Context 退出释放本次成果，成功后显式 release 不影响其他引用或 Host 工作。保留同步 bindNativeSession 的借用语义，开发 Skill/reference 与仓外公共 API 消费同步更新。

  修复 WebSocket 取消、断线和远端终止时上行循环的退出竞态，阻止关闭后发送迟到结束帧，同时保留错误监听直到实际 close。浏览器试验使用官方 rc.3 模块加载器、真实 Client Sessions 与认证 Gateway 验证引用计数、失败和取消、代际重开以及关闭后零发送。

### Patch Changes

- Updated dependencies [2a3e750]
  - @hanger-source/dsh-reactive-native-ui@0.13.0
  - @hanger-source/dsh-reactive-client@0.13.0
  - @hanger-source/dsh-reactive-remote@0.13.0
  - @hanger-source/dsh-reactive@0.13.0
  - @hanger-source/dsh-reactive-dsh@0.13.0

## 0.12.1

### Patch Changes

- 5270fb3: 客户端退出时结束工作区发现与会话创建、打开、恢复的在途回执等待，并向 transport 传递 SDK 取消信号。已经发出的创建保留原引用并报告 CreationUnknownError；释放借用 SDK 不关闭公共 Context，也不撤销 Host 已受理的工作。开发 reference 同步说明取消与创建恢复的归属。
- Updated dependencies [5270fb3]
  - @hanger-source/dsh-reactive-client@0.12.1
  - @hanger-source/dsh-reactive-remote@0.12.1
  - @hanger-source/dsh-reactive-native-ui@0.12.1
  - @hanger-source/dsh-reactive@0.12.1
  - @hanger-source/dsh-reactive-dsh@0.12.1

## 0.12.0

### Patch Changes

- 5081230: 新增 bindProgramClient(program, ctx)，供已安装 Connection、Gateway 和 Program 远程贡献的 dsh 客户端复用同一装配。借用 SDK 只释放自己的 Session 与观察，Context 退出会关闭其 SDK；独立 createProgramClient 继续拥有并释放自己的 Context。原生 UI 现在核验 SDK 对象的实际 Connection 归属，防止不同服务器或连接上的同 ID 会话串用。原生扩展应改用其客户端 Context 的 bindProgramClient 再 open Session；独立 SDK 不能凭相同地址或 ID 接入原生 generation。
- Updated dependencies [5081230]
  - @hanger-source/dsh-reactive-client@0.12.0
  - @hanger-source/dsh-reactive-native-ui@0.12.0
  - @hanger-source/dsh-reactive-remote@0.12.0
  - @hanger-source/dsh-reactive@0.12.0
  - @hanger-source/dsh-reactive-dsh@0.12.0

## 0.11.1

### Patch Changes

- Updated dependencies [9fb4b1a]
- Updated dependencies [be26221]
  - @hanger-source/dsh-reactive@0.11.1
  - @hanger-source/dsh-reactive-client@0.11.1
  - @hanger-source/dsh-reactive-dsh@0.11.1
  - @hanger-source/dsh-reactive-native-ui@0.11.1
  - @hanger-source/dsh-reactive-remote@0.11.1

## 0.11.0

### Patch Changes

- 19c9a36: Program、本地与远程 SDK 提供 progress 资源及 Request.state，统一当前读取、Observable 与 hooks 的消费，沿用原请求关联、原生活跃前缀和会话连接。原生 UI 按需借入相同读侧，绑定本身不额外启动进展连接；单次读取取消和 UI 释放不停止原生工作。createRequestHandle 的 progress 参数改为资源对象并显式接收读侧 owner；框架入口自动完成这些装配。
- Updated dependencies [19c9a36]
  - @hanger-source/dsh-reactive@0.11.0
  - @hanger-source/dsh-reactive-dsh@0.11.0
  - @hanger-source/dsh-reactive-client@0.11.0
  - @hanger-source/dsh-reactive-native-ui@0.11.0
  - @hanger-source/dsh-reactive-remote@0.11.0

## 0.10.0

### Patch Changes

- 8060a41: Program、本地和远程 SDK 提供同源 human 资源；question(callId) 返回固定原问题的可复用对象，将状态观察、回答和前台等待放在同一对象上。问题状态保留 active、foreground、settled 原事实，不建立新的阶段、计时器或批准权。可选原生 UI 完整保留 pending/error，问题控制沿原 UI generation 与 SDK 准入。服务端口的人工回答和 Goal 控制在运行时明确拒绝，绕过类型声明也不能借用人工身份。
- Updated dependencies [8060a41]
  - @hanger-source/dsh-reactive@0.10.0
  - @hanger-source/dsh-reactive-dsh@0.10.0
  - @hanger-source/dsh-reactive-client@0.10.0
  - @hanger-source/dsh-reactive-remote@0.10.0
  - @hanger-source/dsh-reactive-native-ui@0.10.0

## 0.9.1

### Patch Changes

- Updated dependencies [f6050e9]
  - @hanger-source/dsh-reactive-dsh@0.9.1
  - @hanger-source/dsh-reactive-native-ui@0.9.1
  - @hanger-source/dsh-reactive-remote@0.9.1
  - @hanger-source/dsh-reactive-client@0.9.1
  - @hanger-source/dsh-reactive@0.9.1

## 0.9.0

### Patch Changes

- 7a31aa8: shareResource 将已有或组合后的 Observation 流交付为共享只读资源，普通函数、Promise、RxJS 与 hooks 复用同一快照和读取寿命。OperationHandle.state 复用该入口，当前状态读取与 settled 终态等待保持独立，本地和远程操作及可选原生 UI 不再另存观察状态。操作来源结束仍没有业务终态时明确进入查询错误，不永久等待；已有最终观察保留至最后消费者退出，之后显式读取重新查询原操作，不再次执行 Action 或登记 follow。
- Updated dependencies [7a31aa8]
  - @hanger-source/dsh-reactive@0.9.0
  - @hanger-source/dsh-reactive-native-ui@0.9.0
  - @hanger-source/dsh-reactive-client@0.9.0
  - @hanger-source/dsh-reactive-dsh@0.9.0
  - @hanger-source/dsh-reactive-remote@0.9.0

## 0.8.0

### Patch Changes

- 7451ffd: Jobs、Schedule、Team 的会话 SDK 和固定任务提供 state 资源，Job 的 output 提供非消费输出资源；函数、Promise、RxJS、hooks 和公开 View 共用正式资源机制。Schedule 与 Team 的原生变化先撤销旧 ready，异步读取失败作为可恢复的 error，后续通知继续读取。最后资源消费者退出取消读取且不停止已受理原生执行；各域的控制、单次权威查询和持久性责任保持独立。
- Updated dependencies [7451ffd]
  - @hanger-source/dsh-reactive-dsh@0.8.0
  - @hanger-source/dsh-reactive-remote@0.8.0
  - @hanger-source/dsh-reactive-client@0.8.0
  - @hanger-source/dsh-reactive@0.8.0
  - @hanger-source/dsh-reactive-native-ui@0.8.0

## 0.7.0

### Patch Changes

- 424b75d: 本地、远程及动态集合提供稳定的 viewResource 读侧对象，普通函数、Promise、Observable、程序 hooks 与可选原生 UI 共用现有 View 状态和 SDK 读取寿命。ResourceReader 统一已有快照的读取、取消与终止结算，不建立新缓存或来源连接。自定义 SurfaceCollectionTransport 以 viewResource 返回已有读侧，取代分开的 observeView/readView 传输方法。
- Updated dependencies [424b75d]
  - @hanger-source/dsh-reactive@0.7.0
  - @hanger-source/dsh-reactive-dsh@0.7.0
  - @hanger-source/dsh-reactive-client@0.7.0
  - @hanger-source/dsh-reactive-native-ui@0.7.0
  - @hanger-source/dsh-reactive-remote@0.7.0

## 0.6.0

### Patch Changes

- 4596edd: Files 的文本页、元数据与目录提供同一正式 Resource 实例，支持普通 read、共享观察和程序 hooks。首次观察在原生 watch ready 后读取，同路径查询共享 watch，失效取消旧读，查询退出释放原生监听；目录变化不按目录版本过滤。文件范围直接读取已核验会话 header，不再为文件 I/O 读取或冲刷会话日志，关闭 Agent 不终止仍有效来源的文件观察。独立字节缓冲区与异步 unwatch 保留原边界，产物沿既有资源 Surface 对本地和远程公开，开发 Skill 与仓外类型消费者同步。
- Updated dependencies [4596edd]
  - @hanger-source/dsh-reactive-dsh@0.6.0
  - @hanger-source/dsh-reactive-remote@0.6.0
  - @hanger-source/dsh-reactive-client@0.6.0
  - @hanger-source/dsh-reactive@0.6.0
  - @hanger-source/dsh-reactive-native-ui@0.6.0

## 0.5.0

### Patch Changes

- 339cacf: Team 提供经原任务板核验的固定任务对象 task(id)，state$、read 和显式 update 共用原生投影与 CAS。冷 Lead 可读取、绑定，删除后原对象读侧返回 undefined，写入仍要求实际活跃成员，调用参数不能改指其他任务。Team 投影观察接入实际 Agent 关闭、恢复，取消在途旧切面，不复制任务状态、不创建成员或启动模型；开发 Skill 同步组合与生命周期语义。

  日常 CI 与发行增加可选原生域的仓外消费者，共用环境选择，从实际宿主 tgz 的 optional peer 声明安装所选依赖，严格编译并实际加载七个原生子入口；默认后台与原生 UI 的独立安装验证继续保留。
- Updated dependencies [339cacf]
  - @hanger-source/dsh-reactive-dsh@0.5.0
  - @hanger-source/dsh-reactive-remote@0.5.0
  - @hanger-source/dsh-reactive-client@0.5.0
  - @hanger-source/dsh-reactive@0.5.0
  - @hanger-source/dsh-reactive-native-ui@0.5.0

## 0.4.0

### Patch Changes

- 923671d: 局部 createResource 和 createReaction 可省略仅用于诊断的名称，继续使用相同的参数实例、Scope 和流，不增加名称注册或共享状态。Reaction 的来源与执行结果在同步完成时，等待订阅返回并接收清理函数后再结算；连接传播出的清理失败进入 failed，不回滚已交付的业务结果。开发 Skill 同步这两项公共语义。
- baaa98b: 原生长期切面和日志观察跟随实际 Agent 的关闭与恢复，取消旧 owner 的在途读取并接续当前原生日志，避免迟到切面覆盖新现场或重复交付事件。原生挂载只保留一份可观察引用，输入、人工等待与 Operation 共用该事实；观察不自动恢复会话或启动模型，真正读取错误保持原语义。开发 Skill 同步说明观察寿命。
- Updated dependencies [923671d]
- Updated dependencies [baaa98b]
  - @hanger-source/dsh-reactive@0.4.0
  - @hanger-source/dsh-reactive-dsh@0.4.0
  - @hanger-source/dsh-reactive-client@0.4.0
  - @hanger-source/dsh-reactive-native-ui@0.4.0
  - @hanger-source/dsh-reactive-remote@0.4.0

## 0.3.0

### Minor Changes

- 1448bec: 原生 Jobs、Schedule、Team、Files、Approval 和 Clock 接受宿主核验的 Program conversation，普通函数与业务 hooks 共用会话能力实例。原生观察延迟到首次订阅，Program 失败或退出只释放其来源观察，不取消已受理工作。根会话在 Program 构造前持久完成工作区归属，创建 ACK 前不签发外部句柄；本地与远程 open/resume 保留同一创建引用，沿原入口完成登记。恢复继续核验成员撤销；开发 Skill 同步说明来源与生命周期。
- f713b74: 新增 useResourceInstance 与 useReaction，让业务函数、Observable 和程序 hooks 共用同一程序拥有的资源及反应实例。取得实例不自动读取或执行，参数身份保持稳定；Program 失败时释放查询、感知和排队关系，已受理业务继续由原 owner 结算。开发 Skill 同步说明公共入口和生命周期。

### Patch Changes

- Updated dependencies [1448bec]
- Updated dependencies [f713b74]
- Updated dependencies [e4273f8]
  - @hanger-source/dsh-reactive-dsh@0.3.0
  - @hanger-source/dsh-reactive-client@0.3.0
  - @hanger-source/dsh-reactive-remote@0.3.0
  - @hanger-source/dsh-reactive@0.3.0
  - @hanger-source/dsh-reactive-native-ui@0.3.0

## 0.2.2

### Patch Changes

- Updated dependencies [5f53764]
  - @hanger-source/dsh-reactive@0.2.2
  - @hanger-source/dsh-reactive-client@0.2.2
  - @hanger-source/dsh-reactive-dsh@0.2.2
  - @hanger-source/dsh-reactive-native-ui@0.2.2
  - @hanger-source/dsh-reactive-remote@0.2.2

## 0.2.1

### Patch Changes

- ed8c8dc: 明确六包 TypeScript 构建依赖与客户端合同的唯一产物来源，严格编译并执行仓外消费者；以 GitHub OIDC 发布可追溯的同步版本及开发 Skill，并从 npm registry 安装验收。
- 871403f: 明确 Reaction 失败通过 state$ 与 settled 观察，events$ 只交付成功结果；同步应用开发指导。
- Updated dependencies [ed8c8dc]
- Updated dependencies [871403f]
  - @hanger-source/dsh-reactive@0.2.1
  - @hanger-source/dsh-reactive-dsh@0.2.1
  - @hanger-source/dsh-reactive-client@0.2.1
  - @hanger-source/dsh-reactive-remote@0.2.1
  - @hanger-source/dsh-reactive-native-ui@0.2.1

## 0.2.0

首次公开源码。

- 提供与框架版本对应的开发者 Skill 及五份分域 references。
- 通过 getDeveloperSkill() 和 dsh-reactive-skill 定位安装内容，不复制到全局目录；五个框架包使用同版可选 peer。
