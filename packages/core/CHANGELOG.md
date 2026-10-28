# @hanger-source/dsh-reactive

## 3.0.0

### Major Changes

- 1c9e7ee: 显式owner的ResourceReader将来源正常完成与资源有效性结束分开：有限来源立即释放真实连接，资源观察和values$继续接受owner撤销，不让活跃的外部hook、Context或视图永久保留旧ready。空完成或pending完成交付EmptyError以结算读取，不重复查询或添加状态缓存。远程Invocation.activity仍与SDK共用同一资源，原生流在终态释放，业务完成改用result或按returned/failed筛选；不再用lastValueFrom(activity$)推断业务完成。未指定owner的本地不可变事实及普通Observable保留原来源的有限语义。更新本地/远程及浏览器直接消费者、原生UI试验和同版本开发指导，迁移见docs/migration-resource-completion.md。

### Minor Changes

- dc676c6: Workspace提供直接的resource与reaction绑定入口，复用既有参数身份、Resource、Reaction和Scope；本地application.workspace(id)与组件props.workspace取得同一对象，在没有Session时也可构造、读取和显式运行。工作区编程合同及内核装配归core/workspaces，公共类型从根入口与/workspaces导出。跨环境、会话共享，同实现与目标在不同工作区隔离。正式工作区登记删除或宿主退出结束其owner，撤销旧资源与排队关系，保留已受理业务；清理重入不能复活已经删除的owner。共享连接在来源工厂执行前登记实际订阅，工厂内同步退出立即关闭它；直接释放来源后结束观察，使teardown失败返回Scope。删除后的清理失败保留到宿主退出汇总，其他工作区仍完整释放。同步开发指导和仓外消费入口。

## 2.1.0

### Minor Changes

- d502e72: 公开集合成员与Action目录的同源资源：collection.instances与session.actionManifest可供普通读取、Promise、RxJS和hooks直接消费；对应值流继续借用同一查询，不复制状态、建立额外缓存或打开额外连接。本地、远程、服务参与端口及嵌套集合共用公共合同；嵌套目录随父模块撤下报告error，恢复后继续更新，SDK退出撤销借出读侧。目录只描述当前声明，不授予执行权限。同步仓外类型消费者和版本化开发指导。

## 2.0.0

### Major Changes

- 581b301: 统一资源有效性与原生依赖的生命周期：owner撤销先交付原reason的error Observation再完成，迟到观察只取得撤销而不回放旧业务值，values$在自身owner退出时正常完成。父Scope撤销立即传播到后代，读取拒绝保留同一原因，实际清理仍由Scope树汇总。九个原生能力按实际配置声明正式Fiber服务依赖，provider卸载结束旧能力及其读侧，不随替换服务重定向，并释放本实例安装范围。Jobs未选工具时明确贡献空目录且不依赖ToolRuntime。更新同版本Skill及迁移说明，见docs/migration-resource-ownership.md。

## 1.1.0

### Minor Changes

- 66a1d07: abortable 接受外部 AbortSignal，以原取消原因结束本次观察与 Promise 等待，忽略不配合取消的迟到结果。客户端等待复用同一互操作；Schedule 只读查询、冷 Team 投影和显式 Files 读取不再被迟到的原生回执阻塞。Files 保留实际 I/O 的退出等待；原生历史读取继续传递查询信号，不取消已受理业务或其他消费者。
- b4af2f5: combineResources 和 switchResource 直接组合 ResourceReadable 对象与 Observation 流；公开 ResourceSource 表达同一来源的两种消费入口。保持领域类型、原观察流、共享与 owner，不额外读取、解析或取得业务执行所有权。

## 1.0.0

### Minor Changes

- dd50bb1: 局部 Action 可在同一个 implementAction 选项对象中声明合同与实现，也可由 useAction 的 factory 同时构造实现和明确目标。沿用现有绑定、宿主准入、Invocation 与 Operation owner，不引入额外状态或执行生命周期；共享合同入口继续保持独立。
- 60f19d6: 本地与远程 SDK 共用 core 的 ProgramSessionClient 和 SurfaceClient 消费合同，根会话与集合成员可直接供同一个模块使用。保留各端的调用时序、权限与资源 owner，不增加代理状态；远程会话公开同一纯 Surface，本地人工观察直接来自原 human 资源。

## 0.20.2

### Patch Changes

- 9aca405: useShared 与 factory 形式的 useResource 将共享连接和终止缓存绑定到 Program 寿命。Program 失败或退出后，既有观察完成，之前捕获的流不再重放旧值或错误，也不重新打开尚未读取的查询；完成清理期间的重入订阅遵循同一边界。直接借入的 ResourceReadable 保留原对象与原 owner。

## 0.20.1

No changes in this release.

## 0.20.0

### Minor Changes

- fdab096: Action 以名称与正整数版本声明跨进程合同身份，新定义省略 version 时为 1。远程解析与业务受理前拒绝不匹配合同，原 Invocation 回执、原生展示与子程序委托恢复保留原版本，不再用当前同名合同解释历史结果。

  迁移时必须同步更新宿主与客户端。自建 wire 调用须提供 contract；原生 manifest 增加 actionVersion，reactiveAction metadata 格式升至 version 2 并增加 actionVersion，委托与子会话记录也要求 actionVersion。缺少这些字段的旧记录不能直接恢复；保留相应旧版本消费者，或由业务明确核验历史合同后进行数据迁移，框架不自动猜测版本。

## 0.19.2

No changes in this release.

## 0.19.1

### Patch Changes

- 84f447f: NativeTools 保留原生注册名中的大小写与连字符，使正式 MCP client 已生成的公开身份可以直接声明；不改名、不重新映射服务器工具。重复项、无效字符、长度和保留执行入口的限制继续生效。

## 0.19.0

### Minor Changes

- f690a47: Jobs 与 MCP 的 toolNames、MCP 的 resourceToolNames 提供正式 ResourceReader，普通函数、Promise、Observable 与 hooks 共用实际工具注册事实。已有值流直接取自资源，最后消费者退出只释放查询，能力退出结束旧读取，原生连接、执行及准入仍由原 owner 负责。

  NativeTools 接受显式空名称集合并贡献零个声明，使异步准备或全部工具撤销能直接组合；无效名称、重复项和准入配置继续拒绝。

## 0.18.2

No changes in this release.

## 0.18.1

### Patch Changes

- 48fbba0: 步骤前检查按每次登记持有独立身份与取消寿命，多个组件使用同一函数时互不误删。依赖替换、组件退出、原生步骤取消及 Program 失败及时结束检查等待；不配合取消的旧 Promise 迟到完成或失败不再阻塞或覆盖当前准备。准备过程重新核对当前检查集合，仍有效的检查错误继续传播。公共回调接口保持原样，开发指导与仓外消费者使用传入信号读取实际资源。

## 0.18.0

No changes in this release.

## 0.17.2

No changes in this release.

## 0.17.1

### Patch Changes

- d158683: 修复工作区共享来源在 owner 退出后仍向迟到订阅者重放旧值或旧错误的问题。活跃 owner 内继续共享唯一来源并保留有限来源终态；退出时释放可重放对象，结束现有观察，complete/teardown 中重入或之后捕获的旧流都直接完成且不重连。Workspace 会话与工作区自身寿命保持独立，同版本开发指导说明此边界。

## 0.17.0

### Minor Changes

- db574d1: useResourceInstance 与 useReaction 支持局部选项 factory，由当前 Program 持有已有 Resource/Reaction 实例，React 依赖决定组件内身份。自定义业务 hook 可直接定义并返回完整资源与反应组合，无需另建参数合同或传入 Scope；构造不读取或执行，目标切换不重定向旧对象，显式运行、读取取消与已受理业务继续使用原 owner。共享 implementation/params 入口与 schema 输入输出类型保持原语义；同版本开发指导和仓外消费者同步覆盖局部组合。

## 0.16.0

No changes in this release.

## 0.15.0

No changes in this release.

## 0.14.0

### Minor Changes

- 8dc0fb9: Invocation 新增同源 activity 资源，普通读取、Observable、hooks 与原生 UI 共用同一份调用事实；activity$ 直接引用 values$，业务终态后有限结束，晚读取保留终态，result 继续独立表达业务完成。本地调用事实不借用已退出组件或执行取消的寿命，远程读侧仍由 SDK 拥有；未终结活动的连接断开立即失效，重连与单次读取取消均不重做业务，已确认终态不被流清理错误撤销。ResourceReader 支持省略借用选项而直接沿用来源寿命，问答和请求工厂保留明确 owner；开发 Skill/reference 与实际包消费者同步更新。

## 0.13.0

No changes in this release.

## 0.12.1

No changes in this release.

## 0.12.0

No changes in this release.

## 0.11.1

### Patch Changes

- 9fb4b1a: 在 Promise 读取及 Resource 的观察、失效工厂进入业务代码前登记取消，修复读取工厂尚未返回时触发失效、旧读取未及时收到 abort 的窗口。工厂返回前释放 Scope 时，提供者也能立即通过原 signal 阻止后续副作用。保留原有共享、参数身份和迟到结果隔离，不增加查询生命周期或重试策略。
- be26221: 修复参数、失效与派生资源切换在旧来源清理中再次收到通知时的重叠读取及失管连接。旧 ready 立即撤下，旧读取立即取消，RxJS 同步队列在整次清理结束后连接最新来源；被后续通知替代的连接不再启动。切换期间的通知共用原来源，有限来源不会被内部取消监听重新订阅。

## 0.11.0

### Minor Changes

- 19c9a36: Program、本地与远程 SDK 提供 progress 资源及 Request.state，统一当前读取、Observable 与 hooks 的消费，沿用原请求关联、原生活跃前缀和会话连接。原生 UI 按需借入相同读侧，绑定本身不额外启动进展连接；单次读取取消和 UI 释放不停止原生工作。createRequestHandle 的 progress 参数改为资源对象并显式接收读侧 owner；框架入口自动完成这些装配。

## 0.10.0

### Minor Changes

- 8060a41: Program、本地和远程 SDK 提供同源 human 资源；question(callId) 返回固定原问题的可复用对象，将状态观察、回答和前台等待放在同一对象上。问题状态保留 active、foreground、settled 原事实，不建立新的阶段、计时器或批准权。可选原生 UI 完整保留 pending/error，问题控制沿原 UI generation 与 SDK 准入。服务端口的人工回答和 Goal 控制在运行时明确拒绝，绕过类型声明也不能借用人工身份。

## 0.9.1

No changes in this release.

## 0.9.0

### Minor Changes

- 7a31aa8: shareResource 将已有或组合后的 Observation 流交付为共享只读资源，普通函数、Promise、RxJS 与 hooks 复用同一快照和读取寿命。OperationHandle.state 复用该入口，当前状态读取与 settled 终态等待保持独立，本地和远程操作及可选原生 UI 不再另存观察状态。操作来源结束仍没有业务终态时明确进入查询错误，不永久等待；已有最终观察保留至最后消费者退出，之后显式读取重新查询原操作，不再次执行 Action 或登记 follow。

## 0.8.0

No changes in this release.

## 0.7.0

### Minor Changes

- 424b75d: 本地、远程及动态集合提供稳定的 viewResource 读侧对象，普通函数、Promise、Observable、程序 hooks 与可选原生 UI 共用现有 View 状态和 SDK 读取寿命。ResourceReader 统一已有快照的读取、取消与终止结算，不建立新缓存或来源连接。自定义 SurfaceCollectionTransport 以 viewResource 返回已有读侧，取代分开的 observeView/readView 传输方法。

## 0.6.0

No changes in this release.

## 0.5.0

No changes in this release.

## 0.4.0

### Minor Changes

- 923671d: 局部 createResource 和 createReaction 可省略仅用于诊断的名称，继续使用相同的参数实例、Scope 和流，不增加名称注册或共享状态。Reaction 的来源与执行结果在同步完成时，等待订阅返回并接收清理函数后再结算；连接传播出的清理失败进入 failed，不回滚已交付的业务结果。开发 Skill 同步这两项公共语义。

## 0.3.0

### Minor Changes

- f713b74: 新增 useResourceInstance 与 useReaction，让业务函数、Observable 和程序 hooks 共用同一程序拥有的资源及反应实例。取得实例不自动读取或执行，参数身份保持稳定；Program 失败时释放查询、感知和排队关系，已受理业务继续由原 owner 结算。开发 Skill 同步说明公共入口和生命周期。

### Patch Changes

- e4273f8: 资源观察在同步基线或连接通知中退订时立即撤销连接，避免已退出的消费者仍启动业务读取；后续显式读取继续使用同一个资源身份。

## 0.2.2

### Patch Changes

- 5f53764: 完善已验证 CI 候选的首次登记、来源校验与 OIDC 直接发行流程，统一发布运行时与配套开发 Skill。

## 0.2.1

### Patch Changes

- ed8c8dc: 明确六包 TypeScript 构建依赖与客户端合同的唯一产物来源，严格编译并执行仓外消费者；以 GitHub OIDC 发布可追溯的同步版本及开发 Skill，并从 npm registry 安装验收。
- 871403f: 明确 Reaction 失败通过 state$ 与 settled 观察，events$ 只交付成功结果；同步应用开发指导。

## 0.2.0

首次公开源码。

- 提供不依赖原生宿主的响应式应用合同：Resource、Reaction、Action、Invocation、Operation、Program、Module 与 Surface。
- 支持参数化资源共享、依赖失效传播、类型化输入、人工参与归属和多轮 Operation 反馈。
