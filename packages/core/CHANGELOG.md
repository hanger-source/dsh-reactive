# @hanger-source/dsh-reactive

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
