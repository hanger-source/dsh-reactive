# @hanger-source/dsh-reactive-skills

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
