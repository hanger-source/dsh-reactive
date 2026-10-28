# @hanger-source/dsh-reactive-client

此文件保留源码版本准备记录；条目和版本号本身不表示 npm 已发布或公共 API 已稳定。

## 0.21.0

### Patch Changes

- 81d05bc: 输入提交、队列编辑和上传的消费者等待与实际受理分开。并发提交共享同一请求事务，每个调用者可以独立取消等待；共享登记只随实际事务结算退出。Program 来源与句柄退出结束各自等待，已受理输入和编辑仍由原生 owner 完成持久回执，迟到结算失败保留在原会话错误链。

  本地与远程入口统一及时结束回执及 Blob 转换等待，转换取消后不启动上传；保留执行前取消、身份和正文冲突核验。远程提交与编辑只在请求已启动且回执无法确认时报告受理未知，明确业务拒绝和本地格式错误仍保留原错误，不自动重试或撤回已受理工作。
- Updated dependencies [00f0503]
- Updated dependencies [81d05bc]
- Updated dependencies [9c5d4b0]
- Updated dependencies [73e4c02]
  - @hanger-source/dsh-reactive@0.21.0

## 4.0.0

### Major Changes

- 2fd5838: ProgramSessionClient 表达人工和服务共同的会话读取、输入、Surface 与 Operation 能力，HumanProgramSessionClient 单独表达问答、前台认领及 Goal 控制。host.service(actorId) 返回绑定固定身份的应用视图，服务可创建、打开和恢复会话、编辑自己的输入及上传文件，逐调用不能改写 actor。

  Gateway 通过必填 participant(principal) 签发角色及 actor，替换只返回字符串的 actor 配置；输入、Action、Operation 与上传使用同一受信端口，Invocation 关联同时核验角色与 actor，服务器拒绝服务执行人工控制。新增服务 SDK 工厂，与人工 SDK 复用同一套正式连接、资源与恢复机制。固定 rc.3 的 answer 传输不授予 human 权限，none 连接不伪装为具有完整恢复能力。角色错配错误保留宿主已确认的会话身份。开发指导及仓外消费者同步此合同，迁移见 docs/migration-participant-ports.md。
- 6847a95: 根会话与集合成员通过 module(mount) 借出按局部 Surface 编程的 SDK，保留 source/selected 合同、原始资源对象、参数类型和本地/远程调用时序；模块不再要求消费函数知道根路径。挂载与父选择共同限制可见性，投射不创建缓存、业务 owner 或控制权限。

  ProgramSessionClient 统一权威请求核对，HumanProgramSessionClient 提供暂停和取消；控制回执与请求状态类型归核心合同，client 重导出同一类型。调用级 signal 及句柄退出只结束本次等待，已经进入原生控制的工作继续完成。服务身份可读请求状态但不能取得人工控制。远程 journal 保留正式 carrier 的专有合同。

  本地 cancel 现在返回 ProgramCancelResult，而非 Promise<void>；自定义 SurfaceClient 和 ProgramSessionClient 实现需提供同一模块与会话能力。迁移方式见 docs/migration-sdk-modules.md。同版本开发 Skill、本地与远程直接消费者、实际 Gateway 和三环境包消费入口同步此合同。

  dsh 发行声明排除已标记 internal 的构造及装配入口，公开观察类型直接来自核心合同；本地 SDK 不再因内部宿主类型牵入持久投影 Registry 的实现声明。运行时装配继续使用同一实现，跨包内核合同保持独立出口。

### Patch Changes

- Updated dependencies [2fd5838]
- Updated dependencies [6847a95]
- Updated dependencies [f8d99d1]
  - @hanger-source/dsh-reactive@4.0.0

## 3.0.0

### Major Changes

- 1c9e7ee: 显式owner的ResourceReader将来源正常完成与资源有效性结束分开：有限来源立即释放真实连接，资源观察和values$继续接受owner撤销，不让活跃的外部hook、Context或视图永久保留旧ready。空完成或pending完成交付EmptyError以结算读取，不重复查询或添加状态缓存。远程Invocation.activity仍与SDK共用同一资源，原生流在终态释放，业务完成改用result或按returned/failed筛选；不再用lastValueFrom(activity$)推断业务完成。未指定owner的本地不可变事实及普通Observable保留原来源的有限语义。更新本地/远程及浏览器直接消费者、原生UI试验和同版本开发指导，迁移见docs/migration-resource-completion.md。

### Patch Changes

- Updated dependencies [1c9e7ee]
- Updated dependencies [dc676c6]
  - @hanger-source/dsh-reactive@3.0.0

## 2.1.0

### Minor Changes

- d502e72: 公开集合成员与Action目录的同源资源：collection.instances与session.actionManifest可供普通读取、Promise、RxJS和hooks直接消费；对应值流继续借用同一查询，不复制状态、建立额外缓存或打开额外连接。本地、远程、服务参与端口及嵌套集合共用公共合同；嵌套目录随父模块撤下报告error，恢复后继续更新，SDK退出撤销借出读侧。目录只描述当前声明，不授予执行权限。同步仓外类型消费者和版本化开发指导。

### Patch Changes

- Updated dependencies [d502e72]
  - @hanger-source/dsh-reactive@2.1.0

## 2.0.0

### Patch Changes

- Updated dependencies [581b301]
  - @hanger-source/dsh-reactive@2.0.0

## 1.1.0

### Patch Changes

- 66a1d07: abortable 接受外部 AbortSignal，以原取消原因结束本次观察与 Promise 等待，忽略不配合取消的迟到结果。客户端等待复用同一互操作；Schedule 只读查询、冷 Team 投影和显式 Files 读取不再被迟到的原生回执阻塞。Files 保留实际 I/O 的退出等待；原生历史读取继续传递查询信号，不取消已受理业务或其他消费者。
- Updated dependencies [66a1d07]
- Updated dependencies [b4af2f5]
  - @hanger-source/dsh-reactive@1.1.0

## 1.0.0

### Major Changes

- bdd5279: SDK、Session及其远程资源统一由正式dsh Fiber/effect持有构造与异步释放，替换各域独立的退出和退休队列。bindProgramClient现在返回Promise并等待作用域安装，直接消费者需要await；ProgramJournal通过session.openEvents取得。取消立即撤销使用权，退出等待实际清理静止并汇总失败，已受理业务继续遵循原owner。迁移方式见docs/migration-sdk-scopes.md。

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

- bdf0417: 远程 Operation 观察使用原生 baseline/delta 流。事件连接恢复时，Operation、View、人工资源与执行进展统一失效并通过原生 restart 重新取得 baseline，避免查询流仍存活却只清空快照后永久 pending。Session release 与客户端 dispose 等待自己拥有的 Operation 原生流完成异步清理，包括已退订但尚未退出的查询；状态解析和共享仍归同一 core ResourceReader。

  Session 显式查询与已受理 Invocation 的 Operation 观察保持不同寿命，原 Session 退出不会撤销后者。客户端退出也等待 Invocation 活动流真正退出，而不只发出取消。清理等待只证明本地原生迭代器和回调静止，不增加远端关闭 ACK 或取消业务操作。
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

- ebf227d: 远程 View 与 human 读侧只在实际消费者读取或订阅时连接，共享正式资源快照，最后消费者退出取消查询并撤下旧值。会话释放等待退休查询完成，不再在打开会话时隐式请求全部 View 与人工观察权限。原生 Gateway 仍负责传输代际和恢复，业务执行与 Invocation 的寿命保持独立。
- @hanger-source/dsh-reactive@0.19.2

## 0.19.1

### Patch Changes

- Updated dependencies [84f447f]
  - @hanger-source/dsh-reactive@0.19.1

## 0.19.0

### Patch Changes

- Updated dependencies [f690a47]
  - @hanger-source/dsh-reactive@0.19.0

## 0.18.2

### Patch Changes

- @hanger-source/dsh-reactive@0.18.2

## 0.18.1

### Patch Changes

- Updated dependencies [48fbba0]
  - @hanger-source/dsh-reactive@0.18.1

## 0.18.0

### Patch Changes

- @hanger-source/dsh-reactive@0.18.0

## 0.17.2

### Patch Changes

- d458636: requestStatus、pause、cancel 的远程回执等待随调用 Session 句柄或 SDK 退出及时结算，借用的公共 Context 和其他 SDK 继续可用。复用现有取消等待接点，并将所属信号传入正式 Host 合同；Host 异步取得真实句柄后、进入读取或原生控制前重新检查取消。已经进入原生控制的工作仍由 Host 结算，客户端退出不回滚控制或已受理业务。同版本开发指导说明这些等待和执行 owner 的边界。
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

### Patch Changes

- @hanger-source/dsh-reactive@0.16.0

## 0.15.0

### Patch Changes

- @hanger-source/dsh-reactive@0.15.0

## 0.14.0

### Minor Changes

- 8dc0fb9: Invocation 新增同源 activity 资源，普通读取、Observable、hooks 与原生 UI 共用同一份调用事实；activity$ 直接引用 values$，业务终态后有限结束，晚读取保留终态，result 继续独立表达业务完成。本地调用事实不借用已退出组件或执行取消的寿命，远程读侧仍由 SDK 拥有；未终结活动的连接断开立即失效，重连与单次读取取消均不重做业务，已确认终态不被流清理错误撤销。ResourceReader 支持省略借用选项而直接沿用来源寿命，问答和请求工厂保留明确 owner；开发 Skill/reference 与实际包消费者同步更新。

### Patch Changes

- Updated dependencies [8dc0fb9]
  - @hanger-source/dsh-reactive@0.14.0

## 0.13.0

### Patch Changes

- 2a3e750: 新增 retainNativeSession，以明确的应用 Context 持有原生 SessionReference、独立 Program SDK 和 UI 观察。取得时刷新官方目录并核验真实代际；失败、取消和 Context 退出释放本次成果，成功后显式 release 不影响其他引用或 Host 工作。保留同步 bindNativeSession 的借用语义，开发 Skill/reference 与仓外公共 API 消费同步更新。

  修复 WebSocket 取消、断线和远端终止时上行循环的退出竞态，阻止关闭后发送迟到结束帧，同时保留错误监听直到实际 close。浏览器试验使用官方 rc.3 模块加载器、真实 Client Sessions 与认证 Gateway 验证引用计数、失败和取消、代际重开以及关闭后零发送。
- @hanger-source/dsh-reactive@0.13.0

## 0.12.1

### Patch Changes

- 5270fb3: 客户端退出时结束工作区发现与会话创建、打开、恢复的在途回执等待，并向 transport 传递 SDK 取消信号。已经发出的创建保留原引用并报告 CreationUnknownError；释放借用 SDK 不关闭公共 Context，也不撤销 Host 已受理的工作。开发 reference 同步说明取消与创建恢复的归属。
- @hanger-source/dsh-reactive@0.12.1

## 0.12.0

### Minor Changes

- 5081230: 新增 bindProgramClient(program, ctx)，供已安装 Connection、Gateway 和 Program 远程贡献的 dsh 客户端复用同一装配。借用 SDK 只释放自己的 Session 与观察，Context 退出会关闭其 SDK；独立 createProgramClient 继续拥有并释放自己的 Context。原生 UI 现在核验 SDK 对象的实际 Connection 归属，防止不同服务器或连接上的同 ID 会话串用。原生扩展应改用其客户端 Context 的 bindProgramClient 再 open Session；独立 SDK 不能凭相同地址或 ID 接入原生 generation。

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

- @hanger-source/dsh-reactive@0.9.1

## 0.9.0

### Patch Changes

- Updated dependencies [7a31aa8]
  - @hanger-source/dsh-reactive@0.9.0

## 0.8.0

### Patch Changes

- @hanger-source/dsh-reactive@0.8.0

## 0.7.0

### Minor Changes

- 424b75d: 本地、远程及动态集合提供稳定的 viewResource 读侧对象，普通函数、Promise、Observable、程序 hooks 与可选原生 UI 共用现有 View 状态和 SDK 读取寿命。ResourceReader 统一已有快照的读取、取消与终止结算，不建立新缓存或来源连接。自定义 SurfaceCollectionTransport 以 viewResource 返回已有读侧，取代分开的 observeView/readView 传输方法。

### Patch Changes

- Updated dependencies [424b75d]
  - @hanger-source/dsh-reactive@0.7.0

## 0.6.0

### Patch Changes

- @hanger-source/dsh-reactive@0.6.0

## 0.5.0

### Patch Changes

- @hanger-source/dsh-reactive@0.5.0

## 0.4.0

### Patch Changes

- Updated dependencies [923671d]
  - @hanger-source/dsh-reactive@0.4.0

## 0.3.0

### Patch Changes

- 1448bec: 原生 Jobs、Schedule、Team、Files、Approval 和 Clock 接受宿主核验的 Program conversation，普通函数与业务 hooks 共用会话能力实例。原生观察延迟到首次订阅，Program 失败或退出只释放其来源观察，不取消已受理工作。根会话在 Program 构造前持久完成工作区归属，创建 ACK 前不签发外部句柄；本地与远程 open/resume 保留同一创建引用，沿原入口完成登记。恢复继续核验成员撤销；开发 Skill 同步说明来源与生命周期。
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

- 提供独立类型化 Program 客户端、动态模块集合、按精确 scope 解析 Action、输入提交与编辑、进度和 Operation 读取。
- 取消选项保留请求、观察与业务执行各自的生命周期；客户端不加载宿主实现。
