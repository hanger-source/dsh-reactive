# @hanger-source/dsh-reactive-client

此文件保留源码版本准备记录；条目和版本号本身不表示 npm 已发布或公共 API 已稳定。

## 0.34.0

### Minor Changes

- 7cf35a7: Action Invocation 携带在输入解析和准入前捕获的不可变 origin，保留原会话、Action、绑定、目标、真实来源与完整输入；本地、远程及 lookup 消费同一归属合同。远程受理回执核验原绑定与目标，矛盾回执仍表示受理未知，不重复执行业务。

  Work 通过 `{ sessions?, invocations? }` 显式组合会话与已持有调用，可在无会话的 CLI 中使用，也可用只读工作标签组织共同进展。动态来源保留原 activity 读侧、失效和释放语义，不读取结果、取消业务或授予 Goal 续接权限。旧会话数组参数须迁移，SDK 与 Gateway 须使用匹配版本；详见 docs/migration-work-invocations.md。

### Patch Changes

- Updated dependencies [7cf35a7]
  - @hanger-source/dsh-reactive@0.34.0

## 0.33.2

### Patch Changes

- @hanger-source/dsh-reactive@0.33.2

## 0.33.1

### Patch Changes

- Updated dependencies [005bc2d]
  - @hanger-source/dsh-reactive@0.33.1

## 0.33.0

### Patch Changes

- @hanger-source/dsh-reactive@0.33.0

## 0.32.0

### Patch Changes

- @hanger-source/dsh-reactive@0.32.0

## 0.31.0

### Patch Changes

- @hanger-source/dsh-reactive@0.31.0

## 0.30.1

### Patch Changes

- Updated dependencies [4d5abf9]
  - @hanger-source/dsh-reactive@0.30.1

## 0.30.0

### Minor Changes

- 09d733c: 公开 ToolActivity.invocationId、tool.invocation 事实和 OperationFollowSnapshot.callSequence，将业务 Invocation、原生工具发生及原操作参与按正式身份连接。重复 provider 标签不再要求消费者解析不透明 ID，业务结果与原生后置结果分别保留。

  完整 Invocation 在执行前登记来源关联，失败通过 registration 阶段结算且不进入业务或 Operation 准备。关系使用现有原生日志和投影恢复；缺证保留 null，不推测历史来源，也不改变既有 Source 或业务恢复合同。同版本开发指导说明固定 rc.3 下重复 PTC 子 ID 的正式冷读取限制。

### Patch Changes

- Updated dependencies [09d733c]
  - @hanger-source/dsh-reactive@0.30.0

## 0.29.0

### Minor Changes

- 5396d88: 人工问题公开 origin.continuation，直接、PTC 和 Action 内提问均持久保留原模型步骤的 Goal 关联，冷恢复及远程读取消费同一事实。迟答作为原参与反馈准入，不借用后来 Goal 或新修订的权限；缺证、暂停或被替换时保留原消息，新的可信输入才可带入旧答案事实。

  WorkSelector 从同源事实提供 question.reply-blocked 及 unavailable、superseded、inactive 原因，保留完整原问题、交付身份和当前 Goal。响应者政策、业务授权、原生问答生命周期与业务 Operation 仍由各自 owner 管理。

### Patch Changes

- Updated dependencies [5396d88]
  - @hanger-source/dsh-reactive@0.29.0

## 0.28.1

### Patch Changes

- edc5796: 将工具的 Goal 控制关联固定到真正准备并准入的模型步骤，完成、等待与 Operation Follow 共同保留原修订。修复暂停目标后的独立问题仍可完成该目标，以及旧模型响应在 Goal 编辑后借用新修订的问题；PTC 子调用沿根步骤使用同一关联。

  结果续接在原生 prompt 装配之前完成准入和 rearm，异步等待后按本次实际准备事实复核，避免旧 prompt 与新 Goal 身份混用。无 Goal 的结果在连续模型步骤中保留原关联，不能借用会话中另一个 Goal；真实原生 Goal 控制仍可为后续步骤建立新关联。业务执行、原生 Goal 控制与读取的 owner 保持各自职责。

  正式工具 dispatch 结束后，后台代码继承的 Agent 身份不再签发 Goal 控制；同步重入通知不能覆盖较新关联。结果 rearm 后拒准入时撤下本次尚未消费的自动推进，原消息与原关联保留，供新的可信输入处理。
- Updated dependencies [edc5796]
  - @hanger-source/dsh-reactive@0.28.1

## 0.28.0

### Minor Changes

- 3e1bc54: 在 progress 中提供同源的 ModelPreparation，观察当前模型准备实际等待的输入、委托、步骤检查、Context、Skill 与原生装配。请求和 Work 按完整原输入关联同一准备事实；本地、远程 SDK 与原生 UI 消费相同合同。

  完整有效声明相同时保留原绑定和修订，程序可以观察自己的进展而不重复装配。并列 Context 重收集先结束原批次读取；Program 失败撤销原生绑定时保留实际错误原因。

  ConversationProgressSnapshot 和 RequestSnapshot 的自有来源需要提供 preparation（无当前准备时为 null）。准备观察不持久化、不参与调度、不签发权限；取消、结束及 Agent 退出撤下旧等待，冷会话不复活历史准备。

### Patch Changes

- Updated dependencies [3e1bc54]
  - @hanger-source/dsh-reactive@0.28.0

## 0.27.0

### Minor Changes

- 8ebff7b: 以原生 turn/end:error 提供同源 ExecutionFailure，贯通会话、请求、Work 与远程 SDK，保留真实输入和未入历史消息的失败因果。自定义 ConversationActivity 与 RequestActivity 来源需要提供 failures。

  晚答失败后沿原消息 ID 与内容交还正式 inbox，实际 Agent 激活也核验并恢复进程中断留下的交接；只读打开不修改队列。保留等待原生 flush，不重答、不自动重试或恢复 Goal，取消和已消费消息不复活。

### Patch Changes

- Updated dependencies [8ebff7b]
  - @hanger-source/dsh-reactive@0.27.0

## 0.26.0

### Minor Changes

- 8b71ea6: 公开晚答的 HumanReplyDelivery，区分原生排队、步骤提议与问题结算。整体与固定问题快照均提供 replies，Work 按原问题输入关联同一交付事实；人工读侧使用单一完整快照，本地与远程 SDK、可选 UI 共用 HumanSnapshot。

  自定义 HumanSnapshot 与 HumanQuestionSnapshot 来源需要提供 replies；ProgramQuestions 保留为传输合同，运行对象的人工观察使用 HumanSnapshot。取消或提议窗口结束不代表模型消费，未进入输入历史的晚答提议失败不会改写原请求结算。

### Patch Changes

- Updated dependencies [8b71ea6]
  - @hanger-source/dsh-reactive@0.26.0

## 0.25.0

### Minor Changes

- 45be451: 提供 SessionAttachments.readImage，使 Program、本地与远程 SDK 和可选原生 UI 按原会话资格读取已登记图片。复用原生 Controller 的引用授权与冷读取，字节采用独占缓冲区和正式 binary codec；取消只结束消费者等待，原生 owner 退出仍等待实际读取结算。

  历史文件附件仍缺少 dsh rc.3 的公开会话授权读取入口，不以内容哈希或上传回执扩大读取权限。

### Patch Changes

- Updated dependencies [45be451]
  - @hanger-source/dsh-reactive@0.25.0

## 0.24.0

### Minor Changes

- d1bcc55: 远程 Action 在分派前固定 UUID 受理身份，受理回执未知错误携带 invocationId，调用者可沿原身份只读查回。TypedRPC startAction 请求必须提供该身份；不自动重发或推断业务未执行。

  宿主唯一调用记录在解析与准入前登记身份，相同请求返回原 Invocation，不同请求冲突，同步重入明确报告受理中。普通本地自动身份保留原输入能力；显式身份使用 JSON 原值并受既有进程 owner、保留期和数量限制约束。

### Patch Changes

- Updated dependencies [d1bcc55]
- Updated dependencies [6db04b3]
  - @hanger-source/dsh-reactive@0.24.0

## 0.23.0

### Minor Changes

- 0a8fcdd: Action 绑定支持共同业务资格资源，声明、调用与本地和远程消费者共享其观察；资格撤销约束尚未进入业务执行的准入，并保留业务执行与持久交接的责任边界。同步版本化开发指导。
- eff2825: 公开协作反馈的持久观察、工作范围局部资源及调用发生身份。
- df03bbc: 共享资源资格并明确独立 Follow 登记准入与未登记事实。
- de77251: 公开请求声明、真实输入、独立调用与回复发生身份，以及当前 Goal 的权威输入关联。请求的工具和最后回复由同一原生投影事实派生，未取得步骤证据的外部回复不归入业务请求。

  工具发生以正式 start 序号区分，保留跨轮复用 provider callId 的完整历史。人工问题只关联唯一真实的同 ID 提问，后来的普通工具不改变旧问题来源；rc.3 无法核验重复提问代际时明确拒绝来源读取或人工参与，不猜测原工作。

  新增可提取的业务范围选择器，按应用定义的工作身份共同观察多个已取得的会话。保留完整多输入、原会话、问题、等待和 Goal 修订，分别呈现进展与关联条件，不产生新的继续权限或任务状态。

  本地和远程消费者共用原始因果事件，纯范围观察不执行业务输入 transform。远程会话保留真实父会话身份。共享派生读侧允许沿来源和观察者连接寿命使用，无须构造额外 owner。

### Patch Changes

- Updated dependencies [0a8fcdd]
- Updated dependencies [eff2825]
- Updated dependencies [df85eac]
- Updated dependencies [35dce22]
- Updated dependencies [df03bbc]
- Updated dependencies [2a103b4]
- Updated dependencies [f3f7b42]
- Updated dependencies [de77251]
  - @hanger-source/dsh-reactive@0.23.0

## 0.22.0

### Minor Changes

- d254b19: 将工作区成员资格贯通到 Session、Program、本地与远程 SDK、原生资源和人工控制。成员撤销或工作区删除结束旧读取与准入资格，已受理调用、持久回执及独立业务 Operation 继续由原 owner 结算。

  会话取得回执携带 accessId，后续会话请求在装配前核验原资格；无观察的旧 SDK 也不能跨过移除再加入继续使用。显式重新取得保留原生 Agent，并建立新的 Program 图与读取身份。提供 ConversationAccessRevokedError 和只读 signal，开发指导及迁移说明同步更新。
- 2cadde4: 会话上传同步返回 FileUploadInvocation，普通 Promise、独立等待和有限活动资源消费同一次调用。字节准备可以取消；开始分派后退出等待不覆盖实际回执，也不重新上传。本地会话退出等待已经开始的原生存储，远程运输失败明确保留回执未知及原始原因。原生 UI、公开消费类型和版本化开发指导共同使用这份合同。原先直接 await uploadFile 的消费者需要改为读取调用的 result 或 wait()。
- a7ad54c: Invocation 统一提供 wait({ signal })，Action、原生写入和上传可以通过同一个合同消费。每次等待只借原 result，取消不改写 activity、结果或 Operation 关联；各签发者的读取 owner 继续适用。

  本地 SDK Action 的调用者 signal 与真正执行 signal 分开：前者限制进入 execute 之前的准入和持久准备，业务开始后不再把真实成功改成 failed 或丢失 Operation 回执。真正的 Host、工具与显式 execution 取消语义保持；本地按原地址查回仍返回原对象，首次调用者取消不会成为永久等待条件。

  远程 Invocation 的 wait 仅限定本 SDK 的等待，不向 Host 发送执行取消。公共类型、泛型仓外消费者、开发 Skill 与调用回执等待指导同步更新。自建 Invocation 实现需补 wait 方法，现有普通消费者使用框架签发对象即可。
- d5dc92f: 同次 Program 步骤准备并列连接独立模块检查；pending 依赖不再遮住兄弟模块的真实错误，检查集合变化释放旧次等待并重新准备当前集合。

  原生 Skills 的目录和正文使用会话真实当前工作目录，并随同会话目录变化失效。Subagents 提供可组合的后代资源与正式 ancestor 权限下的选定子分支中断；读取、控制和带 Goal 的委托保留各自原生 owner。

  认证 transport 拥有物理连接及 uplink 的真实清理，退出等待实际静止并保留清理失败。独立 SDK 释放自有 transport；借用 Connection 的 SDK 不取得其装配销毁权，应用需把自建 transport 的 dispose 交给原装配 owner。远程进展流的载体断线继续使用官方恢复；终止错误保留在原观察 owner，连接重置不将已终止的入口变成无法完成的 pending，重新 open 会话取得新的观察。

  开发 Skill 同步说明这些公共能力与生命周期边界。源码继续保持 0.x，版本准备统一合并待发 Changesets，不按建设批次升号。
- 214a438: 工作区可直接共享已有 ResourceSource，并保留读取寿命与原失效原因；资源同步投影不再交付未观察的 Promise。

  Jobs 提供正式终态移除控制，Schedule 排队写入与 SDK 等待分离取消信号；Jobs、Team、Schedule 分域提供公开纯合同叶。

  客户端前置合同构建按目录分权，Inbox 拥有独立入口；浏览器公共入口共用拆分产物，保持运行时类与合同身份。仓外消费覆盖跨入口的实际传输与纯原生合同浏览器闭包。
- b757de0: Session 持久归属与子委托受理意图保存实际 Program 合同身份。创建补齐、冷读取、恢复及子输入解释先核原身份；共享换约或具名/本地转换抛出公共 SessionProgramMismatchError，远程 SDK 保留同一类型及原身份事实。旧归属或委托记录缺少 program 字段时明确拒绝，不自动迁移。

  Application 与 Environment 在定义时取得自己的环境及 Skill 声明快照，异步装配不受调用者后续修改影响，Program 实现对象保持原身份。原生同源审计读取在调用者或借用资格退出时及时结束等待，真实持久 flush 和句柄清理继续由原 owner 完成。
- fb602dd: 提供 Program 的 useSharedResource、Action.activeInvocations 与 ReactionRun.state，让派生资源及运行事实通过同一对象供 Promise、Observable 与 hooks 消费，并保留真实执行的结算寿命。

  统一根、局部 Surface 与嵌套集合的 Invocation 查回。回执保留由实际 Host 调用 owner 负责；completedRetentionMs/maxCompleted 配置移至 RuntimeOptions/HostOptions，Gateway 只传输同一回执，不重执行或依赖当前 Expose。

  修复可选原生 Action 展示在同步注册、退出与重入时的贡献归属和释放失败结算。同步开发 Skill 与迁移说明。
- 6f9b816: Program、本地与远程 SDK、可选原生 UI 共用 session.files、SessionFiles、FileResource 和 FileWatch；路径资源复用同一读侧与原 owner，原生范围和正式 WorkspaceFiles 类型贯穿 Gateway 字节及变化端点。

  HostOptions.files 与 RuntimeOptions.files 借入已安装的 NativeFiles，应用退出等待自身实际文件读取与监听清理。Gateway 可显式借用已有 Registry，文件端点逐次复核 Program、accessId、认证主体和交付权限；重连撤旧 ready 后等新 watch 重读。

  NativeSession.dispose 返回同一次 Promise，先同步撤观察资格，再等待本 UI 创建的 raw watch；需要完整清理结果的消费者改为 await dispose() 或 await using。借入 SDK 和全域文件能力仍由其原 owner 销毁。

  开发 Skill/reference 同步公共入口与权限、范围和清理边界。本次只记录待发变化，源码保持 0.x，不按批次升号或执行发布。

  文件资源持续更新遵循正式 FS 通知边界；固定 rc.3 本地 provider 可能抑制同路径 50ms 内的 change 且不补尾通知。已知写入完成后显式 invalidate，立即核验使用单次权威查询，不承诺自动追上每次连续写入。
- b2db37f: 共享 Program 使用发布者声明的 name/version，远程 SDK 只接受明确共享的程序定义。创建、补齐、恢复及所有会话请求与流重连共同核验真实 Program；同名且 wire 兼容的视图不能使另一程序被误认。局部无名 Program 继续用于本地组合，本地传入真实定义仍核对象身份；共享身份不替代认证、Workspace 或 Session 权限。原生 UI、真实包消费者与版本化开发指导同步使用这份合同。
- 6f120f7: 本地、远程及 Program 会话共用 journal 的原生消息分页、冷安全开场和持久追加观察。Host 借用确切 Context 中已安装的 Controller/Query 并拥有读侧的取消与实际清理；子会话使用已核验的原生父子地址，冷会话恢复沿原生日志接合追加顺序。Gateway 转发同一入口，独立 Runtime 不隐装 Controller。远程 openEvents 继续由官方 RemoteJournalStream 管理重连和缺口，新增单次开场取消，并保留读取与清理同时失败的完整原因；同一会话资格撤销只向高级流交付一次原原因。

### Patch Changes

- 7a56dd0: 动态集合合同只签发轻量地址，不永久缓存历史业务 key。Module 持有当前实例的动态声明名称：同地址重复渲染稳定，切换目标或撤下重挂产生新代际；已受理调用继续由原 owner 结算，SDK 仍按完整地址投射。

  Job.wait 改为非消费状态等待，不再调用原生 registry.wait 或影响模型完成通知。显式 collect 消费原生输出 cursor，首个终态消费可取得 producer result；取消与清理不伪造结果确认。

  公开 Disposable/AsyncDisposable 声明显式携带标准库依赖，浏览器不再依赖 Node ambient 补齐。仓外 UI 安装增加 ES2024/DOM、types:[] 的严格声明消费，日常与正式发行共用已有验证链。
- Updated dependencies [d254b19]
- Updated dependencies [2cadde4]
- Updated dependencies [546f161]
- Updated dependencies [a7ad54c]
- Updated dependencies [7a56dd0]
- Updated dependencies [d5dc92f]
- Updated dependencies [50d10a4]
- Updated dependencies [214a438]
- Updated dependencies [b757de0]
- Updated dependencies [fb602dd]
- Updated dependencies [6f120f7]
- Updated dependencies [fd163f2]
- Updated dependencies [6f9b816]
- Updated dependencies [b2db37f]
- Updated dependencies [6f120f7]
  - @hanger-source/dsh-reactive@0.22.0

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
