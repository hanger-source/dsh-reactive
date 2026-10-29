# @hanger-source/dsh-reactive

此文件保留源码版本准备记录；条目和版本号本身不表示 npm 已发布或公共 API 已稳定。

## 0.29.0

### Minor Changes

- 5396d88: 人工问题公开 origin.continuation，直接、PTC 和 Action 内提问均持久保留原模型步骤的 Goal 关联，冷恢复及远程读取消费同一事实。迟答作为原参与反馈准入，不借用后来 Goal 或新修订的权限；缺证、暂停或被替换时保留原消息，新的可信输入才可带入旧答案事实。

  WorkSelector 从同源事实提供 question.reply-blocked 及 unavailable、superseded、inactive 原因，保留完整原问题、交付身份和当前 Goal。响应者政策、业务授权、原生问答生命周期与业务 Operation 仍由各自 owner 管理。

## 0.28.1

### Patch Changes

- edc5796: 将工具的 Goal 控制关联固定到真正准备并准入的模型步骤，完成、等待与 Operation Follow 共同保留原修订。修复暂停目标后的独立问题仍可完成该目标，以及旧模型响应在 Goal 编辑后借用新修订的问题；PTC 子调用沿根步骤使用同一关联。

  结果续接在原生 prompt 装配之前完成准入和 rearm，异步等待后按本次实际准备事实复核，避免旧 prompt 与新 Goal 身份混用。无 Goal 的结果在连续模型步骤中保留原关联，不能借用会话中另一个 Goal；真实原生 Goal 控制仍可为后续步骤建立新关联。业务执行、原生 Goal 控制与读取的 owner 保持各自职责。

  正式工具 dispatch 结束后，后台代码继承的 Agent 身份不再签发 Goal 控制；同步重入通知不能覆盖较新关联。结果 rearm 后拒准入时撤下本次尚未消费的自动推进，原消息与原关联保留，供新的可信输入处理。

## 0.28.0

### Minor Changes

- 3e1bc54: 在 progress 中提供同源的 ModelPreparation，观察当前模型准备实际等待的输入、委托、步骤检查、Context、Skill 与原生装配。请求和 Work 按完整原输入关联同一准备事实；本地、远程 SDK 与原生 UI 消费相同合同。

  完整有效声明相同时保留原绑定和修订，程序可以观察自己的进展而不重复装配。并列 Context 重收集先结束原批次读取；Program 失败撤销原生绑定时保留实际错误原因。

  ConversationProgressSnapshot 和 RequestSnapshot 的自有来源需要提供 preparation（无当前准备时为 null）。准备观察不持久化、不参与调度、不签发权限；取消、结束及 Agent 退出撤下旧等待，冷会话不复活历史准备。

## 0.27.0

### Minor Changes

- 8ebff7b: 以原生 turn/end:error 提供同源 ExecutionFailure，贯通会话、请求、Work 与远程 SDK，保留真实输入和未入历史消息的失败因果。自定义 ConversationActivity 与 RequestActivity 来源需要提供 failures。

  晚答失败后沿原消息 ID 与内容交还正式 inbox，实际 Agent 激活也核验并恢复进程中断留下的交接；只读打开不修改队列。保留等待原生 flush，不重答、不自动重试或恢复 Goal，取消和已消费消息不复活。

## 0.26.0

### Minor Changes

- 8b71ea6: 公开晚答的 HumanReplyDelivery，区分原生排队、步骤提议与问题结算。整体与固定问题快照均提供 replies，Work 按原问题输入关联同一交付事实；人工读侧使用单一完整快照，本地与远程 SDK、可选 UI 共用 HumanSnapshot。

  自定义 HumanSnapshot 与 HumanQuestionSnapshot 来源需要提供 replies；ProgramQuestions 保留为传输合同，运行对象的人工观察使用 HumanSnapshot。取消或提议窗口结束不代表模型消费，未进入输入历史的晚答提议失败不会改写原请求结算。

## 0.25.0

### Minor Changes

- 45be451: 提供 SessionAttachments.readImage，使 Program、本地与远程 SDK 和可选原生 UI 按原会话资格读取已登记图片。复用原生 Controller 的引用授权与冷读取，字节采用独占缓冲区和正式 binary codec；取消只结束消费者等待，原生 owner 退出仍等待实际读取结算。

  历史文件附件仍缺少 dsh rc.3 的公开会话授权读取入口，不以内容哈希或上传回执扩大读取权限。

## 0.24.0

### Minor Changes

- d1bcc55: 远程 Action 在分派前固定 UUID 受理身份，受理回执未知错误携带 invocationId，调用者可沿原身份只读查回。TypedRPC startAction 请求必须提供该身份；不自动重发或推断业务未执行。

  宿主唯一调用记录在解析与准入前登记身份，相同请求返回原 Invocation，不同请求冲突，同步重入明确报告受理中。普通本地自动身份保留原输入能力；显式身份使用 JSON 原值并受既有进程 owner、保留期和数量限制约束。
- 6db04b3: 模块通过 useWorkspace 和 useProgramSession 借用真实所属环境，无需逐层转交会话与工作区。类型化输入和提交须核验当前 Program 合同，普通输入保持原服务参与语义。

  OperationFollow 的结果观察与恢复筛选共同核验正式结果、原 continuation 与完整 wait；同名无关请求不再冒认原操作反馈或阻止恢复。

## 0.23.0

### Minor Changes

- 0a8fcdd: Action 绑定支持共同业务资格资源，声明、调用与本地和远程消费者共享其观察；资格撤销约束尚未进入业务执行的准入，并保留业务执行与持久交接的责任边界。同步版本化开发指导。
- eff2825: 公开协作反馈的持久观察、工作范围局部资源及调用发生身份。
- df85eac: 支持组件关系作用域及其异步关闭结算，明确 Program、Workspace 和业务 Operation 的归属边界。
- 35dce22: 模块实现可携带类型化子模块与 Operation owner，Program 只登记直接模块。dsh 原生模块沿同一静态依赖图安装原生能力，动态挂载继续负责声明、出口和组件观察；移除声明不撤销应用恢复 owner 或已受理业务。

  多个模块可以贡献同一个必需的人工响应政策。按函数身份共享，最后一项退出才撤销当前资格，不合并不同政策或扩大原问题的授权。

  Operation owner 可一次接入既有可靠通知来源。宿主核验真实 owner 与服务参与者，只对已持久受理或确认未开始的结果执行来源确认；未知、待定或失败保留待办。关闭停止新交接，等待真实登记、投递和确认结算后释放来源，继续复用原 follow、反馈与输入身份。

  公共类型、实际包消费者及版本化开发指导同步这些装配和生命周期规则。
- df03bbc: 共享资源资格并明确独立 Follow 登记准入与未登记事实。
- 2a103b4: 让原生 Tool 与 Skill 共享资源资格；审批中撤销阻止旧调用，方法准备保留完整贡献来源并丢弃失效读取。
- f3f7b42: 支持为 Reaction 运行显式指定附加 Scope，使实例 owner 与组件关系共同持有同一运行并等待真实结算。
- de77251: 公开请求声明、真实输入、独立调用与回复发生身份，以及当前 Goal 的权威输入关联。请求的工具和最后回复由同一原生投影事实派生，未取得步骤证据的外部回复不归入业务请求。

  工具发生以正式 start 序号区分，保留跨轮复用 provider callId 的完整历史。人工问题只关联唯一真实的同 ID 提问，后来的普通工具不改变旧问题来源；rc.3 无法核验重复提问代际时明确拒绝来源读取或人工参与，不猜测原工作。

  新增可提取的业务范围选择器，按应用定义的工作身份共同观察多个已取得的会话。保留完整多输入、原会话、问题、等待和 Goal 修订，分别呈现进展与关联条件，不产生新的继续权限或任务状态。

  本地和远程消费者共用原始因果事件，纯范围观察不执行业务输入 transform。远程会话保留真实父会话身份。共享派生读侧允许沿来源和观察者连接寿命使用，无须构造额外 owner。

## 0.22.0

### Minor Changes

- d254b19: 将工作区成员资格贯通到 Session、Program、本地与远程 SDK、原生资源和人工控制。成员撤销或工作区删除结束旧读取与准入资格，已受理调用、持久回执及独立业务 Operation 继续由原 owner 结算。

  会话取得回执携带 accessId，后续会话请求在装配前核验原资格；无观察的旧 SDK 也不能跨过移除再加入继续使用。显式重新取得保留原生 Agent，并建立新的 Program 图与读取身份。提供 ConversationAccessRevokedError 和只读 signal，开发指导及迁移说明同步更新。
- 2cadde4: 会话上传同步返回 FileUploadInvocation，普通 Promise、独立等待和有限活动资源消费同一次调用。字节准备可以取消；开始分派后退出等待不覆盖实际回执，也不重新上传。本地会话退出等待已经开始的原生存储，远程运输失败明确保留回执未知及原始原因。原生 UI、公开消费类型和版本化开发指导共同使用这份合同。原先直接 await uploadFile 的消费者需要改为读取调用的 result 或 wait()。
- a7ad54c: Invocation 统一提供 wait({ signal })，Action、原生写入和上传可以通过同一个合同消费。每次等待只借原 result，取消不改写 activity、结果或 Operation 关联；各签发者的读取 owner 继续适用。

  本地 SDK Action 的调用者 signal 与真正执行 signal 分开：前者限制进入 execute 之前的准入和持久准备，业务开始后不再把真实成功改成 failed 或丢失 Operation 回执。真正的 Host、工具与显式 execution 取消语义保持；本地按原地址查回仍返回原对象，首次调用者取消不会成为永久等待条件。

  远程 Invocation 的 wait 仅限定本 SDK 的等待，不向 Host 发送执行取消。公共类型、泛型仓外消费者、开发 Skill 与调用回执等待指导同步更新。自建 Invocation 实现需补 wait 方法，现有普通消费者使用框架签发对象即可。
- 7a56dd0: 动态集合合同只签发轻量地址，不永久缓存历史业务 key。Module 持有当前实例的动态声明名称：同地址重复渲染稳定，切换目标或撤下重挂产生新代际；已受理调用继续由原 owner 结算，SDK 仍按完整地址投射。

  Job.wait 改为非消费状态等待，不再调用原生 registry.wait 或影响模型完成通知。显式 collect 消费原生输出 cursor，首个终态消费可取得 producer result；取消与清理不伪造结果确认。

  公开 Disposable/AsyncDisposable 声明显式携带标准库依赖，浏览器不再依赖 Node ambient 补齐。仓外 UI 安装增加 ES2024/DOM、types:[] 的严格声明消费，日常与正式发行共用已有验证链。
- 50d10a4: Team 的运行态 roster 与持久成员投影分开，通过正式原生成员和 Agent 状态通知提供同源资源，可与任务板、其他资源、RxJS 和 hooks 共同组合。读取冷会话不创建 Agent，旧来源及 provider 撤销仍沿实际 owner。

  Team 与 Schedule 写入同步返回 NativeInvocation，复用核心 Invocation 合同并保留真实 native 来源、同次 result、有限活动资源及独立 wait。取消等待或退出 SDK 不丢失原生最终成功、拒绝或迟到存储失败；调用拒绝不冒充能力清理失败。直接 Promise 消费迁移为 call.result，可取消等待使用 call.wait。能力 owner 等待真实写入结算，正式安装 effect 按实际 disposer 身份释放读依赖和自有 provider，不建立第二个原生执行队列。

  同步公共类型出口、仓外消费者、运行语义及同版本开发 Skill。迁移见 docs/migration-native-invocations.md。
- 214a438: 工作区可直接共享已有 ResourceSource，并保留读取寿命与原失效原因；资源同步投影不再交付未观察的 Promise。

  Jobs 提供正式终态移除控制，Schedule 排队写入与 SDK 等待分离取消信号；Jobs、Team、Schedule 分域提供公开纯合同叶。

  客户端前置合同构建按目录分权，Inbox 拥有独立入口；浏览器公共入口共用拆分产物，保持运行时类与合同身份。仓外消费覆盖跨入口的实际传输与纯原生合同浏览器闭包。
- b757de0: Session 持久归属与子委托受理意图保存实际 Program 合同身份。创建补齐、冷读取、恢复及子输入解释先核原身份；共享换约或具名/本地转换抛出公共 SessionProgramMismatchError，远程 SDK 保留同一类型及原身份事实。旧归属或委托记录缺少 program 字段时明确拒绝，不自动迁移。

  Application 与 Environment 在定义时取得自己的环境及 Skill 声明快照，异步装配不受调用者后续修改影响，Program 实现对象保持原身份。原生同源审计读取在调用者或借用资格退出时及时结束等待，真实持久 flush 和句柄清理继续由原 owner 完成。
- fb602dd: 提供 Program 的 useSharedResource、Action.activeInvocations 与 ReactionRun.state，让派生资源及运行事实通过同一对象供 Promise、Observable 与 hooks 消费，并保留真实执行的结算寿命。

  统一根、局部 Surface 与嵌套集合的 Invocation 查回。回执保留由实际 Host 调用 owner 负责；completedRetentionMs/maxCompleted 配置移至 RuntimeOptions/HostOptions，Gateway 只传输同一回执，不重执行或依赖当前 Expose。

  修复可选原生 Action 展示在同步注册、退出与重入时的贡献归属和释放失败结算。同步开发 Skill 与迁移说明。
- fd163f2: 将跨作用域派生的读取资格接入同一 signals 图；读取失败通过订阅错误接点传播到普通消费者和原生 UI。

  Reaction 停止立即撤销来源与排队工作，draining 和 settled 保留已开始 Promise 的实际结算与迟到失败；运行实例结算前不接受第二次 start。

  Action 和 NativeTools 的准入等待随原调用、声明代际与 Program 退出及时结束。原 execution 身份保持不变，原生最终 guard 拒绝已撤销的旧准入；已经进入业务的工作保留原 owner。
- 6f9b816: Program、本地与远程 SDK、可选原生 UI 共用 session.files、SessionFiles、FileResource 和 FileWatch；路径资源复用同一读侧与原 owner，原生范围和正式 WorkspaceFiles 类型贯穿 Gateway 字节及变化端点。

  HostOptions.files 与 RuntimeOptions.files 借入已安装的 NativeFiles，应用退出等待自身实际文件读取与监听清理。Gateway 可显式借用已有 Registry，文件端点逐次复核 Program、accessId、认证主体和交付权限；重连撤旧 ready 后等新 watch 重读。

  NativeSession.dispose 返回同一次 Promise，先同步撤观察资格，再等待本 UI 创建的 raw watch；需要完整清理结果的消费者改为 await dispose() 或 await using。借入 SDK 和全域文件能力仍由其原 owner 销毁。

  开发 Skill/reference 同步公共入口与权限、范围和清理边界。本次只记录待发变化，源码保持 0.x，不按批次升号或执行发布。

  文件资源持续更新遵循正式 FS 通知边界；固定 rc.3 本地 provider 可能抑制同路径 50ms 内的 change 且不补尾通知。已知写入完成后显式 invalidate，立即核验使用单次权威查询，不承诺自动追上每次连续写入。
- b2db37f: 共享 Program 使用发布者声明的 name/version，远程 SDK 只接受明确共享的程序定义。创建、补齐、恢复及所有会话请求与流重连共同核验真实 Program；同名且 wire 兼容的视图不能使另一程序被误认。局部无名 Program 继续用于本地组合，本地传入真实定义仍核对象身份；共享身份不替代认证、Workspace 或 Session 权限。原生 UI、真实包消费者与版本化开发指导同步使用这份合同。
- 6f120f7: 本地、远程及 Program 会话共用 journal 的原生消息分页、冷安全开场和持久追加观察。Host 借用确切 Context 中已安装的 Controller/Query 并拥有读侧的取消与实际清理；子会话使用已核验的原生父子地址，冷会话恢复沿原生日志接合追加顺序。Gateway 转发同一入口，独立 Runtime 不隐装 Controller。远程 openEvents 继续由官方 RemoteJournalStream 管理重连和缺口，新增单次开场取消，并保留读取与清理同时失败的完整原因；同一会话资格撤销只向高级流交付一次原原因。

### Patch Changes

- 546f161: 框架代码构建与打包资产准备分开，普通 core prepack、工作区打包和正式发行共享完整准备依赖。默认框架类型与行为检查独立于示例验证，保留现有发行资产。

  Native UI 独立构建包含 client 浏览器产物前置任务。日常 tgz 与正式发行消费同一份安装包 Skill/CLI 检查，打包校验同时拒绝缺少 manifest 字面资产的产物。
- d5dc92f: 同次 Program 步骤准备并列连接独立模块检查；pending 依赖不再遮住兄弟模块的真实错误，检查集合变化释放旧次等待并重新准备当前集合。

  原生 Skills 的目录和正文使用会话真实当前工作目录，并随同会话目录变化失效。Subagents 提供可组合的后代资源与正式 ancestor 权限下的选定子分支中断；读取、控制和带 Goal 的委托保留各自原生 owner。

  认证 transport 拥有物理连接及 uplink 的真实清理，退出等待实际静止并保留清理失败。独立 SDK 释放自有 transport；借用 Connection 的 SDK 不取得其装配销毁权，应用需把自建 transport 的 dispose 交给原装配 owner。远程进展流的载体断线继续使用官方恢复；终止错误保留在原观察 owner，连接重置不将已终止的入口变成无法完成的 pending，重新 open 会话取得新的观察。

  开发 Skill 同步说明这些公共能力与生命周期边界。源码继续保持 0.x，版本准备统一合并待发 Changesets，不按建设批次升号。
- 6f120f7: 公开视图每次 ready 的合同验证失败只使当前观察失败；同一资源后续交付有效值时，本地、远程与 hooks 沿原连接恢复。来源真正的错误或完成仍结束该连接。

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

## 4.0.0

### Major Changes

- 2fd5838: ProgramSessionClient 表达人工和服务共同的会话读取、输入、Surface 与 Operation 能力，HumanProgramSessionClient 单独表达问答、前台认领及 Goal 控制。host.service(actorId) 返回绑定固定身份的应用视图，服务可创建、打开和恢复会话、编辑自己的输入及上传文件，逐调用不能改写 actor。

  Gateway 通过必填 participant(principal) 签发角色及 actor，替换只返回字符串的 actor 配置；输入、Action、Operation 与上传使用同一受信端口，Invocation 关联同时核验角色与 actor，服务器拒绝服务执行人工控制。新增服务 SDK 工厂，与人工 SDK 复用同一套正式连接、资源与恢复机制。固定 rc.3 的 answer 传输不授予 human 权限，none 连接不伪装为具有完整恢复能力。角色错配错误保留宿主已确认的会话身份。开发指导及仓外消费者同步此合同，迁移见 docs/migration-participant-ports.md。
- 6847a95: 根会话与集合成员通过 module(mount) 借出按局部 Surface 编程的 SDK，保留 source/selected 合同、原始资源对象、参数类型和本地/远程调用时序；模块不再要求消费函数知道根路径。挂载与父选择共同限制可见性，投射不创建缓存、业务 owner 或控制权限。

  ProgramSessionClient 统一权威请求核对，HumanProgramSessionClient 提供暂停和取消；控制回执与请求状态类型归核心合同，client 重导出同一类型。调用级 signal 及句柄退出只结束本次等待，已经进入原生控制的工作继续完成。服务身份可读请求状态但不能取得人工控制。远程 journal 保留正式 carrier 的专有合同。

  本地 cancel 现在返回 ProgramCancelResult，而非 Promise<void>；自定义 SurfaceClient 和 ProgramSessionClient 实现需提供同一模块与会话能力。迁移方式见 docs/migration-sdk-modules.md。同版本开发 Skill、本地与远程直接消费者、实际 Gateway 和三环境包消费入口同步此合同。

  dsh 发行声明排除已标记 internal 的构造及装配入口，公开观察类型直接来自核心合同；本地 SDK 不再因内部宿主类型牵入持久投影 Registry 的实现声明。运行时装配继续使用同一实现，跨包内核合同保持独立出口。

### Minor Changes

- f8d99d1: ResourceSource 统一表达资源对象、业务 hook 返回的观察对象和原 Observation 流。资源算子、shareResource、useResource、资源视图及 Context 直接消费同一来源，不要求外围逐层拆开 observations$；普通值视图继续保持值流语义。

  useObservable、useShared、useRelation 可直接接收已有 Observable，现场构造来源继续使用 factory 与显式依赖。useResource 按实际选中的来源决定处理，factory 选择已有资源对象仍借用其原观察流及 owner；自建流继续由程序持有查询与共享。公开视图按原观察流身份保留连接，资源对象与其流之间切换不重新查询。同步公共消费者与同版本开发指导，不增加业务状态、执行权或生命周期。

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
