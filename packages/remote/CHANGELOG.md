# @hanger-source/dsh-reactive-remote

此文件保留源码版本准备记录；条目和版本号本身不表示 npm 已发布或公共 API 已稳定。

## 0.33.2

### Patch Changes

- Updated dependencies [f245779]
  - @hanger-source/dsh-reactive-dsh@0.33.2
  - @hanger-source/dsh-reactive-client@0.33.2
  - @hanger-source/dsh-reactive@0.33.2

## 0.33.1

### Patch Changes

- Updated dependencies [005bc2d]
  - @hanger-source/dsh-reactive@0.33.1
  - @hanger-source/dsh-reactive-client@0.33.1
  - @hanger-source/dsh-reactive-dsh@0.33.1

## 0.33.0

### Patch Changes

- @hanger-source/dsh-reactive-client@0.33.0
  - @hanger-source/dsh-reactive@0.33.0
  - @hanger-source/dsh-reactive-dsh@0.33.0

## 0.32.0

### Patch Changes

- Updated dependencies [29e5a31]
  - @hanger-source/dsh-reactive-dsh@0.32.0
  - @hanger-source/dsh-reactive-client@0.32.0
  - @hanger-source/dsh-reactive@0.32.0

## 0.31.0

### Patch Changes

- Updated dependencies [6fea66e]
  - @hanger-source/dsh-reactive-dsh@0.31.0
  - @hanger-source/dsh-reactive-client@0.31.0
  - @hanger-source/dsh-reactive@0.31.0

## 0.30.1

### Patch Changes

- Updated dependencies [4d5abf9]
  - @hanger-source/dsh-reactive@0.30.1
  - @hanger-source/dsh-reactive-client@0.30.1
  - @hanger-source/dsh-reactive-dsh@0.30.1

## 0.30.0

### Minor Changes

- 09d733c: 公开 ToolActivity.invocationId、tool.invocation 事实和 OperationFollowSnapshot.callSequence，将业务 Invocation、原生工具发生及原操作参与按正式身份连接。重复 provider 标签不再要求消费者解析不透明 ID，业务结果与原生后置结果分别保留。

  完整 Invocation 在执行前登记来源关联，失败通过 registration 阶段结算且不进入业务或 Operation 准备。关系使用现有原生日志和投影恢复；缺证保留 null，不推测历史来源，也不改变既有 Source 或业务恢复合同。同版本开发指导说明固定 rc.3 下重复 PTC 子 ID 的正式冷读取限制。

### Patch Changes

- Updated dependencies [09d733c]
  - @hanger-source/dsh-reactive@0.30.0
  - @hanger-source/dsh-reactive-dsh@0.30.0
  - @hanger-source/dsh-reactive-client@0.30.0

## 0.29.0

### Minor Changes

- 5396d88: 人工问题公开 origin.continuation，直接、PTC 和 Action 内提问均持久保留原模型步骤的 Goal 关联，冷恢复及远程读取消费同一事实。迟答作为原参与反馈准入，不借用后来 Goal 或新修订的权限；缺证、暂停或被替换时保留原消息，新的可信输入才可带入旧答案事实。

  WorkSelector 从同源事实提供 question.reply-blocked 及 unavailable、superseded、inactive 原因，保留完整原问题、交付身份和当前 Goal。响应者政策、业务授权、原生问答生命周期与业务 Operation 仍由各自 owner 管理。

### Patch Changes

- Updated dependencies [5396d88]
  - @hanger-source/dsh-reactive@0.29.0
  - @hanger-source/dsh-reactive-dsh@0.29.0
  - @hanger-source/dsh-reactive-client@0.29.0

## 0.28.1

### Patch Changes

- edc5796: 将工具的 Goal 控制关联固定到真正准备并准入的模型步骤，完成、等待与 Operation Follow 共同保留原修订。修复暂停目标后的独立问题仍可完成该目标，以及旧模型响应在 Goal 编辑后借用新修订的问题；PTC 子调用沿根步骤使用同一关联。

  结果续接在原生 prompt 装配之前完成准入和 rearm，异步等待后按本次实际准备事实复核，避免旧 prompt 与新 Goal 身份混用。无 Goal 的结果在连续模型步骤中保留原关联，不能借用会话中另一个 Goal；真实原生 Goal 控制仍可为后续步骤建立新关联。业务执行、原生 Goal 控制与读取的 owner 保持各自职责。

  正式工具 dispatch 结束后，后台代码继承的 Agent 身份不再签发 Goal 控制；同步重入通知不能覆盖较新关联。结果 rearm 后拒准入时撤下本次尚未消费的自动推进，原消息与原关联保留，供新的可信输入处理。
- Updated dependencies [edc5796]
  - @hanger-source/dsh-reactive@0.28.1
  - @hanger-source/dsh-reactive-dsh@0.28.1
  - @hanger-source/dsh-reactive-client@0.28.1

## 0.28.0

### Minor Changes

- 3e1bc54: 在 progress 中提供同源的 ModelPreparation，观察当前模型准备实际等待的输入、委托、步骤检查、Context、Skill 与原生装配。请求和 Work 按完整原输入关联同一准备事实；本地、远程 SDK 与原生 UI 消费相同合同。

  完整有效声明相同时保留原绑定和修订，程序可以观察自己的进展而不重复装配。并列 Context 重收集先结束原批次读取；Program 失败撤销原生绑定时保留实际错误原因。

  ConversationProgressSnapshot 和 RequestSnapshot 的自有来源需要提供 preparation（无当前准备时为 null）。准备观察不持久化、不参与调度、不签发权限；取消、结束及 Agent 退出撤下旧等待，冷会话不复活历史准备。

### Patch Changes

- Updated dependencies [3e1bc54]
  - @hanger-source/dsh-reactive@0.28.0
  - @hanger-source/dsh-reactive-dsh@0.28.0
  - @hanger-source/dsh-reactive-client@0.28.0

## 0.27.0

### Minor Changes

- 8ebff7b: 以原生 turn/end:error 提供同源 ExecutionFailure，贯通会话、请求、Work 与远程 SDK，保留真实输入和未入历史消息的失败因果。自定义 ConversationActivity 与 RequestActivity 来源需要提供 failures。

  晚答失败后沿原消息 ID 与内容交还正式 inbox，实际 Agent 激活也核验并恢复进程中断留下的交接；只读打开不修改队列。保留等待原生 flush，不重答、不自动重试或恢复 Goal，取消和已消费消息不复活。

### Patch Changes

- Updated dependencies [8ebff7b]
  - @hanger-source/dsh-reactive@0.27.0
  - @hanger-source/dsh-reactive-dsh@0.27.0
  - @hanger-source/dsh-reactive-client@0.27.0

## 0.26.0

### Minor Changes

- 8b71ea6: 公开晚答的 HumanReplyDelivery，区分原生排队、步骤提议与问题结算。整体与固定问题快照均提供 replies，Work 按原问题输入关联同一交付事实；人工读侧使用单一完整快照，本地与远程 SDK、可选 UI 共用 HumanSnapshot。

  自定义 HumanSnapshot 与 HumanQuestionSnapshot 来源需要提供 replies；ProgramQuestions 保留为传输合同，运行对象的人工观察使用 HumanSnapshot。取消或提议窗口结束不代表模型消费，未进入输入历史的晚答提议失败不会改写原请求结算。

### Patch Changes

- Updated dependencies [8b71ea6]
  - @hanger-source/dsh-reactive@0.26.0
  - @hanger-source/dsh-reactive-dsh@0.26.0
  - @hanger-source/dsh-reactive-client@0.26.0

## 0.25.0

### Minor Changes

- 45be451: 提供 SessionAttachments.readImage，使 Program、本地与远程 SDK 和可选原生 UI 按原会话资格读取已登记图片。复用原生 Controller 的引用授权与冷读取，字节采用独占缓冲区和正式 binary codec；取消只结束消费者等待，原生 owner 退出仍等待实际读取结算。

  历史文件附件仍缺少 dsh rc.3 的公开会话授权读取入口，不以内容哈希或上传回执扩大读取权限。

### Patch Changes

- Updated dependencies [45be451]
  - @hanger-source/dsh-reactive@0.25.0
  - @hanger-source/dsh-reactive-dsh@0.25.0
  - @hanger-source/dsh-reactive-client@0.25.0

## 0.24.0

### Minor Changes

- d1bcc55: 远程 Action 在分派前固定 UUID 受理身份，受理回执未知错误携带 invocationId，调用者可沿原身份只读查回。TypedRPC startAction 请求必须提供该身份；不自动重发或推断业务未执行。

  宿主唯一调用记录在解析与准入前登记身份，相同请求返回原 Invocation，不同请求冲突，同步重入明确报告受理中。普通本地自动身份保留原输入能力；显式身份使用 JSON 原值并受既有进程 owner、保留期和数量限制约束。

### Patch Changes

- Updated dependencies [d1bcc55]
- Updated dependencies [6db04b3]
  - @hanger-source/dsh-reactive@0.24.0
  - @hanger-source/dsh-reactive-dsh@0.24.0
  - @hanger-source/dsh-reactive-client@0.24.0

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
- Updated dependencies [f5b98ce]
- Updated dependencies [df03bbc]
- Updated dependencies [45fb314]
- Updated dependencies [2a103b4]
- Updated dependencies [f3f7b42]
- Updated dependencies [de77251]
  - @hanger-source/dsh-reactive@0.23.0
  - @hanger-source/dsh-reactive-dsh@0.23.0
  - @hanger-source/dsh-reactive-client@0.23.0

## 0.22.0

### Minor Changes

- d254b19: 将工作区成员资格贯通到 Session、Program、本地与远程 SDK、原生资源和人工控制。成员撤销或工作区删除结束旧读取与准入资格，已受理调用、持久回执及独立业务 Operation 继续由原 owner 结算。

  会话取得回执携带 accessId，后续会话请求在装配前核验原资格；无观察的旧 SDK 也不能跨过移除再加入继续使用。显式重新取得保留原生 Agent，并建立新的 Program 图与读取身份。提供 ConversationAccessRevokedError 和只读 signal，开发指导及迁移说明同步更新。
- 2cadde4: 会话上传同步返回 FileUploadInvocation，普通 Promise、独立等待和有限活动资源消费同一次调用。字节准备可以取消；开始分派后退出等待不覆盖实际回执，也不重新上传。本地会话退出等待已经开始的原生存储，远程运输失败明确保留回执未知及原始原因。原生 UI、公开消费类型和版本化开发指导共同使用这份合同。原先直接 await uploadFile 的消费者需要改为读取调用的 result 或 wait()。
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

- 214a438: 工作区可直接共享已有 ResourceSource，并保留读取寿命与原失效原因；资源同步投影不再交付未观察的 Promise。

  Jobs 提供正式终态移除控制，Schedule 排队写入与 SDK 等待分离取消信号；Jobs、Team、Schedule 分域提供公开纯合同叶。

  客户端前置合同构建按目录分权，Inbox 拥有独立入口；浏览器公共入口共用拆分产物，保持运行时类与合同身份。仓外消费覆盖跨入口的实际传输与纯原生合同浏览器闭包。
- 1e15519: 日常实包消费与正式发行验收使用同一 Playwright 浏览器流程，实际执行 SDK、原生 Slot 与请求进展页面。认证 Host 从独立后台实包安装启动，原生 UI 继续使用独立 React 18 安装与构建产物。成功回执在异步退出完成后生成，发行证据包含真实页面结果；仅有 bundle 构建不再满足浏览器验收。
- Updated dependencies [d254b19]
- Updated dependencies [6f120f7]
- Updated dependencies [2cadde4]
- Updated dependencies [546f161]
- Updated dependencies [1cff47e]
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
- Updated dependencies [11cd2b9]
  - @hanger-source/dsh-reactive@0.22.0
  - @hanger-source/dsh-reactive-dsh@0.22.0
  - @hanger-source/dsh-reactive-client@0.22.0

## 0.21.0

### Patch Changes

- Updated dependencies [00f0503]
- Updated dependencies [81d05bc]
- Updated dependencies [9c5d4b0]
- Updated dependencies [73e4c02]
  - @hanger-source/dsh-reactive@0.21.0
  - @hanger-source/dsh-reactive-dsh@0.21.0
  - @hanger-source/dsh-reactive-client@0.21.0

## 4.0.0

### Major Changes

- 2fd5838: ProgramSessionClient 表达人工和服务共同的会话读取、输入、Surface 与 Operation 能力，HumanProgramSessionClient 单独表达问答、前台认领及 Goal 控制。host.service(actorId) 返回绑定固定身份的应用视图，服务可创建、打开和恢复会话、编辑自己的输入及上传文件，逐调用不能改写 actor。

  Gateway 通过必填 participant(principal) 签发角色及 actor，替换只返回字符串的 actor 配置；输入、Action、Operation 与上传使用同一受信端口，Invocation 关联同时核验角色与 actor，服务器拒绝服务执行人工控制。新增服务 SDK 工厂，与人工 SDK 复用同一套正式连接、资源与恢复机制。固定 rc.3 的 answer 传输不授予 human 权限，none 连接不伪装为具有完整恢复能力。角色错配错误保留宿主已确认的会话身份。开发指导及仓外消费者同步此合同，迁移见 docs/migration-participant-ports.md。

### Patch Changes

- Updated dependencies [337f349]
- Updated dependencies [2fd5838]
- Updated dependencies [6847a95]
- Updated dependencies [f8d99d1]
- Updated dependencies [0b94998]
  - @hanger-source/dsh-reactive-dsh@4.0.0
  - @hanger-source/dsh-reactive@4.0.0
  - @hanger-source/dsh-reactive-client@4.0.0

## 3.0.0

### Patch Changes

- Updated dependencies [1c9e7ee]
- Updated dependencies [dc676c6]
  - @hanger-source/dsh-reactive@3.0.0
  - @hanger-source/dsh-reactive-client@3.0.0
  - @hanger-source/dsh-reactive-dsh@3.0.0

## 2.1.0

### Patch Changes

- Updated dependencies [5b81e34]
- Updated dependencies [d502e72]
  - @hanger-source/dsh-reactive-dsh@2.1.0
  - @hanger-source/dsh-reactive@2.1.0
  - @hanger-source/dsh-reactive-client@2.1.0

## 2.0.0

### Patch Changes

- Updated dependencies [581b301]
  - @hanger-source/dsh-reactive@2.0.0
  - @hanger-source/dsh-reactive-dsh@2.0.0
  - @hanger-source/dsh-reactive-client@2.0.0

## 1.1.0

### Patch Changes

- Updated dependencies [66a1d07]
- Updated dependencies [7b3ef97]
- Updated dependencies [b4af2f5]
  - @hanger-source/dsh-reactive@1.1.0
  - @hanger-source/dsh-reactive-dsh@1.1.0
  - @hanger-source/dsh-reactive-client@1.1.0

## 1.0.0

### Patch Changes

- Updated dependencies [dd50bb1]
- Updated dependencies [bdd5279]
- Updated dependencies [60f19d6]
  - @hanger-source/dsh-reactive@1.0.0
  - @hanger-source/dsh-reactive-client@1.0.0
  - @hanger-source/dsh-reactive-dsh@1.0.0

## 0.20.2

### Patch Changes

- Updated dependencies [9aca405]
  - @hanger-source/dsh-reactive@0.20.2
  - @hanger-source/dsh-reactive-client@0.20.2
  - @hanger-source/dsh-reactive-dsh@0.20.2

## 0.20.1

### Patch Changes

- Updated dependencies [bdf0417]
  - @hanger-source/dsh-reactive-client@0.20.1
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

## 0.19.2

### Patch Changes

- Updated dependencies [ebf227d]
  - @hanger-source/dsh-reactive-client@0.19.2
  - @hanger-source/dsh-reactive@0.19.2
  - @hanger-source/dsh-reactive-dsh@0.19.2

## 0.19.1

### Patch Changes

- Updated dependencies [84f447f]
  - @hanger-source/dsh-reactive@0.19.1
  - @hanger-source/dsh-reactive-client@0.19.1
  - @hanger-source/dsh-reactive-dsh@0.19.1

## 0.19.0

### Patch Changes

- Updated dependencies [f690a47]
  - @hanger-source/dsh-reactive@0.19.0
  - @hanger-source/dsh-reactive-dsh@0.19.0
  - @hanger-source/dsh-reactive-client@0.19.0

## 0.18.2

### Patch Changes

- Updated dependencies [65812d1]
  - @hanger-source/dsh-reactive-dsh@0.18.2
  - @hanger-source/dsh-reactive-client@0.18.2
  - @hanger-source/dsh-reactive@0.18.2

## 0.18.1

### Patch Changes

- Updated dependencies [48fbba0]
  - @hanger-source/dsh-reactive@0.18.1
  - @hanger-source/dsh-reactive-client@0.18.1
  - @hanger-source/dsh-reactive-dsh@0.18.1

## 0.18.0

### Patch Changes

- Updated dependencies [f13c00f]
  - @hanger-source/dsh-reactive-dsh@0.18.0
  - @hanger-source/dsh-reactive-client@0.18.0
  - @hanger-source/dsh-reactive@0.18.0

## 0.17.2

### Patch Changes

- d458636: requestStatus、pause、cancel 的远程回执等待随调用 Session 句柄或 SDK 退出及时结算，借用的公共 Context 和其他 SDK 继续可用。复用现有取消等待接点，并将所属信号传入正式 Host 合同；Host 异步取得真实句柄后、进入读取或原生控制前重新检查取消。已经进入原生控制的工作仍由 Host 结算，客户端退出不回滚控制或已受理业务。同版本开发指导说明这些等待和执行 owner 的边界。
- Updated dependencies [d458636]
  - @hanger-source/dsh-reactive-client@0.17.2
  - @hanger-source/dsh-reactive@0.17.2
  - @hanger-source/dsh-reactive-dsh@0.17.2

## 0.17.1

### Patch Changes

- Updated dependencies [d158683]
  - @hanger-source/dsh-reactive@0.17.1
  - @hanger-source/dsh-reactive-client@0.17.1
  - @hanger-source/dsh-reactive-dsh@0.17.1

## 0.17.0

### Patch Changes

- Updated dependencies [db574d1]
  - @hanger-source/dsh-reactive@0.17.0
  - @hanger-source/dsh-reactive-client@0.17.0
  - @hanger-source/dsh-reactive-dsh@0.17.0

## 0.16.0

### Patch Changes

- Updated dependencies [469c5c5]
  - @hanger-source/dsh-reactive-dsh@0.16.0
  - @hanger-source/dsh-reactive-client@0.16.0
  - @hanger-source/dsh-reactive@0.16.0

## 0.15.0

### Patch Changes

- Updated dependencies [cb08a3b]
  - @hanger-source/dsh-reactive-dsh@0.15.0
  - @hanger-source/dsh-reactive-client@0.15.0
  - @hanger-source/dsh-reactive@0.15.0

## 0.14.0

### Patch Changes

- Updated dependencies [8dc0fb9]
  - @hanger-source/dsh-reactive@0.14.0
  - @hanger-source/dsh-reactive-client@0.14.0
  - @hanger-source/dsh-reactive-dsh@0.14.0

## 0.13.0

### Patch Changes

- Updated dependencies [2a3e750]
  - @hanger-source/dsh-reactive-client@0.13.0
  - @hanger-source/dsh-reactive@0.13.0
  - @hanger-source/dsh-reactive-dsh@0.13.0

## 0.12.1

### Patch Changes

- 5270fb3: 客户端退出时结束工作区发现与会话创建、打开、恢复的在途回执等待，并向 transport 传递 SDK 取消信号。已经发出的创建保留原引用并报告 CreationUnknownError；释放借用 SDK 不关闭公共 Context，也不撤销 Host 已受理的工作。开发 reference 同步说明取消与创建恢复的归属。
- Updated dependencies [5270fb3]
  - @hanger-source/dsh-reactive-client@0.12.1
  - @hanger-source/dsh-reactive@0.12.1
  - @hanger-source/dsh-reactive-dsh@0.12.1

## 0.12.0

### Patch Changes

- Updated dependencies [5081230]
  - @hanger-source/dsh-reactive-client@0.12.0
  - @hanger-source/dsh-reactive@0.12.0
  - @hanger-source/dsh-reactive-dsh@0.12.0

## 0.11.1

### Patch Changes

- Updated dependencies [9fb4b1a]
- Updated dependencies [be26221]
  - @hanger-source/dsh-reactive@0.11.1
  - @hanger-source/dsh-reactive-client@0.11.1
  - @hanger-source/dsh-reactive-dsh@0.11.1

## 0.11.0

### Patch Changes

- Updated dependencies [19c9a36]
  - @hanger-source/dsh-reactive@0.11.0
  - @hanger-source/dsh-reactive-dsh@0.11.0
  - @hanger-source/dsh-reactive-client@0.11.0

## 0.10.0

### Patch Changes

- 8060a41: Program、本地和远程 SDK 提供同源 human 资源；question(callId) 返回固定原问题的可复用对象，将状态观察、回答和前台等待放在同一对象上。问题状态保留 active、foreground、settled 原事实，不建立新的阶段、计时器或批准权。可选原生 UI 完整保留 pending/error，问题控制沿原 UI generation 与 SDK 准入。服务端口的人工回答和 Goal 控制在运行时明确拒绝，绕过类型声明也不能借用人工身份。
- Updated dependencies [8060a41]
  - @hanger-source/dsh-reactive@0.10.0
  - @hanger-source/dsh-reactive-dsh@0.10.0
  - @hanger-source/dsh-reactive-client@0.10.0

## 0.9.1

### Patch Changes

- Updated dependencies [f6050e9]
  - @hanger-source/dsh-reactive-dsh@0.9.1
  - @hanger-source/dsh-reactive-client@0.9.1
  - @hanger-source/dsh-reactive@0.9.1

## 0.9.0

### Patch Changes

- Updated dependencies [7a31aa8]
  - @hanger-source/dsh-reactive@0.9.0
  - @hanger-source/dsh-reactive-client@0.9.0
  - @hanger-source/dsh-reactive-dsh@0.9.0

## 0.8.0

### Patch Changes

- Updated dependencies [7451ffd]
  - @hanger-source/dsh-reactive-dsh@0.8.0
  - @hanger-source/dsh-reactive-client@0.8.0
  - @hanger-source/dsh-reactive@0.8.0

## 0.7.0

### Patch Changes

- Updated dependencies [424b75d]
  - @hanger-source/dsh-reactive@0.7.0
  - @hanger-source/dsh-reactive-dsh@0.7.0
  - @hanger-source/dsh-reactive-client@0.7.0

## 0.6.0

### Patch Changes

- Updated dependencies [4596edd]
  - @hanger-source/dsh-reactive-dsh@0.6.0
  - @hanger-source/dsh-reactive-client@0.6.0
  - @hanger-source/dsh-reactive@0.6.0

## 0.5.0

### Patch Changes

- Updated dependencies [339cacf]
  - @hanger-source/dsh-reactive-dsh@0.5.0
  - @hanger-source/dsh-reactive-client@0.5.0
  - @hanger-source/dsh-reactive@0.5.0

## 0.4.0

### Patch Changes

- Updated dependencies [923671d]
- Updated dependencies [baaa98b]
  - @hanger-source/dsh-reactive@0.4.0
  - @hanger-source/dsh-reactive-dsh@0.4.0
  - @hanger-source/dsh-reactive-client@0.4.0

## 0.3.0

### Patch Changes

- 1448bec: 原生 Jobs、Schedule、Team、Files、Approval 和 Clock 接受宿主核验的 Program conversation，普通函数与业务 hooks 共用会话能力实例。原生观察延迟到首次订阅，Program 失败或退出只释放其来源观察，不取消已受理工作。根会话在 Program 构造前持久完成工作区归属，创建 ACK 前不签发外部句柄；本地与远程 open/resume 保留同一创建引用，沿原入口完成登记。恢复继续核验成员撤销；开发 Skill 同步说明来源与生命周期。
- Updated dependencies [1448bec]
- Updated dependencies [f713b74]
- Updated dependencies [e4273f8]
  - @hanger-source/dsh-reactive-dsh@0.3.0
  - @hanger-source/dsh-reactive-client@0.3.0
  - @hanger-source/dsh-reactive@0.3.0

## 0.2.2

### Patch Changes

- Updated dependencies [5f53764]
  - @hanger-source/dsh-reactive@0.2.2
  - @hanger-source/dsh-reactive-client@0.2.2
  - @hanger-source/dsh-reactive-dsh@0.2.2

## 0.2.1

### Patch Changes

- ed8c8dc: 明确六包 TypeScript 构建依赖与客户端合同的唯一产物来源，严格编译并执行仓外消费者；以 GitHub OIDC 发布可追溯的同步版本及开发 Skill，并从 npm registry 安装验收。
- Updated dependencies [ed8c8dc]
- Updated dependencies [871403f]
  - @hanger-source/dsh-reactive@0.2.1
  - @hanger-source/dsh-reactive-dsh@0.2.1
  - @hanger-source/dsh-reactive-client@0.2.1

## 0.2.0

首次公开源码。

- 提供已认证 Gateway 宿主，将 Program、输入、Action、Operation、人工参与和进度映射到正式远程合同。
- 复用原生连接与上传服务，保留真实调用者、准入失败和受理未知事实。
