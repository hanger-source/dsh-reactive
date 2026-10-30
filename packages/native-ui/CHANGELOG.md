# @hanger-source/dsh-reactive-native-ui

此文件保留源码版本准备记录；条目和版本号本身不表示 npm 已发布或公共 API 已稳定。

## 0.36.0

### Patch Changes

- Updated dependencies [138eca2]
  - @hanger-source/dsh-reactive@0.36.0
  - @hanger-source/dsh-reactive-client@0.36.0

## 0.35.1

### Patch Changes

- Updated dependencies [cda75bd]
  - @hanger-source/dsh-reactive-client@0.35.1
  - @hanger-source/dsh-reactive@0.35.1

## 0.35.0

### Patch Changes

- 44b319e: 通过纯合同 approvalSurface 和 createNativeApprovalModule 组合原生审批参与。本地、远程 SDK 与可选原生 UI 使用相同 pending 资源和 answer Action，模块携带原生安装与当前等待关联，业务只提供批准要求和响应者授权。

  答复核验真实 human 来源、会话与目标，异步授权后复核当前等待，竞争只交付一次；取消、模块卸载和观察退出保留各自语义。submitted 仅证明答复交给原生等待者，政策、审计、一次性授权与工具执行继续由原生服务决定。不恢复持久 pending、不合成原生审计身份，也不新增远程协议或审批引擎。
- Updated dependencies [44b319e]
  - @hanger-source/dsh-reactive@0.35.0
  - @hanger-source/dsh-reactive-client@0.35.0

## 0.34.1

### Patch Changes

- Updated dependencies [c844248]
  - @hanger-source/dsh-reactive@0.34.1
  - @hanger-source/dsh-reactive-client@0.34.1

## 0.34.0

### Patch Changes

- 7cf35a7: Action Invocation 携带在输入解析和准入前捕获的不可变 origin，保留原会话、Action、绑定、目标、真实来源与完整输入；本地、远程及 lookup 消费同一归属合同。远程受理回执核验原绑定与目标，矛盾回执仍表示受理未知，不重复执行业务。

  Work 通过 `{ sessions?, invocations? }` 显式组合会话与已持有调用，可在无会话的 CLI 中使用，也可用只读工作标签组织共同进展。动态来源保留原 activity 读侧、失效和释放语义，不读取结果、取消业务或授予 Goal 续接权限。旧会话数组参数须迁移，SDK 与 Gateway 须使用匹配版本；详见 docs/migration-work-invocations.md。
- Updated dependencies [7cf35a7]
  - @hanger-source/dsh-reactive@0.34.0
  - @hanger-source/dsh-reactive-client@0.34.0

## 0.33.2

### Patch Changes

- @hanger-source/dsh-reactive-client@0.33.2
  - @hanger-source/dsh-reactive@0.33.2

## 0.33.1

### Patch Changes

- Updated dependencies [005bc2d]
  - @hanger-source/dsh-reactive@0.33.1
  - @hanger-source/dsh-reactive-client@0.33.1

## 0.33.0

### Minor Changes

- 31bae94: 原生 UI 会话可直接消费静态模块、动态模块集合、公开 Operation 槽与已受理 Invocation。模块读侧和 Action 资格沿用同一 SDK 事实，并受原生 UI 代际与作用域约束；UI 退出不改变已受理调用的结果、原操作身份或借入 SDK 的寿命。

### Patch Changes

- @hanger-source/dsh-reactive-client@0.33.0
  - @hanger-source/dsh-reactive@0.33.0

## 0.32.0

### Patch Changes

- @hanger-source/dsh-reactive-client@0.32.0
  - @hanger-source/dsh-reactive@0.32.0

## 0.31.0

### Patch Changes

- @hanger-source/dsh-reactive-client@0.31.0
  - @hanger-source/dsh-reactive@0.31.0

## 0.30.1

### Patch Changes

- Updated dependencies [4d5abf9]
  - @hanger-source/dsh-reactive@0.30.1
  - @hanger-source/dsh-reactive-client@0.30.1

## 0.30.0

### Minor Changes

- 09d733c: 公开 ToolActivity.invocationId、tool.invocation 事实和 OperationFollowSnapshot.callSequence，将业务 Invocation、原生工具发生及原操作参与按正式身份连接。重复 provider 标签不再要求消费者解析不透明 ID，业务结果与原生后置结果分别保留。

  完整 Invocation 在执行前登记来源关联，失败通过 registration 阶段结算且不进入业务或 Operation 准备。关系使用现有原生日志和投影恢复；缺证保留 null，不推测历史来源，也不改变既有 Source 或业务恢复合同。同版本开发指导说明固定 rc.3 下重复 PTC 子 ID 的正式冷读取限制。

### Patch Changes

- Updated dependencies [09d733c]
  - @hanger-source/dsh-reactive@0.30.0
  - @hanger-source/dsh-reactive-client@0.30.0

## 0.29.0

### Minor Changes

- 5396d88: 人工问题公开 origin.continuation，直接、PTC 和 Action 内提问均持久保留原模型步骤的 Goal 关联，冷恢复及远程读取消费同一事实。迟答作为原参与反馈准入，不借用后来 Goal 或新修订的权限；缺证、暂停或被替换时保留原消息，新的可信输入才可带入旧答案事实。

  WorkSelector 从同源事实提供 question.reply-blocked 及 unavailable、superseded、inactive 原因，保留完整原问题、交付身份和当前 Goal。响应者政策、业务授权、原生问答生命周期与业务 Operation 仍由各自 owner 管理。

### Patch Changes

- Updated dependencies [5396d88]
  - @hanger-source/dsh-reactive@0.29.0
  - @hanger-source/dsh-reactive-client@0.29.0

## 0.28.1

### Patch Changes

- edc5796: 将工具的 Goal 控制关联固定到真正准备并准入的模型步骤，完成、等待与 Operation Follow 共同保留原修订。修复暂停目标后的独立问题仍可完成该目标，以及旧模型响应在 Goal 编辑后借用新修订的问题；PTC 子调用沿根步骤使用同一关联。

  结果续接在原生 prompt 装配之前完成准入和 rearm，异步等待后按本次实际准备事实复核，避免旧 prompt 与新 Goal 身份混用。无 Goal 的结果在连续模型步骤中保留原关联，不能借用会话中另一个 Goal；真实原生 Goal 控制仍可为后续步骤建立新关联。业务执行、原生 Goal 控制与读取的 owner 保持各自职责。

  正式工具 dispatch 结束后，后台代码继承的 Agent 身份不再签发 Goal 控制；同步重入通知不能覆盖较新关联。结果 rearm 后拒准入时撤下本次尚未消费的自动推进，原消息与原关联保留，供新的可信输入处理。
- Updated dependencies [edc5796]
  - @hanger-source/dsh-reactive@0.28.1
  - @hanger-source/dsh-reactive-client@0.28.1

## 0.28.0

### Minor Changes

- 3e1bc54: 在 progress 中提供同源的 ModelPreparation，观察当前模型准备实际等待的输入、委托、步骤检查、Context、Skill 与原生装配。请求和 Work 按完整原输入关联同一准备事实；本地、远程 SDK 与原生 UI 消费相同合同。

  完整有效声明相同时保留原绑定和修订，程序可以观察自己的进展而不重复装配。并列 Context 重收集先结束原批次读取；Program 失败撤销原生绑定时保留实际错误原因。

  ConversationProgressSnapshot 和 RequestSnapshot 的自有来源需要提供 preparation（无当前准备时为 null）。准备观察不持久化、不参与调度、不签发权限；取消、结束及 Agent 退出撤下旧等待，冷会话不复活历史准备。

### Patch Changes

- Updated dependencies [3e1bc54]
  - @hanger-source/dsh-reactive@0.28.0
  - @hanger-source/dsh-reactive-client@0.28.0

## 0.27.0

### Minor Changes

- 8ebff7b: 以原生 turn/end:error 提供同源 ExecutionFailure，贯通会话、请求、Work 与远程 SDK，保留真实输入和未入历史消息的失败因果。自定义 ConversationActivity 与 RequestActivity 来源需要提供 failures。

  晚答失败后沿原消息 ID 与内容交还正式 inbox，实际 Agent 激活也核验并恢复进程中断留下的交接；只读打开不修改队列。保留等待原生 flush，不重答、不自动重试或恢复 Goal，取消和已消费消息不复活。

### Patch Changes

- Updated dependencies [8ebff7b]
  - @hanger-source/dsh-reactive@0.27.0
  - @hanger-source/dsh-reactive-client@0.27.0

## 0.26.0

### Minor Changes

- 8b71ea6: 公开晚答的 HumanReplyDelivery，区分原生排队、步骤提议与问题结算。整体与固定问题快照均提供 replies，Work 按原问题输入关联同一交付事实；人工读侧使用单一完整快照，本地与远程 SDK、可选 UI 共用 HumanSnapshot。

  自定义 HumanSnapshot 与 HumanQuestionSnapshot 来源需要提供 replies；ProgramQuestions 保留为传输合同，运行对象的人工观察使用 HumanSnapshot。取消或提议窗口结束不代表模型消费，未进入输入历史的晚答提议失败不会改写原请求结算。

### Patch Changes

- Updated dependencies [8b71ea6]
  - @hanger-source/dsh-reactive@0.26.0
  - @hanger-source/dsh-reactive-client@0.26.0

## 0.25.0

### Minor Changes

- 45be451: 提供 SessionAttachments.readImage，使 Program、本地与远程 SDK 和可选原生 UI 按原会话资格读取已登记图片。复用原生 Controller 的引用授权与冷读取，字节采用独占缓冲区和正式 binary codec；取消只结束消费者等待，原生 owner 退出仍等待实际读取结算。

  历史文件附件仍缺少 dsh rc.3 的公开会话授权读取入口，不以内容哈希或上传回执扩大读取权限。

### Patch Changes

- Updated dependencies [45be451]
  - @hanger-source/dsh-reactive@0.25.0
  - @hanger-source/dsh-reactive-client@0.25.0

## 0.24.0

### Patch Changes

- Updated dependencies [d1bcc55]
- Updated dependencies [6db04b3]
  - @hanger-source/dsh-reactive@0.24.0
  - @hanger-source/dsh-reactive-client@0.24.0

## 0.23.0

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
  - @hanger-source/dsh-reactive-client@0.23.0

## 0.22.0

### Minor Changes

- 2cadde4: 会话上传同步返回 FileUploadInvocation，普通 Promise、独立等待和有限活动资源消费同一次调用。字节准备可以取消；开始分派后退出等待不覆盖实际回执，也不重新上传。本地会话退出等待已经开始的原生存储，远程运输失败明确保留回执未知及原始原因。原生 UI、公开消费类型和版本化开发指导共同使用这份合同。原先直接 await uploadFile 的消费者需要改为读取调用的 result 或 wait()。
- fd163f2: 将跨作用域派生的读取资格接入同一 signals 图；读取失败通过订阅错误接点传播到普通消费者和原生 UI。

  Reaction 停止立即撤销来源与排队工作，draining 和 settled 保留已开始 Promise 的实际结算与迟到失败；运行实例结算前不接受第二次 start。

  Action 和 NativeTools 的准入等待随原调用、声明代际与 Program 退出及时结束。原 execution 身份保持不变，原生最终 guard 拒绝已撤销的旧准入；已经进入业务的工作保留原 owner。
- 6f9b816: Program、本地与远程 SDK、可选原生 UI 共用 session.files、SessionFiles、FileResource 和 FileWatch；路径资源复用同一读侧与原 owner，原生范围和正式 WorkspaceFiles 类型贯穿 Gateway 字节及变化端点。

  HostOptions.files 与 RuntimeOptions.files 借入已安装的 NativeFiles，应用退出等待自身实际文件读取与监听清理。Gateway 可显式借用已有 Registry，文件端点逐次复核 Program、accessId、认证主体和交付权限；重连撤旧 ready 后等新 watch 重读。

  NativeSession.dispose 返回同一次 Promise，先同步撤观察资格，再等待本 UI 创建的 raw watch；需要完整清理结果的消费者改为 await dispose() 或 await using。借入 SDK 和全域文件能力仍由其原 owner 销毁。

  开发 Skill/reference 同步公共入口与权限、范围和清理边界。本次只记录待发变化，源码保持 0.x，不按批次升号或执行发布。

  文件资源持续更新遵循正式 FS 通知边界；固定 rc.3 本地 provider 可能抑制同路径 50ms 内的 change 且不补尾通知。已知写入完成后显式 invalidate，立即核验使用单次权威查询，不承诺自动追上每次连续写入。
- b2db37f: 共享 Program 使用发布者声明的 name/version，远程 SDK 只接受明确共享的程序定义。创建、补齐、恢复及所有会话请求与流重连共同核验真实 Program；同名且 wire 兼容的视图不能使另一程序被误认。局部无名 Program 继续用于本地组合，本地传入真实定义仍核对象身份；共享身份不替代认证、Workspace 或 Session 权限。原生 UI、真实包消费者与版本化开发指导同步使用这份合同。

### Patch Changes

- 546f161: 框架代码构建与打包资产准备分开，普通 core prepack、工作区打包和正式发行共享完整准备依赖。默认框架类型与行为检查独立于示例验证，保留现有发行资产。

  Native UI 独立构建包含 client 浏览器产物前置任务。日常 tgz 与正式发行消费同一份安装包 Skill/CLI 检查，打包校验同时拒绝缺少 manifest 字面资产的产物。
- 7a56dd0: 动态集合合同只签发轻量地址，不永久缓存历史业务 key。Module 持有当前实例的动态声明名称：同地址重复渲染稳定，切换目标或撤下重挂产生新代际；已受理调用继续由原 owner 结算，SDK 仍按完整地址投射。

  Job.wait 改为非消费状态等待，不再调用原生 registry.wait 或影响模型完成通知。显式 collect 消费原生输出 cursor，首个终态消费可取得 producer result；取消与清理不伪造结果确认。

  公开 Disposable/AsyncDisposable 声明显式携带标准库依赖，浏览器不再依赖 Node ambient 补齐。仓外 UI 安装增加 ES2024/DOM、types:[] 的严格声明消费，日常与正式发行共用已有验证链。
- 1e15519: 日常实包消费与正式发行验收使用同一 Playwright 浏览器流程，实际执行 SDK、原生 Slot 与请求进展页面。认证 Host 从独立后台实包安装启动，原生 UI 继续使用独立 React 18 安装与构建产物。成功回执在异步退出完成后生成，发行证据包含真实页面结果；仅有 bundle 构建不再满足浏览器验收。
- fb602dd: 提供 Program 的 useSharedResource、Action.activeInvocations 与 ReactionRun.state，让派生资源及运行事实通过同一对象供 Promise、Observable 与 hooks 消费，并保留真实执行的结算寿命。

  统一根、局部 Surface 与嵌套集合的 Invocation 查回。回执保留由实际 Host 调用 owner 负责；completedRetentionMs/maxCompleted 配置移至 RuntimeOptions/HostOptions，Gateway 只传输同一回执，不重执行或依赖当前 Expose。

  修复可选原生 Action 展示在同步注册、退出与重入时的贡献归属和释放失败结算。同步开发 Skill 与迁移说明。
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
  - @hanger-source/dsh-reactive-client@0.22.0

## 0.21.0

### Patch Changes

- Updated dependencies [00f0503]
- Updated dependencies [81d05bc]
- Updated dependencies [9c5d4b0]
- Updated dependencies [73e4c02]
  - @hanger-source/dsh-reactive@0.21.0
  - @hanger-source/dsh-reactive-client@0.21.0

## 4.0.0

### Major Changes

- 2fd5838: 原生人工会话绑定在运行时核验 human 身份，服务端口可以独立使用通用资源呈现。ProgramSessionClient 表达人工和服务共同的会话读取、输入、Surface 与 Operation 能力，HumanProgramSessionClient 单独表达问答、前台认领及 Goal 控制。host.service(actorId) 返回绑定固定身份的应用视图，服务可创建、打开和恢复会话、编辑自己的输入及上传文件，逐调用不能改写 actor。

  Gateway 通过必填 participant(principal) 签发角色及 actor，替换只返回字符串的 actor 配置；输入、Action、Operation 与上传使用同一受信端口，Invocation 关联同时核验角色与 actor，服务器拒绝服务执行人工控制。新增服务 SDK 工厂，与人工 SDK 复用同一套正式连接、资源与恢复机制。固定 rc.3 的 answer 传输不授予 human 权限，none 连接不伪装为具有完整恢复能力。角色错配错误保留宿主已确认的会话身份。开发指导及仓外消费者同步此合同，迁移见 docs/migration-participant-ports.md。

### Patch Changes

- Updated dependencies [2fd5838]
- Updated dependencies [6847a95]
- Updated dependencies [f8d99d1]
  - @hanger-source/dsh-reactive@4.0.0
  - @hanger-source/dsh-reactive-client@4.0.0

## 3.0.0

### Patch Changes

- Updated dependencies [1c9e7ee]
- Updated dependencies [dc676c6]
  - @hanger-source/dsh-reactive@3.0.0
  - @hanger-source/dsh-reactive-client@3.0.0

## 2.1.0

### Patch Changes

- Updated dependencies [d502e72]
  - @hanger-source/dsh-reactive@2.1.0
  - @hanger-source/dsh-reactive-client@2.1.0

## 2.0.0

### Patch Changes

- Updated dependencies [581b301]
  - @hanger-source/dsh-reactive@2.0.0
  - @hanger-source/dsh-reactive-client@2.0.0

## 1.1.0

### Patch Changes

- Updated dependencies [66a1d07]
- Updated dependencies [b4af2f5]
  - @hanger-source/dsh-reactive@1.1.0
  - @hanger-source/dsh-reactive-client@1.1.0

## 1.0.0

### Major Changes

- bdd5279: SDK、Session及其远程资源统一由正式dsh Fiber/effect持有构造与异步释放，替换各域独立的退出和退休队列。bindProgramClient现在返回Promise并等待作用域安装，直接消费者需要await；ProgramJournal通过session.openEvents取得。取消立即撤销使用权，退出等待实际清理静止并汇总失败，已受理业务继续遵循原owner。迁移方式见docs/migration-sdk-scopes.md。

### Patch Changes

- Updated dependencies [dd50bb1]
- Updated dependencies [bdd5279]
- Updated dependencies [60f19d6]
  - @hanger-source/dsh-reactive@1.0.0
  - @hanger-source/dsh-reactive-client@1.0.0

## 0.20.2

### Patch Changes

- Updated dependencies [9aca405]
  - @hanger-source/dsh-reactive@0.20.2
  - @hanger-source/dsh-reactive-client@0.20.2

## 0.20.1

### Patch Changes

- Updated dependencies [bdf0417]
  - @hanger-source/dsh-reactive-client@0.20.1
  - @hanger-source/dsh-reactive@0.20.1

## 0.20.0

### Minor Changes

- fdab096: Action 以名称与正整数版本声明跨进程合同身份，新定义省略 version 时为 1。远程解析与业务受理前拒绝不匹配合同，原 Invocation 回执、原生展示与子程序委托恢复保留原版本，不再用当前同名合同解释历史结果。

  迁移时必须同步更新宿主与客户端。自建 wire 调用须提供 contract；原生 manifest 增加 actionVersion，reactiveAction metadata 格式升至 version 2 并增加 actionVersion，委托与子会话记录也要求 actionVersion。缺少这些字段的旧记录不能直接恢复；保留相应旧版本消费者，或由业务明确核验历史合同后进行数据迁移，框架不自动猜测版本。

### Patch Changes

- Updated dependencies [fdab096]
  - @hanger-source/dsh-reactive@0.20.0
  - @hanger-source/dsh-reactive-client@0.20.0

## 0.19.2

### Patch Changes

- Updated dependencies [ebf227d]
  - @hanger-source/dsh-reactive-client@0.19.2
  - @hanger-source/dsh-reactive@0.19.2

## 0.19.1

### Patch Changes

- Updated dependencies [84f447f]
  - @hanger-source/dsh-reactive@0.19.1
  - @hanger-source/dsh-reactive-client@0.19.1

## 0.19.0

### Patch Changes

- Updated dependencies [f690a47]
  - @hanger-source/dsh-reactive@0.19.0
  - @hanger-source/dsh-reactive-client@0.19.0

## 0.18.2

### Patch Changes

- @hanger-source/dsh-reactive-client@0.18.2
  - @hanger-source/dsh-reactive@0.18.2

## 0.18.1

### Patch Changes

- Updated dependencies [48fbba0]
  - @hanger-source/dsh-reactive@0.18.1
  - @hanger-source/dsh-reactive-client@0.18.1

## 0.18.0

### Patch Changes

- @hanger-source/dsh-reactive-client@0.18.0
  - @hanger-source/dsh-reactive@0.18.0

## 0.17.2

### Patch Changes

- Updated dependencies [d458636]
  - @hanger-source/dsh-reactive-client@0.17.2
  - @hanger-source/dsh-reactive@0.17.2

## 0.17.1

### Patch Changes

- Updated dependencies [d158683]
  - @hanger-source/dsh-reactive@0.17.1
  - @hanger-source/dsh-reactive-client@0.17.1

## 0.17.0

### Patch Changes

- Updated dependencies [db574d1]
  - @hanger-source/dsh-reactive@0.17.0
  - @hanger-source/dsh-reactive-client@0.17.0

## 0.16.0

### Patch Changes

- @hanger-source/dsh-reactive-client@0.16.0
  - @hanger-source/dsh-reactive@0.16.0

## 0.15.0

### Patch Changes

- @hanger-source/dsh-reactive-client@0.15.0
  - @hanger-source/dsh-reactive@0.15.0

## 0.14.0

### Patch Changes

- 8dc0fb9: Invocation 新增同源 activity 资源，普通读取、Observable、hooks 与原生 UI 共用同一份调用事实；activity$ 直接引用 values$，业务终态后有限结束，晚读取保留终态，result 继续独立表达业务完成。本地调用事实不借用已退出组件或执行取消的寿命，远程读侧仍由 SDK 拥有；未终结活动的连接断开立即失效，重连与单次读取取消均不重做业务，已确认终态不被流清理错误撤销。ResourceReader 支持省略借用选项而直接沿用来源寿命，问答和请求工厂保留明确 owner；开发 Skill/reference 与实际包消费者同步更新。
- Updated dependencies [8dc0fb9]
  - @hanger-source/dsh-reactive@0.14.0
  - @hanger-source/dsh-reactive-client@0.14.0

## 0.13.0

### Minor Changes

- 2a3e750: 新增 retainNativeSession，以明确的应用 Context 持有原生 SessionReference、独立 Program SDK 和 UI 观察。取得时刷新官方目录并核验真实代际；失败、取消和 Context 退出释放本次成果，成功后显式 release 不影响其他引用或 Host 工作。保留同步 bindNativeSession 的借用语义，开发 Skill/reference 与仓外公共 API 消费同步更新。

  修复 WebSocket 取消、断线和远端终止时上行循环的退出竞态，阻止关闭后发送迟到结束帧，同时保留错误监听直到实际 close。浏览器试验使用官方 rc.3 模块加载器、真实 Client Sessions 与认证 Gateway 验证引用计数、失败和取消、代际重开以及关闭后零发送。

### Patch Changes

- Updated dependencies [2a3e750]
  - @hanger-source/dsh-reactive-client@0.13.0
  - @hanger-source/dsh-reactive@0.13.0

## 0.12.1

### Patch Changes

- Updated dependencies [5270fb3]
  - @hanger-source/dsh-reactive-client@0.12.1
  - @hanger-source/dsh-reactive@0.12.1

## 0.12.0

### Minor Changes

- 5081230: 新增 bindProgramClient(program, ctx)，供已安装 Connection、Gateway 和 Program 远程贡献的 dsh 客户端复用同一装配。借用 SDK 只释放自己的 Session 与观察，Context 退出会关闭其 SDK；独立 createProgramClient 继续拥有并释放自己的 Context。原生 UI 现在核验 SDK 对象的实际 Connection 归属，防止不同服务器或连接上的同 ID 会话串用。原生扩展应改用其客户端 Context 的 bindProgramClient 再 open Session；独立 SDK 不能凭相同地址或 ID 接入原生 generation。

### Patch Changes

- Updated dependencies [5081230]
  - @hanger-source/dsh-reactive-client@0.12.0
  - @hanger-source/dsh-reactive@0.12.0

## 0.11.1

### Patch Changes

- Updated dependencies [9fb4b1a]
- Updated dependencies [be26221]
  - @hanger-source/dsh-reactive@0.11.1
  - @hanger-source/dsh-reactive-client@0.11.1

## 0.11.0

### Minor Changes

- 19c9a36: Program、本地与远程 SDK 提供 progress 资源及 Request.state，统一当前读取、Observable 与 hooks 的消费，沿用原请求关联、原生活跃前缀和会话连接。原生 UI 按需借入相同读侧，绑定本身不额外启动进展连接；单次读取取消和 UI 释放不停止原生工作。createRequestHandle 的 progress 参数改为资源对象并显式接收读侧 owner；框架入口自动完成这些装配。

### Patch Changes

- Updated dependencies [19c9a36]
  - @hanger-source/dsh-reactive@0.11.0
  - @hanger-source/dsh-reactive-client@0.11.0

## 0.10.0

### Minor Changes

- 8060a41: Program、本地和远程 SDK 提供同源 human 资源；question(callId) 返回固定原问题的可复用对象，将状态观察、回答和前台等待放在同一对象上。问题状态保留 active、foreground、settled 原事实，不建立新的阶段、计时器或批准权。可选原生 UI 完整保留 pending/error，问题控制沿原 UI generation 与 SDK 准入。服务端口的人工回答和 Goal 控制在运行时明确拒绝，绕过类型声明也不能借用人工身份。

### Patch Changes

- Updated dependencies [8060a41]
  - @hanger-source/dsh-reactive@0.10.0
  - @hanger-source/dsh-reactive-client@0.10.0

## 0.9.1

### Patch Changes

- f6050e9: 为 dsh 直接使用的 LLM/附件合同和原生 UI 的公开 Session 类型补齐正式依赖，避免借用工作区或传递安装。依赖边界检查同时核验声明、运行时 devDependency 与跨包相对导入。日常 CI 和发行验收共用仓外编译配置，并从实际安装包按 browser 条件完整打包纯合同、SDK 与可选 UI，检查外部导入、后台 renderer 和 core 模块身份。
- @hanger-source/dsh-reactive-client@0.9.1
  - @hanger-source/dsh-reactive@0.9.1

## 0.9.0

### Patch Changes

- 7a31aa8: shareResource 将已有或组合后的 Observation 流交付为共享只读资源，普通函数、Promise、RxJS 与 hooks 复用同一快照和读取寿命。OperationHandle.state 复用该入口，当前状态读取与 settled 终态等待保持独立，本地和远程操作及可选原生 UI 不再另存观察状态。操作来源结束仍没有业务终态时明确进入查询错误，不永久等待；已有最终观察保留至最后消费者退出，之后显式读取重新查询原操作，不再次执行 Action 或登记 follow。
- Updated dependencies [7a31aa8]
  - @hanger-source/dsh-reactive@0.9.0
  - @hanger-source/dsh-reactive-client@0.9.0

## 0.8.0

### Patch Changes

- @hanger-source/dsh-reactive-client@0.8.0
  - @hanger-source/dsh-reactive@0.8.0

## 0.7.0

### Patch Changes

- 424b75d: 本地、远程及动态集合提供稳定的 viewResource 读侧对象，普通函数、Promise、Observable、程序 hooks 与可选原生 UI 共用现有 View 状态和 SDK 读取寿命。ResourceReader 统一已有快照的读取、取消与终止结算，不建立新缓存或来源连接。自定义 SurfaceCollectionTransport 以 viewResource 返回已有读侧，取代分开的 observeView/readView 传输方法。
- Updated dependencies [424b75d]
  - @hanger-source/dsh-reactive@0.7.0
  - @hanger-source/dsh-reactive-client@0.7.0

## 0.6.0

### Patch Changes

- @hanger-source/dsh-reactive-client@0.6.0
  - @hanger-source/dsh-reactive@0.6.0

## 0.5.0

### Patch Changes

- @hanger-source/dsh-reactive-client@0.5.0
  - @hanger-source/dsh-reactive@0.5.0

## 0.4.0

### Patch Changes

- Updated dependencies [923671d]
  - @hanger-source/dsh-reactive@0.4.0
  - @hanger-source/dsh-reactive-client@0.4.0

## 0.3.0

### Patch Changes

- Updated dependencies [1448bec]
- Updated dependencies [f713b74]
- Updated dependencies [e4273f8]
  - @hanger-source/dsh-reactive-client@0.3.0
  - @hanger-source/dsh-reactive@0.3.0

## 0.2.2

### Patch Changes

- Updated dependencies [5f53764]
  - @hanger-source/dsh-reactive@0.2.2
  - @hanger-source/dsh-reactive-client@0.2.2

## 0.2.1

### Patch Changes

- ed8c8dc: 明确六包 TypeScript 构建依赖与客户端合同的唯一产物来源，严格编译并执行仓外消费者；以 GitHub OIDC 发布可追溯的同步版本及开发 Skill，并从 npm registry 安装验收。
- Updated dependencies [ed8c8dc]
- Updated dependencies [871403f]
  - @hanger-source/dsh-reactive@0.2.1
  - @hanger-source/dsh-reactive-client@0.2.1

## 0.2.0

首次公开源码。

- 提供可选的原生 dsh UI 绑定：类型化 Action 视图、资源观察、会话来源和正式 slots 组合。
- 依赖独立客户端连接，不在界面层复制业务权限或执行 owner。
