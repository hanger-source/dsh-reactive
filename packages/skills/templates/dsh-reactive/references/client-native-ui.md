# 客户端、输入与可选原生 UI

适用版本：`{{PACKAGE_VERSION}}`。

## 用同一纯 Program 消费

独立模块从 core 导入 `ProgramSessionClient<P>`，直接借用本地 `ConversationHandle<P>` 或远程 `RemoteProgramSession<P>`，共用原来的 View、progress、human 和 request 资源。只消费模块出口时使用 `SurfaceClient<S>`；根会话和集合成员使用同一合同。普通业务函数可以 `await` 两端的 Action 解析和调用，具体 SDK 类型仍保留各自同步/异步时序。不为复用创建 SDK 代理或第二套状态，不把组件内部的 `ProgramSession` 当成外部消费者权限。

本地或远程的人工、服务会话可直接作为 `WorkSource` 交给 `createWorkSelector` 的 `observe(key, sources, options?)`；`select(key, snapshots)` 同步投影已有快照。工作读侧的 `state` 与 `state$` 共用一个 reader，events$ 和 progressFrames$ 只交付新事实。所有来源 ready 才交付完整快照；pending/error 撤下旧 ready，来源退出保留原错误，各会话 generation 不组成事务。最后观察退出只释放观察。identity 的 parentSessionId 来自真实 Session；观察不会打开会话、启动 Agent 或补充权限。

WorkHandle.sessions 为每个显式来源提供 WorkSessionHandle，含 identity、state、state$、events$ 和 progressFrames$；通过 `sessions[i].state.read()` 读取局部 ResourceReader，整体资源要求全部来源 ready；一个来源退出时其他局部资源可继续，不兜底或授予权限。WorkSessionSnapshot.operationFollows 包含 resultRequest，条件包含 operation.not-started、operation.follow-unregistered、operation.delivery-prepared、continuation.unconsumed、goal.round-limit。

输入目录、调用、回复及 Goal 的输入关联来自 Activity；请求的工具和最后回复由这些事实派生。每次调用保留正式 start 事件序号 callSequence，provider callId 可跨轮复用；回复保留 messageId。声明含 `result: { continuation, wait, ready } | null`，无 messageId 不等于已受理输入；回复 inputs 为 null 时不归组。每个匹配项保完整 value、原身份和 inputs，matchedInputs 只解释观察分组，不裁剪事实或授予续接权限；多输入调用或回复可同时出现在多个工作。原始 scope 在两端均不执行 Program transform。

`session.module(mount)` 供业务消费者使用局部槽名；返回 SurfaceClient 的合同是 mount.selected，mount.surface 保留组件实现的原合同。资源对象、调用选项和同步/异步时序继续来自原客户端，不增加连接、owner 或 release。根会话、集合成员及局部模块均支持 module；继续选择嵌套模块时，须属于父原合同且保持父已公开的子集，不能用更宽的挂载恢复隐藏槽。

`session.requestStatus(requestId, { signal })` 是共同 ProgramSessionClient 方法；`pause({ signal })`、`cancel({ signal })` 和问答控制属于 HumanProgramSessionClient。ProgramRequestStatus、ProgramGoalControl、ProgramCancelResult 从 core 导入。暂停回执提供原 Goal 的 id/revision/phase；canceled:true 表示原生处理已 idle 并完成持久 flush，不代表外部业务 Operation 已取消。requestStatus 核验原生日志、投影、成员及代际，不把客户端缺项或读取失败当 absent；冷查询不激活 Agent。服务参与者可以读取问题与请求状态，不能控制 Goal 或答复人工问题。

远程服务用 createServiceProgramClient/bindServiceProgramClient，返回的 RemoteServiceProgramSession 可直接交给共同模块，运行对象本身不包含人工方法。Gateway 的 participant(principal) 明确签发角色和 actor，wire 不能自报。固定 rc.3 的完整流与 journal 恢复仍要求正式 answer 传输连接；它与业务 human 权限分离，不表示后台程序需要人工界面。人工原生 UI 只接受 RemoteProgramSession。

调用级 signal、release 或所属 SDK dispose 拒绝尚未返回的回执等待，公共 Context 与其他 SDK 保持原寿命。已取消的调用不启动控制，Host 在取得句柄后、进入实际读取或控制前重新检查请求取消。已受理控制继续结算，Host 等待真正完成后才释放自己的短命句柄；取消等待不回滚已受理业务，也不因回执未知自动重发控制。openEvents 继续使用远程 SDK 的官方 carrier、分页与重连合同，不要求本地接口伪造传输状态。

submit、editQueuedInput 分别结束本调用者的回执等待。同一 requestId 的提交共享原事务，后加入者的取消不删除共享受理；首发 signal 仍约束入队或正文替换前的执行检查，已经 send/replace 的输入由原生 owner 完成持久回执。Program 失败后不能沿旧 conversation.submit 开始新受理。本地已知准入错误保留原错误；远程 submit/edit 已发出而回执未知时保留各自的 AdmissionUnknown 错误，通过同 requestId 查询核对。释放 Session 只结束本消费者等待，已启动的 submit/edit HTTP 请求仍沿所属 SDK 与原 caller signal 结算。

uploadFile 在本地、远程与原生 UI 都同步返回 FileUploadInvocation，不提供 thenable。result 保留真实上传回执，wait({ signal }) 只等待同一结果；方法 options.signal 同时约束准备、分派前检查及默认 wait。Uint8Array 在调用时复制；Blob 读取被取消后，迟到字节不再激活会话或开始 HTTP。Session 或 UI 退出可阻止尚未分派的准备；分派后这些取消只结束 wait，不改写 result。所属 SDK 或 Connection 物理退出可能使远程 result 拒绝为客户端公开的 FileUploadReceiptUnknownError，不证明服务器没有文件；调用 id 不能找回 receipt，框架不自动重传。

远程 SDK 从 `@hanger-source/dsh-reactive-client` 导入，不是 Gateway 包的 `/client`；`@hanger-source/dsh-reactive-remote` 只装在认证 Gateway 宿主。纯 Program 文件不能导后台组件/宿主，从而让 CLI、浏览器、UI 共用定义。

共享传输合同从客户端的 `/types`、`/errors` 和 `/inbox` 入口取得；连接行为从 `/transport`、生成的远程调用入口从 `/remote` 取得。根入口与这些子入口共用同一份错误类和 Inbox。浏览器条件下各入口共享构建模块，可以在一个应用中共同导入，不能将每个入口分别内联成独立 SDK 副本。

Invocation 的有界保留配置 completedRetentionMs/maxCompleted 属于 RuntimeOptions/HostOptions，Gateway 不持另一份回执。局部 Module 和嵌套 Collection 的 invocation(name,id) 由原 Surface 合同投射精确地址；换 SDK 后仍核原参与者、Program 与 Action，读取不激活 Session、不重新执行。

createProgramClient、bindProgramClient、createServiceProgramClient、bindServiceProgramClient 与 retainNativeSession 要求真实 SharedProgramContract：在纯合同文件使用 `defineProgram({ name: 'inventory', version: 1, input, surface })`，与宿主共享同一发布者合同。name/version 随 Program 请求发送；Gateway 核目标环境或会话，create、completeCreation 和 resume 在实际副作用前拒绝错合同，SDK 核建立句柄的返回身份。工作区登记与 describe 不绑定 Program。

合同不匹配使用 ProgramContractMismatchError 或宿主 reactive/program-contract-mismatch，并保留 expected/actual；无名目标不能远程消费，使用 SharedProgramRequiredError 或 reactive/shared-program-required。version 默认 1，独立于 npm 版本，不推断 schema/codec 等价、不授予权限。纯本地 Program 可继续无名；已有 ProgramSessionClient<P>、RemoteProgramSession<P> 与 NativeSession<P> 的通用消费者仍保留 P extends ProgramContract，无需自己创建 SDK。

```ts
import { createProgramClient } from '@hanger-source/dsh-reactive-client';
import type { CancellationOptions } from '@hanger-source/dsh-reactive';

// InventoryProgram 是纯合同文件中 defineProgram 签发的具名共享定义。
// 认证 transport 由应用提供。
const client = await createProgramClient(InventoryProgram, authenticatedTransport);
const session = await client.open(sessionId);
const options: CancellationOptions = { signal };
const facts = await session.readView('facts', options);
const save = await session.resolveAction('save', { sku }, options);
const invocation = await save({ note: '交付前确认包装' }, options);
const receipt = await invocation.wait({ signal: receiptWaitSignal });
```

解析得到的 `resolvedAction.eligibility`（包括集合成员）是绑定共同资格的同源只读 ResourceReader；它不授予执行权。远程资格随既有 resolveAction 流更新，不另建订阅或执行缓存。

local resolveAction 同步，remote 异步；统一代码 `await` 两者。预期 scope 是调用者持有的**原始**目标，用同 schema 与当前绑定核验；不要把 scope 重复进业务 input，不按当前页面重定向。捕获后保 bindingId，撤下/替换/重挂硬拒旧句柄。远程调用在 JSON 校验及预取消检查后生成 UUID invocationId，只分派一次。接收 `ActionAdmissionUnknownError` 时，使用原会话的 `session.invocation(actionName, error.invocationId)` 只读查回；错误还保留 sessionId、name 和 cause。请求仍在路上、记录未知或过期、Host 重启时查不到，不证明未执行；不要寻找最近调用或直接重发。长期业务 Operation 由业务 owner 定位。

Session 显式 attach 或绑定的 Operation 观察随该 Session 退出；`await invocation.operation` 取得的操作句柄沿所属客户端寿命，原 Session release 不撤销已经受理调用的独立观察。最后消费者退出会取消查询，release/dispose 还会等待各自本地原生流及回调完成异步清理，包括已退订但仍在清理中的流；SDK dispose 也等待原 Invocation 活动流退出。原生协议不提供逐流远端关闭 ACK，不把该等待解释为服务端已确认退订，更不把查询退出当作取消业务任务。事件连接重置时，Operation、View、人工资源和进展先失效，再通过原生 restart 重新取得 baseline；查询复用原生 baseline/delta 管线，core 仍持有唯一 Operation 状态。

远程 SDK 自动从共享 Action 定义发送名称与版本，无需调用者重复装配。宿主在解析目标及受理业务前拒绝不匹配合同；`session.invocation(name, id)` 也先核对原回执身份，再连接活动和解码结果。确定的宿主拒绝使用 `reactive/action-contract-mismatch`，包含 expected/actual；如果收到自相矛盾的受理 ACK，返回 ActionAdmissionUnknownError，不能声称业务未执行。

原生 Action 视图按名称与版本注册，同名不同版本可并存；同一真实工具名仍不得指向两个不同合同。manifest 的 `actionVersion` 与持久 `reactiveAction.actionVersion` 表达业务合同版本，`reactiveAction.version = 2` 表达 metadata 格式版本。缺少合同版本的旧网络数据或持久记录不会被猜成版本 1。

创建尚未完成时，create、completeCreation、open、resume 都可能返回 `CreationIncompleteError`，reference 保留原 environmentId、workspaceId、sessionId。原生已有成员或内存中已有完成事件仍不能证明创建已 ACK。沿原引用显式 completeCreation，不把 open/resume 写成隐式创建恢复，也不换一个会话重试。

client.dispose 或借入的 Context 退出时，在途的 createWorkspace、describe、create、completeCreation、open、resume 回执等待都会结算。SDK 向 transport 传递取消信号，也独立结束自己的等待；这不撤销 Host 已受理的工作。create 已发出而尚未确认时返回 CreationUnknownError，保留原创建引用，之后用有效 SDK 显式 completeCreation。已经退出的 SDK 拒绝新调用；借用 SDK 退出不关闭公共 Context 或其他 SDK。

Session 的身份回执另含 accessId，由 SDK 自动用于后续会话读取、控制和 Operation 查询。成员移除或工作区删除交付 ConversationAccessRevokedError 并关闭这次 Session owner，普通业务 View.error 不关闭它。即使没有订阅，旧 accessId 也不能跨过一次移除再加入；显式重新 open/resume 取得新 Session，旧对象不复活。原 Invocation result/activity 沿外层 SDK owner 继续；派生 Operation 以原 Invocation 地址核验并观察实际业务句柄，不用任意 reference 重新取得会话资格。原生 UI 直接消费同一资源失效，不维护另一份成员状态。自定义 wire 消费者使用 ProgramSessionAddress，不将 accessId 当作认证权限。

动态集合：`session.collection('items').instances` 是实际成员 key 的同源资源，可 read、peek、组合 observations$ 或交给 useResource，instances$ 是其值流。嵌套集合沿用相同入口，父模块撤下交付 error，同地址恢复后继续更新。`collection.instance(key).readView('facts', options)`、`.resolveAction('save', rawScope, options)`、`.operations` 消费同一局部合同。不要维护第二张 key→工具名表或解析命名 namespace。

`session.actionManifest` 同样是资源对象，描述实际声明的 Action 版本、绑定、模块路径、公开槽与 enabled，actionManifest$ 是其值流。可将它与集合成员、View、progress 和业务资源直接组合；不另造目录缓存，也不把目录中的 enabled 当作执行授权。取得对象不打开远程查询，观察沿原 View endpoint 权限和 Session owner；release 撤销所有借出资源，不取消已受理业务。

`session.viewResource('facts')` 与动态实例的 `viewResource('facts')` 返回稳定只读资源。直接使用 `resource.read(signal)`、`peek()`、`subscribe`、`values$`、`observations$`，或将同一对象交给 `useResource(resource)`、`nativeSession.resource(resource)`；不要再复制 BehaviorSubject 或缓存。SDK release 结束该对象的读取与观察，取消某一次 read 不影响其他消费者或原 Program。动态资源固定逻辑地址，模块撤下报告 error，同地址重挂继续更新；这不改变 Action 旧绑定必须拒绝的规则。

远程 View 与 human 读侧按消费者连接：open、取得资源或值流以及 peek 都不查询 Gateway，首个 read/subscribe 才打开原生快照流。多个消费者共享一条连接，最后消费者退出撤下快照并取消查询；之后显式读取重新取得 baseline，不回放无人持有的旧 ready。release 等待所有已退休查询释放，不停止 Host 的程序或业务工作。人工、View 和控制 endpoint 各自授权，不必为了打开会话而授予未消费的观察权限；持有 Invocation 及其 result 的业务寿命仍独立。

readView 返回当前有效业务值，observeView 保完整 Observation；这些入口共用 viewResource 的状态，views 是值流，不能用它声称失效仍有效。progress 和 request(id).state 是同源资源，可 read、组合 observations$ 或交给 useResource；progress$/state$ 直接引用相应资源的观察，events$ 只交付已确认关联的新事实。一个模型步骤可消费整组输入，不能承诺每请求独占一轮答案，也不能把 read 当前快照当作等待业务或 Goal 完成。

Program 的 conversation.progress 与 conversation.request(id).state、SDK 和原生 UI 的对应入口都借入原进展 owner。首次显式取得 SDK 进展入口才建立远程连接；UI 绑定本身不要求 progress grant。原生 UI 的 native.progress 与 native.request(id).state 保留 pending/error，native.request(id).events$ 保留原事件与当前 UI 的观察寿命，不另存模型输出或按消息正文猜请求。

进展流的物理 carrier 中断沿官方恢复继续；真正的终止错误结束该句柄的 progress、request 和 events 观察。Connection reset 不会让已经结束的消费者重新变为 pending，也不清除原 error；需要新观察时显式 `client.open(sessionId)` 取得新 Session。资源 baseline 中的业务 error 仍由原来源决定后续事实。

## 观察人工答复交付

人工观察的运行合同是 `HumanSnapshot`，远程 `ProgramQuestions` 只用于 wire。`session.human` 与固定问题 `question.state` 均提供 replies；每项带真实 messageId、questionCallId、原 origin、原 target 和 queued/proposed 状态。queued 是仍在原生 inbox，proposed 是已提出且尚未进入输入历史；与 active/foreground/settled 并列，不另建问答状态。Work 的 questions.replies 及 question.reply-pending 保留同一事实和完整原输入。question.pending 只表示原问题未结算，不能直接显示成“尚未回答”。

答复取消、重新入队或提议窗口结束都可能让 replies 消失；不能据空集合声称从未回答或模型已消费。冷读的 proposed 只是最后持久事实，不证明当前有 Agent 在执行。前台回答沿原生工具返回结算；晚答进入 user/message 才推进原生问题结算，仍不证明模型已处理或业务成功。观察不签发原 Goal 的续接权限。

## 读取当前模型准备

`session.progress.read(signal)` 的 preparation 是 `ModelPreparation | null`，保留当前准备 id、已知原始 inputs 和实际仍在等待的 waiting。Context、Skill 名称与目录使用已有身份，check.id 只是不透明的登记身份。委托、输入准备、步骤检查、Skill 服务/目录/正文、工作目录、Context 和原生装配各自报告自己的等待，不维护另一份综合任务状态。

`session.request(id).state` 只展示 inputs 确切命中的 preparation；WorkSessionSnapshot.preparation 保完整事实和 matchedInputs，实际有等待时给出 preparation.waiting 条件。多输入可同时属于多个工作，观察不签发权限，也不证明输入已被模型消费。结束、失败、取消及 Agent 退出撤下当前准备；冷读为 null，不从持久失败或旧模型步骤复活等待。null 不代表业务或模型已完成。

## 读取执行失败

`session.progress.read(signal)` 的 activity.failures、`session.request(id).state.read(signal)` 的 activity?.failures，以及 WorkSessionSnapshot.failures 共用 ExecutionFailure。该事实来自原生 turn/end:error，带原生 LlmFailure、turn/sequence/observedAt、原 inputs 和尚未入历史的 unentered 消息。Work 条件 execution.failed 保同一来源；不能把它解释为整个业务、Goal 或 Operation 的最终失败。自定义 Activity/Request 来源必须提供 failures，不能用读侧 error 代替有效的失败事实。

未入历史的晚答会沿原 ID、原内容保留回正式 inbox，当前有交付时仍读 human.replies；保留不自动重试。实际 Agent 激活会核验进程中断留下的精确失败关联并等待恢复，纯 open/观察不激活或改队列。已消费/取消/结算不复活，同题竞争明确冲突。显式新输入仍走 native Program/member/Goal 准入，不重 answer、不找最近 Goal。持久性沿原生 checkpoint/flush，不增加另一层日志或队列。

## 读取原生日志

用同一个 `session.journal` 消费本地或远程的人工、服务会话。`page({ throughSeq, beforeSeq?, maxMessages?, turnWindow? }, { signal })` 使用 Controller 的原生消息分页；`follow(window, { signal })` 先交 `opened` 的 cursor/page，再交水位后的原生 entry。它保留持久原始日志，不把瞬态 `events$` 或 assistant stream 拼成历史。

需要 Observable 时使用 `defer(() => from(session.journal.follow(window, { signal })))`，沿 RxJS 组合和 `useObservable` 读取；每次 follow 调用是独立连接。连接退出只撤本次读取，Session release 撤销句柄资格，不取消真实业务。原生服务由 Host 确切 Context 已安装的 Controller/Query 提供，缺服务抛 `SessionJournalUnavailableError`；不为了历史读取隐装 Controller、提升 Agent 或复制分页算法。

远程 `openEvents(options, request, { signal })` 在同一 journal 上使用官方 RemoteJournalStream 管理重连、prepend 和缺口修补。第三参数只取消这次 opening，取得连接后不延长该信号的寿命，也不关闭兄弟资源；退出仍归原 Session/SDK。不要再在业务层维护游标修补状态或直接调用原生 Controller.follow。

## 读取会话图片附件

Program、本地与远程 Session 以及可选原生 UI 共用 `session.attachments.readImage(attachmentId, { signal })`，返回 `{ attachment: ImageAttachmentRef, data: Uint8Array }`。从已登记图片引用取 attachmentId；正式 Controller 仍核验原会话引用，不能用别的会话引用或全局内容哈希取得权限。字节按需读取、每次独占，不放进共享 Resource。远程使用 `reactivePrograms/readImage` 的独立 Session grant 与正式 binary codec。

Host 借用已安装的 sessionController，缺失报 SessionAttachmentsUnavailableError。冷读取不激活 Agent，子会话范围由原生 sessionId 入口裁决。取消只结束当前等待，原生读取无法取消，原生 reader/Host 退出等待其真实结算。UI 只借原 SDK，不创建附件存储或关闭 SDK。rc.3 未公开历史 FileAttachmentRef 的会话授权读取；不要自行扫日志、用上传 receipt 或全局 readFileStream 拼装下载授权。

## 输入保 intent 与 delivery 独立

```ts
await session.submit({ requestId: stableRequestId, intent: 'question',
  scope: { sku }, content: '请确认当前库存', delivery: 'followup' }, options);
```

普通 Submission 必填 content 和 delivery；旧 instruction 已退出。content 是 string 或有序 PromptContentPart[]，string 保原文，仅归一 text。

- followup：原生 next-turn，唤醒；不保证每条输入独占模型 turn。
- steer：原生 nearest-step，唤醒。
- inject：原生 nearest-step，不唤醒；明确写入待消费现场。

intent 描述 question/goal/followup，不能代替 delivery；goal 必须有实际非空文字作为目标。result 分支禁止 delivery，只由可信关联核验沿原 Goal 续接，不给调用者自报 Goal 权限。

附件先取得同步调用，再等真实回执：

```ts
const call = session.uploadFile(bytesOrBlob, { name, signal });
const upload = await call.result;
// 需要随本消费者退出等待时，使用 await call.wait({ signal })。
await session.submit({ requestId: stableRequestId, intent: 'followup', delivery: 'inject',
  scope: { sku }, content: [{ type: 'file', receiptId: upload.receiptId }] });
```

receipt 绑定 exact 会话实例，未提交而关闭会失效，不接任意持久 file ref。首次提升后的正文持久保存；同 requestId、同请求内容可复用 prepared 事实，重试不能先重新消费已退役 receipt。正文、scope、来源或 delivery 不同不能重用原幂等身份。

`editQueuedInput(requestId, content, options)` 只编辑同操作者尚未消费的原请求，保初次目标/来源/顺序，不唤醒模型。`InputEditAdmissionUnknownError` 不触发自动重投。

共享取消对象统一 `{ signal }`，不传裸 signal。remote 不允许 actorId，认证端口决定真实身份；local SubmitOptions 的 actor 仅供可信宿主。取消某次读/解析/受理等待，不意味着取消已经执行的业务。OperationHandle.settled 和各次观察有各自读取寿命；明确业务 cancel 沿 owner。

## 可选原生 UI 借入已有对象

安装 `@hanger-source/dsh-reactive-native-ui` 只用于 dsh 原生客户端扩展，不加载后台 renderer。原生 SlotCore、UiSession、UiConversation 仍拥有 slots、节点与会话状态，应用提供视觉组件。

`bindNativeSession(Program, exactSessionBinding, remoteSession)` 同时核真实 Program 对象、SDK definition、实际 Connection 与 exact native 代际。原生扩展在其客户端 Context 使用 `await bindProgramClient(Program, ctx)`，等待 SDK 安装后再通过 open 取得 Session。可以在异步扩展装配函数内直接等待，SDK 的依赖由其自身作用域声明。Context 的装配 owner 负责登记一次 Program 远程贡献：

```ts
import { bindProgramClient } from '@hanger-source/dsh-reactive-client';
import programRemotes from '@hanger-source/dsh-reactive-client/remote';

// 原生客户端装配阶段，ctx 已安装 Connection 与 Gateway。
await ctx.remote.$mount(programRemotes);
const client = await bindProgramClient(InventoryProgram, ctx);
const sdk = await client.open(exactSessionBinding.sessionId);
const native = bindNativeSession(InventoryProgram, exactSessionBinding, sdk);
```

本地与远程 Session 的 `files` 具有同一纯 SessionFiles 合同；text/metadata/directory 可直接参与资源组合，readText/readBytes/stat/list 是显式权威读取。原生 UI 使用 `native.resource(native.files.text(path))` 将同一资源交给 HostObservable，不复制文件快照。`native.files.watch` 的退出归当前 UI，`await native.dispose()` 先同步撤资格，再等待本 UI 创建的 raw watch 实际关闭；它不销毁借入 SDK，也不取消写文件业务。NativeSession 支持 await using；同步 Symbol.dispose 只能发起退出，不能代表远端已经 unwatch。

SDK 与 Session 句柄各有独立原生 Fiber。构造物理查询前登记 effect，已经开始的清理继续归同一 owner；退出要等待全部实际清理，再汇总失败。`ProgramJournal` 通过 `session.openEvents` 取得，不直接构造；手动 dispose 也不会脱离 Session 的退出等待。core Scope 仍管理同步响应图。

`createAuthenticatedTransport(options)` 拥有自己建立的 socket 和 uplink。自建 Context 时，把 `transport.dispose()` 登记到该 Context 的 effect，再将同一 transport 交给 `installConnection`；Context 装配 owner 可显式等待同一个 dispose 回执取得物理清理错误。独立 createProgramClient/createServiceProgramClient 已登记并等待此回执。bindProgramClient/bindServiceProgramClient 借用已有 Connection，SDK 退出等待自己子流的实际清理；固定 rc.3 会吸收底层 iterator.return 的拒绝，外部 transport 的失败须由原装配 owner 等待 transport.dispose 取得，不能由借用 SDK 保证交付。

transport.dispose 先停止物理流并要求 uplink.return，与已有 pending next 并列等待真正结算；不合作的来源必须自行完成取消。退出后 fetch/openStream 都拒绝新调用。已分派 HTTP 仍由原请求 signal 与响应消费者拥有，transport.dispose 不另行取消、等待或改写它的回执。

Action 受理 ACK 已确认后，即使 SDK 在构造观察时退出，仍保留原 Invocation id。观察及 result 等待按取消结算，可以在另一有效 SDK 用 `session.invocation(name, id)` 恢复原结果；不要重发 Action，也不要把观察退出解释为取消业务或 Operation。

该 Context 必须允许 `interactions:'answer'`。多个 SDK 可以借用同一装配；SDK dispose 只退出自己的会话和观察，Context 退出关闭全部借用 SDK。独立 createProgramClient 拥有另一份 Connection，适用于独立客户端；原生 UI 按实际连接归属绑定，不能用相同 URL、sessionId 或复制出来的同形句柄代替。

需要整体取得并拥有引用时，使用 `const reference = await retainNativeSession(Program, clientContext, sessionId, { signal })`；该入口自行创建 SDK，Program 必须是 SharedProgramContract，并在取得引用前核验。消费 `reference.session`，退出时 `await reference.release()`，也支持 await using。Context 由客户端根或应用插件拥有，须已有官方 Client Sessions 与上述 Program 远程贡献；不要用目标 Agent generation 的 Context 保留其自身寿命。框架通过官方目录刷新、精确代际 retain 和 SDK open 完成装配，权限仍由原生目录及 Program 读取入口分别核验。取得失败或取消释放部分成果，成功后的 signal 不释放已返回引用；显式 release 或所属 Context 退出释放本次 UI、SDK 和原生引用，不影响其他引用或 Host 工作。已有 binding 和 SDK 时用 bindNativeSession，原对象仍由调用者释放。

- `nativeSource` 直接接已有 peek/subscribe；NativeReadable 的可选第二个 subscribe 参数接收读取错误。值失效也通知原生消费者重新读取 getSnapshot，不能继续显示旧缓存。`observeNativeResource` 保 pending/error；NativeSession.resource/operation/invocation/view 将同一个对象交给原生观察。
- Invocation 的 activity 资源可直接 read、peek 或交给业务 hook；activity$ 是其 values$。native.invocation(invocation) 保留调用的 source 类型，直接读取同一个 Observation，不在 UI 重新物化活动。Action 的远程资源在 returned/failed 后继续保留 SDK 的有效性观察，result 独立等待业务完成。按 state 等待终态，不用 lastValueFrom 推断业务完成。业务 failed 是 ready.value 的调用事实，SDK 失效或观察错误不会伪装成业务失败。
- FileUploadInvocation 的 source 是 `{ kind: 'file-upload', sessionId }`，id 只关联本次内存调用，不授予 Action、Goal、幂等或持久恢复身份。上传活动是有限事实，running 只表示已分派，不证明存储受理；Session 或 UI 退出不改写真实终态，晚读仍有效。原生 UI 的 uploadFile 返回 SDK 原调用对象，可直接传给 native.invocation(call)、useResource(call.activity) 或组合 observations$。
- 未终结的远程 Action 活动连接断开时，资源立即 pending，重连读取原调用的当前事实；取消单次 activity.read 只退出本次等待，不重发 Action 或关闭共享观察。已确认终态不被流清理错误撤销，清理失败保留日志。
- `nativeHooks`、`nativeSessionInject` 复用原生 InjectFace；注入的 `program` 不覆盖原生 props.session。名为 current 的来源成为 `useCurrent(selector, equality?)`，selector 必填；需要完整 Observation 时使用 `useCurrent(value => value)`，并显式处理 pending/error/ready。`provideNativeSources` 交原 UiSession.provide 持有注册/更新/退出，不再建 registry。
- `registerNativeConversation` 组合原节点/视图/分组/渲染注册，部分失败回滚整组；所有解析与 replay 留给原 UiConversation。
- `createNativeActionViews` 与 `readActionPresentation` 读取真实调用 metadata，不用当前 Action scope 解释历史结果，也不反向 encode 单向 transform。

NativeSession.dispose 退出 UI 观察/贡献，不 release 借入 SDK/SessionReference，不取消已受理工作。代际变化或 SDK.release 后旧参与端口硬拒。人工回答走绑定的纯 SDK 与原 question.origin，不接 Controller 当前页的默认路径冒充准入。

`nativeSession.human` 保留 SDK 的完整人工快照；questions 和 foregroundQuestions 同样传递 pending/error，不在断线时继续展示旧 ready。`await nativeSession.question(callId, options)` 返回固定问题的原生 UI 绑定，state 是原生 HostObservable，answer 与 attachWait 继续核验这个 UI generation，并委托原 SDK；绑定不创建新问题、不自动 claim 或代答。

dsh rc.3 SessionController 缺 Program 创建/prompt/队列执行前接点，不能声称其默认输入框已贯通框架准入；这不妨碍 typed slots、节点、资源与明确 SDK 参与组合。后台 React 19 renderer 与原生 React 18 UI 需独立编译、独立部署。
