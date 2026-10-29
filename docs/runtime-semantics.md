# 运行语义

## 程序、组件与提交

纯本地程序合同由 `defineProgram({ input, surface })` 定义；远程共享定义增加 name 与可选 version，version 默认 1。宿主使用 `implementProgram(program, { Component, modules?, operations? })` 提供组件及静态依赖，并把实现放入工作环境的 `program` 字段。会话 owner 创建后台 React 根，组件通过 props 取得对话事实、显式输入入口、工作区接点和已声明的直接模块。程序不依赖页面挂载，也不产生 DOM。

四种远程 SDK 工厂与 retainNativeSession 只接受真实的 SharedProgramContract。Program 请求携带预期 name/version，宿主核实际目标，SDK 核返回身份；create、completeCreation 和 resume 在原生副作用前拒绝错合同。本地传入 Program 定义继续核 exact 对象，通用 ProgramSessionClient、RemoteProgramSession 与 NativeSession 的类型仍接受 ProgramContract。合同版本独立于 npm 版本，不证明两份 schema 或 codec 等价。

组件类型、位置与 key 遵循 React 身份规则。保持身份时 hooks 可以保留目标绑定与来源对象；替换 key 或卸载子树会清理该部分的查询、关系和声明。目标改变时，应将目标依赖明确传给相关 hook，不能只更换闭包中的业务对象而省略依赖。

组件求值只构造来源和声明，不订阅查询、注册工具或调用业务。React commit 后，renderer 收集完整声明集并校验品牌，collector 合并为一帧。普通文本、任意 host 节点及手工伪造的声明不能作为模型现场进入该帧。渲染、effect 或 collector 失败进入程序错误，不能用空声明继续执行。

每帧包含完整的 Context、Offer、Skill、HumanInput 与 Surface 贡献。同名 Context 或 Offer、重复 Skill 来源及重复 Surface 槽均失败。子组件可以向同一 Surface 提供不同槽；调用方必须显式选择名称与出口，React key 不会自动生成另一套命名空间。

复合程序用 composeSurface 组合各模块的局部 Surface，以其挂载点建立 Module 边界；动态对象集合用 collectionSurface 与 mount.instance(key)。模块接收业务目标，使用局部 Context、Offer、Follow 和 Expose，框架映射原生名称与完整公开地址。模块无需接收应用提供的名称或 Outlet，也不导入根 Surface；本地及远程消费者沿同一模块地址读取、调用和查回 Invocation。

会话程序按名称及实际来源身份增量更新内部接点，并在 batch 后发布 revision。原生准备使用 revision 判断异步等待期间声明是否变化；保持不变的 Action、工具、目录和视图来源不会因无关 render 全部重建。

## 查询、共享与长期关系

`ResourceInstance`、`ResourceValue`、`shareResource` 和显式指定 owner signal 的 `ResourceReader` 把 owner 撤销表达为资源失效。仍活跃的观察先收到原 `signal.reason` 的 error Observation，再正常完成；退出后订阅只取得新的撤销观察，不连接来源或回放旧业务值。`peek`、`read` 和回调式 `subscribe` 拒绝已经结束的 owner。`values$` 在自身 owner 退出时正常完成；owner 活跃时的查询错误仍是值流错误。显式 owner 的资源在来源正常完成后继续观察有效性，来源 I/O 按其本来完成时点释放；owner 退出才结束这种观察。空完成或停在 pending 后完成交付 EmptyError。未指定 owner 的 ResourceReader 与普通 Observable 保留来源的有限语义。

`Scope.dispose()` 同步 abort 并执行清理，`scope.closed` 等待该 Scope 实际持有的已开始 Promise、异步清理及子 Scope 结算；清理失败可使其拒绝。手动释放已经开始的异步清理后，即使子 Scope 已撤下，父 Scope 仍等待它。停止观察不等于取消已开始的业务，也不意味着等待独立业务 owner 的整个 Operation 结束。Reaction 失败以 `run.settled` 的 failed 结果结算，不自动使 `Scope.closed` 拒绝。

最后一个冷会话借用释放时，程序图同步退出；应用关闭仍等待已经收集的图所持有的真实工作和清理。`host.close(sessionId)` 结束原生 Agent 挂载，仍有借用的 Program 冷图保持原身份；工作区关系则直到实际 Workspace owner 退出才停止并结算。

Scope 子树共享父撤销的原原因，父退出后即使尚在执行其他同步清理，子资源也已经不可使用。实际释放仍走原有 Scope 树，父汇总子清理错误；逻辑撤销不代替原生异步释放的完成回执。

`useObservable(factory, deps)` 用 React `useMemo` 保持来源身份，并以 `useSyncExternalStore` 观察当前快照。factory 只构造 Observable；订阅在提交后的观察阶段建立。首次未发出为 pending，成功发出为 ready，来源错误为 error；空完成产生错误，已经发出后完成保留最后值。观察退出后断开所属查询连接，再次观察时按该来源及订阅语义重新准备。

`useShared(factory, deps)` 返回标准 RxJS 共享查询，使用一个值的 ReplaySubject。多个观察者共享来源，最后一个观察者退出时断开尚未终止的来源；完成与错误在该来源身份下保留。它不因为某次通知或重新订阅自动重试，也不把查询变化转换成模型输入。

`useSharedResource(source)` 或 `useSharedResource(factory, deps)` 返回 Program 拥有的 ResourceReader，按原 observations$ 共享，构造不订阅。普通函数、read/peek、值流与 useResource(reader) 使用同一资源；Program 退出以原原因结束本读侧，借入来源保留原 owner。Workspace 与 Program 使用同一共享机制及清理失败汇总，不建立两套事实。

`action.activeInvocations` 和 `run.state` 分别提供在途集合与 Reaction 运行事实的资源读取，原 activeInvocations$/state$ 是相应 reader.values$。它们归实际绑定/run，保持执行后的 draining 与真实终态；同步事件重入中的 read/peek 读取同一当前事实，完成或停止关系不覆盖尚未结算的业务。

同一查询同时供 Context 与 Expose 使用时，应先明确共享，再派生各自需要的形态。Surface 出口自身也是来源的消费者，因此组件观察退出不一定立即结束仍由出口持有的连接。工作区级共享通过 props 中的 `workspace.share(source$)` 明确持有，同一应用宿主内，同一原生工作区与同一来源对象复用连接，跨该工作区中的不同 Environment 共用；其寿命独立于页面和单个会话。Environment 的程序与 Preset 配置不拥有目录或工作区共享连接。

`useRelation(factory, deps)` 在 effect 中订阅长期 RxJS 关系，依赖替换或组件退出时清理订阅。未在流内处理的错误使程序 owner 失败，后续 prepare 和调用被拒绝，公开视图进入错误。已受理业务调用继续遵循自己的执行信号。关系可以显式调用 Action 或 `conversation.submit`；组件 render、来源读取和外部观察本身不隐式执行这些命令。

React effect 遵循 React 的重建与清理规则，业务不能把 effect 挂载当成可靠的“只执行一次”凭据。持久去重和跨重启执行仍由业务 owner 的正式设施承担。

`useResourceInstance(implementation, params)` 与 `useReaction(implementation, params)` 只取得当前程序持有的参数实例，不建立来源或执行命令。解析后的参数与实现对象决定共享身份，组件位置和 key 不另建资源缓存；目标变化取得另一实例，原对象不重定向。`useResource(implementation, params)` 与显式实例入口共用同一个绑定。跨会话共享仍须由外部 Scope 持有并传入实际实例。

这些参数绑定的 Scope 由 Program 持有，并随 Program 失败或会话退出释放。组件卸载只结束其查询观察，已经显式启动的 Reaction 保留到 `run.stop()` 或程序终止；未处理的 Reaction 执行失败留在本次 `run.state$` 和 `run.settled`，不隐式使 Program 失败。需要传播失败时由业务关系明确处理。退出停止感知和排队工作，不追加已受理 Action、Job 或业务 Operation 的取消。

局部 `createResource(scope, options)` 与 `createReaction(scope, options)` 不要求名称。可选 name 只提供诊断标签，不参与身份或登记；局部对象直接持有自己的实现，多个未命名对象不会共享事实或运行。需要跨模块参数化复用时，仍用具名合同及明确实现绑定，沿同一套 ResourceInstance、ReactionInstance 与 Scope 运行。

Reaction 的来源及执行结果可以同步完成。框架等待该次订阅返回、接收其清理函数后才交付终止，避免先结算成功再丢掉同步清理失败；已发出的业务结果不回滚，来源或结果流向框架传播的清理错误使本次运行 failed。框架不接管来源内部已经吞掉的错误，也不改变 RxJS 的操作符、调度或全局错误配置。

原生会话读侧在来源仍有效时，跟随实际 Agent 的关闭和恢复。原生挂载引用只有一份，输入、人工等待、Operation 与原生观察都读取它；owner 切换取消在途切面，按当前 owner 重新取原生历史，迟到的旧读取不再交付。日志继续使用原始 seq 去重和排序，不回放已经交付的事件；这是实际 owner 变化引起的查询切换，不是错误重试，也不创建 Agent、投递输入或唤醒模型。真正读取错误仍进入原错误边界。

冷 Program 图与原生声明安装分别归属。关闭 Agent 时，Context、Tool、Skill 的注册、目录监听和准备查询随该次原生安装退出，公开句柄仍可保留冷图；恢复为同一图创建新的原生安装。Program 失败同样撤销原生声明及其准备工作，异步清理由正式 Fiber 和会话关闭共同等待，不追加对已受理 Action 或业务 Operation 的取消。open、service 与 resume 的预期合同不匹配时不求值新程序；恢复失败归还本次取得的原生句柄，不把仍有公开引用的冷图一起销毁。

`fromSnapshot` 先建立监听再读取基线。`abortable(read, signal?)` 把退订接到本次读取的 AbortSignal；外部 signal 取消时立即以原 reason 结束本次观察及 Promise 等待，不等忽略取消的提供者返回。已经取消的订阅不进入 read，迟到结果与拒绝不再交付；其他消费者保留自己的读取寿命。取消等待不宣称提供者已经静止，实际资源清理仍由原 owner 等待。串行、并行、切换、调度和重试均使用 RxJS 的标准操作符。

## 资源失效与 Context

`observeResource({ key$, read, invalidations? })` 是冷流。每次订阅先监听当前 key 的失效，再触发读取；key 或失效通知变化时，通过 `switchMap` 退订旧读取，新代次立即发出 pending，随后发出 ready 或 error。读取错误保留为资源状态，后续失效仍可重新读取；key 或通知来源自身的终止继续遵循 RxJS 规则。

generation 表示该订阅的读取顺序，业务修订由业务合同提供。`combineLatest` 组合最近发出值，不提供跨对象或跨服务事务；对象选择与派生资料需要在同一切换分支组织，或由应用明确核验身份和版本。

`Context` 接受 JSON 快照，或 `Observation<T>` 与纯 render 函数。资源处于 ready 时才渲染成 JSON；pending、error 保留其状态，不能通过空字符串或旧成功值冒充当前现场。字符串按原文进入上下文，其他 JSON 值被序列化为文本。

`useObservable` 观察普通来源自身的发射状态；`useResource(factory, deps)` 则明确消费已有的 Observation 流，返回直接可供 Context 使用的 state，以及同一条标准 observations$。它复用共享查询和 useSyncExternalStore，不另建业务数据图。资源的终止错误、空完成或停在 pending 后完成都进入 error；正常 Observation 对象原样传播，不按业务值里的 status 字段自动猜测资源。

同名 Context 在后续完整帧中复用同一个资源接点。当前值进入 pending 时，原生 prepare 等待该接点；名称撤下时，旧接点释放，正在等待它的 prepare 改读当前集合。源失败与业务空值保持不同含义。

## 原生步骤的准备边界

dsh 的实际顺序是：inbox 提取并报告本步输入，执行 `system-prompt/assemble`，再进入 `agent/pre-step`，最后按原生请求准入流程提交模型调用。Skill 的原生 catalog 由 pre-step 生成，因而能读取 assemble 准备阶段已经装配的目录。

宿主在 assemble 中先核验工作区成员关系、公布本步输入事实并复核应用准入，处理适用的 Goal 输入关联。随后进入准备循环：

1. 冲刷组件工作与提交，取得完整帧 revision。
2. 执行 `useBeforeStep` 登记的检查，完成当前 Skill 的原生装配与正文读取。
3. 等待当前所有必需 Context，记录本次提交文本，再次检查程序。
4. 核验帧 revision、Context 当前值、Skill catalog 与 cwd 仍稳定，通过公开 API 重新 assemble，并在其返回后再次核验。

等待期间有声明或资源变化时，循环重新读取当前集合。准备失败、程序失败或取消会拒绝该步骤。内部资源准备不受页面订阅数量控制，也不另建模型队列。

`useBeforeStep(check, deps)` 在布局 effect 中登记异步检查。同次准备并列连接当前全部检查，任一当前检查失败立即拒绝准备并释放其余本次等待；一个 pending 依赖不能遮住兄弟模块的真实失败。登记集合变化会结束旧次等待并重新核对当前集合，组件顺序不构成检查执行顺序。检查可能因依赖等待或重装配执行多次，必须幂等并使用传入 signal；正式业务命令应进入 Action，不能藏在这项检查里。

通过准备边界的文本与工具 schema 构成本步装配。后续事实可以继续变化，原生请求保留已取得的步骤声明，工具开始执行时再核验当前绑定、enabled 与业务准入。这里的稳定性检查服务进程内装配，不构成多个权威业务 owner 之间的原子事务。

普通业务通知更新组件、查询和公开视图；模型推进来自显式可信输入、原生 Goal 或有效续接。需要自动投递时，由程序关系显式调用 `conversation.submit`，仍经过同一输入准入与持久回执边界。

## Action 绑定与三类调用

`defineAction` 是可共享的纯合同，拥有目标、输入、输出、观察投影及可选的 Operation 映射。`implementAction` 将同一合同接到宿主 execute 与 Operation owner；消费者不需要导入实现。`useAction(implementation, targetFactory, deps)` 只构造绑定，目标在绑定时解析、复制并冻结；同一 owner、实现和依赖保留同一绑定，目标改变必须改变依赖。

绑定在提交后激活，组件退出时失去后续受理资格。释放或撤下声明不取消已经进入业务 execute 的调用，调用继续沿实际信号与业务 owner 结算。目标、输入关联和 continuation 使用各自快照，调用过程中不把当前选中对象偷偷替换为另一个对象。

每次显式调用立即创建 Invocation。读取其 `result` 或订阅 `activity$` 都观察同一次执行，订阅不会重做。输入解析、准入、Operation 接合、执行和输出解析的失败保留各自阶段；输出在成功解析后交付。观察投影错误记录在 Invocation 的返回活动中，不回滚已经执行的业务。原生工具的最终成功或失败仍以原生工具管线结算为准。

`Invocation.wait({ signal })` 只等待原 result，取消只结束这一次等待；其他等待者、activity、result 和可选 operation 保留原身份与真实结算。本地 Action 不将首次 SDK 调用者信号捕获为永久等待条件，绑定、组件或查回句柄退出也不重新绑定原 call 的读取 owner。远程 Invocation 的等待仍受所属 SDK 的读取寿命约束；原生写入及上传继续使用原签发者的默认等待寿命。`call.wait(reference)` 是业务执行上下文中的原生续接登记，与 Invocation 的结果等待分别使用。

本地 SDK Action 的 options.signal 只限制受理前准入和持久准备结束后进入 execute 的边界。业务开始后，其 result 和 Operation 关联由 Host 的真实执行信号及业务 owner 决定，取消消费者等待不会把已经成功的写入改成 failed。显式 ActionExecutionContext.signal、原生工具 signal 的真实执行权限与后置复核继续适用；Operation 的业务终态另用 handle.settled({ signal }) 读取。

| 入口 | 身份、准入与因果 |
| --- | --- |
| `Offer` | 显式工具名和 enabled 控制当前模型可调用性，实际执行复用当前 Action 绑定；通过真实 tool occurrence 取得 callId、rootCallId、parentCallId 和消费的输入 |
| 程序显式调用绑定 | 宿主以 serviceActorId 建立独立服务参与身份；没有隐式模型输入或 Goal continuation |
| 同进程 `conversation.resolveAction` | 按明确的原始 scope 捕获当前 Expose 绑定，调用保留句柄的 human 或 service 角色与对应可信 actorId，服务逐调用不能改写身份，执行前核验同一 bindingId |

普通参与调用不凭参数自报原生执行身份，也不继承当前 Goal；需要 Goal 等待或完成接线时，只有实际原生工具入口提供对应权限。手工使用 `bindAction` 时，调用方必须提供自己的可信执行来源与准入合同。远程入口使用同一纯 Surface 合同及宿主绑定，认证、传输和恢复边界见[外部参与与观察](transport.md)。

SDK 的解析与调用是两个明确阶段。解析只对调用者提交的原始目标运行 scope schema，并与当前绑定的已解析目标核对；不对绑定输出再次运行单向 transform。调用保留真实 bindingId，异步准入结束与 execute 开始前仍核对声明及宿主权威。另一实例即使目标相同也不能承接旧捕获；同一个真实对象暂时撤下再公开仍保留身份。bindingId 不承担权限或跨进程恢复。

宿主对工具、SDK 和组件服务调用核验同一会话的 Environment、持久 binding、原生 Workspace 实体与成员关系。异步准入和 Operation 准备结束后，进入 execute 前再次核验；已撤销的成员或已移除的工作区不能凭保留句柄开始新业务。已经进入 execute 的调用仍由原业务 owner 结算，撤除登记不等于取消该操作。冷会话的核验只读原生事实，不为检查而启动 Agent。

原生工具声明从 Action 合同投影到 dsh 支持的 JSON Schema 子集，完整 Zod 合同仍负责输入与输出校验。不能正确投影的合同拒绝装配。PTC 子调用继续通过原生工具管线，parentCallId 来自已记录的原生子调用事实；`concurrency: 'parallel'` 仅向原生调度器声明可并行性，不创建另一个执行器。

`z.json()`、可空 JSON 和 `z.record(z.string(), z.json())` 可以直接用于业务合同；框架把 Zod 的递归 JSON 表示归一到原生任意 JSON 节点，并保留业务描述。此转换只接收明确等价的 JSON 结构，未知引用、其他递归及原生子集无法表达的字典约束仍拒绝装配。

模型能力暂时撤下与 Surface 公开槽撤下各自生效。Offer 不自动成为公开动作槽，Expose 也不自动成为模型工具。原生 guard 和 Action 准入会核验当前声明，已经返回给模型的旧 schema 不授予过期绑定新的执行权限。

`BoundAction.invocations$` 是具体绑定的新调用热流，不回放历史调用，订阅不会执行 Action。AI、程序与 SDK 经同一绑定创建 Invocation，流交付调用方取得的同一个句柄。句柄已经成立但输入或准入失败的调用仍可观察；无法取得可信执行来源、未建立句柄的调用不产生事实。

当前调用的 result 与 activity$ 独立结算，单次失败不会终止调用流。流的发出不承诺调用仍在 pending；activity$ 保留它当下的状态。绑定释放完成新调用流，已开始的调用继续结算。result 返回只证明业务函数链返回，原生工具后处理、Operation 与 Goal 的完成各有原事实，不能互相替代。用 RxJS 高阶操作符等待异步结果；需要失败后继续接收下一次调用时，在每个 inner 中明确处理业务失败。

### 共同资格与准入寿命

资源层 `Eligibility` 沿同一 Observation 在 Action、Context 和 Follow 间传播，Program 共享 reader。首次 pending 等待首次 ready，不借用旧 ready；首次 error 或 ready denied 拒绝新调用。取得 allowed 后，任何 pending、error 或 denied 都撤销尚未进入 `execute` 的本次准入，后续恢复不会复活它；连续 ready allowed 的代次变化不是业务事务版本。来源真实报错成为资格 error，不因此终止整个 Program。

资格撤销发生在 `operation.prepare` 期间时，等待 prepare 真实结算，确定尚未开始后记录 `not-started`。进入 `execute` 后退出资格观察不取消业务；观察清理失败也等待业务实际结算，保留正式结果和 Operation 回执，通过返回活动的 `observationError` 报告清理失败。资格观察不替代逐次 admit。

Offer 自动消费绑定资格，并与声明的 enabled 共同决定模型可见性。Context 的资格 denied 撤下声明；pending/error 对必需 Context 保留准备屏障，对 optional Context 撤下声明，不阻塞无关模块。

## Surface 领域值与 wire

`defineSurface` 用 `view(schema)` 声明值事件流，用 `resourceView(schema)` 声明 Observation 资源流，并列出每个动作的精确 Action 定义。Expose 可以贡献部分槽；同一完整帧中重复贡献同一槽会失败。未知槽、非 Observable 视图来源与不同 Action 定义身份均被拒绝，JSX 返回类型擦除不能绕过运行时校验。

两种视图的本地域值均为 schema 的 `z.output`。资源视图收到 pending 立即使当前读取失效，收到 error 保留当前失败；只有 ready 才编码，后续资源状态可以恢复。普通值视图不自动展开值中的状态字段。出口对每次领域值用 `z.encode(schema, domainValue)` 得到 wire，再校验 JSON；本地领域值保留原对象，不通过第二次 parse/decode 重做 transform。`decodeView` 使用同一 schema 的 `z.decode` 处理远端 wire。普通验证可以恒等编码，单向 transform 的反向编码明确失败，Date 等值必须使用输出为领域值、输入为 JSON 的双向 codec。

出口按 Observable 对象身份保留连接。替换或撤下来源会使槽进入 pending 并结束旧查询；尚未提供的已声明槽保持 pending。普通值来源空完成或 encode 失败进入 error，已经发出值后完成保留最后成功值。资源来源完成时保留最终 ready 或 error；最终状态仍为 pending 则进入 EmptyError，不能让当前读取永久等待。公开观察只按声明的 view 模式处理状态。

`views` 观察本地领域值，`observeView` 观察发射状态，`readView` 等待当前就绪值。跨进程使用出口已经编码的 wire 值，由同一 Surface 合同解码；发布产物消费和真实网络是独立验证边界。

## Operation 与可靠交接

Operation 合同分别描述命令、正式引用、状态、终态与可选反馈 schema。命令和引用必须具有明确 JSON wire，并可以通过同一 schema 双向编解码；业务 owner 取得冻结的解析后身份。状态与节点 payload 由 owner 提供 schema input，框架在各自边界解析，公开领域值保留 schema output。

Action 可以声明 Operation 映射，宿主实现必须登记精确匹配的真实 owner。具有该合同的 Invocation 提供 operation Promise，正式回执经过映射后连接原操作；attach 和 locate 只执行读取，不进入 Action.execute。Invocation 返回、Operation 终态及 Goal 完成分别结算。

observations$ 是标准共享 Observable。它保留 ready、pending 与 error，最后一个观察退出只结束查询。settled() 只等待 settled 判定不为 undefined 的状态；业务失败、取消可以是正常领域终态，读取异常及观察者 abort 则拒绝本次读取。false、0 和 null 不会被当成尚未完成。

follow 只接受具有 registerFollow 能力的已配置 owner；装配时缺少实际 owner、feedback 判定或发布者身份会失败。registerFollow 不只是存储回调：它必须核验原调用与当前通知者是否有权建立这条关联，并 ACK 已提交的可靠事务。命令尚未受理和命令已到反馈节点两侧都需要可靠通知，不能只订阅未来变化。

这项登记是业务通知订阅，不是另一套会话驱动。owner 保存订阅所需的业务身份、参与 ID 与通知地址即可；原 continuation、Goal 修订、wait、会话恢复及结果投递由框架沿原生日志持有。owner 不需要解释 Goal、另调原生等待或生成对话提示，也不应把这份登记交回业务应用原有的 Agent 续接驱动。

同一 command 永久指向同一 canonical reference。新的执行代次使用新的业务命令身份。业务回执与权威 locate 两边都核验这一不变量，先到的通知不能把原调用悄悄接到另一操作。

可靠通知与原 prepare 的网络 ACK 可以交错。通知先确认可靠关联，再补齐该原工作组的阻塞等待；已经解除的同一等待不会被迟到 ACK 重新打开。恢复时先建立同一原工作组的全部阻塞等待，再允许结果触发继续；不同 Goal 修订各有边界，旧关联没有资格阻断新的授权工作。

feedback 的 payload 使用独立纯合同。owner.readFeedback 按确切的持久 link 读取该次登记锁定的节点 `{ id, payload }`，可表示非终态的调查、接手或阶段核验；它不调用 registerFollow、不重新开始观察。节点 id 由业务 owner 签发并解释，没有框架全局顺序。查询及权限异常抛出，不包装成业务 rejected。

框架固定该次等待的首次投递正文与完整节点 wire，重试使用相同 requestId。owner 返回与原投递冲突的节点时明确失败，不能拿 F2 改写 L1 的 F1。待交回事实需要由 owner 可靠保留；本接口不从历史 Observable 发射反推它，也不要求 owner 新建全局事件日志。

固定节点不要求额外建立每个参与的选点表。如果业务已有不可变、有确定顺序的事件源，owner 可以按 after 稳定读取首个后继节点；原生子程序使用的事件序号就是这种方式。会被覆盖的当前状态、截断的最近消息列表和没有历史身份的通知无法替代这份事实。

`useOperation` / `bindOperation` 只绑定不可变原 command。`useOperation` 的工厂可以明确返回 null，表示当前没有操作可公开；它不定位、不登记等待或取消此前的业务。locate 与 attach 继续使用正式查询，participation(id).readFeedback 是对已登记参与的一次快照读取。Surface.operations resolver 捕获当前公开绑定；所得句柄不因槽切换或撤下而改读另一命令。宿主可按会话、合同、原 command 和参与 ID 恢复读取，核验真实持久 link 和 owner 读权，不加载 Agent、不登记等待。

`Follow` 是独立的能力声明，其资格 pending、error 或 denied 时撤下可用声明。模型调用在真正进入 owner.register 前 pin 准入；异步 checkpoint 和原输入 Application.admit 后最终复核真实 native 来源、成员与声明。资格撤销后恢复不会复活旧调用。真实原生工具调用才创建新 link，取得本次输入与当前 Goal 修订；它没有 Action.execute。feedback 参与的输入明确包含 after: nodeId | null，settled 参与不使用节点后继条件。after 不携带 Goal 权限。

`Offer.follow` 和 `Follow` 共同使用 `mode: 'wait' | 'background'`，省略时为 wait。wait 等待可靠登记和原生 wait 的持久 ACK，暂缓精确原 Goal 并结论当前轮次；background 同样先可靠登记关联，但不增加阻塞等待、不结论当前轮次，Agent 可以继续其它工作。两者的结果都按固定身份交回原会话，自动唤醒仍须原 Goal 的精确修订有效、未暂停并且不存在其它阻塞等待。background 不获得新 Goal 的权限，也不因为模块退出而取消原业务。

独立 Follow 在首次 owner 登记前被拒绝时，持久记录 `operation-follow-unregistered`（id、continuation、wait），投影为 `unregistered`。它只说明该 Follow 未进入登记，原 Operation 可能早已运行，不能视为 `operation.not-started`。通知仍核验真实 notifier，返回 `{status: 'unregistered'}` 可 ACK，不激活 Agent 或重新登记。进程内首次进入门阻止恢复和通知抢跑尚未准入的 intent；进入 owner 即放行，不等待 ACK，ACK 期间撤声明不取消已受理登记。Host 持有已进入的 Promise 至真实结算；Goal 旧 revision 与暂停仍遵循原规则。进入门仅存在于进程内；进程中断后可能只剩持久 intent，不能据此判断 owner 从未被调用。冷恢复按当前 Application 与业务 owner 对原关联的授权幂等重建，不序列化 JavaScript 资格 pin 或调用栈。只有已持久的 unregistered 证明该参与被拒绝且未进入登记；不能保证所有未通过原运行内准入的崩溃 intent 跨进程都不会登记。未登记结案 checkpoint 失败属于目标 link 和原 Goal 组的故障，不封锁同一 Session 的其他模块。

只需要观察时，直接组合 `OperationHandle.observations$`、`settled()` 或业务资源，不声明 Follow。观察本身不会登记可靠通知、唤醒 Agent 或改变 Goal。后台反馈、暂缓原工作与只读观察因此可以在同一个模块中明确选择，保留各自的执行含义。

L1 交回 F1 后，明确调用 Follow 以 after=F1.id 登记 L2。owner 可沿同一 task 建立新的观察尝试 W2；即使两次观察失败正文相同，真实节点仍有不同身份。同 L2 重试不创建 W3，旧 L1 重放不替换 W2。恢复只接续已经保存的 link，没有新工具调用不会从旧反馈自动生成下一次参与。

原生投递由固定的服务发布主体负责，实际通知者分别准入与审计。第一次合法通知者不能占有后续恢复发布权；配置更换到另一主体也不能静默改写已有投递身份。accepted 仅确认原生持久受理，unknown 或 pending 不能用于清除业务 outbox。

`OperationOwnerOptions.notifications` 将业务已有可靠设施接入 Host：`connect({ deliver, signal })` 返回 cleanup 或其 Promise，设施负责持久化、租约与重投。`deliver` 接收 sessionId、followId 和 acknowledge，以固定 `serviceActorId` 核验真实 owner；仅 accepted、not-started 或 unregistered 才调用 acknowledge，unknown、pending、拒绝或失败不 ACK。ACK 只移除业务 outbox 待办，不移除 follow 或 feedback。signal 停止新交接；Host 等真实业务调用、投递及已开始的 ACK 结算后调用 cleanup，异步连接迟到和部分启动失败也遵循此顺序。仍可显式调用 `notifyOperation`，Host 不创建另一份队列。

## 人工问题与前台等待

HumanInput 声明当前程序的响应者策略；多个贡献必须使用同一个必需的 `admitResponder` 函数引用，冲突时拒绝，最后一项退出时撤销资格，不设默认政策。它不创建第二份问题记录或问答状态机。宿主使用实际 userQuestions waterfall、计时服务、投影与 answer：前台返回、超时 pending 和 continued 迟答仍沿原生生命周期。

Action execute 中的 askHuman 使用本次真实 ToolRunContext 发起标准 ask_user_question 子调用。它保留原 parent token、rootCallId、parentCallId、Agent 和取消信号，通过原生工具管线执行；Session 日志记录实际 programmatic 子调用的开始与最终结果，而不是制造模型直接调用。pre/post policy、附加上下文和结论当前 turn 的标记仍沿原生结果传递。

不带超时的请求等待本次函数里的答案；带正整数 timeoutSeconds 的请求允许原生 timed 工具返回 pending。只有结果已按原生合同记录的问题才能从持久投影恢复；进程中断不重建旧前台 resolver，也不恢复任意函数栈。SDK 和服务参与调用没有原生父工具，不能使用这项能力；被委托 Agent 继续遵循原生问答服务的限制。

wait、completeGoal 与 askHuman 共同受 Action 调用体的执行边界约束。业务 execute 返回或抛错时，先关闭这份控制能力，取消并等待该调用尚未结束的提问，之后才处理父调用的返回结果；清理不会重做业务或覆盖原业务返回及异常。原生父取消持续传到每个问题，不能通过附加的等待撤销原取消关系。

questions$ 提供持久问题视图，foregroundQuestions$ 提供当前真实 waterfall 的前台 resolver。PTC 前台等待没有对应持久 open 问题，不能把持久快照冒充前台；进程恢复也不会重建旧 JavaScript resolver。

公开问题、已回答问题与前台问题的 origin 从同一原调用记录取得，包含原会话和 ToolOccurrence 的完整输入集合。原生工具使用真实 tool/call，PTC 使用真实子调用；冷读取在恢复的原生投影切面中关联 userQuestions 与 reactiveInputs.calls。问题生命周期仍由 userQuestions 拥有，框架只派生归属，不另存一份问题状态。缺少原调用时明确失败，不使用当前输入、当前商家选择或另一会话的调用补齐。

rc.3 的原生问题只按 callId 寻址，没有公开调用发生序号。框架只在同一会话里存在唯一的同 ID `ask_user_question` 发生时签发 origin；同 ID 普通工具不会改变原问题来源。同 ID 多次提问产生无法核验的来源歧义时，明确报 `REACTIVE_HUMAN_QUESTION_ORIGIN_AMBIGUOUS`；问题观察会终止，本框架的前台接入或响应准入也可能拒绝，不宣称能自动恢复或回答其中某一代问题。原生问题状态仍由原生服务持有。

answerQuestion 和 attachWait 都核验真实 actor、原问题、响应通道和当前策略。claim 准入的 answer 为 null，提交准入取得完整答案批次；异步准入后再次核验策略身份、问题和 signal，避免撤下策略后迟到答复进入原生服务。无声明硬拒，不委派其他 answerer 绕过准入。

当前策略通过 admission.question.origin 核验原工作的参与范围；观察与准入各取得自有快照，消费者不能通过修改问题对象改变权威关联。origin.inputs 保留原始 JSON 与 sourceSessionId，不重复执行 Program 的输入转换。它不携带 Goal 的继续权限，也不使回答者继承原请求者的授权。

attachWait 接手原生前台计时，物理流取消释放 claim。句柄退出、策略撤下与问题取消具有不同含义；撤入口只释放所属连接，问题仍由原生 owner 保留。冷 continued 回答在恢复 Agent 之前预审，恢复后仍走实际原生准入。

continued 答复按原问题的 callId 恢复工具调用保存的输入集合，保留原请求、操作者和业务范围。响应者另行准入，不改写原工作身份，也不产生新的输入授权。缺少原调用记录时拒绝恢复，不能拿当前输入或宿主身份补齐。

相同策略函数在无关声明更新时保留身份，不重置前台 claim；更换函数表示更换策略。应用需要用稳定函数表达稳定策略，并把事实读取与正式权限核验放在业务服务中。

## Skill 目录与方法正文

环境的静态 Skill 目录由原生 Preset standing scope 持有。动态 `Skill` directory 声明要求明确绝对目录，Agent owner 在 ready 边界增量装配原生 filesystem plugin，退出时等待相应 fiber 释放。provider 的发现、覆盖、catalog 失效与 watcher 生命周期由原生实现负责；未声明的默认根不会由该 provider 自动引入。

`Skill` name 在当前 Agent scope、原生 working directory 和调用 signal 下读取既有定义，核验其允许模型调用，再用公开 `renderSkillContent` 将方法正文及资源基址写入 required prompt context。缺失或不允许模型使用的方法使准备失败。name 撤下只撤正文贡献，仍有目录 provider 的方法可以继续出现在原生 catalog 中。

声明提交只更新期望集合，不读文件。ready 等待原生 provider 与 context 注册就绪，prepare 加载正文并返回稳定性校验。装配期间的名称、目录、catalog 或 cwd 变化会使读取重新核验；其他 Agent 和全局层不会因当前 Agent 挂载一个目录而获得它。

## 当前状态、新事件与输入

`createWorkSelector({ identify })` 用应用定义的 JSON 键选择原始输入或持久请求声明；返回 undefined 表示不属于该工作。`select` 同步投影显式快照，`observe` 组合显式会话来源的 progress 与 human，并共享一个 ResourceReader。`state$` 是该 reader 的观察，`events$` 和 `progressFrames$` 只交付订阅后的匹配事实，不重放历史。冷观察不复活 assistant attempt 草稿，也不发现会话、启动 Agent 或取得权限。

Activity.operationFollows 包含原 link、receipt|null、preparedDelivery|null、notStarted|null 和 unregistered|null；preparedDelivery 保留 requestId、actorId、notifierId 及可选 reference、node。这些是同一原生投影的持久事实，读取不触发 locate、register 或 notifier。intent 不证明 register ACK，缺少 receipt 不证明未执行，prepared 不代表结果已受理、消费或结算。直播追加产生 operation.follow.changed，change 为 intent、receipt、delivery-prepared、not-started 或 unregistered，携带完整 inputs；冷恢复读取投影，不重放这些事件。

Activity 的 inputs 是输入目录，calls 和 replies 是完整发生事实，Goal.inputs 保存关联；Request.tools 与最后回复由这些事实派生。calls 保留每次真实发生，ToolActivity 和 ToolOccurrence 的 `callSequence` 是该次正式 start 事件的序号；provider 的 callId 可以跨轮复用。RequestDeclaration 是真实持久声明，包含 `result: { continuation, wait, ready } | null`；没有 messageId 的声明不是已受理输入。回复 `inputs: null` 表示关联未知，不按工作归组。每个命中项保留完整 value、原 callSequence 或 messageId 及完整 inputs；matchedInputs 只解释分组，不裁剪事实或授予续接权限。多输入共同调用或回复可出现在多个工作。原始 scope 在本地与远程均不执行 Program transform。

每个来源保留 sessionId、workspaceId、environmentId 和真实 Session 的 parentSessionId。会话中的 source、execution 是背景，不表示工作独占会话。条件表达 `goal.paused`、`goal.blocked`、`wait.pending`、`question.pending`、`call.failed` 及原续接约束。`continuation.unconsumed` 表示结果请求已 accepted、result.ready 为 true，但尚未推进到 consumed，不代表模型已处理；`continuation.superseded` 和 `continuation.inactive` 表达其所引用 Goal 已被替换或不再 active。`goal.round-limit` 只在 active Goal 已用尽轮数时出现；`operation.follow-unregistered` 表示该 Follow 未进入登记；`operation.not-started` 表示已知业务未进入，`operation.delivery-prepared` 表示投递已准备而结果请求不存在或仍为 declared。不据此判定整个工作 busy 或完成。跨会话观察不共享 Goal 或答复权限。

所有必需来源 ready 才有完整工作 ready；任一来源 pending 或 error 撤下旧 ready，来源退出保留原错误，不替换为空数组。不同会话 generation 不构成事务快照。options 可省略：无显式 owner 时读侧遵循 refCount 与来源有效性；最后观察退出只释放观察，不停止会话或业务。需要业务 Operation 阶段或资源错误时使用现有 RxJS 组合。

`activity$` 交付当前原生活动；`request(id).state$` 从会话进展派生该请求的受理、消费、工具、持久回复与活跃 attempt；`events$` 及 `request(id).events$` 只交付订阅后新发生的事实。请求句柄固定 sessionId/requestId，只读构造与观察不提交输入。ready 的 activity:null 表示当前事实中没有该请求，不能用 pending 或空成功冒充。

请求 state 中的 execution 明确属于会话；一次模型步骤共同消费的输入都关联到同一工具或回复。输入按 sourceSessionId/messageId 唯一，请求按 sourceSessionId/requestId 选择；不同会话中相同字符串的 requestId 不成为同一请求。请求的 settled 只表示相关 turn/end，不意味着业务 Operation 或 Goal 完成，也不结束后续观察。持久 reply 是最近已记录的文本，不能被解释为该请求独占的最终答案。工具活动保留原 callId、rootCallId 与 parentCallId。

`progress$` 交付会话活动和当前活跃 attempt 的 lossless compact 前缀，`progressFrames$` 交付原生 start/chunk/end 增量。开始帧固定本步完整输入集合，后续片段与结算继续使用该集合，不读取最新页面选择。共享累积接点复用官方 AssistantStreamAccumulator，校验读取复用 expandAssistantStream；只维护当前活跃前缀，不建立另一份持久日志或模型状态机。无人读取时不反复物化完整前缀，同版本读取复用快照。

冷打开和重连只交付当前快照，不重放片段或制造新的 start。断线期间已经结束的瞬态 attempt 不保证恢复，已提交结果由持久 journal 提供。进程退出后活跃草稿不成为持久事实。程序观察带基线的状态流时，应区分“当前事实”与“新命令”。

`reactiveInputs` 原生投影保存请求、输入、调用、Goal 与等待的关联；原生 projection registry 拥有水位与恢复。冷读取使用原生 session query 取得日志与身份。持久事件保留日志序号；助手流片段和驱动状态是瞬态反馈，其 sequence 为 null。事件通过日志追加边界之外的交付进入程序关系。

存活驱动的活动标记为 `source: 'live'`，execution 来自实际 Agent；冷观察标记为 `source: 'durable'`、`execution: 'unavailable'`，Goal activation 为 null。持久记录不推断另一进程的即时驱动状态。

提交使用稳定 requestId、content、intent 与经过当前 Program 输入合同校验的原始 JSON scope。content 可以是字符串或有序的原生 prompt parts；字符串只归一为 text，不 trim，不合并或重排已有 parts。图片提交原字节的 canonical base64，文件只提交同一真实 Session 上传签发的 receipt，客户端持久附件引用不是输入权限。goal 必须包含真实非空文本目标，不能从附件名或旧输入猜测目标。

普通输入必填 delivery，业务 intent 不决定队列或唤醒：

| delivery | 原生投递 | 本次投递请求唤醒 |
| --- | --- | --- |
| followup | next-turn，下一轮边界 | 是 |
| steer | next-step，在最近后续步骤参与 | 是 |
| inject | next-step，在最近后续步骤补充现场 | 否 |

轮次首步会共同取得已排队的 next-step 输入与至多一条 next-turn 输入，因此同一步仍可包含多个请求，工具与回复不属于其中某一请求独占。表中是提交时选择的边界；取消中的原生 Agent 可以把新的唤醒输入留到下一轮，不能据此越过取消。

inject 本次投递不请求唤醒，可能错过已经 claim 输入的步骤；驱动空闲时保留在原生 inbox，等待已有工作或后续显式唤醒开启步骤。冷会话提交仍需加载原生实例，已受理子工作可以沿原授权恢复，因此调用期间仍可能有原工作的模型活动。inject 不暂停或撤销既有 Goal 和业务工作，重试也不为这条输入增加唤醒资格。三种投递都不打断或另建模型执行器；claim、取消和队列寿命继续由原生 Agent 持有。

上传使用 `uploadFile(Uint8Array | Blob, options)`，同步取得非 thenable 的 `FileUploadInvocation`。result 保留这一次真实上传的成功回执或错误；wait(options?) 只结束本消费者对同一 result 的等待。Uint8Array 在调用时快照，Blob 在异步准备阶段读取；准备取消后迟到字节不能再触发激活或 HTTP。options.signal 约束准备与分派前检查，并作为 wait 的默认信号。Session 或原生 UI 退出也可阻止尚未分派的准备；分派后这些取消只结束 wait，不撤回上传或改写 result。

activity 是同源有限调用事实，activity$ 是其 values$。pending 表达准备中的调用，running 只表示已分派，不证明存储受理；returned 表达 result 成功，failed 保留原错误及失败阶段。失败仍是 ready.value，Session 或 UI 退出不撤销终态，晚读和晚订阅仍取得同一次事实。source 固定为 `{ kind: 'file-upload', sessionId }`，不继承 Action 或 Goal；id 只标识本次内存调用，不是幂等键、receipt 或持久恢复身份。

远程 SDK 或 Connection 物理退出可能使 result 拒绝为 `FileUploadReceiptUnknownError`。这是正式回执未知，不证明服务器没有文件；框架不重传，也不凭调用 id 恢复 receipt。Gateway 只传官方 `FileUploadValue`，调用对象及其活动由本次 SDK 调用持有。

已有 canonical base64 的 transport 使用 `uploadEncodedFile(request, options)`，原生服务检查编码，不先 decode 再重编码洗掉非法输入。receipt 由 exact 原生 Session 持有，未提交时不能跨 Agent 关闭或进程重启恢复。首次提交先解析 receipt、提升附件，再把 `AdmittedSubmission` 交应用准入；消费前以同一持久正文和原 actor 复核，不重建原始 receipt 或字节。

组件输入及 typed 事件消费 schema 的 output；raw 事件、进展、等待及 journal 保留原始事实。单向 transform 允许输出 Date 等领域值，不将它重新编码为提交输入。actorId 来自可信宿主或认证所得调用选项，保持原身份值，业务正文不能授予操作者身份。常规输入进入原生队列并完成持久化后取得 ACK；ACK 不等待模型回复或业务完成。

原生输入必须能关联到已受理的身份记录。未关联的原生输入保留未知操作者，模型准备时明确拒绝，不以宿主默认操作者代替。

现有输入关联记录保存 sourceDigest、已提升的 content 和消息 digest。sourceDigest 核对归一后的原正文、原始 scope、intent、delivery、续接信息及 actor；消息 digest 核对实际原生正文。对象字段排列不改变身份，数组顺序、文字、字节或 receipt 的变化仍会冲突。记录不另存 base64 或上传 receipt 注册表。

相同 requestId 必须命中同一原请求。重试先核已有记录，再处理 receipt；已受理的输入返回原确认，不重新提升附件。prepared record 已持久但尚未进入 inbox 时，同身份显式重试使用原记录的持久正文完成受理，即使 receipt 已随进程消失也不另换文件。仅有 prepared record 仍是 declared；原生 inbox 表示 accepted，user/message 表示 consumed，后续 turn/end 才结算相关轮次。回执丢失保留未知受理状态，客户端不自动重发；调用方可以观察原 requestId，并使用同一身份核对或重试。一次步骤可以消费多个输入，工具沿真实输入集合关联，后续 Goal 轮次保留原目标输入。

## 等待、退出与持久续接

使用 `Offer.follow` 时，框架先持久化原调用与业务命令的 intent，再等待 owner 的 `registerFollow` 可靠关联 ACK；wait 模式还需原生等待的持久 ACK，之后才执行业务。没有 Operation 的原生工具也可以显式 `await call.wait(reference)`；Promise 完成才表示等待已经跨过原生持久边界。登记阻塞等待会解除精确匹配 Goal 的自推进，并结论当前工具所在轮次。业务事务与会话日志没有共同的原子提交，可靠通知使用业务已有的事务、outbox 或等价设施。

组件子树或业务模块退出时，只撤当前声明、查询和关系连接。原请求、已受理调用、业务操作及持久等待仍归原来的 owner，退出不自动取消外部任务，不删除因果记录，也不授予后来挂载对象接管旧 continuation 的权限。

业务 owner 核验正式结果后，以稳定请求身份提交 `intent: 'result'`，携带 content、原 continuation、wait 与 ready，不得提供 delivery。ready 为 false 的进度只保存为已受理进度，不进入模型队列或关闭原等待。ready 为 true 的持久关联记录解除其精确匹配的阻塞等待，结果作为 next-step 输入进入原生 inbox；等待完成不等于模型已消费结果。框架核验原会话、原输入、已登记阻塞等待或可靠后台关联与 Goal 身份，调用方不能用投递选项覆盖原目标的继续条件。工具 `completeGoal` 使用调用取得的目标引用，原生 Goal 核验其 revision。

等待按 Goal id 与 revision 精确分组。只有该组全部就绪、当前 Goal 仍 active 且有剩余轮次时，结果才取得自动唤醒资格。恢复器及原生步骤最终执行前重新检查这些条件；等待期间新登记的条件也能阻止原结果继续。多条已就绪结果由原生 next-step claim 一起消费，不另建汇合队列。

暂停保留已 claim、尚未消费结果的原消息 ID，再由原生 pause 取消本轮并保留 inbox；显式 cancel 后不恢复被取消消息。人的新目标修订可以独立推进，旧等待不能阻止新权限，旧完成结果只能作为事实随合适输入进入，不能自行恢复新目标。重复提交使用原持久记录与 inbox 状态完成同一次受理，不再次执行业务操作。

恢复以原生已受理队列为权威，控制恢复信号不成为模型输入，也不提供额外权限。跨进程恢复重建持久关联及当前业务程序，不保存任意 Promise、React hook 状态或 JavaScript 调用栈。原生受理、输入消费、工具结算、轮次结束和业务正式完成分别表达。

## 句柄、原生 scope 与关闭

`open` 重建或复用会话程序而不加载 Agent；`resume` 和主动输入进入原生驱动路径。原生 Agent 可以在仍有观察句柄时关闭并恢复，会话组件程序保留，工具闭包及动态 Skill owner 归新的 Agent。

每个同进程句柄持有自己的观察 scope。`release()` 停止该句柄观察；有存活驱动或其他句柄时，程序继续存在。没有驱动且最后一个句柄释放后，组件树、出口和会话查询释放，工作区共享来源继续由工作区持有。

工作区成员撤销或实体删除使旧 Session/Program 图和已签句柄单向退出，资源与人工控制保留同一资格原因。工作区按需签发的 accessId 跨冷图收集保持，真实撤销后重新取得签发新身份。远程会话请求在装配前核原 accessId，未观察期间的移除与重新加入也不能复活旧 SDK。原生 Agent 的执行归属不变；显式重新 open 已知驻留 Agent 时只替换已退出的 Program 安装，不重启模型循环。

SDK 使用 `{ signal }` 选项表达取消。`readView` 只约束这一次等待；`attachOperation` 与 `operations.slot` 的信号限定所创建的只读观察句柄或 reader，停止其读取不等于取消业务 Operation。`resolveAction` 完成后，绑定连接属于 Session，解析时的临时信号不再持有它。远程 Action 的请求取消与已受理执行分别结算，不能把 transport 退出变成业务撤销。

同一会话与 requestId 的并发提交共享一份受理事务，各调用者分别等待。后加入者的取消不删除共享事务，也不替换首发的执行前取消信号；句柄 release 只结束自己的等待。首发已取消但原事务尚未退出时，新的显式重提先等待其真实结算，再核对当前共享事务与持久事实，不提前清表或并行重发。首次持久关联仍是 declared，编辑的持久准备也尚未替换队列正文，这两个阶段继续核验首发取消。输入已 send、编辑已 replace 或 not-ready 结果已经提交后，exact Agent 完成所属持久回执；回执等待取消不回滚输入、不自动重发。独立恢复的等待登记同样由原 owner 完成，通知者只退出对共享屏障的等待。已受理结算的迟到失败仍进入会话的结算与关闭错误链。

Program 的 conversation.submit 借用该 Program 的来源权限，Program 失败后不能沿捕获的对象开始新受理，进行中的等待也随其退出。本地已知准入、格式和身份错误保留原错误；远程 submit/edit 在请求已启动后无法确认回执时保留对应的 AdmissionUnknown 错误及原请求身份，供显式查询核对。上传已分派后的存储与 receipt 继续由原生 owner 处理，消费者退出不改写其 result；回执未知仍不能推断未上传。

`snapshot()` 是明确的同步活动读取。`pause()` 暂停原生 Goal；`cancel()` 停止当前原生模型处理并等待相应持久化。它们不替业务 owner 决定取消外部业务操作。`whenIdle()` 等待当前原生活动、对应输入恢复和持久化，不代表未来 Goal 轮次或外部业务工作全部结束；句柄释放只结束本消费者的等待，原结算仍继续。

句柄 `release()` 释放本消费者的观察与回执等待；宿主 `close(sessionId)` 释放实际 Agent owner，沿原生 teardown 取消尚未消费的 inbox。通知与恢复读取绑定取得时的精确原生 owner；关闭期间迟到的准入或读取不能隐式重开该 owner。持久关联保留，新的显式通知可以按原引用重试。正常关闭与进程中断不同：已 checkpoint 后的进程中断可以从持久 inbox 恢复，不能用 close 模拟这个边界。宿主释放 owned Agent handles 后退休 Preset，再释放程序与工作区资源；原生 registry 负责已经 mount 的代际引用。Preset 装配为 broken 时启动失败，并释放失败登记。

每次 Operation 参与分别保留原调用、条件与一次固定投递；多阶段协作通过显式的新参与组合。[原生子程序委托](native-delegation.md) 复用这份 Operation 合同，子 Goal 正式完成、原生执行反馈及父 Goal 续接保留各自含义。长会话和完整运营服务接入还需要连续证据。原生依赖可用、编译通过及确定性模型边界都不等于真实业务或部署环境已经验收。
