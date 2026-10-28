# 宿主、工作区与原生能力

适用版本：`{{PACKAGE_VERSION}}`。宿主从 `@hanger-source/dsh-reactive-dsh` 导入；不要从核心或客户端导原生安装闭包。

## 装配主线

纯合同 `defineProgram` → `/program` 的 `implementProgram` → `defineEnvironment({ id, instructions, program, skills? })` → `defineApplication({ name, environments, admit })`。admit 消费 `AdmittedSubmission`：正文已是宿主准入的 text/attachment refs，首次投递与模型消费前使用同形合同，并核真实 actor/participant。

```ts
import { defineApplication, defineEnvironment,
  type ApplicationDefinition, type ProgramContract, type ProgramImplementation } from '@hanger-source/dsh-reactive';
import { createRuntime, type RuntimeModel } from '@hanger-source/dsh-reactive-dsh';

export async function start<P extends ProgramContract>(home: string, model: RuntimeModel,
  program: ProgramImplementation<P>, admit: ApplicationDefinition['admit']) {
  return createRuntime(() => defineApplication({ name: 'inventory',
    environments: [defineEnvironment({ id: 'operations', instructions: '按当前事实处理库存', program })],
    admit,
  }), { home, model, actorId: 'operator', serviceActorId: 'inventory-service' });
}
```

保留具体 Program 类型，不用任意 T 或宽泛 interface 冒充已验证合同。admit 来自业务可信政策，不写空 policy 冒充正式权限。

可复用宿主模块接收 `ApplicationAssembly`：用 `assembly.native(createNativeTools({ service: 'provided' }))` 等登记原生能力，用 `assembly.operation(actualOwner)` 登记已有真实 Operation owner，用 `assembly.implementDelegation(...)` 构造子程序委托实现。native 和 operation 返回原对象及其精确类型，模块把它们交给自己的 Program、hooks 或服务，并返回 environment 供应用组合；无需启动层再列一份模块的能力与 owner。相同原对象重复登记只装配一次，不同 Operation owner 的 kind/version 冲突硬失败；工厂返回即封闭登记，组件及迟到回调不能继续注册。

应用挂载安装已登记能力，失败释放本次装配，Host 退出等待实际释放。Runtime 传递其归一化 home；已有宿主调用 `mountApplication(ctx, factory, { home })` 时，只有登记原生能力才必须明确提供绝对 home。`options.operations` 与 `RuntimeOptions.installNative` 继续表达宿主选择的共享业务 owner 和底座安装，模块自身的登记无须重复配置。Operation 的可靠状态与已受理工作继续归业务 owner，登记不改变它们的寿命。

native 选择尚未安装的能力实例并交给一个应用；另一应用不能重复取得该实例，退出或失败后也不能复用。各应用使用独立能力实例，共享底层服务用各域已有 provided 选项。自定义能力 dispose 需允许安装前回滚。operation 借用业务 owner，允许多个应用明确共用。

Runtime 的 home 必须显式绝对路径，隔离会话、附件、存储与凭据；不能改全局环境变量或进程 cwd 给多 Runtime 共享个人目录。`deepseek`/`piAi`/`nativeModel` 接模型；使用实际模型声明的容量和能力，不猜 contextWindow 或 vision。需要控制摘要/预留量时显式传 RuntimeOptions.compaction；官方 retry、token meter、compaction、image offload 已由 Runtime 安装。

```ts
const workspace = await runtime.application.createWorkspace({ directory: absoluteExistingDirectory, title: '仓库 A' });
const session = await runtime.application.create('operations', { workspaceId: workspace.workspaceId });
const recovered = await runtime.application.open(session.id, InventoryProgram);
```

createWorkspace 注册原生 existing 目录，不隐式 mkdir。Environment 不拥有目录；一个环境可用于多个 workspace，一个 workspace 可含不同环境。已有会话从自身权威 binding 恢复，open/resume 传 expected Program 核同一合同；resume 显式激活，open 只读不借恢复名义执行。

需要工作区级业务资源时，用 runtime.application.workspace(workspaceId) 借用本地 Workspace，再调用 workspace.resource(implementation, params) 或 workspace.reaction(implementation, params)，按正式实现与参数稳定复用实例。局部业务直接用 workspace.resource({ value, read, invalidations? }) 或 observe 分支，以及 workspace.reaction({ event, result, observe, operator, execute })；选项与 Program 局部入口相同，每次构造独立实例，相同名称或选项引用不合并。显式保留返回对象供函数、Promise、RxJS 和组件共用，在组件中借用已构造对象。不必先创建 Session 或从组件向外捕获 Scope；构造不读取、反应不自动启动，Session 退出不结束工作区实例。Workspace 类型从 core 的 /workspaces 或根入口导入；远程登记信息不携带本地 Scope，公开业务观察仍通过 Surface。

正式登记删除后，原生 domain/changed 提交事实结束工作区 owner，不等待整个宿主退出。旧资源、共享流和持续关系一起撤销，清理重入不能利用原生尚未撤下的实体缓存重建 owner。目录和会话历史由原生保留，已受理业务不因查询退出而回滚。清理失败保留诊断并由宿主退出汇总，其他工作区仍须释放。

`host.service(actorId)` 在取得时固定服务身份；省略参数时使用宿主配置的 serviceActorId。这个应用视图通过 create、open、resume、completeCreation 取得服务会话，复用原工作区、会话图和取得回滚，不增加 owner 或应用销毁权。服务会话可读、解析 Action、submit/edit 输入、上传文件及观察 Operation，逐调用不能自报 actor，不能回答人工问题或控制 Goal。ProgramSession.submit 仍使用宿主配置的服务来源。普通 UI/人工端口与服务不可混用。

## 嵌入与可选能力

原生子 Agent 发现从 `@hanger-source/dsh-reactive-dsh/subagents` 导入 `createNativeSubagents({ service: 'provided' })`。它使用宿主已有的底座 subagents 服务，不安装另一套 provider。安装后取得 `const children = capability.forSession(source)`：children.catalog 是同源 ResourceReader，children.catalog$ 是其值流，支持普通函数、Promise、RxJS 和 hooks；children.readDescendants({ signal }) 显式读取可达子树。保留 one-shot、continuable、external 原生身份及不可读分支诊断，不把目录模式当成业务阶段。其他父会话的目录事件不使此资源失效；关闭/恢复原 Agent 重新取得原父目录，冷读不恢复 Agent 或启动模型。

Subagents 的 dispose 只撤销读侧，不停止已有 Activation。普通原生发现不要求 Goal；带独立 Goal 的子程序委托继续通过 assembly.implementDelegation 和 NativeChildWork 组合 Action、Operation 与反馈。此入口不包装缺少可信 Program 执行前准入的 raw start/send，也不把这个缺口扩大为整个子 Agent 域不可用。

Schedule 管理读取、冷 Team 投影及 Files 的显式读取使用所属 SDK 与调用者的取消信号；等待立即结算，不被迟到的原生查询回执拖住。Files 仍独立跟踪真实 I/O，实例退出等待这些读取与 unwatch 完成。只读等待的取消不改变原生任务、提醒投递或成员执行的 owner。

Team 的 createTask、updateTask、task.update 与 Schedule 的 task.update/delete 在通过执行前检查后同步返回 `NativeInvocation<T>`。已退出的 Scope、预取消、保留来源字段或固定目标不匹配等检查同步抛错，不启动原生调用，也没有可等待的调用对象；已经进入原生方法的拒绝才由 call.result 与 failed 活动表达。调用对象复用核心 Invocation 合同，source 包含真实 native 来源、原 sessionId、capability 与 operation，不冒充 Action 或工具调用。id 只关联本次内存调用，不是持久 commandId；进程重启仍按领域任务身份与原生持久事实恢复。

普通代码保存 `const call = task.update(request)` 后用 `await call.result` 取得原生最终结果，订阅或重新等待不重做写入。`call.activity` 是同源 ResourceReader，可 read、peek、组合 observations$ 或交给 useResource；activity$ 是同一次调用的值流。读取活动只取得当下事实，不等待写入完成；running 仅表示已经调用原生方法，不证明通过了 FIFO、CAS 或持久化，原生方法拒绝是 ready 的 failed 调用事实。活动按本次 result 有限结算，独立于 SDK 与能力的观察 Scope；SDK 或能力退出期间及结束后，晚订阅仍取得同一次调用的实际终态，不把来源释放改判为写入取消。

需要可取消的回执等待时使用 `call.wait({ signal })`。方法的 options.signal 保留执行前取消并作为默认 wait 信号；预取消不进入原生事务，调用者取消只结束该次 wait，SDK 退出结束所属 wait 与会话资源观察，原调用的 result 与有限活动继续结算。已经开始的原生创建、CAS 或删除仍由原生 owner 负责。保留原 call.result 可以在退出等待后继续取得成功或失败，不把取消误认为回滚，也不据此无条件重试；需要核对权威事实时仍使用原任务身份与实际 revision。

能力实例的 `dispose()` 撤销 SDK 资源观察，等待本实例已经开始的实际写入及安装范围真正退出，即使外层 Context 已开始卸载或同步清理中再次调用退出。原生写入拒绝保留在原 call.result 与 failed 活动中，不冒充清理失败；观察或安装清理失败在实际原生资源静止后报告。实例自行安装的服务由原生 owner 执行 drain；provided 服务的卸载仍由外部宿主决定，框架不延长它的寿命，服务卸载导致的调用拒绝同样保留原 result/failed 事实。`forSession(source)` 返回的会话 SDK 只释放自己的资源观察和 call.wait，不借此停止原生执行或覆盖原调用结果及活动终态。

必需的原生服务由正式 Fiber inject 持有依赖。provider 卸载会撤销能力实例及其借出的资源，并释放该实例自行安装的范围；之后安装同名 provider 不复活旧能力或重定向旧 SDK，需要创建并安装新实例。只读资源通过 error Observation 通知仍活跃的外部消费者；值流随自身 owner 正常完成。组件需要在 provider 退出后呈现失效时，应按 conversation 身份取得并保留原资源，观察该对象，不在每次重渲染时重新请求已退出能力。

各子入口的原生依赖是版本固定为 `0.2.1-rc.3` 的 optional peers。应用选择某个域时，显式安装该行依赖；import 不等于安装原生服务，服务装配仍经 installNative 或已有宿主完成。默认后台和原生 UI 消费不因这些入口自动装入所有原生集成。

| 子入口 | 需安装的 `@deepseek-ai/` 包 |
| --- | --- |
| jobs | dsh-jobs、dsh-jobs-local、dsh-tool-jobs |
| mcp | dsh-mcp-client、dsh-mcp-resources |
| approval | dsh-user-approval |
| time | dsh-time-context |
| schedule | dsh-schedule |
| team | dsh-experimental-agent-team |
| files | dsh-api-workspace-files |

已有 Cordis 宿主使用 `mountApplication(ctx, factory, options)`；需真实安装 agents、sessions、persistence/query/projections、workspaceRegistry、goals/tools/systemPrompt/loader、preset、skills/fs/cwd、userQuestions、attachments/fileUploads 等正式服务。不要只提供同名空对象满足 inject，也不要另装一套不同 home 的附件服务。

`RuntimeOptions.installNative` 在任何 Agent 创建前安装可选能力；`nativeCapabilities(...selected)` 组合正式能力安装。Jobs、MCP、Approval、Time、Files 等按包公开 subpath 和实际插件能力选择，原生 owner 继续拥有状态、权限、取消和资源寿命。使用当前安装 subpath 的 DTS，不为了统一外形给所有工具再包 Action。

组件用 `<NativeTools names={actualInstalledNames} enabled={condition} admit={policy} />` 选择原生已安装工具，保持原执行/guard/PTC语义。它不安装后端，enabled 不等于业务授权。

Jobs、Schedule、Team、Files、Approval、Clock 用 `capability.forSession(conversation)` 消费 Program 收到的原 conversation，也可消费 Host 签发的本地 SDK 句柄。不得拼接 `{ id }` 或复制 identity 对象代替原对象。取得会话能力不建立观察，首次订阅才连接；业务 hook 可以用 `useObservable(() => jobs.list$, [jobs])` 读同一流。启动 Job 或写入仍是显式命令，须在真实会话 owner 已经活跃时调用，不在 render 中执行。

同一原来源共享 SDK 实例，Program 来源和外部句柄分别拥有观察寿命。Jobs、Schedule、Team 会话与固定任务的 `state` 是可直接传给 `useResource` 的只读资源，Job 的 `output` 独立表示非消费输出切面。`state.read(signal)`、peek、subscribe、values$、observations$ 共用现有资源机制；原 state$/list$/catalog$/projection$ 是相应值流，不能用它们推断旧值仍有效。最后一个资源消费者退出会撤销查询并撤下旧 ready，组件退订不 dispose 会话 SDK，也不停止已受理工作。SDK dispose、Program 失败或来源退出释放其会话资源观察，不杀死 Job、不撤回提醒、不取消 Team 成员；独立的有限调用活动仍沿原 result 结算。Program 来源不自动取得人工、ToolOccurrence 或 Goal 权限。

Team 的 `roster` 是原生 listMembers 的实时成员资源，`roster$` 直接使用它的 values$；保留原生 running、inactive、provisioning、failed 状态，不把持久 `state`、projection$ 或 members$ 的成员阶段当成当前 turn 运行态。普通函数用 roster.read(signal)、peek 或 listMembers 读取，RxJS 从 roster.observations$ 派生并保留 pending/error，hooks 用 useResource(roster) 消费同源事实；可直接与任务板、Subagents 目录或其他资源 combineResources。构造不读取，首次观察连接真实成员与 Agent 状态通知，最后消费者退出释放观察。冷会话保持 pending，实际 Agent 恢复后重新读取，读取不 resume、创建成员或启动模型。

Jobs 的 jobs.job(id)、Schedule 与 Team 的 task(id) 都同步返回固定目标，同一来源与 id 复用对象。构造不读原生注册表、catalog 或投影，也不授予存在性与修改权限；可立即把它们的 state/output 交给 combineResources、useResource、Context 或资源 View。读取、历史和修改才分别进入原生存在性、所属会话、成员及 CAS 核验。Workspace、Conversation、值、资源和反应句柄同样不暴露所属 Scope；借用对象不能用于销毁共享 owner。

Schedule 与 Team 的变化先撤下旧 ready 再读取新切面，读错误保留为资源 error，后续原生通知可以恢复，不使用无条件 retry。组合不同能力时直接用 combineResources/mapResource 组合 observations$，将同一观察交给 hooks、Context 和资源 View；不要把查询 pending/error 当成 Job 执行或任务业务状态。需要重新查询权威服务时，使用原 list/catalog/readProjection/任务 read 等显式入口。

来源仍有效时，依赖原生切面或日志的长期观察会跟随实际 Agent 的关闭和恢复：在途旧读取取消，恢复后读取当前历史，日志不重复交付。关闭原生 Agent 不等于退出仍持有的 SDK 观察；读取本身不 resume 或启动模型。不要为这一转换维护第二份 Agent 状态或用无条件 retry 掩盖真正读取错误。

Team 任务可用 `const task = team.forSession(source).task(id)` 取得。`task.state$` 供 RxJS 和 `useObservable` 使用，`task.read({ signal })` 读取同一原生投影；显式 `task.update({ expectedRevision, action, ... })` 由原生 CAS 决定是否受理，不重复传 taskId，也不能向请求中塞入 taskId 更换目标。对象身份属于原来源与任务，不按当前界面选择重定向。缺席或删除后流和 read 返回 undefined；需要原生墓碑时用活跃成员的 getTask。Schedule 的 task(id) 同样允许先取得未知目标，再显式 read 查询；Jobs 对未知或外来目标的读取和控制则由原生边界拒绝。冷 Lead 可以读取与取得对象，修改仍要求真实活跃成员，不会为此自动 resume、spawn 或 send。

Files 复用已安装的 WorkspaceFiles、fs、sandboxPolicy 和 Typert，不自动安装文件服务或改变沙箱政策。`const page = files.forSession(source).text(path, { offset, limit })` 返回正式 Resource 实例；metadata(path) 与 directory(path) 分别提供元数据和目录资源。同一原始路径与窗口取得同一对象，read、peek、values$、observations$、invalidate 与 `useResource(page)` 共用它。构造不做 I/O；首次观察等待 watch ready，之后变化取消旧读、重新取权威切面；同一 SDK 的文本页和元数据共享该路径的 watch，最后一个查询退出后释放它。读取失败保留为资源 error，后续文件变化可恢复；watch 初始化失败明确进入 error，不偷偷轮询或降级。SDK dispose 等待实际 unwatch，Agent close 不取消仍有效来源的文件观察。readBytes 继续每次返回独立缓冲区。产物需要公开时，用 mapResource 选择领域字段交给资源 View，沿现有本地或远程 Surface 消费，不给 UI 复制文件监听。

原生FS可合并短窗口的失效通知；资源读取当前切面，通知不承担每次写入的持久事件含义。业务已知写入成功时可显式 invalidate，内容版本仍以实际读取的 version 为准。

Schedule 创建与 Team spawn/send 尚无 Program 来源准入接点。它们与已支持的提醒观察/更新、任务板创建/CAS、等待和 interrupt 分开；不能因为插件可安装，就声称 UI 或服务创建链已经贯通。

运行时业务 Skill 使用 `@hanger-source/dsh-reactive-dsh/skills` 的 `createNativeSkills({ service: 'provided' })`，由 installNative 或 nativeCapabilities 安装读取接点；宿主必须已有正式 skills Registry。`const skills = capability.forSession(source)` 后，`skills.catalog` 是目录资源，`skills.definition(name)` 是固定名称的正文资源，同一来源和名称复用对象。read(signal)、peek、values$、observations$、invalidate 与 useResource 消费同一正式 Resource，构造不读取；最后消费者退出撤销读取与原生监听。需要重试不完整发现或主动重取正文时显式 invalidate，不轮询或无条件 retry。

目录保留原生 `complete`、modelInvocable、userInvocable 与资源基址，不提供 provider locator；发现和读取不授予调用权限。未找到正文返回 ready(undefined)，真正读取失败保留 error，之后 skills/change 可恢复。Agent 关闭时资源保持 pending，显式恢复后按真实新 Agent 层级和该会话工作目录重读；读取不恢复 Agent、不启动模型。SDK dispose 只释放观察，provider 与 Skill 声明仍归原生注册者或 Program。这与独立 `@hanger-source/dsh-reactive-skills` 的开发指导产品分开。

原生工具目录使用 `@hanger-source/dsh-reactive-dsh/tools` 的 `createNativeTools({ service: 'provided' })`，沿相同安装与 exact source 规则取得 `tools.forSession(source).catalog`。它是可 read、peek、invalidate、订阅和交给 useResource 的正式资源，也可以与 Skill 目录、正文和业务资源一起 combineResources。首次观察连接 tools/change；真实注册、撤销、restriction 和 Agent 代际变化先撤下旧 ready。冷会话保持 pending，读取不恢复 Agent、不启动模型；最后消费者或 SDK 退出只释放观察，不撤工具或 restriction。

目录由原生 `schemas(agent)` 生成，保留真实继承、遮蔽和限制，且只含模型 schema 数据，不外泄 execute、presentResult 等闭包。它表达注册表当前视图；Program 的 enabled/admit 与模型请求前的 prepare 仍属于执行边界，目录读取不主动 prepare，也不授予调用权。PTC 模式仍提供完整可见工具目录，模型直接调用折叠到 run_code 由原生执行管线决定。业务操作继续使用已有 Action/Invocation 或原生工具入口，不通过目录对象绕过 guard。

Approval 的 `capability.forSession(source).policy` 提供 ResourceReader，read(signal)、peek、subscribe、values$、observations$ 与 useResource 使用同一份政策观察；policy$ 直接是其 values$。首次观察读取正式会话切面，相关 approval/policy 日志和真实 Agent 代际变化先撤下旧 ready 再重读；读取错误保留为 error，后续正式通知可恢复。冷会话可以读取持久政策，不恢复 Agent；没有会话 override 时使用已确认切面对应的部署默认政策。

政策值不是一次调用的 grant。setPolicy 仍要求真实活跃 Agent，由原生服务追加日志；SDK 退出只释放观察，不改变政策。audit$ 与 readAudit 保留原生 asked/decided/policy 事件的顺序与身份，历史 asked 不代表仍可批准。业务问答继续与原生权限批准分开，读取、组合或展示政策不会触发 answerer。Clock readings$ 保留原生日志的实际采样事件，不根据政策或工具资源合成批准或时间事件。

Jobs 与 MCP 实例的 `toolNames`、MCP 的 `resourceToolNames` 是 ResourceReader，构造不连接；同一资源支持 read(signal)、peek、subscribe、observations$、values$ 与 useResource。相应 toolNames$、resourceToolNames$ 直接等于该资源的 values$，不建立第二套状态。工具名来自实际注册表并随 tools/change 更新，未选择工具时为 ready 的空数组；MCP 同步后反映当前连接代际，连接状态和调用权不能仅凭名称推断。最后消费者退出释放查询，随后读取重新取得当前快照；能力 dispose 结束旧观察、拒绝旧读取并释放原生安装。Job 执行、MCP 连接及 guard 仍属于各自原生 owner，读取与组件退出不停止业务工作。

将资源交给 `useResource` 后，按当前有效状态声明，例如 `NativeTools({ names: observed.state.status === 'ready' ? observed.state.value : [] })`。显式空数组贡献零个工具声明，可表达 pending/error 或实际空集合；名称、重复项及准入参数仍严格校验，工具读取不代替调用时的授权。

`NativeTools.names` 使用原生注册名，保留大小写与连字符，例如 MCP 的 `mcp__Inventory-North__Stock-Count`。不要为通过声明校验而自行小写、改名或重新构造 MCP 名称；原生 client 持有服务器与原工具的精确映射，名称资源直接提供其当前公开身份。

## 子程序沿原生 owner

`createRuntime(assembly => ...)` 的 assembly 提供 `implementDelegation(action, { program, mode:'spawn'|'fork', childInput, objective, authorize })`。返回普通 ActionImplementation，继续 useAction/Offer/Operation/Follow；不用新增 Delegate 组件或子队列。

子 Program 独立输入和 Surface；父 cause 保每条 sourceSessionId/raw scope。authorize 收真实 parent、executor、requester、causes 和 childInput，不把 service actor 或子初始 user source 猜作原 human。fork 合法继承历史，不继承父动态现场/当前 Offer；spawn 使用独立原生子程序配置。完成和反馈沿 NativeChildWork Operation，不把普通 turn 结束当正式业务完成。

Scope.dispose、SDK release、Runtime.dispose、业务 cancel 不是同一命令。显式释放自己拥有的 Runtime/能力；借入会话句柄或观察退出不能取消原业务。正常 host close 会处理未消费输入，不能把正常关闭冒充 crash 恢复试验。
