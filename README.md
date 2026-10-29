# dsh-reactive

服务于多工作区、多会话的对话式 harness 的响应式编程框架。业务程序用可组合的组件声明当前现场、可用行为、Skill 和公开出口，用标准 RxJS 组织随时间变化的事实与事件；框架把声明持续落实到 dsh 的原生会话和执行边界。

组件运行在服务端或 Agent 宿主，渲染目标是对话环境。页面、CLI 和后台服务消费公开状态与行为，不拥有模型循环或已受理业务操作。

框架处于 0.x 建设阶段，公共编程模型与生命周期合同仍在收敛。API 与组件主链、SDK、原生接入按公共合同和版本记录演进；公开能力的限制以本文和分域文档为准，聚焦试验不代表完整框架已经验收。

Action、Context、Follow、NativeTools 与 Skill 可共享业务资格；具体可见性与调用边界见[共同资格迁移](docs/migration-eligibility.md)。

跨作用域派生、Reaction 停止后的真实结算及异步准入寿命见[响应式读取与执行结算](docs/migration-reactive-lifetimes.md)。

工作区资源共享、原生控制与浏览器合同入口见[原生资源与客户端边界](docs/migration-native-resource-boundaries.md)。

会话成员移除、读取资格和远程请求身份见[会话资格迁移](docs/migration-conversation-access.md)。

Program 派生资源、活动读侧及局部 Surface 调用查回见[Program 资源与调用回执](docs/migration-program-resources.md)。

Action、原生写入与上传共用 Invocation.wait({ signal })；调用者准入、单次结果等待和真实执行取消的边界见[调用回执等待](docs/migration-invocation-waits.md)。

调整公共合同时，按实际安装版本查看[共享 Program 合同迁移](docs/migration-shared-program-contracts.md)、[模块 SDK 迁移](docs/migration-sdk-modules.md)与[参与端口迁移](docs/migration-participant-ports.md)，对齐合同身份、局部模块、人工控制与服务来源。异步装配及资源寿命的变化见 [SDK 作用域迁移](docs/migration-sdk-scopes.md)、[资源有效性迁移](docs/migration-resource-ownership.md)及[来源完成与有效性迁移](docs/migration-resource-completion.md)。

借用对象的生命周期权限、内核工作区装配及原生任务的同步构造见[借用能力迁移](docs/migration-borrowed-capabilities.md)。

源码位于 [hanger-source/dsh-reactive](https://github.com/hanger-source/dsh-reactive)，由 hanger 维护。六个公共包统一使用 `@hanger-source/dsh-reactive*` 名称与同一版本；正式发行通过 GitHub Actions 的 Trusted Publishing 完成，源码版本和 GitHub 提交不表示包已在 npm 发布。版本管理、候选验收和发布方式见[发行说明](docs/releasing.md)。

`@hanger-source/dsh-reactive` 提供纯 Program、Surface、Action、Operation、输入与观察合同，供业务模块和本地、远程消费者共享。后台 React 程序从 `@hanger-source/dsh-reactive/program` 导入 `implementProgram`、声明组件与 hooks；纯合同和远程客户端不加载这个入口。原生 Runtime、模型与宿主装配位于独立包 `@hanger-source/dsh-reactive-dsh`，按宿主的实际能力选择依赖。

`@hanger-source/dsh-reactive-client` 提供独立客户端与远程数据合同；`@hanger-source/dsh-reactive-remote` 负责认证 Gateway 宿主。选择扩展 dsh 原生界面时安装 `@hanger-source/dsh-reactive-native-ui`。纯资源程序、后台服务和 CLI 无需安装原生 UI。

Program、本地与远程 SDK 通过共同 `session.files` 读取会话文件：text、metadata、directory 提供可组合资源，readText、readBytes、stat、list 与 watch 提供显式查询和观察。Host 借入已安装的文件能力，原生 UI 按需消费同一资源；范围、权限、重连与实际清理边界见[编程模型](docs/programming-model.md#消费会话文件)和[外部参与](docs/transport.md#同一份-program-合同)。

原生扩展用 `retainNativeSession(Program, clientContext, sessionId)` 取得拥有原生代际引用、独立 SDK 和 UI 观察的对象，使用 `reference.session` 并在退出时 `await reference.release()`。共享客户端装配、目录和 Host 工作仍由原 owner 管理；已经持有原生 binding 与 SDK 的消费者继续用同步 `bindNativeSession` 借用它们。

远程 SDK 工厂与 retainNativeSession 要求 `defineProgram({ name, version, input, surface })` 签发的共享 Program；合同 version 默认 1，与 npm 版本分开。宿主在创建、补齐创建、恢复及会话请求前核验目标合同，SDK 复核返回身份。纯本地 Program 可以保持无名，本地实际定义仍核 exact 对象；通用会话与模块消费者不因此要求共享名称。

`@hanger-source/dsh-reactive-skills` 提供同版本的应用开发 Skill 与按需 references。安装后运行 `dsh-reactive-skill` 取得该版本入口的绝对路径，编码 Agent 按需读取；它不安装运行时业务 Skill，也不修改全局 Agent 配置。

运行时业务 Skill 从 `@hanger-source/dsh-reactive-dsh/skills` 的 `createNativeSkills({ service: 'provided' })` 接入宿主原生 Registry 与工作目录服务。安装后用 `forSession(source).catalog` 和 `definition(name)` 取得同源资源，可 read、peek、invalidate、组合 observations$ 或交给 useResource；目录保留原生 complete 与 invocation policy，正文不存在为 undefined。目录和正文按该会话真实当前工作目录解析；原生变更、同会话目录变化或 Agent 代际变化撤下旧事实。冷会话保持 pending，读取不初始化目录或启动模型；provider、Skill 声明与执行准入继续由原 owner 管理。

`@hanger-source/dsh-reactive-dsh/tools` 的 `createNativeTools({ service: 'provided' })` 提供同样可组合的 `forSession(source).catalog`，读取真实 Agent 作用域下的原生 Tool schema。注册、撤销与 restriction 变化使资源失效；快照不含执行或展示函数。目录可以与 Skill、Files 和业务资源直接组合；具体调用仍由 NativeTools 声明、原生 guard 与执行边界决定，PTC 模式的完整可见目录和模型直调入口保持各自语义。

Approval 的 `forSession(source).policy` 是同源政策资源，可与工具、业务资源及 hooks 组合；policy$ 是它的值流，审计继续使用原生有序事件和 readAudit。读取和冷会话恢复观察不产生批准请求或 grant，政策变更继续显式调用 setPolicy 并由原生服务持久化。

`@hanger-source/dsh-reactive-dsh/subagents` 的 `createNativeSubagents({ service: 'provided' })` 接入原生子 Agent 发现。`forSession(source).catalog` 与 `descendants` 是可组合资源，保留 one-shot、continuable、external 目录身份和正式递归查询的分支诊断；`readDescendants({ signal })` 显式取得独立查询结果。冷读不加载 Agent、不创建 Goal，也不投递消息。`interrupt(targetId, { signal })` 使用原来源真实 Agent 的 ancestor 权限中断选定后代，冷来源拒绝控制；未驻留目标沿用原生无操作语义，返回不表示执行结算。带独立 Goal 的子程序执行继续使用 `assembly.implementDelegation` 与 `NativeChildWork`，不把普通目录项转换为业务 Operation。

Jobs 与 MCP 的 `toolNames`、MCP 的 `resourceToolNames` 是实际注册工具名的 ResourceReader，可 read、peek、组合 observations$ 或交给 useResource；对应 `$` 属性直接使用同一资源的 values$。查询按原生 tools/change 更新，未选择的工具为空集合；`NativeTools({ names: [] })` 贡献零个声明，可表达准备中或工具全部撤下。退出观察不停止 Job 或断开 MCP。能力 dispose 才释放其安装范围，旧资源不再提供可用快照。工具名表达注册事实，调用仍经过 NativeTools 和原生 guard。

Job 的 state/output 及 wait(timeoutMs, { signal }) 是非消费观察，等待不抑制模型完成通知。显式 collect({ signal }) 才推进原生模型输出 cursor，并按原生一次性语义取得首个终态 result；它会影响其他消费端，不能当作可重复资源读取。remove() 只显式删除终态记录，不自动收取结果或补发通知。

后台 Program renderer 使用 React `19.3.1` 与 react-reconciler `0.34.0`。原生 UI 扩展把呈现函数交给 dsh 自己的 Slot renderer，不创建第二个 React root，也不安装后台 reconciler；仓外原生 UI 消费与浏览器试验使用独立的 React 与 ReactDOM `18.3.1`。两种运行环境分别安装、分别编译。

`session.attachments.readImage(attachmentId, { signal })` 按原会话读取已经登记的图片，返回正式引用与本次独占的字节。它借用原生 Controller 的引用授权与冷读取，不启动模型；与工作区路径文件读取各有范围。历史文件附件尚缺 rc.3 的公开会话授权读取入口，见[图片附件读取](docs/programming-model.md#读取会话图片附件)。

## 编程模型

模块通过 `/program` 的 `useWorkspace()` 和 `useProgramSession()` 直接借用所属环境，业务目标与依赖仍显式传入。需要类型化输入和提交能力时使用 `useProgramSession(Program)`，框架先核验实际 Program 合同；普通提交保持原服务参与身份。

本地应用用 `runtime.application.workspace(id)`、组件用 `props.workspace` 取得同一工作区编程对象。`workspace.resource(implementation, params)` 与 `workspace.reaction(implementation, params)` 按正式实现与参数复用实例；局部业务直接使用 `workspace.resource({ value, read })`、`workspace.reaction({ event, result, observe, operator, execute })`，每次构造独立实例，显式保留返回对象即可跨 Environment、Session 共享。构造不读取，反应显式启动。不同工作区隔离；登记删除或宿主退出结束工作区 owner，撤销观察及排队关系，已受理业务继续由原 owner 完成。

Workspace、Conversation、值、资源与反应绑定是借给消费者的编程对象，不公开所属 Scope 的销毁权。消费者退出自己的订阅、读取等待或 ReactionRun；真正创建 Scope 的 owner 负责释放共享关系，不能通过一个借用对象结束其他消费者的生命周期。

- `defineResource`、`implementResource`、`bindResource` 定义事实来源并按参数取得稳定实例；普通函数、业务 hook 与公开视图消费同一观察。多个消费者共享来源，失效取消旧读取，最后观察者退出后撤掉旧的有效现场。
- `defineReaction`、`implementReaction`、`bindReaction` 定义持续感知与动作之间的关系。实例显式 `start()`，`state$`、`events$` 与 `settled` 观察同一次运行；条件、去重和并发直接使用 RxJS。停止反应释放关系，已受理的业务仍由原 owner 结算。
- 局部关系使用 `createResource(scope, options)`、`createReaction(scope, options)` 直接取得同一类运行实例；资源构造不读取，反应构造不执行。组件用 `useResource(implementation, params)` 从程序 owner 取得共享绑定，自定义业务 hook 继续组合标准观察流。
- `useResourceInstance(implementation, params)` 与 `useReaction(implementation, params)` 取得程序拥有的完整实例；普通函数、Promise、流与 hooks 共用同一对象。反应仍须显式 `start()`，程序失败或退出停止查询、感知和排队关系，已受理业务保留原 owner。
- 局部业务 hook 使用 `useResourceInstance(() => ({ value, read }), dependencies)` 或 `useReaction(() => ({ event, result, observe, operator, execute }), dependencies)` 直接构造同一类实例，无需额外参数合同或 Scope。依赖决定本组件的身份，目标变化取得新实例，旧对象保留原目标；factory 不读取、不启动反应。
- `host.service(actorId).open(sessionId, Program)` 提供在取得时固定身份的服务端口。同一服务视图也可创建、恢复会话及上传文件，与人工端共享准入链，持久保留各自来源；服务事件不继承人的输入或 Goal 授权。
- `defineAction` 定义共享业务合同，`implementAction` 接入宿主执行，`useAction` 绑定明确目标。显式调用产生一次 Invocation；关联的 Operation 可以独立定位和持续观察。
- `defineProgram` 定义可共享的输入与公开 Surface 合同，`implementProgram` 将宿主组件接到同一合同；`defineEnvironment` 组合程序、指令、Skill 与 Preset。`createWorkspace` 注册明确目录，创建会话时同时选择环境与工作区。
- `composeSurface` 从领域模块的局部合同生成类型化实例路径、根 Surface 和挂载点；`Module` 统一隔离 Context、Offer、Follow 名称并映射局部 Expose。`selectSurface` 按合同选择公开能力，模块不需要接收 names 或应用自定义 Outlet。
- `collectionSurface` 声明动态模块集合。根合同保持稳定，`mount.instance(key)` 挂载完整业务模块，嵌套 Module 自动映射；本地及远程 `conversation.collection(name).instance(key)` 用同一局部合同读、调用和观察，`instances$` 提供实际成员。
- `Context`、`Offer`、`Follow`、`Skill`、`HumanInput`、`Expose` 描述当前现场、业务能力、操作参与、方法、人工参与与公开出口。组件更新提交完整声明帧，保持已有绑定与调用的身份。
- `useObservable` 读取标准值流，`useShared` 共享查询，`useRelation` 建立长期关系；已有 Observable 可直接传入，构造管道时使用 factory 与依赖。`useBeforeStep` 表达原生步骤前必须完成的检查。
- `ResourceSource` 是 Observation 流或提供同一 `observations$` 的对象。资源实例、SDK 资源、`useResource` 的返回对象与派生流可直接交给 `useResource`、`shareResource`、`Context source` 和 `resourceView` 的 Expose 槽，不机械拆流或另建 owner。普通值流仍由 `view` 与 `useObservable` 消费，不按业务字段猜资源状态。
- `combineResources` 组合多路资源，任一依赖失效立即撤下派生 ready；`mapResource` 只投影当前有效事实，保留 pending/error。业务关系使用标准 RxJS 管道，并共同影响模型上下文、行为条件和公开观察。
- `switchResource` 让下游查询跟随当前依赖，失效时退订旧读取；`Context required={false}` 显式撤下未就绪的模块现场，其他有效模块可以继续参与，公开资源仍保留真实失效或错误。
- `switchResource(query, { dependencies })` 直接选择影响查询的业务字段；连续有效依赖相同时保留查询，失效后即使依赖相同也重新读取。业务无需编码 JSON 或编写 Observation 比较器。
- 同一 Action 可以通过 AI 工具、普通代码和 Gateway SDK 调用，入口保留各自真实身份；同一业务来源同时更新上下文、能力条件和外部观察。
- SDK 通过 `resolveAction(name, expectedScope)` 捕获具体业务绑定；切换目标、撤下或重挂后，旧句柄不会悄悄调用新对象。调用前与异步准入后均重新核验，已执行的业务沿原 owner 结算。
- 输入以 `content` 保留文字、图片和上传文件的顺序，业务 `intent` 与 `delivery: 'followup' | 'steer' | 'inject'` 分开；投递复用原生 inbox，inject 不唤醒模型。文件使用同会话上传签发的 receipt，受理回执、模型消费和业务完成分别观察。
- `action.invocations$` 观察这个具体绑定新产生的类型化调用，同一个 Invocation 同时提供结果、活动和 Operation。`invocation.activity` 是可 read、peek、组合和交给 useResource 的同源资源，`activity$` 是其值流；当前事实与等待业务完成的 result 分开。业务关系可以直接组合调用流，无需按工具字符串名筛选会话事件。
- `action.activeInvocations$` 提供本绑定当前在途调用集合；晚挂载的自定义业务 hook 也能继续组合多次调用，各次结果和长期 Operation 保持独立。结束观察或撤下模块不取消已执行业务。
- `NativeTools` 声明宿主已安装的原生能力与可选异步业务政策，保留官方工具身份、执行及 guard。可用性同时落实到原生工具与 PTC SDK，等待政策后再次核验声明，不必为文件、待办等工具重写 Action。
- `actionManifest$` 提供真实工具名、Action、绑定、完整模块路径与公开地址；原生结果及 PTC 结果的 metadata 携带同源结构。业务卡片用 `readActionPresentation` 读取合同结果，无需反解工具 namespace 或模型文本。
- 原生工具执行中的 Action 通过 `call.askHuman(...)` 主动请求人工补充，复用标准提问工具及真实父子调用。可以等待本次答案，或超时返回 pending，由原对话接收迟答；调用体退出会取消并清理未完成提问。普通 SDK 调用不借用原生工具身份。
- Operation 的 `settled` 与类型化 `feedback` 分别表达业务终态和协作节点。`useOperation` 绑定已有命令，`Follow` 提供再次等待的能力；每次参与保留节点边界和本次 Goal 权限，业务不因再次观察而重新执行。Surface.operations 与 SDK 只读入口保留原绑定；人工问答沿 dsh 原生前台等待、超时继续与迟到回答。
- `useOperation` 可以明确绑定 null，业务 hook 不必为尚未出现的命令拆出条件组件。`Offer.follow` 与 `Follow` 的 wait 模式暂缓原工作，background 模式可靠关联结果并允许当前工作继续；只观察使用现有 Operation 流。三种关系都不重新发起原操作。
- `conversation.request(id)` 提供该请求的当前事实与新事件，`progress$` 提供会话事实和当前流式前缀。工具及回复保留实际消费的整组输入；只读观察、退订与重连不会再次提交请求或启动 Agent。
- 宿主通过 `assembly.implementDelegation` 将普通 Action 接到原生 spawn / fork；子程序保留自己的输入、Goal 与 Operation，父工作沿原参与关联接收反馈。

[编程模型](docs/programming-model.md) 展示定义、组合和调用；[原生子程序委托](docs/native-delegation.md) 展示完整模块与宿主装配；[运行语义](docs/runtime-semantics.md) 解释身份、订阅、执行边界与持久续接；[架构与责任](docs/architecture.md) 说明运行责任；[外部参与与观察](docs/transport.md) 说明当前传输入口。

React 负责组件身份和声明提交，RxJS 负责业务流组合，dsh 负责 AI 执行。React commit 之后，宿主仍须等待当前依赖并核对声明修订，才允许原生模型步骤取得现场。

[运营平台模块参考](examples/merchant-operations/README.md) 将准备、知识、接待、商家命令组合到两套 Program：框架组合局部 Surface、隔离实例名称并选择公开能力；跨域资源派生共同约束知识和接待行为，宿主集中接真实业务服务和 Operation owner。参考源码参与严格类型检查并随包交付；它不包含模拟业务后端，正式运营服务的可靠续接接点仍需按其合同贯通。

## 责任

| 责任 | 拥有者 |
| --- | --- |
| 业务事实、权限、副作用、操作生命周期和正式完成条件 | 应用业务服务 |
| 流变换、串并行、切换、调度、错误、终止与共享 | RxJS |
| 组件身份、Hooks、动态子树与声明提交 | React 与固定版本的 react-reconciler |
| 原生会话、inbox、模型循环、工具、Goal、持久化和投影 | dsh |
| 声明与原生执行边界的接合、输入及调用关联、公开合同与观察 | dsh-reactive |

可靠后台任务使用应用已有的事务与任务设施。持久续接沿原会话、业务操作、输入和 Goal 修订保存，内存中的订阅与 Promise 不承担跨重启执行。

多等待按原 Goal 的精确修订共同约束自动续接，完成事实与模型消费分别结算；暂停、取消及进程中断恢复使用原生 inbox 与持久投影。远程宿主与 SDK 分别由 `@hanger-source/dsh-reactive-remote` 和 `@hanger-source/dsh-reactive-client` 提供，复用 Gateway/Typert 和原生快照、日志恢复。长会话与真实运营服务的整体消费仍需连续验证。

`editQueuedInput` 对同一操作者尚未消费的请求重新准入正文，保留目标、来源、顺序及首次提交身份；持久准备不会提前改写原输入，编辑也不唤醒模型。自动 Follow 在框架确定未进入业务执行时持久结案；已经执行但受理未知时仍等待 owner 定位。

固定 dsh `0.2.1-rc.3` 的 SessionController 尚无公开的创建、恢复、prompt 与队列编辑宿主接点。当前原生 UI 直接操作 Controller 不能宣称已接通 Program 受理；正式集成需要上游提供这些执行前接点，框架不替换私有控制器或放宽根会话和输入身份核验。本地与 Gateway SDK 使用框架的统一入口。

独立 `createRuntime` 一起安装官方重试执行器、TokenMeter、自动压缩和图片卸载。模型配置中的 `retryPolicy` 由原生请求管线执行；`compaction` 直接使用原生配置，容量来自实际模型路由。小容量模型应明确配置摘要配额、保留量和 headroom，不能仅调一个压力阈值。`deepseek` 使用正式 Messages 协议；已有 OpenAI、Anthropic 等网关可用 `piAi({ provider, model, profile })`，直接消费官方协议、模型目录和凭据引用合同，密钥经 `apiKeyEnv` 在请求边界解析。

原生子程序委托已接入框架。Team 的具名成员与任务板、Schedule 的持久计时与投递、Jobs 的进程内任务各有独立 owner；它们不能替代业务 Operation，也不默认全部启用。Team 和 Schedule 的完整 Program 准入仍需要明确的子程序绑定和原生投递接点，安装相应工具不代表这些链路已贯通。

`nativeCapabilities(...selected)` 将显式选择的配置实例交给独立或嵌入宿主。Jobs、MCP、Approval 与时区 SDK 使用真实会话句柄和各自的原生 owner；释放观察与停止执行分别表达。[原生能力组合](examples/capabilities/native.tsx)、[资源与失效](examples/capabilities/resources.ts)、[持续反应](examples/capabilities/reactions.ts) 和[原生 UI 扩展](packages/native-ui/examples/actions.ts) 展示这些对象怎样共同使用。UI 与后台示例分别编译，避免把原生客户端和宿主的服务声明合并到一个 TypeScript 程序。

会话能力的 `forSession` 也接受 Program 收到的原 `conversation`，沿同一 Host 身份链核验。首次观察才建立原生读侧连接；Program 失败只释放该来源的观察，已受理的原生工作保留各自 owner。

`/schedule` 提供现有原生提醒的绑定、变更与历史观察、CAS 更新和删除；`/team` 提供真实成员和任务板的观察、任务创建与 CAS、等待及 interrupt；`/files` 在明确提供 WorkspaceFiles 的宿主按原会话目录读取并异步释放文件监听。它们位于 `@hanger-source/dsh-reactive-dsh` 的独立可选入口，未安装的能力不会进入宿主根入口的类型依赖。Schedule 创建与 Team spawn/send 的 Program 来源准入仍受上述原生接点限制。

Team.roster 提供可与任务板共同组合的实时成员资源；持久成员阶段继续来自 Team.state 与 members$。Team 和 Schedule 的写入返回 NativeInvocation，保存 `const call = task.update(request)` 后用 `call.result` 取得完整结果、`call.activity` 观察同一次调用，或用 `call.wait({ signal })` 独立等待回执。释放 SDK 不改写原调用结果。迁移方式见[原生调用与独立等待](docs/migration-native-invocations.md)。

[多工作区周期巡检](examples/fleet-inspection/application.ts) 把共享感知、普通 Action、程序资源、定时反应和异常人工参与组合为完整应用。周期关系直接使用 RxJS `timer`，寿命属于应用进程；普通检查不启动模型，异常才通过固定服务来源进入选定会话。持久提醒仍归原生 Schedule，业务修订与正式结果仍归实际业务服务。

## 开发

使用 Node.js 24.18.0 或更高版本。根目录是私有 npm 工作区，六个发行包位于 `packages`；各包源码、依赖和正式 exports 与发行边界一致。开发和公共 API 变更方式见[贡献说明](CONTRIBUTING.md)。

```sh
npm ci
npm run typecheck
npm run check:boundaries
npm run check:packages
npm test
```

仓内自动化验证覆盖真实 React renderer、标准流组合、dsh 循环、工具及 PTC、Goal、Skill、原生投影、JSONL 持久化、句柄、人工参与和 Gateway 网络观察，其中模型边界使用确定性适配器。真实 provider、仓外安装和正式业务服务分别验收，不由自动化测试通过推定成立。

业务参考覆盖工作区与会话隔离、业务现场变化、双向参与、过程反馈，以及等待后台结果后继续原工作。示例的具体事实与正式完成条件由业务 owner 提供。
