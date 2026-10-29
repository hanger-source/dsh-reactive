# 架构与责任

dsh-reactive 将业务程序接入 dsh 的多工作区、多会话运行环境。React 管理后台组件树及其身份，RxJS 组合事实与事件，dsh 执行模型步骤、原生工具和 Goal。组件提交的完整声明帧把这三者连接起来。

程序运行在服务端或 Agent 宿主。页面、CLI、后台服务与原生会话 UI 消费公开观察并显式参与，页面是否挂载不决定会话程序是否存在。

## 程序定义与组件树

`defineProgram({ input, surface })` 声明纯本地程序合同；跨宿主与远程消费者共享的定义使用 `defineProgram({ name, version, input, surface })`，返回 SharedProgramContract，version 省略时为 1。`implementProgram(program, { Component, modules?, operations? })` 将宿主组件及静态依赖绑定到合同，组件不进入共享定义。`defineEnvironment` 将程序实现、静态指令与静态 Skill 目录组成可复用场景，`defineApplication` 声明环境集合及输入准入。

Program 的 name/version 由发布者声明，与 npm 包版本、Environment 名称和业务目标分开。远程工厂及自行取得 SDK 的原生 UI retain 入口要求真实共享定义；Gateway 在 Program 请求的实际目标上核验身份，创建、补齐创建和恢复先核合同再产生副作用。SDK 也核对返回身份。本地传入实际定义仍核 exact 对象；模块 Surface 和纯本地 Program 无须增加共享名称，通用会话消费者保留 ProgramContract。身份一致不证明 schema、codec 或实现等价，不替代认证和业务准入。

Workspace 由原生 registry 持有目录、稳定身份与成员。`createWorkspace({ directory, title })` 注册已存在的绝对目录，`create(environmentId, { workspaceId })` 显式组合场景和工作区。同一场景可以用于多个工作区，同一工作区也可以运行不同场景。会话自己的持久 binding 决定 application、environmentId 与 workspaceId，宿主交叉核验 Preset、header.cwd 及原生成员；不根据目录猜测场景，也不另存一套环境映射。

根组件取得对话身份、输入及活动流、后续新事件和显式 `submit`，也取得工作区身份与共享来源接点。组件通过标准 React 组合、条件分支与 key 管理业务模块，通过 RxJS 派生查询和事件关系。后台 declaration renderer 不生成 DOM；其输出是 `Context`、`Offer`、`Follow`、`Expose`、`Skill` 与 `HumanInput` 声明。

相同组件类型在相同位置及 key 下保留 React 状态与 hooks 身份。`useAction` 的目标构造函数与依赖决定业务绑定是否复用；Context 名称、Offer 名称、Surface 对象和出口槽名各有明确合同。模块实例的路径来自 `composeSurface` 的合同树，React key 不自动成为原生工具名或公开出口名。

可复用业务模块定义自己的局部 Surface，使用局部 Context、Offer、Follow 与 Expose。应用通过 `composeSurface` 组合模块合同树，再用其签发的挂载点建立 `Module` 边界。框架统一生成稳定的原生名称和根 Surface 的精确槽映射，模块不接收应用自定义 names 或 Outlet，也不导入应用根 Surface。Offer 可以直接使用 Action 合同名，Context 和 Follow 的局部名只须在模块实例内唯一。

组合结果保留各叶合同身份，SDK 使用如 `a.knowledge.facts`、`a.knowledge.initialize` 的精确类型化路径。`selectSurface` 选择模块公开的视图、动作和操作；未选择的参与面不进入该根 Surface，但不改变模型工具声明或业务权限。挂载点有运行时身份，空模块也参与完整帧的归属与重复核验；错误根合同、重复挂载和名称过长均硬失败。原生名字采用无歧义的路径编码，不截断或猜测；组件重排不改变路径，业务操作身份仍来自原 Action 和 owner。

变化中的业务对象集合使用 `collectionSurface(localSurface)`，根合同不随成员改变。`mount.instance(key)` 保留完整业务 key，框架签发独立的原生名称；嵌套 Module 的合同、选择和地址由外层挂载自动重定位。本地与远程 SDK 均使用 `collection(name).instance(key)` 及本地槽名，`instances$` 来自真正提交的挂载成员。移除会撤下入口和所属观察，重挂生成新绑定；已接受执行及业务 Operation 保留原 owner。动态地址只负责寻址，实际调用继续核目标、绑定及参与者权限。

`combineResources` 与 `mapResource` 在标准 Observable 上保留资源有效性。跨域规则可以把准备、知识和接待事实派生成共享协作前提，同一派生同时驱动 Context、Offer 与公开资源；任一依赖 pending/error 都使当前派生失效。组合代次只属于本订阅，不声明跨业务服务事务一致性。共享和释放使用现有 RxJS owner；资源派生不自动重发命令、登记持久等待或取得 Goal 权限。

持续依赖也有方向：知识前提只依赖准备和知识，接待前提再依赖接待现场，不能让下游接待查询反过来阻断知识修复。`switchResource` 在上游失效时退出旧查询，依赖恢复时重新连接 owner。Context 默认必需并进入原生准备等待；显式 `required:false` 的模块现场在 pending/error 时从声明帧撤下，其公开资源保留真实状态、关联能力按条件撤下，其他有效模块可以继续工作。

## 运行责任

| owner | 拥有的责任 |
| --- | --- |
| React | 组件求值、树结构、key、hooks 与 effect 的挂载和清理 |
| RxJS | Observable 的组合、订阅、切换、并发、调度、错误与终止 |
| dsh | 会话、inbox、模型循环、工具管线、PTC、Goal、持久化、查询与投影 |
| 业务应用 | 权威事实、权限、业务修订、副作用、可靠任务及正式完成条件 |
| dsh-reactive | 程序 owner、完整声明帧、原生装配、Surface 出口及输入和调用的因果关联 |

内部 scope、cell 和资源快照用于持有连接、等待就绪及核验装配。业务组合继续使用 React 和 RxJS；模型与工具继续进入 dsh 的执行管线。

工作区、对话、值、资源实例和反应绑定向消费者提供业务能力，不暴露其所属 Scope。退出自己的订阅、取消本次读取或停止一个 ReactionRun，不等于销毁共享 owner。内核装配用 `createWorkspace(scope, identity)` 和 `new Conversation(scope, identity, workspace, program, initial)` 显式借入 Scope；创建者保留释放责任，公开对象不能沿引用取得该权限。普通应用取得现成 Workspace 与会话句柄，无需参与这层装配。

会话域持有冷 Program 图、公开句柄引用与实际 Agent 归属，并签发带成员、合同和代际核验的访问接点。事实投影只读取原生日志和已登记会话，不签发执行身份。每次原生 Program 安装由实际 Agent 下的正式 Fiber 持有，声明所需服务依赖；Context、Tool、Skill、人工参与及 Operation 续接借用同一安装作用域。Program 失败撤销这一份声明安装，关闭 Agent 等待其异步清理；仍有公开句柄的冷程序图和已受理业务保留各自 owner。

打开冷会话先核对预期 Program 合同，再构造程序图。恢复的并发调用等待同一次原生取得和子程序恢复；取得失败只归还本次实际获得的 AgentHandle。已有冷句柄及其他会话继续有效，清理失败交回会话域汇总，不由原生日志代替失败回执。

组件通过 `useScope(setup, dependencies)` 在提交后的 effect 中建立自身关系的子 Scope，卸载或依赖变化时立即撤销关系；异步清理与该 Scope 持有的已开始工作由 `closed` 等待。Scope 的销毁权不随借入对象扩展到 Program 或 Workspace。实例身份、运行归属和观察退出各自独立：Program hooks 持有的实例仍归 Program，Workspace 资源与反应归 Workspace，已受理的 Operation 则归独立业务 owner。

Reaction 的实例 owner 始终保留；`start({ scope })` 可在开始 observe、operator 和 execute 之前，为同一 run 登记附加 owner。任一 owner 退出即停止，双方 `closed` 等待真实 settled，同一 Scope 去重，启动和运行准入检查双方。默认 `start()` 只使用实例 owner。运行停止后若仍在 draining，同实例不能重新 start；组件代际需要独立运行时创建独立实例。execute 不得等待自己的 owner.closed，以免形成自等。运行失败通过 failed 状态和 resolve 的 settled 表达，不改变 Scope.closed 的失败语义。

Workspace 是独立的本地编程 owner。应用通过 application.workspace(id)、组件通过 props.workspace 取得同一对象，以 workspace.resource(implementation, params) 和 workspace.reaction(implementation, params) 复用现有绑定身份；局部选项则每次构造独立实例，返回对象可以显式组合和共享。两种入口都由实际工作区 Scope 持有，跨 Environment 和 Session 使用，同一实现与目标在不同工作区隔离。构造不读取、不执行，反应显式 start。原生登记成功删除或宿主退出时结束该 owner，撤销资源有效性及持续关系，不取消已受理业务。core/workspaces 拥有该编程合同与内核装配，原生登记、成员和持久化继续归 dsh。

业务自定义 hook 可以直接返回资源、Action 和标准 Observable，供下一层业务 hook 继续派生。`invocations$` 表达新发生的调用，`activeInvocations$` 表达本绑定当前在途调用集合；单次 `Invocation.activity$`、`result` 和独立 `operation` 不合并为一个 loading 或完成标记。晚观察者读取当前集合不会重发命令，内存集合也不承担跨重启任务恢复。

hook 自建查询的共享连接和终止缓存受 Program 寿命约束。`useShared` 与 factory 形式的 `useResource` 复用 RxJS 的同一重放值，不另存一份业务状态；Program 失败或退出后，之前捕获的流直接完成，不重放旧事实或重新打开查询。借入的资源实例仍由原 owner 持有，hooks 不改变其身份或销毁共享事实。

受管理资源的有效性由资源 owner 决定。owner 撤销时，仍在观察的 `observations$` 先交付以原取消原因构成的 error，再完成；迟到观察只取得撤销，不回放旧值或重连。借入资源的 hook、Context 和公开资源消费这份同源失效；普通有限 Observable 的完成仍保留其最后值。父 Scope 撤销同步传播到全部子 Scope，实际清理仍由所属 Scope 树汇总。

原生能力将其必需服务声明为正式 Fiber 的 inject 依赖。提供者卸载结束这一能力实例的读取和安装范围；更换 provider 必须建立新的能力实例，旧 SDK 不随同名服务重定向。观察退出不取得 Job、Operation 或已受理调用的执行所有权，原生服务自身的 drain 继续由其原 owner 负责。

`NativeTools` 声明宿主已经安装的原生能力及业务政策。框架不重新注册同名工具，仍由原生实现和所有 guard 执行；异步政策返回后核验当前声明，模型可见性与生成的 PTC SDK 使用同一原生限制。业务文件系统的只读材料、草稿范围和路径准入继续由真实宿主实施。

`actionManifest$` 提供工具名与 Action 名称、合同版本、模块完整路径、绑定和公开槽的关联；原生与 PTC 的持久结果 metadata 提供同源 canonical 输出。原生 UI slot 可以按实际工具名挂业务呈现，通过 `readActionPresentation` 读取相应名称与版本的合同结果，不反解命名编码或解析模型正文。它不定义业务卡片组件或视觉布局。

Action 定义拥有显式 `version`，省略时建立初始版本 1，必须为正安全整数。本地继续核验真实定义对象；远程解析与执行前核对发布者声明的名称和版本，Invocation 回执保存受理时的合同身份。名称和版本不能证明两个 schema 或函数等价，也不是权限或幂等键；合同发布者负责为输入、目标、输出或业务语义的不兼容变化增加版本。原生展示和委托恢复同样按名称与版本寻找对应消费者，不以当前同名合同解释历史结果。

固定 dsh `0.2.1-rc.3` 的 SessionController 内部直接创建私有 Agent、命令和历史控制器，并持有文件上传 resolver，尚无统一公开宿主接点。原生创建、历史 promote、预上传、composer 和队列 mutation 需要上游在执行前委托同一 Program Host；post-commit 观察无法替代准入。当前本地与 Gateway SDK 已使用框架受理入口，原生 UI 直接调用 Controller 的完整接入仍待该上游能力，不通过私有替换或放宽身份核验完成。

```mermaid
flowchart LR
    I[可信输入] --> D[dsh 原生受理与实际提取]
    D --> F[对话事实与新事件]
    B[业务事实与通知] --> R[RxJS 来源与派生]
    F --> R
    R --> C[React 组件树]
    C --> P[完整声明帧]
    P --> N[原生 prepare 与装配]
    N --> M[dsh 模型与工具]
    M --> A[同一 Action 绑定]
    A --> B
    P --> S[Surface 视图与动作出口]
    S --> O[页面 / CLI / 服务]
    O --> I
```

组件求值与声明构造保持纯投影。它们不注册工具、不连接查询、不执行 Action。查询连接由提交后的观察及出口 owner 持有；长期事件关系由 `useRelation` 的 effect 持有。明确的 Action 调用才开始业务执行。

子 Agent 的发现与执行保留不同责任。Subagents 的 catalog 与 descendants 是现有读取 Scope 下的资源，分别直接消费原生父目录与 listDescendants，one-shot、continuable、external、驻留 activity 及分支诊断使用原生类型，冷读不加载 Agent。interrupt 只把来源的 exact live Agent 交给正式 ancestor 权限核验，显式中断选定分支；撤销观察不取得执行销毁权。NativeChildWork 只表达带独立 Goal 的子程序委托，Team 继续拥有具名成员及任务板；三者不合并为泛化任务状态，也不因观察目录建立新的 Goal 或输入。

运行时 Skill 的目录和正文与 Program 方法准备使用同一正式当前工作目录。读取接点依赖 skills 与 workingDirectory，目录变更撤掉旧读取；发现不调用目录 ensure，不改会话初始工作区或文件权限根。provider、声明安装与执行准入保留原 owner，资源只负责可组合读取及失效。

Jobs 的 `job(id)`、Schedule 与 Team 的 `task(id)` 同步构造原来源下的固定目标，复用同一个目标对象及资源。构造不读注册表、投影或 catalog，也不证明目标存在或授予控制权；读取、历史及原生修改各自在实际边界核验归属与权限。三类对象的资源可以直接组合，首次观察才建立查询。Schedule 与 Team 的缺席目标在资源或 read 中为 undefined；Job 未知或外来目标由其原生读取与控制边界拒绝。Team 与 Schedule 的写入同步返回 NativeInvocation，result 保存同一次原生结算的自有快照，activity 是有限调用事实，wait 单独控制消费者等待。预取消不开始调用，已开始的 CAS 和持久投递仍归原生 owner，不能把等待取消当作回滚或自动重试依据。

显式 owner 的资源把“来源完成”与“有效性结束”分开：正常有限来源立即释放自己的连接，资源仍可观察 owner 的后续撤销，不让外部活跃消费者永久保留旧 ready。值流同样沿读取 owner 完成；业务结果使用 Invocation.result 或 Operation.settled，活动状态不借用 RxJS EOF 表示业务完成。无显式读取 owner 的不可变事实和普通 Observable 保留自己的有限完成语义。

## 完整声明帧与稳定身份

renderer 在 React commit 后收集整棵树的声明，经过私有运行时品牌校验及 collector 合并，再交给会话的 `ProgramInstance`。渲染或声明失败进入程序错误边界，不能发布一个空帧来掩盖失败。

`Context` 提供 JSON 快照或保留 pending/error 的资源观察；`Offer` 将一个已有 Action 绑定以显式名称和 enabled 状态提供给模型；`Follow` 为已绑定原命令的 Operation 提供新的参与能力；`Expose` 贡献视图、动作或只读操作槽；`Skill` 选择原生方法正文或提供明确目录。

同名 Context 复用其资源接点并更新当前观察。相同 Offer 名称与 Action 绑定复用能力身份；enabled 决定当前是否进入原生工具表。相同视图来源对象保留查询连接，替换来源才断开旧连接。静态 Skill 装配及仍在声明中的动态目录也保留各自原生 owner。

一帧中的声明更新通过内部 batch 发布，revision 让异步准备能够发现等待期间的变化。整帧提交不等于把全部组件、Action 或工具重新挂载。原生工具定义以具体 Agent 和绑定为归属；同一会话程序恢复到新 Agent 时，需要取得新 Agent 的执行闭包。

## 原生装配

每个应用与环境共同确定稳定 Preset 身份。Loader 登记的环境插件在原生 standing scope 中持有静态指令、静态 filesystem Skill provider 和作用域内的 Agent 接入。创建与恢复均挂载精确的 Preset，session header 与工作区绑定共同核验身份。原生 registry 持有 standing scope 的代际引用，宿主先释放自己持有的 Agent handles，再退休声明。

Environment 定义捕获自有 Skill 声明数组；Application 定义捕获环境声明及数组，保留 Program 实现对象。异步安装读取这份已校验声明，不借用调用者后续可改的配置。Session 的单条持久归属固定实际 Program 合同，共享定义记录 name/version，无名本地明确为 null；子委托受理意图在 child 出生前固定其目标。恢复在输入解析、Program 图构造及 Agent 激活之前核验原身份，不能以当前环境配置覆盖历史事实。

动态 Context、Offer 和 Skill 接在具体 Agent 上。原生 loop 提取本步输入后先进行 `system-prompt/assemble`，再执行 `agent/pre-step`。宿主在 assemble 边界更新输入关联、核验应用准入、冲刷组件声明，等待当前上下文、步骤检查与选中 Skill，随后通过公开 API 重新装配。

准备期间的帧替换、上下文失效或 Skill catalog 变化会使稳定性检查失败，宿主重新读取当前集合。Context 只把准备完成的文本交给原生 prompt provider；模型请求保留该步骤取得的装配，工具实际调用时再核验当前绑定与可用性。业务权限、数据修订和副作用条件仍由业务 owner 核验。

动态 Skill 目录由 Agent scope 下的原生 filesystem provider 持有，其 catalog 与工具调用沿原生 SkillRegistry 工作。`Skill` 的 name 声明在当前 scope、cwd 和取消信号下加载既有方法，以原生 `renderSkillContent` 生成正文并贡献 required context；它会实际改变模型取得的方法现场。

## Action 的三类入口

`defineAction` 只保存共享合同与可选 Operation 映射，`implementAction` 保存宿主执行与实际业务 owner。`useAction` 将实现绑定到不可变业务目标，同一个绑定可以提供给模型、程序和公开出口。Action 定义对象本身决定合同身份，结构相同的另一对象不自动成为同一合同。

| 入口 | 当前接线与归属 |
| --- | --- |
| `Offer` 对应的模型工具 | 进入原生工具及 PTC 管线，携带真实 callId、rootCallId、parentCallId、已消费输入和适用 Goal continuation |
| 组件程序显式调用绑定 | 宿主以配置的 serviceActorId 建立服务参与身份，携带独立 invocationId，不借用当前模型的输入或 Goal 权限 |
| 同进程句柄的 `resolveAction` | 按调用者明确期望的 scope 捕获 `Expose` 的实际绑定；保留句柄的 human 或 service 角色，服务 actor 在取得端口时固定，人工 actor 由可信选项或宿主配置提供，执行前核验同一绑定 |

远程宿主从同一 Surface 合同分派到实际绑定，调用者身份由认证载体取得。各入口共用绑定的解析与准入链；独立调用不因参数中出现 callId 或 Goal 引用就取得原生调用权限。

公开调用的实际 ActionAuthority 持有唯一有界 Invocation 回执索引，Host/Service 与本地、远程 Surface 都读取原调用。Module 和 Collection 只投射精确地址；查回核验原参与者、Program 与 Action 合同，不依赖当前声明。Gateway 负责传输，不另存回执或延长业务执行寿命。

声明撤下会阻止后续受理。已经进入执行的 Action 沿原调用信号和业务 owner 继续结算；Invocation 的结果和活动观察不受消费者订阅次数控制。业务结果返回、原生工具最终结算与业务任务正式完成各有自己的事实来源。

共同业务资格是资源层的 `Eligibility`，可由 Action、Context 和 Follow 绑定同一来源，供声明、发现和各调用入口共享；业务 owner 仍负责真实调用者、原始输入、目标与事务的逐次准入。Offer 自动消费资格，Context 可借用相同来源或绑定的 reader；Expose 保持合同稳定。本地和远程解析所得 Action 的 `eligibility` 是同源只读观察，不授予执行权。远程观察随既有 `resolveAction` 流传递，不另建订阅协议或执行缓存。

## 长期业务操作与人工参与

Operation 是业务系统已有操作的类型化观察对象。Action Invocation 返回受理回执后，Operation 继续由业务 owner 持有；组件、页面和当前函数退出不改变其寿命。共享合同中的 settled 描述操作终态，feedback schema 描述协作节点 payload；owner 按确切登记读取带稳定 id 的反馈。

远程 Operation 的状态仍由 core 的同一个 ResourceReader 解析和共享，客户端只持有原生流连接及其异步清理。Session 的显式查询随该句柄退出；由已受理 Invocation 取得的 Operation 观察沿整个 SDK 寿命，Session release 不撤销它。release 与 dispose 等待各自原生迭代器及回调静止，连接重置先令观察失效，重连须重新接收 baseline。该等待不等于远端逐流关闭 ACK；服务器收到原生取消后自行释放观察，业务操作及其他 SDK 的查询保持各自归属。

Offer.follow 接合原调用、原工作、命令、正式引用和等待。ConversationActivity.operationFollows 从同一原生投影读取持久关联、回执、准备投递和未启动事实，不调用 locate、register 或 notifier。意图不证明登记 ACK，缺少回执不证明未执行；准备投递不等于受理、消费或结算。原生 reactiveOperations 投影只保存这份因果关联及固定投递，不保存或调度业务任务。业务 owner 使用已有可靠设施登记关联和投递通知，框架按真实 owner 事实与精确 Goal 修订决定是否能够继续。

后续工具调用通过 Follow 为同一原 command 登记新参与，after 指出业务节点边界。新参与使用本次调用的输入和 Goal 修订，原投递保持不变。owner 可为新登记建立新的观察尝试；同节点重放不再观察，进程恢复也不重新执行业务动作。原任务的正式结果与某次观察产生的调查要求分别保存。

归属树、响应依赖图和因果关系分别存在：组件拥有查询连接，RxJS 决定变化传播，持久原调用决定结果归属。两个会话可以观察同一业务操作，它们不会因此共享 Goal 权限。

HumanInput 把响应者准入与当前程序一起组合。实际提出问题、前台等待、客户端 claim、超时继续与迟到回答沿原生服务；持久问题和瞬态前台请求对外分别观察。输入提交权限、人工答复权限及正式业务批准分别核验。

人工答复先取得自有答案快照，完整答案必须恰好覆盖原问题的全部题目。冷会话在准入和激活之前完成这项验证；恢复后继续核对原问题、声明和响应者，不重新消费调用方的可变输入。原生事实读取、响应者准入和前台 claim 的等待响应调用者及来源取消；不配合取消的迟到结果不能继续激活或提交答案。已经进入原生激活的工作仍由原 owner 结算，取消不伪造回滚。

批准安装拥有本安装请求的取消信号，自有回答者和借用的原生 UI 回答者都通过官方 Approval.request 参与。安装退出使尚未决策的请求按原生取消路径结算，迟到批准不能产生新 grant；原生服务继续持有 asked/decided 审计、政策和单次批准语义。已经作出的决定不因随后退出而改写，外部回答者自身的销毁责任仍属于其宿主。

Program conversation 的 human 资源组合这两类原事实，本地 SDK 借用同一对象的读侧，Gateway 将共同快照传给远程 SDK，不让消费者再拼接两个观察。question(callId) 绑定固定原会话和已知问题，state 只投影 active、foreground、settled，回答与前台 claim 仍由原入口按真实响应者重新准入，不维护另一套问答阶段。服务端口在运行时也拒绝人工答复与 Goal 控制。可选原生 UI 借用这份资源，完整传递 pending/error，其问题控制仍核验原 UI generation。

真实工具调用中的 Action 可通过 askHuman 主动请求补充信息，框架将它作为实际 programmatic 子调用接回标准工具与问答服务。调用体拥有这次等待，退出时取消并等待未完成提问；超时返回 pending 后，晚答继续属于原对话，不恢复原函数栈。普通 SDK 参与者不因此取得原生工具或 Goal 身份。

## Surface 与跨进程边界

Surface 显式列出视图 schema、Action 定义与只读 Operation 定义。多个子组件可以向同一 Surface 贡献不同槽，collector 对重复槽硬失败；类型检查和运行时校验共同拒绝未知槽、错误来源与不同合同身份。

operations 槽的 resolver 从当前声明取得原 command，之后的查询保留这份绑定；旧句柄不会因组件切对象而重定向。已登记参与还可以按持久身份单独恢复读取，不要求当前组件继续公开它。读取与新参与登记是不同接口，SDK 不能通过只读句柄借用当前 Goal 权限。

输入的持久原值和事件 wire 保持 JSON；`Program.input` 在本地程序与远程客户端的读侧解析成领域值。单向输入 transform 不承担视图的反向编码责任。`InputReference.sourceSessionId` 标明消息真实所属会话；每个程序只解析本会话输入，跨会话 cause 保留原合同下的原始值和来源，不塞入当前程序的 typed 输入集合。

`view(schema)` 明确消费值事件，`resourceView(schema)` 明确消费资源 Observation；后者保留 pending/error，只有 ready 才产生领域值和 wire。两者的本地 SDK 均使用 `z.output` 领域类型。出口持有一次来源连接，并用同一 schema 的 `z.encode` 得到 wire 值，再核验其确实是 JSON。普通验证 schema 可以使用恒等方向，单向 transform 不能冒充可逆编码；Date 等领域值需要显式双向 Zod codec。

同进程观察保留领域值，跨进程观察发送已经编码的 wire 值。公开 `encodeView`、`decodeView` 供调用方复用同一 Surface 合同；JSX 返回值本身不承担跨进程编解码。Surface 及其映射壳会冻结，外部业务源不被深度冻结。

## 生命周期与持久因果

宿主、能力实例、Preset 和动态 Skill 的异步退出先保留同一个完成回执，再撤销观察和原生装配。同步清理触发的重入取得这个原回执；原生父作用域已开始卸载时，退出仍等待原 Fiber 静止。观察清理失败由其 owner 保存，等待实际原生清理结束后报告。能力实例退出会释放其安装的原生服务，服务执行各自的 drain；借入服务和只读会话 SDK 的观察退出保留各自责任。

| 范围 | 生命周期 |
| --- | --- |
| 工作区 | 持有明确共享的查询连接与权威会话成员关系 |
| 会话程序 | 持有后台组件树、声明、公开出口及组件关系 |
| 组件子树 | 退出时撤下其声明并清理所属查询和关系订阅 |
| 原生 Agent | 持有模型驱动、原生 scope、动态工具和 Skill provider |
| 消费者句柄 | 持有自己的观察订阅，释放时不撤销已受理业务 |

工作区持久成员事实决定 Session 资格。Workspaces 只登记实际签发的资格身份，并直接核原生当前表；不复制成员表。成员撤销先退休旧公开图，再撤观察与声明；原生 Agent、已受理调用、持久回执及独立 Operation 保留各自 owner。远程会话请求携带取得时的 accessId，在装配前拒绝过期引用，Program 冷图回收不会人为改变仍有效的成员资格。

同宿主关闭与恢复可以在仍有句柄的情况下保留会话组件程序，同时替换原生 Agent。冷 `open` 可以从持久记录重建程序而不加载驱动；`resume` 和主动输入才进入驱动路径。进程重启重建当前程序与持久关联，React 局部状态和 JavaScript 调用栈不作为可靠业务任务保存。

业务模块退出只撤下当前声明和查询。原请求、工具调用、业务操作、等待及 Goal revision 的持久因果继续归原生会话与原业务 owner；组件清理不能顺手取消原业务操作、抹去等待或把新对象接到旧操作上。

实时与冷观察复用原生 session projection/query。`reactiveInputs` 单元只记录框架拥有的请求、输入、调用、Goal 与等待关联；原生 inbox 是受理输入的权威来源。持久回执不代表模型已消费、工具已返回或业务已完成，跨业务事务与会话日志的可靠投递继续使用业务已有设施。

请求观察从同一会话事实按原 requestId 派生。助手流的 start 固定本步整组输入；活跃前缀由官方 AssistantStreamAccumulator 保存，后续只接受原生增量，持久结算仍归 session。远程开场发送当前快照，后续传活动更新、原生流帧和新事件；SnapshotStream 与 JournalStream 分别承担恢复，不增加另一份请求投影、游标或回复状态机。观察与日志读取都不提升冷 Agent。

工作范围选择器由应用用原始输入或请求声明映射 JSON 工作键，对显式提供的本地或远程会话事实做纯投影或共享观察。调用以正式 start 事件序号 callSequence 区分每次发生，provider callId 可跨轮复用；回复保留 messageId。它保留完整发生事实、输入与命中的输入，匹配只用于观察分组，不授予续接权限；多输入调用不属于某个工作独占，未知关联的回复不归组。跨会话观察只组合事实：Goal、人工答复和业务 Operation 仍归各自 owner；工作条件解释原生事实及特定续接资格，不建立第二个业务状态引擎。

多个业务等待按 Goal id 与 revision 分组。ready 完成事实先在原生关联投影解除自己的等待，再进入原生 next-step inbox；只有该修订已无其他等待、目标仍 active 且有剩余轮次时，结果才有自动唤醒资格。模型步骤最终执行前仍重新核权。暂停期间已 claim 但未消费的结果保留同一消息身份；显式取消不把它重新放回队列。人的新修订有独立权限，旧等待不能阻止它，旧结果也不能取得它的续接权限。

## 实现边界

仓库根是私有 npm 工作区，管理统一配置、工具、示例和行为试验。六个正式发行包均位于 `packages`；每个包有独立 manifest、exports 和构建入口。以下核心能力域均位于 `packages/core/src`。

`applications` 与 `environments` 定义应用准入和环境配置；`conversations` 拥有输入协议、对话事实及请求观察。`actions`、`operations` 分别持有调用与长期操作的合同、绑定和 owner 接点，`participation` 定义人工参与，`collaboration` 定义子工作协作合同，`tools` 定义原生能力准入。

`surfaces` 持有纯公开合同、组合与集合地址、客户端消费和出口连接；组合挂载点的身份与映射也在该域，不依赖 React。`resources` 持有资源定义、参数实例、有效性、查询和派生；`reactions` 持有持续感知与显式运行的合同、实例和结算，直接消费标准 RxJS 并发操作符。`reactivity` 提供同步响应 owner、Observable 与取消信号互操作。`program` 负责 React 实现、Module、hooks、声明、renderer 和完整帧，作为 `@hanger-source/dsh-reactive/program` 的显式入口。根合同入口不静态导入它；`@hanger-source/dsh-reactive/kernel` 是宿主扩展接点，由真实宿主显式加载。

`packages/dsh` 作为独立的 `@hanger-source/dsh-reactive-dsh` 包装配原生服务、模型、关联、查询、投影和准备边界，Jobs、MCP、Approval、时区各有显式可选入口与配置实例。`packages/remote` 贡献认证 Gateway/Typert 宿主；`packages/client` 是独立客户端发行包，承载纯 wire 合同与官方生成的 remote contribution，不依赖原生宿主、模型 provider 或 React。`packages/native-ui` 将真实 Action manifest 接到 dsh 原生 keyed Tool slots，并读取原调用的持久结果；它由应用显式选择，不进入其他包的安装闭包。

客户端以独立原生 Fiber 表达 SDK 与 Session 句柄的归属。作用域先安装完成再交付 SDK；每次物理连接在构造前交给正式 effect，手动退休的异步清理继续对 owner 可见。框架的取得／释放接点仅关联立即撤销信号、原生清理与退出错误，不复制原生执行或清理引擎。视图、Operation、进展、绑定、人工等待和 journal 不各自保存退出队列；Invocation 观察归 SDK，业务和可靠操作仍归其原 owner。core Scope 保留同步响应图的责任。

宿主的 `conversations` 分别持有会话与 live Agent 的关联、输入受理事务、句柄和原生观察；`execution` 持有唯一执行身份签发、原生工具与政策；`programs` 持有声明与具体 Agent 的装配及稳定准备；`operations` 持有操作参与、结果投递与 Goal 续接条件；`participation` 持有答复和前台等待入口。`applications` 完成正式挂载与配置，ApplicationHost 组合这些责任及整体释放顺序。各域继续读取原生 inbox、Goal、日志与投影，不各自保存一份会话或工作状态。

`createRuntime(assembly => definition, options)` 使用独立 Home 和显式模型配置，并持有原生附件存储、FileUploads 与所需 Connection、凭据服务；已有宿主从同样接收装配工厂的 `mountApplication` 接入，须明确提供官方 attachments、fileUploads 及其依赖。Gateway 沿用宿主这份服务，不另选 home 或建立第二个附件 owner。

可复用模块通过 `implementModule` 携带子模块和实际 Operation owner；dsh 的 `implementNativeModule` 同时携带原生能力。Program 静态列出直接模块，应用工厂返回后递归汇总所有 environment 和 delegation 程序的依赖。同一原对象只装配一次，不同 owner 占用相同 kind/version 在安装前失败。动态模块退出只撤声明和组件读侧，静态 owner 与能力由 App 持有。每个 App 的模块工厂须创建新的原生能力实例；业务 owner 可以显式跨 App 共享。需要直接装配时仍可使用 `assembly.native`、`assembly.operation`，原生委托通过 `assembly.implementDelegation` 构造；工厂返回后不能追加登记。

应用挂载负责安装已登记能力，安装失败释放本次挂载，Host 退出等待能力实际释放；底层服务与已受理业务继续遵循原 owner。Runtime 传递其已经归一化的 home；嵌入宿主仅在登记原生能力时必须向 `mountApplication` 显式提供绝对 home。Operation 登记使恢复能找到同一实际 owner，不把业务操作寿命交给 Host。组件层已有实现和聚焦试验不等于完整框架或真实业务验收。

`native` 接走尚未安装能力实例的一次安装归属。同一应用内重复选择同一对象只安装一次；另一应用不能再次取得该实例，也不能在失败回滚时撤销原应用的能力。复用模块时为各应用构造能力实例，共享底层服务则使用各域已有的 provided 选项。自定义能力的 dispose 必须允许安装前回滚。`operation` 借用实际业务 owner，可由多个应用明确共用，不取得其业务销毁责任。

独立宿主同时安装官方 `llm-retry`、`token-meter`、`compaction-basic` 与 `compaction-image-offload`。重试策略归实际模型 adapter；失败请求的重试不重新执行已经返回的业务工具。TokenMeter 沿原生持久事件形成用量和上下文压力，自动压缩依赖该压力、模型容量及显式压缩配置；图片卸载只响应原生明确的容量错误和 occurrence，不删除原附件。它们共同使用原生取消、日志和恢复机制，不增加自己的重试队列或摘要状态。

`RuntimeOptions.installNative` 保留为宿主显式选择的共享底座安装边界，在基础服务与模型装配完成后、应用能力安装前执行；`options.operations` 保留为宿主显式提供的共享业务 owner。模块自己的能力和 owner 随静态依赖收集，无须在这些选项中再列一次。安装仍使用 Cordis 的插件与作用域；`requireActivePlugin` 区分已结束 await 和实际 ACTIVE，拒绝缺依赖的 pending 安装并清理该次 fiber。独立 Runtime 启动失败会等待原生释放。普通业务组件只消费资源、Action 与 NativeTools 声明，不承担插件装配。

`deepseek` 直接连接官方 Messages adapter，`piAi` 安装官方多协议 provider，`nativeModel` 允许已有宿主提供自己的 adapter。Provider profile 中模型目录的容量与输入模态是部署声明，不能从网关名字推测；目录的默认值也不是已向供应商验证的能力。真实模型自主协作、正式协议传输边界和确定性故障注入各有独立验证范围。

可选原生能力保留各自责任。Jobs 使用原生 owner fence、观察和取消，但其进程内 store 不提供业务 Operation 的跨重启持久性；Approval 需要真实原生 turn 和 answerer，业务问答不能代替权限批准；MCP 复用原生 scoped registry、重同步、取消与释放，不声称支持其明确拒绝的 required Tasks 扩展。Jobs/MCP 显式选中的实际工具名由同一 ResourceReader 提供 Promise、快照、Observable 和 hooks 读取；最后消费者退出只释放查询，原生安装与执行继续属于能力 owner。Schedule 继续由原生 storage、timer 和历史负责，当前投递缺少 Program 输入的身份、范围及准入接点；Team 的成员创建需要显式子程序绑定，成员消息也不能借用原 Goal 或人工授权。框架不以另一套队列、Mailbox 或 TaskManager 填补这些边界。

Jobs、Schedule、Team、Files、Approval 与 Clock 的 `forSession` 接受 Host 签发的本地 SDK 句柄或 Program 原 conversation。同形字段不授予访问权；每次调用仍核 exact 来源、原生服务、工作区和观察寿命。实际 Agent 在首次 Program 求值前完成绑定，原生来源的观察在首次订阅时连接，组件求值不启动读取。Program 来源随程序失败或退出释放，外部句柄按自己的 release 释放，原生已受理工作继续归各自 owner。

Jobs、Schedule 与 Team 的会话 SDK 和固定任务提供只读 `state` 资源；Job 的 `output` 单独提供非消费输出切面。普通 read、peek、完整 Observation、RxJS、hooks 和资源 View 共享正式 Resource 实例，不从值流另存有效性。状态构造不读取，首次消费者建立原生监听后读取，最后消费者退出撤销查询并撤下旧 ready。Schedule 和 Team 发生变化时立即 pending，旧切面失效，读取失败成为 error，后续原生通知可恢复。资源只持有观察，执行、CAS、投递与成员仍由各自原生 owner 负责。显式 SDK list、catalog、readProjection、任务 read 等单次查询保留原生权威读取含义，不因已有资源快照跳过调用。

Job 的输出事件同时触发 `state` 和 `output` 重读。`collect` 只执行一次原生消费读取，消费产生的 trim 随后通知同一物理 Registry 中相应 owner 的已连接 Job state、output 和会话列表重读；通知跨 exact 来源及借用该 Registry 的 provided 能力共享，`events$` 仍只交付原生事件。显式安装 Jobs 工具时，正式 `tools/result` 中的 `job_output` 完成后也触发重读，最终结果为 `isError` 时仍可能已经消费。宿主直接调用原生 `registry.read` 不产生官方消费通知，调用后须对对应 Job 显式 `invalidate(options?)`。资源不保证所有外部消费路径自动实时，也不通过轮询或复制输出 ring 补充通知。

Team 的 `task(id)` 同步绑定固定来源会话与 id，构造不读取原生任务板或证明目标存在。同一 SDK 重复取得同一任务会复用对象；`state`、`state$`、`read` 与显式 `update` 共用原生投影和任务 CAS，在实际读取或修改时核验存在、归属与权限，不保存另一份任务状态或修订。删除从任务板撤下后，原对象的读侧返回 undefined；原生 getTask 的墓碑查询保留其独立语义。冷 Lead 可读取和绑定任务，写入仍要求真实活跃成员；任务对象不会随其他选择或调用参数改指目标。Team 通过官方 `SessionProjectionRegistry.onChanged` 监听 `agentTeam` 投影，不枚举日志事件；mailbox 投影 failure 交付为资源 error。投影观察跟随实际 Agent 关闭、恢复，取消旧读取，不因读取创建成员或启动模型。`waitForChange` 的取消只结束本次等待；即使原生等待先 resolve，若同轮随后来源退出，也不会交付过期成功结果，交付前重新核验原 liveAgent 和权限。

Team 的 roster 与 roster$ 读取原生 listMembers 的运行态，和持久 state、members$ 分别表达运行状态与成员阶段。首次观察连接公开成员及 Agent 状态通知，再读取同一原生成员；冷会话保持 pending，不通过读取创建 Agent。运行态可与任务板和其他资源共同派生参与条件，不在业务 hook 里复制 waitForChange 循环。

NativeCapabilityOwner 只保留本实例已开始调用的结算关系，不建立原生写入队列。调用的原始拒绝进入自身 result 与 failed 活动；正常业务拒绝不会污染能力清理。SDK 退出只撤本消费者等待和读侧，有限调用事实继续保留终态。正式 Cordis effect 收拢实际 Fiber 的 disposer，在退出时先截新调用、撤观察并等写入结算，再释放读依赖与自有 provider。provided 服务仍由外部宿主持有，框架不延长其寿命；它提前退出引起的调用拒绝仍沿原调用报告。

Program、外部本地 SDK、远程 SDK 与可选原生 UI 通过 `session.files` 借用会话文件资格。纯 `/files` 入口提供 `SessionFiles`、`FileResource` 与 `FileWatch`；官方 WorkspaceFiles 类型只做 type-only 重导。Host 的 `files` 选项借入已安装的 NativeFiles，按 exact 来源惰性取得读侧，不重复安装或销毁整个能力。本地未配置时取得入口抛出 `SessionFilesUnavailableError`；远程入口构造不发请求，首次查询或观察交付同一错误，不隐式装配服务。

资源 ready 表达最近一次已读切面，持续更新依赖正式 FS 通知。单父目录观察只消费原生通知；通知可能合并或缺失，Linux 上裸 writeFile 恢复也可能没有通知，已知裸写入后须显式 invalidate。rc.3 本地 provider 的同路径 change 在 50ms 内可能被抑制且不补尾通知；连续正文写入的最终状态须由已知写入者显式 invalidate 或单次权威读取确认。框架不以轮询、第二文件状态或自建 native watcher 掩盖这项上游限制。

Files 的 text(path, range)、metadata(path)、directory(path) 固定原始路径和读取窗口，提供同一个 read、peek、values$、observations$ 与 invalidate。共同工厂复用现有资源和 RxJS，不另建 Scope。构造不做 I/O，首次观察等待正式 watch ready 后读取；同一路径的不同资源共享 watch，最后一个查询退出释放它。目录每次变更重新列举。原生读取以 exact 会话 header 的 cwd 和正式 FS resolve/contains 核目标与 baseFile 范围，文本、目录与元数据仍由 WorkspaceFiles 决定版本、限额和错误。字节读取每次持有独立缓冲区，不加入共享快照。

文件及缺失文件的观察使用包含目录的唯一正式 `WorkspaceFiles.changes` 流，因为创建、删除和替换会改变目录命名空间。连接时包含目录必须存在且为目录，否则报 `REACTIVE_NATIVE_FILE_WATCH_DIRECTORY_REQUIRED`；目录资格退出时来源报错，创建或恢复目录后需显式 invalidate 重新取得目录流，不自动搜索祖先、重绑或轮询。目录来源 ready 后，通过公开 FS 的 resolve、contains、stat 取得目标基线；每个目录帧都重新核验 scope、权限和路径并读取目标的不透明 version 或缺席状态，不按目录 version 过滤。每个目录 change 帧都交付真实回读的目标帧，允许相同状态重复；兄弟文件活动也可能引起额外 stat 与资源重读；目录目标仍直通原流。目标帧是 FS 现态投影，不是逐次写入的持久事件，不能保证捕获所有瞬时状态或无损通知。观察沿原 owner 使用单个 watch，closed 等待唯一 generator 实际 unwatch。

观察资格退出立即结束本消费者等待；原生读侧 dispose 与 Host 关闭等待实际读取和 unwatch，不取消写文件的 Action、Job 或 Operation。Agent 关闭不结束仍有效来源的文件观察。远程端点逐次核 Program、accessId、认证主体、文件路径及交付时权限，字节沿正式 binary codec；断线撤下旧 ready，新 watch ready 后重读，不承诺补齐断线事件。客户端释放只证明本端清理，没有远端 unwatch ACK。原生 UI 借用同一资源状态，自己的 dispose 等待其创建的观察退出，不销毁借入 SDK。需要选择领域产物或授予更窄的公开内容时，仍使用 Program 的资源 View。

创建根会话时，宿主持久写入原工作区 binding 并完成原生 workspace 成员登记，之后才构造 Program；最终创建回执仍在实际 Agent setup 成功后写入并确认持久 flush。登记失败保留原 CreationIncompleteError 引用；只有成员、或完成记录仍在等待创建 ACK 的会话不能经普通 open、resume、conversation 或 service 取得外部句柄，须沿原创建引用完成登记。创建和已有会话激活由同一启动 owner 区分，普通只读访问不等待或借用新会话的创建许可。已有完整回执的恢复只核成员，不重新加入已被撤销的成员关系。

子程序通过原生 spawn / fork 在同一工作区与环境运行独立 Program 与 Goal，沿原调用持久保存父原因、真实子输入及双边确认，见[原生子程序委托](native-delegation.md)。长会话以及真实运营服务的整体消费仍需连续验证。编译、确定性模型试验、发布产物和正式业务验收分别报告，不由依赖安装推定成立。
