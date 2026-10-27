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

Runtime 的 home 必须显式绝对路径，隔离会话、附件、存储与凭据；不能改全局环境变量或进程 cwd 给多 Runtime 共享个人目录。`deepseek`/`piAi`/`nativeModel` 接模型；使用实际模型声明的容量和能力，不猜 contextWindow 或 vision。需要控制摘要/预留量时显式传 RuntimeOptions.compaction；官方 retry、token meter、compaction、image offload 已由 Runtime 安装。

```ts
const workspace = await runtime.application.createWorkspace({ directory: absoluteExistingDirectory, title: '仓库 A' });
const session = await runtime.application.create('operations', { workspaceId: workspace.workspaceId });
const recovered = await runtime.application.open(session.id, InventoryProgram);
```

createWorkspace 注册原生 existing 目录，不隐式 mkdir。Environment 不拥有目录；一个环境可用于多个 workspace，一个 workspace 可含不同环境。已有会话从自身权威 binding 恢复，open/resume 传 expected Program 核同一合同；resume 显式激活，open 只读不借恢复名义执行。

`host.service(id, Program)` 是固定 serviceActorId 的服务参与端口：可读、解析 Action、submit/edit 输入及观察 Operation，不能回答人工问题、控制 Goal 或自报 actor。ProgramSession.submit 也保固定服务来源。普通 UI/人工端口与服务不可混用。

## 嵌入与可选能力

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

同一原来源共享 SDK 实例，Program 来源和外部句柄分别拥有观察寿命。Jobs、Schedule、Team 会话与固定任务的 `state` 是可直接传给 `useResource` 的只读资源，Job 的 `output` 独立表示非消费输出切面。`state.read(signal)`、peek、subscribe、values$、observations$ 共用现有资源机制；原 state$/list$/catalog$/projection$ 是相应值流，不能用它们推断旧值仍有效。最后一个资源消费者退出会撤销查询并撤下旧 ready，组件退订不 dispose 会话 SDK，也不停止已受理工作。SDK dispose、Program 失败或来源退出释放其全部观察，不杀死 Job、不撤回提醒、不取消 Team 成员。Program 来源不自动取得人工、ToolOccurrence 或 Goal 权限。

Schedule 与 Team 的变化先撤下旧 ready 再读取新切面，读错误保留为资源 error，后续原生通知可以恢复，不使用无条件 retry。组合不同能力时直接用 combineResources/mapResource 组合 observations$，将同一观察交给 hooks、Context 和资源 View；不要把查询 pending/error 当成 Job 执行或任务业务状态。需要重新查询权威服务时，使用原 list/catalog/readProjection/任务 read 等显式入口。

来源仍有效时，依赖原生切面或日志的长期观察会跟随实际 Agent 的关闭和恢复：在途旧读取取消，恢复后读取当前历史，日志不重复交付。关闭原生 Agent 不等于退出仍持有的 SDK 观察；读取本身不 resume 或启动模型。不要为这一转换维护第二份 Agent 状态或用无条件 retry 掩盖真正读取错误。

Team 任务可用 `const task = await team.forSession(source).task(id)` 取得。`task.state$` 供 RxJS 和 `useObservable` 使用，`task.read({ signal })` 读取同一原生投影；显式 `task.update({ expectedRevision, action, ... })` 由原生 CAS 决定是否受理，不重复传 taskId，也不能向请求中塞入 taskId 更换目标。对象身份属于原来源与任务，不按当前界面选择重定向。删除后流和 read 返回 undefined；需要原生墓碑时用活跃成员的 getTask。冷 Lead 可以读取与取得对象，修改仍要求真实活跃成员，不会为此自动 resume、spawn 或 send。

Files 复用已安装的 WorkspaceFiles、fs、sandboxPolicy 和 Typert，不自动安装文件服务或改变沙箱政策。`const page = files.forSession(source).text(path, { offset, limit })` 返回正式 Resource 实例；metadata(path) 与 directory(path) 分别提供元数据和目录资源。同一原始路径与窗口取得同一对象，read、peek、values$、observations$、invalidate 与 `useResource(page)` 共用它。构造不做 I/O；首次观察等待 watch ready，之后变化取消旧读、重新取权威切面；同一 SDK 的文本页和元数据共享该路径的 watch，最后一个查询退出后释放它。读取失败保留为资源 error，后续文件变化可恢复；watch 初始化失败明确进入 error，不偷偷轮询或降级。SDK dispose 等待实际 unwatch，Agent close 不取消仍有效来源的文件观察。readBytes 继续每次返回独立缓冲区。产物需要公开时，用 mapResource 选择领域字段交给资源 View，沿现有本地或远程 Surface 消费，不给 UI 复制文件监听。

原生FS可合并短窗口的失效通知；资源读取当前切面，通知不承担每次写入的持久事件含义。业务已知写入成功时可显式 invalidate，内容版本仍以实际读取的 version 为准。

Schedule 创建与 Team spawn/send 尚无 Program 来源准入接点。它们与已支持的提醒观察/更新、任务板创建/CAS、等待和 interrupt 分开；不能因为插件可安装，就声称 UI 或服务创建链已经贯通。

## 子程序沿原生 owner

`createRuntime(assembly => ...)` 的 assembly 提供 `implementDelegation(action, { program, mode:'spawn'|'fork', childInput, objective, authorize })`。返回普通 ActionImplementation，继续 useAction/Offer/Operation/Follow；不用新增 Delegate 组件或子队列。

子 Program 独立输入和 Surface；父 cause 保每条 sourceSessionId/raw scope。authorize 收真实 parent、executor、requester、causes 和 childInput，不把 service actor 或子初始 user source 猜作原 human。fork 合法继承历史，不继承父动态现场/当前 Offer；spawn 使用独立原生子程序配置。完成和反馈沿 NativeChildWork Operation，不把普通 turn 结束当正式业务完成。

Scope.dispose、SDK release、Runtime.dispose、业务 cancel 不是同一命令。显式释放自己拥有的 Runtime/能力；借入会话句柄或观察退出不能取消原业务。正常 host close 会处理未消费输入，不能把正常关闭冒充 crash 恢复试验。
