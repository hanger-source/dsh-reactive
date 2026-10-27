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

已有 Cordis 宿主使用 `mountApplication(ctx, factory, options)`；需真实安装 agents、sessions、persistence/query/projections、workspaceRegistry、goals/tools/systemPrompt/loader、preset、skills/fs/cwd、userQuestions、attachments/fileUploads 等正式服务。不要只提供同名空对象满足 inject，也不要另装一套不同 home 的附件服务。

`RuntimeOptions.installNative` 在任何 Agent 创建前安装可选能力；`nativeCapabilities(...selected)` 组合正式能力安装。Jobs、MCP、Approval、Time、Files 等按包公开 subpath 和实际插件能力选择，原生 owner 继续拥有状态、权限、取消和资源寿命。使用当前安装 subpath 的 DTS，不为了统一外形给所有工具再包 Action。

组件用 `<NativeTools names={actualInstalledNames} enabled={condition} admit={policy} />` 选择原生已安装工具，保持原执行/guard/PTC语义。它不安装后端，enabled 不等于业务授权。

Schedule 创建与 Team spawn/send 尚无 Program 来源准入接点。它们与已支持的提醒观察/更新、任务板创建/CAS、等待和 interrupt 分开；不能因为插件可安装，就声称 UI 或服务创建链已经贯通。

## 子程序沿原生 owner

`createRuntime(assembly => ...)` 的 assembly 提供 `implementDelegation(action, { program, mode:'spawn'|'fork', childInput, objective, authorize })`。返回普通 ActionImplementation，继续 useAction/Offer/Operation/Follow；不用新增 Delegate 组件或子队列。

子 Program 独立输入和 Surface；父 cause 保每条 sourceSessionId/raw scope。authorize 收真实 parent、executor、requester、causes 和 childInput，不把 service actor 或子初始 user source 猜作原 human。fork 合法继承历史，不继承父动态现场/当前 Offer；spawn 使用独立原生子程序配置。完成和反馈沿 NativeChildWork Operation，不把普通 turn 结束当正式业务完成。

Scope.dispose、SDK release、Runtime.dispose、业务 cancel 不是同一命令。显式释放自己拥有的 Runtime/能力；借入会话句柄或观察退出不能取消原业务。正常 host close 会处理未消费输入，不能把正常关闭冒充 crash 恢复试验。
