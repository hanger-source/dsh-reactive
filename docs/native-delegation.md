# 原生子程序委托

委托是一份普通 Action 合同和一个由宿主实现的原生子工作。业务模块绑定、公开和提供这份 Action；宿主 assembly 将它接到 dsh 的 spawn / fork、原生输入、资源树与持久会话。`NativeChildWork` 提供只读 Operation 合同，正式完成条件是子工作自己的 Goal 完成。

## 原生发现

原生子 Agent 发现独立于这份 Goal 合同。从 `@hanger-source/dsh-reactive-dsh/subagents` 创建 `createNativeSubagents({ service: 'provided' })`，通过 installNative 或已有宿主安装后，用 `forSession(source).catalog` 观察原会话的持久子目录，或用 `readDescendants({ signal })` 显式读取可达子树。普通函数、Promise、RxJS 和 hooks 消费同一 catalog；它保留 one-shot、continuable、external 模式，不据此推断业务完成。递归读取使用 rc.3 的正式查询，缺失或损坏分支保留原生诊断，不伪装为空子树。

目录观察只随原父会话的目录事件及实际 Agent 代际失效，冷读取不恢复 Agent。SDK 或能力退出撤销读侧，不中断已有子执行。创建、投递和恢复执行仍需要各自的可信 Program 准入；此入口不提供绕过准入的 raw start/send，也不复制原生子树调度、持久化或执行引擎。

## 共享合同

`contracts.ts` 只包含可共享合同。`nativeChildWorkAction` 贡献标准回执及 Operation 映射，应用继续定义业务名字、原始输入与目标：

```ts
import { composeSurface, defineAction, defineProgram, defineSurface, nativeChildWorkAction, z } from '@hanger-source/dsh-reactive';

export const Investigate = defineAction({
  name: 'investigate_case', description: '委托调查明确案件',
  scope: z.string().transform(caseId => ({ caseId })),
  input: z.object({ question: z.string(), asOf: z.iso.datetime().transform(value => new Date(value)) }).strict(),
  ...nativeChildWorkAction,
});
export const ConfirmArtifact = defineAction({
  name: 'confirm_artifact', description: '核验正式产物并完成子工作',
  scope: z.object({ caseId: z.string() }).strict(),
  input: z.object({ artifactId: z.string() }).strict(), output: z.boolean(),
});
export const ChildProgram = defineProgram({
  input: z.object({ caseId: z.string(), question: z.string(), asOf: z.iso.datetime().transform(value => new Date(value)) }).strict(),
  surface: defineSurface({ views: {}, actions: {} }),
});
export const InvestigationSurface = defineSurface({ views: {}, actions: { investigate: Investigate } });
export const composition = composeSurface({ investigation: InvestigationSurface });
export const ParentProgram = defineProgram({
  input: z.object({ caseId: z.string() }).strict(),
  surface: composition.surface,
});
```

Action 的原始 scope 是案件 ID 字符串，执行及授权时取得 `{ caseId }`；原始 input 的 `asOf` 是 ISO 字符串，执行及授权时取得 Date。委托持久关联保留原始 input/scope，恢复时重新按同一合同解析，不把解析结果再次当原始值，也不要求单向 transform 可以逆编码。

`ChildProgram` 拥有自己的输入合同。`childInput` 必须返回该合同的 `z.input`，因此日期应明确提供 ISO 字符串；子组件的 `inputs$` 再取得 Date。每条子输入的 `sourceSessionId` 是子会话本身。父输入作为带各自来源会话及原始范围的 causes 保存在委托关联中，不混入子程序的领域输入。

## 业务模块与类型化出口

`investigation-module.tsx` 只依赖 core；局部合同中的绑定由模块公开，挂载点负责映射名字与出口。父 Goal 的 follow scope 是父 Program 的原始输入。

```tsx
import { Expose, Offer, implementModule, useAction } from '@hanger-source/dsh-reactive/program';
import { type ActionImplementation } from '@hanger-source/dsh-reactive';
import { InvestigationSurface, Investigate } from './contracts.js';

export function createInvestigationModule(implementation: ActionImplementation<typeof Investigate>) {
  return implementModule(InvestigationSurface, { Component({ caseId }: { caseId: string }) {
    const investigate = useAction(implementation, () => caseId, [caseId]);
    return <>
      <Offer action={investigate} follow={{ when: 'feedback', scope: { caseId } }} />
      <Expose surface={InvestigationSurface} value={{ actions: { investigate } }} />
    </>;
  } });
}
```

## 宿主装配与正式完成

`host.tsx` 使用 `createRuntime(assembly => definition, options)`。`assembly.implementDelegation` 返回普通 `ActionImplementation<typeof Investigate>`，应用把它交给上述模块。应用提供真实的输入准入、委托授权、产物核验及模型；这些函数不是框架内置的业务完成规则。

```tsx
import { Context, Offer, implementProgram, useAction, useObservable } from '@hanger-source/dsh-reactive/program';
import { defineApplication, defineEnvironment, implementAction, type ActionCallContext, type ActionScope, type ApplicationDefinition, type ProgramParsedInput } from '@hanger-source/dsh-reactive';
import { createRuntime, type DelegationAdmission, type RuntimeModel } from '@hanger-source/dsh-reactive-dsh';
import { ChildProgram, ConfirmArtifact, Investigate, ParentProgram, composition } from './contracts.js';
import { createInvestigationModule } from './investigation-module.js';

export interface CaseServices {
  admit: ApplicationDefinition['admit'];
  authorize(admission: DelegationAdmission<typeof Investigate>): void | PromiseLike<void>;
  confirmArtifact(input: { caseId: string; artifactId: string },
    call: ActionCallContext<ActionScope<typeof ConfirmArtifact>>): Promise<void>;
}
export async function startCaseRuntime(config: {
  home: string; caseId: string;
  actorId: string; serviceActorId: string; model: RuntimeModel; services: CaseServices;
}) {
  const completion = implementAction(ConfirmArtifact, { async execute(input, call) {
    await config.services.confirmArtifact({ caseId: call.scope.caseId, artifactId: input.artifactId }, call);
    call.completeGoal();
    return true;
  } });
  function ChildWork({ task }: { task: ProgramParsedInput<typeof ChildProgram> }) {
    const complete = useAction(completion, () => ({ caseId: task.caseId }), [task.caseId]);
    return <>
      <Context name="investigation" value={{ ...task, asOf: task.asOf.toISOString() }} />
      <Offer name="confirm_artifact" action={complete} />
    </>;
  }
  const child = implementProgram(ChildProgram, { Component({ conversation }) {
    const inputs = useObservable(() => conversation.inputs$, [conversation]);
    if (inputs.status === 'error') throw inputs.error;
    if (inputs.status === 'pending' || inputs.value.length === 0) return null;
    const delegated = inputs.value.filter(input => input.source === 'delegation');
    if (delegated.length !== 1) throw new Error('CHILD_DELEGATION_INPUT_REQUIRED');
    return <ChildWork task={delegated[0]!.scope} />;
  } });
  return createRuntime(assembly => {
    const delegation = assembly.implementDelegation(Investigate, {
      program: child, mode: 'spawn',
      childInput: (input, call) => ({ caseId: call.scope.caseId, question: input.question, asOf: input.asOf.toISOString() }),
      objective: (input, call) => `调查案件 ${call.scope.caseId}：${input.question}`,
      authorize: admission => config.services.authorize(admission),
    });
    const investigation = createInvestigationModule(delegation);
    const parent = implementProgram(ParentProgram, { modules: { investigation }, Component({ modules }) {
      const Investigation = modules.investigation.Component;
      return <Investigation mount={composition.mounts.investigation} caseId={config.caseId} />;
    } });
    return defineApplication({ name: 'case-investigation', admit: config.services.admit,
      environments: [defineEnvironment({ id: 'cases',
        instructions: '依据当前业务现场调查案件；正式完成以产物核验为准。', program: parent })],
    });
  }, { home: config.home, actorId: config.actorId, serviceActorId: config.serviceActorId,
    model: config.model, tools: 'native', subagents: { maxDepth: 3, maxActiveSubagents: 8 } });
}
```

`home` 使用绝对路径；工作区在调用时明确注册和选择。示例的 `caseId` 是应用已核权的绑定目标，`services.admit` 仍负责准入实际输入及其作用范围。`confirmArtifact` 必须核对产物属于该案件、当前业务权限及正式完成事实；成功后，真实子工具调用中的 `call.completeGoal()` 完成它关联的子 Goal。示例子组件处理原始委托及其 Goal 延续；其他子输入路径应由应用按自己的合同与准入规则明确提供。

`spawn` 与 `fork` 都沿原生父子 ownership，留在同一工作区和环境，使用声明的 ChildProgram。`fork` 继承原生历史切面，当前动态 Context、Offer、Follow 和 Skill 由子组件重新声明。子工作另建独立 Goal，不借用父 Goal 的完成或续接权限。

普通成功 turn、assistant 回复和原生 activation 退出不满足 `NativeChildWork.settled`，也不会自动制造反馈节点。子 Goal 的 paused、blocked、complete，以及真实执行的 error、cancelled、interrupted 等边界可以形成反馈；正式 settled 只取子 Goal 的 complete。`when: 'feedback'` 等待的是有权威来源的协作节点，不是任意一条子回复。原参与交回的节点保持固定，之后读取当前 Operation 与登记新的参与各有独立语义。

## 谁执行，谁请求本次访问

`authorize` 的 `executor` 固定为最初启动该委托的真实 Action 来源。模型工具保留准确的 callId、rootCallId 与 parentCallId；普通 SDK 或组件调用保留 human/service participant、actorId 与 entrypoint，不能借当前模型的工具身份、父输入或 Goal 权限。

`requester` 表示本次准入来源。create、read、follow 可带当前 ActionSource；恢复沿精确存活父实例取得原生 ownership 后，使用 `{ kind: 'parent', sessionId }` 表达父宿主恢复，不伪装成原工具再次调用。应用分别决定这些阶段的权限，不能把 executor 曾经获准当作所有后续 requester 的通行证。

授权参数中的 `input` 与 `target` 已按原 Action 合同解析，日期是 Date，目标是 `{ caseId }`；`childInput` 和 `causes` 保持原始 JSON。`objective` 是原委托的目标。恢复会重新核对父子 lineage、持久绑定、原始输入来源及应用准入。

`phase: 'resume'` 的 admission 另提供 `recovery`：`child` 是真实子引用，`messageId` 是初始消息身份，`input` 为 `pending`、`claimed`、`consumed` 或 `cancelled`，`goal` 是当前 Goal 的只读快照或 `null`（权威事实为尚未创建）。`claimed` 表示消息已被领取但尚未进入历史，可以恢复同一消息。create、read 和 follow 不提供 `recovery`。这些快照及其 blockedReason 是独立冻结副本，不暴露 Agent。

业务 `authorize` 应以原委托目标和 `recovery.goal` 的当前 revision、objective 等事实判断本次恢复是否仍获准，而不是仅凭同一个 Goal ID 推断。授权前捕获目标；等待授权后若 Goal 或输入状态变化，宿主不会借用新目标，精确原生 CAS 或硬失败暴露竞争。框架负责证明真实调用与关联，业务服务继续拥有权限、副作用和业务完成事实。

## 调用、只读观察与冷恢复

普通 SDK 调用使用公开的同一 Action。下面函数从前述 runtime 创建会话，返回应由应用保存的原 command 与正式 receipt：

```ts
import type { startCaseRuntime } from './host.js';
import { composition } from './contracts.js';

export async function requestInvestigation(runtime: Awaited<ReturnType<typeof startCaseRuntime>>, directory: string, caseId: string) {
  const workspace = await runtime.application.createWorkspace({ directory });
  const parent = await runtime.application.create('cases', { workspaceId: workspace.workspaceId });
  const investigate = parent.module(composition.mounts.investigation).resolveAction('investigate', caseId);
  const invocation = investigate({ question: '核验最近一笔异常交易', asOf: '2026-10-27T00:00:00.000Z' });
  const command = { parentSessionId: parent.id, invocationId: invocation.id };
  const receipt = await invocation.result;
  const work = await invocation.operation;
  return { parent, command, receipt, work };
}
```

这条普通调用创建独立 participant Invocation。`Offer.follow` 的父 Goal 续接只由真实模型工具调用建立；SDK 调用不会因为同一个 Action 出现在 Offer 中就取得这份权限。模型路径由父 Program 接受真实输入、取得当前 Offer 并实际调用，框架持久化原 continuation 和反馈参与。

receipt 包含 parentSessionId、childSessionId、初始消息的真实 messageId 及子 Goal 引用，证明原生初始输入已经受理并取得双边确认；它不表示子工作已经完成。`work.observations$` 读取当前 Operation 状态，`work.settled({ signal })` 只等待子 Goal 的正式 complete。取消观察或释放消费者句柄不会重新启动委托，也不会取消已受理子工作。

重启后，以相同应用装配和合同恢复，只读读取与主动唤醒分开：

```ts
import { NativeChildWork, type NativeChildCommand } from '@hanger-source/dsh-reactive';
import { ChildProgram, ParentProgram } from './contracts.js';
import type { startCaseRuntime } from './host.js';

export async function readExistingWork(runtime: Awaited<ReturnType<typeof startCaseRuntime>>,
  command: NativeChildCommand, actorId: string) {
  const parent = await runtime.application.open(command.parentSessionId, ParentProgram);
  const located = await parent.locateOperation(NativeChildWork, command, { actorId });
  if (located.status !== 'accepted') throw new Error('CHILD_WORK_NOT_LOCATED');
  const work = await parent.attachOperation(NativeChildWork, located.reference, { actorId });
  const child = await runtime.application.open(located.reference.childSessionId, ChildProgram);
  return { parent, work, child };
}
```

上述 open、locate 和观察读取不创建 Agent、不调用模型。`runtime.application.resume(child.id, ChildProgram)` 加载原 Agent；普通人工 Goal 续行须通过会话的 `resumeGoal(expected)` 显式指定准确 Goal 引用，加载本身不代替这项决定。`open(id, expectedProgram)` 与 `resume(id, expectedProgram)` 核对当前宿主实际定义身份，不能用任意泛型把动态会话伪装为另一个 Program。

委托恢复沿持久父子关系核验精确父实例和子 owner，在 created 与存活实例修复路径以 `phase: 'resume'` 请求业务授权。已 claim 且未消费的初始输入恢复同一个 messageId；已消费输入不作为新消息重发，已获准的活跃子 Goal 可以自主进入新的 round，这是独立于原消息恢复的执行。已取消输入不会重发。父 Goal 暂停不自动取消已受理的独立子工作；子 Goal 自身持久 paused 时不会被自动激活。

创建确认失败抛出从 `@hanger-source/dsh-reactive-dsh` 导出的 `ChildCreationIncompleteError`，其中 `command` 与 `reference` 保留原委托和预留子会话身份。应用可以按这个 command 调用父句柄的 `locateOperation(NativeChildWork, command, { actorId, signal })` 核对 reserved / creation-incomplete 事实，不应再发一个新的 Action Invocation。进程仍保有真实 accepted messageId 或子端已有正式持久 receipt 时，显式 resume 可以补齐确认；跨重启缺少该 messageId 或正式 receipt 时仍保持 creation-incomplete，不从 prompt、历史条数或最新消息猜身份，也不重建一份子任务。

已登记 feedback 参与可通过父句柄的 `readOperationFeedback(NativeChildWork, command, participationId, { actorId, signal })` 只读查询；participationId 必须来自真实持久参与，不能拿 Invocation ID 充当。读取当前 Operation、读取原参与反馈和让模型登记新参与是不同权限与行为。远程消费者可使用同一纯 Program 和 `NativeChildWork` 合同，认证与 endpoint 权限见 [外部参与与观察](transport.md)。

这些入口说明框架的原生接合与恢复语义；正式应用仍须用自己的模型、权限、产物服务和持久介质核验完整业务结果。
