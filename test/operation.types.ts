import { of } from 'rxjs';
import { z } from 'zod';
import { bindAction, defineAction, implementAction, type ActionExecutionContext, type BoundAction } from '@hanger-source/dsh-reactive';
import { bindOperation, defineOperation, implementOperation, type OperationAccessContext, type OperationFollowLink, type OperationHandle, type OperationSettled } from '@hanger-source/dsh-reactive';
import { readOperationFeedback } from '@hanger-source/dsh-reactive/kernel';

const timestamp = z.codec(z.iso.datetime(), z.date(), { decode: value => new Date(value), encode: value => value.toISOString() });
const state = z.discriminatedUnion('status', [
  z.object({ status: z.literal('running'), stage: z.string(), updatedAt: timestamp }),
  z.object({ status: z.literal('succeeded'), artifactId: z.string(), updatedAt: timestamp }),
  z.object({ status: z.literal('failed'), reason: z.string(), updatedAt: timestamp }),
  z.object({ status: z.literal('cancelled'), by: z.string(), updatedAt: timestamp }),
]);
const preparation = defineOperation({ kind: 'merchant-preparation', version: 1,
  command: z.object({ merchantId: z.string(), commandId: z.string() }).strict(),
  reference: z.object({ merchantId: z.string(), operationId: z.string() }).strict(),
  state,
  settled: value => value.status === 'running' ? undefined : value,
  feedback: z.object({ reason: z.string(), observedAt: timestamp }).strict(),
});
const owner = implementOperation(preparation, {
  locate(command, access) {
    command.commandId.toUpperCase();
    access.source.kind;
    // @ts-expect-error 查询身份不取得原工作 Goal 权限。
    access.continuation;
    // @ts-expect-error command 是自有只读引用。
    command.merchantId = 'other';
    return { status: 'accepted', reference: { merchantId: command.merchantId, operationId: 'operation-a' },
      state: { status: 'running', stage: 'accepted', updatedAt: '2026-10-26T00:00:00.000Z' } };
  },
  observe(reference) {
    reference.operationId.toUpperCase();
    return of({ status: 'ready' as const, generation: 1, observedAt: 0,
      value: { status: 'succeeded' as const, artifactId: 'artifact-a', updatedAt: '2026-10-26T00:00:00.000Z' } });
  },
  async registerFollow(command, link) {
    command.commandId.toUpperCase();
    const when: 'settled' | 'feedback' = link.when;
    const callId: string = link.source.callId;
    void when; void callId;
  },
  async readFeedback(command, link, access) {
    command.commandId.toUpperCase();
    const after: string | null = link.after;
    // @ts-expect-error 反馈查询只读登记，不取得原工作 Goal 控制。
    access.completeGoal();
    void after;
    return { status: 'ready', reference: { merchantId: command.merchantId, operationId: 'operation-a' },
      node: { id: 'observation-a', payload: { reason: 'investigate', observedAt: '2026-10-27T00:00:00.000Z' } } };
  },
});

function operationBindingConsumer(access: OperationAccessContext, persisted: OperationFollowLink) {
  const binding = bindOperation(owner, { merchantId: 'merchant-a', commandId: 'command-a' }, {
    access: () => access,
    readFeedback: (reference, _id, reader) => readOperationFeedback(owner, reference.command, persisted, reader),
  });
  const participation = binding.participation('actual-persisted-participation');
  binding.command.commandId.toUpperCase();
  participation.command.merchantId.toUpperCase();
  void participation.readFeedback().then(value => {
    if (value.status === 'ready') {
      value.node.payload.observedAt.toISOString();
      value.node.payload.reason.toUpperCase();
      // @ts-expect-error 领域反馈由原 schema decode，不再是原始 wire 时间字符串。
      value.node.payload.observedAt.toUpperCase();
    }
  });
  // @ts-expect-error 原 command 的绑定不能伪造 canonical reference 或终态等待。
  binding.settled();
  // @ts-expect-error 没有 ref 之前不能声称已经具备状态观察来源。
  binding.observations$;
  // @ts-expect-error 普通读取不能登记当前 Goal 的下一等待。
  participation.follow();
  // @ts-expect-error 原命令必须符合此 Operation 合同。
  bindOperation(owner, { operationId: 'fiction' }, { access: () => access,
    readFeedback: (reference, _id, reader) => readOperationFeedback(owner, reference.command, persisted, reader) });
}
void operationBindingConsumer;
const prepare = defineAction({ name: 'prepare_merchant', description: '受理正式准备操作',
  scope: z.object({ merchantId: z.string() }).strict(),
  input: z.object({ commandId: z.string(), revision: z.string().transform(Number) }).strict(),
  output: z.object({ operation: preparation.reference }).strict(),
  operation: {
    definition: preparation,
    command(input, scope, origin) {
      input.revision.toFixed();
      origin.invocationId.toUpperCase();
      origin.inputs[0]?.requestId?.toUpperCase();
      // @ts-expect-error origin 只提供调用事实，不暴露执行控制。
      origin.signal;
      // @ts-expect-error origin 不取得 Goal 结束权限。
      origin.completeGoal();
      return { commandId: input.commandId, merchantId: scope.merchantId };
    },
    reference: receipt => receipt.operation,
  },
});
const implementation = implementAction(prepare, { operationOwner: owner,
  execute: (_input, call) => ({ operation: { merchantId: call.scope.merchantId, operationId: 'operation-a' } }),
});

async function consume(execution: ActionExecutionContext) {
  const bound = bindAction(implementation, { merchantId: 'merchant-a' }, { execution: () => execution, admit() {} });
  const exposed: BoundAction<typeof prepare> = bound;
  const invocation = bound({ commandId: 'prepare-1', revision: '7' });
  const handle: OperationHandle<typeof preparation> = await invocation.operation;
  const receipt = await invocation.result;
  receipt.operation.operationId.toUpperCase();
  const terminal: OperationSettled<typeof preparation> = await handle.settled();
  terminal.updatedAt.toISOString();
  // @ts-expect-error settled 不会把仍在执行的状态当作终态。
  const running: 'running' = terminal.status;
  void running;
  const attached: OperationHandle<typeof preparation> = exposed.attach(handle.reference, execution);
  const located = await bound.locate({ merchantId: 'merchant-a', commandId: 'prepare-1' });
  if (located.status === 'accepted') located.state.updatedAt.toISOString();
  void attached;
  // @ts-expect-error command 合同贯穿 attach 之外的独立查询。
  bound.locate({ operationId: 'operation-a' });
}
void consume;

// @ts-expect-error 关联 Operation 的实现必须显式提供真实 owner。
implementAction(prepare, { execute: () => ({ operation: { merchantId: 'merchant-a', operationId: 'operation-a' } }) });
const immediate = defineAction({ name: 'immediate', description: '直接返回', scope: z.null(), input: z.string(), output: z.number() });
const immediateImplementation = implementAction(immediate, { execute: input => input.length });
function immediateConsumer(execution: ActionExecutionContext) {
  const bound = bindAction(immediateImplementation, null, { admit() {} });
  // @ts-expect-error 普通返回行为没有操作读句柄。
  bound('value', execution).operation;
  // @ts-expect-error 没有关联 Operation 时不能声称可重连任务。
  bound.attach({ operationId: 'fiction' }, execution);
}
void immediateConsumer;

function linkConsumer(link: OperationFollowLink) {
  const snapshot: OperationFollowLink = link;
  // @ts-expect-error 可靠关联必须明确 settled 或 feedback，不猜何时交回工作。
  const missingWhen: OperationFollowLink = { id: link.id, operation: link.operation, command: link.command,
    continuation: link.continuation, wait: link.wait, scope: link.scope, target: link.target, source: link.source, mode: link.mode };
  void snapshot; void missingWhen;
  if (link.when === 'settled') { const after: null = link.after; void after; }
  // @ts-expect-error settled 等待不接受反馈节点作为游标。
  const invalidSettled: OperationFollowLink = { ...link, when: 'settled', after: 'feedback-a' };
  void invalidSettled;
}
void linkConsumer;
