import { test } from 'node:test';
import assert from 'node:assert/strict';
import { firstValueFrom, of } from 'rxjs';
import { z } from 'zod';
import { bindAction, defineAction, implementAction, type ActionExecutionContext, type ActionCallContext, type ActionReference, type ActionInvocation } from '@hanger-source/dsh-reactive';
import { invokeBoundAction, isBoundAction } from '../packages/core/src/actions/contract.ts';
import { observeBoundAction, captureActionArguments, actionInvocationArguments, guardActionExecution } from '@hanger-source/dsh-reactive/kernel';
import { defineOperation, implementOperation } from '@hanger-source/dsh-reactive';
import type { WaitReference } from '@hanger-source/dsh-reactive';

function participant(invocationId: string): ActionExecutionContext {
  return { invocationId, sessionId: 'delivery', source: { kind: 'participant', actorId: 'operator', participant: 'human', entrypoint: 'authenticated-http' },
    inputs: [], signal: new AbortController().signal, continuation: null };
}

test('调用立即进入准入，多个观察者共享一次执行；声明退出不取消已开始业务，变换只在合同边界运行一次', async () => {
  const admission = Promise.withResolvers<void>();
  const started = Promise.withResolvers<void>();
  const operation = Promise.withResolvers<string>();
  let admissions = 0, executions = 0, scopeParses = 0, inputParses = 0, outputParses = 0;
  const definition = defineAction({ name: 'prepare', description: '正式准备',
    scope: z.object({ merchantId: z.string().transform(value => { scopeParses++; return value.toUpperCase(); }) }),
    input: z.object({ revision: z.string().transform(value => { inputParses++; return Number(value); }) }),
    output: z.string().transform(operationId => { outputParses++; return { operationId }; }).pipe(z.object({ operationId: z.string() })),
    observe: output => ({ operationId: output.operationId }),
  });
  const definitionImplementation = implementAction(definition, {
    async execute(input, call) {
      executions++;
      assert.equal(input.revision, 7);
      assert.equal(call.scope.merchantId, 'MERCHANT');
      started.resolve();
      return operation.promise;
    }
  });
  const execution = participant('prepare-one');
  const action = bindAction(definitionImplementation, { merchantId: 'merchant' }, {
    async admit(input, _call, original) {
      admissions++;
      assert.equal(original, execution);
      assert.equal(input.revision, 7);
      await admission.promise;
    },
  });
  assert.equal(executions, 0);
  const reference: ActionReference = action;
  const counterfeit: ActionReference = { definition, bindingId: action.bindingId, scope: action.scope, released: false };
  assert.equal(isBoundAction(reference), true);
  assert.equal(isBoundAction(counterfeit), false);
  assert.throws(() => invokeBoundAction(counterfeit, { revision: '7' }, execution), /REACTIVE_ACTION_BINDING_INVALID/u);
  assert.throws(() => observeBoundAction(counterfeit, { operationId: 'forged' }), /REACTIVE_ACTION_BINDING_INVALID/u);
  const invocation = invokeBoundAction(reference, { revision: '7' }, execution);
  assert.equal(admissions, 1);
  assert.equal(executions, 0);
  const first: string[] = [], second: string[] = [];
  invocation.activity$.subscribe(value => first.push(value.state));
  invocation.activity$.subscribe(value => second.push(value.state));
  admission.resolve();
  await started.promise;
  action.release();
  await assert.rejects(action.invoke({ revision: '7' }, participant('prepare-two')).result, /REACTIVE_ACTION_BINDING_RELEASED/u);
  operation.resolve('accepted-operation');
  assert.deepEqual(await invocation.result, { operationId: 'accepted-operation' });
  assert.deepEqual(first, ['pending', 'running', 'returned']);
  assert.deepEqual(second, first);
  assert.deepEqual(await firstValueFrom(invocation.activity$), { state: 'returned', observation: { operationId: 'accepted-operation' } });
  assert.deepEqual(action.observe({ operationId: 'accepted-operation' }), { operationId: 'accepted-operation' });
  assert.deepEqual(observeBoundAction(reference, { operationId: 'native-final-result' }), { operationId: 'native-final-result' });
  assert.equal(executions, 1);
  assert.deepEqual({ scopeParses, inputParses, outputParses }, { scopeParses: 1, inputParses: 1, outputParses: 1 });
});

test('准入仍在等待时释放声明，准入完成也不能再启动业务执行', async () => {
  const entered = Promise.withResolvers<void>();
  const admission = Promise.withResolvers<void>();
  let executions = 0;
  const definition = defineAction({ name: 'pending_admission', description: '等待准入', scope: z.null(), input: z.null(), output: z.boolean() });
  const definitionImplementation = implementAction(definition, {
    execute() { executions++; return true; }
  });
  const action = bindAction(definitionImplementation, null, { async admit() { entered.resolve(); await admission.promise; } });
  const invocation = action.invoke(null, participant('pending-one'));
  await entered.promise;
  action.release();
  admission.resolve();
  await assert.rejects(invocation.result, /REACTIVE_ACTION_BINDING_RELEASED/u);
  assert.equal(executions, 0);
  const settled = await firstValueFrom(invocation.activity$);
  assert.equal(settled.state, 'failed');
  if (settled.state === 'failed') assert.equal(settled.phase, 'admission');
});

test('准入阶段和同步业务返回后排队的微任务都不能再使用本次执行控制', async () => {
  const microtask = Promise.withResolvers<void>();
  const hostCalls: string[] = [];
  const prompts = [{ id: 'delivery-mode', question: '如何接收交付资料？', options: [{ label: '站内' }, { label: '邮件' }] }];
  const wait: WaitReference = { id: 'late-control', operation: { commandId: 'original-command' } };
  const execution: ActionExecutionContext = { ...participant('sync-body'),
    async wait() { hostCalls.push('wait'); },
    completeGoal() { hostCalls.push('completeGoal'); },
    async askHuman() { hostCalls.push('askHuman'); return { answers: [{ id: 'delivery-mode', selected: ['站内'] }] }; },
  };
  const inactive = async (call: ActionCallContext<null>) => {
    assert.throws(() => call.wait(wait), /REACTIVE_ACTION_CONTROL_INACTIVE/u);
    assert.throws(() => call.completeGoal(), /REACTIVE_ACTION_CONTROL_INACTIVE/u);
    await assert.rejects(call.askHuman(prompts), /REACTIVE_ACTION_CONTROL_INACTIVE/u);
  };
  const definition = defineAction({ name: 'synchronous_record', description: '同步记录本次资料',
    scope: z.null(), input: z.null(), output: z.string() });
  let executions = 0;
  const action = bindAction(implementAction(definition, { execute(_input, call) {
    executions++;
    queueMicrotask(() => { void inactive(call).then(microtask.resolve, microtask.reject); });
    return 'recorded';
  } }), null, { async admit(_input, call) { await inactive(call); } });
  try {
    const invocation = action(null, execution);
    const [result] = await Promise.all([invocation.result, microtask.promise]);
    assert.equal(result, 'recorded');
    assert.equal(executions, 1);
    assert.deepEqual(hostCalls, [], '同步 execute 的返回就是控制寿命终点，不能延长到下一微任务');
  } finally { action.release(); }
});

test('异步prepare期间撤销执行权会拒绝副作用，已进入execute的业务保留原结果', { timeout: 15_000 }, async () => {
  const preparing = Promise.withResolvers<void>(), prepare = Promise.withResolvers<void>();
  const started = Promise.withResolvers<void>(), finish = Promise.withResolvers<void>();
  const denied = new Error('EXECUTION_AUTHORITY_REVOKED');
  let allowed = true, executions = 0;
  const operation = defineOperation({ kind: 'prepared-execution', version: 1,
    command: z.string(), reference: z.string(), state: z.boolean(), settled: value => value ? value : undefined });
  const owner = implementOperation(operation, {
    locate: () => ({ status: 'unknown' }),
    observe: () => of({ status: 'ready', generation: 1, value: true, observedAt: Date.now() }),
  });
  const definition = defineAction({ name: 'prepared_execution', description: '准备完成后开始业务',
    scope: z.null(), input: z.string(), output: z.string(),
    operation: { definition: operation, command: input => input, reference: output => output } });
  const action = bindAction(implementAction(definition, { operationOwner: owner, async execute(input) {
    executions++;
    if (input === 'revoked-during-prepare') throw new Error('REVOKED_EXECUTION_STARTED');
    started.resolve(); await finish.promise; return input;
  } }), null, {
    admit() { if (!allowed) throw denied; },
    async operation(event) {
      if (event.phase === 'prepare' && event.command === 'revoked-during-prepare') {
        preparing.resolve(); await prepare.promise;
      }
    },
  });
  try {
    const original = participant('revoked-during-prepare');
    guardActionExecution(original, () => { if (!allowed) throw denied; });
    const blocked = action('revoked-during-prepare', original);
    const states: string[] = [];
    blocked.activity$.subscribe(value => states.push(value.state));
    await preparing.promise;
    allowed = false; prepare.resolve();
    await assert.rejects(blocked.result, error => error === denied);
    assert.equal(executions, 0);
    assert.deepEqual(states, ['pending', 'failed']);

    allowed = true;
    const acceptedExecution = participant('accepted-before-revoke');
    guardActionExecution(acceptedExecution, () => { if (!allowed) throw denied; });
    const accepted = action('accepted-before-revoke', acceptedExecution);
    await started.promise;
    allowed = false; finish.resolve();
    assert.equal(await accepted.result, 'accepted-before-revoke');
    assert.equal((await accepted.operation).reference, 'accepted-before-revoke');
    assert.equal(executions, 1);
  } finally { prepare.resolve(); finish.resolve(); action.release(); }
});

test('准入和业务等待期间修改调用者对象不能改写目标与原调用归属，信号和回调仍属于原宿主', async () => {
  const admission = Promise.withResolvers<void>();
  const entered = Promise.withResolvers<void>();
  const operation = Promise.withResolvers<void>();
  const controller = new AbortController(), replacement = new AbortController();
  const target = { merchantId: 'merchant', selection: { revision: 1 }, channels: ['direct'] };
  const source = { kind: 'participant' as const, actorId: 'operator', participant: 'human' as const, entrypoint: 'authenticated-http' };
  const inputScope = { merchantId: 'merchant', roles: ['prepare'] };
  const input = { sourceSessionId: 'conversation', messageId: 'original-message', requestId: 'original-request', source: 'user', intent: 'goal' as const, scope: inputScope, actorId: 'operator', participant: 'human' as const };
  const inputs = [input];
  const goal = { id: 'original-goal', revision: 1 };
  const continuation = { sessionId: 'delivery', inputs, goal };
  const waits: string[] = [];
  let completed = 0, replacements = 0;
  const execution = { invocationId: 'original-invocation', sessionId: 'delivery', source, inputs, continuation, signal: controller.signal,
    async wait(reference: WaitReference): Promise<void> { assert.equal(this, execution); waits.push(reference.id); },
    completeGoal(): void { assert.equal(this, execution); completed++; },
  };
  const definition = defineAction({ name: 'owned_context', description: '使用原调用的正式归属',
    scope: z.object({ merchantId: z.string(), selection: z.object({ revision: z.number() }), channels: z.array(z.string()) }).transform(() => target),
    input: z.null(), output: z.object({ merchantId: z.string(), actorId: z.string(), goalRevision: z.number() }),
  });
  const definitionImplementation = implementAction(definition, {
    async execute(_input, call) {
      entered.resolve();
      await operation.promise;
      assert.equal(call.signal, controller.signal);
      assert.equal(call.invocationId, 'original-invocation');
      assert.deepEqual(call.scope, { merchantId: 'merchant', selection: { revision: 1 }, channels: ['direct'] });
      assert.deepEqual(call.inputs, [{ sourceSessionId: 'conversation', messageId: 'original-message', requestId: 'original-request', source: 'user', intent: 'goal',
        scope: { merchantId: 'merchant', roles: ['prepare'] }, actorId: 'operator', participant: 'human' }]);
      assert.deepEqual(call.continuation, { sessionId: 'delivery', inputs: call.inputs, goal: { id: 'original-goal', revision: 1 } });
      assert.equal(call.source.kind, 'participant');
      if (call.source.kind !== 'participant' || !call.continuation?.goal) throw new Error('原调用归属缺失');
      assert.equal(Reflect.set(call.scope.selection, 'revision', 99), false);
      await call.wait({ id: 'original-wait', operation: { id: 'operation' } });
      call.completeGoal();
      return { merchantId: call.scope.merchantId, actorId: call.source.actorId, goalRevision: call.continuation.goal.revision };
    }
  });
  const action = bindAction(definitionImplementation, target, { async admit(_input, call, original) {
    assert.equal(original, execution);
    await admission.promise;
    assert.equal(call.source.kind, 'participant');
    if (call.source.kind === 'participant') assert.equal(call.source.actorId, 'operator');
  } });
  const invocation = action.invoke(null, execution);
  target.selection.revision = 2; target.channels.push('changed');
  source.actorId = 'different-operator'; inputScope.roles.push('admin'); input.messageId = 'different-message'; goal.revision = 2;
  execution.signal = replacement.signal;
  execution.wait = async () => { replacements++; };
  execution.completeGoal = () => { replacements++; };
  replacement.abort(new Error('替换信号无权取消原调用'));
  admission.resolve();
  await entered.promise;
  target.merchantId = 'other-merchant'; source.entrypoint = 'different-entrypoint'; goal.revision = 3;
  inputs.push({ ...input, messageId: 'foreign-message' });
  execution.invocationId = 'different-invocation';
  operation.resolve();
  assert.deepEqual(await invocation.result, { merchantId: 'merchant', actorId: 'operator', goalRevision: 1 });
  assert.equal(invocation.id, 'original-invocation');
  assert.deepEqual(invocation.source, { kind: 'participant', actorId: 'operator', participant: 'human', entrypoint: 'authenticated-http' });
  assert.deepEqual(waits, ['original-wait']);
  assert.equal(completed, 1);
  assert.equal(replacements, 0);
  assert.equal(Object.isFrozen(target), false);
  assert.equal(Object.isFrozen(source), false);
});

test('公开观察投影失败不改写已经完成的业务结果，晚订阅仍能区分两个事实', async () => {
  let accepted = 0;
  const projectionError = new Error('公开投影失败');
  const definition = defineAction({ name: 'projection_failure', description: '提交原业务后投影', scope: z.null(), input: z.null(), output: z.string(),
    observe() { throw projectionError; },
  });
  const definitionImplementation = implementAction(definition, {
    execute() { accepted++; return 'accepted-operation'; }
  });
  const invocation = bindAction(definitionImplementation, null, { admit() {} }).invoke(null, participant('projection-one'));
  assert.equal(await invocation.result, 'accepted-operation');
  assert.equal(accepted, 1);
  const settled = await firstValueFrom(invocation.activity$);
  assert.equal(settled.state, 'returned');
  if (settled.state === 'returned') assert.equal(settled.observationError, projectionError);
  const current = await invocation.activity.read();
  assert.equal(current.state, 'returned');
  if (current.state === 'returned') assert.equal(current.observationError, projectionError);
});

test('准入等待期间声明失活拒绝旧调用，重新激活只允许新的显式调用', async () => {
  const admission = Promise.withResolvers<void>();
  let active = true, admissions = 0, executions = 0;
  const definition = defineAction({ name: 'commit_active', description: '只在声明激活时受理', scope: z.null(), input: z.null(), output: z.number() });
  const definitionImplementation = implementAction(definition, {
    execute() { return ++executions; }
  });
  const action = bindAction(definitionImplementation, null, {
    active: () => active,
    async admit() { if (++admissions === 1) await admission.promise; },
  });
  const first = action.invoke(null, participant('inactive-one'));
  active = false;
  admission.resolve();
  await assert.rejects(first.result, /REACTIVE_ACTION_BINDING_INACTIVE/u);
  assert.equal(executions, 0);
  active = true;
  assert.equal(executions, 0);
  assert.equal(await action.invoke(null, participant('active-two')).result, 1);
  assert.equal(admissions, 2);
  const settled = await firstValueFrom(first.activity$);
  assert.equal(settled.state, 'failed');
  assert.equal(executions, 1);
});

test('绑定的热调用流交付同一个 Invocation，单次失败和观察退出不影响后续调用及已开始结果', async () => {
  const firstResult = Promise.withResolvers<number>();
  const failure = new Error('本次正式执行失败');
  const definition = defineAction({ name: 'observable_calls', description: '按真实调用组合后续行为',
    scope: z.null(), input: z.number().positive(), output: z.number() });
  let executions = 0;
  const implementation = implementAction(definition, {
    execute(value) {
      executions++;
      if (value === 1) return firstResult.promise;
      if (value === 2) throw failure;
      return value * 2;
    },
  });
  const action = bindAction(implementation, null, { admit() {} });
  const firstObserved: ActionInvocation<typeof definition>[] = [];
  const secondObserved: ActionInvocation<typeof definition>[] = [];
  let completed = false, observerErrors = 0;
  const firstObserver = action.invocations$.subscribe(value => firstObserved.push(value));
  const first = action(1, participant('first'));
  assert.equal(firstObserved[0], first);
  action.invocations$.subscribe({ next: value => secondObserved.push(value),
    complete() { completed = true; }, error() { observerErrors++; } });
  assert.deepEqual(secondObserved, []);
  const invalid = action(-1, participant('invalid'));
  assert.equal(secondObserved[0], invalid);
  await assert.rejects(invalid.result);
  const failed = action(2, participant('failed'));
  await assert.rejects(failed.result, error => error === failure);
  const successful = invokeBoundAction(action, 3, participant('successful'));
  assert.equal(secondObserved[2], successful);
  assert.equal(await successful.result, 6);
  firstObserver.unsubscribe();
  action.release();
  assert.equal(completed, true);
  firstResult.resolve(8);
  assert.equal(await first.result, 8);
  await assert.rejects(action(4, participant('released')).result, /REACTIVE_ACTION_BINDING_RELEASED/u);
  assert.equal(observerErrors, 0);
  assert.equal(executions, 3);
  assert.deepEqual(firstObserved, [first, invalid, failed, successful]);
  assert.deepEqual(secondObserved, [invalid, failed, successful]);
  let lateValues = 0, lateComplete = false;
  action.invocations$.subscribe({ next() { lateValues++; }, complete() { lateComplete = true; } });
  assert.equal(lateValues, 0);
  assert.equal(lateComplete, true);
});

test('持久委托的原输入在异步准入前自有捕获，普通 Action 仍可接收非 JSON 参数', async () => {
  const admitted = Promise.withResolvers<void>();
  const raw = { date: '2026-10-27T00:00:00.000Z', request: { revision: 1 } };
  const definition = defineAction({ name: 'captured_input', description: '保存原始委托输入', scope: z.string().transform(merchantId => ({ merchantId })),
    input: z.object({ date: z.string().transform(value => new Date(value)), request: z.object({ revision: z.number() }) }),
    output: z.number() });
  const implementation = implementAction(definition, {
    execute(input, call) {
      const captured = actionInvocationArguments(call);
      assert.deepEqual(captured, { input: { date: '2026-10-27T00:00:00.000Z', request: { revision: 1 } }, scope: 'merchant-a' });
      assert.deepEqual(definition.scope.parse(captured.scope), call.scope);
      assert.equal(input.date.toISOString(), '2026-10-27T00:00:00.000Z');
      return input.request.revision;
    },
  });
  captureActionArguments(implementation, { input: value => z.json().parse(value), scope: value => z.json().parse(value) });
  const action = bindAction(implementation, 'merchant-a', { async admit(input, call) {
    await admitted.promise;
    assert.equal(input.request.revision, 1);
    const snapshot = actionInvocationArguments(call).input;
    assert.deepEqual(snapshot, { date: '2026-10-27T00:00:00.000Z', request: { revision: 1 } });
    assert.equal(Reflect.set(snapshot as object, 'date', 'changed'), false);
  } });
  const invocation = action(raw, participant('capture-original'));
  raw.date = '2027-01-01T00:00:00.000Z'; raw.request.revision = 2;
  admitted.resolve();
  assert.equal(await invocation.result, 1);
  assert.equal(Object.isFrozen(raw), false);

  const callback = defineAction({ name: 'local_callback', description: '调用本地函数参数', scope: z.date().transform(date => ({ at: date.toISOString() })),
    input: z.custom<() => number>(value => typeof value === 'function'), output: z.number() });
  const local = bindAction(implementAction(callback, { execute(run, call) {
    assert.equal(call.scope.at, '2026-10-27T00:00:00.000Z'); return run();
  } }), new Date('2026-10-27T00:00:00.000Z'), { admit(_input, call) {
    assert.throws(() => actionInvocationArguments(call), /REACTIVE_ACTION_ARGUMENTS_NOT_CAPTURED/u);
  } });
  assert.equal(await local(() => 19, participant('callback')).result, 19);
  action.release(); local.release();
});
