import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate } from 'node:timers/promises';
import { EMPTY, EmptyError, Subject, filter, firstValueFrom, of } from 'rxjs';
import { z } from 'zod';
import { bindAction, defineAction, implementAction, type ActionExecutionContext } from '@hanger-source/dsh-reactive';
import { attachOperation, defineOperation, implementOperation } from '@hanger-source/dsh-reactive';
import { observeResource } from '@hanger-source/dsh-reactive';
import type { Observation } from '@hanger-source/dsh-reactive';

test('同次受理回执派生操作观察，持久准备被等待，声明退出不丢回执且多个观察不重复执行', async () => {
  let stateParses = 0;
  const stateSchema = z.discriminatedUnion('status', [
    z.object({ status: z.literal('running') }),
    z.object({ status: z.literal('failed'), reason: z.string() }),
  ]).transform(value => { stateParses++; return value; });
  let state: z.input<typeof stateSchema> = { status: 'running' };
  const changed = new Subject<void>();
  const queryEntered = Promise.withResolvers<void>();
  let queries = 0, executions = 0, outputParses = 0;
  const operation = defineOperation({ kind: 'prepare-merchant', version: 1,
    command: z.object({ merchantId: z.string(), commandId: z.string() }).strict(),
    reference: z.object({ merchantId: z.string(), operationId: z.string() }).strict(),
    state: stateSchema, settled: value => value.status === 'running' ? undefined : value });
  const owner = implementOperation(operation, {
    locate(command) { return { status: 'accepted', reference: { merchantId: command.merchantId, operationId: 'operation-a' }, state }; },
    observe(reference, access) {
      assert.equal(reference.merchantId, 'merchant-a');
      assert.deepEqual(access.source, { kind: 'participant', participant: 'human', actorId: 'operator-a', entrypoint: 'sdk' });
      queries++; queryEntered.resolve();
      return observeResource({ key$: of(reference.operationId), read: () => state, invalidations: () => changed });
    },
  });
  const definition = defineAction({ name: 'prepare_merchant', description: '正式受理操作',
    scope: z.object({ merchantId: z.string() }), input: z.object({ commandId: z.string() }),
    output: z.object({ operation: operation.reference }).transform(receipt => { outputParses++; return receipt; }),
    operation: { definition: operation,
      command: (input, scope) => ({ commandId: input.commandId, merchantId: scope.merchantId }), reference: receipt => receipt.operation },
  });
  const preparing = Promise.withResolvers<void>(), prepared = Promise.withResolvers<void>();
  const executing = Promise.withResolvers<void>(), returned = Promise.withResolvers<z.input<typeof definition.output>>();
  const implementation = implementAction(definition, { operationOwner: owner, execute() {
    executions++; executing.resolve(); return returned.promise;
  } });
  const execution: ActionExecutionContext = { invocationId: 'human-invocation', sessionId: 'session-a',
    source: { kind: 'participant', participant: 'human', actorId: 'operator-a', entrypoint: 'sdk' },
    inputs: [], continuation: null, signal: new AbortController().signal };
  const stages: string[] = [];
  const action = bindAction(implementation, { merchantId: 'merchant-a' }, {
    execution: () => execution, admit() {},
    async operation(event, original) {
      assert.equal(original, execution); stages.push(event.phase);
      if (event.phase === 'prepare') { preparing.resolve(); await prepared.promise; }
    },
  });
  const invocation = action({ commandId: 'prepare-a' });
  const waiter = new AbortController(), waitReason = new Error('LEAVE_ONE_RECEIPT_WAIT');
  const cancelledWait = assert.rejects(invocation.wait({ signal: waiter.signal }), error => error === waitReason);
  const receiptWait = invocation.wait();
  await preparing.promise;
  assert.equal(executions, 0);
  waiter.abort(waitReason);
  await cancelledWait;
  assert.equal(execution.signal.aborted, false);
  prepared.resolve(); await executing.promise;
  action.release();
  returned.resolve({ operation: { merchantId: 'merchant-a', operationId: 'operation-a' } });
  const receipt = await invocation.result;
  assert.equal(await receiptWait, receipt);
  const handle = await invocation.operation;
  assert.equal(handle.observations$, handle.state.observations$);
  assert.deepEqual(handle.reference, receipt.operation);
  assert.deepEqual(stages, ['prepare', 'receipt']);
  assert.equal(outputParses, 1);
  assert.equal(queries, 0);
  const observations: string[][] = [[], []];
  const first = handle.observations$.subscribe(value => observations[0]!.push(value.status));
  const second = handle.observations$.subscribe(value => observations[1]!.push(value.status));
  try {
    await queryEntered.promise;
    await firstValueFrom(handle.observations$.pipe(filter(value => value.status === 'ready')));
    assert.equal(queries, 1);
    assert.deepEqual(await handle.state.read(), { status: 'running' }); assert.equal(stateParses, 1);
    const settlement = handle.settled();
    state = { status: 'failed', reason: '业务 owner 确认准备失败' };
    changed.next();
    assert.deepEqual(await settlement, state);
    assert.deepEqual(await handle.state.read(), state); assert.equal(stateParses, 2);
    assert.equal(executions, 1);
    assert.equal(outputParses, 1);
    assert.deepEqual(observations[0], observations[1]);
    const located = await action.locate({ merchantId: 'merchant-a', commandId: 'prepare-a' });
    assert.equal(located.status, 'accepted');
    assert.equal(executions, 1);
    assert.deepEqual(stages, ['prepare', 'receipt', 'access']);
    await assert.rejects(action({ commandId: 'prepare-again' }).result, /REACTIVE_ACTION_BINDING_RELEASED/u);
    assert.equal(executions, 1);
  } finally { first.unsubscribe(); second.unsubscribe(); }
  assert.equal(handle.state.peek().status, 'pending');
});

test('query失败或等待者取消只结束读取，之后可重读原操作并取得领域取消终态', async () => {
  const stateSchema = z.discriminatedUnion('status', [
    z.object({ status: z.literal('running') }), z.object({ status: z.literal('cancelled'), by: z.string() }),
  ]);
  let state: z.input<typeof stateSchema> = { status: 'running' };
  const queryFailure = new Error('业务查询暂时不可用');
  const waiting = Promise.withResolvers<void>();
  let mode: 'error' | 'waiting' | 'ready' = 'error';
  let readingSignal: AbortSignal | undefined;
  const definition = defineOperation({ kind: 'owner-operation', version: 1, command: z.string(), reference: z.string(),
    state: stateSchema, settled: value => value.status === 'running' ? undefined : value });
  const owner = implementOperation(definition, {
    locate() { return { status: 'accepted', reference: 'operation-a', state }; },
    observe(reference) {
      return observeResource({ key$: of(reference), read(_id, signal) {
        if (mode === 'error') throw queryFailure;
        if (mode === 'ready') return state;
        readingSignal = signal; waiting.resolve();
        return new Promise<z.input<typeof stateSchema>>(() => {});
      } });
    },
  });
  const access = { sessionId: 'session-a', source: { kind: 'participant' as const, participant: 'service' as const,
    actorId: 'operation-reader', entrypoint: 'program' }, signal: new AbortController().signal };
  const handle = attachOperation(owner, 'operation-a', access);
  await assert.rejects(handle.settled(), error => error === queryFailure);
  assert.deepEqual(state, { status: 'running' });
  mode = 'waiting';
  const cancelled = new AbortController(), reason = new Error('只结束本次等待');
  const settlement = handle.settled({ signal: cancelled.signal });
  await waiting.promise;
  cancelled.abort(reason);
  await assert.rejects(settlement, error => error === reason);
  assert.equal(readingSignal?.aborted, true);
  assert.deepEqual(state, { status: 'running' });
  mode = 'ready'; state = { status: 'cancelled', by: 'business-owner' };
  assert.deepEqual(await handle.settled(), state);
});

test('execute已持久受理而原调用中断时保留unknown语义，可按原command查回并重连而不重执行', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-owner-'));
  const receiptFile = join(directory, 'accepted.json');
  const receiptSchema = z.object({ command: z.string(), operationId: z.string(), value: z.number() }).strict();
  const identity = z.codec(z.string(), z.object({ id: z.string() }).strict(), { decode: id => ({ id }), encode: value => value.id });
  const definition = defineOperation({ kind: 'accepted-before-abort', version: 1, command: identity, reference: identity,
    state: z.object({ status: z.literal('complete'), value: z.number() }), settled: state => state });
  let executions = 0;
  const bodyEntered = Promise.withResolvers<void>(), bodyReturned = Promise.withResolvers<string>();
  const readReceipt = async () => receiptSchema.parse(JSON.parse(await readFile(receiptFile, 'utf8')));
  const locate = async (command: { readonly id: string }) => {
    assert.equal(Object.isFrozen(command), true);
    const receipt = await readReceipt();
    return receipt.command === command.id ? { status: 'accepted' as const, reference: receipt.operationId,
      state: { status: 'complete' as const, value: receipt.value } } : { status: 'unknown' as const };
  };
  const observe = (reference: { readonly id: string }) => observeResource({ key$: of(reference.id), async read(operationId) {
    const receipt = await readReceipt();
    assert.equal(receipt.operationId, operationId);
    return { status: 'complete' as const, value: receipt.value };
  } });
  const owner = implementOperation(definition, { locate, observe });
  const command = defineAction({ name: 'accept_then_abort', description: '验证丢回执后的查回', scope: z.null(), input: z.string(), output: z.string(),
    operation: { definition, command: input => input, reference: receipt => receipt } });
  const implementation = implementAction(command, { operationOwner: owner, async execute(input) {
    executions++;
    await writeFile(receiptFile, JSON.stringify({ command: input, operationId: 'operation:' + input, value: 7 }));
    bodyEntered.resolve(); await bodyReturned.promise; return 'operation:' + input;
  } });
  const controller = new AbortController();
  const source = { kind: 'participant' as const, participant: 'human' as const, actorId: 'operator-a', entrypoint: 'sdk' };
  const original: ActionExecutionContext = { invocationId: 'original', sessionId: 'session-a', source,
    inputs: [], continuation: null, signal: controller.signal };
  const action = bindAction(implementation, null, { admit() {} });
  const invocation = action('stable-business-command', original);
  await bodyEntered.promise;
  const waiting = new AbortController(), waitingReason = new Error('LEAVE_COMMITTED_RECEIPT_WAIT');
  const cancelledWait = assert.rejects(invocation.wait({ signal: waiting.signal }), error => error === waitingReason);
  const remainingWait = invocation.wait();
  waiting.abort(waitingReason); await cancelledWait;
  assert.equal(original.signal.aborted, false);
  assert.deepEqual(await readReceipt(), { command: 'stable-business-command', operationId: 'operation:stable-business-command', value: 7 });
  const reason = new Error('调用连接已中断'); controller.abort(reason); bodyReturned.resolve('returned');
  await assert.rejects(invocation.result, error => error === reason);
  await assert.rejects(remainingWait, error => error === reason);
  await assert.rejects(invocation.operation, error => error === reason);
  // 新 owner 从业务存储重建查询能力，不依赖前一次 execute 的闭包保存受理结果。
  const restoredOwner = implementOperation(definition, { locate, observe });
  const restoredImplementation = implementAction(command, { operationOwner: restoredOwner,
    execute() { throw new Error('RECOVERY_MUST_NOT_EXECUTE'); } });
  const restored = bindAction(restoredImplementation, null, { admit() {} });
  const reader: ActionExecutionContext = { ...original, invocationId: 'fresh-read', signal: new AbortController().signal };
  const located = await restored.locate('stable-business-command', reader);
  assert.equal(located.status, 'accepted');
  if (located.status !== 'accepted') throw new Error('ACCEPTED_OPERATION_REQUIRED');
  assert.equal(located.reference, 'operation:stable-business-command');
  assert.deepEqual(await restored.attach(located.reference, reader).settled(), { status: 'complete', value: 7 });
  await setImmediate();
  assert.equal(executions, 1);
});

test('owner资源空完成、异步pending后完成和来源异常均向观察者发布同源error，settled不会永久等待', async () => {
  const state = z.discriminatedUnion('status', [
    z.object({ status: z.literal('running') }), z.object({ status: z.literal('complete') }),
  ]);
  const definition = defineOperation({ kind: 'resource-endings', version: 1, command: z.string(), reference: z.string(),
    state, settled: value => value.status === 'running' ? undefined : value });
  for (const mode of ['empty', 'pending-complete', 'ready-not-terminal', 'source-error'] as const) {
    const source = new Subject<Observation<z.input<typeof state>>>();
    const entered = Promise.withResolvers<void>();
    const failure = new Error('owner来源已终止');
    let connections = 0;
    const owner = implementOperation(definition, {
      locate: () => ({ status: 'accepted', reference: 'operation-a', state: { status: 'running' } }),
      observe() { connections++; entered.resolve(); return mode === 'empty' ? EMPTY : source; },
    });
    const handle = attachOperation(owner, 'operation-a', { sessionId: 'session-a', signal: new AbortController().signal,
      source: { kind: 'participant', actorId: 'reader', participant: 'service', entrypoint: 'program' } });
    let observed: Observation<z.output<typeof state>> | undefined;
    const observer = handle.observations$.subscribe(value => { observed = value; });
    const settlement = handle.settled();
    void settlement.catch(() => {});
    try {
      await entered.promise;
      if (mode !== 'empty') {
        source.next(mode === 'ready-not-terminal'
          ? { status: 'ready', generation: 7, value: { status: 'running' }, observedAt: Date.now() }
          : { status: 'pending', generation: 7 });
        await setImmediate();
        if (mode === 'pending-complete' || mode === 'ready-not-terminal') source.complete();
        else source.error(failure);
      }
      await assert.rejects(settlement, error => {
        assert.equal(observed?.status, 'error');
        if (observed?.status !== 'error') return false;
        assert.equal(error, observed.error);
        return mode === 'source-error' ? error === failure : error instanceof EmptyError;
      });
      assert.equal(connections, 1);
    } finally { observer.unsubscribe(); }
  }
});
