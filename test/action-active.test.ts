import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { BehaviorSubject, Observable, filter, firstValueFrom, lastValueFrom, of } from 'rxjs';
import { EligibilityError, Scope, bindAction, combineResources, createResource, defineAction, defineOperation, implementAction,
  implementOperation, mapResource, z, type Eligibility, type Observation, type ActionExecutionContext } from '@hanger-source/dsh-reactive';
import { invokeBoundAction } from '../packages/core/src/actions/contract.ts';

const eligibilityExecution = (invocationId: string): ActionExecutionContext => ({ invocationId, sessionId: 'eligibility',
  source: { kind: 'participant', actorId: 'operator', participant: 'human', entrypoint: 'cli' },
  signal: new AbortController().signal, inputs: [], continuation: null });

test('绑定release与原调用取消在不合作准入返回前结案，迟到准入不能开始真实写入', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-action-admission-'));
  for (const mode of ['release', 'cancel', 'caller'] as const) {
    const file = join(directory, mode);
    const entered = Promise.withResolvers<AbortSignal>(), gate = Promise.withResolvers<void>();
    const controller = new AbortController(), caller = new AbortController(), reason = new Error('CALL_CANCELLED');
    const original: ActionExecutionContext = { invocationId: mode, sessionId: 'admission',
      source: { kind: 'participant', actorId: 'operator', participant: 'human', entrypoint: 'cli' },
      signal: controller.signal, inputs: [], continuation: null };
    const action = bindAction(implementAction({ name: 'write_file', description: '写入本次目标', scope: z.null(),
      input: z.null(), output: z.string(), async execute() { await writeFile(file, 'written'); return file; } }), null, {
      async admit(_input, _call, execution, signal) {
        assert.equal(execution, original);
        entered.resolve(signal); await gate.promise;
      },
    });
    const invocation = mode === 'caller' ? invokeBoundAction(action, null, original, caller.signal) : action(null, original);
    const signal = await entered.promise;
    const rejected = assert.rejects(invocation.result, mode === 'release' ? /REACTIVE_ACTION_BINDING_RELEASED/u : reason);
    try {
      if (mode === 'release') action.release();
      else if (mode === 'caller') caller.abort(reason);
      else controller.abort(reason);
      await rejected;
      assert.equal(signal.aborted, true);
      if (mode === 'caller') assert.equal(original.signal.aborted, false, '调用者退出准入不撤销宿主签发的执行身份');
      assert.deepEqual(await action.activeInvocations.read(), []);
      gate.resolve();
      await new Promise<void>(resolve => setImmediate(resolve));
      await assert.rejects(readFile(file), { code: 'ENOENT' });
    } finally { gate.resolve(); action.release(); }
  }
});

test('绑定当前调用集合包含并发准入与执行，晚订和退订不丢调用，release后原业务独立结算', async () => {
  const firstStarted = Promise.withResolvers<void>(), firstResult = Promise.withResolvers<string>();
  const secondAdmission = Promise.withResolvers<void>(), secondStarted = Promise.withResolvers<void>(), secondResult = Promise.withResolvers<string>();
  const execution = (invocationId: string): ActionExecutionContext => ({ invocationId, sessionId: 'merchant-session',
    source: { kind: 'participant', actorId: 'merchant-operator', participant: 'human', entrypoint: 'authenticated-sdk' },
    inputs: [], signal: new AbortController().signal, continuation: null });
  const effects: string[] = [];
  const definition = defineAction({ name: 'prepare', description: '处理当前商家本次资料', scope: z.object({ merchantId: z.string() }),
    input: z.enum(['first', 'second']), output: z.string() });
  const action = bindAction(implementAction(definition, { async execute(input, call) {
    effects.push(input);
    if (input === 'first') { firstStarted.resolve(); return firstResult.promise; }
    secondStarted.resolve();
    const result = await secondResult.promise;
    call.signal.throwIfAborted();
    return result;
  } }), { merchantId: 'merchant-a' }, { async admit(input) { if (input === 'second') await secondAdmission.promise; } });
  assert.deepEqual(await firstValueFrom(action.activeInvocations$), []);
  const first = action('first', execution('first'));
  await firstStarted.promise;
  const caller = new AbortController(), secondExecution = execution('second');
  const second = invokeBoundAction(action, 'second', secondExecution, caller.signal);
  const late = await action.activeInvocations.read();
  assert.deepEqual(late, [first, second]);
  assert.deepEqual(await firstValueFrom(second.activity$), { state: 'pending' });
  assert.deepEqual(await second.activity.read(), { state: 'pending' });
  assert.deepEqual(effects, ['first']);
  const waiter = new AbortController(), reason = new Error('LEAVE_ONE_ACTION_WAIT');
  const cancelledWait = assert.rejects(second.wait({ signal: waiter.signal }), error => error === reason);
  const remainingWait = second.wait();
  waiter.abort(reason);
  await cancelledWait;
  assert.deepEqual(await second.activity.read(), { state: 'pending' }, '等待者取消不撤准入，也不制造调用失败');
  const seen: string[][] = [];
  const subscription = action.activeInvocations$.subscribe(current => seen.push(current.map(invocation => invocation.id)));
  firstResult.resolve('first-recorded');
  assert.equal(await first.result, 'first-recorded');
  assert.equal((await lastValueFrom(first.activity$)).state, 'returned');
  assert.equal((await first.activity.read()).state, 'returned');
  assert.deepEqual(await firstValueFrom(action.activeInvocations$), [second]);
  assert.deepEqual(late, [first, second], '此前交付的快照不会随当前集合删除成员');
  const runningObserved: boolean[] = [];
  const receipt = second.activity$.subscribe(activity => {
    if (activity.state === 'running') runningObserved.push(effects.includes('second'));
  });
  const running = firstValueFrom(combineResources({ active: action.activeInvocations, call: second.activity }).pipe(
    filter(observation => observation.status === 'ready' && observation.value.call.state === 'running'),
  ));
  secondAdmission.resolve();
  await secondStarted.promise;
  caller.abort(new Error('CALLER_LEFT_AFTER_EXECUTE'));
  assert.equal(secondExecution.signal.aborted, false, '进入业务后调用者signal不成为真正execution的取消authority');
  const current = await running;
  assert.equal(current.status, 'ready');
  if (current.status === 'ready') assert.deepEqual(current.value.active, [second]);
  assert.deepEqual(runningObserved, [true], 'running通知前业务必须已经进入，观察者同步重入不能抢先改变准入');
  receipt.unsubscribe();
  assert.equal((await second.activity.read()).state, 'running');
  action.release();
  await assert.rejects(action('first', execution('after-release')).result, /REACTIVE_ACTION_BINDING_RELEASED/u);
  assert.deepEqual(await action.activeInvocations.read(), [second]);
  const drained = lastValueFrom(action.activeInvocations.values$);
  let ended = false;
  void drained.then(() => { ended = true; });
  await new Promise<void>(resolve => setImmediate(resolve));
  assert.equal(ended, false, 'release封住新准入，集合仍等原业务实际结算后完成');
  subscription.unsubscribe();
  secondResult.resolve('second-recorded');
  assert.equal(await second.result, 'second-recorded');
  assert.equal(await remainingWait, 'second-recorded');
  assert.equal(await second.wait(), 'second-recorded', '原调用的后续等待不继承已退出消费者或绑定的signal');
  await assert.rejects(second.wait({ signal: waiter.signal }), error => error === reason);
  assert.equal((await lastValueFrom(second.activity.observations$)).status, 'ready');
  assert.equal((await second.activity.read()).state, 'returned');
  assert.deepEqual(await drained, []);
  assert.deepEqual(await action.activeInvocations.read(), []);
  const empty = action.activeInvocations.peek();
  assert.equal(empty.status, 'ready');
  if (empty.status === 'ready') assert.deepEqual(empty.value, []);
  assert.deepEqual(effects, ['first', 'second']);
  assert.deepEqual(seen, [['first', 'second'], ['second']]);
});

test('冷业务事实先完成读取，异步准入期间失效不能借恢复执行，已受理写入独立结算', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-eligibility-'));
  const factsFile = join(directory, 'facts'), output = join(directory, 'accepted');
  await writeFile(factsFile, 'ready');
  const scope = new Scope(), admission = Promise.withResolvers<void>(), entered = Promise.withResolvers<AbortSignal>();
  const started = Promise.withResolvers<void>(), finish = Promise.withResolvers<void>();
  let reads = 0;
  const facts = createResource(scope, { value: z.string(), async read({ signal }) {
    reads++; return readFile(factsFile, { encoding: 'utf8', signal });
  } });
  const action = bindAction(implementAction({ name: 'eligible_write', description: '按当前资料写入', scope: z.null(),
    input: z.enum(['old', 'new']), output: z.string(), async execute(value, call) {
      started.resolve(); await finish.promise; call.signal.throwIfAborted();
      await writeFile(output, value); return value;
    } }), null, {
    eligibility: facts.observations$.pipe(mapResource((): Eligibility => ({ allowed: true }))),
    async admit(value, _call, _execution, signal) { if (value === 'old') { entered.resolve(signal); await admission.promise; } },
  });
  try {
    const original = action('old', eligibilityExecution('old'));
    const signal = await entered.promise;
    assert.equal(reads, 1, '首次调用本身取得冷资源，不依赖Offer或预热观察');
    facts.invalidate();
    await assert.rejects(original.result, error => error instanceof EligibilityError && error.reason === 'pending');
    assert.equal(signal.aborted, true);
    admission.resolve();
    const next = action('new', eligibilityExecution('new'));
    await started.promise;
    facts.invalidate(); action.release();
    finish.resolve();
    assert.equal(await next.result, 'new');
    assert.equal(await readFile(output, 'utf8'), 'new');
    assert.deepEqual(await action.activeInvocations.read(), []);
  } finally { admission.resolve(); finish.resolve(); action.release(); scope.dispose(); }
});

test('资格撤销等待真实持久prepare结算后才结案，恢复允许不复活原调用', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-eligibility-prepare-'));
  const journal = join(directory, 'journal'), output = join(directory, 'output');
  const qualification = new BehaviorSubject<Observation<Eligibility>>({ status: 'ready', generation: 1,
    observedAt: Date.now(), value: { allowed: true } });
  const prepared = Promise.withResolvers<void>(), finishPrepare = Promise.withResolvers<void>();
  const operation = defineOperation({ kind: 'qualification-prepared', version: 1, command: z.string(), reference: z.string(),
    state: z.boolean(), settled: value => value ? value : undefined });
  const owner = implementOperation(operation, { locate: () => ({ status: 'unknown' }),
    observe: () => of({ status: 'ready', generation: 1, observedAt: Date.now(), value: true }) });
  const action = bindAction(implementAction(defineAction({ name: 'qualified_operation', description: '持久准备之后执行', scope: z.null(),
    input: z.string(), output: z.string(), operation: { definition: operation, command: input => input, reference: result => result },
  }), { operationOwner: owner, async execute(input) { await writeFile(output, input); return input; } }), null, {
    eligibility: qualification, admit() {}, async operation(event) {
      if (event.phase === 'prepare') {
        await writeFile(journal, 'preparing'); prepared.resolve(); await finishPrepare.promise;
        await writeFile(journal, 'prepared');
      } else if (event.phase === 'not-started') {
        assert.equal(await readFile(journal, 'utf8'), 'prepared'); await writeFile(journal, 'not-started');
      }
    },
  });
  try {
    const original = action('old-operation', eligibilityExecution('prepared'));
    await prepared.promise;
    qualification.next({ status: 'ready', generation: 2, observedAt: Date.now(), value: { allowed: true } });
    qualification.next({ status: 'pending', generation: 3 });
    qualification.next({ status: 'ready', generation: 4, observedAt: Date.now(), value: { allowed: true } });
    assert.equal((await original.activity.read()).state, 'pending');
    assert.equal(await readFile(journal, 'utf8'), 'preparing', '持久prepare未完成时不能提前写not-started');
    finishPrepare.resolve();
    await assert.rejects(original.result, error => error instanceof EligibilityError && error.reason === 'pending');
    assert.equal(await readFile(journal, 'utf8'), 'not-started');
    await assert.rejects(readFile(output), { code: 'ENOENT' });
    const next = action('accepted-operation', eligibilityExecution('recovered'));
    assert.equal(await next.result, 'accepted-operation');
    assert.equal((await next.operation).reference, 'accepted-operation');
  } finally { finishPrepare.resolve(); action.release(); qualification.complete(); }
});

test('资格来源释放失败仍等已开始的真实写入并保留结果，以观察错误报告清理失败', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-eligibility-cleanup-'));
  const output = join(directory, 'committed'), started = Promise.withResolvers<void>(), finish = Promise.withResolvers<void>();
  const cleanupError = new Error('QUALIFICATION_SOURCE_CLEANUP_FAILED');
  const action = bindAction(implementAction({ name: 'cleanup_qualified_write', description: '独立结算已受理写入', scope: z.null(),
    input: z.null(), output: z.string(), async execute() { started.resolve(); await finish.promise; await writeFile(output, 'committed'); return output; },
  }), null, { admit() {}, eligibility: new Observable<Observation<Eligibility>>(subscriber => {
    subscriber.next({ status: 'ready', generation: 1, observedAt: Date.now(), value: { allowed: true } });
    return () => { throw cleanupError; };
  }) });
  try {
    const invocation = action(null, eligibilityExecution('cleanup'));
    await started.promise;
    assert.equal((await invocation.activity.read()).state, 'running');
    finish.resolve();
    assert.equal(await invocation.result, output);
    assert.equal(await readFile(output, 'utf8'), 'committed');
    const state = await invocation.activity.read();
    assert.equal(state.state, 'returned');
    if (state.state === 'returned') assert.match(String(state.observationError), /QUALIFICATION_SOURCE_CLEANUP_FAILED/u);
  } finally { finish.resolve(); action.release(); }
});
