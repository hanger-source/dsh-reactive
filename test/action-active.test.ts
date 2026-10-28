import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { filter, firstValueFrom, lastValueFrom } from 'rxjs';
import { bindAction, combineResources, defineAction, implementAction, z, type ActionExecutionContext } from '@hanger-source/dsh-reactive';

test('绑定release与原调用取消在不合作准入返回前结案，迟到准入不能开始真实写入', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-action-admission-'));
  for (const mode of ['release', 'cancel'] as const) {
    const file = join(directory, mode);
    const entered = Promise.withResolvers<AbortSignal>(), gate = Promise.withResolvers<void>();
    const controller = new AbortController(), reason = new Error('CALL_CANCELLED');
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
    const invocation = action(null, original);
    const signal = await entered.promise;
    const rejected = assert.rejects(invocation.result, mode === 'cancel' ? reason : /REACTIVE_ACTION_BINDING_RELEASED/u);
    try {
      if (mode === 'release') action.release(); else controller.abort(reason);
      await rejected;
      assert.equal(signal.aborted, true);
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
  const second = action('second', execution('second'));
  const late = await action.activeInvocations.read();
  assert.deepEqual(late, [first, second]);
  assert.deepEqual(await firstValueFrom(second.activity$), { state: 'pending' });
  assert.deepEqual(await second.activity.read(), { state: 'pending' });
  assert.deepEqual(effects, ['first']);
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
