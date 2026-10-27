import assert from 'node:assert/strict';
import { test } from 'node:test';
import { NEVER, Observable, Subject, Subscription, concatMap, firstValueFrom, lastValueFrom, map, of, take, tap } from 'rxjs';
import { Scope, createResource, createReaction, defineResource, implementResource, bindResource, defineReaction, implementReaction, bindReaction, z } from '@hanger-source/dsh-reactive';
import type { Observation } from '@hanger-source/dsh-reactive';

test('读取中同步失效先释放独占来源，下一次读取才能取得连接，迟到结果不覆盖当前事实', async () => {
  const scope = new Scope(), invalidations = new Subject<void>();
  const active = new Set<symbol>();
  const reads: { signal: AbortSignal; result: ReturnType<typeof Promise.withResolvers<number>> }[] = [];
  const resource = createResource(scope, { value: z.number(), invalidations: () => invalidations,
    read({ signal }) {
      if (active.size) throw new Error('PREVIOUS_RESOURCE_READ_STILL_ACTIVE');
      const lease = Symbol('exclusive-source'); active.add(lease);
      signal.addEventListener('abort', () => active.delete(lease), { once: true });
      const result = Promise.withResolvers<number>();
      reads.push({ signal, result });
      if (reads.length === 1) invalidations.next();
      return result.promise.finally(() => active.delete(lease));
    },
  });
  const observed: Observation<number>[] = [];
  const watch = resource.observations$.subscribe(value => observed.push(value));
  try {
    assert.equal(reads.length, 2, '取消旧读之前启动新读，会被实际独占来源拒绝');
    assert.equal(reads[0]!.signal.aborted, true);
    reads[1]!.result.resolve(2); reads[0]!.result.resolve(1);
    await new Promise<void>(resolve => setImmediate(resolve));
    assert.deepEqual(observed.flatMap(value => value.status === 'ready' ? [value.value] : []), [2]);
    assert.equal(observed.some(value => value.status === 'error'), false);
    assert.equal(active.size, 0);
  } finally { watch.unsubscribe(); scope.dispose(); }
});

test('资源工厂尚未返回时退出作用域，提供者可用原 signal 阻止退出后的读取或监听副作用', () => {
  for (const kind of ['read', 'observe', 'invalidations'] as const) {
    const scope = new Scope();
    let opened = 0, completed = false;
    const stopped = ({ signal }: { signal: AbortSignal }) => { scope.dispose(); signal.throwIfAborted(); opened++; };
    const resource = kind === 'observe'
      ? createResource(scope, { value: z.number(), observe(context) { stopped(context); return NEVER; } })
      : kind === 'invalidations'
        ? createResource(scope, { value: z.number(), read() { opened++; return 1; },
          invalidations(context) { stopped(context); return NEVER; } })
        : createResource(scope, { value: z.number(), read(context) { stopped(context); return 1; } });
    const watch = resource.observations$.subscribe({ complete: () => { completed = true; } });
    try {
      assert.equal(opened, 0, kind + ' 不应在 owner 退出后打开来源');
      assert.equal(completed, true);
    } finally { watch.unsubscribe(); scope.dispose(); }
  }
});

test('同步退出资源观察不启动业务读取，连接中退出也立即撤销来源，后续显式读取仍可正常建立连接', async () => {
  const scope = new Scope();
  let reads = 0;
  const resource = createResource(scope, { value: z.number(), read: () => ++reads });
  try {
    // 只取当前状态的消费者在基线交付时已经退出，不应为它启动查询。
    assert.equal((await firstValueFrom(resource.observations$)).status, 'pending');
    assert.equal(reads, 0);
    // 来源建立连接时也会同步交付 pending；此时退出必须阻止后续实际读取。
    assert.equal((await lastValueFrom(resource.observations$.pipe(take(2)))).status, 'pending');
    assert.equal(reads, 0);
    assert.equal(resource.peek().status, 'pending');
    assert.equal(await resource.read(), 1);
    assert.equal(reads, 1);
    assert.equal(resource.peek().status, 'pending');
  } finally { scope.dispose(); }
});

test('基线观察中接入持续消费者再同步退出，留下的消费者仍共享实际读取', async () => {
  const scope = new Scope();
  let reads = 0;
  const resource = createResource(scope, { value: z.number(), read: () => ++reads });
  const values: number[] = [];
  let monitor = new Subscription();
  try {
    await firstValueFrom(resource.observations$.pipe(take(1), tap(() => {
      monitor = resource.values$.subscribe(value => values.push(value));
    })));
    assert.equal(reads, 1);
    assert.equal(await resource.read(), 1);
    assert.deepEqual(values, [1]);
    assert.equal(reads, 1);
    monitor.unsubscribe();
    assert.equal(resource.peek().status, 'pending');
  } finally { monitor.unsubscribe(); scope.dispose(); }
});

test('两个资源消费者共享读取；失效取消旧读且迟到旧结果不能覆盖新现场', async () => {
  const scope = new Scope();
  const invalidations = new Subject<void>();
  const reads: { signal: AbortSignal; value: ReturnType<typeof Promise.withResolvers<number>> }[] = [];
  const resource = defineResource({ name: 'document.revision', params: z.object({ id: z.string() }), value: z.number() });
  const implementation = implementResource(resource, {
    read(_params, { signal }) {
      const value = Promise.withResolvers<number>(); reads.push({ signal, value }); return value.promise;
    },
    invalidations: () => invalidations,
  });
  const binding = bindResource(implementation, { scope });
  const instance = binding.instance({ id: 'merchant-a' });
  assert.equal(binding.instance({ id: 'merchant-a' }), instance);
  const received: number[] = [];
  const first = instance.values$.subscribe(value => received.push(value));
  const second = instance.observations$.subscribe();
  assert.equal(reads.length, 1);
  invalidations.next();
  assert.equal(reads[0].signal.aborted, true);
  assert.equal(reads.length, 2);
  reads[1].value.resolve(2);
  await Promise.resolve(); await Promise.resolve();
  reads[0].value.resolve(1);
  await Promise.resolve(); await Promise.resolve();
  assert.deepEqual(received, [2]);
  first.unsubscribe();
  assert.equal(instance.peek().status, 'ready');
  invalidations.next();
  assert.equal(reads.length, 3);
  second.unsubscribe();
  assert.equal(instance.peek().status, 'pending');
  assert.equal(reads[2].signal.aborted, true);
  scope.dispose();
  assert.throws(() => instance.peek(), /REACTIVE_SCOPE_DISPOSED/);
});

test('停止持续反应撤掉排队关系，已经受理的业务仍可按原 owner 完成', async () => {
  const scope = new Scope();
  const source = new Subject<{ commandId: string }>();
  const accepted = Promise.withResolvers<string>();
  const started: string[] = [];
  const committed: string[] = [];
  const instance = createReaction(scope, { event: z.object({ commandId: z.string() }), result: z.string(),
    observe: () => source, operator: concatMap,
    execute(event) {
      started.push(event.commandId);
      return accepted.promise.then(value => { committed.push(event.commandId); return value; });
    },
  });
  const run = instance.start();
  const one = run.events$.subscribe(), two = run.state$.subscribe();
  assert.throws(() => instance.start(), /ALREADY_RUNNING/);
  source.next({ commandId: 'accepted-one' }); source.next({ commandId: 'queued-two' });
  run.stop();
  assert.deepEqual(started, ['accepted-one']);
  assert.equal((await run.settled).status, 'stopped');
  accepted.resolve('official-result');
  await accepted.promise; await Promise.resolve();
  assert.deepEqual(committed, ['accepted-one']);
  assert.deepEqual(started, ['accepted-one']);
  assert.equal(run.snapshot().emitted, 0);
  one.unsubscribe(); two.unsubscribe(); scope.dispose();
});

test('同步来源完成后登记的清理失败进入原运行结算，并保留先发生的来源错误', async () => {
  const scope = new Scope();
  const failure = new Error('source cleanup failed'), original = new Error('source failed before cleanup');
  let released = 0;
  try {
    const completed = createReaction(scope, { event: z.number(), result: z.number(), operator: concatMap,
      observe: () => new Observable<number>(subscriber => {
        subscriber.next(1); subscriber.complete();
        return () => { released++; throw failure; };
      }), execute: event => event,
    }).start();
    const completedSettlement = await completed.settled;
    assert.equal(completedSettlement.status, 'failed');
    if (completedSettlement.status === 'failed') assert.equal(completedSettlement.error, failure);
    assert.equal(completedSettlement.emitted, 1, '已经完成的业务不因来源释放失败而回滚');
    const failed = createReaction(scope, { event: z.number(), result: z.number(), operator: concatMap,
      observe: () => new Observable<number>(subscriber => {
        subscriber.error(original);
        return () => { released++; throw failure; };
      }), execute: event => event,
    }).start();
    const failedSettlement = await failed.settled;
    assert.equal(failedSettlement.status, 'failed');
    if (failedSettlement.status === 'failed') {
      assert.ok(failedSettlement.error instanceof AggregateError);
      assert.deepEqual(failedSettlement.error.errors, [original, failure]);
    }
    assert.equal(released, 2);
  } finally { scope.dispose(); }
});

test('同步业务结果的清理失败停止后续工作，晚到监控取得失败而不重新执行业务', async () => {
  const scope = new Scope();
  const started: number[] = [], released: number[] = [];
  const failure = new Error('result cleanup failed');
  let sourceSignal: AbortSignal | undefined;
  try {
    const reaction = createReaction(scope, { event: z.number(), result: z.number(), operator: concatMap,
      observe: ({ signal }) => { sourceSignal = signal; return of(1, 2); },
      execute(event) {
        started.push(event);
        return new Observable<number>(subscriber => {
          subscriber.next(event); subscriber.complete();
          return () => { released.push(event); throw failure; };
        });
      },
    });
    const run = reaction.start();
    const settlement = await run.settled;
    assert.equal(settlement.status, 'failed');
    if (settlement.status === 'failed') assert.equal(settlement.error, failure);
    const reported: unknown[] = [];
    run.state$.subscribe(state => { if (state.status === 'failed') reported.push(state.error); });
    assert.deepEqual(reported, [failure]);
    assert.deepEqual(started, [1]); assert.deepEqual(released, [1]);
    assert.equal(sourceSignal?.aborted, true);
    assert.equal(settlement.emitted, 1);
  } finally { scope.dispose(); }
});

test('来源失败成为可观察结算，Scope 退出释放另一项持续感知', async () => {
  const scope = new Scope();
  const failures = new Subject<number>();
  const definition = defineReaction({ name: 'watch-revisions', params: z.number(), event: z.number(), result: z.number() });
  let signal: AbortSignal | undefined;
  const implementation = implementReaction(definition, { observe(_params, context) { signal = context.signal; return failures; },
    operator: concatMap, execute: event => event });
  const binding = bindReaction(implementation, { scope });
  const failed = binding.instance(1).start();
  const error = new Error('business notification connection lost'); failures.error(error);
  const settlement = await failed.settled;
  assert.equal(settlement.status, 'failed');
  if (settlement.status === 'failed') assert.equal(settlement.error, error);
  assert.equal(signal?.aborted, true);
  // 已终止的来源会在 start 返回前同步失败，随后接入的应用监控仍须收到原因。
  const synchronous = binding.instance(3).start();
  const reported: unknown[] = [];
  synchronous.state$.subscribe(state => { if (state.status === 'failed') reported.push(state.error); });
  assert.deepEqual(reported, [error]);
  const never = new Subject<number>();
  let released = false;
  const second = implementReaction(definition, { observe(_params, context) {
    context.signal.addEventListener('abort', () => { released = true; }, { once: true }); return never;
  }, operator: concatMap, execute: value => value });
  const run = bindReaction(second, { scope }).instance(2).start();
  scope.dispose();
  assert.equal((await run.settled).status, 'stopped');
  assert.equal(released, true);
});

test('执行失败释放来源并撤掉排队工作；晚到监控取得失败，只有显式重启才继续执行', async () => {
  const scope = new Scope();
  try {
    const source = new Subject<number>();
    const accepted = Promise.withResolvers<number>();
    const started: number[] = [];
    let sourceSignal: AbortSignal | undefined;
    const definition = defineReaction({ name: 'failed-delivery', params: z.null(), event: z.number(), result: z.number() });
    const instance = bindReaction(implementReaction(definition, {
      observe(_params, { signal }) { sourceSignal = signal; return source; },
      operator: concatMap,
      execute(event) { started.push(event); return event === 1 ? accepted.promise : event; },
    }), { scope }).instance(null);
    const run = instance.start();
    source.next(1); source.next(2);
    const failure = new Error('accepted service request failed');
    accepted.reject(failure);
    const settlement = await run.settled;
    assert.equal(settlement.status, 'failed');
    if (settlement.status === 'failed') assert.equal(settlement.error, failure);
    assert.equal(sourceSignal?.aborted, true);
    assert.equal(source.observed, false);
    // 应用可能在 start 返回后、甚至收到其他异步消息后才接上监控。
    const reported: unknown[] = [];
    run.state$.subscribe(state => { if (state.status === 'failed') reported.push(state.error); });
    assert.deepEqual(reported, [failure]);
    source.next(3);
    assert.deepEqual(started, [1]);
    const restarted = instance.start();
    source.next(4); source.complete();
    assert.equal((await restarted.settled).status, 'completed');
    assert.deepEqual(started, [1, 4]);
  } finally { scope.dispose(); }
});

test('同步来源首次业务处理释放Scope后，不再开始下一项处理或资源读取', async () => {
  const scope = new Scope();
  const effects: number[] = [];
  const reaction = defineReaction({ name: 'synchronous-delivery', params: z.null(), event: z.number(), result: z.number() });
  const implementation = implementReaction(reaction, { observe: () => of(1, 2), operator: concatMap,
    execute(event) {
      effects.push(event);
      scope.dispose();
      return event;
    },
  });
  const run = bindReaction(implementation, { scope }).instance(null).start();
  await run.settled;
  assert.deepEqual(effects, [1]);

  const resourceScope = new Scope();
  const reads: number[] = [];
  const resource = defineResource({ name: 'synchronous-facts', params: z.null(), value: z.number() });
  const facts = bindResource(implementResource(resource, { observe: () => of(1, 2).pipe(map(value => {
    reads.push(value);
    return { status: 'ready' as const, generation: value, value, observedAt: Date.now() };
  })) }), { scope: resourceScope }).instance(null);
  facts.observations$.subscribe(observation => { if (observation.status === 'ready') resourceScope.dispose(); });
  assert.deepEqual(reads, [1]);
});
