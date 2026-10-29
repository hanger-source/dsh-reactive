import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { NEVER, Observable, Subject, Subscription, concatMap, firstValueFrom, lastValueFrom, map, mergeMap, of, switchMap, take, tap } from 'rxjs';
import { Scope, createResource, createReaction, defineResource, implementResource, bindResource, defineReaction, implementReaction, bindReaction, z } from '@hanger-source/dsh-reactive';
import type { Observation } from '@hanger-source/dsh-reactive';

test('观察来源清理中再次失效先完成旧连接释放，只连接最新一代且没有失管观察', () => {
  const scope = new Scope(), active = new Set<number>();
  let connections = 0, repeat = true;
  const resource = createResource(scope, { value: z.number(), observe() {
    if (active.size) throw new Error('PREVIOUS_RESOURCE_CONNECTION_STILL_ACTIVE');
    const id = ++connections; active.add(id);
    return new Observable<Observation<number>>(subscriber => {
      subscriber.next({ status: 'ready', generation: id, observedAt: Date.now(), value: id });
      return () => {
        // 通知先于关闭完成，下一次工厂必须等整个清理返回才能取得独占来源。
        if (repeat) { repeat = false; resource.invalidate(); }
        active.delete(id);
      };
    });
  } });
  const values: number[] = [], errors: unknown[] = [];
  const watch = resource.observations$.subscribe(value => {
    if (value.status === 'ready') values.push(value.value);
    else if (value.status === 'error') errors.push(value.error);
  });
  try {
    resource.invalidate();
    assert.deepEqual(values, [1, 2]); assert.deepEqual(errors, []);
    assert.equal(connections, 2); assert.deepEqual([...active], [2]);
  } finally { watch.unsubscribe(); scope.dispose(); }
  assert.equal(active.size, 0);
});

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
  const delivered: string[] = [];
  let completed = false, settled = false;
  run.events$.subscribe({ next: event => delivered.push(event.result), complete: () => { completed = true; } });
  void run.settled.then(() => { settled = true; });
  const one = run.events$.subscribe(), two = run.state$.subscribe();
  assert.throws(() => instance.start(), /ALREADY_RUNNING/);
  source.next({ commandId: 'accepted-one' }); source.next({ commandId: 'queued-two' });
  run.stop();
  assert.deepEqual(started, ['accepted-one']);
  assert.equal(source.observed, false);
  assert.equal(completed, true);
  assert.deepEqual(run.snapshot(), { status: 'draining', reason: 'stopped', pending: 1, emitted: 0 });
  assert.throws(() => instance.start(), /ALREADY_RUNNING/);
  await new Promise<void>(resolve => setImmediate(resolve));
  assert.equal(settled, false);
  accepted.resolve('official-result');
  assert.equal((await run.settled).status, 'stopped');
  assert.deepEqual(committed, ['accepted-one']);
  assert.deepEqual(started, ['accepted-one']);
  assert.equal(run.snapshot().emitted, 0);
  assert.deepEqual(delivered, []);
  instance.start().stop();
  one.unsubscribe(); two.unsubscribe(); scope.dispose();
});

test('Scope 退出后真实文件写入的迟到拒绝仍结算原反应，不执行队列或补发结果', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-reaction-drain-'));
  const scope = new Scope(), source = new Subject<number>();
  const release = Promise.withResolvers<void>();
  const started: number[] = [];
  let writeFailure: unknown, completed = false, settled = false;
  const instance = createReaction(scope, { event: z.number(), result: z.number(), observe: () => source, operator: concatMap,
    async execute(value) {
      started.push(value); await release.promise;
      // 路径的父项是实际普通文件；失败来自真实 writeFile，而非模拟写入者。
      try { await writeFile(join(directory, 'unavailable', 'result'), String(value)); }
      catch (error) { writeFailure = error; throw error; }
      return value;
    },
  });
  await writeFile(join(directory, 'unavailable'), 'not a directory');
  const run = instance.start(), delivered: number[] = [];
  const terminal = lastValueFrom(run.state.values$);
  run.events$.subscribe({ next: event => delivered.push(event.result), complete: () => { completed = true; } });
  void run.settled.then(() => { settled = true; });
  try {
    source.next(1); source.next(2); scope.dispose();
    assert.equal(source.observed, false); assert.equal(completed, true);
    await new Promise<void>(resolve => setImmediate(resolve));
    assert.equal(settled, false);
    assert.deepEqual(await run.state.read(), { status: 'draining', reason: 'stopped', pending: 1, emitted: 0 });
    release.resolve();
    const settlement = await run.settled;
    assert.equal(settlement.status, 'failed'); assert.ok(writeFailure);
    if (settlement.status === 'failed') assert.equal(settlement.error, writeFailure);
    assert.equal(await terminal, settlement, '来源Scope退出不结束原run对真实写入结算的观察');
    const late = await run.state.read();
    assert.equal(late.status, 'failed');
    if (late.status === 'failed') assert.equal(late.error, writeFailure);
    const current = run.state.peek();
    assert.equal(current.status, 'ready');
    if (current.status === 'ready') assert.equal(current.value, settlement);
    assert.deepEqual(started, [1]); assert.deepEqual(delivered, []);
    assert.equal(await readFile(join(directory, 'unavailable'), 'utf8'), 'not a directory');
  } finally { release.resolve(); scope.dispose(); }
});

test('附加 owner 在启动链中同步退出会撤掉当前感知，实例可由另一 owner 显式重启', async () => {
  for (const boundary of ['observe', 'subscribe', 'execute'] as const) {
    const instanceScope = new Scope();
    let relation = new Scope(), exitDuringStart = true, observations = 0, connections = 0, resultSubscriptions = 0;
    const executed: number[] = [];
    const instance = createReaction(instanceScope, { event: z.number(), result: z.number(), operator: concatMap,
      observe() {
        observations++;
        if (exitDuringStart && boundary === 'observe') relation.dispose();
        return new Observable<number>(subscriber => {
          connections++;
          if (exitDuringStart && boundary === 'subscribe') relation.dispose();
          subscriber.next(1); subscriber.next(2); subscriber.complete();
          return () => { connections--; };
        });
      },
      execute(value) {
        executed.push(value);
        if (exitDuringStart && boundary === 'execute') relation.dispose();
        return new Observable<number>(subscriber => {
          resultSubscriptions++; subscriber.next(value); subscriber.complete();
        });
      },
    });
    const unavailable = new Scope(); unavailable.dispose();
    assert.throws(() => instance.start({ scope: unavailable }), /REACTIVE_SCOPE_DISPOSED/);
    assert.equal(observations, 0, '预退出的附加 owner 不得启动观察工厂');
    const stopped = instance.start({ scope: relation });
    assert.equal((await stopped.settled).status, 'stopped');
    await relation.closed;
    assert.equal(connections, 0);
    assert.deepEqual(executed, boundary === 'execute' ? [1] : []);
    assert.equal(resultSubscriptions, 0, '同步退出后不得订阅 execute 返回的冷来源');
    exitDuringStart = false; relation = new Scope(); executed.length = 0;
    try {
      assert.equal((await instance.start({ scope: relation }).settled).status, 'completed');
      assert.deepEqual(executed, [1, 2]);
      assert.equal(connections, 0);
    } finally { relation.dispose(); instanceScope.dispose(); }
    await Promise.all([relation.closed, instanceScope.closed]);
  }
});

test('任一运行 owner 退出都等待真实文件写入结算，独立、同一及父子 owner 不自等或提前关闭', async () => {
  for (const ownership of ['relation-first', 'instance-first', 'same', 'parent-child'] as const) {
    const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-reaction-owners-'));
    await writeFile(join(directory, 'unavailable'), 'not a directory');
    const instanceScope = new Scope();
    const relation = ownership === 'same' ? instanceScope : ownership === 'parent-child' ? instanceScope.child() : new Scope();
    const source = new Subject<number>(), release = Promise.withResolvers<void>();
    const started: number[] = [], delivered: number[] = [];
    let writeFailure: unknown, instanceClosed = false, relationClosed = false, settled = false;
    const instance = createReaction(instanceScope, { event: z.number(), result: z.number(), observe: () => source, operator: concatMap,
      async execute(value) {
        started.push(value); await release.promise;
        try { await writeFile(join(directory, 'unavailable', 'result'), String(value)); }
        catch (error) { writeFailure = error; throw error; }
        return value;
      },
    });
    const run = instance.start({ scope: relation });
    run.events$.subscribe(event => delivered.push(event.result));
    void run.settled.then(() => { settled = true; });
    void instanceScope.closed.then(() => { instanceClosed = true; });
    void relation.closed.then(() => { relationClosed = true; });
    try {
      source.next(1); source.next(2);
      if (ownership === 'instance-first') instanceScope.dispose();
      else relation.dispose();
      assert.equal(source.observed, false);
      if (!instanceScope.disposed) assert.throws(() => instance.start(), /ALREADY_RUNNING/);
      instanceScope.dispose(); relation.dispose();
      await new Promise<void>(resolve => setImmediate(resolve));
      assert.equal(settled, false);
      assert.equal(instanceClosed, false, `${ownership}: 实例 owner 必须等待已经开始的写入`);
      assert.equal(relationClosed, false, `${ownership}: 附加 owner 必须等待同一运行`);
      assert.equal(run.snapshot().status, 'draining');
      release.resolve();
      const settlement = await run.settled;
      assert.equal(settlement.status, 'failed'); assert.ok(writeFailure);
      if (settlement.status === 'failed') assert.equal(settlement.error, writeFailure);
      await Promise.all([instanceScope.closed, relation.closed]);
      assert.deepEqual(started, [1], '未执行的排队项不能在退出后开始');
      assert.deepEqual(delivered, [], '已停止运行不补发迟到结果');
      assert.equal(await readFile(join(directory, 'unavailable'), 'utf8'), 'not a directory');
    } finally { release.resolve(); instanceScope.dispose(); relation.dispose(); }
  }
});

test('来源与清理失败先停 RxJS 并发关系，所有已开始 Promise 的迟到失败仍保留且同错只记一次', async () => {
  const scope = new Scope(), source = new Subject<number>();
  const accepted = [Promise.withResolvers<number>(), Promise.withResolvers<number>(), Promise.withResolvers<number>()];
  const original = new Error('source lost'), cleanup = new Error('source cleanup failed'), late = new Error('late write failed');
  const started: number[] = [];
  let sourceSignal: AbortSignal | undefined, settled = false;
  const run = createReaction(scope, { event: z.number(), result: z.number(), operator: mergeMap,
    observe: ({ signal }) => {
      sourceSignal = signal;
      return new Observable<number>(subscriber => {
        const connection = source.subscribe(subscriber);
        return () => { connection.unsubscribe(); throw cleanup; };
      });
    },
    execute(value) { started.push(value); return accepted[value - 1]!.promise; },
  }).start();
  void run.settled.then(() => { settled = true; });
  try {
    source.next(1); source.next(2); source.next(3); source.error(original);
    assert.equal(source.observed, false); assert.equal(sourceSignal?.aborted, true);
    const draining = run.snapshot();
    assert.equal(draining.status, 'draining');
    if (draining.status === 'draining') { assert.equal(draining.reason, 'failed'); assert.equal(draining.pending, 3); }
    accepted[0]!.reject(original); accepted[1]!.reject(late);
    await new Promise<void>(resolve => setImmediate(resolve));
    assert.equal(settled, false);
    assert.equal(run.snapshot().status, 'draining');
    const remaining = run.snapshot();
    if (remaining.status === 'draining') assert.equal(remaining.pending, 1);
    accepted[2]!.resolve(3);
    const settlement = await run.settled;
    assert.equal(settlement.status, 'failed');
    if (settlement.status === 'failed') {
      assert.ok(settlement.error instanceof AggregateError);
      assert.deepEqual(settlement.error.errors, [original, cleanup, late]);
    }
    assert.deepEqual(started, [1, 2, 3]); assert.equal(settlement.emitted, 0);
  } finally { for (const call of accepted) call.resolve(0); scope.dispose(); }
});

test('switchMap 只交付当前结果，失选的原 Promise 及其一次解析仍由同一运行等到真实结算', async () => {
  for (const outcome of ['success', 'parse-failure'] as const) {
    const scope = new Scope(), source = new Subject<number>();
    const previous = Promise.withResolvers<string>(), current = Promise.withResolvers<string>();
    const parseFailure = new Error('late result rejected by schema');
    let parses = 0;
    const instance = createReaction(scope, { event: z.number(), result: z.string().transform(raw => {
      parses++; if (raw === 'invalid') throw parseFailure; return Number(raw);
    }), observe: () => source, operator: switchMap,
    execute(value) { return value === 1 ? previous.promise : current.promise; },
    });
    const run = instance.start(), delivered: number[] = [];
    run.events$.subscribe(event => delivered.push(event.result));
    try {
      source.next(1); source.next(2);
      const selected = firstValueFrom(run.events$);
      current.resolve('2');
      assert.equal((await selected).result, 2);
      source.complete();
      assert.deepEqual(run.snapshot(), { status: 'draining', reason: 'completed', pending: 1, emitted: 1 });
      assert.throws(() => instance.start(), /ALREADY_RUNNING/);
      previous.resolve(outcome === 'success' ? '1' : 'invalid');
      const settlement = await run.settled;
      assert.equal(settlement.status, outcome === 'success' ? 'completed' : 'failed');
      if (settlement.status === 'failed') assert.equal(settlement.error, parseFailure);
      assert.deepEqual(delivered, [2]); assert.equal(parses, 2, 'selected 与迟到 Promise 各只解析一次');
    } finally { previous.resolve('1'); current.resolve('2'); scope.dispose(); }
  }
});

test('RxJS operator 自身失败也等待已开始的 Promise，并保留两条真实失败原因', async () => {
  const scope = new Scope(), source = new Subject<number>();
  const accepted = Promise.withResolvers<number>();
  const original = new Error('operator rejected next occurrence'), late = new Error('started work later failed');
  const started: number[] = [];
  const run = createReaction(scope, { event: z.number(), result: z.number(), observe: () => source,
    operator<Input, Output>(project: (event: Input, index: number) => Observable<Output>) {
      return mergeMap((event: Input, index) => {
        if (index === 1) throw original;
        return project(event, index);
      });
    },
    execute(value) { started.push(value); return accepted.promise; },
  }).start();
  try {
    source.next(1); source.next(2);
    assert.equal(source.observed, false);
    const draining = run.snapshot();
    assert.equal(draining.status, 'draining');
    if (draining.status === 'draining') { assert.equal(draining.reason, 'failed'); assert.equal(draining.pending, 1); }
    accepted.reject(late);
    const settlement = await run.settled;
    assert.equal(settlement.status, 'failed');
    if (settlement.status === 'failed') {
      assert.ok(settlement.error instanceof AggregateError);
      assert.deepEqual(settlement.error.errors, [original, late]);
    }
    assert.deepEqual(started, [1]);
  } finally { accepted.resolve(1); scope.dispose(); }
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
  const accepted = Promise.withResolvers<number>();
  const effects: number[] = [];
  const reaction = defineReaction({ name: 'synchronous-delivery', params: z.null(), event: z.number(), result: z.number() });
  const implementation = implementReaction(reaction, { observe: () => of(1, 2), operator: concatMap,
    execute(event) {
      effects.push(event);
      scope.dispose();
      return accepted.promise;
    },
  });
  const run = bindReaction(implementation, { scope }).instance(null).start();
  let settled = false; void run.settled.then(() => { settled = true; });
  await new Promise<void>(resolve => setImmediate(resolve));
  assert.equal(settled, false, 'execute 返回之前发生 stop，随后取得的 Promise 仍归原运行');
  assert.deepEqual(run.snapshot(), { status: 'draining', reason: 'stopped', pending: 1, emitted: 0 });
  accepted.resolve(1);
  assert.equal((await run.settled).status, 'stopped');
  assert.deepEqual(effects, [1]);

  const coldScope = new Scope();
  let coldStarts = 0;
  const cold = createReaction(coldScope, { event: z.number(), result: z.number(), observe: () => of(1, 2), operator: concatMap,
    execute() {
      coldScope.dispose();
      return new Observable<number>(subscriber => { coldStarts++; subscriber.next(1); subscriber.complete(); });
    },
  }).start();
  assert.equal((await cold.settled).status, 'stopped');
  assert.equal(coldStarts, 0, 'execute 返回冷 Observable 前，订阅资格已被同步退出撤销');

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

test('业务订阅和 teardown 中同步 stop 保持原运行身份，释放失败在最终结算之前收齐', async () => {
  const scope = new Scope(), source = new Subject<number>();
  const failure = new Error('teardown failed after reentrant stop');
  let stop = () => {};
  let teardowns = 0;
  const instance = createReaction(scope, { event: z.number(), result: z.number(), observe: () => source, operator: concatMap,
    execute: () => new Observable<number>(() => {
      stop();
      return () => {
        teardowns++; stop();
        assert.throws(() => instance.start(), /ALREADY_RUNNING/);
        throw failure;
      };
    }),
  });
  const run = instance.start(); stop = () => run.stop();
  try {
    source.next(1);
    const settlement = await run.settled;
    assert.equal(settlement.status, 'failed');
    if (settlement.status === 'failed') assert.equal(settlement.error, failure);
    assert.equal(teardowns, 1); assert.equal(source.observed, false);
    instance.start().stop();
  } finally { scope.dispose(); }
});

test('热事件先交付再通知计数，状态观察者同步 stop 不制造未交付事件或终态后的 running', async () => {
  const scope = new Scope(), source = new Subject<number>();
  const run = createReaction(scope, { event: z.number(), result: z.number(), observe: () => source,
    operator: concatMap, execute: value => value,
  }).start();
  const delivered: number[] = [], states: string[] = [], following: string[] = [], counts: Promise<number>[] = [];
  run.events$.subscribe(event => {
    delivered.push(event.result);
    counts.push(run.state.read().then(state => state.emitted));
    const current = run.state.peek();
    assert.equal(current.status, 'ready');
    if (current.status === 'ready') assert.equal(current.value.emitted, delivered.length);
  });
  run.state.subscribe(observation => {
    assert.equal(observation.status, 'ready');
    if (observation.status !== 'ready') return;
    const state = observation.value;
    states.push(state.status);
    if (state.status === 'running' && state.emitted === 1) run.stop();
  });
  run.state$.subscribe(state => following.push(state.status));
  try {
    source.next(1); source.next(2);
    const settlement = await run.settled;
    assert.equal(settlement.status, 'stopped'); assert.equal(settlement.emitted, 1);
    assert.deepEqual(delivered, [1]);
    assert.deepEqual(await Promise.all(counts), [1], '事件回调中的read与peek观察同一个已交付计数');
    assert.equal(states.at(-1), 'stopped');
    assert.equal(states.slice(states.indexOf('draining')).includes('running'), false);
    assert.equal(following.at(-1), 'stopped');
    assert.equal(following.slice(following.indexOf('draining')).includes('running'), false, '并列消费者也不能在重入stop后收到旧running');
  } finally { scope.dispose(); }
});
