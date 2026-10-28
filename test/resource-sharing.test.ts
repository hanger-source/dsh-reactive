import assert from 'node:assert/strict';
import { test } from 'node:test';
import { BehaviorSubject, EMPTY, EmptyError, Observable, Subject, UnsubscriptionError, defer, firstValueFrom, of, take, throwError } from 'rxjs';
import { combineResources, mapResource, shareResource, ResourceReader, Scope, createResource, z, type Observation } from '@hanger-source/dsh-reactive';

const ready = <T>(value: T): Observation<T> => ({ status: 'ready', generation: 1, observedAt: Date.now(), value });

test('跨owner派生在来源退出后撤销全部读取，观察流结错而callback交付原错误', async () => {
  const reported: unknown[] = [];
  const provider = new Scope(), consumer = new Scope({ onObserverError: error => reported.push(error) });
  const source = provider.state(7), useProvider = consumer.state(true);
  const derived = consumer.computed(() => useProvider.value ? source.value * 2 : 9);
  const values: number[] = [], callbacks: number[] = [], streamErrors: unknown[] = [], callbackErrors: unknown[] = [];
  const callbackFailure = new Error('ERROR_CALLBACK_FAILED'), recovered: number[] = [];
  let released = 0;
  provider.own(() => { released++; });
  const stream = derived.changes$.subscribe({ next: value => values.push(value), error: error => streamErrors.push(error) });
  const stop = derived.subscribe(value => callbacks.push(value), error => callbackErrors.push(error));
  const stopDefault = derived.subscribe(() => {});
  const stopThrowing = derived.subscribe(value => recovered.push(value), () => { throw callbackFailure; });
  assert.equal(derived.value, 14); assert.equal(derived.get(), 14); assert.equal(derived.peek(), 14);
  assert.equal(await derived.read(), 14);
  assert.doesNotThrow(() => provider.dispose());
  const reason: unknown = provider.signal.reason;
  assert.equal(consumer.disposed, false); assert.equal(released, 1);
  assert.deepEqual(values, [14]); assert.deepEqual(callbacks, [14]);
  assert.deepEqual(streamErrors, [reason]); assert.deepEqual(callbackErrors, [reason]);
  assert.equal(reported.length, 2); assert.ok(reported.includes(reason)); assert.ok(reported.includes(callbackFailure));
  assert.equal(stream.closed, true);
  for (const read of [() => derived.value, () => derived.get(), () => derived.peek(), () => derived.read()]) {
    assert.throws(read, error => error === reason);
  }
  const lateValues: number[] = [], lateErrors: unknown[] = [];
  assert.equal(derived.changes$.subscribe({ next: value => lateValues.push(value), error: error => lateErrors.push(error) }).closed, true);
  assert.equal(lateValues.length, 0); assert.deepEqual(lateErrors, [reason]);
  const stopLate = derived.subscribe(value => lateValues.push(value), error => lateErrors.push(error));
  assert.equal(lateValues.length, 0); assert.deepEqual(lateErrors, [reason, reason]);
  useProvider.set(false);
  assert.equal(derived.value, 9); assert.deepEqual(callbacks, [14, 9]); assert.deepEqual(recovered, [14, 9]);
  assert.deepEqual(lateValues, [9]); assert.deepEqual(values, [14]); assert.equal(reported.length, 2);
  stop(); stopDefault(); stopThrowing(); stopLate(); consumer.dispose();
});

test('派生只借用当前分支，读取失败后callback可随分支恢复，交付回调的读取不成为依赖', () => {
  const leftOwner = new Scope(), rightOwner = new Scope(), consumer = new Scope();
  const left = leftOwner.state(1), right = rightOwner.state(2), chooseLeft = consumer.state(true), incidental = consumer.state(0);
  const derived = consumer.computed(() => chooseLeft.value ? left.value : right.value);
  const values: number[] = [], errors: unknown[] = [], streamValues: number[] = [], streamErrors: unknown[] = [];
  const stop = derived.subscribe(value => { values.push(value); incidental.value; }, error => { errors.push(error); incidental.value; });
  const stream = derived.changes$.subscribe({ next: value => streamValues.push(value), error: error => streamErrors.push(error) });
  incidental.set(1); assert.deepEqual(values, [1]);
  chooseLeft.set(false); leftOwner.dispose();
  assert.equal(derived.value, 2); assert.deepEqual(values, [1, 2]); assert.deepEqual(errors, []);
  chooseLeft.set(true);
  const reason: unknown = leftOwner.signal.reason;
  assert.throws(() => derived.value, error => error === reason);
  assert.deepEqual(errors, [reason]); assert.deepEqual(streamErrors, [reason]); assert.equal(stream.closed, true);
  incidental.set(2); assert.deepEqual(errors, [reason]);
  chooseLeft.set(false); right.set(3);
  assert.equal(derived.value, 3); assert.deepEqual(values, [1, 2, 2, 3]);
  assert.deepEqual(streamValues, [1, 2]); assert.deepEqual(streamErrors, [reason]);
  const resumed: number[] = [];
  const resumedStream = derived.changes$.subscribe(value => resumed.push(value));
  assert.deepEqual(resumed, [3]);
  stop(); resumedStream.unsubscribe(); consumer.dispose(); rightOwner.dispose();
});

test('peek不跟踪业务值变化，但跨owner借用仍在来源退出时失效', () => {
  const provider = new Scope(), consumer = new Scope(), source = provider.state(1);
  const derived = consumer.computed(() => source.peek());
  const values: number[] = [], errors: unknown[] = [];
  const stream = derived.changes$.subscribe({ next: value => values.push(value), error: error => errors.push(error) });
  source.set(2);
  assert.equal(source.value, 2); assert.equal(derived.value, 1); assert.deepEqual(values, [1]);
  provider.dispose();
  assert.throws(() => derived.peek(), error => error === provider.signal.reason);
  assert.deepEqual(errors, [provider.signal.reason]); assert.equal(stream.closed, true);
  consumer.dispose();
});

test('computed求值期间来源owner退出时，返回值不能穿过借用边界', () => {
  const provider = new Scope(), consumer = new Scope();
  const source = provider.computed(() => { provider.dispose(); return 9; });
  const derived = consumer.computed(() => source.value);
  assert.throws(() => derived.value, error => error === provider.signal.reason);
  assert.equal(provider.disposed, true); assert.equal(consumer.disposed, false);
  const errors: unknown[] = [];
  const stop = derived.subscribe(() => assert.fail('退出后的计算结果不能交付'), error => errors.push(error));
  assert.deepEqual(errors, [provider.signal.reason]);
  stop(); consumer.dispose();
});

test('ResourceValue经computed和observe组合后，来源退出不保留旧ready并沿现有观察结算', () => {
  const provider = new Scope(), consumer = new Scope();
  const source = provider.observe(of(7)), derived = consumer.computed(() => source.value);
  const combined = consumer.observe(derived.changes$), observations: Observation<number>[] = [];
  source.observations$.subscribe(value => observations.push(value));
  assert.equal(derived.value.status, 'ready');
  const before = combined.value;
  assert.equal(before.status, 'ready');
  if (before.status === 'ready') assert.equal(before.value, source.value);
  provider.dispose();
  const reason: unknown = provider.signal.reason;
  assert.throws(() => source.value, error => error === reason);
  assert.throws(() => derived.value, error => error === reason);
  const after = combined.value;
  assert.equal(after.status, 'error');
  if (after.status === 'error') assert.equal(after.error, reason);
  assert.deepEqual(observations.map(value => value.status), ['ready', 'error']);
  const ended = observations.at(-1)!;
  if (ended.status === 'error') assert.equal(ended.error, reason);
  assert.equal(consumer.disposed, false); consumer.dispose();
});

test('有限来源释放真实连接后，显式owner的资源仍撤销外部缓存的旧ready，值流仅随owner完成', () => {
  const owner = new AbortController(), consumer = new Scope(), reason = new Error('FINITE_OWNER_ENDED');
  const frame = ready(42);
  let opened = 0, released = 0, completed = false;
  const resource = new ResourceReader({ peek: () => frame, observations$: new Observable<Observation<number>>(subscriber => {
    opened++; subscriber.next(frame); subscriber.complete(); return () => { released++; };
  }) }, { signal: owner.signal });
  const stored = consumer.observe(resource.observations$);
  resource.values$.subscribe({ complete: () => { completed = true; } });
  assert.equal(released, opened, '有效性观察不延长已经完成的来源I/O');
  assert.equal(completed, false);
  owner.abort(reason);
  const observed = stored.peek();
  assert.equal(observed.status, 'ready');
  if (observed.status === 'ready') {
    assert.equal(observed.value.status, 'error');
    if (observed.value.status === 'error') assert.equal(observed.value.error, reason);
  }
  assert.equal(completed, true);
  assert.throws(() => resource.peek(), error => error === reason);
  consumer.dispose();
});

test('显式owner的空完成和pending完成结算等待，来源不被重新执行或永久挂起', async () => {
  for (const source of [EMPTY, of<Observation<number>>({ status: 'pending', generation: 1 })]) {
    const owner = new AbortController();
    const resource = new ResourceReader<number>({ observations$: source, peek: () => ({ status: 'pending', generation: 0 }) }, { signal: owner.signal });
    try { await assert.rejects(resource.read(), EmptyError); } finally { owner.abort(); }
  }
});

test('owner撤销以原原因替代共享资源的旧有效性，值流正常结束，迟到观察不回放也不重连', () => {
  const owner = new AbortController(), reason = new Error('RESOURCE_OWNER_ENDED');
  let connections = 0;
  const source = shareResource(defer(() => { connections++; return of(ready(42)); }), { signal: owner.signal });
  const observations: Observation<number>[] = [], values: number[] = [], errors: unknown[] = [];
  let completed = false;
  source.observations$.subscribe(value => observations.push(value));
  source.values$.subscribe({ next: value => values.push(value), error: error => errors.push(error), complete: () => { completed = true; } });
  assert.equal(source.peek().status, 'ready');
  owner.abort(reason);
  assert.deepEqual(values, [42]); assert.deepEqual(errors, []); assert.equal(completed, true);
  const ended = observations.at(-1)!;
  assert.equal(ended.status, 'error');
  if (ended.status === 'error') assert.equal(ended.error, reason);
  assert.ok(ended.generation > observations.at(-2)!.generation);
  const late: Observation<number>[] = [];
  assert.equal(source.observations$.subscribe(value => late.push(value)).closed, true);
  assert.equal(late.length, 1); assert.equal(late[0]!.status, 'error');
  if (late[0]!.status === 'error') assert.equal(late[0]!.error, reason);
  assert.equal(connections, 1); assert.throws(() => source.peek(), error => error === reason);
});

test('父Scope撤销先传播到全部后代，重入清理不可读旧资源或启动effect，失败仍由父清理汇总', async () => {
  const parent = new Scope(), child = parent.child(), nested = child.child();
  const value = nested.state(1), resource = createResource(nested, { value: z.number(), read: () => 42 });
  const failure = new Error('CHILD_CLEANUP_FAILED'), values: number[] = [], errors: unknown[] = [];
  const borrowedErrors: unknown[] = [], streamErrors: unknown[] = [], duringAbort: unknown[] = [];
  const consumer = new Scope({ onObserverError: error => borrowedErrors.push(error) });
  const borrowed = consumer.computed(() => value.value * 2);
  const stream = borrowed.changes$.subscribe({ error: error => streamErrors.push(error) });
  borrowed.subscribe(() => {});
  parent.signal.addEventListener('abort', () => {
    try { duringAbort.push(borrowed.value); } catch (error) { duringAbort.push(error); }
  }, { once: true });
  resource.values$.subscribe({ next: value => values.push(value), error: error => errors.push(error) });
  await resource.read();
  value.changes$.subscribe(value => values.push(value));
  let effects = 0, cleanups = 0;
  child.own(() => { cleanups++; throw failure; });
  parent.own(() => {
    assert.equal(child.disposed, true); assert.equal(nested.disposed, true);
    assert.equal(nested.signal.reason, parent.signal.reason);
    assert.throws(() => resource.peek(), error => error === parent.signal.reason);
    assert.throws(() => borrowed.peek(), error => error === parent.signal.reason);
    assert.throws(() => nested.effect(() => { effects++; }), error => error === parent.signal.reason);
    assert.throws(() => value.set(2), error => error === parent.signal.reason);
  });
  assert.throws(() => parent.dispose(), error => error instanceof AggregateError && error.errors.length === 1
    && error.errors[0] instanceof AggregateError && error.errors[0].errors.length === 1 && error.errors[0].errors[0] === failure);
  assert.deepEqual(values, [42, 1]); assert.deepEqual(errors, []); assert.equal(effects, 0); assert.equal(cleanups, 1);
  assert.deepEqual(duringAbort, [parent.signal.reason]);
  assert.deepEqual(streamErrors, [parent.signal.reason]); assert.deepEqual(borrowedErrors, [parent.signal.reason]);
  assert.equal(stream.closed, true); assert.equal(consumer.disposed, false);
  parent.dispose(); child.dispose(); nested.dispose();
  assert.equal(cleanups, 1);
  const late: Observation<number>[] = [];
  assert.equal(resource.observations$.subscribe(value => late.push(value)).closed, true);
  assert.equal(late.length, 1); assert.equal(late[0]!.status, 'error');
  consumer.dispose();
});

test('来源工厂内同步撤销借用时订阅立即关闭，迟到副作用无法接入消费者', () => {
  const owner = new AbortController(), reason = new Error('SYNCHRONOUS_OWNER_EXIT');
  let active = 0, releases = 0;
  const resource = new ResourceReader({ peek: () => ready(1), observations$: new Observable<Observation<number>>(subscriber => {
    owner.abort(reason);
    assert.equal(subscriber.closed, true);
    if (!subscriber.closed) active++;
    subscriber.next(ready(99));
    return () => { releases++; };
  }) }, { signal: owner.signal });
  const observed: Observation<number>[] = [];
  assert.equal(resource.observations$.subscribe(value => observed.push(value)).closed, true);
  assert.equal(active, 0); assert.equal(releases, 1); assert.equal(observed.length, 1);
  assert.equal(observed[0]!.status, 'error');
});

test('同一派生资源共享真实RxJS连接，首帧同步退出不启动来源，取消一次read不影响其他读者且owner不可换绑', async () => {
  const left = new BehaviorSubject<Observation<Date>>({ status: 'pending', generation: 0 });
  const right = new BehaviorSubject(ready(1));
  const composed = combineResources({ left, right }).pipe(mapResource(value => ({ at: value.left, revision: value.right })));
  let connections = 0, releases = 0;
  const owner = new AbortController(), options = { signal: owner.signal };
  const resource = shareResource({ observations$: new Observable<Observation<{ at: Date; revision: number }>>(subscriber => {
    connections++;
    const subscription = composed.subscribe(subscriber);
    return () => { releases++; subscription.unsubscribe(); };
  }) }, options);
  assert.equal(resource.peek().status, 'pending'); assert.equal(connections, 0);
  assert.equal((await firstValueFrom(resource.observations$.pipe(take(1)))).status, 'pending');
  assert.equal(connections, 0, '同步基线已经结束消费者时不能再启动查询');
  const stop = resource.subscribe(() => {});
  let completed = false;
  const monitor = resource.observations$.subscribe({ complete: () => { completed = true; } });
  try {
    const cancelled = new AbortController(), reason = new Error('ONLY_ONE_READER');
    const continued = resource.read(), reading = resource.read(cancelled.signal);
    cancelled.abort(reason); await assert.rejects(reading, error => error === reason);
    const at = new Date('2026-10-28T00:00:00Z'); left.next(ready(at));
    assert.equal((await continued).at, at, '已经解析的Date不经JSON转换或再次parse');
    assert.equal((await resource.read()).revision, 1); assert.equal(connections, 1);
    left.next({ status: 'pending', generation: 2 });
    assert.equal(resource.peek().status, 'pending');
    const pending = resource.read(), end = new Error('ORIGINAL_OWNER_ENDED');
    const rejected = assert.rejects(pending, error => error === end);
    options.signal = new AbortController().signal;
    owner.abort(end); await rejected;
    assert.equal(completed, true); assert.equal(releases, 1);
    assert.throws(() => resource.peek(), error => error === end);
  } finally { stop(); monitor.unsubscribe(); owner.abort(); }
});

test('有限来源的最终观察供现有消费者复用，全部退出后显式重读，空完成和来源失败不会永等或偷偷重试', async () => {
  const owner = new AbortController();
  let calls = 0, value = new Date('2026-10-28T01:00:00Z');
  const resource = shareResource(defer(() => { calls++; return of(ready(value)); }), { signal: owner.signal });
  const stop = resource.subscribe(() => {});
  assert.equal(resource.peek().status, 'ready'); assert.equal(await resource.read(), value); assert.equal(calls, 1);
  stop(); assert.equal(resource.peek().status, 'pending');
  value = new Date('2026-10-28T02:00:00Z');
  assert.equal(await resource.read(), value); assert.equal(calls, 2); assert.equal(resource.peek().status, 'pending');
  const failure = new Error('QUERY_FAILED');
  const failed = shareResource(defer(() => { calls++; return throwError(() => failure); }), { signal: owner.signal });
  await assert.rejects(failed.read(), error => error === failure); assert.equal(calls, 3);
  await assert.rejects(failed.read(), error => error === failure); assert.equal(calls, 4);
  const empty = shareResource<never>(EMPTY, { signal: owner.signal });
  await assert.rejects(empty.read(), EmptyError);
  owner.abort();
});

test('最后消费者显式退订同步接收来源清理失败，错误接收方固定且不接管消费者自己的teardown', () => {
  for (const owned of [false, true]) {
    const owner = new AbortController(), source = new Subject<Observation<number>>();
    const failure = new Error('RESOURCE_SOURCE_RELEASE_FAILED'), consumerFailure = new Error('CONSUMER_RELEASE_FAILED');
    const observerFailure = new Error('CONSUMER_OBSERVER_FAILED'), cleanups: unknown[] = [], observers: unknown[] = [];
    let released = 0;
    const options = { signal: owner.signal, onObserverError: (error: unknown) => observers.push(error),
      onCleanupError: owned ? (error: unknown) => { cleanups.push(error); } : undefined };
    const resource = shareResource(new Observable<Observation<number>>(subscriber => {
      source.subscribe(subscriber);
      return () => { released++; throw failure; };
    }), options);
    const stop = resource.subscribe(observation => { if (observation.status === 'ready') throw observerFailure; });
    const consumer = resource.observations$.subscribe();
    consumer.add(() => { throw consumerFailure; });
    options.onCleanupError = () => { assert.fail('构造后不能更换来源清理错误的owner'); };
    source.next(ready(7));
    assert.deepEqual(observers, [observerFailure]); assert.equal(cleanups.length, 0);
    stop(); assert.equal(released, 0, '还有消费者时不释放共享来源');
    assert.throws(() => consumer.unsubscribe(), error => error instanceof UnsubscriptionError
      && error.errors.length === (owned ? 1 : 2) && error.errors.includes(consumerFailure)
      && (owned || error.errors.includes(failure)));
    assert.equal(released, 1); assert.equal(resource.peek().status, 'pending');
    assert.equal(cleanups.length, owned ? 1 : 0);
    if (owned) assert.ok(cleanups[0] instanceof UnsubscriptionError && cleanups[0].errors.length === 1 && cleanups[0].errors[0] === failure);
    assert.deepEqual(observers, [observerFailure], '来源清理不进入消费者回调错误出口');
    owner.abort(); source.complete();
  }
});

test('Signal撤销先释放真实来源，清理失败单独报告而观察与迟到读取保留原owner原因', t => {
  const reports: unknown[][] = [];
  t.mock.method(console, 'error', (...args: unknown[]) => { reports.push(args); });
  const owner = new AbortController(), source = new Subject<Observation<number>>();
  const failure = new Error('ABORT_SOURCE_RELEASE_FAILED'), reason = new Error('RESOURCE_OWNER_ABORTED');
  let released = 0, completed = false;
  const resource = shareResource(new Observable<Observation<number>>(subscriber => {
    source.subscribe(subscriber);
    return () => { released++; throw failure; };
  }), { signal: owner.signal });
  const observations: Observation<number>[] = [];
  const subscription = resource.observations$.subscribe({ next: value => observations.push(value), complete: () => { completed = true; } });
  source.next(ready(7));
  assert.doesNotThrow(() => owner.abort(reason));
  assert.equal(subscription.closed, true); assert.equal(completed, true); assert.equal(released, 1);
  assert.equal(reports.length, 1); assert.equal(reports[0]![0], 'reactive.resource.cleanup.failed');
  const reported = reports[0]![1];
  assert.ok(reported instanceof UnsubscriptionError && reported.errors.length === 1 && reported.errors[0] === failure);
  const ended = observations.at(-1)!;
  assert.equal(ended.status, 'error');
  if (ended.status === 'error') assert.equal(ended.error, reason);
  const late: Observation<number>[] = [];
  assert.equal(resource.observations$.subscribe(value => late.push(value)).closed, true);
  assert.equal(late.length, 1); assert.equal(late[0]!.status, 'error');
  if (late[0]!.status === 'error') assert.equal(late[0]!.error, reason);
  assert.throws(() => resource.read(), error => error === reason);
  assert.equal(released, 1); source.complete();
});

test('read取得值或被取消时即使最后连接清理失败仍按原事实结算，清理失败独立报告', { timeout: 2_000 }, async t => {
  const reports: unknown[][] = [];
  t.mock.method(console, 'error', (...args: unknown[]) => { reports.push(args); });
  for (const cancelled of [false, true]) {
    const owner = new AbortController(), caller = new AbortController(), source = new Subject<Observation<number>>();
    const failure = new Error('READ_SOURCE_RELEASE_FAILED'), reason = new Error('READ_CALLER_CANCELLED');
    let released = 0;
    const resource = shareResource(new Observable<Observation<number>>(subscriber => {
      source.subscribe(subscriber);
      return () => { released++; throw failure; };
    }), { signal: owner.signal });
    const reading = resource.read(caller.signal);
    if (cancelled) {
      const rejected = assert.rejects(reading, error => error === reason);
      assert.doesNotThrow(() => caller.abort(reason)); await rejected;
    } else {
      source.next(ready(7)); assert.equal(await reading, 7);
    }
    assert.equal(released, 1); assert.equal(resource.peek().status, 'pending');
    const reported = reports.at(-1)!;
    assert.equal(reported[0], 'reactive.resource.cleanup.failed');
    const error = reported[1];
    assert.ok(error instanceof UnsubscriptionError && error.errors.length === 1 && error.errors[0] === failure);
    owner.abort(); source.complete();
  }
  assert.equal(reports.length, 2);
});

test('旧连接清理中同步接入新消费者，新快照不被旧退出撤下或重复连接', async () => {
  const owner = new AbortController();
  let connections = 0, releases = 0, reconnect = true, stopNested: (() => void) | undefined;
  const resource = shareResource(new Observable<Observation<number>>(subscriber => {
    subscriber.next(ready(++connections));
    return () => {
      releases++;
      if (reconnect) {
        reconnect = false;
        assert.equal(resource.peek().status, 'pending', '旧来源teardown开始前已撤销其缓存');
        stopNested = resource.subscribe(() => {});
      }
    };
  }), { signal: owner.signal });
  const first = resource.observations$.subscribe();
  assert.equal(await resource.read(), 1);
  first.unsubscribe();
  assert.equal(releases, 1); assert.equal(connections, 2);
  assert.equal(await resource.read(), 2); assert.equal(connections, 2);
  stopNested?.(); assert.equal(releases, 2); assert.equal(resource.peek().status, 'pending');
  owner.abort();
});
