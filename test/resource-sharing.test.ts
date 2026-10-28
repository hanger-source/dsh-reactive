import assert from 'node:assert/strict';
import { test } from 'node:test';
import { BehaviorSubject, EMPTY, EmptyError, Observable, defer, firstValueFrom, of, take, throwError } from 'rxjs';
import { combineResources, mapResource, shareResource, ResourceReader, Scope, createResource, z, type Observation } from '@hanger-source/dsh-reactive';

const ready = <T>(value: T): Observation<T> => ({ status: 'ready', generation: 1, observedAt: Date.now(), value });

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
  resource.values$.subscribe({ next: value => values.push(value), error: error => errors.push(error) });
  await resource.read();
  value.changes$.subscribe(value => values.push(value));
  let effects = 0, cleanups = 0;
  child.own(() => { cleanups++; throw failure; });
  parent.own(() => {
    assert.equal(child.disposed, true); assert.equal(nested.disposed, true);
    assert.equal(nested.signal.reason, parent.signal.reason);
    assert.throws(() => resource.peek(), error => error === parent.signal.reason);
    assert.throws(() => nested.effect(() => { effects++; }), error => error === parent.signal.reason);
    assert.throws(() => value.set(2), error => error === parent.signal.reason);
  });
  assert.throws(() => parent.dispose(), error => error instanceof AggregateError
    && error.errors.some(error => error instanceof AggregateError && error.errors.includes(failure)));
  assert.deepEqual(values, [42, 1]); assert.deepEqual(errors, []); assert.equal(effects, 0); assert.equal(cleanups, 1);
  parent.dispose(); child.dispose(); nested.dispose();
  assert.equal(cleanups, 1);
  const late: Observation<number>[] = [];
  assert.equal(resource.observations$.subscribe(value => late.push(value)).closed, true);
  assert.equal(late.length, 1); assert.equal(late[0]!.status, 'error');
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

test('旧连接清理中同步接入新消费者，新快照不被旧退出撤下或重复连接', async () => {
  const owner = new AbortController();
  let connections = 0, releases = 0, reconnect = true, stopNested: (() => void) | undefined;
  const resource = shareResource(new Observable<Observation<number>>(subscriber => {
    subscriber.next(ready(++connections));
    return () => {
      releases++;
      if (reconnect) { reconnect = false; stopNested = resource.subscribe(() => {}); }
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
