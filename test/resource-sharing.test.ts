import assert from 'node:assert/strict';
import { test } from 'node:test';
import { BehaviorSubject, EMPTY, EmptyError, Observable, defer, firstValueFrom, of, take, throwError } from 'rxjs';
import { combineResources, mapResource, shareResource, type Observation } from '@hanger-source/dsh-reactive';

const ready = <T>(value: T): Observation<T> => ({ status: 'ready', generation: 1, observedAt: Date.now(), value });

test('同一派生资源共享真实RxJS连接，首帧同步退出不启动来源，取消一次read不影响其他读者且owner不可换绑', async () => {
  const left = new BehaviorSubject<Observation<Date>>({ status: 'pending', generation: 0 });
  const right = new BehaviorSubject(ready(1));
  const composed = combineResources({ left, right }).pipe(mapResource(value => ({ at: value.left, revision: value.right })));
  let connections = 0, releases = 0;
  const owner = new AbortController(), options = { signal: owner.signal };
  const resource = shareResource(new Observable<Observation<{ at: Date; revision: number }>>(subscriber => {
    connections++;
    const subscription = composed.subscribe(subscriber);
    return () => { releases++; subscription.unsubscribe(); };
  }), options);
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
