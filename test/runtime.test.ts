import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Observable, Subject, BehaviorSubject, EMPTY, of, firstValueFrom, mergeMap, take } from 'rxjs';
import { Scope } from '@hanger-source/dsh-reactive/kernel';
import { ScopeDisposedError, type Observation } from '@hanger-source/dsh-reactive';
import { connect } from '@hanger-source/dsh-reactive/kernel';
import { fromSnapshot, abortable } from '@hanger-source/dsh-reactive';
import { observeResource } from '@hanger-source/dsh-reactive';

const flush = () => new Promise<void>(resolve => setImmediate(resolve));

test('外部signal结束本次Promise等待并保留原因，其他消费者、已完成结果及迟到拒绝各自结算', async () => {
  const source = Promise.withResolvers<number>();
  const first = new AbortController(), second = new AbortController();
  let firstSignal: AbortSignal | undefined, secondSignal: AbortSignal | undefined;
  const cancelled = firstValueFrom(abortable(signal => { firstSignal = signal; return source.promise; }, first.signal));
  const continued = firstValueFrom(abortable(signal => { secondSignal = signal; return source.promise; }, second.signal));
  const reason = new Error('ONLY_THIS_CONSUMER');
  const rejected = assert.rejects(cancelled, error => error === reason);
  first.abort(reason); await rejected;
  assert.equal(firstSignal?.reason, reason); assert.equal(secondSignal?.aborted, false);
  source.resolve(7); assert.equal(await continued, 7);
  second.abort(new Error('AFTER_COMPLETION')); assert.equal(secondSignal?.aborted, false);

  let calls = 0;
  await assert.rejects(firstValueFrom(abortable(() => { calls++; return 1; }, first.signal)), error => error === reason);
  assert.equal(calls, 0, '已取消的消费不进入提供者');
  const late = Promise.withResolvers<number>(), synchronous = new AbortController();
  const ended = firstValueFrom(abortable(() => { synchronous.abort(reason); return late.promise; }, synchronous.signal));
  await assert.rejects(ended, error => error === reason);
  late.reject(new Error('LATE_PROVIDER_FAILURE')); await flush();
});

test('snapshot 先监听再读取，登记期间的变化不会遗失，同步完成也释放监听', () => {
  let value = 1;
  const listeners = new Set<() => void>();
  const source$ = fromSnapshot({
    getSnapshot: () => value,
    subscribe(notify) {
      listeners.add(notify);
      value = 2; notify();
      return () => { listeners.delete(notify); };
    },
  });
  const seen: number[] = [];
  const subscription = source$.subscribe(current => seen.push(current));
  assert.deepEqual(seen, [2]);
  value = 3; for (const notify of listeners) notify();
  assert.deepEqual(seen, [2, 3]);
  subscription.unsubscribe();
  assert.equal(listeners.size, 0);
  source$.pipe(take(1)).subscribe();
  assert.equal(listeners.size, 0);
});

test('owner 立即连接一次，多消费者与无人观察时共享事实，owner释放取消来源并结束观察', () => {
  const scope = new Scope();
  const events = new Subject<number>();
  let starts = 0, stops = 0;
  const source$ = new Observable<number>(subscriber => {
    starts++;
    const subscription = events.subscribe(subscriber);
    return () => { stops++; subscription.unsubscribe(); };
  });
  const shared$ = connect(scope, source$);
  events.next(1);
  const first: number[] = [], second: number[] = [];
  const a = shared$.subscribe(value => first.push(value));
  const b = connect(scope, source$).subscribe(value => second.push(value));
  events.next(2); a.unsubscribe(); b.unsubscribe();
  events.next(3);
  const late: number[] = [];
  let completed = false;
  shared$.subscribe({ next: value => late.push(value), complete: () => { completed = true; } });
  assert.deepEqual(first, [1, 2]);
  assert.deepEqual(second, [1, 2]);
  assert.deepEqual(late, [3]);
  assert.equal(starts, 1);
  assert.equal(stops, 0);
  const reentered: number[] = [];
  shared$.subscribe({ complete: () => { shared$.subscribe(value => reentered.push(value)); } });
  scope.dispose();
  events.next(4);
  assert.equal(stops, 1);
  assert.equal(completed, true);
  assert.deepEqual(late, [3]);
  assert.deepEqual(reentered, [], 'owner退出的complete中重入订阅也不能取得旧值');
  const after: number[] = [];
  assert.equal(shared$.subscribe(value => after.push(value)).closed, true);
  assert.deepEqual(after, []);
  assert.equal(starts, 1, '退出后的迟到订阅不能重连来源');
});

test('共享来源失败保留同一错误，不随新消费者重新执行，不同owner独立连接', () => {
  const first = new Scope(), second = new Scope();
  const failure = new Error('正式来源拒绝');
  let starts = 0;
  const source$ = new Observable<number>(subscriber => { starts++; subscriber.error(failure); });
  const shared$ = connect(first, source$);
  const errors: unknown[] = [];
  shared$.subscribe({ error: error => errors.push(error) });
  connect(first, source$).subscribe({ error: error => errors.push(error) });
  assert.equal(starts, 1);
  connect(second, source$).subscribe({ error: error => errors.push(error) });
  assert.equal(starts, 2);
  assert.deepEqual(errors, [failure, failure, failure]);
  first.dispose(); second.dispose();
  let ended = false;
  shared$.subscribe({ next() { assert.fail('已退出owner不能交付旧值'); },
    error() { assert.fail('已退出owner不能重放旧错误'); }, complete: () => { ended = true; } });
  assert.equal(ended, true);
  assert.equal(starts, 2);
});

test('key切换和失效立即撤销旧读取并发布pending，失败是当前代次状态且后续通知可恢复', async () => {
  const scope = new Scope();
  const keys = new BehaviorSubject('a');
  const notifications = new Map([['a', new Subject<void>()], ['b', new Subject<void>()]]);
  const reads: Array<{ key: string; signal: AbortSignal; result: ReturnType<typeof Promise.withResolvers<string>> }> = [];
  const resource$ = observeResource({
    key$: keys,
    read(key, signal) {
      const result = Promise.withResolvers<string>();
      reads.push({ key, signal, result });
      return result.promise;
    },
    invalidations: key => notifications.get(key)!,
  });
  const observed: Observation<string>[] = [];
  connect(scope, resource$).subscribe(value => observed.push(value));
  assert.equal(observed.at(-1)?.status, 'pending');
  keys.next('b');
  assert.equal(reads[0]!.signal.aborted, true);
  assert.equal(reads[1]!.signal.aborted, false);
  assert.equal(observed.at(-1)?.status, 'pending');
  reads[0]!.result.resolve('旧key的迟到值');
  const failure = new Error('b暂时不可读');
  reads[1]!.result.reject(failure);
  await flush();
  const failed = observed.at(-1)!;
  assert.equal(failed.status, 'error');
  if (failed.status === 'error') assert.equal(failed.error, failure);
  notifications.get('a')!.next();
  assert.equal(reads.length, 2);
  notifications.get('b')!.next();
  assert.equal(observed.at(-1)?.status, 'pending');
  reads[2]!.result.resolve('b当前事实');
  await flush();
  const current = observed.at(-1)!;
  assert.equal(current.status, 'ready');
  if (current.status === 'ready') assert.equal(current.value, 'b当前事实');
  assert.ok(!observed.some(item => item.status === 'ready' && item.value === '旧key的迟到值'));
  notifications.get('b')!.next();
  assert.equal(observed.at(-1)?.status, 'pending');
  const pending = reads[3]!;
  scope.dispose();
  assert.equal(pending.signal.aborted, true);
  pending.result.resolve('owner释放后的迟到值');
  await flush();
  assert.ok(!observed.some(item => item.status === 'ready' && item.value === 'owner释放后的迟到值'));
});

test('冷资源不暗中共享，通知先于初读建立，读取中的同步失效不会丢失', async () => {
  const invalidations = new Subject<void>();
  let calls = 0;
  const reads = [Promise.withResolvers<number>(), Promise.withResolvers<number>(), Promise.withResolvers<number>()];
  const signals: AbortSignal[] = [];
  const source$ = observeResource({
    key$: of(null),
    invalidations: () => invalidations,
    read(_key, signal) {
      const index = calls++;
      signals.push(signal);
      if (index === 0) invalidations.next();
      return reads[index]!.promise;
    },
  });
  const first: Observation<number>[] = [];
  const a = source$.subscribe(value => first.push(value));
  assert.equal(calls, 2);
  assert.equal(signals[0]!.aborted, true);
  const b = source$.subscribe();
  assert.equal(calls, 3);
  reads[0]!.resolve(-1); reads[1]!.resolve(2); reads[2]!.resolve(3);
  await flush();
  assert.deepEqual(first.filter(value => value.status === 'ready').map(value => value.value), [2]);
  a.unsubscribe(); b.unsubscribe();
});

test('owner释放将退订传给两个实际异步边界，来源忽略取消后迟到结果仍不交付', async () => {
  const scope = new Scope();
  const signals: AbortSignal[] = [];
  const results = [Promise.withResolvers<number>(), Promise.withResolvers<number>()];
  let index = 0;
  const parallel$ = connect(scope, of(1, 2).pipe(mergeMap(() => abortable(signal => {
    signals.push(signal); return results[index++]!.promise;
  }))));
  const seen: number[] = [];
  let complete = false;
  parallel$.subscribe({ next: value => seen.push(value), complete: () => { complete = true; } });
  assert.equal(signals.length, 2);
  assert.ok(signals.every(signal => !signal.aborted));
  scope.dispose();
  assert.ok(signals.every(signal => signal.aborted));
  results[0]!.resolve(1); results[1]!.resolve(2);
  await flush();
  assert.deepEqual(seen, []);
  assert.equal(complete, true);
});

test('内部快照读取不导致重复来源执行，来源空完成明确失败，释放拒绝挂起读取', async () => {
  const scope = new Scope();
  const events = new Subject<number>();
  const current = scope.observe(events);
  events.next(1);
  assert.equal(await current.read(), 1);
  const stop = current.values$.subscribe();
  stop.unsubscribe(); events.next(2);
  assert.equal(await current.read(), 2);
  const hanging = scope.observe(new Observable<never>(() => {})).read();
  scope.dispose();
  await assert.rejects(hanging, ScopeDisposedError);
  const emptyScope = new Scope();
  await assert.rejects(emptyScope.observe(EMPTY).read(), { name: 'EmptyError' });
  emptyScope.dispose();
});
