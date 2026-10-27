import { test } from 'node:test';
import assert from 'node:assert/strict';
import { BehaviorSubject, Observable, Subject, lastValueFrom, of } from 'rxjs';
import type { Observation } from '@hanger-source/dsh-reactive';
import { observeResource } from '@hanger-source/dsh-reactive';
import { combineResources, mapResource, switchResource } from '@hanger-source/dsh-reactive';

const ready = <T>(value: T, generation: number): Observation<T> => ({ status: 'ready', generation, value, observedAt: Date.now() });

test('持续组合在任一依赖失效时撤销旧资格，读取和投影错误可随新事实恢复且不能扩大业务许可', () => {
  const preparation = new BehaviorSubject(ready({ onboardingId: 'a', prepared: true }, 7));
  const knowledge = new BehaviorSubject(ready({ onboardingId: 'a', usable: true }, 7));
  const reception = new BehaviorSubject(ready({ onboardingId: 'a', canEnable: true }, 3));
  const seen: Observation<{ onboardingId: string; eligible: boolean }>[] = [];
  const subscription = combineResources({ preparation, knowledge, reception }).pipe(mapResource(facts => {
    if (facts.preparation.onboardingId !== facts.knowledge.onboardingId || facts.preparation.onboardingId !== facts.reception.onboardingId)
      throw new Error('BUSINESS_TARGET_CHANGED');
    return { onboardingId: facts.preparation.onboardingId,
      eligible: facts.preparation.prepared && facts.knowledge.usable && facts.reception.canEnable };
  })).subscribe(value => seen.push(value));
  const current = () => seen.at(-1)!;
  const initial = current();
  assert.deepEqual(initial.status === 'ready' ? initial.value : undefined, { onboardingId: 'a', eligible: true });
  knowledge.next({ status: 'pending', generation: 8 });
  assert.equal(current().status, 'pending', '知识失效后不能继续交付旧 ready 资格');
  const invalidated = current().generation;
  preparation.next(ready({ onboardingId: 'a', prepared: false }, 8));
  assert.equal(current().status, 'pending');
  assert.ok(current().generation > invalidated, '独立来源相同代次也须推进组合代次');
  const unavailable = new Error('KNOWLEDGE_SOURCE_UNAVAILABLE');
  knowledge.next({ status: 'error', generation: 8, error: unavailable });
  const failed = current();
  assert.equal(failed.status, 'error');
  if (failed.status === 'error') assert.equal(failed.error, unavailable);
  reception.next({ status: 'pending', generation: 4 });
  assert.equal(current().status, 'error', '另一来源 pending 不掩盖已知读取错误');
  knowledge.next(ready({ onboardingId: 'a', usable: true }, 9));
  assert.equal(current().status, 'pending');
  reception.next(ready({ onboardingId: 'a', canEnable: false }, 5));
  preparation.next(ready({ onboardingId: 'a', prepared: true }, 9));
  const denied = current();
  assert.equal(denied.status, 'ready');
  if (denied.status === 'ready') assert.equal(denied.value.eligible, false, '派生前提不能把后端拒绝改成可操作');
  knowledge.next(ready({ onboardingId: 'b', usable: true }, 10));
  const mismatched = current();
  assert.equal(mismatched.status, 'error');
  if (mismatched.status === 'error') assert.match(String(mismatched.error), /BUSINESS_TARGET_CHANGED/u);
  knowledge.next(ready({ onboardingId: 'a', usable: true }, 11));
  assert.equal(current().status, 'ready', '一次投影错误不终止后来代次');
  subscription.unsubscribe();
});

test('组合由各消费者独立连接，退订一个查询不停止另一个查询', () => {
  const changes = new Subject<Observation<number>>();
  let connected = 0, released = 0;
  const source = new Observable<Observation<number>>(subscriber => {
    connected++;
    const subscription = changes.subscribe(subscriber);
    return () => { released++; subscription.unsubscribe(); };
  });
  const resource = combineResources({ source });
  assert.equal(connected, 0);
  const first: Observation<{ readonly source: number }>[] = [], second: Observation<{ readonly source: number }>[] = [];
  const a = resource.subscribe(value => first.push(value)), b = resource.subscribe(value => second.push(value));
  assert.equal(connected, 2, '共享必须由既有 owner 显式持有');
  changes.next(ready(1, 0)); a.unsubscribe(); changes.next(ready(2, 1));
  assert.equal(released, 1);
  const firstLast = first.at(-1)!, secondLast = second.at(-1)!;
  assert.equal(firstLast.status, 'ready'); assert.equal(secondLast.status, 'ready');
  if (firstLast.status === 'ready') assert.equal(firstLast.value.source, 1);
  if (secondLast.status === 'ready') assert.equal(secondLast.value.source, 2);
  b.unsubscribe(); assert.equal(released, 2);

});

test('具名来源将__proto__和constructor作为业务键交付各自的实际值', async () => {
  const sources = Object.fromEntries([['__proto__', of(ready('actual-source', 1))], ['constructor', of(ready('other-source', 1))]]);
  const observation = await lastValueFrom(combineResources(sources));
  assert.equal(observation.status, 'ready');
  if (observation.status !== 'ready') return;
  assert.deepEqual(Object.entries(observation.value), [['__proto__', 'actual-source'], ['constructor', 'other-source']]);
});

test('依赖失效立即退订真实下游读取，旧结果迟到不回写，来源与构造错误后可选新依赖', async () => {
  const upstream = new Subject<Observation<string>>();
  const reads: Array<{ key: string; signal: AbortSignal; result: ReturnType<typeof Promise.withResolvers<string>> }> = [];
  const released: string[] = [];
  const seen: Observation<string>[] = [];
  const resource = upstream.pipe(switchResource(key => {
    if (key === 'invalid') throw new Error('DEPENDENT_SOURCE_INVALID');
    return new Observable<Observation<string>>(subscriber => {
      const subscription = observeResource({ key$: of(key), read(current, signal) {
        const result = Promise.withResolvers<string>();
        reads.push({ key: current, signal, result });
        return result.promise;
      } }).subscribe(subscriber);
      return () => { released.push(key); subscription.unsubscribe(); };
    });
  }));
  const subscription = resource.subscribe(value => seen.push(value));
  const current = () => seen.at(-1)!;
  upstream.next(ready('old-source', 10));
  assert.equal(reads.length, 1); assert.equal(reads[0]!.signal.aborted, false);
  upstream.next({ status: 'pending', generation: 11 });
  assert.equal(reads[0]!.signal.aborted, true); assert.deepEqual(released, ['old-source']);
  assert.equal(current().status, 'pending');
  reads[0]!.result.resolve('stale-knowledge');
  await new Promise<void>(resolve => setImmediate(resolve));
  assert.equal(current().status, 'pending');
  upstream.next(ready('current-source', 12));
  assert.equal(reads[1]!.key, 'current-source');
  assert.equal(current().status, 'pending');
  reads[1]!.result.resolve('current-knowledge');
  await new Promise<void>(resolve => setImmediate(resolve));
  const available = current();
  assert.equal(available.status, 'ready');
  if (available.status === 'ready') assert.equal(available.value, 'current-knowledge');
  const failure = new Error('PREPARATION_UNAVAILABLE');
  upstream.next({ status: 'error', generation: 13, error: failure });
  const failed = current();
  assert.equal(failed.status, 'error');
  if (failed.status === 'error') assert.equal(failed.error, failure);
  assert.deepEqual(released, ['old-source', 'current-source']);
  upstream.next(ready('invalid', 14));
  const invalid = current();
  assert.equal(invalid.status, 'error');
  if (invalid.status === 'error') assert.match(String(invalid.error), /DEPENDENT_SOURCE_INVALID/u);
  upstream.next(ready('next-source', 15));
  assert.equal(current().status, 'pending');
  assert.equal(reads.length, 3);
  subscription.unsubscribe();
  assert.equal(reads[2]!.signal.aborted, true);
  assert.deepEqual(released, ['old-source', 'current-source', 'next-source']);
  assert.ok(!seen.some(value => value.status === 'ready' && value.value === 'stale-knowledge'));
});

test('显式业务依赖保留同一读取，tuple修改不能篡改身份，失效及校验错误后同依赖重新读取', async () => {
  type Facts = { merchantId: string; revision: number; label: string; invalidDependency?: boolean };
  const upstream = new Subject<Observation<Facts>>();
  const selected: unknown[] = [];
  const selectorFailure = new Error('DEPENDENCY_SELECTION_FAILED');
  const reads: Array<{ revision: number; signal: AbortSignal; result: ReturnType<typeof Promise.withResolvers<string>> }> = [];
  const seen: Observation<string>[] = [];
  const resource = upstream.pipe(mapResource(value => {
    if (value.merchantId !== 'merchant-a') throw new Error('BUSINESS_TARGET_CHANGED');
    return value;
  }), switchResource(value => observeResource({ key$: of(value.revision), read(revision, signal) {
    const result = Promise.withResolvers<string>(); reads.push({ revision, signal, result }); return result.promise;
  } }), { dependencies(value) {
    if (value.invalidDependency) throw selectorFailure;
    selected[0] = value.merchantId; selected[1] = value.revision;
    return selected;
  } }));
  const subscription = resource.subscribe(value => seen.push(value));
  const current = () => seen.at(-1)!;
  const publish = (revision: number, label: string, extra: Partial<Facts> = {}) =>
    upstream.next(ready({ merchantId: 'merchant-a', revision, label, ...extra }, seen.length + 1));
  const flush = () => new Promise<void>(resolve => setImmediate(resolve));
  try {
    publish(1, 'first label');
    assert.equal(reads.length, 1);
    const pending = current();
    publish(1, 'changed label');
    assert.equal(reads.length, 1, '未选中的业务字段变化不重读，也不撤销当前读取');
    assert.equal(reads[0]!.signal.aborted, false);
    assert.equal(current(), pending);

    selected[1] = 2;
    publish(2, 'new revision');
    assert.equal(reads.length, 2, '复用tuple在外部被修改后，原先捕获的revision仍是1，必须开始revision2读取');
    assert.equal(reads[0]!.signal.aborted, true);
    assert.equal(current().status, 'pending');
    reads[0]!.result.resolve('stale-revision-1');
    await flush(); assert.equal(current().status, 'pending');
    reads[1]!.result.resolve('knowledge-revision-2'); await flush();
    const available = current();
    assert.equal(available.status, 'ready');
    if (available.status === 'ready') assert.equal(available.value, 'knowledge-revision-2');
    publish(2, 'another label');
    assert.equal(current(), available);
    assert.equal(reads.length, 2);

    publish(3, 'revision3');
    upstream.next({ status: 'pending', generation: 20 });
    assert.equal(reads[2]!.signal.aborted, true);
    reads[2]!.result.resolve('stale-revision-3'); await flush();
    assert.equal(current().status, 'pending');
    publish(3, 'same revision after invalidation');
    assert.equal(reads.length, 4, 'pending清除身份，同一业务依赖恢复也必须重新读取');
    publish(3, 'selector failed', { invalidDependency: true });
    assert.equal(reads[3]!.signal.aborted, true);
    const failed = current(); assert.equal(failed.status, 'error');
    if (failed.status === 'error') assert.equal(failed.error, selectorFailure);
    publish(3, 'selector recovered');
    assert.equal(reads.length, 5, 'selector错误不能让上游终止，也不能留下旧依赖身份');
    const unavailable = new Error('PREPARATION_UNAVAILABLE');
    upstream.next({ status: 'error', generation: 30, error: unavailable });
    assert.equal(reads[4]!.signal.aborted, true);
    const sourceFailure = current(); assert.equal(sourceFailure.status, 'error');
    if (sourceFailure.status === 'error') assert.equal(sourceFailure.error, unavailable);
    publish(3, 'source recovered');
    assert.equal(reads.length, 6);
    publish(3, 'foreign facts', { merchantId: 'merchant-b' });
    assert.equal(reads[5]!.signal.aborted, true);
    assert.equal(current().status, 'error');
    publish(3, 'correct facts after foreign target');
    assert.equal(reads.length, 7, '领域校验用mapResource交付当前error，不能永久终止同一个权威事实流');
    reads[6]!.result.resolve('verified-current-knowledge'); await flush();
    const recovered = current(); assert.equal(recovered.status, 'ready');
    if (recovered.status === 'ready') assert.equal(recovered.value, 'verified-current-knowledge');
    assert.ok(!seen.some(value => value.status === 'ready' && value.value.startsWith('stale-')));
  } finally { subscription.unsubscribe(); upstream.complete(); }
});
