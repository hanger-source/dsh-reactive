import { test } from 'node:test';
import assert from 'node:assert/strict';
import { setImmediate } from 'node:timers/promises';
import { EmptyError, Subject, defer, finalize, of, tap } from 'rxjs';
import { z } from 'zod';
import { bindAction, defineAction, implementAction } from '@hanger-source/dsh-reactive';
import { Scope } from '@hanger-source/dsh-reactive/kernel';
import { ScopeDisposedError } from '../packages/core/src/reactivity/scope.ts';
import { abortable } from '@hanger-source/dsh-reactive';
import { observeResource } from '@hanger-source/dsh-reactive';
import { type Observation } from '@hanger-source/dsh-reactive';
import type { ProgramFrame, SurfaceContribution } from '../packages/core/src/program/frame.ts';
import { createProgramOutputs } from '../packages/core/src/surfaces/outputs.ts';
import { defineSurface, resourceView, view } from '@hanger-source/dsh-reactive';

function frame(...surfaces: SurfaceContribution[]): ProgramFrame {
  return { revision: 1, contexts: {}, offers: {}, follows: {}, nativeTools: {}, skills: [], surfaces };
}

test('公开出口随完整帧切换来源，消费者退订不停止连接，旧查询取消且晚结果不能覆盖新结果', async () => {
  const scope = new Scope();
  let executions = 0;
  const save = defineAction({ name: 'save', description: '保存业务结果', scope: z.object({ merchantId: z.string() }),
    input: z.null(), output: z.boolean() });
  const saveImplementation = implementAction(save, {
    execute: () => { executions++; return true; }
  });
  const firstAction = bindAction(saveImplementation, { merchantId: 'first' }, { admit() {} });
  const secondAction = bindAction(saveImplementation, { merchantId: 'second' }, { admit() {} });
  const surface = defineSurface({ views: { total: view(z.number()) }, actions: { save } });
  const outputs = createProgramOutputs(scope, surface);
  const total = outputs.views.total;
  const wire = outputs.wireViews.total;
  const observations$ = total.observations$;
  const values$ = total.values$;
  const first = Promise.withResolvers<number>(), second = Promise.withResolvers<number>(), third = Promise.withResolvers<number>();
  const firstSignals: AbortSignal[] = [], secondSignals: AbortSignal[] = [], thirdSignals: AbortSignal[] = [];
  const first$ = abortable(signal => { firstSignals.push(signal); return first.promise; });
  const second$ = abortable(signal => { secondSignals.push(signal); return second.promise; });
  const third$ = abortable(signal => { thirdSignals.push(signal); return third.promise; });
  try {
    assert.equal(total.peek().status, 'pending');
    const consumer = observations$.subscribe();
    outputs.apply(frame({ surface, views: { total: first$ }, actions: { save: firstAction } }));
    const waiting = total.read();
    outputs.apply(frame({ surface, views: { total: first$ }, actions: { save: firstAction } }));
    consumer.unsubscribe();
    assert.equal(firstSignals.length, 1);
    assert.equal(firstSignals[0]!.aborted, false);
    assert.equal(outputs.actions.peek().save, firstAction);

    const firstPending = total.peek();
    outputs.apply(frame({ surface, views: { total: second$ }, actions: { save: secondAction } }));
    const secondPending = total.peek();
    assert.equal(firstSignals[0]!.aborted, true);
    assert.equal(secondSignals.length, 1);
    assert.equal(secondPending.status, 'pending');
    assert.ok(secondPending.generation > firstPending.generation);
    assert.equal(outputs.actions.peek().save, secondAction);
    first.resolve(99);
    await setImmediate();
    assert.equal(total.peek(), secondPending);
    second.resolve(7);
    assert.equal(await waiting, 7);
    assert.equal(await wire.read(), 7);
    assert.equal(outputs.views.total, total);
    assert.equal(outputs.wireViews.total, wire);
    assert.equal(total.observations$, observations$);
    assert.equal(total.values$, values$);
    assert.equal(executions, 0);

    outputs.apply(frame());
    assert.equal(total.peek().status, 'pending');
    assert.equal(wire.peek().status, 'pending');
    assert.deepEqual(outputs.actions.peek(), {});
    assert.equal(firstAction.released, false);
    assert.equal(secondAction.released, false);
    outputs.apply(frame({ surface, views: { total: third$ } }));
    const pendingRead = total.read();
    let ended = false;
    total.observations$.subscribe({ complete: () => { ended = true; } });
    scope.dispose();
    await assert.rejects(pendingRead, ScopeDisposedError);
    assert.equal(thirdSignals[0]!.aborted, true);
    assert.equal(ended, true);
  } finally { scope.dispose(); }
});

test('每次领域值只 encode 一次，领域与 wire 各保留合同形态，编码失败和来源错误正式进入两边的 error', async () => {
  const scope = new Scope();
  let encodes = 0, decodes = 0, connections = 0;
  const encodingFailure = new Error('业务日期拒绝编码');
  const date = view(z.codec(z.iso.datetime(), z.date(), {
    decode: value => { decodes++; return new Date(value); },
    encode: value => { encodes++; if (value.getUTCFullYear() === 2040) throw encodingFailure; return value.toISOString(); },
  }));
  const surface = defineSurface({ views: { observedAt: date }, actions: {} });
  const outputs = createProgramOutputs(scope, surface);
  const values = new Subject<Date>();
  const source$ = defer(() => { connections++; return values; });
  try {
    outputs.apply(frame({ surface, views: { observedAt: source$ } }));
    const domainRead = outputs.views.observedAt.read();
    const wireRead = outputs.wireViews.observedAt.read();
    const value = new Date('2026-10-26T00:00:00.000Z');
    values.next(value);
    assert.equal(await domainRead, value);
    assert.equal(await wireRead, '2026-10-26T00:00:00.000Z');
    assert.equal(encodes, 1); assert.equal(decodes, 0);
    const domainObserver = outputs.views.observedAt.values$.subscribe();
    const wireObserver = outputs.wireViews.observedAt.values$.subscribe();
    domainObserver.unsubscribe(); wireObserver.unsubscribe();
    outputs.apply(frame({ surface, views: { observedAt: source$ } }));
    assert.equal(encodes, 1); assert.equal(connections, 1);
    values.next(new Date('2040-01-01T00:00:00.000Z'));
    assert.equal(encodes, 2); assert.equal(decodes, 0);
    await assert.rejects(outputs.views.observedAt.read(), error => error === encodingFailure);
    await assert.rejects(outputs.wireViews.observedAt.read(), error => error === encodingFailure);
    assert.equal(values.observed, false);
    outputs.apply(frame({ surface, views: { observedAt: source$ } }));
    assert.equal(connections, 1);

    const replacement = new Subject<Date>();
    outputs.apply(frame({ surface, views: { observedAt: replacement } }));
    assert.equal(outputs.views.observedAt.peek().status, 'pending');
    const sourceFailure = new Error('业务读取失败');
    replacement.error(sourceFailure);
    await assert.rejects(outputs.views.observedAt.read(), error => error === sourceFailure);
    await assert.rejects(outputs.wireViews.observedAt.read(), error => error === sourceFailure);
  } finally { scope.dispose(); }
});

test('资源出口保留真实查询的失效与错误，原连接可恢复，换源取消查询且仅 ready 编码一次', async () => {
  const scope = new Scope();
  let encodes = 0, decodes = 0, connections = 0, disconnections = 0;
  const date = resourceView(z.codec(z.iso.datetime(), z.date(), {
    decode: value => { decodes++; return new Date(value); },
    encode: value => { encodes++; return value.toISOString(); },
  }));
  const surface = defineSurface({ views: { current: date }, actions: {} });
  const outputs = createProgramOutputs(scope, surface);
  const domain = outputs.views.current, wire = outputs.wireViews.current;
  const invalidations = new Subject<void>();
  const reads: { signal: AbortSignal; result: ReturnType<typeof Promise.withResolvers<Date>> }[] = [];
  const sourceObservations: Observation<Date>[] = [];
  const source$ = defer(() => {
    connections++;
    return observeResource({ key$: of('merchant'), invalidations: () => invalidations,
      read(key, signal) {
        assert.equal(key, 'merchant');
        const result = Promise.withResolvers<Date>();
        reads.push({ signal, result });
        return result.promise;
      },
    }).pipe(tap(observation => { sourceObservations.push(observation); }), finalize(() => { disconnections++; }));
  });
  try {
    const consumer = domain.observations$.subscribe();
    outputs.apply(frame({ surface, views: { current: source$ } }));
    const firstRead = domain.read();
    const first = new Date('2026-10-26T00:00:00.000Z');
    reads[0]!.result.resolve(first);
    assert.equal(await firstRead, first);
    assert.equal(await wire.read(), first.toISOString());
    const firstObservation = domain.peek();
    const latest = sourceObservations.at(-1);
    assert.ok(firstObservation.status === 'ready' && latest?.status === 'ready');
    assert.equal(firstObservation.observedAt, latest.observedAt);
    consumer.unsubscribe();
    outputs.apply(frame({ surface, views: { current: source$ } }));
    assert.equal(connections, 1); assert.equal(disconnections, 0);

    invalidations.next();
    assert.equal(domain.peek().status, 'pending');
    assert.equal(wire.peek().status, 'pending');
    assert.equal(encodes, 1);
    const waiting = domain.read();
    invalidations.next();
    const pending = domain.peek();
    assert.equal(reads[1]!.signal.aborted, true);
    reads[1]!.result.resolve(new Date('2040-01-01T00:00:00.000Z'));
    await setImmediate();
    assert.equal(domain.peek(), pending);
    const sourceFailure = new Error('查询暂时失败');
    reads[2]!.result.reject(sourceFailure);
    await assert.rejects(waiting, error => error === sourceFailure);
    await assert.rejects(wire.read(), error => error === sourceFailure);
    assert.equal(connections, 1); assert.equal(disconnections, 0);
    assert.equal(encodes, 1);

    invalidations.next();
    assert.equal(domain.peek().status, 'pending');
    assert.equal(wire.peek().status, 'pending');
    const recovered = domain.read();
    const second = new Date('2026-10-27T00:00:00.000Z');
    reads[3]!.result.resolve(second);
    assert.equal(await recovered, second);
    assert.equal(await wire.read(), second.toISOString());
    assert.equal(encodes, 2); assert.equal(decodes, 0);

    invalidations.next();
    const switched = domain.read();
    const replacement = new Subject<Observation<Date>>();
    outputs.apply(frame({ surface, views: { current: replacement } }));
    assert.equal(reads[4]!.signal.aborted, true);
    assert.equal(invalidations.observed, false);
    assert.equal(disconnections, 1);
    const replacementPending = domain.peek();
    reads[4]!.result.resolve(first);
    await setImmediate();
    assert.equal(domain.peek(), replacementPending);
    const third = new Date('2026-10-28T00:00:00.000Z');
    replacement.next({ status: 'ready', generation: 0, value: third, observedAt: -1 });
    assert.equal(await switched, third);
    assert.equal(await wire.read(), third.toISOString());
    const thirdObservation = domain.peek();
    assert.ok(thirdObservation.status === 'ready');
    assert.equal(thirdObservation.observedAt, -1);
    replacement.complete();
    assert.equal(await domain.read(), third);
    assert.equal(encodes, 3); assert.equal(decodes, 0);
  } finally { scope.dispose(); }
});

test('资源模式拒绝坏观察且终止该来源，未就绪完成不能永等，普通值的 status 不被猜成资源', async () => {
  const scope = new Scope();
  const surface = defineSurface({ views: {
    business: view(z.object({ status: z.literal('pending'), generation: z.number() })),
    resource: resourceView(z.number()),
  }, actions: {} });
  const outputs = createProgramOutputs(scope, surface);
  const businessValue = { status: 'pending' as const, generation: 3 };
  const business$ = of(businessValue);
  try {
    const invalid: readonly [unknown, RegExp][] = [
      [7, /REACTIVE_VIEW_OBSERVATION_INVALID/u],
      [{ status: 'pending', generation: 0.5 }, /REACTIVE_VIEW_GENERATION_INVALID/u],
      [{ status: 'ready', generation: 1, value: 7, observedAt: Infinity }, /REACTIVE_VIEW_OBSERVED_AT_INVALID/u],
      [{ status: 'error', generation: 1 }, /REACTIVE_VIEW_OBSERVATION_INVALID/u],
    ];
    for (const [value, pattern] of invalid) {
      const source = new Subject<unknown>();
      outputs.apply(frame({ surface, views: { business: business$, resource: source } }));
      source.next(value);
      await assert.rejects(outputs.views.resource.read(), pattern);
      const failure = outputs.views.resource.peek();
      assert.ok(failure.status === 'error');
      await assert.rejects(outputs.wireViews.resource.read(), error => error === failure.error);
      assert.equal(source.observed, false);
      source.next({ status: 'ready', generation: 2, value: 99, observedAt: 0 });
      assert.equal(outputs.views.resource.peek(), failure);
      outputs.apply(frame({ surface, views: { business: business$, resource: source } }));
      assert.equal(source.observed, false);
    }
    assert.equal(await outputs.views.business.read(), businessValue);
    assert.deepEqual(await outputs.wireViews.business.read(), businessValue);

    const pending: Observation<number> = { status: 'pending', generation: 1 };
    const ready: Observation<number> = { status: 'ready', generation: 1, value: 42, observedAt: 0 };
    outputs.apply(frame({ surface, views: { business: business$, resource: of(ready, pending) } }));
    await assert.rejects(outputs.views.resource.read(), EmptyError);
    await assert.rejects(outputs.wireViews.resource.read(), EmptyError);
  } finally { scope.dispose(); }
});

test('只接受精确 root Surface，错误帧不部分改写出口，程序终止错误撤下引用并结束所有等待', async () => {
  const scope = new Scope();
  let executions = 0;
  const action = defineAction({ name: 'confirm', description: '正式确认', scope: z.null(), input: z.null(), output: z.boolean() });
  const actionImplementation = implementAction(action, {
    execute: () => { executions++; return true; }
  });
  const bound = bindAction(actionImplementation, null, { admit() {} });
  const surface = defineSurface({ views: { current: view(z.number().nullable()), absent: view(z.number()) }, actions: { confirm: action } });
  const foreign = defineSurface({ views: { current: view(z.number().nullable()) }, actions: {} });
  const outputs = createProgramOutputs(scope, surface);
  const source = new Subject<number | null>();
  try {
    outputs.apply(frame({ surface, views: { current: source }, actions: { confirm: bound } }));
    source.next(42);
    assert.equal(await outputs.views.current.read(), 42);
    assert.throws(() => outputs.apply(frame({ surface, views: { current: of(1) } }, { surface: foreign, views: { current: of(2) } })),
      /REACTIVE_PROGRAM_SURFACE_MISMATCH/u);
    assert.equal(source.observed, true);
    assert.equal(await outputs.views.current.read(), 42);
    assert.equal(outputs.actions.peek().confirm, bound);
    assert.throws(() => outputs.apply(frame({ surface }, { surface })), /REACTIVE_PROGRAM_SURFACE_DUPLICATE/u);

    const foreignAction = defineAction({ name: 'foreign', description: '另一合同', scope: z.null(), input: z.null(), output: z.boolean() });
    const foreignActionImplementation = implementAction(foreignAction, {
      execute: () => false
    });
    const foreignBound = bindAction(foreignActionImplementation, null, { admit() {} });
    assert.throws(() => outputs.apply(frame({ surface, actions: { confirm: foreignBound } })), /REACTIVE_SURFACE_ACTION_CONTRACT_MISMATCH/u);
    assert.equal(outputs.actions.peek().confirm, bound);
    outputs.apply(frame({ surface, views: { current: of(null) }, actions: { confirm: bound } }));
    assert.equal(await outputs.views.current.read(), null);
    assert.equal(await outputs.wireViews.current.read(), null);
    const absentRead = outputs.views.absent.read();
    const wireAbsentRead = outputs.wireViews.absent.read();
    const failure = new Error('组件程序已终止');
    outputs.fail(failure);
    await assert.rejects(absentRead, error => error === failure);
    await assert.rejects(wireAbsentRead, error => error === failure);
    await assert.rejects(outputs.views.current.read(), error => error === failure);
    await assert.rejects(outputs.wireViews.current.read(), error => error === failure);
    assert.equal(outputs.views.current.peek().status, 'error');
    assert.deepEqual(outputs.actions.peek(), {});
    assert.equal(bound.released, false);
    assert.equal(executions, 0);
    assert.throws(() => outputs.apply(frame()), error => error === failure);
    outputs.fail(new Error('不能覆盖首次失败'));
    await assert.rejects(outputs.views.absent.read(), error => error === failure);
  } finally { scope.dispose(); }
});
