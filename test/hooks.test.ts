import { test } from 'node:test';
import assert from 'node:assert/strict';
import { StrictMode, createElement as h, useLayoutEffect } from 'react';
import { BehaviorSubject, Observable, Subject, Subscription, isObservable, tap } from 'rxjs';
import { z } from 'zod';
import { defineAction, implementAction, type ActionExecutionContext, type BoundAction, type Invocation } from '@hanger-source/dsh-reactive';
import { Scope } from '@hanger-source/dsh-reactive/kernel';
import { observeResource } from '@hanger-source/dsh-reactive';
import { type Observation } from '@hanger-source/dsh-reactive';
import { ProgramHookProvider, type ProgramHookOwner } from '../packages/core/src/program/hooks.ts';
import { useAction, useBeforeStep, useObservable, useRelation, useResource, useShared, type ObservedResource } from '@hanger-source/dsh-reactive/program';
import { createDeclarationRenderer, declaration, type Frame } from '../packages/core/src/program/renderer.ts';

const noOperationQueries = {
  operationAccess(): never { throw new Error('UNEXPECTED_OPERATION_ACCESS'); },
  readOperationFeedback(): never { throw new Error('UNEXPECTED_OPERATION_FEEDBACK'); },
};

test('StrictMode 与目标替换只改变声明准入；普通调用由 owner 逐次签发来源，退出不取消已开始业务', async () => {
  const scope = new Scope();
  const trusted = new WeakSet<ActionExecutionContext>();
  const admission = Promise.withResolvers<void>();
  const admitted = Promise.withResolvers<void>();
  const started = Promise.withResolvers<void>();
  const operation = Promise.withResolvers<string>();
  const errors: unknown[] = [];
  const committed: BoundAction<typeof definition, true>[] = [];
  let identities = 0, admissions = 0, executions = 0, setups = 0, cleanups = 0;
  const activityStates: string[] = [];
  const definition = defineAction({ name: 'prepare_target', description: '准备绑定目标', scope: z.object({ merchantId: z.string() }),
    input: z.null(), output: z.string() });
  const definitionImplementation = implementAction(definition, {
    async execute(_input, call) {
      executions++;
      assert.equal(call.scope.merchantId, 'second');
      assert.equal(call.source.kind, 'participant');
      started.resolve();
      return operation.promise;
    }
  });
  const owner: ProgramHookOwner = {
    ...noOperationQueries,
    scope, reactivityScope: scope, signal: scope.signal,
    execution() {
      const execution: ActionExecutionContext = { invocationId: 'program-' + ++identities, sessionId: 'session',
        source: { kind: 'participant', participant: 'service', actorId: 'configured-service', entrypoint: 'program' },
        inputs: [], continuation: null, signal: scope.signal };
      trusted.add(execution);
      return execution;
    },
    async admit(_action, _input, call, execution) {
      if (!trusted.has(execution)) throw new Error('UNTRUSTED_EXECUTION');
      assert.equal(call.source.kind, 'participant');
      if (++admissions === 1) { admitted.resolve(); await admission.promise; }
    },
    fail: error => errors.push(error),
    beforeStep: () => () => {},
  };
  function Work(props: { merchantId: string }) {
    const action = useAction(definitionImplementation, () => ({ merchantId: props.merchantId }), [props.merchantId]);
    useLayoutEffect(() => { setups++; committed.push(action); return () => { cleanups++; }; }, [action]);
    return declaration(props.merchantId);
  }
  function Activity({ invocation }: { invocation: Invocation<string> }) {
    const { state } = useResource(invocation.activity);
    useLayoutEffect(() => { if (state.status === 'ready') activityStates.push(state.value.state); }, [state]);
    return null;
  }
  const renderer = createDeclarationRenderer({ decode: value => z.string().parse(value), onCommit() {}, onError: owner.fail });
  const tree = (merchantId: string, invocation?: Invocation<string>) => h(StrictMode, null,
    h(ProgramHookProvider, { owner }, h(Work, { merchantId }), invocation ? h(Activity, { invocation }) : null));
  try {
    renderer.render(tree('first'));
    const first = committed.at(-1)!;
    assert.ok(setups >= 2);
    assert.equal(first.released, false);
    assert.equal(identities, 0);
    renderer.render(tree('first'));
    assert.equal(committed.at(-1), first);
    const pending = first(null);
    await admitted.promise;
    renderer.render(tree('second'));
    const second = committed.at(-1)!;
    assert.notEqual(second, first);
    assert.equal(first.scope.merchantId, 'first');
    admission.resolve();
    await assert.rejects(pending.result, /REACTIVE_ACTION_BINDING_INACTIVE/u);
    assert.equal(executions, 0);
    const forged: ActionExecutionContext = { invocationId: 'forged', sessionId: 'session', inputs: [], continuation: null,
      signal: scope.signal, source: { kind: 'participant', participant: 'service', actorId: 'configured-service', entrypoint: 'program' } };
    await assert.rejects(second(null, forged).result, /UNTRUSTED_EXECUTION/u);
    const accepted = second.invoke(null);
    await started.promise;
    renderer.render(tree('second', accepted)); renderer.flush();
    assert.equal(activityStates.at(-1), 'running');
    renderer.unmount();
    const observedBeforeExit = activityStates.length;
    operation.resolve('accepted-operation');
    assert.equal(await accepted.result, 'accepted-operation');
    assert.equal(activityStates.length, observedBeforeExit, '组件退出只停止自己的调用事实观察');
    await assert.rejects(second(null).result, /REACTIVE_ACTION_BINDING_INACTIVE/u);
    assert.equal(executions, 1);
    assert.equal(first.released, false);
    scope.dispose();
    assert.equal((await accepted.activity.read()).state, 'returned', '本地调用事实不借用已退出的程序执行信号');
    assert.equal(first.released, true);
    assert.equal(second.released, true);
    assert.equal(setups, cleanups);
    assert.deepEqual(errors, []);
  } finally { admission.resolve(); operation.resolve('cleanup'); renderer.dispose(); scope.dispose(); }
});

test('memo 来源只在 commit 订阅，共享查询供现场与公开观察读取一次，切换即 pending，错误保留且退出结束查询', () => {
  const scope = new Scope();
  const queries = { first: new Subject<number>(), second: new Subject<number>(), empty: new Subject<number>() };
  const errors: unknown[] = [];
  const publicValues: number[] = [];
  let starts = 0, stops = 0, rendering = false;
  const owner: ProgramHookOwner = { ...noOperationQueries, scope, reactivityScope: scope, signal: scope.signal, execution() { throw new Error('UNEXPECTED_EXECUTION'); },
    admit() { throw new Error('UNEXPECTED_ADMISSION'); }, fail: error => errors.push(error), beforeStep: () => () => {} };
  const frames: Frame<{ observation: Observation<number>; source: Observable<number> }>[] = [];
  let published: Observable<number> | undefined;
  let publication = new Subscription();
  function Work(props: { target: keyof typeof queries; label: string }) {
    rendering = true;
    const source = useShared(() => new Observable<number>(subscriber => {
      assert.equal(rendering, false);
      starts++;
      const subscription = queries[props.target].subscribe(subscriber);
      return () => { stops++; subscription.unsubscribe(); };
    }), [props.target]);
    const observation = useObservable(() => source, [source]);
    rendering = false;
    return declaration({ observation, source });
  }
  const schema = z.object({ observation: z.custom<Observation<number>>(), source: z.custom<Observable<number>>(isObservable) });
  const renderer = createDeclarationRenderer({ decode: value => schema.parse(value), onError: owner.fail,
    onCommit(frame) {
      frames.push(frame);
      const source = frame.declarations[0]?.source;
      if (source === published) return;
      publication.unsubscribe(); published = source;
      publication = source?.subscribe({ next: value => publicValues.push(value), error() {} }) ?? new Subscription();
    } });
  const tree = (target: keyof typeof queries, label = '') => h(ProgramHookProvider, { owner }, h(Work, { target, label }));
  try {
    renderer.render(tree('first'));
    assert.equal(frames.at(-1)!.declarations[0]!.observation.status, 'pending');
    assert.equal(starts, 1);
    queries.first.next(7); renderer.flush();
    const ready = frames.at(-1)!.declarations[0]!.observation;
    assert.equal(ready.status, 'ready');
    if (ready.status === 'ready') assert.equal(ready.value, 7);
    assert.deepEqual(publicValues, [7]);
    const firstSource = published;
    renderer.render(tree('first', 'unrelated-render'));
    assert.equal(published, firstSource);
    assert.equal(starts, 1);
    renderer.render(tree('second'));
    assert.equal(frames.at(-1)!.declarations[0]!.observation.status, 'pending');
    assert.equal(starts, 2); assert.equal(stops, 1);
    const rejected = new Error('QUERY_REJECTED');
    queries.second.error(rejected); renderer.flush();
    const failed = frames.at(-1)!.declarations[0]!.observation;
    assert.equal(failed.status, 'error');
    if (failed.status === 'error') assert.equal(failed.error, rejected);
    renderer.render(tree('empty'));
    queries.empty.complete(); renderer.flush();
    const empty = frames.at(-1)!.declarations[0]!.observation;
    assert.equal(empty.status, 'error');
    if (empty.status === 'error') assert.match(String(empty.error), /no elements in sequence/u);
    renderer.unmount();
    assert.equal(starts, stops);
    assert.deepEqual(errors, []);
  } finally { publication.unsubscribe(); renderer.dispose(); scope.dispose(); }
});

test('长期关系的未恢复错误交还程序 owner，步骤检查随声明注册并等待真实异步业务边界', async () => {
  const scope = new Scope();
  const relation = new Subject<number>();
  const checks = new Set<(signal: AbortSignal) => void | Promise<void>>();
  const gate = Promise.withResolvers<void>();
  const entered = Promise.withResolvers<void>();
  const failure = new Error('RELATION_FAILED');
  const failures: unknown[] = [];
  const seen: number[] = [];
  const owner: ProgramHookOwner = { ...noOperationQueries, scope, reactivityScope: scope, signal: scope.signal, execution() { throw new Error('UNEXPECTED_EXECUTION'); },
    admit() { throw new Error('UNEXPECTED_ADMISSION'); }, fail: error => failures.push(error),
    beforeStep(check) { checks.add(check); return () => { checks.delete(check); }; } };
  function Work() {
    useRelation(() => relation.pipe(tap(value => seen.push(value))), [relation]);
    useBeforeStep(async signal => { signal.throwIfAborted(); entered.resolve(); await gate.promise; }, []);
    return declaration('work');
  }
  const renderer = createDeclarationRenderer({ decode: value => z.string().parse(value), onCommit() {}, onError: owner.fail });
  try {
    renderer.render(h(ProgramHookProvider, { owner }, h(Work)));
    assert.equal(checks.size, 1);
    let finished = false;
    const preparing = Promise.all([...checks].map(check => check(scope.signal))).then(() => { finished = true; });
    await entered.promise;
    assert.equal(finished, false);
    relation.next(3);
    assert.deepEqual(seen, [3]);
    relation.error(failure);
    assert.deepEqual(failures, [failure]);
    gate.resolve(); await preparing;
    renderer.unmount();
    assert.equal(checks.size, 0);
    assert.equal(relation.observed, false);
  } finally { gate.resolve(); renderer.dispose(); scope.dispose(); }
});

test('资源观察共享一次读取，失效同步撤下旧 ready，迟到读取被取消，错误恢复与公开来源保持同一事实', async () => {
  type Record = { merchantId: string; revision: number };
  const scope = new Scope();
  const key$ = new BehaviorSubject('first');
  const invalidations = new Subject<void>();
  const reads: { key: string; signal: AbortSignal; result: ReturnType<typeof Promise.withResolvers<Record>> }[] = [];
  const failures: unknown[] = [];
  let rendering = false, connections = 0, stops = 0;
  const owner: ProgramHookOwner = { ...noOperationQueries, scope, reactivityScope: scope, signal: scope.signal, execution() { throw new Error('UNEXPECTED_EXECUTION'); },
    admit() { throw new Error('UNEXPECTED_ADMISSION'); }, fail: error => failures.push(error), beforeStep: () => () => {} };
  const frames: Frame<ObservedResource<Record>>[] = [];
  const published: Observation<Record>[] = [];
  let source: Observable<Observation<Record>> | undefined;
  let publication = new Subscription();
  let waiting: { accept: (state: Observation<Record>) => boolean; resolve: (state: Observation<Record>) => void } | undefined;
  const commit = (accept: (state: Observation<Record>) => boolean) => {
    const completed = Promise.withResolvers<Observation<Record>>();
    waiting = { accept, resolve: completed.resolve };
    return completed.promise;
  };
  function Work(_props: { label: string }) {
    rendering = true;
    const resource = useResource(() => new Observable<Observation<Record>>(subscriber => {
      assert.equal(rendering, false);
      connections++;
      const subscription = observeResource({ key$, invalidations: () => invalidations,
        read(key, signal) {
          assert.equal(rendering, false);
          const result = Promise.withResolvers<Record>();
          reads.push({ key, signal, result });
          return result.promise;
        },
      }).subscribe(subscriber);
      return () => { stops++; subscription.unsubscribe(); };
    }), [key$, invalidations]);
    rendering = false;
    return declaration(resource);
  }
  const schema = z.object({ state: z.custom<Observation<Record>>(), observations$: z.custom<Observable<Observation<Record>>>(isObservable) });
  const renderer = createDeclarationRenderer({ decode: value => schema.parse(value), onError: owner.fail,
    onCommit(frame) {
      frames.push(frame);
      const resource = frame.declarations[0];
      if (resource?.observations$ !== source) {
        publication.unsubscribe(); source = resource?.observations$;
        publication = source?.subscribe(value => published.push(value)) ?? new Subscription();
      }
      if (resource && waiting?.accept(resource.state)) { const complete = waiting.resolve; waiting = undefined; complete(resource.state); }
    } });
  const tree = (label = '') => h(ProgramHookProvider, { owner }, h(Work, { label }));
  try {
    renderer.render(tree());
    assert.equal(reads.length, 1);
    assert.equal(connections, 1);
    assert.equal(frames.at(-1)!.declarations[0]!.state.status, 'pending');
    const ready = commit(state => state.status === 'ready');
    reads[0]!.result.resolve({ merchantId: 'first', revision: 1 });
    const current = await ready;
    assert.equal(current.status, 'ready');
    if (current.status === 'ready') assert.deepEqual(current.value, { merchantId: 'first', revision: 1 });
    assert.equal(current, published.at(-1));

    invalidations.next(); renderer.flush();
    const pending = frames.at(-1)!.declarations[0]!.state;
    assert.equal(pending.status, 'pending');
    assert.equal(pending, published.at(-1));
    assert.equal(reads.length, 2);
    key$.next('second'); renderer.flush();
    assert.equal(reads[1]!.signal.aborted, true);
    assert.equal(reads.length, 3);
    reads[1]!.result.resolve({ merchantId: 'first', revision: 99 });
    const rejected = new Error('BUSINESS_READ_REJECTED');
    const failed = commit(state => state.status === 'error');
    reads[2]!.result.reject(rejected);
    const error = await failed;
    assert.equal(error.status, 'error');
    if (error.status === 'error') assert.equal(error.error, rejected);
    assert.equal(error, published.at(-1));
    assert.ok(!published.some(state => state.status === 'ready' && state.value.revision === 99));

    invalidations.next(); renderer.flush();
    assert.equal(frames.at(-1)!.declarations[0]!.state.status, 'pending');
    const recovered = commit(state => state.status === 'ready');
    reads[3]!.result.resolve({ merchantId: 'second', revision: 7 });
    const recovery = await recovered;
    assert.equal(recovery.status, 'ready');
    if (recovery.status === 'ready') assert.deepEqual(recovery.value, { merchantId: 'second', revision: 7 });
    assert.equal(recovery, published.at(-1));
    const shared = source;
    renderer.render(tree('unrelated-render'));
    assert.equal(source, shared);
    assert.equal(connections, 1);
    assert.equal(reads.length, 4);
    invalidations.next(); renderer.flush();
    assert.equal(reads.length, 5);
    renderer.unmount();
    assert.equal(reads[4]!.signal.aborted, true);
    assert.equal(stops, 1);
    assert.equal(invalidations.observed, false);
    assert.deepEqual(failures, []);
  } finally {
    for (const read of reads) read.result.resolve({ merchantId: read.key, revision: 0 });
    publication.unsubscribe(); renderer.dispose(); scope.dispose();
  }
});

test('资源来源终止错误同时交给组件与公开观察，重渲染不重启查询或嵌套成ready值', () => {
  const terminalError = new Error('RESOURCE_SOURCE_TERMINATED');
  const scope = new Scope();
  const input = new Subject<Observation<number>>();
  const failures: unknown[] = [];
  const owner: ProgramHookOwner = { ...noOperationQueries, scope, reactivityScope: scope, signal: scope.signal, execution() { throw new Error('UNEXPECTED_EXECUTION'); },
    admit() { throw new Error('UNEXPECTED_ADMISSION'); }, fail: error => failures.push(error), beforeStep: () => () => {} };
  const publicStates: Observation<number>[] = [];
  let publication: Subscription | undefined;
  let latest: ObservedResource<number> | undefined;
  function Work() { return declaration(useResource(() => input, [input])); }
  const schema = z.object({ state: z.custom<Observation<number>>(), observations$: z.custom<Observable<Observation<number>>>(isObservable) });
  const renderer = createDeclarationRenderer({ decode: value => schema.parse(value), onError: owner.fail,
    onCommit(frame) {
      latest = frame.declarations[0];
      if (latest && !publication) publication = latest.observations$.subscribe({ next: value => publicStates.push(value),
        error: error => failures.push(error) });
    } });
  try {
    const tree = h(ProgramHookProvider, { owner }, h(Work));
    renderer.render(tree);
    input.next({ status: 'ready', generation: 3, value: 7, observedAt: 0 });
    input.error(terminalError);
    renderer.flush();
    assert.ok(latest);
    const failed = latest.state;
    assert.equal(failed.status, 'error');
    if (failed.status === 'error') assert.equal(failed.error, terminalError);
    assert.equal(failed, publicStates.at(-1));
    renderer.render(tree);
    assert.equal(latest.state, failed);
    assert.equal(input.observed, false);
    assert.deepEqual(failures, []);
  } finally { publication?.unsubscribe(); renderer.dispose(); scope.dispose(); }
});
