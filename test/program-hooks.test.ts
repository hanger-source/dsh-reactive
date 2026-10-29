import { test } from 'node:test';
import assert from 'node:assert/strict';
import { setImmediate as immediate } from 'node:timers/promises';
import { Fragment, createElement, useLayoutEffect } from 'react';
import { BehaviorSubject, Observable, Subject, Subscription, concatMap, map, of, tap } from 'rxjs';
import { z } from 'zod';
import { defineAction, implementAction, type BoundAction } from '@hanger-source/dsh-reactive';
import { Conversation } from '@hanger-source/dsh-reactive/kernel';
import { type Workspace } from '@hanger-source/dsh-reactive';
import { defineOperation, implementOperation, type BoundOperation } from '@hanger-source/dsh-reactive';
import { Expose } from '@hanger-source/dsh-reactive/program';
import { defineProgram } from '@hanger-source/dsh-reactive';
import { useAction, useObservable, useOperation, useRelation, useResource } from '@hanger-source/dsh-reactive/program';
import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { ProgramInstance } from '@hanger-source/dsh-reactive/kernel';
import { defineSurface, resourceView } from '@hanger-source/dsh-reactive';
import { Scope, createWorkspace } from '@hanger-source/dsh-reactive/kernel';
import { observeResource } from '@hanger-source/dsh-reactive';
import { type Observation } from '@hanger-source/dsh-reactive';
import { defineResource, implementResource, defineReaction, implementReaction, type ResourceInstance, type ReactionInstance } from '@hanger-source/dsh-reactive';
import { useResourceInstance, useReaction } from '@hanger-source/dsh-reactive/program';

test('Program fatal 在同次广播中停止其他关系与查询，已受理的 Action 仍按原执行寿命结算', async () => {
  const scope = new Scope();
  const workspace: Workspace = createWorkspace(scope, { id: 'workspace', directory: '/workspace' });
  const sessionScope = scope.child();
  const identity = { sessionId: 'fatal-program', workspaceId: workspace.id, environmentId: 'work',
    directory: workspace.directory, parentSessionId: null };
  const trigger = new BehaviorSubject(false), events = new Subject<number>();
  const started = Promise.withResolvers<AbortSignal>(), finish = Promise.withResolvers<string>();
  const read = Promise.withResolvers<number>();
  const directRead = Promise.withResolvers<number>(), acceptedReaction = Promise.withResolvers<number>();
  const reactionEvents = new Subject<number>();
  const directSignals: AbortSignal[] = [], startedReactions: number[] = [], committedReactions: number[] = [];
  const record = defineResource({ name: 'direct-record', params: z.null(), value: z.number() });
  const recordOwner = implementResource(record, { read(_params, { signal }) { directSignals.push(signal); return directRead.promise; } });
  const process = defineReaction({ name: 'process-change', params: z.null(), event: z.number(), result: z.number() });
  const processOwner = implementReaction(process, { observe: () => reactionEvents, operator: concatMap,
    async execute(event) { startedReactions.push(event); const result = await acceptedReaction.promise; committedReactions.push(event); return result; } });
  const attached: { record: ResourceInstance<typeof record>; process: ReactionInstance<typeof process> }[] = [];
  let directObservation = new Subscription();
  const failure = new Error('BUSINESS_RELATION_FAILED');
  const delivered: number[] = [], queryValues: number[] = [], signals: AbortSignal[] = [];
  const failingEvents = events.pipe(map(value => { if (value === 1) throw failure; return value; }));
  const deliveredEvents = events.pipe(tap(value => delivered.push(value)));
  const save = defineAction({ name: 'save_record', description: '保存已受理的业务资料',
    scope: z.null(), input: z.null(), output: z.string() });
  const saveOwner = implementAction(save, { async execute(_input, call) {
    started.resolve(call.signal); await finish.promise; call.signal.throwIfAborted(); return 'saved';
  } });
  const surface = defineSurface({ views: { current: resourceView(z.number()) }, actions: { save } });
  const definition = defineProgram({ input: z.null(), surface });
  const conversation = new Conversation(sessionScope, identity, workspace, definition, { sessionId: identity.sessionId,
    sequence: -1, source: 'live', execution: 'idle', inputs: [], calls: [], replies: [], goal: null, requests: {}, waits: [], operationFollows: [], failures: [] });
  const bindings: BoundAction<typeof save, true>[] = [];
  const implementation = implementProgram(definition, { Component() {
    const changed = useObservable(trigger);
    useRelation(failingEvents);
    useRelation(deliveredEvents);
    const current = useResource<number>(() => observeResource<string, number>({ key$: of('merchant-a'),
      read(_key, signal) { signals.push(signal); return read.promise; },
    }).pipe(tap<Observation<number>>(value => { if (value.status === 'ready') queryValues.push(value.value); })), []);
    const action = useAction(saveOwner, () => null, []);
    const resource = useResourceInstance(recordOwner, null);
    const reaction = useReaction(processOwner, null);
    useLayoutEffect(() => { attached.push({ record: resource, process: reaction }); }, [resource, reaction]);
    useLayoutEffect(() => { bindings.push(action); }, [action]);
    useLayoutEffect(() => { if (changed.status === 'ready' && changed.value) events.next(1); }, [changed]);
    return Expose({ surface, value: { views: { current: current.observations$ }, actions: { save: action } } });
  } });
  const program = new ProgramInstance(sessionScope, implementation, conversation, workspace, {
    execution: () => ({ invocationId: 'save-before-fatal', sessionId: identity.sessionId,
      source: { kind: 'participant', participant: 'service', actorId: 'business-owner', entrypoint: 'program' },
      inputs: [], continuation: null, signal: sessionScope.signal }),
  });
  try {
    program.mount();
    assert.equal(signals.length, 1);
    assert.equal(directSignals.length, 0, '取得资源实例不启动读取');
    assert.equal(reactionEvents.observed, false, '取得反应实例不开始感知');
    const resource = attached.at(-1)!.record, reaction = attached.at(-1)!.process;
    directObservation = resource.values$.subscribe(value => queryValues.push(value));
    const directWaiting = assert.rejects(resource.read(), error => error === failure);
    const run = reaction.start();
    reactionEvents.next(1); reactionEvents.next(2);
    assert.deepEqual(startedReactions, [1]);
    const invocation = bindings.at(-1)!(null);
    const executingSignal = await started.promise;
    events.next(0);
    assert.deepEqual(delivered, [0]);
    trigger.next(true);
    assert.throws(() => program.flush(), error => error === failure);
    assert.deepEqual(delivered, [0], '关系 A 的 fatal 必须在同次 Subject 广播交付 B 之前断开 B');
    assert.equal(signals[0]!.aborted, true, '查询立即停止，不等待 React 延后卸载');
    assert.equal(directSignals[0]!.aborted, true, '普通函数和直接流的读取也归程序拥有');
    assert.equal(directObservation.closed, true);
    await directWaiting;
    assert.deepEqual(run.snapshot(), { status: 'draining', reason: 'stopped', pending: 1, emitted: 0 });
    assert.throws(() => reaction.start(), error => error === failure);
    assert.throws(() => resource.read(), error => error === failure);
    assert.equal(sessionScope.disposed, false);
    assert.equal(executingSignal.aborted, false, '程序失败不能冒充已受理业务的取消');
    finish.resolve('saved');
    assert.equal(await invocation.result, 'saved');
    acceptedReaction.resolve(7); directRead.resolve(8); read.resolve(9); await immediate();
    assert.equal((await run.settled).status, 'stopped');
    assert.deepEqual(committedReactions, [1], '已进入业务函数的处理继续由业务 owner 完成');
    assert.deepEqual(startedReactions, [1], '尚未开始的第二项不在程序失败后执行');
    assert.deepEqual(queryValues, [], '迟到的查询结果不能在 fatal 后重新交付');
    await assert.rejects(program.outputs.views.current.read(), error => error === failure);
  } finally { directObservation.unsubscribe(); finish.resolve('cleanup'); acceptedReaction.resolve(7); directRead.resolve(8); read.resolve(9); scope.dispose(); }
});

test('程序 hooks 跨组件共享资源和反应，跨会话隔离，目标切换不改写已取得实例', async () => {
  const scope = new Scope();
  const workspace: Workspace = createWorkspace(scope, { id: 'workspace', directory: '/workspace' });
  const selected = new BehaviorSubject('a'), changes = new Subject<number>();
  const reads: string[] = [];
  let observing = 0, rendering = false;
  const record = defineResource({ name: 'shared-record', params: z.object({ target: z.string() }), value: z.string() });
  const recordOwner = implementResource(record, { read({ target }) { assert.equal(rendering, false); reads.push(target); return target; } });
  const process = defineReaction({ name: 'shared-process', params: z.object({ target: z.string() }), event: z.number(), result: z.string() });
  const processOwner = implementReaction(process, {
    observe: () => new Observable<number>(subscriber => {
      observing++; const subscription = changes.subscribe(subscriber);
      return () => { observing--; subscription.unsubscribe(); };
    }), operator: concatMap, execute: (event, context) => context.params.target + ':' + event,
  });
  type Binding = { record: ResourceInstance<typeof record>; process: ReactionInstance<typeof process>; observed: Observable<Observation<string>> };
  const attached = new Map<string, Binding>();
  function Consumer(props: { session: string; side: string; target: string }) {
    rendering = true;
    const resource = useResourceInstance(recordOwner, { target: props.target });
    const reaction = useReaction(processOwner, { target: props.target });
    const current = useResource(recordOwner, { target: props.target });
    useLayoutEffect(() => { attached.set(props.session + ':' + props.side, { record: resource, process: reaction, observed: current.observations$ }); }, [props.session, props.side, resource, reaction, current.observations$]);
    rendering = false;
    return null;
  }
  const definition = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
  const implementation = implementProgram(definition, { Component({ conversation }) {
    const current = useObservable(() => selected, []);
    if (current.status !== 'ready') return null;
    return createElement(Fragment, null, ...['left', 'right'].map(side => createElement(Consumer,
      { key: side, session: conversation.identity.sessionId, side, target: current.value })));
  } });
  const programs = ['one', 'two'].map(sessionId => {
    const sessionScope = scope.child();
    const identity = { sessionId, workspaceId: workspace.id, environmentId: 'work', directory: workspace.directory, parentSessionId: null };
    const conversation = new Conversation(sessionScope, identity, workspace, definition, { sessionId, sequence: -1, source: 'live', execution: 'idle', inputs: [], calls: [], replies: [], goal: null, requests: {}, waits: [], operationFollows: [], failures: [] });
    return new ProgramInstance(sessionScope, implementation, conversation, workspace, {
      execution() { throw new Error('UNEXPECTED_EXECUTION'); },
    });
  });
  try {
    for (const program of programs) program.mount();
    const first = attached.get('one:left')!, sibling = attached.get('one:right')!, other = attached.get('two:left')!;
    assert.equal(first.record, sibling.record);
    assert.equal(first.process, sibling.process);
    assert.equal(first.observed, first.record.observations$);
    assert.notEqual(first.record, other.record);
    assert.notEqual(first.process, other.process);
    assert.deepEqual(reads, ['a', 'a'], '两个会话各读取一次，同会话两个组件不重复查询');
    assert.equal(await first.record.read(), 'a');
    assert.equal(observing, 0);
    const run = first.process.start();
    assert.throws(() => sibling.process.start(), /ALREADY_RUNNING/);
    const delivered: string[] = [];
    run.events$.subscribe(event => delivered.push(event.result));
    selected.next('b');
    for (const program of programs) program.flush();
    const next = attached.get('one:left')!;
    assert.notEqual(next.record, first.record);
    assert.notEqual(next.process, first.process);
    assert.equal(await next.record.read(), 'b');
    changes.next(3);
    assert.deepEqual(delivered, ['a:3'], '旧运行保留原目标，组件切换不重定向或停止显式启动的工作');
    programs[0].fail(new Error('FIRST_PROGRAM_FAILED'));
    assert.equal((await run.settled).status, 'stopped');
    assert.equal(observing, 0);
    assert.equal(await attached.get('two:left')!.record.read(), 'b', '另一个会话的资源继续有效');
  } finally { scope.dispose(); }
});

test('nullable Operation 只撤下当前绑定，恢复和换目标不重定向旧 command 或中止已有读取', async () => {
  const scope = new Scope();
  const workspace: Workspace = createWorkspace(scope, { id: 'workspace', directory: '/workspace' });
  const sessionScope = scope.child();
  const identity = { sessionId: 'nullable-program', workspaceId: workspace.id, environmentId: 'work',
    directory: workspace.directory, parentSessionId: null };
  const selected = new BehaviorSubject<string | null>(null);
  const operation = defineOperation({ kind: 'merchant-preparation', version: 1,
    command: z.object({ merchantId: z.string() }), reference: z.object({ merchantId: z.string() }),
    state: z.object({ merchantId: z.string(), done: z.boolean() }), settled: state => state.done ? state : undefined });
  const states = new Map<string, BehaviorSubject<Observation<{ merchantId: string; done: boolean }>>>(['a', 'b'].map(merchantId => [merchantId,
    new BehaviorSubject<Observation<{ merchantId: string; done: boolean }>>({ status: 'ready', generation: 1,
      value: { merchantId, done: false }, observedAt: 0 })]));
  const located: string[] = [], observed: string[] = [], stopped: string[] = [];
  const owner = implementOperation(operation, {
    locate(command) {
      located.push(command.merchantId);
      return { status: 'accepted', reference: command, state: { merchantId: command.merchantId, done: false } };
    },
    observe(reference) {
      return new Observable(subscriber => {
        observed.push(reference.merchantId);
        const connection = states.get(reference.merchantId)!.subscribe(subscriber);
        return () => { stopped.push(reference.merchantId); connection.unsubscribe(); };
      });
    },
  });
  const surface = defineSurface({ views: {}, actions: {}, operations: { preparation: operation } });
  const definition = defineProgram({ input: z.null(), surface });
  const conversation = new Conversation(sessionScope, identity, workspace, definition, { sessionId: identity.sessionId,
    sequence: -1, source: 'live', execution: 'idle', inputs: [], calls: [], replies: [], goal: null, requests: {}, waits: [], operationFollows: [], failures: [] });
  const committed: (BoundOperation<typeof operation> | null)[] = [];
  const implementation = implementProgram(definition, { Component() {
    const choice = useObservable(() => selected, []);
    const merchantId = choice.status === 'ready' ? choice.value : null;
    const binding = useOperation(owner, () => merchantId === null ? null : { merchantId }, [merchantId]);
    useLayoutEffect(() => { committed.push(binding); }, [binding]);
    return binding ? Expose({ surface, value: { operations: { preparation: binding } } }) : null;
  } });
  const program = new ProgramInstance(sessionScope, implementation, conversation, workspace, {
    execution() { throw new Error('READ_MUST_NOT_EXECUTE_ACTION'); },
    operationAccess: () => ({ sessionId: identity.sessionId, signal: sessionScope.signal,
      source: { kind: 'participant', participant: 'service', actorId: 'business-reader', entrypoint: 'program' } }),
  });
  try {
    program.mount();
    assert.equal(committed.at(-1), null);
    assert.deepEqual(located, []); assert.deepEqual(observed, []);
    selected.next('a'); program.flush();
    const original = committed.at(-1)!;
    assert.ok(original);
    assert.deepEqual(located, [], '绑定构造不查询或执行既有命令');
    const location = await original.locate();
    assert.equal(location.status, 'accepted');
    if (location.status !== 'accepted') throw new Error('ACCEPTED_OPERATION_REQUIRED');
    const reader = original.attach(location.reference);
    const terminal = reader.settled();
    void terminal.catch(() => {});
    await immediate();
    assert.deepEqual(observed, ['a']);
    selected.next(null); program.flush();
    assert.equal(committed.at(-1), null);
    assert.deepEqual(stopped, [], '当前没有绑定不取消先前取得的 reader');
    selected.next('b'); program.flush();
    const replacement = committed.at(-1)!;
    assert.ok(replacement); assert.notEqual(replacement, original);
    await original.locate(); await replacement.locate();
    assert.deepEqual(located, ['a', 'a', 'b'], '原捕获绑定仍查询 A，新绑定才查询 B');
    states.get('b')!.next({ status: 'ready', generation: 2, value: { merchantId: 'b', done: true }, observedAt: 1 });
    states.get('a')!.next({ status: 'ready', generation: 2, value: { merchantId: 'a', done: true }, observedAt: 1 });
    assert.deepEqual(await terminal, { merchantId: 'a', done: true });
    assert.deepEqual(stopped, ['a']);
  } finally { scope.dispose(); }
});
