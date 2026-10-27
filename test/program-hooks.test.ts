import { test } from 'node:test';
import assert from 'node:assert/strict';
import { setImmediate as immediate } from 'node:timers/promises';
import { useLayoutEffect } from 'react';
import { BehaviorSubject, Observable, Subject, map, of, tap } from 'rxjs';
import { z } from 'zod';
import { defineAction, implementAction, type BoundAction } from '@hanger-source/dsh-reactive';
import { Conversation } from '@hanger-source/dsh-reactive/kernel';
import { type WorkspaceContext } from '@hanger-source/dsh-reactive';
import { defineOperation, implementOperation, type BoundOperation } from '@hanger-source/dsh-reactive';
import { Expose } from '@hanger-source/dsh-reactive/program';
import { defineProgram } from '@hanger-source/dsh-reactive';
import { useAction, useObservable, useOperation, useRelation, useResource } from '@hanger-source/dsh-reactive/program';
import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { ProgramInstance } from '@hanger-source/dsh-reactive/kernel';
import { defineSurface, resourceView } from '@hanger-source/dsh-reactive';
import { Scope } from '@hanger-source/dsh-reactive/kernel';
import { observeResource } from '@hanger-source/dsh-reactive';
import { type Observation } from '@hanger-source/dsh-reactive';

test('Program fatal 在同次广播中停止其他关系与查询，已受理的 Action 仍按原执行寿命结算', async () => {
  const scope = new Scope();
  const workspace: WorkspaceContext = { id: 'workspace', directory: '/workspace', scope, share: source => source };
  const identity = { sessionId: 'fatal-program', workspaceId: workspace.id, environmentId: 'work',
    directory: workspace.directory, parentSessionId: null };
  const trigger = new BehaviorSubject(false), events = new Subject<number>();
  const started = Promise.withResolvers<AbortSignal>(), finish = Promise.withResolvers<string>();
  const read = Promise.withResolvers<number>();
  const failure = new Error('BUSINESS_RELATION_FAILED');
  const delivered: number[] = [], queryValues: number[] = [], signals: AbortSignal[] = [];
  const save = defineAction({ name: 'save_record', description: '保存已受理的业务资料',
    scope: z.null(), input: z.null(), output: z.string() });
  const saveOwner = implementAction(save, { async execute(_input, call) {
    started.resolve(call.signal); await finish.promise; call.signal.throwIfAborted(); return 'saved';
  } });
  const surface = defineSurface({ views: { current: resourceView(z.number()) }, actions: { save } });
  const definition = defineProgram({ input: z.null(), surface });
  const conversation = new Conversation(identity, workspace, definition, { sessionId: identity.sessionId,
    sequence: -1, source: 'live', execution: 'idle', goal: null, requests: {}, waits: [] });
  const bindings: BoundAction<typeof save, true>[] = [];
  const implementation = implementProgram(definition, { Component() {
    const changed = useObservable(() => trigger, []);
    useRelation(() => events.pipe(map(value => { if (value === 1) throw failure; return value; })), []);
    useRelation(() => events.pipe(tap(value => delivered.push(value))), []);
    const current = useResource<number>(() => observeResource<string, number>({ key$: of('merchant-a'),
      read(_key, signal) { signals.push(signal); return read.promise; },
    }).pipe(tap<Observation<number>>(value => { if (value.status === 'ready') queryValues.push(value.value); })), []);
    const action = useAction(saveOwner, () => null, []);
    useLayoutEffect(() => { bindings.push(action); }, [action]);
    useLayoutEffect(() => { if (changed.status === 'ready' && changed.value) events.next(1); }, [changed]);
    return Expose({ surface, value: { views: { current: current.observations$ }, actions: { save: action } } });
  } });
  const program = new ProgramInstance(conversation.scope, implementation, conversation, workspace, {
    execution: () => ({ invocationId: 'save-before-fatal', sessionId: identity.sessionId,
      source: { kind: 'participant', participant: 'service', actorId: 'business-owner', entrypoint: 'program' },
      inputs: [], continuation: null, signal: conversation.scope.signal }),
  });
  try {
    program.mount();
    assert.equal(signals.length, 1);
    const invocation = bindings.at(-1)!(null);
    const executingSignal = await started.promise;
    events.next(0);
    assert.deepEqual(delivered, [0]);
    trigger.next(true);
    assert.throws(() => program.flush(), error => error === failure);
    assert.deepEqual(delivered, [0], '关系 A 的 fatal 必须在同次 Subject 广播交付 B 之前断开 B');
    assert.equal(signals[0]!.aborted, true, '查询立即停止，不等待 React 延后卸载');
    assert.equal(conversation.scope.disposed, false);
    assert.equal(executingSignal.aborted, false, '程序失败不能冒充已受理业务的取消');
    finish.resolve('saved');
    assert.equal(await invocation.result, 'saved');
    read.resolve(9); await immediate();
    assert.deepEqual(queryValues, [], '迟到的查询结果不能在 fatal 后重新交付');
    await assert.rejects(program.outputs.views.current.read(), error => error === failure);
  } finally { finish.resolve('cleanup'); read.resolve(9); scope.dispose(); }
});

test('nullable Operation 只撤下当前绑定，恢复和换目标不重定向旧 command 或中止已有读取', async () => {
  const scope = new Scope();
  const workspace: WorkspaceContext = { id: 'workspace', directory: '/workspace', scope, share: source => source };
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
  const conversation = new Conversation(identity, workspace, definition, { sessionId: identity.sessionId,
    sequence: -1, source: 'live', execution: 'idle', goal: null, requests: {}, waits: [] });
  const committed: (BoundOperation<typeof operation> | null)[] = [];
  const implementation = implementProgram(definition, { Component() {
    const choice = useObservable(() => selected, []);
    const merchantId = choice.status === 'ready' ? choice.value : null;
    const binding = useOperation(owner, () => merchantId === null ? null : { merchantId }, [merchantId]);
    useLayoutEffect(() => { committed.push(binding); }, [binding]);
    return binding ? Expose({ surface, value: { operations: { preparation: binding } } }) : null;
  } });
  const program = new ProgramInstance(conversation.scope, implementation, conversation, workspace, {
    execution() { throw new Error('READ_MUST_NOT_EXECUTE_ACTION'); },
    operationAccess: () => ({ sessionId: identity.sessionId, signal: conversation.scope.signal,
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
