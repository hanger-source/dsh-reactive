import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement as h, Fragment } from 'react';
import { BehaviorSubject, combineLatest, defer, distinctUntilChanged, filter, firstValueFrom, from, map, mergeMap, tap } from 'rxjs';
import { Context, Expose, Offer, useAction, useObservable, useRelation, useShared } from '@hanger-source/dsh-reactive/program';
import { defineAction, implementAction, defineApplication, defineEnvironment, defineProgram, defineSurface, view, z, type ActionCallContext, type ActionInvocation, type PartialSurfaceBindings, type ProgramProps } from '@hanger-source/dsh-reactive';
import type { GenerateOptions } from '@deepseek-ai/dsh-llm';
import { boot, text, tool } from './native-support.ts';

test('原生输入和真实绑定的调用结果经标准流改变业务事实、上下文和视图，观察释放不撤销动作，冷读不重放反应', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-feedback-'));
  const business$ = new BehaviorSubject({ merchantId: null as string | null, revision: 0,
    editable: false, phase: 'awaiting-input', completedInvocationId: null as string | null });
  const entered = Promise.withResolvers<void>();
  const finish = Promise.withResolvers<void>();
  const requests: GenerateOptions[] = [];
  let ownerConnections = 0;
  let acceptedReactions = 0;
  let inputReactions = 0;
  let resultReactions = 0;
  let saves = 0;
  const input = z.object({ merchantId: z.string() });
  const calls: ActionCallContext<z.output<typeof input>>[] = [];
  const business = z.object({ merchantId: z.string().nullable(), revision: z.number(), editable: z.boolean(), phase: z.string(),
    completedInvocationId: z.string().nullable() });
  const save = defineAction({ name: 'save_delivery', description: '保存业务交付', scope: input, input: z.object({}).strict(),
    output: z.object({ merchantId: z.string(), saved: z.literal(true) }), observe: output => output });
  const invocations: ActionInvocation<typeof save>[] = [];
  const saveImplementation = implementAction(save, {
    async execute(_input, call) {
      calls.push(call);
      saves++;
      assert.equal(call.scope.merchantId, 'merchant-a');
      assert.deepEqual(call.inputs[0]!.scope, { merchantId: 'merchant-a' });
      business$.next({ ...business$.value, editable: false, phase: 'saving' });
      entered.resolve();
      await finish.promise;
      call.signal.throwIfAborted();
      return { merchantId: call.scope.merchantId, saved: true as const };
    }
  });
  const surface = defineSurface({ views: { board: view(z.object({ state: business, writable: z.boolean() })) }, actions: { save } });
  const Public = Expose<typeof surface, PartialSurfaceBindings<typeof surface>>;
  function Save(props: { merchantId: string; editable: boolean }) {
    const action = useAction(saveImplementation, () => ({ merchantId: props.merchantId }), [props.merchantId]);
    useRelation(() => action.invocations$.pipe(tap(invocation => invocations.push(invocation)), mergeMap(invocation => from(invocation.result).pipe(tap(result => {
      assert.equal(invocation.source.kind, 'tool');
      resultReactions++;
      business$.next({ ...business$.value, merchantId: result.merchantId,
        revision: business$.value.revision + 1, phase: 'returned', completedInvocationId: invocation.id });
    })))), [action]);
    if (!props.editable) return null;
    return h(Fragment, null, h(Offer<typeof save>, { name: 'save_delivery', action }),
      h(Public, { surface, value: { actions: { save: action } } }));
  }
  function Delivery({ conversation }: ProgramProps<{ input: typeof input; surface: typeof surface }>) {
    const state$ = useShared(() => defer(() => { ownerConnections++; return business$; }), []);
    useRelation(() => conversation.events$.pipe(filter(event => event.kind === 'input.accepted'), tap(event => {
      if (event.kind !== 'input.accepted') return;
      acceptedReactions++;
      business$.next({ ...business$.value, merchantId: event.inputs.at(-1)!.scope.merchantId,
        revision: 1, editable: true, phase: 'working' });
    })), [conversation]);
    useRelation(() => conversation.events$.pipe(filter(event => event.kind === 'input.entered'), tap(() => { inputReactions++; })), [conversation]);
    const state = useObservable(() => state$, [state$]);
    const board$ = useShared(() => combineLatest({ state: state$,
      writable: state$.pipe(map(value => value.editable), distinctUntilChanged()),
    }), [state$]);
    return h(Fragment, null, h(Context<z.output<typeof business>>, { name: 'business', observation: state, render: value => value }),
      h(Public, { surface, value: { views: { board: board$ } } }),
      state.status === 'ready' && state.value.merchantId !== null
        ? h(Save, { key: state.value.merchantId, merchantId: state.value.merchantId, editable: state.value.editable }) : null);
  }
  const definition = defineApplication({ name: 'feedback', admit: async () => {}, environments: [defineEnvironment({
    id: 'delivery', instructions: '按当前业务事实工作',
    program: implementProgram(defineProgram({ input, surface }), { Component: Delivery }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* (options) {
    requests.push(options);
    if (requests.length === 1) yield* tool('save_delivery', 'reactive-save-call', {});
    else yield* text('正式保存已完成');
  });
  try {
    const session = await app.host.create('delivery', { workspaceId: app.workspace.workspaceId });
    const observer = app.host.conversation(session.id, definition.environments[0].program.definition);
    const first = session.views.board.subscribe();
    const second = observer.views.board.subscribe();
    assert.equal(ownerConnections, 1);
    await session.submit({ requestId: 'prepare-merchant-a', content: '保存商家交付', intent: 'question', delivery: 'followup' as const, scope: { merchantId: 'merchant-a' } });
    await entered.promise;
    assert.equal((await firstValueFrom(observer.views.board)).writable, false);
    first.unsubscribe();
    session.release();
    assert.equal(saves, 1);
    finish.resolve();
    await observer.whenIdle();
    const board = await firstValueFrom(observer.views.board);
    assert.equal(calls.length, 1);
    assert.equal(invocations.length, 1);
    const call = calls[0]!, invocation = invocations[0]!;
    assert.ok(call.source.kind === 'tool');
    assert.equal(call.source.callId, 'reactive-save-call');
    assert.deepEqual(invocation.source, call.source);
    assert.equal(invocation.id, call.invocationId);
    assert.deepEqual(await invocation.result, { merchantId: 'merchant-a', saved: true });
    assert.equal(board.state.completedInvocationId, invocation.id);
    assert.equal(board.state.revision, 2);
    assert.equal(board.state.phase, 'returned');
    assert.equal(acceptedReactions, 1);
    assert.equal(inputReactions, 1);
    assert.equal(resultReactions, 1);
    assert.equal(ownerConnections, 1);
    assert.equal(requests.length, 2);
    const acceptedPrompt = requests[0]!.messages.flatMap(message => message.content)
      .flatMap(block => block.type === 'text' ? [block.text] : []).join('\n');
    assert.ok(acceptedPrompt.includes('"merchantId":"merchant-a"'));
    assert.ok(acceptedPrompt.includes('"phase":"working"'));
    const currentPrompt = requests[1]!.messages.flatMap(message => message.content)
      .flatMap(block => block.type === 'text' ? [block.text] : []).join('\n');
    assert.ok(currentPrompt.includes('"phase":"returned"'));
    assert.ok(!requests[1]!.tools?.some(value => value.name === 'save_delivery'));
    assert.equal((await firstValueFrom(observer.request('prepare-merchant-a').state$.pipe(filter(state => state.status === 'ready'), map(state => state.value.activity))))?.tools[0]?.state, 'returned');
    const id = observer.id;
    second.unsubscribe();
    await app.host.close(id);
    observer.release();
    const cold = await app.host.open(id, definition.environments[0].program.definition);
    assert.equal((await firstValueFrom(cold.views.board)).state.completedInvocationId, invocation.id);
    assert.equal(acceptedReactions, 1);
    assert.equal(inputReactions, 1);
    assert.equal(resultReactions, 1);
    assert.equal(saves, 1);
    assert.equal(requests.length, 2);
    assert.equal(cold.snapshot().source, 'durable');
    cold.release();
    assert.deepEqual(app.errors, []);
  } finally { finish.resolve(); await app.dispose(); }
});
