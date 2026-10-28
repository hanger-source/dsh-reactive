import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SessionId } from '@deepseek-ai/dsh-session';
import { Fragment, createElement as h } from 'react';
import { BehaviorSubject, defer, filter, firstValueFrom, of } from 'rxjs';
import { ActionBindingUnavailableError, ActionTargetMismatchError, defineApplication, defineEnvironment, z } from '@hanger-source/dsh-reactive';
import { defineAction, implementAction, type ActionCallContext } from '@hanger-source/dsh-reactive';
import { Context, Expose, Offer } from '@hanger-source/dsh-reactive/program';
import { defineProgram } from '@hanger-source/dsh-reactive';
import { useAction, useObservable, useShared } from '@hanger-source/dsh-reactive/program';
import { defineSurface, view } from '@hanger-source/dsh-reactive';
import { boot, text, tool } from './native-support.ts';

test('同一绑定同时供人和 AI 调用，真实来源及 Goal 权限分别归入口，反馈共享且观察退出不取消已受理业务', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-program-actions-'));
  const visible = new BehaviorSubject(true);
  const facts = new BehaviorSubject({ merchantId: 'merchant-a', revision: 0 });
  const humanStarted = Promise.withResolvers<void>(), aiStarted = Promise.withResolvers<void>();
  const humanResult = Promise.withResolvers<void>(), aiResult = Promise.withResolvers<void>();
  const calls: { operation: 'human' | 'ai'; call: ActionCallContext<{ readonly merchantId: string }> }[] = [];
  const modelContexts: string[][] = [];
  const contextSource = z.object({ kind: z.literal('runtime-context'), form: z.literal('snapshot'),
    sections: z.object({ name: z.string(), text: z.string() }).array() });
  const prepare = defineAction({ name: 'prepare_merchant', description: '准备原商家的正式业务',
    scope: z.object({ merchantId: z.string() }), input: z.object({ operation: z.enum(['human', 'ai']) }).strict(),
    output: z.object({ merchantId: z.string(), revision: z.number() }), concurrency: 'parallel', observe: output => output });
  const prepareImplementation = implementAction(prepare, {
    async execute(input, call) {
      if (call.scope.merchantId !== facts.value.merchantId) throw new Error('BUSINESS_TARGET_NOT_AUTHORIZED');
      calls.push({ operation: input.operation, call });
      if (input.operation === 'human') {
        assert.equal(call.source.kind, 'participant');
        assert.deepEqual(call.inputs, []);
        assert.equal(call.continuation, null);
        assert.throws(() => call.completeGoal(), /REACTIVE_ACTION_GOAL_COMPLETION_UNAVAILABLE/u);
        assert.throws(() => call.wait({ id: 'foreign-turn', operation: null }), /REACTIVE_ACTION_WAIT_UNAVAILABLE/u);
        humanStarted.resolve(); await humanResult.promise;
      } else {
        assert.equal(call.source.kind, 'tool');
        assert.equal(call.inputs[0]?.requestId, 'original-goal');
        assert.ok(call.continuation?.goal);
        aiStarted.resolve(); await aiResult.promise;
        call.completeGoal();
      }
      call.signal.throwIfAborted();
      const result = { merchantId: call.scope.merchantId, revision: facts.value.revision + 1 };
      await writeFile(join(root, input.operation + '.json'), JSON.stringify(result));
      facts.next(result);
      return result;
    }
  });
  const surface = defineSurface({ views: { record: view(z.object({ merchantId: z.string(), revision: z.number() })) },
    actions: { prepare } });
  function Merchant() {
    const record$ = useShared(() => facts, [facts]);
    const record = useObservable(() => record$, [record$]);
    const action = useAction(prepareImplementation, () => ({ merchantId: 'merchant-a' }), []);
    return h(Fragment, null, Context({ name: 'merchant', observation: record, render: value => value }),
      Offer({ name: 'prepare_merchant', action }), Expose({ surface, value: { views: { record: record$ }, actions: { prepare: action } } }));
  }
  const definition = defineApplication({ name: 'program-actions', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '准备明确商家，完成正式 Goal',
    program: implementProgram(defineProgram({ input: z.object({ merchantId: z.string() }), surface }), { Component() {
      const mounted = useObservable(() => visible, [visible]);
      return mounted.status === 'ready' && mounted.value ? h(Merchant) : null;
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* (options) {
    modelContexts.push(options.messages.flatMap(message => {
      if (message.role !== 'user') return [];
      const source = contextSource.safeParse(message.source);
      if (!source.success) return [];
      return source.data.sections.filter(section => section.name === 'merchant').map(section => section.text);
    }));
    if (modelContexts.length === 1) yield* tool('prepare_merchant', 'native-prepare', { operation: 'ai' });
    else yield* text('已核验正式准备结果');
  });
  try {
    const first = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const observer = app.host.conversation(first.id, definition.environments[0].program.definition);
    assert.deepEqual(await observer.readView('record'), { merchantId: 'merchant-a', revision: 0 });
    const original = { requestId: 'original-goal', content: '持续完成原商家的正式准备', intent: 'goal' as const, delivery: 'followup' as const, scope: { merchantId: 'merchant-a' } };
    await first.submit(original);
    await aiStarted.promise;
    const goal = observer.snapshot().goal;
    assert.ok(goal); assert.equal(goal.phase, 'active');
    const updated = firstValueFrom(observer.views.record.pipe(filter(value => value.revision === 1)));
    const caller = new AbortController(), waiting = new AbortController();
    const invocation = first.resolveAction('prepare', { merchantId: 'merchant-a' })({ operation: 'human' }, { signal: caller.signal });
    await humanStarted.promise;
    const reason = new Error('HUMAN_ACTION_WAIT_FINISHED');
    const withdrawn = assert.rejects(invocation.wait({ signal: waiting.signal }), error => error === reason);
    const retained = invocation.wait();
    assert.equal(invocation.source.kind, 'participant');
    if (invocation.source.kind === 'participant') {
      assert.equal(invocation.source.participant, 'human');
      assert.equal(invocation.source.actorId, 'operator-test');
      assert.equal(invocation.source.entrypoint, 'sdk');
    }
    caller.abort(new Error('HUMAN_ACTION_CALLER_EXITED'));
    waiting.abort(reason); await withdrawn;
    first.release();
    humanResult.resolve();
    assert.deepEqual(await invocation.result, { merchantId: 'merchant-a', revision: 1 });
    assert.deepEqual(await retained, await invocation.result);
    assert.deepEqual(JSON.parse(await readFile(join(root, 'human.json'), 'utf8')), await invocation.result);
    assert.equal(observer.invocation('prepare', invocation.id), invocation);
    assert.deepEqual(await observer.invocation('prepare', invocation.id).wait(), await invocation.result);
    assert.deepEqual(await updated, { merchantId: 'merchant-a', revision: 1 });
    assert.equal(observer.snapshot().goal?.phase, 'active');
    assert.equal(observer.snapshot().goal?.id, goal.id);
    aiResult.resolve();
    await observer.whenIdle();
    assert.deepEqual(await observer.readView('record'), { merchantId: 'merchant-a', revision: 2 });
    const human = calls.find(value => value.operation === 'human')!, ai = calls.find(value => value.operation === 'ai')!;
    assert.equal(human.call.scope, ai.call.scope);
    assert.equal(human.call.scope.merchantId, 'merchant-a');
    assert.equal(ai.call.source.kind, 'tool');
    if (ai.call.source.kind === 'tool') {
      assert.equal(ai.call.source.callId, 'native-prepare');
      assert.equal(ai.call.source.rootCallId, 'native-prepare');
      assert.equal(ai.call.source.parentCallId, null);
    }
    assert.equal(ai.call.continuation?.goal?.id, goal.id);
    assert.equal(ai.call.continuation?.goal?.revision, goal.revision);
    assert.equal(observer.snapshot().goal?.phase, 'complete');
    assert.equal(observer.snapshot().requests['original-goal']?.tools.length, 1);
    assert.equal(observer.snapshot().requests['original-goal']?.tools[0]?.callId, 'native-prepare');
    assert.deepEqual(JSON.parse(modelContexts[0]!.at(-1)!), { merchantId: 'merchant-a', revision: 0 });
    assert.deepEqual(JSON.parse(modelContexts.at(-1)!.at(-1)!), { merchantId: 'merchant-a', revision: 2 });
    const invoke = observer.resolveAction('prepare', { merchantId: 'merchant-a' });
    visible.next(false);
    assert.throws(() => invoke({ operation: 'human' }), ActionBindingUnavailableError);
    assert.equal(calls.length, 2);
    assert.deepEqual(app.errors, []);
    observer.release();
  } finally { humanResult.resolve(); aiResult.resolve(); await app.dispose(); }
});

test('冷 SDK 按原始目标解析真实绑定，同 revision 换商家或同目标新绑定均拒旧调用，已执行业务由原 owner 结算', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-action-binding-'));
  const initial = { merchantId: 'merchant-a', generation: 1, visible: true };
  const selection = new BehaviorSubject(initial);
  const entered = Promise.withResolvers<void>(), finish = Promise.withResolvers<void>();
  const effects: { merchantId: string; revision: number }[] = [];
  let scopeParses = 0, modelCalls = 0;
  const prepare = defineAction({ name: 'prepare_merchant', description: '处理明确商家的当前修订',
    scope: z.string().min(1).transform(merchantId => { scopeParses++; return { merchantId }; }),
    input: z.object({ expectedRevision: z.literal(7) }).strict(), output: z.object({ merchantId: z.string(), revision: z.literal(7) }) });
  const implementation = implementAction(prepare, { async execute(_input, call) {
    if (call.source.kind !== 'participant' || call.source.actorId !== 'operator-test') throw new Error('BUSINESS_ACTOR_DENIED');
    entered.resolve(); await finish.promise;
    const result = { merchantId: call.scope.merchantId, revision: 7 as const };
    effects.push(result); return result;
  } });
  const surface = defineSurface({ views: { merchant: view(z.object({ merchantId: z.string(), revision: z.literal(7) })) }, actions: { prepare } });
  const program = defineProgram({ input: z.null(), surface });
  function Merchant({ current }: { current: typeof initial }) {
    const action = useAction(implementation, () => current.merchantId, [current.merchantId, current.generation]);
    return Expose({ surface, value: {
      views: { merchant: of({ merchantId: current.merchantId, revision: 7 as const }) },
      actions: current.visible ? { prepare: action } : {},
    } });
  }
  const definition = defineApplication({ name: 'captured-action', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '只处理解析时明确指定的商家', program: implementProgram(program, { Component() {
      const observed = useObservable(() => selection, []);
      return observed.status === 'ready' ? h(Merchant, { current: observed.value }) : null;
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () { modelCalls++; yield* text('没有请求模型'); });
  try {
    const created = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    await app.host.close(created.id); created.release();
    const session = await app.host.open(created.id, program);
    assert.deepEqual(await session.readView('merchant'), { merchantId: 'merchant-a', revision: 7 });
    assert.equal(app.native.agents.get(SessionId(session.id)), undefined);
    const beforeResolve = scopeParses;
    const original = session.resolveAction('prepare', 'merchant-a');
    assert.equal(scopeParses, beforeResolve + 1, '只解析消费者的原始期待目标，不再解析绑定的领域输出');
    assert.deepEqual(original.scope, { merchantId: 'merchant-a' });
    const changed = (error: unknown) => error instanceof ActionBindingUnavailableError
      && error.actionName === 'prepare' && error.reason === 'binding-changed';
    const mismatch = (error: unknown) => error instanceof ActionTargetMismatchError && error.actionName === 'prepare';
    selection.next({ merchantId: 'merchant-b', generation: 1, visible: true });
    assert.throws(() => original({ expectedRevision: 7 }), changed);
    assert.deepEqual(await session.readView('merchant'), { merchantId: 'merchant-b', revision: 7 });
    assert.throws(() => session.resolveAction('prepare', 'merchant-a'), mismatch, '视图A与解析B之间的窗口必须拒绝');
    const currentB = session.resolveAction('prepare', 'merchant-b');
    assert.notEqual(currentB.bindingId, original.bindingId);
    selection.next({ merchantId: 'merchant-a', generation: 2, visible: true });
    const currentA = session.resolveAction('prepare', 'merchant-a');
    assert.deepEqual(await session.readView('merchant'), { merchantId: 'merchant-a', revision: 7 });
    assert.notEqual(currentA.bindingId, original.bindingId, '同一scope的新真实绑定不能复用旧身份');
    assert.throws(() => original({ expectedRevision: 7 }), changed);
    assert.throws(() => session.invokeAction('prepare', currentB.bindingId, { expectedRevision: 7 }), changed);
    selection.next({ merchantId: 'merchant-a', generation: 2, visible: false });
    assert.throws(() => currentA({ expectedRevision: 7 }), error => error instanceof ActionBindingUnavailableError && error.reason === 'not-exposed');
    selection.next({ merchantId: 'merchant-a', generation: 2, visible: true });
    assert.equal(session.resolveAction('prepare', 'merchant-a').bindingId, currentA.bindingId, '同一真实绑定重新公开仍是原owner');
    const cancelled = new AbortController(), cancelledReason = new Error('ACTION_ADMISSION_CALLER_EXITED');
    const notStarted = currentA({ expectedRevision: 7 }, { signal: cancelled.signal });
    cancelled.abort(cancelledReason);
    await assert.rejects(notStarted.result, error => error === cancelledReason);
    assert.deepEqual(effects, [], '原生冷检查尚未准入时的caller取消不能开始execute');
    assert.equal(app.native.agents.get(SessionId(session.id)), undefined);
    assert.throws(() => currentA({ expectedRevision: 7 }, { signal: cancelled.signal }), error => error === cancelledReason);
    const admitting = currentA({ expectedRevision: 7 });
    selection.next({ merchantId: 'merchant-b', generation: 3, visible: true });
    await assert.rejects(admitting.result, changed, '准入await期间换绑不能执行旧调用或改指新商家');
    assert.deepEqual(effects, []);
    const captured = session.resolveAction('prepare', 'merchant-b');
    await assert.rejects(captured({ expectedRevision: 7 }, { actorId: 'intruder' }).result, /BUSINESS_ACTOR_DENIED/u);
    const running = captured({ expectedRevision: 7 });
    await entered.promise;
    selection.next({ merchantId: 'merchant-a', generation: 4, visible: true });
    assert.throws(() => captured({ expectedRevision: 7 }), changed);
    assert.deepEqual(await session.readView('merchant'), { merchantId: 'merchant-a', revision: 7 });
    finish.resolve();
    assert.deepEqual(await running.result, { merchantId: 'merchant-b', revision: 7 });
    assert.deepEqual(effects, [{ merchantId: 'merchant-b', revision: 7 }]);
    assert.equal(app.native.agents.get(SessionId(session.id)), undefined);
    assert.equal(modelCalls, 0); assert.deepEqual(app.errors, []);
    session.release();
  } finally { finish.resolve(); selection.complete(); await app.dispose(); }
});

test('完整帧先撤下 Action 与 Offer，再启动冷查询，同步 producer 重入 SDK 不能取得旧执行许可', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-commit-reentry-'));
  const phase$ = new BehaviorSubject<'available' | 'withdrawn'>('available');
  const started = Promise.withResolvers<void>();
  const attempts: Promise<string>[] = [];
  const queryStarts: string[] = [];
  const modelOffersSave: boolean[] = [];
  let invoke: (() => Promise<string>) | undefined;
  let rejection: unknown;
  let executions = 0;
  const save = defineAction({ name: 'save', description: '保存当前许可操作', scope: z.null(), input: z.null(), output: z.string() });
  const saveImplementation = implementAction(save, {
    execute() { executions++; return 'executed'; }
  });
  const surface = defineSurface({ views: { phase: view(z.string()) }, actions: { save } });
  function Work() {
    const phase = useObservable(() => phase$, []);
    // 同一个绑定始终挂载，不能由子树退出时的 inactive 检查掩盖旧许可。
    const action = useAction(saveImplementation, () => null, []);
    const current = phase.status === 'ready' ? phase.value : 'uncommitted';
    const query$ = useShared(() => defer(() => {
      queryStarts.push(current);
      if (current === 'withdrawn') {
        try {
          if (!invoke) throw new Error('SDK_NOT_ATTACHED');
          attempts.push(invoke());
        } catch (error) { rejection = error; }
        started.resolve();
      }
      return of(current);
    }), [current]);
    if (phase.status !== 'ready') return null;
    return h(Fragment, null, Context({ name: 'phase', value: current }),
      Offer({ name: 'save', action, enabled: current === 'available' }),
      Expose({ surface, value: { views: { phase: query$ }, actions: current === 'available' ? { save: action } : {} } }));
  }
  const definition = defineApplication({ name: 'commit-reentry', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '只使用完整当前声明',
    program: implementProgram(defineProgram({ input: z.null(), surface }), { Component: Work }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* (options) {
    modelOffersSave.push(options.tools?.some(tool => tool.name === 'save') === true);
    yield* text('已读取当前声明');
  });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    assert.equal(await session.readView('phase'), 'available');
    const saveCurrent = session.resolveAction('save', null);
    invoke = () => saveCurrent(null).result;
    phase$.next('withdrawn');
    await started.promise;
    await Promise.allSettled(attempts);
    assert.deepEqual(queryStarts, ['available', 'withdrawn']);
    assert.ok(rejection instanceof Error);
    assert.ok(rejection instanceof ActionBindingUnavailableError);
    assert.equal(executions, 0);
    assert.equal(await session.readView('phase'), 'withdrawn');
    assert.throws(() => saveCurrent(null), ActionBindingUnavailableError);
    await session.submit({ requestId: 'after-withdrawal', content: '读取撤下后的声明', intent: 'question', delivery: 'followup' as const, scope: null });
    await session.whenIdle();
    assert.deepEqual(modelOffersSave, [false]);
    assert.deepEqual(app.errors, []);
    session.release();
  } finally { await app.dispose(); }
});
