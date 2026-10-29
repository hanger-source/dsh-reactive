import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { appendFile, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SessionId } from '@deepseek-ai/dsh-session';
import { Fragment, createElement as h } from 'react';
import { BehaviorSubject, defer, filter, firstValueFrom, of } from 'rxjs';
import { ActionAdmissionInProgressError, ActionInvocationConflictError, ActionBindingUnavailableError, ActionTargetMismatchError, defineApplication, defineEnvironment, z } from '@hanger-source/dsh-reactive';
import { InvocationUnavailableError } from '@hanger-source/dsh-reactive-dsh';
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

test('公开宿主按原始请求身份只受理一次，同步重入、撤声明及冲突不重执或覆盖原调用', { timeout: 15_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-action-admission-'));
  const file = join(root, 'writes.jsonl');
  const visible = new BehaviorSubject(true);
  const entered = Promise.withResolvers<void>(), finish = Promise.withResolvers<void>();
  let parses = 0, executions = 0, dateExecutions = 0, models = 0;
  let onParse: (() => void) | undefined;
  const save = defineAction({ name: 'save_once', description: '按原始请求受理文件写入', scope: z.null(),
    input: z.object({ amount: z.string(), labels: z.object({ first: z.string(), second: z.string() }) }).transform(input => {
      parses++;
      onParse?.();
      return { ...input, amount: Number(input.amount) };
    }), output: z.number() });
  const date = defineAction({ name: 'local_date', description: '本地直接消费日期对象', scope: z.null(), input: z.date(), output: z.number() });
  const saveImplementation = implementAction(save, { async execute(input) {
    executions++; entered.resolve(); await finish.promise;
    await appendFile(file, JSON.stringify(input) + '\n');
    return input.amount;
  } });
  const dateImplementation = implementAction(date, { execute(input) { dateExecutions++; return input.getTime(); } });
  const surface = defineSurface({ views: { visible: view(z.boolean()) }, actions: { save, date } });
  const program = defineProgram({ input: z.null(), surface });
  const definition = defineApplication({ name: 'action-admission', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '仅通过显式宿主调用写入文件', program: implementProgram(program, { Component() {
      const observation = useObservable(() => visible, []);
      const save = useAction(saveImplementation, () => null, []);
      const date = useAction(dateImplementation, () => null, []);
      if (observation.status !== 'ready') return null;
      return Expose({ surface, value: { views: { visible: of(observation.value) },
        actions: observation.value ? { save, date } : {} } });
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () { models++; yield* text('不应启动模型'); });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const peer = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const bindingId = session.resolveAction('save', null).bindingId;
    const input = { amount: '7', labels: { first: 'a', second: 'b' } };
    const reordered = { labels: { second: 'b', first: 'a' }, amount: '7' };
    const invocationId = randomUUID();
    let reentries = 0;
    onParse = () => {
      onParse = undefined;
      reentries++;
      assert.throws(() => session.invokeAction('save', bindingId, reordered, { invocationId }),
        error => error instanceof ActionAdmissionInProgressError && error.invocationId === invocationId);
      assert.throws(() => app.host.readInvocation(session.id, invocationId, program), InvocationUnavailableError);
    };
    const original = session.invokeAction('save', bindingId, input, { invocationId });
    await entered.promise;
    assert.equal(session.invokeAction('save', bindingId, reordered, { invocationId }), original);
    assert.equal(app.host.readInvocation(session.id, invocationId, program).invocation, original);
    // 两个 raw 输入解析后同为 7，仍不能把另一份请求认作原调用。
    assert.throws(() => session.invokeAction('save', bindingId, { ...input, amount: '07' }, { invocationId }), ActionInvocationConflictError);
    assert.throws(() => session.invokeAction('save', randomUUID(), input, { invocationId }), ActionInvocationConflictError);
    assert.throws(() => session.invokeAction('save', bindingId, input, { invocationId, actorId: 'other-human' }), ActionInvocationConflictError);
    assert.throws(() => peer.invokeAction('save', peer.resolveAction('save', null).bindingId, input, { invocationId }), ActionInvocationConflictError);
    assert.equal(app.host.readInvocation(session.id, invocationId, program).invocation, original);
    assert.equal(parses, 1); assert.equal(executions, 1); assert.equal(reentries, 1);

    const withdrawn = firstValueFrom(session.views.visible.pipe(filter(value => value === false)));
    visible.next(false);
    await withdrawn;
    assert.equal(await session.readView('visible'), false);
    assert.throws(() => session.resolveAction('save', null), ActionBindingUnavailableError);
    assert.equal(session.invokeAction('save', bindingId, reordered, { invocationId }), original,
      '同一已受理请求不能依赖当前 Expose 重建执行');
    finish.resolve();
    assert.equal(await original.result, 7);
    assert.equal(await readFile(file, 'utf8'), JSON.stringify({ amount: 7, labels: { first: 'a', second: 'b' } }) + '\n');

    const rejectedId = randomUUID();
    let refusal: unknown;
    assert.throws(() => session.invokeAction('save', bindingId, input, { invocationId: rejectedId }), error => {
      refusal = error;
      return error instanceof ActionBindingUnavailableError && error.reason === 'not-exposed';
    });
    assert.throws(() => app.host.readInvocation(session.id, rejectedId, program), InvocationUnavailableError);
    const restored = firstValueFrom(session.views.visible.pipe(filter(value => value === true)));
    visible.next(true);
    await restored;
    assert.equal(await session.readView('visible'), true);
    assert.throws(() => session.invokeAction('save', bindingId, reordered, { invocationId: rejectedId }), error => error === refusal,
      '声明恢复不能让原同步拒绝占位重新进入业务');
    assert.equal(parses, 1); assert.equal(executions, 1);
    assert.throws(() => session.invokeAction('save', bindingId, input,
      { invocationId: JSON.stringify([session.id, 1]) }), z.ZodError, '工具调用的数组编码身份不能进入显式参与者身份域');
    const localDate = new Date('2026-10-29T01:02:03.000Z');
    const local = session.resolveAction('date', null)(localDate);
    assert.equal(await local.result, localDate.getTime());
    assert.equal(app.host.readInvocation(session.id, local.id, program).invocation, local);
    assert.equal(dateExecutions, 1); assert.equal(models, 0); assert.deepEqual(app.errors, []);
    session.release(); peer.release();
  } finally { finish.resolve(); visible.complete(); await app.dispose(); }
});
