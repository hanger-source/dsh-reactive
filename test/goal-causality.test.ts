import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement, Fragment } from 'react';
import { SessionId } from '@deepseek-ai/dsh-session';
import type { GenerateOptions, StreamChunk } from '@deepseek-ai/dsh-llm';
import { defineAction, implementAction, defineApplication, defineEnvironment, defineProgram, defineSurface,
  z, type Continuation, type InputReference } from '@hanger-source/dsh-reactive';
import { implementProgram, Offer, useAction } from '@hanger-source/dsh-reactive/program';
import { boot, text, tool } from './native-support.ts';

const wait = { id: 'original-owner', operation: { id: 'original-operation' } };
type Attempt = { goal: Continuation['goal']; inputs: readonly InputReference[]; completed: boolean; error?: string };
async function fixture(mode: 'native' | 'ptc', respond: (step: number, request: GenerateOptions) => AsyncIterable<StreamChunk>, holdAfterFinish = false, onTouch?: () => void) {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-goal-causality-'));
  const attempts: Attempt[] = [];
  let continuation: Continuation | undefined, calls = 0;
  const beginning = implementAction(defineAction({ name: 'begin_owner', description: '登记原工作的业务等待',
    scope: z.null(), input: z.null(), output: z.literal(true) }), { async execute(_input, call) {
    assert.ok(call.continuation); continuation = call.continuation; await call.wait(wait); return true as const;
  } });
  const finishing = implementAction(defineAction({ name: 'finish_owner', description: '完成本次模型步骤明确关联的目标',
    scope: z.null(), input: z.null(), output: z.literal(true) }), { async execute(_input, call) {
    assert.ok(call.continuation);
    const attempt: Attempt = { goal: call.continuation.goal, inputs: call.inputs, completed: false };
    attempts.push(attempt);
    try { call.completeGoal(); attempt.completed = true; }
    catch (error) { attempt.error = (error as Error).message; }
    if (holdAfterFinish) { continuation = call.continuation; await call.wait(wait); }
    return true as const;
  } });
  const touching = implementAction(defineAction({ name: 'touch_owner', description: '观察一次原工作的业务事实',
    scope: z.null(), input: z.null(), output: z.literal(true) }), { execute() { onTouch?.(); return true as const; } });
  const definition = defineApplication({ name: 'goal-causality', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '仅沿本次输入与明确的目标关联执行。',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component() {
      const begin = useAction(beginning, () => null, []), finish = useAction(finishing, () => null, []);
      const touch = useAction(touching, () => null, []);
      return createElement(Fragment, null, Offer({ action: begin }), Offer({ action: finish }), Offer({ action: touch }));
    } }),
  })] });
  const app = await boot(root, definition, request => respond(++calls, request), mode);
  return { app, attempts, continuation: () => { assert.ok(continuation); return continuation; }, calls: () => calls };
}
async function* invoke(mode: 'native' | 'ptc', name: string, id: string, args: unknown = null) {
  if (mode === 'native') yield* tool(name, id, args);
  else yield* tool('run_code', id, { description: '调用同一业务能力', code: `return await tools.${name}(${JSON.stringify(args)});` });
}

for (const mode of ['native', 'ptc'] as const) {
  test(`${mode}: 暂停目标后处理独立问题，Action 无权完成暂停目标`, async () => {
    const f = await fixture(mode, async function* (step) {
      if (step === 1) yield* invoke(mode, 'begin_owner', 'original-wait');
      else if (step === 2) yield* invoke(mode, 'finish_owner', 'independent-question');
      else yield* text('独立问题已处理');
    });
    try {
      const session = await f.app.host.create('work', { workspaceId: f.app.workspace.workspaceId });
      await session.submit({ requestId: 'original', content: '原始目标', intent: 'goal', delivery: 'followup', scope: null });
      await session.whenIdle();
      const paused = await session.pause();
      const pausedSnapshot = session.snapshot().goal;
      assert.equal(pausedSnapshot!.revision, paused.revision);
      await session.submit({ requestId: 'question', content: '只处理独立问题', intent: 'question', delivery: 'followup', scope: null });
      await session.whenIdle();
      assert.equal(f.attempts.length, 1);
      assert.equal(f.attempts[0]!.goal, null);
      assert.equal(f.attempts[0]!.completed, false);
      assert.match(f.attempts[0]!.error!, /REACTIVE_GOAL_ABSENT/u);
      assert.deepEqual(session.snapshot().goal, pausedSnapshot);
      assert.deepEqual(f.app.errors, []);
    } finally { await f.app.dispose(); }
  });

  test(`${mode}: 模型流开始后外部修改 Goal，完成与 wait 保留原步骤修订`, async () => {
    const entered = Promise.withResolvers<void>(), release = Promise.withResolvers<void>();
    const f = await fixture(mode, async function* (step) {
      assert.equal(step, 1); entered.resolve(); await release.promise;
      yield* invoke(mode, 'finish_owner', 'old-model-stream');
    }, true);
    try {
      const session = await f.app.host.create('work', { workspaceId: f.app.workspace.workspaceId });
      await session.submit({ requestId: 'original', content: '原始目标', intent: 'goal', delivery: 'followup', scope: null });
      await entered.promise;
      const agent = f.app.native.agents.get(SessionId(session.id))!;
      const original = f.app.native.goals.get(agent)!;
      const changed = f.app.native.goals.edit(agent, original, { objective: '修改后的工作' });
      f.app.native.goals.disarm(agent);
      release.resolve(); await session.whenIdle();
      assert.equal(f.attempts.length, 1);
      assert.deepEqual(f.attempts[0]!.goal, { id: original.id, revision: original.revision });
      assert.equal(f.attempts[0]!.completed, false);
      assert.ok(f.attempts[0]!.error);
      assert.deepEqual(f.continuation().goal, { id: original.id, revision: original.revision });
      assert.deepEqual(session.snapshot().waits[0]!.continuation.goal, f.continuation().goal);
      assert.equal(session.snapshot().goal!.revision, changed.revision);
      assert.equal(session.snapshot().goal!.phase, 'active');
      assert.deepEqual(f.app.errors, []);
    } finally { release.resolve(); await f.app.dispose(); }
  });

  test(`${mode}: 无 Goal 的结果连续调用两次工具，也不能借用另一个 active Goal`, async () => {
    const f = await fixture(mode, async function* (step) {
      if (step === 1) yield* invoke(mode, 'begin_owner', 'question-wait');
      else if (step <= 3) yield* invoke(mode, 'finish_owner', 'result-finish-' + step);
      else yield* text('原问题的结果已核验');
    });
    try {
      const session = await f.app.host.create('work', { workspaceId: f.app.workspace.workspaceId });
      await session.submit({ requestId: 'question', content: '调查独立问题', intent: 'question', delivery: 'followup', scope: null });
      await session.whenIdle();
      assert.equal(f.continuation().goal, null);
      const agent = f.app.native.agents.get(SessionId(session.id))!;
      const unrelated = f.app.native.goals.create(agent, { objective: '另一个工作' });
      f.app.native.goals.disarm(agent);
      await session.submit({ requestId: 'question-result', content: '原问题的正式结果', intent: 'result', scope: null,
        result: { continuation: f.continuation(), wait, ready: true } });
      await session.whenIdle();
      assert.equal(f.calls(), 4);
      assert.equal(f.attempts.length, 2);
      for (const attempt of f.attempts) { assert.equal(attempt.goal, null); assert.equal(attempt.completed, false); }
      assert.equal(session.snapshot().goal!.id, unrelated.id);
      assert.equal(session.snapshot().goal!.revision, unrelated.revision);
      assert.equal(session.snapshot().goal!.phase, 'active');
      assert.deepEqual(f.app.errors, []);
    } finally { await f.app.dispose(); }
  });

  test(`${mode}: 正式原生 create_goal 的新关联只用于下一模型步骤`, async () => {
    const f = await fixture(mode, async function* (step) {
      if (step === 1) yield* invoke(mode, 'create_goal', 'explicit-goal', { objective: '人明确要求的工作' });
      else if (step === 2) yield* invoke(mode, 'finish_owner', 'finish-created-goal');
      else yield* text('明确创建的目标已完成');
    });
    try {
      const session = await f.app.host.create('work', { workspaceId: f.app.workspace.workspaceId });
      await session.submit({ requestId: 'new-work', content: '请建立目标并完成这项工作', intent: 'question', delivery: 'followup', scope: null });
      await session.whenIdle();
      assert.equal(f.attempts.length, 1);
      assert.ok(f.attempts[0]!.goal);
      assert.equal(f.attempts[0]!.completed, true);
      assert.equal(session.snapshot().goal!.phase, 'complete');
      assert.deepEqual(f.app.errors, []);
    } finally { await f.app.dispose(); }
  });

  test(`${mode}: 旧工具返回后的后台 Native edit 不成为下一步骤的 Goal 授权`, async () => {
    const entered = Promise.withResolvers<void>(), release = Promise.withResolvers<void>(), changed = Promise.withResolvers<void>();
    let touches = 0;
    const f = await fixture(mode, async function* (step) {
      if (step === 1) yield* invoke(mode, 'touch_owner', 'start-background');
      else if (step === 2) { entered.resolve(); await changed.promise; yield* invoke(mode, 'touch_owner', 'current-step'); }
      else if (step === 3) yield* invoke(mode, 'finish_owner', 'finish-after-background');
      else yield* text('已核验原工作，未借用后台修订');
    }, false, () => {
      if (++touches !== 1) return;
      // Promise 在真实工具执行内创建，返回后仍继承原生 Agent initiator。
      void release.promise.then(() => {
        const agent = f.app.native.agents.get(SessionId(session.id))!;
        const goal = f.app.native.goals.get(agent)!;
        f.app.native.goals.edit(agent, goal, { objective: '后台到达的新修订' });
        f.app.native.goals.disarm(agent); changed.resolve();
      }).catch(changed.reject);
    });
    const session = await f.app.host.create('work', { workspaceId: f.app.workspace.workspaceId });
    try {
      await session.submit({ requestId: 'original', content: '原始目标', intent: 'goal', delivery: 'followup', scope: null });
      await entered.promise;
      const original = session.snapshot().goal!;
      release.resolve(); await session.whenIdle();
      assert.equal(f.attempts.length, 1);
      assert.deepEqual(f.attempts[0]!.goal, { id: original.id, revision: original.revision });
      assert.equal(f.attempts[0]!.completed, false);
      assert.equal(session.snapshot().goal!.revision, original.revision + 1);
      assert.equal(session.snapshot().goal!.phase, 'active');
      assert.deepEqual(f.app.errors, []);
    } finally { release.resolve(); await f.app.dispose(); }
  });

  test(`${mode}: 正式 Goal 通知同步重入时，外层旧通知不能覆盖后续关联`, async () => {
    const f = await fixture(mode, async function* (step) {
      if (step === 1) yield* invoke(mode, 'create_goal', 'reentrant-create', { objective: '原始工作' });
      else if (step === 2) yield* invoke(mode, 'finish_owner', 'finish-reentrant-goal');
      else yield* text('已按正式新修订完成');
    });
    const stop = f.app.native.on('goal/changed', ({ agent, change }) => {
      if (change.operation === 'create') f.app.native.goals.edit(agent, change.ref, { objective: '同步确认的新工作' });
    }, { prepend: true });
    try {
      const session = await f.app.host.create('work', { workspaceId: f.app.workspace.workspaceId });
      await session.submit({ requestId: 'new-work', content: '请建立目标并完成这项工作', intent: 'question', delivery: 'followup', scope: null });
      await session.whenIdle();
      assert.equal(f.attempts.length, 1);
      assert.equal(f.attempts[0]!.goal!.revision, 2);
      assert.equal(f.attempts[0]!.completed, true);
      assert.equal(session.snapshot().goal!.phase, 'complete');
      assert.deepEqual(f.app.errors, []);
    } finally { stop(); await f.app.dispose(); }
  });
}

test('异步 pre-step 检查结束后 Goal 已被外部编辑，本步不提交模型', async () => {
  const entered = Promise.withResolvers<void>(), release = Promise.withResolvers<void>();
  const f = await fixture('native', async function* () { yield* text('不应进入模型'); });
  const stop = f.app.native.on('agent/pre-step', async (_event, next) => {
    const decision = await next(); entered.resolve(); await release.promise; return decision;
  });
  try {
    const session = await f.app.host.create('work', { workspaceId: f.app.workspace.workspaceId });
    await session.submit({ requestId: 'original', content: '原始工作', intent: 'goal', delivery: 'followup', scope: null });
    await entered.promise;
    const agent = f.app.native.agents.get(SessionId(session.id))!;
    const changed = f.app.native.goals.edit(agent, f.app.native.goals.get(agent)!, { objective: '外部新修订' });
    f.app.native.goals.disarm(agent); release.resolve(); await session.whenIdle();
    assert.equal(f.calls(), 0, f.app.errors.map(String).join('\n'));
    assert.equal(session.snapshot().goal!.revision, changed.revision);
    assert.equal(f.app.errors.length, 1);
    assert.match(String(f.app.errors[0]), /REACTIVE_STEP_GOAL_CHANGED/u);
  } finally { release.resolve(); stop(); await f.app.dispose(); }
});

test('结果已 rearm 但原生步骤拒准入时不自动继续，原消息随新输入消费且不重执命令', async () => {
  const f = await fixture('native', async function* (step, request) {
    if (step === 1) yield* tool('begin_owner', 'original-wait', null);
    else if (step === 2) {
      assert.match(JSON.stringify(request.messages), /本次正式结果/u);
      yield* tool('finish_owner', 'new-input-authority', null);
    } else yield* text('已沿新的明确输入核验原结果');
  });
  let reject = true;
  const stop = f.app.native.on('agent/pre-step', async ({ messages }, next) => {
    if (reject && messages.some(message => 'rpcId' in message.source && message.source.rpcId === 'owner-result')) {
      reject = false; return { kind: 'reject' };
    }
    return next();
  });
  try {
    const session = await f.app.host.create('work', { workspaceId: f.app.workspace.workspaceId });
    await session.submit({ requestId: 'original', content: '原始工作', intent: 'goal', delivery: 'followup', scope: null });
    await session.whenIdle();
    const original = session.snapshot().goal!;
    await session.submit({ requestId: 'owner-result', content: '本次正式结果', intent: 'result', scope: null,
      result: { continuation: f.continuation(), wait, ready: true } });
    await session.whenIdle();
    assert.equal(reject, false);
    assert.equal(f.calls(), 1);
    assert.equal(session.snapshot().goal!.activation, 'disarmed');
    assert.equal(session.snapshot().goal!.revision, original.revision + 1);
    const agent = f.app.native.agents.get(SessionId(session.id))!;
    const pending = [...agent.inbox.nextStep, ...agent.inbox.nextTurn].find(message =>
      'rpcId' in message.source && message.source.rpcId === 'owner-result');
    assert.ok(pending);
    await session.submit({ requestId: 'new-authority', content: '现在明确核验原结果并完成原工作', intent: 'question', delivery: 'followup', scope: null });
    await session.whenIdle();
    assert.equal(f.calls(), 3);
    assert.equal(f.attempts.length, 1);
    assert.equal(f.attempts[0]!.completed, true);
    assert.ok(f.attempts[0]!.inputs.some(input => input.requestId === 'new-authority'));
    assert.equal(session.snapshot().goal!.phase, 'complete');
    const entered = agent.session.snapshotEvents().filter(event => event.type === 'user/message'
      && event.data.id === pending.id);
    assert.equal(entered.length, 1);
    assert.deepEqual(f.app.errors, []);
  } finally { stop(); await f.app.dispose(); }
});
