import { test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Fragment, createElement as h } from 'react';
import { firstValueFrom, filter } from 'rxjs';
import { SessionId } from '@deepseek-ai/dsh-session';
import type { GenerateOptions, StreamChunk } from '@deepseek-ai/dsh-llm';
import { createWorkSelector, defineAction, implementAction, defineApplication, defineEnvironment, defineProgram, defineSurface,
  z, type Continuation, type HumanQuestionOrigin, type HumanSnapshot, type ConversationProgressSnapshot } from '@hanger-source/dsh-reactive';
import { HumanInput, Offer, useAction, useBeforeStep, implementProgram } from '@hanger-source/dsh-reactive/program';
import { boot, text, tool } from './native-support.ts';

const work = { workId: 'knowledge' };
const answer = { answers: [{ id: 'evidence', selected: [], custom: '这里只补充原问题的证据' }] };
async function* invoke(mode: 'native' | 'ptc', name: string, id: string, args: unknown) {
  if (mode === 'native') yield* tool(name, id, args);
  else yield* tool('run_code', id, { description: '沿真实工具参与原工作', code: `return await tools.${name}(${JSON.stringify(args)});` });
}
async function fixture(mode: 'native' | 'ptc', respond: (step: number, request: GenerateOptions) => AsyncIterable<StreamChunk>, directory?: string) {
  const root = directory ?? await mkdtemp(join(tmpdir(), 'dsh-reactive-human-goal-'));
  const attempts: { continuation: Continuation; completed: boolean; error?: string }[] = [];
  const admissions: HumanQuestionOrigin[] = [];
  const entered = Promise.withResolvers<void>(), release = Promise.withResolvers<void>();
  let calls = 0, hold = false;
  const finish = implementAction(defineAction({ name: 'finish_original_work', description: '仅完成本次模型步骤明确关联的工作',
    scope: z.null(), input: z.null(), output: z.literal(true) }), { execute(_input, call) {
    assert.ok(call.continuation);
    const attempt: typeof attempts[number] = { continuation: call.continuation, completed: false }; attempts.push(attempt);
    try { call.completeGoal(); attempt.completed = true; } catch (error) { attempt.error = (error as Error).message; }
    return true as const;
  } });
  const definition = defineApplication({ name: 'human-goal-causality', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '回答只补充原工作；新的工作必须有自己的参与依据。',
    program: implementProgram(defineProgram({ input: z.object({ workId: z.string() }), surface: defineSurface({ views: {}, actions: {} }) }), { Component() {
      const finishing = useAction(finish, () => null, []);
      useBeforeStep(async () => { if (hold) { entered.resolve(); await release.promise; } }, []);
      return h(Fragment, null, Offer({ action: finishing }), HumanInput({ admitResponder(admission) {
        if (admission.answer) admissions.push(admission.question.origin);
      } }));
    } }),
  })] });
  const responding = (request: GenerateOptions) => respond(++calls, request);
  const app = await boot(root, definition, responding, mode);
  return { root, definition, responding, app, attempts, admissions, entered, release, hold: () => { hold = true; },
    proceed: () => { hold = false; release.resolve(); }, calls: () => calls };
}
async function* ask(mode: 'native' | 'ptc') {
  yield* invoke(mode, 'ask_user_question', 'original-question', {
    questions: [{ id: 'evidence', question: '原工作的证据在哪里？' }], timeout: 1,
  });
}
const selector = createWorkSelector({ identify: origin => origin.kind === 'invocation' ? undefined : z.object({ workId: z.string() }).parse(origin.scope).workId });

type Checkpoint = { sessionId: string; human: HumanSnapshot; revision: number };
async function crashWithPendingReply(directory: string, mode: 'native' | 'ptc', context: TestContext): Promise<Checkpoint> {
  const worker = fork(fileURLToPath(import.meta.url), ['--human-origin-checkpoint', directory, mode], {
    execArgv: [], stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  });
  let output = '';
  worker.stdout?.on('data', chunk => { output += String(chunk); });
  worker.stderr?.on('data', chunk => { output += String(chunk); });
  const ready = new Promise<Checkpoint>((resolve, reject) => {
    worker.once('message', message => resolve(message as Checkpoint)); worker.once('error', reject);
    worker.once('exit', (code, signal) => reject(new Error(`HUMAN_CHECKPOINT_EARLY_EXIT ${code}/${signal}\n${output}`)));
  });
  const abort = () => { worker.kill('SIGKILL'); };
  context.signal.addEventListener('abort', abort, { once: true });
  try {
    const checkpoint = await ready;
    const exited = once(worker, 'exit'); worker.kill('SIGKILL');
    assert.deepEqual(await exited, [null, 'SIGKILL']); return checkpoint;
  } finally { context.signal.removeEventListener('abort', abort); worker.kill('SIGKILL'); }
}

if (process.argv[2] !== '--human-origin-checkpoint') {
for (const mode of ['native', 'ptc'] as const) {
  for (const boundary of ['business-check', 'native-pre-step'] as const) {
    test(`${mode}: 合法迟答在 ${boundary} 等待至真实消费，承接身份可查且全帧不误报阻塞`, { timeout: 15_000 }, async () => {
      const entered = Promise.withResolvers<void>(), release = Promise.withResolvers<void>();
      let hold = false;
      const f = await fixture(mode, async function* (step) {
        if (step === 1) yield* ask(mode);
        else if (step === 3) yield* invoke(mode, 'finish_original_work', 'finish-admitted-answer', null);
        else yield* text('已核验原工作');
      });
      const stop = boundary === 'native-pre-step' ? f.app.native.on('agent/pre-step', async (_event, next) => {
        const decision = await next();
        if (hold) { entered.resolve(); await release.promise; }
        return decision;
      }) : () => {};
      try {
        const session = await f.app.host.create('work', { workspaceId: f.app.workspace.workspaceId });
        const foreground = firstValueFrom(session.foregroundQuestions$.pipe(filter(value => value.length === 1)));
        await session.submit({ requestId: 'original', content: '原工作需补充证据', intent: 'goal', delivery: 'followup', scope: work });
        const [question] = await foreground;
        const agent = f.app.native.agents.get(SessionId(session.id))!;
        f.app.native.goals.disarm(agent); await session.whenIdle();
        const progressFrames: ConversationProgressSnapshot[] = [];
        const workFrames: ReturnType<typeof selector.select>[] = [];
        const progressSubscription = session.progress.values$.subscribe(value => progressFrames.push(value));
        const workSubscription = selector.observe(work.workId, { sessions: [session] }).state.values$.subscribe(value => workFrames.push(value));
        try {
          if (boundary === 'business-check') f.hold(); else hold = true;
          assert.equal(await session.answerQuestion(question!.callId, answer), true);
          await (boundary === 'business-check' ? f.entered.promise : entered.promise);
          const progress = await session.progress.read(), human = await session.human.read();
          const reply = human.replies[0]!;
          assert.equal(reply.state, 'proposed');
          const admission = progress.preparation!.admissions.find(value => value.source.kind === 'human-reply');
          assert.deepEqual(admission, { source: { kind: 'human-reply', messageId: reply.messageId,
            questionCallId: question!.callId, callSequence: question!.origin.callSequence },
            continuation: question!.origin.continuation, goal: question!.origin.continuation!.goal });
          assert.ok(progress.preparation!.waiting.some(value => value.kind === (boundary === 'business-check' ? 'check' : 'step-admission')));
          const selected = await selector.observe(work.workId, { sessions: [session] }).state.read();
          const pending = selected.sessions[0]!.conditions.find(condition => condition.kind === 'question.reply-pending');
          assert.ok(pending && pending.kind === 'question.reply-pending');
          assert.deepEqual(pending.admission, admission);
          assert.equal(f.calls(), 2, '只观察原问题后的准备，迟答尚未交给受控模型');
          hold = false; release.resolve(); f.proceed(); await session.whenIdle();
          assert.equal(f.attempts[0]!.completed, true);
          assert.deepEqual(f.attempts[0]!.continuation.goal, admission!.goal);
          assert.deepEqual((await session.human.read()).replies, []);
          assert.equal((await session.progress.read()).preparation, null);
          assert.equal(agent.session.snapshotEvents().filter(event => event.type === 'user/message'
            && event.data.id === reply.messageId).length, 1);
          assert.ok(progressFrames.some(frame => frame.preparation?.admissions.some(value => value.source.kind === 'human-reply')));
          assert.ok(workFrames.some(frame => frame.sessions[0]!.conditions.some(condition => condition.kind === 'question.reply-pending'
            && condition.admission !== null)));
          assert.equal(workFrames.some(frame => frame.sessions[0]!.conditions.some(condition => condition.kind === 'question.reply-blocked')), false,
            '准备交接和真实消费之间不应短暂误报迟答被阻塞');
          assert.deepEqual(f.app.errors, []);
        } finally { progressSubscription.unsubscribe(); workSubscription.unsubscribe(); }
      } finally { hold = false; release.resolve(); f.proceed(); stop(); await f.app.dispose(); }
    });
  }

  test(`${mode}: 原独立问题的迟答连续调用工具也不能完成后来创建的 Goal`, async () => {
    const f = await fixture(mode, async function* (step) {
      if (step === 1) yield* ask(mode);
      else if (step === 3 || step === 4) yield* invoke(mode, 'finish_original_work', 'late-finish-' + step, null);
      else yield* text('已处理原问题');
    });
    try {
      const session = await f.app.host.create('work', { workspaceId: f.app.workspace.workspaceId });
      await session.submit({ requestId: 'original', content: '询问原问题', intent: 'question', delivery: 'followup', scope: work });
      await session.whenIdle();
      const question = (await session.human.read()).questions.active[0]!;
      assert.ok(question.origin.continuation);
      assert.equal(question.origin.continuation.goal, null);
      const agent = f.app.native.agents.get(SessionId(session.id))!;
      const unrelated = f.app.native.goals.create(agent, { objective: '后来创建的独立工作' }); f.app.native.goals.disarm(agent);
      assert.equal(await session.answerQuestion(question.callId, answer), true);
      await session.whenIdle();
      assert.equal(f.calls(), 5);
      assert.equal(f.attempts.length, 2);
      for (const attempt of f.attempts) { assert.equal(attempt.continuation.goal, null); assert.equal(attempt.completed, false); }
      assert.equal(session.snapshot().goal!.id, unrelated.id);
      assert.equal(session.snapshot().goal!.revision, unrelated.revision);
      assert.equal(session.snapshot().goal!.phase, 'active');
      assert.deepEqual(f.app.errors, []);
    } finally { await f.app.dispose(); }
  });

  test(`${mode}: 旧 Goal 迟答保持待输入与可解释关联，SIGKILL 后新要求才消费同一消息`, async context => {
    const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-human-origin-crash-'));
    const saved = await crashWithPendingReply(directory, mode, context);
    const f = await fixture(mode, async function* (step, request) {
      if (step === 1) {
        assert.match(JSON.stringify(request.messages), /这里只补充原问题的证据/u);
        assert.match(JSON.stringify(request.messages), /按新的要求核验旧回答/u);
        yield* invoke(mode, 'finish_original_work', 'explicit-new-requirement', null);
      } else yield* text('已处理明确要求');
    }, directory);
    try {
      const cold = await f.app.host.open(saved.sessionId, f.definition.environments[0].program.definition);
      const human = await cold.human.read(), reply = human.replies[0]!;
      assert.deepEqual(human, saved.human);
      assert.equal(f.app.native.agents.get(SessionId(cold.id)), undefined);
      assert.equal(f.calls(), 0, '恢复观察没有发出模型请求');
      const selected = await selector.observe(work.workId, { sessions: [cold] }).state.read();
      const blocked = selected.sessions[0]!.conditions.find(condition => condition.kind === 'question.reply-blocked');
      assert.ok(blocked && blocked.kind === 'question.reply-blocked'); assert.equal(blocked.reason, 'superseded');
      assert.equal(blocked.currentGoal!.revision, saved.revision);
      await cold.submit({ requestId: 'new-authority', content: '按新的要求核验旧回答并完成工作', intent: 'question', delivery: 'followup', scope: work });
      await cold.whenIdle();
      assert.equal(f.attempts.length, 1); assert.equal(f.attempts[0]!.completed, true);
      assert.ok(f.attempts[0]!.continuation.inputs.some(input => input.requestId === 'new-authority'));
      assert.equal(cold.snapshot().goal!.phase, 'complete'); assert.deepEqual((await cold.human.read()).replies, []);
      const live = f.app.native.agents.get(SessionId(cold.id))!;
      assert.equal(live.session.snapshotEvents().filter(event => event.type === 'user/message' && event.data.id === reply.messageId).length, 1);
      assert.equal(live.session.snapshotEvents().filter(event => event.type === 'tool/call' && event.data.name === (mode === 'native' ? 'ask_user_question' : 'run_code')
        && event.data.callId === 'original-question').length, 1);
      assert.deepEqual(f.app.errors, []); cold.release();
    } finally { f.proceed(); await f.app.dispose(); }
  });

  test(`${mode}: 无 Goal 迟答准备期间另一个 Goal 编辑，重装后仍可处理且不取得其权限`, async () => {
    const f = await fixture(mode, async function* (step) {
      if (step === 1) yield* ask(mode);
      else if (step === 3) yield* invoke(mode, 'finish_original_work', 'independent-answer', null);
      else yield* text('独立问题已处理');
    });
    try {
      const session = await f.app.host.create('work', { workspaceId: f.app.workspace.workspaceId });
      await session.submit({ requestId: 'original', content: '调查独立问题', intent: 'question', delivery: 'followup', scope: work });
      await session.whenIdle();
      const question = (await session.human.read()).questions.active[0]!;
      const agent = f.app.native.agents.get(SessionId(session.id))!;
      const unrelated = f.app.native.goals.create(agent, { objective: '另一项工作' }); f.app.native.goals.disarm(agent);
      f.hold(); await session.answerQuestion(question.callId, answer); await f.entered.promise;
      const changed = f.app.native.goals.edit(agent, unrelated, { objective: '另一项工作的修订' }); f.app.native.goals.disarm(agent);
      f.proceed(); await session.whenIdle();
      assert.equal(f.attempts.length, 1); assert.equal(f.attempts[0]!.continuation.goal, null);
      assert.equal(f.attempts[0]!.completed, false); assert.deepEqual((await session.human.read()).replies, []);
      assert.equal(session.snapshot().goal!.revision, changed.revision); assert.equal(session.snapshot().goal!.phase, 'active');
      assert.deepEqual(f.app.errors, []);
    } finally { f.proceed(); await f.app.dispose(); }
  });

  test(`${mode}: 同一 Goal 的合法迟答跨宿主重启后仍沿原修订完成`, async () => {
    const f = await fixture(mode, async function* (step) {
      if (step === 1) yield* ask(mode);
      else if (step === 3) yield* invoke(mode, 'finish_original_work', 'finish-original', null);
      else yield* text('已核验原工作');
    });
    try {
      const session = await f.app.host.create('work', { workspaceId: f.app.workspace.workspaceId });
      const foreground = firstValueFrom(session.foregroundQuestions$.pipe(filter(value => value.length === 1)));
      await session.submit({ requestId: 'original', content: '先询问原工作的证据', intent: 'goal', delivery: 'followup', scope: work });
      const [question] = await foreground;
      const agent = f.app.native.agents.get(SessionId(session.id))!;
      f.app.native.goals.disarm(agent); await session.whenIdle();
      const expected = question!.origin.continuation!;
      const id = session.id; await f.app.host.close(id); session.release(); await f.app.dispose();
      f.app = await boot(f.root, f.definition, f.responding, mode);
      const cold = await f.app.host.open(id, f.definition.environments[0].program.definition);
      const restored = (await cold.human.read()).questions.active[0]!;
      assert.deepEqual(restored.origin.continuation, expected);
      assert.equal(await cold.answerQuestion(restored.callId, answer), true); await cold.whenIdle();
      assert.equal(f.attempts.length, 1); assert.deepEqual(f.attempts[0]!.continuation.goal, expected.goal);
      assert.equal(f.attempts[0]!.completed, true); assert.equal(cold.snapshot().goal!.phase, 'complete');
      assert.deepEqual(f.app.errors, []); cold.release();
    } finally { await f.app.dispose(); }
  });

  test(`${mode}: 迟答准备等待中 Goal 被暂停，不开始模型也不消费原回答`, async () => {
    const f = await fixture(mode, async function* (step) {
      if (step === 1) yield* ask(mode); else yield* text('继续不依赖答复的工作');
    });
    try {
      const session = await f.app.host.create('work', { workspaceId: f.app.workspace.workspaceId });
      const foreground = firstValueFrom(session.foregroundQuestions$.pipe(filter(value => value.length === 1)));
      await session.submit({ requestId: 'original', content: '原工作需补充证据', intent: 'goal', delivery: 'followup', scope: work });
      const [question] = await foreground;
      const agent = f.app.native.agents.get(SessionId(session.id))!; f.app.native.goals.disarm(agent); await session.whenIdle();
      f.hold(); await session.answerQuestion(question!.callId, answer); await f.entered.promise;
      const preparation = (await session.progress.read()).preparation!;
      assert.equal(preparation.admissions.length, 1);
      assert.deepEqual(preparation.admissions[0]!.continuation, question!.origin.continuation);
      const preparing = await selector.observe(work.workId, { sessions: [session] }).state.read();
      assert.equal(preparing.sessions[0]!.conditions.some(condition => condition.kind === 'question.reply-blocked'), false);
      const paused = await session.pause(); f.proceed(); await session.whenIdle();
      assert.equal(f.calls(), 2); assert.equal(session.snapshot().goal!.revision, paused.revision);
      const snapshot = await session.human.read(); assert.equal(snapshot.replies.length, 1);
      assert.deepEqual(snapshot.replies[0]!.origin, question!.origin);
      const selected = await selector.observe(work.workId, { sessions: [session] }).state.read();
      assert.ok(selected.sessions[0]!.conditions.some(condition => condition.kind === 'question.reply-blocked'));
      assert.equal((await session.progress.read()).preparation, null);
      assert.deepEqual(f.app.errors, []);
    } finally { f.proceed(); await f.app.dispose(); }
  });
}
} else {
  const mode = process.argv[4] as 'native' | 'ptc';
  const f = await fixture(mode, async function* (step) {
    if (step === 1) yield* ask(mode); else yield* text('原回答尚未取得新 Goal 权限');
  }, process.argv[3]);
  const session = await f.app.host.create('work', { workspaceId: f.app.workspace.workspaceId });
  const foreground = firstValueFrom(session.foregroundQuestions$.pipe(filter(value => value.length === 1)));
  await session.submit({ requestId: 'original', content: '先询问原工作的证据', intent: 'goal', delivery: 'followup', scope: work });
  const [question] = await foreground;
  const agent = f.app.native.agents.get(SessionId(session.id))!;
  const original = f.app.native.goals.get(agent)!; f.app.native.goals.disarm(agent);
  await session.whenIdle();
  const changed = f.app.native.goals.edit(agent, original, { objective: '新的工作要求' }); f.app.native.goals.disarm(agent);
  assert.equal(await session.answerQuestion(question!.callId, answer), true); await session.whenIdle();
  assert.equal(f.calls(), 2); assert.equal(f.attempts.length, 0);
  const human = await session.human.read(), reply = human.replies[0]!;
  assert.equal(reply.state, 'queued'); assert.deepEqual(reply.origin, question!.origin);
  assert.deepEqual(reply.origin.continuation!.goal, { id: original.id, revision: original.revision });
  const selected = await selector.observe(work.workId, { sessions: [session] }).state.read();
  assert.ok(selected.sessions[0]!.conditions.some(condition => condition.kind === 'question.reply-blocked'));
  assert.deepEqual(f.app.errors, []);
  process.send!({ sessionId: session.id, human, revision: changed.revision } satisfies Checkpoint);
  process.on('message', () => {});
}
