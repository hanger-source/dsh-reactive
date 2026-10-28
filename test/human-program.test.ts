import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Fragment, createElement as h } from 'react';
import { BehaviorSubject, filter, firstValueFrom, tap } from 'rxjs';
import { SessionId, type SessionEvent } from '@deepseek-ai/dsh-session';
import type { ContentBlock, GenerateOptions } from '@deepseek-ai/dsh-llm';
import { Context, Expose, HumanInput, Offer, implementProgram, useAction, useObservable, useRelation } from '@hanger-source/dsh-reactive/program';
import { defineAction, implementAction, defineApplication, defineEnvironment, defineProgram, defineSurface, z, type ActionCallContext, type ActionInvocation, type HumanAnswer, type HumanQuestionOrigin } from '@hanger-source/dsh-reactive';
import { boot, text, tool } from './native-support.ts';

const prompts = [{ id: 'delivery-mode', header: '商家甲交付偏好', question: '商家甲希望如何接收交付资料？',
  options: [{ label: '站内' }, { label: '邮件' }], multiSelect: false }];
const deliveryDetails: HumanAnswer = { answers: [{ id: 'delivery-mode', selected: ['站内'] }] };
const submittedScope = { merchantId: 'merchant-a', requestedAt: '2026-10-27T10:00:00.000Z' };
type Step = 'foreground' | 'timed' | 'cancel' | 'drain' | 'log-failure' | 'undeclared';

async function scenario(mode: 'native' | 'ptc') {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-human-program-'));
  const mounted$ = new BehaviorSubject(true);
  const drainReturn = Promise.withResolvers<void>();
  const requests: GenerateOptions[] = [];
  const calls: ActionCallContext<{ readonly merchantId: string }>[] = [];
  const effects: { step: Step; merchantId: string; invocationId: string }[] = [];
  const admitted: HumanQuestionOrigin[] = [];
  const collectDetails = defineAction({ name: 'collect_merchant_details', description: '收集原商家的交付偏好，再记录本次已经准入的资料',
    scope: z.object({ merchantId: z.string() }).strict(),
    input: z.object({ step: z.enum(['foreground', 'timed', 'cancel', 'drain', 'log-failure', 'undeclared']) }).strict(),
    output: z.object({ step: z.enum(['foreground', 'timed', 'cancel', 'drain', 'log-failure', 'undeclared']),
      outcome: z.enum(['recorded', 'pending', 'returned-without-answer']), callId: z.string().optional() }).strict() });
  const invocations: ActionInvocation<typeof collectDetails>[] = [];
  const implementation = implementAction(collectDetails, { async execute(input, call) {
    calls.push(call);
    if (input.step === 'drain') {
      void call.askHuman(prompts);
      await drainReturn.promise;
      return { step: input.step, outcome: 'returned-without-answer' as const };
    }
    const result = input.step === 'timed'
      ? await call.askHuman(prompts, { timeoutSeconds: 1 })
      : await call.askHuman(prompts);
    if ('pending' in result) return { step: input.step, outcome: 'pending' as const, callId: result.callId };
    assert.deepEqual(result, deliveryDetails);
    call.signal.throwIfAborted();
    effects.push({ step: input.step, merchantId: call.scope.merchantId, invocationId: call.invocationId });
    return { step: input.step, outcome: 'recorded' as const };
  } });
  const surface = defineSurface({ views: {}, actions: { collectDetails } });
  const program = defineProgram({
    input: z.object({ merchantId: z.string(), requestedAt: z.iso.datetime() }).strict()
      .transform(value => ({ ...value, requestedAt: new Date(value.requestedAt) })),
    surface,
  });
  const definition = defineApplication({ name: 'human-program-' + mode, admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '调用明确商家的资料收集能力；能力内部询问交付偏好，人工补充后记录本次资料',
    program: implementProgram(program, { Component() {
      const action = useAction(implementation, () => ({ merchantId: 'merchant-a' }), []);
      const mounted = useObservable(() => mounted$, []);
      useRelation(() => action.invocations$.pipe(tap(invocation => { invocations.push(invocation); })), [action]);
      return h(Fragment, null, Context({ name: 'merchant', value: { merchantId: 'merchant-a' } }),
        Offer({ name: 'collect_merchant_details', action }), Expose({ surface, value: { actions: { collectDetails: action } } }),
        mounted.status === 'ready' && mounted.value ? HumanInput({ admitResponder(admission) {
          if (admission.actorId !== 'reviewer') throw new Error('REVIEWER_REQUIRED');
          if (!admission.question.origin.inputs.length || admission.question.origin.inputs.some(input => input.actorId !== 'request-author'
            || z.object({ merchantId: z.literal('merchant-a'), requestedAt: z.iso.datetime() }).strict().parse(input.scope).merchantId !== 'merchant-a')) {
            throw new Error('QUESTION_ORIGIN_DENIED');
          }
          admitted.push(structuredClone(admission.question.origin));
        } }) : null);
    } }),
  })] });
  let next: { step: Step; rootCallId: string } | undefined;
  let sequence = 0;
  const app = await boot(join(root, 'home'), definition, async function* (options) {
    requests.push(options);
    assert.ok(requests.length <= 12, '测试模型出现未预期的重复执行');
    const selected = next;
    next = undefined;
    if (!selected) { yield* text('本次调用已经结束，继续核验原要求'); return; }
    if (mode === 'native') yield* tool('collect_merchant_details', selected.rootCallId, { step: selected.step });
    else yield* tool('run_code', selected.rootCallId, { description: '收集商家甲的交付资料',
      code: `return await tools.collect_merchant_details(${JSON.stringify({ step: selected.step })});` });
  }, mode);
  return { app, program, mounted$, drainReturn, requests, calls, invocations, effects, admitted,
    next(step: Step) {
      assert.equal(next, undefined);
      const rootCallId = `human-program-${mode}-${++sequence}`;
      next = { step, rootCallId };
      return rootCallId;
    },
  };
}

function nestedEnd(events: readonly SessionEvent[], callId: string) {
  const event = events.find(event => event.type === 'tool/ptc-dispatch' && event.data.subCallId === callId);
  assert.ok(event?.type === 'tool/ptc-dispatch', '真实嵌套调用必须有正式结算日志');
  return event;
}
function parentEnd(events: readonly SessionEvent[], mode: 'native' | 'ptc', invocationId: string) {
  const event = events.find(event => mode === 'native'
    ? event.type === 'tool/result' && event.data.message.toolCallId === invocationId
    : event.type === 'tool/ptc-dispatch' && event.data.subCallId === invocationId);
  assert.ok(event?.type === 'tool/result' || event?.type === 'tool/ptc-dispatch');
  return event;
}

for (const mode of ['native', 'ptc'] as const) {
  test(`${mode} Action 先收集交付偏好后记录，嵌套问答沿原调用；未等待子问先 drain，已结束及独立 SDK 调用不能借原生控制`, { timeout: 30_000 }, async () => {
    const scene = await scenario(mode);
    const { app } = scene;
    try {
      const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
      const rootCallId = scene.next('foreground');
      const foreground = firstValueFrom(session.foregroundQuestions$.pipe(filter(value => value.length === 1)));
      const accepted = firstValueFrom(session.rawEvents$.pipe(filter(event => event.kind === 'input.accepted'
        && event.inputs.some(input => input.requestId === 'original-merchant'))));
      await session.submit({ requestId: 'original-merchant', content: '询问商家甲的交付偏好并记录资料', intent: 'question', delivery: 'followup' as const, scope: submittedScope }, { actorId: 'request-author' });
      const [question] = await foreground;
      assert.ok(question);
      assert.equal(question.timed, false);
      assert.deepEqual(question.questions, prompts);
      assert.equal(scene.calls.length, 1);
      assert.deepEqual(scene.effects, [], '没有人工答案时业务副作用不能开始');
      const invocation = scene.invocations[0]!;
      const input = await accepted;
      assert.ok(input.kind === 'input.accepted');
      const origin: HumanQuestionOrigin = { sessionId: session.id, callId: question.callId, rootCallId,
        parentCallId: invocation.id, name: 'ask_user_question', inputs: [{ sourceSessionId: session.id,
          messageId: input.messageId, requestId: 'original-merchant', source: 'user', intent: 'question',
          scope: submittedScope, actorId: 'request-author', participant: 'human' }] };
      assert.deepEqual(question.origin, origin, 'Action 内部提问必须是实际子调用，并保留未解码的原输入');
      const bound = await session.question(question.callId);
      assert.equal(await session.question(question.callId), bound, '同一 SDK 复用固定问题对象');
      assert.deepEqual((await bound.state.read()).foreground, question);
      await assert.rejects(session.question('never-asked'), /REACTIVE_HUMAN_QUESTION_NOT_FOUND/u);
      const service = await app.host.service().open(session.id, scene.program);
      try {
        // 忽略公开类型的 JavaScript 调用也不能把后台服务变成人工参与者。
        const untyped = service as unknown as typeof session;
        await assert.rejects(untyped.question(question.callId), /REACTIVE_HUMAN_PARTICIPANT_REQUIRED/u);
        assert.throws(() => untyped.answerQuestion(question.callId, deliveryDetails), /REACTIVE_HUMAN_PARTICIPANT_REQUIRED/u);
        await assert.rejects(untyped.attachWait(question.callId)[Symbol.asyncIterator]().next(), /REACTIVE_HUMAN_PARTICIPANT_REQUIRED/u);
        await assert.rejects(untyped.pause(), /REACTIVE_HUMAN_PARTICIPANT_REQUIRED/u);
        await assert.rejects(untyped.cancel(), /REACTIVE_HUMAN_PARTICIPANT_REQUIRED/u);
        assert.deepEqual(scene.effects, []);
        assert.deepEqual((await bound.state.read()).foreground, question, '拒绝服务控制后原前台仍在等待');
      } finally { service.release(); }
      const agent = app.native.agents.get(SessionId(session.id))!;
      const starts = agent.session.snapshotEvents().filter(event => event.type === 'tool/ptc-dispatch-start' && event.data.subCallId === question.callId);
      assert.equal(starts.length, 1);
      assert.ok(starts[0]?.type === 'tool/ptc-dispatch-start');
      assert.deepEqual({ rootCallId: starts[0].data.rootCallId, parentCallId: starts[0].data.parentCallId, name: starts[0].data.name },
        { rootCallId, parentCallId: invocation.id, name: 'ask_user_question' });
      await assert.rejects(bound.answer(deliveryDetails, { actorId: 'intruder' }), /REVIEWER_REQUIRED/u);
      assert.deepEqual(scene.effects, []);
      assert.equal(await bound.answer(deliveryDetails, { actorId: 'reviewer' }), true);
      assert.deepEqual(await invocation.result, { step: 'foreground', outcome: 'recorded' });
      await session.whenIdle();
      assert.deepEqual(scene.effects, [{ step: 'foreground', merchantId: 'merchant-a', invocationId: invocation.id }]);
      assert.deepEqual(scene.admitted, [origin]);
      assert.equal((await bound.state.read()).foreground, undefined);
      const events = agent.session.snapshotEvents();
      const child = nestedEnd(events, question.callId);
      assert.equal(child.data.isError, false);
      assert.deepEqual(JSON.parse(child.data.content.flatMap(block => block.type === 'text' ? [block.text] : []).join('')), deliveryDetails);
      assert.ok(child.seq < parentEnd(events, mode, invocation.id).seq);
      const beforeStale = agent.session.snapshotEvents().length;
      await assert.rejects(scene.calls[0]!.askHuman(prompts), /REACTIVE_ACTION_CONTROL_INACTIVE/u);
      assert.equal(agent.session.snapshotEvents().length, beforeStale, '保存已结束 call 不能制造新的子调用');

      scene.next('drain');
      const unawaited = firstValueFrom(session.foregroundQuestions$.pipe(filter(value => value.length === 1)));
      await session.submit({ requestId: 'unawaited-question', content: '允许能力返回而不等待人工答复', intent: 'question', delivery: 'followup' as const, scope: submittedScope }, { actorId: 'request-author' });
      const [unfinished] = await unawaited;
      assert.ok(unfinished);
      const draining = scene.invocations.at(-1)!;
      scene.drainReturn.resolve();
      assert.deepEqual(await draining.result, { step: 'drain', outcome: 'returned-without-answer' });
      await session.whenIdle();
      assert.deepEqual(await firstValueFrom(session.foregroundQuestions$), []);
      const drainedEvents = agent.session.snapshotEvents();
      const drained = nestedEnd(drainedEvents, unfinished.callId);
      assert.equal(drained.data.isError, true);
      const parent = parentEnd(drainedEvents, mode, draining.id);
      assert.ok(drained.seq < parent.seq, '未等待子问结算并退出后，父工具才发布原业务返回');
      if (parent.type === 'tool/result') assert.notEqual(parent.data.message.isError, true);
      else assert.equal(parent.data.isError, false);
      assert.equal(scene.effects.length, 1);

      for (const pressure of ['throw', 'sparse-content'] as const) {
        let failedQuestionId: string | undefined;
        const failedLogs: string[] = [];
        const stopLogFailure = app.native.on('tools/ptc-dispatch-log', async (dispatch, next) => {
          if (dispatch.name !== 'ask_user_question' || dispatch.subCallId !== failedQuestionId) return next();
          failedLogs.push(dispatch.subCallId);
          assert.equal(dispatch.isError, false, '日志策略失败前，底层问答已经取得真实答案');
          assert.deepEqual(JSON.parse(dispatch.content.flatMap(block => block.type === 'text' ? [block.text] : []).join('')), deliveryDetails);
          if (pressure === 'throw') throw new Error('TEST_HUMAN_LOG_POLICY_FAILURE');
          const sparse: ContentBlock[] = [];
          sparse.length = 1;
          return sparse;
        });
        try {
          scene.next('log-failure');
          const loggingQuestion = firstValueFrom(session.foregroundQuestions$.pipe(filter(value => value.length === 1)));
          await session.submit({ requestId: 'question-log-' + pressure, content: '先取得真实答复，再检验本次日志策略失败',
            intent: 'question', delivery: 'followup' as const, scope: submittedScope }, { actorId: 'request-author' });
          const [unlogged] = await loggingQuestion;
          assert.ok(unlogged);
          failedQuestionId = unlogged.callId;
          const failedInvocation = scene.invocations.at(-1)!;
          const rejected = assert.rejects(failedInvocation.result, error => {
            assert.ok(error instanceof Error);
            assert.equal(Reflect.get(error, 'code'), 'REACTIVE_HUMAN_DISPATCH_LOG_FAILED');
            return true;
          });
          assert.equal(await session.answerQuestion(unlogged.callId, deliveryDetails, { actorId: 'reviewer' }), true);
          await rejected;
          await session.whenIdle();
          assert.deepEqual(failedLogs, [unlogged.callId], '日志失败不重新派发已经结束的子问');
          assert.equal(scene.effects.length, 1, '真实答复后的日志策略失败仍阻止业务后段执行');
          assert.deepEqual(await firstValueFrom(session.foregroundQuestions$), []);
          const failedEvents = agent.session.snapshotEvents();
          const failedChild = nestedEnd(failedEvents, unlogged.callId);
          assert.equal(failedChild.data.isError, true);
          assert.equal(failedChild.data.error?.code, 'REACTIVE_HUMAN_DISPATCH_LOG_FAILED');
          assert.equal(failedChild.data.error?.reason, 'tool pipeline returned; dispatch log policy then failed');
          assert.equal(failedChild.data.meta, undefined);
          const loggedContent = failedChild.data.content.flatMap(block => block.type === 'text' ? [block.text] : []).join('');
          assert.equal(loggedContent.includes(JSON.stringify(deliveryDetails)), false);
          assert.equal(loggedContent.includes('delivery-mode'), false, '不能将未经日志策略许可的答案正文存入失败 end');
          assert.equal(failedEvents.filter(event => event.type === 'tool/ptc-dispatch-start'
            && event.data.parentCallId === failedInvocation.id && event.data.name === 'ask_user_question').length, 1);
          assert.equal(failedEvents.filter(event => event.type === 'tool/ptc-dispatch' && event.data.subCallId === unlogged.callId).length, 1);
          const failedParent = parentEnd(failedEvents, mode, failedInvocation.id);
          assert.ok(failedChild.seq < failedParent.seq);
          if (failedParent.type === 'tool/result') assert.equal(failedParent.data.message.isError, true);
          else assert.equal(failedParent.data.isError, true);
        } finally { stopLogFailure(); }
      }

      const id = session.id;
      await app.host.close(id); session.release();
      const cold = await app.host.open(id, scene.program);
      const beforeSdkRequests = scene.requests.length;
      assert.equal(app.native.agents.get(SessionId(id)), undefined);
      const sdk = cold.resolveAction('collectDetails', { merchantId: 'merchant-a' })({ step: 'foreground' }, { actorId: 'reviewer' });
      await assert.rejects(sdk.result, /REACTIVE_ACTION_HUMAN_INPUT_UNAVAILABLE/u);
      assert.equal(sdk.source.kind, 'participant');
      if (sdk.source.kind === 'participant') assert.equal(sdk.source.actorId, 'reviewer');
      assert.equal(app.native.agents.get(SessionId(id)), undefined, '独立 SDK 提问不能激活 Agent 或伪造原生执行');
      assert.equal(scene.requests.length, beforeSdkRequests);
      assert.equal(scene.effects.length, 1);
      assert.deepEqual(await firstValueFrom(cold.foregroundQuestions$), []);

      scene.mounted$.next(false);
      scene.next('undeclared');
      await cold.submit({ requestId: 'without-human-input', content: '当前没有人工参与声明', intent: 'question', delivery: 'followup' as const, scope: submittedScope }, { actorId: 'request-author' });
      await cold.whenIdle();
      const undeclared = scene.invocations.at(-1)!;
      assert.equal(undeclared.source.kind, 'tool');
      await assert.rejects(undeclared.result);
      assert.equal(scene.effects.length, 1, '缺 HumanInput 的实际工具调用不得绕过声明执行业务');
      assert.deepEqual(await firstValueFrom(cold.foregroundQuestions$), []);
      assert.deepEqual(app.errors, []);
      cold.release();
    } finally { scene.drainReturn.resolve(); await app.dispose(); }
  });

  test(`${mode} Action 限时提问先返回 pending，晚答不重跑副作用；取消等待 drain 后仍可执行下一次明确调用`, { timeout: 30_000 }, async () => {
    const scene = await scenario(mode);
    const { app } = scene;
    try {
      const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
      const rootCallId = scene.next('timed');
      const foreground = firstValueFrom(session.foregroundQuestions$.pipe(filter(value => value.length === 1)));
      await session.submit({ requestId: 'timed-merchant', content: '限时询问商家甲的交付偏好，未答不记录资料', intent: 'question', delivery: 'followup' as const, scope: submittedScope }, { actorId: 'request-author' });
      const [question] = await foreground;
      assert.ok(question);
      assert.equal(question.timed, true);
      const timed = scene.invocations[0]!;
      assert.equal(question.origin.rootCallId, rootCallId);
      assert.equal(question.origin.parentCallId, timed.id);
      const bound = await session.question(question.callId);
      assert.deepEqual((await bound.state.read()).foreground, question);
      assert.deepEqual(scene.effects, []);
      assert.deepEqual(await timed.result, { step: 'timed', outcome: 'pending', callId: question.callId });
      await session.whenIdle();
      assert.equal(scene.calls.length, 1);
      assert.deepEqual(scene.effects, []);
      const continued = await firstValueFrom(session.questions$);
      assert.equal(continued.active[0]?.state, 'continued');
      assert.deepEqual(continued.active[0]?.origin, question.origin);
      assert.deepEqual(await firstValueFrom(session.foregroundQuestions$), []);
      assert.equal((await bound.state.read()).active?.state, 'continued');
      assert.equal((await bound.state.read()).foreground, undefined);
      const agent = app.native.agents.get(SessionId(session.id))!;
      const timedEvents = agent.session.snapshotEvents();
      assert.ok(nestedEnd(timedEvents, question.callId).seq < parentEnd(timedEvents, mode, timed.id).seq);
      await app.host.close(session.id);
      const cold = await app.host.open(session.id, scene.program);
      const late = await cold.question(question.callId);
      assert.equal(app.native.agents.get(SessionId(session.id)), undefined, '读取持久问题对象不创建 Agent');
      assert.deepEqual((await late.state.read()).active?.origin, question.origin);
      await assert.rejects(late.attachWait()[Symbol.asyncIterator]().next(), /REACTIVE_HUMAN_FOREGROUND_UNAVAILABLE/u);
      assert.equal(await late.answer(deliveryDetails, { actorId: 'reviewer' }), true);
      cold.release();
      await session.whenIdle();
      assert.equal(scene.calls.length, 1, '晚答只续接原生输入，不重新执行已经返回的 Action body');
      assert.deepEqual(scene.effects, [], 'pending 没有交付资料可记录，晚答也不能重放旧 JavaScript continuation');
      const settled = await firstValueFrom(session.questions$);
      assert.deepEqual(settled.settled, [{ callId: question.callId, answers: deliveryDetails.answers, origin: question.origin }]);
      assert.deepEqual((await bound.state.read()).settled, settled.settled[0]);
      const resumedAgent = app.native.agents.get(SessionId(session.id))!;
      const replies = resumedAgent.session.snapshotEvents().filter(event => event.type === 'user/message' && event.data.source.kind === 'user-question-reply');
      assert.equal(replies.length, 1);
      assert.ok(replies[0]?.type === 'user/message');
      assert.deepEqual(replies[0].data.source, { kind: 'user-question-reply', callId: question.callId, outcome: 'answered' });
      assert.deepEqual(Object.keys(session.snapshot().requests), ['timed-merchant']);

      scene.next('cancel');
      const cancellingQuestion = firstValueFrom(session.foregroundQuestions$.pipe(filter(value => value.length === 1)));
      await session.submit({ requestId: 'cancel-merchant', content: '开始第二次明确问答后取消本轮', intent: 'question', delivery: 'followup' as const, scope: submittedScope }, { actorId: 'request-author' });
      const [cancelledQuestion] = await cancellingQuestion;
      assert.ok(cancelledQuestion);
      const cancelledHandle = await session.question(cancelledQuestion.callId);
      const cancelled = scene.invocations.at(-1)!;
      const rejection = assert.rejects(cancelled.result);
      await session.cancel();
      await rejection;
      await session.whenIdle();
      assert.equal(scene.calls.at(-1)!.signal.aborted, true);
      assert.deepEqual(scene.effects, []);
      assert.deepEqual(await firstValueFrom(session.foregroundQuestions$), []);
      assert.deepEqual(await cancelledHandle.state.read(), { active: undefined, foreground: undefined, settled: undefined },
        '未留持久记录的子问退出后只有缺失事实，不伪造完成或取消阶段');
      const cancelledEvents = resumedAgent.session.snapshotEvents();
      const child = nestedEnd(cancelledEvents, cancelledQuestion.callId);
      assert.equal(child.data.isError, true);
      assert.ok(child.seq < parentEnd(cancelledEvents, mode, cancelled.id).seq, '取消也必须先 drain 实际子调用再结束父调用');

      scene.next('foreground');
      const nextQuestion = firstValueFrom(session.foregroundQuestions$.pipe(filter(value => value.length === 1)));
      await session.submit({ requestId: 'next-merchant', content: '取消后重新收集并记录一次交付资料', intent: 'question', delivery: 'followup' as const, scope: submittedScope }, { actorId: 'request-author' });
      const [next] = await nextQuestion;
      assert.ok(next);
      const nextInvocation = scene.invocations.at(-1)!;
      assert.notEqual(next.callId, cancelledQuestion.callId);
      assert.equal(await session.answerQuestion(next.callId, deliveryDetails, { actorId: 'reviewer' }), true);
      assert.deepEqual(await nextInvocation.result, { step: 'foreground', outcome: 'recorded' });
      await session.whenIdle();
      assert.deepEqual(scene.effects, [{ step: 'foreground', merchantId: 'merchant-a', invocationId: nextInvocation.id }]);
      assert.equal(scene.calls.length, 3);
      assert.deepEqual(await firstValueFrom(session.foregroundQuestions$), []);
      assert.deepEqual(app.errors, []);
      session.release();
    } finally { scene.drainReturn.resolve(); await app.dispose(); }
  });
}
