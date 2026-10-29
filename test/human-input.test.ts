import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay, setImmediate as immediate } from 'node:timers/promises';
import { symbols } from '@deepseek-ai/cordis';
import { Fragment, createElement as h } from 'react';
import { BehaviorSubject, filter, firstValueFrom } from 'rxjs';
import { SessionId } from '@deepseek-ai/dsh-session';
import { WorkspaceId } from '@deepseek-ai/dsh-workspace';
import { createUserMessage, type GenerateOptions } from '@deepseek-ai/dsh-llm';
import { ConversationAccessRevokedError, createWorkSelector, defineApplication, defineEnvironment, z } from '@hanger-source/dsh-reactive';
import { defineAction, implementAction } from '@hanger-source/dsh-reactive';
import type { HumanAnswer, HumanQuestionOrigin, HumanResponderAdmission, HumanSnapshot } from '@hanger-source/dsh-reactive';
import { inputReferenceSchema } from '@hanger-source/dsh-reactive/kernel';
import { Context, Expose, HumanInput, Offer, useBeforeStep } from '@hanger-source/dsh-reactive/program';
import { defineProgram } from '@hanger-source/dsh-reactive';
import { useAction, useObservable } from '@hanger-source/dsh-reactive/program';
import { defineSurface, view } from '@hanger-source/dsh-reactive';
import { boot, text, tool } from './native-support.ts';

const questions = [
  { id: 'merchant-permission', question: '是否允许处理商家甲？', options: [{ label: '允许' }, { label: '拒绝' }] },
  { id: 'receipt-mode', question: '如何交付商家甲的回执？', options: [{ label: '站内' }, { label: '邮件' }] },
];
const answer: HumanAnswer = { answers: [
  { id: 'merchant-permission', selected: ['允许'] }, { id: 'receipt-mode', selected: ['站内'] },
] };
function admitResponder(admission: HumanResponderAdmission, actors: readonly string[] = ['operator-test']) {
  if (!actors.includes(admission.actorId)) throw new Error('MERCHANT_RESPONDER_DENIED');
  if (admission.question.questions[0]?.id !== 'merchant-permission') throw new Error('MERCHANT_QUESTION_DENIED');
  if (admission.answer && admission.answer.answers[0]?.selected[0] !== '允许') throw new Error('MERCHANT_ANSWER_DENIED');
}

test('提问后复用callId的普通工具不改写旧问题来源，再次同ID提问明确拒绝歧义来源', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-human-call-occurrence-'));
  let modelCalls = 0, executions = 0;
  const admitted: HumanQuestionOrigin[] = [];
  const actionDefinition = defineAction({ name: 'ordinary_business', description: '处理另一项独立业务', scope: z.null(),
    input: z.object({}).strict(), output: z.boolean() });
  const implementation = implementAction(actionDefinition, { execute() { executions++; return true; } });
  const definition = defineApplication({ name: 'human-call-occurrence', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '向人询问当前工作，普通业务保持独立',
    program: implementProgram(defineProgram({ input: z.object({ merchantId: z.string(), workId: z.string() }),
      surface: defineSurface({ views: {}, actions: {} }) }), { Component() {
      const action = useAction(implementation, () => null, []);
      return h(Fragment, null, HumanInput({ admitResponder(admission) {
        admitResponder(admission);
        if (admission.answer) admitted.push(structuredClone(admission.question.origin));
      } }), h(Offer<typeof actionDefinition>, { name: 'ordinary_business', action }));
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () {
    modelCalls++;
    if (modelCalls === 1 || modelCalls === 5) yield* tool('ask_user_question', 'reused-question-id', { questions, timeout: 30 });
    else if (modelCalls === 3 || modelCalls === 7) yield* tool('ordinary_business', 'reused-question-id', {});
    else yield* text('本轮正式结束');
  });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const foreground = firstValueFrom(session.foregroundQuestions$.pipe(filter(value => value.length === 1)));
    await session.submit({ requestId: 'original-question', content: '询问第一项工作的授权', intent: 'question', delivery: 'followup',
      scope: { merchantId: 'merchant-a', workId: 'original' } });
    const [pending] = await foreground;
    assert.ok(pending);
    const native = app.native.sessions.get(SessionId(session.id));
    assert.ok(native);
    const originalStart = native.snapshotEvents().find(event => event.type === 'tool/call' && event.data.name === 'ask_user_question');
    assert.ok(originalStart);
    assert.equal(pending.origin.callSequence, originalStart.seq);
    assert.equal(await session.answerQuestion(pending.callId, answer), true);
    await session.whenIdle();
    const original = (await session.human.read()).questions.settled[0];
    assert.ok(original);
    assert.deepEqual(original.origin, pending.origin);
    await session.submit({ requestId: 'ordinary-work', content: '处理第二项独立业务', intent: 'question', delivery: 'followup',
      scope: { merchantId: 'merchant-a', workId: 'ordinary' } });
    await session.whenIdle();
    assert.equal(executions, 1);
    const ordinaryStart = native.snapshotEvents().find(event => event.type === 'tool/call' && event.data.name === 'ordinary_business');
    assert.ok(ordinaryStart);
    assert.notEqual(ordinaryStart.seq, originalStart.seq);
    const preserved = (await session.human.read()).questions.settled[0];
    assert.deepEqual(preserved, original, '普通同ID工具不能成为历史问题的新来源');
    assert.deepEqual(preserved!.origin.inputs.map(input => [input.requestId, input.scope]),
      [['original-question', { merchantId: 'merchant-a', workId: 'original' }]]);
    await session.submit({ requestId: 'ambiguous-question', content: '为第三项工作再次询问', intent: 'question', delivery: 'followup',
      scope: { merchantId: 'merchant-a', workId: 'ambiguous' } });
    await session.whenIdle();
    assert.equal(native.snapshotEvents().filter(event => event.type === 'tool/call' && event.data.name === 'ask_user_question').length, 2,
      '歧义来自两次真实原生提问发生');
    await assert.rejects(async () => session.human.read(), /REACTIVE_HUMAN_QUESTION_ORIGIN_AMBIGUOUS/u);
    await assert.rejects(session.answerQuestion(pending.callId, answer), /REACTIVE_HUMAN_QUESTION_ORIGIN_AMBIGUOUS/u);
    assert.deepEqual(admitted, [original.origin], '歧义问题不能以新work来源进入响应政策');
    assert.equal(executions, 1);
    await session.submit({ requestId: 'ordinary-after-ambiguity', content: '问题来源不明确时继续处理独立业务',
      intent: 'question', delivery: 'followup', scope: { merchantId: 'merchant-a', workId: 'after-ambiguity' } });
    await session.whenIdle();
    assert.equal(executions, 2, 'human读侧错误不撤销可执行的独立Action');
    assert.equal(modelCalls, 8);
    assert.equal(session.snapshot().requests['ordinary-after-ambiguity']?.reply, '本轮正式结束');
    await assert.rejects(async () => session.human.read(), /REACTIVE_HUMAN_QUESTION_ORIGIN_AMBIGUOUS/u,
      '独立业务完成后仍保留原human来源错误，不恢复旧ready');
    session.release();
  } finally { await app.dispose(); }
});

for (const mode of ['native', 'ptc'] as const) {
  test(`${mode} 正式问答前台响应审核完整问题、答案及响应者，PTC 不伪造 open 投影`, async () => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-human-foreground-'));
    const requests: GenerateOptions[] = [];
    const selectedMerchant$ = new BehaviorSubject('merchant-a');
    const merchantScope = z.object({ merchantId: z.string() }).strict();
    const surface = defineSurface({ views: { selectedMerchant: view(z.string()) }, actions: {} });
    const admittedOrigins: HumanQuestionOrigin[] = [];
    const policy = (admission: HumanResponderAdmission) => {
      admitResponder(admission, ['operator-test', 'merchant-b-operator']);
      const merchantId = admission.actorId === 'operator-test' ? 'merchant-a' : 'merchant-b';
      if (!admission.question.origin.inputs.length || admission.question.origin.inputs.some(input =>
        merchantScope.parse(input.scope).merchantId !== merchantId)) throw new Error('MERCHANT_ORIGIN_DENIED');
      if (admission.answer) admittedOrigins.push(structuredClone(admission.question.origin));
    };
    const definition = defineApplication({ name: 'human-foreground', admit: async () => {}, environments: [defineEnvironment({
      id: 'work', instructions: '就商家甲的处理权限和回执方式向人询问',
      program: implementProgram(defineProgram({ input: merchantScope.nullable(), surface }), { Component() {
        const selected = useObservable(() => selectedMerchant$, []);
        return h(Fragment, null, selected.status === 'ready' ? Context({ name: 'selected-merchant', value: { merchantId: selected.value } }) : null,
          HumanInput({ admitResponder: policy }), Expose({ surface, value: { views: { selectedMerchant: selectedMerchant$ } } }));
      } }),
    })] });
    const app = await boot(join(root, 'home'), definition, async function* (options) {
      requests.push(options);
      if (requests.length === 1) {
        if (mode === 'native') yield* tool('ask_user_question', 'human-foreground', { questions, timeout: 30 });
        else yield* tool('run_code', 'human-foreground-root', { description: '询问商家甲的处理许可',
          code: `return await tools.ask_user_question(${JSON.stringify({ questions, timeout: 30 })});` });
      } else yield* text('已收到商家甲的完整答复');
    }, mode);
    try {
      const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
      const foreground = firstValueFrom(session.foregroundQuestions$.pipe(filter(value => value.length === 1)));
      const acceptedInput = firstValueFrom(session.rawEvents$.pipe(filter(event => event.kind === 'input.accepted'
        && event.inputs.some(input => input.requestId === 'merchant-question'))));
      await session.submit({ requestId: 'merchant-question', content: '先明确商家甲的权限与回执方式', intent: 'question', delivery: 'followup' as const, scope: { merchantId: 'merchant-a' } });
      const [pending] = await foreground;
      assert.ok(pending); assert.equal(pending.timed, true);
      assert.deepEqual(pending.questions, questions);
      const initialAccepted = await acceptedInput;
      assert.ok(initialAccepted.kind === 'input.accepted');
      const origin: HumanQuestionOrigin = { sessionId: session.id, callId: pending.callId, callSequence: pending.origin.callSequence,
        rootCallId: mode === 'native' ? 'human-foreground' : 'human-foreground-root',
        parentCallId: mode === 'native' ? null : 'human-foreground-root', name: 'ask_user_question',
        inputs: [{ sourceSessionId: session.id, messageId: initialAccepted.messageId, requestId: 'merchant-question', source: 'user',
          intent: 'question', scope: { merchantId: 'merchant-a' }, actorId: 'operator-test', participant: 'human' }] };
      assert.deepEqual(pending.origin, origin, '前台问题关联实际原生调用或 PTC 子调用及其完整原输入');
      const exposedInput = pending.origin.inputs[0]!;
      assert.ok(exposedInput.scope !== null && typeof exposedInput.scope === 'object' && !Array.isArray(exposedInput.scope));
      assert.equal(Reflect.set(exposedInput.scope, 'merchantId', 'merchant-b'), false);
      assert.equal(Reflect.set(exposedInput, 'requestId', 'foreign-request'), false);
      selectedMerchant$.next('merchant-b');
      assert.equal(await session.readView('selectedMerchant'), 'merchant-b');
      const state = await firstValueFrom(session.questions$);
      if (mode === 'native') {
        assert.deepEqual(state.active.map(value => [value.callId, value.state]), [[pending.callId, 'open']]);
        assert.deepEqual(state.active[0]?.origin, origin);
      }
      else assert.deepEqual(state, { active: [], settled: [] });
      await assert.rejects(session.answerQuestion(pending.callId, answer, { actorId: 'intruder' }), /MERCHANT_RESPONDER_DENIED/u);
      await assert.rejects(session.answerQuestion(pending.callId, answer, { actorId: 'merchant-b-operator' }), /MERCHANT_ORIGIN_DENIED/u);
      await assert.rejects(session.answerQuestion(pending.callId, { answers: [answer.answers[0]!] }), /each question exactly once/u);
      await assert.rejects(session.answerQuestion(pending.callId, { answers: [
        { id: 'merchant-permission', selected: ['拒绝'] }, answer.answers[1]!,
      ] }), /MERCHANT_ANSWER_DENIED/u);
      assert.equal(requests.length, 1);
      assert.equal(await session.answerQuestion(pending.callId, answer), true);
      assert.deepEqual(admittedOrigins, [origin], '当前选择与消费者写入尝试均不得改写策略所核验的原问题');
      await session.whenIdle();
      assert.equal(requests.length, 2);
      const agent = app.native.agents.get(SessionId(session.id))!;
      const events = agent.session.snapshotEvents();
      assert.equal(events.filter(event => event.type === 'user/message' && event.data.source.kind === 'user-question-reply').length, 0);
      if (mode === 'native') {
        const result = events.find(event => event.type === 'tool/result' && event.data.message.toolCallId === pending.callId);
        assert.ok(result?.type === 'tool/result');
        assert.deepEqual(JSON.parse(result.data.message.content.map(block => block.type === 'text' ? block.text : '').join('')), answer);
        assert.deepEqual((await firstValueFrom(session.questions$)).settled, [{ callId: pending.callId, answers: answer.answers, origin }]);
      } else {
        assert.equal(events.filter(event => event.type === 'tool/call' && event.data.name === 'ask_user_question').length, 0);
        const dispatch = events.find(event => event.type === 'tool/ptc-dispatch' && event.data.subCallId === pending.callId);
        assert.ok(dispatch?.type === 'tool/ptc-dispatch');
        assert.deepEqual(JSON.parse(dispatch.data.content.map(block => block.type === 'text' ? block.text : '').join('')), answer);
        assert.deepEqual(await firstValueFrom(session.questions$), { active: [], settled: [] });
      }
      await assert.rejects(session.answerQuestion(pending.callId, answer), /REACTIVE_HUMAN_QUESTION_NOT_OPEN/u);
      assert.deepEqual(Object.keys(session.snapshot().requests), ['merchant-question']);
      assert.equal(app.errors.length, 0);
      const unassociated = createUserMessage({ source: { kind: 'user' }, content: [{ type: 'text', text: '未经框架受理的原生输入' }] });
      const accepted = firstValueFrom(session.events$.pipe(filter(event => event.kind === 'input.accepted' && event.messageId === unassociated.id)));
      agent.send(unassociated, 'next-turn', true);
      const occurrence = await accepted;
      assert.ok(occurrence.kind === 'input.accepted');
      assert.deepEqual(occurrence.inputs, [{ sourceSessionId: session.id, messageId: unassociated.id, requestId: null, source: 'user',
        intent: 'question', scope: null, actorId: null, participant: 'native' }], '宿主默认 actor 不能冒认未关联原生输入');
      await session.whenIdle();
      assert.equal(requests.length, 2, '缺少真实 actor 的原生输入在 prepare 拒绝，不能调用模型');
      assert.equal(app.errors.length, 1); assert.ok(app.errors[0] instanceof Error);
      assert.equal(app.errors[0].message, 'REACTIVE_INPUT_ACTOR_REQUIRED');
      assert.equal(agent.session.snapshotEvents().some(event => event.type === 'user/message' && event.data.id === unassociated.id), false);
      assert.deepEqual(Object.keys(session.snapshot().requests), ['merchant-question']);
      session.release();
    } finally { await app.dispose(); }
  });

  test(`${mode} 同政策贡献共享原生 claim，最后贡献退出释放等待，continued 冷读和晚答沿同一问题`, async () => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-human-continued-'));
    const mounted$ = new BehaviorSubject(true), secondMounted$ = new BehaviorSubject(true), revision$ = new BehaviorSubject(0);
    const requests: GenerateOptions[] = [];
    let holdReply = false;
    let stopHuman: (() => void) | undefined;
    const proposedReply = Promise.withResolvers<void>(), releaseReply = Promise.withResolvers<void>();
    const merchantScope = z.object({ merchantId: z.string() }).strict();
    const inspect = defineAction({ name: 'inspect_merchant', description: '核验当前明确商家',
      scope: merchantScope, input: z.object({}).strict(), output: z.string() });
    const inspectImplementation = implementAction(inspect, { execute: (_input, call) => call.scope.merchantId });
    const surface = defineSurface({ views: { inputs: view(inputReferenceSchema.array().readonly()) }, actions: {} });
    const responders: string[] = [];
    const admittedOrigins: HumanQuestionOrigin[] = [];
    const policy = (admission: HumanResponderAdmission) => {
      admitResponder(admission, ['alice', 'bob', 'charlie']);
      const merchantId = admission.actorId === 'charlie' ? 'merchant-b' : 'merchant-a';
      if (!admission.question.origin.inputs.length || admission.question.origin.inputs.some(input =>
        merchantScope.parse(input.scope).merchantId !== merchantId)) throw new Error('MERCHANT_ORIGIN_DENIED');
      if (admission.answer) {
        responders.push(admission.actorId);
        admittedOrigins.push(structuredClone(admission.question.origin));
      }
    };
    function Merchant({ merchantId }: { merchantId: string }) {
      const action = useAction(inspectImplementation, () => ({ merchantId }), [merchantId]);
      return h(Fragment, null, Context({ name: 'merchant', value: { merchantId } }), Offer({ name: 'inspect_merchant', action }));
    }
    const definition = defineApplication({ name: 'human-continued', admit: async () => {}, environments: [defineEnvironment({
      id: 'work', instructions: '超时不算许可，可以先继续独立工作',
      program: implementProgram(defineProgram({ input: merchantScope.transform(value => ({ ...value, decoded: true as const })), surface }), { Component({ conversation }) {
        const mounted = useObservable(() => mounted$, []), secondMounted = useObservable(() => secondMounted$, []);
        const revision = useObservable(() => revision$, []);
        const inputs = useObservable(() => conversation.inputs$, [conversation]);
        useBeforeStep(async () => {
          if (holdReply) { proposedReply.resolve(); await releaseReply.promise; }
        }, []);
        const selected = inputs.status === 'ready' ? inputs.value.at(-1) : undefined;
        return h(Fragment, null, Context({ name: 'revision', value: revision.status === 'ready' ? revision.value : null }),
          selected ? h(Merchant, { merchantId: selected.scope.merchantId }) : null,
          mounted.status === 'ready' && mounted.value ? HumanInput({ admitResponder: policy }) : null,
          secondMounted.status === 'ready' && secondMounted.value ? HumanInput({ admitResponder: policy }) : null,
          Expose({ surface, value: { views: { inputs: conversation.inputs$ } } }));
      } }),
    })] });
    const app = await boot(join(root, 'home'), definition, async function* (options) {
      requests.push(options);
      if (requests.length === 1) {
        if (mode === 'native') yield* tool('ask_user_question', 'human-continued', { questions, timeout: 1 });
        else yield* tool('run_code', 'human-continued-root', { description: '限时询问商家甲',
          code: `return await tools.ask_user_question(${JSON.stringify({ questions, timeout: 1 })});` });
      } else yield* text(requests.length === 2 ? '先进行不依赖该许可的独立核验'
        : requests.length === 3 ? '已核查商家乙当前现场，商家甲问题仍待原许可' : '已按原问题收到完整晚答');
    }, mode);
    try {
      const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
      const foreground = firstValueFrom(session.foregroundQuestions$.pipe(filter(value => value.length === 1)));
      const acceptedInput = firstValueFrom(session.rawEvents$.pipe(filter(event => event.kind === 'input.accepted'
        && event.inputs.some(input => input.requestId === 'merchant-question'))));
      await session.submit({ requestId: 'merchant-question', content: '核实商家甲许可，等候时继续独立工作',
        intent: 'question', delivery: 'followup' as const, scope: { merchantId: 'merchant-a' } }, { actorId: 'alice' });
      const [pending] = await foreground;
      assert.ok(pending);
      const initialAccepted = await acceptedInput;
      assert.ok(initialAccepted.kind === 'input.accepted');
      const origin: HumanQuestionOrigin = { sessionId: session.id, callId: pending.callId, callSequence: pending.origin.callSequence,
        rootCallId: mode === 'native' ? 'human-continued' : 'human-continued-root',
        parentCallId: mode === 'native' ? null : 'human-continued-root', name: 'ask_user_question',
        inputs: [{ sourceSessionId: session.id, messageId: initialAccepted.messageId, requestId: 'merchant-question', source: 'user',
          intent: 'question', scope: { merchantId: 'merchant-a' }, actorId: 'alice', participant: 'human' }] };
      assert.deepEqual(pending.origin, origin);
      const claimSignal = new AbortController();
      const claim = session.attachWait(pending.callId, { actorId: 'alice', signal: claimSignal.signal })[Symbol.asyncIterator]();
      const frame = await claim.next();
      assert.ok(!frame.done); assert.ok(frame.value.remainingMs > 0 && frame.value.remainingMs <= 1_000);
      let claimEnded = false;
      const ended = claim.next().then(value => { claimEnded = true; return value; });
      const disconnected = app.host.conversation(session.id, definition.environments[0].program.definition);
      const disconnectedClaim = disconnected.attachWait(pending.callId, { actorId: 'alice' })[Symbol.asyncIterator]();
      assert.equal((await disconnectedClaim.next()).done, false);
      const disconnectedEnded = disconnectedClaim.next();
      disconnected.release();
      assert.equal((await disconnectedEnded).done, true);
      assert.equal(claimEnded, false, '另一个观察者断开只释放自己的 claim');
      await delay(frame.value.remainingMs + 30);
      revision$.next(1);
      await assert.rejects(session.answerQuestion(pending.callId, answer, { actorId: 'intruder' }), /MERCHANT_RESPONDER_DENIED/u);
      assert.equal(claimEnded, false, '无关声明帧不得释放仍获当前参与策略允许的原生 claim');
      assert.equal(requests.length, 1);
      mounted$.next(false);
      await assert.rejects(session.answerQuestion(pending.callId, answer, { actorId: 'intruder' }), /MERCHANT_RESPONDER_DENIED/u);
      await immediate();
      assert.equal(claimEnded, false, '先收集的同政策贡献退出后，剩余贡献仍维持已领取的原生 claim');
      assert.equal(requests.length, 1, '贡献数量变化不能释放超时等待并继续模型');
      secondMounted$.next(false);
      await assert.rejects(session.answerQuestion(pending.callId, answer), /REACTIVE_HUMAN_INPUT_NOT_DECLARED/u);
      assert.equal((await ended).done, true);
      const continued = await firstValueFrom(session.questions$.pipe(filter(state => state.active.some(value => value.callId === pending.callId && value.state === 'continued'))));
      assert.equal(continued.active[0]?.callId, pending.callId);
      assert.deepEqual(continued.active[0]?.origin, origin);
      await session.whenIdle();
      assert.equal(requests.length, 2);
      assert.deepEqual(await firstValueFrom(session.foregroundQuestions$), []);
      const originalInputs = await session.readView('inputs');
      assert.equal(originalInputs.length, 1);
      assert.deepEqual(originalInputs.map(input => ({ requestId: input.requestId, actorId: input.actorId, scope: input.scope })),
        [{ requestId: 'merchant-question', actorId: 'alice', scope: { merchantId: 'merchant-a', decoded: true } }]);
      await session.submit({ requestId: 'merchant-b-question', content: '核查商家乙当前现场，不回答商家甲原问题',
        intent: 'question', delivery: 'followup' as const, scope: { merchantId: 'merchant-b' } }, { actorId: 'charlie' });
      await session.whenIdle();
      assert.equal(requests.length, 3);
      const currentInputs = await session.readView('inputs');
      assert.deepEqual(currentInputs.map(input => ({ requestId: input.requestId, actorId: input.actorId, scope: input.scope })),
        [{ requestId: 'merchant-b-question', actorId: 'charlie', scope: { merchantId: 'merchant-b', decoded: true } }]);
      const id = session.id;
      await app.host.close(id); session.release();
      const cold = await app.host.open(id, definition.environments[0].program.definition);
      assert.equal(app.native.agents.get(SessionId(id)), undefined);
      const coldState = await firstValueFrom(cold.questions$);
      assert.equal(coldState.active[0]?.state, 'continued');
      assert.deepEqual(coldState.active[0]?.origin, origin, '冷读从原问题的调用恢复 raw 输入，不使用后来商家乙的当前输入或 Program 解码输出');
      assert.deepEqual(await cold.readView('inputs'), currentInputs);
      await assert.rejects(cold.answerQuestion(pending.callId, answer), /REACTIVE_HUMAN_INPUT_NOT_DECLARED/u);
      assert.equal(app.native.agents.get(SessionId(id)), undefined, '未获当前参与策略允许的晚答不得唤醒冷 Agent');
      mounted$.next(true);
      await assert.rejects(cold.answerQuestion(pending.callId, answer, { actorId: 'intruder' }), /MERCHANT_RESPONDER_DENIED/u);
      await assert.rejects(cold.answerQuestion(pending.callId, answer, { actorId: 'charlie' }), /MERCHANT_ORIGIN_DENIED/u);
      assert.equal(app.native.agents.get(SessionId(id)), undefined);
      const humanFrames: HumanSnapshot[] = [];
      const humanSubscription = cold.human.values$.subscribe(value => humanFrames.push(value));
      stopHuman = () => humanSubscription.unsubscribe();
      const fixedQuestion = await cold.question(pending.callId);
      holdReply = true;
      assert.equal(await cold.answerQuestion(pending.callId, answer, { actorId: 'bob' }), true);
      await proposedReply.promise;
      const proposal = await cold.human.read();
      const [delivery] = proposal.replies;
      assert.ok(delivery);
      assert.equal(delivery.state, 'proposed');
      assert.equal(delivery.questionCallId, pending.callId);
      assert.deepEqual(delivery.origin, origin);
      assert.equal(proposal.questions.active[0]?.state, 'continued');
      assert.deepEqual(proposal.questions.settled, []);
      assert.ok(humanFrames.some(frame => frame.replies.some(reply => reply.messageId === delivery.messageId && reply.state === 'queued')),
        '原生排队已公开，不能继续把 continued 当成未回答');
      assert.deepEqual((await fixedQuestion.state.read()).replies, proposal.replies);
      const actual = app.native.agents.get(SessionId(id))!.session.snapshotEvents().flatMap(event => event.type === 'agent/inbox/spliced'
        ? event.data.inserted.filter(message => message.source.kind === 'user-question-reply') : []);
      assert.deepEqual(actual.map(message => message.id), [delivery.messageId], '公开交付沿实际原生消息身份');
      const selector = createWorkSelector({ identify: selected => merchantScope.parse(selected.scope).merchantId });
      const selected = selector.select('merchant-a', [{ identity: cold.identity, progress: await cold.progress.read(), human: proposal }]).sessions[0]!;
      assert.deepEqual(selected.questions.replies.map(reply => reply.matchedInputs.map(input => input.requestId)), [['merchant-question']]);
      assert.ok(selected.conditions.some(condition => condition.kind === 'question.reply-pending' && condition.value.messageId === delivery.messageId));
      holdReply = false; releaseReply.resolve();
      const settled = await firstValueFrom(cold.questions$.pipe(filter(state => state.settled.some(value => value.callId === pending.callId))));
      await cold.whenIdle();
      assert.deepEqual((await cold.human.read()).replies, [], 'user/message 结算后不遗留 proposal');
      humanSubscription.unsubscribe();
      assert.equal(requests.length, 4);
      assert.deepEqual(settled.active, []);
      assert.deepEqual(settled.settled, [{ callId: pending.callId, answers: answer.answers, origin }]);
      assert.deepEqual(await cold.readView('inputs'), originalInputs, 'Bob 回答原问题，当前现场仍沿原 call 关联 Alice 的商家输入');
      assert.deepEqual(responders, ['bob', 'bob'], '冷预审与 live 准入都核验真实响应者，不产生 Bob 的新业务输入');
      assert.deepEqual(admittedOrigins, [origin, origin], '冷预审与 live 准入沿同一原问题核验原始商家、请求和提出者');
      const snapshots = requests[3]!.messages.flatMap(message => {
        const source = z.object({ kind: z.literal('runtime-context'), form: z.literal('snapshot'),
          sections: z.array(z.object({ name: z.string(), text: z.string() })) }).safeParse(message.source);
        return source.success ? [source.data.sections] : [];
      });
      assert.deepEqual(snapshots.at(-1)?.filter(section => section.name === 'merchant').map(section => JSON.parse(section.text)),
        [{ merchantId: 'merchant-a' }], '迟答步骤的当前 Context 快照仍有原商家，不借历史快照证明');
      assert.equal(requests[3]!.tools?.some(tool => tool.name === 'inspect_merchant'), mode === 'native');
      const agent = app.native.agents.get(SessionId(id))!;
      assert.ok(app.native.tools.get('inspect_merchant', agent), '原商家能力仍注册在当前 Agent，PTC 也使用同一真实能力');
      const events = agent.session.snapshotEvents();
      const replies = events.filter(event => event.type === 'user/message' && event.data.source.kind === 'user-question-reply');
      assert.equal(replies.length, 1);
      assert.ok(replies[0]?.type === 'user/message');
      assert.deepEqual(replies[0].data.source, { kind: 'user-question-reply', callId: pending.callId, outcome: 'answered' });
      if (mode === 'ptc') assert.equal(events.filter(event => event.type === 'tool/call' && event.data.name === 'ask_user_question').length, 0);
      assert.deepEqual(Object.keys(cold.snapshot().requests), ['merchant-question', 'merchant-b-question']);
      assert.equal(cold.snapshot().goal, null);
      assert.deepEqual(app.errors, []);
      claimSignal.abort(); cold.release();
    } finally { stopHuman?.(); releaseReply.resolve(); await app.dispose(); }
  });
}

test('异步响应准入随取消或政策替换退出，迟到策略不能借新授权回答或claim，受理沿原答案快照', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-human-admission-'));
  const mounted$ = new BehaviorSubject(true);
  const gates = [{ entered: Promise.withResolvers<void>(), release: Promise.withResolvers<void>(), finished: Promise.withResolvers<void>() }];
  let gate: typeof gates[number] | undefined = gates[0];
  let calls = 0;
  const policy = async (admission: HumanResponderAdmission) => {
    admitResponder(admission);
    const held = gate; gate = undefined;
    if (held) { held.entered.resolve(); await held.release.promise; held.finished.resolve(); }
  };
  const policy$ = new BehaviorSubject<(admission: HumanResponderAdmission) => void | Promise<void>>(policy);
  let replacementAttempts = 0;
  const replacementPolicy = (admission: HumanResponderAdmission) => {
    replacementAttempts++;
    admitResponder(admission);
  };
  const definition = defineApplication({ name: 'human-admission', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '取得当前正式参与许可',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component() {
      const mounted = useObservable(() => mounted$, []);
      const currentPolicy = useObservable(() => policy$, []);
      return mounted.status === 'ready' && mounted.value && currentPolicy.status === 'ready'
        ? HumanInput({ admitResponder: currentPolicy.value }) : null;
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () {
    calls++;
    if (calls === 1) yield* tool('ask_user_question', 'human-async-admission', { questions, timeout: 30 });
    else yield* text('当前策略已经审核并接受完整答复');
  });
  const nativeQuestions = Reflect.get(app.native.userQuestions, symbols.original) as typeof app.native.userQuestions;
  const attachWait = nativeQuestions.attachWait;
  let attached = 0;
  nativeQuestions.attachWait = function (...args) { attached++; return attachWait.apply(this, args); };
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const foreground = firstValueFrom(session.foregroundQuestions$.pipe(filter(value => value.length === 1)));
    await session.submit({ requestId: 'merchant-question', content: '请求商家甲的处理许可', intent: 'question', delivery: 'followup' as const, scope: null });
    const [pending] = await foreground;
    assert.ok(pending);
    const answering = session.answerQuestion(pending.callId, answer);
    let rejectedBeforeRelease = false;
    const rejected = assert.rejects(answering, /REACTIVE_HUMAN_INPUT_CHANGED/u).then(() => { rejectedBeforeRelease = true; });
    await gates[0]!.entered.promise;
    mounted$.next(false);
    await assert.rejects(session.answerQuestion(pending.callId, answer), /REACTIVE_HUMAN_INPUT_NOT_DECLARED/u);
    await immediate();
    assert.equal(rejectedBeforeRelease, true, '声明撤下不等待不配合signal的旧策略结束');
    await rejected;
    gates[0]!.release.resolve(); await gates[0]!.finished.promise; await immediate();
    assert.equal(calls, 1);
    assert.equal((await firstValueFrom(session.questions$)).active[0]?.state, 'open');
    assert.equal((await firstValueFrom(session.foregroundQuestions$))[0]?.callId, pending.callId);
    mounted$.next(true);

    for (const entry of ['answer', 'claim'] as const) {
      const held = { entered: Promise.withResolvers<void>(), release: Promise.withResolvers<void>(), finished: Promise.withResolvers<void>() };
      gates.push(held); gate = held;
      policy$.next(policy);
      const waiting = entry === 'answer' ? session.answerQuestion(pending.callId, answer)
        : session.attachWait(pending.callId)[Symbol.asyncIterator]().next();
      let rejectedBeforeRelease = false;
      const rejected = assert.rejects(waiting, /REACTIVE_HUMAN_INPUT_CHANGED/u).then(() => { rejectedBeforeRelease = true; });
      await held.entered.promise;
      policy$.next(replacementPolicy);
      const attempts = replacementAttempts;
      await assert.rejects(session.answerQuestion(pending.callId, answer, { actorId: 'intruder' }), /MERCHANT_RESPONDER_DENIED/u);
      await immediate();
      assert.equal(replacementAttempts, attempts + 1, '替换后的声明已正式使用新的响应政策');
      assert.equal(rejectedBeforeRelease, true, '新政策仍允许正式响应，也必须立即结束等待旧政策的请求');
      await rejected;
      held.release.resolve(); await held.finished.promise; await immediate();
      assert.equal(attached, 0, '旧政策的迟到allow不能借新声明领取原生claim');
      assert.equal(calls, 1, '旧政策的迟到allow不能借新声明提交答案或继续模型');
      assert.equal((await firstValueFrom(session.questions$)).active[0]?.state, 'open');
      assert.equal((await firstValueFrom(session.foregroundQuestions$))[0]?.callId, pending.callId);
    }
    policy$.next(policy);

    for (const entry of ['answer', 'claim'] as const) for (const ending of ['caller', 'sdk'] as const) {
      const held = { entered: Promise.withResolvers<void>(), release: Promise.withResolvers<void>(), finished: Promise.withResolvers<void>() };
      gates.push(held); gate = held;
      const observer = app.host.conversation(session.id, definition.environments[0].program.definition);
      const cancellation = new AbortController(), reason = new Error(`CANCEL_HUMAN_${entry.toUpperCase()}_ADMISSION`);
      let outcome: unknown;
      const waiting = (entry === 'answer'
        ? observer.answerQuestion(pending.callId, answer, { signal: cancellation.signal })
        : observer.attachWait(pending.callId, { signal: cancellation.signal })[Symbol.asyncIterator]().next())
        .then(value => { outcome = value; }, error => { outcome = error; });
      try {
        await held.entered.promise;
        if (ending === 'caller') cancellation.abort(reason); else observer.release();
        await immediate();
        if (ending === 'caller') assert.equal(outcome, reason, '响应准入不配合signal时仍及时交回原取消原因');
        else { assert.ok(outcome instanceof Error); assert.match(outcome.message, /REACTIVE_SCOPE_DISPOSED/u); }
        held.release.resolve(); await held.finished.promise; await waiting; await immediate();
        assert.equal(attached, 0, '迟到准入不能进入原生claim');
        assert.equal(calls, 1, '迟到准入不能提交答案或继续模型');
        assert.equal((await firstValueFrom(session.questions$)).active[0]?.state, 'open');
        assert.equal((await firstValueFrom(session.foregroundQuestions$))[0]?.callId, pending.callId);
      } finally { held.release.resolve(); observer.release(); await waiting; }
    }

    const held = { entered: Promise.withResolvers<void>(), release: Promise.withResolvers<void>(), finished: Promise.withResolvers<void>() };
    gates.push(held); gate = held;
    const changing = structuredClone(answer);
    const accepted = session.answerQuestion(pending.callId, changing);
    await held.entered.promise;
    changing.answers[0]!.selected[0] = '拒绝';
    changing.answers[1]!.id = 'merchant-permission';
    held.release.resolve();
    assert.equal(await accepted, true);
    await session.whenIdle();
    assert.equal(calls, 2);
    assert.deepEqual((await firstValueFrom(session.questions$)).settled, [{ callId: pending.callId, answers: answer.answers, origin: pending.origin }]);
    assert.deepEqual(app.errors, []);
    session.release();
  } finally {
    gates.forEach(held => held.release.resolve());
    nativeQuestions.attachWait = attachWait;
    await app.dispose();
  }
});

test('正式成员撤销结束不配合取消的响应准入，迟到allow不答题也不领取前台等待', { timeout: 15_000 }, async () => {
  for (const entry of ['answer', 'claim'] as const) {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-human-membership-'));
    const entered = Promise.withResolvers<AbortSignal>(), release = Promise.withResolvers<void>(), returned = Promise.withResolvers<void>();
    const definition = defineApplication({ name: 'human-membership', admit: async () => {}, environments: [defineEnvironment({
      id: 'work', instructions: '只由仍有工作区成员资格的响应者受理答复',
      program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), {
        Component: () => HumanInput({ async admitResponder(admission) {
          admitResponder(admission); entered.resolve(admission.signal); await release.promise; returned.resolve();
        } }),
      }),
    })] });
    let calls = 0;
    const app = await boot(join(root, 'home'), definition, async function* () {
      calls++; yield* tool('ask_user_question', 'human-membership-' + entry, { questions, timeout: 30 });
    });
    const nativeQuestions = Reflect.get(app.native.userQuestions, symbols.original) as typeof app.native.userQuestions;
    const attachWait = nativeQuestions.attachWait;
    let attached = 0;
    nativeQuestions.attachWait = function (...args) { attached++; return attachWait.apply(this, args); };
    try {
      const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
      const foreground = firstValueFrom(session.foregroundQuestions$.pipe(filter(value => value.length === 1)));
      await session.submit({ requestId: 'merchant-question', content: '请求商家甲的处理许可', intent: 'question', delivery: 'followup', scope: null });
      const [question] = await foreground; assert.ok(question);
      const agent = app.native.agents.get(SessionId(session.id))!;
      let outcome: unknown;
      const waiting = (entry === 'answer' ? session.answerQuestion(question.callId, answer)
        : session.attachWait(question.callId)[Symbol.asyncIterator]().next()).then(value => { outcome = value; }, error => { outcome = error; });
      const admission = await entered.promise;
      const workspace = app.native.workspaceRegistry.get(WorkspaceId(app.workspace.workspaceId))!;
      await workspace.detachSession(SessionId(session.id));
      await immediate();
      const reason: unknown = session.signal.reason;
      assert.ok(reason instanceof ConversationAccessRevokedError && reason.reason === 'membership-removed');
      assert.equal(admission.aborted, true); assert.equal(admission.reason, reason);
      assert.equal(outcome, reason, '原策略门闩尚未放行时本次准入已经结案');
      release.resolve(); await returned.promise; await waiting; await agent.whenIdle();
      assert.equal(attached, 0, '迟到allow不能进入原生claim');
      const result = agent.session.snapshotEvents().find(event => event.type === 'tool/result' && event.data.message.toolCallId === question.callId);
      assert.ok(result?.type === 'tool/result');
      assert.equal(result.data.message.isError, true, '真实工具记录资格退出，未把迟到答案记录为成功');
      assert.equal(calls, 1, '已撤下的Program不能因迟到allow继续模型');
      assert.throws(() => session.answerQuestion(question.callId, answer), error => error === reason);
      session.release();
    } finally {
      release.resolve(); nativeQuestions.attachWait = attachWait; await app.dispose();
    }
  }
});

test('晚答已提出但装配失败时保留原答复和失败因果，明确新输入才消费同一消息', { timeout: 15_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-human-reply-preparation-'));
  const proposed = Promise.withResolvers<void>(), release = Promise.withResolvers<void>();
  const pauseEntered = Promise.withResolvers<void>(), releasePause = Promise.withResolvers<void>();
  const failure = new Error('HUMAN_REPLY_BUSINESS_PREPARATION_REJECTED');
  let rejectReply = false, pauseReply = false, modelCalls = 0;
  const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
  const definition = defineApplication({ name: 'human-reply-preparation', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '许可超时后继续独立工作，收到晚答时重新核验业务条件',
    program: implementProgram(program, { Component() {
      useBeforeStep(async () => {
        if (pauseReply) { pauseEntered.resolve(); await releasePause.promise; return; }
        if (!rejectReply) return;
        proposed.resolve();
        await release.promise;
        throw failure;
      }, []);
      return HumanInput({ admitResponder });
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () {
    modelCalls++;
    if (modelCalls === 1) yield* tool('ask_user_question', 'reply-preparation-question', { questions, timeout: 1 });
    else yield* text(modelCalls === 2 ? '许可尚未返回，本轮独立工作已经结束' : '已经收到原晚答和修复后的明确要求');
  });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const foreground = firstValueFrom(session.foregroundQuestions$.pipe(filter(value => value.length === 1)));
    await session.submit({ requestId: 'original-question', content: '先询问许可，超时后继续独立工作',
      intent: 'question', delivery: 'followup', scope: null });
    const [question] = await foreground;
    assert.ok(question);
    await session.whenIdle();
    const before = await session.human.read();
    assert.equal(before.questions.active[0]?.state, 'continued');
    assert.deepEqual(before.questions.settled, []);
    const original = structuredClone(session.snapshot().requests['original-question']);
    assert.ok(original);
    assert.equal(original.state, 'settled');
    assert.notEqual(original.outcome, 'error');
    assert.equal(modelCalls, 2);

    rejectReply = true;
    const liveError = firstValueFrom(session.rawEvents$.pipe(filter(event => event.kind === 'execution.error')));
    assert.equal(await session.answerQuestion(question.callId, answer), true);
    await proposed.promise;
    const deliveryFrame = await session.human.read();
    const [delivery] = deliveryFrame.replies;
    assert.ok(delivery);
    assert.equal(delivery.state, 'proposed');
    assert.equal(delivery.questionCallId, question.callId);
    assert.deepEqual(delivery.origin, question.origin);
    const native = app.native.agents.get(SessionId(session.id));
    assert.ok(native);
    const beforeFailure = native.session.snapshotEvents();
    assert.ok(!beforeFailure.some(event => event.type === 'user/message' && event.data.id === delivery.messageId));
    const eventCount = beforeFailure.length;
    release.resolve();
    await native.whenIdle();
    await session.whenIdle();
    const failedEvents = native.session.snapshotEvents().slice(eventCount);
    assert.ok(failedEvents.some(event => event.type === 'turn/end' && event.data.reason.kind === 'error'),
      '真实装配错误必须结束提议所属 turn');
    assert.ok(!native.session.snapshotEvents().some(event => event.type === 'user/message' && event.data.id === delivery.messageId),
      '被提出的晚答没有进入正式模型输入历史');
    assert.ok(app.errors.some(error => String(error).includes(failure.message)));
    assert.equal(modelCalls, 2, '装配失败没有执行下一次模型请求');
    const observedError = await liveError;
    assert.ok(observedError.kind === 'execution.error');
    assert.equal(observedError.error, failure, 'live 失败保留原 Error 身份');
    assert.deepEqual(observedError.inputs?.map(input => input.requestId), ['original-question']);
    const retained = await session.human.read();
    assert.deepEqual(retained.replies, [{ ...delivery, state: 'queued' }], '普通 error 后同一消息落回原队列，不因退出提议窗口丢失答案');
    assert.deepEqual(retained.questions, before.questions, '保留答案不伪造问题已经进入模型');
    const [failureFact] = session.snapshot().failures;
    assert.ok(failureFact);
    const ended = failedEvents.find(event => event.type === 'turn/end');
    assert.ok(ended);
    assert.equal(failureFact.sequence, ended.seq);
    assert.equal(failureFact.observedAt, ended.time);
    assert.deepEqual(failureFact.unentered.map(item => [item.messageId, item.questionCallId]), [[delivery.messageId, question.callId]]);
    const failedRequest = session.snapshot().requests['original-question'];
    assert.ok(failedRequest);
    assert.equal(failedRequest.state, original.state);
    assert.equal(failedRequest.outcome, original.outcome);
    assert.equal(failedRequest.reply, original.reply);
    assert.deepEqual(failedRequest.failures, [failureFact]);
    assert.deepEqual((await (await session.question(question.callId)).state.read()).replies, retained.replies);

    rejectReply = false;
    const goal = app.native.goals.create(native, { objective: '保留原问题答案，明确暂停时不得丢失或恢复 Goal' });
    app.native.goals.disarm(native);
    pauseReply = true;
    await session.submit({ requestId: 'prepare-pause', content: '在当前业务条件下核验原答案',
      intent: 'question', delivery: 'followup', scope: null });
    await pauseEntered.promise;
    assert.equal((await session.human.read()).replies[0]?.state, 'proposed');
    const pausing = session.pause();
    releasePause.resolve();
    await pausing;
    await session.whenIdle();
    const paused = await session.human.read();
    assert.deepEqual(paused.replies, retained.replies, '正式 Goal pause 在 keepInbox 前保留已 claim 的原晚答');
    assert.deepEqual(paused.questions, before.questions);
    assert.equal(session.snapshot().goal?.id, goal.id);
    assert.equal(session.snapshot().goal?.phase, 'paused');
    assert.equal(modelCalls, 2, '暂停不唤醒模型或恢复 Goal');
    pauseReply = false;
    await session.submit({ requestId: 'continue-after-repair', content: '业务条件已修复，继续处理已受理的原答案',
      intent: 'question', delivery: 'followup', scope: null });
    await session.whenIdle();
    assert.equal(modelCalls, 3);
    assert.equal(native.session.snapshotEvents().filter(event => event.type === 'user/message' && event.data.id === delivery.messageId).length, 1,
      '明确新输入唤醒后消费原答复 ID，未重新 answer 或生成第二份答案消息');
    const entered = await session.human.read();
    assert.deepEqual(entered.replies, []);
    assert.deepEqual(entered.questions.active, []);
    assert.deepEqual(entered.questions.settled, [{ callId: question.callId, origin: question.origin, answers: answer.answers }]);
    assert.equal(session.snapshot().goal?.phase, 'paused', '明确新问题消费原答案仍不自动恢复 Goal');

    const id = session.id;
    await app.host.close(id);
    session.release();
    const cold = await app.host.open(id, program);
    try {
      assert.equal(app.native.agents.get(SessionId(id)), undefined);
      const recovered = await cold.human.read();
      assert.deepEqual(recovered.replies, [], '原晚答消费后没有遗留的排队或提议');
      assert.deepEqual(recovered.questions, entered.questions);
      assert.deepEqual(cold.snapshot().failures, [failureFact], '后续消费及冷恢复保留原生失败发生，而非仅保留瞬态 Error');
      assert.deepEqual((await (await cold.question(question.callId)).state.read()).replies, []);
      assert.equal(modelCalls, 3, '读取恢复事实不唤醒模型');
    } finally { cold.release(); }
  } finally { release.resolve(); releasePause.resolve(); await app.dispose(); }
});
