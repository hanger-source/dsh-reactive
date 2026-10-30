import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement as h, Fragment } from 'react';
import { firstValueFrom, filter, timeout } from 'rxjs';
import WebSocket from 'ws';
import { RemoteStream } from '@deepseek-ai/dsh-api-gateway/client';
import { LlmAdapter, ToolCallId } from '@deepseek-ai/dsh-llm';
import { SessionId } from '@deepseek-ai/dsh-session';
import { defineApplication, defineEnvironment, defineProgram, defineSurface, defineAction, implementAction, HumanQuestionSourceConflictError, createWorkSelector, z } from '@hanger-source/dsh-reactive';
import { HumanInput, Offer, useAction, implementProgram, useBeforeStep } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import { createProgramClient } from '@hanger-source/dsh-reactive-client';
import { mountProgramGateway, RemoteAuthenticationError } from '@hanger-source/dsh-reactive-remote';

const next = (source, predicate) => firstValueFrom(source.pipe(filter(predicate), timeout(5000)));

for (const failPreparation of [false, true]) test(failPreparation
  ? '晚答装配失败经过真实 HTTP 保留失败因果，明确新输入消费原消息，冷读仍解释原失败'
  : '冷会话晚答经过真实 HTTP，交付中的原消息和问题归属与模型输入结算分开', { timeout: 15000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-human-delivery-'));
  const proposed = Promise.withResolvers(), release = Promise.withResolvers();
  let hold = false, reject = failPreparation, modelCalls = 0, gateway, client, local;
  const requests = [], admissions = [], errors = [];
  const scope = { workId: 'knowledge', submittedAt: '2026-10-29T00:00:00.000Z' };
  const program = defineProgram({ name: 'human_delivery', input: z.object({ workId: z.string(), submittedAt: z.iso.datetime() })
    .transform(value => ({ ...value, submittedAt: new Date(value.submittedAt) })), surface: defineSurface({ views: {}, actions: {} }) });
  const definition = defineApplication({ name: 'human-delivery', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '补充资料不代表新的操作授权', program: implementProgram(program, { Component() {
      useBeforeStep(async () => { if (hold) {
        proposed.resolve(); await release.promise;
        if (reject) throw new Error('REMOTE_REPLY_PREPARATION_FAILED');
      } }, []);
      return h(HumanInput, { admitResponder(admission) {
        assert.equal(admission.actorId, 'bob'); admissions.push(admission.question.origin);
      } });
    } }),
  })] });
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'alice', tools: 'native',
    onError: value => errors.push(value.error), model: nativeModel({ provider: 'human-delivery', model: 'controlled', create: () => new class extends LlmAdapter {
      async *stream(options) {
        requests.push(options.messages); modelCalls++;
        if (modelCalls === 1) {
          const id = ToolCallId('original-question'), args = JSON.stringify({ questions: [{ id: 'evidence', question: '原始证据在哪里？' }], timeout: 1 });
          yield { type: 'block-start', index: 0, blockType: 'tool-call' };
          yield { type: 'tool-call-delta', index: 0, id, name: 'ask_user_question', argumentsDelta: args };
          yield { type: 'block-end', index: 0, block: { type: 'tool-call', id, name: 'ask_user_question', arguments: args } };
          yield { type: 'finish', reason: { kind: 'tool-calls' } }; return;
        }
        yield { type: 'block-start', index: 0, blockType: 'text' };
        yield { type: 'block-end', index: 0, block: { type: 'text', text: '保留原问题并按实际输入继续' } };
        yield { type: 'finish', reason: { kind: 'stop' } };
      }
    }() }),
  });
  try {
    const workspace = await runtime.application.createWorkspace({ directory: root });
    local = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    await local.submit({ requestId: 'original', intent: 'question', delivery: 'followup', content: '调查知识证据', scope }, { actorId: 'alice' });
    await local.whenIdle();
    assert.equal(modelCalls, 2);
    await runtime.application.close(local.id);
    gateway = await mountProgramGateway(runtime.native, { application: runtime.application,
      model: { provider: 'human-delivery', model: 'controlled' }, participant: principal => ({ participant: 'human', actorId: principal.id }),
      access: access => access.registerPolicy('delivery', { bindings: () => [{
        role: { name: 'participate', resource: 'session', actions: ['reactivePrograms/open', 'reactivePrograms/questions',
          'reactivePrograms/answerQuestion', 'reactivePrograms/progress', 'reactivePrograms/submit'] },
        resource: { kind: 'session', id: local.id },
      }] }), carrier: { onError: error => errors.push(error), async authenticate(request) {
        if (request.headers.get('authorization') !== 'Bearer bob') throw new RemoteAuthenticationError(401);
        return { kind: 'delivery', id: 'bob' };
      } },
    });
    client = await createProgramClient(program, { baseUrl: gateway.url,
      fetch: (url, init) => fetch(url, { ...init, headers: { ...Object.fromEntries(new Headers(init?.headers)), authorization: 'Bearer bob' } }),
      createWebSocket: url => new WebSocket(url, { headers: { authorization: 'Bearer bob' } }),
    });
    const remote = await client.open(local.id);
    const baseline = await remote.human.read();
    const question = baseline.questions.active[0];
    assert.equal(question.state, 'continued'); assert.deepEqual(baseline.replies, []);
    assert.equal(runtime.native.agents.get(SessionId(local.id)), undefined, '冷观察不激活 Agent');
    assert.deepEqual(question.origin.inputs.map(input => ({ actorId: input.actorId, requestId: input.requestId, scope: input.scope })),
      [{ actorId: 'alice', requestId: 'original', scope }]);
    assert.deepEqual(question.origin.continuation, { sessionId: local.id, inputs: question.origin.inputs, goal: null },
      'HTTP 保留原独立问题的已知参与关联，不能省略成缺证');
    const bound = await remote.question(question.callId);
    hold = true;
    const delivering = next(remote.human.values$, value => value.replies.some(reply => reply.state === 'proposed'));
    assert.equal(await bound.answer({ answers: [{ id: 'evidence', selected: [], custom: 'file:///evidence/original.md' }] }), true);
    await proposed.promise;
    const snapshot = await delivering, [reply] = snapshot.replies;
    const preparing = await next(remote.progress.values$, value => value.preparation?.waiting.some(wait => wait.kind === 'check'));
    assert.deepEqual(preparing.preparation.inputs, question.origin.inputs, 'HTTP 保留准备中的原输入，未借新的回答者身份');
    const [feedbackAdmission] = preparing.preparation.admissions;
    assert.deepEqual(feedbackAdmission, { source: { kind: 'human-reply', messageId: reply.messageId,
      questionCallId: question.callId, callSequence: question.origin.callSequence },
      continuation: question.origin.continuation, goal: null }, 'HTTP 保留精确迟答承接及其原始 JSON 范围');
    assert.equal(typeof feedbackAdmission.continuation.inputs[0].scope.submittedAt, 'string',
      '反馈的原范围不经过 Program 的 Date 转换');
    assert.equal((await remote.request('original').state.read()).preparation.id, preparing.preparation.id);
    assert.equal((await remote.request('unrelated').state.read()).preparation, null);
    assert.equal(reply.questionCallId, question.callId); assert.equal(reply.target, 'next-step');
    assert.deepEqual(reply.origin, question.origin);
    assert.equal(snapshot.questions.active[0].state, 'continued'); assert.deepEqual(snapshot.questions.settled, []);
    assert.deepEqual((await bound.state.read()).replies, snapshot.replies);
    const selector = createWorkSelector({ identify: origin => origin.kind === 'invocation' ? undefined : z.object({ workId: z.string() }).parse(origin.scope).workId });
    const work = selector.select('knowledge', { sessions: [{ identity: local.identity, progress: await local.progress.read(), human: snapshot }] }).sessions[0];
    const pendingReply = work.conditions.find(condition => condition.kind === 'question.reply-pending' && condition.value.messageId === reply.messageId);
    assert.deepEqual(pendingReply.admission, feedbackAdmission);
    const agent = runtime.native.agents.get(SessionId(local.id));
    assert.equal(agent.session.snapshotEvents().some(event => event.type === 'user/message' && event.data.id === reply.messageId), false);
    assert.equal(modelCalls, 2, '提议尚未进入模型输入或启动新模型请求');
    const original = (await remote.request('original').state.read()).activity;
    release.resolve();
    await local.whenIdle();
    if (failPreparation) {
      const retained = await next(remote.human.values$, value => value.replies.some(item => item.state === 'queued'));
      assert.equal(retained.replies[0].messageId, reply.messageId);
      assert.deepEqual(retained.questions, snapshot.questions, '装配失败不结算原问题');
      const failed = await next(remote.progress.values$, value => value.activity.failures.length === 1);
      const [failure] = failed.activity.failures;
      assert.equal(failed.preparation, null, '装配失败撤下当前等待，失败历史另行保留');
      const ended = agent.session.snapshotEvents().find(event => event.seq === failure.sequence);
      assert.equal(ended.type, 'turn/end'); assert.equal(ended.data.reason.kind, 'error');
      assert.deepEqual(failure.error, ended.data.reason.error, 'HTTP 保留原生 LlmFailure');
      assert.match(failure.error.message, /REMOTE_REPLY_PREPARATION_FAILED/);
      assert.deepEqual(failure.inputs, question.origin.inputs);
      assert.deepEqual(failure.unentered, [{ messageId: reply.messageId, questionCallId: question.callId,
        target: reply.target, inputs: question.origin.inputs }]);
      const request = (await remote.request('original').state.read()).activity;
      assert.deepEqual(request.failures, [failure]);
      assert.deepEqual([request.state, request.outcome, request.reply], [original.state, original.outcome, original.reply]);
      const failedWork = selector.select('knowledge', { sessions: [{ identity: local.identity, progress: failed, human: retained }] }).sessions[0];
      assert.ok(failedWork.conditions.some(condition => condition.kind === 'execution.failed' && condition.value.sequence === failure.sequence));
      assert.equal(modelCalls, 2, '失败保留答复没有自动重试或重新调用模型');
      assert.equal(agent.session.snapshotEvents().some(event => event.type === 'user/message' && event.data.id === reply.messageId), false);
      hold = false; reject = false;
      await remote.submit({ requestId: 'continue', content: '明确继续处理原始证据', scope, intent: 'question', delivery: 'followup' });
      await local.whenIdle();
    }
    hold = false;
    const settled = await next(remote.human.values$, value => value.questions.settled.some(item => item.callId === question.callId));
    assert.deepEqual(settled.replies, []);
    assert.ok(agent.session.snapshotEvents().some(event => event.type === 'user/message' && event.data.id === reply.messageId));
    assert.ok(requests[2].some(message => message.role === 'user' && message.id === reply.messageId));
    assert.equal(modelCalls, 3); assert.equal(admissions.length, 2, '冷预审和真实 live 准入沿同一原问题');
    assert.equal((await remote.progress.read()).preparation, null);
    if (failPreparation) {
      assert.equal(agent.session.snapshotEvents().filter(event => event.type === 'user/message' && event.data.id === reply.messageId).length, 1);
      assert.equal(errors.length, 1); assert.match(String(errors[0]), /REMOTE_REPLY_PREPARATION_FAILED/);
      const failure = (await remote.progress.read()).activity.failures[0];
      await runtime.application.close(local.id);
      const cold = await client.open(local.id);
      assert.equal((await cold.progress.read()).preparation, null, '冷读不复活曾经等待的准备');
      assert.deepEqual((await cold.request('original').state.read()).activity.failures, [failure]);
      assert.equal(runtime.native.agents.get(SessionId(local.id)), undefined, '失败历史的冷读不激活 Agent');
      assert.equal(modelCalls, 3);
    } else assert.deepEqual(errors, []);
  } finally {
    hold = false; release.resolve(); await client?.dispose(); local?.release();
    try { await gateway?.close(); } finally { await runtime.dispose(); }
  }
});

test('真实 HTTP 隔离复用 callId 的业务答案，原问题控制拒绝迟答且 Work 保留原工作归属', { timeout: 15000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-human-source-'));
  const entered = Promise.withResolvers(), release = Promise.withResolvers();
  let phase = 'original', emitted = false, hold = false, gateway, client, local;
  const errors = [], admissions = [];
  const answer = { answers: [{ id: 'evidence', selected: [], custom: '业务 JSON 不是人工回答' }] };
  const action = defineAction({ name: 'business_answer', description: '返回业务 JSON', scope: z.null(), input: z.object({}), output: z.object({ answers: z.array(z.object({ id: z.string(), selected: z.array(z.string()), custom: z.string() })) }) });
  const implementation = implementAction(action, { execute: () => answer });
  const program = defineProgram({ name: 'human_source', input: z.object({ workId: z.string() }), surface: defineSurface({ views: {}, actions: {} }) });
  const definition = defineApplication({ name: 'human-source', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '实际调用由受控模型指定', program: implementProgram(program, { Component() {
      const business = useAction(implementation, () => null, []);
      return h(Fragment, null, h(Offer, { name: 'business_answer', action: business }), h(HumanInput, { async admitResponder(admission) {
        admissions.push(admission.question.origin);
        if (hold) { entered.resolve(); await release.promise; }
      } }));
    } }),
  })] });
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'alice', tools: 'native',
    onError: value => errors.push(value.error), model: nativeModel({ provider: 'human-source', model: 'controlled', create: () => new class extends LlmAdapter {
      async *stream() {
        if (!emitted) {
          emitted = true;
          const id = ToolCallId(phase === 'independent' ? 'independent-question' : 'shared-question');
          const name = phase === 'foreign' ? 'business_answer' : 'ask_user_question';
          const args = JSON.stringify(phase === 'foreign' ? {} : { questions: [{ id: 'evidence', question: '证据在哪里？' }], timeout: 1 });
          yield { type: 'block-start', index: 0, blockType: 'tool-call' };
          yield { type: 'tool-call-delta', index: 0, id, name, argumentsDelta: args };
          yield { type: 'block-end', index: 0, block: { type: 'tool-call', id, name, arguments: args } };
          yield { type: 'finish', reason: { kind: 'tool-calls' } }; return;
        }
        yield { type: 'block-start', index: 0, blockType: 'text' };
        yield { type: 'block-end', index: 0, block: { type: 'text', text: '等待后续明确输入' } };
        yield { type: 'finish', reason: { kind: 'stop' } };
      }
    }() }),
  });
  try {
    const workspace = await runtime.application.createWorkspace({ directory: root });
    local = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    await local.submit({ requestId: 'original', intent: 'question', delivery: 'followup', content: '原问题', scope: { workId: 'original' } }, { actorId: 'alice' });
    await local.whenIdle();
    gateway = await mountProgramGateway(runtime.native, { application: runtime.application,
      model: { provider: 'human-source', model: 'controlled' }, participant: principal => ({ participant: 'human', actorId: principal.id }),
      access: access => access.registerPolicy('source', { bindings: () => [{
        role: { name: 'participate', resource: 'session', actions: ['reactivePrograms/open', 'reactivePrograms/questions', 'reactivePrograms/answerQuestion', 'reactivePrograms/attachWait', 'reactivePrograms/progress'] },
        resource: { kind: 'session', id: local.id },
      }] }), carrier: { onError: error => errors.push(error), async authenticate() { return { kind: 'source', id: 'bob' }; } },
    });
    client = await createProgramClient(program, { baseUrl: gateway.url, createWebSocket: url => new WebSocket(url) });
    const remote = await client.open(local.id), original = (await remote.human.read()).questions.active[0];
    const bound = await remote.question(original.callId);
    assert.deepEqual(bound.origin, original.origin);
    hold = true;
    const expected = { sessionId: local.id, callId: original.callId, callSequence: original.origin.callSequence };
    const answering = remote.answerQuestion(original.callId, answer, {}, expected).then(value => ({ value }), error => ({ error }));
    expected.callId = 'independent-question'; expected.callSequence += 100;
    await entered.promise;
    phase = 'foreign'; emitted = false;
    const changed = next(remote.human.values$, value => value.sourceConflicts.length > 0);
    await local.submit({ requestId: 'foreign', intent: 'question', delivery: 'followup', content: '独立业务工作', scope: { workId: 'foreign' } }, { actorId: 'alice' });
    await local.whenIdle();
    const conflicted = await changed, [conflict] = conflicted.sourceConflicts;
    assert.deepEqual(conflict.origin, original.origin);
    assert.equal(conflict.reason, 'foreign-result');
    assert.equal(conflict.occurrence.inputs[0].requestId, 'foreign');
    assert.deepEqual(conflicted.questions, { active: [], settled: [] });
    release.resolve(); hold = false;
    const result = await answering;
    assert.ok(result.error instanceof HumanQuestionSourceConflictError, JSON.stringify(result, (_key, value) => value instanceof Error
      ? { name: value.name, message: value.message, code: value.code, details: value.details, cause: value.cause } : value));
    assert.deepEqual(result.error.conflicts, conflicted.sourceConflicts);
    await assert.rejects(bound.answer(answer), HumanQuestionSourceConflictError);
    await assert.rejects(bound.attachWait()[Symbol.asyncIterator]().next(), HumanQuestionSourceConflictError);
    await assert.rejects(remote.answerQuestion(original.callId, answer), HumanQuestionSourceConflictError);
    await assert.rejects(remote.attachWait(original.callId)[Symbol.asyncIterator]().next(), HumanQuestionSourceConflictError);
    const selector = createWorkSelector({ identify: origin => origin.scope?.workId });
    const sources = { sessions: [{ identity: remote.identity, progress: await remote.progress.read(), human: conflicted }] };
    assert.deepEqual(selector.select('original', sources).sessions[0].questions.sourceConflicts[0].value, conflict);
    assert.ok(selector.select('original', sources).sessions[0].conditions.some(value => value.kind === 'question.source-conflict'));
    assert.deepEqual(selector.select('foreign', sources).sessions[0].questions.sourceConflicts, []);
    phase = 'independent'; emitted = false;
    await local.submit({ requestId: 'independent', intent: 'question', delivery: 'followup', content: '另一合法问题', scope: { workId: 'independent' } }, { actorId: 'alice' });
    await local.whenIdle();
    const independent = await next(remote.human.values$, value => value.questions.active.some(item => item.callId === 'independent-question'));
    assert.equal(independent.sourceConflicts.length, 1);
    assert.equal(await (await remote.question('independent-question')).answer(answer), true);
    await local.whenIdle();
    const settled = await next(remote.human.values$, value => value.questions.settled.some(item => item.callId === 'independent-question'));
    assert.equal(settled.questions.settled.some(item => item.callId === original.callId), false);
    assert.ok(admissions.every(origin => origin.callId === original.callId || origin.callId === 'independent-question'));
    assert.deepEqual(errors, []);
    // 在真实 Gateway 的传输消费边界损坏来源证据，不能把未核验的 settled 发布成 ready。
    const iterate = RemoteStream.prototype[Symbol.asyncIterator];
    for (const malformed of ['missing', 'not-array']) {
      let damaged = 0;
      RemoteStream.prototype[Symbol.asyncIterator] = function (...args) {
        const iterator = iterate.apply(this, args), next = iterator.next.bind(iterator);
        iterator.next = async (...args) => {
          const result = await next(...args);
          const frame = result.value?.value;
          if (!result.done && frame?.type === 'baseline' && frame.value?.questions) {
            const value = { ...frame.value };
            if (malformed === 'missing') delete value.sourceConflicts;
            else value.sourceConflicts = {};
            damaged++;
            return { ...result, value: { ...result.value, value: { ...frame, value } } };
          }
          return result;
        };
        return iterator;
      };
      let stop;
      try {
        const unchecked = await client.open(local.id), visible = [];
        stop = unchecked.human.values$.subscribe({ next: value => visible.push(value), error() {} });
        await assert.rejects(unchecked.human.read(), /REACTIVE_REMOTE_HUMAN_SOURCE_EVIDENCE_INVALID/);
        assert.equal(damaged, 1);
        assert.deepEqual(visible, []);
        stop.unsubscribe(); stop = undefined;
        await unchecked.release();
      } finally { stop?.unsubscribe(); RemoteStream.prototype[Symbol.asyncIterator] = iterate; }
    }
  } finally {
    hold = false; release.resolve(); await client?.dispose(); local?.release();
    try { await gateway?.close(); } finally { await runtime.dispose(); }
  }
});
