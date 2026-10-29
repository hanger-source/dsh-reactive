import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement as h } from 'react';
import { firstValueFrom, filter, timeout } from 'rxjs';
import WebSocket from 'ws';
import { LlmAdapter, ToolCallId } from '@deepseek-ai/dsh-llm';
import { SessionId } from '@deepseek-ai/dsh-session';
import { defineApplication, defineEnvironment, defineProgram, defineSurface, createWorkSelector, z } from '@hanger-source/dsh-reactive';
import { HumanInput, implementProgram, useBeforeStep } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import { createProgramClient } from '@hanger-source/dsh-reactive-client';
import { mountProgramGateway, RemoteAuthenticationError } from '@hanger-source/dsh-reactive-remote';

const next = (source, predicate) => firstValueFrom(source.pipe(filter(predicate), timeout(5000)));

test('冷会话晚答经过真实 HTTP，交付中的原消息和问题归属与模型输入结算分开', { timeout: 15000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-human-delivery-'));
  const proposed = Promise.withResolvers(), release = Promise.withResolvers();
  let hold = false, modelCalls = 0, gateway, client, local;
  const requests = [], admissions = [], errors = [];
  const scope = { workId: 'knowledge', submittedAt: '2026-10-29T00:00:00.000Z' };
  const program = defineProgram({ name: 'human_delivery', input: z.object({ workId: z.string(), submittedAt: z.iso.datetime() })
    .transform(value => ({ ...value, submittedAt: new Date(value.submittedAt) })), surface: defineSurface({ views: {}, actions: {} }) });
  const definition = defineApplication({ name: 'human-delivery', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '补充资料不代表新的操作授权', program: implementProgram(program, { Component() {
      useBeforeStep(async () => { if (hold) { proposed.resolve(); await release.promise; } }, []);
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
        role: { name: 'participate', resource: 'session', actions: ['reactivePrograms/open', 'reactivePrograms/questions', 'reactivePrograms/answerQuestion'] },
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
    const bound = await remote.question(question.callId);
    hold = true;
    const delivering = next(remote.human.values$, value => value.replies.some(reply => reply.state === 'proposed'));
    assert.equal(await bound.answer({ answers: [{ id: 'evidence', selected: [], custom: 'file:///evidence/original.md' }] }), true);
    await proposed.promise;
    const snapshot = await delivering, [reply] = snapshot.replies;
    assert.equal(reply.questionCallId, question.callId); assert.equal(reply.target, 'next-step');
    assert.deepEqual(reply.origin, question.origin);
    assert.equal(snapshot.questions.active[0].state, 'continued'); assert.deepEqual(snapshot.questions.settled, []);
    assert.deepEqual((await bound.state.read()).replies, snapshot.replies);
    const selector = createWorkSelector({ identify: origin => z.object({ workId: z.string() }).parse(origin.scope).workId });
    const work = selector.select('knowledge', [{ identity: local.identity, progress: await local.progress.read(), human: snapshot }]).sessions[0];
    assert.ok(work.conditions.some(condition => condition.kind === 'question.reply-pending' && condition.value.messageId === reply.messageId));
    const agent = runtime.native.agents.get(SessionId(local.id));
    assert.equal(agent.session.snapshotEvents().some(event => event.type === 'user/message' && event.data.id === reply.messageId), false);
    assert.equal(modelCalls, 2, '提议尚未进入模型输入或启动新模型请求');
    hold = false; release.resolve();
    await local.whenIdle();
    const settled = await next(remote.human.values$, value => value.questions.settled.some(item => item.callId === question.callId));
    assert.deepEqual(settled.replies, []);
    assert.ok(agent.session.snapshotEvents().some(event => event.type === 'user/message' && event.data.id === reply.messageId));
    assert.ok(requests[2].some(message => message.role === 'user' && message.id === reply.messageId));
    assert.equal(modelCalls, 3); assert.equal(admissions.length, 2, '冷预审和真实 live 准入沿同一原问题');
    assert.deepEqual(errors, []);
  } finally {
    hold = false; release.resolve(); await client?.dispose(); local?.release();
    try { await gateway?.close(); } finally { await runtime.dispose(); }
  }
});
