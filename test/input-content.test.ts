import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { Agent } from '@deepseek-ai/dsh-agent';
import { isAttachmentError } from '@deepseek-ai/dsh-attachment';
import { SessionId, appendPluginRecord, pluginRecordOf } from '@deepseek-ai/dsh-session';
import type { GenerateOptions } from '@deepseek-ai/dsh-llm';
import { defineApplication, defineEnvironment, defineProgram, defineSurface, z, type AdmittedSubmission, type InputDelivery, type PromptContentPart, type Submission } from '@hanger-source/dsh-reactive';
import { implementProgram, useBeforeStep } from '@hanger-source/dsh-reactive/program';
import { InputFileNotStagedError } from '@hanger-source/dsh-reactive-dsh';
import { admitInputContent } from '../packages/dsh/src/files/input-content.ts';
import { submissionSchema } from '@hanger-source/dsh-reactive/kernel';
import { contentDigest, recordSchema, submissionSourceDigest } from '../packages/dsh/src/conversations/records.ts';
import { boot, text } from './native-support.ts';

const gif = 'R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7';

function received(agent: Agent, requestId: string) {
  return agent.session.snapshotEvents().flatMap(event => event.type === 'user/message'
    && 'rpcId' in event.data.source && event.data.source.rpcId === requestId ? [event.data] : []);
}

function consumedTurn(agent: Agent, requestId: string) {
  let turn: number | undefined;
  for (const event of agent.session.snapshotEvents()) {
    if (event.type === 'turn/start') turn = event.data.turn;
    if (event.type === 'user/message' && 'rpcId' in event.data.source && event.data.source.rpcId === requestId) return turn;
  }
  return undefined;
}

test('原生混合正文保留次序，receipt只属于上传Session，消费后同requestId重试不重新取receipt', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-input-content-'));
  const admissions: AdmittedSubmission[] = [], requests: GenerateOptions[] = [];
  const definition = defineApplication({ name: 'mixed-input', admit: async input => {
    admissions.push(structuredClone(input));
    if (input.requestId.startsWith('policy-rejected-')) throw new Error('INPUT_POLICY_DENIED');
  },
    environments: [defineEnvironment({ id: 'work', instructions: '根据本次全部正文核对资料',
      program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component: () => null }),
    })] });
  const app = await boot(join(root, 'home'), definition, async function* (options) { requests.push(options); yield* text('资料已核对'); });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const foreign = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const agent = app.native.agents.get(SessionId(session.id))!;
    const firstBytes = new TextEncoder().encode('merchant-a\nrevision,amount\n7,23\n');
    const first = await session.uploadFile(firstBytes, { name: 'merchant-a.csv', actorId: 'file-author' }).result;
    const second = await session.uploadFile(new Blob(['说明正文\n保留原始字节']), { name: 'notes.txt', actorId: 'file-author' }).result;
    const firstPath = app.native.attachments.fileHostPath(first.file);
    assert.ok(firstPath);
    assert.deepEqual(new Uint8Array(await readFile(firstPath)), firstBytes);
    assert.equal(requests.length, 0);
    await app.host.close(foreign.id);
    assert.equal(app.native.agents.get(SessionId(foreign.id)), undefined);
    await assert.rejects(foreign.submit({ requestId: 'foreign-file', content: [{ type: 'file', receiptId: first.receiptId }],
      intent: 'question', delivery: 'followup', scope: null }), InputFileNotStagedError);
    await assert.rejects(foreign.submit({ requestId: 'policy-rejected-text', content: '不能激活原生驱动的未准入正文',
      intent: 'question', delivery: 'followup', scope: null }), /INPUT_POLICY_DENIED/u);
    await assert.rejects(foreign.submit({ requestId: 'policy-rejected-image', content: [{ type: 'image', mediaType: 'image/gif', data: gif }],
      intent: 'question', delivery: 'followup', scope: null }), /INPUT_POLICY_DENIED/u);
    assert.equal(app.native.agents.get(SessionId(foreign.id)), undefined);
    assert.equal(foreign.snapshot().requests['policy-rejected-text'], undefined);
    assert.equal(foreign.snapshot().requests['policy-rejected-image'], undefined);
    assert.equal(requests.length, 0);
    await assert.rejects(session.submit({ requestId: 'invalid-image', content: [{ type: 'image', mediaType: 'image/png', data: 'not base64!' }],
      intent: 'question', delivery: 'followup', scope: null }), error => isAttachmentError(error) && error.code === 'INVALID_IMAGE_BASE64');
    assert.ok(app.native.fileUploads.resolve(agent, first.receiptId));
    const content: readonly PromptContentPart[] = [
      { type: 'text', text: '  请按原始顺序核对附件\n' },
      { type: 'file', receiptId: first.receiptId },
      { type: 'image', mediaType: 'image/gif', data: gif, name: 'badge.gif' },
      { type: 'text', text: '\n再核对资料说明  ' },
      { type: 'file', receiptId: second.receiptId },
      { type: 'file', receiptId: first.receiptId },
    ];
    const input: Submission<null> = { requestId: 'mixed-content', content, intent: 'question', delivery: 'followup', scope: null };
    const ack = await session.submit(input, { actorId: 'file-author' });
    await session.whenIdle();
    assert.equal(requests.length, 1);
    const messages = received(agent, input.requestId);
    assert.equal(messages.length, 1);
    const admitted = messages[0]!.content;
    assert.deepEqual(admitted.map(part => part.type), ['text', 'file', 'image', 'text', 'file', 'file']);
    assert.deepEqual(admitted[0], content[0]);
    assert.deepEqual(admitted[1], { type: 'file', attachment: first.file });
    assert.deepEqual(admitted[3], content[3]);
    assert.deepEqual(admitted[4], { type: 'file', attachment: second.file });
    assert.deepEqual(admitted[5], admitted[1]);
    assert.ok(admitted[2]?.type === 'image');
    assert.equal(admitted[2].attachment.width, 1);
    assert.equal(admitted[2].attachment.height, 1);
    assert.deepEqual(admissions.filter(item => item.requestId === input.requestId).map(item => item.content), [admitted, admitted]);
    assert.equal(app.native.fileUploads.resolve(agent, first.receiptId), undefined);
    assert.equal(app.native.fileUploads.resolve(agent, second.receiptId), undefined);
    assert.deepEqual(await session.submit(input, { actorId: 'file-author' }), ack);
    await session.whenIdle();
    assert.equal(requests.length, 1);
    assert.equal(received(agent, input.requestId).length, 1);
    await assert.rejects(session.submit({ ...input, content: [{ type: 'text', text: '另一份正文' }] }, { actorId: 'file-author' }), /REACTIVE_REQUEST_ID_CONFLICT/u);
    await assert.rejects(session.submit({ ...input, requestId: 'retired-file' }, { actorId: 'file-author' }), InputFileNotStagedError);
    const record = app.native.sessionProjections.stateOf(agent.session, 'reactiveInputs')!.records[input.requestId]!;
    assert.equal(record.digest, contentDigest(admitted));
    assert.equal(JSON.stringify(record).includes(gif), false);
    assert.deepEqual(app.errors, []);
    foreign.release(); session.release();
  } finally { await app.dispose(); }
});

test('同进程prepared重试、排队取消与已claim未消费取消都退役原生文件receipt', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-input-receipt-lifetime-'));
  const claimed = Promise.withResolvers<void>();
  let requests = 0, holdClaimed = false;
  const definition = defineApplication({ name: 'receipt-lifetime', admit: async () => {},
    environments: [defineEnvironment({ id: 'work', instructions: '只消费明确受理的资料',
      program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component() {
        useBeforeStep(async signal => {
          if (!holdClaimed) return;
          signal.throwIfAborted();
          await new Promise<void>(resolve => {
            signal.addEventListener('abort', () => resolve(), { once: true });
            claimed.resolve();
          });
          signal.throwIfAborted();
        }, []);
        return null;
      } }),
    })] });
  const app = await boot(join(root, 'home'), definition, async function* () { requests++; yield* text('资料消费完成'); });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const agent = app.native.agents.get(SessionId(session.id))!;
    const file = await session.uploadFile(new TextEncoder().encode('same-process original'), { name: 'original.txt' }).result;
    const input = { requestId: 'prepared-before-abort', intent: 'question', delivery: 'followup', scope: null,
      content: [{ type: 'file', receiptId: file.receiptId }] } satisfies Submission<null>;
    const controller = new AbortController(), cancelled = new Error('INPUT_ABORTED_AFTER_DURABLE_PREPARE');
    let checkpointed = false;
    const stop = app.native.on('session/flush', async nativeSession => {
      if (nativeSession !== agent.session || checkpointed
        || !app.native.sessionProjections.stateOf(nativeSession, 'reactiveInputs')?.records[input.requestId]) return;
      checkpointed = true;
      // 等待原生后端的真实durability barrier；外层session.flush仍在等本监听，尚不能send。
      await app.native.sessionPersistence.flush();
      const reader = await app.native.sessionPersistence.open(agent.id, 'read');
      try {
        const durable = await reader.read();
        assert.ok(durable.events.some(event => {
          const record = pluginRecordOf(event);
          return record?.type === 'plugin:reactive/input' && recordSchema.parse(record.data).requestId === input.requestId;
        }));
        assert.equal(durable.events.some(event => event.type === 'agent/inbox/spliced'
          && event.data.inserted.some(message => 'rpcId' in message.source && message.source.rpcId === input.requestId)), false);
      } finally { await reader.close(); }
      controller.abort(cancelled);
    }, { global: true });
    try { await assert.rejects(session.submit(input, { signal: controller.signal }), error => error === cancelled); }
    finally { stop(); }
    assert.equal(checkpointed, true);
    assert.equal(session.snapshot().requests[input.requestId]?.state, 'declared');
    assert.equal(received(agent, input.requestId).length, 0);
    assert.ok(app.native.fileUploads.resolve(agent, file.receiptId));
    assert.equal(requests, 0);
    await session.submit(input);
    await session.whenIdle();
    assert.equal(requests, 1);
    assert.equal(received(agent, input.requestId).length, 1);
    assert.equal(app.native.fileUploads.resolve(agent, file.receiptId), undefined);
    const injected = await session.uploadFile(new TextEncoder().encode('queued but cancelled'), { name: 'queued.txt' }).result;
    const queued = { requestId: 'cancel-injected-file', intent: 'question', delivery: 'inject', scope: null,
      content: [{ type: 'file', receiptId: injected.receiptId }] } satisfies Submission<null>;
    await session.submit(queued);
    assert.equal(agent.status, 'idle');
    assert.equal(requests, 1);
    assert.ok(agent.inbox.nextStep.some(message => 'rpcId' in message.source && message.source.rpcId === queued.requestId));
    assert.ok(app.native.fileUploads.resolve(agent, injected.receiptId));
    await session.cancel();
    assert.equal(agent.inbox.nextStep.length, 0);
    assert.equal(session.snapshot().requests[queued.requestId]?.outcome, 'cancelled');
    assert.equal(app.native.fileUploads.resolve(agent, injected.receiptId), undefined);
    assert.equal(received(agent, queued.requestId).length, 0);
    assert.equal(requests, 1);
    const beforeConsumption = await session.uploadFile(new TextEncoder().encode('claimed but never consumed'), { name: 'claimed.txt' }).result;
    const preparing = { requestId: 'cancel-claimed-file', intent: 'question', delivery: 'followup', scope: null,
      content: [{ type: 'file', receiptId: beforeConsumption.receiptId }] } satisfies Submission<null>;
    holdClaimed = true;
    await session.submit(preparing);
    await claimed.promise;
    assert.equal(agent.status, 'running');
    assert.equal(agent.inbox.nextStep.length, 0);
    assert.equal(agent.inbox.nextTurn.length, 0);
    assert.equal(received(agent, preparing.requestId).length, 0);
    assert.ok(app.native.fileUploads.resolve(agent, beforeConsumption.receiptId));
    assert.equal(requests, 1);
    await session.cancel();
    await session.whenIdle();
    assert.equal(app.native.fileUploads.resolve(agent, beforeConsumption.receiptId), undefined);
    assert.equal(received(agent, preparing.requestId).length, 0);
    assert.equal(session.snapshot().requests[preparing.requestId]?.state, 'settled');
    assert.equal(session.snapshot().requests[preparing.requestId]?.outcome, 'aborted');
    assert.equal(requests, 1);
    assert.deepEqual(app.errors, []);
    session.release();
  } finally { await app.dispose(); }
});

test('首次文件准入等待期间关闭原Agent不能将失效receipt洗入替换Agent的prepared事实', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-input-exact-owner-'));
  const entered = Promise.withResolvers<void>(), proceed = Promise.withResolvers<void>();
  let held = true, requests = 0;
  const definition = defineApplication({ name: 'exact-input-owner', admit: async input => {
    if (input.requestId === 'owner-switched' && held) { entered.resolve(); await proceed.promise; }
  }, environments: [defineEnvironment({ id: 'work', instructions: '只消费本次确切会话实例签发的附件',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component: () => null }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () { requests++; yield* text('新会话资料已消费'); });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const original = app.native.agents.get(SessionId(session.id))!;
    const file = await session.uploadFile(new TextEncoder().encode('original owner bytes'), { name: 'original-owner.txt' }).result;
    const input = { requestId: 'owner-switched', intent: 'question', delivery: 'followup', scope: null,
      content: [{ type: 'file', receiptId: file.receiptId }] } satisfies Submission<null>;
    const refused = assert.rejects(session.submit(input), InputFileNotStagedError);
    await entered.promise;
    await app.host.close(session.id);
    assert.equal(app.native.agents.get(SessionId(session.id)), undefined);
    const replacement = await app.host.resume(session.id, definition.environments[0].program.definition);
    const current = app.native.agents.get(SessionId(session.id))!;
    assert.notEqual(current, original);
    held = false; proceed.resolve();
    await refused;
    assert.equal(replacement.snapshot().requests[input.requestId], undefined);
    assert.equal(current.session.snapshotEvents().some(event => {
      const record = pluginRecordOf(event);
      return record?.type === 'plugin:reactive/input' && recordSchema.parse(record.data).requestId === input.requestId;
    }), false);
    assert.equal(received(current, input.requestId).length, 0);
    await assert.rejects(replacement.submit(input), InputFileNotStagedError);
    assert.equal(requests, 0);
    const fresh = await replacement.uploadFile(new TextEncoder().encode('fresh replacement owner bytes'), { name: 'replacement.txt' }).result;
    await replacement.submit({ ...input, content: [{ type: 'file', receiptId: fresh.receiptId }] });
    await replacement.whenIdle();
    assert.equal(requests, 1);
    assert.deepEqual(received(current, input.requestId).map(message => message.content), [[{ type: 'file', attachment: fresh.file }]]);
    assert.equal(app.native.fileUploads.resolve(current, fresh.receiptId), undefined);
    assert.deepEqual(app.errors, []);
    replacement.release(); session.release();
  } finally { proceed.resolve(); await app.dispose(); }
});

test('prepared附件正文跨重启按原requestId显式受理，未提交receipt不会借持久文件身份恢复', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-prepared-input-'));
  const home = join(root, 'home');
  const admissions: AdmittedSubmission[] = [];
  let requests = 0;
  const definition = defineApplication({ name: 'prepared-input', admit: async input => { admissions.push(structuredClone(input)); },
    environments: [defineEnvironment({ id: 'work', instructions: '只处理明确受理的资料',
      program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component: () => null }),
    })] });
  const respond = async function* () { requests++; yield* text('从持久正文继续核对'); };
  let app = await boot(home, definition, respond);
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const file = await session.uploadFile(new TextEncoder().encode('prepared original bytes'), { name: 'prepared.txt' }).result;
    const unused = await session.uploadFile(new TextEncoder().encode('not submitted'), { name: 'unused.txt' }).result;
    const input = { requestId: 'prepared-file', intent: 'question', delivery: 'followup', scope: null,
      content: [{ type: 'text', text: '  核对已准入资料\n' }, { type: 'file', receiptId: file.receiptId },
        { type: 'image', mediaType: 'image/gif', data: gif }] } satisfies Submission<null>;
    const normalized = submissionSchema.parse(input);
    const agent = app.native.agents.get(SessionId(session.id))!;
    const admission = await admitInputContent(app.native, agent, normalized.content);
    const record = recordSchema.parse({ ...normalized, content: admission.content, actorId: 'operator-test', participant: 'human',
      sourceDigest: submissionSourceDigest(normalized, 'operator-test', 'human'), digest: contentDigest(admission.content) });
    // 固定在真实附件已准入、关联已持久化而原生send尚未发生的边界。
    appendPluginRecord(agent.session, 'plugin:reactive/input', record);
    assert.equal(await app.native.sessions.flush(agent.session), true);
    assert.equal(session.snapshot().requests[input.requestId]?.state, 'declared');
    assert.equal(received(agent, input.requestId).length, 0);
    assert.equal(requests, 0);
    const id = session.id;
    await app.host.close(id); session.release(); await app.dispose();
    app = await boot(home, definition, respond);
    const cold = await app.host.open(id, definition.environments[0].program.definition);
    assert.equal(cold.snapshot().requests[input.requestId]?.state, 'declared');
    assert.equal(app.native.agents.get(SessionId(id)), undefined);
    await assert.rejects(cold.submit({ requestId: 'unprepared-file', intent: 'question', delivery: 'followup', scope: null,
      content: [{ type: 'file', receiptId: unused.receiptId }] }), InputFileNotStagedError);
    assert.equal(app.native.agents.get(SessionId(id)), undefined);
    assert.equal(requests, 0);
    const ack = await cold.submit(input);
    await cold.whenIdle();
    const resumed = app.native.agents.get(SessionId(id))!;
    assert.equal(app.native.fileUploads.resolve(resumed, file.receiptId), undefined);
    assert.equal(requests, 1);
    assert.deepEqual(received(resumed, input.requestId).map(message => message.content), [admission.content]);
    assert.deepEqual(admissions.filter(item => item.requestId === input.requestId).map(item => item.content), [admission.content, admission.content]);
    assert.deepEqual(await cold.submit(input), ack);
    await cold.whenIdle();
    assert.equal(requests, 1);
    assert.equal(received(resumed, input.requestId).length, 1);
    assert.deepEqual(app.errors, []);
    cold.release();
  } finally { await app.dispose(); }
});

for (const delivery of ['followup', 'steer', 'inject'] satisfies readonly InputDelivery[]) {
  test(delivery + '在空闲和活跃模型边界遵守原生投递位置及唤醒语义', async () => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-input-' + delivery + '-'));
    const started = Promise.withResolvers<void>(), finish = Promise.withResolvers<void>();
    const requests: GenerateOptions[] = [];
    const definition = defineApplication({ name: 'delivery-' + delivery, admit: async () => {},
      environments: [defineEnvironment({ id: 'work', instructions: '按实际取得的输入继续',
        program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component: () => null }),
      })] });
    const app = await boot(join(root, 'home'), definition, async function* (options) {
      requests.push(options);
      if (requests.length === 2) { started.resolve(); await finish.promise; }
      yield* text('已处理本步资料');
    });
    try {
      const idle = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
      const idleAgent = app.native.agents.get(SessionId(idle.id))!;
      await idle.submit({ requestId: 'idle-' + delivery, content: '空闲时收到的资料', intent: 'question', delivery, scope: null });
      await idle.whenIdle();
      if (delivery === 'inject') {
        assert.equal(requests.length, 0);
        assert.equal(idleAgent.status, 'idle');
        assert.deepEqual(idleAgent.inbox.nextStep.map(message => 'rpcId' in message.source ? message.source.rpcId : null), ['idle-inject']);
        assert.equal(idle.snapshot().requests['idle-inject']?.state, 'accepted');
        await idle.submit({ requestId: 'wake-injected-input', content: '现在明确开始处理', intent: 'question', delivery: 'followup', scope: null });
        await idle.whenIdle();
        assert.equal(consumedTurn(idleAgent, 'idle-inject'), consumedTurn(idleAgent, 'wake-injected-input'));
      }
      assert.equal(requests.length, 1);
      assert.equal(received(idleAgent, 'idle-' + delivery).length, 1);
      const active = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
      const activeAgent = app.native.agents.get(SessionId(active.id))!;
      await active.submit({ requestId: 'active-start', content: '开始原始模型步骤', intent: 'question', delivery: 'followup', scope: null });
      await started.promise;
      await active.submit({ requestId: 'during-' + delivery, content: '在模型活动期间补入的资料', intent: 'followup', delivery, scope: null });
      assert.equal(requests.length, 2);
      assert.equal(received(activeAgent, 'during-' + delivery).length, 0);
      const target = delivery === 'followup' ? activeAgent.inbox.nextTurn : activeAgent.inbox.nextStep;
      assert.deepEqual(target.map(message => 'rpcId' in message.source ? message.source.rpcId : null), ['during-' + delivery]);
      finish.resolve();
      await active.whenIdle();
      assert.equal(requests.length, 3);
      assert.equal(received(activeAgent, 'during-' + delivery).length, 1);
      assert.equal(consumedTurn(activeAgent, 'active-start'), 1);
      assert.equal(consumedTurn(activeAgent, 'during-' + delivery), delivery === 'followup' ? 2 : 1);
      assert.equal(JSON.stringify(requests[1]!.messages).includes('在模型活动期间补入的资料'), false);
      assert.equal(JSON.stringify(requests[2]!.messages).includes('在模型活动期间补入的资料'), true);
      assert.deepEqual(app.errors, []);
      active.release(); idle.release();
    } finally { finish.resolve(); await app.dispose(); }
  });
}
