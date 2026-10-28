import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate as immediate } from 'node:timers/promises';
import { SessionId, pluginRecordOf, type UserMessage } from '@deepseek-ai/dsh-session';
import { defineApplication, defineEnvironment, defineProgram, defineSurface, InputEditRejectedError, z, type AdmittedSubmission, type Submission } from '@hanger-source/dsh-reactive';
import { implementProgram, useBeforeStep } from '@hanger-source/dsh-reactive/program';
import { inputEditRecordSchema, recordSchema } from '../packages/dsh/src/conversations/records.ts';
import { boot, text } from './native-support.ts';

function queued(messages: readonly UserMessage[], requestId: string) {
  return messages.find(message => message.source.kind === 'user' && 'rpcId' in message.source && message.source.rpcId === requestId);
}

test('队列编辑沿原请求重新准入，保留位置及身份且不唤醒，消费新附件后原receipt一并退役', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-edit-input-'));
  const admissions: AdmittedSubmission[] = [];
  let requests = 0;
  const program = defineProgram({ input: z.object({ merchantId: z.string() }), surface: defineSurface({ views: {}, actions: {} }) });
  const definition = defineApplication({ name: 'edit-input', admit: async input => {
    admissions.push(structuredClone(input));
    if (input.content.some(part => part.type === 'text' && part.text === '拒绝这次编辑')) throw new Error('EDIT_POLICY_DENIED');
  }, environments: [defineEnvironment({ id: 'work', instructions: '核对原请求当前受理的资料',
    program: implementProgram(program, { Component: () => null }) })] });
  const app = await boot(join(root, 'home'), definition, async function* () { requests++; yield* text('资料已核对'); });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const agent = app.native.agents.get(SessionId(session.id))!;
    const oldFile = await session.uploadFile(new TextEncoder().encode('旧资料'), { name: 'old.txt' });
    const input = { requestId: 'merchant-facts', intent: 'question', delivery: 'inject', scope: { merchantId: 'merchant-a' },
      content: [{ type: 'text', text: '旧资料正文' }, { type: 'file', receiptId: oldFile.receiptId }] } satisfies Submission<{ merchantId: string }>;
    await session.submit(input);
    const original = queued(agent.inbox.nextStep, input.requestId)!;
    const service = await app.host.service().open(session.id, program);
    const serviceInput = { ...input, requestId: 'following-facts', content: '排在后面的服务资料' };
    const forgedActor = { signal: new AbortController().signal, actorId: 'program-service' };
    assert.throws(() => service.submit(serviceInput, forgedActor), /REACTIVE_SERVICE_ACTOR_IMMUTABLE/u);
    await service.submit(serviceInput);
    await assert.rejects(session.submit(serviceInput, { actorId: 'program-service' }), /REACTIVE_REQUEST_ID_CONFLICT/u);
    await assert.rejects(session.editQueuedInput(serviceInput.requestId, '同名actor冒充服务', { actorId: 'program-service' }),
      error => error instanceof InputEditRejectedError && error.reason === 'actor-mismatch');
    assert.throws(() => service.editQueuedInput(serviceInput.requestId, '覆盖服务来源', forgedActor), /REACTIVE_SERVICE_ACTOR_IMMUTABLE/u);
    await service.editQueuedInput(serviceInput.requestId, '正式服务资料修订');
    await assert.rejects(session.editQueuedInput(input.requestId, '其他人编辑', { actorId: 'other-actor' }),
      error => error instanceof InputEditRejectedError && error.requestId === input.requestId && error.reason === 'actor-mismatch');
    await assert.rejects(session.editQueuedInput(input.requestId, '拒绝这次编辑'), /EDIT_POLICY_DENIED/u);
    assert.deepEqual(queued(agent.inbox.nextStep, input.requestId), original);
    const newFile = await session.uploadFile(new TextEncoder().encode('新资料'), { name: 'new.txt' });
    const next = [{ type: 'text' as const, text: '修订资料正文' }, { type: 'file' as const, receiptId: newFile.receiptId }];
    assert.deepEqual(await session.editQueuedInput(input.requestId, next), { sessionId: session.id, requestId: input.requestId, accepted: true });
    const expected = [{ type: 'text', text: '修订资料正文' }, { type: 'file', attachment: newFile.file }];
    assert.deepEqual(agent.inbox.nextStep.map(message => message.id), [original.id,
      queued(agent.inbox.nextStep, 'following-facts')!.id]);
    assert.deepEqual(queued(agent.inbox.nextStep, input.requestId), { ...original, content: expected });
    assert.equal(session.snapshot().requests[input.requestId]?.state, 'accepted');
    assert.equal(requests, 0);
    await session.submit(input); // 原首次提交身份仍是同一次请求，不能把旧正文重新发进队列。
    assert.deepEqual(queued(agent.inbox.nextStep, input.requestId)?.content, expected);
    const projected = app.native.sessionProjections.stateOf(agent.session, 'reactiveInputs')!;
    assert.equal(projected.pendingEdits[input.requestId], undefined);
    assert.deepEqual(projected.records[input.requestId]?.scope, input.scope);
    assert.equal(projected.records[input.requestId]?.actorId, 'operator-test');
    assert.equal(projected.records[serviceInput.requestId]?.participant, 'service');
    assert.deepEqual(admissions.filter(item => item.requestId === input.requestId).at(-1)?.content, expected);
    await session.submit({ requestId: 'read-facts', intent: 'followup', delivery: 'followup', scope: input.scope, content: '请核对这些资料' });
    await session.whenIdle();
    const consumed = agent.session.snapshotEvents().flatMap(event => event.type === 'user/message'
      && event.data.id === original.id ? [event.data] : []);
    assert.deepEqual(consumed.map(message => message.content), [expected]);
    assert.deepEqual(agent.session.snapshotEvents().flatMap(event => event.type === 'user/message'
      && event.data.source.kind === 'user' && 'rpcId' in event.data.source && event.data.source.rpcId === serviceInput.requestId
      ? [event.data.content] : []), [[{ type: 'text', text: '正式服务资料修订' }]]);
    assert.equal(app.native.fileUploads.resolve(agent, oldFile.receiptId), undefined);
    assert.equal(app.native.fileUploads.resolve(agent, newFile.receiptId), undefined);
    await assert.rejects(session.editQueuedInput(input.requestId, '消费后篡改'),
      error => error instanceof InputEditRejectedError && error.reason === 'unavailable');
    assert.deepEqual(app.errors, []);
    service.release(); session.release();
  } finally { await app.dispose(); }
});

test('编辑准入等待期间原生claim原输入，编辑不得改写已提取的输入或生成生效修订', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-edit-claimed-'));
  const entered = Promise.withResolvers<void>(), proceed = Promise.withResolvers<void>();
  const claimed = Promise.withResolvers<void>(), nextStep = Promise.withResolvers<void>();
  let requests = 0, held = true;
  const definition = defineApplication({ name: 'edit-claimed', admit: async input => {
    if (input.content.some(part => part.type === 'text' && part.text === '等待准入的修订')) { entered.resolve(); await proceed.promise; }
  }, environments: [defineEnvironment({ id: 'work', instructions: '只消费正式受理的资料',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component() {
      useBeforeStep(async signal => {
        if (!held) return;
        claimed.resolve();
        await Promise.race([nextStep.promise, new Promise<void>(resolve => signal.addEventListener('abort', () => resolve(), { once: true }))]);
        signal.throwIfAborted();
      }, []);
      return null;
    } }) })] });
  const app = await boot(join(root, 'home'), definition, async function* () { requests++; yield* text('原资料已消费'); });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const agent = app.native.agents.get(SessionId(session.id))!;
    await session.submit({ requestId: 'original-facts', intent: 'question', delivery: 'inject', scope: null, content: '原资料' });
    const original = queued(agent.inbox.nextStep, 'original-facts')!;
    const refused = assert.rejects(session.editQueuedInput('original-facts', '等待准入的修订'),
      error => error instanceof InputEditRejectedError && error.reason === 'unavailable');
    await entered.promise;
    await session.submit({ requestId: 'wake-facts', intent: 'followup', delivery: 'followup', scope: null, content: '请继续核对' });
    await claimed.promise;
    assert.equal(queued(agent.inbox.nextStep, 'original-facts'), undefined);
    assert.equal(agent.session.snapshotEvents().some(event => event.type === 'user/message' && event.data.id === original.id), false);
    proceed.resolve();
    await refused;
    assert.equal(agent.session.snapshotEvents().some(event => pluginRecordOf(event)?.type === 'plugin:reactive/input-edit'), false);
    assert.equal(requests, 0);
    held = false; nextStep.resolve();
    await session.whenIdle();
    assert.deepEqual(agent.session.snapshotEvents().flatMap(event => event.type === 'user/message' && event.data.id === original.id
      ? [event.data.content] : []), [[{ type: 'text', text: '原资料' }]]);
    assert.deepEqual(app.errors, []);
    session.release();
  } finally { proceed.resolve(); nextStep.resolve(); await app.dispose(); }
});

test('原生队列已替换后取消编辑等待，exact Agent仍完成最终持久回执', { timeout: 15_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-edit-accepted-'));
  const entered = Promise.withResolvers<void>(), release = Promise.withResolvers<void>();
  const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
  const definition = defineApplication({ name: 'edit-accepted', admit: async () => {}, environments: [defineEnvironment({ id: 'work',
    instructions: '编辑不唤醒执行，受理后持久化仍由原生会话完成。', program: implementProgram(program, { Component: () => null }) })] });
  let models = 0, stop = () => {};
  const app = await boot(join(root, 'home'), definition, async function* () {
    models++; throw new Error('QUEUED_EDIT_MUST_NOT_CALL_MODEL');
  });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const agent = app.native.agents.get(SessionId(session.id))!;
    const requestId = 'accepted-edit';
    await session.submit({ requestId, intent: 'question', delivery: 'inject', scope: null, content: '原正文' });
    const original = queued(agent.inbox.nextStep, requestId)!;
    let paused = false;
    stop = app.native.on('session/flush', async nativeSession => {
      const part = queued(agent.inbox.nextStep, requestId)?.content[0];
      if (nativeSession !== agent.session || paused
        || part?.type !== 'text' || part.text !== '已替换的新正文') return;
      paused = true; entered.resolve(); await release.promise;
    }, { global: true });
    const controller = new AbortController(), reason = new Error('EDIT_WAITER_CANCELLED_AFTER_REPLACE');
    const rejected = assert.rejects(session.editQueuedInput(requestId, '已替换的新正文', { signal: controller.signal }), error => error === reason);
    await entered.promise;
    assert.equal(queued(agent.inbox.nextStep, requestId)?.id, original.id);
    controller.abort(reason);
    assert.equal(await Promise.race([rejected.then(() => 'cancelled'), immediate().then(() => 'pending')]), 'cancelled');
    assert.equal(app.native.agents.get(SessionId(session.id)), agent);
    assert.deepEqual(queued(agent.inbox.nextStep, requestId)?.content, [{ type: 'text', text: '已替换的新正文' }]);
    let settled = false;
    const completion = session.whenIdle().then(() => { settled = true; });
    void completion.catch(() => {});
    await immediate();
    assert.equal(settled, false, '调用者退等后，whenIdle仍等待原Agent尚未完成的实际持久回执');
    release.resolve(); await completion;
    stop();
    await app.native.sessionPersistence.flush();
    const reader = await app.native.sessionPersistence.open(agent.id, 'read');
    try {
      const durable = await reader.read();
      assert.deepEqual(durable.events.flatMap(event => event.type === 'agent/inbox/spliced'
        ? event.data.inserted.filter(message => message.id === original.id).map(message => message.content) : []).at(-1),
      [{ type: 'text', text: '已替换的新正文' }]);
      assert.equal(durable.events.filter(event => {
        const record = pluginRecordOf(event);
        return record?.type === 'plugin:reactive/input-edit' && inputEditRecordSchema.parse(record.data).record.requestId === requestId;
      }).length, 1);
    } finally { await reader.close(); }
    assert.equal(models, 0); assert.deepEqual(app.errors, []);
    session.release();
  } finally { release.resolve(); stop(); await app.dispose(); }
});

test('已持久编辑准备但替换前取消不改原队列，实际消费及冷读取都保留原正文', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-edit-prepared-')), home = join(root, 'home');
  let requests = 0;
  const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
  const definition = defineApplication({ name: 'edit-prepared', admit: async () => {}, environments: [defineEnvironment({ id: 'work',
    instructions: '只消费已替换成功的正文', program: implementProgram(program, { Component: () => null }) })] });
  const respond = async function* () { requests++; yield* text('已核对受理正文'); };
  let app = await boot(home, definition, respond);
  let sessionId: string;
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    sessionId = session.id;
    const agent = app.native.agents.get(SessionId(session.id))!;
    const service = await app.host.service().open(session.id, program);
    await service.submit({ requestId: 'draft-facts', intent: 'question', delivery: 'inject', scope: null, content: '持久原正文' });
    const controller = new AbortController(), cancelled = new Error('EDIT_ABORTED_AFTER_DURABLE_PREPARE');
    let prepared = false;
    const stop = app.native.on('session/flush', async nativeSession => {
      if (nativeSession !== agent.session || prepared || !app.native.sessionProjections.stateOf(nativeSession, 'reactiveInputs')?.pendingEdits['draft-facts']) return;
      prepared = true;
      await app.native.sessionPersistence.flush();
      const reader = await app.native.sessionPersistence.open(agent.id, 'read');
      try {
        const durable = await reader.read();
        assert.ok(durable.events.some(event => {
          const record = pluginRecordOf(event);
          return record?.type === 'plugin:reactive/input-edit' && inputEditRecordSchema.parse(record.data).record.requestId === 'draft-facts';
        }));
        assert.deepEqual(durable.events.flatMap(event => event.type === 'agent/inbox/spliced'
          ? event.data.inserted.filter(message => message.source.kind === 'user'
            && 'rpcId' in message.source && message.source.rpcId === 'draft-facts').map(message => message.content) : []),
        [[{ type: 'text', text: '持久原正文' }]], '编辑准备的真实持久前缀没有替换原生队列正文');
      } finally { await reader.close(); }
      controller.abort(cancelled);
    }, { global: true });
    try { await assert.rejects(service.editQueuedInput('draft-facts', '不能自动生效的草稿', { signal: controller.signal }), error => error === cancelled); }
    finally { stop(); }
    assert.equal(prepared, true);
    assert.deepEqual(queued(agent.inbox.nextStep, 'draft-facts')?.content, [{ type: 'text', text: '持久原正文' }]);
    assert.equal(requests, 0);
    await session.submit({ requestId: 'wake-draft', intent: 'followup', delivery: 'followup', scope: null, content: '请核对正式原正文' });
    await session.whenIdle();
    assert.deepEqual(agent.session.snapshotEvents().flatMap(event => event.type === 'user/message'
      && event.data.source.kind === 'user' && 'rpcId' in event.data.source && event.data.source.rpcId === 'draft-facts'
      ? [event.data.content] : []), [[{ type: 'text', text: '持久原正文' }]]);
    assert.equal(app.native.sessionProjections.stateOf(agent.session, 'reactiveInputs')?.pendingEdits['draft-facts'], undefined);
    service.release(); session.release();
  } finally { await app.dispose(); }
  app = await boot(home, definition, respond);
  try {
    const session = await app.host.open(sessionId!, program);
    assert.equal(app.native.agents.get(SessionId(session.id)), undefined);
    assert.equal(session.snapshot().requests['draft-facts']?.state, 'settled');
    const reader = await app.native.sessionPersistence.open(SessionId(session.id), 'read');
    try {
      const durable = await reader.read();
      assert.deepEqual(durable.events.flatMap(event => event.type === 'user/message'
        && event.data.source.kind === 'user' && 'rpcId' in event.data.source && event.data.source.rpcId === 'draft-facts'
        ? [event.data.content] : []), [[{ type: 'text', text: '持久原正文' }]]);
      const [accepted] = durable.events.flatMap(event => {
        const record = pluginRecordOf(event);
        if (record?.type !== 'plugin:reactive/input') return [];
        const input = recordSchema.parse(record.data);
        return input.requestId === 'draft-facts' ? [input] : [];
      });
      assert.equal(accepted?.participant, 'service');
      assert.equal(accepted?.actorId, 'program-service');
    } finally { await reader.close(); }
    assert.equal(app.native.agents.get(SessionId(session.id)), undefined);
    assert.deepEqual(app.errors, []);
    session.release();
  } finally { await app.dispose(); }
});
