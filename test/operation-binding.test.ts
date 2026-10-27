import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { z } from 'zod';
import { bindOperation, defineOperation, encodeOperationFeedback, implementOperation, operationFollowLinkSchema, type OperationBindingOptions } from '@hanger-source/dsh-reactive';
import { readOperationFeedback } from '@hanger-source/dsh-reactive/kernel';

test('已取得的参与按原命令读取持久节点，组件换目标后仍可读，读取不登记或重查操作', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-participation-'));
  const recordFile = join(directory, 'registered-feedback.json');
  let decodes = 0, encodes = 0, locates = 0, registrations = 0;
  const time = z.codec(z.iso.datetime(), z.date(), {
    decode: raw => { decodes++; return new Date(raw); },
    encode: value => { encodes++; return value.toISOString(); },
  });
  const definition = defineOperation({ kind: 'knowledge-observation', version: 1,
    command: z.object({ taskId: z.string() }).strict(), reference: z.string(),
    state: z.object({ status: z.literal('running') }), settled: () => undefined,
    feedback: z.object({ status: z.literal('unavailable'), observedAt: time }).strict(),
  });
  const recordSchema = z.object({ link: operationFollowLinkSchema,
    node: z.object({ id: z.string(), payload: z.object({ status: z.literal('unavailable'), observedAt: z.iso.datetime() }) }) });
  const link = operationFollowLinkSchema.parse({ id: 'participation-a', operation: { kind: definition.kind, version: definition.version },
    command: { taskId: 'task-a' }, when: 'feedback', after: null, mode: 'wait',
    continuation: { sessionId: 'session-a', inputs: [], goal: { id: 'goal-a', revision: 1 } },
    wait: { id: 'wait-a', operation: { taskId: 'task-a' } }, scope: { taskId: 'task-a' }, target: { taskId: 'task-a' },
    source: { kind: 'tool', callId: 'call-a', rootCallId: 'call-a', parentCallId: null } });
  await writeFile(recordFile, JSON.stringify({ link,
    node: { id: 'observation-a', payload: { status: 'unavailable', observedAt: '2026-10-27T00:00:00.000Z' } } }));
  const owner = implementOperation(definition, {
    locate() { locates++; return { status: 'unknown' }; },
    observe() { throw new Error('FEEDBACK_READ_MUST_NOT_OBSERVE_STATE'); },
    async registerFollow() { registrations++; throw new Error('READ_MUST_NOT_REGISTER'); },
    async readFeedback(command, persisted, access) {
      assert.equal(access.source.kind, 'participant');
      assert.equal(Object.isFrozen(command), true);
      assert.equal(Object.isFrozen(persisted), true);
      const record = recordSchema.parse(JSON.parse(await readFile(recordFile, 'utf8')));
      assert.deepEqual(persisted, record.link);
      assert.deepEqual(command, record.link.command);
      return { status: 'ready', reference: command.taskId, node: record.node };
    },
  });
  const access = { sessionId: 'session-a', signal: new AbortController().signal,
    source: { kind: 'participant' as const, participant: 'human' as const, actorId: 'reader-a', entrypoint: 'sdk' } };
  const options: OperationBindingOptions<typeof definition> = {
    access: () => access,
    async readFeedback(binding, id, reader) {
      // 宿主从持久记录解析真实关联，调用者只给参与 id。
      const record = recordSchema.parse(JSON.parse(await readFile(recordFile, 'utf8')));
      if (id !== record.link.id) throw new Error('PARTICIPATION_NOT_REGISTERED');
      return readOperationFeedback(owner, binding.command, record.link, reader);
    },
  };
  const command = { taskId: 'task-a' };
  let current = bindOperation(owner, command, options);
  const original = current, participation = current.participation(link.id);
  command.taskId = 'task-b';
  current = bindOperation(owner, command, options);
  assert.equal(locates + registrations + decodes, 0);
  assert.equal(current.command.taskId, 'task-b');
  assert.equal(Object.isFrozen(original.command), true);
  const result = await participation.readFeedback();
  assert.equal(result.status, 'ready');
  if (result.status !== 'ready') throw new Error('PERSISTED_FEEDBACK_REQUIRED');
  assert.equal(participation.command.taskId, 'task-a');
  assert.equal(result.reference, 'task-a');
  assert.equal(result.node.payload.observedAt.toISOString(), '2026-10-27T00:00:00.000Z');
  assert.equal(decodes, 1);
  assert.deepEqual(encodeOperationFeedback(definition, result.node), {
    id: 'observation-a', payload: { status: 'unavailable', observedAt: '2026-10-27T00:00:00.000Z' },
  });
  assert.equal(decodes, 1);
  assert.equal(encodes, 1);
  assert.equal(locates + registrations, 0);
  await assert.rejects(current.participation('fiction').readFeedback(), /PARTICIPATION_NOT_REGISTERED/u);
});

test('反馈读取保留来源快照与取消原因，旧节点不能冒充后继，授权查询异常不变为领域结果', async () => {
  const definition = defineOperation({ kind: 'next-observation', version: 1, command: z.string(), reference: z.string(),
    state: z.object({ status: z.literal('running') }), settled: () => undefined, feedback: z.string() });
  const link = operationFollowLinkSchema.parse({ id: 'participation-next', operation: { kind: definition.kind, version: 1 },
    command: 'task-a', when: 'feedback', after: 'observation-1', mode: 'wait',
    continuation: { sessionId: 'session-a', inputs: [], goal: { id: 'goal-a', revision: 2 } },
    wait: { id: 'wait-next', operation: 'task-a' }, scope: null, target: 'task-a',
    source: { kind: 'tool', callId: 'call-next', rootCallId: 'call-next', parentCallId: null } });
  if (link.when !== 'feedback') throw new Error('FEEDBACK_LINK_REQUIRED');
  const entered = Promise.withResolvers<void>(), release = Promise.withResolvers<void>(), finished = Promise.withResolvers<void>();
  let observedActor: string | undefined, observedAfter: string | null | undefined;
  let mode: 'waiting' | 'same' | 'next' | 'denied' = 'waiting';
  const denied = new Error('FOLLOW_READ_NOT_AUTHORIZED');
  const owner = implementOperation(definition, {
    locate() { throw new Error('FEEDBACK_MUST_NOT_LOCATE'); },
    observe() { throw new Error('FEEDBACK_MUST_NOT_OBSERVE'); },
    async readFeedback(_command, persisted, access) {
      if (mode === 'denied') throw denied;
      if (mode === 'waiting') {
        entered.resolve(); await release.promise;
        observedActor = access.source.kind === 'participant' ? access.source.actorId : undefined;
        observedAfter = persisted.after;
        finished.resolve();
      }
      return { status: 'ready', reference: 'task-a',
        node: { id: mode === 'same' ? 'observation-1' : 'observation-2', payload: 'unavailable' } };
    },
  });
  const source = { kind: 'participant' as const, participant: 'human' as const, actorId: 'reader-a', entrypoint: 'sdk' };
  const controller = new AbortController(), reason = new Error('只取消此次反馈读取');
  const access = { sessionId: 'session-a', source, signal: controller.signal };
  const waiting = readOperationFeedback(owner, 'task-a', link, access);
  await entered.promise;
  source.actorId = 'other-reader'; link.after = 'other-node'; controller.abort(reason);
  await assert.rejects(waiting, error => error === reason);
  release.resolve();
  await finished.promise;
  assert.equal(observedActor, 'reader-a');
  assert.equal(observedAfter, 'observation-1');
  const freshAccess = { ...access, signal: new AbortController().signal };
  const originalLink = { ...link, after: 'observation-1' };
  mode = 'same';
  await assert.rejects(readOperationFeedback(owner, 'task-a', originalLink, freshAccess), /REACTIVE_OPERATION_FEEDBACK_NOT_SUCCESSOR/u);
  mode = 'next';
  assert.deepEqual(await readOperationFeedback(owner, 'task-a', originalLink, freshAccess), {
    status: 'ready', reference: 'task-a', node: { id: 'observation-2', payload: 'unavailable' },
  });
  mode = 'denied';
  await assert.rejects(readOperationFeedback(owner, 'task-a', originalLink, freshAccess), error => error === denied);
});
