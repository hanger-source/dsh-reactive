import { test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SessionId, pluginRecordOf } from '@deepseek-ai/dsh-session';
import type { Context } from '@deepseek-ai/cordis';
import type { GenerateOptions } from '@deepseek-ai/dsh-llm';
import { NativeChildWork, nativeChildReceiptSchema, z, type NativeChildReceipt, type ActionInvocation } from '@hanger-source/dsh-reactive';
import { ChildCreationIncompleteError } from '@hanger-source/dsh-reactive-dsh';
import { bootDelegationFixture, DelegateWork, inspectDelegationSession } from './delegation-support.ts';
import { text, tool } from './native-support.ts';

type Inspection = Awaited<ReturnType<typeof inspectDelegationSession>>;
type Invocation = ActionInvocation<typeof DelegateWork>;
const confirmationSchema = z.object({ receipt: nativeChildReceiptSchema }).passthrough();
const followSchema = z.object({ id: z.string(), command: NativeChildWork.command }).passthrough();
function records<S extends z.ZodType>(inspection: Inspection, type: string, schema: S): z.output<S>[] {
  return inspection.events.slice(inspection.inheritedEventCount).flatMap(event => {
    const record = pluginRecordOf(event);
    return record?.type === type ? [schema.parse(record.data)] : [];
  });
}
function ownReceipt(inspection: Inspection) {
  return records(inspection, 'plugin:reactive/child-receipt', confirmationSchema)
    .find(value => value.receipt.childSessionId === inspection.header.id)?.receipt;
}
function followFor(inspection: Inspection, invocationId: string) {
  const link = records(inspection, 'plugin:reactive/operation-follow', followSchema)
    .find(value => value.command.invocationId === invocationId);
  assert.ok(link); return link;
}
const hasCall = (request: GenerateOptions, name: string) => request.messages.some(message =>
  message.content.some(block => block.type === 'tool-call' && block.name === name));

/** 复用同一正式场景；注入点只在模型供应商与真实原生持久边界。 */
async function bootRecovery(options: { root?: string; parentSessionId?: string; holdChild?: boolean; capacity?: number;
  forbidStart?: boolean; denyFollow?: boolean; installNative?(native: Context): void | Promise<void> } = {}) {
  const childEntered = Promise.withResolvers<void>(), childCompleted = Promise.withResolvers<void>();
  const releaseChild = Promise.withResolvers<void>();
  const returned = Promise.withResolvers<{ invocation: Invocation; receipt: NativeChildReceipt }>();
  const failed = Promise.withResolvers<{ invocation: Invocation; error: unknown }>();
  const state = { starts: 0, childCalls: 0 };
  let fixture!: Awaited<ReturnType<typeof bootDelegationFixture>>;
  fixture = await bootDelegationFixture('spawn', {
    root: options.root, parentSessionId: options.parentSessionId,
    subagents: { maxActiveSubagents: options.capacity ?? 8 },
    async installNative(native) {
      native.subagents.startActivation = new Proxy(native.subagents.startActivation, {
        apply(target, receiver, args) {
          state.starts++;
          if (options.forbidStart) throw new Error('RECOVERY_MUST_NOT_START_ANOTHER_CHILD');
          return Reflect.apply(target, receiver, args);
        },
      });
      await options.installNative?.(native);
    },
    authorize(admission) {
      assert.ok(admission.input.at instanceof Date);
      assert.deepEqual(admission.target, { parentId: 'business-parent' });
      if (options.denyFollow && admission.phase === 'follow') throw new Error('CHILD_FOLLOW_DENIED');
    },
    onInvocation(invocation) {
      void invocation.result.then(receipt => returned.resolve({ invocation, receipt }), error => failed.resolve({ invocation, error }));
    },
    onChildComplete() { childCompleted.resolve(); },
    async *respond(request) {
      if (request.tools?.some(value => value.name === 'child_complete')) {
        state.childCalls++; childEntered.resolve();
        if (hasCall(request, 'child_complete')) { yield* text('原子工作完成'); return; }
        if (options.holdChild) await releaseChild.promise;
        yield* tool('child_complete', 'recovery-child-complete', {}); return;
      }
      const calls = request.messages.flatMap(message => message.content)
        .filter(block => block.type === 'tool-call' && block.name === 'delegate_work').length;
      const secondInput = request.messages.some(message => message.content.some(block =>
        block.type === 'text' && block.text.includes('create-second-child')));
      if (!calls || (calls === 1 && secondInput)) yield* tool('delegate_work', 'recovery-delegate-' + (calls + 1), fixture.rawInput);
      else yield* text('已核验子工作反馈');
    },
  });
  return { ...fixture, state, childEntered: childEntered.promise, childCompleted: childCompleted.promise,
    returned: returned.promise, failed: failed.promise, releaseChild: () => releaseChild.resolve(),
    start: () => fixture.parent.submit({ requestId: 'original-input', content: '创建实际独立子工作', intent: 'question', delivery: 'followup' as const,
      scope: { parentId: fixture.rawScope } }),
  };
}

const checkpointSchema = z.object({ parentId: z.string(), receipt: nativeChildReceiptSchema, initial: z.json() }).strict();
async function checkpointWorker(root: string) {
  let stopped = false;
  const fixture = await bootRecovery({ root, holdChild: true, installNative(native) {
    const flush = native.sessions.flush.bind(native.sessions);
    native.sessions.flush = async session => {
      const result = await flush(session);
      if (stopped || !session.header.parentSession) return result;
      const saved = await inspectDelegationSession(native, session.id), receipt = ownReceipt(saved);
      if (!receipt) return result;
      stopped = true;
      const initial = saved.events.flatMap(event => event.type === 'agent/inbox/spliced' ? event.data.inserted : [])
        .find(message => message.id === receipt.messageId);
      assert.ok(initial);
      assert.equal(saved.events.some(event => event.type === 'user/message' && event.data.id === receipt.messageId), false);
      const queues: Record<string, string[]> = { 'next-turn': [], 'next-step': [] };
      let claimed = false;
      for (const event of saved.events.slice(saved.inheritedEventCount)) if (event.type === 'agent/inbox/spliced') {
        const removed = queues[event.data.target]!.splice(event.data.start, event.data.removedCount ?? 0,
          ...event.data.inserted.map(message => message.id));
        if (removed.includes(receipt.messageId)) { assert.notEqual(event.data.outcome, 'canceled'); claimed = true; }
      }
      assert.equal(claimed, true, '故障必须落在原消息已claim而未消费的边界');
      const parent = await inspectDelegationSession(native, session.header.parentSession);
      assert.equal(records(parent, 'plugin:reactive/child-receipt', confirmationSchema).length, 0);
      assert.equal(parent.events.filter(event => event.type === 'subagent/catalog' && event.data.childId === session.id).length, 1);
      process.send!(checkpointSchema.parse({ parentId: session.header.parentSession, receipt, initial }));
      await new Promise<void>(() => {});
      return result;
    };
  } });
  await fixture.start();
  await new Promise<void>(() => {});
}
async function crashAtCheckpoint(root: string, context: TestContext) {
  const worker = fork(import.meta.filename, ['--child-receipt-checkpoint', root], {
    execPath: process.execPath, execArgv: [], stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  });
  let output = '';
  worker.stdout?.on('data', value => { output += String(value); });
  worker.stderr?.on('data', value => { output += String(value); });
  const checkpoint = new Promise<z.output<typeof checkpointSchema>>((resolve, reject) => {
    worker.once('message', message => { try { resolve(checkpointSchema.parse(message)); } catch (error) { reject(error); } });
    worker.once('error', reject);
    worker.once('exit', (code, signal) => reject(new Error(`CHILD_CHECKPOINT_EARLY_EXIT ${code}/${signal}\n${output}`)));
  });
  const abort = () => { worker.kill('SIGKILL'); };
  context.signal.addEventListener('abort', abort, { once: true });
  try {
    const saved = await checkpoint, exited = once(worker, 'exit');
    worker.kill('SIGKILL'); assert.deepEqual(await exited, [null, 'SIGKILL']);
    return saved;
  } finally { context.signal.removeEventListener('abort', abort); worker.kill('SIGKILL'); }
}

if (process.argv[2] === '--child-receipt-checkpoint') await checkpointWorker(process.argv[3]!);
else {
  test('子receipt持久而父ACK尚未持久时硬崩溃，只恢复同一原消息与独立Goal', async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-child-crash-'));
    const checkpoint = await crashAtCheckpoint(root, context);
    const fixture = await bootRecovery({ root, parentSessionId: checkpoint.parentId, forbidStart: true });
    try {
      const parent = await fixture.runtime.application.resume(checkpoint.parentId, fixture.parentProgram);
      await fixture.childCompleted;
      await parent.whenIdle();
      const child = await fixture.runtime.application.open(checkpoint.receipt.childSessionId, fixture.childProgram);
      try {
        assert.equal(child.snapshot().goal?.phase, 'complete');
        assert.equal(child.snapshot().goal?.id, checkpoint.receipt.goal.id);
      } finally { child.release(); }
      const saved = await inspectDelegationSession(fixture.runtime.native, checkpoint.receipt.childSessionId);
      const original = saved.events.filter(event => event.type === 'user/message' && event.data.id === checkpoint.receipt.messageId);
      assert.equal(original.length, 1); assert.deepEqual(original[0]!.data, checkpoint.initial);
      assert.equal(saved.events.slice(saved.inheritedEventCount).filter(event => event.type === 'goal/change' && event.data.operation === 'create').length, 1);
      const parentLog = await inspectDelegationSession(fixture.runtime.native, parent.id);
      assert.equal(records(parentLog, 'plugin:reactive/delegation', z.json()).length, 1);
      assert.equal(records(parentLog, 'plugin:reactive/child-receipt', confirmationSchema).length, 1);
      assert.equal(parentLog.events.filter(event => event.type === 'subagent/catalog').length, 1);
      assert.equal(fixture.state.starts, 0); assert.deepEqual(fixture.errors, []);
      parent.release(); fixture.parent.release();
    } finally { fixture.releaseChild(); await fixture.dispose(); }
  });

  test('受理后父receipt单次拒写保留首步gate，显式resume修复且故障feedback身份保留', async () => {
    let rejected = false;
    const fixture = await bootRecovery({ installNative(native) {
      const flush = native.sessions.flush.bind(native.sessions);
      native.sessions.flush = async session => {
        if (!rejected && !session.header.parentSession && session.snapshotEvents().some(event =>
          pluginRecordOf(event)?.type === 'plugin:reactive/child-receipt')) { rejected = true; throw new Error('PARENT_RECEIPT_WRITE_FAILED'); }
        return flush(session);
      };
    } });
    try {
      await fixture.start(); const failure = await fixture.failed;
      assert.ok(failure.error instanceof ChildCreationIncompleteError);
      const reference = failure.error.reference;
      const before = await inspectDelegationSession(fixture.runtime.native, reference.childSessionId), receipt = ownReceipt(before);
      assert.ok(receipt); assert.equal(fixture.state.childCalls, 0);
      assert.equal(before.events.some(event => event.type === 'agent/inbox/spliced' && event.data.outcome === 'canceled'), false);
      const command = { parentSessionId: fixture.parent.id, invocationId: failure.invocation.id };
      const located = await fixture.parent.locateOperation(NativeChildWork, command);
      assert.equal(located.status, 'accepted'); if (located.status === 'accepted') assert.equal(located.state.phase, 'creation-incomplete');
      const link = followFor(await inspectDelegationSession(fixture.runtime.native, fixture.parent.id), failure.invocation.id);
      const feedback = await fixture.parent.readOperationFeedback(NativeChildWork, command, link.id);
      assert.equal(feedback.status, 'ready'); if (feedback.status === 'ready') assert.equal(feedback.node.payload.phase, 'creation-incomplete');
      const child = await fixture.runtime.application.resume(reference.childSessionId, fixture.childProgram);
      await fixture.childCompleted; await fixture.parent.whenIdle();
      assert.equal(child.snapshot().goal?.id, receipt.goal.id); assert.equal(child.snapshot().goal?.phase, 'complete');
      const after = await inspectDelegationSession(fixture.runtime.native, reference.childSessionId);
      assert.equal(after.events.filter(event => event.type === 'user/message' && event.data.id === receipt.messageId).length, 1);
      const retained = await fixture.parent.readOperationFeedback(NativeChildWork, command, link.id);
      assert.equal(retained.status, 'ready');
      if (retained.status === 'ready' && feedback.status === 'ready') assert.equal(retained.node.id, feedback.node.id);
      assert.equal(fixture.state.starts, 1); assert.deepEqual(fixture.errors, []);
      child.release(); fixture.parent.release();
    } finally { fixture.releaseChild(); await fixture.dispose(); }
  });

  test('首次Follow拒权不会登记原生等待或启动子Agent', async () => {
    const fixture = await bootRecovery({ denyFollow: true });
    try {
      await fixture.start(); const failure = await fixture.failed; await fixture.parent.whenIdle();
      assert.ok(failure.error instanceof Error); assert.equal(failure.error.message, 'CHILD_FOLLOW_DENIED');
      assert.ok(fixture.admissions.some(value => value.phase === 'create'));
      assert.ok(fixture.admissions.some(value => value.phase === 'follow'));
      assert.equal(fixture.parent.snapshot().waits.length, 0); assert.equal(fixture.state.starts, 0);
      assert.equal(records(await inspectDelegationSession(fixture.runtime.native, fixture.parent.id), 'plugin:reactive/delegation', z.json()).length, 0);
      assert.deepEqual(fixture.errors, []); fixture.parent.release();
    } finally { fixture.releaseChild(); await fixture.dispose(); }
  });

  test('原生容量在创建前拒绝时，以未创建的保留child身份反馈creation-incomplete', async () => {
    const fixture = await bootRecovery({ capacity: 1, holdChild: true });
    try {
      await fixture.start(); await fixture.childEntered; await fixture.returned;
      await fixture.parent.submit({ requestId: 'second-input', content: 'create-second-child', intent: 'question', delivery: 'followup' as const, scope: { parentId: fixture.rawScope } });
      const failure = await fixture.failed; assert.ok(failure.error instanceof ChildCreationIncompleteError);
      assert.equal(await fixture.runtime.native.sessionPersistence.stat(SessionId(failure.error.reference.childSessionId)), undefined);
      const command = { parentSessionId: fixture.parent.id, invocationId: failure.invocation.id };
      const located = await fixture.parent.locateOperation(NativeChildWork, command);
      assert.equal(located.status, 'accepted'); if (located.status === 'accepted') assert.equal(located.state.phase, 'creation-incomplete');
      const link = followFor(await inspectDelegationSession(fixture.runtime.native, fixture.parent.id), failure.invocation.id);
      const feedback = await fixture.parent.readOperationFeedback(NativeChildWork, command, link.id);
      assert.equal(feedback.status, 'ready'); if (feedback.status === 'ready') assert.equal(feedback.node.payload.phase, 'creation-incomplete');
      assert.equal(fixture.state.childCalls, 1);
      fixture.releaseChild(); await fixture.parent.whenIdle(); assert.deepEqual(fixture.errors, []);
      fixture.parent.release();
    } finally { fixture.releaseChild(); await fixture.dispose(); }
  });
}
