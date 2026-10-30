import { test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate, setTimeout as delay } from 'node:timers/promises';
import { inspect } from 'node:util';
import { SessionId, pluginRecordOf } from '@deepseek-ai/dsh-session';
import type { Agent } from '@deepseek-ai/dsh-agent';
import type { Context } from '@deepseek-ai/cordis';
import type { GenerateOptions } from '@deepseek-ai/dsh-llm';
import { NativeChildWork, nativeChildReceiptSchema, defineProgram, programIdentity, SessionProgramMismatchError, z, type NativeChildReceipt, type ActionInvocation } from '@hanger-source/dsh-reactive';
import { ChildCreationIncompleteError } from '@hanger-source/dsh-reactive-dsh';
import { bootDelegationFixture, ChildProgram, DelegateWork, inspectDelegationSession, type DelegationFixtureOptions } from './delegation-support.ts';
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
function hasCause(error: unknown, matches: (error: Error) => boolean): boolean {
  const seen = new Set<unknown>();
  while (error instanceof Error && !seen.has(error)) {
    if (matches(error)) return true;
    seen.add(error); error = error.cause;
  }
  return false;
}

/** 复用同一正式场景；注入点只在模型供应商与真实原生持久边界。 */
async function bootRecovery(options: { root?: string; parentSessionId?: string; holdChild?: boolean; capacity?: number;
  childProgram?: typeof ChildProgram; forbidStart?: boolean; denyFollow?: boolean; authorize?: DelegationFixtureOptions['authorize']; installNative?(native: Context): void | Promise<void> } = {}) {
  const childEntered = Promise.withResolvers<void>(), childCompleted = Promise.withResolvers<void>();
  const releaseChild = Promise.withResolvers<void>();
  const returned = Promise.withResolvers<{ invocation: Invocation; receipt: NativeChildReceipt }>();
  const failed = Promise.withResolvers<{ invocation: Invocation; error: unknown }>();
  const state = { starts: 0, childCalls: 0, modelCalls: 0 };
  let fixture!: Awaited<ReturnType<typeof bootDelegationFixture>>;
  fixture = await bootDelegationFixture('spawn', {
    root: options.root, parentSessionId: options.parentSessionId, childProgram: options.childProgram,
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
      return options.authorize?.(admission);
    },
    onInvocation(invocation) {
      void invocation.result.then(receipt => returned.resolve({ invocation, receipt }), error => failed.resolve({ invocation, error }));
    },
    onChildComplete() { childCompleted.resolve(); },
    async *respond(request) {
      state.modelCalls++;
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
async function consumedCheckpointWorker(root: string, pauseChild: boolean) {
  const fixture = await bootRecovery({ root, holdChild: true });
  await fixture.start();
  const { receipt } = await fixture.returned;
  await fixture.childEntered;
  const native = fixture.runtime.native;
  const child = native.agents.get(SessionId(receipt.childSessionId))!;
  const goal = native.goals.get(child)!;
  const edited = native.goals.edit(child, goal, { objective: 'EDITED_CHILD_OBJECTIVE' });
  if (pauseChild) native.goals.pause(child, edited);
  const parent = native.agents.get(SessionId(fixture.parent.id))!;
  const parentGoal = native.goals.create(parent, { objective: '暂停的父目标不撤销已受理独立子工作' });
  native.goals.pause(parent, parentGoal);
  assert.equal(await native.sessions.flush(child.session), true);
  assert.equal(await native.sessions.flush(parent.session), true);
  const saved = await inspectDelegationSession(native, child.id);
  const initial = saved.events.find(event => event.type === 'user/message' && event.data.id === receipt.messageId);
  assert.ok(initial?.type === 'user/message');
  process.send!(checkpointSchema.parse({ parentId: parent.id, receipt, initial: initial.data }));
  await new Promise<void>(() => {});
}
async function crashAtCheckpoint(root: string, context: TestContext, mode = '--child-receipt-checkpoint') {
  const worker = fork(import.meta.filename, [mode, root], {
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

/** 先join本场景已开始的真实模型，再退出宿主；断言与清理错误共同保留。 */
async function finishRecovery(fixture: Awaited<ReturnType<typeof bootRecovery>>, childId: string, failures: unknown[]) {
  fixture.releaseChild();
  for (const id of [childId, fixture.parent.id]) {
    if (id === childId && fixture.state.childCalls === 0) continue; // 未过gate的准备由宿主退出负责，不等待它凭空进入模型。
    try { await fixture.runtime.native.agents.get(SessionId(id))?.whenIdle(); } catch (error) { failures.push(error); }
  }
  try { fixture.parent.release(); } catch (error) { failures.push(error); }
  try { await fixture.dispose(); } catch (error) {
    console.error('delegation.recovery.cleanup.failed', inspect(error, { depth: null }));
    failures.push(error);
  }
  if (failures.length === 1) throw failures[0];
  if (failures.length > 1) throw new AggregateError(failures, 'RECOVERY_SCENARIO_FAILED');
}

if (process.argv[2] === '--child-receipt-checkpoint') await checkpointWorker(process.argv[3]!);
else if (process.argv[2] === '--child-consumed-checkpoint') await consumedCheckpointWorker(process.argv[3]!, false);
else if (process.argv[2] === '--child-paused-checkpoint') await consumedCheckpointWorker(process.argv[3]!, true);
else {
  test('宿主退出先撤销委托观察并等待已开始flush，再关闭真实child持久handle', async () => {
    const entered = Promise.withResolvers<void>(), released = Promise.withResolvers<void>(), closed = Promise.withResolvers<void>();
    let armed = false, observerReleased = false, closeBeforeFlush = false, childId = '';
    const fixture = await bootRecovery({ holdChild: true, installNative(native) {
      const create = native.sessionPersistence.create;
      native.sessionPersistence.create = new Proxy(create, { async apply(target, receiver, args) {
        const handle: Awaited<ReturnType<typeof create>> = await Reflect.apply(target, receiver, args);
        if (args[0].parentSession) {
          const close = handle.close.bind(handle);
          handle.close = async () => {
            if (handle.header.id === childId) {
              closeBeforeFlush = !observerReleased;
              closed.resolve();
            }
            await close();
          };
        }
        return handle;
      } });
      const flush = native.sessions.flush.bind(native.sessions);
      native.sessions.flush = async session => {
        if (armed && session.id === childId) {
          armed = false; entered.resolve(); await released.promise;
        }
        return flush(session);
      };
    } });
    const failures: unknown[] = [];
    let disposing: Promise<void> | undefined;
    try {
      await fixture.start();
      const { receipt } = await fixture.returned;
      childId = receipt.childSessionId;
      await fixture.childEntered;
      const child = fixture.runtime.native.agents.get(SessionId(childId)); assert.ok(child);
      const goal = fixture.runtime.native.goals.get(child); assert.ok(goal);
      armed = true;
      fixture.runtime.native.goals.edit(child, goal, { maxGoalRounds: goal.maxGoalRounds + 1 });
      await entered.promise;
      const models = fixture.state.modelCalls;
      disposing = fixture.dispose();
      // 观察实际writer关闭边界；旧实现会越过仍挂起的observer flush关闭handle。
      assert.equal(await Promise.race([closed.promise.then(() => 'closed'), delay(50, 'waiting')]), 'waiting');
      observerReleased = true; released.resolve();
      fixture.releaseChild();
      await disposing; await closed.promise;
      assert.equal(closeBeforeFlush, false);
      assert.equal(fixture.state.modelCalls, models, '退出后完成flush不能再发布更新并唤醒模型');
    } catch (error) { failures.push(error); }
    finally {
      observerReleased = true; released.resolve();
      fixture.releaseChild();
      fixture.parent.release();
      try { await (disposing ?? fixture.dispose()); } catch (error) { failures.push(error); }
      if (failures.length === 1) throw failures[0];
      if (failures.length > 1) throw new AggregateError(failures, 'OBSERVER_CLOSE_SCENARIO_FAILED');
    }
  });

  test('已消费子工作恢复必须向authorizer提供当前编辑目标，拒权不能借原委托继续模型', async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-child-consumed-'));
    const checkpoint = await crashAtCheckpoint(root, context, '--child-consumed-checkpoint');
    const denied = new Error('EDITED_CHILD_DENIED');
    const fixture = await bootRecovery({ root, parentSessionId: checkpoint.parentId, forbidStart: true, holdChild: true,
      authorize(admission) {
        if (admission.phase === 'resume' && admission.recovery.goal?.objective === 'EDITED_CHILD_OBJECTIVE') {
          assert.equal(admission.recovery.input, 'consumed');
          assert.equal(admission.recovery.child.childSessionId, checkpoint.receipt.childSessionId);
          assert.equal(admission.recovery.messageId, checkpoint.receipt.messageId);
          assert.notEqual(admission.objective, admission.recovery.goal.objective);
          throw denied;
        }
      },
    });
    const failures: unknown[] = [];
    try {
      await assert.rejects(fixture.runtime.application.resume(checkpoint.parentId, fixture.parentProgram), error => hasCause(error, cause => cause === denied));
      assert.equal(fixture.state.childCalls, 0);
      assert.equal(fixture.runtime.native.agents.get(SessionId(checkpoint.receipt.childSessionId)), undefined);
    } catch (error) { failures.push(error); }
    finally { await finishRecovery(fixture, checkpoint.receipt.childSessionId, failures); }
  });

  test('恢复授权挂起期间同id编辑Goal不能借旧授权继续，父暂停不取消独立child', async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-child-goal-cas-'));
    const checkpoint = await crashAtCheckpoint(root, context, '--child-consumed-checkpoint');
    const entered = Promise.withResolvers<void>(), released = Promise.withResolvers<void>();
    let edited = false;
    const fixture = await bootRecovery({ root, parentSessionId: checkpoint.parentId, forbidStart: true, holdChild: true,
      async authorize(admission) {
        if (admission.phase !== 'resume' || edited) return;
        edited = true; entered.resolve(); await released.promise;
      },
    });
    const failures: unknown[] = [];
    try {
      const restoring = fixture.runtime.application.resume(checkpoint.parentId, fixture.parentProgram).then(
        value => ({ value, error: null }), error => ({ value: null, error }));
      await entered.promise;
      const child = fixture.runtime.native.agents.get(SessionId(checkpoint.receipt.childSessionId))!;
      const before = fixture.runtime.native.goals.get(child)!;
      fixture.runtime.native.goals.edit(child, before, { objective: 'EDITED_DURING_AUTHORIZATION' });
      released.resolve();
      const outcome = await restoring;
      assert.ok(outcome.error instanceof Error);
      assert.ok(hasCause(outcome.error, cause => cause.message === 'REACTIVE_CHILD_RECOVERY_CHANGED'));
      assert.equal(fixture.state.childCalls, 0);
      const parent = await fixture.runtime.application.resume(checkpoint.parentId, fixture.parentProgram);
      await fixture.childEntered;
      assert.equal(parent.snapshot().goal?.phase, 'paused');
      assert.equal(fixture.state.childCalls, 1, '新的明确恢复授权允许独立child继续');
      fixture.releaseChild();
      await parent.whenIdle();
      parent.release();
    } catch (error) { failures.push(error); }
    finally { released.resolve(); await finishRecovery(fixture, checkpoint.receipt.childSessionId, failures); }
  });

  test('子Goal持久暂停时父恢复不重启子工作', async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-child-paused-'));
    const checkpoint = await crashAtCheckpoint(root, context, '--child-paused-checkpoint');
    const fixture = await bootRecovery({ root, parentSessionId: checkpoint.parentId, forbidStart: true });
    try {
      const parent = await fixture.runtime.application.resume(checkpoint.parentId, fixture.parentProgram);
      await parent.whenIdle();
      assert.equal(fixture.state.childCalls, 0);
      assert.equal(fixture.runtime.native.agents.get(SessionId(checkpoint.receipt.childSessionId)), undefined);
      const child = await fixture.runtime.application.open(checkpoint.receipt.childSessionId, fixture.childProgram);
      assert.equal(child.snapshot().goal?.phase, 'paused');
      child.release(); parent.release();
    } finally { fixture.parent.release(); await fixture.dispose(); }
  });
  test('子receipt持久而父ACK尚未持久时硬崩溃，父恢复失败与并发等待完整撤回后只恢复原消息和Goal', async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-child-crash-'));
    const checkpoint = await crashAtCheckpoint(root, context);
    const incompatible = defineProgram({ ...ChildProgram, version: ChildProgram.version + 1 });
    const childResumes: string[] = [];
    const changed = await bootRecovery({ root, parentSessionId: checkpoint.parentId, childProgram: incompatible, forbidStart: true,
      installNative(native) {
        const resume = native.agents.resume;
        native.agents.resume = new Proxy(resume, { apply(target, receiver, args) {
          if (args[0].resumeSessionId === checkpoint.receipt.childSessionId) childResumes.push(args[0].resumeSessionId);
          return Reflect.apply(target, receiver, args);
        } });
      },
    });
    try {
      await assert.rejects(changed.runtime.application.resume(checkpoint.parentId, changed.parentProgram), error => error instanceof SessionProgramMismatchError
        && error.sessionId === checkpoint.receipt.childSessionId);
      assert.equal(changed.runtime.native.agents.get(SessionId(checkpoint.parentId)), undefined,
        '父恢复在steer子会话之前拒绝换约，并完成父Agent回滚');
      for (const method of ['open', 'resume'] as const) {
        await assert.rejects(changed.runtime.application[method](checkpoint.receipt.childSessionId, incompatible), error => {
          assert.ok(error instanceof SessionProgramMismatchError);
          assert.equal(error.sessionId, checkpoint.receipt.childSessionId);
          assert.deepEqual(error.recorded, programIdentity(ChildProgram));
          assert.deepEqual(error.current, programIdentity(incompatible));
          return true;
        });
      }
      assert.equal(changed.runtime.native.agents.get(SessionId(checkpoint.receipt.childSessionId)), undefined);
      assert.deepEqual(childResumes, [], 'Program拒绝发生在原生child resume之前，不能靠创建后回滚掩盖');
      assert.equal(changed.state.modelCalls, 0); assert.equal(changed.state.starts, 0);
      assert.deepEqual(changed.errors, []);
    } finally { changed.releaseChild(); await changed.dispose(); }
    const restoreEntered = Promise.withResolvers<void>(), restoreReleased = Promise.withResolvers<void>();
    const failure = new Error('PARENT_CHILD_RESTORE_READ_FAILED');
    const resumedParents: Agent[] = [], closedParents = new Set<Agent>();
    let rejected = false;
    const releaseRestore = () => restoreReleased.resolve();
    context.signal.addEventListener('abort', releaseRestore, { once: true });
    const fixture = await bootRecovery({ root, parentSessionId: checkpoint.parentId, forbidStart: true, installNative(native) {
      const resume = native.agents.resume;
      native.agents.resume = new Proxy(resume, { async apply(target, receiver, args) {
        const handle: Awaited<ReturnType<typeof resume>> = await Reflect.apply(target, receiver, args);
        if (handle.agent.id === checkpoint.parentId) {
          resumedParents.push(handle.agent);
          handle.agent.ctx.effect(() => () => { closedParents.add(handle.agent); });
        }
        return handle;
      } });
      const open = native.sessionPersistence.open;
      native.sessionPersistence.open = new Proxy(open, { async apply(target, receiver, args) {
        // 只有原生resume实际成功返回后的parent只读日志才注入；不干预Agent自身恢复。
        if (!rejected && resumedParents.length && args[0] === checkpoint.parentId && args[1] === 'read') {
          rejected = true; restoreEntered.resolve(); await restoreReleased.promise; throw failure;
        }
        return Reflect.apply(target, receiver, args);
      } });
    } });
    try {
      const coldReader = fixture.parent.actionManifest;
      await coldReader.read();
      const originalBinding = fixture.parent.resolveAction('delegate', fixture.rawScope);
      const beforeRestoreModels = fixture.state.modelCalls;
      const first = fixture.runtime.application.resume(checkpoint.parentId, fixture.parentProgram).then(
        value => ({ value, error: null }), error => ({ value: null, error }));
      await restoreEntered.promise;
      assert.equal(resumedParents.length, 1);
      assert.equal(fixture.runtime.native.agents.get(SessionId(checkpoint.parentId)), resumedParents[0]);
      let secondSettled = false;
      const second = fixture.runtime.application.resume(checkpoint.parentId, fixture.parentProgram).then(
        value => { secondSettled = true; return { value, error: null }; },
        error => { secondSettled = true; return { value: null, error }; });
      await setImmediate();
      assert.equal(secondSettled, false, '并发恢复不能把已创建但子恢复尚未完成的Agent当作成功');
      restoreReleased.resolve();
      const outcomes = await Promise.all([first, second]);
      for (const outcome of outcomes) {
        assert.equal(outcome.error, failure); assert.equal(outcome.value, null);
      }
      assert.equal(closedParents.has(resumedParents[0]!), true, '失败回执返回前必须完成本次实际Agent清理');
      assert.equal(fixture.runtime.native.agents.get(SessionId(checkpoint.parentId)), undefined);
      assert.equal(fixture.runtime.native.agents.get(SessionId(checkpoint.receipt.childSessionId)), undefined);
      assert.equal(fixture.parent.actionManifest, coldReader);
      await coldReader.read();
      assert.equal(fixture.parent.resolveAction('delegate', fixture.rawScope).bindingId, originalBinding.bindingId);
      assert.equal(fixture.state.modelCalls, beforeRestoreModels, '失败恢复与并发等待不增加模型步骤');
      const parent = await fixture.runtime.application.resume(checkpoint.parentId, fixture.parentProgram);
      assert.equal(resumedParents.length, 2);
      assert.notEqual(resumedParents[1], resumedParents[0]);
      assert.equal(fixture.parent.actionManifest, coldReader);
      assert.equal(fixture.parent.resolveAction('delegate', fixture.rawScope).bindingId, originalBinding.bindingId);
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
    } finally {
      context.signal.removeEventListener('abort', releaseRestore); restoreReleased.resolve();
      fixture.releaseChild(); await fixture.dispose();
    }
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

  test('已受理原输入但尚未创建Goal的live恢复向authorizer提供真实absence，准入后只完成原工作', async () => {
    let failedFlush = false, deny = true;
    const denied = new Error('UNCREATED_GOAL_RECOVERY_DENIED');
    const fixture = await bootRecovery({
      installNative(native) {
        const flush = native.sessions.flush.bind(native.sessions);
        native.sessions.flush = async session => {
          const catalog = session.snapshotEvents().findLast(event => event.type === 'subagent/catalog');
          if (!failedFlush && !session.header.parentSession && catalog?.type === 'subagent/catalog') {
            const child = native.agents.get(catalog.data.childId);
            if (child && !native.goals.get(child) && child.session.snapshotEvents().some(event => event.type === 'agent/inbox/spliced'
              && event.data.inserted.length > 0)) {
              failedFlush = true; throw new Error('PRE_GOAL_PARENT_FLUSH_FAILED');
            }
          }
          return flush(session);
        };
      },
      authorize(admission) {
        if (admission.phase !== 'resume') return;
        assert.equal(admission.recovery.goal, null);
        assert.ok(['claimed', 'pending'].includes(admission.recovery.input));
        if (deny) throw denied;
      },
    });
    let childId = '';
    const failures: unknown[] = [];
    try {
      await fixture.start();
      const failure = await fixture.failed;
      assert.ok(failure.error instanceof ChildCreationIncompleteError);
      childId = failure.error.reference.childSessionId;
      assert.equal(fixture.state.childCalls, 0);
      await assert.rejects(fixture.runtime.application.resume(childId, fixture.childProgram), error => error === denied);
      assert.equal(fixture.state.childCalls, 0);
      deny = false;
      const child = await fixture.runtime.application.resume(childId, fixture.childProgram);
      await fixture.childCompleted;
      await fixture.parent.whenIdle();
      assert.equal(child.snapshot().goal?.phase, 'complete');
      const saved = await inspectDelegationSession(fixture.runtime.native, childId);
      const receipt = ownReceipt(saved); assert.ok(receipt);
      assert.equal(saved.events.filter(event => event.type === 'user/message' && event.data.id === receipt.messageId).length, 1);
      assert.equal(saved.events.filter(event => event.type === 'goal/change' && event.data.operation === 'create').length, 1);
      assert.equal(fixture.state.starts, 1);
      child.release();
    } catch (error) { failures.push(error); }
    finally { await finishRecovery(fixture, childId, failures); }
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
