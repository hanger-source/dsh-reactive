import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { Fragment, createElement as h, useLayoutEffect } from 'react';
import { BehaviorSubject, Observable, Subject, filter, firstValueFrom } from 'rxjs';
import { LlmAdapter, ToolCallId, type GenerateOptions, type StreamChunk } from '@deepseek-ai/dsh-llm';
import { SessionId, pluginRecordOf, type SessionEvent } from '@deepseek-ai/dsh-session';
import { WorkspaceId } from '@deepseek-ai/dsh-workspace';
import { defineAction, implementAction, type ActionCallContext } from '@hanger-source/dsh-reactive';
import { defineOperation, implementOperation, operationFollowLinkSchema, type OperationAccessContext, type OperationFollowLink } from '@hanger-source/dsh-reactive';
import { defineApplication, defineEnvironment, z } from '@hanger-source/dsh-reactive';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import { Expose, Offer, Follow } from '@hanger-source/dsh-reactive/program';
import { defineProgram } from '@hanger-source/dsh-reactive';
import { useAction, useObservable, useOperation } from '@hanger-source/dsh-reactive/program';
import { defineSurface } from '@hanger-source/dsh-reactive';
import { text, tool } from './native-support.ts';
import { recordSchema, waitRecordSchema } from '../packages/dsh/src/conversations/records.ts';

const commandSchema = z.object({ merchantId: z.string(), commandId: z.string() }).strict();
const referenceSchema = z.object({ merchantId: z.string(), operationId: z.string() }).strict();
const receiptSchema = z.object({ id: z.string(), reference: referenceSchema }).strict();
const stateSchema = z.discriminatedUnion('status', [
  z.object({ status: z.literal('running') }).strict(),
  z.object({ status: z.literal('awaiting-command'), reason: z.string() }).strict(),
  z.object({ status: z.literal('complete'), artifactId: z.string() }).strict(),
]);
const feedbackSchema = z.object({ status: z.literal('awaiting-command'), reason: z.string() }).strict();
type Command = z.output<typeof commandSchema>;
type State = z.output<typeof stateSchema>;
const operation = defineOperation({ kind: 'merchant-preparation', version: 1,
  command: commandSchema, reference: referenceSchema, state: stateSchema,
  settled: state => state.status === 'complete' ? state : undefined,
  feedback: feedbackSchema,
});
const prepare = defineAction({ name: 'prepare_merchant', description: '可靠受理原商家的准备命令',
  concurrency: 'parallel',
  scope: z.object({ merchantId: z.string() }).strict(), input: z.object({ commandId: z.string() }).strict(),
  output: z.object({ operation: referenceSchema }).strict(),
  operation: { definition: operation,
    command: (input, scope) => ({ merchantId: scope.merchantId, commandId: input.commandId }),
    reference: receipt => receipt.operation },
});
const finish = defineAction({ name: 'finish_original_goal', description: '核验原工作交回事实后完成原 Goal',
  scope: z.null(), input: z.object({}).strict(), output: z.boolean() });

/** 测试中的业务 owner 使用实际 SQLite 受理记录、状态和可靠 follow 关联；通知由测试显式投递。 */
function business(database: string, onRegistered?: (link: OperationFollowLink) => Promise<void>,
  onExistingAck?: (link: OperationFollowLink) => Promise<void>) {
  const db = new DatabaseSync(database);
  db.exec(`PRAGMA journal_mode = WAL;
    CREATE TABLE IF NOT EXISTS commands (command_id TEXT PRIMARY KEY, command_json TEXT NOT NULL,
      reference_json TEXT NOT NULL, state_json TEXT NOT NULL, revision INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS bodies (id INTEGER PRIMARY KEY, command_id TEXT NOT NULL, source_json TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS follows (id TEXT PRIMARY KEY, command_id TEXT NOT NULL, link_json TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS outbox (follow_id TEXT PRIMARY KEY, delivered INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE IF NOT EXISTS authorization (merchant_id TEXT PRIMARY KEY, can_follow INTEGER NOT NULL);
    INSERT INTO authorization VALUES ('merchant-a', 1) ON CONFLICT(merchant_id) DO NOTHING;
    CREATE TABLE IF NOT EXISTS reads (id INTEGER PRIMARY KEY, command_json TEXT NOT NULL, access_json TEXT NOT NULL);`);
  db.exec(`CREATE TABLE IF NOT EXISTS feedbacks (follow_id TEXT PRIMARY KEY, command_id TEXT NOT NULL, node_id TEXT NOT NULL UNIQUE, payload_json TEXT NOT NULL);`);
  const changed = new Subject<string>();
  const rowSchema = z.object({ command_json: z.string(), reference_json: z.string(), state_json: z.string(), revision: z.number() });
  const read = (commandId: string) => {
    const row = db.prepare('SELECT command_json, reference_json, state_json, revision FROM commands WHERE command_id = ?').get(commandId);
    if (!row) return undefined;
    const record = rowSchema.parse(row);
    return { command: commandSchema.parse(JSON.parse(record.command_json)), reference: referenceSchema.parse(JSON.parse(record.reference_json)),
      state: stateSchema.parse(JSON.parse(record.state_json)), revision: record.revision };
  };
  const authorize = (merchantId: string, access: OperationAccessContext) => {
    access.signal.throwIfAborted();
    assert.equal(merchantId, 'merchant-a');
    assert.ok(access.sessionId);
    if (access.source.kind === 'participant') assert.ok(['operator-test', 'operation-publisher', 'notifier-one', 'notifier-two', 'notifier-alternative'].includes(access.source.actorId));
  };
  const owner = implementOperation(operation, {
    locate(command, access) {
      authorize(command.merchantId, access);
      db.prepare('INSERT INTO reads (command_json, access_json) VALUES (?, ?)').run(JSON.stringify(command), JSON.stringify({ sessionId: access.sessionId, source: access.source }));
      const accepted = read(command.commandId);
      if (!accepted) return { status: 'unknown' };
      assert.deepEqual(accepted.command, command);
      return { status: 'accepted', reference: accepted.reference, state: accepted.state };
    },
    observe(reference, access) {
      authorize(reference.merchantId, access);
      const commandId = reference.operationId.slice('operation:'.length);
      return new Observable<{ status: 'ready'; generation: number; value: State; observedAt: number }>(subscriber => {
        const emit = () => {
          const accepted = read(commandId);
          if (!accepted) { subscriber.error(new Error('BUSINESS_OPERATION_NOT_ACCEPTED')); return; }
          assert.deepEqual(accepted.reference, reference);
          subscriber.next({ status: 'ready' as const, generation: accepted.revision, value: accepted.state, observedAt: Date.now() });
        };
        const subscription = changed.subscribe(id => { if (id === commandId) emit(); });
        emit();
        return () => subscription.unsubscribe();
      });
    },
    async registerFollow(command, link, access) {
      authorize(command.merchantId, access);
      if (access.source.kind === 'participant') {
        const actor = access.source.actorId;
        if ((actor === 'notifier-one' || actor === 'notifier-alternative') && command.commandId !== 'prepare-a'
          || actor === 'notifier-two' && command.commandId !== 'prepare-b') throw new Error('BUSINESS_FOLLOW_NOTIFIER_SCOPE_DENIED');
      }
      if (db.prepare('SELECT can_follow FROM authorization WHERE merchant_id = ?').get(command.merchantId)?.can_follow !== 1)
        throw new Error('BUSINESS_FOLLOW_DENIED');
      const previous = db.prepare('SELECT link_json FROM follows WHERE id = ?').get(link.id);
      if (previous) { assert.deepEqual(JSON.parse(z.string().parse(previous.link_json)), link); await onExistingAck?.(link); return; }
      if (link.after !== null && !db.prepare('SELECT node_id FROM feedbacks WHERE command_id = ? AND node_id = ?').get(command.commandId, link.after))
        throw new Error('BUSINESS_FEEDBACK_AFTER_UNKNOWN');
      db.exec('BEGIN IMMEDIATE');
      try {
        db.prepare('INSERT INTO follows (id, command_id, link_json) VALUES (?, ?, ?)').run(link.id, command.commandId, JSON.stringify(link));
        db.prepare('INSERT INTO outbox (follow_id) VALUES (?)').run(link.id);
        const accepted = read(command.commandId);
        if (link.when === 'feedback' && link.after === null && accepted?.state.status === 'awaiting-command')
          db.prepare('INSERT INTO feedbacks VALUES (?, ?, ?, ?)').run(link.id, command.commandId, 'observation:' + link.id, JSON.stringify(accepted.state));
        db.exec('COMMIT');
      } catch (error) { db.exec('ROLLBACK'); throw error; }
      // 可靠事务已经提交；可模拟首个调用的网络 ACK 迟到，不在事务中等待通知。
      await onRegistered?.(link);
    },
    readFeedback(command, link, access) {
      authorize(command.merchantId, access);
      const registration = db.prepare('SELECT link_json FROM follows WHERE id = ? AND command_id = ?').get(link.id, command.commandId);
      if (!registration) throw new Error('BUSINESS_FOLLOW_NOT_REGISTERED');
      assert.deepEqual(JSON.parse(z.string().parse(registration.link_json)), link);
      const accepted = read(command.commandId);
      if (!accepted) return { status: 'unknown' };
      const feedback = db.prepare('SELECT node_id, payload_json FROM feedbacks WHERE follow_id = ?').get(link.id);
      if (!feedback) return { status: 'pending', reference: accepted.reference };
      return { status: 'ready', reference: accepted.reference,
        node: { id: z.string().parse(feedback.node_id), payload: feedbackSchema.parse(JSON.parse(z.string().parse(feedback.payload_json))) } };
    },
  });
  return { db, owner,
    accept(command: Command, call: ActionCallContext<{ readonly merchantId: string }>) {
      assert.equal(command.merchantId, call.scope.merchantId);
      const reference = { merchantId: command.merchantId, operationId: 'operation:' + command.commandId };
      db.exec('BEGIN IMMEDIATE');
      try {
        db.prepare('INSERT INTO bodies (command_id, source_json) VALUES (?, ?)').run(command.commandId, JSON.stringify(call.source));
        db.prepare('INSERT INTO commands VALUES (?, ?, ?, ?, 1) ON CONFLICT(command_id) DO NOTHING')
          .run(command.commandId, JSON.stringify(command), JSON.stringify(reference), JSON.stringify({ status: 'running' }));
        assert.deepEqual(read(command.commandId)?.command, command);
        db.exec('COMMIT');
      } catch (error) { db.exec('ROLLBACK'); throw error; }
      changed.next(command.commandId);
      return { operation: reference };
    },
    update(commandId: string, state: State) {
      assert.equal(Number(db.prepare('UPDATE commands SET state_json = ?, revision = revision + 1 WHERE command_id = ?')
        .run(JSON.stringify(stateSchema.parse(state)), commandId).changes), 1);
      changed.next(commandId);
    },
    recordFeedback(followId: string, id: string, payload: z.input<typeof feedbackSchema>) {
      const registration = db.prepare('SELECT command_id, link_json FROM follows WHERE id = ?').get(followId);
      assert.ok(registration);
      const link = operationFollowLinkSchema.parse(JSON.parse(z.string().parse(registration.link_json)));
      assert.equal(link.when, 'feedback'); assert.notEqual(link.after, id);
      db.prepare('INSERT INTO feedbacks VALUES (?, ?, ?, ?)').run(followId, z.string().parse(registration.command_id), id, JSON.stringify(feedbackSchema.parse(payload)));
      db.prepare('UPDATE outbox SET delivered = 0 WHERE follow_id = ?').run(followId);
    },
    read,
    bodies: () => db.prepare('SELECT command_id, source_json FROM bodies ORDER BY id').all(),
    links: () => db.prepare('SELECT link_json FROM follows ORDER BY id').all().map(row => operationFollowLinkSchema.parse(JSON.parse(z.string().parse(row.link_json)))),
    outbox: () => db.prepare('SELECT follow_id FROM outbox WHERE delivered = 0 ORDER BY follow_id').all().map(row => z.string().parse(row.follow_id)),
    acknowledge(followId: string) { assert.equal(Number(db.prepare('UPDATE outbox SET delivered = 1 WHERE follow_id = ?').run(followId).changes), 1); },
    allowFollow(allowed: boolean) { db.prepare('UPDATE authorization SET can_follow = ? WHERE merchant_id = ?').run(allowed ? 1 : 0, 'merchant-a'); },
    reads: () => db.prepare('SELECT command_json, access_json FROM reads ORDER BY id').all(),
    close() { changed.complete(); db.close(); },
  };
}

async function scenario(root: string, when: 'settled' | 'feedback', options: {
  recovering?: boolean;
  registered?: (link: OperationFollowLink) => Promise<void>;
  existingAck?: (link: OperationFollowLink) => Promise<void>;
  beforeWaitAck?: () => Promise<void>;
  admitReady?: (signal: AbortSignal) => Promise<void>;
  parallel?: boolean;
  multiStage?: boolean;
  admissionUnknown?: boolean;
  backgroundThenWait?: boolean;
} = {}) {
  const store = business(join(root, 'business.sqlite'), options.registered, options.existingAck);
  const enabled$ = new BehaviorSubject(true), enabledCommits$ = new Subject<boolean>();
  const requests: GenerateOptions[] = [], errors: unknown[] = [];
  const bodyWaits: { source: string; waits: number; durable: readonly SessionEvent[] }[] = [];
  const resumed = Promise.withResolvers<void>();
  const prepareImplementation = implementAction(prepare, { operationOwner: store.owner,
    async execute(input, call) {
      if (call.source.kind === 'tool') {
        assert.ok(call.continuation?.goal);
        const link = store.links().find(link => commandSchema.parse(link.command).commandId === input.commandId);
        assert.ok(link, '可靠 owner 关联须在 execute 前存在');
        const persistence = runtime.native.get('sessionPersistence');
        assert.ok(persistence);
        const handle = await persistence.open(SessionId(call.sessionId), 'read');
        let durable: readonly SessionEvent[];
        try { durable = (await handle.read()).events; } finally { await handle.close(); }
        const waiting = durable.some(event => {
          const record = pluginRecordOf(event);
          return record?.type === 'plugin:reactive/wait' && waitRecordSchema.parse(record.data).wait.id === link.wait.id;
        });
        assert.equal(waiting, link.mode === 'wait', 'wait模式须先跨持久ACK，background不得把原操作登记成Goal等待');
        const observer = runtime.application.conversation(call.sessionId, definition.environments[0].program.definition);
        try { bodyWaits.push({ source: call.source.kind, waits: observer.snapshot().waits.length, durable }); }
        finally { observer.release(); }
      } else {
        assert.equal(call.source.participant, 'human');
        assert.equal(call.continuation, null); assert.deepEqual(call.inputs, []);
      }
      if (options.admissionUnknown) {
        // 业务调用已实际进入，外部受理尚无权威回执；框架不能由 throw 推断未开始。
        store.db.prepare('INSERT INTO bodies (command_id, source_json) VALUES (?, ?)')
          .run(input.commandId, JSON.stringify(call.source));
        throw new Error('BUSINESS_ADMISSION_UNKNOWN');
      }
      return store.accept({ merchantId: call.scope.merchantId, commandId: input.commandId }, call);
    },
  });
  const finishImplementation = implementAction(finish, { execute(_input, call) { call.completeGoal(); return true; } });
  const surface = defineSurface({ views: {}, actions: { prepare }, operations: { preparation: operation } });
  const definition = defineApplication({ name: 'operation-follow-' + when,
    async admit(input, _conversation, admission) {
      assert.ok(['operator-test', 'operation-publisher', 'notifier-one', 'notifier-two', 'notifier-alternative'].includes(admission.actorId));
      assert.deepEqual(input.scope, { merchantId: 'merchant-a' });
      if (input.result?.ready) await options.admitReady?.(admission.signal);
    },
    environments: [defineEnvironment({ id: 'work', instructions: '只根据原业务 owner 的明确事实继续原 Goal',
      program: implementProgram(defineProgram({ input: z.object({ merchantId: z.string() }).strict(), surface }), { Component() {
        const action = useAction(prepareImplementation, () => ({ merchantId: 'merchant-a' }), []);
        const complete = useAction(finishImplementation, () => null, []);
        const existing = useOperation(store.owner, () => ({ merchantId: 'merchant-a', commandId: 'prepare-a' }), []);
        const permission = useObservable(() => enabled$, []);
        const enabled = permission.status === 'ready' && permission.value;
        useLayoutEffect(() => { enabledCommits$.next(enabled); }, [enabled]);
        return h(Fragment, null, Offer({ name: 'prepare_merchant', action, enabled, follow: { when, scope: { merchantId: 'merchant-a' } } }),
          options.backgroundThenWait ? Offer({ name: 'prepare_background', action, enabled,
            follow: { when, scope: { merchantId: 'merchant-a' }, mode: 'background' } }) : null,
          Follow({ name: 'await_preparation', description: '只等待原准备操作的下一次明确反馈', operation: existing,
            when: 'feedback', enabled, scope: { merchantId: 'merchant-a' } }),
          Offer({ name: 'finish_original_goal', action: complete }),
          Expose({ surface, value: { actions: { prepare: action }, ...enabled ? { operations: { preparation: existing } } : {} } }));
      } }),
    })],
  });
  const model = nativeModel({ provider: 'isolated', model: 'test', create: () => new class extends LlmAdapter {
      async *stream(request: GenerateOptions) {
        requests.push(request);
        const latest = request.messages.at(-1);
        if (latest?.role === 'tool' && latest.isError) {
          yield* text('本次调用失败，沿原操作核验事实，不自动重试');
          return;
        }
        if (options.backgroundThenWait) {
          if (requests.length > 5) throw new Error('BACKGROUND_MODEL_REQUEST_LIMIT');
          if (requests.length === 1) yield* tool('prepare_background', 'native-background-a', { commandId: 'prepare-a' });
          else if (requests.length === 2) yield* tool('prepare_merchant', 'native-wait-b', { commandId: 'prepare-b' });
          else if (requests.length === 3) {
            resumed.resolve();
            const content = request.messages.flatMap(message => message.role === 'user' ? message.content : [])
              .map(block => block.type === 'text' ? block.text : '').join('\n');
            assert.match(content, /artifact-background-a/u);
            assert.match(content, /artifact-wait-b/u);
            yield* tool('finish_original_goal', 'finish-explicitly-resumed', {});
          } else yield* text('两个原操作反馈已作为事实交回，显式恢复后的工作完成核验');
          return;
        }
        if (!options.recovering && requests.length === 1) {
          if (options.multiStage) yield* tool('await_preparation', 'native-observation-first', { after: null });
          else if (options.parallel) yield* parallelPreparations();
          else yield* tool('prepare_merchant', 'native-prepare', { commandId: 'prepare-a' });
        }
        else if (options.multiStage && !options.recovering && requests.length === 2) {
          const message = request.messages.findLast(message => {
            if (message.role !== 'user') return false;
            const source = message.source;
            return source?.kind === 'user' && 'rpcId' in source && typeof source.rpcId === 'string' && source.rpcId.startsWith('operation:');
          });
          assert.ok(message);
          const content = message.content.map(block => block.type === 'text' ? block.text : '').join('');
          const feedback = z.object({ outcome: z.object({ status: z.literal('feedback'), node: z.object({ id: z.string() }) }) })
            .parse(JSON.parse(content.slice(content.indexOf('\n') + 1)));
          yield* tool('await_preparation', 'native-observation-next', { after: feedback.outcome.node.id });
        }
        else if (requests.length === (options.recovering ? 1 : 2)) {
          resumed.resolve(); yield* tool('finish_original_goal', 'finish-original', {});
        } else yield* text('原工作已经按权威业务事实完成核验');
      }
    }() });
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'operator-test', serviceActorId: 'operation-publisher',
    operations: [store.owner], tools: 'native', goalRounds: 3, onError: event => errors.push(event.error),
    model: { ...model, async install(ctx, home) {
      if (options.beforeWaitAck) ctx.on('session/flush', async session => {
        if (session.snapshotEvents().some(event => pluginRecordOf(event)?.type === 'plugin:reactive/wait')) await options.beforeWaitAck!();
      });
      await model.install(ctx, home);
    } } });
  await mkdir(join(root, 'workspace'), { recursive: true });
  const workspace = await runtime.application.createWorkspace({ directory: join(root, 'workspace') });
  return { runtime, workspace, program: definition.environments[0].program.definition, store, requests, errors, bodyWaits, resumed, enabled$, enabledCommits$,
    async dispose() { try { await runtime.dispose(); } finally { enabled$.complete(); enabledCommits$.complete(); store.close(); } },
  };
}

async function* parallelPreparations(): AsyncIterable<StreamChunk> {
  for (const [index, commandId] of ['prepare-a', 'prepare-b'].entries()) {
    const id = ToolCallId('native-' + commandId), args = JSON.stringify({ commandId });
    yield { type: 'block-start', index, blockType: 'tool-call' };
    yield { type: 'tool-call-delta', index, id, name: 'prepare_merchant', argumentsDelta: args };
    yield { type: 'block-end', index, block: { type: 'tool-call', id, name: 'prepare_merchant', arguments: args } };
  }
  yield { type: 'finish', reason: { kind: 'tool-calls' } };
}

type Checkpoint = { sessionId: string; goalId: string; goalRevision: number;
  operations: readonly { followId: string; command: Command; reference: z.output<typeof referenceSchema> }[] };
function assertOriginalGoalConcluded(events: readonly SessionEvent[], goal: { readonly id: string; readonly revision: number }) {
  const changes = events.flatMap(event => event.type === 'goal/change' && event.data.operation !== 'clear' && event.data.goal.id === goal.id
    ? [{ operation: event.data.operation, revision: event.data.goal.revision }] : []);
  assert.deepEqual(changes, [
    { operation: 'create', revision: goal.revision },
    { operation: 'resume', revision: goal.revision + 1 },
    { operation: 'complete', revision: goal.revision + 2 },
  ]);
}
async function crashCheckpoint(root: string, context: TestContext, mode: 'single' | 'multiple' | 'feedback' = 'single'): Promise<Checkpoint> {
  const worker = fork(import.meta.filename, [mode === 'single' ? '--operation-checkpoint' : mode === 'multiple' ? '--operation-multi-checkpoint' : '--operation-feedback-checkpoint', root], {
    execPath: process.execPath, execArgv: [], stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  });
  let output = '';
  worker.stdout?.on('data', chunk => { output += String(chunk); });
  worker.stderr?.on('data', chunk => { output += String(chunk); });
  const checkpoint = new Promise<Checkpoint>((resolve, reject) => {
    worker.once('message', message => resolve(message as Checkpoint)); worker.once('error', reject);
    worker.once('exit', (code, signal) => reject(new Error(`OPERATION_WORKER_EARLY_EXIT ${code}/${signal}\n${output}`)));
  });
  const abort = () => { worker.kill('SIGKILL'); };
  context.signal.addEventListener('abort', abort, { once: true });
  try {
    const saved = await checkpoint;
    const exited = once(worker, 'exit'); worker.kill('SIGKILL');
    assert.deepEqual(await exited, [null, 'SIGKILL']);
    return saved;
  } finally { context.signal.removeEventListener('abort', abort); worker.kill('SIGKILL'); }
}

if (!['--operation-checkpoint', '--operation-multi-checkpoint', '--operation-feedback-checkpoint'].includes(process.argv[2] ?? '')) {
  test('background受理后继续同一Goal，仍受另一wait和暂停约束，冷恢复不重执且迟到结果不取得新修订权限', async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-background-'));
    const app = await scenario(root, 'settled', { backgroundThenWait: true });
    context.after(() => app.dispose());
    const session = await app.runtime.application.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'original-goal', content: '后台准备甲操作，同时继续核验必须等待的乙操作', intent: 'goal',
      delivery: 'followup', scope: { merchantId: 'merchant-a' } });
    await session.whenIdle();
    const links = app.store.links();
    const background = links.find(link => commandSchema.parse(link.command).commandId === 'prepare-a');
    const waiting = links.find(link => commandSchema.parse(link.command).commandId === 'prepare-b');
    assert.ok(background && waiting);
    assert.equal(app.requests.length, 2, 'A尚未反馈，模型就必须已继续调用B；background不能conclude当前turn或阻止Goal');
    assert.deepEqual(app.store.bodies().map(row => row.command_id), ['prepare-a', 'prepare-b']);
    assert.deepEqual(app.bodyWaits.map(value => value.waits), [0, 1]);
    assert.deepEqual(session.snapshot().waits.map(value => value.wait.id), [waiting.wait.id]);
    const original = background.continuation.goal; assert.ok(original);
    assert.deepEqual(waiting.continuation.goal, original);
    assert.equal(session.snapshot().goal?.phase, 'active');

    const sessionId = session.id;
    // 正常 close 会取消尚未消费的 inbox 输入；先恢复持久关联，再产生两个业务结果。
    session.release(); await app.runtime.application.close(sessionId);
    assert.equal(app.runtime.native.agents.get(SessionId(sessionId)), undefined);
    const cold = await app.runtime.application.open(sessionId, app.program);
    assert.equal(app.runtime.native.agents.get(SessionId(sessionId)), undefined);
    assert.deepEqual(cold.snapshot().waits.map(value => value.wait.id), [waiting.wait.id], '冷读不能给background补一个不存在的wait');
    const reopened = await app.runtime.application.resume(sessionId, app.program);
    await reopened.whenIdle();
    assert.equal(app.requests.length, 2);
    assert.equal(app.store.bodies().length, 2, '恢复只定位原业务命令，不重新执行两个已受理body');
    assert.deepEqual(reopened.snapshot().waits.map(value => value.wait.id), [waiting.wait.id]);

    app.store.update('prepare-a', { status: 'complete', artifactId: 'artifact-background-a' });
    assert.equal((await app.runtime.application.notifyOperation(sessionId, background.id, { actorId: 'notifier-one' })).status, 'accepted');
    await reopened.whenIdle();
    assert.equal(app.requests.length, 2, 'background完成仍不能越过同一原Goal的B等待');
    assert.equal(reopened.snapshot().requests['operation:' + background.id]?.state, 'accepted');
    assert.deepEqual(reopened.snapshot().waits.map(value => value.wait.id), [waiting.wait.id]);

    await reopened.pause();
    const paused = reopened.snapshot().goal; assert.ok(paused);
    assert.equal(paused.phase, 'paused');
    assert.equal(paused.id, original.id);
    assert.notEqual(paused.revision, original.revision);

    app.store.update('prepare-b', { status: 'complete', artifactId: 'artifact-wait-b' });
    assert.equal((await app.runtime.application.notifyOperation(sessionId, waiting.id, { actorId: 'notifier-two' })).status, 'accepted');
    await reopened.whenIdle();
    assert.equal(app.requests.length, 2, '所有业务条件满足也不能让旧反馈越过暂停或取得新Goal修订的唤醒权限');
    assert.equal(reopened.snapshot().goal?.phase, 'paused');
    assert.equal(reopened.snapshot().goal?.revision, paused.revision);
    assert.equal(reopened.snapshot().waits.length, 0);
    assert.equal(reopened.snapshot().requests['operation:' + waiting.id]?.state, 'accepted');
    const agent = app.runtime.native.agents.get(SessionId(sessionId))!;
    const current = app.runtime.native.goals.get(agent); assert.ok(current);
    assert.equal(current.revision, paused.revision);
    const explicitlyResumed = app.runtime.native.goals.resume(agent, current);
    assert.notEqual(explicitlyResumed.revision, original.revision);
    await app.resumed.promise; await reopened.whenIdle();
    assert.deepEqual(app.errors, [], '模型中的真实断言失败必须直接报告，不能伪装成尚未进入模型的等待');
    assert.equal(reopened.snapshot().goal?.id, original.id);
    assert.equal(reopened.snapshot().goal?.phase, 'complete');
    assert.ok(reopened.snapshot().goal!.revision > explicitlyResumed.revision);
    assert.equal(app.store.bodies().length, 2);
    const events = agent.session.snapshotEvents();
    const waitEvents = events.flatMap(event => {
      const record = pluginRecordOf(event);
      return record?.type === 'plugin:reactive/wait' ? [waitRecordSchema.parse(record.data)] : [];
    });
    assert.deepEqual(waitEvents.map(value => value.wait.id), [waiting.wait.id]);
    for (const link of [background, waiting]) {
      const records = events.flatMap(event => {
        const record = pluginRecordOf(event);
        return record?.type === 'plugin:reactive/input' ? [recordSchema.parse(record.data)] : [];
      }).filter(record => record.requestId === 'operation:' + link.id);
      assert.equal(records.length, 1);
      assert.equal(records[0]!.actorId, 'operation-publisher');
      assert.deepEqual(records[0]!.result?.continuation.goal, original, '显式恢复产生的新修订不改写两个旧结果的原续接关联');
      assert.equal(reopened.snapshot().requests['operation:' + link.id]?.state, 'settled');
      assert.equal(events.filter(event => event.type === 'user/message' && 'rpcId' in event.data.source
        && event.data.source.rpcId === 'operation:' + link.id).length, 1);
    }
    assert.deepEqual(app.errors, []);
    cold.release(); reopened.release();
  });

  test('可靠关联 ACK 期间撤 Offer，确定未开始的调用持久结案，冷恢复和旧通知不重开等待', async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-disabled-'));
    const registered = Promise.withResolvers<OperationFollowLink>(), acknowledge = Promise.withResolvers<void>();
    const app = await scenario(root, 'settled', { async registered(link) { registered.resolve(link); await acknowledge.promise; } });
    let disposed = false;
    context.after(() => { acknowledge.resolve(); return disposed ? undefined : app.dispose(); });
    const session = await app.runtime.application.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'original-goal', content: '在当前许可下受理商家甲的准备操作', intent: 'goal', delivery: 'followup' as const, scope: { merchantId: 'merchant-a' } });
    const link = await registered.promise;
    assert.equal(app.store.bodies().length, 0);
    const committed = firstValueFrom(app.enabledCommits$.pipe(filter(enabled => !enabled)));
    app.enabled$.next(false);
    await committed;
    acknowledge.resolve();
    await session.whenIdle();
    assert.equal(app.store.bodies().length, 0, '组件仍挂载，但准备期间撤下的 Offer 许可不能进入 execute');
    assert.equal(app.store.read('prepare-a'), undefined);
    assert.equal(app.requests.length, 2, '原 turn 继续读取实际工具拒绝，不完成未受理的业务');
    const rejected = app.requests[1]!.messages.find(message => message.role === 'tool' && message.toolCallId === link.source.callId);
    assert.ok(rejected?.role === 'tool'); assert.equal(rejected.isError, true);
    assert.deepEqual(await session.locateOperation(operation, { merchantId: 'merchant-a', commandId: 'prepare-a' }), { status: 'unknown' });
    assert.deepEqual(await app.runtime.application.notifyOperation(session.id, link.id, { actorId: 'notifier-one' }), { status: 'not-started' });
    await session.whenIdle();
    assert.equal(app.requests.length, 2, '结案通知不追加模型步骤或伪造业务反馈');
    assert.deepEqual(app.store.links(), [link]);
    assert.deepEqual(session.snapshot().waits, []);
    assert.equal(session.snapshot().requests['operation:' + link.id], undefined);
    assert.equal(session.snapshot().goal?.phase, 'active');
    const agent = app.runtime.native.agents.get(SessionId(session.id))!;
    const events = agent.session.snapshotEvents();
    const result = events.find(event => event.type === 'tool/result' && event.data.message.toolCallId === link.source.callId);
    assert.ok(result?.type === 'tool/result'); assert.equal(result.data.message.isError, true);
    assert.match(result.data.message.content.map(block => block.type === 'text' ? block.text : '').join(''), /REACTIVE_ACTION_NOT_AVAILABLE/u);
    assert.equal(events.filter(event => pluginRecordOf(event)?.type === 'plugin:reactive/operation-follow').length, 1);
    assert.equal(events.filter(event => pluginRecordOf(event)?.type === 'plugin:reactive/operation-delivery').length, 0);
    assert.equal(events.filter(event => pluginRecordOf(event)?.type === 'plugin:reactive/operation-receipt').length, 0);
    assert.deepEqual(app.errors, []);
    const sessionId = session.id;
    session.release(); await app.dispose(); disposed = true;
    const recovered = await scenario(root, 'settled', { recovering: true });
    context.after(() => recovered.dispose());
    const cold = await recovered.runtime.application.open(sessionId, recovered.program);
    assert.deepEqual(cold.snapshot().waits, []);
    assert.equal(recovered.runtime.native.agents.get(SessionId(sessionId)), undefined);
    assert.deepEqual(await recovered.runtime.application.notifyOperation(sessionId, link.id, { actorId: 'notifier-one' }), { status: 'not-started' });
    assert.equal(recovered.runtime.native.agents.get(SessionId(sessionId)), undefined, '旧 outbox 的结案确认不加载模型 Agent');
    const resumed = await recovered.runtime.application.resume(sessionId, recovered.program);
    await resumed.whenIdle();
    assert.deepEqual(resumed.snapshot().waits, []);
    assert.equal(recovered.requests.length, 0);
    assert.equal(recovered.store.bodies().length, 0);
    assert.deepEqual(recovered.store.links(), [link]);
    assert.deepEqual(recovered.errors, []);
    cold.release(); resumed.release();
  });

  for (const boundary of ['cancel', 'workspace-guard'] as const) test(`可靠 ACK 期间${boundary}拒绝后仅结案本次参与，不进入业务或重启 Goal`, async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-' + boundary + '-'));
    const registered = Promise.withResolvers<OperationFollowLink>(), acknowledge = Promise.withResolvers<void>();
    const app = await scenario(root, 'settled', { async registered(link) { registered.resolve(link); await acknowledge.promise; } });
    context.after(() => { acknowledge.resolve(); return app.dispose(); });
    const session = await app.runtime.application.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'original-goal', content: '受理商家甲的准备操作', intent: 'goal', delivery: 'followup', scope: { merchantId: 'merchant-a' } });
    const link = await registered.promise;
    const workspace = app.runtime.native.workspaceRegistry.get(WorkspaceId(app.workspace.workspaceId))!;
    if (boundary === 'cancel') await session.cancel();
    else await workspace.detachSession(SessionId(session.id));
    acknowledge.resolve();
    await session.whenIdle();
    assert.equal(app.store.bodies().length, 0);
    assert.equal(app.bodyWaits.length, 0);
    assert.deepEqual(session.snapshot().waits, []);
    assert.equal(session.snapshot().requests['operation:' + link.id], undefined);
    assert.equal(session.snapshot().goal?.activation, 'disarmed');
    const events = app.runtime.native.agents.get(SessionId(session.id))!.session.snapshotEvents();
    assert.equal(events.filter(event => pluginRecordOf(event)?.type === 'plugin:reactive/operation-not-started').length, 1);
    assert.equal(events.filter(event => pluginRecordOf(event)?.type === 'plugin:reactive/operation-delivery').length, 0);
    if (boundary === 'workspace-guard') {
      assert.equal(app.requests.length, 1, '原生下一步装配也必须拒绝已被移除的成员，不能再次进入模型');
      assert.equal(app.errors.length, 1);
      assert.ok(app.errors[0] instanceof Error);
      assert.equal(app.errors[0].message, 'REACTIVE_WORKSPACE_MEMBERSHIP_REQUIRED');
    } else assert.deepEqual(app.errors, []);
    session.release();
  });

  test('业务函数已进入但受理未知，保留原参与等待，冷恢复只查原命令不重 execute', async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-unknown-'));
    const app = await scenario(root, 'settled', { admissionUnknown: true });
    let disposed = false;
    context.after(() => disposed ? undefined : app.dispose());
    const session = await app.runtime.application.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'original-goal', content: '受理商家甲的准备操作', intent: 'goal', delivery: 'followup', scope: { merchantId: 'merchant-a' } });
    await session.whenIdle();
    const [link] = app.store.links(); assert.ok(link);
    assert.equal(app.bodyWaits.length, 1);
    assert.equal(app.store.bodies().length, 1);
    assert.equal(app.store.read('prepare-a'), undefined);
    assert.deepEqual(await app.runtime.application.notifyOperation(session.id, link.id, { actorId: 'notifier-one' }), { status: 'unknown' });
    assert.equal(session.snapshot().waits.length, 1);
    const events = app.runtime.native.agents.get(SessionId(session.id))!.session.snapshotEvents();
    assert.equal(events.filter(event => pluginRecordOf(event)?.type === 'plugin:reactive/operation-not-started').length, 0);
    assert.deepEqual(app.errors, []);
    const sessionId = session.id;
    session.release(); await app.dispose(); disposed = true;
    const recovered = await scenario(root, 'settled', { recovering: true });
    context.after(() => recovered.dispose());
    const cold = await recovered.runtime.application.open(sessionId, recovered.program);
    assert.equal(cold.snapshot().waits.length, 1);
    const resumed = await recovered.runtime.application.resume(sessionId, recovered.program);
    await resumed.whenIdle();
    assert.deepEqual(await recovered.runtime.application.notifyOperation(sessionId, link.id, { actorId: 'notifier-one' }), { status: 'unknown' });
    assert.equal(resumed.snapshot().waits.length, 1);
    assert.equal(recovered.store.bodies().length, 1);
    assert.equal(recovered.requests.length, 0);
    assert.deepEqual(recovered.errors, []);
    cold.release(); resumed.release();
  });

  test('同一 Action 的 SDK 与 AI 共用 SQLite 受理；可靠反馈先于 prepare wait ACK 到达也不重开等待，非终态不完成 Operation', async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-feedback-'));
    const registered = Promise.withResolvers<OperationFollowLink>(), acknowledge = Promise.withResolvers<void>();
    const waitAckEntered = Promise.withResolvers<void>(), waitAckRelease = Promise.withResolvers<void>();
    const app = await scenario(root, 'feedback', {
      async registered(link) {
        registered.resolve(link); await acknowledge.promise;
      },
      async beforeWaitAck() { waitAckEntered.resolve(); await waitAckRelease.promise; },
    });
    context.after(() => app.dispose());
    try {
      const session = await app.runtime.application.create('work', { workspaceId: app.workspace.workspaceId });
      const human = session.resolveAction('prepare', { merchantId: 'merchant-a' })({ commandId: 'prepare-a' });
      assert.deepEqual(await human.result, { operation: { merchantId: 'merchant-a', operationId: 'operation:prepare-a' } });
      const operationHandle = await human.operation;
      let settled = false;
      const terminal = operationHandle.settled().then(value => { settled = true; return value; });
      void terminal.catch(() => {});
      app.store.update('prepare-a', { status: 'awaiting-command', reason: '商家甲需要补交凭据' });
      await session.submit({ requestId: 'original-goal', content: '核验商家甲当前受理状态并交回调查要求', intent: 'goal', delivery: 'followup' as const, scope: { merchantId: 'merchant-a' } });
      const link = await registered.promise;
      assert.equal(app.store.bodies().length, 1, 'registerFollow 尚未 ACK 时不得进入 AI execute');
      assert.equal(app.requests.length, 1);
      assert.deepEqual(app.store.outbox(), [link.id], '可靠关联与独立通知须在同一 SQLite 事务提交');
      app.store.allowFollow(false);
      const readsBeforeDenied = app.store.reads().length;
      await assert.rejects(app.runtime.application.notifyOperation(link.continuation.sessionId, link.id, { actorId: 'notifier-one' }), /BUSINESS_FOLLOW_DENIED/u);
      assert.equal(app.store.reads().length, readsBeforeDenied, 'locator 读权不能替代可靠 follow 授权');
      assert.equal(app.requests.length, 1);
      app.store.allowFollow(true);
      const notifying = app.runtime.application.notifyOperation(link.continuation.sessionId, link.id, { actorId: 'notifier-one' });
      await waitAckEntered.promise;
      assert.equal(app.store.bodies().length, 1, 'native wait ACK 尚未返回时不得进入 AI execute');
      waitAckRelease.resolve();
      const fastDelivery = await notifying;
      assert.equal(app.store.bodies().length, 1, '独立通知受理后，原 prepare 网络 ACK 未返回仍不能执行');
      assert.deepEqual(await app.runtime.application.notifyOperation(link.continuation.sessionId, link.id, { actorId: 'notifier-alternative' }), fastDelivery,
        '合法通知者变化不改变固定 publisher 或锁死同一持久投递');
      app.store.acknowledge(link.id);
      acknowledge.resolve();
      await app.resumed.promise; await session.whenIdle();
      assert.equal(fastDelivery.status, 'accepted');
      assert.equal(app.store.bodies().length, 2);
      assert.deepEqual(app.store.bodies().map(row => JSON.parse(z.string().parse(row.source_json)).kind), ['participant', 'tool']);
      assert.equal(app.store.db.prepare('SELECT count(*) AS count FROM commands').get()?.count, 1);
      assert.equal(app.bodyWaits[0]?.waits, 0, '快速反馈已经解除的 wait 不能被 prepare 重新登记');
      assert.equal(session.snapshot().waits.length, 0);
      assert.equal(session.snapshot().goal?.id, link.continuation.goal?.id);
      assert.equal(session.snapshot().goal?.phase, 'complete');
      assert.equal(settled, false);
      const result = session.snapshot().requests['operation:' + link.id];
      assert.equal(result?.state, 'settled');
      const resumedText = app.requests[1]!.messages.flatMap(message => message.role === 'user' ? message.content : [])
        .map(block => block.type === 'text' ? block.text : '').join('\n');
      assert.match(resumedText, /"status":"feedback"/u);
      assert.match(resumedText, /商家甲需要补交凭据/u);
      app.store.update('prepare-a', { status: 'complete', artifactId: 'artifact-a' });
      assert.deepEqual(await terminal, { status: 'complete', artifactId: 'artifact-a' });
      const agent = app.runtime.native.agents.get(SessionId(session.id))!;
      assert.ok(link.continuation.goal);
      assertOriginalGoalConcluded(agent.session.snapshotEvents(), link.continuation.goal);
      assert.equal(agent.session.snapshotEvents().filter(event => pluginRecordOf(event)?.type === 'plugin:reactive/wait').length, 1);
      const delivery = agent.session.snapshotEvents().flatMap(event => {
        const record = pluginRecordOf(event);
        return record?.type === 'plugin:reactive/input' ? [recordSchema.parse(record.data)] : [];
      }).find(record => record.requestId === 'operation:' + link.id);
      assert.equal(delivery?.actorId, 'operation-publisher');
      assert.deepEqual(app.errors, []); session.release();
    } finally { acknowledge.resolve(); waitAckRelease.resolve(); }
  });

  test('settled follow 忽略业务非终态；真实进程中断后按原 command 与持久 receipt 恢复，终态才续原 Goal且不重执行', async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-recovery-'));
    const saved = await crashCheckpoint(root, context);
    const accepted = saved.operations[0]!;
    const readyEntered = Promise.withResolvers<AbortSignal>(), readyRelease = Promise.withResolvers<void>();
    let gateReady = true;
    const app = await scenario(root, 'settled', { recovering: true, async admitReady(signal) {
      if (!gateReady) return;
      gateReady = false; readyEntered.resolve(signal); await readyRelease.promise;
    } });
    context.after(() => { readyRelease.resolve(); return app.dispose(); });
      assert.equal(app.store.bodies().length, 1);
      assert.deepEqual(app.store.read('prepare-a')?.reference, accepted.reference);
      const cold = await app.runtime.application.open(saved.sessionId, app.program);
      assert.equal(app.runtime.native.agents.get(SessionId(saved.sessionId)), undefined);
      assert.equal(cold.snapshot().waits.length, 1);
      assert.equal(cold.snapshot().goal?.phase, 'active');
      const location = await cold.locateOperation(operation, { merchantId: 'merchant-a', commandId: 'prepare-a' });
      assert.deepEqual(location, { status: 'accepted', reference: accepted.reference,
        state: { status: 'awaiting-command', reason: '商家甲需要补交凭据' } });
      const existing = await cold.attachOperation(operation, accepted.reference);
      let domainSettled = false;
      const domainTerminal = existing.settled().then(value => { domainSettled = true; return value; });
      void domainTerminal.catch(() => {});
      assert.equal((await firstValueFrom(existing.observations$.pipe(filter(value => value.status === 'ready')))).status, 'ready');
      assert.equal(domainSettled, false);
      assert.equal(app.runtime.native.agents.get(SessionId(saved.sessionId)), undefined, '业务操作冷读不加载 Agent');
      const resumed = await app.runtime.application.resume(saved.sessionId, app.program);
      assert.equal(app.requests.length, 0);
      app.store.update('prepare-a', { status: 'complete', artifactId: 'artifact-after-restart' });
      assert.deepEqual(await domainTerminal, { status: 'complete', artifactId: 'artifact-after-restart' });
      const oldAgent = app.runtime.native.agents.get(SessionId(saved.sessionId))!;
      const notifying = app.runtime.application.notifyOperation(saved.sessionId, accepted.followId, { actorId: 'notifier-one' });
      void notifying.catch(() => {});
      const admittedSignal = await readyEntered.promise;
      assert.equal(oldAgent.session.snapshotEvents().filter(event => pluginRecordOf(event)?.type === 'plugin:reactive/operation-delivery').length, 1);
      const readyInput = () => oldAgent.session.snapshotEvents().some(event => {
        const record = pluginRecordOf(event);
        return record?.type === 'plugin:reactive/input' && recordSchema.parse(record.data).requestId === 'operation:' + accepted.followId;
      });
      assert.equal(readyInput(), false);
      await app.runtime.application.close(saved.sessionId);
      assert.equal(admittedSignal.aborted, true);
      assert.equal(app.runtime.native.agents.get(SessionId(saved.sessionId)), undefined);
      readyRelease.resolve();
      await assert.rejects(notifying, error => error === admittedSignal.reason);
      assert.equal(app.runtime.native.agents.get(SessionId(saved.sessionId)), undefined, '旧 Agent 的迟到准入不得隐式激活新 Agent');
      assert.equal(readyInput(), false);
      assert.deepEqual(app.store.read('prepare-a')?.state, { status: 'complete', artifactId: 'artifact-after-restart' });
      assert.equal((await app.runtime.application.notifyOperation(saved.sessionId, accepted.followId, { actorId: 'notifier-alternative' })).status, 'accepted');
      await app.resumed.promise; await resumed.whenIdle();
      assert.equal(app.store.bodies().length, 1, '恢复只读取已有业务受理，不重新调用 execute');
      assert.equal(resumed.snapshot().waits.length, 0);
      assert.equal(resumed.snapshot().goal?.id, saved.goalId);
      assert.equal(resumed.snapshot().goal?.phase, 'complete');
      assert.equal(resumed.snapshot().requests['operation:' + accepted.followId]?.state, 'settled');
      const reads = app.store.db.prepare('SELECT command_json, access_json FROM reads ORDER BY id').all();
      assert.ok(reads.some(row => JSON.parse(z.string().parse(row.access_json)).source.entrypoint === 'operation-recovery'));
      assert.ok(reads.every(row => JSON.parse(z.string().parse(row.command_json)).commandId === 'prepare-a'));
      assert.equal(app.store.links().length, 1);
      const agent = app.runtime.native.agents.get(SessionId(saved.sessionId))!;
      assertOriginalGoalConcluded(agent.session.snapshotEvents(), { id: saved.goalId, revision: saved.goalRevision });
      const receipts = agent.session.snapshotEvents().flatMap(event => {
        const record = pluginRecordOf(event);
        return record?.type === 'plugin:reactive/operation-receipt' ? [receiptSchema.parse(record.data)] : [];
      });
      assert.deepEqual(receipts, [{ id: accepted.followId, reference: accepted.reference }]);
      assert.deepEqual(app.errors, []); cold.release(); resumed.release();
  });

  test('同 Goal 两个 intent 先持久而 wait 未登记即 SIGKILL，恢复须等全组授权与 wait ACK，单个终态不能越过另一业务条件', async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-barrier-'));
    const saved = await crashCheckpoint(root, context, 'multiple');
    assert.equal(saved.operations.length, 2);
    const secondRegister = Promise.withResolvers<void>(), secondAck = Promise.withResolvers<void>();
    const app = await scenario(root, 'settled', { recovering: true, parallel: true, async existingAck(link) {
      if (commandSchema.parse(link.command).commandId === 'prepare-b') { secondRegister.resolve(); await secondAck.promise; }
    } });
    context.after(() => app.dispose());
    try {
      const cold = await app.runtime.application.open(saved.sessionId, app.program);
      assert.equal(cold.snapshot().waits.length, 0);
      assert.equal(app.store.bodies().length, 2);
      assert.deepEqual(app.store.read('prepare-a')?.state, { status: 'complete', artifactId: 'artifact-before-crash' });
      const first = saved.operations.find(item => item.command.commandId === 'prepare-a')!;
      const second = saved.operations.find(item => item.command.commandId === 'prepare-b')!;
      const notifying = app.runtime.application.notifyOperation(saved.sessionId, first.followId, { actorId: 'notifier-one' });
      void notifying.catch(() => {});
      assert.equal(await Promise.race([secondRegister.promise.then(() => 'blocked'), notifying.then(() => 'delivered')]), 'blocked');
      assert.equal(app.store.reads().length, 0, '同 Goal 的另一可靠关联尚未 ACK，不能先 locate 第一个终态并回投');
      assert.equal(app.requests.length, 0);
      assert.equal(Object.keys(cold.snapshot().requests).length, 1);
      secondAck.resolve();
      assert.equal((await notifying).status, 'accepted');
      const session = await app.runtime.application.resume(saved.sessionId, app.program);
      await session.whenIdle();
      assert.equal(app.requests.length, 0, '第一个终态只解除自己的条件，不能提前进入下一模型步');
      assert.deepEqual(session.snapshot().waits.map(item => item.wait.id), ['operation:' + second.followId]);
      assert.equal(session.snapshot().requests['operation:' + first.followId]?.state, 'accepted');
      assert.equal(session.snapshot().requests['operation:' + second.followId], undefined);
      assert.equal(session.snapshot().goal?.id, saved.goalId);
      assert.equal(session.snapshot().goal?.revision, saved.goalRevision);
      assert.equal(session.snapshot().goal?.phase, 'active');
      const agent = app.runtime.native.agents.get(SessionId(saved.sessionId))!;
      const before = agent.session.snapshotEvents();
      const waits = before.filter(event => pluginRecordOf(event)?.type === 'plugin:reactive/wait');
      assert.equal(waits.length, 2);
      const ready = before.find(event => {
        const record = pluginRecordOf(event);
        return record?.type === 'plugin:reactive/input' && recordSchema.parse(record.data).requestId === 'operation:' + first.followId;
      });
      assert.ok(ready);
      assert.ok(waits.every(event => event.seq < ready.seq), '整组 native wait 须先于任何恢复的 ready 完成事实');
      app.store.update('prepare-b', { status: 'complete', artifactId: 'artifact-after-peer' });
      assert.equal((await app.runtime.application.notifyOperation(saved.sessionId, second.followId, { actorId: 'notifier-two' })).status, 'accepted');
      await app.resumed.promise; await session.whenIdle();
      assert.equal(app.store.bodies().length, 2, '两个已持久 command 都不能重执行原工具 body');
      assert.equal(session.snapshot().waits.length, 0);
      assert.equal(session.snapshot().goal?.id, saved.goalId);
      assert.equal(session.snapshot().goal?.phase, 'complete');
      assertOriginalGoalConcluded(agent.session.snapshotEvents(), { id: saved.goalId, revision: saved.goalRevision });
      for (const item of saved.operations) {
        assert.equal(session.snapshot().requests['operation:' + item.followId]?.state, 'settled');
        const messages = agent.session.snapshotEvents().filter(event => event.type === 'user/message'
          && 'rpcId' in event.data.source && event.data.source.rpcId === 'operation:' + item.followId);
        assert.equal(messages.length, 1, JSON.stringify({ root, sessionId: session.id, followId: item.followId,
          messages: messages.map(event => ({ seq: event.seq, ...event.type === 'user/message' ? { id: event.data.id, source: event.data.source } : {} })),
          modelInputs: app.requests.map(request => request.messages.filter(message => message.role === 'user').map(message => ({ id: message.id, source: message.source }))),
        }));
      }
      assert.equal(app.store.links().length, 2);
      assert.deepEqual(app.errors, []); cold.release(); session.release();
    } finally { secondAck.resolve(); }
  });
  test('原操作 F1 后由新工具登记 after；同正文 F2 按新参与交回，SIGKILL 恢复不重执行或重开旧等待', async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-stages-'));
    const saved = await crashCheckpoint(root, context, 'feedback');
    const app = await scenario(root, 'feedback', { recovering: true, multiStage: true });
    context.after(() => app.dispose());
    const links = app.store.links();
    assert.equal(links.length, 2);
    const first = links.find(link => link.after === null)!;
    const second = links.find(link => link.after !== null)!;
    assert.ok(first && second); assert.notEqual(first.id, second.id);
    assert.deepEqual(first.command, second.command);
    assert.equal(first.source.callId, 'native-observation-first');
    assert.equal(second.source.callId, 'native-observation-next');
    assert.equal(second.continuation.goal?.id, first.continuation.goal?.id);
    assert.equal(second.continuation.goal?.revision, (first.continuation.goal?.revision ?? 0) + 1);
    assert.equal(app.store.bodies().length, 1);
    const cold = await app.runtime.application.open(saved.sessionId, app.program);
    assert.equal(app.runtime.native.agents.get(SessionId(saved.sessionId)), undefined);
    assert.equal(cold.snapshot().waits.length, 0, 'crash 时 L2 intent 与业务登记已落盘，native wait 尚未登记');
    const reader = await cold.operations.preparation();
    const previous = reader.participation(first.id);
    const next = reader.participation(second.id);
    const f1 = await previous.readFeedback();
    assert.ok(f1.status === 'ready');
    assert.equal(second.after, f1.node.id);
    assert.deepEqual(await next.readFeedback(), { status: 'pending', reference: f1.reference });
    assert.equal(app.store.links().length, 2, '纯读取不能建立新的登记');
    assert.equal(app.runtime.native.agents.get(SessionId(saved.sessionId)), undefined);
    await assert.rejects(cold.readOperationFeedback(operation, { merchantId: 'merchant-a', commandId: 'prepare-b' }, first.id), /REACTIVE_OPERATION_FOLLOW_MISMATCH/u);
    const session = await app.runtime.application.resume(saved.sessionId, app.program);
    await session.whenIdle();
    assert.equal(app.requests.length, 0);
    assert.deepEqual(session.snapshot().waits.map(item => item.wait.id), [second.wait.id]);
    app.enabled$.next(false);
    await assert.rejects(async () => session.operations.preparation(), /REACTIVE_OPERATION_NOT_EXPOSED/u);
    assert.deepEqual(await previous.readFeedback(), f1, '已取得的 reader 不依赖当前模块继续公开槽');
    assert.equal((await app.runtime.application.notifyOperation(saved.sessionId, first.id, { actorId: 'notifier-one' })).status, 'accepted');
    await session.whenIdle();
    assert.equal(app.requests.length, 0, '重复 F1 只确认旧投递，不满足 L2');
    assert.deepEqual(session.snapshot().waits.map(item => item.wait.id), [second.wait.id]);
    app.store.recordFeedback(second.id, 'new-observation:' + second.id, f1.node.payload);
    const f2 = await next.readFeedback();
    assert.ok(f2.status === 'ready'); assert.notEqual(f2.node.id, f1.node.id);
    assert.deepEqual(f2.node.payload, f1.node.payload, '相同 unavailable 正文仍可属于两次不同的权威观察');
    assert.equal((await app.runtime.application.notifyOperation(saved.sessionId, second.id, { actorId: 'notifier-alternative' })).status, 'accepted');
    await app.resumed.promise; await session.whenIdle();
    assert.equal(app.store.bodies().length, 1, '两次参与和进程恢复都不能重执行原准备命令');
    assert.equal(session.snapshot().goal?.phase, 'complete');
    assert.equal(session.snapshot().waits.length, 0);
    assert.equal(app.store.links().length, 2);
    const agent = app.runtime.native.agents.get(SessionId(saved.sessionId))!;
    const events = agent.session.snapshotEvents();
    assert.equal(events.filter(event => pluginRecordOf(event)?.type === 'plugin:reactive/operation-receipt').length, 0,
      'Follow 没有 Action.execute，不能生成伪受理回执');
    const delivered = events.flatMap(event => {
      const record = pluginRecordOf(event);
      return record?.type === 'plugin:reactive/operation-delivery' ? [z.object({ id: z.string(), node: z.object({ id: z.string(), payload: feedbackSchema }) }).parse(record.data)] : [];
    });
    assert.deepEqual(delivered.map(item => [item.id, item.node.id]), [[first.id, f1.node.id], [second.id, f2.node.id]]);
    for (const link of [first, second]) assert.equal(events.filter(event => event.type === 'user/message'
      && 'rpcId' in event.data.source && event.data.source.rpcId === 'operation:' + link.id).length, 1);
    const goalChanges = events.flatMap(event => event.type === 'goal/change' && event.data.operation !== 'clear'
      ? [{ operation: event.data.operation, revision: event.data.goal.revision }] : []);
    assert.deepEqual(goalChanges, [{ operation: 'create', revision: 1 }, { operation: 'resume', revision: 2 },
      { operation: 'resume', revision: 3 }, { operation: 'complete', revision: 4 }]);
    app.store.db.prepare('UPDATE feedbacks SET node_id = ? WHERE follow_id = ?').run('owner-returned-different-node', first.id);
    await assert.rejects(previous.readFeedback(), /REACTIVE_OPERATION_FEEDBACK_CONFLICT/u);
    assert.equal(app.store.bodies().length, 1); assert.deepEqual(app.errors, []);
    cold.release(); session.release();
  });
} else if (process.argv[2] === '--operation-checkpoint') {
  const root = process.argv[3]!;
  const app = await scenario(root, 'settled');
  const session = await app.runtime.application.create('work', { workspaceId: app.workspace.workspaceId });
  await session.submit({ requestId: 'original-goal', content: '持续核验商家甲的准备工作', intent: 'goal', delivery: 'followup' as const, scope: { merchantId: 'merchant-a' } });
  await session.whenIdle();
  assert.equal(app.requests.length, 1);
  assert.equal(app.store.bodies().length, 1);
  const link = app.store.links()[0]!;
  app.store.update('prepare-a', { status: 'awaiting-command', reason: '商家甲需要补交凭据' });
  assert.deepEqual(await app.runtime.application.notifyOperation(session.id, link.id), { status: 'pending' });
  await session.whenIdle();
  assert.equal(app.requests.length, 1);
  assert.equal(session.snapshot().waits.length, 1);
  const agent = app.runtime.native.agents.get(SessionId(session.id))!;
  const native = app.runtime.native.get('sessions'); assert.ok(native);
  assert.equal(await native.flush(agent.session), true);
  const receipt = agent.session.snapshotEvents().flatMap(event => {
    const record = pluginRecordOf(event);
    return record?.type === 'plugin:reactive/operation-receipt' ? [receiptSchema.parse(record.data)] : [];
  })[0];
  assert.ok(receipt);
  const goal = session.snapshot().goal; assert.ok(goal);
  assert.deepEqual(app.errors, []);
  process.on('message', () => {});
  process.send!({ sessionId: session.id, goalId: goal.id, goalRevision: goal.revision,
    operations: [{ followId: link.id, command: commandSchema.parse(link.command), reference: receipt.reference }] } satisfies Checkpoint);
  await new Promise<never>(() => {});
} else if (process.argv[2] === '--operation-multi-checkpoint') {
  const root = process.argv[3]!;
  const linked = Promise.withResolvers<void>(), networkAck = Promise.withResolvers<void>();
  let registrations = 0;
  const app = await scenario(root, 'settled', { parallel: true, async registered() {
    if (++registrations === 2) linked.resolve();
    await networkAck.promise;
  } });
  const session = await app.runtime.application.create('work', { workspaceId: app.workspace.workspaceId });
  for (const commandId of ['prepare-a', 'prepare-b']) await session.resolveAction('prepare', { merchantId: 'merchant-a' })({ commandId }).result;
  app.store.update('prepare-a', { status: 'complete', artifactId: 'artifact-before-crash' });
  app.store.update('prepare-b', { status: 'awaiting-command', reason: '第二个命令仍需明确凭据' });
  await session.submit({ requestId: 'original-goal', content: '核验商家甲两个准备命令的正式结果', intent: 'goal', delivery: 'followup' as const, scope: { merchantId: 'merchant-a' } });
  await linked.promise;
  assert.equal(app.requests.length, 1);
  assert.equal(app.store.bodies().length, 2);
  assert.equal(session.snapshot().waits.length, 0);
  const agent = app.runtime.native.agents.get(SessionId(session.id))!;
  assert.equal(agent.session.snapshotEvents().filter(event => pluginRecordOf(event)?.type === 'plugin:reactive/operation-follow').length, 2);
  const sessions = app.runtime.native.get('sessions'); assert.ok(sessions);
  assert.equal(await sessions.flush(agent.session), true);
  const goal = session.snapshot().goal; assert.ok(goal);
  assert.deepEqual(app.errors, []);
  process.on('message', () => {});
  process.send!({ sessionId: session.id, goalId: goal.id, goalRevision: goal.revision,
    operations: app.store.links().map(link => {
      const command = commandSchema.parse(link.command), accepted = app.store.read(command.commandId); assert.ok(accepted);
      return { followId: link.id, command, reference: accepted.reference };
    }) } satisfies Checkpoint);
  await new Promise<never>(() => {});
} else {
  const root = process.argv[3]!;
  const linked = Promise.withResolvers<void>(), networkAck = Promise.withResolvers<void>();
  const app = await scenario(root, 'feedback', { multiStage: true, async registered(link) {
    if (link.after === null) {
      const withdrawn = firstValueFrom(app.enabledCommits$.pipe(filter(enabled => !enabled)));
      app.enabled$.next(false); await withdrawn;
      return;
    }
    linked.resolve(); await networkAck.promise;
  } });
  const session = await app.runtime.application.create('work', { workspaceId: app.workspace.workspaceId });
  await session.resolveAction('prepare', { merchantId: 'merchant-a' })({ commandId: 'prepare-a' }).result;
  app.store.update('prepare-a', { status: 'awaiting-command', reason: '原操作本次观察不可用，继续核验原操作' });
  await session.submit({ requestId: 'original-goal', content: '沿商家甲原准备操作完成两轮明确观察，不重提准备',
    intent: 'goal', delivery: 'followup' as const, scope: { merchantId: 'merchant-a' } });
  await session.whenIdle();
  const first = app.store.links()[0]!;
  assert.equal(app.requests.length, 1, 'owner ACK 前已撤下声明，已受理 Follow 仍成功收尾并结束本 turn');
  const firstAgent = app.runtime.native.agents.get(SessionId(session.id))!;
  const firstResult = firstAgent.session.snapshotEvents().find(event => event.type === 'tool/result'
    && event.data.message.toolCallId === first.source.callId);
  assert.ok(firstResult?.type === 'tool/result'); assert.notEqual(firstResult.data.message.isError, true);
  assert.deepEqual(JSON.parse(firstResult.data.message.content.map(block => block.type === 'text' ? block.text : '').join('')),
    { participationId: first.id, when: 'feedback', mode: 'wait' });
  const restored = firstValueFrom(app.enabledCommits$.pipe(filter(enabled => enabled)));
  app.enabled$.next(true); await restored;
  assert.equal((await app.runtime.application.notifyOperation(session.id, first.id, { actorId: 'notifier-one' })).status, 'accepted');
  await linked.promise;
  assert.equal(app.requests.length, 2); assert.equal(app.store.bodies().length, 1);
  assert.equal(app.store.links().length, 2); assert.equal(session.snapshot().waits.length, 0);
  const agent = app.runtime.native.agents.get(SessionId(session.id))!;
  const native = app.runtime.native.get('sessions'); assert.ok(native);
  assert.equal(await native.flush(agent.session), true);
  const goal = session.snapshot().goal; assert.ok(goal);
  assert.deepEqual(app.errors, []);
  process.on('message', () => {});
  process.send!({ sessionId: session.id, goalId: goal.id, goalRevision: goal.revision,
    operations: app.store.links().map(link => {
      const command = commandSchema.parse(link.command), accepted = app.store.read(command.commandId); assert.ok(accepted);
      return { followId: link.id, command, reference: accepted.reference };
    }) } satisfies Checkpoint);
  await new Promise<never>(() => {});
}
