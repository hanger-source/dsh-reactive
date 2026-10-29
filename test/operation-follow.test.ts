import { implementModule, implementProgram } from '@hanger-source/dsh-reactive/program';
import { test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate as immediate } from 'node:timers/promises';
import { DatabaseSync } from 'node:sqlite';
import { Fragment, createElement as h, useLayoutEffect } from 'react';
import { BehaviorSubject, Observable, Subject, filter, firstValueFrom } from 'rxjs';
import { LlmAdapter, ToolCallId, type GenerateOptions, type StreamChunk } from '@deepseek-ai/dsh-llm';
import { SessionId, pluginRecordOf, type SessionEvent } from '@deepseek-ai/dsh-session';
import { WorkspaceId } from '@deepseek-ai/dsh-workspace';
import { defineAction, implementAction, type ActionCallContext, type Eligibility, type Observation } from '@hanger-source/dsh-reactive';
import { defineOperation, implementOperation, operationFollowLinkSchema, type OperationAccessContext, type OperationFollowLink, type OperationNotificationAcknowledgement, type OperationNotificationConnection, type OperationNotificationResult, type OperationOwnerReference } from '@hanger-source/dsh-reactive';
import { ConversationAccessRevokedError, defineApplication, defineEnvironment, z, type ConversationEvent } from '@hanger-source/dsh-reactive';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import { Expose, Offer, Follow } from '@hanger-source/dsh-reactive/program';
import { createWorkSelector, defineProgram } from '@hanger-source/dsh-reactive';
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

interface BusinessNotifications {
  connected?(connection: OperationNotificationConnection, notify: (followId: string) => Promise<OperationNotificationResult>): void | PromiseLike<void>;
  acknowledge?(followId: string, result: OperationNotificationAcknowledgement): void | PromiseLike<void>;
  cleanup?(): void | PromiseLike<void>;
}

/** 业务 fixture 使用实际 SQLite 受理、follow、反馈和 outbox；设施显式选择本轮待办。 */
function business(database: string, onRegistered?: (link: OperationFollowLink, access: OperationAccessContext) => Promise<void>,
  onExistingAck?: (link: OperationFollowLink, access: OperationAccessContext) => Promise<void>,
  notifications?: BusinessNotifications, definition = operation) {
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
  let connection: OperationNotificationConnection | undefined, connectionCount = 0;
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
  const notify = async (followId: string) => {
    assert.ok(connection, '业务来源必须已经由 Host 连接');
    connection.signal.throwIfAborted();
    const pending = db.prepare('SELECT f.link_json FROM outbox o JOIN follows f ON f.id = o.follow_id WHERE o.follow_id = ? AND o.delivered = 0').get(followId);
    assert.ok(pending, '只交接 SQLite 中仍待确认的通知');
    const link = operationFollowLinkSchema.parse(JSON.parse(z.string().parse(pending.link_json)));
    return connection.deliver({ sessionId: link.continuation.sessionId, followId,
      async acknowledge(result) {
        await notifications?.acknowledge?.(followId, result);
        assert.equal(Number(db.prepare('UPDATE outbox SET delivered = 1 WHERE follow_id = ? AND delivered = 0').run(followId).changes), 1);
      },
    });
  };
  const owner = implementOperation(definition, {
    notifications: notifications === undefined ? undefined : {
      async connect(connected) {
        connectionCount++; connection = connected;
        await notifications.connected?.(connected, notify);
        return async () => {
          await notifications.cleanup?.();
          if (connection === connected) connection = undefined;
        };
      },
    },
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
      if (previous) { assert.deepEqual(JSON.parse(z.string().parse(previous.link_json)), link); await onExistingAck?.(link, access); return; }
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
      await onRegistered?.(link, access);
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
    connectionCount: () => connectionCount,
    notificationConnection() { assert.ok(connection, '业务来源必须已经由 Host 连接'); return connection; },
    notify,
    acknowledge(followId: string) { assert.equal(Number(db.prepare('UPDATE outbox SET delivered = 1 WHERE follow_id = ?').run(followId).changes), 1); },
    allowFollow(allowed: boolean) { db.prepare('UPDATE authorization SET can_follow = ? WHERE merchant_id = ?').run(allowed ? 1 : 0, 'merchant-a'); },
    reads: () => db.prepare('SELECT command_json, access_json FROM reads ORDER BY id').all(),
    close() { changed.complete(); db.close(); },
  };
}

async function scenario(root: string, when: 'settled' | 'feedback', options: {
  recovering?: boolean;
  readOnlyQuestion?: string;
  registered?: (link: OperationFollowLink, access: OperationAccessContext) => Promise<void>;
  existingAck?: (link: OperationFollowLink, access: OperationAccessContext) => Promise<void>;
  beforeWaitAck?: () => Promise<void>;
  beforeFollowAck?: () => Promise<void>;
  beforeUnregisteredAck?: () => Promise<void>;
  followEligibility?: Observable<Observation<Eligibility>>;
  additionalGoal?: { requestId: string; commandId: string };
  admitReady?: (signal: AbortSignal) => Promise<void>;
  parallel?: boolean;
  multiStage?: boolean;
  admissionUnknown?: boolean;
  backgroundThenWait?: boolean;
  reuseProviderCallId?: boolean;
  notifications?: BusinessNotifications;
  notificationPeers?: readonly OperationOwnerReference[];
} = {}) {
  const store = business(join(root, 'business.sqlite'), options.registered, options.existingAck, options.notifications);
  const enabled$ = new BehaviorSubject(true), enabledCommits$ = new Subject<boolean>();
  const requests: GenerateOptions[] = [], errors: unknown[] = [];
  const invocationIds: string[] = [];
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
  const dependencySurface = defineSurface({ views: {}, actions: {} });
  const operationModule = implementModule(dependencySurface, {
    operations: [store.owner, ...options.notificationPeers ?? []], Component() { return null; },
  });
  const nestedModule = implementModule(dependencySurface, { modules: { operations: operationModule }, Component() { return null; } });
  const definition = defineApplication({ name: 'operation-follow-' + when,
    async admit(input, _conversation, admission) {
      assert.ok(['operator-test', 'operation-publisher', 'notifier-one', 'notifier-two', 'notifier-alternative'].includes(admission.actorId));
      assert.deepEqual(input.scope, { merchantId: 'merchant-a' });
      if (input.result?.ready) await options.admitReady?.(admission.signal);
    },
    environments: [defineEnvironment({ id: 'work', instructions: '只根据原业务 owner 的明确事实继续原 Goal',
      program: implementProgram(defineProgram({ input: z.object({ merchantId: z.string() }).strict(), surface }), {
        modules: options.notifications ? { direct: operationModule, nested: nestedModule, repeated: operationModule } : {}, Component() {
        const action = useAction(prepareImplementation, () => ({ merchantId: 'merchant-a' }), []);
        useLayoutEffect(() => {
          const subscription = action.invocations$.subscribe(invocation => invocationIds.push(invocation.id));
          return () => subscription.unsubscribe();
        }, [action]);
        const complete = useAction(finishImplementation, () => null, []);
        const existing = useOperation(store.owner, () => ({ merchantId: 'merchant-a', commandId: 'prepare-a' }), []);
        const permission = useObservable(() => enabled$, []);
        const enabled = permission.status === 'ready' && permission.value;
        useLayoutEffect(() => { enabledCommits$.next(enabled); }, [enabled]);
        return h(Fragment, null, Offer({ name: 'prepare_merchant', action, enabled, follow: { when, scope: { merchantId: 'merchant-a' } } }),
          options.backgroundThenWait ? Offer({ name: 'prepare_background', action, enabled,
            follow: { when, scope: { merchantId: 'merchant-a' }, mode: 'background' } }) : null,
          Follow({ name: 'await_preparation', description: '只等待原准备操作的下一次明确反馈', operation: existing,
            when: 'feedback', enabled, eligibility: options.followEligibility, scope: { merchantId: 'merchant-a' } }),
          Offer({ name: 'finish_original_goal', action: complete }),
          Expose({ surface, value: { actions: { prepare: action }, ...enabled ? { operations: { preparation: existing } } : {} } }));
      } }),
    })],
  });
  const model = nativeModel({ provider: 'isolated', model: 'test', create: () => new class extends LlmAdapter {
      async *stream(request: GenerateOptions) {
        requests.push(request);
        const latestUser = request.messages.findLast(message => message.role === 'user');
        if (options.readOnlyQuestion && latestUser?.content.some(block => block.type === 'text' && block.text === options.readOnlyQuestion)) {
          yield* text('独立问题已读取，原业务仍等待权威结果');
          return;
        }
        const latest = request.messages.at(-1);
        if (latest?.role === 'tool' && latest.isError) {
          yield* text('本次调用失败，沿原操作核验事实，不自动重试');
          return;
        }
        if (options.additionalGoal && request.messages.some(message => message.role === 'user'
          && message.source?.kind === 'user' && 'rpcId' in message.source && message.source.rpcId === options.additionalGoal!.requestId)) {
          yield* tool('prepare_merchant', 'native-independent-command', { commandId: options.additionalGoal.commandId });
          return;
        }
        if (options.reuseProviderCallId) {
          if (requests.length <= 2) yield* tool('prepare_merchant', 'native-reused-provider-id', { commandId: 'prepare-a' });
          else if (requests.length === 3) yield* tool('finish_original_goal', 'finish-repeated-occurrences', {});
          else yield* text('两次真实参与已分别交回事实');
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
    operations: options.notifications ? [] : [store.owner], tools: 'native', goalRounds: 3, onError: event => errors.push(event.error),
    model: { ...model, async install(ctx, home) {
      let heldFollow = false;
      if (options.beforeFollowAck) ctx.on('session/flush', async session => {
        if (!heldFollow && session.snapshotEvents().some(event => pluginRecordOf(event)?.type === 'plugin:reactive/operation-follow')) {
          heldFollow = true; await options.beforeFollowAck!();
        }
      });
      let heldUnregistered = false;
      if (options.beforeUnregisteredAck) ctx.on('session/flush', async session => {
        if (!heldUnregistered && session.snapshotEvents().some(event => pluginRecordOf(event)?.type === 'plugin:reactive/operation-follow-unregistered')) {
          heldUnregistered = true; await options.beforeUnregisteredAck!();
        }
      });
      if (options.beforeWaitAck) ctx.on('session/flush', async session => {
        if (session.snapshotEvents().some(event => pluginRecordOf(event)?.type === 'plugin:reactive/wait')) await options.beforeWaitAck!();
      });
      await model.install(ctx, home);
    } } }).catch(error => {
      enabled$.complete(); enabledCommits$.complete(); store.close(); throw error;
    });
  await mkdir(join(root, 'workspace'), { recursive: true });
  const workspace = await runtime.application.createWorkspace({ directory: join(root, 'workspace') });
  return { runtime, workspace, program: definition.environments[0].program.definition, store, requests, invocationIds, errors, bodyWaits, resumed, enabled$, enabledCommits$,
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
  for (const withdrawal of ['pending', 'denied'] as const) test(`独立Follow intent持久ACK期间资格转${withdrawal}后恢复也不复活，并发通知等待首登结案且冷读保留原工作`, { timeout: 25_000 }, async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-follow-unregistered-'));
    const eligibility = new BehaviorSubject<Observation<Eligibility>>({ status: 'ready', generation: 1,
      value: { allowed: true }, observedAt: Date.now() });
    const checkpoint = Promise.withResolvers<void>(), release = Promise.withResolvers<void>();
    const app = await scenario(root, 'feedback', { multiStage: true, notifications: {}, followEligibility: eligibility,
      async beforeFollowAck() { checkpoint.resolve(); await release.promise; } });
    context.after(() => { release.resolve(); eligibility.complete(); return app.dispose(); });
    const session = await app.runtime.application.create('work', { workspaceId: app.workspace.workspaceId });
    await session.resolveAction('prepare', { merchantId: 'merchant-a' })({ commandId: 'prepare-a' }).result;
    const changes: ConversationEvent[] = [];
    const subscription = session.events$.subscribe(event => { if (event.kind === 'operation.follow.changed') changes.push(event); });
    context.after(() => subscription.unsubscribe());
    await session.submit({ requestId: 'original-observation', content: '只观察已受理商家操作的明确反馈', intent: 'goal',
      delivery: 'followup', scope: { merchantId: 'merchant-a' } });
    await checkpoint.promise;
    const initial = session.snapshot().operationFollows[0]!;
    assert.ok(initial);
    assert.deepEqual(app.store.read('prepare-a')?.state, { status: 'running' });
    assert.deepEqual(app.store.links(), [], 'intent持久ACK尚未返回，业务owner还未取得登记权');
    let notified = false;
    const notifying = app.runtime.application.notifyOperation(session.id, initial.link.id, { actorId: 'notifier-one' })
      .then(result => { notified = true; return result; });
    void notifying.catch(() => {});
    await immediate();
    assert.equal(notified, false, '有权限通知也必须等待初次准入进入门');
    assert.deepEqual(app.store.links(), [], '通知不能越过intent checkpoint抢先登记');
    eligibility.next(withdrawal === 'pending' ? { status: 'pending', generation: 2 }
      : { status: 'ready', generation: 2, observedAt: Date.now(), value: { allowed: false, reason: '商家观察授权已撤销' } });
    eligibility.next({ status: 'ready', generation: 3, observedAt: Date.now(), value: { allowed: true } });
    release.resolve();
    assert.deepEqual(await notifying, { status: 'unregistered' });
    await session.whenIdle();
    const closed = session.snapshot().operationFollows[0]!;
    assert.deepEqual(closed, { link: initial.link, receipt: null, preparedDelivery: null, notStarted: null,
      unregistered: { id: initial.link.id, continuation: initial.link.continuation, wait: initial.link.wait } });
    assert.deepEqual(changes.map(event => event.kind === 'operation.follow.changed' ? event.change : null), ['intent', 'unregistered']);
    const closure = changes.at(-1)!;
    assert.ok(closure.kind === 'operation.follow.changed');
    assert.deepEqual(closure.follow, closed);
    assert.deepEqual(closure.inputs, initial.link.continuation.inputs);
    assert.deepEqual(closure.inputs.map(input => input.requestId), ['original-observation']);
    const work = await createWorkSelector({ identify: origin => origin.scope })
      .observe({ merchantId: 'merchant-a' }, [session], { signal: context.signal }).state.read(context.signal);
    const condition = work.sessions[0]!.conditions.find(condition => condition.kind === 'operation.follow-unregistered');
    assert.ok(condition?.kind === 'operation.follow-unregistered');
    assert.deepEqual(condition.value, closed);
    assert.deepEqual(condition.matchedInputs, initial.link.continuation.inputs);
    assert.equal(condition.resultRequest, null);
    assert.deepEqual(session.snapshot().waits, []);
    assert.equal(session.snapshot().requests['operation:' + initial.link.id], undefined);
    assert.deepEqual(app.store.links(), []);
    assert.equal(app.store.bodies().length, 1, '拒绝观察登记不撤销或重复原业务命令');
    const sessionId = session.id;
    session.release(); await app.runtime.application.close(sessionId);
    const cold = await app.runtime.application.open(sessionId, app.program);
    assert.deepEqual(cold.snapshot().operationFollows, [closed]);
    const acknowledgements: OperationNotificationAcknowledgement[] = [];
    assert.deepEqual(await app.store.notificationConnection().deliver({ sessionId, followId: initial.link.id,
      acknowledge(result) { acknowledgements.push(result); } }), { status: 'unregistered' });
    assert.deepEqual(acknowledgements, [{ status: 'unregistered' }]);
    assert.equal(app.runtime.native.agents.get(SessionId(sessionId)), undefined, '旧通知结案ACK不激活原生Agent');
    assert.deepEqual(app.store.links(), []);
    assert.equal(app.store.bodies().length, 1);
    assert.deepEqual(app.store.read('prepare-a')?.state, { status: 'running' });
    assert.deepEqual(app.errors, []); cold.release();
  });

  test('未登记结案checkpoint拒绝只封锁原link与Goal，同会话另一Goal仍可登记并读取自己的业务命令', { timeout: 25_000 }, async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-follow-checkpoint-isolation-'));
    const checkpointFailure = new Error('UNREGISTERED_CHECKPOINT_ACK_FAILED');
    const checkpoint = Promise.withResolvers<void>(), release = Promise.withResolvers<void>();
    const eligibility = new BehaviorSubject<Observation<Eligibility>>({ status: 'ready', generation: 1,
      observedAt: Date.now(), value: { allowed: true } });
    const app = await scenario(root, 'feedback', { multiStage: true, notifications: {}, followEligibility: eligibility,
      additionalGoal: { requestId: 'independent-goal', commandId: 'prepare-b' },
      async beforeFollowAck() { checkpoint.resolve(); await release.promise; },
      async beforeUnregisteredAck() { throw checkpointFailure; },
    });
    context.after(() => { release.resolve(); eligibility.complete(); return app.dispose(); });
    const session = await app.runtime.application.create('work', { workspaceId: app.workspace.workspaceId });
    await session.resolveAction('prepare', { merchantId: 'merchant-a' })({ commandId: 'prepare-a' }).result;
    await session.submit({ requestId: 'failed-observation', content: '观察原商家操作', intent: 'goal',
      delivery: 'followup', scope: { merchantId: 'merchant-a' } });
    await checkpoint.promise;
    const failed = session.snapshot().operationFollows[0]!;
    let acknowledgements = 0;
    const rejected = assert.rejects(app.store.notificationConnection().deliver({ sessionId: session.id, followId: failed.link.id,
      acknowledge() { acknowledgements++; } }), error => error === checkpointFailure);
    eligibility.next({ status: 'pending', generation: 2 });
    eligibility.next({ status: 'ready', generation: 3, observedAt: Date.now(), value: { allowed: true } });
    release.resolve();
    await rejected; await session.whenIdle();
    assert.equal(acknowledgements, 0, '结案checkpoint未确认时不能向来源伪报unregistered成功');
    assert.deepEqual(app.store.links(), [], '失败目标不能由通知重登记');
    const agent = app.runtime.native.agents.get(SessionId(session.id))!;
    const original = app.runtime.native.goals.get(agent); assert.ok(original);
    // 用正式Goal服务结束原修订；新工作仍从真实submit的原输入创建自己的Goal。
    app.runtime.native.goals.complete(agent, original);
    await session.submit({ requestId: 'independent-goal', content: '受理另一命令并沿自己的Goal核验', intent: 'goal',
      delivery: 'followup', scope: { merchantId: 'merchant-a' } });
    await session.whenIdle();
    const [independent] = app.store.links(); assert.ok(independent);
    assert.equal(commandSchema.parse(independent.command).commandId, 'prepare-b');
    assert.notEqual(independent.continuation.goal?.id, failed.link.continuation.goal?.id);
    assert.deepEqual(independent.continuation.inputs.map(input => input.requestId), ['independent-goal']);
    assert.deepEqual(session.snapshot().waits.map(item => item.wait.id), [independent.wait.id]);
    assert.deepEqual(await session.locateOperation(operation, { merchantId: 'merchant-a', commandId: 'prepare-b' }), {
      status: 'accepted', reference: app.store.read('prepare-b')!.reference, state: { status: 'running' },
    });
    assert.deepEqual(await session.readOperationFeedback(operation, commandSchema.parse(independent.command), independent.id), {
      status: 'pending', reference: app.store.read('prepare-b')!.reference,
    });
    await assert.rejects(app.store.notificationConnection().deliver({ sessionId: session.id, followId: failed.link.id,
      acknowledge() { acknowledgements++; } }), error => error === checkpointFailure);
    assert.equal(acknowledgements, 0);
    assert.deepEqual(app.store.links(), [independent]);
    assert.deepEqual(app.store.bodies().map(body => body.command_id), ['prepare-a', 'prepare-b']);
    session.release();
  });

  test('独立Follow已进入SQLite登记且忽略取消时Host等待真实ACK，取消观察不伪写未登记或重执行原Operation', { timeout: 25_000 }, async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-follow-entry-drain-'));
    const entered = Promise.withResolvers<{ link: OperationFollowLink; signal: AbortSignal }>(), release = Promise.withResolvers<void>();
    let returned = false, disposed = false, disposing: Promise<void> | undefined;
    const app = await scenario(root, 'feedback', { multiStage: true,
      async registered(link, access) { entered.resolve({ link, signal: access.signal }); await release.promise; returned = true; } });
    context.after(() => { release.resolve(); return disposing ?? app.dispose(); });
    const session = await app.runtime.application.create('work', { workspaceId: app.workspace.workspaceId });
    await session.resolveAction('prepare', { merchantId: 'merchant-a' })({ commandId: 'prepare-a' }).result;
    await session.submit({ requestId: 'original-observation', content: '观察原业务，退出观察不撤销其受理', intent: 'goal',
      delivery: 'followup', scope: { merchantId: 'merchant-a' } });
    const registration = await entered.promise;
    const agent = app.runtime.native.agents.get(SessionId(session.id))!;
    assert.deepEqual(app.store.links(), [registration.link], '可靠登记事务已经COMMIT，网络ACK仍在途');
    assert.equal(app.store.bodies().length, 1);
    await session.cancel();
    disposing = app.dispose().then(() => { disposed = true; });
    void disposing.catch(() => {});
    await immediate();
    assert.equal(registration.signal.aborted, true);
    assert.equal(returned, false);
    assert.equal(disposed, false, '工具取消和Host退出不能冒充不配合取消的owner Promise已结算');
    assert.equal(agent.session.snapshotEvents().some(event => pluginRecordOf(event)?.type === 'plugin:reactive/operation-follow-unregistered'), false);
    assert.deepEqual(app.store.links(), [registration.link]);
    release.resolve(); await disposing;
    assert.equal(returned, true);
    const recovered = await scenario(root, 'feedback', { recovering: true, multiStage: true });
    context.after(() => recovered.dispose());
    const cold = await recovered.runtime.application.open(session.id, recovered.program);
    const saved = cold.snapshot().operationFollows[0]!;
    assert.deepEqual(saved.link, registration.link);
    assert.equal(saved.unregistered, null);
    assert.equal(saved.notStarted, null);
    assert.equal(recovered.store.bodies().length, 1);
    assert.deepEqual(recovered.store.read('prepare-a')?.state, { status: 'running' });
    assert.deepEqual(recovered.store.links(), [registration.link]);
    assert.equal(recovered.requests.length, 0);
    assert.equal(recovered.runtime.native.agents.get(SessionId(session.id)), undefined);
    cold.release(); session.release();
  });

  test('同会话跨turn复用provider callId时真实Invocation与follow分离，原inputs和Goal修订独立且冷读不重执', async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-reused-call-'));
    const app = await scenario(root, 'feedback', { reuseProviderCallId: true });
    context.after(() => app.dispose());
    const session = await app.runtime.application.create('work', { workspaceId: app.workspace.workspaceId });
    const events: ConversationEvent[] = [];
    const subscription = session.events$.subscribe(event => events.push(event));
    context.after(() => subscription.unsubscribe());
    await session.submit({ requestId: 'original-goal', content: '先后核验同一商家命令的两次实际参与', intent: 'goal',
      delivery: 'followup', scope: { merchantId: 'merchant-a' } });
    await session.whenIdle();
    const first = app.store.links()[0]!;
    assert.ok(first);
    const firstSnapshot = structuredClone(session.snapshot().operationFollows);
    app.store.recordFeedback(first.id, 'first-occurrence-feedback', { status: 'awaiting-command', reason: '第一次参与需要进一步核验' });
    assert.equal((await app.runtime.application.notifyOperation(session.id, first.id, { actorId: 'notifier-one' })).status, 'accepted');
    await session.whenIdle();
    const second = app.store.links().find(link => link.id !== first.id);
    assert.ok(second, '相同provider标签、owner和command必须形成第二个真实follow');
    assert.equal(second.source.callId, first.source.callId);
    assert.deepEqual(second.command, first.command);
    assert.deepEqual(second.operation, first.operation);
    assert.equal(app.invocationIds.length, 2);
    assert.notEqual(app.invocationIds[0], app.invocationIds[1]);
    const calls = session.snapshot().calls.filter(call => call.callId === first.source.callId);
    assert.equal(calls.length, 2);
    assert.notEqual(calls[0]!.callSequence, calls[1]!.callSequence);
    assert.deepEqual(first.continuation.inputs, calls[0]!.inputs);
    assert.deepEqual(second.continuation.inputs, calls[1]!.inputs);
    assert.deepEqual(first.continuation.inputs.map(input => input.requestId), ['original-goal']);
    assert.ok(second.continuation.inputs.some(input => input.requestId === 'operation:' + first.id));
    assert.ok(first.continuation.goal && second.continuation.goal);
    assert.equal(first.continuation.goal.id, second.continuation.goal.id);
    assert.ok(second.continuation.goal.revision > first.continuation.goal.revision);
    assert.deepEqual(session.snapshot().operationFollows.find(follow => follow.link.id === first.id)?.link,
      firstSnapshot[0]!.link, '第二次同名发生不能改写第一次参与的来源和Goal');
    const intents = events.filter(event => event.kind === 'operation.follow.changed' && event.change === 'intent');
    assert.equal(intents.length, 2);
    assert.deepEqual(intents.map(event => event.kind === 'operation.follow.changed' ? event.follow.link.id : null), [first.id, second.id]);
    assert.deepEqual(session.snapshot().waits.map(item => item.wait.id), [second.wait.id]);
    assert.equal(app.store.bodies().length, 2, '真实第二次调用确实进入业务，不能由provider标签去重');
    const saved = structuredClone(session.snapshot().operationFollows);
    const sessionId = session.id;
    session.release(); await app.runtime.application.close(sessionId);
    const cold = await app.runtime.application.open(sessionId, app.program);
    assert.deepEqual(cold.snapshot().operationFollows, saved);
    assert.deepEqual(app.store.links().map(link => link.id).sort(), [first.id, second.id].sort());
    await cold.locateOperation(operation, commandSchema.parse(second.command));
    const feedback = await cold.readOperationFeedback(operation, commandSchema.parse(first.command), first.id);
    assert.equal(feedback.status, 'ready');
    assert.equal(app.runtime.native.agents.get(SessionId(sessionId)), undefined, '冷读持久关联不能激活模型Agent');
    assert.equal(app.store.bodies().length, 2);
    assert.equal(app.requests.length, 2);
    assert.equal(app.invocationIds.length, 2);
    assert.deepEqual(app.errors, []); cold.release();
  });

  test('同名普通请求被消费后仍恢复原Follow，正式结果身份由准入核验，投递冲突保留prepared诊断和冷读事实', async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-request-collision-'));
    const ordinaryQuestion = '只读取这个独立问题，不推进原商家准备';
    const recoveries: string[] = [];
    const app = await scenario(root, 'settled', { readOnlyQuestion: ordinaryQuestion,
      async existingAck(_link, access) {
        if (access.source.kind === 'participant') recoveries.push(access.source.entrypoint);
      },
    });
    context.after(() => app.dispose());
    const session = await app.runtime.application.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'original-goal', content: '准备商家并等待正式结算', intent: 'goal', delivery: 'followup',
      scope: { merchantId: 'merchant-a' } });
    await session.whenIdle();
    const [link] = app.store.links(); assert.ok(link); assert.ok(link.continuation.goal);
    const requestId = 'operation:' + link.id;
    const invalidResults = [
      { continuation: { ...link.continuation, goal: { ...link.continuation.goal, revision: link.continuation.goal.revision + 1 } },
        wait: link.wait, ready: true },
      { continuation: { ...link.continuation, inputs: [] }, wait: link.wait, ready: true },
      { continuation: link.continuation, wait: { ...link.wait, operation: { unrelated: true } }, ready: true },
    ];
    for (const result of invalidResults) {
      await assert.rejects(session.submit({ requestId, content: '不能取得原工作关联的结果', intent: 'result',
        scope: { merchantId: 'merchant-a' }, result }), /REACTIVE_WAIT_ID_CONFLICT/u);
      assert.equal(session.snapshot().requests[requestId], undefined, '关联冲突不得持久受理为原结果');
    }
    await session.submit({ requestId, content: ordinaryQuestion, intent: 'question', delivery: 'followup',
      scope: { merchantId: 'merchant-a' } });
    await session.whenIdle();
    assert.equal(session.snapshot().requests[requestId]?.state, 'settled');
    assert.equal(session.snapshot().requests[requestId]?.declaration?.intent, 'question');
    const beforeResume = recoveries.length;
    const resumed = await app.runtime.application.resume(session.id, app.program);
    assert.ok(recoveries.slice(beforeResume).includes('operation-recovery'), '同名普通请求已消费仍须幂等恢复原owner关联');
    assert.equal(app.store.bodies().length, 1, '恢复不重新执行业务Action');
    assert.deepEqual(app.store.links(), [link]);
    app.store.update('prepare-a', { status: 'complete', artifactId: 'artifact-with-request-collision' });
    await assert.rejects(app.runtime.application.notifyOperation(session.id, link.id, { actorId: 'notifier-one' }), /REACTIVE_REQUEST_ID_CONFLICT/u);
    const prepared = session.snapshot().operationFollows[0]!;
    assert.equal(prepared.preparedDelivery?.requestId, requestId);
    const selector = createWorkSelector({ identify: origin => origin.scope });
    const progress = await selector.observe({ merchantId: 'merchant-a' }, [session], { signal: context.signal }).state.read(context.signal);
    const observed = progress.sessions[0]!;
    assert.equal(observed.operationFollows[0]?.resultRequest, null, '同名普通输入不能充当原Follow的结果');
    assert.ok(observed.conditions.some(condition => condition.kind === 'operation.delivery-prepared' && condition.value.link.id === link.id),
      '已消费的普通输入不能掩盖正式正文尚未受理');
    assert.deepEqual(session.snapshot().waits, [{ continuation: link.continuation, wait: link.wait }]);
    assert.equal(app.store.bodies().length, 1);
    await app.runtime.application.close(session.id);
    const cold = await app.runtime.application.open(session.id, app.program);
    const coldProgress = await selector.observe({ merchantId: 'merchant-a' }, [cold], { signal: context.signal }).state.read(context.signal);
    assert.deepEqual(coldProgress.sessions[0]!.operationFollows, observed.operationFollows);
    assert.ok(coldProgress.sessions[0]!.conditions.some(condition => condition.kind === 'operation.delivery-prepared'));
    assert.equal(app.runtime.native.agents.get(SessionId(session.id)), undefined, '冷读不能激活Agent或重执Action');
    assert.equal(app.store.bodies().length, 1);
    assert.deepEqual(app.errors, []);
    session.release(); resumed.release(); cold.release();
  });

  test('静态Module owner连接一次，SQLite通知ACK失败后沿原请求重投，暂停结果不被模型消费且保留反馈事实', async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-source-retry-'));
    const ackFailure = new Error('BUSINESS_OUTBOX_ACK_FAILED');
    const acknowledgements: OperationNotificationAcknowledgement[] = [];
    const app = await scenario(root, 'feedback', { notifications: {
      acknowledge(_id, result) {
        acknowledgements.push(result);
        if (acknowledgements.length === 1) throw ackFailure;
      },
    } });
    context.after(() => app.dispose());
    assert.equal(app.store.connectionCount(), 1, '同一owner沿直接、嵌套和重复静态依赖只连接一次');
    const session = await app.runtime.application.create('work', { workspaceId: app.workspace.workspaceId });
    const followEvents: ConversationEvent[] = [];
    const followSubscription = session.events$.subscribe(event => { if (event.kind === 'operation.follow.changed') followEvents.push(event); });
    context.after(() => followSubscription.unsubscribe());
    await session.submit({ requestId: 'original-goal', content: '准备一次并等待商家的明确反馈', intent: 'goal', delivery: 'followup', scope: { merchantId: 'merchant-a' } });
    await session.whenIdle();
    const [link] = app.store.links(); assert.ok(link);
    assert.deepEqual(session.snapshot().operationFollows, [{ link, receipt: { reference: app.store.read('prepare-a')!.reference },
      preparedDelivery: null, notStarted: null, unregistered: null }]);
    assert.deepEqual(await app.store.notify(link.id), { status: 'pending' });
    assert.equal(acknowledgements.length, 0, '业务节点尚未就绪时不能消费outbox');
    assert.deepEqual(app.store.outbox(), [link.id]);
    await session.pause();
    const paused = session.snapshot().goal; assert.ok(paused);
    const node = { id: 'merchant-investigation:' + link.id, payload: { status: 'awaiting-command' as const, reason: '商家甲缺少正式凭据' } };
    app.store.recordFeedback(link.id, node.id, node.payload);
    await assert.rejects(app.store.notify(link.id), error => error === ackFailure);
    const acknowledgedFollow = session.snapshot().operationFollows[0]!;
    assert.deepEqual(acknowledgedFollow.preparedDelivery?.node, node);
    assert.equal(acknowledgedFollow.preparedDelivery?.requestId, 'operation:' + link.id);
    assert.equal(session.snapshot().requests['operation:' + link.id]?.state, 'accepted');
    assert.deepEqual(app.store.outbox(), [link.id], '原生受理成功但业务ACK失败，来源必须保留重投待办');
    const retried = await app.store.notify(link.id);
    assert.equal(retried.status, 'accepted');
    assert.deepEqual(acknowledgements, [retried, retried], '重投确认同一次原生持久受理');
    assert.deepEqual(app.store.outbox(), []);
    assert.deepEqual(app.store.links(), [link], '通知确认不能移除持久follow');
    const feedback = await session.readOperationFeedback(operation, commandSchema.parse(link.command), link.id);
    assert.ok(feedback.status === 'ready'); assert.deepEqual(feedback.node, node, '通知ACK不能移除业务反馈节点');
    await session.whenIdle();
    assert.equal(app.requests.length, 1, 'accepted不代表模型已消费，旧反馈不能越过暂停后的Goal修订');
    assert.equal(session.snapshot().goal?.phase, 'paused');
    assert.equal(session.snapshot().goal?.revision, paused.revision);
    assert.equal(session.snapshot().requests['operation:' + link.id]?.state, 'accepted');
    assert.deepEqual(session.snapshot().operationFollows, [acknowledgedFollow], '来源ACK重试不产生第二份准备正文或改写关联');
    assert.deepEqual(followEvents.map(event => event.kind === 'operation.follow.changed' ? event.change : null),
      ['intent', 'receipt', 'delivery-prepared']);
    const intentEvent = followEvents[0]!, receiptEvent = followEvents[1]!, deliveryEvent = followEvents[2]!;
    assert.ok(intentEvent.kind === 'operation.follow.changed' && receiptEvent.kind === 'operation.follow.changed'
      && deliveryEvent.kind === 'operation.follow.changed');
    assert.equal(intentEvent.follow.receipt, null);
    assert.equal(intentEvent.follow.preparedDelivery, null);
    assert.deepEqual(receiptEvent.follow.receipt, acknowledgedFollow.receipt);
    assert.equal(receiptEvent.follow.preparedDelivery, null);
    assert.deepEqual(deliveryEvent.follow, acknowledgedFollow, '每次事件保留当时事实切面，后续受理不改写历史事件');
    for (const event of followEvents) {
      assert.ok(event.kind === 'operation.follow.changed');
      assert.deepEqual(event.inputs, link.continuation.inputs);
      assert.equal(event.follow.link.id, link.id);
    }
    const work = createWorkSelector({ identify(origin) {
      const scope = origin.scope;
      return scope !== null && typeof scope === 'object' && !Array.isArray(scope)
        && 'merchantId' in scope && typeof scope.merchantId === 'string' ? scope.merchantId : undefined;
    } }).observe('merchant-a', [session], { signal: context.signal });
    const progress = await work.state.read(context.signal);
    const observed = progress.sessions[0]!;
    const operationProgress = observed.operationFollows.find(follow => follow.value.link.id === link.id);
    assert.ok(operationProgress);
    assert.deepEqual(operationProgress.value.preparedDelivery?.node, node);
    assert.equal(operationProgress.resultRequest?.requestId, 'operation:' + link.id);
    assert.equal(operationProgress.resultRequest?.state, 'accepted');
    assert.ok(observed.conditions.some(condition => condition.kind === 'continuation.unconsumed'
      && condition.request.value.requestId === 'operation:' + link.id));
    assert.equal(observed.conditions.some(condition => condition.kind === 'operation.delivery-prepared'), false,
      '原生已受理后不能继续报告为仅准备正文');
    const heldFeedback = observed.conditions.find(condition => condition.kind === 'continuation.superseded'
      && condition.request.value.requestId === 'operation:' + link.id);
    assert.ok(heldFeedback?.kind === 'continuation.superseded');
    assert.deepEqual(heldFeedback.continuation, link.continuation, '原因保留业务原follow的正式继续关联');
    assert.equal(heldFeedback.currentGoal?.revision, paused.revision);
    assert.equal(heldFeedback.currentGoal?.phase, 'paused');
    assert.ok(observed.conditions.some(condition => condition.kind === 'goal.paused'
      && condition.matchedInputs.some(input => input.requestId === 'original-goal')));
    assert.ok(heldFeedback.request.matchedDeclaration, '未消费的反馈仍可按正式请求声明归入原业务工作');
    assert.equal(heldFeedback.request.value.state, 'accepted');
    assert.ok(observed.inputs.some(input => input.value.requestId === 'operation:' + link.id
      && input.value.messageId.length > 0), '暂停的反馈保留原生inbox已受理输入，不能误报为模型已消费');
    assert.equal(app.store.bodies().length, 1, '业务ACK重投不能再次执行原Action');
    const events = app.runtime.native.agents.get(SessionId(session.id))!.session.snapshotEvents();
    const inputs = events.flatMap(event => {
      const record = pluginRecordOf(event);
      return record?.type === 'plugin:reactive/input' ? [recordSchema.parse(record.data)] : [];
    }).filter(record => record.requestId === 'operation:' + link.id);
    assert.equal(inputs.length, 1, '重投沿同一次持久输入关联，不生成第二份投递正文');
    assert.deepEqual(inputs[0]!.result?.continuation.goal, link.continuation.goal);
    assert.deepEqual(app.errors, []); session.release();
  });

  test('来源ACK尚未结算时dispose不释放来源，ACK完成后仍等待异步cleanup真正停止', { timeout: 25_000 }, async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-source-drain-'));
    const ackEntered = Promise.withResolvers<void>(), ackRelease = Promise.withResolvers<void>();
    const cleanupEntered = Promise.withResolvers<void>(), cleanupRelease = Promise.withResolvers<void>();
    let cleaning = false, disposed = false, disposing: Promise<void> | undefined;
    const app = await scenario(root, 'feedback', { notifications: {
      async acknowledge() { ackEntered.resolve(); await ackRelease.promise; },
      async cleanup() { cleaning = true; cleanupEntered.resolve(); await cleanupRelease.promise; },
    } });
    context.after(() => { ackRelease.resolve(); cleanupRelease.resolve(); return disposing ?? app.dispose(); });
    const session = await app.runtime.application.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'original-goal', content: '等待来源可靠交回反馈', intent: 'goal', delivery: 'followup', scope: { merchantId: 'merchant-a' } });
    await session.whenIdle(); await session.pause();
    const [link] = app.store.links(); assert.ok(link);
    app.store.recordFeedback(link.id, 'drain-feedback:' + link.id, { status: 'awaiting-command', reason: '保留通知交接的数据库连接' });
    const connection = app.store.notificationConnection();
    const delivery = app.store.notify(link.id);
    await ackEntered.promise;
    disposing = app.dispose().then(() => { disposed = true; });
    void disposing.catch(() => {});
    await immediate();
    assert.equal(connection.signal.aborted, true);
    assert.equal(disposed, false, 'Host退出必须等待已经开始的真实ACK');
    assert.equal(cleaning, false, '来源cleanup不能提前关闭ACK仍使用的连接');
    assert.deepEqual(app.store.outbox(), [link.id]);
    ackRelease.resolve();
    assert.equal((await delivery).status, 'accepted');
    await cleanupEntered.promise;
    assert.equal(disposed, false, 'ACK结束后仍须等待来源异步cleanup');
    assert.deepEqual(app.store.outbox(), []);
    assert.deepEqual(app.store.links(), [link]);
    assert.equal(app.store.db.prepare('SELECT count(*) AS count FROM feedbacks').get()?.count, 1);
    cleanupRelease.resolve(); await disposing;
  });

  test('部分来源连接失败时等待晚返回连接与其异步cleanup，不提前完成启动失败', { timeout: 25_000 }, async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-source-startup-'));
    const lateEntered = Promise.withResolvers<AbortSignal>(), aborted = Promise.withResolvers<void>(), lateReturn = Promise.withResolvers<void>();
    const cleanupEntered = Promise.withResolvers<void>(), cleanupRelease = Promise.withResolvers<void>();
    const connectFailure = new Error('BUSINESS_SOURCE_CONNECT_FAILED');
    let cleaned = false, settled = false;
    const peerDefinition = defineOperation({ kind: 'warehouse-preparation', version: 1, command: commandSchema, reference: referenceSchema,
      state: stateSchema, settled: state => state.status === 'complete' ? state : undefined, feedback: feedbackSchema });
    const peer = business(join(root, 'peer.sqlite'), undefined, undefined, {
      async connected(connection) {
        connection.signal.addEventListener('abort', () => aborted.resolve(), { once: true });
        lateEntered.resolve(connection.signal); await lateReturn.promise;
      },
      async cleanup() { cleaned = true; cleanupEntered.resolve(); await cleanupRelease.promise; },
    }, peerDefinition);
    const starting = scenario(root, 'settled', { notificationPeers: [peer.owner], notifications: {
      async connected() { await lateEntered.promise; throw connectFailure; },
    } });
    const completed = starting.then(async app => { settled = true; await app.dispose(); assert.fail('部分来源失败不能签发已安装Host'); }, error => { settled = true; return error as unknown; });
    context.after(async () => { lateReturn.resolve(); cleanupRelease.resolve(); try { await completed; } finally { peer.close(); } });
    const signal = await lateEntered.promise;
    await aborted.promise; await immediate();
    assert.equal(signal.aborted, true);
    assert.equal(settled, false, '连接尚未交出cleanup，启动失败不能结束');
    assert.equal(cleaned, false);
    lateReturn.resolve(); await cleanupEntered.promise;
    assert.equal(settled, false, '晚返回的cleanup仍未停止，启动失败不能结束');
    cleanupRelease.resolve();
    const failure = await completed;
    assert.ok(failure instanceof Error);
    assert.equal(peer.connectionCount(), 1);
  });

  test('通知恢复原wait的可靠登记忽略取消时，dispose仍等待真实owner结算后才cleanup', { timeout: 25_000 }, async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-source-recovery-drain-'));
    const entered = Promise.withResolvers<AbortSignal>(), release = Promise.withResolvers<void>();
    let gate = false, cleaned = false, disposed = false, acknowledgements = 0, disposing: Promise<void> | undefined;
    const app = await scenario(root, 'feedback', {
      async existingAck(_link, access) {
        if (!gate || access.source.kind !== 'participant' || access.source.entrypoint !== 'operation-recovery') return;
        entered.resolve(access.signal); await release.promise;
      },
      notifications: { acknowledge() { acknowledgements++; }, cleanup() { cleaned = true; } },
    });
    context.after(() => { release.resolve(); return disposing ?? app.dispose(); });
    const session = await app.runtime.application.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'original-goal', content: '保持原工作等待并可靠交回反馈', intent: 'goal', delivery: 'followup', scope: { merchantId: 'merchant-a' } });
    await session.whenIdle(); await session.pause();
    const [link] = app.store.links(); assert.ok(link);
    app.store.recordFeedback(link.id, 'recovery-drain:' + link.id, { status: 'awaiting-command', reason: '原登记正在等待业务ACK' });
    gate = true;
    const connection = app.store.notificationConnection();
    const rejected = assert.rejects(app.store.notify(link.id), error => error === connection.signal.reason);
    const recoverySignal = await entered.promise;
    disposing = app.dispose().then(() => { disposed = true; });
    void disposing.catch(() => {});
    await rejected; await immediate();
    assert.equal(recoverySignal.aborted, true);
    assert.equal(disposed, false, '外层通知退等不能冒充原owner Promise已经退出');
    assert.equal(cleaned, false, '原可靠登记尚未结算，cleanup不能关闭其数据库来源');
    assert.equal(acknowledgements, 0);
    assert.deepEqual(app.store.outbox(), [link.id]);
    assert.equal(app.store.bodies().length, 1);
    release.resolve(); await disposing;
    assert.equal(cleaned, true);
  });

  test('来源误投另一owner的持久冷follow时拒绝，不激活Agent也不确认通知', async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-source-owner-'));
    const saved = await crashCheckpoint(root, context);
    const [original] = saved.operations; assert.ok(original);
    const history = new DatabaseSync(join(root, 'business.sqlite'), { readOnly: true });
    const readsBeforeRecovery = history.prepare('SELECT command_json, access_json FROM reads ORDER BY id').all();
    history.close();
    let acknowledged = 0;
    const foreignDefinition = defineOperation({ kind: 'foreign-preparation', version: 1, command: commandSchema, reference: referenceSchema,
      state: stateSchema, settled: state => state.status === 'complete' ? state : undefined, feedback: feedbackSchema });
    const foreign = business(join(root, 'foreign.sqlite'), undefined, undefined, {
      async connected(connection) {
        await assert.rejects(connection.deliver({ sessionId: saved.sessionId, followId: original.followId,
          acknowledge() { acknowledged++; } }), /REACTIVE_OPERATION_OWNER_MISMATCH/u);
      },
    }, foreignDefinition);
    const app = await scenario(root, 'settled', { recovering: true, notifications: {}, notificationPeers: [foreign.owner] });
    context.after(async () => { try { await app.dispose(); } finally { foreign.close(); } });
    assert.equal(foreign.connectionCount(), 1);
    assert.equal(acknowledged, 0);
    assert.equal(app.runtime.native.agents.get(SessionId(saved.sessionId)), undefined, '跨owner地址须在冷激活前拒绝');
    assert.deepEqual(app.store.reads(), readsBeforeRecovery, '跨owner误投不能新增或改写原业务owner的读取');
    assert.deepEqual(app.store.outbox(), [original.followId]);
    assert.equal(app.store.bodies().length, 1); assert.equal(app.requests.length, 0);
  });

  test('冷通知的成员资格退出及时结束业务准入，迟到ACK不取得新图且独立Operation仍结算', { timeout: 25_000 }, async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-membership-'));
    const entered = Promise.withResolvers<AbortSignal>(), release = Promise.withResolvers<void>(), returned = Promise.withResolvers<void>();
    let gate = false;
    const app = await scenario(root, 'settled', { async existingAck(_link, access) {
      if (!gate || access.source.kind !== 'participant' || access.source.actorId !== 'notifier-one') return;
      entered.resolve(access.signal); await release.promise; returned.resolve();
    } });
    context.after(() => { release.resolve(); return app.dispose(); });
    const session = await app.runtime.application.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'original-goal', content: '只准备一次并等待原业务结算', intent: 'goal', delivery: 'followup', scope: { merchantId: 'merchant-a' } });
    await session.whenIdle();
    const [link] = app.store.links(); assert.ok(link);
    const reference = app.store.read('prepare-a')!.reference, sessionId = session.id;
    session.release(); await app.runtime.application.close(sessionId);
    const cold = await app.runtime.application.open(sessionId, app.program);
    const bound = cold.operations.preparation();
    const independent = await app.runtime.application.attachOperation(sessionId, operation, reference);
    const terminal = independent.settled(); void terminal.catch(() => {});
    gate = true;
    let outcome: unknown;
    const notifying = app.runtime.application.notifyOperation(sessionId, link.id, { actorId: 'notifier-one' })
      .then(value => { outcome = value; }, error => { outcome = error; });
    const admission = await entered.promise;
    const workspace = app.runtime.native.workspaceRegistry.get(WorkspaceId(app.workspace.workspaceId))!;
    await workspace.detachSession(SessionId(sessionId)); await immediate();
    const reason: unknown = cold.signal.reason;
    assert.ok(reason instanceof ConversationAccessRevokedError && reason.reason === 'membership-removed');
    assert.equal(admission.aborted, true); assert.equal(admission.reason, reason);
    assert.equal(outcome, reason, '不配合signal的业务准入ACK尚未返回时，通知等待已按原资格结束');
    await workspace.attachSession(SessionId(sessionId));
    const fresh = await app.runtime.application.open(sessionId, app.program);
    assert.notEqual(fresh.accessId, cold.accessId);
    await assert.rejects(async () => cold.locateOperation(operation, { merchantId: 'merchant-a', commandId: 'prepare-a' }), error => error === reason);
    assert.throws(() => bound.locate(), error => error === reason, '旧Bound查询不能从相同sessionId取得新资格');
    release.resolve(); await returned.promise; await notifying; await immediate();
    assert.equal(app.runtime.native.agents.get(SessionId(sessionId)), undefined, '迟到ACK没有激活新的原生Agent');
    assert.equal(app.requests.length, 1); assert.equal(app.store.bodies().length, 1);
    assert.equal(fresh.snapshot().requests['operation:' + link.id], undefined);
    app.store.update('prepare-a', { status: 'complete', artifactId: 'artifact-after-membership' });
    assert.deepEqual(await terminal, { status: 'complete', artifactId: 'artifact-after-membership' }, '原独立Operation观察仍由业务owner结算');
    assert.deepEqual(app.errors, []); cold.release(); fresh.release();
  });

  test('background受理后继续同一Goal，仍受另一wait和暂停约束，冷恢复不重执且迟到结果不取得新修订权限', async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-background-'));
    const app = await scenario(root, 'settled', { backgroundThenWait: true, notifications: {} });
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
    assert.equal((await app.store.notify(background.id)).status, 'accepted');
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
    assert.equal((await app.store.notify(waiting.id)).status, 'accepted');
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
    const followChanges: ConversationEvent[] = [];
    const changes = session.events$.subscribe(event => { if (event.kind === 'operation.follow.changed') followChanges.push(event); });
    context.after(() => changes.unsubscribe());
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
    const closedFollow = session.snapshot().operationFollows[0]!;
    assert.deepEqual(closedFollow, { link, receipt: null, preparedDelivery: null, unregistered: null,
      notStarted: { id: link.id, continuation: link.continuation, wait: link.wait } });
    assert.deepEqual(followChanges.map(event => event.kind === 'operation.follow.changed' ? event.change : null), ['intent', 'not-started']);
    const closure = followChanges.at(-1)!;
    assert.ok(closure.kind === 'operation.follow.changed');
    assert.deepEqual(closure.follow, closedFollow);
    assert.deepEqual(closure.inputs, link.continuation.inputs);
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
    const closedNotifications: OperationNotificationAcknowledgement[] = [];
    let initializationDelivery: OperationNotificationResult | undefined;
    const recovered = await scenario(root, 'settled', { recovering: true, notifications: {
      async connected(_connection, notify) { initializationDelivery = await notify(link.id); },
      acknowledge(_id, result) { closedNotifications.push(result); },
    } });
    context.after(() => recovered.dispose());
    const cold = await recovered.runtime.application.open(sessionId, recovered.program);
    assert.deepEqual(cold.snapshot().operationFollows, [closedFollow], '冷恢复读取原未开始事实及原关联身份');
    const coldEvents: ConversationEvent[] = [];
    const coldChanges = cold.events$.subscribe(event => coldEvents.push(event));
    context.after(() => coldChanges.unsubscribe());
    await immediate();
    assert.deepEqual(coldEvents, [], '读取已有持久follow不重放发生事件');
    assert.deepEqual(cold.snapshot().waits, []);
    assert.equal(recovered.runtime.native.agents.get(SessionId(sessionId)), undefined);
    assert.deepEqual(initializationDelivery, { status: 'not-started' }, 'connect可同步开始交接，不等待Host.initialize自身');
    assert.deepEqual(closedNotifications, [{ status: 'not-started' }]);
    assert.deepEqual(recovered.store.outbox(), [], '冷未开始登记只确认通知待办');
    assert.deepEqual(recovered.store.links(), [link]);
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
    const agent = app.runtime.native.agents.get(SessionId(session.id))!;
    const workspace = app.runtime.native.workspaceRegistry.get(WorkspaceId(app.workspace.workspaceId))!;
    if (boundary === 'cancel') await session.cancel();
    else await workspace.detachSession(SessionId(session.id));
    acknowledge.resolve();
    let observed = session;
    let revoked: ConversationAccessRevokedError | undefined;
    if (boundary === 'workspace-guard') {
      const reason: unknown = session.signal.reason;
      assert.ok(reason instanceof ConversationAccessRevokedError && reason.reason === 'membership-removed');
      revoked = reason;
      assert.throws(() => session.whenIdle(), error => error === reason);
      await agent.whenIdle();
      assert.equal(app.runtime.native.agents.get(agent.id), agent, '成员撤销不退出正在结算原调用的Agent');
      await workspace.attachSession(agent.id);
      observed = await app.runtime.application.open(session.id, app.program);
      assert.notEqual(observed.accessId, session.accessId);
      assert.equal(session.signal.reason, reason, '新取得不恢复旧句柄资格');
    }
    await observed.whenIdle();
    assert.equal(app.store.bodies().length, 0);
    assert.equal(app.bodyWaits.length, 0);
    assert.deepEqual(observed.snapshot().waits, []);
    assert.equal(observed.snapshot().requests['operation:' + link.id], undefined);
    assert.equal(observed.snapshot().goal?.activation, 'disarmed');
    const events = agent.session.snapshotEvents();
    assert.equal(events.filter(event => pluginRecordOf(event)?.type === 'plugin:reactive/operation-not-started').length, 1);
    assert.equal(events.filter(event => pluginRecordOf(event)?.type === 'plugin:reactive/operation-delivery').length, 0);
    if (boundary === 'workspace-guard') {
      assert.equal(app.requests.length, 1, '原生下一步装配也必须拒绝已被移除的成员，不能再次进入模型');
      assert.equal(app.errors.length, 1);
      assert.equal(app.errors[0], revoked);
    } else assert.deepEqual(app.errors, []);
    if (observed !== session) observed.release();
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
      const originalFollow = cold.snapshot().operationFollows[0]!;
      assert.equal(originalFollow.link.id, accepted.followId);
      assert.deepEqual(originalFollow.receipt, { reference: accepted.reference });
      assert.equal(originalFollow.preparedDelivery, null);
      assert.equal(originalFollow.notStarted, null);
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
      assert.deepEqual(cold.snapshot().operationFollows, [originalFollow]);
      assert.equal(app.store.bodies().length, 1, '冷读取原receipt和关联不重执命令');
      const resumed = await app.runtime.application.resume(saved.sessionId, app.program);
      const followChanges: ConversationEvent[] = [];
      const changes = resumed.events$.subscribe(event => { if (event.kind === 'operation.follow.changed') followChanges.push(event); });
      context.after(() => changes.unsubscribe());
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
      const prepared = resumed.snapshot().operationFollows[0]!;
      assert.deepEqual(prepared.link, originalFollow.link);
      assert.deepEqual(prepared.receipt, originalFollow.receipt);
      assert.equal(prepared.preparedDelivery?.requestId, 'operation:' + accepted.followId);
      assert.deepEqual(prepared.preparedDelivery?.reference, accepted.reference);
      assert.equal(resumed.snapshot().requests['operation:' + accepted.followId], undefined,
        '业务正文已持久准备，准入尚未完成，不能报告为原生已受理');
      assert.equal(app.requests.length, 0);
      assert.deepEqual(followChanges.map(event => event.kind === 'operation.follow.changed' ? event.change : null), ['delivery-prepared']);
      const preparedEvent = followChanges[0]!;
      assert.ok(preparedEvent.kind === 'operation.follow.changed');
      assert.deepEqual(preparedEvent.follow, prepared);
      assert.deepEqual(preparedEvent.inputs, originalFollow.link.continuation.inputs);
      const preparedWork = await createWorkSelector({ identify: origin => origin.scope })
        .observe({ merchantId: 'merchant-a' }, [resumed], { signal: context.signal }).state.read(context.signal);
      const preparedCondition = preparedWork.sessions[0]!.conditions.find(condition => condition.kind === 'operation.delivery-prepared');
      assert.ok(preparedCondition?.kind === 'operation.delivery-prepared');
      assert.equal(preparedCondition.value.link.id, accepted.followId);
      assert.equal(preparedCondition.resultRequest, null);
      assert.equal(preparedWork.sessions[0]!.conditions.some(condition => condition.kind === 'continuation.unconsumed'), false);
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
      assert.deepEqual(resumed.snapshot().operationFollows, [prepared], '重启继续使用原持久关联和已准备正文，不重签身份');
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
      const selector = createWorkSelector({ identify: origin => origin.scope });
      const completedWork = await selector.observe({ merchantId: 'merchant-a' }, [resumed], { signal: context.signal }).state.read(context.signal);
      const completedFollow = completedWork.sessions[0]!.operationFollows.find(follow => follow.value.link.id === accepted.followId);
      assert.ok(completedFollow);
      assert.equal(completedFollow.resultRequest?.state, 'settled');
      assert.deepEqual(completedFollow.resultRequest?.declaration?.result,
        { continuation: originalFollow.link.continuation, wait: originalFollow.link.wait, ready: true });
      assert.equal(completedWork.sessions[0]!.conditions.some(condition => condition.kind === 'operation.delivery-prepared'), false);
      await app.runtime.application.close(saved.sessionId);
      const completedCold = await app.runtime.application.open(saved.sessionId, app.program);
      const coldWork = await selector.observe({ merchantId: 'merchant-a' }, [completedCold], { signal: context.signal }).state.read(context.signal);
      assert.deepEqual(coldWork.sessions[0]!.operationFollows, completedWork.sessions[0]!.operationFollows,
        '正式结果从受理到消费再到冷读，始终关联同一原工作');
      assert.equal(app.runtime.native.agents.get(SessionId(saved.sessionId)), undefined);
      assert.equal(app.store.bodies().length, 1);
      assert.deepEqual(app.errors, []); cold.release(); resumed.release(); completedCold.release();
  });

  test('同Goal持久intent共用恢复barrier，通知者取消不终止整组，晚失败归owner且原结果不重投', async context => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-barrier-'));
    const saved = await crashCheckpoint(root, context, 'multiple');
    assert.equal(saved.operations.length, 2);
    const secondRegister = Promise.withResolvers<AbortSignal>(), secondAck = Promise.withResolvers<void>();
    const peerEntered = Promise.withResolvers<void>(), failureEntered = Promise.withResolvers<AbortSignal>(), failureRelease = Promise.withResolvers<void>();
    const lateFailure = new Error('RECOVERY_REGISTER_ACK_FAILED_WITHOUT_WAITER');
    const recoveryRegistrations = new Map<AbortSignal, { links: string[]; deliveryDeclared: boolean }>();
    let nativeEvents!: () => readonly SessionEvent[];
    let peerRegistrations = 0, failRecovery = false;
    const app = await scenario(root, 'settled', { recovering: true, parallel: true, async existingAck(link, access) {
      if (access.source.kind !== 'participant') return;
      if (access.source.actorId === 'notifier-alternative' && ++peerRegistrations === 2) peerEntered.resolve();
      if (access.source.entrypoint !== 'operation-recovery') return;
      assert.equal(access.source.participant, 'service'); assert.equal(access.source.actorId, 'operation-publisher');
      const deliveryDeclared = nativeEvents().some(event => pluginRecordOf(event)?.type === 'plugin:reactive/operation-delivery');
      const round = recoveryRegistrations.get(access.signal);
      if (round) {
        assert.equal(round.deliveryDeclared, deliveryDeclared);
        round.links.push(link.id);
      } else recoveryRegistrations.set(access.signal, { links: [link.id], deliveryDeclared });
      if (commandSchema.parse(link.command).commandId === 'prepare-b') {
        secondRegister.resolve(access.signal); await secondAck.promise;
        if (failRecovery) {
          failRecovery = false; failureEntered.resolve(access.signal); await failureRelease.promise; throw lateFailure;
        }
      }
    } });
    context.after(() => app.dispose());
    nativeEvents = () => app.runtime.native.agents.get(SessionId(saved.sessionId))!.session.snapshotEvents();
    try {
      const cold = await app.runtime.application.open(saved.sessionId, app.program);
      assert.equal(cold.snapshot().waits.length, 0);
      assert.equal(app.store.bodies().length, 2);
      assert.deepEqual(app.store.read('prepare-a')?.state, { status: 'complete', artifactId: 'artifact-before-crash' });
      const first = saved.operations.find(item => item.command.commandId === 'prepare-a')!;
      const second = saved.operations.find(item => item.command.commandId === 'prepare-b')!;
      const controller = new AbortController(), reason = new Error('FIRST_NOTIFIER_CANCELLED_OWN_WAIT');
      const notifying = app.runtime.application.notifyOperation(saved.sessionId, first.followId, { actorId: 'notifier-one', signal: controller.signal });
      const cancelled = assert.rejects(notifying, error => error === reason);
      const recoverySignal = await secondRegister.promise;
      assert.equal(app.store.reads().length, 0, '同 Goal 的另一可靠关联尚未 ACK，不能先 locate 第一个终态并回投');
      assert.equal(app.requests.length, 0);
      assert.equal(Object.keys(cold.snapshot().requests).length, 1);
      let peerSettled = false;
      const peer = app.runtime.application.notifyOperation(saved.sessionId, first.followId, { actorId: 'notifier-alternative' })
        .then(value => { peerSettled = true; return value; });
      void peer.catch(() => {});
      await peerEntered.promise; await immediate();
      controller.abort(reason);
      assert.equal(await Promise.race([cancelled.then(() => 'cancelled'), immediate().then(() => 'pending')]), 'cancelled');
      assert.equal(recoverySignal.aborted, false, '通知者不能取消配置publisher持有的整组恢复');
      assert.equal(peerSettled, false);
      assert.equal(recoveryRegistrations.size, 1, 'A与B在未完成的真实恢复signal上等待，不另起一组登记');
      const blockedRound = recoveryRegistrations.get(recoverySignal)!;
      assert.equal(blockedRound.deliveryDeclared, false);
      assert.deepEqual([...blockedRound.links].sort(), [first.followId, second.followId].sort());
      await assert.rejects(app.runtime.application.notifyOperation(saved.sessionId, second.followId, { actorId: 'notifier-one' }),
        /BUSINESS_FOLLOW_NOTIFIER_SCOPE_DENIED/u, '共享barrier不能把另一命令的通知权限借给当前actor');
      secondAck.resolve();
      assert.equal((await peer).status, 'accepted');
      assert.deepEqual([...blockedRound.links].sort(), [first.followId, second.followId].sort(), '退等和并发通知未重复进入原阻塞恢复轮');
      assert.equal(recoveryRegistrations.size, 2);
      const [deliverySignal, deliveryRound] = [...recoveryRegistrations].find(([signal]) => signal !== recoverySignal)!;
      assert.notEqual(deliverySignal, recoverySignal);
      assert.equal(deliveryRound.deliveryDeclared, true, '独立恢复signal在真实delivery checkpoint之后重新核验整组');
      assert.deepEqual([...deliveryRound.links].sort(), [first.followId, second.followId].sort(), '交付前重新核验每个关联一次，不复用先前授权');
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
      failRecovery = true;
      const stopped = new AbortController(), stoppedReason = new Error('LAST_NOTIFIER_CANCELLED_BEFORE_RECOVERY_FAILURE');
      const stoppedNotification = assert.rejects(app.runtime.application.notifyOperation(saved.sessionId, second.followId,
        { actorId: 'notifier-two', signal: stopped.signal }), error => error === stoppedReason);
      const failingSignal = await failureEntered.promise;
      stopped.abort(stoppedReason);
      assert.equal(await Promise.race([stoppedNotification.then(() => 'cancelled'), immediate().then(() => 'pending')]), 'cancelled');
      assert.equal(failingSignal.aborted, false);
      const ownerFailure = assert.rejects(session.whenIdle(), error => error instanceof AggregateError && error.errors.includes(lateFailure));
      failureRelease.resolve(); await ownerFailure;
      assert.equal(session.snapshot().requests['operation:' + second.followId], undefined);
      assert.equal(app.requests.length, 0, '无等待者的恢复失败仍由原owner报告，不追加模型或伪造完成结果');
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
    } finally { secondAck.resolve(); failureRelease.resolve(); }
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
