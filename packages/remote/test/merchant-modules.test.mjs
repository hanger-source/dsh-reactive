import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { setImmediate as immediate } from 'node:timers/promises';
import { build } from 'esbuild';
import { Observable, Subject, filter, firstValueFrom, map, of, timeout } from 'rxjs';
import { LlmAdapter, ToolCallId } from '@deepseek-ai/dsh-llm';
import { defineApplication, defineEnvironment } from '@hanger-source/dsh-reactive';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';

const first = (source, predicate = () => true) => firstValueFrom(source.pipe(filter(predicate), timeout({ first: 8_000 })));
const at = '2026-10-27T01:00:00.000Z';
const preparedAt = '2026-10-27T01:00:01.000Z';
const targetA = { onboardingId: '11111111-1111-4111-8111-111111111111', merchant: {
  subject: { kind: 'merchant', id: 'merchant-a' }, sandboxId: 'sandbox-a', deliveryIdentity: 'sha256:' + 'a'.repeat(64),
} };
const targetB = { onboardingId: '22222222-2222-4222-8222-222222222222', merchant: {
  subject: { kind: 'merchant', id: 'merchant-b' }, sandboxId: 'sandbox-b', deliveryIdentity: 'sha256:' + 'b'.repeat(64),
} };
const operationId = '33333333-3333-4333-8333-333333333333';
const deliveryCommandId = '44444444-4444-4444-8444-444444444444';
const unknownId = '55555555-5555-4555-8555-555555555555';
const domains = ['preparation', 'knowledge', 'reception', 'commands'];
const views = ['preparation.facts', 'knowledge.facts', 'reception.facts', 'commands.facts', 'collaboration.facts'];
const merchant = (session, target) => session.collection('merchants').instance(target.onboardingId);

/** 一个 bundle 保留全部 reference 合同身份；已安装 core、React、RxJS 始终使用同一外部模块。 */
async function installedReference(directory) {
  const core = fileURLToPath(import.meta.resolve('@hanger-source/dsh-reactive'));
  const reference = join(dirname(dirname(core)), 'examples', 'merchant-operations');
  const outfile = join(directory, 'merchant-reference.mjs');
  await build({
    stdin: { contents: [
      "export * from './program.tsx';", "export * from './host.ts';",
      "export * from './contracts/index.ts';", "export * from './program-contracts.ts';",
    ].join('\n'), resolveDir: reference, sourcefile: 'merchant-reference.ts' },
    outfile, bundle: true, platform: 'node', format: 'esm', target: 'node24', jsx: 'automatic', logLevel: 'silent',
    plugins: [{ name: 'installed-package-identity', setup(builder) {
      builder.onResolve({ filter: /^(?:@hanger-source\/dsh-reactive(?:\/.*)?|react(?:\/.*)?|rxjs(?:\/.*)?|zod)$/ }, args =>
        ({ path: fileURLToPath(import.meta.resolve(args.path)), external: true }));
    } }],
  });
  return import(pathToFileURL(outfile).href);
}

/** 这里只保存试验实际经过的权威事实与两类受理记录，不模拟部署、知识生成或接待后端。 */
function business(database, reference) {
  const db = new DatabaseSync(database);
  db.exec(`PRAGMA journal_mode = WAL;
    CREATE TABLE facts (domain TEXT NOT NULL, target TEXT NOT NULL, generation INTEGER NOT NULL, document TEXT NOT NULL,
      PRIMARY KEY(domain, target));
    CREATE TABLE operations (kind TEXT NOT NULL, session_id TEXT NOT NULL, invocation_id TEXT NOT NULL,
      command_json TEXT NOT NULL, reference_json TEXT NOT NULL, state_json TEXT NOT NULL, generation INTEGER NOT NULL,
      PRIMARY KEY(kind, session_id, invocation_id), UNIQUE(kind, reference_json));
    CREATE TABLE bodies (id INTEGER PRIMARY KEY, kind TEXT NOT NULL, session_id TEXT NOT NULL,
      invocation_id TEXT NOT NULL, source_json TEXT NOT NULL);`);
  const changes = new Subject();
  const connections = new Map();
  const pending = new Set();
  const readFailures = new Map();
  const schemas = { preparation: reference.preparationFactsSchema, knowledge: reference.knowledgeFactsSchema,
    reception: reference.receptionFactsSchema, commands: reference.merchantCommandFactsSchema };
  const key = (domain, target) => domain === 'commands'
    ? JSON.stringify([target.subject.kind, target.subject.id, target.sandboxId, target.deliveryIdentity]) : target.onboardingId;
  const address = (domain, target) => domain + ':' + key(domain, target);
  const transaction = body => {
    db.exec('BEGIN IMMEDIATE');
    try { const result = body(); db.exec('COMMIT'); return result; }
    catch (error) { db.exec('ROLLBACK'); throw error; }
  };
  const readFacts = (domain, target) => {
    const row = db.prepare('SELECT generation, document FROM facts WHERE domain = ? AND target = ?').get(domain, key(domain, target));
    assert.ok(row, '只有已存在的权威事实可以 ready');
    return { generation: row.generation, value: schemas[domain].parse(JSON.parse(row.document)) };
  };
  const writeFacts = (domain, target, value) => {
    const parsed = schemas[domain].parse(value);
    assert.equal(Number(db.prepare('UPDATE facts SET generation = generation + 1, document = ? WHERE domain = ? AND target = ?')
      .run(JSON.stringify(parsed), domain, key(domain, target)).changes), 1);
  };
  const facts = (domain, target) => new Observable(subscriber => {
    const id = address(domain, target);
    connections.set(id, (connections.get(id) ?? 0) + 1);
    const emit = () => {
      try {
        const current = readFacts(domain, target);
        if (readFailures.has(id)) { subscriber.next({ status: 'error', generation: current.generation, error: readFailures.get(id) }); return; }
        subscriber.next(pending.has(id) ? { status: 'pending', generation: current.generation }
          : { status: 'ready', ...current, observedAt: Date.now() });
      } catch (error) { subscriber.error(error); }
    };
    const subscription = changes.subscribe(change => { if (change === id) emit(); });
    emit();
    return () => { subscription.unsubscribe(); connections.set(id, connections.get(id) - 1); };
  });
  for (const [scope, revision] of [[targetA, 1], [targetB, 7]]) {
    const initial = {
      preparation: { onboardingId: scope.onboardingId, revision, canPrepare: true, canRunCommand: true, canObserveCommand: true,
        preparation: null, command: null, currentPreparation: null, currentDeliveryCommand: null },
      knowledge: { onboardingId: scope.onboardingId, revision, canInitialize: true, canRetry: true, canCommit: true,
        sourceActivationId: null, ready: false, snapshotReasonCode: null, tasks: [], currentTask: null },
      reception: { onboardingId: scope.onboardingId, revision, canEnable: true, canResume: true, operation: null, currentReception: null },
      commands: { target: scope.merchant, canExecute: true, canObserve: true, canReconcile: true, commands: [], currentCommand: null },
    };
    for (const domain of domains) {
      const target = domain === 'commands' ? scope.merchant : scope;
      db.prepare('INSERT INTO facts VALUES (?, ?, 1, ?)').run(domain, key(domain, target), JSON.stringify(schemas[domain].parse(initial[domain])));
    }
  }
  const authorize = access => {
    access.signal.throwIfAborted();
    assert.equal(access.source.kind, 'participant');
    assert.equal(access.source.actorId, 'module-operator');
    assert.ok(access.sessionId);
  };
  const accepted = (definition, command) => {
    const row = db.prepare('SELECT * FROM operations WHERE kind = ? AND session_id = ? AND invocation_id = ?')
      .get(definition.kind, command.sessionId, command.invocationId);
    if (row) assert.deepEqual(JSON.parse(row.command_json), command);
    return row;
  };
  const stateAt = (definition, rawReference) => {
    const identity = JSON.stringify(definition.reference.parse(rawReference));
    return db.prepare('SELECT * FROM operations WHERE kind = ? AND reference_json = ?').get(definition.kind, identity);
  };
  const owner = definition => ({
    locate(command, access) {
      authorize(access);
      const row = accepted(definition, command);
      return row ? { status: 'accepted', reference: JSON.parse(row.reference_json), state: JSON.parse(row.state_json) } : { status: 'unknown' };
    },
    observe(identity, access) {
      authorize(access);
      return new Observable(subscriber => {
        const emit = () => {
          try {
            const row = stateAt(definition, identity);
            if (!row) throw new Error('BUSINESS_OPERATION_UNKNOWN');
            subscriber.next({ status: 'ready', generation: row.generation, value: JSON.parse(row.state_json), observedAt: Date.now() });
          } catch (error) { subscriber.error(error); }
        };
        const subscription = changes.subscribe(change => { if (change === definition.kind) emit(); });
        emit();
        return () => subscription.unsubscribe();
      });
    },
    // 本试验不登记原生等待；绝不能把一个未实现的可靠登记回答为成功 ACK。
    registerFollow() { throw new Error('REFERENCE_TRIAL_FOLLOW_FORBIDDEN'); },
    readFeedback() { throw new Error('REFERENCE_TRIAL_FOLLOW_FORBIDDEN'); },
  });
  const persist = (definition, command, identity, state, access) => {
    const previous = accepted(definition, command);
    if (previous) return JSON.parse(previous.reference_json);
    db.prepare('INSERT INTO operations VALUES (?, ?, ?, ?, ?, ?, 1)').run(definition.kind, command.sessionId, command.invocationId,
      JSON.stringify(definition.command.parse(command)), JSON.stringify(definition.reference.parse(identity)), JSON.stringify(definition.state.parse(state)));
    db.prepare('INSERT INTO bodies (kind, session_id, invocation_id, source_json) VALUES (?, ?, ?, ?)')
      .run(definition.kind, access.sessionId, access.invocationId, JSON.stringify(access.source));
    return identity;
  };
  const forbidden = () => { throw new Error('REFERENCE_TRIAL_ACTION_FORBIDDEN'); };
  const ownerServices = { preparation: owner(reference.Preparation), deliveryCommand: owner(reference.DeliveryCommand),
    knowledgeTask: owner(reference.KnowledgeTask), reception: owner(reference.Reception), merchantCommand: owner(reference.MerchantCommand) };
  const services = {
    preparation: {
      facts: target => facts('preparation', target),
      prepare(target, input, access) {
        authorize(access); assert.equal(target.onboardingId, targetA.onboardingId);
        const current = readFacts('preparation', target).value;
        if (input.expectedRevision !== current.revision) throw new Error('BUSINESS_REVISION_CONFLICT');
        if (!current.canPrepare) throw new Error('BUSINESS_PREPARATION_NOT_ALLOWED');
        assert.equal(input.deliveryIdentity, targetA.merchant.deliveryIdentity);
        const command = { onboardingId: target.onboardingId, sessionId: access.sessionId, invocationId: access.invocationId };
        const identity = { onboardingId: target.onboardingId, operationId };
        const state = { ...identity, deliveryIdentity: input.deliveryIdentity, updatedAt: at, status: 'awaiting-command', errorCode: null };
        const receipt = transaction(() => {
          const result = persist(reference.Preparation, command, identity, state, access);
          writeFacts('preparation', target, { ...current, canPrepare: false, preparation: state, currentPreparation: command });
          return result;
        });
        changes.next(address('preparation', target)); changes.next(reference.Preparation.kind);
        return receipt;
      },
      runCommand(target, input, access) {
        authorize(access); assert.equal(target.onboardingId, targetA.onboardingId);
        const current = readFacts('preparation', target).value;
        assert.equal(current.canRunCommand, true); assert.equal(input.operationId, operationId);
        assert.equal(input.expectedUpdatedAt, at); assert.deepEqual(input.command, { kind: 'inspect' });
        const command = { onboardingId: target.onboardingId, operationId, sessionId: access.sessionId, invocationId: access.invocationId };
        const identity = { onboardingId: target.onboardingId, operationId, deliveryCommandId };
        const state = { ...identity, settled: false, output: null, reasonCode: null };
        const receipt = transaction(() => {
          const result = persist(reference.DeliveryCommand, command, identity, state, access);
          writeFacts('preparation', target, { ...current, command: state, currentDeliveryCommand: command });
          return result;
        });
        changes.next(address('preparation', target)); changes.next(reference.DeliveryCommand.kind);
        return receipt;
      },
      observeCommand: forbidden,
    },
    knowledge: { facts: target => facts('knowledge', target), initialize: forbidden, retry: forbidden, commit: forbidden },
    reception: { facts: target => facts('reception', target), enable: forbidden, resume: forbidden },
    merchantCommands: { facts: target => facts('commands', target), execute: forbidden, observe: forbidden, reconcile: forbidden },
  };
  return {
    db, services, ownerServices,
    assertConnections(count) {
      for (const scope of [targetA, targetB]) for (const domain of domains)
        assert.equal(connections.get(address(domain, domain === 'commands' ? scope.merchant : scope)), count,
          `${domain}/${scope.onboardingId} 的 Context 与 Surface 共享一个会话查询`);
    },
    invalidateA() {
      for (const domain of domains) {
        const target = domain === 'commands' ? targetA.merchant : targetA;
        pending.add(address(domain, target)); changes.next(address(domain, target));
      }
    },
    publishA() {
      transaction(() => {
        for (const domain of domains) {
          const target = domain === 'commands' ? targetA.merchant : targetA;
          const current = readFacts(domain, target).value;
          for (const property of Object.keys(current)) if (property.startsWith('can')) current[property] = false;
          if ('revision' in current) current.revision = 2;
          if (domain === 'knowledge') current.snapshotReasonCode = 'MANUAL_EVIDENCE_REQUIRED';
          writeFacts(domain, target, current);
        }
      });
      for (const domain of domains) {
        const target = domain === 'commands' ? targetA.merchant : targetA;
        pending.delete(address(domain, target)); changes.next(address(domain, target));
      }
    },
    allowPreparation() {
      const current = readFacts('preparation', targetA).value;
      writeFacts('preparation', targetA, { ...current, revision: 3, canPrepare: true, canRunCommand: true });
      changes.next(address('preparation', targetA));
    },
    finishDelivery() {
      const current = readFacts('preparation', targetA).value;
      const state = { ...current.command, settled: true, output: { inspected: true }, reasonCode: null };
      transaction(() => {
        assert.equal(Number(db.prepare('UPDATE operations SET generation = generation + 1, state_json = ? WHERE kind = ?')
          .run(JSON.stringify(reference.DeliveryCommand.state.parse(state)), reference.DeliveryCommand.kind).changes), 1);
        writeFacts('preparation', targetA, { ...current, command: state });
      });
      changes.next(reference.DeliveryCommand.kind); changes.next(address('preparation', targetA));
    },
    markPrepared() {
      const current = readFacts('preparation', targetA).value;
      const state = { ...current.preparation, updatedAt: preparedAt, status: 'prepared' };
      transaction(() => {
        assert.equal(Number(db.prepare('UPDATE operations SET generation = generation + 1, state_json = ? WHERE kind = ?')
          .run(JSON.stringify(reference.Preparation.state.parse(state)), reference.Preparation.kind).changes), 1);
        writeFacts('preparation', targetA, { ...current, preparation: state });
      });
      changes.next(reference.Preparation.kind); changes.next(address('preparation', targetA));
    },
    allowDependentDomainsA() {
      transaction(() => {
        const knowledge = readFacts('knowledge', targetA).value, reception = readFacts('reception', targetA).value;
        writeFacts('knowledge', targetA, { ...knowledge, revision: knowledge.revision + 1,
          canInitialize: true, canRetry: true, canCommit: true, sourceActivationId: 'activation-a',
          ready: false, snapshotReasonCode: 'KNOWLEDGE_DOCUMENTS_REQUIRED' });
        writeFacts('reception', targetA, { ...reception, revision: reception.revision + 1, canEnable: true });
      });
      changes.next(address('knowledge', targetA)); changes.next(address('reception', targetA));
    },
    recordKnowledgeA(ready, snapshotReasonCode) {
      const current = readFacts('knowledge', targetA).value;
      writeFacts('knowledge', targetA, { ...current, revision: current.revision + 1, ready, snapshotReasonCode });
      const id = address('knowledge', targetA);
      pending.delete(id); readFailures.delete(id); changes.next(id);
    },
    invalidateResourceA(domain) {
      const id = address(domain, targetA);
      pending.add(id); changes.next(id);
    },
    failResourceA(domain, error) {
      const id = address(domain, targetA);
      pending.delete(id); readFailures.set(id, error); changes.next(id);
    },
    recoverReceptionA() {
      const current = readFacts('reception', targetA).value;
      writeFacts('reception', targetA, { ...current, revision: current.revision + 1 });
      const id = address('reception', targetA);
      pending.delete(id); readFailures.delete(id); changes.next(id);
    },
    assertDomainConnections(domain, scope, count) {
      assert.equal(connections.get(address(domain, domain === 'commands' ? scope.merchant : scope)), count);
    },
    receptionTrial() {
      // 原业务 operation 可以被多次明确恢复；命令身份与稳定观察节点分别保存。
      db.exec(`CREATE TABLE reception_resumes (session_id TEXT NOT NULL, invocation_id TEXT NOT NULL, command_json TEXT NOT NULL,
          PRIMARY KEY(session_id, invocation_id));
        CREATE TABLE reception_nodes (id INTEGER PRIMARY KEY, updated_at TEXT NOT NULL UNIQUE, payload_json TEXT NOT NULL);
        CREATE TABLE reception_follows (id TEXT PRIMARY KEY, command_json TEXT NOT NULL, link_json TEXT NOT NULL,
          baseline INTEGER NOT NULL, pending INTEGER NOT NULL DEFAULT 1);`);
      const identity = { onboardingId: targetA.onboardingId, operationId: '66666666-6666-4666-8666-666666666666' };
      const original = reference.Reception.state.parse({ ...identity, preparationOperationId: operationId,
        updatedAt: at, status: 'observation-required', errorCode: 'RECEPTION_READ_REQUIRED', result: null });
      const current = readFacts('reception', targetA).value;
      transaction(() => {
        db.prepare('INSERT INTO reception_nodes (updated_at, payload_json) VALUES (?, ?)').run(at, JSON.stringify(original));
        writeFacts('reception', targetA, { ...current, operation: original, canResume: true });
      });
      const accessReception = access => {
        access.signal.throwIfAborted();
        if (access.source.kind === 'participant') assert.ok(['module-operator', 'module-operation-publisher'].includes(access.source.actorId));
        else { assert.equal(access.source.kind, 'tool'); assert.ok(access.source.callId); assert.ok(access.source.rootCallId); }
        assert.ok(access.sessionId);
      };
      const resumedCommand = command => {
        assert.equal(command.onboardingId, targetA.onboardingId); assert.equal(command.operationId, identity.operationId);
        const row = db.prepare('SELECT command_json FROM reception_resumes WHERE session_id = ? AND invocation_id = ?')
          .get(command.sessionId, command.invocationId);
        if (row) assert.deepEqual(JSON.parse(row.command_json), command);
        return row;
      };
      const receptionOwner = {
        locate(command, access) {
          accessReception(access);
          return resumedCommand(command) ? { status: 'accepted', reference: identity, state: readFacts('reception', targetA).value.operation }
            : { status: 'unknown' };
        },
        observe(raw, access) {
          accessReception(access); assert.deepEqual(raw, identity);
          return facts('reception', targetA).pipe(map(value => value.status === 'ready'
            ? { ...value, value: value.value.operation } : value));
        },
        registerFollow(command, link, access) {
          accessReception(access);
          assert.equal(command.onboardingId, targetA.onboardingId); assert.equal(command.operationId, identity.operationId);
          const before = db.prepare('SELECT command_json, link_json FROM reception_follows WHERE id = ?').get(link.id);
          if (before) { assert.deepEqual(JSON.parse(before.command_json), command); assert.deepEqual(JSON.parse(before.link_json), link); return; }
          const baseline = db.prepare('SELECT id FROM reception_nodes WHERE updated_at = ?').get(command.expectedUpdatedAt);
          assert.ok(baseline, '恢复命令必须携原已核验阶段；不能猜当前节点或从 after:null 交回旧F1');
          transaction(() => db.prepare('INSERT INTO reception_follows (id, command_json, link_json, baseline) VALUES (?, ?, ?, ?)')
            .run(link.id, JSON.stringify(command), JSON.stringify(link), baseline.id));
        },
        readFeedback(command, link, access) {
          accessReception(access);
          const registered = db.prepare('SELECT * FROM reception_follows WHERE id = ?').get(link.id);
          assert.ok(registered); assert.deepEqual(JSON.parse(registered.command_json), command); assert.deepEqual(JSON.parse(registered.link_json), link);
          if (!resumedCommand(command)) return { status: 'unknown' };
          const node = db.prepare('SELECT id, payload_json FROM reception_nodes WHERE id > ? ORDER BY id LIMIT 1').get(registered.baseline);
          return node ? { status: 'ready', reference: identity, node: { id: 'reception-node:' + node.id, payload: JSON.parse(node.payload_json) } }
            : { status: 'pending', reference: identity };
        },
      };
      return {
        identity,
        services: { ...services, reception: { facts: services.reception.facts, enable: forbidden, resume(target, input, access) {
          accessReception(access); assert.equal(target.onboardingId, targetA.onboardingId);
          const previous = readFacts('reception', target).value;
          assert.equal(input.operationId, identity.operationId); assert.equal(input.expectedUpdatedAt, previous.operation.updatedAt);
          assert.equal(previous.operation.status, 'observation-required');
          const command = reference.Reception.command.parse({ onboardingId: target.onboardingId, operationId: input.operationId,
            expectedUpdatedAt: input.expectedUpdatedAt, sessionId: access.sessionId, invocationId: access.invocationId });
          transaction(() => {
            db.prepare('INSERT INTO reception_resumes VALUES (?, ?, ?)').run(command.sessionId, command.invocationId, JSON.stringify(command));
            db.prepare('INSERT INTO bodies (kind, session_id, invocation_id, source_json) VALUES (?, ?, ?, ?)')
              .run(reference.Reception.kind, access.sessionId, access.invocationId, JSON.stringify(access.source));
            writeFacts('reception', target, { ...previous, revision: previous.revision + 1, canResume: false, currentReception: command,
              operation: { ...previous.operation, updatedAt: preparedAt, status: 'queued', errorCode: null } });
          });
          changes.next(address('reception', target));
          return identity;
        } } },
        ownerServices: { ...ownerServices, reception: receptionOwner },
        nextObservation() {
          const previous = readFacts('reception', targetA).value;
          const state = { ...previous.operation, updatedAt: '2026-10-27T01:00:02.000Z', status: 'observation-required', errorCode: 'RECEPTION_READ_REQUIRED' };
          transaction(() => {
            db.prepare('INSERT INTO reception_nodes (updated_at, payload_json) VALUES (?, ?)').run(state.updatedAt, JSON.stringify(state));
            writeFacts('reception', targetA, { ...previous, revision: previous.revision + 1, canResume: true, operation: state });
          });
          changes.next(address('reception', targetA));
        },
      };
    },
    close() { changes.complete(); db.close(); },
  };
}

function nativeFrame(options, manifest) {
  const current = options.messages.findLast(message => message.source?.kind === 'runtime-context' && message.source.form === 'snapshot');
  assert.ok(current, '模型必须取得真实装配的 Context');
  const sections = {};
  for (const section of current.source.sections) {
    // 原生内建正文仍是文本；领域Context以实际业务身份和字段识别，不反解生成namespace。
    if (!section.text.trimStart().startsWith('{')) continue;
    const value = JSON.parse(section.text);
    const target = value.target ?? value;
    const side = target.onboardingId === targetA.onboardingId || target.subject?.id === targetA.merchant.subject.id ? 'a'
      : target.onboardingId === targetB.onboardingId || target.subject?.id === targetB.merchant.subject.id ? 'b' : undefined;
    if (!side) continue;
    const domain = Object.hasOwn(value, 'canPrepare') ? 'preparation' : Object.hasOwn(value, 'canInitialize') ? 'knowledge'
      : Object.hasOwn(value, 'canEnable') ? 'reception' : Object.hasOwn(value, 'canExecute') ? 'commands'
      : Object.hasOwn(value, 'blockers') ? 'collaboration' : Object.hasOwn(value, 'canInitializeKnowledge') ? 'knowledge_prerequisites' : undefined;
    if (!domain) continue;
    const key = domain + '_' + side;
    if (Object.hasOwn(sections, key)) assert.deepEqual(sections[key], value,
      '多个模块消费同一协作事实时，其业务内容必须一致: ' + key);
    sections[key] = value;
  }
  return { sections, manifest, tools: options.tools.map(tool => tool.name) };
}

function hasAction(frame, action, target) {
  const matches = frame.manifest.filter(binding => binding.action === action.name
    && (binding.scope.onboardingId === target.onboardingId || (binding.scope.subject?.id === target.merchant.subject.id
      && binding.scope.sandboxId === target.merchant.sandboxId && binding.scope.deliveryIdentity === target.merchant.deliveryIdentity)));
  assert.ok(matches.length <= 1, '同一个业务目标的Action不能有多个未区分绑定');
  return matches.length === 1 && frame.tools.includes(matches[0].toolName);
}

async function* reply() {
  yield { type: 'block-start', index: 0, blockType: 'text' };
  yield { type: 'block-end', index: 0, block: { type: 'text', text: '已读取当前领域事实' } };
  yield { type: 'finish', reason: { kind: 'stop' } };
}

test('发行包原模块组合在双目标双Program下派生跨域前提、撤销依赖查询并保持独立业务终态', { timeout: 30_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-merchant-modules-'));
  const reference = await installedReference(root);
  const store = business(join(root, 'business.sqlite'), reference);
  const owners = reference.createMerchantOwners(store.ownerServices);
  const commitEntered = Promise.withResolvers(), commitResult = Promise.withResolvers();
  const commitFailure = new Error('KNOWLEDGE_COMMIT_BACKEND_UNAVAILABLE');
  void commitResult.promise.catch(() => {});
  const services = { ...store.services, knowledge: { ...store.services.knowledge,
    async commit(target, input, access) {
      access.signal.throwIfAborted();
      assert.equal(access.source.actorId, 'module-operator');
      assert.deepEqual(target, { onboardingId: targetA.onboardingId });
      const current = store.db.prepare("SELECT document FROM facts WHERE domain = 'knowledge' AND target = ?").get(target.onboardingId);
      assert.equal(input.expectedRevision, JSON.parse(current.document).revision);
      commitEntered.resolve();
      return commitResult.promise;
    },
  } };
  const bindings = reference.createMerchantBindings(services, owners, {
    requiredSkills: [], humanInput: { admitResponder() { throw new Error('REFERENCE_TRIAL_HUMAN_INPUT_FORBIDDEN'); } },
  });
  const programs = reference.createMerchantPrograms(bindings, () => of([targetA, targetB]));
  const definition = defineApplication({ name: 'merchant-module-trial',
    admit(input, _identity, access) {
      assert.equal(access.actorId, 'module-operator');
      assert.deepEqual(input.scope, targetA);
    },
    environments: [defineEnvironment({ id: 'operations', instructions: 'OPERATIONS_REFERENCE', program: programs.operations }),
      defineEnvironment({ id: 'review', instructions: 'REVIEW_REFERENCE', program: programs.review })],
  });
  const errors = [], requests = [];
  const stopRead = new AbortController();
  let runtime, operations, review;
  const subscriptions = [];
  try {
    runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'module-operator', serviceActorId: 'module-operation-publisher',
      operations: reference.merchantOperationOwners(bindings), tools: 'native',
      onError: event => errors.push(event.error), model: nativeModel({ provider: 'merchant-module-trial', model: 'test',
        create: () => new class extends LlmAdapter {
          async *stream(options) {
            const current = [operations, review].find(session => session?.id === options.sessionId);
            assert.ok(current, '模型调用必须属于当前真实Program会话');
            requests.push(nativeFrame(options, await first(current.actionManifest$)));
            yield* reply();
          }
        }() }),
    });
    await mkdir(join(root, 'workspace'));
    const workspace = await runtime.application.createWorkspace({ directory: join(root, 'workspace') });
    operations = await runtime.application.create('operations', { workspaceId: workspace.workspaceId });
    review = await runtime.application.create('review', { workspaceId: workspace.workspaceId });
    for (const session of [operations, review]) {
      await first(session.collection('merchants').instances$, keys => keys.includes(targetA.onboardingId) && keys.includes(targetB.onboardingId));
      const instance = merchant(session, targetA);
      for (const slot of views) subscriptions.push(instance.observeView(slot).subscribe());
      for (const slot of views) await instance.readView(slot);
      subscriptions.push(instance.observeView('collaboration.activity').subscribe());
      const [activityA, activityB] = await Promise.all([instance.readView('collaboration.activity'),
        merchant(session, targetB).readView('collaboration.activity')]);
      assert.equal(activityA.onboardingId, targetA.onboardingId);
      assert.equal(activityB.onboardingId, targetB.onboardingId);
      assert.equal(activityA.conversation.sessionId, session.id);
      assert.deepEqual(activityA.conversation, activityB.conversation,
        '目标活动保留各自归属；Agent/Goal 明确是两目标共享的会话事实');
    }
    assert.equal((await merchant(operations, targetB).readView('preparation.facts')).revision, 7);
    assert.equal((await merchant(review, targetB).readView('preparation.facts')).onboardingId, targetB.onboardingId);
    store.assertConnections(2);
    const captured = merchant(operations, targetA).resolveAction('preparation.prepare', { onboardingId: targetA.onboardingId });
    for (const session of [operations, review]) {
      await session.submit({ requestId: 'initial-' + session.id, content: '读取当前范围', delivery: 'followup', intent: 'question', scope: targetA });
      await session.whenIdle();
    }
    assert.equal(requests.length, 2);
    for (const request of requests) {
      assert.equal(request.sections.preparation_a.revision, 1);
      assert.equal(request.sections.knowledge_b.revision, 7);
      assert.ok(hasAction(request, reference.PrepareMerchant, targetA));
      assert.ok(hasAction(request, reference.ExecuteMerchantCommand, targetB));
      assert.equal(request.sections.knowledge_a.canInitialize, true);
      assert.equal(request.sections.reception_a.canEnable, true);
      assert.equal(request.sections.collaboration_a.preparationReady, false);
      assert.ok(!hasAction(request, reference.InitializeKnowledge, targetA), '后端能力存在也不能越过 prepared 前提');
      assert.ok(!hasAction(request, reference.EnableReception, targetA));
    }

    await assert.rejects(async () => merchant(review, targetA).resolveAction('preparation.prepare', { onboardingId: targetA.onboardingId }),
      'Review 的 selectSurface 不公开准备写入槽');
    store.invalidateA();
    for (const session of [operations, review]) {
      const instance = merchant(session, targetA);
      for (const slot of views) await first(instance.observeView(slot), observed => observed.status === 'pending');
      await session.submit({ requestId: 'invalidated-' + session.id, content: '按新事实继续读取', delivery: 'followup', intent: 'question', scope: targetA });
      await session.whenIdle();
    }
    store.assertDomainConnections('knowledge', targetA, 0);
    store.assertDomainConnections('reception', targetA, 0);
    store.assertDomainConnections('knowledge', targetB, 2);
    store.assertDomainConnections('reception', targetB, 2);
    let readReturned = false;
    const currentRead = merchant(operations, targetA).readView('preparation.facts').then(value => { readReturned = true; return value; });
    void currentRead.catch(() => {});
    await immediate();
    assert.equal(readReturned, false, 'pending 不得读取上一份 ready');
    assert.equal(requests.length, 4, '显式输入仍可调用模型处理就绪的 B，不等待整个 A');
    for (const request of requests.slice(2, 4)) {
      for (const name of ['preparation_a', 'knowledge_a', 'reception_a', 'commands_a', 'collaboration_a', 'knowledge_prerequisites_a'])
        assert.equal(Object.hasOwn(request.sections, name), false, '失效后的旧 Context 必须撤下: ' + name);
      for (const action of [reference.PrepareMerchant, reference.RunPreparationCommand, reference.InitializeKnowledge,
        reference.CommitKnowledge, reference.EnableReception, reference.ExecuteMerchantCommand])
        assert.ok(!hasAction(request, action, targetA), '失效依赖不能继续提供该能力: ' + action.name);
      assert.equal(request.sections.preparation_b.revision, 7);
      assert.equal(request.sections.knowledge_b.revision, 7);
      assert.ok(hasAction(request, reference.PrepareMerchant, targetB)); assert.ok(hasAction(request, reference.ExecuteMerchantCommand, targetB));
    }
    assert.equal(store.db.prepare('SELECT COUNT(*) AS count FROM bodies').get().count, 0);
    store.publishA();
    assert.equal((await currentRead).revision, 2);
    assert.equal(requests.length, 4, '来源恢复不自行调用模型');
    for (const session of [operations, review]) {
      await session.submit({ requestId: 'published-' + session.id, content: '读取当前已更新事实', delivery: 'followup', intent: 'question', scope: targetA });
      await session.whenIdle();
    }
    assert.equal(requests.length, 6);
    for (const request of requests.slice(4, 6)) {
      assert.equal(request.sections.preparation_a.revision, 2);
      assert.equal(request.sections.knowledge_a.snapshotReasonCode, 'MANUAL_EVIDENCE_REQUIRED');
      assert.equal(request.sections.reception_a.canEnable, false);
      assert.equal(request.sections.commands_a.target.sandboxId, 'sandbox-a');
      assert.equal(request.sections.commands_a.canExecute, false);
      assert.equal(request.sections.preparation_b.revision, 7);
      assert.ok(!hasAction(request, reference.PrepareMerchant, targetA));
      assert.ok(!hasAction(request, reference.InitializeKnowledge, targetA));
      assert.ok(!hasAction(request, reference.EnableReception, targetA));
      assert.ok(!hasAction(request, reference.ExecuteMerchantCommand, targetA));
      assert.ok(hasAction(request, reference.PrepareMerchant, targetB));
    }
    assert.equal((await merchant(review, targetA).readView('knowledge.facts')).snapshotReasonCode, 'MANUAL_EVIDENCE_REQUIRED');
    assert.equal((await merchant(operations, targetB).readView('commands.facts')).target.sandboxId, 'sandbox-b');
    assert.equal(merchant(operations, targetA).resolveAction('preparation.prepare', { onboardingId: targetA.onboardingId }).bindingId, captured.bindingId);
    await assert.rejects(captured({ expectedRevision: 1, deliveryIdentity: targetA.merchant.deliveryIdentity }).result, /BUSINESS_REVISION_CONFLICT/u);
    assert.equal(store.db.prepare('SELECT COUNT(*) AS count FROM bodies').get().count, 0);
    assert.deepEqual(await operations.locateOperation(reference.Preparation,
      { onboardingId: targetA.onboardingId, sessionId: operations.id, invocationId: 'unaccepted' }), { status: 'unknown' });
    const unknown = await operations.attachOperation(reference.Preparation, { onboardingId: targetA.onboardingId, operationId: unknownId });
    await assert.rejects(unknown.settled(), /BUSINESS_OPERATION_UNKNOWN/u);

    store.allowPreparation();
    await first(merchant(operations, targetA).observeView('preparation.facts'), observed => observed.status === 'ready' && observed.value.revision === 3);
    const preparationCall = captured({ expectedRevision: 3, deliveryIdentity: targetA.merchant.deliveryIdentity });
    const identity = { onboardingId: targetA.onboardingId, operationId };
    assert.deepEqual(await preparationCall.result, identity);
    const preparation = await preparationCall.operation;
    assert.equal((await first(preparation.observations$, observed => observed.status === 'ready')).value.status, 'awaiting-command');
    const preparing = await merchant(operations, targetA).readView('collaboration.activity');
    assert.equal(preparing.invocations.preparation, 0, '函数返回之后短调用已结束');
    assert.equal(preparing.operations.preparation, 'awaiting-command', '长期操作继续由业务 owner 的事实表达');
    let preparationSettled = false;
    const waiting = preparation.settled({ signal: stopRead.signal }).then(value => { preparationSettled = true; return value; });
    const waitingOutcome = waiting.then(value => ({ value }), error => ({ error }));
    const run = merchant(operations, targetA).resolveAction('preparation.runCommand', { onboardingId: targetA.onboardingId });
    const commandCall = run({ operationId, expectedUpdatedAt: at, command: { kind: 'inspect' } });
    assert.deepEqual(await commandCall.result, { ...identity, deliveryCommandId });
    const command = await commandCall.operation;
    const commandSettled = command.settled();
    void commandSettled.catch(() => {});
    store.finishDelivery();
    const completed = await commandSettled;
    assert.equal(completed.deliveryCommandId, deliveryCommandId);
    assert.deepEqual(completed.output, { inspected: true });
    assert.equal(preparationSettled, false, '单条交付命令结束不得完成准备聚合');
    const rows = store.db.prepare('SELECT kind, session_id, invocation_id, command_json FROM operations ORDER BY kind').all();
    assert.equal(rows.length, 2);
    assert.ok(rows.every(row => row.session_id === operations.id));
    assert.deepEqual(new Set(rows.map(row => row.invocation_id)), new Set([preparationCall.id, commandCall.id]));
    assert.equal(JSON.parse(rows.find(row => row.kind === reference.Preparation.kind).command_json).onboardingId, targetA.onboardingId);
    stopRead.abort(new Error('QUERY_FINISHED'));
    assert.match((await waitingOutcome).error?.message ?? '', /QUERY_FINISHED/u);
    store.assertConnections(2);

    subscriptions.splice(0).forEach(subscription => subscription.unsubscribe());
    const reader = runtime.application.conversation(operations.id, reference.OperationsProgram);
    reader.release();
    store.assertConnections(2, '释放 SDK 观察者不关闭 Program 查询');
    await runtime.application.close(operations.id); operations.release(); operations = undefined;
    store.assertConnections(1);
    assert.equal((await merchant(review, targetA).readView('preparation.facts')).preparation.status, 'awaiting-command');
    const continued = await review.attachOperation(reference.Preparation, identity);
    const final = continued.settled();
    void final.catch(() => {});
    store.markPrepared();
    assert.equal((await final).status, 'prepared');
    assert.equal((await merchant(review, targetA).readView('preparation.facts')).preparation.updatedAt, preparedAt);
    assert.equal(store.db.prepare('SELECT COUNT(*) AS count FROM bodies').get().count, 2, '关闭查询不会重做或取消已受理业务');
    assert.equal((await merchant(review, targetB).readView('preparation.facts')).revision, 7);
    const checkPrerequisites = async (requestId, expected) => {
      await review.submit({ requestId, content: '核对当前业务前提，不执行正式动作', delivery: 'followup', intent: 'question', scope: targetA });
      await review.whenIdle();
      const frame = requests.at(-1), collaboration = frame.sections.collaboration_a;
      assert.equal(collaboration.preparationReady, true);
      assert.equal(collaboration.canInitializeKnowledge, expected.initialize);
      assert.equal(collaboration.canCommitKnowledge, expected.commit);
      assert.equal(collaboration.canEnableReception, expected.enable);
      assert.equal(hasAction(frame, reference.InitializeKnowledge, targetA), expected.initialize);
      assert.equal(hasAction(frame, reference.CommitKnowledge, targetA), expected.commit);
      assert.equal(hasAction(frame, reference.EnableReception, targetA), expected.enable);
      assert.equal(frame.sections.preparation_b.revision, 7);
      assert.equal(frame.sections.collaboration_b.preparationReady, false);
      assert.equal(frame.sections.collaboration_b.canEnableReception, false);
      assert.ok(!hasAction(frame, reference.InitializeKnowledge, targetB));
      assert.equal(store.db.prepare('SELECT COUNT(*) AS count FROM bodies').get().count, 2, '前提变化不自动 commit/enable 或重发命令');
    };
    store.allowDependentDomainsA();
    assert.equal((await merchant(review, targetA).readView('collaboration.facts')).canInitializeKnowledge, true);
    await checkPrerequisites('prepared-prerequisites', { initialize: true, commit: false, enable: false });
    store.recordKnowledgeA(true, 'KNOWLEDGE_SNAPSHOT_REQUIRES_VERIFICATION');
    assert.equal((await merchant(review, targetA).readView('collaboration.facts')).knowledgeReady, false);
    await checkPrerequisites('knowledge-reason-blocks', { initialize: true, commit: false, enable: false });
    store.recordKnowledgeA(true, null);
    assert.equal((await merchant(review, targetA).readView('collaboration.facts')).knowledgeReady, true);
    await checkPrerequisites('knowledge-current-ready', { initialize: true, commit: true, enable: true });
    assert.equal(store.db.prepare("SELECT json_extract(document, '$.ready') AS ready FROM facts WHERE domain = 'knowledge' AND target = ?")
      .get(targetA.onboardingId).ready, 1);
    const observer = runtime.application.conversation(review.id, reference.ReviewProgram);
    try {
      const beforeCommit = requests.length;
      const currentKnowledge = await merchant(review, targetA).readView('knowledge.facts');
      const commit = merchant(review, targetA).resolveAction('knowledge.commit', { onboardingId: targetA.onboardingId });
      const invocation = commit({ expectedRevision: currentKnowledge.revision });
      const rejected = assert.rejects(invocation.result, error => error === commitFailure);
      await commitEntered.promise;
      const active = await first(merchant(observer, targetA).observeView('collaboration.activity'),
        value => value.status === 'ready' && value.value.invocations.knowledge === 1);
      assert.equal(active.value.conversation.sessionId, review.id);
      const during = await first(observer.actionManifest$, manifest => manifest.some(binding =>
        binding.action === reference.EnableReception.name && binding.scope.onboardingId === targetA.onboardingId && !binding.enabled));
      assert.equal(during.find(binding => binding.bindingId === commit.bindingId)?.enabled, true,
        '知识调用在途只约束下游接待，不以自身 busy 撤销自己的 Offer');
      assert.equal((await merchant(observer, targetB).readView('collaboration.activity')).invocations.knowledge, 0);
      assert.equal((await merchant(observer, targetA).readView('collaboration.facts')).canEnableReception, true,
        '业务现场仍然允许；当前不能开通来自上游短调用在途这一独立事实');
      commitResult.reject(commitFailure); await rejected;
      await first(merchant(observer, targetA).observeView('collaboration.activity'),
        value => value.status === 'ready' && value.value.invocations.knowledge === 0);
      await first(observer.actionManifest$, manifest => manifest.some(binding => binding.action === reference.EnableReception.name
        && binding.scope.onboardingId === targetA.onboardingId && binding.enabled));
      assert.equal(requests.length, beforeCommit, '调用、观察与业务异常不暗中唤醒模型');
      assert.equal(store.db.prepare('SELECT COUNT(*) AS count FROM bodies').get().count, 2,
        '尚未固定知识的后端失败不制造受理记录或重做其他领域工作');
    } finally { observer.release(); }
    const beforeInvalidation = requests.length;
    const checkUnavailable = async (domain, phase) => {
      const beforeRequest = requests.length;
      await review.submit({ requestId: domain + '-' + phase, content: '只核对仍就绪的业务范围', delivery: 'followup', intent: 'question', scope: targetA });
      await review.whenIdle();
      assert.equal(requests.length, beforeRequest + 1);
      const frame = requests.at(-1);
      assert.equal(frame.sections.preparation_a.preparation.status, 'prepared');
      assert.ok(hasAction(frame, reference.RunPreparationCommand, targetA), '下游不可读不应撤销可用准备命令');
      assert.equal(Object.hasOwn(frame.sections, 'reception_a'), false);
      assert.equal(Object.hasOwn(frame.sections, 'collaboration_a'), false);
      assert.ok(!hasAction(frame, reference.EnableReception, targetA));
      if (domain === 'knowledge') {
        assert.equal(Object.hasOwn(frame.sections, 'knowledge_a'), false);
        assert.equal(Object.hasOwn(frame.sections, 'knowledge_prerequisites_a'), false);
        assert.ok(!hasAction(frame, reference.InitializeKnowledge, targetA)); assert.ok(!hasAction(frame, reference.CommitKnowledge, targetA));
      } else {
        assert.equal(frame.sections.knowledge_a.ready, true);
        assert.equal(frame.sections.knowledge_a.snapshotReasonCode, null);
        assert.equal(frame.sections.knowledge_prerequisites_a.canInitializeKnowledge, true);
        assert.equal(frame.sections.knowledge_prerequisites_a.canCommitKnowledge, true);
        assert.ok(hasAction(frame, reference.InitializeKnowledge, targetA)); assert.ok(hasAction(frame, reference.CommitKnowledge, targetA));
      }
      assert.equal(frame.sections.preparation_b.revision, 7);
      assert.equal(frame.sections.knowledge_b.revision, 7);
      assert.ok(hasAction(frame, reference.PrepareMerchant, targetB)); assert.ok(hasAction(frame, reference.ExecuteMerchantCommand, targetB));
      assert.equal(store.db.prepare('SELECT COUNT(*) AS count FROM bodies').get().count, 2);
    };
    store.invalidateResourceA('knowledge');
    await first(merchant(review, targetA).observeView('collaboration.facts'), observed => observed.status === 'pending');
    store.assertDomainConnections('knowledge', targetA, 1);
    store.assertDomainConnections('reception', targetA, 0);
    const readAbort = new AbortController();
    const blockedRead = assert.rejects(merchant(review, targetA).readView('collaboration.facts', { signal: readAbort.signal }), /READ_NOT_READY/u);
    await immediate(); readAbort.abort(new Error('READ_NOT_READY')); await blockedRead;
    assert.equal(requests.length, beforeInvalidation, '资源失效不自行调用模型');
    await checkUnavailable('knowledge', 'pending');
    const unavailable = new Error('KNOWLEDGE_READ_UNAVAILABLE');
    store.failResourceA('knowledge', unavailable);
    await first(merchant(review, targetA).observeView('collaboration.facts'), observed => observed.status === 'error');
    await assert.rejects(merchant(review, targetA).readView('collaboration.facts'), /KNOWLEDGE_READ_UNAVAILABLE/u);
    await assert.rejects(merchant(review, targetA).readView('knowledge.facts'), error => error === unavailable);
    const failedActivity = await first(merchant(review, targetA).observeView('collaboration.activity'),
      value => value.status === 'ready' && value.value.sources.knowledge === 'error');
    assert.equal(failedActivity.value.operations.knowledge, null, '局部错误不借上一份 ready 伪造长期阶段');
    const healthyActivity = await merchant(review, targetB).readView('collaboration.activity');
    assert.equal(healthyActivity.sources.knowledge, 'ready');
    assert.equal(healthyActivity.conversation.sessionId, failedActivity.value.conversation.sessionId);
    assert.equal((await merchant(review, targetB).readView('collaboration.facts')).canEnableReception, false);
    assert.equal((await merchant(review, targetB).readView('knowledge.facts')).revision, 7);
    assert.equal(requests.length, beforeInvalidation + 1, '读取错误不自行调用模型');
    await checkUnavailable('knowledge', 'error');
    store.recordKnowledgeA(true, null);
    assert.equal((await merchant(review, targetA).readView('collaboration.facts')).canEnableReception, true);
    store.assertConnections(1);
    await checkPrerequisites('knowledge-recovered', { initialize: true, commit: true, enable: true });
    const beforeReception = requests.length;
    store.invalidateResourceA('reception');
    await first(merchant(review, targetA).observeView('reception.facts'), observed => observed.status === 'pending');
    await first(merchant(review, targetA).observeView('collaboration.facts'), observed => observed.status === 'pending');
    const receptionAbort = new AbortController();
    const receptionRead = assert.rejects(merchant(review, targetA).readView('reception.facts', { signal: receptionAbort.signal }), /RECEPTION_NOT_READY/u);
    await immediate(); receptionAbort.abort(new Error('RECEPTION_NOT_READY')); await receptionRead;
    assert.equal(requests.length, beforeReception);
    await checkUnavailable('reception', 'pending');
    store.failResourceA('reception', new Error('RECEPTION_READ_UNAVAILABLE'));
    await first(merchant(review, targetA).observeView('reception.facts'), observed => observed.status === 'error');
    await assert.rejects(merchant(review, targetA).readView('reception.facts'), /RECEPTION_READ_UNAVAILABLE/u);
    await assert.rejects(merchant(review, targetA).readView('collaboration.facts'), /RECEPTION_READ_UNAVAILABLE/u);
    assert.equal(requests.length, beforeReception + 1, '接待读取失败不反向触发知识或准备动作');
    await checkUnavailable('reception', 'error');
    store.recoverReceptionA();
    assert.equal((await merchant(review, targetA).readView('collaboration.facts')).canEnableReception, true);
    store.assertConnections(1);
    await checkPrerequisites('reception-recovered', { initialize: true, commit: true, enable: true });
    await runtime.application.close(review.id); review.release(); review = undefined;
    store.assertConnections(0);
    assert.equal(requests.length, 15, '只有十五次明确输入调用模型');
    assert.deepEqual(errors, []);
  } finally {
    commitResult.reject(commitFailure);
    stopRead.abort(new Error('QUERY_FINISHED'));
    subscriptions.forEach(subscription => subscription.unsubscribe());
    operations?.release(); review?.release();
    try { await runtime?.dispose(); } finally { store.close(); }
  }
});

test('原接待操作恢复保留已核验修订，旧F1不能解除新参与，F2才交回原会话', { timeout: 30_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-reception-baseline-'));
  const reference = await installedReference(root);
  const store = business(join(root, 'business.sqlite'), reference);
  const trial = store.receptionTrial();
  const owners = reference.createMerchantOwners(trial.ownerServices);
  const bindings = reference.createMerchantBindings(trial.services, owners, {
    requiredSkills: [], humanInput: { admitResponder() { throw new Error('REFERENCE_TRIAL_HUMAN_INPUT_FORBIDDEN'); } },
  });
  const programs = reference.createMerchantPrograms(bindings, () => of([targetA, targetB]));
  const definition = defineApplication({ name: 'merchant-reception-baseline',
    admit(input, _identity, access) {
      assert.ok(['module-operator', 'module-operation-publisher'].includes(access.actorId));
      assert.deepEqual(input.scope, targetA);
    },
    environments: [defineEnvironment({ id: 'operations', instructions: '只恢复原接待操作，不重新开通', program: programs.operations })],
  });
  const requests = [], errors = [];
  const feedbackSeen = Promise.withResolvers(), finishFeedback = Promise.withResolvers();
  let runtime, session;
  try {
    runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'module-operator',
      serviceActorId: 'module-operation-publisher', tools: 'native', operations: reference.merchantOperationOwners(bindings),
      onError: event => errors.push(event.error), model: nativeModel({ provider: 'reception-baseline', model: 'test',
        create: () => new class extends LlmAdapter {
          async *stream(options) {
            requests.push(options);
            if (requests.length === 1) {
              const manifest = await first(session.actionManifest$, values => values.some(value => value.action === reference.ResumeReception.name
                && value.scope.onboardingId === targetA.onboardingId && value.enabled));
              const candidates = manifest.filter(value => value.action === reference.ResumeReception.name
                && value.scope.onboardingId === targetA.onboardingId && value.enabled);
              assert.equal(candidates.length, 1);
              const name = candidates[0].toolName;
              assert.ok(options.tools.some(tool => tool.name === name));
              const args = JSON.stringify({ operationId: trial.identity.operationId, expectedUpdatedAt: at });
              const id = ToolCallId('resume-original-reception');
              yield { type: 'block-start', index: 0, blockType: 'tool-call' };
              yield { type: 'tool-call-delta', index: 0, id, name, argumentsDelta: args };
              yield { type: 'block-end', index: 0, block: { type: 'tool-call', id, name, arguments: args } };
              yield { type: 'finish', reason: { kind: 'tool-calls' } };
            } else {
              assert.equal(requests.length, 2, '试验只消费一次F2，不以无限reply驱动原Goal轮次');
              const message = options.messages.findLast(message => message.role === 'user' && message.source?.kind === 'user'
                && message.source.rpcId?.startsWith('operation:'));
              assert.ok(message);
              const content = message.content.map(block => block.type === 'text' ? block.text : '').join('');
              const result = JSON.parse(content.slice(content.indexOf('\n') + 1));
              assert.equal(result.outcome.status, 'feedback');
              assert.equal(result.outcome.node.id, 'reception-node:2');
              assert.equal(result.outcome.node.payload.updatedAt, '2026-10-27T01:00:02.000Z');
              assert.equal(result.outcome.node.payload.status, 'observation-required');
              feedbackSeen.resolve();
              await finishFeedback.promise;
              yield* reply();
            }
          }
        }() }),
    });
    await mkdir(join(root, 'workspace'));
    const workspace = await runtime.application.createWorkspace({ directory: join(root, 'workspace') });
    session = await runtime.application.create('operations', { workspaceId: workspace.workspaceId });
    await first(session.collection('merchants').instances$, keys => keys.includes(targetA.onboardingId) && keys.includes(targetB.onboardingId));
    await merchant(session, targetA).readView('reception.facts');
    await session.submit({ requestId: 'resume-reception', content: '沿已核验的原接待操作恢复，再交回新的核验阶段',
      delivery: 'followup', intent: 'goal', scope: targetA });
    await session.whenIdle();
    const registrations = store.db.prepare('SELECT * FROM reception_follows').all();
    assert.equal(registrations.length, 1);
    const registration = registrations[0], link = JSON.parse(registration.link_json), command = JSON.parse(registration.command_json);
    assert.equal(command.expectedUpdatedAt, at, '纯reference Action mapping必须保留实际核验的F1修订');
    assert.equal(command.operationId, trial.identity.operationId);
    assert.equal(registration.baseline, 1);
    assert.equal(store.db.prepare('SELECT count(*) AS count FROM bodies WHERE kind = ?').get(reference.Reception.kind).count, 1);
    assert.equal(store.db.prepare('SELECT count(*) AS count FROM reception_nodes').get().count, 1);
    assert.deepEqual(await session.readOperationFeedback(reference.Reception, command, link.id), { status: 'pending', reference: trial.identity });
    assert.deepEqual(await runtime.application.notifyOperation(session.id, link.id), { status: 'pending' });
    await session.whenIdle();
    assert.equal(requests.length, 1, '原F1仍存在，但恢复后的新阶段尚未出现，不能续模型');
    assert.equal(session.snapshot().waits.length, 1);
    trial.nextObservation();
    assert.equal(requests.length, 1, '领域事实变化不自行投递或重执业务');
    const delivered = await runtime.application.notifyOperation(session.id, link.id);
    assert.equal(delivered.status, 'accepted');
    store.db.prepare('UPDATE reception_follows SET pending = 0 WHERE id = ?').run(link.id);
    await feedbackSeen.promise;
    // F2已经实际进入原会话模型；试验在此明确停止，不把自动Goal后续轮次误当重复反馈。
    const cancelling = session.cancel(); finishFeedback.resolve(); await cancelling;
    await session.whenIdle();
    assert.equal(requests.length, 2);
    assert.equal(session.snapshot().waits.length, 0);
    assert.equal(store.db.prepare('SELECT count(*) AS count FROM bodies WHERE kind = ?').get(reference.Reception.kind).count, 1);
    assert.equal(session.snapshot().goal.phase, 'active', '需要核验的F2不是接待enabled或原Goal完成');
    const native = runtime.native.agents.get(session.id).session.snapshotEvents();
    assert.equal(native.filter(event => event.type === 'user/message' && event.data.source.kind === 'user'
      && event.data.source.rpcId === 'operation:' + link.id).length, 1, '原F2只消费一次，自动Goal输入不属于重复投递');
    assert.deepEqual(errors, []);
  } finally {
    finishFeedback.resolve();
    session?.release();
    try { await runtime?.dispose(); } finally { store.close(); }
  }
});
