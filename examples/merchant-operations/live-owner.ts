import { DatabaseSync } from 'node:sqlite';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createHash, randomUUID } from 'node:crypto';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { Observable, Subject } from 'rxjs';
import { z, operationFollowLinkSchema, type ActionContract, type ActionParsedInput, type ActionScope, type ActionCommandOrigin, type InputReference, type OperationContract, type OperationAccessContext } from '@hanger-source/dsh-reactive';
import { Preparation, DeliveryCommand, KnowledgeTask, Reception, MerchantCommand, PrepareMerchant, RunPreparationCommand, InitializeKnowledge, EnableReception, ExecuteMerchantCommand, preparationFactsSchema, knowledgeFactsSchema, receptionFactsSchema, merchantCommandFactsSchema, type PreparationFacts, type KnowledgeFacts, type ReceptionFacts, type MerchantCommandFacts, type MerchantTarget } from './contracts/index.ts';
import { merchantProgramInputSchema, type MerchantProgramScope } from './program-contracts.ts';
import type { BusinessAccess, BusinessMethod, MerchantServices, MerchantOwnerServices, ReliableOwnerOptions } from './host.ts';

export interface LocalDocument { readonly name: string; readonly at: string; readonly content: string; }
export const fileDigest = (content: string) => 'sha256:' + createHash('sha256').update(content).digest('hex');
const json = (value: unknown) => JSON.stringify(value);
const object = (text: unknown) => JSON.parse(z.string().parse(text));
const now = () => new Date().toISOString();
const executeFile = promisify(execFile);
class LocalRejection extends Error {}
const sameMerchant = (left: MerchantTarget, right: MerchantTarget) => left.subject.kind === right.subject.kind
  && left.subject.id === right.subject.id && left.sandboxId === right.sandboxId && left.deliveryIdentity === right.deliveryIdentity;

/** 本地文件和SQLite是此隔离业务的权威；这里不连接运营平台、FAI或商家服务。 */
export async function createLocalMerchantOwner(directory: string, documents: readonly LocalDocument[]) {
  documents = z.object({ name: z.string(), at: z.iso.datetime(), content: z.string() }).strict().array().parse(documents);
  if (!documents.length) throw new Error('LOCAL_DOCUMENTS_REQUIRED');
  for (const item of documents) {
    if (!/^[a-z0-9][a-z0-9.-]*$/u.test(item.name) || !item.content.trim() || !z.iso.datetime().safeParse(item.at).success)
      throw new Error('LOCAL_DOCUMENT_INVALID');
  }
  if (new Set(documents.map(item => item.name)).size !== documents.length) throw new Error('LOCAL_DOCUMENT_DUPLICATE');
  const target: MerchantProgramScope = { onboardingId: randomUUID(), merchant: { subject: { kind: 'merchant', id: 'local-documents' },
    sandboxId: 'local-files', deliveryIdentity: fileDigest(json(documents)) } };
  const observer: MerchantProgramScope = { onboardingId: randomUUID(), merchant: { subject: { kind: 'merchant', id: 'observer-only' },
    sandboxId: 'observer-only', deliveryIdentity: fileDigest('observer-only') } };
  const source = join(directory, 'source'); await mkdir(source);
  for (const item of documents) await writeFile(join(source, item.name), item.content, { flag: 'wx' });
  await writeFile(join(directory, 'verify-reception.mjs'), [
    "import { readFileSync } from 'node:fs'; import { createHash } from 'node:crypto';",
    "const receipt = JSON.parse(readFileSync('reception.json', 'utf8'));",
    "const index = readFileSync('index.json', 'utf8');",
    "const hash = 'sha256:' + createHash('sha256').update(index).digest('hex');",
    "if (receipt.mode !== 'local-file-only' || receipt.indexDigest !== hash) throw new Error('LOCAL_RECEPTION_INVALID');",
    "console.log(JSON.stringify({ indexDigest: hash, indexedDocuments: JSON.parse(index).documents.length }));",
  ].join('\n'), { flag: 'wx' });
  const db = new DatabaseSync(join(directory, 'business.sqlite'));
  try {
  db.exec(`PRAGMA journal_mode=WAL;
    CREATE TABLE facts(domain TEXT NOT NULL, target TEXT NOT NULL, revision INTEGER NOT NULL, value TEXT NOT NULL, PRIMARY KEY(domain,target));
    CREATE TABLE operations(kind TEXT NOT NULL, identity TEXT NOT NULL, state TEXT NOT NULL, PRIMARY KEY(kind,identity));
    CREATE TABLE commands(kind TEXT NOT NULL, command TEXT NOT NULL, identity TEXT NOT NULL, PRIMARY KEY(kind,command));
    CREATE TABLE rejected(kind TEXT NOT NULL, command TEXT NOT NULL, reason TEXT NOT NULL, PRIMARY KEY(kind,command));
    CREATE TABLE nodes(id INTEGER PRIMARY KEY, kind TEXT NOT NULL, identity TEXT NOT NULL, payload TEXT NOT NULL);
    CREATE TABLE follows(id TEXT PRIMARY KEY, kind TEXT NOT NULL, command TEXT NOT NULL, link TEXT NOT NULL, node INTEGER, pending INTEGER NOT NULL);
    CREATE TABLE effects(id INTEGER PRIMARY KEY, action TEXT NOT NULL, invocation TEXT NOT NULL UNIQUE, source TEXT NOT NULL);
    CREATE TABLE committed_snapshot(id TEXT PRIMARY KEY, digest TEXT NOT NULL);
    CREATE VIRTUAL TABLE knowledge_documents USING fts5(name UNINDEXED, content);`);
  const changes$ = new Subject<void>();
  const transaction = <T>(body: () => T): T => {
    db.exec('BEGIN IMMEDIATE');
    try { const value = body(); db.exec('COMMIT'); return value; } catch (error) { db.exec('ROLLBACK'); throw error; }
  };
  type Facts = { preparation: PreparationFacts; knowledge: KnowledgeFacts; reception: ReceptionFacts; commands: MerchantCommandFacts };
  const schemas: { readonly [D in keyof Facts]: z.ZodType<Facts[D]> } = { preparation: preparationFactsSchema, knowledge: knowledgeFactsSchema,
    reception: receptionFactsSchema, commands: merchantCommandFactsSchema };
  type Domain = keyof typeof schemas;
  for (const current of [target, observer]) {
    const allowed = current === target;
    const initial = {
      preparation: { onboardingId: current.onboardingId, revision: 1, canPrepare: allowed, canRunCommand: false, canObserveCommand: false,
        preparation: null, command: null, currentPreparation: null, currentDeliveryCommand: null },
      knowledge: { onboardingId: current.onboardingId, revision: 1, canInitialize: false, canRetry: false, canCommit: false,
        sourceActivationId: null, ready: false, snapshotReasonCode: 'LOCAL_INDEX_REQUIRED', tasks: [], currentTask: null },
      reception: { onboardingId: current.onboardingId, revision: 1, canEnable: false, canResume: false, operation: null, currentReception: null },
      commands: { target: current.merchant, canExecute: false, canObserve: false, canReconcile: false, commands: [], currentCommand: null },
    };
    for (const domain of Object.keys(schemas) as Domain[]) db.prepare('INSERT INTO facts VALUES(?,?,1,?)')
      .run(domain, current.onboardingId, json(schemas[domain].parse(initial[domain])));
  }
  function facts<D extends Domain>(domain: D, id = target.onboardingId): Facts[D] {
    const row = db.prepare('SELECT value FROM facts WHERE domain=? AND target=?').get(domain, id);
    if (!row) throw new Error('LOCAL_TARGET_UNKNOWN');
    return schemas[domain].parse(object(row.value));
  }
  function save<D extends Domain>(domain: D, value: Facts[D]) {
    db.prepare('UPDATE facts SET revision=revision+1,value=? WHERE domain=? AND target=?')
      .run(json(schemas[domain].parse(value)), domain, target.onboardingId);
  }
  function revision(domain: Domain) {
    return z.number().int().parse(db.prepare('SELECT revision FROM facts WHERE domain=? AND target=?').get(domain, target.onboardingId)?.revision);
  }
  function requireRevision(domain: Domain, expected: number, access: BusinessAccess) {
    access.signal.throwIfAborted();
    if (revision(domain) !== expected) throw new LocalRejection('LOCAL_FACT_REVISION_CONFLICT');
  }
  function observe<D extends Domain>(domain: D, id: string): Observable<{ status: 'ready'; generation: number; value: Facts[D]; observedAt: number }> {
    return new Observable(subscriber => {
      const emit = () => {
        try {
          const row = db.prepare('SELECT revision FROM facts WHERE domain=? AND target=?').get(domain, id);
          if (!row) throw new Error('LOCAL_TARGET_UNKNOWN');
          subscriber.next({ status: 'ready', generation: z.number().parse(row.revision), value: facts(domain, id), observedAt: Date.now() });
        } catch (error) { subscriber.error(error); }
      };
      const subscription = changes$.subscribe(emit); emit(); return () => subscription.unsubscribe();
    });
  }
  function authorize(access: OperationAccessContext | BusinessAccess) {
    access.signal.throwIfAborted();
    if (access.source.kind === 'participant' && !['local-operator', 'local-publisher'].includes(access.source.actorId)) throw new Error('LOCAL_ACTOR_DENIED');
  }
  function authorizeInputs(inputs: readonly InputReference[]) {
    if (!inputs.length || inputs.some(input => {
      const selection = merchantProgramInputSchema.safeParse(input.scope);
      const participantAllowed = input.participant === 'human' && input.actorId === 'local-operator'
        || input.participant === 'service' && input.actorId === 'local-publisher';
      return !participantAllowed || !selection.success
        || selection.data.onboardingId !== target.onboardingId || !sameMerchant(selection.data.merchant, target.merchant);
    })) throw new Error('LOCAL_INPUT_SCOPE_DENIED');
  }
  function writable(id: string, access: BusinessAccess) {
    authorize(access);
    if (access.source.kind === 'tool') authorizeInputs(access.inputs);
    if (id !== target.onboardingId) throw new LocalRejection('LOCAL_OBSERVER_READ_ONLY');
  }
  function command(origin: BusinessAccess) { return { onboardingId: target.onboardingId, sessionId: origin.sessionId, invocationId: origin.invocationId }; }
  function effect(action: string, access: BusinessAccess) {
    db.prepare('INSERT INTO effects(action,invocation,source) VALUES(?,?,?)').run(action, access.invocationId, json(access.source));
  }
  function accept(definition: OperationContract, rawCommand: unknown, identity: unknown, state: unknown) {
    const reference = json(definition.reference.parse(identity));
    db.prepare('INSERT INTO operations VALUES(?,?,?) ON CONFLICT(kind,identity) DO UPDATE SET state=excluded.state')
      .run(definition.kind, reference, json(definition.state.parse(state)));
    db.prepare('INSERT INTO commands VALUES(?,?,?)').run(definition.kind, json(definition.command.parse(rawCommand)), reference);
    db.prepare('INSERT INTO nodes(kind,identity,payload) VALUES(?,?,?)').run(definition.kind, reference, json(definition.state.parse(state)));
  }
  function owner<D extends OperationContract>(definition: D): ReliableOwnerOptions<D> {
    const locate = (rawCommand: unknown) => db.prepare('SELECT o.identity,o.state FROM commands c JOIN operations o ON o.kind=c.kind AND o.identity=c.identity WHERE c.kind=? AND c.command=?')
      .get(definition.kind, json(rawCommand));
    return {
      locate(raw, access) { authorize(access); const row = locate(raw);
        const denied = db.prepare('SELECT reason FROM rejected WHERE kind=? AND command=?').get(definition.kind, json(raw));
        return row ? { status: 'accepted', reference: object(row.identity), state: object(row.state) }
          : denied ? { status: 'rejected', reason: object(denied.reason) } : { status: 'unknown' }; },
      observe(reference, access) { authorize(access); return new Observable(subscriber => {
        const emit = () => {
          try {
            const row = db.prepare('SELECT state FROM operations WHERE kind=? AND identity=?').get(definition.kind, json(reference));
            if (!row) throw new Error('LOCAL_OPERATION_UNKNOWN');
            const generation = db.prepare('SELECT max(id) AS generation FROM nodes WHERE kind=? AND identity=?').get(definition.kind, json(reference));
            subscriber.next({ status: 'ready', generation: z.number().parse(generation?.generation), value: object(row.state), observedAt: Date.now() });
          } catch (error) { subscriber.error(error); }
        };
        const subscription = changes$.subscribe(emit); emit(); return () => subscription.unsubscribe();
      }); },
      registerFollow(raw, link, access) {
        authorize(access);
        authorizeInputs(link.continuation.inputs);
        const before = db.prepare('SELECT link FROM follows WHERE id=?').get(link.id);
        if (before && json(object(before.link)) !== json(link)) throw new Error('LOCAL_FOLLOW_CONFLICT');
        if (!before) {
          transaction(() => db.prepare('INSERT INTO follows VALUES(?,?,?,?,NULL,1)').run(link.id, definition.kind, json(raw), json(link)));
          changes$.next();
        }
      },
      readFeedback(raw, link, access) {
        authorize(access);
        authorizeInputs(link.continuation.inputs);
        const follow = db.prepare('SELECT * FROM follows WHERE id=?').get(link.id);
        if (!follow || json(object(follow.link)) !== json(link) || follow.command !== json(raw)) throw new Error('LOCAL_FOLLOW_UNKNOWN');
        const operation = locate(raw);
        if (!operation) {
          const denied = db.prepare('SELECT reason FROM rejected WHERE kind=? AND command=?').get(definition.kind, json(raw));
          return denied ? { status: 'rejected', reason: object(denied.reason) } : { status: 'unknown' };
        }
        let node = follow.node === null ? undefined : db.prepare('SELECT * FROM nodes WHERE id=?').get(follow.node);
        if (!node) {
          if (link.after !== null && !/^local-node:[1-9][0-9]*$/u.test(link.after)) throw new Error('LOCAL_NODE_UNKNOWN');
          const after = link.after === null ? 0 : Number(link.after.slice('local-node:'.length));
          if (after && !db.prepare('SELECT id FROM nodes WHERE id=? AND kind=? AND identity=?').get(after, definition.kind, operation.identity)) throw new Error('LOCAL_NODE_UNKNOWN');
          node = db.prepare('SELECT * FROM nodes WHERE kind=? AND identity=? AND id>? ORDER BY id LIMIT 1').get(definition.kind, operation.identity, after);
          if (node) db.prepare('UPDATE follows SET node=? WHERE id=?').run(node.id, link.id);
        }
        return node ? { status: 'ready', reference: object(operation.identity), node: { id: 'local-node:' + node.id, payload: object(node.payload) } }
          : { status: 'pending', reference: object(operation.identity) };
      },
    };
  }
  const forbidden = () => { throw new Error('LOCAL_CAPABILITY_NOT_AVAILABLE'); };
  const services: MerchantServices = {
    preparation: { facts: value => observe('preparation', value.onboardingId),
      async prepare(value, input, access) {
        writable(value.onboardingId, access); const current = facts('preparation'), expected = revision('preparation');
        if (!current.canPrepare || input.expectedRevision !== current.revision || input.deliveryIdentity !== target.merchant.deliveryIdentity) throw new LocalRejection('LOCAL_PREPARATION_CONFLICT');
        const identity = { onboardingId: target.onboardingId, operationId: randomUUID() };
        const state = { ...identity, deliveryIdentity: input.deliveryIdentity, updatedAt: now(), status: 'awaiting-command' as const, errorCode: null };
        const manifest = documents.map(item => ({ name: item.name, at: item.at, digest: fileDigest(item.content) }));
        await writeFile(join(directory, 'manifest.json'), json(manifest), { flag: 'wx', signal: access.signal });
        transaction(() => { requireRevision('preparation', expected, access); effect('prepare', access); accept(Preparation, command(access), identity, state);
          save('preparation', { ...current, revision: current.revision + 1, canPrepare: false, canRunCommand: true,
            preparation: state, currentPreparation: command(access) }); }); changes$.next(); return identity;
      },
      async runCommand(value, input, access) {
        writable(value.onboardingId, access); const current = facts('preparation'), expected = revision('preparation');
        if (!current.canRunCommand || !current.preparation || input.operationId !== current.preparation.operationId
          || input.expectedUpdatedAt !== current.preparation.updatedAt || input.command.kind !== 'inspect') throw new LocalRejection('LOCAL_INSPECTION_REQUIRED');
        const manifest = z.object({ name: z.string(), at: z.iso.datetime(), digest: z.string() }).array().parse(object(await readFile(join(directory, 'manifest.json'), 'utf8')));
        for (const item of manifest) if (fileDigest(await readFile(join(source, item.name), 'utf8')) !== item.digest) throw new Error('LOCAL_SOURCE_CHANGED');
        const identity = { onboardingId: target.onboardingId, operationId: input.operationId, deliveryCommandId: randomUUID() };
        const state = { ...identity, settled: true, output: { verifiedFiles: manifest.length }, reasonCode: null };
        transaction(() => { requireRevision('preparation', expected, access); effect('inspect-preparation', access); accept(DeliveryCommand, { ...command(access), operationId: input.operationId }, identity, state);
          const prepared = { ...current.preparation!, status: 'prepared' as const, updatedAt: now() };
          const preparationIdentity = json(Preparation.reference.parse({ onboardingId: prepared.onboardingId, operationId: prepared.operationId }));
          db.prepare('UPDATE operations SET state=? WHERE kind=? AND identity=?').run(json(prepared), Preparation.kind, preparationIdentity);
          db.prepare('INSERT INTO nodes(kind,identity,payload) VALUES(?,?,?)').run(Preparation.kind, preparationIdentity, json(prepared));
          save('preparation', { ...current, revision: current.revision + 1, canRunCommand: false, canObserveCommand: true,
            preparation: prepared, command: state, currentDeliveryCommand: { ...command(access), operationId: input.operationId } });
          const knowledge = facts('knowledge'); save('knowledge', { ...knowledge, revision: knowledge.revision + 1, canInitialize: true, sourceActivationId: input.operationId });
        }); changes$.next(); return identity;
      },
      observeCommand(value, input, access) { writable(value.onboardingId, access); const current = facts('preparation').command;
        if (!current || current.operationId !== input.operationId || current.deliveryCommandId !== input.deliveryCommandId) throw new Error('LOCAL_COMMAND_UNKNOWN'); return current; },
    },
    knowledge: { facts: value => observe('knowledge', value.onboardingId), retry: forbidden,
      async initialize(value, input, access) {
        writable(value.onboardingId, access); const current = facts('knowledge'), expected = revision('knowledge');
        if (!current.canInitialize || current.revision !== input.expectedRevision || !current.sourceActivationId) throw new LocalRejection('LOCAL_KNOWLEDGE_CONFLICT');
        const selected = documents.filter(item => Date.parse(item.at) >= Date.parse(input.request.window.startAt)
          && Date.parse(item.at) <= Date.parse(input.request.window.endAt));
        if (selected.length !== documents.length) throw new LocalRejection('LOCAL_SOURCE_WINDOW_INCOMPLETE');
        const content = await Promise.all(selected.map(async item => ({ name: item.name, content: await readFile(join(source, item.name), 'utf8') })));
        const index = json({ activationId: current.sourceActivationId, documents: content });
        await writeFile(join(directory, 'index.json'), index, { flag: 'wx', signal: access.signal });
        const identity = { onboardingId: target.onboardingId, submissionId: randomUUID() };
        const state = { ...identity, activationId: current.sourceActivationId, taskId: randomUUID(), taskStatus: 'completed' as const, rejectionCode: null, reasonCode: null };
        transaction(() => { requireRevision('knowledge', expected, access); effect('index', access); for (const item of content) db.prepare('INSERT INTO knowledge_documents VALUES(?,?)').run(item.name, item.content);
          accept(KnowledgeTask, command(access), identity, state); save('knowledge', { ...current, revision: current.revision + 1, canInitialize: false,
            canCommit: true, ready: true, snapshotReasonCode: null, tasks: [state], currentTask: command(access) }); }); changes$.next(); return identity;
      },
      async commit(value, input, access) {
        writable(value.onboardingId, access); const current = facts('knowledge'), expected = revision('knowledge');
        if (!current.canCommit || current.revision !== input.expectedRevision) throw new Error('LOCAL_SNAPSHOT_CONFLICT');
        const digest = fileDigest(await readFile(join(directory, 'index.json'), 'utf8'));
        transaction(() => { requireRevision('knowledge', expected, access); effect('commit-knowledge', access); db.prepare('INSERT INTO committed_snapshot VALUES(?,?)').run(target.onboardingId, digest);
          save('knowledge', { ...current, revision: current.revision + 1, canCommit: false });
          const reception = facts('reception'); save('reception', { ...reception, revision: reception.revision + 1, canEnable: true });
        }); changes$.next(); return { onboardingId: target.onboardingId, revision: current.revision + 1 };
      },
    },
    reception: { facts: value => observe('reception', value.onboardingId), resume: forbidden,
      async enable(value, input, access) {
        writable(value.onboardingId, access); const current = facts('reception'), expected = revision('reception'), prepared = facts('preparation').preparation;
        if (!current.canEnable || input.expectedRevision !== current.revision || !prepared || prepared.status !== 'prepared') throw new LocalRejection('LOCAL_RECEPTION_CONFLICT');
        const snapshot = db.prepare('SELECT digest FROM committed_snapshot WHERE id=?').get(target.onboardingId);
        if (!snapshot || snapshot.digest !== fileDigest(await readFile(join(directory, 'index.json'), 'utf8'))) throw new LocalRejection('LOCAL_SNAPSHOT_CHANGED');
        const identity = { onboardingId: target.onboardingId, operationId: randomUUID() };
        const result = { activationId: prepared.operationId, servingAccountId: 'local-file-reader', assignmentRevision: current.revision,
          canonicalEntryUrl: new URL('reception.json', 'file://' + directory + '/').href, evaluation: 'not-run' as const };
        await writeFile(join(directory, 'reception.json'), json({ ...identity, indexDigest: snapshot.digest, index: 'index.json', mode: 'local-file-only' }), { flag: 'wx', signal: access.signal });
        const state = { ...identity, preparationOperationId: prepared.operationId, updatedAt: now(), status: 'enabled' as const, errorCode: null, result };
        transaction(() => { requireRevision('reception', expected, access); effect('configure-reception', access); const cmd = { ...command(access), operationId: null, expectedUpdatedAt: null };
          accept(Reception, cmd, identity, state); save('reception', { ...current, revision: current.revision + 1, canEnable: false, operation: state, currentReception: cmd });
          const commands = facts('commands'); save('commands', { ...commands, canExecute: true }); }); changes$.next(); return identity;
      },
    },
    merchantCommands: { facts: value => {
        const selected = [target, observer].find(item => sameMerchant(item.merchant, value));
        if (!selected) throw new Error('LOCAL_TARGET_UNKNOWN');
        return observe('commands', selected.onboardingId);
      },
      async execute(value, input, access) {
        writable(target.onboardingId, access); if (!sameMerchant(value, target.merchant) || input.command.kind !== 'shell'
          || input.command.command !== 'node verify-reception.mjs') throw new LocalRejection('LOCAL_FINAL_INSPECTION_REQUIRED');
        const current = facts('commands'), expected = revision('commands'); if (!current.canExecute) throw new LocalRejection('LOCAL_COMMAND_NOT_ALLOWED');
        const inspected = await executeFile(process.execPath, [join(directory, 'verify-reception.mjs')], { cwd: directory, env: {}, signal: access.signal });
        const state = { ...value, commandId: randomUUID(), sessionId: access.sessionId, providerSessionId: null,
          observedAt: now(), state: 'completed' as const, exitCode: 0, output: inspected.stdout, reasonCode: null };
        const identity = MerchantCommand.reference.parse({ subject: value.subject, sandboxId: value.sandboxId, commandId: state.commandId });
        const cmd = { ...value, sessionId: access.sessionId, invocationId: access.invocationId };
        transaction(() => { requireRevision('commands', expected, access); effect('inspect-reception', access); accept(MerchantCommand, cmd, identity, state);
          save('commands', { ...current, canExecute: false, canObserve: true, commands: [state], currentCommand: cmd }); }); changes$.next(); return identity;
      },
      observe(value, input, access) { writable(target.onboardingId, access); const found = facts('commands').commands.find(item => item.commandId === input.commandId);
        if (!sameMerchant(value, target.merchant) || !found) throw new Error('LOCAL_COMMAND_UNKNOWN'); return found; },
      reconcile: forbidden,
    },
  };
  const owners: MerchantOwnerServices = { preparation: owner(Preparation), deliveryCommand: owner(DeliveryCommand), knowledgeTask: owner(KnowledgeTask),
    reception: owner(Reception), merchantCommand: owner(MerchantCommand) };
  function rejected<D extends ActionContract>(action: D, body: BusinessMethod<D>,
    rawCommand: (input: ActionParsedInput<D>, scope: ActionScope<D>, origin: ActionCommandOrigin) => unknown): BusinessMethod<D> {
    return async (scope, input, access) => {
      try { return await body(scope, input, access); }
      catch (error) {
        // 只有owner明确尚未受理的领域拒绝写为rejected；I/O等未知结果仍保留原命令调查。
        if (error instanceof LocalRejection && action.operation) {
          const mapping = action.operation;
          const raw = rawCommand(input, scope, { sessionId: access.sessionId, invocationId: access.invocationId, source: access.source, inputs: access.inputs });
          db.prepare('INSERT INTO rejected VALUES(?,?,?)').run(mapping.definition.kind,
            json(mapping.definition.command.parse(raw)), json({ code: error.message })); changes$.next();
        }
        throw error;
      }
    };
  }
  const guarded: MerchantServices = { ...services,
    preparation: { ...services.preparation, prepare: rejected(PrepareMerchant, services.preparation.prepare, PrepareMerchant.operation!.command),
      runCommand: rejected(RunPreparationCommand, services.preparation.runCommand, RunPreparationCommand.operation!.command) },
    knowledge: { ...services.knowledge, initialize: rejected(InitializeKnowledge, services.knowledge.initialize, InitializeKnowledge.operation!.command) },
    reception: { ...services.reception, enable: rejected(EnableReception, services.reception.enable, EnableReception.operation!.command) },
    merchantCommands: { ...services.merchantCommands, execute: rejected(ExecuteMerchantCommand, services.merchantCommands.execute, ExecuteMerchantCommand.operation!.command) },
  };
  return { target, observer, services: guarded, owners, changes$: changes$.asObservable(), db,
    pending: () => db.prepare('SELECT id,link FROM follows WHERE pending=1').all().map(row => ({ id: z.string().parse(row.id), link: operationFollowLinkSchema.parse(object(row.link)) })),
    acknowledge(id: string) { db.prepare('UPDATE follows SET pending=0 WHERE id=?').run(id); },
    finished: () => facts('commands').commands.some(item => item.state === 'completed'),
    async verify() {
      if (!facts('commands').commands.some(item => item.state === 'completed') || facts('reception').operation?.status !== 'enabled') throw new Error('LOCAL_COLLABORATION_INCOMPLETE');
      const snapshot = db.prepare('SELECT digest FROM committed_snapshot WHERE id=?').get(target.onboardingId);
      const index = await readFile(join(directory, 'index.json'), 'utf8'), receptionText = await readFile(join(directory, 'reception.json'), 'utf8');
      const reception = z.object({ mode: z.literal('local-file-only'), indexDigest: z.string() }).parse(object(receptionText));
      if (snapshot?.digest !== fileDigest(index) || reception.indexDigest !== snapshot.digest) throw new Error('LOCAL_ARTIFACT_MISMATCH');
      const indexed = z.object({ documents: z.object({ name: z.string(), content: z.string() }).strict().array() }).parse(object(index));
      const stored = db.prepare('SELECT name,content FROM knowledge_documents ORDER BY name').all();
      if (indexed.documents.length !== documents.length || stored.some(row => {
        const item = indexed.documents.find(item => item.name === row.name);
        return !item || row.name !== item.name || row.content !== item.content;
      })) throw new Error('LOCAL_INDEX_STORAGE_MISMATCH');
      for (const item of documents) {
        const entry = indexed.documents.find(entry => entry.name === item.name);
        if (!entry || entry.content !== item.content || await readFile(join(source, item.name), 'utf8') !== item.content) throw new Error('LOCAL_SOURCE_CHANGED');
      }
      const count = z.number().int().parse(db.prepare('SELECT count(*) AS count FROM knowledge_documents').get()?.count);
      if (count !== documents.length || db.prepare('SELECT count(*) AS count FROM effects').get()?.count !== 6) throw new Error('LOCAL_EFFECTS_INCOMPLETE');
      if (facts('preparation', observer.onboardingId).preparation !== null) throw new Error('LOCAL_OBSERVER_CHANGED');
      if (['knowledge', 'reception', 'commands'].some(domain => {
        const row = db.prepare('SELECT revision FROM facts WHERE domain=? AND target=?').get(domain, observer.onboardingId);
        return row?.revision !== 1;
      })) throw new Error('LOCAL_OBSERVER_CHANGED');
      return { indexedDocuments: count, indexDigest: z.string().parse(snapshot.digest), receptionDigest: fileDigest(receptionText),
        effects: db.prepare('SELECT action,invocation,source FROM effects ORDER BY id').all().map(row => ({
          action: z.string().parse(row.action), invocationId: z.string().parse(row.invocation), source: object(row.source),
        })), feedback: db.prepare('SELECT id,link,node,pending FROM follows ORDER BY rowid').all().map(row => ({
          participationId: z.string().parse(row.id), link: operationFollowLinkSchema.parse(object(row.link)),
          nodeId: row.node === null ? null : 'local-node:' + z.number().int().parse(row.node), acknowledged: row.pending === 0,
        })) };
    },
    close() { changes$.complete(); db.close(); },
  };
  } catch (error) { db.close(); throw error; }
}
