import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { Subject, of } from 'rxjs';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { SessionId, pluginRecordOf } from '@deepseek-ai/dsh-session';
import { defineAction, defineApplication, defineEnvironment, defineOperation, defineProgram, defineSurface,
  implementAction, implementOperation, observeResource, view, InputEditRejectedError, z,
  type ActionSource, type Admission, type AdmittedSubmission, type HumanProgramSessionClient,
  type OperationAccessContext, type ProgramSessionClient } from '@hanger-source/dsh-reactive';
import { Expose, implementProgram, useAction, useOperation } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';

test('固定服务端口贯穿会话取得、正文与业务参与，消费者退出不改变已受理业务的身份和结算', { timeout: 30_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-service-participant-'));
  const db = new DatabaseSync(join(root, 'business.sqlite'));
  db.exec('CREATE TABLE work (id TEXT PRIMARY KEY, actorId TEXT NOT NULL, state TEXT NOT NULL)');
  const changed = new Subject<void>();
  const entered = Promise.withResolvers<void>(), finish = Promise.withResolvers<void>();
  const admissions: { input: AdmittedSubmission; actorId: string; participant: Admission['participant'] }[] = [];
  const actionSources: ActionSource[] = [], operationSources: ActionSource[] = [];
  const rowSchema = z.object({ actorId: z.string(), state: z.enum(['running', 'complete']) });
  const read = (id: string) => {
    const row = db.prepare('SELECT actorId, state FROM work WHERE id = ?').get(id);
    return row ? rowSchema.parse(row) : undefined;
  };
  const authorize = (access: OperationAccessContext) => {
    access.signal.throwIfAborted();
    operationSources.push(access.source);
    if (access.source.kind !== 'participant' || access.source.participant !== 'service' || access.source.actorId !== 'worker-a') {
      throw new Error('BUSINESS_WORKER_FORBIDDEN');
    }
  };
  const operation = defineOperation({ kind: 'service-owned-work', version: 1, command: z.string(), reference: z.string(),
    state: z.enum(['running', 'complete']), settled: state => state === 'complete' ? state : undefined });
  const owner = implementOperation(operation, {
    locate(command, access) {
      authorize(access);
      const row = read(command);
      return row ? { status: 'accepted', reference: command, state: row.state } : { status: 'unknown' };
    },
    observe(reference, access) {
      authorize(access);
      return observeResource({ key$: of(reference), read: id => read(id)!.state, invalidations: () => changed });
    },
  });
  const command = defineAction({ name: 'accept_service_work', description: '受理固定服务身份拥有的业务操作',
    scope: z.null(), input: z.object({ id: z.string() }), output: z.object({ operation: z.string(), actorId: z.string() }),
    operation: { definition: operation, command: input => input.id, reference: receipt => receipt.operation } });
  const implementation = implementAction(command, { operationOwner: owner, async execute(input, call) {
    actionSources.push(call.source);
    if (call.source.kind !== 'participant' || call.source.participant !== 'service' || call.source.actorId !== 'worker-a') {
      throw new Error('BUSINESS_WORKER_FORBIDDEN');
    }
    const actorId = call.source.actorId;
    db.prepare('INSERT INTO work VALUES (?, ?, ?)').run(input.id, actorId, 'running');
    entered.resolve();
    await finish.promise;
    call.signal.throwIfAborted();
    return { operation: input.id, actorId };
  } });
  const surface = defineSurface({ views: { status: view(z.string()) }, actions: { accept: command }, operations: { current: operation } });
  const program = defineProgram({ input: z.null(), surface }), status$ = of('shared-facts');
  const definition = defineApplication({ name: 'service-participant', async admit(input, _conversation, admission) {
    admissions.push({ input: structuredClone(input), actorId: admission.actorId, participant: admission.participant });
  }, environments: [defineEnvironment({ id: 'work', instructions: '所有工作经公开服务端口显式受理，不启动模型。',
    program: implementProgram(program, { Component() {
      const action = useAction(implementation, () => null, []);
      const current = useOperation(owner, () => 'job-a', []);
      return Expose({ surface, value: { views: { status: status$ }, actions: { accept: action }, operations: { current } } });
    } }),
  })] });
  let models = 0;
  const errors: unknown[] = [], handles: { release(): void }[] = [];
  const runtime = await createRuntime(assembly => { assembly.operation(owner); return definition; }, {
    home: join(root, 'home'), actorId: 'operator-test', serviceActorId: 'program-service', onError: event => errors.push(event.error),
    model: nativeModel({ provider: 'isolated', model: 'service-participant', create: () => new class extends LlmAdapter {
      async *stream() { models++; throw new Error('SERVICE_PARTICIPATION_MUST_NOT_CALL_MODEL'); }
    }() }),
  });
  async function commonFacts(session: ProgramSessionClient<typeof program>) {
    return { source: session, value: await session.readView('status'), request: await session.requestStatus('not-submitted') };
  }
  try {
    const serviceA = runtime.application.service('worker-a');
    await mkdir(join(root, 'workspace'));
    const workspace = await serviceA.createWorkspace({ directory: join(root, 'workspace') });
    assert.ok((await serviceA.describe()).some(item => item.workspaceId === workspace.workspaceId));
    const service = await serviceA.create('work', { workspaceId: workspace.workspaceId }); handles.push(service);
    const human = await runtime.application.open(service.id, program); handles.push(human);
    const peer = await serviceA.open(service.id, program); handles.push(peer);
    const resource = peer.viewResource('status'), bindingId = peer.resolveAction('accept', null).bindingId;
    const serviceFacts = await commonFacts(service), humanFacts = await commonFacts(human);
    assert.equal(serviceFacts.source, service); assert.equal(humanFacts.source, human);
    assert.equal(serviceFacts.value, 'shared-facts'); assert.equal(humanFacts.value, 'shared-facts');
    assert.deepEqual(serviceFacts.request, { requestId: 'not-submitted', state: 'absent' });
    assert.equal(human.resolveAction('accept', null).bindingId, bindingId);
    const original = runtime.native.agents.get(SessionId(service.id))!;
    await runtime.application.close(service.id);
    assert.equal(runtime.native.agents.get(SessionId(service.id)), undefined);
    const cold = await serviceA.open(service.id, program); handles.push(cold);
    assert.equal(cold.resolveAction('accept', null).bindingId, bindingId);
    assert.equal(peer.viewResource('status'), resource);
    assert.equal(await resource.read(), 'shared-facts');
    assert.equal(runtime.native.agents.get(SessionId(service.id)), undefined);
    const resumed = await serviceA.resume(service.id, program); handles.push(resumed);
    const agent = runtime.native.agents.get(SessionId(service.id))!;
    assert.notEqual(agent, original);
    assert.equal(resumed.resolveAction('accept', null).bindingId, bindingId);

    const input = { requestId: 'facts-a', intent: 'question' as const, delivery: 'inject' as const, scope: null, content: '服务资料' };
    for (const actorId of ['worker-a', 'worker-b', undefined]) {
      const forged = { actorId, signal: new AbortController().signal };
      for (const invoke of [
        () => service.submit(input, forged),
        () => service.editQueuedInput(input.requestId, '伪造修订', forged),
        () => service.resolveAction('accept', null)({ id: 'forged' }, forged),
        () => service.resolveAction('accept', null, forged),
        () => service.uploadFile(new TextEncoder().encode('伪造上传'), forged),
        () => service.uploadEncodedFile({ data: 'YQ==' }, forged),
        () => service.attachOperation(operation, 'job-a', forged),
        () => service.locateOperation(operation, 'job-a', forged),
        () => service.operations.current(forged),
      ]) await assert.rejects(async () => invoke(), /REACTIVE_SERVICE_ACTOR_IMMUTABLE/u);
    }
    const file = await service.uploadFile(new TextEncoder().encode('固定服务上传的实际资料'), { name: 'service.txt' }).result;
    assert.ok(runtime.native.fileUploads.resolve(agent, file.receiptId));
    assert.deepEqual(await service.submit({ ...input, content: [{ type: 'file', receiptId: file.receiptId }] }),
      { sessionId: service.id, requestId: input.requestId, accepted: true });
    const workerB = await runtime.application.service('worker-b').open(service.id, program); handles.push(workerB);
    await workerB.submit({ ...input, requestId: 'facts-b' });
    await assert.rejects(workerB.editQueuedInput(input.requestId, '另一服务篡改'),
      error => error instanceof InputEditRejectedError && error.reason === 'actor-mismatch');
    await assert.rejects(workerB.submit(input), /REACTIVE_REQUEST_ID_CONFLICT/u);
    const revised = [{ type: 'text' as const, text: '固定服务修订资料' }, { type: 'file' as const, receiptId: file.receiptId }];
    await service.editQueuedInput(input.requestId, revised);
    await human.submit({ ...input, requestId: 'human-facts' });
    const projected = runtime.native.sessionProjections.stateOf(agent.session, 'reactiveInputs')!;
    assert.equal(projected.records[input.requestId]?.actorId, 'worker-a');
    assert.equal(projected.records[input.requestId]?.participant, 'service');
    assert.equal(projected.records['facts-b']?.actorId, 'worker-b');
    assert.equal(projected.records['human-facts']?.actorId, 'operator-test');
    assert.deepEqual(admissions.filter(item => item.input.requestId === input.requestId).at(-1), {
      actorId: 'worker-a', participant: 'service', input: { ...input, content: [
        { type: 'text', text: '固定服务修订资料' }, { type: 'file', attachment: file.file },
      ] },
    });
    await runtime.native.sessions.flush(agent.session);
    await runtime.native.sessionPersistence.flush();
    const stored = await runtime.native.sessionPersistence.open(agent.id, 'read');
    try {
      const durable = await stored.read();
      const origins = durable.events.flatMap(event => {
        const record = pluginRecordOf(event);
        return record?.type === 'plugin:reactive/input' ? [z.object({ requestId: z.string(), actorId: z.string(), participant: z.string() }).parse(record.data)] : [];
      });
      assert.deepEqual(origins.find(item => item.requestId === input.requestId), { requestId: input.requestId, actorId: 'worker-a', participant: 'service' });
    } finally { await stored.close(); }

    const artificialHuman = service as unknown as HumanProgramSessionClient<typeof program>;
    await assert.rejects(artificialHuman.pause(), /REACTIVE_HUMAN_PARTICIPANT_REQUIRED/u);
    await assert.rejects(artificialHuman.cancel(), /REACTIVE_HUMAN_PARTICIPANT_REQUIRED/u);
    await assert.rejects(artificialHuman.question('absent'), /REACTIVE_HUMAN_PARTICIPANT_REQUIRED/u);
    await assert.rejects(async () => artificialHuman.answerQuestion('absent', { answers: [] }), /REACTIVE_HUMAN_PARTICIPANT_REQUIRED/u);
    await assert.rejects(artificialHuman.attachWait('absent')[Symbol.asyncIterator]().next(), /REACTIVE_HUMAN_PARTICIPANT_REQUIRED/u);
    assert.deepEqual(await service.requestStatus(input.requestId), { requestId: input.requestId, state: 'accepted' });
    await assert.rejects(workerB.resolveAction('accept', null)({ id: 'forbidden-worker' }).result, /BUSINESS_WORKER_FORBIDDEN/u);
    const caller = new AbortController(), waiting = new AbortController();
    const invocation = service.resolveAction('accept', null)({ id: 'job-a' }, { signal: caller.signal });
    await entered.promise;
    const reason = new Error('SERVICE_RECEIPT_WAIT_FINISHED');
    const withdrawn = assert.rejects(invocation.wait({ signal: waiting.signal }), error => error === reason);
    const retained = invocation.wait();
    const existing = await peer.attachOperation(operation, 'job-a');
    assert.equal(await existing.state.read(), 'running');
    assert.deepEqual(await peer.operations.current().locate(), { status: 'accepted', reference: 'job-a', state: 'running' });
    await assert.rejects(workerB.locateOperation(operation, 'job-a'), /BUSINESS_WORKER_FORBIDDEN/u);
    caller.abort(new Error('SERVICE_ADMISSION_CALLER_EXITED'));
    waiting.abort(reason); await withdrawn;
    assert.deepEqual(read('job-a'), { actorId: 'worker-a', state: 'running' });
    service.release();
    finish.resolve();
    assert.deepEqual(await invocation.result, { operation: 'job-a', actorId: 'worker-a' });
    assert.deepEqual(await retained, await invocation.result);
    const restored = peer.invocation('accept', invocation.id);
    assert.equal(restored, invocation, '查回保留原call，首次caller取消不能污染后续wait');
    assert.deepEqual(await restored.wait(), await invocation.result);
    const receipt = await invocation.operation;
    assert.equal(await receipt.state.read(), 'running');
    const terminal = receipt.settled();
    db.prepare('UPDATE work SET state = ? WHERE id = ?').run('complete', 'job-a'); changed.next();
    assert.equal(await terminal, 'complete');
    assert.equal(read('job-a')?.actorId, 'worker-a');
    assert.deepEqual(actionSources.map(source => source.kind === 'participant' ? [source.participant, source.actorId] : source.kind),
      [['service', 'worker-b'], ['service', 'worker-a']]);
    assert.ok(operationSources.some(source => source.kind === 'participant' && source.participant === 'service' && source.actorId === 'worker-a'));
    assert.ok(operationSources.some(source => source.kind === 'participant' && source.participant === 'service' && source.actorId === 'worker-b'));
    assert.equal(models, 0);
    assert.deepEqual(errors, []);
  } finally {
    finish.resolve();
    for (const handle of handles) handle.release();
    try { await runtime.dispose(); } finally { changed.complete(); db.close(); }
  }
});
