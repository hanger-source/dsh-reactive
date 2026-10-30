import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { setImmediate as checkpoint } from 'node:timers/promises';
import { BehaviorSubject, Subject, filter, firstValueFrom, from, of, timeout } from 'rxjs';
import WebSocket from 'ws';
import { Context } from '@deepseek-ai/cordis';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { SessionId } from '@deepseek-ai/dsh-session';
import { WorkspaceId } from '@deepseek-ai/dsh-workspace';
import * as Registry from '@deepseek-ai/dsh-typert-registry/client';
import * as Gateway from '@deepseek-ai/dsh-api-gateway/client';
import { installConnection } from '@deepseek-ai/dsh-client-connection/client';
import { ConversationAccessRevokedError, defineAction, defineApplication, defineEnvironment, defineOperation, defineProgram, defineSurface,
  implementAction, implementOperation, observeResource, programIdentity, resourceView, z } from '@hanger-source/dsh-reactive';
import { Expose, implementProgram, useAction, useOperation } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import { bindProgramClient, bindServiceProgramClient, createAuthenticatedTransport, createProgramClient,
  createServiceProgramClient, ProgramParticipantMismatchError } from '@hanger-source/dsh-reactive-client';
import contribution from '@hanger-source/dsh-reactive-client/remote';
import { mountProgramGateway, RemoteAuthenticationError } from '@hanger-source/dsh-reactive-remote';

const next = (source, predicate) => firstValueFrom(source.pipe(filter(predicate), timeout(8_000)));
const ready = (value, generation = 1) => ({ status: 'ready', generation, observedAt: Date.now(), value });

test('服务SDK贯通真实认证Gateway；身份来自宿主、人工控制隔离，原连接恢复不重复业务', { timeout: 30_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-service-client-'));
  const db = new DatabaseSync(join(root, 'business.sqlite'));
  db.exec('CREATE TABLE work (id TEXT PRIMARY KEY, actorId TEXT NOT NULL, phase TEXT NOT NULL)');
  const changed = new Subject(), facts = new BehaviorSubject(ready('original-facts'));
  const admissions = [], actionSources = [], operationSources = [], errors = [];
  const clients = [], contexts = [], subscriptions = [], handles = [], journalReaders = [], journalGates = [];
  let restoreJournalPage = () => {};
  let models = 0, gateway;
  const actorId = 'shared-actor';
  const row = id => db.prepare('SELECT actorId, phase FROM work WHERE id = ?').get(id);
  const authorize = access => {
    access.signal.throwIfAborted(); operationSources.push(access.source);
    assert.equal(access.source.kind, 'participant');
    assert.equal(access.source.participant, 'service');
    assert.equal(access.source.actorId, actorId);
  };
  const operation = defineOperation({ kind: 'gateway-service-work', version: 1, command: z.string(), reference: z.string(),
    state: z.enum(['running', 'complete']), settled: value => value === 'complete' ? value : undefined });
  const owner = implementOperation(operation, {
    locate(id, access) {
      authorize(access); const found = row(id);
      return found ? { status: 'accepted', reference: id, state: found.phase } : { status: 'unknown' };
    },
    observe(id, access) {
      authorize(access);
      return observeResource({ key$: of(id), read: key => row(key).phase, invalidations: () => changed });
    },
  });
  const accept = defineAction({ name: 'accept_gateway_service_work', description: '由明确的服务参与者受理一次业务操作',
    scope: z.null(), input: z.string(), output: z.object({ id: z.string(), actorId: z.string() }),
    operation: { definition: operation, command: input => input, reference: receipt => receipt.id } });
  const implementation = implementAction(accept, { operationOwner: owner, execute(id, call) {
    actionSources.push(call.source);
    assert.equal(call.source.kind, 'participant'); assert.equal(call.source.participant, 'service');
    assert.equal(call.source.actorId, actorId);
    db.prepare('INSERT INTO work VALUES (?, ?, ?)').run(id, call.source.actorId, 'running');
    changed.next(); return { id, actorId: call.source.actorId };
  } });
  const surface = defineSurface({ views: { current: resourceView(z.string()) }, actions: { accept }, operations: { current: operation } });
  const program = defineProgram({ name: 'service_client_test', input: z.null(), surface });
  const application = defineApplication({ name: 'gateway-service-client', admit(input, _identity, access) {
    admissions.push({ input: structuredClone(input), actorId: access.actorId, participant: access.participant });
  }, environments: [defineEnvironment({ id: 'work', instructions: '只处理公开服务调用和注入资料，不启动模型。',
    program: implementProgram(program, { Component() {
      const bound = useAction(implementation, () => null, []), current = useOperation(owner, () => 'job-a', []);
      return Expose({ surface, value: { views: { current: facts }, actions: { accept: bound }, operations: { current } } });
    } }),
  })] });
  const runtime = await createRuntime(assembly => { assembly.operation(owner); return application; }, {
    home: join(root, 'home'), actorId: 'host-human', onError: event => errors.push(event.error),
    model: nativeModel({ provider: 'service-client', model: 'unused', create: () => new class extends LlmAdapter {
      async *stream() { models++; throw new Error('SERVICE_CLIENT_MUST_NOT_START_MODEL'); }
    }() }),
  });
  t.after(async () => {
    for (const gate of journalGates) gate.release.resolve();
    for (const reading of journalReaders) reading.controller.abort();
    await Promise.allSettled(journalReaders.map(reading => reading.iterator.return()));
    restoreJournalPage();
    subscriptions.forEach(subscription => subscription.unsubscribe());
    try { await Promise.all(clients.map(client => client.dispose())); }
    finally {
      handles.forEach(handle => handle.release());
      try { await Promise.all(contexts.map(ctx => ctx.fiber.dispose())); }
      finally { try { await gateway?.close(); } finally { await runtime.dispose(); changed.complete(); facts.complete(); db.close(); } }
    }
  });
  const workspace = await runtime.application.createWorkspace({ directory: root });
  const sessionId = crypto.randomUUID(), visible = new Set([sessionId]);
  // 服务principal也明确取得人工endpoint grants；拒绝必须来自participant边界。
  const actions = ['open', 'resume', 'questions', 'progress', 'journal', 'journalPage', 'view', 'requestStatus', 'submit', 'editQueuedInput',
    'uploadFile', 'resolveAction', 'startAction', 'actionInvocation', 'actionResult', 'observeAction', 'resolveOperation', 'locateOperation',
    'observeOperation', 'observeInvocationOperation', 'pause', 'resumeGoal', 'cancel', 'answerQuestion', 'attachWait'].map(name => 'reactivePrograms/' + name);
  gateway = await mountProgramGateway(runtime.native, { application: runtime.application, model: { provider: 'service-client', model: 'unused' },
    participant: principal => ({ participant: principal.id === 'worker' ? 'service' : 'human', actorId }),
    access: service => service.registerPolicy('service-client-test', { bindings: () => [
      ...[...visible].map(id => ({ role: { name: 'service-test-session', resource: 'session', actions }, resource: { kind: 'session', id } })),
      { role: { name: 'service-test-workspace', resource: 'workspace', actions: ['reactivePrograms/create', 'reactivePrograms/completeCreation'] },
        resource: { kind: 'workspace', id: workspace.workspaceId } },
    ] }),
    carrier: { onError: error => errors.push(error), async authenticate(request) {
      const authorization = request.headers.get('authorization');
      if (!['Bearer worker', 'Bearer operator'].includes(authorization)) throw new RemoteAuthenticationError(401);
      return { kind: 'service-client-test', id: authorization.slice('Bearer '.length) };
    } },
  });
  const transport = principal => {
    const sockets = [], forged = new Set();
    const options = { baseUrl: gateway.url, async fetch(url, init) {
      let body = init?.body;
      if (principal === 'worker' && typeof body === 'string') {
        const envelope = JSON.parse(body), request = envelope.payload?.args?.request;
        if (request) {
          // 在官方客户端编码之后改写真正的HTTP wire，不能由SDK提前清理冒充攻击。
          request.actorId = 'forged-human'; request.participant = 'human';
          forged.add(new URL(url).pathname.split('/').at(-1)); body = JSON.stringify(envelope);
        }
      }
      return fetch(url, { ...init, body, headers: { ...Object.fromEntries(new Headers(init?.headers)), authorization: 'Bearer ' + principal } });
    }, createWebSocket(url) {
      const socket = new WebSocket(url, { headers: { authorization: 'Bearer ' + principal } }); sockets.push(socket); return socket;
    } };
    return { options, sockets, forged };
  };
  const ownTransport = transport('worker');
  const serviceClient = await createServiceProgramClient(program, ownTransport.options); clients.push(serviceClient);
  const created = await serviceClient.create('work', { workspaceId: workspace.workspaceId, sessionId });
  const local = await runtime.application.open(sessionId); handles.push(local);
  assert.deepEqual(created.identity, { sessionId, environmentId: 'work', workspaceId: workspace.workspaceId, parentSessionId: null, participant: 'service', actorId,
    program: { name: program.name, version: program.version }, accessId: local.accessId });
  const connect = async principal => {
    const ctx = new Context(), selected = transport(principal); contexts.push(ctx);
    await ctx.plugin(Registry);
    const physical = createAuthenticatedTransport(selected.options);
    ctx.effect(() => () => physical.dispose(), 'authenticated transport');
    installConnection(ctx, { interactions: 'answer', transport: physical });
    await ctx.plugin(Gateway); await ctx.remote.$mount(contribution);
    return { ctx, ...selected };
  };
  const worker = await connect('worker'), operator = await connect('operator');
  const borrowed = await bindServiceProgramClient(program, worker.ctx), humanClient = await bindProgramClient(program, operator.ctx);
  clients.push(borrowed, humanClient);
  const session = await borrowed.open(sessionId), human = await humanClient.open(sessionId);
  const localService = await runtime.application.service(actorId).open(sessionId, program); handles.push(localService);
  const commonFacts = async handle => ({ value: await handle.readView('current'),
    sessionId: (await handle.progress.read(t.signal)).activity.sessionId,
    human: await handle.human.read(t.signal), request: await handle.request('not-submitted').state.read(t.signal) });
  assert.deepEqual(await commonFacts(session), await commonFacts(human));
  assert.equal(session.viewResource('current'), session.viewResource('current'));
  assert.equal(session.request('not-submitted'), session.request('not-submitted'));
  for (const control of ['question', 'answerQuestion', 'attachWait', 'pause', 'resumeGoal', 'cancel']) {
    assert.equal(control in session, false, '服务实际运行对象没有人工方法：' + control);
    assert.equal(control in created, false);
  }
  const initialAgent = runtime.native.agents.get(SessionId(sessionId)); assert.ok(initialAgent);
  await runtime.application.close(sessionId);
  assert.equal(runtime.native.agents.get(SessionId(sessionId)), undefined);
  const cold = await borrowed.open(sessionId);
  assert.equal(await cold.readView('current'), 'original-facts');
  const coldController = new AbortController();
  const coldFollowing = local.journal.follow({}, { signal: coldController.signal })[Symbol.asyncIterator]();
  journalReaders.push({ controller: coldController, iterator: coldFollowing });
  const coldOpening = await coldFollowing.next();
  assert.equal(coldOpening.value.type, 'opened');
  const coldQuery = { throughSeq: coldOpening.value.cursor, turnWindow: { minMessages: 1, minTurns: 1 } };
  const coldPages = await Promise.all([local, localService, cold, human].map(handle => handle.journal.page(coldQuery)));
  for (const page of coldPages) assert.deepEqual(page, coldPages[0], '本地人工、本地服务和远程沿同一原生cut读取');
  assert.deepEqual(await cold.journal.page({ throughSeq: coldOpening.value.cursor }), coldOpening.value.page);
  await coldFollowing.return();
  assert.equal(runtime.native.agents.get(SessionId(sessionId)), undefined, '共同资源冷读取不复活Agent');
  const resumed = await borrowed.resume(sessionId);
  assert.equal(resumed.identity.participant, 'service');
  const agent = runtime.native.agents.get(SessionId(sessionId)); assert.ok(agent); assert.notEqual(agent, initialAgent);
  const controller = runtime.native.sessionController, page = controller.page;
  let pageGate;
  controller.page = new Proxy(page, { async apply(target, receiver, args) {
    const gate = pageGate; pageGate = undefined;
    const result = await Reflect.apply(target, receiver, args);
    if (gate) {
      gate.entered.resolve({ query: args[0], signal: args[1], page: result });
      await gate.release.promise; gate.returned.resolve();
    }
    return result;
  } });
  restoreJournalPage = () => { controller.page = page; };
  const bytes = new TextEncoder().encode('由服务上传并作为同一请求注入的原始资料');
  const uploaded = await session.uploadFile(bytes, { name: 'service.txt' }).result;
  assert.ok(runtime.native.fileUploads.resolve(agent, uploaded.receiptId));
  const chunks = [];
  for await (const chunk of runtime.native.attachments.readFileStream(uploaded.file)) chunks.push(chunk);
  assert.deepEqual(Buffer.concat(chunks), Buffer.from(bytes));
  const input = { requestId: 'service-facts', content: [{ type: 'file', receiptId: uploaded.receiptId }], scope: null, intent: 'question', delivery: 'inject' };
  const receipt = await session.submit(input);
  const revised = [{ type: 'text', text: '服务重新准入的修订资料' }, { type: 'file', receiptId: uploaded.receiptId }];
  assert.deepEqual(await session.editQueuedInput(input.requestId, revised), receipt);
  assert.deepEqual(admissions.at(-1), { actorId, participant: 'service', input: { ...input, content: [
    revised[0], { type: 'file', attachment: uploaded.file },
  ] } });
  const stored = runtime.native.sessionProjections.stateOf(agent.session, 'reactiveInputs').records[input.requestId];
  assert.equal(stored.actorId, actorId); assert.equal(stored.participant, 'service');
  assert.deepEqual(await session.requestStatus(input.requestId), { requestId: input.requestId, state: 'accepted' });
  assert.equal((await next(session.request(input.requestId).state.values$, value => value.activity !== null)).activity.state, 'accepted');
  const journalChanges = [], journalUpdates = new Subject();
  t.after(() => journalUpdates.complete());
  const openedCancellation = new AbortController();
  const journal = await session.openEvents({ publish: change => { journalChanges.push(change); journalUpdates.next(change); },
    failed: error => errors.push(error) }, {}, { signal: openedCancellation.signal });
  assert.equal(journalChanges[0]?.type, 'replace');
  openedCancellation.abort(new Error('OPENING_ALREADY_COMPLETED'));

  // 延迟公开Controller.page的真实回执，提交仍走原Session输入受理与原生日志。
  const openingGate = { entered: Promise.withResolvers(), release: Promise.withResolvers(), returned: Promise.withResolvers() };
  journalGates.push(openingGate); pageGate = openingGate;
  const followController = new AbortController();
  const following = session.journal.follow({}, { signal: followController.signal })[Symbol.asyncIterator]();
  journalReaders.push({ controller: followController, iterator: following });
  const opening = following.next();
  const heldOpening = await openingGate.entered.promise;
  assert.equal(heldOpening.signal.aborted, false);
  await session.submit({ requestId: 'journal-during-opening', content: '开场持久页回执期间追加的资料', scope: null,
    intent: 'question', delivery: 'inject' });
  const throughOpening = agent.session.seq - 1;
  assert.ok(throughOpening > heldOpening.query.throughSeq, '门闩期间确有新的原生持久事件');
  openingGate.release.resolve();
  const opened = await opening;
  assert.equal(opened.value.type, 'opened');
  assert.equal(opened.value.cursor, heldOpening.query.throughSeq);
  assert.deepEqual(opened.value.page, heldOpening.page);
  for (const handle of [local, localService, session, human]) {
    assert.deepEqual(await handle.journal.page({ throughSeq: opened.value.cursor }), opened.value.page,
      '已追加事件不能漂移本次开场cut');
  }
  const openingEvents = agent.session.snapshotEvents().filter(event => event.seq > opened.value.cursor && event.seq <= throughOpening);
  const openingEntries = [];
  for (const _event of openingEvents) {
    const frame = await following.next(); assert.equal(frame.value.type, 'entry'); openingEntries.push(frame.value.entry.event);
  }
  assert.deepEqual(openingEntries.map(event => [event.seq, event.type]), openingEvents.map(event => [event.seq, event.type]),
    '开场read期间追加事件沿原序交付，不丢失也不重复');
  await session.submit({ requestId: 'journal-after-opening', content: '开场后继续追加的资料', scope: null,
    intent: 'question', delivery: 'inject' });
  const afterOpeningEvents = agent.session.snapshotEvents().filter(event => event.seq > throughOpening);
  const afterOpeningEntries = [];
  for (const _event of afterOpeningEvents) {
    const frame = await following.next(); assert.equal(frame.value.type, 'entry'); afterOpeningEntries.push(frame.value.entry.event);
  }
  assert.deepEqual(afterOpeningEntries.map(event => [event.seq, event.type]), afterOpeningEvents.map(event => [event.seq, event.type]),
    '后续append不重放开场缓冲区尾项');
  await following.return();

  const cancelledGate = { entered: Promise.withResolvers(), release: Promise.withResolvers(), returned: Promise.withResolvers() };
  journalGates.push(cancelledGate); pageGate = cancelledGate;
  const cancellation = new AbortController(), cancellationReason = new Error('CANCEL_ONLY_JOURNAL_OPENING');
  const cancelledChanges = [];
  const cancelledOpening = session.openEvents({ publish: change => cancelledChanges.push(change), failed: error => errors.push(error) },
    {}, { signal: cancellation.signal }).then(value => ({ value }), error => ({ error }));
  const heldCancellation = await cancelledGate.entered.promise;
  cancellation.abort(cancellationReason);
  const cancelled = await firstValueFrom(from(cancelledOpening).pipe(timeout(3_000)));
  assert.equal(cancelled.error, cancellationReason, '只取消本次opening，在不合作page回执放行前及时退出');
  assert.deepEqual(await session.journal.page({ throughSeq: heldCancellation.query.throughSeq }), heldCancellation.page,
    '同SDK的共同journal仍能独立读页');
  assert.equal(await session.readView('current'), 'original-facts');
  cancelledGate.release.resolve(); await cancelledGate.returned.promise; await checkpoint();
  assert.deepEqual(cancelledChanges, [], '迟到开场页不向已取消消费者发布');
  const beforeSibling = agent.session.seq - 1;
  const siblingChanged = next(journalUpdates, change => change.type === 'append' && change.entry.event.seq > beforeSibling);
  await session.submit({ requestId: 'journal-sibling-survives', content: '兄弟日志观察仍接收的资料', scope: null,
    intent: 'question', delivery: 'inject' });
  await siblingChanged;

  const invocation = await (await session.resolveAction('accept', null))('job-a');
  assert.deepEqual(await invocation.result, { id: 'job-a', actorId });
  assert.equal(invocation.source.participant, 'service'); assert.equal(invocation.source.actorId, actorId);
  const bound = await session.operations.current();
  assert.deepEqual(await bound.locate(), { status: 'accepted', reference: 'job-a', state: 'running' });
  const business = session.attachOperation(operation, 'job-a');
  subscriptions.push(business.state.values$.subscribe());
  assert.equal(await business.state.read(t.signal), 'running');
  assert.deepEqual(await session.locateOperation(operation, 'job-a'), { status: 'accepted', reference: 'job-a', state: 'running' });
  assert.ok(operationSources.length > 0);
  assert.equal(actionSources.length, 1);

  const sessionAddress = { sessionId, program: programIdentity(program) };
  const address = { ...sessionAddress, invocationId: invocation.id };
  const deniedResult = await operator.ctx.remote.reactivePrograms.actionResult(address);
  assert.equal(deniedResult.ok, false); assert.equal(deniedResult.error.code, 'reactive/invocation-unavailable');
  await assert.rejects(human.invocation('accept', invocation.id), error => error.code === 'reactive/invocation-unavailable');
  const deniedActivity = operator.ctx.remote.reactivePrograms.observeAction(address, t.signal);
  try { await assert.rejects(deniedActivity[Symbol.asyncIterator]().next(), error => error.code === 'reactive/invocation-unavailable'); }
  finally { await deniedActivity.dispose(); }
  const raw = worker.ctx.remote.reactivePrograms;
  const qualifiedAddress = { ...sessionAddress, accessId: session.identity.accessId };
  for (const invoke of [() => raw.pause(qualifiedAddress), () => raw.cancel(qualifiedAddress),
    () => raw.resumeGoal({ ...qualifiedAddress, expected: { id: 'service-forged-goal', revision: 1 } }),
    () => raw.answerQuestion({ ...qualifiedAddress, callId: 'absent', answer: { answers: [] } })]) {
    const result = await invoke(); assert.equal(result.ok, false); assert.equal(result.error.code, 'reactive/human-participant-required');
  }
  const deniedWait = raw.attachWait({ ...qualifiedAddress, callId: 'absent' }, t.signal);
  try { await assert.rejects(deniedWait[Symbol.asyncIterator]().next(), error => error.code === 'reactive/human-participant-required'); }
  finally { await deniedWait.dispose(); }

  const wrong = await createProgramClient(program, transport('worker').options); clients.push(wrong);
  const mismatchId = crypto.randomUUID(); visible.add(mismatchId);
  let mismatch;
  await assert.rejects(wrong.create('work', { workspaceId: workspace.workspaceId, sessionId: mismatchId }), error => {
    assert.ok(error instanceof ProgramParticipantMismatchError); mismatch = error;
    assert.equal(error.expectedParticipant, 'human'); assert.equal(error.identity.participant, 'service');
    assert.equal(error.identity.sessionId, mismatchId); return true;
  });
  const recovered = await serviceClient.open(mismatch.identity.sessionId);
  assert.equal(recovered.id, mismatchId); assert.equal(recovered.identity.participant, 'service');
  assert.deepEqual((await raw.open({ ...sessionAddress, sessionId: mismatchId })).value, mismatch.identity, '错工厂没有删除或悄悄重建已确认的会话');

  const resource = session.viewResource('current'); subscriptions.push(resource.values$.subscribe());
  assert.equal(await resource.read(t.signal), 'original-facts');
  const invalidated = next(resource.observations$, value => value.status === 'pending' && value.generation > 0);
  worker.sockets[0].terminate(); await invalidated;
  const afterReconnect = next(resource.values$, value => value === 'after-connection-reset');
  facts.next(ready('after-connection-reset', 2));
  assert.equal(await afterReconnect, 'after-connection-reset');
  assert.equal(session.viewResource('current'), resource);
  assert.equal((await session.progress.read(t.signal)).activity.sessionId, sessionId);
  assert.deepEqual(await session.human.read(t.signal), await human.human.read(t.signal));
  assert.equal((await session.request(input.requestId).state.read(t.signal)).activity.state, 'accepted');
  assert.deepEqual(await (await session.invocation('accept', invocation.id)).result, { id: 'job-a', actorId });
  db.prepare('UPDATE work SET phase = ? WHERE id = ?').run('complete', 'job-a'); changed.next();
  assert.equal(await business.settled(), 'complete');
  assert.equal(db.prepare('SELECT count(*) AS count FROM work').get().count, 1);
  assert.equal(actionSources.length, 1, '读取和物理断线恢复不重新受理Action');
  for (const endpoint of ['submit', 'editQueuedInput', 'uploadFile', 'startAction', 'locateOperation']) assert.ok(worker.forged.has(endpoint));
  await journal.dispose();

  const qualifiedJournal = session.journal, qualificationController = new AbortController();
  const qualifiedFollowing = qualifiedJournal.follow({}, { signal: qualificationController.signal })[Symbol.asyncIterator]();
  journalReaders.push({ controller: qualificationController, iterator: qualifiedFollowing });
  const qualificationOpening = await qualifiedFollowing.next();
  assert.equal(qualificationOpening.value.type, 'opened');
  const revokedGate = { entered: Promise.withResolvers(), release: Promise.withResolvers(), returned: Promise.withResolvers() };
  journalGates.push(revokedGate); pageGate = revokedGate;
  const latePage = qualifiedJournal.page({ throughSeq: qualificationOpening.value.cursor })
    .then(value => ({ value }), error => ({ error }));
  const heldPage = await revokedGate.entered.promise;
  await local.submit({ requestId: 'journal-before-access-exit', content: '退出前已经持久追加的资料', scope: null,
    intent: 'question', delivery: 'inject' });
  const delivered = await qualifiedFollowing.next();
  assert.equal(delivered.value.type, 'entry');
  await local.submit({ requestId: 'journal-buffered-before-access-exit', content: '旧帧流尚未消费的后续资料', scope: null,
    intent: 'question', delivery: 'inject' });
  assert.ok(agent.session.seq - 1 > delivered.value.entry.event.seq, '原生日志已有尚未交付的后续事件');
  const registered = runtime.native.workspaceRegistry.get(WorkspaceId(workspace.workspaceId));
  await registered.detachSession(SessionId(sessionId));
  const revoked = error => error instanceof ConversationAccessRevokedError && error.reason === 'membership-removed';
  const rejectedPage = await firstValueFrom(from(latePage).pipe(timeout(3_000)));
  assert.ok(revoked(rejectedPage.error), '真实page仍在门闩中时，旧资格的等待已拒绝');
  assert.equal(heldPage.signal.aborted, true);
  await assert.rejects(qualifiedFollowing.next(), revoked, '原生日志已追加也不能向退出的旧帧流交付');
  revokedGate.release.resolve(); await revokedGate.returned.promise; await checkpoint();
  await assert.rejects(async () => qualifiedJournal.page({ throughSeq: qualificationOpening.value.cursor }), revoked,
    '迟到真实回执后，缓存的共同journal端口仍不能沿旧资格继续读页');
  assert.equal(runtime.native.agents.get(SessionId(sessionId)), agent, '读取资格退出不销毁已运行的原生Agent');
  assert.equal(models, 0); assert.deepEqual(errors, []);
});
