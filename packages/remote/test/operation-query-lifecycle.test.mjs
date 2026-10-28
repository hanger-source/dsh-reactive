import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { useLayoutEffect } from 'react';
import { setImmediate as checkpoint } from 'node:timers/promises';
import { RemoteStream } from '@deepseek-ai/dsh-api-gateway/client';
import { BehaviorSubject, Observable, filter, firstValueFrom, from, mergeMap, timeout } from 'rxjs';
import WebSocket from 'ws';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { ConversationAccessRevokedError, defineAction, implementAction, defineApplication, defineEnvironment, defineProgram, defineSurface, defineOperation, implementOperation, resourceView, z } from '@hanger-source/dsh-reactive';
import { SessionId } from '@deepseek-ai/dsh-session';
import { WorkspaceId } from '@deepseek-ai/dsh-workspace';
import { Expose, implementProgram, useAction } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import { createAuthenticatedTransport, createProgramClient } from '@hanger-source/dsh-reactive-client';
import contribution from '@hanger-source/dsh-reactive-client/remote';
import { mountProgramGateway, RemoteAuthenticationError } from '@hanger-source/dsh-reactive-remote';

const next = (source, predicate) => firstValueFrom(source.pipe(filter(predicate), timeout(3_000)));

test('Operation查询按消费者共享连接，Session退出等待本次流退役且成员撤销不改变真实已受理工作', { timeout: 25_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-operation-lifecycle-'));
  const definition = defineOperation({ kind: 'query-lifecycle', version: 1,
    command: z.string(), reference: z.string(), state: z.object({ revision: z.number(), done: z.boolean() }),
    settled: state => state.done ? state : undefined,
  });
  const states = new BehaviorSubject({ status: 'ready', generation: 1, observedAt: Date.now(), value: { revision: 1, done: false } });
  const counts = new BehaviorSubject({ first: 0, peer: 0, invocation: 0 });
  const reconnect = { hold: false, entered: Promise.withResolvers(), release: Promise.withResolvers() };
  const writing = { hold: false, entered: Promise.withResolvers(), release: Promise.withResolvers() };
  const locating = { hold: false, entered: Promise.withResolvers(), release: Promise.withResolvers() };
  const persisted = join(root, 'accepted-operation.txt');
  const change = (reference, delta) => {
    counts.next({ ...counts.value, [reference]: counts.value[reference] + delta });
    console.debug('operation-query.owner', JSON.stringify({ reference, delta, counts: counts.value }));
  };
  const owner = implementOperation(definition, { async locate(reference) {
    if (reference === 'blocked' && locating.hold) { locating.entered.resolve(); await locating.release.promise; }
    return { status: 'accepted', reference, state: states.value.value };
  },
    observe: reference => new Observable(subscriber => {
      change(reference, 1);
      const waiting = reference === 'first' && reconnect.hold;
      const subscription = (waiting ? from(reconnect.release.promise).pipe(mergeMap(() => states)) : states).subscribe(subscriber);
      if (waiting) reconnect.entered.resolve();
      return () => { subscription.unsubscribe(); change(reference, -1); };
    }),
  });
  let bodies = 0, installations = 0;
  const start = defineAction({ name: 'start_operation_query', description: '明确受理一次业务操作', scope: z.null(),
    input: z.literal('invocation'), output: z.string(), operation: { definition, command: input => input, reference: receipt => receipt } });
  const implementation = implementAction(start, { operationOwner: owner, async execute(input) {
    bodies++;
    if (writing.hold) {
      writing.entered.resolve(); await writing.release.promise;
      await writeFile(persisted, input);
    }
    return input;
  } });
  const surface = defineSurface({ views: { current: resourceView(definition.state) }, actions: { start } });
  const program = defineProgram({ name: 'operation_query_lifecycle_test', input: z.null(), surface });
  const application = defineApplication({ name: 'operation-query-lifecycle', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '操作查询不执行模型或业务动作', program: implementProgram(program, { Component() {
      useLayoutEffect(() => { installations++; }, []);
      const bound = useAction(implementation, () => null, []);
      return Expose({ surface, value: { views: { current: states }, actions: { start: bound } } });
    } }),
  })] });
  const errors = [], clients = [], subscriptions = [], sockets = [];
  const calls = new BehaviorSubject({ observeOperation: 0, view: 0, questions: 0, progress: 0 });
  const cleanup = { entered: Promise.withResolvers(), release: Promise.withResolvers() };
  const invocationCleanup = { entered: Promise.withResolvers(), release: Promise.withResolvers() };
  const activityCleanup = { kind: 'iterator', entered: Promise.withResolvers(), release: Promise.withResolvers() };
  // 只延后捕获的真实公开RemoteStream.dispose；不修改只读Remote代理、帧或业务owner。
  const delays = new WeakMap(), iterate = RemoteStream.prototype[Symbol.asyncIterator], dispose = RemoteStream.prototype.dispose;
  let capture;
  RemoteStream.prototype[Symbol.asyncIterator] = function (...args) {
    const iterator = iterate.apply(this, args), delayed = capture;
    capture = undefined;
    if (delayed?.kind === 'dispose-owner') {
      delayed.disposal = delayed.close(); delayed.entered.resolve();
    } else if (delayed?.kind === 'iterator') {
      const close = iterator.return;
      assert.equal(typeof close, 'function');
      iterator.return = async (...args) => {
        const result = await close.apply(iterator, args);
        delayed.entered.resolve(); await delayed.release.promise; return result;
      };
    } else if (delayed) delays.set(this, delayed);
    return iterator;
  };
  RemoteStream.prototype.dispose = async function (...args) {
    await dispose.apply(this, args);
    const delayed = delays.get(this);
    if (delayed) {
      delayed.entered.resolve(); await delayed.release.promise;
      if (delayed.error) { delayed.failed.resolve(); throw delayed.error; }
    }
  };
  t.after(() => { RemoteStream.prototype[Symbol.asyncIterator] = iterate; RemoteStream.prototype.dispose = dispose; });
  let models = 0, gateway, local;
  const runtime = await createRuntime(() => application, { home: join(root, 'home'), actorId: 'reader', operations: [owner],
    onError: event => errors.push(event.error), model: nativeModel({ provider: 'operation-query', model: 'unused',
      create: () => new class extends LlmAdapter { async *stream() { models++; throw new Error('QUERY_MUST_NOT_START_MODEL'); } }(),
    }),
  });
  t.after(async () => {
    cleanup.release.resolve();
    invocationCleanup.release.resolve();
    activityCleanup.release.resolve();
    reconnect.release.resolve();
    writing.release.resolve(); locating.release.resolve();
    subscriptions.forEach(subscription => subscription.unsubscribe());
    await Promise.all(clients.map(client => client.dispose()));
    local?.release(); await gateway?.close(); await runtime.dispose(); states.complete(); counts.complete(); calls.complete();
  });
  const workspace = await runtime.application.createWorkspace({ directory: root });
  local = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
  gateway = await mountProgramGateway(runtime.native, { application: runtime.application,
    model: { provider: 'operation-query', model: 'unused' }, participant: principal => ({ participant: 'human', actorId: principal.id }),
    access: service => service.registerPolicy('reader', { bindings: () => [{ resource: { kind: 'session', id: local.id },
      role: { name: 'operation-reader', resource: 'session', actions: ['reactivePrograms/open', 'reactivePrograms/observeOperation', 'reactivePrograms/observeInvocationOperation',
        'reactivePrograms/resolveAction', 'reactivePrograms/startAction', 'reactivePrograms/actionResult', 'reactivePrograms/actionInvocation', 'reactivePrograms/observeAction',
        'reactivePrograms/view', 'reactivePrograms/questions', 'reactivePrograms/progress',
        'reactivePrograms/locateOperation', 'reactivePrograms/readOperationFeedback', 'reactivePrograms/journal', 'reactivePrograms/journalPage'] },
      condition(request) {
        const endpoint = request.action.slice('reactivePrograms/'.length);
        if (request.args && Object.hasOwn(calls.value, endpoint)) calls.next({ ...calls.value, [endpoint]: calls.value[endpoint] + 1 });
        return { kind: 'allow' };
      },
    }], explain: () => 'query-only' }),
    carrier: { onError: error => errors.push(error), async authenticate(request) {
      const authorization = request.headers.get('authorization');
      if (authorization !== 'Bearer reader' && authorization !== 'Bearer receipt-other') throw new RemoteAuthenticationError(401);
      return { kind: 'reader', id: authorization.slice('Bearer '.length) };
    } },
  });
  const connect = async () => {
    const headers = { authorization: 'Bearer reader' };
    const client = await createProgramClient(program, { baseUrl: gateway.url,
      fetch: (url, init) => fetch(url, { ...init, headers: { ...Object.fromEntries(new Headers(init?.headers)), ...headers } }),
      createWebSocket: url => { const socket = new WebSocket(url, { headers }); sockets.push(socket); return socket; },
    }); clients.push(client); return await client.open(local.id);
  };
  const first = await connect(), peer = await connect();
  const a = first.attachOperation(definition, 'first'), b = peer.attachOperation(definition, 'peer');
  assert.deepEqual(counts.value, { first: 0, peer: 0, invocation: 0 });
  const one = a.state.values$.subscribe(), two = a.state.values$.subscribe(), other = b.state.values$.subscribe();
  subscriptions.push(one, two, other);
  await Promise.all([a.state.read(t.signal), b.state.read(t.signal)]);
  const view = first.viewResource('current'), human = first.human, progress = first.progress;
  subscriptions.push(view.values$.subscribe(), human.values$.subscribe(), progress.values$.subscribe());
  await Promise.all([view.read(t.signal), human.read(t.signal), progress.read(t.signal)]);
  const beforeReset = calls.value;
  assert.deepEqual(counts.value, { first: 1, peer: 1, invocation: 0 });
  reconnect.hold = true;
  console.debug('operation-query.reconnecting', JSON.stringify({ sockets: sockets.map(socket => socket.readyState) }));
  const pending = next(a.observations$, value => value.status === 'pending' && value.generation > 0);
  sockets[0].terminate(); await pending;
  console.debug('operation-query.invalidated', JSON.stringify({ current: a.state.peek(), counts: counts.value }));
  await reconnect.entered.promise;
  assert.equal(a.state.peek().status, 'pending', '重连查询尚未给出事实时不能重放旧ready');
  await next(calls, value => ['view', 'questions', 'progress'].every(endpoint => value[endpoint] > beforeReset[endpoint]));
  await Promise.all([view.read(t.signal), human.read(t.signal), progress.read(t.signal)]);
  states.next({ status: 'ready', generation: 2, observedAt: Date.now(), value: { revision: 2, done: false } });
  reconnect.hold = false; reconnect.release.resolve();
  assert.deepEqual(await a.state.read(t.signal), { revision: 2, done: false }, '物理重连后读取新的开场切面');
  assert.deepEqual(await next(b.state.values$, value => value.revision === 2), { revision: 2, done: false });
  assert.deepEqual(counts.value, { first: 1, peer: 1, invocation: 0 });
  one.unsubscribe(); assert.equal(counts.value.first, 1);
  two.unsubscribe(); await next(counts, value => value.first === 0);
  assert.equal(a.state.peek().status, 'pending');
  capture = cleanup;
  const again = a.state.values$.subscribe(); subscriptions.push(again);
  await a.state.read(t.signal);
  const invoke = await first.resolveAction('start', null);
  capture = activityCleanup;
  const invocation = await invoke('invocation');
  const business = await invocation.operation;
  capture = invocationCleanup;
  const independent = business.state.values$.subscribe(); subscriptions.push(independent);
  await business.state.read(t.signal);
  let released = false;
  const releasing = first.release().then(() => { released = true; });
  assert.equal(await Promise.race([cleanup.entered.promise.then(() => 'cleaning'), releasing.then(() => 'released')]), 'cleaning');
  assert.equal(released, false, 'release须等待本次原生迭代器真正完成异步清理');
  assert.equal(counts.value.invocation, 1, 'Session退出不撤销已受理Invocation的Operation观察');
  let disposed = false;
  const disposing = clients[0].dispose().then(() => { disposed = true; });
  assert.equal(await Promise.race([invocationCleanup.entered.promise.then(() => 'cleaning'), disposing.then(() => 'disposed')]), 'cleaning');
  cleanup.release.resolve(); await releasing;
  assert.equal(disposed, false, '并发SDK退出还须等待Invocation查询自己的清理');
  invocationCleanup.release.resolve();
  await activityCleanup.entered.promise; await checkpoint();
  assert.equal(disposed, false, 'SDK还须等待原Invocation活动流退出，不能只清理其Operation');
  activityCleanup.release.resolve(); await disposing;
  await next(counts, value => value.first === 0 && value.invocation === 0);
  assert.deepEqual(counts.value, { first: 0, peer: 1, invocation: 0 }, 'SDK退出清理两类查询，其他SDK仍持有原来源');
  states.next({ status: 'ready', generation: 3, observedAt: Date.now(), value: { revision: 2, done: true } });
  assert.deepEqual(await b.settled({ signal: t.signal }), { revision: 2, done: true });
  await peer.release(); await next(counts, value => value.peer === 0);
  assert.deepEqual(counts.value, { first: 0, peer: 0, invocation: 0 });
  assert.deepEqual(await (await local.attachOperation(definition, 'first')).state.read(t.signal), { revision: 2, done: true });

  // View已经开始手动退休，人工问题查询仍停在异步清理；一个失败不能提前结束整个owner。
  const exitFailure = new Error('EXPECTED_VIEW_CLEANUP_FAILURE');
  const failedCleanup = { entered: Promise.withResolvers(), release: Promise.withResolvers(), failed: Promise.withResolvers(), error: exitFailure };
  const heldCleanup = { entered: Promise.withResolvers(), release: Promise.withResolvers() };
  const failing = await connect(), failingClient = clients.pop();
  t.after(async () => {
    failedCleanup.release.resolve(); heldCleanup.release.resolve();
    await assert.rejects(failingClient.dispose(), error => error === exitFailure);
  });
  capture = failedCleanup;
  const failingView = failing.viewResource('current');
  const viewing = failingView.values$.subscribe();
  await failingView.read(t.signal);
  viewing.unsubscribe(); await failedCleanup.entered.promise;
  capture = heldCleanup;
  subscriptions.push(failing.human.values$.subscribe());
  await failing.human.read(t.signal);
  let releasedWithFailure = false, disposedWithFailure = false;
  const failedRelease = failing.release();
  void failedRelease.then(() => { releasedWithFailure = true; }, () => { releasedWithFailure = true; });
  const failedDispose = failingClient.dispose();
  void failedDispose.then(() => { disposedWithFailure = true; }, () => { disposedWithFailure = true; });
  await heldCleanup.entered.promise;
  failedCleanup.release.resolve(); await failedCleanup.failed.promise; await checkpoint();
  assert.equal(releasedWithFailure, false, '退出错误要等另一能力的实际清理，不能提前拒绝');
  assert.equal(disposedWithFailure, false, 'SDK加入同一个未完成的Session退出');
  heldCleanup.release.resolve();
  await assert.rejects(failedRelease, error => error === exitFailure);
  await assert.rejects(failedDispose, error => error === exitFailure);
  assert.deepEqual(await (await local.attachOperation(definition, 'first')).state.read(t.signal), { revision: 2, done: true });

  // ACK已到达，在真实观察流的构造中同步退出；保留原Invocation身份供另一SDK恢复。
  const interrupted = await connect(), interruptedClient = clients.at(-1);
  const acceptedAction = await interrupted.resolveAction('start', null);
  const reentrant = { kind: 'dispose-owner', entered: Promise.withResolvers(), close: () => interruptedClient.dispose(), disposal: undefined };
  capture = reentrant;
  const accepted = await acceptedAction('invocation');
  await reentrant.entered.promise; await reentrant.disposal;
  assert.equal(typeof accepted.id, 'string');
  await assert.rejects(accepted.result, /REACTIVE_REMOTE_CLIENT_DISPOSED/u);
  const restoring = await connect();
  const restored = await restoring.invocation('start', accepted.id);
  assert.equal(await restored.result, 'invocation');
  assert.equal(restored.id, accepted.id);
  const originalReceipt = await restored.operation;
  subscriptions.push(originalReceipt.observations$.subscribe());
  await originalReceipt.state.read(t.signal);
  await restoring.release();
  assert.equal(bodies, 2, '只有两次明确Action受理；观察、恢复和退出均不重新执行业务');

  // 各读取入口使用独立SDK，不能靠另一个View先报错而掩盖该endpoint未撤资格。
  const qualifications = [];
  for (const [name, select] of [
    ['view', session => session.viewResource('current')],
    ['manifest', session => session.actionManifest],
    ['human', session => session.human],
    ['progress', session => session.progress],
    ['request', session => session.request('original-request').state],
    ['operation', session => session.attachOperation(definition, 'first').state],
  ]) {
    const session = await connect(), resource = select(session);
    subscriptions.push(resource.observations$.subscribe());
    await resource.read(t.signal);
    qualifications.push({ name, session, resource,
      revoked: next(resource.observations$, observation => observation.status === 'error') });
  }
  const journalSession = await connect(), journalFailed = Promise.withResolvers();
  const journal = await journalSession.openEvents({ publish() {}, failed: journalFailed.resolve });
  const silent = await connect(), silentAccess = silent.identity.accessId;
  const silentLocation = await connect(), silentAttachment = await connect(), silentFeedback = await connect();
  const blocked = await connect(); locating.hold = true;
  const lookup = blocked.locateOperation(definition, 'blocked');
  const lookupRejected = assert.rejects(lookup, ConversationAccessRevokedError);
  await locating.entered.promise;
  const acceptedSession = await connect(), activeAction = await acceptedSession.resolveAction('start', null);
  const acceptedManifest = acceptedSession.actionManifest;
  subscriptions.push(acceptedManifest.observations$.subscribe());
  await acceptedManifest.read(t.signal);
  const acceptedRevoked = next(acceptedManifest.observations$, observation => observation.status === 'error');
  writing.hold = true;
  const acceptedWrite = await activeAction('invocation'); await writing.entered.promise;
  const originalAgent = runtime.native.agents.get(SessionId(local.id));
  const registered = runtime.native.workspaceRegistry.get(WorkspaceId(workspace.workspaceId));
  await registered.detachSession(SessionId(local.id));
  const observations = await Promise.all(qualifications.map(async entry => [entry, await entry.revoked]));
  for (const [entry, observation] of observations) {
    assert.ok(observation.error instanceof ConversationAccessRevokedError, entry.name);
    assert.equal(observation.error.reason, 'membership-removed');
    assert.equal(observation.error.sessionId, local.id);
    assert.equal(observation.error.workspaceId, workspace.workspaceId);
    assert.throws(() => entry.resource.peek(), error => error === observation.error, entry.name);
  }
  assert.ok(await journalFailed.promise instanceof ConversationAccessRevokedError);
  assert.ok((await acceptedRevoked).error instanceof ConversationAccessRevokedError);
  await lookupRejected;
  assert.equal(runtime.native.agents.get(SessionId(local.id)), originalAgent, '撤观察资格不退出实际Agent');
  const receiptUpdated = next(originalReceipt.observations$, observed => observed.status === 'ready' && observed.value.revision === 4);
  states.next({ status: 'ready', generation: 4, observedAt: Date.now(), value: { revision: 4, done: true } });
  assert.deepEqual((await receiptUpdated).value, { revision: 4, done: true }, '原Invocation派生Operation沿实际owner跨过detach，不新open会话');
  const descriptor = contribution.methods['reactivePrograms/observeInvocationOperation'];
  const receiptAddress = { sessionId: local.id, program: { name: program.name, version: program.version }, invocationId: restored.id,
    operation: { kind: definition.kind, version: definition.version }, reference: originalReceipt.reference };
  for (const [actor, request, code] of [
    ['reader', { ...receiptAddress, reference: 'peer' }, 'reactive/invocation-operation-unavailable'],
    ['reader', { ...receiptAddress, operation: { kind: 'another-operation', version: 1 } }, 'reactive/invocation-operation-unavailable'],
    ['reader', { ...receiptAddress, operation: { kind: definition.kind, version: 2 } }, 'reactive/invocation-operation-unavailable'],
    ['receipt-other', receiptAddress, 'reactive/invocation-unavailable'],
  ]) {
    const transport = createAuthenticatedTransport({ baseUrl: gateway.url,
      createWebSocket: url => new WebSocket(url, { headers: { authorization: 'Bearer ' + actor } }),
    });
    t.after(() => transport.dispose());
    const args = descriptor.input({ strict: true }).create().parse({ request });
    const stream = transport.openStream(descriptor.namespace + '/' + descriptor.method, { args }, t.signal);
    const iterator = stream[Symbol.asyncIterator]();
    try { await assert.rejects(iterator.next(), error => error.code === code); }
    finally { await iterator.return(); }
  }
  assert.deepEqual(await originalReceipt.state.read(t.signal), { revision: 4, done: true }, '错误回执地址不改变原观察或外层SDK');
  await registered.attachSession(SessionId(local.id));
  const beforeFreshOpen = installations;
  await assert.rejects(silent.readView('current'), error => error instanceof ConversationAccessRevokedError
    && error.reason === 'access-expired', '未订阅任何流的旧句柄也不能跨过未看见的detach/attach');
  const expired = error => error instanceof ConversationAccessRevokedError && error.reason === 'access-expired';
  await assert.rejects(silentLocation.locateOperation(definition, 'first'), expired);
  await assert.rejects(silentAttachment.attachOperation(definition, 'first').state.read(t.signal), expired);
  await assert.rejects(silentFeedback.readOperationFeedback(definition, 'first', 'original-participation'), expired);
  assert.equal(installations, beforeFreshOpen, '旧资格请求须在装配fresh Program之前拒绝');
  locating.hold = false; locating.release.resolve();
  for (const { session, resource } of qualifications) {
    assert.throws(() => resource.peek(), ConversationAccessRevokedError);
    await assert.rejects(async () => session.readView('current'), ConversationAccessRevokedError);
  }
  assert.throws(() => journal.restart(), ConversationAccessRevokedError);
  await assert.rejects(activeAction('invocation'), ConversationAccessRevokedError);
  const reopened = await connect();
  assert.notEqual(reopened.identity.accessId, silentAccess);
  assert.deepEqual(await reopened.readView('current'), { revision: 4, done: true });
  assert.equal(runtime.native.agents.get(SessionId(local.id)), originalAgent, '新open取得新读取资格而不重建resident Agent');
  writing.hold = false; writing.release.resolve();
  assert.equal(await acceptedWrite.result, 'invocation');
  assert.equal(await readFile(persisted, 'utf8'), 'invocation', '资格退出后原Invocation仍交付真实文件写回执');
  assert.equal(bodies, 3, '新open、失效观察和迟到查询都不重复已受理业务');
  await journal.dispose();
  assert.equal(models, 0); assert.deepEqual(errors, []);
});
