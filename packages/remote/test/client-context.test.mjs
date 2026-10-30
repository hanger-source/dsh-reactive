import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement as h } from 'react';
import { BehaviorSubject, filter, firstValueFrom, timeout } from 'rxjs';
import WebSocket from 'ws';
import { Context } from '@deepseek-ai/cordis';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { SessionId } from '@deepseek-ai/dsh-session';
import * as Registry from '@deepseek-ai/dsh-typert-registry/client';
import * as Gateway from '@deepseek-ai/dsh-api-gateway/client';
import { installConnection } from '@deepseek-ai/dsh-client-connection/client';
import { defineAction, implementAction, defineApplication, defineEnvironment, defineProgram, defineSurface, programIdentity, ProgramContractMismatchError, SessionProgramMismatchError, SharedProgramRequiredError, resourceView, z } from '@hanger-source/dsh-reactive';
import { Expose, implementProgram, useAction } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import { assertProgramSessionConnection, bindProgramClient, createAuthenticatedTransport, createProgramClient, CreationUnknownError,
  SubmissionAdmissionUnknownError, InputEditAdmissionUnknownError, FileUploadReceiptUnknownError } from '@hanger-source/dsh-reactive-client';
import contribution from '@hanger-source/dsh-reactive-client/remote';
import { mountProgramGateway, RemoteAuthenticationError } from '@hanger-source/dsh-reactive-remote';

const next = (source, predicate) => firstValueFrom(source.pipe(filter(predicate), timeout(8_000)));
const ready = value => ({ status: 'ready', generation: 1, observedAt: Date.now(), value });

test('同ID真实Host按Connection绑定；借用SDK退出保留公共上下文，Context退出关闭其SDK而不停止Host', { timeout: 40_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-client-context-'));
  const sessionId = crypto.randomUUID(), errors = [], calls = [], runtimes = [], gateways = [], contexts = [], clients = [], local = [];
  let models = 0;
  const mark = defineAction({ name: 'mark_context_owner', description: '只在当前实际宿主记录一次明确调用', scope: z.null(), input: z.string(), output: z.string() });
  const surface = defineSurface({ actions: { mark }, views: { owner: resourceView(z.string()) } });
  const program = defineProgram({ name: 'client_context_test', input: z.null(), surface });
  t.after(async () => {
    await Promise.all(clients.map(client => client.dispose()));
    await Promise.all(contexts.map(ctx => ctx.fiber.dispose()));
    for (const session of local) session.release();
    await Promise.all(gateways.map(gateway => gateway.close()));
    await Promise.all(runtimes.map(runtime => runtime.dispose()));
  });
  const host = async (label, selectedProgram = program, recordedWorkspace) => {
    const current = new BehaviorSubject(ready(label));
    const visible = new Set([sessionId]);
    const action = implementAction(mark, { execute(input, call) {
      assert.equal(call.source.actorId, 'context-operator');
      calls.push({ label, input }); return label + ':' + input;
    } });
    const application = defineApplication({ name: 'context-' + label, admit: async () => {}, environments: [defineEnvironment({
      id: 'work', instructions: '读取和SDK调用不启动模型', program: implementProgram(selectedProgram, { Component() {
        const bound = useAction(action, () => null, []);
        return h(Expose, { surface, value: { views: { owner: current }, actions: { mark: bound } } });
      } }),
    }), defineEnvironment({ id: 'local_only', instructions: '局部无名Program不提供远程会话',
      program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component: () => null }),
    })] });
    const runtime = await createRuntime(() => application, { home: join(root, label, 'home'), actorId: 'context-operator',
      onError: event => errors.push(event.error), model: nativeModel({ provider: 'client-context', model: 'unused', create: () => new class extends LlmAdapter {
        async *stream() { models++; throw new Error('CONTEXT_READING_MUST_NOT_START_MODEL'); }
      }() }),
    }); runtimes.push(runtime);
    const directory = join(root, label, 'workspace'); await mkdir(directory, { recursive: true });
    const workspace = recordedWorkspace ?? await runtime.application.createWorkspace({ directory });
    const held = recordedWorkspace ? undefined : await runtime.application.create('work', { workspaceId: workspace.workspaceId, sessionId });
    if (held) local.push(held);
    const role = { name: 'context-user', resource: 'session', actions: ['reactivePrograms/open', 'reactivePrograms/resume', 'reactivePrograms/questions',
      'reactivePrograms/view', 'reactivePrograms/resolveAction', 'reactivePrograms/startAction', 'reactivePrograms/actionResult', 'reactivePrograms/observeAction',
      'reactivePrograms/requestStatus', 'reactivePrograms/pause', 'reactivePrograms/resumeGoal', 'reactivePrograms/cancel', 'reactivePrograms/submit',
      'reactivePrograms/editQueuedInput', 'reactivePrograms/uploadFile'] };
    const gateway = await mountProgramGateway(runtime.native, { application: runtime.application,
      model: { provider: 'client-context', model: 'unused' }, participant: principal => ({ participant: 'human', actorId: principal.id }),
      access: service => service.registerPolicy('context-user', { bindings: () => [
        ...[...visible].map(id => ({ role, resource: { kind: 'session', id } })),
        { role: { name: 'workspace-owner', resource: 'workspace', actions: ['reactivePrograms/create', 'reactivePrograms/completeCreation'] },
          resource: { kind: 'workspace', id: workspace.workspaceId } },
        { role: { name: 'workspace-registry', resource: 'global', actions: ['reactivePrograms/createWorkspace', 'reactivePrograms/describe'] }, resource: { kind: 'global' } },
      ] }),
      carrier: { onError: error => errors.push(error), async authenticate(request) {
        if (request.headers.get('authorization') !== 'Bearer context-test') throw new RemoteAuthenticationError(401);
        return { kind: 'context-user', id: 'context-operator' };
      } },
    }); gateways.push(gateway);
    return { gateway, current, workspace, runtime, visible, role, held };
  };
  const transport = baseUrl => ({ baseUrl,
    fetch: (url, init) => fetch(url, { ...init, headers: { ...Object.fromEntries(new Headers(init?.headers)), authorization: 'Bearer context-test' } }),
    createWebSocket: url => new WebSocket(url, { headers: { authorization: 'Bearer context-test' } }),
  });
  const connect = async (baseUrl, delayed, requests, wire) => {
    const ctx = new Context(); contexts.push(ctx);
    await ctx.plugin(Registry);
    const selected = transport(baseUrl), fetch = selected.fetch;
    if (delayed || requests || wire) selected.fetch = async (url, init) => {
      const pathname = new URL(url).pathname;
      requests?.push(pathname);
      const envelope = init?.body && JSON.parse(init.body), request = envelope?.payload?.args?.request;
      if (wire && request?.program) wire.requests.push({ pathname, request: structuredClone(request) });
      if (wire?.replaceProgram && request?.program) {
        request.program = wire.replaceProgram;
        init = { ...init, body: JSON.stringify(envelope) };
      }
      const response = await fetch(url, init);
      if (delayed && !delayed.entered && pathname.endsWith('/reactivePrograms/' + delayed.endpoint)) {
        delayed.entered = true;
        delayed.signal = init?.signal;
        const reply = await response.clone().json();
        assert.ok(response.ok, 'Host必须实际受理延迟回执的请求');
        // 无Goal时pause返回正式业务拒绝；失败回执同样不能越过已退出的等待owner。
        if (!delayed.allowRemoteFailure) assert.equal(reply.result.ok, true, JSON.stringify(reply));
        delayed.reply = reply.result;
        delayed.received.resolve(); await delayed.release.promise;
        if (delayed.transportError) throw delayed.transportError;
        if (delayed.rewriteProgram) {
          const envelope = await response.json();
          envelope.result.value.program = delayed.rewriteProgram;
          return new Response(JSON.stringify(envelope), { status: response.status, headers: { 'content-type': 'application/json' } });
        }
      }
      return response;
    };
    if (wire) {
      const openSocket = selected.createWebSocket;
      selected.createWebSocket = url => {
        const socket = openSocket(url), send = socket.send.bind(socket);
        socket.send = (data, ...args) => {
          const frame = JSON.parse(data);
          if (frame.type === 'open' && frame.endpoint === 'reactivePrograms/view') {
            wire.views.push(structuredClone(frame.payload.args.request));
            if (wire.replaceProgram) {
              frame.payload.args.request.program = wire.replaceProgram;
              return send(JSON.stringify(frame), ...args);
            }
          }
          return send(data, ...args);
        };
        return socket;
      };
    }
    const physical = createAuthenticatedTransport(selected);
    ctx.effect(() => () => physical.dispose(), 'authenticated transport');
    installConnection(ctx, { interactions: 'answer', transport: physical });
    await ctx.plugin(Gateway);
    await ctx.remote.$mount(contribution);
    return ctx;
  };
  const a = await host('a'), b = await host('b');
  const empty = new Context(); contexts.push(empty);
  await assert.rejects(bindProgramClient(program, empty), /REACTIVE_PROGRAM_CLIENT_CONTEXT_REQUIRED/u);
  const contextA = await connect(a.gateway.url), contextB = await connect(b.gateway.url), anotherA = await connect(a.gateway.url);
  const first = await bindProgramClient(program, contextA), peer = await bindProgramClient(program, contextA);
  const second = await bindProgramClient(program, contextB), sameAddress = await bindProgramClient(program, anotherA);
  clients.push(first, peer, second, sameAddress);
  let nested;
  await contextA.plugin({ name: 'program-sdk-app-consumer', async apply(child) {
    const client = await bindProgramClient(program, child); clients.push(client);
    nested = await client.open(sessionId);
  } });
  assert.equal(await nested.readView('owner'), 'a', '应用插件仍在LOADING时也能等待自己的SDK安装');
  await nested.release();
  const [sa, pa, sb, duplicate] = await Promise.all([first.open(sessionId), peer.open(sessionId), second.open(sessionId), sameAddress.open(sessionId)]);
  assert.equal(sa.id, sb.id); assert.equal(sa.definition, sb.definition);
  assert.equal(await sa.readView('owner'), 'a'); assert.equal(await sb.readView('owner'), 'b');
  assertProgramSessionConnection(sa, contextA.get('connection'));
  await contextA.plugin(child => { assertProgramSessionConnection(sa, child.get('connection')); });
  assert.throws(() => assertProgramSessionConnection(sa, contextB.get('connection')), /REACTIVE_REMOTE_SESSION_CONNECTION_MISMATCH/u);
  assert.throws(() => assertProgramSessionConnection(duplicate, contextA.get('connection')), /REACTIVE_REMOTE_SESSION_CONNECTION_MISMATCH/u);
  assert.throws(() => assertProgramSessionConnection({ ...sa }, contextA.get('connection')), /REACTIVE_REMOTE_SESSION_CONNECTION_MISMATCH/u);
  assert.deepEqual(calls, [], '身份核验没有调用业务或制造批准');
  assert.equal(await (await sa.resolveAction('mark', null))('first').then(invocation => invocation.result), 'a:first');
  await first.dispose();
  assert.throws(() => sa.viewResource('owner'), /REACTIVE_REMOTE_CLIENT_DISPOSED/u);
  assert.equal(await pa.readView('owner'), 'a');
  assert.equal((await contextA.remote.reactivePrograms.open({ sessionId, program: { name: program.name, version: program.version } })).ok, true, 'SDK退出不撤销公共远程合同');
  const value = pa.viewResource('owner'); await value.read();
  const invalidated = next(value.observations$, observation => observation.status === 'pending');
  a.current.next({ status: 'pending', generation: 2 }); await invalidated;
  const cancelled = assert.rejects(value.read(), /REACTIVE_REMOTE_CLIENT_DISPOSED/u);
  await contextA.fiber.dispose(); await cancelled;
  assert.throws(() => value.peek(), /REACTIVE_REMOTE_CLIENT_DISPOSED/u);
  assert.equal(await (await sb.resolveAction('mark', null))('still-b').then(invocation => invocation.result), 'b:still-b');
  a.current.next(ready('a-after-context-exit'));
  assert.equal(await duplicate.readView('owner'), 'a-after-context-exit');
  const owned = await createProgramClient(program, transport(a.gateway.url)); clients.push(owned);
  const ownedSession = await owned.open(sessionId);
  assert.equal(await ownedSession.readView('owner'), 'a-after-context-exit');
  assert.throws(() => assertProgramSessionConnection(ownedSession, anotherA.get('connection')), /REACTIVE_REMOTE_SESSION_CONNECTION_MISMATCH/u);
  await owned.dispose();
  assert.equal(await duplicate.readView('owner'), 'a-after-context-exit');

  await t.test('progress致命拒绝后reset保留原error，新open取得新的真实观察', async () => {
    const ctx = await connect(a.gateway.url), sdk = await bindProgramClient(program, ctx); clients.push(sdk);
    const session = await sdk.open(sessionId), progress = session.progress, request = session.request('not-submitted');
    let failure;
    await assert.rejects(progress.read(t.signal), error => { failure = error; return error.code === 'remote-access/denied'; });
    await assert.rejects(firstValueFrom(session.events$), error => error === failure);
    await assert.rejects(firstValueFrom(request.events$), error => error === failure);
    await assert.rejects(request.state.read(t.signal), error => error === failure);
    const states = [], subscription = progress.observations$.subscribe(observation => states.push(observation.status));
    const reset = Promise.withResolvers(), remove = ctx.on('connection/reset', () => reset.resolve());
    try {
      a.role.actions.push('reactivePrograms/progress');
      ctx.get('connection').reconnect(); await reset.promise;
      assert.equal(progress.peek().status, 'error'); assert.equal(progress.peek().error, failure);
      await assert.rejects(progress.read(t.signal), error => error === failure);
      await assert.rejects(firstValueFrom(session.events$), error => error === failure);
      await assert.rejects(request.state.read(t.signal), error => error === failure);
      assert.deepEqual(states, ['error'], '已经结束的consumer不能在reset时伪造pending');
      const fresh = await sdk.open(sessionId);
      assert.equal((await fresh.progress.read(t.signal)).activity.sessionId, sessionId);
      await fresh.release();
    } finally { remove(); subscription.unsubscribe(); }
  });

  await t.test('自建SDK等真实socket退出并交付物理清理失败，重入加入同一回执', async () => {
    const cleanupFailure = new Error('REAL_SOCKET_CLOSE_FAILED'), entered = Promise.withResolvers();
    const closes = [];
    let armed = false, firstFailure = true, finished = false;
    const sdk = await createProgramClient(program, { ...transport(a.gateway.url), createWebSocket(url) {
      const socket = new WebSocket(url, { headers: { authorization: 'Bearer context-test' } }), close = socket.close.bind(socket), send = socket.send.bind(socket);
      socket.send = (data, ...args) => {
        if (armed && JSON.parse(data).type === 'cancel') throw cleanupFailure;
        return send(data, ...args);
      };
      socket.close = (...args) => {
        if (!armed || socket.readyState !== WebSocket.OPEN) return close(...args);
        closes.push(() => close(...args)); entered.resolve();
        if (firstFailure) { firstFailure = false; throw cleanupFailure; }
      };
      return socket;
    } });
    t.after(async () => { armed = false; closes.forEach(close => close()); if (!finished) await sdk.dispose(); });
    const session = await sdk.open(sessionId), resource = session.viewResource('owner');
    let reentered;
    const observing = resource.observations$.subscribe({ complete() { reentered = sdk.dispose(); } });
    assert.equal(await resource.read(t.signal), 'a-after-context-exit');
    armed = true;
    const closing = sdk.dispose();
    assert.equal(reentered, closing);
    let settled = false;
    void closing.then(() => { settled = true; }, () => { settled = true; });
    await entered.promise; await new Promise(resolve => setImmediate(resolve));
    assert.equal(settled, false, 'SDK必须等物理close，不能把Fiber吞错当作完成');
    armed = false; closes.forEach(close => close());
    await assert.rejects(closing, error => error === cleanupFailure); finished = true;
    observing.unsubscribe();
    assert.equal(await duplicate.readView('owner'), 'a-after-context-exit', '物理客户端退出不停止真实Host');
  });
  const registration = join(root, 'registered-with-delayed-ack'); await mkdir(registration);
  const reference = { environmentId: 'work', workspaceId: a.workspace.workspaceId, sessionId: crypto.randomUUID() };
  a.visible.add(reference.sessionId);
  for (const [endpoint, invoke] of [
    ['open', sdk => sdk.open(sessionId)], ['resume', sdk => sdk.resume(sessionId)],
    ['completeCreation', sdk => sdk.completeCreation({ ...reference, sessionId })],
    ['createWorkspace', sdk => sdk.createWorkspace({ directory: registration })], ['describe', sdk => sdk.describe()],
    ['create', sdk => sdk.create(reference.environmentId, reference)],
  ]) await t.test(endpoint + '回执被延迟时SDK退出结束等待，借用Context继续可用', { timeout: 1_000 }, async subject => {
    const delayed = { endpoint, received: Promise.withResolvers(), release: Promise.withResolvers(), entered: false };
    subject.signal.addEventListener('abort', () => delayed.release.resolve(), { once: true });
    const ctx = await connect(a.gateway.url, delayed), pendingClient = await bindProgramClient(program, ctx);
    clients.push(pendingClient);
    const rejected = assert.rejects(invoke(pendingClient), error => {
      if (endpoint === 'create') {
        assert.ok(error instanceof CreationUnknownError); assert.deepEqual(error.reference, reference);
        assert.match(String(error.cause), /REACTIVE_REMOTE_CLIENT_DISPOSED/u);
      } else assert.match(String(error), /REACTIVE_REMOTE_CLIENT_DISPOSED/u);
      return true;
    });
    try {
      await delayed.received.promise;
      await pendingClient.dispose(); await rejected;
      const peer = await bindProgramClient(program, ctx); clients.push(peer);
      const live = await peer.open(sessionId); assert.equal(await live.readView('owner'), 'a-after-context-exit');
      if (endpoint === 'create') {
        const preserved = await peer.completeCreation(reference);
        assert.equal(preserved.id, reference.sessionId, '取消回执等待不删除已在Host登记的同一会话');
      }
    } finally { delayed.release.resolve(); }
  });
  for (const [endpoint, invoke] of [
    ['requestStatus', (session, options) => session.requestStatus('not-submitted', options)],
    ['pause', (session, options) => session.pause(options)], ['cancel', (session, options) => session.cancel(options)],
    ['resumeGoal', (session, options) => session.resumeGoal({ id: 'absent-goal', revision: 1 }, options)],
  ]) for (const owner of ['session', 'client', 'call']) await t.test(endpoint + '回执延迟时' + owner + '取消结束等待且公共Context保活', { timeout: 2_000 }, async subject => {
    const delayed = { endpoint, allowRemoteFailure: endpoint === 'pause' || endpoint === 'resumeGoal',
      received: Promise.withResolvers(), release: Promise.withResolvers(), entered: false };
    subject.signal.addEventListener('abort', () => delayed.release.resolve(), { once: true });
    const ctx = await connect(a.gateway.url, delayed), pendingClient = await bindProgramClient(program, ctx);
    clients.push(pendingClient);
    const session = await pendingClient.open(sessionId);
    const abort = new AbortController(), reason = new Error('REMOTE_CONTROL_RECEIPT_NOT_NEEDED');
    const rejected = assert.rejects(invoke(session, { signal: abort.signal }), error => {
      if (owner === 'call') assert.equal(error, reason);
      else assert.match(String(error), owner === 'session' ? /REACTIVE_CONVERSATION_HANDLE_RELEASED/u : /REACTIVE_REMOTE_CLIENT_DISPOSED/u);
      return true;
    });
    try {
      await delayed.received.promise;
      assert.equal(delayed.reply.ok, endpoint !== 'pause' && endpoint !== 'resumeGoal');
      if (owner === 'session') await session.release();
      else if (owner === 'client') await pendingClient.dispose();
      else abort.abort(reason);
      await rejected;
      const peer = await bindProgramClient(program, ctx); clients.push(peer);
      const live = owner === 'call' ? session : await peer.open(sessionId);
      assert.equal(await live.readView('owner'), 'a-after-context-exit');
      assert.deepEqual(await live.requestStatus('not-submitted'), { requestId: 'not-submitted', state: 'absent' });
    } finally { delayed.release.resolve(); }
  });
  await t.test('submit回执延迟时Session退出只退等，HTTP仍由SDK与caller持有', { timeout: 3_000 }, async subject => {
    const delayed = { endpoint: 'submit', received: Promise.withResolvers(), release: Promise.withResolvers(), entered: false };
    subject.signal.addEventListener('abort', () => delayed.release.resolve(), { once: true });
    const ctx = await connect(a.gateway.url, delayed), sdk = await bindProgramClient(program, ctx), peer = await bindProgramClient(program, ctx);
    clients.push(sdk, peer);
    const session = await sdk.open(sessionId), observer = await peer.open(sessionId);
    const input = { requestId: 'released-submit-ack', content: '只进入原生队列一次', intent: 'followup', delivery: 'inject', scope: null };
    const controller = new AbortController();
    const rejected = assert.rejects(session.submit(input, { signal: controller.signal }), error => {
      assert.ok(error instanceof SubmissionAdmissionUnknownError);
      assert.equal(error.sessionId, sessionId); assert.equal(error.requestId, input.requestId);
      assert.match(String(error.cause), /REACTIVE_CONVERSATION_HANDLE_RELEASED/u);
      return true;
    });
    try {
      await delayed.received.promise;
      const agent = a.runtime.native.agents.get(SessionId(sessionId)); assert.ok(agent);
      const queued = () => agent.inbox.nextStep.filter(message => message.source.rpcId === input.requestId);
      assert.equal(queued().length, 1);
      await session.release(); await rejected;
      assert.equal(delayed.signal.aborted, false, 'Session退出不能撤销原submit HTTP发布');
      const reopened = await sdk.open(sessionId);
      assert.equal(await reopened.readView('owner'), 'a-after-context-exit'); await reopened.release();
      assert.deepEqual(await observer.requestStatus(input.requestId), { requestId: input.requestId, state: 'accepted' });
      assert.equal(queued().length, 1, '取消回执等待不自动重发');
      controller.abort(new Error('SUBMIT_PUBLISHER_CALLER_EXIT'));
      assert.equal(delayed.signal.aborted, true, 'caller仍持有原HTTP取消权');
    } finally { delayed.release.resolve(); }
  });
  for (const cancelledBy of ['caller', 'session']) await t.test(`编辑已替换正文后${cancelledBy}退出等待，原队列与同Context消费者继续有效`, { timeout: 3_000 }, async subject => {
    const delayed = { endpoint: 'editQueuedInput', received: Promise.withResolvers(), release: Promise.withResolvers(), entered: false };
    subject.signal.addEventListener('abort', () => delayed.release.resolve(), { once: true });
    const ctx = await connect(a.gateway.url, delayed), sdk = await bindProgramClient(program, ctx);
    clients.push(sdk);
    const session = await sdk.open(sessionId);
    const input = { requestId: 'cancelled-edit-ack-' + cancelledBy, content: '替换前正文', intent: 'followup', delivery: 'inject', scope: null };
    const receipt = await session.submit(input), agent = a.runtime.native.agents.get(SessionId(sessionId)); assert.ok(agent);
    const queued = agent.inbox.nextStep.find(message => message.source.rpcId === input.requestId); assert.ok(queued);
    const content = [{ type: 'text', text: '正式准入后的替换正文' }];
    const controller = new AbortController(), reason = new Error('EDIT_ACK_NOT_NEEDED');
    const rejected = assert.rejects(session.editQueuedInput(input.requestId, content, { signal: controller.signal }), error => {
      assert.ok(error instanceof InputEditAdmissionUnknownError);
      assert.equal(error.sessionId, sessionId); assert.equal(error.requestId, input.requestId);
      if (cancelledBy === 'caller') assert.equal(error.cause, reason);
      else assert.match(String(error.cause), /REACTIVE_CONVERSATION_HANDLE_RELEASED/u);
      return true;
    });
    try {
      await delayed.received.promise;
      assert.deepEqual(delayed.reply.value, receipt);
      assert.deepEqual(agent.inbox.nextStep.find(message => message.id === queued.id), { ...queued, content });
      if (cancelledBy === 'caller') controller.abort(reason);
      else await session.release();
      await rejected;
      assert.equal(delayed.signal.aborted, cancelledBy === 'caller', '句柄只退等，显式caller仍持有HTTP取消权');
      const peer = await bindProgramClient(program, ctx); clients.push(peer);
      const observer = await peer.open(sessionId);
      assert.equal(await observer.readView('owner'), 'a-after-context-exit');
      assert.deepEqual(await observer.requestStatus(input.requestId), { requestId: input.requestId, state: 'accepted' });
      assert.deepEqual(agent.inbox.nextStep.filter(message => message.source.rpcId === input.requestId), [{ ...queued, content }]);
    } finally { delayed.release.resolve(); }
  });
  for (const cancelledBy of ['wait', 'caller', 'running', 'session', 'sdk', 'context', 'transport']) await t.test(`上传已存储而回执延迟时${cancelledBy}退出，同一调用保存回执或明确未知`, { timeout: 3_000 }, async subject => {
    const delayed = { endpoint: 'uploadFile', received: Promise.withResolvers(), release: Promise.withResolvers(), entered: false }, requests = [];
    if (cancelledBy === 'transport') delayed.transportError = new Error('UPLOAD_RESPONSE_CONNECTION_LOST');
    subject.signal.addEventListener('abort', () => delayed.release.resolve(), { once: true });
    const ctx = await connect(a.gateway.url, delayed, requests), sdk = await bindProgramClient(program, ctx), peer = await bindProgramClient(program, ctx);
    clients.push(sdk, peer);
    const session = await sdk.open(sessionId), observer = await peer.open(sessionId);
    const bytes = new TextEncoder().encode('消费者退出不能撤回已存储字节');
    const controller = new AbortController(), reason = new Error('UPLOAD_ACK_NOT_NEEDED');
    const call = session.uploadFile(bytes, { name: 'ack-delayed.txt',
      ...(cancelledBy === 'caller' || cancelledBy === 'running' ? { signal: controller.signal } : {}) });
    const rejected = assert.rejects(call.wait(cancelledBy === 'wait' || cancelledBy === 'transport' ? { signal: controller.signal } : undefined),
      error => {
        if (cancelledBy === 'wait' || cancelledBy === 'caller' || cancelledBy === 'running' || cancelledBy === 'transport') assert.equal(error, reason);
        else assert.match(String(error), cancelledBy === 'session' ? /REACTIVE_CONVERSATION_HANDLE_RELEASED/u : /REACTIVE_REMOTE_CLIENT_DISPOSED/u);
        return true;
      });
    const runningObserver = cancelledBy === 'running' ? call.activity$.subscribe(activity => {
      if (activity.state === 'running') controller.abort(reason);
    }) : undefined;
    try {
      await delayed.received.promise;
      const uploaded = delayed.reply.value, agent = a.runtime.native.agents.get(SessionId(sessionId)); assert.ok(agent);
      assert.ok(a.runtime.native.fileUploads.resolve(agent, uploaded.receiptId));
      assert.deepEqual(call.source, { kind: 'file-upload', sessionId });
      assert.equal((await call.activity.read()).state, 'running');
      if (cancelledBy === 'sdk') await sdk.dispose();
      else if (cancelledBy === 'context') await ctx.fiber.dispose();
      else if (cancelledBy === 'session') await session.release();
      else if (cancelledBy === 'running') assert.equal(controller.signal.aborted, true, 'running观察者已同步取消默认wait');
      else controller.abort(reason);
      await rejected;
      const transportEnded = cancelledBy === 'sdk' || cancelledBy === 'context';
      assert.equal(delayed.signal.aborted, transportEnded, 'caller与Session只退wait，SDK/Connection退出结束所属HTTP');
      assert.equal((await call.activity.read()).state, 'running', '退等本身不结算调用事实');
      delayed.release.resolve();
      if (transportEnded || cancelledBy === 'transport') {
        let failure;
        await assert.rejects(call.result, error => {
          failure = error;
          assert.ok(error instanceof FileUploadReceiptUnknownError);
          assert.equal(error.name, 'FileUploadReceiptUnknownError'); assert.equal(error.sessionId, sessionId);
          if (cancelledBy === 'transport') assert.equal(error.cause, delayed.transportError);
          else assert.ok(error.cause instanceof Error);
          return true;
        });
        assert.deepEqual(await firstValueFrom(call.activity$), { state: 'failed', phase: 'execute', error: failure });
      } else {
        assert.deepEqual(await call.result, uploaded);
        assert.deepEqual(await firstValueFrom(call.activity$), { state: 'returned' });
      }
      const verifier = cancelledBy === 'context' ? duplicate : observer;
      assert.equal(await verifier.readView('owner'), 'a-after-context-exit');
      const chunks = [];
      for await (const chunk of a.runtime.native.attachments.readFileStream(uploaded.file)) chunks.push(chunk);
      assert.deepEqual(Buffer.concat(chunks), Buffer.from(bytes));
      // 仅fixture在真实响应门闩处取得receipt；未知结果的SDK不能按call.id找回它。
      const input = { requestId: 'use-unobserved-upload-' + cancelledBy, content: [{ type: 'file', receiptId: uploaded.receiptId }],
        intent: 'followup', delivery: 'inject', scope: null };
      assert.deepEqual(await verifier.submit(input), { sessionId, requestId: input.requestId, accepted: true });
      assert.deepEqual(agent.inbox.nextStep.find(message => message.source.rpcId === input.requestId)?.content,
        [{ type: 'file', attachment: uploaded.file }]);
      assert.equal(requests.filter(path => path.endsWith('/reactivePrograms/uploadFile')).length, 1, '退出不自动重传上传');
    } finally { delayed.release.resolve(); runningObserver?.unsubscribe(); }
  });
  await t.test('Blob取消与转换失败不发HTTP，确定的Host拒绝与本地JSON错误保留原语义', { timeout: 3_000 }, async subject => {
    const requests = [], ctx = await connect(a.gateway.url, undefined, requests), sdk = await bindProgramClient(program, ctx);
    clients.push(sdk);
    const session = await sdk.open(sessionId), entered = Promise.withResolvers(), proceed = Promise.withResolvers(), converted = Promise.withResolvers();
    subject.signal.addEventListener('abort', () => proceed.resolve(), { once: true });
    const blob = new Blob(['转换结束后也不能再上传']), arrayBuffer = blob.arrayBuffer.bind(blob);
    blob.arrayBuffer = async () => {
      entered.resolve(); await proceed.promise;
      try { return await arrayBuffer(); } finally { converted.resolve(); }
    };
    const controller = new AbortController(), reason = new Error('BLOB_WAIT_NOT_NEEDED');
    const call = session.uploadFile(blob, { signal: controller.signal });
    const rejected = assert.rejects(call.wait(), error => error === reason);
    try {
      await entered.promise;
      controller.abort(reason); await rejected;
      await assert.rejects(call.result, error => error === reason);
      assert.deepEqual(await firstValueFrom(call.activity$), { state: 'failed', phase: 'input', error: reason });
      proceed.resolve(); await converted.promise;
      const failure = new Error('BLOB_CONVERSION_FAILED'), invalid = new Blob(['转换失败']);
      invalid.arrayBuffer = async () => { throw failure; };
      await assert.rejects(session.uploadFile(invalid).result, error => error === failure);
      const stopped = new AbortController(), stoppedReason = new Error('UPLOAD_PRECHECK_CANCELLED'); stopped.abort(stoppedReason);
      let conversions = 0;
      invalid.arrayBuffer = async () => { conversions++; return arrayBuffer(); };
      assert.throws(() => session.uploadFile(invalid, { signal: stopped.signal }), error => error === stoppedReason);
      assert.equal(conversions, 0);
      assert.equal(requests.filter(path => path.endsWith('/reactivePrograms/uploadFile')).length, 0);
      const submission = { requestId: 'local-json-rejected', content: '不能序列化的scope', intent: 'followup', delivery: 'inject', scope: () => null };
      await assert.rejects(session.submit(submission), error => error instanceof z.ZodError);
      assert.equal(requests.filter(path => path.endsWith('/reactivePrograms/submit')).length, 0);
      await assert.rejects(session.submit({ ...submission, requestId: 'missing-file-rejected', scope: null,
        content: [{ type: 'file', receiptId: 'never-staged' }] }), error =>
        error.code === 'session/attachment-invalid' && error.details.reason === 'FILE_NOT_STAGED');
      await assert.rejects(session.editQueuedInput('not-submitted', '不能替换不存在的输入'), error =>
        error.code === 'reactive/input-edit-rejected' && error.details.reason === 'unavailable');
      assert.equal(await session.readView('owner'), 'a-after-context-exit');
    } finally { proceed.resolve(); }
  });
  for (const endpoint of ['create', 'completeCreation']) await t.test(endpoint + '的成功ACK合同矛盾保留原恢复引用，显式完成不重复创建', { timeout: 3_000 }, async subject => {
    const reference = { environmentId: 'work', workspaceId: a.workspace.workspaceId, sessionId: crypto.randomUUID() };
    const expected = { ...reference };
    a.visible.add(expected.sessionId);
    if (endpoint === 'completeCreation') local.push(await a.runtime.application.create('work', { workspaceId: reference.workspaceId, sessionId: reference.sessionId }));
    const delayed = { endpoint, received: Promise.withResolvers(), release: Promise.withResolvers(), entered: false,
      rewriteProgram: { name: 'wrong_creation_ack', version: 1 } }, requests = [];
    subject.signal.addEventListener('abort', () => delayed.release.resolve(), { once: true });
    const ctx = await connect(a.gateway.url, delayed, requests), selected = await bindProgramClient(program, ctx); clients.push(selected);
    let unknown;
    const rejected = assert.rejects(endpoint === 'create' ? selected.create('work', reference) : selected.completeCreation(reference), error => {
      unknown = error;
      assert.ok(error instanceof CreationUnknownError); assert.deepEqual(error.reference, expected);
      assert.ok(error.cause instanceof ProgramContractMismatchError);
      return true;
    });
    try {
      await delayed.received.promise;
      const agent = a.runtime.native.agents.get(SessionId(expected.sessionId)); assert.ok(agent);
      reference.environmentId = 'caller_changed_environment'; reference.workspaceId = 'caller-changed-workspace'; reference.sessionId = crypto.randomUUID();
      delayed.release.resolve(); await rejected;
      const recovered = await selected.completeCreation(unknown.reference);
      assert.equal(recovered.id, expected.sessionId); assert.deepEqual(recovered.identity.program, programIdentity(program));
      assert.equal(a.runtime.native.agents.get(SessionId(expected.sessionId)), agent, '沿准确引用完成不构造第二个Agent');
      assert.equal(requests.filter(path => path.endsWith('/reactivePrograms/create')).length, endpoint === 'create' ? 1 : 0,
        '错误ACK不自动重发创建');
      assert.equal((await a.runtime.application.describe()).flatMap(workspace => workspace.sessionIds).filter(id => id === expected.sessionId).length, 1);
    } finally { delayed.release.resolve(); }
  });
  await t.test('wire兼容的View不能替代Program身份，错误创建与恢复均在激活前拒绝', { timeout: 5_000 }, async () => {
    const ctx = await connect(a.gateway.url), expected = programIdentity(program);
    const alternatives = [
      defineProgram({ name: 'other_context_program', input: program.input, surface }),
      defineProgram({ name: program.name, version: program.version + 1, input: program.input, surface }),
    ];
    await assert.rejects(bindProgramClient(defineProgram({ input: program.input, surface }), ctx), SharedProgramRequiredError);
    await a.runtime.application.close(sessionId);
    assert.equal(a.runtime.native.agents.get(SessionId(sessionId)), undefined);
    for (const alternative of alternatives) {
      const selected = await bindProgramClient(alternative, ctx); clients.push(selected);
      const mismatch = error => {
        assert.equal(error.code, 'reactive/program-contract-mismatch');
        assert.deepEqual(error.details, { expected: programIdentity(alternative), actual: expected });
        return true;
      };
      await assert.rejects(selected.open(sessionId), mismatch);
      await assert.rejects(selected.resume(sessionId), mismatch);
      await assert.rejects(selected.completeCreation({ environmentId: 'work', workspaceId: a.workspace.workspaceId, sessionId }), mismatch);
      assert.equal(a.runtime.native.agents.get(SessionId(sessionId)), undefined, '错Program不能恢复原Agent');
      const createdId = crypto.randomUUID();
      await assert.rejects(selected.create('work', { workspaceId: a.workspace.workspaceId, sessionId: createdId }), mismatch);
      assert.equal(a.runtime.native.agents.get(SessionId(createdId)), undefined);
      assert.equal(a.runtime.native.sessions.get(SessionId(createdId)), undefined);
      assert.equal((await a.runtime.application.describe()).some(workspace => workspace.sessionIds.includes(createdId)), false,
        '确定合同拒绝不产生新成员，也不误归创建未知');
    }
    const selected = await bindProgramClient(program, ctx); clients.push(selected);
    const localOnlyId = crypto.randomUUID();
    await assert.rejects(selected.create('local_only', { workspaceId: a.workspace.workspaceId, sessionId: localOnlyId }),
      error => error.code === 'reactive/shared-program-required');
    assert.equal(a.runtime.native.agents.get(SessionId(localOnlyId)), undefined);
    assert.equal((await a.runtime.application.describe()).some(workspace => workspace.sessionIds.includes(localOnlyId)), false);
    const resumed = await selected.resume(sessionId);
    assert.deepEqual(resumed.identity.program, expected);
    assert.equal(await resumed.readView('owner'), 'a-after-context-exit');
  });
  await t.test('已握手SDK的后续请求与每次物理View重连继续核原Program身份', { timeout: 8_000 }, async () => {
    const wire = { requests: [], views: [] }, ctx = await connect(a.gateway.url, undefined, undefined, wire);
    const selected = await bindProgramClient(program, ctx); clients.push(selected);
    const session = await selected.open(sessionId), expected = programIdentity(program);
    const view = session.viewResource('owner'), subscription = view.observations$.subscribe();
    try {
      assert.equal(await view.read(), 'a-after-context-exit');
      assert.deepEqual(await session.requestStatus('never-submitted'), { requestId: 'never-submitted', state: 'absent' });
      wire.replaceProgram = { name: 'other_context_program', version: 1 };
      await assert.rejects(session.requestStatus('never-submitted'), error => error.code === 'reactive/program-contract-mismatch');
      const denied = next(view.observations$, value => value.status === 'error');
      ctx.get('connection').reconnect();
      const failure = await denied;
      assert.equal(failure.error.code, 'reactive/program-contract-mismatch');
      assert.equal(view.peek().status, 'error', '错Program重连不能保留旧ready');
      assert.ok(wire.views.length >= 2, '真实Connection重连打开了新的View物理查询');
      assert.ok(wire.views.every(address => address.sessionId === sessionId));
      for (const address of wire.views) assert.deepEqual(address.program, expected, '每个SDK重连闭包仍携原Program身份');
      assert.ok(wire.requests.some(({ pathname }) => pathname.endsWith('/reactivePrograms/requestStatus')));
      for (const { request } of wire.requests) assert.deepEqual(request.program, expected);
    } finally { subscription.unsubscribe(); }
  });
  await t.test('同一持久Session重启后只更换Program名称或版本，新合同SDK仍不能重绑旧会话', { timeout: 15_000 }, async () => {
    const original = await host('persisted-program');
    const workspace = original.workspace;
    const reference = { sessionId, workspaceId: workspace.workspaceId, environmentId: 'work' };
    const stop = async current => {
      current.held?.release();
      await current.gateway.close(); gateways.splice(gateways.indexOf(current.gateway), 1);
      await current.runtime.dispose(); runtimes.splice(runtimes.indexOf(current.runtime), 1);
    };
    await stop(original);
    const alternatives = [
      defineProgram({ name: 'renamed_persisted_program', input: program.input, surface }),
      defineProgram({ name: program.name, version: program.version + 1, input: program.input, surface }),
    ];
    for (const alternative of alternatives) {
      const restarted = await host('persisted-program', alternative, workspace);
      const selected = await createProgramClient(alternative, transport(restarted.gateway.url)); clients.push(selected);
      try {
        const mismatch = error => {
          assert.ok(error instanceof SessionProgramMismatchError, '新SDK合同已匹配部署，拒绝来自原Session的持久身份');
          assert.equal(error.sessionId, sessionId);
          assert.deepEqual(error.recorded, programIdentity(program));
          assert.deepEqual(error.current, programIdentity(alternative));
          return true;
        };
        await assert.rejects(selected.open(sessionId), mismatch);
        await assert.rejects(selected.resume(sessionId), mismatch);
        await assert.rejects(selected.completeCreation(reference), mismatch);
        assert.equal(restarted.runtime.native.agents.get(SessionId(sessionId)), undefined, '持久身份拒绝不能先恢复Agent再报错');
        assert.equal(restarted.runtime.native.sessions.get(SessionId(sessionId)), undefined);
      } finally { await selected.dispose(); await stop(restarted); }
    }
    const restored = await host('persisted-program', program, workspace);
    const selected = await createProgramClient(program, transport(restored.gateway.url)); clients.push(selected);
    try {
      const recovered = await selected.open(sessionId);
      try {
        assert.equal(recovered.id, sessionId);
        assert.equal(await recovered.readView('owner'), 'persisted-program');
        assert.equal(restored.runtime.native.agents.get(SessionId(sessionId)), undefined, '恢复正确合同的冷读不启动Agent或模型');
      } finally { await recovered.release(); }
    } finally { await selected.dispose(); await stop(restored); }
  });
  assert.deepEqual(calls, [{ label: 'a', input: 'first' }, { label: 'b', input: 'still-b' }]);
  assert.equal(models, 0); assert.deepEqual(errors, []);
  console.info('client-context.verified', { sameSessionId: sessionId, hosts: 2, sharedContextClients: 2, models });
});
