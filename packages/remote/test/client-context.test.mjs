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
import * as Registry from '@deepseek-ai/dsh-typert-registry/client';
import * as Gateway from '@deepseek-ai/dsh-api-gateway/client';
import { installConnection } from '@deepseek-ai/dsh-client-connection/client';
import { defineAction, implementAction, defineApplication, defineEnvironment, defineProgram, defineSurface, resourceView, z } from '@hanger-source/dsh-reactive';
import { Expose, implementProgram, useAction } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import { assertProgramSessionConnection, bindProgramClient, createAuthenticatedTransport, createProgramClient } from '@hanger-source/dsh-reactive-client';
import contribution from '@hanger-source/dsh-reactive-client/remote';
import { mountProgramGateway, RemoteAuthenticationError } from '@hanger-source/dsh-reactive-remote';

const next = (source, predicate) => firstValueFrom(source.pipe(filter(predicate), timeout(8_000)));
const ready = value => ({ status: 'ready', generation: 1, observedAt: Date.now(), value });

test('同ID真实Host按Connection绑定；借用SDK退出保留公共上下文，Context退出关闭其SDK而不停止Host', { timeout: 25_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-client-context-'));
  const sessionId = crypto.randomUUID(), errors = [], calls = [], runtimes = [], gateways = [], contexts = [], clients = [], local = [];
  let models = 0;
  const mark = defineAction({ name: 'mark_context_owner', description: '只在当前实际宿主记录一次明确调用', scope: z.null(), input: z.string(), output: z.string() });
  const surface = defineSurface({ actions: { mark }, views: { owner: resourceView(z.string()) } });
  const program = defineProgram({ input: z.null(), surface });
  t.after(async () => {
    await Promise.all(clients.map(client => client.dispose()));
    await Promise.all(contexts.map(ctx => ctx.fiber.dispose()));
    for (const session of local) session.release();
    await Promise.all(gateways.map(gateway => gateway.close()));
    await Promise.all(runtimes.map(runtime => runtime.dispose()));
  });
  const host = async label => {
    const current = new BehaviorSubject(ready(label));
    const action = implementAction(mark, { execute(input, call) {
      assert.equal(call.source.actorId, 'context-operator');
      calls.push({ label, input }); return label + ':' + input;
    } });
    const application = defineApplication({ name: 'context-' + label, admit: async () => {}, environments: [defineEnvironment({
      id: 'work', instructions: '读取和SDK调用不启动模型', program: implementProgram(program, { Component() {
        const bound = useAction(action, () => null, []);
        return h(Expose, { surface, value: { views: { owner: current }, actions: { mark: bound } } });
      } }),
    })] });
    const runtime = await createRuntime(() => application, { home: join(root, label, 'home'), actorId: 'context-operator',
      onError: event => errors.push(event.error), model: nativeModel({ provider: 'client-context', model: 'unused', create: () => new class extends LlmAdapter {
        async *stream() { models++; throw new Error('CONTEXT_READING_MUST_NOT_START_MODEL'); }
      }() }),
    }); runtimes.push(runtime);
    const directory = join(root, label, 'workspace'); await mkdir(directory, { recursive: true });
    const workspace = await runtime.application.createWorkspace({ directory });
    local.push(await runtime.application.create('work', { workspaceId: workspace.workspaceId, sessionId }));
    const role = { name: 'context-user', resource: 'session', actions: ['reactivePrograms/open', 'reactivePrograms/questions',
      'reactivePrograms/view', 'reactivePrograms/resolveAction', 'reactivePrograms/startAction', 'reactivePrograms/actionResult', 'reactivePrograms/observeAction'] };
    const gateway = await mountProgramGateway(runtime.native, { application: runtime.application,
      model: { provider: 'client-context', model: 'unused' }, actor: principal => principal.id,
      access: service => service.registerPolicy('context-user', { bindings: () => [{ role, resource: { kind: 'session', id: sessionId } }] }),
      carrier: { onError: error => errors.push(error), async authenticate(request) {
        if (request.headers.get('authorization') !== 'Bearer context-test') throw new RemoteAuthenticationError(401);
        return { kind: 'context-user', id: 'context-operator' };
      } },
    }); gateways.push(gateway);
    return { gateway, current };
  };
  const transport = baseUrl => ({ baseUrl,
    fetch: (url, init) => fetch(url, { ...init, headers: { ...Object.fromEntries(new Headers(init?.headers)), authorization: 'Bearer context-test' } }),
    createWebSocket: url => new WebSocket(url, { headers: { authorization: 'Bearer context-test' } }),
  });
  const connect = async baseUrl => {
    const ctx = new Context(); contexts.push(ctx);
    await ctx.plugin(Registry);
    installConnection(ctx, { interactions: 'answer', transport: createAuthenticatedTransport(transport(baseUrl)) });
    await ctx.plugin(Gateway);
    await ctx.remote.$mount(contribution);
    return ctx;
  };
  const a = await host('a'), b = await host('b');
  const empty = new Context(); contexts.push(empty);
  assert.throws(() => bindProgramClient(program, empty), /REACTIVE_PROGRAM_CLIENT_CONTEXT_REQUIRED/u);
  const contextA = await connect(a.gateway.url), contextB = await connect(b.gateway.url), anotherA = await connect(a.gateway.url);
  const first = bindProgramClient(program, contextA), peer = bindProgramClient(program, contextA);
  const second = bindProgramClient(program, contextB), sameAddress = bindProgramClient(program, anotherA);
  clients.push(first, peer, second, sameAddress);
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
  assert.equal((await contextA.remote.reactivePrograms.open({ sessionId })).ok, true, 'SDK退出不撤销公共远程合同');
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
  assert.deepEqual(calls, [{ label: 'a', input: 'first' }, { label: 'b', input: 'still-b' }]);
  assert.equal(models, 0); assert.deepEqual(errors, []);
  console.info('client-context.verified', { sameSessionId: sessionId, hosts: 2, sharedContextClients: 2, models });
});
