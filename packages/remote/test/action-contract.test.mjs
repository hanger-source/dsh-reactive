import assert from 'node:assert/strict';
import { test } from 'node:test';
import { access, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement as h } from 'react';
import WebSocket from 'ws';
import { Context } from '@deepseek-ai/cordis';
import * as Registry from '@deepseek-ai/dsh-typert-registry/client';
import * as Gateway from '@deepseek-ai/dsh-api-gateway/client';
import { installConnection } from '@deepseek-ai/dsh-client-connection/client';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { ActionContractMismatchError, actionIdentity, createWorkSelector, defineAction, implementAction, defineApplication,
  defineEnvironment, defineProgram, defineSurface, z } from '@hanger-source/dsh-reactive';
import { Expose, implementProgram, useAction } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import { ActionAdmissionUnknownError, bindProgramClient, createAuthenticatedTransport } from '@hanger-source/dsh-reactive-client';
import contribution from '@hanger-source/dsh-reactive-client/remote';
import { mountProgramGateway, RemoteAuthenticationError } from '@hanger-source/dsh-reactive-remote';

test('真实Gateway在副作用前核对Action合同，原Invocation只用受理版本解码且不重执行', { timeout: 15_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-action-contract-'));
  const file = join(root, 'accepted.txt'), errors = [], clients = [], contexts = [];
  let scopes = 0, inputs = 0, defaults = 0, bodies = 0, wrongDecodes = 0, models = 0;
  let corruptNextAck, corruptNextLookupId, loseNextAck = false, startRequests = 0;
  const lostResponse = new Error('HTTP_ACK_DELIVERY_LOST');
  const save = defineAction({ name: 'save_record', description: '写入正式文件',
    scope: z.string().transform(directory => { scopes++; return { directory }; }),
    input: z.object({ value: z.string().default(() => { defaults++; return '0'; }).transform(value => { inputs++; return Number(value); }) }),
    output: z.object({ stored: z.number() }),
  });
  const surface = defineSurface({ views: {}, actions: { save } });
  const program = defineProgram({ name: 'action_contract_test', input: z.null(), surface });
  const implementation = implementAction(save, { async execute(input, call) {
    assert.equal(call.source.actorId, 'contract-user');
    assert.deepEqual(call.scope, { directory: root });
    bodies++; await writeFile(file, String(input.value)); return { stored: input.value };
  } });
  const definition = defineApplication({ name: 'action-contract', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '显式SDK调用不启动模型', program: implementProgram(program, { Component() {
      const bound = useAction(implementation, () => root, []);
      return h(Expose, { surface, value: { views: {}, actions: { save: bound } } });
    } }),
  })] });
  let gateway, local;
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'contract-user',
    onError: event => errors.push(event.error),
    model: nativeModel({ provider: 'contract', model: 'unused', create: () => new class extends LlmAdapter {
      async *stream() { models++; throw new Error('CONTRACT_CHECK_MUST_NOT_START_MODEL'); }
    }() }),
  });
  t.after(async () => {
    await Promise.all(clients.map(client => client.dispose()));
    await Promise.all(contexts.map(ctx => ctx.fiber.dispose()));
    local?.release(); await gateway?.close(); await runtime.dispose();
  });
  const workspace = await runtime.application.createWorkspace({ directory: root });
  local = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
  gateway = await mountProgramGateway(runtime.native, { application: runtime.application,
    model: { provider: 'contract', model: 'unused' }, participant: principal => ({ participant: 'human', actorId: principal.id }),
    access: service => service.registerPolicy('contract-user', { bindings: () => [{
      resource: { kind: 'session', id: local.id }, role: { name: 'contract-user', resource: 'session', actions: [
        'reactivePrograms/open', 'reactivePrograms/resolveAction', 'reactivePrograms/startAction',
        'reactivePrograms/actionInvocation', 'reactivePrograms/actionResult', 'reactivePrograms/observeAction',
      ] },
    }], explain: () => 'contract-session-only' }),
    carrier: { onError: error => errors.push(error), async authenticate(request) {
      if (request.headers.get('authorization') !== 'Bearer contract-test') throw new RemoteAuthenticationError(401);
      return { kind: 'contract-user', id: 'contract-user' };
    } },
  });
  const ctx = new Context(); contexts.push(ctx);
  await ctx.plugin(Registry);
  const headers = { authorization: 'Bearer contract-test' };
  const transport = createAuthenticatedTransport({ baseUrl: gateway.url,
    async fetch(url, init) {
      const starting = new URL(url).pathname.endsWith('/reactivePrograms/startAction');
      if (starting) startRequests++;
      const response = await fetch(url, { ...init, headers: { ...Object.fromEntries(new Headers(init?.headers)), ...headers } });
      if (starting && loseNextAck) {
        loseNextAck = false;
        assert.equal(response.status, 200);
        // HTTP 已真实返回；消费并丢弃响应后才模拟交付失败，不读取或私藏 Invocation 身份。
        await response.arrayBuffer();
        throw lostResponse;
      }
      if (starting && corruptNextAck) {
        const corruption = corruptNextAck;
        corruptNextAck = undefined;
        // 只损坏已经真实受理后的回执交付，不替换Host、业务执行或Gateway协议实现。
        const payload = await response.json(); assert.equal(payload.result.ok, true);
        if (corruption === 'contract') payload.result.value.origin.action.version = 2;
        if (corruption === 'id') payload.result.value.id = globalThis.crypto.randomUUID();
        if (corruption === 'session') payload.result.value.origin.sessionId = globalThis.crypto.randomUUID();
        if (corruption === 'binding') payload.result.value.origin.bindingId = globalThis.crypto.randomUUID();
        if (corruption === 'scope') payload.result.value.origin.scope = 'another-target';
        return new Response(JSON.stringify(payload), { status: response.status, headers: response.headers });
      }
      if (corruptNextLookupId && new URL(url).pathname.endsWith('/reactivePrograms/actionInvocation')) {
        const payload = await response.json(); assert.equal(payload.result.ok, true);
        // 用另一项已存在的真实 Invocation 制造错位，避免随机不存在身份掩盖串读结果。
        payload.result.value.id = corruptNextLookupId;
        corruptNextLookupId = undefined;
        return new Response(JSON.stringify(payload), { status: response.status, headers: response.headers });
      }
      return response;
    },
    createWebSocket: url => new WebSocket(url, { headers }),
  });
  ctx.effect(() => () => transport.dispose(), 'authenticated transport');
  installConnection(ctx, { interactions: 'answer', transport });
  await ctx.plugin(Gateway); await ctx.remote.$mount(contribution);
  const open = async contract => {
    const selected = defineProgram({ name: 'action_contract_test', input: z.null(), surface: defineSurface({ views: {}, actions: { save: contract } }) });
    const client = await bindProgramClient(selected, ctx); clients.push(client);
    return await client.open(local.id);
  };
  const wrongOutput = z.string().transform(value => { wrongDecodes++; return value; });
  const wrongName = defineAction({ name: 'other_record', description: '错误名称', scope: z.string(), input: save.input, output: wrongOutput });
  const wrongVersion = defineAction({ name: save.name, version: 2, description: '错误版本', scope: z.string(), input: save.input, output: wrongOutput });
  const renamed = await open(wrongName), newer = await open(wrongVersion), correct = await open(save);
  const before = { scopes, inputs, defaults, bodies };
  for (const [session, expected] of [[renamed, wrongName], [newer, wrongVersion]]) {
    await assert.rejects(session.resolveAction('save', root), error => {
      assert.ok(error instanceof ActionContractMismatchError);
      assert.deepEqual(error.expected, actionIdentity(expected));
      assert.deepEqual(error.actual, actionIdentity(save)); return true;
    });
  }
  assert.deepEqual({ scopes, inputs, defaults, bodies }, before, '拒绝合同不能解析目标、输入或触发默认值及副作用');
  await assert.rejects(access(file), { code: 'ENOENT' });
  const invoke = await correct.resolveAction('save', root);
  const beforeStart = { inputs, defaults, bodies };
  for (const contract of [actionIdentity(wrongName), actionIdentity(wrongVersion), undefined]) {
    const reply = await ctx.remote.reactivePrograms.startAction({ sessionId: local.id, program: { name: program.name, version: program.version }, accessId: correct.identity.accessId,
      invocationId: globalThis.crypto.randomUUID(), name: 'save',
      bindingId: invoke.bindingId, contract, input: { value: '9' } }, t.signal);
    assert.equal(reply.ok, false, '服务器独立拒绝错误或缺失合同，不能只信SDK检查');
    if (contract) assert.equal(reply.error.code, 'reactive/action-contract-mismatch');
  }
  assert.deepEqual({ inputs, defaults, bodies }, beforeStart); await assert.rejects(access(file), { code: 'ENOENT' });
  const invocation = await invoke({ value: '7' });
  assert.deepEqual(await invocation.result, { stored: 7 });
  assert.equal(await readFile(file, 'utf8'), '7'); assert.equal(bodies, 1); assert.equal(inputs, 1);
  const work = createWorkSelector({ identify(origin) {
    if (origin.kind !== 'invocation') return undefined;
    const target = origin.origin.scope;
    return target !== null && typeof target === 'object' && !Array.isArray(target) && typeof target.directory === 'string'
      ? target.directory : undefined;
  } });
  const beforeWork = { scopes, inputs, bodies };
  const observed = await work.observe(root, { invocations: [invocation] }, { signal: t.signal }).state.read();
  assert.equal(observed.invocations[0]?.id, invocation.id, '远程原始目标可直接归入独立业务观察');
  assert.deepEqual(observed.invocations[0]?.origin, invocation.origin);
  assert.deepEqual({ scopes, inputs, bodies }, beforeWork, 'Work观察不重复解析业务目标、输入或执行文件写入');
  const wrongProgram = { sessionId: local.id, program: { name: 'another_action_program', version: 1 }, invocationId: invocation.id };
  for (const query of ['actionInvocation', 'actionResult']) {
    const request = query === 'actionInvocation' ? { ...wrongProgram, name: 'save', contract: actionIdentity(save) } : wrongProgram;
    const reply = await ctx.remote.reactivePrograms[query](request, t.signal);
    assert.equal(reply.ok, false); assert.equal(reply.error.code, 'reactive/program-contract-mismatch');
  }
  const wrongObservation = ctx.remote.reactivePrograms.observeAction(wrongProgram, t.signal);
  try {
    await assert.rejects(wrongObservation[Symbol.asyncIterator]().next(), error => error.code === 'reactive/program-contract-mismatch');
  } finally { await wrongObservation.dispose(); }
  assert.deepEqual(await invocation.result, { stored: 7 }, '错Program查询不抹掉原Entry及真实业务结果');
  assert.equal(bodies, 1, '查询不会重新执行原Action');
  await assert.rejects(newer.invocation('save', invocation.id), error => error instanceof ActionContractMismatchError
    && error.expected.version === 2 && error.actual.version === 1);
  await assert.rejects(renamed.invocation('save', invocation.id), ActionContractMismatchError);
  // 另一份JS定义通过同一公开合同身份读取原回执，不拿对象地址作网络身份。
  const independent = defineAction({ name: save.name, version: 1, description: '独立客户端合同',
    scope: z.string(), input: z.object({ value: z.string() }), output: z.object({ stored: z.number() }) });
  const reader = await open(independent);
  assert.deepEqual(await (await reader.invocation('save', invocation.id)).result, { stored: 7 });
  assert.equal(bodies, 1); assert.equal(inputs, 1); assert.equal(defaults, 0); assert.equal(wrongDecodes, 0);
  for (const [corruption, value] of [['contract', '8'], ['id', '9'], ['session', '10'], ['binding', '13'], ['scope', '14']]) {
    const prior = { inputs, bodies, startRequests };
    corruptNextAck = corruption;
    let unknown;
    await assert.rejects(invoke({ value }), error => {
      assert.ok(error instanceof ActionAdmissionUnknownError);
      if (corruption === 'contract') assert.ok(error.cause instanceof ActionContractMismatchError);
      else {
        const field = { id: 'ID', session: 'SESSION', binding: 'BINDING', scope: 'TARGET' }[corruption];
        assert.equal(error.cause.message, `REACTIVE_ACTION_INVOCATION_${field}_MISMATCH`);
      }
      unknown = error; return true;
    });
    assert.deepEqual(await (await correct.invocation('save', unknown.invocationId)).result, { stored: Number(value) });
    assert.equal(await readFile(file, 'utf8'), value);
    assert.equal(startRequests, prior.startRequests + 1, '矛盾ACK不能触发自动重发');
    assert.equal(inputs, prior.inputs + 1); assert.equal(bodies, prior.bodies + 1);
  }
  const beforeLost = { inputs, bodies, startRequests };
  loseNextAck = true;
  let unknown;
  await assert.rejects(invoke({ value: '11' }), error => {
    assert.ok(error instanceof ActionAdmissionUnknownError);
    // 官方 Gateway 把 fetch 异常归为 RemoteError；SDK 保留此正式失败原因。
    assert.equal(error.cause.code, 'gateway/internal');
    assert.match(error.cause.message, /HTTP_ACK_DELIVERY_LOST/);
    unknown = error; return true;
  });
  const recovered = await correct.invocation('save', unknown.invocationId);
  assert.equal(recovered.id, unknown.invocationId);
  assert.deepEqual(await recovered.result, { stored: 11 });
  assert.equal(await readFile(file, 'utf8'), '11');
  assert.equal(startRequests, beforeLost.startRequests + 1, '丢失HTTP响应后SDK只分派一次');
  assert.equal(inputs, beforeLost.inputs + 1); assert.equal(bodies, beforeLost.bodies + 1, '查回结果不重复写文件');

  const beforeLookup = { inputs, bodies, startRequests };
  corruptNextLookupId = recovered.id;
  await assert.rejects(correct.invocation('save', invocation.id), error => {
    assert.ok(!(error instanceof ActionAdmissionUnknownError), '只读查询的错位不代表未知受理');
    assert.equal(error.message, 'REACTIVE_ACTION_INVOCATION_ID_MISMATCH'); return true;
  });
  assert.deepEqual(await (await correct.invocation('save', invocation.id)).result, { stored: 7 });
  assert.deepEqual(await recovered.result, { stored: 11 });
  assert.deepEqual({ inputs, bodies, startRequests }, beforeLookup, '拒绝错位及正常恢复都不能再分派或执行业务');
  assert.equal(await readFile(file, 'utf8'), '11');

  const request = { sessionId: local.id, program: { name: program.name, version: program.version },
    accessId: correct.identity.accessId, invocationId: globalThis.crypto.randomUUID(), name: 'save',
    bindingId: invoke.bindingId, contract: actionIdentity(save), input: { value: '12' } };
  const beforeReplay = { inputs, defaults, bodies };
  const replies = await Promise.all([request, structuredClone(request)].map(raw => ctx.remote.reactivePrograms.startAction(raw, t.signal)));
  for (const reply of replies) {
    assert.equal(reply.ok, true);
    assert.equal(reply.value.id, request.invocationId);
  }
  assert.deepEqual(await (await correct.invocation('save', request.invocationId)).result, { stored: 12 });
  assert.equal(inputs, beforeReplay.inputs + 1); assert.equal(defaults, beforeReplay.defaults);
  assert.equal(bodies, beforeReplay.bodies + 1, '并发重复原始请求只执行一次真实文件写入');
  const conflict = await ctx.remote.reactivePrograms.startAction({ ...request, input: { value: '13' } }, t.signal);
  assert.equal(conflict.ok, false); assert.equal(conflict.error.code, 'reactive/action-invocation-conflict');
  assert.equal(inputs, beforeReplay.inputs + 1); assert.equal(bodies, beforeReplay.bodies + 1);
  assert.equal(await readFile(file, 'utf8'), '12');
  assert.deepEqual(await (await correct.invocation('save', request.invocationId)).result, { stored: 12 });

  const beforeInvalid = { scopes, inputs, defaults, bodies };
  const { invocationId: omitted, ...missingId } = request;
  for (const raw of [missingId, { ...request, invocationId: JSON.stringify([local.id, 1]) }]) {
    const reply = await ctx.remote.reactivePrograms.startAction(raw, t.signal);
    assert.equal(reply.ok, false, 'wire 缺少或传入工具调用身份字符串必须在业务前拒绝');
  }
  assert.deepEqual({ scopes, inputs, defaults, bodies }, beforeInvalid);
  assert.equal(await readFile(file, 'utf8'), '12');

  const cancelled = new AbortController(), cancelledReason = new Error('CANCEL_BEFORE_DISPATCH');
  cancelled.abort(cancelledReason);
  const beforeCancelled = startRequests;
  await assert.rejects(invoke({ value: '14' }, { signal: cancelled.signal }), error => error === cancelledReason);
  assert.equal(startRequests, beforeCancelled, '预取消不分派HTTP请求');
  assert.equal(models, 0); assert.deepEqual(errors, []);
});
