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
import { ActionContractMismatchError, actionIdentity, defineAction, implementAction, defineApplication,
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
  let corruptNextAck = false, acceptedAck;
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
  installConnection(ctx, { interactions: 'answer', transport: createAuthenticatedTransport({ baseUrl: gateway.url,
    async fetch(url, init) {
      const response = await fetch(url, { ...init, headers: { ...Object.fromEntries(new Headers(init?.headers)), ...headers } });
      if (corruptNextAck && new URL(url).pathname.endsWith('/reactivePrograms/startAction')) {
        corruptNextAck = false;
        // 只损坏已经真实受理后的回执交付，不替换Host、业务执行或Gateway协议实现。
        const payload = await response.json(); assert.equal(payload.result.ok, true);
        acceptedAck = structuredClone(payload.result.value);
        payload.result.value.contract.version = 2;
        return new Response(JSON.stringify(payload), { status: response.status, headers: response.headers });
      }
      return response;
    },
    createWebSocket: url => new WebSocket(url, { headers }),
  }) });
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
    const reply = await ctx.remote.reactivePrograms.startAction({ sessionId: local.id, program: { name: program.name, version: program.version }, accessId: correct.identity.accessId, name: 'save',
      bindingId: invoke.bindingId, contract, input: { value: '9' } }, t.signal);
    assert.equal(reply.ok, false, '服务器独立拒绝错误或缺失合同，不能只信SDK检查');
    if (contract) assert.equal(reply.error.code, 'reactive/action-contract-mismatch');
  }
  assert.deepEqual({ inputs, defaults, bodies }, beforeStart); await assert.rejects(access(file), { code: 'ENOENT' });
  const invocation = await invoke({ value: '7' });
  assert.deepEqual(await invocation.result, { stored: 7 });
  assert.equal(await readFile(file, 'utf8'), '7'); assert.equal(bodies, 1); assert.equal(inputs, 1);
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
  corruptNextAck = true;
  await assert.rejects(invoke({ value: '8' }), error => error instanceof ActionAdmissionUnknownError
    && error.cause instanceof ActionContractMismatchError);
  assert.ok(acceptedAck);
  assert.deepEqual(await (await correct.invocation('save', acceptedAck.id)).result, { stored: 8 });
  assert.equal(await readFile(file, 'utf8'), '8'); assert.equal(bodies, 2, '矛盾ACK不能触发自动重发');
  assert.equal(models, 0); assert.deepEqual(errors, []);
});
