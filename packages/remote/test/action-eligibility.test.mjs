import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement as h } from 'react';
import { BehaviorSubject, filter, firstValueFrom, timeout, tap } from 'rxjs';
import WebSocket from 'ws';
import { RemoteError } from '@deepseek-ai/dsh-typert-protocol';
import { EligibilityError, defineAction, implementAction, defineApplication, defineEnvironment, defineProgram, defineSurface, z } from '@hanger-source/dsh-reactive';
import { Expose, implementProgram, useAction } from '@hanger-source/dsh-reactive/program';
import { createProgramClient } from '@hanger-source/dsh-reactive-client';
import { mountProgramGateway, RemoteAuthenticationError } from '@hanger-source/dsh-reactive-remote';
import { boot } from '../../../test/native-support.ts';

const next = async (stage, reader, predicate) => {
  const seen = [];
  try {
    return await firstValueFrom(reader.observations$.pipe(
      tap(value => seen.push(value.status === 'error'
        ? { status: value.status, error: String(value.error), code: value.error?.code } : value.status)),
      filter(predicate), timeout(5_000),
    ));
  } catch (error) { throw new Error(stage + ' observed=' + JSON.stringify(seen), { cause: error }); }
};

test('真实Gateway持续交付共同资格，旧绑定逐次核验，恢复及断线重连不重执文件业务', { timeout: 20_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-action-eligibility-'));
  const file = join(root, 'accepted.txt'), sockets = [], observed = [];
  let generation = 0, bodies = 0, models = 0;
  const ready = value => ({ status: 'ready', generation: ++generation, observedAt: Date.now(), value });
  const qualification = new BehaviorSubject(ready({ allowed: true }));
  const save = defineAction({ name: 'qualified_save', description: '资格有效时写入文件', scope: z.null(), input: z.string(), output: z.string() });
  const independent = defineAction({ name: 'independent_read', description: '独立读取正式文件', scope: z.null(), input: z.null(), output: z.string() });
  const writer = implementAction(save, { async execute(input) { bodies++; await writeFile(file, input); return input; } });
  const reader = implementAction(independent, { execute: () => readFile(file, 'utf8') });
  const surface = defineSurface({ views: {}, actions: { save, independent } });
  const program = defineProgram({ name: 'action_eligibility_test', input: z.null(), surface });
  const definition = defineApplication({ name: 'action-eligibility', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '显式SDK调用不启动模型', program: implementProgram(program, { Component() {
      const save = useAction(writer, () => null, [], { eligibility: qualification });
      const independent = useAction(reader, () => null, []);
      return h(Expose, { surface, value: { views: {}, actions: { save, independent } } });
    } }),
  })] });
  const runtime = await boot(root, definition, async function* () { models++; throw new Error('QUALIFICATION_MUST_NOT_START_MODEL'); });
  let local, gateway, client, subscription;
  t.after(async () => {
    subscription?.unsubscribe(); await client?.dispose(); local?.release(); await gateway?.close(); await runtime.dispose(); qualification.complete();
  });
  local = await runtime.host.create('work', { workspaceId: runtime.workspace.workspaceId });
  gateway = await mountProgramGateway(runtime.native, { application: runtime.host, model: { provider: 'isolated', model: 'test' },
    participant: principal => ({ participant: 'human', actorId: principal.id }),
    access: service => service.registerPolicy('eligibility', { bindings: () => [{ resource: { kind: 'session', id: local.id },
      role: { name: 'eligibility', resource: 'session', actions: ['reactivePrograms/open', 'reactivePrograms/resolveAction',
        'reactivePrograms/startAction', 'reactivePrograms/actionInvocation', 'reactivePrograms/actionResult', 'reactivePrograms/observeAction'] },
    }], explain: () => 'qualification-session-only' }),
    carrier: { async authenticate(request) {
      if (request.headers.get('authorization') !== 'Bearer eligibility') throw new RemoteAuthenticationError(401);
      return { kind: 'eligibility', id: 'operator-test' };
    } },
  });
  const headers = { authorization: 'Bearer eligibility' };
  client = await createProgramClient(program, { baseUrl: gateway.url,
    fetch: (url, init) => fetch(url, { ...init, headers: { ...Object.fromEntries(new Headers(init?.headers)), ...headers } }),
    createWebSocket(url) {
      const socket = new WebSocket(url, { headers }), tracked = { socket, endpoint: undefined, bindingId: undefined };
      sockets.push(tracked);
      const send = socket.send.bind(socket);
      socket.send = (data, ...args) => {
        const message = JSON.parse(String(data));
        if (message.type === 'open') tracked.endpoint = message.endpoint;
        return send(data, ...args);
      };
      socket.on('message', data => {
        const message = JSON.parse(String(data));
        if (message.type === 'item' && tracked.endpoint === 'reactivePrograms/resolveAction') tracked.bindingId = message.value?.bindingId;
      });
      return socket;
    },
  });
  const session = await client.open(local.id), captured = await session.resolveAction('save', null);
  assert.ok(captured.eligibility);
  const eligibility = captured.eligibility, bindingId = captured.bindingId;
  const again = await session.resolveAction('save', null);
  assert.equal(again.bindingId, bindingId);
  assert.equal(again.eligibility, eligibility, '重复解析同一真实绑定继续借用原资格reader');
  subscription = eligibility.observations$.subscribe(value => observed.push(value));
  await next('initial allowed', eligibility, value => value.status === 'ready' && value.value.allowed);
  assert.equal(await (await captured('first')).result, 'first');
  const independentRead = await session.resolveAction('independent', null);
  assert.equal(independentRead.eligibility, undefined);
  qualification.next({ status: 'pending', generation: ++generation });
  await next('source pending', eligibility, value => value.status === 'pending');
  assert.equal(await readFile(file, 'utf8'), 'first');
  qualification.next(ready({ allowed: false, reason: '知识资料未确认' }));
  const denied = await next('source denied', eligibility, value => value.status === 'ready' && !value.value.allowed);
  assert.equal(denied.value.reason, '知识资料未确认');
  await assert.rejects((await captured('forbidden')).result, error => error instanceof EligibilityError && error.reason === 'denied'
    && error.observation.value.reason === '知识资料未确认');
  qualification.next({ status: 'error', generation: ++generation,
    error: new RemoteError('gateway/internal', '资格来源暂时不可用', {}) });
  const failed = await next('source error', eligibility, value => value.status === 'error');
  assert.equal(failed.error.code, 'gateway/internal');
  await assert.rejects((await captured('error-write')).result, error => error instanceof EligibilityError && error.reason === 'error'
    && error.observation.error.code === 'gateway/internal');
  assert.equal(await (await independentRead(null)).result, 'first', '来源error不杀Program，独立Action继续读取真实文件');
  assert.equal(local.resolveAction('save', null).bindingId, bindingId, '资格变化不撤Expose也不换绑定');
  assert.equal(bodies, 1);
  qualification.next(ready({ allowed: true }));
  await next('source recovered', eligibility, value => value.status === 'ready' && value.value.allowed);
  assert.equal(await (await captured('recovered')).result, 'recovered', '同一个旧句柄恢复后允许新的显式调用');
  assert.equal(bodies, 2);
  const bindingSockets = () => sockets.filter(item => item.endpoint === 'reactivePrograms/resolveAction');
  assert.equal(bindingSockets().filter(item => item.socket.readyState === WebSocket.OPEN).length, 2,
    '重复解析核对宿主后释放临时连接，两个Action各持有一个绑定流');
  const disconnected = next('carrier pending', eligibility, value => value.status === 'pending');
  bindingSockets().find(item => item.bindingId === bindingId && item.socket.readyState === WebSocket.OPEN).socket.terminate(); await disconnected;
  assert.equal(observed.at(-1).status, 'pending', '载体失败不能保留旧allowed');
  await next('carrier recovered', eligibility, value => value.status === 'ready' && value.value.allowed);
  assert.equal(await readFile(file, 'utf8'), 'recovered'); assert.equal(bodies, 2, '物理重连不重执已完成业务');
  assert.equal(captured.bindingId, bindingId);
  assert.equal(models, 0);
});
