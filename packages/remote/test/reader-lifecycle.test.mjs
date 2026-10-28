import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement as h } from 'react';
import { BehaviorSubject, filter, firstValueFrom, timeout } from 'rxjs';
import WebSocket from 'ws';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { defineApplication, defineEnvironment, defineProgram, defineSurface, resourceView, z } from '@hanger-source/dsh-reactive';
import { Expose, implementProgram } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import { createProgramClient } from '@hanger-source/dsh-reactive-client';
import { mountProgramGateway, RemoteAuthenticationError } from '@hanger-source/dsh-reactive-remote';

const next = (source, predicate) => firstValueFrom(source.pipe(filter(predicate), timeout(3_000)));
const ready = value => ({ status: 'ready', generation: 1, observedAt: Date.now(), value });

test('View与人工查询按消费者连接，独立endpoint授权与真实Gateway会话引用在退订和release后结算', { timeout: 15_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-reader-lifecycle-'));
  const surface = defineSurface({ actions: {}, views: { current: resourceView(z.string()), unused: resourceView(z.string()) } });
  const program = defineProgram({ input: z.null(), surface });
  const current = new BehaviorSubject(ready('first')), unused = new BehaviorSubject(ready('unused'));
  const definition = defineApplication({ name: 'reader-lifecycle', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '读取不执行业务', program: implementProgram(program, { Component: () => h(Expose, {
      surface, value: { views: { current, unused }, actions: {} },
    }) }),
  })] });
  const errors = [], clients = [], subscriptions = [];
  const calls = new Map(['views-only', 'human-only'].map(kind => [kind, { view: 0, questions: 0 }]));
  // 官方准入合同区分实际调用args和权限快照；统计真实请求，不解析传输内部帧。
  const called = (kind, request) => {
    if (!request.args) return;
    const endpoint = request.action.slice('reactivePrograms/'.length);
    if (Object.hasOwn(calls.get(kind), endpoint)) calls.get(kind)[endpoint]++;
  };
  let models = 0, gateway, local;
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'reader',
    onError: event => errors.push(event.error),
    model: nativeModel({ provider: 'reader-lifecycle', model: 'unused', create: () => new class extends LlmAdapter {
      async *stream() { models++; throw new Error('RESOURCE_READING_MUST_NOT_START_MODEL'); }
    }() }),
  });
  t.after(async () => {
    subscriptions.forEach(subscription => subscription.unsubscribe());
    await Promise.all(clients.map(client => client.dispose()));
    local?.release(); await gateway?.close(); await runtime.dispose();
    current.complete(); unused.complete();
  });
  // 只观察公开句柄的实际取得和释放，不替换会话owner、查询数据或Gateway协议。
  const leases = new BehaviorSubject(0), open = runtime.application.open.bind(runtime.application);
  runtime.application.open = async (...args) => {
    const handle = await open(...args), release = handle.release.bind(handle);
    let released = false;
    leases.next(leases.value + 1);
    handle.release = () => {
      if (released) return;
      released = true; release(); leases.next(leases.value - 1);
    };
    return handle;
  };
  const workspace = await runtime.application.createWorkspace({ directory: root });
  local = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
  gateway = await mountProgramGateway(runtime.native, { application: runtime.application,
    model: { provider: 'reader-lifecycle', model: 'unused' }, actor: principal => principal.id,
    access(service) {
      for (const [kind, endpoint] of [['views-only', 'view'], ['human-only', 'questions']]) {
        service.registerPolicy(kind, { bindings: () => [{ resource: { kind: 'session', id: local.id },
          role: { name: kind, resource: 'session', actions: ['reactivePrograms/open', 'reactivePrograms/' + endpoint] },
          condition(request) { called(kind, request); return { kind: 'allow' }; },
        }], explain(request) { called(kind, request); return 'endpoint-not-selected'; } });
      }
    },
    carrier: { onError: error => errors.push(error), async authenticate(request) {
      const token = request.headers.get('authorization');
      if (token !== 'Bearer views-only' && token !== 'Bearer human-only') throw new RemoteAuthenticationError(401);
      return { kind: token.slice('Bearer '.length), id: 'reader' };
    } },
  });
  const connect = async kind => {
    const headers = { authorization: 'Bearer ' + kind };
    const client = await createProgramClient(program, { baseUrl: gateway.url,
      fetch(url, init) {
        return fetch(url, { ...init, headers: { ...Object.fromEntries(new Headers(init?.headers)), ...headers } });
      }, createWebSocket: url => new WebSocket(url, { headers }),
    }); clients.push(client);
    return { session: await client.open(local.id), calls: calls.get(kind) };
  };
  const views = await connect('views-only'), resource = views.session.viewResource('current');
  const manifest = views.session.actionManifest;
  assert.equal(manifest.peek().status, 'pending');
  assert.deepEqual(views.calls, { view: 0, questions: 0 }, '取得公开目录对象不打开远程查询');
  assert.equal(views.session.views.current, resource.values$);
  assert.equal(resource.peek().status, 'pending');
  assert.equal(views.session.human.peek().status, 'pending');
  const one = [], two = [];
  const first = resource.values$.subscribe(value => one.push(value)); subscriptions.push(first);
  const second = views.session.views.current.subscribe(value => two.push(value)); subscriptions.push(second);
  await next(resource.values$, value => value === 'first');
  assert.equal(await views.session.readView('current', { signal: t.signal }), 'first');
  assert.deepEqual(views.calls, { view: 1, questions: 0 });
  assert.equal(leases.value, 1);
  first.unsubscribe(); current.next(ready('second'));
  await next(resource.values$, value => value === 'second');
  assert.deepEqual(one, ['first']); assert.equal(two.at(-1), 'second');
  second.unsubscribe(); await next(leases, value => value === 0);
  assert.equal(resource.peek().status, 'pending');
  current.next(ready('third'));
  assert.equal(await resource.read(t.signal), 'third');
  await next(leases, value => value === 0);
  assert.deepEqual(views.calls, { view: 2, questions: 0 });
  current.next({ status: 'error', generation: 2, error: new Error('BUSINESS_QUERY_FAILED') });
  await assert.rejects(resource.read(t.signal)); await next(leases, value => value === 0);
  current.next(ready('recovered'));
  assert.equal(await resource.read(t.signal), 'recovered'); await next(leases, value => value === 0);

  const beforeManifest = views.calls.view;
  const manifestResource = manifest.observations$.subscribe(), manifestValues = views.session.actionManifest$.subscribe();
  subscriptions.push(manifestResource, manifestValues);
  assert.deepEqual(await manifest.read(t.signal), []);
  assert.equal(views.calls.view, beforeManifest + 1, '目录资源、值流与Promise共享一个真实远程查询');
  manifestResource.unsubscribe(); manifestValues.unsubscribe(); await next(leases, value => value === 0);

  const human = await connect('human-only');
  assert.equal(human.session.viewResource('unused').peek().status, 'pending');
  let completed = false;
  const questionObserver = human.session.human$.subscribe({ complete() { completed = true; } }); subscriptions.push(questionObserver);
  const foreground = human.session.foregroundQuestions$.subscribe(); subscriptions.push(foreground);
  assert.deepEqual((await human.session.human.read(t.signal)).foreground, []);
  assert.deepEqual(human.calls, { view: 0, questions: 1 });
  assert.equal(leases.value, 1);
  questionObserver.unsubscribe();
  assert.equal(leases.value, 1);
  foreground.unsubscribe(); await next(leases, value => value === 0);
  assert.equal(human.session.human.peek().status, 'pending');
  assert.deepEqual((await human.session.human.read(t.signal)).questions.active, []);
  await next(leases, value => value === 0);
  assert.deepEqual(human.calls, { view: 0, questions: 2 });

  const watching = human.session.human$.subscribe({ complete() { completed = true; } }); subscriptions.push(watching);
  await human.session.human.read(t.signal);
  await views.session.release();
  assert.equal(leases.value, 1, '另一个会话SDK退出不停止此查询');
  await human.session.release(); await next(leases, value => value === 0);
  assert.equal(completed, true); assert.equal(watching.closed, true);
  const before = { ...human.calls };
  assert.throws(() => human.session.human.peek()); assert.throws(() => human.session.human.read());
  assert.deepEqual(human.calls, before);
  assert.equal(await local.readView('current'), 'recovered', '查询退出不结束原Program来源');
  assert.equal(models, 0); assert.deepEqual(errors, []);
});
