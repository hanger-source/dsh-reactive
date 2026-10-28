import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement as h, Fragment } from 'react';
import { BehaviorSubject, Observable, filter, firstValueFrom, timeout } from 'rxjs';
import WebSocket from 'ws';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { collectionSurface, combineResources, mapResource, composeSurface, defineApplication, defineEnvironment, defineProgram, defineSurface, resourceView, z } from '@hanger-source/dsh-reactive';
import { Expose, Module, implementProgram, useObservable, useResource } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import { createProgramClient } from '@hanger-source/dsh-reactive-client';
import { mountProgramGateway, RemoteAuthenticationError } from '@hanger-source/dsh-reactive-remote';

const first = (source, predicate) => firstValueFrom(source.pipe(filter(predicate), timeout(8_000)));
const ready = value => ({ status: 'ready', generation: 1, observedAt: Date.now(), value });
const source = initial => {
  const state = new BehaviorSubject(initial);
  const result = { state, connections: 0, values: new Observable(subscriber => {
    result.connections++;
    const subscription = state.subscribe(subscriber);
    return () => { result.connections--; subscription.unsubscribe(); };
  }) };
  return result;
};

test('同一View资源贯通本地、远程和hooks，动态重挂及真实断线保地址，读取取消和SDK释放各自结算', { timeout: 25_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-view-resources-'));
  const directory = join(root, 'workspace'); await mkdir(directory);
  const date = z.codec(z.iso.datetime(), z.date(), { decode: value => new Date(value), encode: value => value.toISOString() });
  const leaf = defineSurface({ actions: {}, views: { at: resourceView(date) } });
  const composed = composeSurface({ main: leaf, items: collectionSurface(leaf) });
  const program = defineProgram({ input: z.null(), surface: composed.surface });
  const mirrorSurface = defineSurface({ actions: {}, views: { at: leaf.views.at,
    discovery: resourceView(z.object({ members: z.string().array().readonly(), actions: z.number().int() })) } });
  const mirrorProgram = defineProgram({ input: z.null(), surface: mirrorSurface });
  const main = source({ status: 'pending', generation: 0 }), original = source(ready(new Date('2026-10-27T00:00:00Z')));
  const items = new Map([['a/b', original]]), layout = new BehaviorSubject(['a/b']);
  let borrowed, borrowedDirectory, borrowedManifest, models = 0;
  const errors = [], subscriptions = [], sockets = [], reconnected = Promise.withResolvers();
  const expose = value => h(Expose, { surface: leaf, value: { views: { at: value }, actions: {} } });
  const definition = defineApplication({ name: 'view-resources', admit: async () => {}, environments: [
    defineEnvironment({ id: 'source', instructions: '公开资源不启动模型', program: implementProgram(program, { Component() {
      const members = useObservable(() => layout, []);
      return h(Fragment, null, h(Module, { mount: composed.mounts.main }, expose(main.values)),
        members.status === 'ready' ? members.value.map(key => h(Module, { key, mount: composed.mounts.items.instance(key) }, expose(items.get(key).values))) : null);
    } }) }),
    defineEnvironment({ id: 'mirror', instructions: '直接组合已有远程资源', program: implementProgram(mirrorProgram, { Component() {
      const current = useResource(borrowed);
      const discovery = useResource(() => combineResources({ members: borrowedDirectory, actions: borrowedManifest })
        .pipe(mapResource(({ members, actions }) => ({ members, actions: actions.length }))), []);
      return h(Expose, { surface: mirrorSurface, value: { views: {
        at: current.observations$, discovery: discovery.observations$,
      }, actions: {} } });
    } }) }),
  ] });
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'view-owner', onError: event => errors.push(event.error),
    model: nativeModel({ provider: 'view-resources', model: 'unused', create: () => new class extends LlmAdapter {
      async *stream() { models++; throw new Error('VIEW_READING_MUST_NOT_START_MODEL'); }
    }() }),
  });
  let gateway, client, local, remote, mirror;
  try {
    const visible = new Set();
    const role = { name: 'view-reader', resource: 'session', actions: ['reactivePrograms/open', 'reactivePrograms/questions', 'reactivePrograms/view'] };
    gateway = await mountProgramGateway(runtime.native, { application: runtime.application,
      model: { provider: 'view-resources', model: 'unused' }, participant: principal => ({ participant: 'human', actorId: principal.id }),
      access: service => service.registerPolicy('views', { bindings: () => [...visible].map(id => ({ role, resource: { kind: 'session', id } })) }),
      carrier: { async authenticate(request) {
        if (request.headers.get('authorization') !== 'Bearer view-reader') throw new RemoteAuthenticationError(401);
        return { kind: 'views', id: 'view-reader' };
      }, onError: error => errors.push(error) },
    });
    const workspace = await runtime.application.createWorkspace({ directory });
    local = await runtime.application.create('source', { workspaceId: workspace.workspaceId }); visible.add(local.id);
    client = await createProgramClient(program, { baseUrl: gateway.url,
      fetch: (url, init) => fetch(url, { ...init, headers: { ...Object.fromEntries(new Headers(init?.headers)), authorization: 'Bearer view-reader' } }),
      createWebSocket: url => {
        const socket = new WebSocket(url, { headers: { authorization: 'Bearer view-reader' } });
        sockets.push(socket); if (sockets.length > 1) socket.once('open', reconnected.resolve);
        return socket;
      },
    });
    remote = await client.open(local.id);
    console.info('view-resources.connected', { sessionId: local.id });
    const localResource = local.viewResource('main.at'), resource = remote.viewResource('main.at');
    borrowed = resource; borrowedDirectory = remote.collection('items').instances; borrowedManifest = remote.actionManifest;
    assert.equal(local.viewResource('main.at'), localResource); assert.equal(remote.viewResource('main.at'), resource);
    assert.equal(local.views['main.at'], localResource.values$); assert.equal(remote.views['main.at'], resource.values$);
    mirror = await runtime.application.create('mirror', { workspaceId: workspace.workspaceId });
    assert.deepEqual(await mirror.readView('discovery', { signal: t.signal }), { members: ['a/b'], actions: 0 });
    assert.deepEqual(await local.collection('items').instances.read(), ['a/b']);
    const abort = new AbortController(), reason = new Error('ONLY_THIS_READ');
    const cancelled = resource.read(abort.signal), continued = remote.readView('main.at', { signal: t.signal });
    abort.abort(reason); await assert.rejects(cancelled, error => error === reason);
    const time = new Date('2026-10-28T01:00:00Z'); main.state.next(ready(time));
    assert.equal(await localResource.read(t.signal), time, '本地领域对象不再decode一次');
    assert.deepEqual(await continued, time);
    assert.deepEqual(await mirror.readView('at', { signal: t.signal }), time, 'hooks直接消费借入资源');
    console.info('view-resources.composed', { sourceConnections: main.connections });
    assert.equal(main.connections, 1, '读取和组合不另建Program来源连接');

    const localItem = local.collection('items').instance('a/b').viewResource('at');
    const instance = remote.collection('items').instance('a/b'), item = instance.viewResource('at');
    assert.equal(instance.viewResource('at'), item);
    subscriptions.push(item.observations$.subscribe());
    await item.read(t.signal);
    const removed = first(item.observations$, value => value.status === 'error');
    const directoryRemoved = first(borrowedDirectory.observations$, value => value.status === 'ready' && value.value.length === 0);
    const mirrorRemoved = first(mirror.viewResource('discovery').observations$, value => value.status === 'ready' && value.value.members.length === 0);
    layout.next([]); await Promise.all([removed, directoryRemoved, mirrorRemoved]);
    assert.deepEqual(await borrowedDirectory.read(), []);
    assert.deepEqual(await mirror.readView('discovery'), { members: [], actions: 0 });
    assert.equal(original.connections, 0);
    await assert.rejects(localItem.read(), /REACTIVE_MODULE_INSTANCE_NOT_FOUND/);
    await assert.rejects(item.read());
    const replacement = source(ready(new Date('2026-10-28T02:00:00Z'))); items.set('a/b', replacement);
    const restored = first(item.observations$, value => value.status === 'ready');
    const mirrorRestored = first(mirror.viewResource('discovery').observations$, value => value.status === 'ready' && value.value.members.includes('a/b'));
    layout.next(['a/b']); await Promise.all([restored, mirrorRestored]);
    assert.deepEqual(await mirror.readView('discovery'), { members: ['a/b'], actions: 0 });
    assert.deepEqual(await instance.readView('at'), replacement.state.value.value);
    assert.equal(local.collection('items').instance('a/b').viewResource('at'), localItem);
    original.state.next(ready(new Date('2000-01-01T00:00:00Z')));
    assert.deepEqual(item.peek().value, replacement.state.value.value);
    console.info('view-resources.remounted', { oldConnections: original.connections, currentConnections: replacement.connections });

    main.state.next({ status: 'pending', generation: 2 });
    await first(resource.observations$, value => value.status === 'pending');
    const pendingLocal = localResource.read(t.signal), pendingRemote = resource.read(t.signal);
    const rejectedLocal = assert.rejects(pendingLocal, /REACTIVE_SCOPE_DISPOSED/);
    local.release(); await rejectedLocal;
    assert.throws(() => localResource.peek(), /REACTIVE_SCOPE_DISPOSED/);
    sockets[0].terminate(); await reconnected.promise;
    main.state.next(ready(new Date('2026-10-28T03:00:00Z')));
    assert.deepEqual(await pendingRemote, main.state.value.value);
    assert.equal(remote.viewResource('main.at'), resource);
    assert.equal(main.connections, 1);
    console.info('view-resources.reconnected', { sockets: sockets.length, sourceConnections: main.connections });
    main.state.next({ status: 'pending', generation: 3 });
    await first(resource.observations$, value => value.status === 'pending');
    const stopped = assert.rejects(resource.read(), /REACTIVE_CONVERSATION_HANDLE_RELEASED/);
    let completed = false;
    subscriptions.push(resource.observations$.subscribe({ complete: () => { completed = true; } }));
    const discoveryEnded = first(mirror.viewResource('discovery').observations$, value => value.status === 'error');
    await remote.release(); await stopped; await discoveryEnded; assert.equal(completed, true);
    await assert.rejects(mirror.readView('discovery'), /REACTIVE_CONVERSATION_HANDLE_RELEASED/);
    assert.throws(() => resource.peek(), /REACTIVE_CONVERSATION_HANDLE_RELEASED/);
    assert.equal(main.connections, 1, 'SDK释放不撤掉Program来源');
    assert.equal(models, 0); assert.deepEqual(errors, []);
  } catch (error) {
    console.error('view-resources.failed', error); throw error;
  } finally {
    subscriptions.forEach(subscription => subscription.unsubscribe());
    mirror?.release(); local?.release(); await remote?.release(); await client?.dispose(); await gateway?.close(); await runtime.dispose();
  }
});
