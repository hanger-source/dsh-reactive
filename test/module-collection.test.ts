import { test } from 'node:test';
import assert from 'node:assert/strict';
import { appendFile, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement as h, Fragment } from 'react';
import { BehaviorSubject, filter, firstValueFrom } from 'rxjs';
import type { GenerateOptions } from '@deepseek-ai/dsh-llm';
import { z } from 'zod';
import { ActionBindingUnavailableError, defineAction, implementAction } from '@hanger-source/dsh-reactive';
import { defineApplication } from '@hanger-source/dsh-reactive';
import { defineEnvironment } from '@hanger-source/dsh-reactive';
import { Context, Expose, Offer } from '@hanger-source/dsh-reactive/program';
import { defineProgram } from '@hanger-source/dsh-reactive';
import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { useAction, useObservable, useResource } from '@hanger-source/dsh-reactive/program';
import { collectionSurface, composeSurface } from '@hanger-source/dsh-reactive';
import { Module } from '@hanger-source/dsh-reactive/program';
import { defineSurface, resourceView, type PartialSurfaceBindings } from '@hanger-source/dsh-reactive';
import type { Observation, SurfaceClient } from '@hanger-source/dsh-reactive';
import { boot, text, tool } from './native-support.ts';

test('动态商家集合沿嵌套模块公开同一合同，新增与重排保身份，撤下拒旧调用且不取消已开始效果', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-collection-'));
  const a = 'merchant/杭州.a', b = 'merchant:02';
  const layout = new BehaviorSubject<readonly string[]>([a]);
  const factsSchema = z.object({ merchantId: z.string(), revision: z.number().int() });
  type Facts = z.output<typeof factsSchema>;
  const facts = new Map([a, b].map(merchantId => [merchantId, new BehaviorSubject<Observation<Facts>>({
    status: 'ready', generation: 1, observedAt: Date.now(), value: { merchantId, revision: 1 },
  })]));
  const record = defineAction({ name: 'record_details', description: '记录当前商家的交付资料',
    scope: z.string().transform(merchantId => ({ merchantId })), input: z.object({ note: z.string() }),
    output: z.object({ merchantId: z.string(), note: z.string() }) });
  const local = defineSurface({ views: { facts: resourceView(factsSchema) }, actions: { record } });
  const context = defineSurface({ views: { facts: resourceView(factsSchema) }, actions: {} });
  const merchant = composeSurface({ knowledge: local, preparation: context, reviews: collectionSurface(context) });
  const composed = composeSurface({ merchants: collectionSurface(merchant.surface) });
  const consume = (client: SurfaceClient<typeof local>) => client.readView('facts');
  const entered = Promise.withResolvers<void>(), finish = Promise.withResolvers<void>();
  const effects: { merchantId: string; source: string; note: string }[] = [];
  const requests: GenerateOptions[] = [];
  let hold = false, nativeName: string | undefined;
  const implementation = implementAction(record, { async execute(input, call) {
    if (hold && call.scope.merchantId === a) { entered.resolve(); await finish.promise; }
    call.signal.throwIfAborted();
    effects.push({ merchantId: call.scope.merchantId, source: call.source.kind, note: input.note });
    return { merchantId: call.scope.merchantId, note: input.note };
  } });
  function Knowledge({ merchantId }: { merchantId: string }) {
    const action = useAction(implementation, () => merchantId, [merchantId]);
    const resource = useResource(() => facts.get(merchantId)!, [merchantId]);
    return h(Fragment, null,
      h(Context<Facts>, { name: 'facts', required: false, observation: resource.state, render: value => value }),
      h(Offer<typeof record>, { action }),
      h(Expose<typeof local, PartialSurfaceBindings<typeof local>>, { surface: local,
        value: { views: { facts: resource.observations$ }, actions: { record: action } } }));
  }
  function Preparation({ merchantId }: { merchantId: string }) {
    const resource = useResource(() => facts.get(merchantId)!, [merchantId]);
    return h(Expose<typeof context, PartialSurfaceBindings<typeof context>>, { surface: context,
      value: { views: { facts: resource.observations$ } } });
  }
  function Merchant({ merchantId }: { merchantId: string }) {
    return h(Fragment, null,
      h(Module, { mount: merchant.mounts.knowledge }, h(Knowledge, { merchantId })),
      h(Module, { mount: merchant.mounts.preparation }, h(Preparation, { merchantId })),
      h(Module, { mount: merchant.mounts.reviews.instance('current') }, h(Preparation, { merchantId })));
  }
  const program = defineProgram({ input: z.null(), surface: composed.surface });
  const definition = defineApplication({ name: 'module-collection', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '按完整商家身份记录资料', program: implementProgram(program, { Component() {
      const observed = useObservable(() => layout, [layout]);
      return observed.status === 'ready' ? h(Fragment, null, observed.value.map(merchantId =>
        h(Module, { key: merchantId, mount: composed.mounts.merchants.instance(merchantId) }, h(Merchant, { merchantId })))) : null;
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* (options) {
    requests.push(options);
    if (requests.length > 2) throw new Error('COLLECTION_MODEL_CALL_LIMIT');
    if (requests.length === 1) {
      assert.ok(nativeName);
      assert.ok(options.tools?.some(tool => tool.name === nativeName));
      yield* tool(nativeName, 'record-dynamic-b', { note: 'native-b' });
    } else yield* text('已记录商家资料');
  });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const collection = session.collection('merchants');
    const instanceA = collection.instance(a), instanceB = collection.instance(b);
    const knowledgeA = instanceA.module(merchant.mounts.knowledge), knowledgeB = instanceB.module(merchant.mounts.knowledge);
    assert.equal(session.surface, composed.surface);
    assert.equal(composed.mounts.merchants.surface, merchant.surface);
    assert.equal(composed.mounts.merchants.selected, merchant.surface);
    assert.equal(knowledgeA.surface, local);
    assert.equal(knowledgeA.viewResource('facts'), instanceA.viewResource('knowledge.facts'));
    assert.equal(session.module(composed.mounts.merchants.instance(a)).module(merchant.mounts.knowledge).viewResource('facts'), knowledgeA.viewResource('facts'));
    assert.deepEqual(await consume(knowledgeA), { merchantId: a, revision: 1 });
    assert.deepEqual(await collection.instances.read(), [a]);
    const reviews = instanceA.collection('reviews').instances;
    const reviewStates: Observation<readonly string[]>[] = [];
    const stopReviews = reviews.subscribe(value => reviewStates.push(value));
    assert.deepEqual(await reviews.read(), ['current']);
    assert.deepEqual(await instanceA.collection('reviews').instance('current').readView('facts'), { merchantId: a, revision: 1 });
    await assert.rejects(instanceB.readView('knowledge.facts'), /REACTIVE_MODULE_INSTANCE_NOT_FOUND/u);
    const originalA = knowledgeA.resolveAction('record', a);
    layout.next([a, b]);
    await firstValueFrom(collection.instances$.pipe(filter(keys => keys.includes(b))));
    assert.deepEqual(await instanceB.readView('preparation.facts'), { merchantId: b, revision: 1 });
    const originalB = knowledgeB.resolveAction('record', b);
    const manifest = await session.actionManifest.read();
    const offeredB = manifest.find(item => item.modulePath.length === 3 && item.modulePath[0] === 'merchants'
      && item.modulePath[1] === b && item.modulePath[2] === 'knowledge');
    assert.ok(offeredB);
    assert.equal(offeredB.bindingId, originalB.bindingId);
    nativeName = offeredB.toolName;
    layout.next([b, a]);
    assert.equal(knowledgeA.resolveAction('record', a).bindingId, originalA.bindingId);
    assert.equal(knowledgeB.resolveAction('record', b).bindingId, originalB.bindingId);
    // 后续模型仍使用重排前取得的正式工具名；相同逻辑地址的新token不能让当前工具失效。
    await session.submit({ requestId: 'record-b', content: '记录商家b资料', intent: 'question', delivery: 'followup', scope: null });
    await session.whenIdle();
    assert.deepEqual(effects, [{ merchantId: b, source: 'tool', note: 'native-b' }]);

    hold = true;
    const running = originalA({ note: 'accepted-a' });
    await entered.promise;
    layout.next([b]);
    assert.throws(() => originalA({ note: 'withdrawn-a' }), ActionBindingUnavailableError);
    assert.equal(knowledgeA.invocation('record', running.id), running, '撤下Expose仍查回同一个已受理调用');
    assert.deepEqual(await firstValueFrom(collection.instances$), [b]);
    await assert.rejects(instanceA.readView('knowledge.facts'), /REACTIVE_MODULE_INSTANCE_NOT_FOUND/u);
    await assert.rejects(firstValueFrom(instanceA.collection('reviews').instances$), /REACTIVE_MODULE_INSTANCE_NOT_FOUND/u);
    assert.equal(reviewStates.at(-1)!.status, 'error', '嵌套成员目录失效仍保留资源观察，不能把旧成员当作当前成员');
    assert.deepEqual(await originalB({ note: 'sdk-b' }).result, { merchantId: b, note: 'sdk-b' });
    finish.resolve();
    assert.deepEqual(await running.result, { merchantId: a, note: 'accepted-a' });
    const reopened = await app.host.open(session.id, program);
    try {
      assert.equal(reopened.collection('merchants').instance(a).module(merchant.mounts.knowledge).invocation('record', running.id), running,
        '新本地句柄按精确模块地址查回原call，不重新执行');
      assert.throws(() => reopened.collection('merchants').instance(b).module(merchant.mounts.knowledge).invocation('record', running.id),
        /REACTIVE_INVOCATION_UNAVAILABLE/u);
    } finally { reopened.release(); }
    facts.get(a)!.next({ status: 'ready', generation: 2, observedAt: Date.now(), value: { merchantId: a, revision: 2 } });
    layout.next([a, b]);
    await firstValueFrom(collection.instances$.pipe(filter(keys => keys.includes(a))));
    assert.deepEqual(await reviews.read(), ['current'], '原嵌套目录资源随父模块恢复，调用者不重建缓存或来源');
    assert.equal(reviewStates.at(-1)!.status, 'ready');
    assert.deepEqual(await consume(knowledgeA), { merchantId: a, revision: 2 });
    assert.notEqual(knowledgeA.resolveAction('record', a).bindingId, originalA.bindingId);
    assert.throws(() => originalA({ note: 'stale-a' }), ActionBindingUnavailableError);
    assert.equal(session.surface, composed.surface);
    assert.deepEqual(effects, [{ merchantId: b, source: 'tool', note: 'native-b' },
      { merchantId: b, source: 'participant', note: 'sdk-b' }, { merchantId: a, source: 'participant', note: 'accepted-a' }]);
    assert.equal(requests.length, 2);
    assert.deepEqual(app.errors, []);
    stopReviews(); session.release();
  } finally {
    finish.resolve(); layout.complete(); for (const source of facts.values()) source.complete(); await app.dispose();
  }
});

test('同一React模块切换动态地址不复活旧工具，嵌套消费与另一会话各守原实例，已受理写入继续结算', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-collection-generation-'));
  const file = join(root, 'recorded.jsonl');
  await writeFile(file, '');
  const selected = new BehaviorSubject({ merchantId: 'a', revision: 0 });
  const peerSelected = new BehaviorSubject({ merchantId: 'a', revision: 0 });
  const entered = Promise.withResolvers<void>(), released = Promise.withResolvers<void>();
  const result = z.object({ merchantId: z.string(), sessionId: z.string(), note: z.string() });
  const record = defineAction({ name: 'record_details', description: '写入明确商家的正式记录', scope: z.string(),
    input: z.object({ note: z.string() }), output: result });
  const implementation = implementAction(record, { async execute(input, call) {
    if (input.note === 'accepted-a') { entered.resolve(); await released.promise; }
    call.signal.throwIfAborted();
    const recorded = { merchantId: call.scope, sessionId: call.sessionId, note: input.note };
    await appendFile(file, JSON.stringify(recorded) + '\n');
    return recorded;
  } });
  const local = defineSurface({ views: {}, actions: { record } });
  const merchant = composeSurface({ commands: local });
  const composed = composeSurface({ merchants: collectionSurface(merchant.surface) });
  function Commands({ merchantId }: { merchantId: string }) {
    const action = useAction(implementation, () => merchantId, [merchantId]);
    return h(Fragment, null, h(Offer<typeof record>, { action }),
      h(Expose<typeof local, PartialSurfaceBindings<typeof local>>, { surface: local, value: { actions: { record: action } } }));
  }
  const program = defineProgram({ input: z.null(), surface: composed.surface });
  const definition = defineApplication({ name: 'collection-generation', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '只调用当前模块的正式记录工具', program: implementProgram(program, { Component({ conversation }) {
      const current = useObservable(conversation.identity.sessionId === 'first' ? selected : peerSelected);
      if (current.status !== 'ready') return null;
      const merchantId = current.value.merchantId;
      // Module故意不带React key；业务地址替换仍须退出原工具身份。
      return h(Module, { mount: composed.mounts.merchants.instance(merchantId) },
        h(Module, { mount: merchant.mounts.commands }, h(Commands, { merchantId })));
    } }),
  })] });
  let nextTool: { name: string; requestId: string } | undefined;
  let modelCalls = 0;
  const app = await boot(join(root, 'home'), definition, async function* () {
    if (++modelCalls > 20) throw new Error('COLLECTION_GENERATION_MODEL_CALL_LIMIT');
    const selected = nextTool;
    nextTool = undefined;
    if (selected) yield* tool(selected.name, selected.requestId, { note: selected.requestId });
    else yield* text('记录流程已结束');
  });
  try {
    const first = await app.host.create('work', { workspaceId: app.workspace.workspaceId, sessionId: 'first' });
    const peer = await app.host.create('work', { workspaceId: app.workspace.workspaceId, sessionId: 'peer' });
    const collection = first.collection('merchants');
    const clientA = collection.instance('a').module(merchant.mounts.commands);
    const clientB = collection.instance('b').module(merchant.mounts.commands);
    const peerClient = peer.collection('merchants').instance('a').module(merchant.mounts.commands);
    const resolve = (client: SurfaceClient<typeof local>, merchantId: string) => client.resolveAction('record', merchantId);
    const originalA = await resolve(clientA, 'a'), originalPeer = await resolve(peerClient, 'a');
    const originalName = (await first.actionManifest.read()).find(item => item.bindingId === originalA.bindingId)!.toolName;
    const peerName = (await peer.actionManifest.read()).find(item => item.bindingId === originalPeer.bindingId)!.toolName;
    const invoke = async (session: typeof first, name: string, requestId: string) => {
      nextTool = { name, requestId };
      await session.submit({ requestId, content: '执行当前明确工具', intent: 'question', delivery: 'followup', scope: null });
      await session.whenIdle();
      const occurrence = session.snapshot().requests[requestId]?.tools.find(item => item.callId === requestId);
      assert.ok(occurrence, '实际原生工具结算必须关联原请求');
      return occurrence.state;
    };
    selected.next({ merchantId: 'a', revision: 1 });
    assert.equal((await resolve(clientA, 'a')).bindingId, originalA.bindingId);
    assert.equal(await invoke(first, originalName, 'same-key'), 'returned');

    const running = await originalA({ note: 'accepted-a' });
    await entered.promise;
    selected.next({ merchantId: 'b', revision: 2 });
    const currentB = await resolve(clientB, 'b');
    const nameB = (await first.actionManifest.read()).find(item => item.bindingId === currentB.bindingId)!.toolName;
    assert.equal(await invoke(first, originalName, 'stale-a-at-b'), 'failed');
    assert.equal(await invoke(first, nameB, 'current-b'), 'returned');

    selected.next({ merchantId: 'a', revision: 3 });
    const currentA = await resolve(clientA, 'a');
    const currentName = (await first.actionManifest.read()).find(item => item.bindingId === currentA.bindingId)!.toolName;
    assert.throws(() => originalA({ note: 'stale-sdk-a' }), ActionBindingUnavailableError);
    assert.equal(await invoke(first, originalName, 'stale-a-after-return'), 'failed');
    assert.equal(await invoke(first, nameB, 'stale-b-after-return'), 'failed');
    assert.equal(await invoke(first, currentName, 'current-a'), 'returned');
    released.resolve();
    assert.deepEqual(await running.result, { merchantId: 'a', sessionId: first.id, note: 'accepted-a' });
    assert.equal(clientA.invocation('record', running.id), running);
    await app.host.close(first.id);
    first.release();
    assert.equal((await resolve(peerClient, 'a')).bindingId, originalPeer.bindingId);
    assert.equal(await invoke(peer, peerName, 'peer-after-close'), 'returned');
    assert.deepEqual((await readFile(file, 'utf8')).trim().split('\n').map(line => result.parse(JSON.parse(line))), [
      { merchantId: 'a', sessionId: first.id, note: 'same-key' },
      { merchantId: 'b', sessionId: first.id, note: 'current-b' },
      { merchantId: 'a', sessionId: first.id, note: 'current-a' },
      { merchantId: 'a', sessionId: first.id, note: 'accepted-a' },
      { merchantId: 'a', sessionId: peer.id, note: 'peer-after-close' },
    ]);
    assert.deepEqual(app.errors, []);
    peer.release();
  } finally { released.resolve(); selected.complete(); peerSelected.complete(); await app.dispose(); }
});
