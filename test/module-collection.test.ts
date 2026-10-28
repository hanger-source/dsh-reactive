import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
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
    layout.next([b, a]);
    assert.equal(knowledgeA.resolveAction('record', a).bindingId, originalA.bindingId);
    assert.equal(knowledgeB.resolveAction('record', b).bindingId, originalB.bindingId);
    const manifest = await session.actionManifest.read();
    const offeredB = manifest.find(item => item.modulePath.length === 3 && item.modulePath[0] === 'merchants'
      && item.modulePath[1] === b && item.modulePath[2] === 'knowledge');
    assert.ok(offeredB);
    assert.equal(offeredB.bindingId, originalB.bindingId);
    nativeName = offeredB.toolName;
    await session.submit({ requestId: 'record-b', content: '记录商家b资料', intent: 'question', delivery: 'followup', scope: null });
    await session.whenIdle();
    assert.deepEqual(effects, [{ merchantId: b, source: 'tool', note: 'native-b' }]);

    hold = true;
    const running = originalA({ note: 'accepted-a' });
    await entered.promise;
    layout.next([b]);
    assert.throws(() => originalA({ note: 'withdrawn-a' }), ActionBindingUnavailableError);
    assert.deepEqual(await firstValueFrom(collection.instances$), [b]);
    await assert.rejects(instanceA.readView('knowledge.facts'), /REACTIVE_MODULE_INSTANCE_NOT_FOUND/u);
    await assert.rejects(firstValueFrom(instanceA.collection('reviews').instances$), /REACTIVE_MODULE_INSTANCE_NOT_FOUND/u);
    assert.equal(reviewStates.at(-1)!.status, 'error', '嵌套成员目录失效仍保留资源观察，不能把旧成员当作当前成员');
    assert.deepEqual(await originalB({ note: 'sdk-b' }).result, { merchantId: b, note: 'sdk-b' });
    finish.resolve();
    assert.deepEqual(await running.result, { merchantId: a, note: 'accepted-a' });
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
