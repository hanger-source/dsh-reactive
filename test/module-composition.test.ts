import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, rename, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate } from 'node:timers/promises';
import { createElement as h, Fragment } from 'react';
import { BehaviorSubject, Subject, filter, firstValueFrom, of, switchMap } from 'rxjs';
import type { GenerateOptions } from '@deepseek-ai/dsh-llm';
import { ActionBindingUnavailableError, collectionSurface, defineAction, defineOperation, implementAction, implementOperation } from '@hanger-source/dsh-reactive';
import { defineApplication } from '@hanger-source/dsh-reactive';
import { defineEnvironment } from '@hanger-source/dsh-reactive';
import { Context, Expose, Offer } from '@hanger-source/dsh-reactive/program';
import { defineProgram } from '@hanger-source/dsh-reactive';
import { implementModule, implementProgram } from '@hanger-source/dsh-reactive/program';
import { useAction, useObservable, useOperation, useResource } from '@hanger-source/dsh-reactive/program';
import { implementNativeModule, type NativeCapability } from '@hanger-source/dsh-reactive-dsh';
import { createNativeJobs } from '@hanger-source/dsh-reactive-dsh/jobs';
import { composeSurface, selectSurface, type SurfaceClient } from '@hanger-source/dsh-reactive';
import { Module } from '@hanger-source/dsh-reactive/program';
import { defineSurface, resourceView, view, type PartialSurfaceBindings } from '@hanger-source/dsh-reactive';
import { observeResource } from '@hanger-source/dsh-reactive';
import { type Observation } from '@hanger-source/dsh-reactive';
import { z } from 'zod';
import { boot, text, tool } from './native-support.ts';

test('同合同模块自动隔离原生名称与公开槽，重排保留绑定，撤挂不取消原调用或误撤另一个模块', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-modules-'));
  const layout = new BehaviorSubject<readonly ('a' | 'b')[]>(['a', 'b']);
  const save = defineAction({ name: 'save_record', description: '保存当前模块明确绑定的商家',
    scope: z.string().transform(merchantId => ({ merchantId })), input: z.object({ revision: z.literal(1) }),
    output: z.object({ merchantId: z.string() }) });
  const local = defineSurface({ views: { record: resourceView(z.object({ merchantId: z.string(), revision: z.literal(1) })) },
    actions: { save } });
  const composed = composeSurface({ a: { merchant: local }, b: { merchant: local } });
  const entered = Promise.withResolvers<void>(), finish = Promise.withResolvers<void>();
  const effects: { merchantId: string; source: string }[] = [];
  const requests: GenerateOptions[] = [];
  let held = false;
  const implementation = implementAction(save, { async execute(_input, call) {
    if (held && call.scope.merchantId === 'merchant-a') { entered.resolve(); await finish.promise; }
    call.signal.throwIfAborted();
    effects.push({ merchantId: call.scope.merchantId, source: call.source.kind });
    return { merchantId: call.scope.merchantId };
  } });
  function Merchant({ id }: { readonly id: 'a' | 'b' }) {
    const merchantId = 'merchant-' + id;
    const action = useAction(implementation, () => merchantId, [merchantId]);
    const record = useResource(() => of<Observation<{ merchantId: string; revision: 1 }>>({ status: 'ready', generation: 1,
      observedAt: Date.now(), value: { merchantId, revision: 1 } }), [merchantId]);
    return h(Fragment, null,
      h(Context<{ merchantId: string; revision: 1 }>, { name: 'record', observation: record.state, render: value => value }),
      h(Offer<typeof save>, { action }),
      h(Expose<typeof local, PartialSurfaceBindings<typeof local>>, { surface: local, value: { views: { record: record.observations$ }, actions: { save: action } } }));
  }
  const merchant = implementModule(local, { Component: Merchant });
  const program = defineProgram({ input: z.null(), surface: composed.surface });
  const definition = defineApplication({ name: 'module-composition', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '按模块现场调用对应商家的能力', program: implementProgram(program, { modules: { merchant }, Component({ modules }) {
      const order = useObservable(() => layout, [layout]);
      return order.status === 'ready' ? h(Fragment, null, order.value.map(id =>
        h(modules.merchant.Component, { key: id, mount: composed.mounts[id].merchant, id }))) : null;
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* (options) {
    requests.push(options);
    if (requests.length === 1) yield* tool('m_1_b_8_merchant__save_record', 'module-b-native', { revision: 1 });
    else yield* text('已保存商家b');
  });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const moduleA = session.module(composed.mounts.a.merchant), moduleB = session.module(composed.mounts.b.merchant);
    const originalA = moduleA.resolveAction('save', 'merchant-a');
    const originalB = moduleB.resolveAction('save', 'merchant-b');
    assert.equal(moduleA.viewResource('record'), session.viewResource('a.merchant.record'));
    assert.deepEqual(originalA.scope, { merchantId: 'merchant-a' });
    assert.deepEqual(await session.readView('b.merchant.record'), { merchantId: 'merchant-b', revision: 1 });
    layout.next(['b', 'a']);
    assert.equal(session.resolveAction('a.merchant.save', 'merchant-a').bindingId, originalA.bindingId);
    assert.equal(session.resolveAction('b.merchant.save', 'merchant-b').bindingId, originalB.bindingId);
    await session.submit({ requestId: 'native-b', content: '只保存商家b', intent: 'question', delivery: 'followup', scope: null });
    await session.whenIdle();
    const names = requests[0]!.tools?.map(value => value.name) ?? [];
    assert.ok(names.includes('m_1_a_8_merchant__save_record'));
    assert.ok(names.includes('m_1_b_8_merchant__save_record'));
    assert.deepEqual(effects, [{ merchantId: 'merchant-b', source: 'tool' }]);

    held = true;
    const running = originalA({ revision: 1 }); await entered.promise;
    layout.next(['b']);
    assert.throws(() => originalA({ revision: 1 }), ActionBindingUnavailableError);
    assert.equal((await firstValueFrom(session.observeView('a.merchant.record'))).status, 'pending');
    assert.equal(session.resolveAction('b.merchant.save', 'merchant-b').bindingId, originalB.bindingId);
    assert.deepEqual(await originalB({ revision: 1 }).result, { merchantId: 'merchant-b' });
    finish.resolve();
    assert.deepEqual(await running.result, { merchantId: 'merchant-a' });
    layout.next(['a', 'b']);
    const remounted = moduleA.resolveAction('save', 'merchant-a');
    assert.notEqual(remounted.bindingId, originalA.bindingId);
    assert.throws(() => originalA({ revision: 1 }), ActionBindingUnavailableError);
    assert.deepEqual(effects, [{ merchantId: 'merchant-b', source: 'tool' },
      { merchantId: 'merchant-b', source: 'participant' }, { merchantId: 'merchant-a', source: 'participant' }]);
    assert.equal(requests.length, 2);
    assert.deepEqual(app.errors, []);
    session.release();
  } finally { finish.resolve(); layout.complete(); await app.dispose(); }
});

test('嵌套模块静态安装一次Jobs和文件owner，动态双目标与双会话隔离，撤挂后已受理操作继续结算', { timeout: 15_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-module-dependencies-'));
  const file = join(root, 'accepted.json');
  const state = z.discriminatedUnion('status', [z.object({ status: z.literal('running') }),
    z.object({ status: z.literal('complete'), merchantId: z.string(), sessionId: z.string() })]);
  const accepted = z.object({ command: z.string(), reference: z.string(), merchantId: z.string(), sessionId: z.string(), state });
  await writeFile(file, '[]');
  const read = async () => accepted.array().parse(JSON.parse(await readFile(file, 'utf8')));
  const persist = async (records: readonly z.input<typeof accepted>[]) => {
    await writeFile(file + '.next', JSON.stringify(records)); await rename(file + '.next', file);
  };
  const changed = new Subject<void>();
  const operation = defineOperation({ kind: 'module-recorded-work', version: 1, command: z.string(), reference: z.string(),
    state, settled: value => value.status === 'complete' ? value : undefined });
  const owner = implementOperation(operation, {
    async locate(command, access) {
      const saved = (await read()).find(item => item.command === command);
      if (!saved) return { status: 'unknown' };
      if (saved.sessionId !== access.sessionId) return { status: 'rejected', reason: 'foreign-session' };
      return { status: 'accepted', reference: saved.reference, state: saved.state };
    },
    observe(reference, access) {
      return observeResource({ key$: of(reference), async read(value) {
        const saved = (await read()).find(item => item.reference === value);
        assert.ok(saved); assert.equal(saved.sessionId, access.sessionId);
        return saved.state;
      }, invalidations: () => changed });
    },
  });
  const save = defineAction({ name: 'save_record', description: '文件持久受理并完成当前商家工作',
    scope: z.object({ merchantId: z.string(), sessionId: z.string() }), input: z.string(), output: z.string(),
    operation: { definition: operation, command: input => input, reference: reference => reference } });
  const entered = Promise.withResolvers<void>(), finish = Promise.withResolvers<void>();
  const action = implementAction(save, { operationOwner: owner, async execute(command, call) {
    assert.equal(call.scope.sessionId, call.sessionId);
    const reference = 'operation:' + command;
    const record = { command, reference, ...call.scope, state: { status: 'running' as const } };
    await persist([...(await read()), record]); changed.next();
    if (command === 'held-a') { entered.resolve(); await finish.promise; }
    call.signal.throwIfAborted();
    const complete = { status: 'complete' as const, ...call.scope };
    await persist((await read()).map(item => item.command === command ? { ...item, state: complete } : item));
    changed.next(); return reference;
  } });
  const local = defineSurface({ views: {}, actions: { save }, operations: { work: operation } });
  const jobs = createNativeJobs({ registry: { kind: 'local' } });
  let installations = 0;
  const native: NativeCapability = { install(ctx, home) { installations++; return jobs.install(ctx, home); }, dispose: () => jobs.dispose() };
  function Commands({ merchantId, sessionId }: { readonly merchantId: string; readonly sessionId: string }) {
    const bound = useAction(action, () => ({ merchantId, sessionId }), [merchantId, sessionId]);
    const work = useOperation(owner, () => sessionId + ':' + merchantId, [sessionId, merchantId]);
    return h(Expose<typeof local, PartialSurfaceBindings<typeof local>>, { surface: local,
      value: { actions: { save: bound }, operations: { work } } });
  }
  const commands = implementNativeModule(local, { Component: Commands, operations: [owner], native: [native] });
  const nested = composeSurface({ commands: local });
  const merchant = implementModule(nested.surface, { modules: { commands },
    Component({ merchantId, sessionId, modules }: { readonly merchantId: string; readonly sessionId: string;
      readonly modules: { readonly commands: typeof commands } }) {
      return h(modules.commands.Component, { mount: nested.mounts.commands, merchantId, sessionId });
    } });
  const composed = composeSurface({ merchants: collectionSurface(nested.surface) });
  const firstLayout = new BehaviorSubject<readonly string[]>(['a', 'b']), peerLayout = new BehaviorSubject<readonly string[]>(['a', 'b']);
  const definition = defineApplication({ name: 'module-static-dependencies', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '显式调用各会话当前商家模块',
    program: implementProgram(defineProgram({ input: z.null(), surface: composed.surface }), { modules: { merchant },
      Component({ conversation, modules }) {
        const layout = useObservable(conversation.identity.sessionId === 'first' ? firstLayout : peerLayout);
        return layout.status === 'ready' ? h(Fragment, null, layout.value.map(merchantId => h(modules.merchant.Component, {
          key: merchantId, mount: composed.mounts.merchants.instance(merchantId), merchantId, sessionId: conversation.identity.sessionId,
        }))) : null;
      } }),
  })] });
  let models = 0;
  const app = await boot(join(root, 'home'), definition, async function* () { models++; throw new Error('MODULE_QUERY_MUST_NOT_START_MODEL'); });
  try {
    const first = await app.host.create('work', { workspaceId: app.workspace.workspaceId, sessionId: 'first' });
    const peer = await app.host.create('work', { workspaceId: app.workspace.workspaceId, sessionId: 'peer' });
    try {
      assert.equal(installations, 1); assert.deepEqual(await jobs.toolNames.read(), []);
      assert.deepEqual(await read(), [], '创建模块、查询原生资源不得受理业务');
      const client = (session: typeof first, key: string) => session.collection('merchants').instance(key).module(nested.mounts.commands);
      const a = client(first, 'a'), b = client(first, 'b'), peerA = client(peer, 'a');
      const originalA = a.resolveAction('save', { merchantId: 'a', sessionId: 'first' });
      const originalB = b.resolveAction('save', { merchantId: 'b', sessionId: 'first' });
      const originalPeer = peerA.resolveAction('save', { merchantId: 'a', sessionId: 'peer' });
      assert.notEqual(originalA.bindingId, originalB.bindingId); assert.notEqual(originalA.bindingId, originalPeer.bindingId);
      assert.equal(a.operations.work().command, 'first:a'); assert.equal(peerA.operations.work().command, 'peer:a');
      const invocation = originalA('held-a'); await entered.promise;
      const running = await first.attachOperation(operation, 'operation:held-a');
      const settled = running.settled();
      firstLayout.next(['b']);
      assert.throws(() => originalA('after-unmount'), ActionBindingUnavailableError);
      assert.equal(b.resolveAction('save', { merchantId: 'b', sessionId: 'first' }).bindingId, originalB.bindingId);
      assert.equal(peerA.resolveAction('save', { merchantId: 'a', sessionId: 'peer' }).bindingId, originalPeer.bindingId);
      assert.deepEqual(await first.locateOperation(operation, 'held-a'), { status: 'accepted', reference: 'operation:held-a', state: { status: 'running' } });
      assert.deepEqual(await peer.locateOperation(operation, 'held-a'), { status: 'rejected', reason: 'foreign-session' });
      assert.equal(await originalPeer('peer-a').result, 'operation:peer-a');
      finish.resolve();
      assert.equal(await invocation.result, 'operation:held-a');
      assert.deepEqual(await settled, { status: 'complete', merchantId: 'a', sessionId: 'first' });
      firstLayout.next([]);
      assert.deepEqual(await jobs.forSession(first).state.read(), [], '撤下所有模块声明仍保留应用安装的正式Jobs注册表');
      const recovered = await first.attachOperation(operation, 'operation:held-a');
      assert.deepEqual(await recovered.settled(), { status: 'complete', merchantId: 'a', sessionId: 'first' });
      assert.deepEqual((await read()).map(item => [item.command, item.merchantId, item.sessionId]), [['held-a', 'a', 'first'], ['peer-a', 'a', 'peer']]);
      assert.equal(installations, 1); assert.equal(models, 0); assert.deepEqual(app.errors, []);
    } finally { first.release(); peer.release(); }
  } finally { finish.resolve(); firstLayout.complete(); peerLayout.complete(); changed.complete(); await app.dispose(); }
});

test('跨模块资源关系保留pending，关联事实未完成前不把旧ready拼入下一模型步', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-module-relation-'));
  const merchantSchema = z.object({ merchantId: z.string(), revision: z.number(), operationId: z.string() });
  const operationSchema = z.object({ operationId: z.string(), status: z.literal('registered') });
  const merchantSurface = defineSurface({ views: { facts: resourceView(merchantSchema) }, actions: {} });
  const operationSurface = defineSurface({ views: { facts: resourceView(operationSchema) }, actions: {} });
  const composed = composeSurface({ merchant: merchantSurface, operation: operationSurface });
  const source = new BehaviorSubject<Observation<z.output<typeof merchantSchema>>>({ status: 'ready', generation: 1,
    observedAt: Date.now(), value: { merchantId: 'merchant-a', revision: 1, operationId: 'operation-a' } });
  const loading = Promise.withResolvers<void>(), nextOperation = Promise.withResolvers<z.output<typeof operationSchema>>();
  const related = source.pipe(switchMap(state => state.status === 'ready'
    ? observeResource({ key$: of(state.value.operationId), read(operationId) {
      if (operationId === 'operation-a') return { operationId, status: 'registered' as const };
      loading.resolve(); return nextOperation.promise;
    } }) : of(state)));
  function Merchant() {
    const resource = useResource(() => source, [source]);
    return h(Fragment, null,
      h(Context<z.output<typeof merchantSchema>>, { name: 'facts', observation: resource.state, render: value => value }),
      h(Expose<typeof merchantSurface, PartialSurfaceBindings<typeof merchantSurface>>, { surface: merchantSurface, value: { views: { facts: resource.observations$ } } }));
  }
  function Operation() {
    const resource = useResource(() => related, [related]);
    return h(Fragment, null,
      h(Context<z.output<typeof operationSchema>>, { name: 'facts', observation: resource.state, render: value => value }),
      h(Expose<typeof operationSurface, PartialSurfaceBindings<typeof operationSurface>>, { surface: operationSurface, value: { views: { facts: resource.observations$ } } }));
  }
  const requests: GenerateOptions[] = [];
  const definition = defineApplication({ name: 'module-relation', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '只根据当前商家和关联操作事实判断',
    program: implementProgram(defineProgram({ input: z.null(), surface: composed.surface }), { Component() {
      return h(Fragment, null, h(Module, { mount: composed.mounts.merchant }, h(Merchant)),
        h(Module, { mount: composed.mounts.operation }, h(Operation)));
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* (options) {
    requests.push(options); yield* text('已读取当前关联事实');
  });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'read-a', content: '读取当前现场', intent: 'question', delivery: 'followup', scope: null });
    await session.whenIdle();
    source.next({ status: 'ready', generation: 2, observedAt: Date.now(),
      value: { merchantId: 'merchant-a', revision: 2, operationId: 'operation-b' } });
    await loading.promise;
    assert.equal((await firstValueFrom(session.observeView('operation.facts'))).status, 'pending');
    let readFinished = false;
    const read = session.readView('operation.facts').then(value => { readFinished = true; return value; });
    await session.submit({ requestId: 'read-b', content: '按变更后的现场继续', intent: 'followup', delivery: 'followup', scope: null });
    await setImmediate();
    assert.equal(requests.length, 1); assert.equal(readFinished, false);
    nextOperation.resolve({ operationId: 'operation-b', status: 'registered' });
    await session.whenIdle();
    assert.deepEqual(await read, { operationId: 'operation-b', status: 'registered' });
    assert.equal(requests.length, 2);
    const runtimeSnapshot = z.object({ kind: z.literal('runtime-context'), form: z.literal('snapshot'),
      sections: z.object({ name: z.string(), text: z.string() }).array() });
    const current = requests[1]!.messages.map(message => runtimeSnapshot.safeParse(message.source))
      .filter(result => result.success).at(-1)?.data;
    assert.ok(current);
    const merchantSection = current.sections.find(section => section.name === 'm_8_merchant__facts');
    const operationSection = current.sections.find(section => section.name === 'm_9_operation__facts');
    assert.ok(merchantSection); assert.ok(operationSection);
    assert.deepEqual(merchantSchema.parse(JSON.parse(merchantSection.text)),
      { merchantId: 'merchant-a', revision: 2, operationId: 'operation-b' });
    assert.deepEqual(operationSchema.parse(JSON.parse(operationSection.text)),
      { operationId: 'operation-b', status: 'registered' });
    assert.deepEqual(app.errors, []);
    session.release();
  } finally { nextOperation.resolve({ operationId: 'operation-b', status: 'registered' }); source.complete(); await app.dispose(); }
});

test('显式可选模块的pending和error不阻断其他模块，恢复贡献Context而失效撤下旧现场且保留资源状态', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-optional-module-'));
  const factsSchema = z.object({ merchantId: z.string(), revision: z.number() });
  const optional = new BehaviorSubject<Observation<z.output<typeof factsSchema>>>({ status: 'pending', generation: 1 });
  const save = defineAction({ name: 'save_record', description: '只执行当前明确就绪模块的保存',
    scope: z.object({ merchantId: z.string() }), input: z.null(), output: z.string() });
  const local = defineSurface({ views: { facts: resourceView(factsSchema) }, actions: { save } });
  const composed = composeSurface({ optional: local, ready: local });
  const effects: string[] = [], requests: GenerateOptions[] = [];
  const implementation = implementAction(save, { execute(_input, call) { effects.push(call.scope.merchantId); return call.scope.merchantId; } });
  function Merchant({ isOptional }: { readonly isOptional: boolean }) {
    const merchantId = isOptional ? 'merchant-optional' : 'merchant-ready';
    const resource = useResource(() => isOptional ? optional : of<Observation<z.output<typeof factsSchema>>>({
      status: 'ready', generation: 1, observedAt: Date.now(), value: { merchantId, revision: 1 },
    }), [isOptional, merchantId]);
    const action = useAction(implementation, () => ({ merchantId }), [merchantId]);
    const ready = resource.state.status === 'ready';
    return h(Fragment, null,
      h(Context<z.output<typeof factsSchema>>, { name: 'facts', observation: resource.state, render: value => value,
        ...(isOptional ? { required: false } : {}) }),
      h(Offer<typeof save>, { action, enabled: ready }),
      h(Expose<typeof local, PartialSurfaceBindings<typeof local>>, { surface: local,
        value: { views: { facts: resource.observations$ }, actions: ready ? { save: action } : {} } }));
  }
  let expectedContext: z.output<typeof factsSchema> | undefined;
  const definition = defineApplication({ name: 'optional-module-context', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '每次只保存就绪模块，缺失的可选事实不代表其他商家的情况',
    program: implementProgram(defineProgram({ input: z.null(), surface: composed.surface }), { Component() {
      return h(Fragment, null, h(Module, { mount: composed.mounts.optional }, h(Merchant, { isOptional: true })),
        h(Module, { mount: composed.mounts.ready }, h(Merchant, { isOptional: false })));
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* (options) {
    requests.push(options);
    const runtimeSnapshot = z.object({ kind: z.literal('runtime-context'), form: z.literal('snapshot'),
      sections: z.object({ name: z.string(), text: z.string() }).array() });
    const current = options.messages.map(message => runtimeSnapshot.safeParse(message.source))
      .filter(result => result.success).at(-1)?.data;
    assert.ok(current);
    const optionalSection = current.sections.find(section => section.name === 'm_8_optional__facts');
    if (expectedContext) {
      assert.ok(optionalSection);
      assert.deepEqual(factsSchema.parse(JSON.parse(optionalSection.text)), expectedContext);
    } else assert.equal(optionalSection, undefined, '可选模块失效后不能继续贡献旧ready现场');
    const names = options.tools?.map(value => value.name) ?? [];
    assert.ok(names.includes('m_5_ready__save_record'));
    assert.equal(names.includes('m_8_optional__save_record'), expectedContext !== undefined);
    if (requests.length % 2 === 1) yield* tool('m_5_ready__save_record', 'ready-module-' + requests.length, null);
    else yield* text('已保存就绪模块');
  });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const run = async (requestId: string) => {
      await session.submit({ requestId, content: '保存当前就绪模块', intent: 'question', delivery: 'followup', scope: null });
      await session.whenIdle();
    };
    assert.equal((await firstValueFrom(session.observeView('optional.facts'))).status, 'pending');
    await run('optional-pending');
    assert.equal(requests.length, 2); assert.equal(effects.length, 1);
    const queryFailure = new Error('OPTIONAL_FACTS_UNAVAILABLE');
    optional.next({ status: 'error', generation: 2, error: queryFailure });
    const failed = await firstValueFrom(session.observeView('optional.facts'));
    assert.equal(failed.status, 'error');
    if (failed.status === 'error') assert.equal(failed.error, queryFailure);
    await run('optional-error');
    assert.equal(requests.length, 4); assert.equal(effects.length, 2);
    expectedContext = { merchantId: 'merchant-optional', revision: 7 };
    optional.next({ status: 'ready', generation: 3, observedAt: Date.now(), value: expectedContext });
    assert.deepEqual(await session.readView('optional.facts'), expectedContext);
    await run('optional-ready');
    assert.equal(requests.length, 6); assert.equal(effects.length, 3);
    expectedContext = undefined;
    optional.next({ status: 'pending', generation: 4 });
    session.resolveAction('ready.save', { merchantId: 'merchant-ready' });
    assert.equal((await firstValueFrom(session.observeView('optional.facts'))).status, 'pending');
    assert.throws(() => session.resolveAction('optional.save', { merchantId: 'merchant-optional' }), ActionBindingUnavailableError);
    await run('optional-invalidated');
    assert.equal(requests.length, 8);
    assert.deepEqual(effects, ['merchant-ready', 'merchant-ready', 'merchant-ready', 'merchant-ready']);
    assert.deepEqual(app.errors, []);
    session.release();
  } finally { optional.complete(); await app.dispose(); }
});

test('模块挂载在真实Program中拒绝其他root的token、伪token、重复挂载和错误local合同', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-module-ownership-'));
  const local = defineSurface({ views: { count: view(z.number()) }, actions: {} });
  const other = defineSurface({ views: { count: view(z.number()) }, actions: {} });
  const composition = composeSurface({ counter: local }), foreign = composeSurface({ counter: local });
  const fake = Object.freeze({ ...composition.mounts.counter });
  const source = of(7);
  const expose = (surface: typeof local) => h(Expose<typeof local, PartialSurfaceBindings<typeof local>>, { surface, value: { views: { count: source } } });
  let unauthorizedRenders = 0;
  const uninstalled = implementModule(local, { Component() { unauthorizedRenders++; return expose(local); } });
  const cases = [
    { id: 'uninstalled', code: /REACTIVE_MODULE_NOT_INSTALLED/u,
      Component: () => h(uninstalled.Component, { mount: composition.mounts.counter }) },
    { id: 'wrong-root', code: /REACTIVE_MODULE_ROOT_MISMATCH/u,
      Component: () => h(Module, { mount: foreign.mounts.counter }, expose(local)) },
    { id: 'fake-token', code: /REACTIVE_MODULE_MOUNT_INVALID/u,
      Component: () => h(Module, { mount: fake }, expose(local)) },
    { id: 'duplicate', code: /REACTIVE_MODULE_MOUNT_DUPLICATE/u,
      Component: () => h(Fragment, null, h(Module, { mount: composition.mounts.counter }, expose(local)),
        h(Module, { mount: composition.mounts.counter }, expose(local))) },
    { id: 'wrong-local', code: /REACTIVE_MODULE_SURFACE_MISMATCH/u,
      Component: () => h(Module, { mount: composition.mounts.counter }, expose(other)) },
  ];
  const definition = defineApplication({ name: 'module-ownership', admit: async () => {}, environments: cases.map(scenario => defineEnvironment({
    id: scenario.id, instructions: '拒绝错误装配',
    program: implementProgram(defineProgram({ input: z.null(), surface: composition.surface }), { Component: scenario.Component }),
  })) });
  let modelCalls = 0;
  const app = await boot(join(root, 'home'), definition, async function* () { modelCalls++; yield* text('不应运行'); });
  try {
    for (const scenario of cases) {
      await assert.rejects(app.host.create(scenario.id, { workspaceId: app.workspace.workspaceId }), scenario.code, scenario.id);
    }
    assert.equal(unauthorizedRenders, 0, '未列入Program静态图的模块在业务组件求值前失败');
    assert.equal(modelCalls, 0);
  } finally { await app.dispose(); }
});

test('静态模块SDK复用文件资源和局部消费函数，选择边界不恢复隐藏能力，观察变化不执行命令', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-module-client-'));
  const file = join(root, 'record.json');
  const facts = z.object({ revision: z.number().int(), value: z.string() });
  await writeFile(file, JSON.stringify({ revision: 1, value: 'registered' }));
  const changed = new Subject<void>();
  let reads = 0, executions = 0, modelCalls = 0;
  const observations$ = observeResource({ key$: of(file), async read(path) {
    reads++; return facts.parse(JSON.parse(await readFile(path, 'utf8')));
  }, invalidations: () => changed });
  const save = defineAction({ name: 'save_record', description: '显式保存文件', scope: z.string(), input: facts, output: facts });
  const implementation = implementAction(save, { async execute(input) {
    executions++; await writeFile(file, JSON.stringify(input)); changed.next(); return input;
  } });
  const local = defineSurface({ views: { facts: resourceView(facts), secret: view(z.string()) }, actions: { save } });
  const review = selectSurface(local, { views: ['facts'] });
  const nested = composeSurface({ record: local });
  const restricted = selectSurface(nested.surface, { views: ['record.facts'] });
  const composed = composeSurface({ full: local, review, restricted });
  const foreign = composeSurface({ full: local });
  function Record() {
    const action = useAction(implementation, () => 'record-a', []);
    return h(Expose<typeof local, PartialSurfaceBindings<typeof local>>, { surface: local,
      value: { views: { facts: observations$, secret: of('operator-only') }, actions: { save: action } } });
  }
  const program = defineProgram({ input: z.null(), surface: composed.surface });
  const definition = defineApplication({ name: 'module-client-files', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '文件查询与命令分别显式使用。', program: implementProgram(program, { Component: () => h(Fragment, null,
      h(Module, { mount: composed.mounts.full }, h(Record)),
      h(Module, { mount: composed.mounts.review }, h(Record)),
      h(Module, { mount: composed.mounts.restricted }, h(Module, { mount: nested.mounts.record }, h(Record)))) }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () {
    modelCalls++; throw new Error('MODULE_CLIENT_FILE_QUERY_MUST_NOT_START_MODEL');
  });
  const consume = (client: SurfaceClient<typeof local>) => client.readView('facts');
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const original = session.viewResource('full.facts');
    await original.read();
    await session.readView('review.facts'); await session.readView('restricted.record.facts');
    const beforeProjection = reads;
    const module = session.module(composed.mounts.full), reviewed = session.module(composed.mounts.review);
    assert.equal(reads, beforeProjection, '构造投射不重新打开业务读取');
    assert.equal(module.viewResource('facts'), original);
    assert.equal(reviewed.viewResource('facts'), session.viewResource('review.facts'));
    assert.equal(composed.mounts.full.surface, local); assert.equal(composed.mounts.full.selected, local);
    assert.equal(composed.mounts.review.surface, local); assert.equal(composed.mounts.review.selected, review.surface);
    assert.equal(module.surface, local); assert.equal(reviewed.surface, review.surface);
    assert.deepEqual(await consume(module), { revision: 1, value: 'registered' });
    assert.equal(reads, beforeProjection);
    assert.throws(() => session.module(foreign.mounts.full), /REACTIVE_MODULE_ROOT_MISMATCH/u);
    assert.throws(() => session.module({ ...composed.mounts.full }), /REACTIVE_MODULE_MOUNT_INVALID/u);
    const unsafe = reviewed as SurfaceClient<typeof local>;
    assert.throws(() => unsafe.viewResource('secret'), /REACTIVE_VIEW_NOT_FOUND/u);
    assert.throws(() => unsafe.resolveAction('save', 'record-a'), /REACTIVE_ACTION_SLOT_UNKNOWN/u);
    assert.throws(() => session.module(composed.mounts.restricted).module(nested.mounts.record), /REACTIVE_MODULE_SLOT_NOT_VISIBLE/u);
    const updated = firstValueFrom(module.observeView('facts').pipe(filter(value => value.status === 'ready' && value.value.revision === 2)));
    await writeFile(file, JSON.stringify({ revision: 2, value: 'changed-externally' })); changed.next();
    await updated;
    assert.deepEqual(await consume(module), { revision: 2, value: 'changed-externally' });
    assert.equal(executions, 0);
    const command = module.resolveAction('save', 'record-a');
    assert.deepEqual(await command({ revision: 3, value: 'explicit-command' }).result, { revision: 3, value: 'explicit-command' });
    assert.equal(executions, 1);
    await firstValueFrom(module.observeView('facts').pipe(filter(value => value.status === 'ready' && value.value.revision === 3)));
    await consume(module); await reviewed.readView('facts');
    assert.equal(executions, 1); assert.equal(modelCalls, 0);
    assert.deepEqual(app.errors, []);
    session.release();
  } finally { changed.complete(); await app.dispose(); }
});
