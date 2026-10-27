import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { createElement, Fragment, useEffect, useMemo } from 'react';
import { Observable, distinctUntilChanged, filter, firstValueFrom, map } from 'rxjs';
import { Context, Expose, Offer, Skill, useAction, useObservable, useShared } from '@hanger-source/dsh-reactive/program';
import { defineAction, implementAction, defineApplication, defineEnvironment, defineProgram, defineSurface, observeResource, view, z, type Continuation } from '@hanger-source/dsh-reactive';
import type { GenerateOptions } from '@deepseek-ai/dsh-llm';
import { SessionId } from '@deepseek-ai/dsh-session';
import { mountApplication } from '@hanger-source/dsh-reactive-dsh';
import { boot, text, tool } from './native-support.ts';

test('原生 Loader 拒绝环境插件时启动失败并释放声明，移除拒绝后可在同一宿主重新装配', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-preset-failure-'));
  const app = await boot(join(root, 'home'), defineApplication({ name: 'resident', admit: async () => {}, environments: [defineEnvironment({
    id: 'existing', instructions: '宿主已有应用',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component: () => null }),
  })] }), async function* () { yield* text('observed'); });
  const definition = defineApplication({ name: 'load-checked', admit: async () => {}, environments: [defineEnvironment({
    id: 'delivery', instructions: '拒绝后重新装配',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component: () => null }),
  })] });
  let rejected = 0;
  const rejectLoad = app.native.on('loader/patch-context', (entry, next) => {
    if (entry.options.name === 'cordis:reactive:load-checked:delivery') { rejected++; throw new Error('ENVIRONMENT_PLUGIN_REJECTED_9f61'); }
    return next();
  });
  try {
    try { await assert.rejects(mountApplication(app.native, () => definition), /REACTIVE_PRESET_INVALID:/u); }
    finally { rejectLoad(); }
    assert.ok(rejected > 0);
    assert.equal(Object.hasOwn(app.native.loader.builtins, 'reactive:load-checked:delivery'), false);
    const mounted = await mountApplication(app.native, () => definition);
    try {
      const conversation = await mounted.create('delivery', { workspaceId: app.workspace.workspaceId });
      const agent = app.native.agents.get(SessionId(conversation.id))!;
      assert.equal(app.native.agentPresets.composedPreset(agent.ctx), 'reactive:load-checked:delivery');
      conversation.release();
    } finally { await mounted.dispose(); }
    assert.deepEqual(app.errors, []);
  } finally { rejectLoad(); await app.dispose(); }
});

test('真实 dsh 首次请求取得同一输入的上下文和能力，空闲变化可订阅且不唤醒模型，多环境会话隔离', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-scopes-'));
  const owners = new Map([['merchant-a', { ready: true, revision: 1 }], ['merchant-b', { ready: false, revision: 9 }]]);
  const listeners = new Set<() => void>();
  const calls: GenerateOptions[] = [];
  const prepare = defineAction({ name: 'prepare_merchant', description: '正式准备', scope: z.object({ merchantId: z.string().nullable() }),
    input: z.object({}).strict(), output: z.object({ ok: z.boolean() }) });
  const prepareImplementation = implementAction(prepare, {
    async execute() { return { ok: true }; }
  });
  const recordSchema = z.object({ merchantId: z.string(), ready: z.boolean(), revision: z.number() }).nullable();
  const surface = defineSurface({ views: { record: view(recordSchema) }, actions: {} });
  const definition = defineApplication({ name: 'operations', admit: async () => {},
    environments: ['delivery', 'analysis'].map(id => defineEnvironment({ id,
      instructions: id === 'delivery' ? 'ENV_DELIVERY_OWNER_f82c' : 'ENV_ANALYSIS_OWNER_4d91',
      program: implementProgram(defineProgram({ input: z.object({ merchantId: z.string() }), surface }), { Component({ conversation }) {
        const merchantId$ = useMemo(() => conversation.inputs$.pipe(map(inputs => {
          return inputs.at(-1)?.scope.merchantId ?? null;
        }), distinctUntilChanged()), [conversation]);
        const selection = useObservable(() => merchantId$, [merchantId$]);
        const recordState$ = useShared(() => observeResource({ key$: merchantId$, read: key => key === null ? null : { merchantId: key, ...owners.get(key)! },
          invalidations: () => new Observable<void>(subscriber => {
            const invalidate = () => { subscriber.next(); };
            listeners.add(invalidate);
            return () => { listeners.delete(invalidate); };
          }) }), [merchantId$]);
        const observed = useObservable(() => recordState$, [recordState$]);
        const current = observed.status === 'ready' ? observed.value : observed;
        const record = useMemo(() => recordState$.pipe(filter(state => state.status === 'ready'), map(state => state.value)), [recordState$]);
        const action = useAction(prepareImplementation, () => ({ merchantId: selection.status === 'ready' ? selection.value : null }),
          [selection.status === 'ready' ? selection.value : null]);
        return createElement(Fragment, null,
          Context({ name: 'business', observation: current, render: value => value }),
          Offer({ name: 'prepare_merchant', action, enabled: current.status === 'ready' && current.value?.ready === true }),
          Expose({ surface, value: { views: { record } } }));
      } }) })),
  });
  const app = await boot(join(root, 'sessions'), definition, async function* (options) { calls.push(options); yield* text('observed'); });
  try {
    const a = await app.host.create('delivery', { workspaceId: app.workspace.workspaceId });
    const b = await app.host.create('analysis', { workspaceId: app.workspace.workspaceId });
    for (const [session, presetId] of [[a, 'reactive:operations:delivery'], [b, 'reactive:operations:analysis']] as const) {
      const agent = app.native.agents.get(SessionId(session.id))!;
      assert.equal(agent.session.header.agentPreset, presetId);
      assert.equal(app.native.agentPresets.composedPreset(agent.ctx), presetId);
    }
    await a.submit({ requestId: 'page-a', content: '核对当前商家', intent: 'question', delivery: 'followup' as const, scope: { merchantId: 'merchant-a' } });
    await a.whenIdle();
    await b.submit({ requestId: 'page-b', content: '核对另一个商家', intent: 'question', delivery: 'followup' as const, scope: { merchantId: 'merchant-b' } });
    await b.whenIdle();
    assert.deepEqual(app.errors, []);
    assert.equal(calls.length, 2);
    const first = calls[0]!;
    assert.ok(JSON.stringify(first.messages).includes('ENV_DELIVERY_OWNER_f82c'));
    assert.ok(!JSON.stringify(first.messages).includes('ENV_ANALYSIS_OWNER_4d91'));
    assert.ok(JSON.stringify(calls[1]!.messages).includes('ENV_ANALYSIS_OWNER_4d91'));
    assert.ok(!JSON.stringify(calls[1]!.messages).includes('ENV_DELIVERY_OWNER_f82c'));
    assert.ok(JSON.stringify(first.messages).includes('merchant-a'));
    assert.ok(first.tools?.some(value => value.name === 'prepare_merchant'));
    assert.ok(!calls[1]!.tools?.some(value => value.name === 'prepare_merchant'));
    const observed: unknown[] = [];
    const observation = a.views.record.subscribe(value => observed.push(value));
    owners.set('merchant-a', { ready: false, revision: 2 });
    for (const notify of listeners) notify();
    await new Promise<void>(resolve => setImmediate(resolve));
    assert.ok(JSON.stringify(observed.at(-1)).includes('"revision":2'));
    assert.equal(calls.length, 2);
    observation.unsubscribe();
    await a.submit({ requestId: 'follow-a', content: '继续核对', intent: 'followup', delivery: 'followup' as const, scope: { merchantId: 'merchant-a' } });
    await a.whenIdle();
    assert.ok(!calls[2]!.tools?.some(value => value.name === 'prepare_merchant'));
    assert.equal((await firstValueFrom(a.request('page-a').state$.pipe(filter(state => state.status === 'ready'), map(state => state.value.activity))))?.reply, 'observed');
    assert.equal(await firstValueFrom(b.request('page-a').state$.pipe(filter(state => state.status === 'ready'), map(state => state.value.activity))), null);
    assert.deepEqual(app.errors, []);
  } finally { await app.dispose(); }
});

test('正式能力沿真实原生调用执行，等待释放 Goal，持久重启后精确续接并完成原工作，重复投递不重做副作用', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-continuation-'));
  let continuation: Continuation | undefined;
  let operationCount = 0;
  let ready = false;
  let calls = 0;
  const target = z.object({ merchantId: z.string() }).nullable();
  const prepare = defineAction({ name: 'prepare', description: '关联原业务任务并等待', scope: target,
    input: z.object({}).strict(), output: z.object({ waiting: z.boolean() }), observe: output => output });
  const prepareImplementation = implementAction(prepare, {
    async execute(_input, call) {
      operationCount++;
      assert.ok(call.continuation);
      continuation = call.continuation;
      await call.wait({ id: 'original-wait', operation: { operationId: 'operation-1' } });
      return { waiting: true };
    }
  });
  const finish = defineAction({ name: 'finish', description: '核验原业务结果后完成', scope: target,
    input: z.object({}).strict(), output: z.object({ completed: z.boolean() }) });
  const finishImplementation = implementAction(finish, {
    async execute(_input, call) { assert.equal(ready, true); call.completeGoal(); return { completed: true }; }
  });
  const definition = defineApplication({ name: 'operations', admit: async () => {}, environments: [defineEnvironment({
    id: 'delivery', instructions: '按正式业务能力处理原操作。',
    program: implementProgram(defineProgram({ input: z.object({ merchantId: z.string() }), surface: defineSurface({ views: {}, actions: {} }) }), { Component({ conversation }) {
        const inputs = useObservable(() => conversation.inputs$, [conversation]);
        const selection = inputs.status === 'ready' ? inputs.value.at(-1)?.scope ?? null : null;
        const preparing = useAction(prepareImplementation, () => selection, [selection?.merchantId ?? null]);
        const finishing = useAction(finishImplementation, () => selection, [selection?.merchantId ?? null]);
        return createElement(Fragment, null,
          Offer({ name: 'prepare', action: preparing, enabled: selection !== null }),
          Offer({ name: 'finish', action: finishing, enabled: selection !== null }));
      } }),
  })] });
  const respond = async function* () {
    calls++;
    if (!ready) yield* tool('prepare', randomUUID(), {});
    else if (calls === 2) yield* tool('finish', randomUUID(), {});
    else yield* text('原任务已完成');
  };
  let app = await boot(join(root, 'sessions'), definition, respond);
  let conversation = await app.host.create('delivery', { workspaceId: app.workspace.workspaceId });
  const id = conversation.id;
  try {
    const original = { requestId: 'original-page', content: '持续完成准备', intent: 'goal' as const, delivery: 'followup' as const, scope: { merchantId: 'merchant-a' } };
    await conversation.submit(original);
    await conversation.whenIdle();
    assert.deepEqual(app.errors, []);
    assert.equal(calls, 1); assert.equal(operationCount, 1);
    assert.equal(conversation.snapshot().waits.length, 1);
    assert.ok(continuation?.goal);
    assert.equal(continuation.inputs[0]?.requestId, original.requestId);
    await app.host.close(id); await app.dispose();
    ready = true;
    app = await boot(join(root, 'sessions'), definition, respond);
    const cold = await app.host.open(id);
    assert.equal(app.native.agents.get(SessionId(id)), undefined);
    cold.release();
    conversation = await app.host.resume(id, definition.environments[0].program.definition);
    const resumed = app.native.agents.get(SessionId(id))!;
    assert.equal(resumed.session.header.agentPreset, 'reactive:operations:delivery');
    assert.equal(app.native.agentPresets.composedPreset(resumed.ctx), 'reactive:operations:delivery');
    assert.equal(conversation.snapshot().waits[0]?.wait.id, 'original-wait');
    const notification = { requestId: 'result-operation-1', content: '原准备操作已结束，请核对正式结果。', intent: 'result' as const,
      scope: original.scope, result: { continuation: continuation!, wait: { id: 'original-wait', operation: { operationId: 'operation-1' } }, ready: true } };
    await conversation.submit(notification);
    await conversation.whenIdle();
    assert.deepEqual(app.errors, []);
    assert.equal(operationCount, 1); assert.equal(calls, 3);
    assert.equal((await firstValueFrom(conversation.request(original.requestId).state$.pipe(filter(state => state.status === 'ready'), map(state => state.value.activity))))?.reply, '原任务已完成');
    assert.equal(conversation.snapshot().waits.length, 0);
    await conversation.submit(notification); await conversation.submit(original);
    await conversation.whenIdle();
    assert.equal(operationCount, 1); assert.equal(calls, 3);
    await assert.rejects(conversation.submit({ ...notification, content: '同 ID 的不同内容' }), /REACTIVE_REQUEST_ID_CONFLICT/);
  } finally { await app.dispose(); }
});

test('保留组件程序的同宿主关闭与恢复使用新 Agent 的工具归属，原 Action 只受理一次执行', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-program-resume-'));
  let mounts = 0;
  let executions = 0;
  let modelCalls = 0;
  const inspect = defineAction({ name: 'inspect_owner', description: '读取当前真实工具归属', scope: z.null(),
    input: z.object({}).strict(), output: z.object({ sessionId: z.string() }) });
  const inspectImplementation = implementAction(inspect, {
    async execute(_input, call) { executions++; return { sessionId: call.sessionId }; }
  });
  const definition = defineApplication({ name: 'retained-program', admit: async () => {}, environments: [defineEnvironment({
    id: 'delivery', instructions: '核验恢复后的真实调用',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component() {
      useEffect(() => { mounts++; }, []);
      const inspectOwner = useAction(inspectImplementation, () => null, []);
      return Offer({ name: 'inspect_owner', action: inspectOwner });
    } }),
  })] });
  const app = await boot(join(root, 'sessions'), definition, async function* () {
    modelCalls++;
    if (modelCalls === 1) yield* tool('inspect_owner', randomUUID(), {});
    else yield* text('恢复后的工具已取得当前归属');
  });
  try {
    // 句柄继续持有同一 ProgramInstance；只替换原生驱动，不能借整宿主重启重建闭包。
    const original = await app.host.create('delivery', { workspaceId: app.workspace.workspaceId });
    const previous = app.native.agents.get(SessionId(original.id))!;
    assert.equal(mounts, 1);
    await app.host.close(original.id);
    const resumed = await app.host.resume(original.id);
    assert.notEqual(app.native.agents.get(SessionId(resumed.id)), previous);
    assert.equal(mounts, 1);
    await resumed.submit({ requestId: 'after-native-resume', content: '执行当前驱动的正式能力', intent: 'question', delivery: 'followup' as const, scope: null });
    await resumed.whenIdle();
    const request = await firstValueFrom(original.request('after-native-resume').state$.pipe(filter(state => state.status === 'ready'), map(state => state.value.activity)));
    assert.equal(request?.tools[0]?.state, 'returned');
    assert.equal(request?.reply, '恢复后的工具已取得当前归属');
    assert.equal(executions, 1);
    assert.equal(modelCalls, 2);
    assert.deepEqual(app.errors, []);
    resumed.release(); original.release();
  } finally { await app.dispose(); }
});

test('暂停之后迟到通知及并发重复投递不能恢复原 Goal，外部关联篡改在受理前失败', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-pause-'));
  let continuation: Continuation | undefined;
  let calls = 0;
  const waitForOwner = defineAction({ name: 'wait_for_owner', description: '登记等待',
    scope: z.object({ merchantId: z.string(), revision: z.number() }).nullable(), input: z.object({}).strict(), output: z.boolean() });
  const waitForOwnerImplementation = implementAction(waitForOwner, {
    async execute(_input, call) {
      assert.ok(call.continuation);
      continuation = call.continuation;
      await call.wait({ id: 'wait-1', operation: { id: 'operation-1', revision: 1 } });
      return true;
    }
  });
  const definition = defineApplication({ name: 'operations', admit: async () => {}, environments: [defineEnvironment({ id: 'delivery',
    instructions: '沿原操作等待结果',
    program: implementProgram(defineProgram({ input: z.object({ merchantId: z.string(), revision: z.number() }), surface: defineSurface({ views: {}, actions: {} }) }), { Component({ conversation }) {
        const inputs = useObservable(() => conversation.inputs$, [conversation]);
        const selection = inputs.status === 'ready' ? inputs.value.at(-1)?.scope ?? null : null;
        const waiting = useAction(waitForOwnerImplementation, () => selection, [selection?.merchantId ?? null, selection?.revision ?? null]);
        return Offer({ name: 'wait_for_owner', action: waiting, enabled: selection !== null });
      } }) })] });
  const app = await boot(join(root, 'sessions'), definition, async function* () { calls++; yield* tool('wait_for_owner', randomUUID(), {}); });
  try {
    const conversation = await app.host.create('delivery', { workspaceId: app.workspace.workspaceId });
    await conversation.submit({ requestId: 'original', intent: 'goal', delivery: 'followup' as const, content: '持续准备', scope: { merchantId: 'm', revision: 1 } });
    await conversation.whenIdle();
    const paused = await conversation.pause();
    assert.equal(paused.phase, 'paused');
    const notification = { requestId: 'result', intent: 'result' as const, content: '原操作完成', scope: { merchantId: 'm', revision: 1 },
      result: { continuation: continuation!, wait: { id: 'wait-1', operation: { id: 'operation-1', revision: 1 } }, ready: true } };
    await Promise.all([conversation.submit(notification), conversation.submit({ ...notification,
      scope: { revision: 1, merchantId: 'm' }, result: { ...notification.result, wait: { id: 'wait-1', operation: { revision: 1, id: 'operation-1' } } } })]);
    await conversation.submit(notification); await conversation.whenIdle();
    assert.equal(calls, 1);
    assert.equal((await firstValueFrom(conversation.request('result').state$.pipe(filter(state => state.status === 'ready'), map(state => state.value.activity))))?.state, 'accepted');
    await assert.rejects(conversation.submit({ ...notification, requestId: 'foreign', result: { ...notification.result,
      continuation: { ...continuation!, sessionId: 'another-session' } } }), /REACTIVE_CONTINUATION_SESSION_MISMATCH/);
    assert.equal(await firstValueFrom(conversation.request('foreign').state$.pipe(filter(state => state.status === 'ready'), map(state => state.value.activity))), null);
    assert.deepEqual(app.errors, []);
  } finally { await app.dispose(); }
});

test('工作环境只装配声明的 Skill，真实原生 skill 调用可读取方法，其他环境和隐式目录不进入会话', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-skills-'));
  const delivery = join(root, 'delivery');
  const method = join(root, 'methods', 'merchant-delivery');
  const hidden = join(delivery, '.agents', 'skills', 'implicit-method');
  const analysis = join(root, 'methods', 'quality-analysis');
  for (const directory of [method, hidden, analysis]) await mkdir(directory, { recursive: true });
  await writeFile(join(method, 'SKILL.md'), '---\nname: merchant-delivery\ndescription: 商家交付方法\n---\n\nOWNER_METHOD_DELIVERY_7c32\n');
  await writeFile(join(hidden, 'SKILL.md'), '---\nname: implicit-method\ndescription: 隐式方法\n---\n\nDO_NOT_IMPORT_IMPLICIT_7c32\n');
  await writeFile(join(analysis, 'SKILL.md'), '---\nname: quality-analysis\ndescription: 分析方法\n---\n\nOWNER_METHOD_ANALYSIS_7c32\n');
  let calls = 0;
  const app = await boot(join(root, 'sessions'), defineApplication({ name: 'operations', admit: async () => {}, environments: [
    defineEnvironment({ id: 'delivery', instructions: '按交付方法工作', skills: [method],
      program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component: () => Skill({ name: 'merchant-delivery' }) }) }),
    defineEnvironment({ id: 'analysis', instructions: '分析', skills: [analysis],
      program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component: () => Skill({ name: 'quality-analysis' }) }) }),
  ] }), async function* (options) {
    calls++;
    if (calls === 1) {
      assert.ok(JSON.stringify(options.messages).includes('OWNER_METHOD_DELIVERY_7c32'));
      yield* tool('skill', randomUUID(), { name: 'merchant-delivery' });
    }
    else if (calls === 2) {
      const received = JSON.stringify(options.messages);
      assert.ok(received.includes('OWNER_METHOD_DELIVERY_7c32'));
      assert.ok(!received.includes('DO_NOT_IMPORT_IMPLICIT_7c32'));
      assert.ok(!received.includes('OWNER_METHOD_ANALYSIS_7c32'));
      yield* text('已读取工作方法');
    } else if (calls === 3) {
      assert.ok(JSON.stringify(options.messages).includes('OWNER_METHOD_ANALYSIS_7c32'));
      yield* tool('skill', randomUUID(), { name: 'quality-analysis' });
    }
    else {
      const received = JSON.stringify(options.messages);
      assert.ok(received.includes('OWNER_METHOD_ANALYSIS_7c32'));
      assert.ok(!received.includes('OWNER_METHOD_DELIVERY_7c32'));
      assert.ok(!received.includes('DO_NOT_IMPORT_IMPLICIT_7c32'));
      yield* text('已读取分析方法');
    }
  }, 'native', delivery);
  try {
    const conversation = await app.host.create('delivery', { workspaceId: app.workspace.workspaceId });
    await conversation.submit({ requestId: 'method-request', content: '按工作方法核对', intent: 'question', delivery: 'followup' as const, scope: null });
    await conversation.whenIdle();
    assert.deepEqual(app.errors, []);
    assert.equal(calls, 2);
    assert.equal((await firstValueFrom(conversation.request('method-request').state$.pipe(filter(state => state.status === 'ready'), map(state => state.value.activity))))?.tools[0]?.state, 'returned');
    const analysisConversation = await app.host.create('analysis', { workspaceId: app.workspace.workspaceId });
    await analysisConversation.submit({ requestId: 'analysis-method-request', content: '读取分析方法', intent: 'question', delivery: 'followup' as const, scope: null });
    await analysisConversation.whenIdle();
    assert.deepEqual(app.errors, []);
    assert.equal(calls, 4);
    assert.equal((await firstValueFrom(analysisConversation.request('analysis-method-request').state$.pipe(filter(state => state.status === 'ready'), map(state => state.value.activity))))?.tools[0]?.state, 'returned');
  } finally { await app.dispose(); }
});
