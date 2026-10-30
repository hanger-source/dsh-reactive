import { test } from 'node:test';
import assert from 'node:assert/strict';
import { appendFile, mkdtemp, readFile, realpath, rename, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DatabaseSync } from 'node:sqlite';
import { setTimeout as delay } from 'node:timers/promises';
import { Fragment, createElement as h, useLayoutEffect } from 'react';
import { BehaviorSubject, Observable, Subject, filter, firstValueFrom, of, timeout } from 'rxjs';
import WebSocket from 'ws';
import { LlmAdapter, ToolCallId } from '@deepseek-ai/dsh-llm';
import { expandAssistantStream } from '@deepseek-ai/dsh-llm/assistant-stream';
import { SessionId, pluginRecordOf } from '@deepseek-ai/dsh-session';
import { RemoteError } from '@deepseek-ai/dsh-typert-protocol';
import { z, defineAction, implementAction, defineOperation, implementOperation, defineApplication, defineEnvironment, defineProgram, defineSurface, composeSurface, collectionSurface, view, resourceView, operationFollowLinkSchema, createWorkSelector } from '@hanger-source/dsh-reactive';
import { implementProgram, Module, Expose, Offer, Follow, HumanInput, useAction, useOperation, useObservable } from '@hanger-source/dsh-reactive/program';
import { ChildCreationIncompleteError, createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import { mountProgramGateway, RemoteAuthenticationError } from '@hanger-source/dsh-reactive-remote';
import { createProgramClient, CreationIncompleteError, SubmissionAdmissionUnknownError } from '@hanger-source/dsh-reactive-client';

const next = (source, predicate = () => true, label = '观察') => firstValueFrom(source.pipe(filter(predicate), timeout({ first: 8_000, meta: label })));
const failure = error => ({ code: error.code, message: error.message, details: error.details });

test('动态模块集合经真实Gateway保留Date、单向目标与独立Operation，撤挂拒旧绑定且重挂不重定向', async context => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-remote-modules-'));
  const errors = [], carrierErrors = [], executed = [];
  const records = join(root, 'accepted-records.jsonl');
  const members = new BehaviorSubject(['a']);
  const date = z.codec(z.iso.datetime(), z.date(), { decode: value => new Date(value), encode: value => value.toISOString() });
  const instants = { a: '2026-10-27T01:00:00.000Z', b: '2026-10-27T02:00:00.000Z' };
  const states = Object.fromEntries(['a', 'b'].map(id => [id, new BehaviorSubject({ status: 'ready', generation: 1,
    observedAt: Date.now(), value: { merchantId: 'merchant-' + id, phase: 'running' } })]));
  const work = defineOperation({ kind: 'remote-module-work', version: 1,
    command: z.object({ merchantId: z.string() }), reference: z.object({ merchantId: z.string() }),
    state: z.object({ merchantId: z.string(), phase: z.enum(['running', 'done']) }),
    settled: state => state.phase === 'done' ? state : undefined });
  const owner = implementOperation(work, {
    locate(command) {
      const state = states[command.merchantId.slice(-1)].value.value;
      return { status: 'accepted', reference: command, state };
    },
    observe(reference) { return states[reference.merchantId.slice(-1)]; },
  });
  const save = defineAction({ name: 'save_record', description: '在已捕获的模块商家保存一次',
    scope: z.string().transform(merchantId => ({ merchantId })), input: z.object({ amount: z.string().transform(Number) }),
    output: z.object({ merchantId: z.string(), amount: z.number(), at: date }) });
  const implementation = implementAction(save, { async execute(input, call) {
    assert.equal(call.source.kind, 'participant'); assert.equal(call.source.actorId, 'module-user');
    await appendFile(records, JSON.stringify({ merchantId: call.scope.merchantId, amount: input.amount }) + '\n');
    executed.push({ merchantId: call.scope.merchantId, amount: input.amount });
    return { merchantId: call.scope.merchantId, amount: input.amount, at: instants[call.scope.merchantId.slice(-1)] };
  } });
  const leaf = defineSurface({ views: { at: resourceView(date) }, actions: { save }, operations: { work } });
  const child = composeSurface({ business: leaf, reviews: collectionSurface(leaf) });
  const composed = composeSurface({ merchants: collectionSurface(child.surface) });
  const program = defineProgram({ name: 'gateway_test', input: z.null(), surface: composed.surface });
  function Merchant({ id }) {
    const action = useAction(implementation, () => 'merchant-' + id, [id]);
    const existing = useOperation(owner, () => ({ merchantId: 'merchant-' + id }), [id]);
    const value = { views: { at: of({ status: 'ready', generation: 1,
      observedAt: Date.now(), value: new Date(instants[id]) }) }, actions: { save: action }, operations: { work: existing } };
    return h(Fragment, null,
      h(Module, { mount: child.mounts.business }, Expose({ surface: leaf, value })),
      h(Module, { mount: child.mounts.reviews.instance('current') }, Expose({ surface: leaf, value })));
  }
  const definition = defineApplication({ name: 'remote-module-composition', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '读取组合模块的确切业务事实', program: implementProgram(program, { Component() {
      const observed = useObservable(() => members, [members]);
      return observed.status === 'ready' ? h(Fragment, null, observed.value.map(id =>
        h(Module, { key: id, mount: composed.mounts.merchants.instance(id) }, h(Merchant, { id })))) : null;
    } }),
  })] });
  let modelCalls = 0, gateway, client, local;
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'module-user', operations: [owner], tools: 'native',
    onError: event => errors.push(event.error), model: nativeModel({ provider: 'remote-module', model: 'test', create: () => new class extends LlmAdapter {
      async *stream() { modelCalls++; yield { type: 'finish', reason: { kind: 'stop' } }; }
    }() }) });
  context.after(async () => {
    await client?.dispose(); local?.release(); await gateway?.close(); await runtime.dispose();
    Object.values(states).forEach(state => state.complete());
    members.complete();
  });
  const workspace = await runtime.application.createWorkspace({ directory: root });
  local = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
  const role = { name: 'module-reader', resource: 'session', actions: ['reactivePrograms/open', 'reactivePrograms/view', 'reactivePrograms/questions',
    'reactivePrograms/resolveAction', 'reactivePrograms/startAction', 'reactivePrograms/actionInvocation', 'reactivePrograms/actionResult', 'reactivePrograms/observeAction',
    'reactivePrograms/resolveOperation', 'reactivePrograms/locateOperation', 'reactivePrograms/observeOperation', 'reactivePrograms/observeInvocationOperation'] };
  gateway = await mountProgramGateway(runtime.native, { application: runtime.application, model: { provider: 'remote-module', model: 'test' },
    participant: principal => ({ participant: 'human', actorId: principal.id }),
    access: service => service.registerPolicy('module-user', { bindings: () => [{ role, resource: { kind: 'session', id: local.id } }] }),
    carrier: { onError: error => carrierErrors.push(error), authenticate(request) {
      if (request.headers.get('authorization') !== 'Bearer module-user') throw new RemoteAuthenticationError(401);
      return { kind: 'module-user', id: 'module-user' };
    } },
  });
  const connect = () => createProgramClient(program, { baseUrl: gateway.url,
    fetch: (url, init) => fetch(url, { ...init, headers: { ...Object.fromEntries(new Headers(init?.headers)), authorization: 'Bearer module-user' } }),
    createWebSocket: url => new WebSocket(url, { headers: { authorization: 'Bearer module-user' } }),
  });
  client = await connect();
  const remote = await client.open(local.id);
  const localMerchants = local.collection('merchants'), remoteMerchants = remote.collection('merchants');
  assert.deepEqual(await next(remoteMerchants.instances$), ['a']);
  const localInstanceB = localMerchants.instance('b'), remoteInstanceB = remoteMerchants.instance('b');
  const localB = localInstanceB.module(child.mounts.business), remoteB = remoteInstanceB.module(child.mounts.business);
  assert.equal(localB.viewResource('at'), localInstanceB.viewResource('business.at'));
  assert.equal(remoteB.viewResource('at'), remoteInstanceB.viewResource('business.at'));
  await assert.rejects(remoteB.readView('at'), error => error instanceof RemoteError);
  members.next(['a', 'b']);
  assert.deepEqual(await next(remoteMerchants.instances$, keys => keys.length === 2), ['a', 'b']);
  assert.ok(await localB.readView('at') instanceof Date);
  await next(remoteB.observeView('at'), observed => observed.status === 'ready');
  const captured = new Map(), invocations = new Map(), originals = new Map();
  for (const id of ['a', 'b']) {
    const localInstance = localMerchants.instance(id).module(child.mounts.business);
    const remoteInstance = remoteMerchants.instance(id).module(child.mounts.business);
    const localDate = await localInstance.readView('at'), remoteDate = await remoteInstance.readView('at');
    assert.ok(localDate instanceof Date); assert.ok(remoteDate instanceof Date);
    assert.equal(localDate.toISOString(), instants[id]); assert.equal(remoteDate.toISOString(), instants[id]);
    const action = await remoteInstance.resolveAction('save', 'merchant-' + id);
    captured.set(id, action);
    assert.deepEqual(action.scope, { merchantId: 'merchant-' + id });
    const invocation = await action({ amount: '7' });
    invocations.set(id, invocation);
    const original = localInstance.invocation('save', invocation.id);
    originals.set(id, original);
    assert.equal(runtime.application.readInvocation(local.id, invocation.id, program).invocation, original);
    const value = await invocation.result;
    assert.equal(value.merchantId, 'merchant-' + id); assert.equal(value.amount, 7);
    assert.ok(value.at instanceof Date); assert.equal(value.at.toISOString(), instants[id]);
  }
  assert.deepEqual(executed, [{ merchantId: 'merchant-a', amount: 7 }, { merchantId: 'merchant-b', amount: 7 }]);
  const remoteA = remoteMerchants.instance('a').module(child.mounts.business);
  const nestedA = remoteMerchants.instance('a').collection('reviews').instance('current');
  const nestedCall = await (await nestedA.resolveAction('save', 'merchant-a'))({ amount: '9' });
  assert.equal((await nestedCall.result).amount, 9);
  const originalNested = localMerchants.instance('a').collection('reviews').instance('current').invocation('save', nestedCall.id);
  await assert.rejects(remoteA.resolveAction('save', 'merchant-b'), error => error.code === 'reactive/action-target-mismatch');
  const a = await remoteA.operations.work(), b = await remoteB.operations.work();
  assert.deepEqual(a.command, { merchantId: 'merchant-a' }); assert.deepEqual(b.command, { merchantId: 'merchant-b' });
  const localOperation = await localB.operations.work();
  assert.deepEqual(localOperation.command, b.command);
  members.next(['b']);
  await localB.readView('at');
  assert.deepEqual(await next(remoteMerchants.instances$, keys => keys.length === 1 && keys[0] === 'b'), ['b']);
  await next(remoteA.observeView('at'), observed => observed.status === 'error');
  await assert.rejects(remoteA.readView('at'), error => error instanceof RemoteError);
  await assert.rejects(captured.get('a')({ amount: '8' }), error => error.code === 'reactive/action-binding-unavailable');
  const recoveredA = await remoteA.invocation('save', invocations.get('a').id);
  assert.deepEqual(await recoveredA.result, await invocations.get('a').result, '模块撤下仍按原精确地址查回原调用');
  await assert.rejects(remoteB.invocation('save', invocations.get('a').id), error => error.code === 'reactive/invocation-unavailable');
  assert.equal((await (await nestedA.invocation('save', nestedCall.id)).result).amount, 9);
  await assert.rejects(remoteA.invocation('save', nestedCall.id), error => error.code === 'reactive/invocation-unavailable',
    '同一Action定义也不能把嵌套集合的原地址混同为静态模块地址');
  assert.equal(localMerchants.instance('a').module(child.mounts.business).invocation('save', invocations.get('a').id), originals.get('a'));
  assert.equal((await remoteB.resolveAction('save', 'merchant-b')).bindingId, captured.get('b').bindingId);
  const located = await a.locate(); assert.equal(located.status, 'accepted');
  const operation = a.attach(located.reference), settled = operation.settled();
  assert.deepEqual(await operation.state.read(), { merchantId: 'merchant-a', phase: 'running' });
  assert.equal(operation.observations$, operation.state.observations$);
  states.a.next({ status: 'ready', generation: 2, observedAt: Date.now(), value: { merchantId: 'merchant-a', phase: 'done' } });
  assert.deepEqual(await settled, { merchantId: 'merchant-a', phase: 'done' });
  assert.equal((await b.locate()).state.phase, 'running');
  members.next(['b', 'a']);
  await next(remoteMerchants.instances$, keys => keys.length === 2);
  await localMerchants.instance('a').module(child.mounts.business).readView('at');
  await next(remoteA.observeView('at'), observed => observed.status === 'ready');
  const remounted = await remoteA.resolveAction('save', 'merchant-a');
  assert.notEqual(remounted.bindingId, captured.get('a').bindingId);
  await assert.rejects(captured.get('a')({ amount: '8' }), error => error.code === 'reactive/action-binding-unavailable');
  assert.equal(local.surface, composed.surface);
  members.next(['b']);
  await next(remoteMerchants.instances$, keys => keys.length === 1 && keys[0] === 'b');
  await client.dispose();
  client = await connect();
  const fresh = await client.open(local.id);
  const freshA = fresh.collection('merchants').instance('a').module(child.mounts.business);
  const restored = await freshA.invocation('save', invocations.get('a').id);
  const originalValue = await restored.result;
  assert.ok(originalValue.at instanceof Date);
  assert.equal(originalValue.at.toISOString(), instants.a);
  assert.equal(originalValue.merchantId, 'merchant-a');
  assert.equal(originalValue.amount, 7);
  const restoredNested = await fresh.collection('merchants').instance('a').collection('reviews').instance('current').invocation('save', nestedCall.id);
  assert.equal((await restoredNested.result).amount, 9);
  const previous = local;
  local = await runtime.application.open(previous.id, program);
  previous.release();
  assert.equal(local.collection('merchants').instance('a').module(child.mounts.business).invocation('save', restored.id), originals.get('a'),
    '同进程新本地句柄取得原call对象，无需原Expose');
  assert.equal(local.collection('merchants').instance('a').collection('reviews').instance('current').invocation('save', nestedCall.id), originalNested);
  assert.deepEqual((await readFile(records, 'utf8')).trim().split('\n').map(value => JSON.parse(value)), executed,
    '模块撤下、重挂、换SDK与本地句柄查回均不重复持久副作用');
  assert.equal(executed.length, 3, '集合及Operation变化和原调用查回不重执行业务Action');
  assert.equal(modelCalls, 0); assert.deepEqual(errors, []); assert.deepEqual(carrierErrors, []);
});

test('真实认证 carrier 复用 Gateway：Action 主动问人、codec/resource、断线重读、Operation 和原生 journal', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-remote-'));
  const facts = new BehaviorSubject({ status: 'ready', generation: 0, observedAt: Date.now(), value: new Date('2026-10-26T00:00:00Z') });
  const operationFacts = new BehaviorSubject({ status: 'ready', generation: 0, observedAt: Date.now(), value: { phase: 'running', at: '2026-10-26T00:00:00Z' } });
  const accepted = new Map();
  const entered = Promise.withResolvers(), finish = Promise.withResolvers();
  const questions = [{ id: 'allow', question: '是否完成核验？', options: [{ label: '是' }, { label: '否' }] }];
  const preferenceQuestions = [{ id: 'delivery-mode', header: '核验资料', question: '希望如何接收本次核验资料？',
    options: [{ label: '站内' }, { label: '邮件' }], multiSelect: false }];
  const preferenceAnswer = { answers: [{ id: 'delivery-mode', selected: ['站内'] }] };
  const preferenceCalls = [], deliveries = [];
  const responderAdmissions = [];
  const date = z.codec(z.iso.datetime(), z.date(), { decode: value => new Date(value), encode: value => value.toISOString() });
  let inputParses = 0, executions = 0, queries = 0, requests = 0, blocking, finiteQuery = false;
  let locationError, childFailure;
  let collectPreferences = false;
  const selection = new BehaviorSubject({ merchantId: 'merchant-a', generation: 0, visible: true });
  const edits = [], editEntered = Promise.withResolvers(), editFinish = Promise.withResolvers();
  let scopeParses = 0, holdEdit = false;
  const editMerchant = defineAction({ name: 'edit_merchant', description: '调用明确商家的业务能力',
    scope: z.string().transform(merchantId => { scopeParses++; return { merchantId }; }),
    input: z.object({ expectedRevision: z.literal(7) }), output: z.object({ merchantId: z.string() }) });
  const editImplementation = implementAction(editMerchant, { async execute(_input, call) {
    assert.equal(call.source.actorId, 'alice');
    edits.push(call.scope.merchantId);
    if (holdEdit) { editEntered.resolve(); await editFinish.promise; }
    call.signal.throwIfAborted();
    return { merchantId: call.scope.merchantId };
  } });
  const businessFailure = new RemoteError('test/revision-conflict', '订单版本已变化', { orderId: 'order-7', expectedRevision: 7, actualRevision: 8 });
  const operation = defineOperation({ kind: 'remote-check', version: 1,
    command: z.object({ id: z.string() }), reference: z.object({ id: z.string() }),
    state: z.object({ phase: z.enum(['running', 'done']), at: date }),
    settled: value => value.phase === 'done' ? value : undefined });
  const owner = implementOperation(operation, {
    locate(command, access) {
      assert.equal(access.source.actorId, 'alice');
      if (locationError) throw locationError;
      return accepted.has(command.id) ? { status: 'accepted', reference: { id: command.id }, state: operationFacts.value.value } : { status: 'unknown' };
    },
    observe(reference, access) {
      assert.equal(reference.id, 'op-7'); assert.equal(access.source.actorId, 'alice'); queries++;
      return finiteQuery ? of(operationFacts.value) : operationFacts;
    },
  });
  const check = defineAction({ name: 'check', description: '处理明确对象一次', scope: z.null(),
    input: z.object({ amount: z.string().transform(value => { inputParses++; return Number(value); }).pipe(z.number()) }),
    output: z.object({ operation: operation.reference, at: date }),
    operation: { definition: operation, command: input => ({ id: 'op-' + input.amount }), reference: receipt => receipt.operation },
    observe: receipt => ({ id: receipt.operation.id }) });
  const implementation = implementAction(check, { operationOwner: owner, async execute(input, call) {
    executions++; assert.equal(input.amount, 7); assert.equal(call.source.actorId, 'alice');
    entered.resolve(); await finish.promise; call.signal.throwIfAborted();
    await writeFile(join(root, 'accepted-check.json'), JSON.stringify({ operationId: 'op-7', actorId: call.source.actorId, amount: input.amount }));
    accepted.set('op-7', true);
    facts.next({ status: 'ready', generation: 3, observedAt: Date.now(), value: new Date('2026-10-26T00:00:07Z') });
    return { operation: { id: 'op-7' }, at: '2026-10-26T00:00:07Z' };
  } });
  const fail = defineAction({ name: 'fail', description: '业务明确拒绝当前版本', scope: z.null(), input: z.null(), output: z.null() });
  const failed = implementAction(fail, { execute() { throw businessFailure; } });
  const unconfirmed = defineAction({ name: 'unconfirmed', description: '报告宿主明确持有的子创建未确认事实', scope: z.null(), input: z.null(), output: z.null() });
  const incomplete = implementAction(unconfirmed, { execute(_input, call) {
    childFailure = new ChildCreationIncompleteError({ parentSessionId: call.sessionId, invocationId: call.invocationId },
      { parentSessionId: call.sessionId, childSessionId: 'reserved-child-session' }, new Error('PRIVATE_CREATION_CAUSE'));
    throw childFailure;
  } });
  const collectDelivery = defineAction({ name: 'collect_delivery_preferences', description: '收集核验资料的交付偏好，再记录本次资料发送方式',
    scope: z.null(), input: z.object({}).strict(), output: z.union([
      z.object({ status: z.literal('pending'), callId: z.string() }).strict(),
      z.object({ status: z.literal('recorded'), delivery: z.string() }).strict(),
    ]), observe: receipt => receipt });
  const collectDeliveryImplementation = implementAction(collectDelivery, { async execute(_input, call) {
    preferenceCalls.push({ sessionId: call.sessionId, invocationId: call.invocationId, source: call.source,
      inputs: call.inputs, continuation: call.continuation });
    const result = await call.askHuman(preferenceQuestions, { timeoutSeconds: 30 });
    if ('pending' in result) return { status: 'pending', callId: result.callId };
    assert.deepEqual(result, preferenceAnswer);
    call.signal.throwIfAborted();
    deliveries.push({ sessionId: call.sessionId, invocationId: call.invocationId, delivery: result.answers[0].selected[0] });
    return { status: 'recorded', delivery: result.answers[0].selected[0] };
  } });
  const surface = defineSurface({ views: { current: resourceView(date), selection: view(z.object({
    merchantId: z.string(), generation: z.number(), visible: z.boolean(),
  })) }, actions: { check, fail, unconfirmed, collectDelivery, editMerchant }, operations: { preparation: operation } });
  function MerchantBinding({ target }) {
    const edit = useAction(editImplementation, () => target.merchantId, [target.merchantId, target.generation]);
    return Expose({ surface, value: { actions: { editMerchant: edit } } });
  }
  const program = defineProgram({ name: 'gateway_test', input: z.object({ merchantId: z.string() }).strict()
    .transform(input => ({ merchant: input.merchantId })).nullable(), surface });
  const definition = defineApplication({ name: 'remote-test', admit: async (_input, _identity, access) => {
    assert.equal(access.actorId, 'alice');
  }, environments: [defineEnvironment({ id: 'work', instructions: '询问实际参与者',
    program: implementProgram(program, { Component() {
      const action = useAction(implementation, () => null, []);
      const reject = useAction(failed, () => null, []), unknownChild = useAction(incomplete, () => null, []);
      const collect = useAction(collectDeliveryImplementation, () => null, []);
      const preparation = useOperation(owner, () => ({ id: 'op-7' }), []);
      const selected = useObservable(() => selection, [selection]);
      return h(Fragment, null, Offer({ name: 'collect_delivery_preferences', action: collect }),
        Expose({ surface, value: { views: { current: facts, selection }, actions: { check: action, fail: reject, unconfirmed: unknownChild, collectDelivery: collect }, operations: { preparation } } }),
        selected.status === 'ready' && selected.value.visible ? h(MerchantBinding, { target: selected.value }) : null,
        HumanInput({ admitResponder: admission => {
          const { origin } = admission.question;
          const [input] = origin.inputs;
          if (admission.actorId !== 'alice' || origin.inputs.length !== 1 || input.actorId !== admission.actorId
            || input.sourceSessionId !== origin.sessionId || !['ask-once', 'delivery-once'].includes(input.requestId)
            || input.scope?.merchantId !== 'merchant-7' || origin.callId !== admission.question.callId) {
            throw new Error('RESPONDER_OUTSIDE_ORIGINAL_WORK');
          }
          responderAdmissions.push({ actorId: admission.actorId, answer: admission.answer, origin });
        } }));
    } }),
  })] });
  const runtimeErrors = [], carrierErrors = [];
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), operations: [owner], tools: 'native', completedRetentionMs: 500,
    onError: event => runtimeErrors.push(event.error),
    model: nativeModel({ provider: 'remote-test', model: 'test', create: () => new class extends LlmAdapter {
      async *stream(options) {
        requests++;
        if (blocking) {
          blocking.resolve();
          const signal = options.signal;
          assert.ok(signal);
          await new Promise((_resolve, reject) => {
            if (signal.aborted) reject(signal.reason);
            else signal.addEventListener('abort', () => reject(signal.reason), { once: true });
          });
        }
        let selected;
        if (requests === 1) selected = { id: 'remote-question', name: 'ask_user_question', args: JSON.stringify({ questions, timeout: 30 }) };
        else if (collectPreferences) {
          collectPreferences = false;
          selected = { id: 'remote-delivery-preferences', name: 'collect_delivery_preferences', args: '{}' };
        }
        if (selected) {
          yield { type: 'block-start', index: 0, blockType: 'tool-call' };
          yield { type: 'tool-call-delta', index: 0, id: ToolCallId(selected.id), name: selected.name, argumentsDelta: selected.args };
          yield { type: 'block-end', index: 0, block: { type: 'tool-call', id: ToolCallId(selected.id), name: selected.name, arguments: selected.args } };
          yield { type: 'finish', reason: { kind: 'tool-calls' } };
        } else {
          yield { type: 'block-start', index: 0, blockType: 'text' };
          yield { type: 'block-end', index: 0, block: { type: 'text', text: '已收到真实答复' } };
          yield { type: 'finish', reason: { kind: 'stop' } };
        }
      }
    }() }),
  });
  const sessions = new Set(), workspaces = new Set(), bindingSessions = new Set();
  const globalRole = { name: 'register-discover', resource: 'global', actions: ['reactivePrograms/createWorkspace', 'reactivePrograms/describe'] };
  const discoveryRole = { name: 'discover', resource: 'global', actions: ['reactivePrograms/describe'] };
  const workspaceRole = { name: 'workspace-participation', resource: 'workspace', actions: ['reactivePrograms/createWorkspace', 'reactivePrograms/create', 'reactivePrograms/completeCreation', 'reactivePrograms/describe'] };
  const sessionRole = { name: 'participate', resource: 'session', actions: [
    'reactivePrograms/open', 'reactivePrograms/submit', 'reactivePrograms/view', 'reactivePrograms/resolveAction', 'reactivePrograms/startAction',
    'reactivePrograms/createWorkspace', 'reactivePrograms/describe', 'reactivePrograms/resume', 'reactivePrograms/pause', 'reactivePrograms/cancel',
    'reactivePrograms/actionInvocation', 'reactivePrograms/actionResult', 'reactivePrograms/observeAction',
    'reactivePrograms/resolveOperation', 'reactivePrograms/locateOperation', 'reactivePrograms/observeOperation', 'reactivePrograms/observeInvocationOperation', 'reactivePrograms/questions',
    'reactivePrograms/answerQuestion', 'reactivePrograms/attachWait', 'reactivePrograms/requestStatus', 'reactivePrograms/progress',
    'reactivePrograms/journal', 'reactivePrograms/journalPage',
  ] };
  const readerRole = { name: 'read', resource: 'session', actions: ['reactivePrograms/open', 'reactivePrograms/view', 'reactivePrograms/questions'] };
  const bindingRole = { name: 'business-action-only', resource: 'session', actions: ['reactivePrograms/open', 'reactivePrograms/resolveAction',
    'reactivePrograms/startAction', 'reactivePrograms/actionResult', 'reactivePrograms/observeAction'] };
  const gateway = await mountProgramGateway(runtime.native, { application: runtime.application,
    model: { provider: 'remote-test', model: 'test' }, participant: principal => ({ participant: 'human', actorId: principal.id }),
    access: service => service.registerPolicy('remote-test-user', { bindings(principal) {
      return [{ role: principal.id === 'alice' ? globalRole : discoveryRole, resource: { kind: 'global' } }, ...[...sessions]
        .map(id => ({ role: principal.id === 'alice' ? sessionRole : readerRole, resource: { kind: 'session', id } })),
        ...[...workspaces].filter(() => principal.id === 'alice').map(id => ({ role: workspaceRole, resource: { kind: 'workspace', id } })),
        ...[...bindingSessions].filter(() => principal.id === 'alice').map(id => ({ role: bindingRole, resource: { kind: 'session', id } }))];
    } }),
    carrier: { onError: error => carrierErrors.push(error), async authenticate(request) {
      const token = request.headers.get('authorization');
      if (token !== 'Bearer alice' && token !== 'Bearer bob') throw new RemoteAuthenticationError(401);
      return { kind: 'remote-test-user', id: token.slice(7) };
    } },
  });
  const clients = [];
  const streamSockets = [];
  let loseSubmitAck = false;
  const connect = async actor => {
    const client = await createProgramClient(program, { baseUrl: gateway.url,
      async fetch(url, init) {
        const response = await fetch(url, { ...init, headers: { ...Object.fromEntries(new Headers(init?.headers)), authorization: 'Bearer ' + actor } });
        if (loseSubmitAck && new URL(url).pathname === '/api/reactivePrograms/submit') {
          loseSubmitAck = false; await response.arrayBuffer(); throw new TypeError('ACK_LOST_AFTER_HOST_ACCEPTED');
        }
        return response;
      },
      createWebSocket: url => {
        const socket = new WebSocket(url, { headers: { authorization: 'Bearer ' + actor } });
        const tracked = { actor, socket, endpoint: undefined };
        streamSockets.push(tracked);
        const send = socket.send.bind(socket);
        socket.send = (data, ...rest) => {
          const message = JSON.parse(String(data));
          if (message.type === 'open') tracked.endpoint = message.endpoint;
          return send(data, ...rest);
        };
        return socket;
      },
    }); clients.push(client); return client;
  };
  try {
    assert.equal((await fetch(gateway.url + '/api')).status, 401);
    const client = await connect('alice');
    const workspace = await client.createWorkspace({ directory: root, title: '远程共享工作目录' });
    assert.equal(workspace.directory, await realpath(root)); assert.deepEqual(workspace.sessionIds, []);
    assert.equal(Object.hasOwn(workspace, 'environmentId'), false);
    // 注册的 global grant 不自动授予新 Workspace 的内容或创建权。
    await assert.rejects(client.create('work', { workspaceId: workspace.workspaceId }), error => error.code === 'remote-access/denied');
    await assert.rejects(client.completeCreation({ environmentId: 'work', workspaceId: workspace.workspaceId, sessionId: crypto.randomUUID() }), error => error.code === 'remote-access/denied');
    workspaces.add(workspace.workspaceId);
    const medium = join(runtime.home, 'state'), preserved = join(root, 'preserved-state');
    let reference;
    await rename(medium, preserved); await writeFile(medium, 'storage path is unavailable', { flag: 'wx' });
    try {
      await assert.rejects(client.create('work', { workspaceId: workspace.workspaceId }), error => {
        if (!(error instanceof CreationIncompleteError)) return false;
        reference = error.reference; return reference.environmentId === 'work' && reference.workspaceId === workspace.workspaceId;
      });
    } finally { await rename(medium, join(root, 'unavailable-medium-marker')); await rename(preserved, medium); }
    assert.ok(reference); sessions.add(reference.sessionId);
    const creationEntered = Promise.withResolvers(), creationReleased = Promise.withResolvers();
    const removeCreationGate = runtime.native.on('agent/created', async ({ agent }) => {
      if (agent.id !== reference.sessionId) return;
      creationEntered.resolve(); await creationReleased.promise;
    });
    const completing = client.completeCreation(reference);
    let created;
    try {
      await creationEntered.promise;
      for (const method of ['open', 'resume']) {
        await assert.rejects(client[method](reference.sessionId), error => error instanceof CreationIncompleteError
          && error.reference.sessionId === reference.sessionId && error.reference.workspaceId === workspace.workspaceId,
        '真实Gateway保留同一创建引用，不能把未完成创建降为通用远程错误或提前返回句柄');
      }
    } finally { creationReleased.resolve(); removeCreationGate(); created = await completing; }
    assert.equal(created.id, reference.sessionId); assert.equal(requests, 0);
    console.info('gateway.human.creation.ready', { sessionId: created.id });
    assert.equal(created.identity.actorId, 'alice');
    assert.equal((await created.readView('current')).toISOString(), '2026-10-26T00:00:00.000Z');
    const stoppedSubmit = new AbortController(), stoppedReason = new Error('SUBMIT_NOT_ACCEPTED');
    stoppedSubmit.abort(stoppedReason);
    await assert.rejects(created.submit({ requestId: 'pre-aborted-submit', content: '这次提交已明确取消',
      delivery: 'followup', intent: 'question', scope: { merchantId: 'merchant-7' } }, { signal: stoppedSubmit.signal }),
    error => error === stoppedReason);
    assert.equal((await created.requestStatus('pre-aborted-submit')).state, 'absent');
    assert.equal(requests, 0); assert.equal(executions, 0); assert.deepEqual(edits, []);
    const selectMerchant = async (merchantId, generation, visible = true) => {
      const observed = next(created.views.selection, value => value.merchantId === merchantId
        && value.generation === generation && value.visible === visible, '明确商家绑定');
      selection.next({ merchantId, generation, visible }); await observed;
    };
    const initialScopeParses = scopeParses;
    const editA = await created.resolveAction('editMerchant', 'merchant-a');
    assert.equal(scopeParses, initialScopeParses + 1, 'raw scope 只在宿主解析，不把已转换输出再次 parse');
    assert.deepEqual(editA.scope, { merchantId: 'merchant-a' }); assert.ok(Object.isFrozen(editA.scope));
    await selectMerchant('merchant-b', 0);
    await assert.rejects(editA({ expectedRevision: 7 }), error => error.code === 'reactive/action-binding-unavailable'
      && error.details.reason === 'binding-changed');
    await assert.rejects(created.resolveAction('editMerchant', 'merchant-a'), error => error.code === 'reactive/action-target-mismatch');
    assert.deepEqual(edits, []);
    const editB = await created.resolveAction('editMerchant', 'merchant-b');
    assert.deepEqual(await (await editB({ expectedRevision: 7 })).result, { merchantId: 'merchant-b' });
    await selectMerchant('merchant-a', 1);
    const resolving = new AbortController();
    const replacementA = await created.resolveAction('editMerchant', 'merchant-a', { signal: resolving.signal });
    resolving.abort(new Error('RESOLVE_WAIT_FINISHED'));
    assert.notEqual(replacementA.bindingId, editA.bindingId, '同目标重新挂载是新的真实绑定');
    await assert.rejects(editA({ expectedRevision: 7 }), error => error.code === 'reactive/action-binding-unavailable');
    holdEdit = true;
    const starting = new AbortController();
    const acceptedEdit = await replacementA({ expectedRevision: 7 }, { signal: starting.signal }); await editEntered.promise;
    starting.abort(new Error('START_WAIT_FINISHED'));
    await selectMerchant('merchant-b', 1);
    editFinish.resolve();
    assert.deepEqual(await acceptedEdit.result, { merchantId: 'merchant-a' }, '已进入业务执行的调用仍由原 owner 结算');
    holdEdit = false;
    await selectMerchant('merchant-b', 2, false);
    await assert.rejects(created.resolveAction('editMerchant', 'merchant-b'), error => error.code === 'reactive/action-binding-unavailable'
      && error.details.reason === 'not-exposed');
    assert.deepEqual(edits, ['merchant-b', 'merchant-a']); assert.equal(requests, 0);
    const bob = await connect('bob');
    await assert.rejects(bob.createWorkspace({ directory: root }), error => error.code === 'remote-access/denied');
    await assert.rejects(bob.create('work', { workspaceId: workspace.workspaceId }), error => error.code === 'remote-access/denied');
    await assert.rejects(bob.completeCreation(reference), error => error.code === 'remote-access/denied');
    const reader = await bob.open(created.id);
    assert.equal((await reader.readView('current')).toISOString(), '2026-10-26T00:00:00.000Z');
    await assert.rejects(reader.resolveAction('check', null), error => error.code === 'remote-access/denied');
    await assert.rejects(bob.open('foreign-session'), error => error.code === 'remote-access/denied');
    await assert.rejects(reader.pause(), error => error.code === 'remote-access/denied');
    await assert.rejects(reader.cancel(), error => error.code === 'remote-access/denied');
    await assert.rejects(bob.resume(created.id), error => error.code === 'remote-access/denied');
    assert.deepEqual(await bob.describe(), []);
    const hidden = await runtime.application.create('work', { workspaceId: workspace.workspaceId }); hidden.release();
    workspaces.delete(workspace.workspaceId);
    try {
      const registered = await client.createWorkspace({ directory: root });
      assert.equal(registered.workspaceId, workspace.workspaceId); assert.deepEqual(registered.sessionIds, []);
    } finally { workspaces.add(workspace.workspaceId); }
    const registered = await client.createWorkspace({ directory: root });
    assert.deepEqual(registered.sessionIds, [created.id]);
    const discovered = await client.describe();
    assert.deepEqual(discovered.map(workspace => workspace.sessionIds), [[created.id]]);
    // 同一个业务合同可从 SDK 调用，但冷会话参与者不能借用原生提问权限。
    await runtime.application.close(hidden.id); sessions.add(hidden.id);
    const cold = await client.open(hidden.id);
    assert.equal(runtime.native.agents.get(SessionId(hidden.id)), undefined);
    const collectDeliveryAction = await cold.resolveAction('collectDelivery', null);
    const ordinary = await collectDeliveryAction({});
    assert.deepEqual(ordinary.source, { kind: 'participant', actorId: 'alice', participant: 'human', entrypoint: 'sdk' });
    await assert.rejects(ordinary.result, error => {
      assert.deepEqual(failure(error), { code: 'gateway/internal', message: 'REACTIVE_ACTION_HUMAN_INPUT_UNAVAILABLE', details: {} }); return true;
    });
    assert.equal(preferenceCalls.length, 1); assert.deepEqual(preferenceCalls[0].source, ordinary.source);
    assert.deepEqual(preferenceCalls[0].inputs, []); assert.equal(preferenceCalls[0].continuation, null);
    assert.deepEqual(await firstValueFrom(cold.foregroundQuestions$), []);
    assert.equal(runtime.native.agents.get(SessionId(hidden.id)), undefined);
    assert.equal(requests, 0); assert.deepEqual(deliveries, []); sessions.delete(hidden.id);
    await cold.release();
    await selectMerchant('merchant-a', 3);
    const isolated = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    await runtime.application.close(isolated.id); isolated.release(); bindingSessions.add(isolated.id);
    const actionOnly = await client.open(isolated.id);
    const capturedCold = await actionOnly.resolveAction('editMerchant', 'merchant-a');
    await assert.rejects(actionOnly.readView('current'), error => error.code === 'remote-access/denied');
    const sameCold = await actionOnly.resolveAction('editMerchant', 'merchant-a');
    assert.equal(sameCold.bindingId, capturedCold.bindingId, '绑定流持有真实冷程序，无需视图或人工观察权限');
    assert.deepEqual(await (await capturedCold({ expectedRevision: 7 })).result, { merchantId: 'merchant-a' }, '同绑定再次解析不使旧捕获失效');
    assert.equal(runtime.native.agents.get(SessionId(isolated.id)), undefined); assert.equal(requests, 0);
    await actionOnly.release();
    await assert.rejects(capturedCold({ expectedRevision: 7 }), /REACTIVE_CONVERSATION_HANDLE_RELEASED/u);
    bindingSessions.delete(isolated.id);
    const pending = next(created.observeView('current'), value => value.status === 'pending' && value.generation > 0, 'resource pending');
    facts.next({ status: 'pending', generation: 1 }); await pending;
    const reading = new AbortController();
    const cancelledRead = assert.rejects(created.readView('current', { signal: reading.signal }), /VIEW_WAIT_CANCELLED/u);
    reading.abort(new Error('VIEW_WAIT_CANCELLED')); await cancelledRead;
    const broken = next(created.observeView('current'), value => value.status === 'error', 'resource error');
    facts.next({ status: 'error', generation: 2, error: new Error('QUERY_FAILED') });
    assert.deepEqual(failure((await broken).error), { code: 'gateway/internal', message: 'QUERY_FAILED', details: {} });
    const checkAction = await created.resolveAction('check', null);
    const invoking = new AbortController(), waiting = new AbortController(), waitReason = new Error('ACTION_RECEIPT_WAIT_FINISHED');
    const invocation = await checkAction({ amount: '7' }, { signal: invoking.signal }); await entered.promise;
    const withdrawn = assert.rejects(invocation.wait({ signal: waiting.signal }), error => error === waitReason);
    const retained = invocation.wait();
    let retainedEnded = false;
    void retained.then(() => { retainedEnded = true; }, () => { retainedEnded = true; });
    invoking.abort(new Error('ACTION_START_CALLER_EXITED'));
    waiting.abort(waitReason); await withdrawn;
    assert.equal(retainedEnded, false, '退出一个等待不结束另一读取，也不取消原Host执行');
    const currentInvocation = await next(invocation.activity.observations$, value => value.status === 'ready' && value.value.state === 'running');
    assert.equal(currentInvocation.value.state, 'running');
    const activityDisconnected = next(invocation.activity.observations$,
      value => value.status === 'pending' && value.generation > currentInvocation.generation, '调用活动连接失效');
    const activitySocket = streamSockets.findLast(item => item.actor === 'alice'
      && item.endpoint === 'reactivePrograms/observeAction' && item.socket.readyState === WebSocket.OPEN);
    assert.ok(activitySocket); activitySocket.socket.terminate(); await activityDisconnected;
    const activityAbort = new AbortController(), activityReason = new Error('CANCEL_ONLY_ACTIVITY_READ');
    const interruptedActivity = assert.rejects(invocation.activity.read(activityAbort.signal), error => error === activityReason);
    const recoveredActivity = invocation.activity.read();
    activityAbort.abort(activityReason); await interruptedActivity;
    assert.equal((await recoveredActivity).state, 'running');
    assert.equal(inputParses, 1); assert.equal(executions, 1);
    assert.equal(invocation.source.actorId, 'alice');
    const id = invocation.id, sessionId = created.id;
    const disconnected = assert.rejects(invocation.result);
    const endedWait = assert.rejects(retained, /REACTIVE_REMOTE_CLIENT_DISPOSED/u);
    await client.dispose(); await disconnected; await endedWait;
    await assert.rejects(async () => invocation.activity.read(), /REACTIVE_REMOTE_CLIENT_DISPOSED/u);
    finish.resolve();
    const reconnected = await connect('alice'), session = await reconnected.open(sessionId);
    console.info('gateway.human.reconnected', { sessionId });
    const original = await session.invocation('check', id);
    const receipt = await original.wait();
    assert.deepEqual(receipt, await original.result);
    assert.deepEqual(JSON.parse(await readFile(join(root, 'accepted-check.json'), 'utf8')),
      { operationId: 'op-7', actorId: 'alice', amount: 7 });
    assert.equal((await next(original.activity$, value => value.state === 'returned')).state, 'returned');
    assert.equal((await original.activity.read()).state, 'returned');
    const completedActivity = [];
    const completedObserver = original.activity.observations$.subscribe(frame => completedActivity.push(frame));
    let completedValues = false;
    const completedValueObserver = original.activity$.subscribe({ complete: () => { completedValues = true; } });
    assert.equal(completedObserver.closed, false, '有限业务来源释放后，SDK仍拥有资源有效性观察');
    assert.equal(receipt.at.toISOString(), '2026-10-26T00:00:07.000Z');
    assert.equal((await session.readView('current')).toISOString(), receipt.at.toISOString());
    assert.equal(inputParses, 1); assert.equal(executions, 1);
    const liveOperation = await original.operation;
    const observing = liveOperation.observations$.subscribe();
    await next(liveOperation.observations$, value => value.status === 'ready', 'operation ready');
    const completed = liveOperation.settled();
    operationFacts.next({ status: 'ready', generation: 1, observedAt: Date.now(), value: { phase: 'done', at: '2026-10-26T00:00:08Z' } });
    assert.equal((await completed).at.toISOString(), '2026-10-26T00:00:08.000Z'); observing.unsubscribe();
    assert.equal(queries, 1);
    assert.equal((await session.locateOperation(operation, { id: 'op-7' })).status, 'accepted');
    assert.equal((await session.locateOperation(operation, { id: 'never-started' })).status, 'unknown');
    assert.equal(executions, 1);
    finiteQuery = true;
    const finite = session.attachOperation(operation, { id: 'op-7' });
    const finiteStates = [];
    const finiteSubscription = finite.observations$.subscribe(value => finiteStates.push(value.status));
    // 业务完成由合同的 settled 判定；有限查询结束后，正在使用的读侧仍可读取最终事实。
    assert.equal((await finite.settled()).at.toISOString(), '2026-10-26T00:00:08.000Z');
    assert.equal((await finite.state.read()).phase, 'done');
    finiteSubscription.unsubscribe();
    assert.equal(finiteStates.at(-1), 'ready'); assert.equal(queries, 2); assert.equal(executions, 1);
    // 同一个公开业务错误经过不同读取入口；比较公开字段，不依赖原生 Error 原型跨进程相等。
    const localFailureHandle = await runtime.application.open(sessionId, program);
    try {
      const localFail = localFailureHandle.resolveAction('fail', null);
      const localInvocation = localFail(null, { actorId: 'alice' });
      await assert.rejects(localInvocation.result, error => error === businessFailure);
      assert.equal((await next(localInvocation.activity$, value => value.state === 'failed')).error, businessFailure);
      const failAction = await session.resolveAction('fail', null);
      const rejected = await failAction(null);
      await assert.rejects(rejected.result, error => { assert.deepEqual(failure(error), failure(businessFailure)); return true; });
      assert.deepEqual(failure((await next(rejected.activity$, value => value.state === 'failed')).error), failure(businessFailure));
      const failedActivity = rejected.activity.peek();
      assert.equal(failedActivity.status, 'ready');
      assert.deepEqual(failure(failedActivity.value.error), failure(businessFailure));
      facts.next({ status: 'error', generation: 4, error: businessFailure });
      assert.equal((await next(localFailureHandle.observeView('current'), value => value.status === 'error')).error, businessFailure);
      assert.deepEqual(failure((await next(session.observeView('current'), value => value.status === 'error' && value.generation >= 4)).error), failure(businessFailure));
      await assert.rejects(session.readView('current'), error => { assert.deepEqual(failure(error), failure(businessFailure)); return true; });
      operationFacts.next({ status: 'error', generation: 2, error: businessFailure });
      const failedOperation = session.attachOperation(operation, { id: 'op-7' });
      assert.deepEqual(failure((await next(failedOperation.observations$, value => value.status === 'error')).error), failure(businessFailure));
      await assert.rejects(failedOperation.settled(), error => { assert.deepEqual(failure(error), failure(businessFailure)); return true; });
      const localOperation = await localFailureHandle.attachOperation(operation, { id: 'op-7' }, { actorId: 'alice' });
      assert.equal((await next(localOperation.observations$, value => value.status === 'error')).error, businessFailure);

      // 仅验证真实 Gateway 对公开宿主异常的传输，不模拟子进程启动或崩溃恢复。
      const unconfirmedAction = await session.resolveAction('unconfirmed', null);
      const unconfirmedCall = await unconfirmedAction(null);
      const expectedChildFailure = { code: 'reactive/child-creation-incomplete', message: 'Child session creation is not yet confirmed',
        details: { command: { parentSessionId: sessionId, invocationId: unconfirmedCall.id },
          reference: { parentSessionId: sessionId, childSessionId: 'reserved-child-session' } } };
      const isChildFailure = error => { assert.deepEqual(failure(error), expectedChildFailure); return true; };
      await assert.rejects(unconfirmedCall.result, isChildFailure);
      assert.deepEqual(failure((await next(unconfirmedCall.activity$, value => value.state === 'failed')).error), expectedChildFailure);
      locationError = childFailure;
      await assert.rejects(session.locateOperation(operation, { id: 'op-7' }), isChildFailure);
      locationError = undefined;
      operationFacts.next({ status: 'error', generation: 3, error: childFailure });
      assert.deepEqual(failure((await next(session.attachOperation(operation, { id: 'op-7' }).observations$, value => value.status === 'error')).error), expectedChildFailure);
      const originalOpen = runtime.application.open, originalResume = runtime.application.resume;
      try {
        runtime.application.open = async () => { throw childFailure; };
        await assert.rejects(session.operations.preparation(), isChildFailure);
        await assert.rejects(failAction(null), isChildFailure);
        runtime.application.resume = async () => { throw childFailure; };
        await assert.rejects(reconnected.resume(sessionId), isChildFailure);
      } finally { runtime.application.open = originalOpen; runtime.application.resume = originalResume; }
    } finally {
      localFailureHandle.release(); locationError = undefined;
      facts.next({ status: 'ready', generation: 5, observedAt: Date.now(), value: new Date('2026-10-26T00:00:07Z') });
      operationFacts.next({ status: 'ready', generation: 4, observedAt: Date.now(), value: { phase: 'done', at: '2026-10-26T00:00:08Z' } });
    }
    const changes = [], journal = await session.openEvents({ publish: change => changes.push(change), failed: error => runtimeErrors.push(error) });
    assert.equal(changes[0]?.type, 'replace');
    const foreground = next(session.foregroundQuestions$, value => value.length === 1, 'foreground question').catch(error => {
      const events = runtime.native.agents.get(SessionId(sessionId))?.session.snapshotEvents().filter(event => event.type === 'tool/result' || event.type === 'tool/call');
      throw new Error(JSON.stringify({ requests, errors: runtimeErrors.map(value => String(value)), events }), { cause: error });
    });
    await session.submit({ requestId: 'ask-once', content: '确认处理结果', delivery: 'followup', intent: 'question', scope: { merchantId: 'merchant-7' } });
    const [question] = await foreground;
    console.info('gateway.human.foreground', { callId: question.callId });
    assert.deepEqual({ sessionId: question.origin.sessionId, callId: question.origin.callId, rootCallId: question.origin.rootCallId,
      parentCallId: question.origin.parentCallId, name: question.origin.name },
    { sessionId, callId: 'remote-question', rootCallId: 'remote-question', parentCallId: null, name: 'ask_user_question' });
    assert.deepEqual(question.origin.inputs.map(input => ({ sourceSessionId: input.sourceSessionId, requestId: input.requestId,
      actorId: input.actorId, scope: input.scope })),
    [{ sourceSessionId: sessionId, requestId: 'ask-once', actorId: 'alice', scope: { merchantId: 'merchant-7' } }]);
    assert.equal(Object.hasOwn(question.origin, 'goal'), false);
    const observedQuestion = await reader.question(question.callId);
    console.info('gateway.human.reader.bound', { callId: question.callId });
    assert.deepEqual((await observedQuestion.state.read()).foreground.origin, question.origin);
    await assert.rejects(observedQuestion.answer({ answers: [{ id: 'allow', selected: ['是'] }] }), error => error.code === 'remote-access/denied');
    const boundQuestion = await session.question(question.callId);
    console.info('gateway.human.responder.bound', { callId: question.callId });
    assert.equal(await session.question(question.callId), boundQuestion);
    await assert.rejects(session.notifyOperation('ungranted-publisher'), error => error.code === 'remote-access/denied');
    const claimAbort = new AbortController();
    const claim = boundQuestion.attachWait({ signal: claimAbort.signal })[Symbol.asyncIterator]();
    assert.ok((await claim.next()).value.remainingMs > 0);
    const settledQuestion = next(session.questions$, state => state.settled.some(item => item.callId === question.callId), 'settled question');
    assert.equal(await boundQuestion.answer({ answers: [{ id: 'allow', selected: ['是'] }] }), true);
    console.info('gateway.human.answer.accepted', { callId: question.callId });
    const settled = await settledQuestion;
    assert.deepEqual(settled.settled.find(item => item.callId === question.callId).origin, question.origin);
    assert.deepEqual((await boundQuestion.state.read()).settled?.origin, question.origin);
    assert.ok(responderAdmissions.some(admission => admission.answer === null));
    assert.ok(responderAdmissions.some(admission => admission.answer !== null));
    for (const admission of responderAdmissions) {
      assert.equal(admission.actorId, 'alice'); assert.deepEqual(admission.origin, question.origin);
    }
    claimAbort.abort(); await claim.return();
    const local = await runtime.application.open(sessionId); await local.whenIdle(); local.release();
    assert.equal(requests, 2);
    // 模型调用业务 Action；Action 主动收集偏好，SDK claim 与答复后才记录交付方式。
    const deliveryRequest = session.request('delivery-once');
    await next(deliveryRequest.state$, value => value.status === 'ready', 'Action request baseline');
    collectPreferences = true;
    const deliveryForeground = next(session.foregroundQuestions$, value => value.length === 1, 'Action foreground question');
    await session.submit({ requestId: 'delivery-once', content: '收集核验资料交付偏好并记录', delivery: 'followup', intent: 'question', scope: { merchantId: 'merchant-7' } });
    const [deliveryQuestion] = await deliveryForeground;
    assert.equal(deliveryQuestion.timed, true); assert.deepEqual(deliveryQuestion.questions, preferenceQuestions);
    assert.notEqual(deliveryQuestion.callId, 'remote-delivery-preferences');
    assert.equal(preferenceCalls.length, 2);
    assert.deepEqual(deliveryQuestion.origin, { sessionId, callId: deliveryQuestion.callId, callSequence: deliveryQuestion.origin.callSequence,
      rootCallId: 'remote-delivery-preferences', parentCallId: 'remote-delivery-preferences', name: 'ask_user_question',
      inputs: preferenceCalls[1].inputs, continuation: { sessionId, inputs: preferenceCalls[1].inputs, goal: null } });
    assert.deepEqual(deliveryQuestion.origin.inputs.map(input => ({ sourceSessionId: input.sourceSessionId, requestId: input.requestId,
      actorId: input.actorId, scope: input.scope })),
    [{ sourceSessionId: sessionId, requestId: 'delivery-once', actorId: 'alice', scope: { merchantId: 'merchant-7' } }]);
    assert.deepEqual(preferenceCalls[1].source, { kind: 'tool', callId: 'remote-delivery-preferences',
      rootCallId: 'remote-delivery-preferences', parentCallId: null });
    assert.deepEqual(deliveries, [], 'SDK 尚未回答时，业务不能记录交付方式');
    const deliveryHandle = await session.question(deliveryQuestion.callId);
    const deliveryClaimAbort = new AbortController();
    const deliveryClaim = deliveryHandle.attachWait({ signal: deliveryClaimAbort.signal })[Symbol.asyncIterator]();
    assert.ok((await deliveryClaim.next()).value.remainingMs > 0);
    assert.deepEqual(deliveries, [], '持有前台 claim 不是业务答复');
    const deliveryChildSettled = next(deliveryRequest.events$, event => event.kind === 'tool.settled'
      && event.callId === deliveryQuestion.callId, 'Action human child returned');
    const deliveryReturned = next(deliveryRequest.events$, event => event.kind === 'tool.settled'
      && event.callId === 'remote-delivery-preferences', 'Action returned');
    const preferenceAdmissions = responderAdmissions.length;
    try {
      assert.equal(await deliveryHandle.answer(preferenceAnswer), true);
      const returned = await deliveryReturned, child = await deliveryChildSettled;
      assert.equal(returned.outcome, 'returned'); assert.deepEqual(returned.result, { status: 'recorded', delivery: '站内' });
      assert.equal(child.outcome, 'returned'); assert.equal(child.parentCallId, returned.callId);
      assert.ok(child.sequence < returned.sequence, '原生子问结算后，父 Action 才能发布业务结果');
      assert.deepEqual(returned.inputs.map(input => input.scope), [{ merchant: 'merchant-7' }]);
    } finally { deliveryClaimAbort.abort(); await deliveryClaim.return(); }
    const afterDelivery = await runtime.application.open(sessionId); await afterDelivery.whenIdle(); afterDelivery.release();
    assert.deepEqual(deliveries, [{ sessionId, invocationId: preferenceCalls[1].invocationId, delivery: '站内' }]);
    assert.ok(responderAdmissions.slice(preferenceAdmissions).some(admission => admission.answer !== null));
    for (const admission of responderAdmissions.filter(admission => admission.origin.callId === deliveryQuestion.callId)) {
      assert.equal(admission.actorId, 'alice'); assert.deepEqual(admission.origin, deliveryQuestion.origin);
    }
    assert.deepEqual(await firstValueFrom(session.foregroundQuestions$), []);
    assert.equal(requests, 4);
    const questionObservation = boundQuestion.state.observations$.subscribe();
    try {
      await next(boundQuestion.state.observations$, value => value.status === 'ready', '断线前问题基线');
      const questionDisconnected = next(boundQuestion.state.observations$, value => value.status === 'pending', '问题连接失效');
      const humanSocket = streamSockets.findLast(item => item.actor === 'alice'
        && item.endpoint === 'reactivePrograms/questions' && item.socket.readyState === WebSocket.OPEN);
      assert.ok(humanSocket);
      humanSocket.socket.terminate();
      await questionDisconnected;
      const readAbort = new AbortController(), readReason = new Error('CANCEL_ONLY_QUESTION_READ');
      const interrupted = boundQuestion.state.read(readAbort.signal);
      const deniedRead = assert.rejects(interrupted, error => error === readReason);
      const recovered = boundQuestion.state.read();
      readAbort.abort(readReason); await deniedRead;
      assert.deepEqual((await recovered).settled?.origin, question.origin, '单次读取取消不停止原生连接恢复');
      assert.equal(await session.question(question.callId), boundQuestion, '重连仍复用原问题身份');
      console.info('gateway.human.connection.recovered', { callId: question.callId });
    } finally { questionObservation.unsubscribe(); }
    const submission = { requestId: 'ack-lost', content: '继续独立核验', delivery: 'followup', intent: 'question', scope: null };
    loseSubmitAck = true;
    await assert.rejects(session.submit(submission), error => error instanceof SubmissionAdmissionUnknownError
      && error.sessionId === sessionId && error.requestId === 'ack-lost');
    assert.notEqual((await session.requestStatus('ack-lost')).state, 'absent');
    await session.submit(submission);
    const afterAck = await runtime.application.open(sessionId); await afterAck.whenIdle();
    assert.equal(Object.keys(afterAck.snapshot().requests).filter(id => id === 'ack-lost').length, 1);
    afterAck.release(); assert.equal(requests, 5); assert.equal(executions, 1);
    assert.ok(changes.some(change => change.type === 'append'));
    await journal.dispose();
    await delay(550);
    await assert.rejects(session.invocation('check', id), error => error.code === 'reactive/invocation-unavailable');
    assert.equal(executions, 1);
    blocking = Promise.withResolvers();
    await session.submit({ requestId: 'pause-goal', content: '持续核验直到暂停', delivery: 'followup', intent: 'goal', scope: null });
    await blocking.promise;
    const paused = await session.pause(); assert.equal(paused.phase, 'paused');
    const resumed = await reconnected.resume(sessionId);
    assert.equal(resumed.id, sessionId);
    blocking = Promise.withResolvers();
    await resumed.submit({ requestId: 'cancel-turn', content: '取消当前核验', delivery: 'followup', intent: 'question', scope: null });
    await blocking.promise;
    assert.deepEqual(await resumed.cancel(), { canceled: true });
    blocking = undefined;
    await session.release();
    assert.throws(() => boundQuestion.state.read(), /REACTIVE_CONVERSATION_HANDLE_RELEASED/u);
    assert.throws(() => boundQuestion.answer({ answers: [{ id: 'allow', selected: ['是'] }] }), /REACTIVE_CONVERSATION_HANDLE_RELEASED/u);
    await assert.rejects(boundQuestion.attachWait()[Symbol.asyncIterator]().next(), /REACTIVE_CONVERSATION_HANDLE_RELEASED/u);
    await reconnected.dispose();
    assert.equal(completedObserver.closed, true); assert.equal(completedValueObserver.closed, true); assert.equal(completedValues, true);
    assert.equal(completedActivity.at(-1).status, 'error');
    assert.match(String(completedActivity.at(-1).error), /REACTIVE_REMOTE_CLIENT_DISPOSED/u);
    assert.equal(executions, 1, '已完成调用的读侧撤销不重新执行或改写业务结果');
    assert.deepEqual(runtimeErrors, []); assert.deepEqual(carrierErrors, []);
  } finally {
    finish.resolve(); editFinish.resolve(); await Promise.all(clients.map(client => client.dispose()));
    await gateway.close(); await runtime.dispose();
  }
});

test('获授权远程 notify 读取 SQLite 可靠 follow 与业务终态，沿原请求和原 Goal 继续而不重做 body', async context => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-remote-follow-'));
  const db = new DatabaseSync(join(root, 'business.sqlite'));
  db.exec(`PRAGMA journal_mode = WAL;
    CREATE TABLE operations (command_id TEXT PRIMARY KEY, reference_json TEXT NOT NULL, state_json TEXT NOT NULL, revision INTEGER NOT NULL);
    CREATE TABLE outbox (id TEXT PRIMARY KEY, command_json TEXT NOT NULL, link_json TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0);`);
  const changed = new Subject();
  const clients = [];
  let runtime, gateway, local;
  context.after(async () => {
    for (const client of clients) await client.dispose();
    local?.release(); await gateway?.close(); await runtime?.dispose(); changed.complete(); db.close();
  });
  const registered = Promise.withResolvers(), resumed = Promise.withResolvers();
  const command = z.object({ orderId: z.string(), commandId: z.string() }).strict();
  const reference = z.object({ orderId: z.string(), operationId: z.string() }).strict();
  const state = z.discriminatedUnion('status', [z.object({ status: z.literal('running') }).strict(),
    z.object({ status: z.literal('complete'), artifactId: z.string() }).strict()]);
  const operation = defineOperation({ kind: 'remote-reliable-order', version: 1, command, reference, state,
    settled: value => value.status === 'complete' ? value : undefined });
  const read = id => {
    const row = db.prepare('SELECT reference_json, state_json, revision FROM operations WHERE command_id = ?').get(id);
    return row ? { reference: reference.parse(JSON.parse(row.reference_json)), state: state.parse(JSON.parse(row.state_json)), revision: row.revision } : undefined;
  };
  const registrations = [], reads = [], admissions = [], requests = [], errors = [], carrierErrors = [];
  const authorize = access => {
    access.signal.throwIfAborted();
    if (access.source.kind === 'participant') assert.ok(['wire-notifier', 'operation-publisher'].includes(access.source.actorId));
  };
  const owner = implementOperation(operation, {
    locate(value, access) {
      authorize(access); assert.deepEqual(value, { orderId: 'order-a', commandId: 'prepare-a' });
      reads.push(access.source);
      const found = read(value.commandId);
      return found ? { status: 'accepted', reference: found.reference, state: found.state } : { status: 'unknown' };
    },
    observe(value, access) {
      authorize(access); assert.deepEqual(value, { orderId: 'order-a', operationId: 'operation:prepare-a' });
      return new Observable(subscriber => {
        const emit = () => {
          const found = read('prepare-a');
          if (!found) { subscriber.error(new Error('OPERATION_NOT_ACCEPTED')); return; }
          subscriber.next({ status: 'ready', generation: found.revision, observedAt: Date.now(), value: found.state });
        };
        const subscription = changed.subscribe(emit); emit();
        return () => subscription.unsubscribe();
      });
    },
    registerFollow(value, link, access) {
      authorize(access); registrations.push(access.source);
      assert.deepEqual(value, { orderId: 'order-a', commandId: 'prepare-a' });
      const existing = db.prepare('SELECT command_json, link_json FROM outbox WHERE id = ?').get(link.id);
      if (existing) { assert.deepEqual(JSON.parse(existing.command_json), value); assert.deepEqual(JSON.parse(existing.link_json), link); return; }
      // 同一个真实 SQLite 事务保存 command/link 与待投递事实；ACK 返回前已提交。
      db.exec('BEGIN IMMEDIATE');
      try {
        db.prepare('INSERT INTO outbox (id, command_json, link_json) VALUES (?, ?, ?)').run(link.id, JSON.stringify(value), JSON.stringify(link));
        db.exec('COMMIT');
      } catch (error) { db.exec('ROLLBACK'); throw error; }
      registered.resolve(link.id);
    },
  });
  const prepare = defineAction({ name: 'prepare_order', description: '受理明确订单一次',
    scope: z.object({ orderId: z.string() }).strict(), input: z.object({ commandId: z.string() }).strict(),
    output: z.object({ operation: reference }).strict(),
    operation: { definition: operation, command: (input, scope) => ({ orderId: scope.orderId, commandId: input.commandId }), reference: receipt => receipt.operation } });
  const finish = defineAction({ name: 'finish_order_goal', description: '核验原订单反馈后完成原 Goal', scope: z.null(), input: z.object({}).strict(), output: z.boolean() });
  let bodies = 0;
  const prepareImplementation = implementAction(prepare, { operationOwner: owner, execute(input, call) {
    assert.equal(call.source.kind, 'tool'); assert.equal(call.scope.orderId, 'order-a');
    const persisted = db.prepare('SELECT link_json FROM outbox').get(); assert.ok(persisted);
    const link = operationFollowLinkSchema.parse(JSON.parse(persisted.link_json));
    assert.ok(call.continuation?.goal); assert.deepEqual(link.continuation, call.continuation);
    const value = { orderId: call.scope.orderId, operationId: 'operation:' + input.commandId };
    db.prepare('INSERT INTO operations VALUES (?, ?, ?, 0)').run(input.commandId, JSON.stringify(value), JSON.stringify({ status: 'running' }));
    bodies++; changed.next(); return { operation: value };
  } });
  const finishImplementation = implementAction(finish, { execute(_input, call) { call.completeGoal(); return true; } });
  const surface = defineSurface({ views: {}, actions: { prepare } });
  const program = defineProgram({ name: 'gateway_test', input: prepare.scope, surface });
  const definition = defineApplication({ name: 'remote-reliable-follow', async admit(input, _identity, admission) {
    assert.deepEqual(input.scope, { orderId: 'order-a' });
    assert.ok(['wire-operator', 'wire-notifier', 'operation-publisher'].includes(admission.actorId));
    admissions.push({ actorId: admission.actorId, requestId: input.requestId, ready: input.result?.ready });
  }, environments: [defineEnvironment({ id: 'work', instructions: '沿原订单的权威事实继续原 Goal',
    program: implementProgram(program, { Component() {
      const action = useAction(prepareImplementation, () => ({ orderId: 'order-a' }), []);
      const complete = useAction(finishImplementation, () => null, []);
      return h(Fragment, null, Offer({ name: 'prepare_order', action, follow: { when: 'settled', scope: { orderId: 'order-a' } } }),
        Offer({ name: 'finish_order_goal', action: complete }), Expose({ surface, value: { actions: { prepare: action } } }));
    } }),
  })] });
  async function* tool(name, id, args) {
    yield { type: 'block-start', index: 0, blockType: 'tool-call' };
    yield { type: 'tool-call-delta', index: 0, id: ToolCallId(id), name, argumentsDelta: JSON.stringify(args) };
    yield { type: 'block-end', index: 0, block: { type: 'tool-call', id: ToolCallId(id), name, arguments: JSON.stringify(args) } };
    yield { type: 'finish', reason: { kind: 'tool-calls' } };
  }
  runtime = await createRuntime(() => definition, { home: join(root, 'home'), operations: [owner], serviceActorId: 'operation-publisher', tools: 'native', goalRounds: 3,
    onError: event => errors.push(event.error), model: nativeModel({ provider: 'remote-follow', model: 'test', create: () => new class extends LlmAdapter {
      async *stream(options) {
        requests.push(options);
        if (requests.length === 1) yield* tool('prepare_order', 'native-order', { commandId: 'prepare-a' });
        else if (requests.length === 2) { resumed.resolve(); yield* tool('finish_order_goal', 'finish-original', {}); }
        else {
          yield { type: 'block-start', index: 0, blockType: 'text' };
          yield { type: 'block-end', index: 0, block: { type: 'text', text: '已核验原订单的实际产物' } };
          yield { type: 'finish', reason: { kind: 'stop' } };
        }
      }
    }() }),
  });
  const sessionId = crypto.randomUUID();
  gateway = await mountProgramGateway(runtime.native, { application: runtime.application, model: { provider: 'remote-follow', model: 'test' },
    participant: principal => ({ participant: 'human', actorId: principal.id }),
    access: service => service.registerPolicy('remote-follow-user', { bindings(principal) {
      const actions = ['reactivePrograms/open', 'reactivePrograms/questions', 'reactivePrograms/progress'];
      if (principal.id === 'wire-operator') actions.push('reactivePrograms/submit');
      if (principal.id === 'wire-notifier') actions.push('reactivePrograms/notifyOperation');
      return [{ role: { name: 'follow-participant', resource: 'session', actions }, resource: { kind: 'session', id: sessionId } }];
    } }),
    carrier: { onError: error => carrierErrors.push(error), async authenticate(request) {
      const authorization = request.headers.get('authorization');
      const actorId = authorization?.startsWith('Bearer ') ? authorization.slice('Bearer '.length) : undefined;
      if (!['wire-operator', 'wire-notifier', 'wire-reader'].includes(actorId)) throw new RemoteAuthenticationError(401);
      return { kind: 'remote-follow-user', id: actorId };
    } },
  });
  const connect = async actorId => {
    const client = await createProgramClient(program, { baseUrl: gateway.url,
      fetch: (url, init) => fetch(url, { ...init, headers: { ...Object.fromEntries(new Headers(init?.headers)), authorization: 'Bearer ' + actorId } }),
      createWebSocket: url => new WebSocket(url, { headers: { authorization: 'Bearer ' + actorId } }),
    });
    clients.push(client); return client.open(sessionId);
  };
  const workspace = await runtime.application.createWorkspace({ directory: root });
  local = await runtime.application.create('work', { workspaceId: workspace.workspaceId, sessionId });
  const operator = await connect('wire-operator'), notifier = await connect('wire-notifier'), reader = await connect('wire-reader');
  const wireEvents = [], wireProgress = [];
  const eventSubscription = reader.rawEvents$.subscribe(event => wireEvents.push(event));
  const progressSubscription = operator.progress.observations$.subscribe(value => { if (value.status === 'ready') wireProgress.push(value.value.activity); });
  context.after(() => { eventSubscription.unsubscribe(); progressSubscription.unsubscribe(); });
  await Promise.all([reader.progress.read(), operator.progress.read()]);
  const receiptSeen = next(reader.rawEvents$, event => event.kind === 'operation.follow.changed' && event.change === 'receipt');
  await operator.submit({ requestId: 'wire-original', content: '准备订单甲，并在实际产物就绪后核验完成', delivery: 'followup', intent: 'goal', scope: { orderId: 'order-a' } });
  const followId = await registered.promise;
  await local.whenIdle();
  const row = db.prepare('SELECT command_json, link_json, delivered FROM outbox WHERE id = ?').get(followId);
  assert.ok(row); assert.equal(row.delivered, 0);
  const link = operationFollowLinkSchema.parse(JSON.parse(row.link_json));
  assert.deepEqual(JSON.parse(row.command_json), { orderId: 'order-a', commandId: 'prepare-a' });
  assert.equal(link.continuation.sessionId, sessionId); assert.equal(link.continuation.inputs[0].requestId, 'wire-original');
  assert.ok(link.continuation.goal); assert.equal(local.snapshot().goal.id, link.continuation.goal.id);
  assert.equal(local.snapshot().goal.phase, 'active'); assert.deepEqual(local.snapshot().waits.map(item => item.wait.id), [link.wait.id]);
  assert.equal(bodies, 1); assert.equal(requests.length, 1);
  await next(reader.progress.observations$, value => value.status === 'ready' && value.value.activity.operationFollows.some(item => item.link.id === followId && item.receipt !== null));
  const receiptEvent = await receiptSeen;
  assert.ok(receiptEvent); assert.deepEqual(receiptEvent.inputs, link.continuation.inputs);
  assert.deepEqual(receiptEvent.follow.link, link); assert.equal(receiptEvent.follow.preparedDelivery, null);
  assert.deepEqual((await reader.progress.read()).activity.operationFollows, local.snapshot().operationFollows);
  db.prepare('UPDATE operations SET state_json = ?, revision = revision + 1 WHERE command_id = ?')
    .run(JSON.stringify({ status: 'complete', artifactId: 'artifact-wire' }), 'prepare-a');
  changed.next();
  const beforeDenied = registrations.length;
  await assert.rejects(reader.notifyOperation(followId), error => error.code === 'remote-access/denied');
  assert.equal(registrations.length, beforeDenied, '无 endpoint grant 的 Peer 不得进入业务 follow 准入');
  assert.equal(requests.length, 1);
  const preparedSeen = next(reader.rawEvents$, event => event.kind === 'operation.follow.changed' && event.change === 'delivery-prepared');
  const notified = await notifier.notifyOperation(followId);
  await preparedSeen;
  assert.deepEqual(notified, { status: 'accepted', receipt: { sessionId, requestId: 'operation:' + followId, accepted: true } });
  db.prepare('UPDATE outbox SET delivered = 1 WHERE id = ?').run(followId);
  await resumed.promise; await local.whenIdle();
  assert.equal(bodies, 1); assert.equal(db.prepare('SELECT count(*) AS count FROM operations').get().count, 1);
  assert.ok(registrations.some(source => source.kind === 'participant' && source.actorId === 'wire-notifier' && source.entrypoint === 'operation-feedback'));
  assert.ok(reads.some(source => source.kind === 'participant' && source.actorId === 'wire-notifier'));
  assert.ok(admissions.some(value => value.actorId === 'wire-notifier' && value.requestId === 'operation:' + followId && value.ready === false));
  assert.ok(admissions.some(value => value.actorId === 'operation-publisher' && value.requestId === 'operation:' + followId && value.ready === true));
  assert.equal(local.snapshot().goal.id, link.continuation.goal.id); assert.equal(local.snapshot().goal.phase, 'complete');
  assert.equal(local.snapshot().waits.length, 0); assert.equal(local.snapshot().requests['operation:' + followId].state, 'settled');
  assert.equal(local.snapshot().requests['wire-original'].state, 'settled');
  const native = runtime.native.agents.get(SessionId(sessionId)); assert.ok(native);
  const events = native.session.snapshotEvents();
  await next(reader.progress.observations$, value => value.status === 'ready' && value.value.activity.requests['operation:' + followId]?.state === 'settled');
  await next(operator.progress.observations$, value => value.status === 'ready' && value.value.activity.requests['operation:' + followId]?.state === 'settled');
  const followEvents = wireEvents.filter(event => event.kind === 'operation.follow.changed');
  assert.deepEqual(followEvents.map(event => event.change), ['intent', 'call-associated', 'receipt', 'delivery-prepared']);
  for (const event of followEvents) {
    const nativeEvent = events.find(item => item.seq === event.sequence);
    assert.ok(nativeEvent); assert.equal(event.time, nativeEvent.time);
    assert.equal(pluginRecordOf(nativeEvent).type, 'plugin:reactive/operation-' + ({ intent: 'follow', 'call-associated': 'follow-call', receipt: 'receipt', 'delivery-prepared': 'delivery' })[event.change]);
    if (event.change === 'call-associated') assert.equal(event.follow.callSequence, pluginRecordOf(nativeEvent).data.callSequence);
    assert.deepEqual(event.inputs, link.continuation.inputs); assert.deepEqual(event.follow.link, link);
    assert.equal(event.follow.notStarted, null);
  }
  assert.equal(followEvents[0].follow.receipt, null);
  const delivered = followEvents.find(event => event.change === 'delivery-prepared');
  assert.ok(delivered.follow.receipt); assert.equal(delivered.follow.preparedDelivery.requestId, 'operation:' + followId);
  assert.ok(wireProgress.some(activity => activity.operationFollows.some(item => item.link.id === followId && item.preparedDelivery !== null)
    && !['accepted', 'settled'].includes(activity.requests['operation:' + followId]?.state)), '投递准备切面不能冒充请求受理');
  assert.deepEqual((await reader.progress.read()).activity.operationFollows, local.snapshot().operationFollows);
  const deliveries = events.flatMap(event => { const record = pluginRecordOf(event); return record?.type === 'plugin:reactive/input' && record.data.requestId === 'operation:' + followId ? [record.data] : []; });
  assert.equal(deliveries.length, 1); assert.equal(deliveries[0].actorId, 'operation-publisher');
  assert.deepEqual(deliveries[0].result.continuation, link.continuation); assert.deepEqual(deliveries[0].result.wait, link.wait);
  assert.deepEqual(events.flatMap(event => event.type === 'goal/change' && event.data.operation !== 'clear' && event.data.goal.id === link.continuation.goal.id
    ? [{ operation: event.data.operation, revision: event.data.goal.revision }] : []), [
    { operation: 'create', revision: link.continuation.goal.revision }, { operation: 'resume', revision: link.continuation.goal.revision + 1 },
    { operation: 'complete', revision: link.continuation.goal.revision + 2 },
  ]);
  const resumedText = requests[1].messages.flatMap(message => message.role === 'user' ? message.content : []).map(block => block.type === 'text' ? block.text : '').join('\n');
  assert.match(resumedText, /artifact-wire/u); assert.deepEqual(errors, []); assert.deepEqual(carrierErrors, []);
});

test('远程只读 Operation 槽捕获原命令，F1 后新 Follow 等 F2，Date 反馈与冷恢复不登记或重执行', async context => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-remote-feedback-'));
  const db = new DatabaseSync(join(root, 'business.sqlite'));
  db.exec(`PRAGMA journal_mode = WAL;
    CREATE TABLE accepted (command_id TEXT PRIMARY KEY, reference_json TEXT NOT NULL);
    CREATE TABLE outbox (id TEXT PRIMARY KEY, command_json TEXT NOT NULL, link_json TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE feedback (seq INTEGER PRIMARY KEY AUTOINCREMENT, node_id TEXT UNIQUE NOT NULL, command_id TEXT NOT NULL, payload_json TEXT NOT NULL);`);
  const selection = new BehaviorSubject('prepare-a'), commits = new Subject();
  const clients = [], registrations = [], feedbackReads = [], requests = [], errors = [], carrierErrors = [];
  const firstRegistered = Promise.withResolvers(), secondRegistered = Promise.withResolvers(), finished = Promise.withResolvers();
  let runtime, gateway, local, bodies = 0;
  context.after(async () => {
    for (const client of clients) await client.dispose();
    local?.release(); await gateway?.close(); await runtime?.dispose(); selection.complete(); commits.complete(); db.close();
  });
  const date = z.codec(z.iso.datetime(), z.date(), { decode: value => new Date(value), encode: value => value.toISOString() });
  const command = z.object({ orderId: z.string(), commandId: z.string() }).strict();
  const reference = z.object({ orderId: z.string(), operationId: z.string() }).strict();
  const operation = defineOperation({ kind: 'remote-feedback-order', version: 1, command, reference,
    state: z.object({ status: z.literal('running') }).strict(), settled: () => undefined,
    feedback: z.object({ stage: z.enum(['first', 'second']), at: date }).strict() });
  const original = { orderId: 'order-a', commandId: 'prepare-a' };
  const authorize = access => {
    access.signal.throwIfAborted();
    if (access.source.kind === 'participant') assert.ok(['operator', 'notifier', 'reader', 'operation-publisher'].includes(access.source.actorId));
  };
  const accepted = value => {
    const row = db.prepare('SELECT reference_json FROM accepted WHERE command_id = ?').get(value.commandId);
    return row ? reference.parse(JSON.parse(row.reference_json)) : undefined;
  };
  const owner = implementOperation(operation, {
    locate(value, access) {
      authorize(access); const found = accepted(value);
      return found ? { status: 'accepted', reference: found, state: { status: 'running' } } : { status: 'unknown' };
    },
    observe(value, access) { authorize(access); assert.equal(value.orderId, 'order-a'); return of({ status: 'ready', generation: 0, observedAt: Date.now(), value: { status: 'running' } }); },
    registerFollow(value, link, access) {
      authorize(access);
      if (access.source.kind === 'participant') assert.ok(['notifier', 'operation-publisher'].includes(access.source.actorId));
      registrations.push({ id: link.id, source: access.source });
      const previous = db.prepare('SELECT command_json, link_json FROM outbox WHERE id = ?').get(link.id);
      if (previous) { assert.deepEqual(JSON.parse(previous.command_json), value); assert.deepEqual(JSON.parse(previous.link_json), link); return; }
      db.exec('BEGIN IMMEDIATE');
      try {
        db.prepare('INSERT INTO outbox (id, command_json, link_json) VALUES (?, ?, ?)').run(link.id, JSON.stringify(value), JSON.stringify(link));
        db.exec('COMMIT');
      } catch (error) { db.exec('ROLLBACK'); throw error; }
      const count = db.prepare('SELECT count(*) AS count FROM outbox').get().count;
      (count === 1 ? firstRegistered : secondRegistered).resolve(link.id);
    },
    readFeedback(value, link, access) {
      authorize(access); feedbackReads.push({ command: value, id: link.id, source: access.source });
      const registered = db.prepare('SELECT command_json, link_json FROM outbox WHERE id = ?').get(link.id);
      assert.ok(registered); assert.deepEqual(JSON.parse(registered.command_json), value); assert.deepEqual(JSON.parse(registered.link_json), link);
      const found = accepted(value); if (!found) return { status: 'unknown' };
      let after = 0;
      if (link.after !== null) {
        const previous = db.prepare('SELECT seq FROM feedback WHERE command_id = ? AND node_id = ?').get(value.commandId, link.after);
        assert.ok(previous, 'after 必须属于同一个业务操作，opaque ID 不由框架排序'); after = previous.seq;
      }
      const node = db.prepare('SELECT node_id, payload_json FROM feedback WHERE command_id = ? AND seq > ? ORDER BY seq LIMIT 1').get(value.commandId, after);
      return node ? { status: 'ready', reference: found, node: { id: node.node_id, payload: JSON.parse(node.payload_json) } } : { status: 'pending', reference: found };
    },
  });
  const prepare = defineAction({ name: 'start_order', description: '只受理一次订单准备', scope: z.null(), input: z.object({}).strict(), output: reference,
    operation: { definition: operation, command: () => original, reference: value => value } });
  const finish = defineAction({ name: 'finish_feedback_goal', description: '核验第二节点后完成原 Goal', scope: z.null(), input: z.object({}).strict(), output: z.boolean() });
  const implementation = implementAction(prepare, { operationOwner: owner, execute() {
    const value = { orderId: 'order-a', operationId: 'operation:prepare-a' };
    db.prepare('INSERT INTO accepted VALUES (?, ?)').run('prepare-a', JSON.stringify(value)); bodies++; return value;
  } });
  const completion = implementAction(finish, { execute(_input, call) { call.completeGoal(); return true; } });
  const surface = defineSurface({ views: {}, actions: { prepare }, operations: { preparation: operation } });
  const program = defineProgram({ name: 'gateway_test', input: z.object({ orderId: z.string() }).strict(), surface });
  function OperationPanel({ commandId }) {
    const binding = useOperation(owner, () => ({ orderId: 'order-a', commandId }), [commandId]);
    useLayoutEffect(() => { commits.next(commandId); return () => commits.next(null); }, [commandId]);
    return h(Fragment, null, Follow({ name: 'wait_order_feedback', description: '显式等待原订单的后续反馈节点', operation: binding, when: 'feedback', scope: { orderId: 'order-a' } }),
      Expose({ surface, value: { operations: { preparation: binding } } }));
  }
  const definition = defineApplication({ name: 'remote-feedback', async admit(input, _identity, access) {
    assert.deepEqual(input.scope, { orderId: 'order-a' }); assert.ok(['operator', 'notifier', 'operation-publisher'].includes(access.actorId));
  }, environments: [defineEnvironment({ id: 'work', instructions: '收到F1后显式再次等待同操作F2，不重发原业务命令',
    program: implementProgram(program, { Component() {
      const action = useAction(implementation, () => null, []), complete = useAction(completion, () => null, []);
      const selected = useObservable(() => selection, []);
      return h(Fragment, null, Offer({ name: 'start_order', action, follow: { when: 'feedback', scope: { orderId: 'order-a' } } }),
        Offer({ name: 'finish_feedback_goal', action: complete }), Expose({ surface, value: { actions: { prepare: action } } }),
        selected.status === 'ready' && selected.value !== null ? h(OperationPanel, { commandId: selected.value }) : null);
    } }),
  })] });
  async function* tool(name, id, args) {
    yield { type: 'block-start', index: 0, blockType: 'tool-call' };
    yield { type: 'tool-call-delta', index: 0, id: ToolCallId(id), name, argumentsDelta: JSON.stringify(args) };
    yield { type: 'block-end', index: 0, block: { type: 'tool-call', id: ToolCallId(id), name, arguments: JSON.stringify(args) } };
    yield { type: 'finish', reason: { kind: 'tool-calls' } };
  }
  runtime = await createRuntime(() => definition, { home: join(root, 'home'), operations: [owner], serviceActorId: 'operation-publisher', tools: 'native', goalRounds: 5,
    onError: event => errors.push(event.error), model: nativeModel({ provider: 'remote-feedback', model: 'test', create: () => new class extends LlmAdapter {
      async *stream(options) {
        requests.push(options);
        if (requests.length === 1) yield* tool('start_order', 'start-original-order', {});
        else if (requests.length === 2) yield* tool('wait_order_feedback', 'wait-second-node', { after: 'node-1' });
        else if (requests.length === 3) { finished.resolve(); yield* tool('finish_feedback_goal', 'complete-original-goal', {}); }
        else { yield { type: 'block-start', index: 0, blockType: 'text' }; yield { type: 'block-end', index: 0, block: { type: 'text', text: '同操作两次反馈已核验' } }; yield { type: 'finish', reason: { kind: 'stop' } }; }
      }
    }() }),
  });
  const sessionId = crypto.randomUUID();
  gateway = await mountProgramGateway(runtime.native, { application: runtime.application, model: { provider: 'remote-feedback', model: 'test' },
    participant: principal => ({ participant: 'human', actorId: principal.id }), access: service => service.registerPolicy('feedback-peer', { bindings(principal) {
      const actions = ['reactivePrograms/open', 'reactivePrograms/questions', 'reactivePrograms/progress'];
      if (principal.id === 'operator') actions.push('reactivePrograms/submit', 'reactivePrograms/resolveOperation', 'reactivePrograms/locateOperation', 'reactivePrograms/readOperationFeedback');
      if (principal.id === 'notifier') actions.push('reactivePrograms/notifyOperation');
      if (principal.id === 'reader') actions.push('reactivePrograms/readOperationFeedback');
      return [{ role: { name: 'feedback-peer', resource: 'session', actions }, resource: { kind: 'session', id: sessionId } }];
    } }), carrier: { onError: error => carrierErrors.push(error), async authenticate(request) {
      const header = request.headers.get('authorization'), actorId = header?.startsWith('Bearer ') ? header.slice(7) : undefined;
      if (!['operator', 'notifier', 'reader'].includes(actorId)) throw new RemoteAuthenticationError(401);
      return { kind: 'feedback-peer', id: actorId };
    } },
  });
  const workspace = await runtime.application.createWorkspace({ directory: root });
  local = await runtime.application.create('work', { workspaceId: workspace.workspaceId, sessionId });
  const connect = async actorId => {
    const client = await createProgramClient(program, { baseUrl: gateway.url,
      fetch: (url, init) => fetch(url, { ...init, headers: { ...Object.fromEntries(new Headers(init?.headers)), authorization: 'Bearer ' + actorId } }),
      createWebSocket: url => new WebSocket(url, { headers: { authorization: 'Bearer ' + actorId } }),
    }); clients.push(client); return client.open(sessionId);
  };
  const operator = await connect('operator'), notifier = await connect('notifier'), reader = await connect('reader');
  const wireEvents = [], wireProgress = [];
  const eventSubscription = reader.rawEvents$.subscribe(event => wireEvents.push(event));
  const progressSubscription = operator.progress.observations$.subscribe(value => { if (value.status === 'ready') wireProgress.push(value.value.activity); });
  context.after(() => { eventSubscription.unsubscribe(); progressSubscription.unsubscribe(); });
  await Promise.all([reader.progress.read(), operator.progress.read()]);
  const captured = await operator.operations.preparation();
  assert.deepEqual(captured.command, original); assert.deepEqual(await captured.locate(), { status: 'unknown' });
  await operator.submit({ requestId: 'two-feedbacks', content: '受理后分别核验两个阶段反馈', delivery: 'followup', intent: 'goal', scope: { orderId: 'order-a' } });
  const firstId = await firstRegistered.promise; await local.whenIdle();
  assert.equal(bodies, 1); assert.equal(requests.length, 1);
  const first = captured.participation(firstId), beforeRead = registrations.length;
  assert.equal((await first.readFeedback()).status, 'pending'); assert.equal(registrations.length, beforeRead);
  const beforeDenied = feedbackReads.length;
  await assert.rejects(notifier.readOperationFeedback(operation, original, firstId), error => error.code === 'remote-access/denied');
  assert.equal(feedbackReads.length, beforeDenied);
  db.prepare('INSERT INTO feedback (node_id, command_id, payload_json) VALUES (?, ?, ?)').run('node-1', 'prepare-a', JSON.stringify({ stage: 'first', at: '2026-10-27T00:00:01Z' }));
  assert.equal((await notifier.notifyOperation(firstId)).status, 'accepted');
  const secondId = await secondRegistered.promise; await local.whenIdle();
  assert.notEqual(secondId, firstId); assert.equal(bodies, 1); assert.equal(requests.length, 2);
  const secondLink = operationFollowLinkSchema.parse(JSON.parse(db.prepare('SELECT link_json FROM outbox WHERE id = ?').get(secondId).link_json));
  assert.equal(secondLink.after, 'node-1'); assert.deepEqual(secondLink.command, original);
  const firstValue = await first.readFeedback(); assert.equal(firstValue.status, 'ready'); assert.ok(firstValue.node.payload.at instanceof Date);
  assert.equal(firstValue.node.payload.at.toISOString(), '2026-10-27T00:00:01.000Z');
  const changed = next(commits, value => value === 'prepare-b', '新command的组件提交'); selection.next('prepare-b'); await changed;
  const replacement = await operator.operations.preparation(); assert.equal(replacement.command.commandId, 'prepare-b');
  assert.deepEqual(captured.command, original); assert.equal((await captured.participation(secondId).readFeedback()).status, 'pending');
  const removed = next(commits, value => value === null, 'Operation组件撤下'); selection.next(null); await removed;
  await assert.rejects(operator.operations.preparation());
  assert.equal((await captured.participation(secondId).readFeedback()).status, 'pending');
  db.prepare('INSERT INTO feedback (node_id, command_id, payload_json) VALUES (?, ?, ?)').run('node-2', 'prepare-a', JSON.stringify({ stage: 'second', at: '2026-10-27T00:00:02Z' }));
  const secondPrepared = next(reader.rawEvents$, event => event.kind === 'operation.follow.changed' && event.change === 'delivery-prepared' && event.follow.link.id === secondId);
  assert.equal((await notifier.notifyOperation(secondId)).status, 'accepted'); await finished.promise; await local.whenIdle();
  await secondPrepared;
  const firstAgain = await first.readFeedback(), second = await captured.participation(secondId).readFeedback();
  assert.equal(firstAgain.status, 'ready'); assert.equal(firstAgain.node.id, 'node-1');
  assert.equal(second.status, 'ready'); assert.equal(second.node.id, 'node-2'); assert.ok(second.node.payload.at instanceof Date);
  assert.equal(second.node.payload.at.toISOString(), '2026-10-27T00:00:02.000Z');
  assert.equal(local.snapshot().goal.id, secondLink.continuation.goal.id); assert.equal(local.snapshot().goal.phase, 'complete');
  assert.equal(local.snapshot().requests['operation:' + firstId].state, 'settled'); assert.equal(local.snapshot().requests['operation:' + secondId].state, 'settled');
  await next(reader.progress.observations$, value => value.status === 'ready' && value.value.activity.requests['operation:' + secondId]?.state === 'settled');
  await next(operator.progress.observations$, value => value.status === 'ready' && value.value.activity.requests['operation:' + secondId]?.state === 'settled');
  const nativeEvents = runtime.native.agents.get(SessionId(sessionId)).session.snapshotEvents();
  for (const id of [firstId, secondId]) {
    const changes = wireEvents.filter(event => event.kind === 'operation.follow.changed' && event.follow.link.id === id);
    assert.deepEqual(changes.map(event => event.change), id === firstId ? ['intent', 'call-associated', 'receipt', 'delivery-prepared'] : ['intent', 'call-associated', 'delivery-prepared']);
    for (const event of changes) {
      const native = nativeEvents.find(item => item.seq === event.sequence);
      assert.ok(native); assert.equal(event.time, native.time); assert.deepEqual(event.inputs, event.follow.link.continuation.inputs);
      assert.equal(pluginRecordOf(native).type, 'plugin:reactive/operation-' + ({ intent: 'follow', 'call-associated': 'follow-call', receipt: 'receipt', 'delivery-prepared': 'delivery' })[event.change]);
      if (event.change === 'call-associated') assert.equal(event.follow.callSequence, pluginRecordOf(native).data.callSequence);
      assert.equal(event.follow.notStarted, null);
    }
    const prepared = changes.at(-1).follow;
    assert.equal(prepared.preparedDelivery.node.id, id === firstId ? 'node-1' : 'node-2');
    assert.equal(typeof prepared.preparedDelivery.node.payload.at, 'string', 'follow保持原领域wire，显式readFeedback才解码Date');
    assert.ok(wireProgress.some(activity => activity.operationFollows.some(item => item.link.id === id && item.preparedDelivery !== null)
      && !['accepted', 'settled'].includes(activity.requests['operation:' + id]?.state)));
  }
  const durableFollows = local.snapshot().operationFollows;
  assert.deepEqual((await reader.progress.read()).activity.operationFollows, durableFollows);
  await runtime.application.close(sessionId); assert.equal(runtime.native.agents.get(SessionId(sessionId)), undefined);
  const beforeCold = registrations.length, beforeQueries = feedbackReads.length, beforeModel = requests.length;
  const restored = await reader.readOperationFeedback(operation, original, secondId);
  const cold = await connect('reader'), coldEvents = [];
  const coldSubscription = cold.rawEvents$.subscribe(event => coldEvents.push(event));
  context.after(() => coldSubscription.unsubscribe());
  assert.deepEqual((await cold.progress.read()).activity.operationFollows, durableFollows);
  await cold.readOperationFeedback(operation, original, secondId);
  assert.deepEqual(coldEvents, [], '冷open只交付同源基线，不回放历史follow事件');
  assert.equal(restored.status, 'ready'); assert.equal(restored.node.id, 'node-2'); assert.ok(restored.node.payload.at instanceof Date);
  assert.equal(registrations.length, beforeCold); assert.equal(feedbackReads.length, beforeQueries + 2); assert.equal(requests.length, beforeModel);
  assert.equal(runtime.native.agents.get(SessionId(sessionId)), undefined);
  await assert.rejects(reader.readOperationFeedback(operation, { orderId: 'order-a', commandId: 'prepare-b' }, secondId));
  assert.equal(feedbackReads.length, beforeQueries + 2, '错误command必须在owner读取前拒绝');
  assert.equal(bodies, 1); assert.equal(db.prepare('SELECT count(*) AS count FROM accepted').get().count, 1);
  assert.deepEqual(errors, []); assert.deepEqual(carrierErrors, []);
});

test('纯 Program 将同 step 原始输入分别解为本地和远程 Date；Work、请求、重连及冷 journal 保持真实身份', async context => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-request-progress-'));
  const prefix = Promise.withResolvers(), continueChunk = Promise.withResolvers(), secondChunk = Promise.withResolvers(), finish = Promise.withResolvers();
  const maintenanceGate = Promise.withResolvers();
  const errors = [], carrierErrors = [], clients = [], sockets = [], wire = [];
  const surface = defineSurface({ views: {}, actions: {} });
  const program = defineProgram({ name: 'gateway_test', input: z.object({ at: z.iso.datetime().transform(value => new Date(value)) }).strict(), surface });
  let remoteInputDecodes = 0;
  // 独立的消费者合同仅计数 SDK transform，宿主仍使用原 Program 的相同单向 Date 合同。
  const remoteProgram = defineProgram({ name: program.name, version: program.version, surface,
    input: z.object({ at: z.iso.datetime().transform(value => { remoteInputDecodes++; return new Date(value); }) }).strict() });
  const scopes = { 'request-a': { at: '2026-10-27T00:00:07.000Z' }, 'request-b': { at: '2026-10-27T00:00:08.000Z' } };
  const definition = defineApplication({ name: 'request-progress', admit: async (input, _identity, access) => {
    assert.equal(access.actorId, 'request-observer');
    assert.deepEqual(input.scope, scopes[input.requestId]);
  }, environments: [defineEnvironment({ id: 'work',
    instructions: '同时核验本步已进入的输入', program: implementProgram(program, { Component: () => null }) })] });
  let models = 0;
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), tools: 'native', onError: event => errors.push(event.error),
    model: nativeModel({ provider: 'request-progress', model: 'test', create: () => new class extends LlmAdapter {
      async *stream() {
        models++;
        yield { type: 'block-start', index: 0, blockType: 'text' };
        yield { type: 'text-delta', index: 0, text: '共同处理' };
        prefix.resolve(); await continueChunk.promise;
        yield { type: 'text-delta', index: 0, text: '两个输入' };
        secondChunk.resolve(); await finish.promise;
        yield { type: 'block-end', index: 0, block: { type: 'text', text: '共同处理两个输入' } };
        yield { type: 'finish', reason: { kind: 'stop' } };
      }
    }() }),
  });
  const workspace = await runtime.application.createWorkspace({ directory: root });
  const local = await runtime.application.create('work', { workspaceId: workspace.workspaceId }), sessionId = local.id;
  const role = { name: 'request-reader', resource: 'session', actions: ['reactivePrograms/open', 'reactivePrograms/questions',
    'reactivePrograms/progress', 'reactivePrograms/journal', 'reactivePrograms/journalPage', 'reactivePrograms/submit', 'reactivePrograms/resume'] };
  const gateway = await mountProgramGateway(runtime.native, { application: runtime.application, model: { provider: 'request-progress', model: 'test' },
    participant: principal => ({ participant: 'human', actorId: principal.id }), access: service => service.registerPolicy('request-peer', { bindings: () => [{ role, resource: { kind: 'session', id: sessionId } }] }),
    carrier: { onError: error => carrierErrors.push(error), authenticate: async () => ({ kind: 'request-peer', id: 'request-observer' }) },
  });
  context.after(async () => {
    maintenanceGate.resolve(); continueChunk.resolve(); finish.resolve();
    await Promise.all(clients.map(client => client.dispose())); local.release(); await gateway.close(); await runtime.dispose();
  });
  const connect = async () => {
    const group = crypto.randomUUID();
    const client = await createProgramClient(remoteProgram, { baseUrl: gateway.url, createWebSocket: url => {
      const socket = new WebSocket(url), tracked = { group, socket, endpoint: undefined };
      sockets.push(tracked);
      const send = socket.send.bind(socket);
      socket.send = (data, ...rest) => {
        const message = JSON.parse(String(data));
        if (message.type === 'open') tracked.endpoint = message.endpoint;
        return send(data, ...rest);
      };
      socket.on('message', data => {
        const message = JSON.parse(String(data));
        if (tracked.endpoint === 'reactivePrograms/progress' && message.type === 'item') wire.push({ group, socket, value: message.value });
      });
      return socket;
    } });
    clients.push(client); return { group, client, session: await client.open(sessionId) };
  };
  await runtime.application.close(sessionId);
  assert.equal(runtime.native.agents.get(SessionId(sessionId)), undefined);
  const early = await connect(), a = early.session.request('request-a'), b = early.session.request('request-b');
  assert.equal(early.session.request('request-a'), a);
  const occurrences = [], raw = [], changes = [];
  const selector = createWorkSelector({ identify: origin => origin.kind === 'invocation' ? undefined : origin.requestId });
  const workSources = new BehaviorSubject({ status: 'ready', generation: 0, observedAt: Date.now(), value: [early.session] });
  const workLifetime = new AbortController();
  context.after(() => { workLifetime.abort(new Error('WORK_OBSERVATION_DISPOSED')); workSources.complete(); });
  const remoteWork = selector.observe('request-b', { sessions: workSources }, { signal: workLifetime.signal });
  const localWork = selector.observe('request-b', { sessions: [local] });
  const [earlyWorkMember] = await remoteWork.sessions.read();
  const workEvents = [];
  let workCompleted = false;
  const workSubscription = remoteWork.events$.subscribe({ next: value => workEvents.push(value), complete: () => { workCompleted = true; } });
  const frames = early.session.progressFrames$.subscribe(value => raw.push(value));
  const cold = await a.state.read();
  assert.equal(cold.source, 'durable'); assert.equal(cold.execution, 'unavailable'); assert.equal(cold.activity, null);
  assert.deepEqual(cold.attempts, []);
  const journal = await early.session.openEvents({ publish: change => changes.push(change), failed: error => errors.push(error) });
  await journal.prepend({ maxMessages: 10 });
  assert.equal(runtime.native.agents.get(SessionId(sessionId)), undefined);
  assert.equal(models, 0); assert.deepEqual(occurrences, []);
  assert.equal(sockets.filter(item => item.group === early.group && item.endpoint === 'reactivePrograms/progress').length, 1);

  const resumed = await early.client.resume(sessionId);
  const native = runtime.native.agents.get(SessionId(sessionId)); assert.ok(native);
  // 两份输入都经真实 Host 准入；原生 maintenance 暂存唤醒，公开 inbox 接口安排同一步消费。
  const maintenance = native.runMaintenance(() => maintenanceGate.promise);
  const acceptedWork = next(remoteWork.events$, value => value.event.kind === 'input.accepted');
  await early.session.submit({ requestId: 'request-b', content: '另一个真实已受理输入', delivery: 'followup', intent: 'question', scope: scopes['request-b'] });
  const accepted = await acceptedWork;
  assert.equal(remoteInputDecodes, 0, '真实远程 raw-only Work 消费不执行业务 input transform');
  assert.deepEqual(accepted.matchedInputs.map(input => input.requestId), ['request-b']);
  assert.deepEqual(accepted.event.inputs[0].scope, scopes['request-b']);
  assert.deepEqual(accepted.source, { sessionId, workspaceId: local.identity.workspaceId,
    environmentId: local.identity.environmentId, parentSessionId: local.identity.parentSessionId });
  assert.equal(early.session.identity.parentSessionId, local.identity.parentSessionId);
  const events = a.events$.subscribe(value => occurrences.push(value));
  const position = native.inbox.nextTurn.findIndex(message => message.source.kind === 'user' && message.source.rpcId === 'request-b');
  assert.ok(position >= 0);
  const [coInput] = native.inbox.splice('next-turn', position, 1, []);
  native.inbox.splice('next-step', native.inbox.nextStep.length, 0, [coInput]);
  await early.session.submit({ requestId: 'request-a', content: '核验同一步的输入', delivery: 'followup', intent: 'question', scope: scopes['request-a'] });
  maintenanceGate.resolve(); await maintenance;
  await prefix.promise;
  const activeA = await next(a.state$, value => value.status === 'ready' && value.value.attempts.length === 1);
  const activeB = await next(b.state$, value => value.status === 'ready' && value.value.attempts.length === 1);
  const inputs = activeA.value.attempts[0].inputs;
  assert.deepEqual(inputs.map(input => input.requestId).sort(), ['request-a', 'request-b']);
  assert.equal(inputs.find(input => input.requestId === 'request-a').actorId, 'request-observer');
  assert.equal(inputs.find(input => input.requestId === 'request-b').actorId, 'request-observer');
  for (const input of inputs) {
    assert.equal(input.sourceSessionId, sessionId); assert.equal(input.source, 'user');
    assert.deepEqual(input.scope, scopes[input.requestId], '当前 progress 保留真正消费的原始 JSON');
  }
  assert.equal(activeA.value.attempts[0].attemptId, activeB.value.attempts[0].attemptId);
  const activeWork = value => value.status === 'ready' && value.value.sessions[0].attempts.length === 1;
  const [remoteActiveWork, localActiveWork] = await Promise.all([next(remoteWork.state$, activeWork), next(localWork.state$, activeWork)]);
  for (const observed of [remoteActiveWork, localActiveWork]) {
    const session = observed.value.sessions[0], occurrence = session.attempts[0];
    assert.deepEqual(session.identity, accepted.source);
    assert.equal(occurrence.value.attemptId, activeA.value.attempts[0].attemptId);
    assert.deepEqual(occurrence.value.inputs.map(input => input.requestId).sort(), ['request-a', 'request-b']);
    assert.deepEqual(occurrence.matchedInputs.map(input => input.requestId), ['request-b']);
    for (const input of occurrence.value.inputs) assert.deepEqual(input.scope, scopes[input.requestId]);
  }
  assert.ok(remoteInputDecodes > 0, 'typed 消费者恢复后才执行 SDK 领域解码');

  const late = await connect(), lateA = late.session.request('request-a'), lateEvents = [];
  const lateSubscription = lateA.events$.subscribe(value => lateEvents.push(value));
  const opened = await next(lateA.state$, value => value.status === 'ready' && value.value.attempts.length === 1);
  const attempt = opened.value.attempts[0], members = expandAssistantStream(attempt.stream);
  assert.equal(attempt.nextIndex, members.length);
  assert.ok(members.some(member => member.chunk.type === 'block-start'));
  assert.equal(members.filter(member => member.chunk.type === 'text-delta').map(member => member.chunk.text).join(''), '共同处理');
  assert.deepEqual(lateEvents, [], '迟订阅 prefix 是基线，不广播为新 token');
  assert.equal(wire.filter(item => item.group === late.group && item.value.type === 'baseline').length, 1);
  const isSecondDelta = value => value.kind === 'assistant.stream' && value.frame.phase === 'text' && value.frame.text === '两个输入';
  const earlyDelta = next(a.events$, isSecondDelta), lateDelta = next(lateA.events$, isSecondDelta);
  continueChunk.resolve(); await secondChunk.promise;
  const extended = value => value.status === 'ready' && value.value.attempts.some(current => expandAssistantStream(current.stream)
    .filter(member => member.chunk.type === 'text-delta').map(member => member.chunk.text).join('') === '共同处理两个输入');
  await Promise.all([next(a.state$, extended), next(lateA.state$, extended), earlyDelta, lateDelta]);
  assert.ok(raw.some(value => value.frame.type === 'chunk' && value.frame.chunk.type === 'block-start'));
  assert.ok(lateEvents.some(value => value.kind === 'assistant.stream' && value.frame.phase === 'text' && value.frame.text === '两个输入'));
  assert.equal(wire.filter(item => item.group === late.group && item.value.type === 'baseline').length, 1, 'active chunk 不重发完整 prefix');
  const increment = wire.find(item => item.group === late.group && item.value.type === 'assistant' && item.value.progress.frame.type === 'chunk');
  assert.equal(increment.value.progress.frame.chunk.type, 'text-delta');
  assert.equal(increment.value.progress.frame.chunk.text, '两个输入');

  const beforeReconnect = occurrences.length;
  const workBeforeReconnect = workEvents.length;
  const disconnected = next(a.state$, value => value.status === 'pending');
  const originalSocket = sockets.find(item => item.group === early.group && item.endpoint === 'reactivePrograms/progress').socket;
  originalSocket.terminate(); await disconnected;
  const stopped = new AbortController(), stoppedReason = new Error('CANCEL_ONLY_PROGRESS_READ');
  const cancelledRead = assert.rejects(a.state.read(stopped.signal), error => error === stoppedReason);
  const recoveredRead = a.state.read();
  stopped.abort(stoppedReason); await cancelledRead;
  assert.equal((await recoveredRead).attempts[0]?.attemptId, attempt.attemptId);
  assert.equal((await early.session.progress.read()).attempts[0]?.attemptId, attempt.attemptId);
  await next(a.state$, extended);
  assert.equal(occurrences.length, beforeReconnect, '重连全 prefix 不重放已发生事件');
  assert.equal(workEvents.length, workBeforeReconnect, 'Work 同样把重连 prefix 当基线，不重放事件');
  assert.equal(workCompleted, false);
  assert.ok(sockets.filter(item => item.group === early.group && item.endpoint === 'reactivePrograms/progress').length >= 2);
  assert.equal(sockets.filter(item => item.group === early.group && item.endpoint === 'reactivePrograms/progress' && item.socket.readyState === WebSocket.OPEN).length, 1);
  const beforeReplacement = workEvents.length;
  workSources.next({ status: 'ready', generation: 1, observedAt: Date.now(), value: [late.session] });
  const [lateWorkMember] = await remoteWork.sessions.read();
  assert.notEqual(lateWorkMember, earlyWorkMember, '同会话的新SDK来源建立独立成员，旧成员不被重定向');
  assert.deepEqual(lateWorkMember.identity, earlyWorkMember.identity);
  assert.equal((await remoteWork.state.read()).sessions[0].attempts[0].value.attemptId, attempt.attemptId);
  assert.equal(workEvents.length, beforeReplacement, '动态替换来源的prefix仍是基线，不回放历史事件');
  events.unsubscribe(); frames.unsubscribe(); lateSubscription.unsubscribe();
  assert.equal(sockets.filter(item => item.group === late.group && item.endpoint === 'reactivePrograms/progress' && item.socket.readyState === WebSocket.OPEN).length, 1,
    '消费者退出不取消 Session owner 连接');
  const messagesA = next(a.events$, value => value.kind === 'assistant.message'), messagesB = next(b.events$, value => value.kind === 'assistant.message');
  const localMessage = next(local.request('request-a').events$, value => value.kind === 'assistant.message');
  const rawMessage = next(local.rawEvents$, value => value.kind === 'assistant.message');
  const workMessage = next(remoteWork.events$, value => value.event.kind === 'assistant.message');
  finish.resolve();
  const [messageA, messageB, localFact, rawFact, workFact] = await Promise.all([messagesA, messagesB, localMessage, rawMessage, workMessage]);
  assert.equal(messageA.messageId, messageB.messageId); assert.equal(messageA.text, '共同处理两个输入');
  assert.equal(localFact.messageId, messageA.messageId); assert.equal(rawFact.messageId, messageA.messageId);
  assert.deepEqual(workFact.event, rawFact);
  assert.deepEqual(workFact.source, accepted.source);
  assert.deepEqual(workFact.matchedInputs.map(input => input.requestId), ['request-b']);
  for (const fact of [messageA, messageB, localFact]) {
    assert.deepEqual(fact.inputs.map(input => input.requestId).sort(), ['request-a', 'request-b']);
    for (const input of fact.inputs) {
      assert.equal(input.sourceSessionId, sessionId); assert.equal(input.actorId, 'request-observer'); assert.equal(input.source, 'user');
      assert.ok(input.scope.at instanceof Date);
      assert.equal(input.scope.at.toISOString(), scopes[input.requestId].at);
    }
  }
  const wireFact = wire.find(item => item.value.type === 'event' && item.value.event.kind === 'assistant.message'
    && item.value.event.messageId === messageA.messageId)?.value.event;
  assert.ok(wireFact);
  for (const fact of [rawFact, wireFact]) {
    assert.deepEqual(fact.inputs.map(input => input.requestId).sort(), ['request-a', 'request-b']);
    for (const input of fact.inputs) {
      assert.equal(input.sourceSessionId, sessionId); assert.equal(input.actorId, 'request-observer'); assert.equal(input.source, 'user');
      assert.deepEqual(input.scope, scopes[input.requestId], '传输直接消费原始事实，不逆编码单向 transform');
    }
  }
  const settled = await next(a.state$, value => value.status === 'ready' && value.value.activity?.state === 'settled');
  assert.equal(settled.value.activity.reply, messageA.text); assert.deepEqual(settled.value.attempts, []);
  assert.equal((await next(b.state$, value => value.status === 'ready' && value.value.activity?.state === 'settled')).value.activity.reply, messageA.text);
  const repliedWork = value => value.status === 'ready' && value.value.sessions[0].replies.some(reply => reply.value.messageId === messageA.messageId);
  const [remoteReplyWork, localReplyWork] = await Promise.all([next(remoteWork.state$, repliedWork), next(localWork.state$, repliedWork)]);
  const remoteReply = remoteReplyWork.value.sessions[0].replies.find(reply => reply.value.messageId === messageA.messageId);
  const localReply = localReplyWork.value.sessions[0].replies.find(reply => reply.value.messageId === messageA.messageId);
  assert.deepEqual(remoteReply, localReply, '网络与本地 Work 保留同一真实 reply occurrence');
  assert.equal(remoteReply.value.sequence, rawFact.sequence);
  assert.equal(remoteReply.value.text, messageA.text);
  assert.deepEqual(remoteReply.value.inputs.map(input => input.requestId).sort(), ['request-a', 'request-b']);
  assert.deepEqual(remoteReply.matchedInputs.map(input => input.requestId), ['request-b']);
  await local.whenIdle();
  const persisted = native.session.snapshotEvents().flatMap(event => {
    const record = pluginRecordOf(event); return record?.type === 'plugin:reactive/input' ? [record.data] : [];
  });
  assert.equal(persisted.length, 2);
  for (const input of persisted) { assert.deepEqual(input.scope, scopes[input.requestId]); assert.equal(input.actorId, 'request-observer'); }
  assert.equal(models, 1); assert.ok(changes.some(change => change.type === 'append'));
  await journal.dispose();
  await runtime.application.close(sessionId);
  assert.equal((await next(a.state$, value => value.status === 'ready' && value.value.source === 'durable')).value.activity.reply, messageA.text);
  assert.equal(runtime.native.agents.get(SessionId(sessionId)), undefined);
  let completed = false;
  const completion = a.state$.subscribe({ complete: () => { completed = true; } });
  assert.equal(completed, false, 'settled 不终止 request 观察');
  const closingConnection = next(a.state$, value => value.status === 'pending');
  sockets.findLast(item => item.group === early.group && item.endpoint === 'reactivePrograms/progress'
    && item.socket.readyState === WebSocket.OPEN).socket.terminate();
  await closingConnection;
  const pendingRequest = assert.rejects(a.state.read(), /REACTIVE_CONVERSATION_HANDLE_RELEASED/u);
  const pendingProgress = assert.rejects(early.session.progress.read(), /REACTIVE_CONVERSATION_HANDLE_RELEASED/u);
  await early.session.release(); await Promise.all([pendingRequest, pendingProgress]);
  assert.equal(workCompleted, false, '退出旧SDK来源不结束已切换的群组观察');
  await assert.rejects(earlyWorkMember.state.read(), /REACTIVE_CONVERSATION_HANDLE_RELEASED/u);
  assert.equal((await remoteWork.state.read()).sessions[0].source, 'durable');
  assert.throws(() => a.state.peek(), /REACTIVE_CONVERSATION_HANDLE_RELEASED/u);
  await late.session.release(); await resumed.release();
  await assert.rejects(remoteWork.state.read(), /REACTIVE_CONVERSATION_HANDLE_RELEASED/u);
  assert.equal(workCompleted, false, '成员退出仍可等待名单接入新来源，未签发新的读取资格');
  workLifetime.abort(new Error('WORK_OBSERVATION_FINISHED')); workSources.complete();
  assert.equal(workCompleted, true, '显式群组owner结束所有成员观察');
  completion.unsubscribe(); workSubscription.unsubscribe(); assert.equal(completed, true);
  await Promise.all(sockets.filter(item => item.endpoint === 'reactivePrograms/progress').map(({ socket }) => socket.readyState === WebSocket.CLOSED
    ? undefined : new Promise(resolve => socket.once('close', resolve))));
  assert.deepEqual(errors, []); assert.deepEqual(carrierErrors, []);
});
