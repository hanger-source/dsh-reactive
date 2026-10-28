import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate } from 'node:timers/promises';
import { createElement as h, Fragment, useEffect } from 'react';
import { BehaviorSubject, NEVER, Subject, combineLatest, concat, defer, distinctUntilChanged, filter, firstValueFrom, fromEvent, map, of, switchMap } from 'rxjs';
import { Context, Expose, Offer, useResource, useAction, useObservable, useShared } from '@hanger-source/dsh-reactive/program';
import { defineAction, implementAction, defineApplication, defineEnvironment, defineProgram, defineSurface, observeResource, resourceView, view, z, type PartialSurfaceBindings, type ProgramProps, type ProgramWorkspace } from '@hanger-source/dsh-reactive';
import type { GenerateOptions } from '@deepseek-ai/dsh-llm';
import { boot, text, tool } from './native-support.ts';

test('组合程序随输入切换完整模块，共享工作区事实，保存导致能力退出仍保留原调用结果，冷观察和恢复复用同一资源图', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-program-'));
  const changes = new EventEmitter();
  const merchants = new Map([
    ['a', { id: 'a', operationId: 'op-a', ready: true, revision: 1 }],
    ['b', { id: 'b', operationId: 'op-b', ready: false, revision: 1 }],
  ]);
  let policyReads = 0;
  let setups = 0;
  let commands = 0;
  const requests: GenerateOptions[] = [];
  const policy$ = defer(() => { policyReads++; return concat(of({ canPrepare: true }), NEVER); });
  const input = z.object({ merchantId: z.string() }).strict();
  const merchant = z.object({ id: z.string(), operationId: z.string(), ready: z.boolean(), revision: z.number() });
  const operation = z.object({ id: z.string(), state: z.string() });
  const prepare = defineAction({ name: 'prepare_merchant', description: '保存正式准备结果', scope: input,
    input: z.object({}).strict(), output: z.object({ merchantId: z.string(), status: z.literal('prepared') }), observe: result => result });
  const prepareImplementation = implementAction(prepare, {
    async execute(_input, call) {
      const id = call.scope.merchantId;
      assert.deepEqual(call.inputs[0]?.scope, { merchantId: id });
      commands++;
      merchants.set(id, { ...merchants.get(id)!, ready: false, revision: 2 });
      changes.emit(id);
      await setImmediate();
      call.signal.throwIfAborted();
      return { merchantId: id, status: 'prepared' as const };
    }
  });
  const surface = defineSurface({ views: {
    board: view(z.object({ merchant, operation, writable: z.boolean() }).nullable()),
    execution: view(z.enum(['idle', 'running', 'unavailable'])),
  }, actions: { prepare } });
  const Public = Expose<typeof surface, PartialSurfaceBindings<typeof surface>>;
  const emptyBoard$ = of(null);
  function Prepare(props: { merchantId: string }) {
    const action = useAction(prepareImplementation, () => ({ merchantId: props.merchantId }), [props.merchantId]);
    return h(Fragment, null, h(Offer<typeof prepare>, { name: 'prepare_merchant', action }),
      h(Public, { surface, value: { actions: { prepare: action } } }));
  }
  function Merchant(props: { id: string; workspace: ProgramWorkspace }) {
    const permissions$ = useShared(() => defer(() => props.workspace.share(policy$)), [props.workspace]);
    const recordState$ = useShared(() => observeResource({ key$: of(props.id),
      async read(key) { await setImmediate(); return { ...merchants.get(key)! }; },
      invalidations: key => fromEvent(changes, key),
    }), [props.id]);
    const operationState$ = useShared(() => recordState$.pipe(switchMap(state => state.status === 'ready'
      ? observeResource({ key$: of(state.value.operationId), async read(key) { await setImmediate(); return { id: key, state: 'registered' }; } })
      : of(state))), [recordState$]);
    const writable$ = useShared(() => combineLatest([permissions$, recordState$]).pipe(
      map(([permissions, state]) => permissions.canPrepare && state.status === 'ready' && state.value.ready), distinctUntilChanged()),
    [permissions$, recordState$]);
    const recordEmission = useObservable(() => recordState$, [recordState$]);
    const operationEmission = useObservable(() => operationState$, [operationState$]);
    const writable = useObservable(() => writable$, [writable$]);
    const recordState = recordEmission.status === 'ready' ? recordEmission.value : recordEmission;
    const operationState = operationEmission.status === 'ready' ? operationEmission.value : operationEmission;
    const board$ = useShared(() => combineLatest({
      merchant: recordState$.pipe(filter(state => state.status === 'ready'), map(state => state.value)),
      operation: operationState$.pipe(filter(state => state.status === 'ready'), map(state => state.value)), writable: writable$,
    }), [recordState$, operationState$, writable$]);
    return h(Fragment, null,
      h(Context<z.output<typeof merchant>>, { name: 'merchant', observation: recordState, render: value => value }),
      h(Context<z.output<typeof operation>>, { name: 'operation', observation: operationState, render: value => value }),
      h(Public, { surface, value: { views: { board: board$ } } }),
      writable.status === 'ready' && writable.value ? h(Prepare, { merchantId: props.id }) : null);
  }
  function Delivery({ conversation, workspace }: ProgramProps<{ input: typeof input; surface: typeof surface }>) {
    useEffect(() => { setups++; }, []);
    const inputs = useObservable(() => conversation.inputs$, [conversation]);
    const execution$ = useShared(() => conversation.activity$.pipe(map(value => value.execution)), [conversation]);
    let selected: string | null = null;
    if (inputs.status === 'ready') {
      const ids = [...new Set(inputs.value.map(value => value.scope.merchantId))];
      if (ids.length > 1) throw new Error('一次交付判断需要明确的商家范围');
      selected = ids[0] ?? null;
    }
    return h(Fragment, null, h(Public, { surface, value: { views: { execution: execution$ } } }),
      inputs.status !== 'ready' ? null : selected === null
        ? h(Public, { surface, value: { views: { board: emptyBoard$ } } })
        : h(Merchant, { key: selected, id: selected, workspace }));
  }
  const analysis = implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component: () => null });
  const definition = defineApplication({ name: 'operations', admit: async () => {}, environments: [
    defineEnvironment({ id: 'delivery', instructions: '按当前商家事实处理',
      program: implementProgram(defineProgram({ input, surface }), { Component: Delivery }) }),
    defineEnvironment({ id: 'analysis', instructions: '只读分析', program: analysis }),
  ] });
  const app = await boot(join(root, 'home'), definition, async function* (options) {
    requests.push(options);
    if (requests.length === 1) yield* tool('prepare_merchant', 'prepare-call', {});
    else yield* text('已核对当前事实');
  });
  try {
    const first = await app.host.create('delivery', { workspaceId: app.workspace.workspaceId });
    const second = await app.host.create('delivery', { workspaceId: app.workspace.workspaceId });
    assert.equal(await firstValueFrom(first.views.board), null);
    await first.submit({ requestId: 'prepare-a', content: '准备商家 a', intent: 'question', delivery: 'followup' as const, scope: { merchantId: 'a' } });
    await first.whenIdle();
    assert.equal(commands, 1);
    const prepared = await firstValueFrom(first.request('prepare-a').state$.pipe(filter(state => state.status === 'ready'), map(state => state.value.activity)));
    assert.equal(prepared?.tools[0]?.state, 'returned');
    assert.deepEqual(prepared?.tools[0]?.result, { merchantId: 'a', status: 'prepared' });
    assert.equal((await firstValueFrom(first.views.board))?.merchant.revision, 2);
    assert.ok(!requests[1]!.tools?.some(value => value.name === 'prepare_merchant'));
    assert.equal(policyReads, 1);
    await second.submit({ requestId: 'read-b', content: '读取商家 b', intent: 'question', delivery: 'followup' as const, scope: { merchantId: 'b' } });
    await second.whenIdle();
    await first.submit({ requestId: 'switch-b', content: '切换到商家 b', intent: 'followup', delivery: 'followup' as const, scope: { merchantId: 'b' } });
    await first.whenIdle();
    assert.equal((await firstValueFrom(first.views.board))?.operation.id, 'op-b');
    assert.equal(changes.listenerCount('a'), 0);
    assert.equal(changes.listenerCount('b'), 2);
    const observer = app.host.conversation(second.id, definition.environments[0].program.definition);
    let notifications = 0;
    observer.views.board.subscribe(() => { notifications++; });
    observer.release();
    const received = notifications;
    assert.equal(setups, 2);
    const calls = requests.length;
    const id = first.id;
    await app.host.close(id);
    assert.equal(first.snapshot().source, 'durable');
    assert.equal(await firstValueFrom(first.views.execution), 'unavailable');
    merchants.set('b', { ...merchants.get('b')!, revision: 3 }); changes.emit('b');
    assert.equal((await firstValueFrom(first.views.board.pipe(filter(value => value?.merchant.revision === 3))))?.merchant.revision, 3);
    assert.equal((await firstValueFrom(second.views.board.pipe(filter(value => value?.merchant.revision === 3))))?.merchant.revision, 3);
    assert.equal(notifications, received);
    first.release();
    assert.equal(changes.listenerCount('b'), 1);
    const otherProgram = definition.environments[1].program.definition;
    await assert.rejects(app.host.open(id, otherProgram), /REACTIVE_PROGRAM_CONTRACT_MISMATCH/);
    await assert.rejects(app.host.service(id, otherProgram), /REACTIVE_PROGRAM_CONTRACT_MISMATCH/);
    await assert.rejects(app.host.resume(id, otherProgram), /REACTIVE_PROGRAM_CONTRACT_MISMATCH/);
    assert.equal(setups, 2, '错合同的冷读取和恢复不求值程序');
    assert.equal(changes.listenerCount('b'), 1, '取得句柄失败不留下业务来源连接');
    const reopened = await app.host.open(id, definition.environments[0].program.definition);
    assert.equal((await firstValueFrom(reopened.views.board))?.merchant.revision, 3);
    assert.equal(requests.length, calls);
    assert.equal(setups, 3);
    await assert.rejects(app.host.open(id, definition.environments[1].program.definition), /REACTIVE_PROGRAM_CONTRACT_MISMATCH/);
    await reopened.submit({ requestId: 'resume-b', content: '继续核对', intent: 'followup', delivery: 'followup' as const, scope: { merchantId: 'b' } });
    await reopened.whenIdle();
    assert.equal(setups, 3);
    assert.equal(reopened.snapshot().execution, 'idle');
    assert.equal(reopened.snapshot().source, 'live');
    assert.deepEqual(app.errors, []);
    reopened.release(); second.release();
  } finally { await app.dispose(); }
});

test('装配期间业务选择改变会撤换旧模块，忽略取消的旧来源不阻塞新上下文或进入模型', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-reselection-'));
  const selection$ = new BehaviorSubject('a');
  const old = Promise.withResolvers<{ selected: string }>();
  const started = Promise.withResolvers<void>();
  const requests: GenerateOptions[] = [];
  const input = z.null();
  const surface = defineSurface({ views: { record: view(z.object({ selected: z.string() })) }, actions: {} });
  const Public = Expose<typeof surface, PartialSurfaceBindings<typeof surface>>;
  function Selected(props: { id: string }) {
    const state$ = useShared(() => observeResource({ key$: of(props.id),
      read(id) { if (id === 'a') { started.resolve(); return old.promise; } return { selected: id }; },
    }), [props.id]);
    const emitted = useObservable(() => state$, [state$]);
    const state = emitted.status === 'ready' ? emitted.value : emitted;
    const record$ = useShared(() => state$.pipe(filter(value => value.status === 'ready'), map(value => value.value)), [state$]);
    return h(Fragment, null, h(Context<{ selected: string }>, { name: 'facts', observation: state, render: value => value }),
      h(Public, { surface, value: { views: { record: record$ } } }));
  }
  function Work({ workspace }: ProgramProps<{ input: typeof input; surface: typeof surface }>) {
    const selection = useObservable(() => defer(() => workspace.share(selection$)), [workspace]);
    return h(Fragment, null, h(Context<string>, { name: 'selection', observation: selection, render: value => value }),
      selection.status === 'ready' ? h(Selected, { key: selection.value, id: selection.value }) : null);
  }
  const definition = defineApplication({ name: 'switching', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '只使用当前选择', program: implementProgram(defineProgram({ input, surface }), { Component: Work }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* (request) { requests.push(request); yield* text('当前选择已核对'); });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    await started.promise;
    await session.submit({ requestId: 'read-current', content: '核对当前选择', intent: 'question', delivery: 'followup' as const, scope: null });
    await setImmediate();
    assert.equal(requests.length, 0);
    selection$.next('b');
    await session.whenIdle();
    assert.equal(requests.length, 1);
    const prompt = requests[0]!.messages.flatMap(message => message.content).flatMap(block => block.type === 'text' ? [block.text] : []).join('\n');
    assert.ok(prompt.includes('"selected":"b"'));
    old.resolve({ selected: 'a' });
    await setImmediate();
    assert.deepEqual(await firstValueFrom(session.views.record), { selected: 'b' });
    assert.deepEqual(app.errors, []);
    session.release();
  } finally { old.resolve({ selected: 'a' }); await app.dispose(); }
});

test('商家变更操作引用后，模型等待新操作事实，不用旧 ready 值拼装新现场', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-dependent-context-'));
  const changes$ = new Subject<void>();
  let merchantFact = { id: 'merchant-a', revision: 1, operationId: 'operation-a' };
  const started = Promise.withResolvers<void>();
  const nextOperation = Promise.withResolvers<{ id: string; state: string }>();
  const requests: GenerateOptions[] = [];
  const promptText = (request: GenerateOptions) => request.messages.flatMap(message => message.content)
    .flatMap(block => block.type === 'text' ? [block.text] : []).join('\n');
  const input = z.null();
  const merchantSchema = z.object({ id: z.string(), revision: z.number(), operationId: z.string() });
  const operationSchema = z.object({ id: z.string(), state: z.string() });
  const surface = defineSurface({ views: { merchant: resourceView(merchantSchema), operation: resourceView(operationSchema) }, actions: {} });
  const Public = Expose<typeof surface, PartialSurfaceBindings<typeof surface>>;
  function Delivery() {
    const merchant = useResource(() => observeResource({ key$: of('merchant-a'),
      read: () => ({ ...merchantFact }), invalidations: () => changes$ }), []);
    const operation = useResource(() => merchant.observations$.pipe(switchMap(state => state.status === 'ready'
      ? observeResource({ key$: of(state.value.operationId), read(id) {
        if (id === 'operation-a') return { id, state: 'current' };
        started.resolve(); return nextOperation.promise;
      } }) : of(state))), [merchant.observations$]);
    return h(Fragment, null,
      h(Context<z.output<typeof merchantSchema>>, { name: 'merchant', observation: merchant.state, render: value => value }),
      h(Context<z.output<typeof operationSchema>>, { name: 'operation', observation: operation.state, render: value => value }),
      h(Public, { surface, value: { views: { merchant: merchant.observations$, operation: operation.observations$ } } }));
  }
  const definition = defineApplication({ name: 'dependent-context', admit: async () => {}, environments: [defineEnvironment({
    id: 'delivery', instructions: '依据当前关联操作判断',
    program: implementProgram(defineProgram({ input, surface }), { Component: Delivery }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* (options) {
    requests.push(options); yield* text('已读取当前关联事实');
  });
  try {
    const session = await app.host.create('delivery', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'read-a', content: '读取当前关联操作', intent: 'question', delivery: 'followup' as const, scope: null });
    await session.whenIdle();
    assert.equal(requests.length, 1);
    assert.ok(promptText(requests[0]!).includes('"operationId":"operation-a"'));
    merchantFact = { ...merchantFact, revision: 2, operationId: 'operation-b' };
    changes$.next();
    await started.promise;
    assert.equal((await firstValueFrom(session.observeView('operation'))).status, 'pending');
    let resolved = false;
    const currentRead = session.readView('operation').then(value => { resolved = true; return value; });
    await session.submit({ requestId: 'read-b', content: '依据变更后的操作继续判断', intent: 'followup', delivery: 'followup' as const, scope: null });
    await setImmediate();
    assert.equal(requests.length, 1);
    assert.equal(resolved, false);
    nextOperation.resolve({ id: 'operation-b', state: 'current' });
    await session.whenIdle();
    assert.equal(requests.length, 2);
    assert.deepEqual(await currentRead, { id: 'operation-b', state: 'current' });
    const runtimeSnapshot = z.object({ kind: z.literal('runtime-context'), form: z.literal('snapshot'),
      sections: z.object({ name: z.string(), text: z.string() }).array() });
    const current = requests[1]!.messages.map(message => runtimeSnapshot.safeParse(message.source))
      .filter(result => result.success).at(-1)?.data;
    assert.ok(current);
    const merchantSection = current.sections.find(section => section.name === 'merchant');
    const operationSection = current.sections.find(section => section.name === 'operation');
    assert.ok(merchantSection);
    assert.ok(operationSection);
    const currentMerchant = JSON.parse(merchantSection.text);
    const currentOperation = JSON.parse(operationSection.text);
    assert.deepEqual(currentMerchant, { id: 'merchant-a', revision: 2, operationId: 'operation-b' });
    assert.deepEqual(currentOperation, { id: 'operation-b', state: 'current' });
    assert.equal(currentMerchant.operationId, currentOperation.id);
    assert.deepEqual(app.errors, []);
    session.release();
  } finally { nextOperation.resolve({ id: 'operation-b', state: 'current' }); await app.dispose(); }
});

test('正式 PTC 执行通过生成的工具 SDK 组合并行能力，原调用和子调用沿真实输入关联并可观察', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-ptc-'));
  let entered = 0;
  let release!: () => void;
  let fail!: (error: Error) => void;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const both = new Promise<void>((resolve, reject) => { release = resolve; fail = reject; });
  const calls: string[] = [];
  let steps = 0;
  const input = z.object({ caseId: z.string() });
  const readOwner = defineAction({ name: 'read_owner', description: '读取独立来源', scope: input,
    input: z.object({ id: z.string() }), output: z.object({ id: z.string(), caseId: z.string() }), concurrency: 'parallel', observe: result => result });
  const readOwnerImplementation = implementAction(readOwner, {
    async execute(input, call) {
      assert.deepEqual(call.inputs[0]?.scope, { caseId: call.scope.caseId });
      entered++;
      if (entered === 1) timer = setTimeout(() => fail(new Error('原生调用没有并行进入两个独立来源')), 2000);
      if (entered === 2) { clearTimeout(timer); release(); }
      await both;
      assert.ok(call.source.kind === 'tool');
      calls.push(call.source.rootCallId);
      return { id: input.id, caseId: call.scope.caseId };
    }
  });
  const surface = defineSurface({ views: {}, actions: { readOwner } });
  const Public = Expose<typeof surface, PartialSurfaceBindings<typeof surface>>;
  function Owner(props: { caseId: string }) {
    const action = useAction(readOwnerImplementation, () => ({ caseId: props.caseId }), [props.caseId]);
    return h(Fragment, null, h(Offer<typeof readOwner>, { name: 'read_owner', action }),
      h(Public, { surface, value: { actions: { readOwner: action } } }));
  }
  function Investigation({ conversation }: ProgramProps<{ input: typeof input; surface: typeof surface }>) {
    const inputs = useObservable(() => conversation.inputs$, [conversation]);
    const selected = inputs.status === 'ready' ? inputs.value.at(-1)?.scope.caseId : undefined;
    return h(Fragment, null, h(Context<readonly { scope: { caseId: string } }[]>, {
      name: 'case-inputs', observation: inputs, render: values => values.map(value => value.scope),
    }), selected === undefined ? null : h(Owner, { key: selected, caseId: selected }));
  }
  const definition = defineApplication({ name: 'composition', admit: async () => {}, environments: [defineEnvironment({
    id: 'investigation', instructions: '组合读取两个正式来源',
    program: implementProgram(defineProgram({ input, surface }), { Component: Investigation }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* (options) {
    steps++;
    assert.deepEqual(options.tools?.map(value => value.name), ['run_code']);
    if (steps === 1) yield* tool('run_code', 'composition-call', { description: '并行核对两个来源',
      code: 'const values = await Promise.all([tools.read_owner({ id: "left" }), tools.read_owner({ id: "right" })]); console.log(values);' });
    else yield* text('两个来源均已返回');
  }, 'ptc');
  try {
    const session = await app.host.create('investigation', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'case-request', content: '核对两个来源', intent: 'question', delivery: 'followup' as const, scope: { caseId: 'case-1' } });
    await session.whenIdle();
    assert.deepEqual(calls, ['composition-call', 'composition-call']);
    const activity = (await firstValueFrom(session.request('case-request').state$.pipe(filter(state => state.status === 'ready'), map(state => state.value.activity))))!;
    const children = activity.tools.filter(value => value.name === 'read_owner');
    assert.equal(children.length, 2);
    assert.ok(children.every(value => value.rootCallId === 'composition-call' && value.parentCallId === 'composition-call' && value.callId !== value.rootCallId
      && value.state === 'returned' && value.inputs[0]?.requestId === 'case-request'));
    assert.deepEqual(children.map(value => value.result), [{ id: 'left', caseId: 'case-1' }, { id: 'right', caseId: 'case-1' }]);
    assert.deepEqual(app.errors, []);
  } finally { clearTimeout(timer); await app.dispose(); }
});
