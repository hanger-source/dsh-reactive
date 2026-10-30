import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement as h } from 'react';
import { BehaviorSubject, Observable, Subject, UnsubscriptionError, combineLatest, defer, finalize, map, merge } from 'rxjs';
import { SessionId } from '@deepseek-ai/dsh-session';
import { bindAction, createWorkSelector, defineAction, defineApplication, defineEnvironment, defineProgram, defineSurface, ResourceReader,
  implementAction, z, type ConversationEvent, type WorkEvent, type WorkProgressFrame, type WorkOrigin,
  type WorkInvocationInput, type InvocationActivity, type Observation, type WorkSnapshot, type WorkSource } from '@hanger-source/dsh-reactive';
import { implementProgram, Expose, Offer, useAction } from '@hanger-source/dsh-reactive/program';
import { boot, text, tool } from './native-support.ts';

type WorkKey = { merchantId: string; workId: string };
const firstWork = { merchantId: 'merchant-a', workId: 'first' };
const secondWork = { merchantId: 'merchant-a', workId: 'second' };
function identify(origin: WorkOrigin): WorkKey | undefined {
  const scope = origin.kind === 'invocation' ? origin.origin.scope : origin.scope;
  if (scope !== null && typeof scope === 'object' && !Array.isArray(scope)
    && 'merchantId' in scope && 'workId' in scope
    && typeof scope.merchantId === 'string' && typeof scope.workId === 'string') {
    return { merchantId: scope.merchantId, workId: scope.workId };
  }
  return undefined;
}

// 只计数真实SDK资源与事件的连接；不替代会话事实或业务执行。
function countedSource(source: WorkSource, onProgressRelease?: () => void) {
  const counts = Object.fromEntries(['progress', 'human', 'events', 'frames'].map(name =>
    [name, { active: 0, subscriptions: 0 }])) as Record<'progress' | 'human' | 'events' | 'frames', { active: number; subscriptions: number }>;
  const count = <T,>(stream: Observable<T>, name: keyof typeof counts) => defer(() => {
    counts[name].active++; counts[name].subscriptions++;
    return stream.pipe(finalize(() => { counts[name].active--; if (name === 'progress') onProgressRelease?.(); }));
  });
  return { counts, source: {
    identity: source.identity,
    progress: new ResourceReader({ observations$: count(source.progress.observations$, 'progress'), peek: () => source.progress.peek() }),
    human: new ResourceReader({ observations$: count(source.human.observations$, 'human'), peek: () => source.human.peek() }),
    rawEvents$: count(source.rawEvents$, 'events'), progressFrames$: count(source.progressFrames$, 'frames'),
  } satisfies WorkSource };
}

async function dynamicSessionsFixture() {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-work-members-'));
  const entered = Promise.withResolvers<void>(), finish = Promise.withResolvers<void>();
  let modelCalls = 0;
  const record = defineAction({ name: 'write_member_work', description: '完成已受理的文件工作', scope: z.object({ merchantId: z.string(), workId: z.string() }),
    input: z.string(), output: z.string() });
  const implementation = implementAction(record, { async execute(file, call) {
    entered.resolve(); await finish.promise; call.signal.throwIfAborted();
    await writeFile(join(root, file), call.scope.workId); return file;
  } });
  const surface = defineSurface({ views: {}, actions: { write: record } });
  const definition = defineApplication({ name: 'dynamic-work-members', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '处理当前显式输入',
    program: implementProgram(defineProgram({ input: z.object({ merchantId: z.string(), workId: z.string() }), surface }), { Component() {
      const write = useAction(implementation, () => firstWork, []);
      return Expose({ surface, value: { actions: { write } } });
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () { modelCalls++; yield* text('已处理本次工作'); });
  const first = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
  const second = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
  return { app, root, definition, first, second, entered, finish, modelCalls: () => modelCalls };
}

test('动态会话名单增删重排沿真实SDK传播，捕获成员仍可读取，退出观察不取消已受理业务', { timeout: 30_000 }, async () => {
  const f = await dynamicSessionsFixture();
  const first = countedSource(f.first), second = countedSource(f.second);
  const list = new BehaviorSubject<Observation<readonly WorkSource[]>>({ status: 'ready', generation: 1, observedAt: 0, value: [first.source] });
  let listActive = 0, listSubscriptions = 0, generation = 1;
  const sources = defer(() => { listActive++; listSubscriptions++; return list.pipe(finalize(() => { listActive--; })); });
  const work = createWorkSelector({ identify }).observe(firstWork, { sessions: sources });
  assert.equal(listSubscriptions, 0, '构造不读取名单或连接会话');
  assert.ok(Object.values(first.counts).every(value => value.subscriptions === 0));
  const [detached] = await work.sessions.read();
  assert.equal(listActive, 0, '单次成员读取结束名单观察');
  assert.equal((await detached!.state.read()).sessionId, f.first.id, '成员读侧不依赖已经退出的名单订阅');
  assert.ok(Object.values(first.counts).every(value => value.active === 0));
  const states: Observation<WorkSnapshot<WorkKey>>[] = [], events: WorkEvent<WorkKey>[] = [], frames: WorkProgressFrame<WorkKey>[] = [];
  const stateSubscription = work.state$.subscribe(value => states.push(value));
  const eventSubscription = work.events$.subscribe(value => events.push(value));
  const frameSubscription = work.progressFrames$.subscribe(value => frames.push(value));
  const publish = (value: readonly WorkSource[]) => list.next({ status: 'ready', generation: ++generation, observedAt: 0, value });
  try {
    const [captured] = await work.sessions.read();
    assert.equal(captured, detached);
    assert.equal((await captured!.state.read()).sessionId, f.first.id);
    await f.first.submit({ requestId: 'before-add', content: '原成员工作', intent: 'question', delivery: 'followup', scope: firstWork });
    await f.first.whenIdle();
    const firstConnections = Object.fromEntries(Object.entries(first.counts).map(([name, value]) => [name, value.subscriptions]));
    publish([first.source, second.source]);
    await work.state.read();
    const beforeReorder = [events.length, frames.length];
    publish([second.source, first.source]); publish([second.source, first.source]);
    assert.deepEqual((await work.state.read()).sessions.map(value => value.sessionId), [f.second.id, f.first.id]);
    assert.equal((await work.sessions.read())[1], captured);
    assert.deepEqual(Object.fromEntries(Object.entries(first.counts).map(([name, value]) => [name, value.subscriptions])), firstConnections,
      '添加其他会话和重排不重连未变来源');
    assert.deepEqual([events.length, frames.length], beforeReorder, '名单更新不重放已发生事件或流帧');
    const invocation = f.first.resolveAction('write', firstWork)('accepted.txt');
    await f.entered.promise;
    publish([second.source]);
    assert.deepEqual((await work.state.read()).sessions.map(value => value.sessionId), [f.second.id]);
    assert.ok(Object.values(first.counts).every(value => value.active === 0), '移除释放群组连接，单次读取不留下watch');
    const eventsAfterRemoval = events.length, framesAfterRemoval = frames.length;
    await f.first.submit({ requestId: 'removed-work', content: '原source继续工作', intent: 'question', delivery: 'followup', scope: firstWork });
    await f.first.whenIdle();
    assert.equal(events.length, eventsAfterRemoval); assert.equal(frames.length, framesAfterRemoval);
    assert.ok((await captured!.state.read()).requests.some(value => value.value.requestId === 'removed-work'),
      '一次members.read捕获的成员借原source，名单移除后仍可读取');
    f.finish.resolve(); assert.equal(await invocation.result, 'accepted.txt');
    assert.equal(await readFile(join(f.root, 'accepted.txt'), 'utf8'), firstWork.workId);
    await f.second.submit({ requestId: 'remaining-work', content: '存活成员工作', intent: 'question', delivery: 'followup', scope: firstWork });
    await f.second.whenIdle();
    assert.ok(events.slice(eventsAfterRemoval).some(value => value.source.sessionId === f.second.id));
    assert.ok(frames.slice(framesAfterRemoval).some(value => value.source.sessionId === f.second.id));
    assert.ok(states.some(value => value.status === 'ready' && value.value.sessions.length === 2));
    stateSubscription.unsubscribe(); eventSubscription.unsubscribe(); frameSubscription.unsubscribe();
    assert.equal(listActive, 0); assert.ok(Object.values(second.counts).every(value => value.active === 0));
    const reconnectedEvents: WorkEvent<WorkKey>[] = [];
    const reconnected = work.events$.subscribe(value => reconnectedEvents.push(value));
    assert.deepEqual(reconnectedEvents, [], '重新连接事件不回放历史'); reconnected.unsubscribe();
    assert.ok(listSubscriptions > 1, '最后订阅退出后按名单owner重新连接');
    assert.equal(listActive, 0); assert.equal(f.modelCalls(), 3); assert.deepEqual(f.app.errors, []);
  } finally {
    stateSubscription.unsubscribe(); eventSubscription.unsubscribe(); frameSubscription.unsubscribe();
    f.finish.resolve(); f.first.release(); f.second.release(); await f.app.dispose();
  }
});

test('动态名单pending/error撤下旧ready，释放来源剔除后恢复，同会话新source不重定向捕获成员', { timeout: 30_000 }, async () => {
  const f = await dynamicSessionsFixture();
  const first = countedSource(f.first), second = countedSource(f.second);
  const list = new BehaviorSubject<Observation<readonly WorkSource[]>>({ status: 'ready', generation: 1, observedAt: 0, value: [first.source, second.source] });
  const work = createWorkSelector({ identify }).observe(firstWork, { sessions: list });
  const seen: Observation<WorkSnapshot<WorkKey>>[] = [], eventErrors: unknown[] = [], frameErrors: unknown[] = [];
  const stateSubscription = work.state$.subscribe(value => seen.push(value));
  const eventSubscription = work.events$.subscribe({ error: error => eventErrors.push(error) });
  const frameSubscription = work.progressFrames$.subscribe({ error: error => frameErrors.push(error) });
  try {
    const [captured, survivor] = await work.sessions.read();
    const survivorSubscription = survivor!.state$.subscribe();
    try {
      list.next({ status: 'pending', generation: 2 });
      assert.equal(work.state.peek().status, 'pending'); assert.equal(work.sessions.peek().status, 'pending');
      assert.equal(eventSubscription.closed, false); assert.equal(frameSubscription.closed, false);
      assert.ok(Object.values(first.counts).every(value => value.active === 0));
      assert.equal(second.counts.events.active, 0); assert.equal(second.counts.frames.active, 0);
      assert.equal((await survivor!.state.read()).sessionId, f.second.id, '局部成员保留自己的来源');
      const unavailable = new Error('SESSION_MEMBERS_UNAVAILABLE');
      list.next({ status: 'error', generation: 3, error: unavailable });
      await assert.rejects(work.state.read(), error => error === unavailable);
      await assert.rejects(work.sessions.read(), error => error === unavailable);
      assert.deepEqual(eventErrors, [unavailable]); assert.deepEqual(frameErrors, [unavailable]);
      list.next({ status: 'ready', generation: 4, observedAt: 0, value: [first.source, second.source] });
      assert.equal((await work.state.read()).sessions.length, 2);
      f.first.release();
      assert.notEqual(work.state.peek().status, 'ready');
      assert.equal((await survivor!.state.read()).sessionId, f.second.id);
      list.next({ status: 'ready', generation: 5, observedAt: 0, value: [second.source] });
      assert.deepEqual((await work.state.read()).sessions.map(value => value.sessionId), [f.second.id]);
      const replacement = await f.app.host.open(f.first.id, f.definition.environments[0].program.definition);
      try {
        list.next({ status: 'ready', generation: 6, observedAt: 0, value: [replacement, second.source] });
        const [newMember] = await work.sessions.read();
        assert.notEqual(newMember, captured);
        assert.equal((await newMember!.state.read()).sessionId, f.first.id);
        await assert.rejects(captured!.state.read(), /REACTIVE_SCOPE_DISPOSED/u,
          '旧成员保留已释放source的失败，不自动重定向同sessionId新来源');
        assert.equal((await work.state.read()).sessions.length, 2);
        assert.equal(survivorSubscription.closed, false);
        assert.ok(seen.some(value => value.status === 'pending') && seen.some(value => value.status === 'error'));
      } finally { replacement.release(); }
    } finally { survivorSubscription.unsubscribe(); }
    assert.deepEqual(f.app.errors, []);
  } finally {
    stateSubscription.unsubscribe(); eventSubscription.unsubscribe(); frameSubscription.unsubscribe();
    f.finish.resolve(); f.first.release(); f.second.release(); await f.app.dispose();
  }
});

test('动态群组teardown重入采用较新名单，真实来源事件失败按同一错误终止消费者', { timeout: 30_000 }, async () => {
  const f = await dynamicSessionsFixture();
  const list = new BehaviorSubject<Observation<readonly WorkSource[]>>({ status: 'ready', generation: 1, observedAt: 0, value: [] });
  let reenter = false;
  const first = countedSource(f.first, () => {
    if (!reenter) return;
    reenter = false;
    list.next({ status: 'ready', generation: 3, observedAt: 0, value: [second.source] });
  });
  const second = countedSource(f.second);
  const work = createWorkSelector({ identify }).observe(firstWork, { sessions: list });
  list.next({ status: 'ready', generation: 1, observedAt: 0, value: [first.source] });
  const stateSubscription = work.state$.subscribe();
  try {
    await work.state.read(); reenter = true;
    list.next({ status: 'ready', generation: 2, observedAt: 0, value: [] });
    assert.deepEqual((await work.state.read()).sessions.map(value => value.sessionId), [f.second.id],
      '释放旧来源时同步到达的新名单不能被外层旧名单覆盖');
    assert.equal((await work.sessions.read())[0]!.identity.sessionId, f.second.id);
    assert.equal(first.counts.progress.active, 0); assert.equal(second.counts.progress.active, 1);

    // 真实SDK事件流外的传输边界故障；查询仍取真实会话，不制造事件或状态。
    const transport = new Subject<ConversationEvent>();
    const disconnected = new Error('SESSION_EVENT_TRANSPORT_DISCONNECTED');
    const failingSource: WorkSource = { identity: f.second.identity, progress: f.second.progress, human: f.second.human,
      rawEvents$: merge(f.second.rawEvents$, transport), progressFrames$: f.second.progressFrames$ };
    list.next({ status: 'ready', generation: 4, observedAt: 0, value: [failingSource] });
    const errors: unknown[] = [], events: WorkEvent<WorkKey>[] = [];
    const eventSubscription = work.events$.subscribe({ next: value => events.push(value), error: error => errors.push(error) });
    try {
      await f.second.submit({ requestId: 'before-disconnect', content: '先观察真实事件', intent: 'question', delivery: 'followup', scope: firstWork });
      await f.second.whenIdle();
      assert.ok(events.some(value => value.matchedInputs.some(input => input.requestId === 'before-disconnect')));
      transport.error(disconnected);
      assert.deepEqual(errors, [disconnected]); assert.equal(eventSubscription.closed, true);
      assert.equal((await work.state.read()).sessions[0]!.requests[0]!.value.requestId, 'before-disconnect',
        '事件传输错误不伪造查询失败，也不自动重试消费者');
    } finally { eventSubscription.unsubscribe(); }

    const cleanupFailure = new Error('SESSION_EVENT_TEARDOWN_FAILED');
    const throwingSource: WorkSource = { ...first.source,
      rawEvents$: first.source.rawEvents$.pipe(finalize(() => { throw cleanupFailure; })) };
    list.next({ status: 'ready', generation: 5, observedAt: 0, value: [throwingSource, second.source] });
    const cleanupSubscription = work.events$.subscribe();
    assert.equal(first.counts.events.active, 1); assert.equal(second.counts.events.active, 1);
    assert.throws(() => cleanupSubscription.unsubscribe(), error =>
      error instanceof UnsubscriptionError && error.errors.includes(cleanupFailure),
    '真实来源teardown失败必须交付给退出观察的调用方');
    assert.equal(first.counts.events.active, 0);
    assert.equal(second.counts.events.active, 0, '前一来源清理抛错，仍必须尝试释放后一来源的真实SDK订阅');
    assert.deepEqual(f.app.errors, []);
  } finally {
    stateSubscription.unsubscribe(); f.finish.resolve(); f.first.release(); f.second.release(); await f.app.dispose();
  }
});

test('两个work保留同会话复用callId的历史，跨会话同请求身份独立，重开home只冷读原事实', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-work-history-'));
  const selector = createWorkSelector({ identify });
  let modelCalls = 0, executions = 0, inputTransforms = 0;
  const record = defineAction({ name: 'record_work', description: '记录本次实际业务执行', scope: z.null(),
    input: z.object({}).strict(), output: z.number() });
  const implementation = implementAction(record, { execute() { return ++executions; } });
  const definition = defineApplication({ name: 'work-history', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '每轮调用业务后回复',
    program: implementProgram(defineProgram({ input: z.object({ merchantId: z.string(), workId: z.string() })
      .transform(value => { inputTransforms++; return { domain: value }; }), surface: defineSurface({ views: {}, actions: {} }) }), { Component() {
      const action = useAction(implementation, () => null, []);
      return h(Offer<typeof record>, { name: 'record_work', action });
    } }),
  })] });
  const respond = async function* () {
    modelCalls++;
    if (modelCalls % 2 === 1) yield* tool('record_work', 'provider-reused-id', {});
    else yield* text('完成工作' + executions);
  };
  let app = await boot(join(root, 'home'), definition, respond);
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const events: ConversationEvent[] = [];
    session.rawEvents$.subscribe(event => events.push(event));
    await session.submit({ requestId: 'same-request', content: '第一项工作', intent: 'question', delivery: 'followup', scope: firstWork });
    await session.whenIdle();
    await session.submit({ requestId: 'other-request', content: '同一商家的另一项工作', intent: 'question', delivery: 'followup', scope: secondWork });
    await session.whenIdle();
    const peer = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    await peer.submit({ requestId: 'same-request', content: '另一个会话的相同工作', intent: 'question', delivery: 'followup', scope: firstWork });
    await peer.whenIdle();
    assert.equal(executions, 3);
    const transformsBeforeRead = inputTransforms;
    const first = await selector.observe(firstWork, { sessions: [session, peer] }).state.read();
    const second = await selector.observe(secondWork, { sessions: [session, peer] }).state.read();
    const projected = selector.select(firstWork, { sessions: [
      { identity: session.identity, progress: await session.progress.read(), human: await session.human.read() },
      { identity: peer.identity, progress: await peer.progress.read(), human: await peer.human.read() },
    ] });
    assert.deepEqual(projected, first);
    assert.equal(inputTransforms, transformsBeforeRead, 'raw work读取不再次执行Program领域transform');
    assert.deepEqual(first.sessions.map(value => [value.sessionId, value.requests.map(item => item.value.requestId)]),
      [[session.id, ['same-request']], [peer.id, ['same-request']]]);
    assert.deepEqual(second.sessions.map(value => value.requests.map(item => item.value.requestId)), [['other-request'], []]);
    assert.deepEqual(first.sessions.map(value => value.calls.map(item => [item.value.callId, item.value.state])),
      [[['provider-reused-id', 'returned']], [['provider-reused-id', 'returned']]]);
    const native = app.native.sessions.get(SessionId(session.id));
    assert.ok(native);
    const starts = native.snapshotEvents().filter(event => event.type === 'tool/call');
    assert.equal(starts.length, 2);
    const firstCall = first.sessions[0]!.calls[0]!.value, secondCall = second.sessions[0]!.calls[0]!.value;
    assert.equal(firstCall.callSequence, starts[0]!.seq);
    assert.equal(secondCall.callSequence, starts[1]!.seq);
    assert.notEqual(firstCall.callSequence, secondCall.callSequence);
    assert.deepEqual(events.flatMap(event => event.kind === 'tool.started' || event.kind === 'tool.settled'
      ? [[event.kind, event.callSequence, event.inputs[0]?.requestId]] : []), [
      ['tool.started', firstCall.callSequence, 'same-request'], ['tool.settled', firstCall.callSequence, 'same-request'],
      ['tool.started', secondCall.callSequence, 'other-request'], ['tool.settled', secondCall.callSequence, 'other-request'],
    ]);
    assert.deepEqual(first.sessions[0]!.replies.map(item => item.value.text).filter(Boolean), ['完成工作1']);
    assert.deepEqual(second.sessions[0]!.replies.map(item => item.value.text).filter(Boolean), ['完成工作2']);
    assert.deepEqual(first.sessions[0]!.calls[0]!.matchedInputs.map(value => value.scope), [firstWork]);
    assert.deepEqual(second.sessions[0]!.calls[0]!.matchedInputs.map(value => value.scope), [secondWork]);
    const id = session.id, peerId = peer.id;
    await app.host.close(id); await app.host.close(peerId);
    session.release(); peer.release();
    assert.deepEqual(app.errors, []);
    await app.dispose();
    app = await boot(join(root, 'home'), definition, respond);
    const cold = await app.host.open(id, definition.environments[0].program.definition);
    const coldPeer = await app.host.open(peerId, definition.environments[0].program.definition);
    const replayed: WorkEvent<WorkKey>[] = [], frames: WorkProgressFrame<WorkKey>[] = [];
    const coldWork = selector.observe(firstWork, { sessions: [cold, coldPeer] });
    const eventSubscription = coldWork.events$.subscribe(event => replayed.push(event));
    const frameSubscription = coldWork.progressFrames$.subscribe(frame => frames.push(frame));
    const restored = await coldWork.state.read();
    assert.deepEqual(restored.sessions.map(value => value.calls), first.sessions.map(value => value.calls));
    assert.deepEqual(restored.sessions.map(value => value.replies), first.sessions.map(value => value.replies));
    assert.ok(restored.sessions.every(value => value.source === 'durable' && value.attempts.length === 0));
    assert.equal(app.native.agents.get(SessionId(id)), undefined);
    assert.equal(app.native.agents.get(SessionId(peerId)), undefined);
    assert.deepEqual(replayed, []); assert.deepEqual(frames, []);
    assert.equal(modelCalls, 6); assert.equal(executions, 3);
    assert.deepEqual(app.errors, []);
    eventSubscription.unsubscribe(); frameSubscription.unsubscribe(); cold.release(); coldPeer.release();
  } finally { await app.dispose(); }
});

test('CLI显式组合已开始的SDK调用，两个work跨会话归组，退出观察后真实文件工作仍各自结算', { timeout: 30_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-work-sdk-'));
  const entered = Promise.withResolvers<void>(), finish = Promise.withResolvers<void>();
  let executions = 0, modelCalls = 0;
  const record = defineAction({ name: 'write_work_file', description: '写入本次业务文件',
    scope: z.object({ merchantId: z.string(), workId: z.string() }),
    input: z.object({ file: z.string(), fail: z.boolean() }), output: z.string() });
  const failed = new Error('WORK_FILE_REJECTED');
  const implementation = implementAction(record, { async execute(input, call) {
    executions++;
    if (executions === 3) entered.resolve();
    await finish.promise;
    call.signal.throwIfAborted();
    if (input.fail) throw failed;
    await writeFile(join(root, input.file), call.scope.workId);
    return input.file;
  } });
  const surface = defineSurface({ views: {}, actions: { first: record, second: record } });
  const definition = defineApplication({ name: 'work-sdk', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '仅由外部SDK显式调用',
    program: implementProgram(defineProgram({ input: z.null(), surface }), { Component() {
      const first = useAction(implementation, () => firstWork, []);
      const second = useAction(implementation, () => secondWork, []);
      return Expose({ surface, value: { actions: { first, second } } });
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () { modelCalls++; yield* text('不应启动模型'); });
  const lifetime = new AbortController();
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const peer = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const actor = { actorId: 'cli-operator' };
    const first = session.resolveAction('first', firstWork)({ file: 'first.txt', fail: false }, actor);
    const second = session.resolveAction('second', secondWork)({ file: 'second.txt', fail: true }, actor);
    const peerFirst = peer.resolveAction('first', firstWork)({ file: 'peer.txt', fail: false }, actor);
    const rejected = assert.rejects(second.result, /WORK_FILE_REJECTED/u);
    await entered.promise;
    const selector = createWorkSelector({ identify });
    const sources = { invocations: [first, second, peerFirst] };
    const cli = selector.observe(firstWork, sources, { signal: lifetime.signal });
    const surviving = selector.observe(firstWork, { sessions: [session, peer], ...sources });
    const other = selector.observe(secondWork, sources);
    const seen: Observation<WorkSnapshot<WorkKey>>[] = [];
    const subscription = surviving.state$.subscribe(value => seen.push(value));
    const cliSubscription = cli.state$.subscribe();
    const running = await cli.state.read();
    assert.equal(running.sessions.length, 0, 'CLI无需会话观察来源即可读取SDK调用');
    assert.deepEqual(running.invocations.map(value => value.origin.sessionId), [session.id, peer.id]);
    assert.ok(running.invocations.every(value => value.activity.status === 'ready' && value.activity.value.state === 'running'));
    assert.deepEqual(running.invocations.flatMap(value => value.matchedInputs), [], 'SDK不伪造会话输入');
    const otherRunning = await other.state.read();
    assert.deepEqual(otherRunning.invocations.map(value => value.id), [second.id]);
    assert.deepEqual((await surviving.state.read()).sessions.map(value => value.calls), [[], []]);
    const projected = selector.select(firstWork, { invocations: [first, second, peerFirst].map(invocation => ({
      id: invocation.id, origin: invocation.origin, activity: invocation.activity.peek(),
    })) });
    assert.deepEqual(projected, running, '一次性投影与持续观察表达同一组调用');
    lifetime.abort(new Error('CLI_OBSERVER_EXITED'));
    assert.equal(cliSubscription.closed, true);
    finish.resolve();
    assert.deepEqual(await Promise.all([first.result, peerFirst.result]), ['first.txt', 'peer.txt']);
    await rejected;
    assert.equal(await readFile(join(root, 'first.txt'), 'utf8'), firstWork.workId);
    assert.equal(await readFile(join(root, 'peer.txt'), 'utf8'), firstWork.workId);
    assert.ok((await surviving.state.read()).invocations.every(value => value.activity.status === 'ready' && value.activity.value.state === 'returned'));
    const failure = (await other.state.read()).invocations[0]!.activity;
    assert.equal(failure.status, 'ready');
    if (failure.status === 'ready') {
      assert.equal(failure.value.state, 'failed');
      if (failure.value.state === 'failed') assert.equal(failure.value.phase, 'execute');
    }
    assert.ok(seen.some(value => value.status === 'ready' && value.value.invocations.some(invocation => invocation.activity.status === 'ready'
      && invocation.activity.value.state === 'returned')));
    assert.equal(executions, 3); assert.equal(modelCalls, 0);
    assert.deepEqual(app.errors, []);
    subscription.unsubscribe(); session.release(); peer.release();
  } finally { lifetime.abort(); finish.resolve(); await app.dispose(); }
});

test('动态调用成员固定首个观察来源，读取故障不冒充业务失败，移除和列表失效释放旧订阅', async () => {
  const finish = Promise.withResolvers<void>();
  const action = bindAction(implementAction(defineAction({ name: 'held_work', description: '已开始的独立工作',
    scope: z.object({ merchantId: z.string(), workId: z.string() }), input: z.null(), output: z.boolean() }), {
    async execute() { await finish.promise; return true; },
  }), firstWork, { admit() {} });
  const execution = { invocationId: 'held-work', sessionId: 'sdk-only',
    source: { kind: 'participant' as const, actorId: 'cli', participant: 'human' as const, entrypoint: 'cli' },
    inputs: [], signal: new AbortController().signal, continuation: null };
  const invocation = action(null, execution);
  const otherInvocation = action(null, { ...execution, invocationId: 'other-held-work' });
  const availability = new BehaviorSubject<Error | null>(null);
  let firstConnections = 0, secondConnections = 0;
  let firstSubscriptions = 0, firstReleases = 0;
  let secondSubscriptions = 0, secondReleases = 0;
  let onSecondRelease: (() => void) | undefined;
  const first = { id: invocation.id, origin: invocation.origin, activity: new ResourceReader({
    peek: () => invocation.activity.peek(),
    observations$: new Observable<Observation<InvocationActivity>>(subscriber => {
      firstConnections++;
      firstSubscriptions++;
      const connection = combineLatest([invocation.activity.observations$, availability]).pipe(map(([activity, error]) => error
        ? { status: 'error' as const, generation: activity.generation, error } : activity)).subscribe(subscriber);
      return () => { connection.unsubscribe(); firstConnections--; firstReleases++; };
    }),
  }) };
  const second = { id: invocation.id, origin: invocation.origin, activity: new ResourceReader({
    peek: () => invocation.activity.peek(),
    observations$: new Observable<Observation<InvocationActivity>>(subscriber => {
      secondConnections++;
      secondSubscriptions++;
      const connection = invocation.activity.observations$.pipe(map(value => ({ ...value, generation: value.generation + 1000 }))).subscribe(subscriber);
      return () => {
        connection.unsubscribe(); secondConnections--; secondReleases++;
        const reenter = onSecondRelease;
        onSecondRelease = undefined;
        reenter?.();
      };
    }),
  }) };
  const members = new BehaviorSubject<Observation<readonly WorkInvocationInput<WorkKey>[]>>({ status: 'ready', generation: 1,
    observedAt: 0, value: [first, second, { invocation: first, key: secondWork }] });
  const selector = createWorkSelector({ identify });
  const work = selector.observe(firstWork, { invocations: members });
  assert.equal(firstConnections, 0); assert.equal(secondConnections, 0, '构造Work不连接调用活动');
  const subscription = work.state$.subscribe();
  try {
    assert.equal((await work.state.read()).invocations.length, 1);
    assert.equal(firstConnections, 1); assert.equal(secondConnections, 0);
    const reordered: Observation<readonly WorkInvocationInput<WorkKey>[]> = { status: 'ready', generation: 2,
      observedAt: 0, value: [second, first] };
    members.next(reordered);
    await work.state.read();
    members.next(reordered);
    await work.state.read();
    members.next({ status: 'ready', generation: 3, observedAt: 0, value: [second, first, invocation] });
    await work.state.read();
    members.next({ status: 'ready', generation: 4, observedAt: 0, value: [second, first, invocation, otherInvocation] });
    assert.equal((await work.state.read()).invocations.length, 2);
    members.next({ status: 'ready', generation: 5, observedAt: 0, value: [second, first, invocation] });
    assert.equal((await work.state.read()).invocations.length, 1);
    assert.equal(firstSubscriptions, 1, '重排、重复ready、添加别名与增删其他调用都保留已选Reader连接');
    assert.equal(firstReleases, 0);
    assert.equal(firstConnections, 1); assert.equal(secondConnections, 0);
    const tagged = await selector.observe(secondWork, { invocations: [{ invocation, key: secondWork }] }).state.read();
    assert.deepEqual(tagged.invocations[0]!.matchedInputs, []);
    assert.deepEqual(tagged.invocations[0]!.keys, [secondWork]);
    assert.deepEqual(tagged.invocations[0]!.origin.inputs, [], '显式Work标签不会伪造调用原输入');
    const unavailable = new Error('INVOCATION_OBSERVER_UNAVAILABLE');
    availability.next(unavailable);
    members.next({ status: 'ready', generation: 6, observedAt: 0, value: [second, first, invocation] });
    const unreadable = (await work.state.read()).invocations[0]!.activity;
    assert.equal(unreadable.status, 'error');
    if (unreadable.status === 'error') assert.equal(unreadable.error, unavailable);
    assert.equal(secondConnections, 0, '首来源读取错误不会偷偷切换另一个别名');
    assert.equal(firstSubscriptions, 1); assert.equal(firstReleases, 0);
    members.next({ status: 'ready', generation: 7, observedAt: 0, value: [second, invocation] });
    const replacement = (await work.state.read()).invocations[0]!.activity;
    assert.ok(replacement.status === 'ready' && replacement.value.state === 'running');
    assert.equal(firstConnections, 0); assert.equal(secondConnections, 1);
    assert.equal(firstReleases, 1, '仅显式移除已选Reader才释放并切换同身份别名');
    onSecondRelease = () => members.next({ status: 'ready', generation: 9, observedAt: 0, value: [second, otherInvocation] });
    members.next({ status: 'pending', generation: 8 });
    assert.equal(work.state.peek().status, 'ready', '旧pending触发的真实teardown同步送入新列表后，旧pending不得覆盖新ready');
    assert.equal((await work.state.read()).invocations.length, 2);
    assert.equal(secondSubscriptions, 2);
    assert.equal(secondReleases, 1);
    assert.equal(secondConnections, 1, '旧列表清理不得关闭重入时创建的新连接，也不得循环重建');
    members.next({ status: 'ready', generation: 10, observedAt: 0, value: [second] });
    await work.state.read();
    assert.equal(secondSubscriptions, 2); assert.equal(secondReleases, 1);
    members.next({ status: 'pending', generation: 11 });
    assert.equal(work.state.peek().status, 'pending');
    assert.equal(secondConnections, 0, '列表pending释放旧成员，不能借用旧ready');
    const listingFailure = new Error('INVOCATION_LIST_UNAVAILABLE');
    members.next({ status: 'error', generation: 11, error: listingFailure });
    await assert.rejects(work.state.read(), error => error === listingFailure);
    members.next({ status: 'ready', generation: 12, observedAt: 0, value: [second] });
    await work.state.read();
    const conflict = { ...second, origin: { ...second.origin, source: { ...second.origin.source, actorId: 'different-actor' } } };
    members.next({ status: 'ready', generation: 13, observedAt: 0, value: [second, conflict] });
    await assert.rejects(work.state.read(), /REACTIVE_WORK_INVOCATION_ORIGIN_CONFLICT/u);
    assert.equal(secondConnections, 0);
    finish.resolve();
    assert.equal(await invocation.result, true, '来源失效不会取消已经开始的实际调用');
  } finally { subscription.unsubscribe(); finish.resolve(); await Promise.all([invocation.result, otherInvocation.result]); action.release(); }
});

test('steer使正式step共同消费两个work，完整发生保留同行输入，观察与来源退出不撤销已执行的业务', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-work-shared-step-'));
  const firstStream = Promise.withResolvers<void>(), finishFirstStream = Promise.withResolvers<void>();
  const executing = Promise.withResolvers<void>(), finishBusiness = Promise.withResolvers<void>();
  let modelCalls = 0, executions = 0;
  const record = defineAction({ name: 'shared_work', description: '执行当前步骤共同授权的工作', scope: z.null(),
    input: z.object({}).strict(), output: z.boolean() });
  const implementation = implementAction(record, { async execute() { executions++; executing.resolve(); await finishBusiness.promise; return true; } });
  const definition = defineApplication({ name: 'work-shared-step', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '处理本轮所有实际输入',
    program: implementProgram(defineProgram({ input: z.object({ merchantId: z.string(), workId: z.string() }),
      surface: defineSurface({ views: {}, actions: {} }) }), { Component() {
      const action = useAction(implementation, () => null, []);
      return h(Offer<typeof record>, { name: 'shared_work', action });
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () {
    modelCalls++;
    if (modelCalls === 1) { firstStream.resolve(); await finishFirstStream.promise; yield* text('首步只读第一项工作'); }
    else if (modelCalls === 2) yield* tool('shared_work', 'shared-call', {});
    else yield* text('两项工作共同完成');
  });
  const observerLifetime = new AbortController();
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const source = await app.host.open(session.id, definition.environments[0].program.definition);
    const peer = await app.host.open(session.id, definition.environments[0].program.definition);
    const otherSession = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const selector = createWorkSelector({ identify });
    const first = selector.observe(firstWork, { sessions: [source] }, { signal: observerLifetime.signal });
    const second = selector.observe(secondWork, { sessions: [source, otherSession] });
    const peerFirst = selector.observe(firstWork, { sessions: [peer] }), peerSecond = selector.observe(secondWork, { sessions: [peer] });
    const firstEvents: WorkEvent<WorkKey>[] = [], peerEvents: WorkEvent<WorkKey>[] = [];
    first.events$.subscribe(event => firstEvents.push(event));
    const peerEventsSubscription = peerFirst.events$.subscribe(event => peerEvents.push(event));
    const firstStates = first.state$.subscribe();
    const sourceStates = second.state$.subscribe();
    const secondMembers = await second.sessions.read();
    const surviving = secondMembers[1]!;
    assert.equal(surviving.identity.sessionId, otherSession.id);
    const survivingStates = surviving.state$.subscribe();
    const survivingEvents: WorkEvent<WorkKey>[] = [];
    const survivingEventsSubscription = surviving.events$.subscribe(event => survivingEvents.push(event));
    const peerStates = peerFirst.state$.subscribe();
    const frames: WorkProgressFrame<WorkKey>[] = [];
    const frameSubscription = peerSecond.progressFrames$.subscribe(frame => frames.push(frame));
    await session.submit({ requestId: 'first-input', content: '原先进行中的工作', intent: 'question', delivery: 'followup', scope: firstWork });
    await firstStream.promise;
    await session.submit({ requestId: 'second-input', content: '同一轮插入的另一项工作', intent: 'followup', delivery: 'steer', scope: secondWork });
    finishFirstStream.resolve(); await executing.promise;
    const runningFirst = await peerFirst.state.read(), runningSecond = await peerSecond.state.read();
    const firstCall = runningFirst.sessions[0]!.calls[0]!, secondCall = runningSecond.sessions[0]!.calls[0]!;
    assert.equal(firstCall.value.state, 'running'); assert.deepEqual(firstCall.value, secondCall.value);
    assert.deepEqual(firstCall.value.inputs.map(input => input.requestId), ['first-input', 'second-input']);
    assert.deepEqual(firstCall.matchedInputs.map(input => input.requestId), ['first-input']);
    assert.deepEqual(secondCall.matchedInputs.map(input => input.requestId), ['second-input']);
    const localSharedCall = (await (await peerSecond.sessions.read())[0]!.state.read()).calls[0]!;
    assert.deepEqual(localSharedCall.value.inputs.map(input => input.requestId), ['first-input', 'second-input']);
    assert.deepEqual(localSharedCall.matchedInputs.map(input => input.requestId), ['second-input']);
    observerLifetime.abort(new Error('FIRST_WORK_OBSERVER_LEFT'));
    const eventsAtExit = firstEvents.length;
    assert.equal(firstStates.closed, true);
    assert.equal((await peerFirst.state.read()).sessions[0]!.calls[0]!.value.state, 'running');
    source.release();
    assert.notEqual(second.state.peek().status, 'ready', '来源退出撤下完整ready，不能留下半份或旧现场');
    assert.notEqual(secondMembers[0]!.state.peek().status, 'ready');
    assert.equal((await surviving.state.read()).sessionId, otherSession.id);
    assert.equal(survivingStates.closed, false, '一个来源释放不关闭另一会话的局部观察');
    assert.equal(peerStates.closed, false); assert.equal(executions, 1);
    finishBusiness.resolve(); await session.whenIdle();
    const completedFirst = await peerFirst.state.read(), completedSecond = await peerSecond.state.read();
    const settledCall = completedFirst.sessions[0]!.calls[0]!.value;
    assert.equal(settledCall.state, 'returned');
    assert.equal(settledCall.callSequence, firstCall.value.callSequence);
    const starts = peerEvents.filter(event => event.event.kind === 'tool.started');
    const settlements = peerEvents.filter(event => event.event.kind === 'tool.settled');
    assert.equal(starts.length, 1); assert.equal(settlements.length, 1);
    assert.ok(starts[0]!.event.kind === 'tool.started' && settlements[0]!.event.kind === 'tool.settled');
    assert.equal(starts[0]!.event.callSequence, settledCall.callSequence);
    assert.equal(settlements[0]!.event.callSequence, settledCall.callSequence);
    assert.deepEqual(settlements[0]!.event.inputs, settledCall.inputs);
    const firstReply = completedFirst.sessions[0]!.replies.find(item => item.value.text === '两项工作共同完成');
    const secondReply = completedSecond.sessions[0]!.replies.find(item => item.value.text === '两项工作共同完成');
    assert.ok(firstReply && secondReply);
    assert.deepEqual(firstReply.value, secondReply.value);
    assert.deepEqual(firstReply.value.inputs?.map(input => input.requestId), ['first-input', 'second-input']);
    assert.deepEqual(firstReply.matchedInputs.map(input => input.requestId), ['first-input']);
    assert.deepEqual(secondReply.matchedInputs.map(input => input.requestId), ['second-input']);
    assert.ok(!completedSecond.sessions[0]!.replies.some(item => item.value.text === '首步只读第一项工作'));
    assert.ok(frames.some(frame => frame.frame.inputs.length === 2 && frame.matchedInputs[0]?.requestId === 'second-input'));
    await otherSession.submit({ requestId: 'surviving-input', content: '另一会话继续处理当前工作',
      intent: 'question', delivery: 'followup', scope: secondWork });
    await otherSession.whenIdle();
    const survivingSnapshot = await surviving.state.read();
    assert.deepEqual(survivingSnapshot.requests.map(item => item.value.requestId), ['surviving-input']);
    assert.ok(survivingSnapshot.replies.some(item => item.value.text === '两项工作共同完成'
      && item.matchedInputs.some(input => input.requestId === 'surviving-input')));
    assert.ok(survivingEvents.some(event => event.event.kind === 'assistant.message'
      && event.matchedInputs.some(input => input.requestId === 'surviving-input')));
    assert.ok(survivingEvents.every(event => event.source.sessionId === otherSession.id));
    assert.notEqual(second.state.peek().status, 'ready', '局部会话继续更新也不能把缺失来源的整体状态变成ready');
    assert.equal(firstEvents.length, eventsAtExit); assert.equal(modelCalls, 4); assert.equal(executions, 1);
    assert.deepEqual(app.errors, []);
    sourceStates.unsubscribe(); peerStates.unsubscribe(); peerEventsSubscription.unsubscribe(); frameSubscription.unsubscribe();
    survivingStates.unsubscribe(); survivingEventsSubscription.unsubscribe();
    peer.release(); otherSession.release(); session.release();
  } finally { observerLifetime.abort(); finishFirstStream.resolve(); finishBusiness.resolve(); await app.dispose(); }
});
