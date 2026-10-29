import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement as h } from 'react';
import { SessionId } from '@deepseek-ai/dsh-session';
import { createWorkSelector, defineAction, defineApplication, defineEnvironment, defineProgram, defineSurface,
  implementAction, z, type ConversationEvent, type WorkEvent, type WorkProgressFrame, type WorkOrigin } from '@hanger-source/dsh-reactive';
import { implementProgram, Offer, useAction } from '@hanger-source/dsh-reactive/program';
import { boot, text, tool } from './native-support.ts';

type WorkKey = { merchantId: string; workId: string };
const firstWork = { merchantId: 'merchant-a', workId: 'first' };
const secondWork = { merchantId: 'merchant-a', workId: 'second' };
function identify(origin: WorkOrigin): WorkKey | undefined {
  const scope = origin.scope;
  if (scope !== null && typeof scope === 'object' && !Array.isArray(scope)
    && 'merchantId' in scope && 'workId' in scope
    && typeof scope.merchantId === 'string' && typeof scope.workId === 'string') {
    return { merchantId: scope.merchantId, workId: scope.workId };
  }
  return undefined;
}

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
    const first = await selector.observe(firstWork, [session, peer]).state.read();
    const second = await selector.observe(secondWork, [session, peer]).state.read();
    const projected = selector.select(firstWork, [
      { identity: session.identity, progress: await session.progress.read(), human: await session.human.read() },
      { identity: peer.identity, progress: await peer.progress.read(), human: await peer.human.read() },
    ]);
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
    const coldWork = selector.observe(firstWork, [cold, coldPeer]);
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
    const first = selector.observe(firstWork, [source], { signal: observerLifetime.signal });
    const second = selector.observe(secondWork, [source, otherSession]);
    const peerFirst = selector.observe(firstWork, [peer]), peerSecond = selector.observe(secondWork, [peer]);
    const firstEvents: WorkEvent<WorkKey>[] = [], peerEvents: WorkEvent<WorkKey>[] = [];
    first.events$.subscribe(event => firstEvents.push(event));
    const peerEventsSubscription = peerFirst.events$.subscribe(event => peerEvents.push(event));
    const firstStates = first.state$.subscribe();
    const sourceStates = second.state$.subscribe();
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
    observerLifetime.abort(new Error('FIRST_WORK_OBSERVER_LEFT'));
    const eventsAtExit = firstEvents.length;
    assert.equal(firstStates.closed, true);
    assert.equal((await peerFirst.state.read()).sessions[0]!.calls[0]!.value.state, 'running');
    source.release();
    assert.notEqual(second.state.peek().status, 'ready', '来源退出撤下完整ready，不能留下半份或旧现场');
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
    assert.equal(firstEvents.length, eventsAtExit); assert.equal(modelCalls, 3); assert.equal(executions, 1);
    assert.deepEqual(app.errors, []);
    sourceStates.unsubscribe(); peerStates.unsubscribe(); peerEventsSubscription.unsubscribe(); frameSubscription.unsubscribe();
    peer.release(); otherSession.release(); session.release();
  } finally { observerLifetime.abort(); finishFirstStream.resolve(); finishBusiness.resolve(); await app.dispose(); }
});
