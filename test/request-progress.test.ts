import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement as h } from 'react';
import { firstValueFrom, tap, type Subscription } from 'rxjs';
import { createAssistantMessage, type GenerateOptions } from '@deepseek-ai/dsh-llm';
import { expandAssistantStream } from '@deepseek-ai/dsh-llm/assistant-stream';
import { SessionId } from '@deepseek-ai/dsh-session';
import { createWorkSelector, defineApplication } from '@hanger-source/dsh-reactive';
import { defineEnvironment } from '@hanger-source/dsh-reactive';
import { z } from '@hanger-source/dsh-reactive';
import { defineAction, implementAction } from '@hanger-source/dsh-reactive';
import type { AssistantProgressFrame, ConversationEvent } from '@hanger-source/dsh-reactive';
import { defineProgram } from '@hanger-source/dsh-reactive';
import { defineSurface } from '@hanger-source/dsh-reactive';
import { Offer } from '@hanger-source/dsh-reactive/program';
import { useAction, useBeforeStep, useRelation, useResource } from '@hanger-source/dsh-reactive/program';
import { boot, text, tool } from './native-support.ts';

test('晚订阅取得真实活动前缀，排队的新请求不会改写原 attempt 输入或分享旧回复', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-request-prefix-'));
  const streamed = Promise.withResolvers<void>(), finish = Promise.withResolvers<void>();
  const programObserved = Promise.withResolvers<void>();
  let calls = 0;
  // custom 保留传入对象，能真实暴露将原生 JSON 引用直接交给领域 schema 的错误。
  const ticket = z.custom<{ ticket: string }>(value => value !== null && typeof value === 'object'
    && 'ticket' in value && typeof value.ticket === 'string' && Object.keys(value).length === 1);
  const definition = defineApplication({ name: 'request-prefix', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '依次回答每个实际输入',
    program: implementProgram(defineProgram({ input: ticket, surface: defineSurface({ views: {}, actions: {} }) }), { Component({ conversation }) {
      useResource(conversation.progress);
      const { state: request } = useResource(conversation.request('first').state);
      if (request.status === 'ready' && request.value.activity?.state === 'settled') programObserved.resolve();
      useRelation(() => conversation.inputs$.pipe(tap(values => {
        for (const input of values) input.scope.ticket = 'changed-by-input-observer';
      })), [conversation]);
      return null;
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () {
    calls++;
    if (calls !== 1) { yield* text('second-response'); return; }
    yield { type: 'block-start', index: 0, blockType: 'text' };
    yield { type: 'text-delta', index: 0, text: 'first-' };
    streamed.resolve(); await finish.promise;
    yield { type: 'text-delta', index: 0, text: 'response' };
    yield { type: 'block-end', index: 0, block: { type: 'text', text: 'first-response' } };
    yield { type: 'finish', reason: { kind: 'stop' } };
  });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const first = session.request('first'), second = session.request('second');
    assert.equal(session.request('first'), first);
    const firstEvents: ConversationEvent<{ ticket: string }>[] = [], secondEvents: ConversationEvent<{ ticket: string }>[] = [];
    first.events$.subscribe(event => {
      if ('inputs' in event && event.inputs !== undefined) {
        for (const input of event.inputs) input.scope.ticket = 'changed-by-event-observer';
      }
      firstEvents.push(event);
    });
    second.events$.subscribe(event => secondEvents.push(event));
    await session.submit({ requestId: 'first', content: '处理第一张单', intent: 'question', delivery: 'followup' as const, scope: { ticket: 'one' } }, { actorId: 'Alice' });
    await streamed.promise;
    await session.submit({ requestId: 'second', content: '处理第二张单', intent: 'question', delivery: 'followup' as const, scope: { ticket: 'two' } }, { actorId: 'Bob' });
    const late = await app.host.open(session.id, definition.environments[0].program.definition);
    const baseline = late.progress.peek();
    assert.ok(baseline.status === 'ready');
    const active = baseline.value.attempts[0];
    assert.ok(active);
    assert.equal(active.nextIndex, 2);
    assert.deepEqual(active.inputs.map(input => [input.requestId, input.actorId, input.scope]), [['first', 'Alice', { ticket: 'one' }]]);
    assert.equal(expandAssistantStream(active.stream).flatMap(member => member.chunk.type === 'text-delta' ? [member.chunk.text] : []).join(''), 'first-');
    const lateFirst = await late.request('first').state.read(), lateSecond = await late.request('second').state.read();
    assert.equal(lateFirst.attempts[0]?.attemptId, active.attemptId);
    assert.equal(lateSecond.activity?.state, 'accepted');
    assert.equal(lateSecond.attempts.length, 0);
    const native = app.native.sessions.get(SessionId(session.id));
    assert.ok(native);
    const nativeState = app.native.sessionProjections.stateOf(native, 'reactiveInputs');
    assert.ok(nativeState);
    assert.deepEqual(nativeState.records.first?.scope, { ticket: 'one' });
    assert.deepEqual(nativeState.records.second?.scope, { ticket: 'two' });
    const frames: AssistantProgressFrame[] = [];
    late.progressFrames$.subscribe(value => frames.push(value));
    assert.equal(frames.length, 0);
    let boundarySubscription: Subscription | undefined, boundarySequence: number | undefined;
    const boundaryEvents: ConversationEvent<{ ticket: string }>[] = [];
    const stopBoundary = app.native.on('session/event', (nativeSession, event) => {
      if (nativeSession.id !== session.id || event.type !== 'assistant/message' || boundarySubscription) return;
      // 这一原生事实已进入当前快照；在追加 callback 内才订阅，不能再收到它的延迟通知。
      assert.equal(session.snapshot().sequence, event.seq);
      boundarySequence = event.seq;
      boundarySubscription = first.events$.subscribe(value => boundaryEvents.push(value));
    }, { global: true });
    finish.resolve();
    await session.whenIdle();
    await programObserved.promise;
    assert.equal(calls, 2);
    const firstMessages = firstEvents.filter(event => event.kind === 'assistant.message');
    const secondMessages = secondEvents.filter(event => event.kind === 'assistant.message');
    assert.deepEqual(firstMessages.map(event => event.text), ['first-response']);
    assert.deepEqual(secondMessages.map(event => event.text), ['second-response']);
    const firstTail = frames.filter(value => value.frame.attemptId === active.attemptId);
    assert.ok(firstTail.some(value => value.frame.type === 'chunk' && value.frame.chunk.type === 'block-end'));
    assert.ok(firstTail.every(value => value.inputs[0]?.requestId === 'first' && value.inputs[0]?.actorId === 'Alice'));
    assert.ok(firstTail.some(value => value.frame.type === 'end' && value.frame.outcome.kind === 'committed'
      && value.frame.outcome.eventType === 'assistant/message'));
    const completed = await firstValueFrom(first.state$);
    assert.ok(completed.status === 'ready');
    assert.equal(completed.value.activity?.reply, 'first-response');
    assert.equal(completed.value.attempts.length, 0);
    assert.ok(boundarySequence !== undefined && boundarySubscription);
    assert.ok(!boundaryEvents.some(event => event.kind === 'assistant.message' && event.sequence === boundarySequence));
    assert.ok(boundaryEvents.some(event => event.kind === 'assistant.stream' && event.frame.phase === 'end'
      && event.frame.outcome.kind === 'committed' && event.frame.outcome.seq === boundarySequence));
    boundarySubscription.unsubscribe(); stopBoundary();
    const selector = createWorkSelector({ identify(origin) {
      const scope = origin.scope;
      return scope !== null && typeof scope === 'object' && !Array.isArray(scope) && 'ticket' in scope && typeof scope.ticket === 'string'
        ? scope.ticket : undefined;
    } });
    const work = selector.observe('one', [session]);
    const unrelated = createAssistantMessage({ content: [{ type: 'text', text: '外部发布者追加的独立说明' }],
      source: { provider: 'external-publisher', model: 'published-message' } });
    const externalObserved = Promise.withResolvers<void>();
    const rawSubscription = session.rawEvents$.subscribe(event => {
      if (event.kind === 'assistant.message' && event.messageId === unrelated.id) {
        assert.equal(event.inputs, undefined); externalObserved.resolve();
      }
    });
    const workMessages: string[] = [];
    const workSubscription = work.events$.subscribe(value => {
      if (value.event.kind === 'assistant.message') workMessages.push(value.event.messageId);
    });
    // 真正的外部日志发布发生在原生step结束之后；沿用已有turn/step数字也不构成输入归属证据。
    native.append('assistant/message', { turn: active.turn, step: active.step, message: unrelated, stream: [] }, { surfaceOp: 'append' });
    await externalObserved.promise;
    const externalReply = session.snapshot().replies.find(reply => reply.messageId === unrelated.id);
    assert.ok(externalReply);
    assert.equal(externalReply.inputs, null);
    assert.equal((await first.state.read()).activity?.reply, 'first-response');
    assert.ok(!(await work.state.read()).sessions[0]!.replies.some(reply => reply.value.messageId === unrelated.id));
    assert.deepEqual(workMessages, []);
    assert.equal(calls, 2);
    rawSubscription.unsubscribe(); workSubscription.unsubscribe();
    assert.equal(app.errors.length, 0);
    const releasedRequest = late.request('first');
    late.release();
    assert.throws(() => releasedRequest.state.read(), /REACTIVE_SCOPE_DISPOSED/u);
    assert.throws(() => late.progress.peek(), /REACTIVE_SCOPE_DISPOSED/u);
    assert.equal((await first.state.read()).activity?.reply, 'first-response', '释放借用者不撤下另一 SDK 的事实');
    session.release();
  } finally { finish.resolve(); await app.dispose(); }
});

test('无首 token 的原生取消提交 assistant/attempt，丢弃排队请求，持久冷读不复活活动草稿', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-request-cancel-'));
  const started = Promise.withResolvers<void>();
  let calls = 0;
  const definition = defineApplication({ name: 'request-cancel', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '等待模型适配器响应',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component: () => null }),
  })] });
  const respond = async function* (options: GenerateOptions) {
    calls++; started.resolve();
    const signal = options.signal;
    if (!signal) throw new Error('NATIVE_REQUEST_SIGNAL_REQUIRED');
    await new Promise<void>(resolve => {
      if (signal.aborted) resolve();
      else signal.addEventListener('abort', () => resolve(), { once: true });
    });
  };
  let app = await boot(join(root, 'home'), definition, respond);
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const frames: AssistantProgressFrame[] = [], queuedEvents: ConversationEvent<null>[] = [];
    session.progressFrames$.subscribe(value => frames.push(value));
    session.request('queued').events$.subscribe(value => queuedEvents.push(value));
    await session.submit({ requestId: 'running', content: '等待模型的原请求', intent: 'question', delivery: 'followup' as const, scope: null });
    await started.promise;
    const active = session.progressSnapshot();
    assert.ok(active.status === 'ready');
    assert.equal(active.value.attempts[0]?.nextIndex, 0);
    await session.submit({ requestId: 'queued', content: '稍后才会进入的请求', intent: 'question', delivery: 'followup' as const, scope: null });
    await session.cancel();
    assert.equal(calls, 1);
    const ended = frames.find(value => value.frame.type === 'end');
    assert.ok(ended?.frame.type === 'end' && ended.frame.outcome.kind === 'committed');
    assert.equal(ended.frame.outcome.eventType, 'assistant/attempt');
    assert.equal(ended.inputs[0]?.requestId, 'running');
    const outcome = ended.frame.outcome;
    const native = app.native.sessions.get(SessionId(session.id));
    assert.ok(native);
    const committed = native.snapshotEvents().find(event => event.seq === outcome.seq);
    assert.equal(committed?.type, 'assistant/attempt');
    assert.ok(!native.snapshotEvents().some(event => event.type === 'assistant/message'));
    assert.ok(!queuedEvents.some(event => event.kind === 'input.entered' || event.kind === 'assistant.stream' || event.kind === 'assistant.message'));
    const queued = await firstValueFrom(session.request('queued').state$);
    assert.ok(queued.status === 'ready');
    assert.equal(queued.value.activity?.outcome, 'cancelled');
    assert.equal(queued.value.attempts.length, 0);
    const id = session.id;
    await app.host.close(id); session.release();
    assert.equal(app.errors.length, 0);
    await app.dispose();
    app = await boot(join(root, 'home'), definition, respond);
    const cold = await app.host.open(id, definition.environments[0].program.definition);
    const coldState = await firstValueFrom(cold.request('running').state$);
    assert.ok(coldState.status === 'ready');
    assert.equal(coldState.value.source, 'durable');
    assert.equal(coldState.value.activity?.state, 'settled');
    assert.equal(coldState.value.activity?.outcome, 'aborted');
    assert.equal(coldState.value.activity?.reply, null);
    assert.equal(coldState.value.attempts.length, 0);
    const replayed: AssistantProgressFrame[] = [];
    cold.progressFrames$.subscribe(value => replayed.push(value));
    assert.equal(replayed.length, 0);
    assert.equal(app.native.agents.get(SessionId(id)), undefined);
    assert.equal(calls, 1);
    cold.release();
  } finally { await app.dispose(); }
});

test('注入的供应商边界错误沿原生step关联请求，后续装配失败不借上一已完成step的输入', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-request-errors-'));
  let calls = 0, rejectPreparation = false;
  const change = defineAction({ name: 'change_business', description: '改变随后必须核验的业务条件', scope: z.null(),
    input: z.object({}).strict(), output: z.boolean() });
  const implementation = implementAction(change, { execute() { rejectPreparation = true; return true; } });
  const definition = defineApplication({ name: 'request-errors', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '业务变化后重新核验步骤准入',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component() {
      const action = useAction(implementation, () => null, []);
      useBeforeStep(() => { if (rejectPreparation) throw new Error('BUSINESS_CHANGED_BEFORE_NEXT_STEP'); }, []);
      return h(Offer<typeof change>, { name: 'change_business', action });
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () {
    calls++;
    if (calls === 1) {
      yield { type: 'block-start', index: 0, blockType: 'text' };
      yield { type: 'text-delta', index: 0, text: '尚未形成正式回复' };
      yield { type: 'finish', reason: { kind: 'error', failure: { code: 'UPSTREAM_UNAVAILABLE', message: '模型连接断开' } } };
    } else yield* tool('change_business', 'change-call', {});
  });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const errors: Extract<ConversationEvent<null>, { kind: 'execution.error' }>[] = [];
    const firstEvents: ConversationEvent<null>[] = [], changedEvents: ConversationEvent<null>[] = [];
    session.events$.subscribe(event => { if (event.kind === 'execution.error') errors.push(event); });
    session.request('model-error').events$.subscribe(event => firstEvents.push(event));
    session.request('business-changed').events$.subscribe(event => changedEvents.push(event));
    await session.submit({ requestId: 'model-error', content: '触发注入的供应商边界错误', intent: 'question', delivery: 'followup' as const, scope: null });
    await session.whenIdle();
    assert.equal(errors.length, 1);
    assert.equal(errors[0]?.inputs?.[0]?.requestId, 'model-error');
    assert.ok(firstEvents.some(event => event.kind === 'execution.error'));
    assert.ok(firstEvents.some(event => event.kind === 'assistant.stream' && event.frame.phase === 'end'
      && event.frame.outcome.kind === 'committed' && event.frame.outcome.eventType === 'assistant/attempt'));
    assert.ok(!firstEvents.some(event => event.kind === 'assistant.message'));
    await session.submit({ requestId: 'business-changed', content: '改变下一步骤的业务准入', intent: 'question', delivery: 'followup' as const, scope: null });
    await session.whenIdle();
    assert.equal(calls, 2);
    assert.equal(errors.length, 2);
    assert.equal(errors[1]?.step, 1);
    assert.match(String(errors[1]?.error), /BUSINESS_CHANGED_BEFORE_NEXT_STEP/u);
    assert.equal(errors[1]?.inputs, undefined);
    assert.ok(changedEvents.some(event => event.kind === 'tool.settled' && event.callId === 'change-call'));
    assert.ok(!changedEvents.some(event => event.kind === 'execution.error'));
    const observed = await firstValueFrom(session.request('business-changed').state$);
    assert.ok(observed.status === 'ready' && observed.value.activity);
    const activity = observed.value.activity;
    const changedTool = activity.tools.find(value => value.callId === 'change-call');
    assert.ok(changedTool);
    const native = app.native.sessions.get(SessionId(session.id));
    assert.ok(native);
    const projected = app.native.sessionProjections.stateOf(native, 'reactiveInputs');
    assert.ok(projected);
    assert.notEqual(activity, projected.requests['business-changed']);
    // 页面若把快照当作可编辑 state，也不能伪造消费状态、工具结果或输入归属。
    assert.equal(Reflect.set(activity, 'state', 'accepted'), false);
    assert.equal(Reflect.set(changedTool, 'state', 'running'), false);
    assert.equal(Reflect.set(changedTool.inputs[0]!, 'requestId', 'model-error'), false);
    assert.equal(Reflect.set(activity.tools, 'length', 0), false);
    assert.equal(projected.requests['business-changed']?.state, 'settled');
    const originalCall = Object.values(projected.calls).find(call => call.callId === 'change-call');
    assert.equal(originalCall?.state, 'returned');
    assert.equal(originalCall?.inputs[0]?.requestId, 'business-changed');
    // 同一请求的显式确定性重投仍依原生已消费事实去重，不因观察者写入重新执行业务。
    await session.submit({ requestId: 'business-changed', content: '改变下一步骤的业务准入', intent: 'question', delivery: 'followup' as const, scope: null });
    await session.whenIdle();
    assert.equal(calls, 2);
    assert.equal(app.errors.length, 2);
    session.release();
  } finally { await app.dispose(); }
});
