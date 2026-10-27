import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Fragment, createElement as h } from 'react';
import { Subject, concatMap, filter, firstValueFrom, from, map, tap, withLatestFrom } from 'rxjs';
import { abortable, defineApplication, defineEnvironment, z, type Continuation, type ConversationEvent } from '@hanger-source/dsh-reactive';
import { defineAction, implementAction } from '@hanger-source/dsh-reactive';
import { inputReferenceSchema } from '@hanger-source/dsh-reactive/kernel';
import { defineProgram } from '@hanger-source/dsh-reactive';
import { defineSurface, view } from '@hanger-source/dsh-reactive';
import { Context, Expose, Offer } from '@hanger-source/dsh-reactive/program';
import { useAction, useBeforeStep, useObservable, useRelation } from '@hanger-source/dsh-reactive/program';
import type { GenerateOptions } from '@deepseek-ai/dsh-llm';
import { boot, text, tool } from './native-support.ts';

test('模型前的业务边界失败会结算已 claim 的请求，新请求不继承旧输入，冷读保留失败事实', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-preparation-'));
  let reject = true;
  const requests: GenerateOptions[] = [];
  const events: ConversationEvent<{ ticket: string }>[] = [];
  const input = z.object({ ticket: z.string() });
  const surface = defineSurface({ views: { active: view(inputReferenceSchema.extend({ scope: input }).array().readonly()) }, actions: {} });
  const definition = defineApplication({ name: 'preparation', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '仅处理当前授权输入',
    program: implementProgram(defineProgram({ input, surface }), { Component({ conversation }) {
      const active = useObservable(() => conversation.inputs$.pipe(map(inputs =>
        inputs.map(input => ({ requestId: input.requestId, ticket: input.scope.ticket })))), [conversation]);
      useBeforeStep(() => { if (reject) throw new Error('BUSINESS_PREPARATION_REJECTED'); }, []);
      useRelation(() => conversation.events$.pipe(tap(event => events.push(event))), [conversation]);
      return h(Fragment, null, Context({ name: 'active-input', observation: active, render: value => value }),
        Expose({ surface, value: { views: { active: conversation.inputs$ } } }));
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* (options) { requests.push(options); yield* text('允许的新工作'); });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'rejected', content: '不应进入模型的旧请求', intent: 'question', delivery: 'followup' as const, scope: { ticket: 'old-ticket' } });
    await session.whenIdle();
    assert.equal(requests.length, 0);
    assert.equal(session.snapshot().requests.rejected?.state, 'settled');
    assert.equal(session.snapshot().requests.rejected?.outcome, 'error');
    assert.equal(app.errors.length, 1);
    assert.match(String(app.errors[0]), /BUSINESS_PREPARATION_REJECTED/u);
    assert.ok(events.some(event => event.kind === 'execution.error'));
    assert.ok(!events.some(event => event.kind === 'input.entered'));

    reject = false;
    await session.submit({ requestId: 'allowed', content: '处理新请求', intent: 'question', delivery: 'followup' as const, scope: { ticket: 'new-ticket' } });
    await session.whenIdle();
    assert.equal(requests.length, 1);
    const modelInput = JSON.stringify(requests[0]!.messages);
    assert.ok(modelInput.includes('new-ticket'));
    assert.ok(!modelInput.includes('old-ticket'));
    assert.equal(session.snapshot().requests.rejected?.outcome, 'error');
    const id = session.id;
    await app.host.close(id); session.release();
    const before = events.length;
    const cold = await app.host.open(id, definition.environments[0].program.definition);
    assert.equal(cold.snapshot().requests.rejected?.outcome, 'error');
    assert.equal(cold.snapshot().requests.allowed?.reply, '允许的新工作');
    assert.equal(events.length, before);
    assert.equal(requests.length, 1);
    cold.release();
  } finally { await app.dispose(); }
});

test('未就绪进度不会入模型或消除等待，新结果必须命中当前等待，已经受理的旧结果仍可幂等确认', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-progress-'));
  let continuation: Continuation | undefined;
  let calls = 0;
  const events: ConversationEvent<{ merchantId: string }>[] = [];
  const wait = { id: 'owner-operation', operation: { operationId: 'prepare-merchant' } };
  const startWork = defineAction({ name: 'start_owner_work', description: '登记已有业务操作的等待', scope: z.null(),
    input: z.object({}).strict(), output: z.boolean() });
  const startWorkImplementation = implementAction(startWork, {
    async execute(_input, call) {
      if (!call.continuation) throw new Error('NATIVE_CONTINUATION_REQUIRED');
      continuation = call.continuation;
      await call.wait(wait); return true;
    }
  });
  const definition = defineApplication({ name: 'progress', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '业务 owner 未就绪时继续等待',
    program: implementProgram(defineProgram({ input: z.object({ merchantId: z.string() }), surface: defineSurface({ views: {}, actions: {} }) }), { Component({ conversation }) {
        const start = useAction(startWorkImplementation, () => null, []);
        useRelation(() => conversation.events$.pipe(tap(event => events.push(event))), [conversation]);
        return h(Offer<typeof startWork>, { name: 'start_owner_work', action: start });
      } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () {
    calls++;
    if (calls === 1) yield* tool('start_owner_work', 'start-operation', {});
    else yield* text('已核对本次输入');
  });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const scope = { merchantId: 'merchant-a' };
    await session.submit({ requestId: 'original', content: '开始原业务工作', intent: 'question', delivery: 'followup' as const, scope });
    await session.whenIdle();
    assert.ok(continuation);
    const progress = { requestId: 'progress-1', content: '原业务仍在执行', intent: 'result' as const, scope,
      result: { continuation: continuation!, wait, ready: false } };
    await session.submit(progress); await session.whenIdle();
    assert.equal(calls, 1);
    assert.equal(session.snapshot().requests['progress-1']?.outcome, 'not-ready');
    assert.equal(session.snapshot().requests['progress-1']?.state, 'settled');
    assert.equal(session.snapshot().waits.length, 1);
    assert.equal(events.filter(event => event.kind === 'input.progress').length, 1);
    assert.ok(!events.some(event => event.kind === 'input.entered' && event.inputs.some(input => input.requestId === 'progress-1')));

    await session.submit({ requestId: 'other-question', content: '处理一个无关问题', intent: 'question', delivery: 'followup' as const, scope });
    await session.whenIdle();
    assert.equal(calls, 2);
    assert.equal(session.snapshot().waits.length, 1);
    assert.equal(events.filter(event => event.kind === 'wait.resolved').length, 0);
    await assert.rejects(session.submit({ ...progress, requestId: 'unknown-operation', result: { ...progress.result,
      wait: { id: 'never-opened', operation: wait.operation }, ready: true } }), /REACTIVE_WAIT_NOT_FOUND/u);
    assert.equal(session.snapshot().requests['unknown-operation'], undefined);

    const completed = { ...progress, requestId: 'result-1', content: '原业务已经就绪', result: { ...progress.result, ready: true } };
    await session.submit(completed); await session.whenIdle();
    assert.equal(calls, 3);
    assert.equal(session.snapshot().waits.length, 0);
    assert.equal(events.filter(event => event.kind === 'wait.resolved').length, 1);
    await session.submit(completed); await session.submit(progress); await session.whenIdle();
    assert.equal(calls, 3);
    assert.equal(events.filter(event => event.kind === 'input.progress').length, 1);
    assert.deepEqual(app.errors, []);
  } finally { await app.dispose(); }
});

test('真实 PTC 只产生一次子调用反馈，保持 root 与 parent 归属，关闭和持久重启不重放旧反应', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-ptc-events-'));
  let calls = 0;
  let writes = 0;
  let reactions = 0;
  const events: ConversationEvent<null>[] = [];
  const observedStates: string[] = [];
  const saveOwner = defineAction({ name: 'save_owner', description: '保存正式业务值', scope: z.null(),
    input: z.object({ revision: z.number() }).strict(), output: z.object({ revision: z.number() }), observe: output => output });
  const saveOwnerImplementation = implementAction(saveOwner, {
    async execute(input) { writes++; return input; }
  });
  const definition = defineApplication({ name: 'ptc-events', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '通过生成 SDK 写入业务事实',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component({ conversation }) {
      const save = useAction(saveOwnerImplementation, () => null, []);
      useRelation(() => conversation.events$.pipe(tap(event => events.push(event)),
        filter((event): event is Extract<ConversationEvent<null>, { kind: 'tool.settled' }> => event.kind === 'tool.settled' && event.name === 'save_owner'),
        withLatestFrom(conversation.activity$), tap(([event, activity]) => {
          reactions++;
          observedStates.push(activity.requests['write-request']!.tools.find(tool => tool.callId === event.callId)!.state);
        })), [conversation]);
      return h(Offer<typeof saveOwner>, { name: 'save_owner', action: save });
    } }),
  })] });
  const respond = async function* () {
    calls++;
    if (calls === 1) yield* tool('run_code', 'ptc-root', { description: '保存当前修订', code: 'const saved = await tools.save_owner({ revision: 7 }); console.log(saved);' });
    else yield* text('写入完成');
  };
  let app = await boot(join(root, 'home'), definition, respond, 'ptc');
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'write-request', content: '保存正式修订', intent: 'question', delivery: 'followup' as const, scope: null });
    await session.whenIdle();
    assert.deepEqual(app.errors, []);
    assert.equal(writes, 1); assert.equal(reactions, 1);
    assert.deepEqual(observedStates, ['returned']);
    const started = events.filter((event): event is Extract<ConversationEvent<null>, { kind: 'tool.started' }> => event.kind === 'tool.started' && event.name === 'save_owner');
    const settled = events.filter((event): event is Extract<ConversationEvent<null>, { kind: 'tool.settled' }> => event.kind === 'tool.settled' && event.name === 'save_owner');
    assert.equal(started.length, 1); assert.equal(settled.length, 1);
    assert.equal(started[0]!.callId, settled[0]!.callId);
    assert.equal(settled[0]!.rootCallId, 'ptc-root');
    assert.equal(settled[0]!.parentCallId, 'ptc-root');
    assert.equal(settled[0]!.inputs[0]!.requestId, 'write-request');
    assert.deepEqual(settled[0]!.result, { revision: 7 });
    assert.ok(events.some(event => event.kind === 'assistant.stream' && event.frame.phase === 'text'));
    const id = session.id;
    await app.host.close(id); session.release();
    await app.dispose();
    const before = events.length;
    app = await boot(join(root, 'home'), definition, respond, 'ptc');
    const cold = await app.host.open(id, definition.environments[0].program.definition);
    assert.equal(cold.snapshot().requests['write-request']?.tools.find(tool => tool.name === 'save_owner')?.state, 'returned');
    assert.equal(events.length, before);
    assert.equal(calls, 2); assert.equal(writes, 1); assert.equal(reactions, 1);
    cold.release();
    const resumed = await app.host.resume(id, definition.environments[0].program.definition);
    await resumed.whenIdle();
    assert.equal(events.length, before);
    assert.equal(calls, 2); assert.equal(reactions, 1);
    resumed.release();
  } finally { await app.dispose(); }
});

test('对话事实的直接订阅回调可以提交后续输入，原生追加完成后才交付事件', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-event-command-'));
  let calls = 0;
  let receipts = 0;
  const admitted: { requestId: string; actorId: string; participant: string }[] = [];
  const definition = defineApplication({ name: 'event-command', admit: async (input, _identity, admission) => {
    admitted.push({ requestId: input.requestId, actorId: admission.actorId, participant: admission.participant });
    if (input.requestId === 'follow-command' && (admission.participant !== 'service' || admission.actorId !== 'program-service'))
      throw new Error('PROGRAM_SERVICE_INPUT_REQUIRED');
  }, environments: [defineEnvironment({
    id: 'work', instructions: '按输入依次回答',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component({ conversation }) {
      useRelation(() => conversation.events$.pipe(filter(event => event.kind === 'assistant.message' && event.text === 'first'),
        concatMap(() => from(conversation.submit({ requestId: 'follow-command', content: '响应已完成的第一条回复', intent: 'followup', delivery: 'followup' as const, scope: null }))),
        tap(() => { receipts++; })), [conversation]);
      return null;
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () { calls++; yield* text(calls === 1 ? 'first' : 'second'); });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'first-command', content: '先完成第一条回复', intent: 'question', delivery: 'followup' as const, scope: null });
    await session.whenIdle();
    assert.equal(calls, 2);
    assert.equal(receipts, 1);
    const observed = await firstValueFrom(session.request('follow-command').state$.pipe(filter(value => value.status === 'ready')));
    assert.equal(observed.value.activity?.reply, 'second');
    assert.ok(admitted.some(input => input.requestId === 'first-command' && input.participant === 'human' && input.actorId === 'operator-test'));
    assert.ok(admitted.some(input => input.requestId === 'follow-command' && input.participant === 'service' && input.actorId === 'program-service'));
    assert.deepEqual(app.errors, []);
  } finally { await app.dispose(); }
});

test('反应在等待上下文期间失败，随后上下文就绪也不能让已失败程序继续请求模型', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-admission-failure-'));
  const context$ = new Subject<string>();
  const reaction$ = new Subject<void>();
  const checked = Promise.withResolvers<void>();
  let calls = 0;
  const definition = defineApplication({ name: 'admission-failure', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '必须等待程序和现场都有效',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component() {
      useRelation(() => reaction$, [reaction$]);
      const context = useObservable(() => context$, [context$]);
      useBeforeStep(() => { checked.resolve(); }, []);
      return Context({ name: 'owner-context', observation: context, render: value => value });
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () { calls++; yield* text('不应请求'); });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'blocked-program', content: '等待业务现场', intent: 'question', delivery: 'followup' as const, scope: null });
    await checked.promise;
    // 让已通过的校验和随后的微任务执行完；上下文仍无值，admission 必须停在依赖等待中。
    await new Promise<void>(resolve => setImmediate(resolve));
    assert.equal(calls, 0);
    reaction$.error(new Error('BUSINESS_REACTION_FAILED_WHILE_WAITING'));
    context$.next('业务现场现在就绪');
    await session.whenIdle();
    assert.equal(calls, 0);
    assert.equal(session.snapshot().requests['blocked-program']?.state, 'settled');
    assert.equal(session.snapshot().requests['blocked-program']?.outcome, 'error');
    assert.equal(app.errors.length, 1);
    assert.match(String(app.errors[0]), /BUSINESS_REACTION_FAILED_WHILE_WAITING/u);
  } finally { context$.complete(); await app.dispose(); }
});

test('释放一个观察句柄只取消自己的挂起读取，其他句柄继续读取同一业务来源', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-read-lifetime-'));
  const loaded = Promise.withResolvers<{ revision: number }>();
  const started = Promise.withResolvers<void>();
  let ownerSignal: AbortSignal | undefined;
  let connections = 0;
  let calls = 0;
  const source$ = abortable(async signal => {
    connections++;
    ownerSignal = signal;
    started.resolve();
    return loaded.promise;
  });
  const surface = defineSurface({ views: { record: view(z.object({ revision: z.number() })) }, actions: {} });
  const definition = defineApplication({ name: 'read-lifetime', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '只观察业务来源',
    program: implementProgram(defineProgram({ input: z.null(), surface }), { Component() { return Expose({ surface, value: { views: { record: source$ } } }); } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () { calls++; yield* text('不应请求'); });
  try {
    const first = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const second = app.host.conversation(first.id, definition.environments[0].program.definition);
    await started.promise;
    const firstRead = first.readView('record');
    const rejected = assert.rejects(firstRead, /REACTIVE_SCOPE_DISPOSED/u);
    const secondRead = second.readView('record');
    first.release();
    await rejected;
    assert.equal(ownerSignal!.aborted, false);
    assert.equal(connections, 1);
    loaded.resolve({ revision: 8 });
    assert.deepEqual(await secondRead, { revision: 8 });
    assert.deepEqual(await second.readView('record'), { revision: 8 });
    const caller = new AbortController();
    caller.abort(new Error('CALLER_ALREADY_LEFT'));
    await assert.rejects(second.readView('record', { signal: caller.signal }), /CALLER_ALREADY_LEFT/u);
    assert.equal(ownerSignal!.aborted, false);
    assert.equal(connections, 1);
    assert.equal(calls, 0);
    assert.deepEqual(app.errors, []);
    second.release();
  } finally { loaded.resolve({ revision: 8 }); await app.dispose(); }
});
