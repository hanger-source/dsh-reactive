import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { test, type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement, Fragment } from 'react';
import { SessionId, appendPluginRecord } from '@deepseek-ai/dsh-session';
import { ToolCallId, type StreamChunk } from '@deepseek-ai/dsh-llm';
import type { Agent } from '@deepseek-ai/dsh-agent';
import { Offer, useAction, useBeforeStep } from '@hanger-source/dsh-reactive/program';
import { defineAction, implementAction, defineApplication, defineEnvironment, defineProgram, defineSurface, z, type Continuation, type Submission, type WaitReference } from '@hanger-source/dsh-reactive';
import { boot, text, tool } from './native-support.ts';
import { contentDigest, submissionSourceDigest } from '../packages/dsh/src/conversations/records.ts';
import { admittedSubmissionSchema, submissionSchema } from '@hanger-source/dsh-reactive/kernel';

const waits = {
  first: { id: 'first-owner', operation: { id: 'operation-first' } },
  second: { id: 'second-owner', operation: { id: 'operation-second' } },
};
function result(requestId: string, continuation: Continuation, wait: WaitReference): Submission<null> {
  return { requestId, content: '正式完成 ' + wait.id, intent: 'result', scope: null,
    result: { continuation, wait, ready: true } };
}
async function* parallelWaits(): AsyncIterable<StreamChunk> {
  for (const [index, branch] of ['first', 'second'].entries()) {
    const id = ToolCallId('wait-' + branch);
    const args = { branch };
    yield { type: 'block-start', index, blockType: 'tool-call' };
    yield { type: 'tool-call-delta', index, id, name: 'begin_wait', argumentsDelta: JSON.stringify(args) };
    yield { type: 'block-end', index, block: { type: 'tool-call', id, name: 'begin_wait', arguments: JSON.stringify(args) } };
  }
  yield { type: 'finish', reason: { kind: 'tool-calls' } };
}
function pending(agent: Agent, requestId: string) {
  return [...agent.inbox.nextStep, ...agent.inbox.nextTurn].find(message =>
    'rpcId' in message.source && message.source.rpcId === requestId);
}
function admittedId(agent: Agent, requestId: string) {
  const message = agent.session.snapshotEvents().flatMap(event => event.type === 'agent/inbox/spliced' ? event.data.inserted : [])
    .find(message => 'rpcId' in message.source && message.source.rpcId === requestId);
  assert.ok(message);
  return message.id;
}

type PauseBoundary = 'assemble' | 'request';
type ControlOrder = 'pause-cancel' | 'cancel-pause';
function pauseApplication(root: string, boundary: PauseBoundary, initialCalls = 0) {
  const entered = Promise.withResolvers<void>(), proceed = Promise.withResolvers<void>();
  let armed = false;
  let continuation: Continuation | undefined;
  let calls = initialCalls;
  const begin = defineAction({ name: 'begin_owner', description: '登记真实业务等待', scope: z.null(),
    input: z.object({}).strict(), output: z.boolean() });
  const beginImplementation = implementAction(begin, {
    async execute(_input, call) {
      assert.ok(call.continuation?.goal);
      continuation = call.continuation;
      await call.wait(waits.first);
      return true;
    }
  });
  const definition = defineApplication({ name: 'pause-wait-' + boundary, admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '尊重人的暂停，不重放原业务',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component() {
      const beginning = useAction(beginImplementation, () => null, []);
      useBeforeStep(async () => {
        if (boundary !== 'assemble' || !armed) return;
        armed = false; entered.resolve(); await proceed.promise;
      }, []);
      return Offer({ name: 'begin_owner', action: beginning });
    } }),
  })] });
  const respond = async function* () {
    calls++;
    if (calls !== 1) throw new Error('PAUSED_RESULT_MUST_NOT_ENTER_MODEL');
    yield* tool('begin_owner', 'begin-before-pause', {});
  };
  return { definition, respond, entered, proceed, calls: () => calls,
    arm: () => { armed = true; },
    pauseRequest: async () => { if (armed) { armed = false; entered.resolve(); await proceed.promise; } },
    completed: () => result('paused-result', continuation!, waits.first) };
}
async function pauseScenario(root: string, boundary: PauseBoundary, order: ControlOrder) {
  const fixture = pauseApplication(root, boundary);
  const app = await boot(join(root, 'home'), fixture.definition, fixture.respond);
  let release = () => {};
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'original', content: '持续处理原业务', intent: 'goal', delivery: 'followup' as const, scope: null });
    await session.whenIdle();
    if (boundary === 'request') release = app.native.on('agent/request', async (_payload, next) => {
      const config = await next();
      await fixture.pauseRequest();
      return config;
    });
    fixture.arm();
    const completed = fixture.completed();
    await session.submit(completed);
    await fixture.entered.promise;
    const agent = app.native.agents.get(SessionId(session.id))!;
    const messageId = admittedId(agent, completed.requestId);
    assert.equal(session.snapshot().waits.length, 0);
    const cancelling = order === 'cancel-pause' ? session.cancel() : undefined;
    await session.pause();
    fixture.proceed.resolve();
    await cancelling;
    await session.whenIdle();
    return { app, fixture, session, agent, messageId, completed, release };
  } catch (error) { release(); fixture.proceed.resolve(); await app.dispose(); throw error; }
}

type PauseCheckpoint = { id: string; completed: Submission<null>; messageId: string; pendingId: string | undefined;
  snapshot: ReturnType<Awaited<ReturnType<typeof pauseScenario>>['session']['snapshot']>; calls: number; errors: string[] };
async function crashAfterPause(root: string, context: TestContext): Promise<PauseCheckpoint> {
  const worker = fork(import.meta.filename, ['--pause-checkpoint', root], {
    execPath: process.execPath, execArgv: [], stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  });
  let output = '';
  worker.stdout?.on('data', chunk => { output += String(chunk); });
  worker.stderr?.on('data', chunk => { output += String(chunk); });
  const checkpoint = new Promise<PauseCheckpoint>((resolve, reject) => {
    worker.once('message', message => resolve(message as PauseCheckpoint));
    worker.once('error', reject);
    worker.once('exit', (code, signal) => reject(new Error(`PAUSE_WORKER_EARLY_EXIT ${code}/${signal}\n${output}`)));
  });
  const abort = () => { worker.kill('SIGKILL'); };
  context.signal.addEventListener('abort', abort, { once: true });
  try {
    const saved = await checkpoint;
    const exited = once(worker, 'exit');
    worker.kill('SIGKILL');
    assert.deepEqual(await exited, [null, 'SIGKILL']);
    return saved;
  } finally { context.signal.removeEventListener('abort', abort); worker.kill('SIGKILL'); }
}

if (process.argv[2] !== '--pause-checkpoint') {
test('真实并行工具的两个业务等待各自完成，最后一个结果才驱动原 Goal，一批结果只消费一次', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-two-waits-'));
  const continuations = new Map<string, Continuation>();
  let started = 0, finished = 0, calls = 0;
  const begin = defineAction({ name: 'begin_wait', description: '登记各业务 owner 的等待', scope: z.null(),
    input: z.object({ branch: z.enum(['first', 'second']) }), output: z.boolean(), concurrency: 'parallel' });
  const beginImplementation = implementAction(begin, {
    async execute(input, call) {
      assert.ok(call.continuation?.goal);
      continuations.set(input.branch, call.continuation);
      started++;
      await call.wait(waits[input.branch]);
      return true;
    }
  });
  const finish = defineAction({ name: 'finish', description: '所有条件完成后核验并结束', scope: z.null(),
    input: z.object({}).strict(), output: z.boolean() });
  const finishImplementation = implementAction(finish, {
    async execute(_input, call) { finished++; call.completeGoal(); return true; }
  });
  const definition = defineApplication({ name: 'two-waits', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '两个业务条件都完成才继续',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component() {
      const beginWait = useAction(beginImplementation, () => null, []);
      const complete = useAction(finishImplementation, () => null, []);
      return createElement(Fragment, null, Offer({ name: 'begin_wait', action: beginWait }), Offer({ name: 'finish', action: complete }));
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* (options) {
    calls++;
    if (calls === 1) yield* parallelWaits();
    else if (calls === 2) {
      const received = JSON.stringify(options.messages);
      assert.match(received, /正式完成 first-owner/u);
      assert.match(received, /正式完成 second-owner/u);
      yield* tool('finish', 'finish-both', {});
    } else yield* text('两个正式结果已经核验');
  });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'original', content: '持续完成两个业务条件', intent: 'goal', delivery: 'followup' as const, scope: null });
    await session.whenIdle();
    assert.equal(started, 2); assert.equal(calls, 1);
    assert.equal(session.snapshot().waits.length, 2);
    assert.deepEqual(continuations.get('first')?.goal, continuations.get('second')?.goal);
    const first = result('first-result', continuations.get('first')!, waits.first);
    const second = result('second-result', continuations.get('second')!, waits.second);
    await session.submit(first);
    await session.whenIdle();
    assert.deepEqual(session.snapshot().waits.map(item => item.wait.id), ['second-owner']);
    assert.equal(session.snapshot().requests['first-result']?.state, 'accepted');
    assert.equal(calls, 1);
    await session.submit(second);
    await session.whenIdle();
    assert.equal(calls, 3); assert.equal(started, 2); assert.equal(finished, 1);
    assert.equal(session.snapshot().waits.length, 0);
    assert.equal(session.snapshot().goal?.phase, 'complete');
    assert.equal(session.snapshot().requests.original?.reply, '两个正式结果已经核验');
    const agent = app.native.agents.get(SessionId(session.id))!;
    const entered = agent.session.snapshotEvents().flatMap(event => event.type === 'user/message'
      && 'rpcId' in event.data.source ? [event.data.source.rpcId] : []);
    assert.equal(entered.filter(id => id === 'first-result').length, 1);
    assert.equal(entered.filter(id => id === 'second-result').length, 1);
    await Promise.all([session.submit(first), session.submit(second)]);
    await session.whenIdle();
    assert.equal(calls, 3); assert.equal(started, 2); assert.equal(finished, 1);
    assert.deepEqual(app.errors, []);
  } finally { await app.dispose(); }
});

test('唯一等待的结果先到而并行工具随后登记第二等待，原消息被保留且恢复器不能越过新条件', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-late-wait-'));
  const firstRegistered = Promise.withResolvers<void>();
  const secondRunning = Promise.withResolvers<void>();
  const allowSecond = Promise.withResolvers<void>();
  const continuations = new Map<string, Continuation>();
  let calls = 0, executions = 0;
  const begin = defineAction({ name: 'begin_wait', description: '并行登记有真实时序差的业务等待', scope: z.null(),
    input: z.object({ branch: z.enum(['first', 'second']) }), output: z.boolean(), concurrency: 'parallel' });
  const beginImplementation = implementAction(begin, {
    async execute(input, call) {
      assert.ok(call.continuation?.goal);
      continuations.set(input.branch, call.continuation);
      executions++;
      if (input.branch === 'second') { secondRunning.resolve(); await allowSecond.promise; }
      await call.wait(waits[input.branch]);
      if (input.branch === 'first') firstRegistered.resolve();
      return true;
    }
  });
  const finish = defineAction({ name: 'finish', description: '核验两个结果并结束原 Goal', scope: z.null(),
    input: z.object({}).strict(), output: z.boolean() });
  const finishImplementation = implementAction(finish, {
    execute(_input, call) { call.completeGoal(); return true; }
  });
  const definition = defineApplication({ name: 'late-wait', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '等待所有已经登记的条件',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component() {
      const beginning = useAction(beginImplementation, () => null, []), finishing = useAction(finishImplementation, () => null, []);
      return createElement(Fragment, null, Offer({ name: 'begin_wait', action: beginning }), Offer({ name: 'finish', action: finishing }));
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () {
    calls++;
    if (calls === 1) yield* parallelWaits();
    else if (calls === 2) yield* tool('finish', 'finish-after-late-wait', {});
    else yield* text('迟登记的条件也已经完成');
  });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'original', content: '持续完成并行工作', intent: 'goal', delivery: 'followup' as const, scope: null });
    await Promise.all([firstRegistered.promise, secondRunning.promise]);
    const first = result('early-result', continuations.get('first')!, waits.first);
    await session.submit(first);
    assert.equal(session.snapshot().waits.length, 0);
    const agent = app.native.agents.get(SessionId(session.id))!;
    const messageId = pending(agent, 'early-result')!.id;
    allowSecond.resolve();
    await session.whenIdle();
    assert.equal(calls, 1); assert.equal(executions, 2);
    assert.deepEqual(session.snapshot().waits.map(item => item.wait.id), ['second-owner']);
    assert.equal(pending(agent, 'early-result')?.id, messageId);
    assert.equal(session.snapshot().requests['early-result']?.state, 'accepted');
    const turns = agent.session.snapshotEvents().filter(event => event.type === 'turn/start').length;
    await session.submit(first); await session.whenIdle();
    assert.equal(agent.session.snapshotEvents().filter(event => event.type === 'turn/start').length, turns);
    await session.submit(result('last-result', continuations.get('second')!, waits.second));
    await session.whenIdle();
    assert.equal(calls, 3); assert.equal(executions, 2);
    assert.equal(session.snapshot().goal?.phase, 'complete');
    assert.equal(session.snapshot().waits.length, 0);
    assert.deepEqual(app.errors, []);
  } finally { allowSecond.resolve(); await app.dispose(); }
});

for (const boundary of ['assemble', 'request'] as const) for (const order of ['pause-cancel', 'cancel-pause'] as const) {
  test(`结果已唤醒后在 ${boundary} 边界执行 ${order}，完成事实落盘且显式取消后不回生`, async () => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-pause-wait-'));
    const scenario = await pauseScenario(root, boundary, order);
    const { fixture, completed, messageId, agent } = scenario;
    let { app, session, release } = scenario;
    try {
      assert.equal(fixture.calls(), 1);
      assert.equal(session.snapshot().goal?.phase, 'paused');
      assert.equal(session.snapshot().requests[completed.requestId]?.state, order === 'pause-cancel' ? 'accepted' : 'settled');
      if (order === 'pause-cancel') assert.equal(pending(agent, completed.requestId)?.id, messageId);
      else {
        assert.equal(pending(agent, completed.requestId), undefined);
        assert.equal(session.snapshot().requests[completed.requestId]?.outcome, 'aborted');
      }
      await session.submit(completed); await session.whenIdle();
      assert.equal(fixture.calls(), 1);
      if (order === 'pause-cancel') await session.cancel();
      assert.equal(pending(agent, completed.requestId), undefined);
      assert.equal(session.snapshot().requests[completed.requestId]?.outcome, order === 'pause-cancel' ? 'cancelled' : 'aborted');
      assert.deepEqual(app.errors, []);
      const id = session.id;
      release(); release = () => {};
      await app.host.close(id); session.release(); await app.dispose();
      app = await boot(join(root, 'home'), fixture.definition, fixture.respond);
      const cold = await app.host.open(id);
      assert.equal(cold.snapshot().waits.length, 0);
      assert.equal(cold.snapshot().goal?.phase, 'paused');
      assert.equal(cold.snapshot().requests[completed.requestId]?.state, 'settled');
      assert.equal(app.native.agents.get(SessionId(id)), undefined);
      session = await app.host.resume(id, fixture.definition.environments[0].program.definition);
      await session.whenIdle();
      const restored = app.native.agents.get(SessionId(id))!;
      assert.equal(pending(restored, completed.requestId), undefined);
      assert.equal(pending(restored, completed.requestId), undefined);
      assert.equal(session.snapshot().requests[completed.requestId]?.outcome, order === 'pause-cancel' ? 'cancelled' : 'aborted');
      await session.submit(completed); await session.whenIdle();
      assert.equal(pending(restored, completed.requestId), undefined);
      assert.equal(fixture.calls(), 1);
      assert.deepEqual(app.errors, []);
      cold.release(); session.release();
    } finally { release(); fixture.proceed.resolve(); await app.dispose(); }
  });
}

test('checkpoint 后真实进程崩溃保留原生同 ID 结果，冷恢复后正常 close 以原生取消结算', async context => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-pause-crash-'));
  const checkpoint = await crashAfterPause(root, context);
  const { id, completed, messageId } = checkpoint;
  assert.equal(checkpoint.calls, 1);
  assert.equal(checkpoint.pendingId, messageId);
  assert.equal(checkpoint.snapshot.waits.length, 0);
  assert.equal(checkpoint.snapshot.goal?.phase, 'paused');
  assert.equal(checkpoint.snapshot.requests[completed.requestId]?.state, 'accepted');
  assert.deepEqual(checkpoint.errors, []);
  const fixture = pauseApplication(root, 'request', checkpoint.calls);
  const app = await boot(join(root, 'home'), fixture.definition, fixture.respond);
  try {
    const cold = await app.host.open(id);
    assert.equal(app.native.agents.get(SessionId(id)), undefined);
    assert.equal(cold.snapshot().waits.length, 0);
    assert.equal(cold.snapshot().goal?.phase, 'paused');
    assert.equal(cold.snapshot().requests[completed.requestId]?.state, 'accepted');
    const session = await app.host.resume(id);
    const agent = app.native.agents.get(SessionId(id))!;
    await session.whenIdle();
    assert.equal(pending(agent, completed.requestId)?.id, messageId);
    assert.equal(session.snapshot().requests[completed.requestId]?.state, 'accepted');
    assert.equal(fixture.calls(), 1);
    await app.host.close(id);
    assert.equal(app.native.agents.get(SessionId(id)), undefined);
    assert.equal(session.snapshot().requests[completed.requestId]?.state, 'settled');
    assert.equal(session.snapshot().requests[completed.requestId]?.outcome, 'cancelled');
    await session.submit(completed); await session.whenIdle();
    const resumed = app.native.agents.get(SessionId(id))!;
    assert.equal(pending(resumed, completed.requestId), undefined);
    assert.equal(fixture.calls(), 1);
    assert.deepEqual(app.errors, []);
    cold.release(); session.release();
  } finally { await app.dispose(); }
});

test('人的新 Goal revision 不受旧等待阻断，旧完成结果由新输入带入时只作事实而不授予 Goal 权限', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-wait-revision-'));
  const newRevisionEntered = Promise.withResolvers<void>();
  let continuation: Continuation | undefined;
  let calls = 0, executions = 0;
  const begin = defineAction({ name: 'begin_owner', description: '保留旧业务条件', scope: z.null(), input: z.object({}).strict(), output: z.boolean() });
  const beginImplementation = implementAction(begin, {
    async execute(_input, call) {
      assert.ok(call.continuation?.goal);
      continuation = call.continuation;
      executions++; await call.wait(waits.first); return true;
    }
  });
  const finish = defineAction({ name: 'finish', description: '按人的新授权完成目标', scope: z.null(), input: z.object({}).strict(), output: z.boolean() });
  const finishImplementation = implementAction(finish, {
    execute(_input, call) { call.completeGoal(); return true; }
  });
  const definition = defineApplication({ name: 'wait-revision', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '区分旧业务等待与人的新授权',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component() {
      const beginning = useAction(beginImplementation, () => null, []), finishing = useAction(finishImplementation, () => null, []);
      return createElement(Fragment, null, Offer({ name: 'begin_owner', action: beginning }), Offer({ name: 'finish', action: finishing }));
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* (options) {
    calls++;
    if (calls === 1) yield* tool('begin_owner', 'old-owner', {});
    else if (calls === 2) { newRevisionEntered.resolve(); yield* tool('finish', 'new-authority', {}); }
    else if (calls === 3) yield* text('人的新授权已经完成');
    else {
      assert.match(JSON.stringify(options.messages), /正式完成 first-owner/u);
      assert.match(JSON.stringify(options.messages), /处理独立的新问题/u);
      yield* text('旧结果作为事实进入了新的问题');
    }
  });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'original', content: '开始原目标', intent: 'goal', delivery: 'followup' as const, scope: null });
    await session.whenIdle();
    const agent = app.native.agents.get(SessionId(session.id))!;
    await app.native.inject(['goals'], native => {
      const goal = native.goals.get(agent)!;
      const resumed = native.goals.resume(agent, goal);
      assert.notEqual(resumed.revision, continuation!.goal!.revision);
    });
    await newRevisionEntered.promise;
    await session.whenIdle();
    assert.equal(calls, 3);
    assert.equal(session.snapshot().goal?.phase, 'complete');
    assert.equal(session.snapshot().waits.length, 1);
    const completedGoal = session.snapshot().goal;
    const completed = result('old-result', continuation!, waits.first);
    await session.submit(completed); await session.whenIdle();
    assert.equal(calls, 3); assert.equal(session.snapshot().waits.length, 0);
    assert.equal(session.snapshot().requests['old-result']?.state, 'accepted');
    assert.deepEqual(session.snapshot().goal, completedGoal);
    await session.submit({ requestId: 'new-question', content: '处理独立的新问题', intent: 'question', delivery: 'followup' as const, scope: null });
    await session.whenIdle();
    assert.equal(calls, 4); assert.equal(executions, 1);
    assert.equal(session.snapshot().requests['old-result']?.state, 'settled');
    assert.equal(session.snapshot().requests['new-question']?.reply, '旧结果作为事实进入了新的问题');
    assert.deepEqual(session.snapshot().goal, completedGoal);
    assert.deepEqual(app.errors, []);
  } finally { await app.dispose(); }
});

test('完成事实已持久而尚未进入原生 inbox，重启后的同请求重试接续受理而不重做业务', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-wait-admission-'));
  let continuation: Continuation | undefined;
  let calls = 0, executions = 0;
  const begin = defineAction({ name: 'begin_owner', description: '登记真实业务等待', scope: z.null(),
    input: z.object({}).strict(), output: z.boolean() });
  const beginImplementation = implementAction(begin, {
    async execute(_input, call) {
      assert.ok(call.continuation);
      continuation = call.continuation;
      executions++; await call.wait(waits.first); return true;
    }
  });
  const definition = defineApplication({ name: 'wait-admission', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '正式结果沿原请求受理',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component() {
      const beginning = useAction(beginImplementation, () => null, []);
      return Offer({ name: 'begin_owner', action: beginning });
    } }),
  })] });
  const respond = async function* () {
    calls++;
    if (calls === 1) yield* tool('begin_owner', 'owner-before-interruption', {});
    else yield* text('持久完成事实已受理');
  };
  let app = await boot(join(root, 'home'), definition, respond);
  try {
    let session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'original', content: '处理业务操作', intent: 'question', delivery: 'followup' as const, scope: null });
    await session.whenIdle();
    const completed = result('interrupted-result', continuation!, waits.first);
    const normalized = submissionSchema.parse(completed);
    const admitted = admittedSubmissionSchema.parse(normalized);
    const agent = app.native.agents.get(SessionId(session.id))!;
    // 精确停在宿主已写关联并 flush、尚未 native.send 的持久边界。
    appendPluginRecord(agent.session, 'plugin:reactive/input', { ...admitted, actorId: 'operator-test', participant: 'human',
      sourceDigest: submissionSourceDigest(normalized, 'operator-test', 'human'), digest: contentDigest(admitted.content) });
    await app.native.inject(['sessions'], async native => assert.equal(await native.sessions.flush(agent.session), true));
    assert.equal(session.snapshot().waits.length, 0);
    assert.equal(session.snapshot().requests[completed.requestId]?.state, 'declared');
    assert.equal(pending(agent, completed.requestId), undefined);
    const id = session.id;
    await app.host.close(id); session.release(); await app.dispose();
    app = await boot(join(root, 'home'), definition, respond);
    const cold = await app.host.open(id);
    assert.equal(cold.snapshot().waits.length, 0);
    assert.equal(cold.snapshot().requests[completed.requestId]?.state, 'declared');
    assert.equal(app.native.agents.get(SessionId(id)), undefined);
    session = await app.host.resume(id, definition.environments[0].program.definition);
    await session.submit(completed); await session.whenIdle();
    assert.equal(calls, 2); assert.equal(executions, 1);
    assert.equal(session.snapshot().requests[completed.requestId]?.reply, '持久完成事实已受理');
    await session.submit(completed); await session.whenIdle();
    assert.equal(calls, 2); assert.equal(executions, 1);
    assert.deepEqual(app.errors, []);
    cold.release(); session.release();
  } finally { await app.dispose(); }
});
} else {
  const scenario = await pauseScenario(process.argv[3]!, 'request', 'pause-cancel');
  const { app, fixture, session, agent, completed, messageId } = scenario;
  await app.native.inject(['sessions'], async native => assert.equal(await native.sessions.flush(agent.session), true));
  const checkpoint: PauseCheckpoint = { id: session.id, completed, messageId,
    pendingId: pending(agent, completed.requestId)?.id, snapshot: session.snapshot(), calls: fixture.calls(),
    errors: app.errors.map(error => error instanceof Error ? error.stack ?? error.message : String(error)) };
  process.on('message', () => {});
  process.send!(checkpoint);
  // 父进程以 SIGKILL 验证最后一个真实 checkpoint，不走 owned handle 的正常 dispose。
  await new Promise<never>(() => {});
}
