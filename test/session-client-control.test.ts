import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { foldGoal } from '@deepseek-ai/dsh-goal';
import { SessionId, SessionLogOffset } from '@deepseek-ai/dsh-session';
import { defineApplication, defineEnvironment, defineProgram, defineSurface, z, type HumanProgramSessionClient } from '@hanger-source/dsh-reactive';
import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { boot } from './native-support.ts';

const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
function definition() {
  return defineApplication({ name: 'session-client-control', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '控制与只读查询均不启动模型。',
    program: implementProgram(program, { Component: () => null }),
  })] });
}

for (const control of ['pause', 'cancel'] as const) for (const cancellation of ['call', 'handle'] as const) {
  test(`${control}已进入原生checkpoint后${cancellation}取消只结束回执等待，实际控制继续持久化`, { timeout: 15_000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-session-control-'));
    let models = 0;
    const app = await boot(join(directory, 'home'), definition(), async function* () {
      models++; throw new Error('SESSION_CONTROL_MUST_NOT_CALL_MODEL');
    });
    const entered = Promise.withResolvers<void>(), proceed = Promise.withResolvers<void>();
    const completed = Promise.withResolvers<void>();
    let stop = () => {};
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const peer = await app.host.open(session.id, program);
    const common: HumanProgramSessionClient<typeof program> = session;
    try {
      const agent = app.native.agents.get(SessionId(session.id))!;
      assert.ok(agent);
      if (control === 'cancel') {
        await common.submit({ requestId: 'queued', intent: 'question', delivery: 'inject', content: '保持在队列中', scope: null });
        assert.deepEqual(await common.requestStatus('queued'), { requestId: 'queued', state: 'accepted' });
      }
      let armed = true;
      stop = app.native.on('session/flush', async native => {
        if (!armed || native !== agent.session || control === 'pause' && app.native.goals.get(agent)?.phase !== 'paused') return;
        armed = false;
        entered.resolve();
        await proceed.promise;
        completed.resolve();
      });
      if (control === 'pause') app.native.goals.create(agent, { objective: '由本次控制暂停', maxGoalRounds: 1 });
      const abort = new AbortController(), reason = new Error('CALLER_STOPPED_WAITING_FOR_' + control.toUpperCase());
      const pending = common[control]({ signal: abort.signal });
      const rejected = assert.rejects(pending, cancellation === 'call'
        ? error => error === reason
        : /REACTIVE_SCOPE_DISPOSED/u);
      await entered.promise;
      if (control === 'pause') assert.equal(app.native.goals.get(agent)?.phase, 'paused');
      else assert.equal(agent.inbox.nextStep.length, 0);
      if (cancellation === 'call') abort.abort(reason);
      else session.release();
      await rejected;
      proceed.resolve();
      await completed.promise;
      assert.equal(await app.native.sessions.flush(agent.session), true);
      const stored = await app.native.sessionPersistence.open(SessionId(session.id), 'read');
      try {
        const { events } = await stored.read(SessionLogOffset(0));
        if (control === 'pause') assert.equal(foldGoal(events).goal?.phase, 'paused');
        else {
          assert.ok(events.some(event => event.type === 'agent/inbox/spliced' && event.data.outcome === 'canceled'),
            '已取消调用者等待后，原生队列取消事实仍到达真实持久日志');
          assert.deepEqual(await peer.requestStatus('queued'), { requestId: 'queued', state: 'settled' });
        }
      } finally { await stored.close(); }
      assert.equal(models, 0);
      assert.deepEqual(app.errors, []);
    } finally { proceed.resolve(); stop(); peer.release(); session.release(); await app.dispose(); }
  });
}

test('预取消不进入Goal或队列控制，服务身份可读权威请求但不能取得人工控制', { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-session-control-admission-'));
  let models = 0;
  const app = await boot(join(directory, 'home'), definition(), async function* () {
    models++; throw new Error('SESSION_CONTROL_ADMISSION_MUST_NOT_CALL_MODEL');
  });
  const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
  const service = await app.host.service().open(session.id, program);
  try {
    const common: HumanProgramSessionClient<typeof program> = session;
    const agent = app.native.agents.get(SessionId(session.id))!;
    await common.submit({ requestId: 'preserved', intent: 'question', delivery: 'inject', content: '显式取消前保留', scope: null });
    const abort = new AbortController(), reason = new Error('CONTROL_CANCELLED_BEFORE_ADMISSION');
    abort.abort(reason);
    const goal = app.native.goals.create(agent, { objective: '预取消不得暂停此Goal', maxGoalRounds: 1 });
    const paused = common.pause({ signal: abort.signal });
    app.native.goals.disarm(agent);
    await assert.rejects(paused, error => error === reason);
    await assert.rejects(common.cancel({ signal: abort.signal }), error => error === reason);
    await assert.rejects(common.requestStatus('preserved', { signal: abort.signal }), error => error === reason);
    assert.equal(app.native.goals.get(agent)?.phase, 'active');
    assert.equal(app.native.goals.get(agent)?.revision, goal.revision);
    assert.ok(agent.inbox.nextStep.some(message => 'rpcId' in message.source && message.source.rpcId === 'preserved'));
    assert.deepEqual(await service.requestStatus('preserved'), { requestId: 'preserved', state: 'accepted' });
    const untyped = service as unknown as typeof session;
    await assert.rejects(untyped.pause(), /REACTIVE_HUMAN_PARTICIPANT_REQUIRED/u);
    await assert.rejects(untyped.cancel(), /REACTIVE_HUMAN_PARTICIPANT_REQUIRED/u);
    assert.deepEqual(await common.cancel(), { canceled: true });
    assert.deepEqual(await common.requestStatus('preserved'), { requestId: 'preserved', state: 'settled' });
    await app.host.close(session.id);
    assert.equal(app.native.agents.get(SessionId(session.id)), undefined);
    assert.deepEqual(await service.requestStatus('preserved'), { requestId: 'preserved', state: 'settled' });
    assert.deepEqual(await common.requestStatus('not-submitted'), { requestId: 'not-submitted', state: 'absent' });
    assert.equal(app.native.agents.get(SessionId(session.id)), undefined, '权威冷查询不得提升为原生Agent');
    assert.equal(models, 0);
    assert.deepEqual(app.errors, []);
  } finally { service.release(); session.release(); await app.dispose(); }
});

test('权威请求查询的调用者或句柄退出结束等待，不影响同会话其他只读消费者', { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-session-status-owner-'));
  let models = 0;
  const app = await boot(join(directory, 'home'), definition(), async function* () {
    models++; throw new Error('SESSION_STATUS_MUST_NOT_CALL_MODEL');
  });
  const peer = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
  try {
    await peer.submit({ requestId: 'visible', intent: 'question', delivery: 'inject', content: '仅观察原受理事实', scope: null });
    const agent = app.native.agents.get(SessionId(peer.id))!;
    for (const cancellation of ['call', 'handle'] as const) {
      const session = await app.host.open(peer.id, program);
      const entered = Promise.withResolvers<void>(), proceed = Promise.withResolvers<void>();
      let armed = true;
      const stop = app.native.on('session/flush', async native => {
        if (!armed || native !== agent.session) return;
        armed = false; entered.resolve(); await proceed.promise;
      });
      try {
        const abort = new AbortController(), reason = new Error('REQUEST_STATUS_WAIT_CANCELLED');
        const pending = session.requestStatus('visible', { signal: abort.signal });
        const rejected = assert.rejects(pending, cancellation === 'call'
          ? error => error === reason
          : /REACTIVE_SCOPE_DISPOSED/u);
        await entered.promise;
        if (cancellation === 'call') abort.abort(reason);
        else session.release();
        await rejected;
      } finally { proceed.resolve(); stop(); session.release(); }
      assert.deepEqual(await peer.requestStatus('visible'), { requestId: 'visible', state: 'accepted' });
    }
    assert.equal(models, 0);
    assert.deepEqual(app.errors, []);
  } finally { peer.release(); await app.dispose(); }
});
