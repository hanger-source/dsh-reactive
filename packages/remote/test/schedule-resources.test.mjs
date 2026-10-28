import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate as immediate } from 'node:timers/promises';
import { filter, firstValueFrom, timeout } from 'rxjs';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import ScheduleService from '@deepseek-ai/dsh-schedule';
import { SessionId } from '@deepseek-ai/dsh-session';
import { defineApplication, defineEnvironment, defineProgram, defineSurface, mapResource, resourceView, z } from '@hanger-source/dsh-reactive';
import { Expose, implementProgram, useResource } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel, requireActivePlugin } from '@hanger-source/dsh-reactive-dsh';
import { createNativeSchedule } from '@hanger-source/dsh-reactive-dsh/schedule';
import { mountProgramGateway, RemoteAuthenticationError } from '@hanger-source/dsh-reactive-remote';

const first = (source, predicate) => firstValueFrom(source.pipe(filter(predicate), timeout(8_000)));

test('Schedule真实CAS和持久变更驱动资源失效，迟到切面不回写，读取失败后通知恢复且冷读不投递', { timeout: 25_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-schedule-resources-'));
  const directory = join(root, 'workspace'); await mkdir(directory);
  const capability = createNativeSchedule({ service: { kind: 'install' } });
  const surface = defineSurface({ actions: {}, views: { reminders: resourceView(z.array(z.object({ id: z.string(), title: z.string(), status: z.string() }))) } });
  const program = defineProgram({ input: z.null(), surface });
  let models = 0;
  const errors = [], subscriptions = [], entered = Promise.withResolvers(), release = Promise.withResolvers();
  const definition = defineApplication({ name: 'schedule-resources', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '只观察和管理提醒，不启动模型', program: implementProgram(program, { Component({ conversation }) {
      const observed = useResource(capability.forSession(conversation).state);
      return Expose({ surface, value: { views: { reminders: observed.observations$.pipe(mapResource(rows => rows.map(({ id, title, status }) => ({ id, title, status })))) } } });
    } }),
  })] });
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'schedule-owner', onError: event => errors.push(event.error),
    model: nativeModel({ provider: 'schedule-resources', model: 'unused', create: () => new class extends LlmAdapter {
      async *stream() { models++; throw new Error('SCHEDULE_READING_MUST_NOT_START_MODEL'); }
    }() }),
  });
  let gateway, session, foreign, sdk;
  try {
    // 正式Controller/Gateway提供Schedule公开依赖；不构造同名空服务或调用私有安装接点。
    gateway = await mountProgramGateway(runtime.native, { application: runtime.application,
      model: { provider: 'schedule-resources', model: 'unused' }, participant: principal => ({ participant: 'human', actorId: principal.id }),
      access: () => {}, carrier: { authenticate: async () => { throw new RemoteAuthenticationError(401); } },
    });
    await capability.install(runtime.native, join(root, 'home'));
    assert.ok(runtime.native.schedule instanceof ScheduleService);
    const workspace = await runtime.application.createWorkspace({ directory });
    session = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    foreign = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    sdk = capability.forSession(session);
    assert.equal(sdk.state.peek().status, 'pending', '构造SDK不读取');
    const catalog = sdk.catalog.bind(sdk), failure = new Error('SCHEDULE_CATALOG_READ_FAILED');
    let held = false, failNext = false, calls = 0;
    sdk.catalog = async options => {
      const rows = await catalog(options); calls++;
      if (held) { held = false; entered.resolve({ rows, signal: options.signal }); await release.promise; }
      if (failNext) { failNext = false; throw failure; }
      return rows;
    };
    const states = [];
    subscriptions.push(sdk.state.observations$.subscribe(value => states.push(value)));
    await sdk.state.read(t.signal);
    // 测试准备只经正式原生create写入一个小时后的记录；框架管理API并未获得创建/投递授权。
    const original = await runtime.native.schedule.create(SessionId(session.id), { title: '原提醒', prompt: '未来提醒，不在本次试验执行', after_seconds: 3600 });
    await first(sdk.state.observations$, value => value.status === 'ready' && value.value.some(row => row.id === original.id));
    const task = await sdk.task(original.id), before = calls;
    assert.equal(await sdk.task(original.id), task);
    subscriptions.push(task.state.observations$.subscribe());
    await task.state.read(t.signal);
    assert.equal(calls, before + 1, '绑定核验读一次，任务资源复用会话观察而不再查询catalog');
    await assert.rejects(capability.forSession(foreign).task(original.id), /REACTIVE_NATIVE_SCHEDULE_NOT_FOUND/);

    held = true;
    const changed = await task.update({ expected: original, title: '中间提醒' }); assert.equal(changed.updated, true);
    const old = await entered.promise;
    assert.equal(sdk.state.peek().status, 'pending'); assert.equal(task.state.peek().status, 'pending');
    const newer = first(task.state.observations$, value => value.status === 'ready' && value.value?.title === '当前提醒');
    const current = await task.update({ expected: changed.record, title: '当前提醒' }); assert.equal(current.updated, true);
    await newer; assert.equal(old.signal.aborted, true);
    release.resolve(); await immediate();
    assert.equal(task.state.peek().value.title, '当前提醒');
    assert.equal((await session.readView('reminders')).find(row => row.id === task.id).title, '当前提醒');

    failNext = true;
    const failed = first(task.state.observations$, value => value.status === 'error');
    const updated = await task.update({ expected: current.record, title: '恢复目标' }); assert.equal(updated.updated, true);
    assert.equal((await failed).error, failure);
    const recovered = first(task.state.observations$, value => value.status === 'ready' && value.value?.title === '已恢复');
    await task.update({ expected: updated.record, title: '已恢复' }); await recovered;
    assert.ok(states.some(value => value.status === 'error' && value.error === failure));
    assert.equal((await task.history({ limit: 10 })).records.length, 0, '观察不会生成投递回执');
    await runtime.application.close(session.id);
    assert.equal(runtime.native.agents.get(SessionId(session.id)), undefined);
    assert.equal((await task.read()).title, '已恢复');
    assert.equal((await task.state.read(t.signal)).title, '已恢复');
    const ended = first(task.state.observations$, value => value.status === 'ready' && value.value?.status !== 'active');
    await task.delete(); await ended;
    assert.equal((await sdk.active()).length, 0);
    subscriptions.forEach(subscription => subscription.unsubscribe());
    assert.equal(sdk.state.peek().status, 'pending');
    assert.equal(task.state.peek().status, 'pending');
    await immediate();
    const nativeList = ScheduleService.prototype.list;
    for (const mode of ['caller', 'sdk']) {
      const read = Promise.withResolvers(), finish = Promise.withResolvers();
      ScheduleService.prototype.list = async function (...args) {
        const rows = await nativeList.apply(this, args);
        read.resolve(); await finish.promise; return rows;
      };
      let waiting;
      try {
        const cancellation = new AbortController(), reason = new Error('CANCEL_ONLY_THIS_SCHEDULE_READ');
        let outcome;
        waiting = sdk.active({ signal: cancellation.signal }).then(value => { outcome = value; }, error => { outcome = error; });
        await read.promise;
        if (mode === 'caller') cancellation.abort(reason); else sdk.dispose();
        await immediate();
        if (mode === 'caller') assert.equal(outcome, reason, '调用者取消不等原生list回执');
        else { assert.ok(outcome instanceof Error); assert.match(outcome.message, /REACTIVE_SCOPE_DISPOSED/); }
      } finally { finish.resolve(); await waiting; ScheduleService.prototype.list = nativeList; }
    }
    assert.throws(() => task.state.peek(), /REACTIVE_SCOPE_DISPOSED/);
    assert.equal(models, 0); assert.deepEqual(errors, []);
  } finally {
    release.resolve(); subscriptions.forEach(subscription => subscription.unsubscribe());
    sdk?.dispose(); foreign?.release(); session?.release(); await capability.dispose(); await gateway?.close(); await runtime.dispose();
  }
});
