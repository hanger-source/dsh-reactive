import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate as immediate } from 'node:timers/promises';
import { useLayoutEffect, useMemo } from 'react';
import { ReplaySubject, filter, firstValueFrom, of, timeout, type Subscription } from 'rxjs';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { symbols, type Fiber } from '@deepseek-ai/cordis';
import TeamService from '@deepseek-ai/dsh-experimental-agent-team';
import { SessionId } from '@deepseek-ai/dsh-session';
import { defineApplication, defineEnvironment, defineProgram, defineSurface, view, z, type Observation } from '@hanger-source/dsh-reactive';
import { Expose, implementProgram, useResource } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel, requireActivePlugin, type ConversationHandle } from '@hanger-source/dsh-reactive-dsh';
import { createNativeTeam, TeamError, TeamTaskId, type NativeSessionTeam, type TeamProjection, type TeamTaskView } from '@hanger-source/dsh-reactive-dsh/team';
import { text } from './native-support.ts';

for (const service of ['install', 'provided'] as const) {
test(`${service} Team任务板的CAS、恢复及provider撤销沿同一资源传播，不执行模型或重定向旧能力`, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-native-team-'));
  const directory = join(root, 'workspace'); await mkdir(directory);
  const capability = createNativeTeam({ service: { kind: service } });
  let provider: (Fiber & PromiseLike<Fiber>) | undefined;
  const program = defineProgram({ input: z.null(), surface: defineSurface({ views: { phase: view(z.string()) }, actions: {} }) });
  const programTeams = new Map<string, NativeSessionTeam>();
  const errors: unknown[] = [];
  let modelCalls = 0;
  const definition = defineApplication({ name: 'native-team', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: 'Team管理不产生模型输入', program: implementProgram(program, { Component({ conversation }) {
      const team = useMemo(() => capability.forSession(conversation), [conversation]);
      const observed = useResource(team.state);
      useLayoutEffect(() => { programTeams.set(conversation.identity.sessionId, team); }, [conversation, team]);
      return Expose({ surface: program.surface, value: { views: { phase: of(observed.state.status) } } });
    } }),
  })] });
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'team-owner',
    installNative: async (ctx, home) => {
      if (service === 'provided') { provider = ctx.plugin(TeamService, {}); await requireActivePlugin(provider); }
      await capability.install(ctx, home);
    }, onError: event => errors.push(event.error),
    model: nativeModel({ provider: 'native-team', model: 'unused', create: () => new class extends LlmAdapter {
      stream() { modelCalls++; return text('任务板读写不需要模型'); }
    }() }),
  });
  let session: ConversationHandle<typeof program> | undefined, resumed: ConversationHandle<typeof program> | undefined;
  let other: ConversationHandle<typeof program> | undefined, subscription: Subscription | undefined;
  const release = Promise.withResolvers<void>();
  try {
    const workspace = await runtime.application.createWorkspace({ directory });
    session = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    const team = capability.forSession(session);
    const inspect = team.readProjection.bind(team);
    const entered = Promise.withResolvers<{ signal: AbortSignal; projection: TeamProjection }>();
    let first = true;
    // 只延后公开读取取得的真实切面，不替换Agent、Team journal或持久状态。
    team.readProjection = async options => {
      const projection = await inspect(options);
      if (first) {
        first = false; assert.ok(options?.signal);
        entered.resolve({ signal: options.signal, projection }); await release.promise;
      }
      return projection;
    };
    const observed = new ReplaySubject<TeamProjection>(1), delivered: TeamProjection[] = [];
    const states: Observation<TeamProjection>[] = [];
    assert.equal(team.projection$, team.state.values$);
    subscription = team.state.observations$.subscribe({
      next: observation => {
        states.push(observation);
        if (observation.status === 'ready') { delivered.push(observation.value); observed.next(observation.value); }
      },
      error: error => { errors.push(error); observed.error(error); },
    });
    const old = await entered.promise;
    await runtime.application.close(session.id);
    assert.equal(old.signal.aborted, true, '关闭原生owner须撤销SDK在途投影读取');
    assert.equal(team.state.peek().status, 'pending', '切换真实owner先撤下旧有效性');
    resumed = await runtime.application.resume(session.id, program);
    await firstValueFrom(observed.pipe(timeout(5_000)));
    release.resolve(); await immediate();
    assert.equal(delivered.includes(old.projection), false, '旧切面迟到不能覆盖恢复后的Team现场');
    team.readProjection = inspect;
    const absent = team.task(TeamTaskId('999999'));
    assert.equal(team.task(absent.id), absent);
    assert.equal(await absent.read(), undefined);
    assert.equal(await absent.state.read(), undefined);
    await assert.rejects(absent.update({ expectedRevision: 1, action: 'claim' }),
      error => error instanceof TeamError && error.code === 'TEAM_TASK_NOT_FOUND');

    other = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    const foreign = capability.forSession(other);
    const original = await team.createTask({ subject: '准备知识', description: '按原生任务板完成前置资料' });
    const waiting = await team.createTask({ subject: '开始接待', description: '资料完成后才能认领', blockedBy: [original.id] });
    const independent = await foreign.createTask({ subject: '其他Team', description: '相同局部任务id仍属于另一个Team' });
    assert.equal(independent.id, original.id);
    const task = team.task(original.id), dependent = team.task(waiting.id);
    assert.equal(team.task(original.id), task);
    assert.equal((await dependent.state.read())?.ready, false);
    const foreignDependent = foreign.task(waiting.id);
    assert.equal(await foreignDependent.read(), undefined, '相同任务目标不能借入其他Team的记录');
    await assert.rejects(foreignDependent.update({ expectedRevision: waiting.revision, action: 'claim' }),
      error => error instanceof TeamError && error.code === 'TEAM_TASK_NOT_FOUND');

    const mutationHandle = await runtime.application.open(session.id, program);
    const writer = capability.forSession(mutationHandle);
    const lead = runtime.native.agents.get(SessionId(session.id))!;
    const sessions = Reflect.get(runtime.native.sessions, symbols.original) as typeof runtime.native.sessions;
    const flush = sessions.flush;
    const createdAtFlush = Promise.withResolvers<TeamTaskView['id']>(), updatedAtFlush = Promise.withResolvers<void>();
    const releaseCreate = Promise.withResolvers<void>(), releaseUpdate = Promise.withResolvers<void>();
    const createFlushed = Promise.withResolvers<void>(), updateFlushed = Promise.withResolvers<void>();
    let holding: 'create' | 'update' | undefined = 'create';
    let createOutcome: unknown, updateOutcome: unknown;
    let creating: Promise<void> | undefined, updating: Promise<void> | undefined;
    // Team仍执行真实append/CAS/flush；只延后同一个公开flush的回执，不替换事务或持久事实。
    sessions.flush = async function (...args) {
      const result = await flush.apply(this, args);
      if (args[0] !== lead.session) return result;
      const event = lead.session.snapshotEvents().findLast(event => event.type === 'team/task');
      if (holding === 'create' && event?.data.task.subject === '取消等待的正式创建') {
        holding = undefined; createdAtFlush.resolve(event.data.task.id);
        await releaseCreate.promise; createFlushed.resolve();
      } else if (holding === 'update' && event?.data.task.subject === '退出等待的正式修订') {
        holding = undefined; updatedAtFlush.resolve();
        await releaseUpdate.promise; updateFlushed.resolve();
      }
      return result;
    };
    try {
      const controller = new AbortController(), reason = new Error('CANCEL_TEAM_CREATE_RECEIPT');
      creating = writer.createTask({ subject: '取消等待的正式创建', description: '回执取消不重建原生事务' }, { signal: controller.signal })
        .then(value => { createOutcome = value; }, error => { createOutcome = error; });
      const createdId = await Promise.race([createdAtFlush.promise, creating.then(() => { throw new Error('TEAM_CREATE_DID_NOT_REACH_FLUSH'); })]);
      controller.abort(reason); await immediate();
      assert.equal(createOutcome, reason, '真实flush仍未归还时，调用取消已经结束本次创建回执等待');
      releaseCreate.resolve(); await createFlushed.promise;
      const committedCreate = team.getTask(createdId);
      assert.equal(committedCreate.revision, 1);
      const createdTask = writer.task(createdId);
      const preCancelled = new AbortController(), preCancelledReason = new Error('TEAM_MUTATION_CANCELLED_BEFORE_ADMISSION');
      preCancelled.abort(preCancelledReason);
      const before = lead.session.snapshotEvents().filter(event => event.type === 'team/task').length;
      await assert.rejects(writer.createTask({ subject: '不得追加的创建', description: '预取消不进入原生队列' }, { signal: preCancelled.signal }),
        error => error === preCancelledReason);
      await assert.rejects(createdTask.update({ expectedRevision: committedCreate.revision, action: 'edit', subject: '不得追加的修订' },
        { signal: preCancelled.signal }), error => error === preCancelledReason);
      assert.equal(lead.session.snapshotEvents().filter(event => event.type === 'team/task').length, before);

      holding = 'update';
      updating = createdTask.update({ expectedRevision: committedCreate.revision, action: 'edit', subject: '退出等待的正式修订' })
        .then(value => { updateOutcome = value; }, error => { updateOutcome = error; });
      await Promise.race([updatedAtFlush.promise, updating.then(() => { throw new Error('TEAM_UPDATE_DID_NOT_REACH_FLUSH'); })]);
      writer.dispose(); await immediate();
      assert.ok(updateOutcome instanceof Error);
      assert.match(updateOutcome.message, /REACTIVE_SCOPE_DISPOSED/u,
        '任务对象复用同一更新入口，SDK退出不等待原生flush回执');
      releaseUpdate.resolve(); await updateFlushed.promise;
      const committedUpdate = team.getTask(createdId);
      assert.equal(committedUpdate.subject, '退出等待的正式修订');
      assert.equal(committedUpdate.revision, committedCreate.revision + 1);
      await assert.rejects(team.updateTask({ taskId: createdId, expectedRevision: committedCreate.revision, action: 'edit', subject: '过时重试' }),
        error => error instanceof TeamError && error.code === 'TEAM_TASK_STALE_REVISION');
      assert.deepEqual(team.getTask(createdId), committedUpdate);
      await runtime.native.sessionPersistence.flush();
      const stored = await runtime.native.sessionPersistence.open(lead.id, 'read');
      try {
        const durable = await stored.read();
        const mutations = durable.events.flatMap(event => event.type === 'team/task' && event.data.task.id === createdId
          ? [{ revision: event.data.task.revision, subject: event.data.task.subject }] : []);
        assert.deepEqual(mutations, [
          { revision: 1, subject: '取消等待的正式创建' },
          { revision: 2, subject: '退出等待的正式修订' },
        ], '取消、SDK退出和旧CAS重试都不丢失或重复正式持久修订');
      } finally { await stored.close(); }
    } finally {
      releaseCreate.resolve(); releaseUpdate.resolve();
      sessions.flush = flush;
      await Promise.allSettled([creating, updating]);
      writer.dispose(); mutationHandle.release();
    }

    const failure = new Error('TEAM_PROJECTION_READ_FAILED');
    team.readProjection = async options => { await inspect(options); throw failure; };
    const failed = firstValueFrom(team.state.observations$.pipe(filter(value => value.status === 'error'), timeout(5_000)));
    const extra = await team.createTask({ subject: '读取恢复', description: '真实任务提交产生后续投影通知' });
    assert.equal((await failed).status, 'error');
    assert.equal(team.state.peek().status, 'error');
    team.readProjection = inspect;
    const recovered = firstValueFrom(team.state.observations$.pipe(filter(value => value.status === 'ready'), timeout(5_000)));
    await team.updateTask({ taskId: extra.id, expectedRevision: extra.revision, action: 'edit', subject: '读取已恢复' });
    await recovered;
    assert.ok(states.some(value => value.status === 'error' && value.error === failure));
    assert.equal((await dependent.read())?.ready, false);
    await assert.rejects(dependent.update({ expectedRevision: waiting.revision, action: 'claim' }),
      error => error instanceof TeamError && error.code === 'TEAM_TASK_BLOCKED');
    const retarget = { expectedRevision: original.revision, action: 'edit' as const, subject: '不能转写其他任务', taskId: waiting.id };
    await assert.rejects(task.update(retarget), /REACTIVE_NATIVE_TEAM_TASK_SOURCE_RESERVED/);

    const ready = firstValueFrom(dependent.state$.pipe(filter(value => value?.ready === true), timeout(5_000)));
    const claimed = await task.update({ expectedRevision: original.revision, action: 'claim' });
    const completed = await task.update({ expectedRevision: claimed.revision, action: 'complete' });
    const readyTask = await ready;
    assert.equal(readyTask?.revision, waiting.revision, '依赖修复使资格变化，不伪造本任务的业务修订');
    await assert.rejects(task.update({ expectedRevision: original.revision, action: 'edit', subject: '过时修订' }),
      error => error instanceof TeamError && error.code === 'TEAM_TASK_STALE_REVISION');
    const programTeam = programTeams.get(session.id)!;
    const programTask = programTeam.task(original.id);
    assert.deepEqual(await programTask.read(), completed, 'Program与SDK共用原生任务事实');
    assert.equal(foreign.task(independent.id).id, task.id);
    assert.equal((await foreign.task(independent.id).read())?.subject, independent.subject);

    const removed = firstValueFrom(dependent.state$.pipe(filter(value => value === undefined), timeout(5_000)));
    await dependent.update({ expectedRevision: waiting.revision, action: 'delete' });
    await removed;
    assert.equal(await dependent.read(), undefined);
    assert.equal(team.getTask(waiting.id).status, 'deleted', '原生墓碑与当前任务板保持各自语义');
    assert.equal(team.task(waiting.id), dependent, '任务撤下不改变原父对象中固定目标的身份');

    await runtime.application.close(session.id);
    const cold = team.task(original.id);
    assert.equal(cold, task, '同一来源的固定任务跨关闭恢复仍保留身份');
    assert.deepEqual(await cold.read(), completed);
    assert.equal(runtime.native.agents.get(SessionId(session.id)), undefined);
    await assert.rejects(cold.update({ expectedRevision: completed.revision, action: 'reopen' }), /REACTIVE_NATIVE_TEAM_LIVE_MEMBER_REQUIRED/);
    await immediate();
    const persistence = Reflect.get(runtime.native.sessionPersistence, symbols.original) as typeof runtime.native.sessionPersistence;
    const open = persistence.open;
    const enteredRead = Promise.withResolvers<void>(), continueRead = Promise.withResolvers<void>();
    let reading: Promise<void> | undefined;
    persistence.open = async function (...args) {
      const handle = await open.apply(this, args);
      enteredRead.resolve(); await continueRead.promise; return handle;
    };
    try {
      const cancellation = new AbortController(), reason = new Error('CANCEL_ONLY_THIS_TEAM_READ');
      let outcome: unknown;
      reading = team.readProjection({ signal: cancellation.signal }).then(value => { outcome = value; }, error => { outcome = error; });
      await enteredRead.promise; cancellation.abort(reason); await immediate();
      assert.equal(outcome, reason, '冷Team只读等待不被不配合取消的原生open阻塞');
    } finally { continueRead.resolve(); await reading; persistence.open = open; }
    const again = await runtime.application.resume(session.id, program);
    resumed.release(); resumed = again;
    team.dispose();
    await assert.rejects(task.read(), /REACTIVE_SCOPE_DISPOSED/);
    assert.deepEqual(await programTask.read(), completed, '外部SDK释放不停止Program的独立观察或删除任务');
    await immediate();
    assert.equal(await resumed.readView('phase'), 'ready');
    const held = programTeams.get(session.id)!;
    const retiring: Observation<TeamProjection>[] = [];
    held.state.observations$.subscribe(value => retiring.push(value));
    if (provider) await provider.dispose();
    else await capability.dispose();
    await immediate();
    assert.throws(() => held.state.peek(), /REACTIVE_SCOPE_DISPOSED/);
    assert.equal(retiring.at(-1)!.status, 'error', '借出资源先撤下旧ready再完成');
    assert.equal(await resumed.readView('phase'), 'error', '仍活跃的Program hook与公开视图取得同一撤销事实');
    assert.throws(() => capability.forSession(resumed!), /REACTIVE_SCOPE_DISPOSED/);
    if (provider) {
      const replacement = runtime.native.plugin(TeamService, {});
      await requireActivePlugin(replacement);
      const fresh = createNativeTeam({ service: { kind: 'provided' } });
      try {
        await fresh.install(runtime.native, runtime.home);
        assert.deepEqual((await fresh.forSession(resumed).state.read()).tasks.find(task => task.id === completed.id), completed);
        assert.throws(() => capability.forSession(resumed!), /REACTIVE_SCOPE_DISPOSED/, '新provider不复活旧能力');
      } finally { await fresh.dispose(); await replacement.dispose(); }
    }
    assert.equal(modelCalls, 0); assert.deepEqual(errors, []);
  } finally {
    release.resolve(); subscription?.unsubscribe(); other?.release(); resumed?.release(); session?.release();
    await runtime.dispose();
  }
});
}
