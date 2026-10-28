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
import { combineResources, defineApplication, defineEnvironment, defineProgram, defineSurface, mapResource, resourceView, view, z, type Observation } from '@hanger-source/dsh-reactive';
import { Expose, implementProgram, useResource } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel, requireActivePlugin, type ConversationHandle } from '@hanger-source/dsh-reactive-dsh';
import { createNativeTeam, TeamError, TeamTaskId, type NativeSessionTeam, type TeamMemberView, type TeamProjection, type TeamTaskView } from '@hanger-source/dsh-reactive-dsh/team';
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
  let other: ConversationHandle<typeof program> | undefined, subscription: Subscription | undefined, rosterSubscription: Subscription | undefined;
  const release = Promise.withResolvers<void>();
  try {
    const workspace = await runtime.application.createWorkspace({ directory });
    session = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    const team = capability.forSession(session);
    assert.equal(team.roster$, team.roster.values$);
    const rosterStates: Observation<readonly TeamMemberView[]>[] = [];
    rosterSubscription = team.roster.observations$.subscribe(value => rosterStates.push(value));
    assert.deepEqual(await team.roster.read(), team.listMembers());
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
    assert.equal(team.roster.peek().status, 'pending', '冷会话不能保留旧Agent运行态');
    const cancellation = new AbortController(), reason = new Error('CANCEL_ONLY_COLD_ROSTER_READ');
    const rosterReading = assert.rejects(team.roster.read(cancellation.signal), error => error === reason);
    cancellation.abort(reason); await rosterReading;
    assert.equal(runtime.native.agents.get(SessionId(session.id)), undefined, '取消运行态读取不创建或恢复Agent');
    resumed = await runtime.application.resume(session.id, program);
    await firstValueFrom(observed.pipe(timeout(5_000)));
    assert.equal((await team.roster.read())[0]!.id, session.id);
    release.resolve(); await immediate();
    assert.equal(delivered.includes(old.projection), false, '旧切面迟到不能覆盖恢复后的Team现场');
    team.readProjection = inspect;
    const absent = team.task(TeamTaskId('999999'));
    assert.equal(team.task(absent.id), absent);
    assert.equal(await absent.read(), undefined);
    assert.equal(await absent.state.read(), undefined);
    await assert.rejects(absent.update({ expectedRevision: 1, action: 'claim' }).result,
      error => error instanceof TeamError && error.code === 'TEAM_TASK_NOT_FOUND');

    other = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    const foreign = capability.forSession(other);
    const original = await team.createTask({ subject: '准备知识', description: '按原生任务板完成前置资料' }).result;
    const waiting = await team.createTask({ subject: '开始接待', description: '资料完成后才能认领', blockedBy: [original.id] }).result;
    const independent = await foreign.createTask({ subject: '其他Team', description: '相同局部任务id仍属于另一个Team' }).result;
    assert.equal(independent.id, original.id);
    const task = team.task(original.id), dependent = team.task(waiting.id);
    assert.equal(team.task(original.id), task);
    assert.equal((await dependent.state.read())?.ready, false);
    const foreignDependent = foreign.task(waiting.id);
    assert.equal(await foreignDependent.read(), undefined, '相同任务目标不能借入其他Team的记录');
    await assert.rejects(foreignDependent.update({ expectedRevision: waiting.revision, action: 'claim' }).result,
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
      const create = writer.createTask({ subject: '取消等待的正式创建', description: '回执取消不重建原生事务' }, { signal: controller.signal });
      creating = create.wait()
        .then(value => { createOutcome = value; }, error => { createOutcome = error; });
      const createdId = await Promise.race([createdAtFlush.promise, creating.then(() => { throw new Error('TEAM_CREATE_DID_NOT_REACH_FLUSH'); })]);
      assert.equal((await create.activity.read()).state, 'running');
      assert.equal(create.source.sessionId, session.id);
      controller.abort(reason); await immediate();
      assert.equal(createOutcome, reason, '真实flush仍未归还时，调用取消已经结束本次创建回执等待');
      releaseCreate.resolve(); await createFlushed.promise;
      const committedCreate = team.getTask(createdId);
      assert.deepEqual(await create.result, committedCreate, 'result保留取消等待后的实际原生返回');
      assert.equal((await create.activity.read()).state, 'returned');
      assert.equal(committedCreate.revision, 1);
      const createdTask = writer.task(createdId);
      const preCancelled = new AbortController(), preCancelledReason = new Error('TEAM_MUTATION_CANCELLED_BEFORE_ADMISSION');
      preCancelled.abort(preCancelledReason);
      const before = lead.session.snapshotEvents().filter(event => event.type === 'team/task').length;
      assert.throws(() => writer.createTask({ subject: '不得追加的创建', description: '预取消不进入原生队列' }, { signal: preCancelled.signal }),
        error => error === preCancelledReason);
      assert.throws(() => createdTask.update({ expectedRevision: committedCreate.revision, action: 'edit', subject: '不得追加的修订' },
        { signal: preCancelled.signal }), error => error === preCancelledReason);
      assert.equal(lead.session.snapshotEvents().filter(event => event.type === 'team/task').length, before);

      holding = 'update';
      const update = createdTask.update({ expectedRevision: committedCreate.revision, action: 'edit', subject: '退出等待的正式修订' });
      updating = update.wait()
        .then(value => { updateOutcome = value; }, error => { updateOutcome = error; });
      await Promise.race([updatedAtFlush.promise, updating.then(() => { throw new Error('TEAM_UPDATE_DID_NOT_REACH_FLUSH'); })]);
      writer.dispose(); await immediate();
      assert.ok(updateOutcome instanceof Error);
      assert.match(updateOutcome.message, /REACTIVE_SCOPE_DISPOSED/u,
        '任务对象复用同一更新入口，SDK退出不等待原生flush回执');
      assert.equal((await create.activity.read()).state, 'returned', 'SDK退出后仍可晚读先前调用的实际终态');
      releaseUpdate.resolve(); await updateFlushed.promise;
      const committedUpdate = team.getTask(createdId);
      assert.deepEqual(await update.result, committedUpdate, 'SDK退出不篡改已受理事务的实际result');
      assert.equal(committedUpdate.subject, '退出等待的正式修订');
      assert.equal(committedUpdate.revision, committedCreate.revision + 1);
      await assert.rejects(team.updateTask({ taskId: createdId, expectedRevision: committedCreate.revision, action: 'edit', subject: '过时重试' }).result,
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
    const extra = await team.createTask({ subject: '读取恢复', description: '真实任务提交产生后续投影通知' }).result;
    assert.equal((await failed).status, 'error');
    assert.equal(team.state.peek().status, 'error');
    team.readProjection = inspect;
    const recovered = firstValueFrom(team.state.observations$.pipe(filter(value => value.status === 'ready'), timeout(5_000)));
    await team.updateTask({ taskId: extra.id, expectedRevision: extra.revision, action: 'edit', subject: '读取已恢复' }).result;
    await recovered;
    assert.ok(states.some(value => value.status === 'error' && value.error === failure));
    assert.equal((await dependent.read())?.ready, false);
    await assert.rejects(dependent.update({ expectedRevision: waiting.revision, action: 'claim' }).result,
      error => error instanceof TeamError && error.code === 'TEAM_TASK_BLOCKED');
    const retarget = { expectedRevision: original.revision, action: 'edit' as const, subject: '不能转写其他任务', taskId: waiting.id };
    assert.throws(() => task.update(retarget), /REACTIVE_NATIVE_TEAM_TASK_SOURCE_RESERVED/);

    const ready = firstValueFrom(dependent.state$.pipe(filter(value => value?.ready === true), timeout(5_000)));
    const claimed = await task.update({ expectedRevision: original.revision, action: 'claim' }).result;
    const completed = await task.update({ expectedRevision: claimed.revision, action: 'complete' }).result;
    const readyTask = await ready;
    assert.equal(readyTask?.revision, waiting.revision, '依赖修复使资格变化，不伪造本任务的业务修订');
    await assert.rejects(task.update({ expectedRevision: original.revision, action: 'edit', subject: '过时修订' }).result,
      error => error instanceof TeamError && error.code === 'TEAM_TASK_STALE_REVISION');
    const programTeam = programTeams.get(session.id)!;
    const programTask = programTeam.task(original.id);
    assert.deepEqual(await programTask.read(), completed, 'Program与SDK共用原生任务事实');
    assert.equal(foreign.task(independent.id).id, task.id);
    assert.equal((await foreign.task(independent.id).read())?.subject, independent.subject);

    const removed = firstValueFrom(dependent.state$.pipe(filter(value => value === undefined), timeout(5_000)));
    await dependent.update({ expectedRevision: waiting.revision, action: 'delete' }).result;
    await removed;
    assert.equal(await dependent.read(), undefined);
    assert.equal(team.getTask(waiting.id).status, 'deleted', '原生墓碑与当前任务板保持各自语义');
    assert.equal(team.task(waiting.id), dependent, '任务撤下不改变原父对象中固定目标的身份');

    await runtime.application.close(session.id);
    const cold = team.task(original.id);
    assert.equal(cold, task, '同一来源的固定任务跨关闭恢复仍保留身份');
    assert.deepEqual(await cold.read(), completed);
    assert.equal(runtime.native.agents.get(SessionId(session.id)), undefined);
    assert.throws(() => cold.update({ expectedRevision: completed.revision, action: 'reopen' }), /REACTIVE_NATIVE_TEAM_LIVE_MEMBER_REQUIRED/);
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
    assert.throws(() => team.roster.peek(), /REACTIVE_SCOPE_DISPOSED/);
    assert.equal(rosterStates.at(-1)!.status, 'error', '退出先撤销当前运行态再结束观察');
    await assert.rejects(task.read(), /REACTIVE_SCOPE_DISPOSED/);
    assert.deepEqual(await programTask.read(), completed, '外部SDK释放不停止Program的独立观察或删除任务');
    await immediate();
    assert.equal(await resumed.readView('phase'), 'ready');
    const held = programTeams.get(session.id)!;
    const retiring: Observation<TeamProjection>[] = [];
    const retiringRoster: Observation<readonly TeamMemberView[]>[] = [];
    held.state.observations$.subscribe(value => retiring.push(value));
    held.roster.observations$.subscribe(value => retiringRoster.push(value));
    await held.roster.read();
    const currentLead = runtime.native.agents.get(SessionId(session.id))!;
    const lateFailure = new Error('TEAM_FLUSH_FAILED_AFTER_CALLER_LEFT');
    const lateEntered = Promise.withResolvers<void>(), releaseLate = Promise.withResolvers<void>();
    const beforeLateFlush = sessions.flush;
    let closing: Promise<void> | undefined;
    let heldLateFlush = false;
    sessions.flush = async function (...args) {
      const result = await beforeLateFlush.apply(this, args);
      if (!heldLateFlush && args[0] === currentLead.session && currentLead.session.snapshotEvents().findLast(event => event.type === 'team/task')
        ?.data.task.subject === '等待者退出后的flush失败') {
        heldLateFlush = true;
        lateEntered.resolve(); await releaseLate.promise; throw lateFailure;
      }
      return result;
    };
    try {
      const controller = new AbortController(), reason = new Error('CALLER_LEFT_BEFORE_FLUSH_FAILED');
      const call = held.createTask({ subject: '等待者退出后的flush失败', description: '实际事务异常由result报告' }, { signal: controller.signal });
      const actualFailure = assert.rejects(call.result, error => error === lateFailure);
      const cancelledWait = assert.rejects(call.wait(), error => error === reason);
      await lateEntered.promise;
      const checkpoint = await runtime.native.sessionPersistence.open(currentLead.id, 'read');
      try {
        const durable = await checkpoint.read();
        assert.equal(durable.events.filter(event => event.type === 'team/task'
          && event.data.task.subject === '等待者退出后的flush失败').length, 1,
        '门闩只延迟真实flush回执，已接受的任务必须真实持久化且不重复');
      } finally { await checkpoint.close(); }
      controller.abort(reason); await cancelledWait;
      let closed = false;
      const rootExited = Promise.withResolvers<void>();
      const stopDisposed = runtime.native.on('agent/disposed', ({ agent }) => {
        if (agent === currentLead) rootExited.resolve();
      });
      try {
        closing = (service === 'install' ? runtime.dispose() : capability.dispose()).then(() => { closed = true; });
        if (service === 'install') {
          await Promise.race([rootExited.promise, closing.then(() => { throw new Error('RUNTIME_CLOSED_BEFORE_NATIVE_RESULT'); })]);
        }
        await immediate();
        assert.equal(closed, false, '能力或外层Runtime退出必须等待已经进入原生FIFO的实际事务');
      } finally { stopDisposed(); }
      releaseLate.resolve(); await actualFailure;
      const failedActivity = await firstValueFrom(call.activity.observations$);
      assert.equal(failedActivity.status, 'ready', '退出中的能力不撤销实际调用终态的读取资格');
      if (failedActivity.status !== 'ready' || failedActivity.value.state !== 'failed') assert.fail('调用活动必须是实际失败终态');
      assert.equal(failedActivity.value.error, lateFailure);
      await closing;
      assert.equal(closed, true, '原生写入失败与普通CAS拒绝不冒充资源清理失败');
      const terminal = await call.activity.read();
      assert.equal(terminal.state, 'failed');
      if (terminal.state === 'failed') assert.equal(terminal.error, lateFailure, '能力退出后晚读保留同一个原生异常');
    } finally {
      releaseLate.resolve(); sessions.flush = beforeLateFlush;
      await Promise.allSettled([closing]);
    }
    if (provider) await provider.dispose();
    await immediate();
    assert.throws(() => held.state.peek(), /REACTIVE_SCOPE_DISPOSED/);
    assert.throws(() => held.roster.peek(), /REACTIVE_SCOPE_DISPOSED/);
    assert.equal(retiringRoster.at(-1)!.status, 'error', 'provider退出也撤销同一运行态资源');
    assert.equal(retiring.at(-1)!.status, 'error', '借出资源先撤下旧ready再完成');
    if (provider) assert.equal(await resumed.readView('phase'), 'error', '仍活跃的Program hook与公开视图取得同一撤销事实');
    else await assert.rejects(resumed.readView('phase'), /REACTIVE_SCOPE_DISPOSED|REACTIVE_HOST_DISPOSED/u,
      '外层Runtime退出同时撤销Program公开视图');
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
    release.resolve(); subscription?.unsubscribe(); rosterSubscription?.unsubscribe(); other?.release(); resumed?.release(); session?.release();
    await runtime.dispose();
  }
});
}

test('Team运行态资源跟随真实turn并与任务资格组合，持久成员阶段不受turn影响', { timeout: 20_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-team-roster-'));
  const directory = join(root, 'workspace'); await mkdir(directory);
  const capability = createNativeTeam({ service: { kind: 'install' } });
  const program = defineProgram({ input: z.null(), surface: defineSurface({
    views: { canClaim: resourceView(z.boolean()) }, actions: {},
  }) });
  const definition = defineApplication({ name: 'team-roster', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '处理当前输入期间暂停接新任务', program: implementProgram(program, { Component({ conversation }) {
      const team = useMemo(() => capability.forSession(conversation), [conversation]);
      const roster = useResource(team.roster), board = useResource(team.state);
      return Expose({ surface: program.surface, value: { views: {
        canClaim: combineResources({ roster: roster.observations$, board: board.observations$ }).pipe(mapResource(current =>
          current.roster.some(member => member.status === 'inactive')
          && current.board.tasks.some(task => task.ready && task.status === 'pending'))),
      } } });
    } }),
  })] });
  const entered = Promise.withResolvers<void>(), release = Promise.withResolvers<void>();
  const errors: unknown[] = [];
  let calls = 0;
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'team-roster-owner',
    installNative: capability.install, onError: event => errors.push(event.error),
    model: nativeModel({ provider: 'team-roster', model: 'controlled', create: () => new class extends LlmAdapter {
      async *stream() { calls++; entered.resolve(); await release.promise; yield* text('当前输入已处理'); }
    }() }),
  });
  let session: ConversationHandle<typeof program> | undefined, other: ConversationHandle<typeof program> | undefined;
  let rosterSubscription: Subscription | undefined, boardSubscription: Subscription | undefined;
  try {
    const workspace = await runtime.application.createWorkspace({ directory });
    session = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    other = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    const team = capability.forSession(session), foreign = capability.forSession(other);
    const rosters = new ReplaySubject<readonly TeamMemberView[]>(1), boards = new ReplaySubject<TeamProjection>(1);
    rosterSubscription = team.roster$.subscribe(rosters);
    boardSubscription = team.projection$.subscribe(boards);
    await team.createTask({ subject: '待接任务', description: '当前turn结束后可以接取' }).result;
    assert.equal(await session.readView('canClaim'), true);
    const before = await firstValueFrom(boards);
    const unrelated = foreign.roster.observations$.subscribe();
    try {
      await foreign.roster.read();
      const foreignBefore = foreign.roster.peek();
      await session.submit({ requestId: 'occupy-lead', content: '请处理当前输入', scope: null, intent: 'question', delivery: 'followup' });
      await entered.promise;
      const running = await firstValueFrom(rosters.pipe(filter(rows => rows[0]?.status === 'running'), timeout(5_000)));
      assert.equal(running[0]!.id, session.id);
      assert.equal(await session.readView('canClaim'), false, '同一持久任务仍就绪，但当前成员忙碌');
      assert.deepEqual((await team.readProjection()).members, before.members, 'turn不改写持久成员阶段');
      assert.equal(foreign.roster.peek(), foreignBefore, '其他Team的turn不能撤销此Team的运行态');
      const inactive = firstValueFrom(rosters.pipe(filter(rows => rows[0]?.status === 'inactive'), timeout(5_000)));
      release.resolve(); await session.whenIdle(); await inactive;
      assert.equal(await session.readView('canClaim'), true);
      assert.deepEqual((await team.readProjection()).members, before.members);
      assert.equal(calls, 1);
    } finally { unrelated.unsubscribe(); }
    const source = await runtime.application.open(session.id, program);
    const borrowed = capability.forSession(source);
    const retiring: Observation<readonly TeamMemberView[]>[] = [];
    borrowed.roster.observations$.subscribe(value => retiring.push(value));
    await borrowed.roster.read(); source.release();
    assert.throws(() => borrowed.roster.peek(), /REACTIVE_SCOPE_DISPOSED/);
    assert.equal(retiring.at(-1)!.status, 'error', '源句柄退出撤销借出的运行态');
    assert.equal(runtime.native.agents.get(SessionId(session.id))!.status, 'idle', '借用结束不退出原生成员');
    assert.equal(await session.readView('canClaim'), true, '源句柄退出不影响Program的独立借用');
    assert.deepEqual(errors, []);
  } finally {
    release.resolve(); rosterSubscription?.unsubscribe(); boardSubscription?.unsubscribe(); other?.release(); session?.release();
    await capability.dispose(); await runtime.dispose();
  }
});
