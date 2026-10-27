import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate as immediate } from 'node:timers/promises';
import { useLayoutEffect } from 'react';
import { ReplaySubject, filter, firstValueFrom, timeout, type Subscription } from 'rxjs';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { SessionId } from '@deepseek-ai/dsh-session';
import { defineApplication, defineEnvironment, defineProgram, defineSurface, z } from '@hanger-source/dsh-reactive';
import { implementProgram, useObservable } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel, type ConversationHandle } from '@hanger-source/dsh-reactive-dsh';
import { createNativeTeam, TeamError, type NativeSessionTeam, type TeamProjection, type TeamTaskView } from '@hanger-source/dsh-reactive-dsh/team';
import { text } from './native-support.ts';

test('Team固定任务沿真实任务板执行CAS和依赖派生，关闭恢复不丢观察，冷读与SDK释放不执行模型或重定向目标', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-native-team-'));
  const directory = join(root, 'workspace'); await mkdir(directory);
  const capability = createNativeTeam({ service: { kind: 'install' } });
  const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
  const programTeams = new Map<string, NativeSessionTeam>();
  const errors: unknown[] = [];
  let modelCalls = 0;
  const definition = defineApplication({ name: 'native-team', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: 'Team管理不产生模型输入', program: implementProgram(program, { Component({ conversation }) {
      const team = capability.forSession(conversation);
      useObservable(() => team.tasks$, [team]);
      useLayoutEffect(() => { programTeams.set(conversation.identity.sessionId, team); }, [conversation, team]);
      return null;
    } }),
  })] });
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'team-owner',
    installNative: capability.install, onError: event => errors.push(event.error),
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
    subscription = team.projection$.subscribe({
      next: projection => { delivered.push(projection); observed.next(projection); },
      error: error => { errors.push(error); observed.error(error); },
    });
    const old = await entered.promise;
    await runtime.application.close(session.id);
    assert.equal(old.signal.aborted, true, '关闭原生owner须撤销SDK在途投影读取');
    resumed = await runtime.application.resume(session.id, program);
    await firstValueFrom(observed.pipe(timeout(5_000)));
    release.resolve(); await immediate();
    assert.equal(delivered.includes(old.projection), false, '旧切面迟到不能覆盖恢复后的Team现场');
    team.readProjection = inspect;

    other = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    const foreign = capability.forSession(other);
    const original = await team.createTask({ subject: '准备知识', description: '按原生任务板完成前置资料' });
    const waiting = await team.createTask({ subject: '开始接待', description: '资料完成后才能认领', blockedBy: [original.id] });
    const independent = await foreign.createTask({ subject: '其他Team', description: '相同局部任务id仍属于另一个Team' });
    assert.equal(independent.id, original.id);
    const task = await team.task(original.id), dependent = await team.task(waiting.id);
    assert.equal(await team.task(original.id), task);
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
    const programTask = await programTeam.task(original.id);
    assert.deepEqual(await programTask.read(), completed, 'Program与SDK共用原生任务事实');
    assert.equal((await foreign.task(independent.id)).id, task.id);
    assert.equal((await (await foreign.task(independent.id)).read())?.subject, independent.subject);

    const removed = firstValueFrom(dependent.state$.pipe(filter(value => value === undefined), timeout(5_000)));
    await dependent.update({ expectedRevision: waiting.revision, action: 'delete' });
    await removed;
    assert.equal(await dependent.read(), undefined);
    assert.equal(team.getTask(waiting.id).status, 'deleted', '原生墓碑与当前任务板保持各自语义');
    await assert.rejects(team.task(waiting.id), error => error instanceof TeamError && error.code === 'TEAM_TASK_NOT_FOUND');

    await runtime.application.close(session.id);
    const cold = await team.task(original.id);
    assert.equal(cold, task, '同一来源的固定任务跨关闭恢复仍保留身份');
    assert.deepEqual(await cold.read(), completed);
    assert.equal(runtime.native.agents.get(SessionId(session.id)), undefined);
    await assert.rejects(cold.update({ expectedRevision: completed.revision, action: 'reopen' }), /REACTIVE_NATIVE_TEAM_LIVE_MEMBER_REQUIRED/);
    const again = await runtime.application.resume(session.id, program);
    resumed.release(); resumed = again;
    team.dispose();
    await assert.rejects(task.read(), /REACTIVE_SCOPE_DISPOSED/);
    assert.deepEqual(await programTask.read(), completed, '外部SDK释放不停止Program的独立观察或删除任务');
    assert.equal(modelCalls, 0); assert.deepEqual(errors, []);
  } finally {
    release.resolve(); subscription?.unsubscribe(); other?.release(); resumed?.release(); session?.release();
    await runtime.dispose();
  }
});
