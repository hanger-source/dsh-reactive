import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { access, mkdir, mkdtemp, rename, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement as h, Fragment } from 'react';
import { defer, finalize, firstValueFrom, of, Subject } from 'rxjs';
import { Context as NativeContext } from '@deepseek-ai/cordis';
import SessionStore, { SessionId } from '@deepseek-ai/dsh-session';
import * as persistence from '@deepseek-ai/dsh-session-persistence-jsonl';
import Storage from '@deepseek-ai/dsh-storage';
import * as storageJson from '@deepseek-ai/dsh-storage-json';
import * as storageDomain from '@deepseek-ai/dsh-storage-domain';
import WorkspaceRegistry, { WorkspaceId } from '@deepseek-ai/dsh-workspace';
import type { GenerateOptions } from '@deepseek-ai/dsh-llm';
import { Context, Expose, useAction, useResource } from '@hanger-source/dsh-reactive/program';
import { defineAction, implementAction, defineApplication, defineEnvironment, defineProgram, defineSurface, observeResource, resourceView, z, type ProgramProps } from '@hanger-source/dsh-reactive';
import { CreationIncompleteError, mountApplication } from '@hanger-source/dsh-reactive-dsh';
import { boot, text } from './native-support.ts';

test('真实存储拒写后沿原创建引用完成登记，原生工作区移除成员后重启与创建恢复均不能重新授予身份', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-workspace-'));
  const home = join(root, 'home');
  const medium = join(home, 'state');
  const definition = defineApplication({ name: 'workspace-owner', admit: async () => {}, environments: [defineEnvironment({
    id: 'delivery', instructions: '交付',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component: () => null }),
  })] });
  let modelCalls = 0;
  const respond = async function* () { modelCalls++; yield* text('observed'); };
  let app = await boot(home, definition, respond);
  try {
    const workspaceId = app.workspace.workspaceId;
    const otherDirectory = join(root, 'other-workspace');
    await mkdir(otherDirectory);
    const other = await app.host.createWorkspace({ directory: otherDirectory });
    let reference: CreationIncompleteError['reference'] | undefined;
    const preserved = join(root, 'preserved-state');
    await rename(medium, preserved);
    await writeFile(medium, 'storage path is unavailable', { flag: 'wx' });
    try {
      await assert.rejects(app.host.create('delivery', { workspaceId }), error => {
        if (!(error instanceof CreationIncompleteError)) return false;
        reference = error.reference;
        return reference.environmentId === 'delivery' && reference.workspaceId === workspaceId;
      });
    } finally {
      await rename(medium, join(root, 'unavailable-medium-marker'));
      await rename(preserved, medium);
    }
    assert.ok(reference);
    await assert.rejects(app.host.completeCreation({ ...reference, workspaceId: other.workspaceId }));
    const restored = await app.host.completeCreation(reference!);
    assert.equal(restored.id, reference!.sessionId);
    restored.release();
    assert.ok((await app.host.describe()).find(workspace => workspace.workspaceId === workspaceId)!.sessionIds.includes(restored.id));
    assert.equal(modelCalls, 0);
    await app.dispose();

    // 以独立原生管理者修改权威成员关系，不修改框架缓存或手工伪造存储记录。
    const admin = new NativeContext();
    try {
      await admin.plugin(SessionStore);
      await admin.plugin(persistence, { root: join(home, 'sessions'), compression: 'none' });
      await admin.plugin(Storage);
      await admin.plugin(storageJson, { root: medium });
      await admin.plugin(storageDomain, { backend: 'json' });
      await admin.plugin(WorkspaceRegistry);
      await admin.inject(['workspaceRegistry'], async native => {
        const workspace = native.workspaceRegistry.get(WorkspaceId(workspaceId))!;
        await workspace.detachSession(SessionId(reference!.sessionId));
      });
    } finally { await admin.fiber.dispose(); }

    app = await boot(home, definition, respond);
    assert.equal(app.workspace.workspaceId, workspaceId);
    assert.deepEqual((await app.host.describe()).find(workspace => workspace.workspaceId === workspaceId)!.sessionIds, []);
    await assert.rejects(app.host.open(reference!.sessionId), /REACTIVE_WORKSPACE_MEMBERSHIP_REQUIRED/);
    await assert.rejects(app.host.resume(reference!.sessionId), /REACTIVE_WORKSPACE_MEMBERSHIP_REQUIRED/);
    await assert.rejects(app.host.completeCreation(reference!), /REACTIVE_WORKSPACE_MEMBERSHIP_REQUIRED/);
    assert.equal(modelCalls, 0);
    assert.deepEqual(app.errors, []);
  } finally { await app.dispose(); }
});

test('环境和工作区独立组合，共享来源跨环境持有，冷恢复保留原装配并过滤外应用会话', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-workspace-composition-'));
  const home = join(root, 'home');
  let connections = 0, releases = 0, revision = 1;
  const changes = new Subject<void>();
  const observations$ = defer(() => {
    const connection = ++connections;
    return observeResource({ key$: of(connection), read: id => ({ connection: id, revision }), invalidations: () => changes })
      .pipe(finalize(() => { releases++; }));
  });
  const surface = defineSurface({ views: { facts: resourceView(z.object({ connection: z.number(), revision: z.number() })) }, actions: {} });
  const deliveryProgram = defineProgram({ input: z.null(), surface });
  const analysisProgram = defineProgram({ input: z.null(), surface });
  function SharedFacts({ workspace }: ProgramProps<typeof deliveryProgram>) {
    const facts = useResource(() => defer(() => workspace.share(observations$)), [workspace]);
    return h(Fragment, null, Context({ name: 'shared-workspace-facts', observation: facts.state, render: value => value }),
      Expose({ surface, value: { views: { facts: facts.observations$ } } }));
  }
  const definition = defineApplication({ name: 'workspace-composition', admit: async () => {}, environments: [
    defineEnvironment({ id: 'delivery', instructions: 'DELIVERY_ENVIRONMENT_ONLY',
      program: implementProgram(deliveryProgram, { Component: SharedFacts }) }),
    defineEnvironment({ id: 'analysis', instructions: 'ANALYSIS_ENVIRONMENT_ONLY',
      program: implementProgram(analysisProgram, { Component: SharedFacts }) }),
  ] });
  const requests: string[] = [];
  const respond = async function* (request: GenerateOptions) {
    requests.push(JSON.stringify(request.messages)); yield* text('已读取当前工作区事实');
  };
  let app = await boot(home, definition, respond);
  try {
    const missing = join(root, 'not-created');
    await assert.rejects(app.host.createWorkspace({ directory: missing }));
    await assert.rejects(access(missing), '注册工作区不能隐式建立目录');
    const firstWorkspace = app.workspace;
    const secondDirectory = join(root, 'second-workspace');
    await mkdir(secondDirectory);
    const secondWorkspace = await app.host.createWorkspace({ directory: secondDirectory, title: '独立工作区' });
    const delivery = await app.host.create('delivery', { workspaceId: firstWorkspace.workspaceId });
    const analysis = await app.host.create('analysis', { workspaceId: firstWorkspace.workspaceId });
    const second = await app.host.create('delivery', { workspaceId: secondWorkspace.workspaceId });
    const firstFacts = await firstValueFrom(delivery.views.facts);
    assert.deepEqual(await firstValueFrom(analysis.views.facts), firstFacts);
    assert.notEqual((await firstValueFrom(second.views.facts)).connection, firstFacts.connection);
    assert.equal(connections, 2, '每个原生工作区连接一次，不按环境重新建立来源');
    for (const session of [delivery, analysis, second]) {
      const expected = session === second ? secondWorkspace : firstWorkspace;
      assert.equal(app.native.agents.get(SessionId(session.id))!.session.header.cwd, expected.directory);
      await session.submit({ requestId: 'read-' + session.id, content: '读取当前现场', intent: 'question', delivery: 'followup' as const, scope: null });
      await session.whenIdle();
    }
    assert.ok(requests[0]!.includes('DELIVERY_ENVIRONMENT_ONLY'));
    assert.equal(requests[0]!.includes('ANALYSIS_ENVIRONMENT_ONLY'), false);
    assert.ok(requests[1]!.includes('ANALYSIS_ENVIRONMENT_ONLY'));
    assert.equal(requests[1]!.includes('DELIVERY_ENVIRONMENT_ONLY'), false);
    const ids = { delivery: delivery.id, analysis: analysis.id, second: second.id };
    await app.host.close(delivery.id); delivery.release();
    revision = 2; changes.next();
    assert.equal((await firstValueFrom(analysis.views.facts)).revision, 2);
    assert.equal(releases, 0, '关闭一个会话不能释放工作区来源');

    const foreignProgram = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
    const foreignHost = await mountApplication(app.native, () => defineApplication({ name: 'another-application', admit: async () => {},
      environments: [defineEnvironment({ id: 'delivery', instructions: '外应用',
        program: implementProgram(foreignProgram, { Component: () => null }) })] }));
    let foreignId: string | undefined;
    try {
      const foreign = await foreignHost.create('delivery', { workspaceId: firstWorkspace.workspaceId });
      foreignId = foreign.id; foreign.release();
      const described = (await app.host.describe()).find(workspace => workspace.workspaceId === firstWorkspace.workspaceId)!;
      assert.ok(described.sessionIds.includes(ids.delivery));
      assert.ok(described.sessionIds.includes(ids.analysis));
      assert.equal(described.sessionIds.includes(foreignId), false);
      await assert.rejects(app.host.open(foreignId));
    } finally { await foreignHost.dispose(); }
    assert.ok(foreignId);
    const persistedForeignId = foreignId;
    analysis.release(); second.release();
    assert.deepEqual(app.errors, []);
    await app.dispose();
    assert.equal(releases, 2);

    app = await boot(home, definition, respond);
    assert.equal(app.workspace.workspaceId, firstWorkspace.workspaceId);
    const coldDelivery = await app.host.open(ids.delivery, deliveryProgram);
    const coldAnalysis = await app.host.open(ids.analysis, analysisProgram);
    const coldSecond = await app.host.open(ids.second, deliveryProgram);
    assert.equal(coldDelivery.snapshot().source, 'durable');
    assert.equal(coldDelivery.identity.workspaceId, firstWorkspace.workspaceId);
    assert.equal(coldAnalysis.identity.workspaceId, firstWorkspace.workspaceId);
    assert.equal(coldSecond.identity.workspaceId, secondWorkspace.workspaceId);
    assert.equal(coldDelivery.identity.environmentId, 'delivery');
    assert.equal(coldAnalysis.identity.environmentId, 'analysis');
    assert.deepEqual(await firstValueFrom(coldDelivery.views.facts), await firstValueFrom(coldAnalysis.views.facts));
    assert.notEqual((await firstValueFrom(coldSecond.views.facts)).connection, (await firstValueFrom(coldDelivery.views.facts)).connection);
    assert.equal((await app.host.describe()).some(workspace => workspace.sessionIds.includes(persistedForeignId)), false);
    await assert.rejects(app.host.open(ids.delivery, analysisProgram), /REACTIVE_PROGRAM_CONTRACT_MISMATCH/);
    assert.equal(requests.length, 3, '冷观察不启动模型或重投原输入');
    const resumed = await app.host.resume(ids.delivery, deliveryProgram);
    await resumed.submit({ requestId: 'after-restart', content: '沿原环境继续读取', intent: 'question', delivery: 'followup' as const, scope: null });
    await resumed.whenIdle();
    assert.ok(requests.at(-1)!.includes('DELIVERY_ENVIRONMENT_ONLY'));
    assert.equal(requests.at(-1)!.includes('ANALYSIS_ENVIRONMENT_ONLY'), false);
    assert.equal((await firstValueFrom(resumed.views.facts)).revision, 2);
    assert.deepEqual(app.errors, []);
    resumed.release(); coldDelivery.release(); coldAnalysis.release(); coldSecond.release();
  } finally { await app.dispose(); }
});

test('活宿主撤销工作区成员或实体后保留的SDK动作拒绝新执行，已受理业务仍可返回', { timeout: 30_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-workspace-authority-'));
  const entered = Promise.withResolvers<void>();
  const finish = Promise.withResolvers<void>();
  const executions: { operationId: string; workspaceId: string; actorId: string }[] = [];
  let completed = 0, modelCalls = 0;
  const commit = defineAction({ name: 'commit_workspace_work', description: '完成已受理的工作区业务',
    scope: z.object({ workspaceId: z.string() }), input: z.object({ operationId: z.string() }),
    output: z.object({ operationId: z.string(), workspaceId: z.string(), actorId: z.string() }) });
  const implementation = implementAction(commit, { async execute(input, call) {
    assert.equal(call.source.kind, 'participant');
    if (call.source.kind !== 'participant') throw new Error('SDK_PARTICIPANT_REQUIRED');
    assert.equal(call.source.participant, 'human');
    assert.equal(call.source.actorId, 'sdk-workspace-owner');
    const execution = { operationId: input.operationId, workspaceId: call.scope.workspaceId, actorId: call.source.actorId };
    executions.push(execution);
    if (executions.length === 2) entered.resolve();
    await finish.promise;
    call.signal.throwIfAborted();
    completed++;
    return execution;
  } });
  const surface = defineSurface({ views: {}, actions: { commit } });
  const definition = defineApplication({ name: 'workspace-authority', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '只显式执行业务',
    program: implementProgram(defineProgram({ input: z.null(), surface }), { Component({ workspace }) {
      const action = useAction(implementation, () => ({ workspaceId: workspace.id }), [workspace]);
      return Expose({ surface, value: { actions: { commit: action } } });
    } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () { modelCalls++; yield* text('未请求模型工作'); });
  try {
    const secondDirectory = join(root, 'deleted-workspace');
    await mkdir(secondDirectory);
    const secondWorkspace = await app.host.createWorkspace({ directory: secondDirectory });
    const detached = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const deleted = await app.host.create('work', { workspaceId: secondWorkspace.workspaceId });
    const detachedAction = detached.resolveAction('commit', { workspaceId: app.workspace.workspaceId });
    const deletedAction = deleted.resolveAction('commit', { workspaceId: secondWorkspace.workspaceId });
    const actor = { actorId: 'sdk-workspace-owner' };
    const first = detachedAction({ operationId: 'accepted-before-detach' }, actor);
    const second = deletedAction({ operationId: 'accepted-before-delete' }, actor);
    await entered.promise;
    const workspace = app.native.workspaceRegistry.get(WorkspaceId(app.workspace.workspaceId))!;
    await workspace.detachSession(SessionId(detached.id));
    assert.equal(await app.native.workspaceRegistry.delete(WorkspaceId(secondWorkspace.workspaceId)), true);
    await assert.rejects(async () => detachedAction({ operationId: 'rejected-after-detach' }, actor).result);
    await assert.rejects(async () => deletedAction({ operationId: 'rejected-after-delete' }, actor).result);
    const expected = [
      { operationId: 'accepted-before-detach', workspaceId: app.workspace.workspaceId, actorId: actor.actorId },
      { operationId: 'accepted-before-delete', workspaceId: secondWorkspace.workspaceId, actorId: actor.actorId },
    ];
    assert.deepEqual(executions.toSorted((a, b) => a.operationId.localeCompare(b.operationId)),
      expected.toSorted((a, b) => a.operationId.localeCompare(b.operationId)));
    finish.resolve();
    assert.deepEqual(await Promise.all([first.result, second.result]), expected);
    assert.equal(completed, 2, '撤销后续准入不能取消已开始的业务');
    assert.equal(modelCalls, 0);
    assert.deepEqual(app.errors, []);
    detached.release(); deleted.release();
  } finally { finish.resolve(); await app.dispose(); }
});
