import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { access, mkdir, mkdtemp, readFile, rename, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createElement as h, Fragment, useLayoutEffect } from 'react';
import { Observable, concatMap, defer, finalize, firstValueFrom, of, Subject } from 'rxjs';
import { Context as NativeContext } from '@deepseek-ai/cordis';
import SessionStore, { SessionId, pluginRecordOf } from '@deepseek-ai/dsh-session';
import * as persistence from '@deepseek-ai/dsh-session-persistence-jsonl';
import Storage from '@deepseek-ai/dsh-storage';
import * as storageJson from '@deepseek-ai/dsh-storage-json';
import * as storageDomain from '@deepseek-ai/dsh-storage-domain';
import WorkspaceRegistry, { WorkspaceId } from '@deepseek-ai/dsh-workspace';
import type { GenerateOptions } from '@deepseek-ai/dsh-llm';
import { Context, Expose, useAction, useResource } from '@hanger-source/dsh-reactive/program';
import { defineAction, implementAction, defineApplication, defineEnvironment, defineProgram, defineSurface, defineResource, implementResource,
  defineReaction, implementReaction, observeResource, resourceView, z, type ProgramProps, type ProgramWorkspace } from '@hanger-source/dsh-reactive';
import { CreationIncompleteError, mountApplication } from '@hanger-source/dsh-reactive-dsh';
import { boot, text } from './native-support.ts';

test('工作区共享流跨会话保活，宿主退出后活动与有限来源均不再重放旧事实', { timeout: 20_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-workspace-sharing-'));
  const captured = new Map<string, ProgramWorkspace>();
  const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
  const definition = defineApplication({ name: 'workspace-sharing', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '只观察共享来源', program: implementProgram(program, { Component({ conversation, workspace }) {
      useLayoutEffect(() => { captured.set(conversation.identity.sessionId, workspace); }, [conversation, workspace]);
      return null;
    } }),
  })] });
  let models = 0, starts = 0, stops = 0;
  const app = await boot(root, definition, async function* () { models++; yield* text('unused'); });
  const changes = new Subject<number>();
  const source = new Observable<number>(subscriber => {
    starts++;
    const connection = changes.subscribe(subscriber);
    return () => { stops++; connection.unsubscribe(); };
  });
  const sessions: Array<{ release(): void }> = [];
  try {
    const one = await app.host.create('work', { workspaceId: app.workspace.workspaceId }); sessions.push(one);
    const two = await app.host.create('work', { workspaceId: app.workspace.workspaceId }); sessions.push(two);
    const directory = join(root, 'other-workspace'); await mkdir(directory);
    const otherWorkspace = await app.host.createWorkspace({ directory });
    const other = await app.host.create('work', { workspaceId: otherWorkspace.workspaceId }); sessions.push(other);
    const first = captured.get(one.id)!, sibling = captured.get(two.id)!, isolated = captured.get(other.id)!;
    assert.equal(app.host.workspace(app.workspace.workspaceId), first, '组件与宿主借用同一工作区编程对象');
    assert.equal(first, sibling); assert.notEqual(first, isolated);
    const shared = first.share(source), foreign = isolated.share(source);
    assert.equal(sibling.share(source), shared); assert.equal(starts, 2);
    changes.next(1);
    const values: number[] = [];
    const consumer = shared.subscribe(value => values.push(value));
    assert.deepEqual(values, [1]); consumer.unsubscribe();
    await app.host.close(one.id); await app.host.close(two.id);
    changes.next(2);
    assert.equal(await firstValueFrom(shared), 2);
    assert.equal(await firstValueFrom(foreign), 2);
    assert.equal(stops, 0, '会话与最后消费者退出不结束工作区持有的来源');
    const final = first.share(of('settled-workspace-fact'));
    assert.equal(await firstValueFrom(final), 'settled-workspace-fact');
    assert.equal(await firstValueFrom(final), 'settled-workspace-fact', '有限来源的终态在owner活跃时仍可读取');
    let completed = false, reentrant: unknown;
    shared.subscribe({ complete: () => {
      completed = true;
      try { app.host.workspace(app.workspace.workspaceId); } catch (error) { reentrant = error; }
    } });
    await app.native.workspaceRegistry.delete(WorkspaceId(app.workspace.workspaceId));
    assert.equal(stops, 1); assert.equal(completed, true, '登记删除撤销工作区共享来源，不等待整个宿主退出');
    assert.ok(reentrant instanceof Error && reentrant.message === 'REACTIVE_WORKSPACE_NOT_FOUND', '清理重入不能从原生尚未撤下的实体缓存复活owner');
    assert.equal(await firstValueFrom(foreign), 2, '另一工作区的来源仍活跃');
    assert.throws(() => first.share(source), /REACTIVE_SCOPE_DISPOSED/);
    assert.throws(() => app.host.workspace(app.workspace.workspaceId), /REACTIVE_WORKSPACE_NOT_FOUND/);
    await app.dispose();
    assert.equal(stops, 2); assert.equal(completed, true);
    const late: Array<number | string> = [];
    assert.equal(shared.subscribe(value => late.push(value)).closed, true);
    assert.equal(foreign.subscribe(value => late.push(value)).closed, true);
    assert.equal(final.subscribe(value => late.push(value)).closed, true);
    assert.deepEqual(late, []);
    assert.equal(starts, 2);
    assert.throws(() => first.share(source), /REACTIVE_SCOPE_DISPOSED/);
    assert.equal(models, 0); assert.deepEqual(app.errors, []);
  } finally { changes.complete(); for (const session of sessions) session.release(); await app.dispose(); }
});

test('工作区删除已经持久后共享清理失败仍撤销owner，宿主退出保留失败原因并释放其他工作区', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-workspace-failed-cleanup-'));
  const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
  const definition = defineApplication({ name: 'workspace-cleanup', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '没有执行任务', program: implementProgram(program, { Component: () => null }),
  })] });
  const app = await boot(root, definition, async function* () { throw new Error('WORKSPACE_CLEANUP_MUST_NOT_START_MODEL'); });
  const failure = new Error('WORKSPACE_SOURCE_RELEASE_FAILED');
  const workspace = app.host.workspace(app.workspace.workspaceId);
  workspace.share(new Observable<never>(() => () => { throw failure; }));
  const directory = join(root, 'other'); await mkdir(directory);
  const other = app.host.workspace((await app.host.createWorkspace({ directory })).workspaceId);
  let stopped = false;
  other.share(new Observable<never>(() => () => { stopped = true; }));
  const hasCause = (error: unknown): boolean => error === failure || error !== null && typeof error === 'object'
    && 'errors' in error && Array.isArray(error.errors) && error.errors.some(hasCause);
  try {
    assert.equal(await app.native.workspaceRegistry.delete(WorkspaceId(workspace.id)), true);
    assert.equal(app.native.workspaceRegistry.get(WorkspaceId(workspace.id)), undefined, '观察清理失败不伪造已提交登记仍存在');
    assert.throws(() => workspace.share(of(1)), /REACTIVE_SCOPE_DISPOSED/);
    await assert.rejects(app.dispose(), hasCause);
    assert.equal(stopped, true, '一个工作区清理失败仍释放其他工作区');
  } finally { await app.dispose().catch(() => {}); }
});

test('正式资源和反应直接归工作区所有，跨环境复用且隔离，登记删除停止排队关系而不撤销已受理文件写入', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-workspace-capabilities-'));
  const quantity = join(root, 'quantity'), result = join(root, 'result');
  await writeFile(quantity, '42');
  const resource = defineResource({ name: 'workspace_quantity', params: z.null(), value: z.string().transform(Number) });
  let reads = 0, models = 0;
  const facts = implementResource(resource, { read: async (_params, { signal }) => { reads++; return readFile(quantity, { encoding: 'utf8', signal }); } });
  const commands = new Subject<number>(), accepted = Promise.withResolvers<void>(), release = Promise.withResolvers<void>(), committed = Promise.withResolvers<void>();
  const started: number[] = [];
  const reaction = defineReaction({ name: 'workspace_writer', params: z.null(), event: z.number(), result: z.number() });
  const writer = implementReaction(reaction, { observe: () => commands, operator: concatMap, async execute(value) {
    started.push(value); accepted.resolve(); await release.promise;
    await writeFile(result, String(value)); committed.resolve(); return value;
  } });
  const program = defineProgram({ input: z.null(), surface: defineSurface({ actions: {}, views: { quantity: resourceView(z.number()) } }) });
  const implementation = implementProgram(program, { Component({ workspace }) {
    const observed = useResource(workspace.resource(facts, null));
    return Expose({ surface: program.surface, value: { views: { quantity: observed.observations$ }, actions: {} } });
  } });
  const definition = defineApplication({ name: 'workspace-capabilities', admit: async () => {}, environments: [
    defineEnvironment({ id: 'one', instructions: '共用工作区业务资源', program: implementation }),
    defineEnvironment({ id: 'two', instructions: '共用工作区业务资源', program: implementation }),
  ] });
  const app = await boot(root, definition, async function* () { models++; yield* text('没有模型任务'); });
  const workspace = app.host.workspace(app.workspace.workspaceId);
  const current = workspace.resource(facts, null), running = workspace.reaction(writer, null);
  const watch = current.observations$.subscribe();
  const run = running.start();
  try {
    assert.equal(await current.read(), 42, '没有Session时也能定义并读取工作区资源');
    const one = await app.host.create('one', { workspaceId: workspace.id });
    const two = await app.host.create('two', { workspaceId: workspace.id });
    try {
      assert.equal(await one.readView('quantity'), 42); assert.equal(await two.readView('quantity'), 42);
      assert.equal(reads, 1, '两个环境与普通函数共用同一正式资源读取');
      const directory = join(root, 'isolated'); await mkdir(directory);
      const registration = await app.host.createWorkspace({ directory });
      const isolated = app.host.workspace(registration.workspaceId).resource(facts, null);
      const isolatedWatch = isolated.observations$.subscribe();
      try {
        assert.equal(await isolated.read(), 42); assert.equal(reads, 2, '相同实现和目标在不同工作区隔离');
        commands.next(1); commands.next(2); await accepted.promise;
        await app.native.workspaceRegistry.delete(WorkspaceId(workspace.id));
        assert.equal((await run.settled).status, 'stopped');
        assert.deepEqual(started, [1]);
        assert.throws(() => current.peek(), /REACTIVE_SCOPE_DISPOSED/);
        assert.throws(() => workspace.resource(facts, null), /REACTIVE_SCOPE_DISPOSED/);
        assert.equal(await isolated.read(), 42);
        release.resolve(); await committed.promise;
        assert.equal(await readFile(result, 'utf8'), '1', '已经受理的业务写入仍由原函数完成');
        assert.deepEqual(started, [1], '排队的第二项没有越过已结束的反应关系');
      } finally { isolatedWatch.unsubscribe(); }
    } finally { one.release(); two.release(); }
    assert.equal(models, 0); assert.deepEqual(app.errors, []);
  } finally { release.resolve(); watch.unsubscribe(); commands.complete(); await app.dispose(); }
});

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

test('已有持久成员但Agent setup尚未完成时不能取得公开句柄，setup失败后沿原引用恢复同一创建', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-creation-boundary-'));
  const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
  const definition = defineApplication({ name: 'creation-boundary', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '构造与公开受理分别确认', program: implementProgram(program, { Component: () => null }),
  })] });
  let modelCalls = 0;
  const app = await boot(join(root, 'home'), definition, async function* () { modelCalls++; yield* text('无模型工作'); });
  const entered = Promise.withResolvers<void>(), released = Promise.withResolvers<void>();
  const failure = new Error('AGENT_SETUP_CHECKPOINT_FAILED'), sessionId = 'creating-session';
  let reference: CreationIncompleteError['reference'] | undefined;
  const unfinished = (error: unknown) => {
    if (!(error instanceof CreationIncompleteError)) return false;
    reference = error.reference;
    return reference.sessionId === sessionId && reference.workspaceId === app.workspace.workspaceId && reference.environmentId === 'work';
  };
  const remove = app.native.on('agent/created', async ({ agent }) => {
    if (agent.id !== sessionId) return;
    entered.resolve(); await released.promise; throw failure;
  });
  let removePublication = () => {};
  let creating: Promise<void> | undefined;
  try {
    creating = assert.rejects(app.host.create('work', { workspaceId: app.workspace.workspaceId, sessionId }), error => error === failure);
    await entered.promise;
    assert.ok(app.native.workspaceRegistry.get(WorkspaceId(app.workspace.workspaceId))!.sessionIds.includes(SessionId(sessionId)),
      '先有真实持久成员，再由原生Agent完成其全部setup');
    await assert.rejects(app.host.open(sessionId), unfinished);
    await assert.rejects(app.host.resume(sessionId), unfinished);
    await assert.rejects(app.host.service(sessionId, program), unfinished);
    assert.throws(() => app.host.conversation(sessionId), unfinished);
    released.resolve(); await creating;
    remove();
    await assert.rejects(app.host.open(sessionId), unfinished, 'setup失败后的冷读取不能把成员关系当作完成回执');
    let boundaryRead: Promise<{ issued: true } | { error: unknown }> | undefined;
    removePublication = app.native.on('session/event', (session, event) => {
      if (session.id !== sessionId || pluginRecordOf(event)?.type !== 'plugin:reactive/created') return;
      // 观察正式追加事件时，实际持久 flush 和原创建 Promise 仍未确认。
      boundaryRead = app.host.open(sessionId).then(handle => { handle.release(); return { issued: true as const }; }, error => ({ error }));
    });
    const recovered = await app.host.completeCreation(reference!);
    assert.ok(boundaryRead);
    const boundary = await boundaryRead;
    assert.ok('error' in boundary && unfinished(boundary.error), '内存中的完成记录还不是已 ACK 的创建结果');
    assert.equal(recovered.id, sessionId);
    const observer = await app.host.open(sessionId);
    observer.release(); recovered.release();
    assert.equal(modelCalls, 0);
    assert.deepEqual(app.errors, []);
  } finally { released.resolve(); remove(); removePublication(); await creating; await app.dispose(); }
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
