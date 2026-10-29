import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, mkdir, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { Context } from '@deepseek-ai/cordis';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import * as skillFilesystem from '@deepseek-ai/dsh-skill-filesystem';
import type { SkillProviderControl } from '@deepseek-ai/dsh-skill';
import { setImmediate as immediate } from 'node:timers/promises';
import { ReplaySubject, filter, firstValueFrom, timeout, type Subscription } from 'rxjs';
import { defineApplication, defineEnvironment, defineProgram, defineSurface, z,
  type Observation } from '@hanger-source/dsh-reactive';
import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel, type ConversationHandle } from '@hanger-source/dsh-reactive-dsh';
import { createNativeSkills, type SkillDefinition, type SkillCatalogSnapshot } from '@hanger-source/dsh-reactive-dsh/skills';
import { nativeSessionAccess } from '../packages/dsh/src/conversations/session-access.ts';
import { boot, text } from './native-support.ts';

test('Skill资源读取真实工作区和Agent层级，原生文件变化失效，关闭恢复与读取取消不启动模型', { timeout: 30_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-native-skills-'));
  const workspaces = [join(root, 'one'), join(root, 'two')];
  const file = (directory: string) => join(directory, '.dsh/skills/workspace-method/SKILL.md');
  const content = (body: string) => '---\nname: workspace-method\ndescription: 真实工作区方法 ' + body + '\n---\n' + body;
  for (const [index, directory] of workspaces.entries()) {
    await mkdir(join(directory, '.dsh/skills/workspace-method'), { recursive: true });
    await writeFile(file(directory), content('workspace-' + index));
  }
  const capability = createNativeSkills({ service: 'provided' });
  const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
  const definition = defineApplication({ name: 'native-skills', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '读取Skill不启动模型', program: implementProgram(program, { Component: () => null }),
  })] });
  const errors: unknown[] = [];
  let models = 0, native!: Context;
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'skills-owner',
    async installNative(ctx, home) {
      native = ctx;
      await ctx.plugin(skillFilesystem, { includeDefaultRoots: true, providerName: 'real-files',
        dshHome: join(root, 'user-dsh'), agentsHome: join(root, 'user-agents'),
        watch: true, watchStabilityThresholdMs: 20, watchPollIntervalMs: 10 });
      await capability.install(ctx, home);
    },
    onError: event => errors.push(event.error),
    model: nativeModel({ provider: 'native-skills', model: 'unused', create: () => new class extends LlmAdapter {
      stream() { models++; return text('只读流程不应调用模型'); }
    }() }),
  });
  let one: ConversationHandle<typeof program> | undefined, two: ConversationHandle<typeof program> | undefined;
  let resumed: ConversationHandle<typeof program> | undefined;
  const subscriptions: Subscription[] = [];
  try {
    const [firstWorkspace, secondWorkspace] = await Promise.all(workspaces.map(directory => runtime.application.createWorkspace({ directory })));
    one = await runtime.application.create('work', { workspaceId: firstWorkspace!.workspaceId });
    two = await runtime.application.create('work', { workspaceId: secondWorkspace!.workspaceId });
    const first = capability.forSession(one), second = capability.forSession(two);
    assert.equal(capability.forSession(one), first);
    assert.throws(() => capability.forSession({ id: one!.id }), /REACTIVE_NATIVE_SESSION_REQUIRED/);
    const body = first.definition('workspace-method');
    assert.equal(first.definition('workspace-method'), body);
    assert.throws(() => first.definition('../invalid'), /REACTIVE_SKILL_NAME_INVALID/);
    const values = new ReplaySubject<SkillDefinition | undefined>(1);
    const catalogs = new ReplaySubject<SkillCatalogSnapshot>(1);
    subscriptions.push(body.values$.subscribe(values), first.catalog.values$.subscribe(catalogs));
    const firstBody = await body.read();
    assert.equal(firstBody?.content.trim(), 'workspace-0');
    assert.equal((await second.definition('workspace-method').read())?.content.trim(), 'workspace-1');
    assert.equal((await first.catalog.read()).complete, true);

    const unregisterGlobal = native.get('skills')!.register({ name: 'human-method', source: 'runtime',
      description: '只有人工可调用', content: 'human', invocation: { modelInvocable: false, userInvocable: true } });
    const catalog = await firstValueFrom(catalogs.pipe(filter(value => value.skills.some(skill => skill.name === 'human-method')), timeout(5_000)));
    assert.deepEqual(catalog.skills.find(skill => skill.name === 'human-method')?.invocation,
      { modelInvocable: false, userInvocable: true });
    const agent = nativeSessionAccess(one).liveAgent()!;
    const initialProject = agent.session.header.cwd, permissionRoot = nativeSessionAccess(one).workspaceRoot();
    const unregisterPrivate = agent.ctx.get('skills')!.register({ name: 'private-method', source: 'runtime',
      description: '只属于第一个Agent', content: 'private' });
    assert.equal((await first.definition('private-method').read())?.content, 'private');
    assert.equal(await second.definition('private-method').read(), undefined);
    unregisterPrivate(); unregisterGlobal();
    assert.equal(await first.definition('private-method').read(), undefined);

    const movedBody = firstValueFrom(values.pipe(filter(value => value?.content.trim() === 'workspace-1'), timeout(5_000)));
    const movedCatalog = firstValueFrom(catalogs.pipe(filter(value => value.skills.some(skill =>
      skill.name === 'workspace-method' && skill.description.endsWith('workspace-1'))), timeout(5_000)));
    await agent.ctx.get('workingDirectory')!.set(agent, workspaces[1]!);
    await Promise.all([movedBody, movedCatalog]);
    assert.equal(agent.session.header.cwd, initialProject, '当前目录变更不改初始项目目录');
    assert.equal(nativeSessionAccess(one).workspaceRoot(), permissionRoot, 'Skill发现不改变文件权限root');
    const returnedBody = firstValueFrom(values.pipe(filter(value => value?.content.trim() === 'workspace-0'), timeout(5_000)));
    await agent.ctx.get('workingDirectory')!.set(agent, workspaces[0]!);
    await returnedBody;

    await writeFile(file(workspaces[0]!), content('changed-on-disk'));
    await firstValueFrom(values.pipe(filter(value => value?.content.trim() === 'changed-on-disk'), timeout(8_000)));
    assert.equal((await second.definition('workspace-method').read())?.content.trim(), 'workspace-1');

    await runtime.application.close(one.id);
    assert.equal(body.peek().status, 'pending');
    const cancel = new AbortController();
    const waiting = body.read(cancel.signal);
    cancel.abort(new Error('只取消这次读取'));
    await assert.rejects(waiting, /只取消这次读取/);
    assert.equal(nativeSessionAccess(one).liveAgent(), undefined);
    resumed = await runtime.application.resume(one.id, program);
    assert.notEqual(nativeSessionAccess(one).liveAgent(), agent);
    assert.equal((await body.read())?.content.trim(), 'changed-on-disk');
    assert.equal(await first.definition('private-method').read(), undefined);
    const observations: Observation<SkillDefinition | undefined>[] = [];
    subscriptions.push(body.observations$.subscribe(observation => observations.push(observation)));
    body.invalidate();
    assert.equal(observations.at(-1)?.status, 'pending');
    await body.read();
    one.release();
    assert.throws(() => body.peek());
    assert.equal((await second.definition('workspace-method').read())?.content.trim(), 'workspace-1');
    assert.equal(models, 0);
    assert.deepEqual(errors, []);
  } finally {
    for (const subscription of subscriptions) subscription.unsubscribe();
    resumed?.release(); one?.release(); two?.release();
    await runtime.dispose();
  }
});

test('正式Skill provider的不完整发现、失败恢复和跨代际迟到读取保持真实资源语义', { timeout: 20_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-skill-provider-'));
  const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
  const definition = defineApplication({ name: 'skill-provider', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: 'provider观察不产生模型执行', program: implementProgram(program, { Component: () => null }),
  })] });
  let models = 0;
  const app = await boot(root, definition, async function* () { models++; yield* text('unused'); });
  const capability = createNativeSkills({ service: 'provided' });
  let control!: SkillProviderControl, complete = false, failure = false;
  const entered = Promise.withResolvers<AbortSignal>(), release = Promise.withResolvers<void>();
  const directoryEntered = Promise.withResolvers<AbortSignal>(), directoryRelease = Promise.withResolvers<void>();
  let delay: 'directory' | 'generation' | undefined;
  const summary = { name: 'remote-method', description: '来自正式provider的业务方法', source: 'custom',
    provider: 'remote-business', invocation: { modelInvocable: true, userInvocable: false } };
  const unregister = app.native.get('skills')!.registerProvider(current => {
    control = current;
    return { name: summary.provider,
      async list() { return { complete, candidates: [{ ...summary, rank: 100, locator: 'remote-row' }] }; },
      async get(_candidate, options) {
        if (delay) {
          const current = delay; delay = undefined;
          const gate = current === 'directory' ? { entered: directoryEntered, release: directoryRelease } : { entered, release };
          assert.ok(options.signal); gate.entered.resolve(options.signal);
          // 第三方provider忽略取消，仍由原生Registry和资源代际隔离迟到正文。
          await gate.release.promise;
          return { ...summary, content: 'obsolete' };
        }
        if (failure) throw new Error('REMOTE_SKILL_OFFLINE');
        return { ...summary, content: 'current' };
      },
    };
  });
  let session: ConversationHandle<typeof program> | undefined, resumed: ConversationHandle<typeof program> | undefined;
  let peer: ConversationHandle<typeof program> | undefined;
  let subscription: Subscription | undefined;
  try {
    await capability.install(app.native, root);
    session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const skills = capability.forSession(session);
    assert.equal((await skills.catalog.read()).complete, false);
    complete = true; skills.catalog.invalidate();
    assert.equal((await skills.catalog.read()).complete, true);
    const body = skills.definition('remote-method');
    const values: string[] = [], errors: unknown[] = [];
    subscription = body.observations$.subscribe({ next: observation => {
      if (observation.status === 'ready' && observation.value) values.push(observation.value.content);
    }, error: error => errors.push(error) });
    assert.equal((await body.read())?.content, 'current');
    failure = true; control.invalidate();
    await assert.rejects(body.read(), /REMOTE_SKILL_OFFLINE/);
    assert.equal(body.peek().status, 'error');
    failure = false; control.invalidate();
    assert.equal((await body.read())?.content, 'current');
    peer = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    delay = 'directory'; control.invalidate();
    const directorySignal = await directoryEntered.promise;
    const peerAgent = nativeSessionAccess(peer).liveAgent()!;
    await peerAgent.ctx.get('workingDirectory')!.set(peerAgent, root);
    assert.equal(directorySignal.aborted, false, '另一Session的目录事件不撤销这次正式provider读取');
    const agent = nativeSessionAccess(session).liveAgent()!;
    await agent.ctx.get('workingDirectory')!.set(agent, root);
    assert.equal(directorySignal.aborted, true, '同一Session已提交的目录变化撤销旧目录读取');
    assert.equal((await body.read())?.content, 'current');
    directoryRelease.resolve(); await immediate();
    assert.equal(values.includes('obsolete'), false, '不合作provider迟到正文不覆盖当前目录资源');

    delay = 'generation'; control.invalidate();
    const oldSignal = await entered.promise;
    await app.host.close(session.id);
    assert.equal(oldSignal.aborted, true);
    assert.equal(body.peek().status, 'pending');
    resumed = await app.host.resume(session.id, program);
    assert.equal((await body.read())?.content, 'current');
    release.resolve(); await immediate();
    assert.equal(values.includes('obsolete'), false);
    assert.equal((await body.read())?.content, 'current');
    unregister();
    assert.equal(await body.read(), undefined);
    assert.equal(models, 0);
    assert.deepEqual(errors, []); assert.deepEqual(app.errors, []);
  } finally {
    release.resolve(); directoryRelease.resolve(); subscription?.unsubscribe(); unregister();
    resumed?.release(); session?.release(); peer?.release(); await capability.dispose(); await app.dispose();
  }
});
