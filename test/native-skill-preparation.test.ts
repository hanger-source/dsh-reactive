import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, mkdir, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate as immediate } from 'node:timers/promises';
import { Fragment, createElement as h } from 'react';
import { symbols, type Context } from '@deepseek-ai/cordis';
import { assembleContextFor } from '@deepseek-ai/dsh-agent';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { SessionId } from '@deepseek-ai/dsh-session';
import type { SkillProviderControl } from '@deepseek-ai/dsh-skill';
import { FileSystemSkillProvider } from '@deepseek-ai/dsh-skill-filesystem';
import type SystemPrompt from '@deepseek-ai/dsh-system-prompt';
import { BehaviorSubject, filter, firstValueFrom, from, timeout, type Subscription } from 'rxjs';
import { collectionSurface, composeSurface, defineApplication, defineEnvironment, defineProgram, defineSurface, z,
  type ApplicationDefinition, type AnyEnvironment } from '@hanger-source/dsh-reactive';
import type { Eligibility, Observation } from '@hanger-source/dsh-reactive';
import { implementProgram, Module, Skill, useObservable } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import { createNativeSkills } from '@hanger-source/dsh-reactive-dsh/skills';

const contract = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
const wait = <T>(work: Promise<T>) => firstValueFrom(from(work).pipe(timeout(3_000)));
const permitted = (generation: number): Observation<Eligibility> => ({ status: 'ready', generation, observedAt: Date.now(), value: { allowed: true } });
const denied = (generation: number): Observation<Eligibility> => ({ status: 'ready', generation, observedAt: Date.now(), value: { allowed: false, reason: '方法当前不可用' } });
async function start<E extends readonly AnyEnvironment[]>(root: string, definition: ApplicationDefinition<E>) {
  let native!: Context, models = 0;
  const errors: unknown[] = [];
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'skill-preparation',
    installNative(ctx) { native = ctx; }, onError: event => errors.push(event.error),
    model: nativeModel({ provider: 'skill-preparation', model: 'unused', create: () => new class extends LlmAdapter {
      async *stream(): AsyncGenerator<never> { models++; throw new Error('PREPARATION_MUST_NOT_REQUEST_MODEL'); }
    }() }),
  });
  return { runtime, native, errors, models: () => models };
}

test('集合模块共享真实目录和方法正文，最后退出无需装配即撤贡献，原owner仍等provider释放', { timeout: 15_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-skill-contributions-'));
  const directory = join(root, 'method'), name = 'shared-method';
  await mkdir(directory);
  await writeFile(join(directory, 'SKILL.md'), '---\nname: ' + name
    + '\ndescription: 多模块共同依赖的实际方法\n---\n\nSHARED_METHOD_BODY\n');
  const selected = new BehaviorSubject<readonly string[]>(['left', 'right']);
  const empty = defineSurface({ views: {}, actions: {} });
  const composed = composeSurface({ dependencies: collectionSurface(empty) });
  const program = defineProgram({ input: z.null(), surface: composed.surface });
  function Dependency({ member }: { member: string }) {
    return h(Fragment, null, Skill({ directory: member === 'left' ? directory : directory + '/.' }), Skill({ name }));
  }
  const app = await start(root, defineApplication({ name: 'skill-contributions', admit: async () => {}, environments: [
    defineEnvironment({ id: 'work', instructions: '共同依赖保留会话级原生归属。',
      program: implementProgram(program, { Component() {
        const observed = useObservable(() => selected, []);
        return observed.status === 'ready' ? h(Fragment, null, observed.value.map(member =>
          h(Module, { key: member, mount: composed.mounts.dependencies.instance(member) }, h(Dependency, { member })))) : null;
      } }) }),
  ] }));
  const capability = createNativeSkills({ service: 'provided' });
  const subscriptions: Subscription[] = [];
  const releasedContexts: string[] = [];
  const prompt: SystemPrompt = Reflect.get(app.native.get('systemPrompt')!, symbols.original);
  const registerContext = prompt.context;
  prompt.context = function (context) {
    const release = registerContext.call(this, context);
    return () => {
      release();
      if (context.name === 'reactive:skill:' + name) releasedContexts.push(context.name);
    };
  };
  const disposeProvider = FileSystemSkillProvider.prototype.dispose;
  const closing = Promise.withResolvers<void>(), releaseClose = Promise.withResolvers<void>();
  let provider: string | undefined, providerClosing: Promise<void> | undefined, providerClosed = false;
  FileSystemSkillProvider.prototype.dispose = function () {
    const actual = disposeProvider.call(this);
    if (this.name !== provider) return actual;
    if (!providerClosing) {
      closing.resolve();
      providerClosing = actual.then(async () => { await releaseClose.promise; providerClosed = true; });
    }
    return providerClosing;
  };
  try {
    await capability.install(app.native, join(root, 'home'));
    const workspace = await app.runtime.application.createWorkspace({ directory: root });
    const session = await app.runtime.application.create('work', { workspaceId: workspace.workspaceId });
    const members = session.collection('dependencies');
    const agent = app.native.get('agents')!.get(SessionId(session.id))!;
    const signal = new AbortController().signal;
    const assemble = () => wait(agent.ctx.systemPrompt.assemble(assembleContextFor(agent, signal)));
    const catalog = () => app.native.get('skills')!.list({ scope: agent, cwd: root, signal });
    const initial = await assemble();
    assert.deepEqual(await members.instances.read(), ['left', 'right']);
    provider = (await catalog()).find(skill => skill.name === name)?.provider;
    assert.ok(provider);
    assert.equal(initial.contexts.filter(context => context.name === 'reactive:skill:' + name).length, 1);
    assert.ok(JSON.stringify(initial.contexts).includes('SHARED_METHOD_BODY'));
    const reader = capability.forSession(session), body = reader.definition(name);
    subscriptions.push(reader.catalog.values$.subscribe(), body.values$.subscribe());
    assert.equal((await body.read())?.content.trim(), 'SHARED_METHOD_BODY');

    const retained = firstValueFrom(members.instances$.pipe(filter(ids => ids.length === 1 && ids[0] === 'right'), timeout(3_000)));
    selected.next(['right']); await retained;
    assert.equal((await catalog()).find(skill => skill.name === name)?.provider, provider);
    assert.deepEqual(releasedContexts, [], '还有一个声明时不退休共享named context');
    assert.equal(providerClosing, undefined, '还有一个声明时不退休共享目录provider');
    const single = await assemble();
    assert.equal((await catalog()).find(skill => skill.name === name)?.provider, provider);
    assert.equal(single.contexts.filter(context => context.name === 'reactive:skill:' + name).length, 1);
    assert.ok(JSON.stringify(single.contexts).includes('SHARED_METHOD_BODY'));

    const removed = firstValueFrom(members.instances$.pipe(filter(ids => ids.length === 0), timeout(3_000)));
    const catalogRemoved = firstValueFrom(reader.catalog.values$.pipe(filter(value => !value.skills.some(skill => skill.name === name)), timeout(3_000)));
    const bodyRemoved = firstValueFrom(body.values$.pipe(filter(value => value === undefined), timeout(3_000)));
    selected.next([]); await removed;
    await wait(closing.promise);
    await Promise.all([catalogRemoved, bodyRemoved]);
    t.diagnostic('skills.withdrawn 原生catalog与同源资源已撤贡献，provider实际释放回执仍未完成');
    assert.equal((await catalog()).some(skill => skill.name === name), false);
    assert.deepEqual(releasedContexts, ['reactive:skill:' + name], '最后退出在任何后续装配前释放原named context');
    assert.equal(providerClosed, false);
    // 只按正式scope读取已经注册的prompt；不提供agent，因此不进入Program准备续体。
    const absent = await wait(agent.ctx.systemPrompt.assemble({ scope: agent, signal }));
    assert.equal(absent.contexts.some(context => context.name === 'reactive:skill:' + name), false);
    let stopped = false;
    const stopping = app.runtime.application.close(session.id).then(() => { stopped = true; });
    await immediate(); assert.equal(stopped, false, '原Agent关闭仍加入已退休provider的同一清理回执');
    releaseClose.resolve(); await wait(stopping);
    assert.equal(providerClosed, true);
    assert.equal(app.models(), 0); assert.deepEqual(app.errors, []);
    session.release();
  } finally {
    releaseClose.resolve(); subscriptions.forEach(subscription => subscription.unsubscribe()); selected.complete();
    try { await capability.dispose(); await app.runtime.dispose(); }
    finally { prompt.context = registerContext; FileSystemSkillProvider.prototype.dispose = disposeProvider; }
  }
});

test('撤下named Skill时一个原生释放失败仍释放其他贡献，准备和owner退出保留原错误', { timeout: 15_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-skill-withdrawal-failure-'));
  const selected = new BehaviorSubject(true), names = ['first-method', 'second-method'];
  const app = await start(root, defineApplication({ name: 'skill-withdrawal-failure', admit: async () => {}, environments: [
    defineEnvironment({ id: 'work', instructions: '方法贡献退出的清理责任', program: implementProgram(contract, { Component() {
      const active = useObservable(() => selected, []);
      return active.status === 'ready' && active.value ? h(Fragment, null, names.map(name => h(Skill, { key: name, name }))) : null;
    } }) }),
  ] }));
  const failure = new Error('NATIVE_NAMED_SKILL_RELEASE_FAILED');
  const released = new Set<string>(), releasedAll = Promise.withResolvers<void>();
  const prompt: SystemPrompt = Reflect.get(app.native.get('systemPrompt')!, symbols.original);
  const registerContext = prompt.context;
  prompt.context = function (context) {
    const release = registerContext.call(this, context);
    return () => {
      release();
      if (!names.some(name => context.name === 'reactive:skill:' + name)) return;
      released.add(context.name);
      if (released.size === names.length) releasedAll.resolve();
      if (context.name === 'reactive:skill:' + names[0]) throw failure;
    };
  };
  const registrations = names.map(name => app.native.get('skills')!.register({ name, description: name,
    source: 'runtime', content: 'CURRENT_METHOD_' + name }));
  const hasCause = (error: unknown): boolean => error === failure || error instanceof AggregateError && error.errors.some(hasCause);
  try {
    const workspace = await app.runtime.application.createWorkspace({ directory: root });
    const session = await app.runtime.application.create('work', { workspaceId: workspace.workspaceId });
    const agent = app.native.get('agents')!.get(SessionId(session.id))!, signal = new AbortController().signal;
    const assemble = () => agent.ctx.systemPrompt.assemble(assembleContextFor(agent, signal));
    const initial = await wait(assemble());
    assert.equal(initial.contexts.filter(context => context.name.startsWith('reactive:skill:')).length, names.length);
    selected.next(false); await wait(releasedAll.promise);
    const absent = await wait(agent.ctx.systemPrompt.assemble({ scope: agent, signal }));
    assert.equal(absent.contexts.some(context => context.name.startsWith('reactive:skill:')), false);
    await assert.rejects(wait(assemble()), hasCause);
    await assert.rejects(app.runtime.dispose(), hasCause);
    assert.equal(app.models(), 0);
  } finally {
    selected.complete(); registrations.forEach(release => release());
    await app.runtime.dispose().catch(() => {}); prompt.context = registerContext;
  }
});

test('真实Skill准备替换声明，不配合取消的共享provider不阻塞新集合或取消另一会话', { timeout: 15_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-skill-preparation-'));
  const selected = new BehaviorSubject('old-method');
  function Current() {
    const name = useObservable(() => selected, []);
    return name.status === 'ready' ? Skill({ name: name.value }) : null;
  }
  const app = await start(root, defineApplication({ name: 'skill-preparation', admit: async () => {}, environments: [
    defineEnvironment({ id: 'changing', instructions: '声明替换', program: implementProgram(contract, { Component: Current }) }),
    defineEnvironment({ id: 'shared', instructions: '共享来源', program: implementProgram(contract, { Component: () => Skill({ name: 'old-method' }) }) }),
  ] }));
  const entered = [Promise.withResolvers<AbortSignal>(), Promise.withResolvers<AbortSignal>()];
  const bodies = [Promise.withResolvers<string>(), Promise.withResolvers<string>()];
  let calls = 0, control!: SkillProviderControl;
  const summary = (name: string) => ({ name, description: name, source: 'custom', provider: 'shared-methods',
    invocation: { modelInvocable: true, userInvocable: false } });
  const unregister = app.native.get('skills')!.registerProvider(current => {
    control = current;
    return { name: 'shared-methods',
      async list() { return ['old-method', 'new-method'].map(name => ({ ...summary(name), rank: 100, locator: name })); },
      async get(candidate, options) {
        if (candidate.name === 'new-method') return { ...summary(candidate.name), content: 'CURRENT_METHOD_BODY' };
        const index = calls++;
        if (index < bodies.length) {
          assert.ok(options.signal); entered[index]!.resolve(options.signal);
          // 真实 Registry 负责结束查询；共享来源故意不配合单次查询的取消。
          return { ...summary(candidate.name), content: await bodies[index]!.promise };
        }
        return { ...summary(candidate.name), content: 'SHARED_METHOD_STILL_AVAILABLE' };
      },
    };
  });
  try {
    const workspace = await app.runtime.application.createWorkspace({ directory: root });
    const first = await app.runtime.application.create('changing', { workspaceId: workspace.workspaceId });
    const second = await app.runtime.application.create('shared', { workspaceId: workspace.workspaceId });
    const one = app.native.get('agents')!.get(SessionId(first.id))!, two = app.native.get('agents')!.get(SessionId(second.id))!;
    const signal = new AbortController().signal;
    const changing = one.ctx.systemPrompt.assemble(assembleContextFor(one, signal));
    const oldSignal = await wait(entered[0]!.promise);
    const firstPreparation = (await first.progress.read()).preparation!;
    assert.ok(firstPreparation.waiting.some(wait => wait.kind === 'skill' && wait.name === 'old-method'));
    const shared = two.ctx.systemPrompt.assemble(assembleContextFor(two, signal));
    const sharedSignal = await wait(entered[1]!.promise);
    const secondPreparation = (await second.progress.read()).preparation!;
    assert.notEqual(secondPreparation.id, firstPreparation.id);
    selected.next('new-method');
    const current = await wait(changing);
    assert.equal((await first.progress.read()).preparation, null);
    assert.equal((await second.progress.read()).preparation?.id, secondPreparation.id, '另一会话仍在原准备中等待自己的读取');
    assert.equal(oldSignal.aborted, true); assert.equal(sharedSignal.aborted, false);
    assert.equal(control.signal.aborted, false); assert.equal(signal.aborted, false);
    assert.ok(JSON.stringify(current.contexts).includes('CURRENT_METHOD_BODY'));
    assert.equal(current.contexts.some(context => context.name === 'reactive:skill:old-method'), false);
    bodies[0]!.reject(new Error('OBSOLETE_PROVIDER_FAILED_LATE'));
    bodies[1]!.resolve('OTHER_SESSION_METHOD_BODY');
    assert.ok(JSON.stringify((await wait(shared)).contexts).includes('OTHER_SESSION_METHOD_BODY'));
    assert.equal((await second.progress.read()).preparation, null);
    await immediate();
    assert.equal((await first.progress.read()).preparation, null, '被替换读取的迟到失败不能复活旧准备');
    assert.ok(JSON.stringify((await one.ctx.systemPrompt.assemble(assembleContextFor(one, signal))).contexts).includes('CURRENT_METHOD_BODY'));
    assert.equal((await app.native.get('skills')!.get('old-method', { scope: two, cwd: root, signal }))?.content,
      'SHARED_METHOD_STILL_AVAILABLE');
    assert.equal(app.models(), 0); assert.deepEqual(app.errors, []);
    first.release(); second.release();
  } finally {
    for (const body of bodies) body.resolve('released');
    selected.complete(); unregister(); await app.runtime.dispose();
  }
});

test('Skill准备随真实工作目录和catalog重读，当前失败、步骤取消及会话关闭保留原生边界', { timeout: 15_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-skill-directory-'));
  const directories = [join(root, 'one'), join(root, 'two')];
  for (const [index, directory] of directories.entries()) {
    await mkdir(directory); await writeFile(join(directory, 'method.txt'), 'DIRECTORY_BODY_' + index);
  }
  const app = await start(root, defineApplication({ name: 'skill-directory', admit: async () => {}, environments: [
    defineEnvironment({ id: 'work', instructions: '当前工作目录的方法',
      program: implementProgram(contract, { Component: () => Skill({ name: 'workspace-method' }) }) }),
  ] }));
  const summary = { name: 'workspace-method', description: '当前目录的真实文件', source: 'custom', provider: 'directory-method',
    invocation: { modelInvocable: true, userInvocable: false } };
  const gates: ReturnType<typeof Promise.withResolvers<void>>[] = [];
  let pending: { entered: ReturnType<typeof Promise.withResolvers<AbortSignal>>; gate: ReturnType<typeof Promise.withResolvers<void>> } | undefined;
  const delay = () => {
    const next = { entered: Promise.withResolvers<AbortSignal>(), gate: Promise.withResolvers<void>() };
    gates.push(next.gate); pending = next; return next;
  };
  let control!: SkillProviderControl, failure: Error | undefined;
  const unregister = app.native.get('skills')!.registerProvider(current => {
    control = current;
    return { name: summary.provider,
      async list() { return [{ ...summary, rank: 100, locator: 'method.txt' }]; },
      async get(_candidate, options) {
        const current = pending; pending = undefined;
        if (current) { assert.ok(options.signal); current.entered.resolve(options.signal); await current.gate.promise; }
        if (failure) throw failure;
        return { ...summary, content: await readFile(join(options.cwd!, 'method.txt'), 'utf8') };
      },
    };
  });
  try {
    const workspace = await app.runtime.application.createWorkspace({ directory: directories[0]! });
    const session = await app.runtime.application.create('work', { workspaceId: workspace.workspaceId });
    const agent = app.native.get('agents')!.get(SessionId(session.id))!;
    const assemble = (signal = new AbortController().signal) => agent.ctx.systemPrompt.assemble(assembleContextFor(agent, signal));
    const oldDirectory = delay(), changingDirectory = assemble();
    const oldSignal = await wait(oldDirectory.entered.promise);
    const [, prepared] = await Promise.all([
      agent.ctx.get('workingDirectory')!.set(agent, directories[1]!), wait(changingDirectory),
    ]);
    assert.ok(JSON.stringify(prepared.contexts).includes('DIRECTORY_BODY_1'));
    assert.equal(oldSignal.aborted, true);
    oldDirectory.gate.reject(new Error('OLD_DIRECTORY_FAILED_LATE'));

    const oldCatalog = delay(), changingCatalog = assemble();
    const catalogSignal = await wait(oldCatalog.entered.promise);
    await writeFile(join(directories[1]!, 'method.txt'), 'CATALOG_CURRENT_BODY');
    control.invalidate();
    assert.ok(JSON.stringify((await wait(changingCatalog)).contexts).includes('CATALOG_CURRENT_BODY'));
    assert.equal(catalogSignal.aborted, true);
    oldCatalog.gate.resolve();

    const cancelled = delay(), step = new AbortController(), cancellation = new Error('STEP_CANCELLED');
    const cancelResult = assemble(step.signal), rejected = assert.rejects(wait(cancelResult), error => error === cancellation);
    const cancelSignal = await wait(cancelled.entered.promise);
    step.abort(cancellation); await rejected;
    assert.equal((await session.progress.read()).preparation, null, '取消步骤立即撤下准备观察，不等不合作的来源结束');
    assert.equal(cancelSignal.aborted, true); cancelled.gate.resolve();

    failure = new Error('CURRENT_METHOD_OFFLINE');
    await assert.rejects(wait(assemble()), error => error === failure);
    assert.equal((await session.progress.read()).preparation, null, '当前读取失败不保留过期等待');
    failure = undefined;
    assert.ok(JSON.stringify((await wait(assemble())).contexts).includes('CATALOG_CURRENT_BODY'));

    const closing = delay(), preparing = assemble();
    const disposed = assert.rejects(wait(preparing));
    const closingSignal = await wait(closing.entered.promise);
    await assert.doesNotReject(app.runtime.application.close(session.id), '会话关闭只拒绝在途准备，不使关闭失败');
    await disposed;
    assert.equal((await session.progress.read()).preparation, null, 'Agent退出后冷图不保留活跃准备');
    assert.equal(closingSignal.aborted, true); assert.equal(control.signal.aborted, false);
    assert.equal(app.models(), 0); assert.deepEqual(app.errors, []);
    session.release();
  } finally {
    for (const gate of gates) gate.resolve();
    unregister(); await assert.doesNotReject(app.runtime.dispose(), '准备试验退出后完整释放Runtime');
  }
});

for (const unavailable of ['denied', 'pending'] as const) test('正文读取期间资格' + unavailable + '后恢复仍取消旧读，并采用真实文件的当前正文', { timeout: 15_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-skill-qualification-'));
  const name = 'qualified-method', file = join(root, 'method.txt');
  await writeFile(file, 'OBSOLETE_QUALIFIED_BODY');
  const qualification = new BehaviorSubject<Observation<Eligibility>>(permitted(1));
  const alternate = new BehaviorSubject<Observation<Eligibility>>(permitted(1));
  const app = await start(root, defineApplication({ name: 'skill-qualification-' + unavailable, admit: async () => {}, environments: [
    defineEnvironment({ id: 'work', instructions: '资格变化在原生正文读取边界生效',
      program: implementProgram(contract, { Component: () => h(Fragment, null,
        Skill({ name, eligibility: qualification }), Skill({ name, eligibility: alternate })) }) }),
  ] }));
  const entered = Promise.withResolvers<AbortSignal>(), release = Promise.withResolvers<void>();
  const summary = { name, description: name, source: 'custom', provider: 'qualified-files',
    invocation: { modelInvocable: true, userInvocable: false } };
  let calls = 0;
  const unregister = app.native.get('skills')!.registerProvider(() => ({ name: summary.provider,
    async list() { return [{ ...summary, rank: 100, locator: file }]; },
    async get(_candidate, options) {
      const content = await readFile(file, 'utf8');
      if (++calls === 1) {
        assert.ok(options.signal); entered.resolve(options.signal);
        // 已读到旧文件，来源故意不响应取消；Registry必须让新准备独立继续。
        await release.promise;
      }
      return { ...summary, content };
    },
  }));
  try {
    const workspace = await app.runtime.application.createWorkspace({ directory: root });
    const session = await app.runtime.application.create('work', { workspaceId: workspace.workspaceId });
    const agent = app.native.get('agents')!.get(SessionId(session.id))!, signal = new AbortController().signal;
    const assemble = () => agent.ctx.systemPrompt.assemble(assembleContextFor(agent, signal));
    const preparing = assemble(), oldSignal = await wait(entered.promise);
    await writeFile(file, 'CURRENT_QUALIFIED_BODY');
    qualification.next(unavailable === 'denied' ? denied(2) : { status: 'pending', generation: 2 });
    qualification.next(permitted(3));
    const current = await wait(preparing);
    assert.equal(oldSignal.aborted, true, '另一来源一直有效，选中来源的瞬态撤销仍永久取消该次Registry读取');
    assert.equal(signal.aborted, false);
    assert.ok(calls >= 2, '恢复后发生新的真实文件读取');
    assert.ok(JSON.stringify(current.contexts).includes('CURRENT_QUALIFIED_BODY'));
    assert.equal(JSON.stringify(current.contexts).includes('OBSOLETE_QUALIFIED_BODY'), false);
    release.resolve(); await immediate();
    const afterLateResult = await wait(assemble());
    assert.ok(JSON.stringify(afterLateResult.contexts).includes('CURRENT_QUALIFIED_BODY'));
    assert.equal(JSON.stringify(afterLateResult.contexts).includes('OBSOLETE_QUALIFIED_BODY'), false);
    assert.equal(app.models(), 0); assert.deepEqual(app.errors, []);
    session.release();
  } finally {
    release.resolve(); qualification.complete(); alternate.complete(); unregister(); await app.runtime.dispose();
  }
});

test('同name和规范化目录的不同资格独立贡献，最后退出才释放原生接点且无关方法继续装配', { timeout: 15_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-skill-qualified-contributions-'));
  const directory = join(root, 'method'), name = 'multi-qualified-method', unrelated = 'unrelated-method';
  await mkdir(directory);
  await writeFile(join(directory, 'SKILL.md'), '---\nname: ' + name + '\ndescription: 多资格贡献的真实目录\n---\n\nMULTI_QUALIFIED_BODY\n');
  const left = new BehaviorSubject<Observation<Eligibility>>(permitted(1));
  const right = new BehaviorSubject<Observation<Eligibility>>(permitted(1));
  const app = await start(root, defineApplication({ name: 'skill-qualified-contributions', admit: async () => {}, environments: [
    defineEnvironment({ id: 'work', instructions: '目标共享原生接点，资格保留各自来源',
      program: implementProgram(contract, { Component: () => h(Fragment, null,
        Skill({ directory, eligibility: left }), Skill({ directory: directory + '/.', eligibility: right }),
        Skill({ name, eligibility: left }), Skill({ name, eligibility: right }), Skill({ name: unrelated })) }) }),
  ] }));
  const unregister = app.native.get('skills')!.register({ name: unrelated, description: unrelated, source: 'runtime', content: 'UNRELATED_BODY' });
  const prompt: SystemPrompt = Reflect.get(app.native.get('systemPrompt')!, symbols.original);
  const registerContext = prompt.context, disposeProvider = FileSystemSkillProvider.prototype.dispose;
  let registeredContexts = 0, releasedContexts = 0, provider: string | undefined;
  const retiredProviders = new Map<FileSystemSkillProvider, Promise<void>>();
  const withdrawn = Promise.withResolvers<void>();
  prompt.context = function (context) {
    const release = registerContext.call(this, context);
    if (context.name === 'reactive:skill:' + name) registeredContexts++;
    return () => {
      release();
      if (context.name === 'reactive:skill:' + name) { releasedContexts++; withdrawn.resolve(); }
    };
  };
  FileSystemSkillProvider.prototype.dispose = function () {
    const disposal = disposeProvider.call(this);
    if (this.name === provider) {
      // 原生控制信号与插件effect都可请求释放；同一实例必须返回同一watcher清理回执。
      const previous = retiredProviders.get(this);
      if (previous) assert.equal(disposal, previous);
      else retiredProviders.set(this, disposal);
    }
    return disposal;
  };
  try {
    const workspace = await app.runtime.application.createWorkspace({ directory: root });
    const session = await app.runtime.application.create('work', { workspaceId: workspace.workspaceId });
    const agent = app.native.get('agents')!.get(SessionId(session.id))!, signal = new AbortController().signal;
    const assemble = () => wait(agent.ctx.systemPrompt.assemble(assembleContextFor(agent, signal)));
    const catalog = () => app.native.get('skills')!.list({ scope: agent, cwd: root, signal });
    const initial = await assemble();
    provider = (await catalog()).find(skill => skill.name === name)?.provider;
    assert.ok(provider); assert.equal(registeredContexts, 1);
    assert.ok(JSON.stringify(initial.contexts).includes('MULTI_QUALIFIED_BODY'));
    left.next(denied(2));
    const retained = await assemble();
    assert.equal((await catalog()).find(skill => skill.name === name)?.provider, provider);
    assert.equal(registeredContexts, 1, '同name另一资格保留原context身份');
    assert.equal(releasedContexts, 0); assert.equal(retiredProviders.size, 0, '规范化同目录另一资格保留原provider');
    assert.ok(JSON.stringify(retained.contexts).includes('MULTI_QUALIFIED_BODY'));

    right.next({ status: 'pending', generation: 2 });
    await wait(withdrawn.promise);
    // 先从原生公开入口观察即时撤贡献；没有agent，不触发后续Program准备。
    const absent = await wait(agent.ctx.systemPrompt.assemble({ scope: agent, signal }));
    assert.equal(absent.contexts.some(context => context.name === 'reactive:skill:' + name), false);
    const remaining = await assemble();
    assert.equal((await catalog()).some(skill => skill.name === name), false);
    assert.equal(releasedContexts, 1); assert.equal(retiredProviders.size, 1, '最后有效贡献退出只退休同一个真实provider实例');
    await wait(Promise.all([...retiredProviders.values()]));
    assert.ok(JSON.stringify(remaining.contexts).includes('UNRELATED_BODY'), '稳定pending和denied均不阻塞无关方法');
    assert.equal(remaining.contexts.some(context => context.name === 'reactive:skill:' + name), false);

    right.next({ status: 'error', generation: 3, error: new Error('QUALIFICATION_FACT_OFFLINE') });
    assert.ok(JSON.stringify((await assemble()).contexts).includes('UNRELATED_BODY'), '来源错误只撤本方法贡献');
    right.next(permitted(4));
    assert.ok(JSON.stringify((await assemble()).contexts).includes('MULTI_QUALIFIED_BODY'));
    assert.equal(app.models(), 0); assert.deepEqual(app.errors, []);
    session.release();
  } finally {
    left.complete(); right.complete(); unregister();
    try { await app.runtime.dispose(); }
    finally { prompt.context = registerContext; FileSystemSkillProvider.prototype.dispose = disposeProvider; }
  }
});

test('同资格来源的两会话各自持有owner，关闭一个只取消自己的真实正文读取', { timeout: 15_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-skill-qualified-owners-'));
  const name = 'owner-qualified-method', file = join(root, 'method.txt');
  await writeFile(file, 'OTHER_OWNER_CURRENT_BODY');
  const qualification = new BehaviorSubject<Observation<Eligibility>>(permitted(1));
  const app = await start(root, defineApplication({ name: 'skill-qualified-owners', admit: async () => {}, environments: [
    defineEnvironment({ id: 'work', instructions: '共享事实不共享会话取消权',
      program: implementProgram(contract, { Component: () => Skill({ name, eligibility: qualification }) }) }),
  ] }));
  const entered = [Promise.withResolvers<AbortSignal>(), Promise.withResolvers<AbortSignal>()];
  const releases = [Promise.withResolvers<void>(), Promise.withResolvers<void>()];
  const summary = { name, description: name, source: 'custom', provider: 'qualified-owner-files',
    invocation: { modelInvocable: true, userInvocable: false } };
  let calls = 0, control!: SkillProviderControl;
  const unregister = app.native.get('skills')!.registerProvider(current => {
    control = current;
    return { name: summary.provider,
      async list() { return [{ ...summary, rank: 100, locator: file }]; },
      async get(_candidate, options) {
        const index = calls++;
        if (index < entered.length) {
          assert.ok(options.signal); entered[index]!.resolve(options.signal); await releases[index]!.promise;
        }
        return { ...summary, content: await readFile(file, 'utf8') };
      },
    };
  });
  try {
    const workspace = await app.runtime.application.createWorkspace({ directory: root });
    const first = await app.runtime.application.create('work', { workspaceId: workspace.workspaceId });
    const second = await app.runtime.application.create('work', { workspaceId: workspace.workspaceId });
    const one = app.native.get('agents')!.get(SessionId(first.id))!, two = app.native.get('agents')!.get(SessionId(second.id))!;
    const signal = new AbortController().signal;
    const firstReading = one.ctx.systemPrompt.assemble(assembleContextFor(one, signal));
    const rejected = assert.rejects(wait(firstReading));
    const firstSignal = await wait(entered[0]!.promise);
    const secondReading = two.ctx.systemPrompt.assemble(assembleContextFor(two, signal));
    const secondSignal = await wait(entered[1]!.promise);
    await wait(app.runtime.application.close(first.id)); await rejected;
    assert.equal(firstSignal.aborted, true); assert.equal(secondSignal.aborted, false);
    assert.equal(control.signal.aborted, false); assert.equal(signal.aborted, false);
    releases[1]!.resolve();
    assert.ok(JSON.stringify((await wait(secondReading)).contexts).includes('OTHER_OWNER_CURRENT_BODY'));
    releases[0]!.resolve(); await immediate();
    qualification.next(denied(2));
    const absent = await wait(two.ctx.systemPrompt.assemble(assembleContextFor(two, signal)));
    assert.equal(absent.contexts.some(context => context.name === 'reactive:skill:' + name), false);
    qualification.next(permitted(3));
    const restored = await wait(two.ctx.systemPrompt.assemble(assembleContextFor(two, signal)));
    assert.ok(JSON.stringify(restored.contexts).includes('OTHER_OWNER_CURRENT_BODY'), '另一会话关闭后共享事实仍继续驱动存活owner');
    assert.equal(app.models(), 0); assert.deepEqual(app.errors, []);
    first.release(); second.release();
  } finally {
    releases.forEach(release => release.resolve()); qualification.complete(); unregister(); await app.runtime.dispose();
  }
});
