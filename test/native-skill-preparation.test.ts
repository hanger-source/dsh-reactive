import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, mkdir, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate as immediate } from 'node:timers/promises';
import { Fragment, createElement as h } from 'react';
import type { Context } from '@deepseek-ai/cordis';
import { assembleContextFor } from '@deepseek-ai/dsh-agent';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { SessionId } from '@deepseek-ai/dsh-session';
import type { SkillProviderControl } from '@deepseek-ai/dsh-skill';
import { BehaviorSubject, filter, firstValueFrom, from, timeout } from 'rxjs';
import { collectionSurface, composeSurface, defineApplication, defineEnvironment, defineProgram, defineSurface, z,
  type ApplicationDefinition, type AnyEnvironment } from '@hanger-source/dsh-reactive';
import { implementProgram, Module, Skill, useObservable } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';

const contract = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
const wait = <T>(work: Promise<T>) => firstValueFrom(from(work).pipe(timeout(3_000)));
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

test('集合模块共享真实目录和方法正文，单项退出保留依赖，最后退出撤销原生贡献', { timeout: 15_000 }, async () => {
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
  try {
    const workspace = await app.runtime.application.createWorkspace({ directory: root });
    const session = await app.runtime.application.create('work', { workspaceId: workspace.workspaceId });
    const members = session.collection('dependencies');
    const agent = app.native.get('agents')!.get(SessionId(session.id))!;
    const signal = new AbortController().signal;
    const assemble = () => wait(agent.ctx.systemPrompt.assemble(assembleContextFor(agent, signal)));
    const catalog = () => app.native.get('skills')!.list({ scope: agent, cwd: root, signal });
    const initial = await assemble();
    assert.deepEqual(await members.instances.read(), ['left', 'right']);
    const provider = (await catalog()).find(skill => skill.name === name)?.provider;
    assert.ok(provider);
    assert.equal(initial.contexts.filter(context => context.name === 'reactive:skill:' + name).length, 1);
    assert.ok(JSON.stringify(initial.contexts).includes('SHARED_METHOD_BODY'));

    const retained = firstValueFrom(members.instances$.pipe(filter(ids => ids.length === 1 && ids[0] === 'right'), timeout(3_000)));
    selected.next(['right']); await retained;
    const single = await assemble();
    assert.equal((await catalog()).find(skill => skill.name === name)?.provider, provider);
    assert.equal(single.contexts.filter(context => context.name === 'reactive:skill:' + name).length, 1);
    assert.ok(JSON.stringify(single.contexts).includes('SHARED_METHOD_BODY'));

    const removed = firstValueFrom(members.instances$.pipe(filter(ids => ids.length === 0), timeout(3_000)));
    selected.next([]); await removed;
    const absent = await assemble();
    assert.equal((await catalog()).some(skill => skill.name === name), false);
    assert.equal(absent.contexts.some(context => context.name === 'reactive:skill:' + name), false);
    assert.equal(app.models(), 0); assert.deepEqual(app.errors, []);
    session.release();
  } finally { selected.complete(); await app.runtime.dispose(); }
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
    const shared = two.ctx.systemPrompt.assemble(assembleContextFor(two, signal));
    const sharedSignal = await wait(entered[1]!.promise);
    selected.next('new-method');
    const current = await wait(changing);
    assert.equal(oldSignal.aborted, true); assert.equal(sharedSignal.aborted, false);
    assert.equal(control.signal.aborted, false); assert.equal(signal.aborted, false);
    assert.ok(JSON.stringify(current.contexts).includes('CURRENT_METHOD_BODY'));
    assert.equal(current.contexts.some(context => context.name === 'reactive:skill:old-method'), false);
    bodies[0]!.reject(new Error('OBSOLETE_PROVIDER_FAILED_LATE'));
    bodies[1]!.resolve('OTHER_SESSION_METHOD_BODY');
    assert.ok(JSON.stringify((await wait(shared)).contexts).includes('OTHER_SESSION_METHOD_BODY'));
    await immediate();
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
    assert.equal(cancelSignal.aborted, true); cancelled.gate.resolve();

    failure = new Error('CURRENT_METHOD_OFFLINE');
    await assert.rejects(wait(assemble()), error => error === failure);
    failure = undefined;
    assert.ok(JSON.stringify((await wait(assemble())).contexts).includes('CATALOG_CURRENT_BODY'));

    const closing = delay(), preparing = assemble();
    const disposed = assert.rejects(wait(preparing));
    const closingSignal = await wait(closing.entered.promise);
    await assert.doesNotReject(app.runtime.application.close(session.id), '会话关闭只拒绝在途准备，不使关闭失败');
    await disposed;
    assert.equal(closingSignal.aborted, true); assert.equal(control.signal.aborted, false);
    assert.equal(app.models(), 0); assert.deepEqual(app.errors, []);
    session.release();
  } finally {
    for (const gate of gates) gate.resolve();
    unregister(); await assert.doesNotReject(app.runtime.dispose(), '准备试验退出后完整释放Runtime');
  }
});
