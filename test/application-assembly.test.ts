import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { symbols, type Context, type Fiber } from '@deepseek-ai/cordis';
import LocalJobs from '@deepseek-ai/dsh-jobs-local';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { SessionId } from '@deepseek-ai/dsh-session';
import { of } from 'rxjs';
import { defineApplication, defineEnvironment, defineOperation, defineProgram, defineSurface, implementOperation, observeResource, z } from '@hanger-source/dsh-reactive';
import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { createRuntime, mountApplication, nativeModel, requireActivePlugin, type ApplicationAssembly, type ApplicationHost, type NativeCapability } from '@hanger-source/dsh-reactive-dsh';
import { createNativeJobs } from '@hanger-source/dsh-reactive-dsh/jobs';
import { createNativeSkills } from '@hanger-source/dsh-reactive-dsh/skills';
import { boot } from './native-support.ts';

function definition(name: string) {
  return defineApplication({ name, admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '仅显式读取业务与原生资源。',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), { Component: () => null }),
  })] });
}

/** 实际 Cordis 插件的安装、监听与异步退出；计数只观察原生生命周期。 */
function capability(name: string, beforeRelease?: () => void | Promise<void>, startup?: () => void | Promise<void>) {
  let installation: (Fiber & PromiseLike<Fiber>) | undefined;
  const homes: string[] = [];
  let installed = 0, released = 0, patches = 0;
  const selected: NativeCapability = {
    async install(parent: Context, home: string) {
      homes.push(home);
      installation = parent.plugin({ name, async apply(ctx: Context) {
        installed++;
        ctx.on('loader/patch-context', (_entry, next) => { patches++; return next(); });
        ctx.effect(() => async () => { await beforeRelease?.(); released++; });
        await startup?.();
      } });
      await requireActivePlugin(installation);
    },
    async dispose() {
      if (installation) { await installation.dispose(); await installation; }
    },
  };
  return { selected, homes, get installed() { return installed; }, get released() { return released; }, get patches() { return patches; } };
}

test('应用模块只在factory选择原生能力与恢复owner，独立Runtime安装一次并从文件恢复业务结果', { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-assembly-runtime-'));
  const workMethod = join(directory, 'work-method'), analysisMethod = join(directory, 'analysis-method');
  await Promise.all([workMethod, analysisMethod].map(path => mkdir(path, { recursive: true })));
  await Promise.all([
    writeFile(join(workMethod, 'SKILL.md'), '---\nname: assembly-work\ndescription: 工作方法\n---\n\nORIGINAL_WORK_METHOD\n'),
    writeFile(join(analysisMethod, 'SKILL.md'), '---\nname: assembly-analysis\ndescription: 分析方法\n---\n\nORIGINAL_ANALYSIS_METHOD\n'),
  ]);
  const contract = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
  const program = implementProgram(contract, { Component: () => null });
  const replacement = implementProgram(defineProgram({ input: contract.input, surface: contract.surface }), { Component: () => null });
  const workSkills = [workMethod];
  const work = defineEnvironment({ id: 'work', instructions: '使用已定义的工作方法', skills: workSkills, program });
  // Environment必须已经捕获声明；后续Application构造不能替它延后取得Skill目录。
  workSkills.splice(0, 1, analysisMethod);
  const analysisSkills = [analysisMethod];
  const analysis = { ...defineEnvironment({ id: 'analysis', instructions: '使用已定义的分析方法', program }), skills: analysisSkills };
  const environments = [work, analysis];
  const application = defineApplication({ name: 'assembly-runtime', admit: async () => {}, environments });
  const receiptFile = join(directory, 'accepted.json');
  const receipt = { command: 'accepted-a', reference: 'operation-a', state: { status: 'complete' as const, artifact: 'artifact-a' } };
  await writeFile(receiptFile, JSON.stringify(receipt));
  const record = z.object({ command: z.string(), reference: z.string(), state: z.object({ status: z.literal('complete'), artifact: z.string() }) });
  const operation = defineOperation({ kind: 'assembly-recorded-work', version: 1, command: z.string(), reference: z.string(),
    state: record.shape.state, settled: state => state });
  const load = async () => record.parse(JSON.parse(await readFile(receiptFile, 'utf8')));
  const owner = implementOperation(operation, {
    async locate(command) {
      const saved = await load();
      return saved.command === command ? { status: 'accepted', reference: saved.reference, state: saved.state } : { status: 'unknown' };
    },
    observe(reference) {
      return observeResource({ key$: of(reference), async read(value) {
        const saved = await load(); assert.equal(value, saved.reference); return saved.state;
      } });
    },
  });
  const native = capability('assembly-runtime-selection');
  const skills = createNativeSkills({ service: 'provided' });
  let assembly!: ApplicationAssembly;
  let modelCalls = 0;
  const model = nativeModel({ provider: 'assembly', model: 'unused', create: () => new class extends LlmAdapter {
    async *stream() { modelCalls++; throw new Error('ASSEMBLY_QUERY_MUST_NOT_CALL_MODEL'); }
  }() });
  const providerEntered = Promise.withResolvers<void>(), providerReleased = Promise.withResolvers<void>();
  const starting = createRuntime(selected => {
    assembly = selected;
    assert.equal(selected.native(native.selected), native.selected);
    selected.native(native.selected);
    selected.native(skills);
    assert.equal(selected.operation(owner), owner);
    selected.operation(owner);
    return application;
  }, {
    home: join(directory, 'home'), actorId: 'assembly-reader',
    model: { ...model, async install(ctx, home) {
      await model.install(ctx, home);
      providerEntered.resolve();
      await providerReleased.promise;
    } },
  });
  let runtime: Awaited<typeof starting> | undefined;
  try {
    await Promise.race([providerEntered.promise, starting]);
    // 正式provider尚未完成安装，调用者仍可修改自己的配置；本次应用沿定义时的声明继续装配。
    analysis.program = replacement;
    analysisSkills.splice(0, 1, workMethod);
    environments.splice(0, environments.length, analysis, analysis);
    providerReleased.resolve();
    runtime = await starting;
    assert.equal(native.installed, 1);
    assert.deepEqual(native.homes, [runtime.home]);
    assert.throws(() => assembly.native(native.selected), /REACTIVE_APPLICATION_ASSEMBLY_CLOSED/u);
    assert.throws(() => assembly.operation(owner), /REACTIVE_APPLICATION_ASSEMBLY_CLOSED/u);
    const workspace = await runtime.application.createWorkspace({ directory });
    const session = await runtime.application.create('work', { workspaceId: workspace.workspaceId }, contract);
    try {
      assert.equal((await skills.forSession(session).definition('assembly-work').read())?.content.trim(), 'ORIGINAL_WORK_METHOD');
      assert.equal(await skills.forSession(session).definition('assembly-analysis').read(), undefined);
      assert.deepEqual(await session.locateOperation(operation, receipt.command), { status: 'accepted', reference: receipt.reference, state: receipt.state });
      assert.deepEqual(await (await session.attachOperation(operation, receipt.reference)).settled(), receipt.state);
    } finally { session.release(); }
    const other = await runtime.application.create('analysis', { workspaceId: workspace.workspaceId }, contract);
    try {
      assert.equal((await skills.forSession(other).definition('assembly-analysis').read())?.content.trim(), 'ORIGINAL_ANALYSIS_METHOD');
      assert.equal(await skills.forSession(other).definition('assembly-work').read(), undefined);
    } finally { other.release(); }
    assert.equal(modelCalls, 0);
  } finally {
    providerReleased.resolve();
    await (runtime ?? await starting).dispose();
  }
  assert.equal(native.released, 1);
  await (await starting).dispose();
  assert.equal(native.released, 1);
});

test('两个嵌入应用分别拥有安装Fiber，异步退出可重入并撤销监听，借入Jobs与另一应用继续可用', { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-assembly-embedded-'));
  let modelCalls = 0;
  const resident = await boot(join(directory, 'home'), definition('assembly-resident'), async function* () {
    modelCalls++; throw new Error('ASSEMBLY_EMBEDDED_MUST_NOT_CALL_MODEL');
  });
  const cleanupEntered = Promise.withResolvers<void>(), cleanupReleased = Promise.withResolvers<void>();
  let first: ApplicationHost | undefined, second: ApplicationHost | undefined, witness: ApplicationHost | undefined;
  let reentrant: Promise<void> | undefined;
  const firstNative = capability('assembly-first-selection', async () => {
    if (first) { reentrant = first.dispose(); cleanupEntered.resolve(); await cleanupReleased.promise; }
  });
  const secondNative = capability('assembly-second-selection');
  const firstJobs = createNativeJobs({ registry: { kind: 'provided' } });
  const secondJobs = createNativeJobs({ registry: { kind: 'provided' } });
  try {
    await requireActivePlugin(resident.native.plugin(LocalJobs, {}));
    const providedJobs = Reflect.get(resident.native.get('jobs')!, symbols.original);
    first = await mountApplication(resident.native, selected => {
      selected.native(firstNative.selected); selected.native(firstNative.selected);
      selected.native(firstJobs);
      return definition('assembly-first');
    }, { home: join(directory, 'home'), actorId: 'assembly-reader' });
    second = await mountApplication(resident.native, selected => {
      selected.native(secondNative.selected); selected.native(secondJobs);
      return definition('assembly-second');
    }, { home: join(directory, 'home'), actorId: 'assembly-reader' });
    assert.equal(firstNative.installed, 1); assert.equal(secondNative.installed, 1);
    assert.deepEqual(await firstJobs.toolNames.read(), []);
    assert.deepEqual(await secondJobs.toolNames.read(), []);
    await assert.rejects(mountApplication(resident.native, selected => {
      selected.native(firstJobs);
      return definition('assembly-wrong-owner');
    }, { home: join(directory, 'home') }), /REACTIVE_NATIVE_CAPABILITY_ALREADY_OWNED/u);
    assert.deepEqual(await firstJobs.toolNames.read(), [], '拒绝另一应用重复取得owner不能撤销原实例');
    const firstSession = await first.create('work', { workspaceId: resident.workspace.workspaceId });
    firstSession.release();
    assert.ok(firstNative.patches > 0);
    let closed = false;
    const closing = first.dispose();
    void closing.then(() => { closed = true; }, () => {});
    await cleanupEntered.promise;
    assert.equal(reentrant, closing);
    assert.equal(first.dispose(), closing);
    assert.equal(closed, false);
    cleanupReleased.resolve(); await closing;
    assert.equal(firstNative.released, 1);
    const stoppedPatches = firstNative.patches, activePatches = secondNative.patches;
    // Session复用已解析的standing preset；新应用登记与resolve才必然触发真实Loader装配。
    witness = await mountApplication(resident.native, () => definition('assembly-after-first'));
    const secondSession = await second.create('work', { workspaceId: resident.workspace.workspaceId });
    secondSession.release();
    assert.equal(firstNative.patches, stoppedPatches, '已退出应用不能继续响应真实原生Loader事件');
    assert.ok(secondNative.patches > activePatches);
    assert.equal(Reflect.get(resident.native.get('jobs')!, symbols.original), providedJobs);
    assert.deepEqual(await secondJobs.toolNames.read(), []);
    await assert.rejects(async () => firstJobs.toolNames.read());
    const parentSession = await resident.host.create('work', { workspaceId: resident.workspace.workspaceId });
    parentSession.release();
    assert.equal(modelCalls, 0);
    await second.dispose();
    assert.equal(secondNative.released, 1);
    assert.equal(Reflect.get(resident.native.get('jobs')!, symbols.original), providedJobs);
    assert.deepEqual(resident.errors, []);
  } finally {
    cleanupReleased.resolve();
    await first?.dispose(); await second?.dispose(); await witness?.dispose(); await resident.dispose();
  }
});

test('后续原生插件安装失败撤回已有资源和监听，factory失败撤销未安装选择，父宿主仍可装配', { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-assembly-rollback-'));
  let modelCalls = 0;
  const resident = await boot(join(directory, 'home'), definition('assembly-rollback-resident'), async function* () {
    modelCalls++; throw new Error('ASSEMBLY_ROLLBACK_MUST_NOT_CALL_MODEL');
  });
  const failure = new Error('ASSEMBLY_SECOND_PLUGIN_STARTUP_FAILED');
  const first = capability('assembly-before-startup-failure');
  const failing = capability('assembly-startup-failure', undefined, async () => {
    const witness = await mountApplication(resident.native, () => definition('assembly-during-startup'));
    try { assert.ok(first.patches > 0, '失败前已安装插件必须实际参与Loader事件'); }
    finally { await witness.dispose(); }
    throw failure;
  });
  let recovered: ApplicationHost | undefined;
  try {
    await assert.rejects(mountApplication(resident.native, assembly => {
      assembly.native(first.selected); assembly.native(failing.selected);
      return definition('assembly-startup-rollback');
    }, { home: join(directory, 'home') }), error => error === failure);
    assert.equal(first.released, 1); assert.equal(failing.released, 1);
    const firstPatches = first.patches, failedPatches = failing.patches;
    recovered = await mountApplication(resident.native, () => definition('assembly-startup-rollback'));
    assert.equal(first.patches, firstPatches);
    assert.equal(failing.patches, failedPatches);
    const session = await recovered.create('work', { workspaceId: resident.workspace.workspaceId });
    session.release();
    const selected = createNativeJobs({ registry: { kind: 'provided' } });
    assert.equal(selected.toolNames.peek().status, 'pending');
    const factoryFailure = new Error('ASSEMBLY_FACTORY_FAILED_AFTER_SELECTION');
    await assert.rejects(mountApplication(resident.native, assembly => {
      assembly.native(selected); throw factoryFailure;
    }), error => error === factoryFailure);
    assert.throws(() => selected.toolNames.peek(), '未安装选择也必须结束其真实资源owner');
    assert.equal(resident.native.get('jobs'), undefined, '失败factory没有安装其选择的原生服务');
    const parentSession = await resident.host.create('work', { workspaceId: resident.workspace.workspaceId });
    parentSession.release();
    assert.equal(modelCalls, 0);
    assert.deepEqual(resident.errors, []);
  } finally { await recovered?.dispose(); await resident.dispose(); }
});

test('原生父scope退出先排空实际Agent再释放应用能力，异步Agent清理期间保留能力与父服务', { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-assembly-parent-exit-'));
  let modelCalls = 0;
  const resident = await boot(join(directory, 'home'), definition('assembly-parent-resident'), async function* () {
    modelCalls++; throw new Error('ASSEMBLY_PARENT_EXIT_MUST_NOT_CALL_MODEL');
  });
  let parent!: Context;
  const installation = resident.native.plugin({ name: 'assembly-native-parent', apply(ctx: Context) { parent = ctx; } });
  const selected = capability('assembly-parent-selected');
  const agentCleanupEntered = Promise.withResolvers<void>(), agentCleanupReleased = Promise.withResolvers<void>();
  let host: ApplicationHost | undefined;
  try {
    await requireActivePlugin(installation);
    host = await mountApplication(parent, assembly => {
      assembly.native(selected.selected);
      return definition('assembly-parent-child');
    }, { home: join(directory, 'home') });
    const session = await host.create('work', { workspaceId: resident.workspace.workspaceId });
    const agent = resident.native.agents.get(SessionId(session.id));
    assert.ok(agent);
    agent.ctx.effect(() => async () => { agentCleanupEntered.resolve(); await agentCleanupReleased.promise; });
    session.release();
    const closing = installation.dispose();
    await agentCleanupEntered.promise;
    assert.equal(selected.released, 0, '原Agent还在退出时应用依赖必须保持安装');
    const rootSession = await resident.host.create('work', { workspaceId: resident.workspace.workspaceId });
    rootSession.release();
    agentCleanupReleased.resolve();
    await closing; await installation;
    assert.equal(selected.released, 1);
    assert.equal(resident.native.agents.get(SessionId(session.id)), undefined);
    const after = await resident.host.create('work', { workspaceId: resident.workspace.workspaceId });
    after.release();
    assert.equal(modelCalls, 0);
    assert.deepEqual(resident.errors, []);
  } finally {
    agentCleanupReleased.resolve();
    await host?.dispose(); await installation.dispose(); await installation; await resident.dispose();
  }
});
