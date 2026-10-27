import { test } from 'node:test';
import assert from 'node:assert/strict';
import { access, mkdir, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { spawn, type ChildProcess } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Fragment, createElement as h, useLayoutEffect } from 'react';
import { BehaviorSubject, Observable, Subject, filter, firstValueFrom, tap } from 'rxjs';
import { LlmAdapter, type GenerateOptions } from '@deepseek-ai/dsh-llm';
import type { Context } from '@deepseek-ai/cordis';
import LocalJobs from '@deepseek-ai/dsh-jobs-local';
import * as jobTools from '@deepseek-ai/dsh-tool-jobs';
import type { JobOutcome } from '@deepseek-ai/dsh-jobs';
import { SessionId } from '@deepseek-ai/dsh-session';
import type { ToolDefinition } from '@deepseek-ai/dsh-tools';
import { defineAction, implementAction, defineApplication, defineEnvironment, defineProgram, defineSurface, composeSurface, readActionPresentation, Scope, z, type NativeToolCall, type ProgramSession } from '@hanger-source/dsh-reactive';
import { implementProgram, NativeTools, Offer, Expose, Module, useAction, useObservable, useRelation } from '@hanger-source/dsh-reactive/program';
import { boot, text, tool } from './native-support.ts';
import type { ConversationHandle } from '@hanger-source/dsh-reactive-dsh';
import { createRuntime, nativeModel, requireActivePlugin } from '@hanger-source/dsh-reactive-dsh';
import { createNativeJobs, type NativeSessionJobs } from '@hanger-source/dsh-reactive-dsh/jobs';
import { createNativeApproval } from '@hanger-source/dsh-reactive-dsh/approval';
import { nativeObservation } from '../packages/dsh/src/native-capability.ts';

declare module '@deepseek-ai/dsh-jobs/view' {
  interface JobKindMap { nativeLifecycle: 'native-lifecycle'; }
}

const nativeArguments = z.object({ marker: z.string().regex(/^[a-z-]+$/u) }).strict();

test('原生读侧构造不连接，首次订阅后共享同一来源，owner退出不向迟到观察者回放旧快照', () => {
  const scope = new Scope(), changes = new Subject<number>();
  let connections = 0, releases = 0;
  const observed = nativeObservation(scope, new Observable<number>(subscriber => {
    connections++;
    const subscription = changes.subscribe(subscriber);
    return () => { releases++; subscription.unsubscribe(); };
  }));
  try {
    assert.equal(connections, 0);
    const one: number[] = [], two: number[] = [];
    const first = observed.subscribe(value => one.push(value));
    changes.next(1);
    const second = observed.subscribe(value => two.push(value));
    assert.equal(connections, 1);
    first.unsubscribe(); changes.next(2); second.unsubscribe();
    assert.deepEqual(one, [1]); assert.deepEqual(two, [1, 2]);
    assert.equal(releases, 0, '观察连接归原生读侧owner，组件退订不冒充SDK dispose');
    scope.dispose();
    assert.equal(releases, 1);
    const late: number[] = [];
    assert.equal(observed.subscribe(value => late.push(value)).closed, true);
    assert.deepEqual(late, []);
  } finally { scope.dispose(); }
});

for (const mode of ['native', 'ptc'] as const) {
  test(`${mode} 原生声明沿真实registry执行；未声明SDK不可调用，异步政策撤销和其他guard都保留`, { timeout: 30_000 }, async () => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-native-capabilities-'));
    const enabled$ = new BehaviorSubject(true), commits$ = new Subject<boolean>();
    const entered = Promise.withResolvers<NativeToolCall>(), release = Promise.withResolvers<void>();
    const admissions: NativeToolCall[] = [], requests: GenerateOptions[] = [];
    const admit = async (call: NativeToolCall) => {
      admissions.push(call);
      if (nativeArguments.parse(call.arguments).marker === 'revoked') { entered.resolve(call); await release.promise; }
    };
    const definition = defineApplication({ name: 'native-capabilities-' + mode, admit: async () => {}, environments: [defineEnvironment({
      id: 'work', instructions: '只调用本会话明确声明的原生工具',
      program: implementProgram(defineProgram({ input: z.object({ merchantId: z.string() }), surface: defineSurface({ views: {}, actions: {} }) }), {
        Component() {
          const observed = useObservable(() => enabled$, []);
          const enabled = observed.status === 'ready' && observed.value;
          useLayoutEffect(() => commits$.next(enabled), [enabled]);
          return NativeTools({ names: ['native_tool'], enabled, admit });
        },
      }),
    })] });
    let next: { name: string; marker: string; id: string } | undefined;
    const app = await boot(join(root, 'home'), definition, async function* (request) {
      requests.push(request);
      assert.ok(!request.tools?.some(item => item.name === 'undeclared_tool'));
      const planned = next; next = undefined;
      if (!planned) { yield* text('已核对实际工具结果'); return; }
      if (mode === 'native') yield* tool(planned.name, planned.id, { marker: planned.marker });
      else yield* tool('run_code', planned.id, { description: '执行已声明的原生能力',
        code: `return await tools.${planned.name}(${JSON.stringify({ marker: planned.marker })});` });
    }, mode);
    const installed = (name: string): ToolDefinition => ({ name, description: '写入本次调用的实际文件',
      parameters: { type: 'object', properties: { marker: { type: 'string' } }, required: ['marker'], additionalProperties: false },
      output: { schema: { type: 'object', properties: { written: { type: 'string' } }, required: ['written'], additionalProperties: false },
        render: (_args, output) => [{ type: 'text', text: JSON.stringify(output) }] },
      async execute(raw, execution) {
        const input = nativeArguments.parse(raw); execution.signal.throwIfAborted();
        await writeFile(join(root, input.marker + '.txt'), execution.callId, { signal: execution.signal });
        return { written: input.marker };
      },
    });
    const removeNative = app.native.tools.register(installed('native_tool'));
    const removeUndeclared = app.native.tools.register(installed('undeclared_tool'));
    let guardCalls = 0;
    const removeGuard = app.native.tools.guard(execution => {
      if (execution.name !== 'native_tool' || nativeArguments.parse(execution.arguments).marker !== 'guarded') return;
      guardCalls++; return 'NATIVE_OTHER_GUARD_DENIED';
    });
    try {
      const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
      let sequence = 0;
      const send = async (name: string, marker: string) => {
        next = { name, marker, id: `${mode}-native-${++sequence}` };
        await session.submit({ requestId: marker, content: '核对原生能力 ' + marker,
          intent: 'question', delivery: 'followup', scope: { merchantId: 'merchant-a' } });
      };
      await send('native_tool', 'accepted'); await session.whenIdle();
      const written = await readFile(join(root, 'accepted.txt'), 'utf8');
      assert.ok(written.length > 0);
      assert.equal(admissions[0]?.source.callId, written);
      assert.equal(admissions[0]?.source.rootCallId, `${mode}-native-1`);
      assert.equal(admissions[0]?.sessionId, session.id);
      assert.equal(admissions[0]?.workspaceId, app.workspace.workspaceId);
      assert.equal(admissions[0]?.inputs[0]?.actorId, 'operator-test');
      assert.deepEqual(admissions[0]?.inputs[0]?.scope, { merchantId: 'merchant-a' });
      const agent = app.native.agents.get(SessionId(session.id))!;
      assert.equal(Reflect.set(admissions[0]!.inputs[0]!.scope as object, 'merchantId', 'changed-policy-copy'), true);
      const nativeInputs = app.native.sessionProjections.stateOf(agent.session, 'reactiveInputs');
      assert.deepEqual(nativeInputs?.calls[written]?.inputs[0]?.scope, { merchantId: 'merchant-a' },
        '政策收到独立的输入快照，不能修改原生调用的原请求权限事实');
      assert.ok(agent.ctx.tools.schemas(agent).some(value => value.name === 'native_tool'));
      assert.ok(!agent.ctx.tools.schemas(agent).some(value => value.name === 'undeclared_tool'), '同一registry可见性决定PTC SDK，不仅过滤模型列表');
      await send('undeclared_tool', 'hidden'); await session.whenIdle();
      await assert.rejects(access(join(root, 'hidden.txt')), { code: 'ENOENT' });
      assert.ok(!admissions.some(call => call.name === 'undeclared_tool'));
      await send('native_tool', 'revoked');
      const pending = await entered.promise;
      const disabled = firstValueFrom(commits$.pipe(filter(value => !value)));
      enabled$.next(false); await disabled;
      assert.equal(pending.signal.aborted, false, '撤下能力不是取消已经进入政策的原生调用signal');
      release.resolve();
      await session.whenIdle();
      await assert.rejects(access(join(root, 'revoked.txt')), { code: 'ENOENT' });
      const enabled = firstValueFrom(commits$.pipe(filter(value => value)));
      enabled$.next(true); await enabled;
      await send('native_tool', 'guarded'); await session.whenIdle();
      assert.ok(guardCalls > 0, '原生其他guard实际拒绝了本次调用，声明不能覆盖其结论');
      await assert.rejects(access(join(root, 'guarded.txt')), { code: 'ENOENT' });
      const events = agent.session.snapshotEvents();
      const denied = events.filter(event => event.type === 'tool/result' ? event.data.message.isError
        : event.type === 'tool/ptc-dispatch' && event.data.isError);
      assert.ok(denied.length >= 3, '不可见、政策撤销、其他guard各自留下真实失败结算');
      assert.deepEqual(app.errors, []);
      session.release();
    } finally {
      release.resolve(); removeGuard(); removeUndeclared(); removeNative(); enabled$.complete(); commits$.complete(); await app.dispose();
    }
  });

  test(`${mode} 模块Action原生结果携真实绑定和已解析scope，消费者无需解析工具文本或再次transform`, { timeout: 30_000 }, async () => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-action-presentation-'));
    const save = defineAction({ name: 'record_merchant', description: '记录明确绑定的商家',
      scope: z.string().transform(merchantId => ({ subject: { kind: 'merchant' as const, id: merchantId } })),
      input: z.object({ revision: z.literal(3) }).strict(), output: z.object({ recorded: z.string(), revision: z.literal(3) }).strict() });
    const local = defineSurface({ views: {}, actions: { save } });
    const composed = composeSurface({ operations: { merchant: local } });
    const effects: string[] = [];
    const implementation = implementAction(save, { execute(input, call) {
      effects.push(call.scope.subject.id); return { recorded: call.scope.subject.id, revision: input.revision };
    } });
    function Merchant() {
      const action = useAction(implementation, () => 'merchant-a', []);
      return h(Fragment, null, Offer({ action }), Expose({ surface: local, value: { actions: { save: action } } }));
    }
    const program = defineProgram({ input: z.null(), surface: composed.surface });
    const definition = defineApplication({ name: 'action-presentation-' + mode, admit: async () => {}, environments: [defineEnvironment({
      id: 'work', instructions: '调用明确商家的记录能力', program: implementProgram(program, { Component() {
        return h(Module, { mount: composed.mounts.operations.merchant }, h(Merchant));
      } }),
    })] });
    let requested = false, toolName = '';
    let session: ConversationHandle<typeof program>;
    const app = await boot(join(root, 'home'), definition, async function* (request) {
      if (requested) { yield* text('已读取结构化结果'); return; }
      requested = true;
      const manifest = await firstValueFrom(session.actionManifest$);
      const matches = manifest.filter(value => value.action === save.name && value.enabled);
      assert.equal(matches.length, 1); toolName = matches[0]!.toolName;
      assert.ok(mode === 'ptc' || request.tools?.some(item => item.name === toolName));
      if (mode === 'native') yield* tool(toolName, 'native-record', { revision: 3 });
      else yield* tool('run_code', 'ptc-record', { description: '记录当前商家', code: `return await tools.${toolName}({ revision: 3 });` });
    }, mode);
    try {
      session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
      const [binding] = await firstValueFrom(session.actionManifest$); assert.ok(binding);
      await session.submit({ requestId: 'record', content: '记录商家甲', intent: 'question', delivery: 'followup', scope: null });
      await session.whenIdle();
      const events = app.native.agents.get(SessionId(session.id))!.session.snapshotEvents();
      const event = events.find(event => mode === 'native'
        ? event.type === 'tool/result' && event.data.message.toolCallId === 'native-record'
        : event.type === 'tool/ptc-dispatch' && event.data.name === toolName);
      assert.ok(event?.type === 'tool/result' || event?.type === 'tool/ptc-dispatch');
      const presentation = readActionPresentation(save, event.data.meta);
      assert.ok(presentation);
      assert.equal(presentation.bindingId, binding.bindingId);
      assert.deepEqual(presentation.modulePath, ['operations', 'merchant']);
      assert.deepEqual(presentation.scope, { subject: { kind: 'merchant', id: 'merchant-a' } });
      assert.deepEqual(presentation.output, { recorded: 'merchant-a', revision: 3 });
      assert.deepEqual(effects, ['merchant-a']);
      assert.deepEqual(app.errors, []);
      session.release();
    } finally { await app.dispose(); }
  });
}

test('声明未安装的原生工具在模型装配边界硬失败，不静默过滤为可运行会话', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-native-missing-'));
  const definition = defineApplication({ name: 'native-missing', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '缺失能力必须明确报告',
    program: implementProgram(defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) }), {
      Component: () => NativeTools({ names: ['never_installed_native_tool'] }),
    }),
  })] });
  let modelCalls = 0;
  const app = await boot(join(root, 'home'), definition, async function* () { modelCalls++; yield* text('不应进入模型'); });
  try {
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    await session.submit({ requestId: 'missing', content: '核验原生能力', intent: 'question', delivery: 'followup', scope: null });
    const completion = await Promise.allSettled([session.whenIdle()]);
    assert.equal(modelCalls, 0);
    const errors = [...app.errors, ...completion.flatMap(item => item.status === 'rejected' ? [item.reason] : [])];
    assert.ok(errors.some(error => error instanceof Error && error.message.includes('REACTIVE_NATIVE_TOOL_NOT_INSTALLED')));
    assert.equal(session.snapshot().requests.missing?.state, 'settled');
    session.release();
  } finally { await app.dispose(); }
});

test('opt-in原生安装拒绝并释放PENDING与失败插件；官方Jobs围绕实际Node进程持有观察、停止及owner释放', { timeout: 15_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-native-installation-'));
  const directory = join(root, 'workspace'); await mkdir(directory);
  const errors: unknown[] = [], children: Array<{ child: ChildProcess; closed: Promise<JobOutcome> }> = [];
  const capability = createNativeJobs({ registry: { kind: 'provided' }, tools: { completionDelivery: 'quiet' } });
  const approval = createNativeApproval({ policy: 'never', require: () => undefined, answer: 'provided' });
  const forceStop = () => {
    for (const { child } of children) if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
  };
  t.signal.addEventListener('abort', forceStop, { once: true });
  let modelCalls = 0;
  const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
  const programSessions = new Map<string, { source: ProgramSession<typeof program>; jobs: NativeSessionJobs }>();
  const retired = new Subject<string>(), retiredFailure = new Error('PROGRAM_NATIVE_SOURCE_RETIRED');
  const definition = defineApplication({ name: 'native-installation', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '只有显式选择的Job控制工具可用，Job不代表持久业务完成。',
    program: implementProgram(program, {
      Component({ conversation }) {
        const jobs = capability.forSession(conversation);
        useObservable(() => jobs.list$, [jobs]);
        useObservable(() => approval.forSession(conversation).policy$, [conversation]);
        useRelation(() => retired.pipe(filter(id => id === conversation.identity.sessionId), tap(() => { throw retiredFailure; })), [conversation]);
        useLayoutEffect(() => { programSessions.set(conversation.identity.sessionId, { source: conversation, jobs }); }, [conversation, jobs]);
        return NativeTools({ names: ['job_output', 'job_list', 'job_kill'] });
      },
    }),
  })] });
  const runtime = await createRuntime(() => definition, {
    home: join(root, 'home'), actorId: 'native-owner', tools: 'ptc', onError: event => errors.push(event.error),
    model: nativeModel({ provider: 'native-installation', model: 'unused', create: () => new class extends LlmAdapter {
      stream() { modelCalls++; return text('原生Job无需启动模型'); }
    }() }),
    async installNative(ctx, home) {
      const pending = ctx.plugin(jobTools, { completionDelivery: 'quiet' });
      await assert.rejects(requireActivePlugin(pending), /REACTIVE_NATIVE_PLUGIN_NOT_ACTIVE/);
      await requireActivePlugin(ctx.plugin(LocalJobs, {}));
      await pending;
      assert.equal(ctx.tools.get('job_list'), undefined, '依赖后来装入不能使已拒绝的旧控制器复活');
      await capability.install(ctx, home);
      await approval.install(ctx, home);
      assert.ok(ctx.tools.get('job_list'));
      const failure = new Error('ACTUAL_PLUGIN_STARTUP_REJECTED');
      await assert.rejects(requireActivePlugin(ctx.plugin({ name: 'partial-startup', inject: ['tools'], async apply(native: Context) {
        native.tools.register({ name: 'partial_startup_tool', description: '部分装配应随失败释放',
          parameters: { type: 'object', properties: {}, additionalProperties: false },
          output: { schema: { type: 'null' }, render: () => [{ type: 'text', text: 'null' }] }, async execute() { return null; } });
        throw failure;
      } })), error => error === failure, '启动错误保留原对象，不改成活跃或一般状态错误');
      assert.equal(ctx.tools.get('partial_startup_tool'), undefined, '失败插件已注册的真实工具也必须退出');
    },
  });
  try {
    const workspace = await runtime.application.createWorkspace({ directory });
    const session = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    const foreign = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    const owner = SessionId(session.id), jobs = runtime.native.jobs;
    const programSession = programSessions.get(session.id)!;
    assert.equal(await firstValueFrom(approval.forSession(programSession.source).policy$), 'never', 'Program首次提交即可读取同一原生审批政策');
    assert.equal(capability.forSession(programSession.source), programSession.jobs);
    assert.throws(() => capability.forSession({ identity: programSession.source.identity }), /REACTIVE_NATIVE_SESSION_REQUIRED/);
    let observedSession = capability.forSession(session);
    const launch = () => programSession.jobs.start({ kind: 'native-lifecycle', label: '隔离Node进程，显式停止前保持运行',
      run(job) {
        // 此生产方只持有实际ChildProcess；id、游标、事件与终态均由官方JobRegistry拥有。
        const child = spawn(process.execPath, ['-e', "process.stdout.write('native-job-ready\\n');setInterval(()=>{},1000);"],
          { cwd: directory, env: {}, stdio: ['ignore', 'pipe', 'pipe'] });
        let cancelled = false, failure: Error | undefined;
        child.stdout.on('data', (data: Buffer) => job.append(data.toString('utf8'), { channel: 'stdout' }));
        child.stderr.on('data', (data: Buffer) => job.append(data.toString('utf8'), { channel: 'stderr' }));
        child.once('error', error => { failure = error; });
        const closed = new Promise<JobOutcome>(resolve => child.once('close', (code, signal) => resolve({
          status: cancelled ? 'killed' : failure || code !== 0 ? 'failed' : 'completed',
          detail: failure?.message ?? `exit=${code};signal=${signal}`,
        })));
        children.push({ child, closed });
        return { cancel() { cancelled = true; child.kill('SIGTERM'); }, done: closed };
      },
    });
    const ready = (job: ReturnType<typeof launch>) => firstValueFrom(observedSession.job(job).output$
      .pipe(filter(value => value.chunks.some(chunk => chunk.text.includes('native-job-ready')))));
    const first = launch(), reader = observedSession.job(first);
    const firstObserver = firstValueFrom(reader.output$.pipe(filter(value => value.chunks.some(chunk => chunk.text.includes('native-job-ready')))));
    const secondObserver = firstValueFrom(reader.output$.pipe(filter(value => value.chunks.some(chunk => chunk.text.includes('native-job-ready')))));
    const [observed, secondObserved] = await Promise.all([firstObserver, secondObserver]);
    assert.deepEqual(secondObserved, observed, '两个SDK观察者读取同一个真实输出，均不消费模型cursor');
    assert.throws(() => capability.forSession(foreign).job(first), '另一个真实owner不能读取输出');
    assert.deepEqual(reader.readAt(0), observed, '重复SDK观察不消耗模型游标');
    assert.deepEqual(programSession.jobs.job(first).readAt(0), observed, 'Program与外部SDK消费同一原生Job，不复制输出状态');
    assert.equal(jobs.read(first, owner).chunks.map(chunk => chunk.text).join(''), 'native-job-ready\n');
    assert.equal(jobs.read(first, owner).chunks.length, 0, '只有正式consuming read才推进模型游标');
    assert.deepEqual(reader.readAt(0), observed, '模型读取也不破坏SDK观察者的绝对offset');
    observedSession.dispose();
    assert.equal(jobs.get(first, owner).status, 'running', '释放SDK观察不调用原生kill');
    assert.doesNotThrow(() => process.kill(children[0]!.child.pid!, 0), '真实Node进程仍存活');
    observedSession = capability.forSession(session);
    const controlling = observedSession.job(first);
    assert.equal(controlling.stop('明确停止本次Node进程'), 'requested');
    const stopped = await controlling.wait(5_000);
    assert.equal(stopped.status, 'killed'); await children[0]!.closed;
    assert.throws(() => process.kill(children[0]!.child.pid!, 0), { code: 'ESRCH' });

    const otherDefinition = defineApplication({ name: 'native-installation', admit: async () => {}, environments: [defineEnvironment({
      id: 'work', instructions: '独立宿主未选择这个能力实例', program: implementProgram(program, { Component: () => null }),
    })] });
    const otherHost = await boot(join(root, 'other-home'), otherDefinition, async function* () { yield* text('不会为SDK读取启动模型'); });
    try {
      const other = await otherHost.host.create('work', { workspaceId: otherHost.workspace.workspaceId });
      assert.throws(() => capability.forSession(other), /REACTIVE_NATIVE_CAPABILITY_HOST_MISMATCH/);
      other.release();
    } finally { await otherHost.dispose(); }

    const second = launch(); await ready(second);
    const secondClosed = children[1]!.closed;
    const agent = runtime.native.agents.get(owner)!;
    const registrations = agent.session.snapshotEvents().filter(event => event.type === 'job/registered');
    assert.deepEqual(registrations.map(event => event.data.owner), [owner, owner]);
    assert.deepEqual(registrations.map(event => event.data.jobId), [first, second]);
    foreign.release(); session.release();
    assert.throws(() => capability.forSession(session), /REACTIVE_CONVERSATION_HANDLE_RELEASED|REACTIVE_SCOPE_DISPOSED/);
    assert.equal(programSession.jobs.job(second).snapshot().status, 'running', '外部SDK退出不释放Program来源');
    retired.next(session.id);
    assert.throws(() => capability.forSession(programSession.source), error => error === retiredFailure);
    assert.throws(() => programSession.jobs.list(), /REACTIVE_SCOPE_DISPOSED/);
    assert.equal(jobs.get(second, owner).status, 'running', 'Program失败只撤观察，不能停止已受理的原生进程');
    assert.doesNotThrow(() => process.kill(children[1]!.child.pid!, 0));
    await runtime.dispose();
    assert.equal((await secondClosed).status, 'killed', 'Runtime释放等待真实owner的生产进程退出');
    assert.throws(() => process.kill(children[1]!.child.pid!, 0), { code: 'ESRCH' });
    assert.equal(modelCalls, 0, 'quiet Job观察与控制不制造业务输入或模型轮次');
    assert.deepEqual(errors, []);
  } finally {
    try { await runtime.dispose(); }
    finally {
      // 失败试验也释放自己创建的真实进程；这不把Runtime未释放进程伪装成成功。
      forceStop();
      await Promise.all(children.map(item => item.closed));
      t.signal.removeEventListener('abort', forceStop);
    }
  }
});
