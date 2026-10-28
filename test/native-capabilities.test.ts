import { test } from 'node:test';
import assert from 'node:assert/strict';
import { access, mkdir, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { spawn, type ChildProcess } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Fragment, createElement as h, useLayoutEffect } from 'react';
import { BehaviorSubject, Observable, Subject, filter, firstValueFrom, from, tap, timeout } from 'rxjs';
import { LlmAdapter, type GenerateOptions } from '@deepseek-ai/dsh-llm';
import { assembleContextFor } from '@deepseek-ai/dsh-agent';
import { Context } from '@deepseek-ai/cordis';
import LocalJobs from '@deepseek-ai/dsh-jobs-local';
import * as jobTools from '@deepseek-ai/dsh-tool-jobs';
import type { JobOutcome } from '@deepseek-ai/dsh-jobs';
import { SessionId } from '@deepseek-ai/dsh-session';
import type { ToolDefinition, ToolExecutionResult } from '@deepseek-ai/dsh-tools';
import { collectionSurface, defineAction, implementAction, defineApplication, defineEnvironment, defineProgram, defineSurface, composeSurface, readActionPresentation, Scope, z, type NativeToolCall, type NativeToolAdmission, type ProgramSession } from '@hanger-source/dsh-reactive';
import { implementProgram, NativeTools, Offer, Expose, Module, useAction, useObservable, useResource, useRelation } from '@hanger-source/dsh-reactive/program';
import { boot, text, tool } from './native-support.ts';
import type { ConversationHandle } from '@hanger-source/dsh-reactive-dsh';
import { createRuntime, nativeModel, requireActivePlugin } from '@hanger-source/dsh-reactive-dsh';
import { createNativeJobs, JobId, type NativeSessionJobs } from '@hanger-source/dsh-reactive-dsh/jobs';
import { createNativeApproval } from '@hanger-source/dsh-reactive-dsh/approval';
import { NativeCapabilityOwner, nativeObservation } from '../packages/dsh/src/native-capability.ts';

declare module '@deepseek-ai/dsh-jobs/view' {
  interface JobKindMap { nativeLifecycle: 'native-lifecycle'; }
}

const nativeArguments = z.object({ marker: z.string().regex(/^[a-z-]+$/u) }).strict();

test('外层原生卸载与能力退出加入同一清理，观察释放失败也要等原生资源静止', async () => {
  const parent = new Context();
  const entered = Promise.withResolvers<void>(), release = Promise.withResolvers<void>();
  const cleanupFailure = new Error('NATIVE_RESOURCE_RELEASE_FAILED');
  let released = false;
  const owner: NativeCapabilityOwner = new NativeCapabilityOwner('native-owner-cleanup', [], function* () {
    yield () => { released = true; };
    yield owner.cleanup(async () => { entered.resolve(); await release.promise; throw cleanupFailure; });
  });
  const failure = new Error('OBSERVATION_RELEASE_FAILED');
  let reentrant: Promise<void> | undefined;
  owner.scope.own(() => {
    reentrant = owner.dispose();
    void reentrant.catch(() => {});
    throw failure;
  });
  await owner.install(parent, '/tmp');
  const external = parent.fiber.dispose();
  try {
    await entered.promise;
    const closing = owner.dispose();
    assert.equal(closing, reentrant, '同步释放中的重入取得原退出回执');
    let settled = false;
    const result = closing.then(() => { settled = true; }, error => { settled = true; return error; });
    await new Promise<void>(resolve => setImmediate(resolve));
    assert.equal(settled, false, 'Scope错误不能使尚未静止的原生清理提前结束');
    release.resolve();
    const error = await result;
    assert.ok(error instanceof AggregateError);
    assert.equal(released, true, '原生资源清理拒绝后仍释放同一正式effect中其余资源');
    assert.equal(error.errors.length, 2);
    assert.ok(error.errors[0] instanceof AggregateError);
    assert.deepEqual(error.errors[0].errors, [failure]);
    assert.equal(error.errors[1], cleanupFailure);
  } finally {
    release.resolve();
    await external;
    await owner.dispose().catch(() => {});
  }
});

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
  test(`${mode} 共享原生工具的准入随真实调用和声明退出，迟到政策不越过正式guard`, { timeout: 30_000 }, async () => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-native-capabilities-'));
    const enabled$ = new BehaviorSubject(true), commits$ = new Subject<boolean>();
    const selected = new BehaviorSubject<readonly string[]>(['left', 'right']);
    const entered = Promise.withResolvers<NativeToolCall>(), release = Promise.withResolvers<void>();
    const sharedEntered = Promise.withResolvers<NativeToolCall>(), sharedRelease = Promise.withResolvers<void>();
    const gates = new Map(['cancelled', 'replaced', 'downstream', 'failed'].map(marker => [marker, {
      entered: Promise.withResolvers<NativeToolCall>(), release: Promise.withResolvers<void>(),
      settled: Promise.withResolvers<Readonly<ToolExecutionResult>>(),
    }]));
    const downstreamEntered = Promise.withResolvers<void>(), downstreamRelease = Promise.withResolvers<void>();
    const failed = new BehaviorSubject(false), failure = new Error('NATIVE_POLICY_PROGRAM_FAILED');
    const admissions: NativeToolCall[] = [], requests: GenerateOptions[] = [];
    const executions: string[] = [];
    const admit = async (call: NativeToolCall) => {
      admissions.push(call);
      const { marker } = nativeArguments.parse(call.arguments);
      if (marker === 'revoked') { entered.resolve(call); await release.promise; }
      if (marker === 'shared') { sharedEntered.resolve(call); await sharedRelease.promise; }
      const gate = gates.get(marker);
      if (gate) { gate.entered.resolve(call); if (marker !== 'downstream') await gate.release.promise; }
    };
    const policy = new BehaviorSubject<NativeToolAdmission>(admit);
    const empty = defineSurface({ views: {}, actions: {} });
    const composed = composeSurface({ dependencies: collectionSurface(empty) });
    const definition = defineApplication({ name: 'native-capabilities-' + mode, admit: async () => {}, environments: [defineEnvironment({
      id: 'work', instructions: '只调用本会话明确声明的原生工具',
      program: implementProgram(defineProgram({ input: z.object({ merchantId: z.string() }), surface: composed.surface }), {
        Component() {
          const observed = useObservable(() => enabled$, []);
          const members = useObservable(() => selected, []);
          const admission = useObservable(() => policy, []);
          const broken = useObservable(() => failed, []);
          const enabled = observed.status === 'ready' && observed.value;
          const current = admission.status === 'ready' ? admission.value : undefined;
          useLayoutEffect(() => commits$.next(enabled), [enabled, current]);
          if (broken.status === 'ready' && broken.value) throw failure;
          return members.status === 'ready' && admission.status === 'ready' ? h(Fragment, null, members.value.map(member =>
            h(Module, { key: member, mount: composed.mounts.dependencies.instance(member) },
              NativeTools({ names: ['native_tool'], enabled, admit: current })))) : null;
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
        executions.push(input.marker);
        await writeFile(join(root, input.marker + '.txt'), execution.callId, { signal: execution.signal });
        return { written: input.marker };
      },
    });
    const removeNative = app.native.tools.register(installed('native_tool'));
    const removeUndeclared = app.native.tools.register(installed('undeclared_tool'));
    const removeResults = app.native.on('tools/result', (execution, result) => {
      if (execution.name === 'native_tool') gates.get(nativeArguments.parse(execution.arguments).marker)?.settled.resolve(result);
    });
    const removeDownstream = app.native.on('tools/pre-execute', async (execution, next) => {
      if (execution.name === 'native_tool' && nativeArguments.parse(execution.arguments).marker === 'downstream') {
        assert.ok(admissions.some(call => call.source.callId === execution.callId), '本框架政策先完成，再进入另一正式gate');
        downstreamEntered.resolve(); await downstreamRelease.promise;
      }
      return next();
    });
    let guardCalls = 0;
    const removeGuard = app.native.tools.guard(execution => {
      if (execution.name !== 'native_tool' || nativeArguments.parse(execution.arguments).marker !== 'guarded') return;
      guardCalls++; return 'NATIVE_OTHER_GUARD_DENIED';
    });
    try {
      const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
      const idle = () => firstValueFrom(from(session.whenIdle()).pipe(timeout(3_000)));
      const members = session.collection('dependencies');
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
      assert.equal(agent.ctx.tools.schemas(agent).filter(value => value.name === 'native_tool').length, 1);
      assert.ok(!agent.ctx.tools.schemas(agent).some(value => value.name === 'undeclared_tool'), '同一registry可见性决定PTC SDK，不仅过滤模型列表');
      await send('native_tool', 'shared');
      const shared = await sharedEntered.promise;
      const retained = firstValueFrom(members.instances$.pipe(filter(ids => ids.length === 1 && ids[0] === 'right'), timeout(3_000)));
      selected.next(['right']); await retained;
      assert.equal(shared.signal.aborted, false, '退出一个共享贡献不取消原生准入或已受理执行');
      sharedRelease.resolve(); await session.whenIdle();
      assert.equal(await readFile(join(root, 'shared.txt'), 'utf8'), shared.source.callId);
      assert.equal(admissions.filter(call => nativeArguments.parse(call.arguments).marker === 'shared').length, 1);
      assert.equal(executions.filter(marker => marker === 'shared').length, 1, '原生执行没有因模块退出重新投递');
      assert.equal(agent.ctx.tools.schemas(agent).filter(value => value.name === 'native_tool').length, 1);
      await send('undeclared_tool', 'hidden'); await session.whenIdle();
      await assert.rejects(access(join(root, 'hidden.txt')), { code: 'ENOENT' });
      assert.ok(!admissions.some(call => call.name === 'undeclared_tool'));
      await send('native_tool', 'revoked');
      const pending = await entered.promise;
      const disabled = firstValueFrom(commits$.pipe(filter(value => !value)));
      enabled$.next(false); await disabled;
      await idle();
      assert.equal(pending.signal.aborted, true, '声明撤销立即结束本次准入，不等待不合作政策返回');
      release.resolve();
      await assert.rejects(access(join(root, 'revoked.txt')), { code: 'ENOENT' });
      const enabled = firstValueFrom(commits$.pipe(filter(value => value)));
      enabled$.next(true); await enabled;
      await send('native_tool', 'replaced');
      const replacing = gates.get('replaced')!;
      const oldPolicy = await replacing.entered.promise;
      const replaced = firstValueFrom(commits$);
      const replacement: NativeToolAdmission = call => admit(call);
      policy.next(replacement); await replaced;
      await idle();
      assert.equal(oldPolicy.signal.aborted, true);
      assert.equal((await replacing.settled.promise).isError, true);
      replacing.release.reject(new Error('RETIRED_NATIVE_POLICY_FAILED_LATE'));
      await assert.rejects(access(join(root, 'replaced.txt')), { code: 'ENOENT' });
      const restored = firstValueFrom(commits$);
      policy.next(admit); await restored;
      await send('native_tool', 'cancelled');
      const cancelling = gates.get('cancelled')!;
      const cancelled = await cancelling.entered.promise;
      assert.deepEqual(await firstValueFrom(from(session.cancel()).pipe(timeout(3_000))), { canceled: true });
      assert.equal(cancelled.signal.aborted, true);
      assert.equal((await cancelling.settled.promise).isError, true);
      cancelling.release.resolve();
      await assert.rejects(access(join(root, 'cancelled.txt')), { code: 'ENOENT' });
      await send('native_tool', 'downstream'); await downstreamEntered.promise;
      const withdrawn = firstValueFrom(members.instances$.pipe(filter(ids => ids.length === 0), timeout(3_000)));
      selected.next([]); await withdrawn;
      const returned = firstValueFrom(members.instances$.pipe(filter(ids => ids.length === 1), timeout(3_000)));
      selected.next(['right']); await returned;
      assert.equal((await gates.get('downstream')!.entered.promise).signal.aborted, true,
        '下游gate期间曾真实撤声明，即使同政策恢复，原准入也不能复活');
      downstreamRelease.resolve(); await idle();
      assert.equal((await gates.get('downstream')!.settled.promise).isError, true);
      await assert.rejects(access(join(root, 'downstream.txt')), { code: 'ENOENT' });
      await send('native_tool', 'guarded'); await session.whenIdle();
      assert.ok(guardCalls > 0, '原生其他guard实际拒绝了本次调用，声明不能覆盖其结论');
      await assert.rejects(access(join(root, 'guarded.txt')), { code: 'ENOENT' });
      const events = agent.session.snapshotEvents();
      const denied = events.filter(event => event.type === 'tool/result' ? event.data.message.isError
        : event.type === 'tool/ptc-dispatch' && event.data.isError);
      assert.ok(denied.length >= 3, '不可见、政策撤销、其他guard各自留下真实失败结算');
      const removed = firstValueFrom(members.instances$.pipe(filter(ids => ids.length === 0), timeout(3_000)));
      selected.next([]); await removed;
      await agent.ctx.systemPrompt.assemble(assembleContextFor(agent, new AbortController().signal));
      assert.equal(agent.ctx.tools.schemas(agent).some(value => value.name === 'native_tool'), false,
        '最后一个贡献退出才撤销本会话原生工具可见性，registry中的实际工具仍由安装方持有');
      assert.ok(app.native.tools.get('native_tool'));
      assert.deepEqual(executions, ['accepted', 'shared']);
      assert.deepEqual(app.errors, []);
      const reinstated = firstValueFrom(members.instances$.pipe(filter(ids => ids.length === 1), timeout(3_000)));
      selected.next(['right']); await reinstated;
      await send('native_tool', 'failed');
      const failing = gates.get('failed')!;
      const retiring = await failing.entered.promise;
      failed.next(true);
      assert.equal((await firstValueFrom(from(failing.settled.promise).pipe(timeout(3_000)))).isError, true);
      assert.equal(retiring.signal.aborted, true, 'Program失败撤销Agent声明安装，同时结束自有政策等待');
      failing.release.resolve();
      await assert.rejects(access(join(root, 'failed.txt')), { code: 'ENOENT' });
      session.release();
    } finally {
      release.resolve(); sharedRelease.resolve(); downstreamRelease.resolve();
      for (const gate of gates.values()) gate.release.resolve();
      removeGuard(); removeDownstream(); removeResults(); removeUndeclared(); removeNative();
      selected.complete(); enabled$.complete(); commits$.complete(); policy.complete(); failed.complete(); await app.dispose();
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

test('同会话模块原生工具的enabled或admit冲突在取得时硬失败，不进入准入或文件副作用', { timeout: 15_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-native-contribution-conflict-'));
  const empty = defineSurface({ views: {}, actions: {} });
  const composed = composeSurface({ dependencies: collectionSurface(empty) });
  const program = defineProgram({ input: z.null(), surface: composed.surface });
  let admissions = 0, executions = 0, models = 0;
  const admit = async () => { admissions++; }, otherAdmit = async () => { admissions++; };
  const definition = defineApplication({ name: 'native-contribution-conflict', admit: async () => {}, environments:
    (['enabled', 'admit'] as const).map(conflict => defineEnvironment({ id: conflict, instructions: '相同名称必须有一致的实际参与政策。',
      program: implementProgram(program, { Component() {
        return h(Fragment, null,
          h(Module, { mount: composed.mounts.dependencies.instance('left') },
            NativeTools({ names: ['native_tool'], enabled: true, admit })),
          h(Module, { mount: composed.mounts.dependencies.instance('right') },
            NativeTools({ names: ['native_tool'], enabled: conflict !== 'enabled', admit: conflict === 'admit' ? otherAdmit : admit })));
      } }),
    })),
  });
  const app = await boot(join(root, 'home'), definition, async function* () {
    models++; throw new Error('NATIVE_CONTRIBUTION_CONFLICT_MUST_NOT_CALL_MODEL');
  });
  const remove = app.native.tools.register({ name: 'native_tool', description: '冲突声明不能到达的真实副作用',
    parameters: { type: 'object', properties: {}, additionalProperties: false },
    output: { schema: { type: 'null' }, render: () => [{ type: 'text', text: 'null' }] },
    async execute() { executions++; await writeFile(join(root, 'unexpected.txt'), 'executed'); return null; },
  });
  try {
    for (const environment of ['enabled', 'admit'] as const) {
      const sessionId = 'native-conflict-' + environment;
      await assert.rejects(app.host.create(environment, { workspaceId: app.workspace.workspaceId, sessionId }),
        /REACTIVE_NATIVE_TOOL_CONTRIBUTION_CONFLICT: native_tool/u);
      assert.equal(app.native.agents.get(SessionId(sessionId)), undefined);
    }
    assert.equal(admissions, 0); assert.equal(executions, 0); assert.equal(models, 0);
    await assert.rejects(access(join(root, 'unexpected.txt')), { code: 'ENOENT' });
    assert.deepEqual(app.errors, []);
  } finally { remove(); await app.dispose(); }
});

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
        useResource(jobs.state);
        const toolNames = useResource(capability.toolNames);
        useResource(approval.forSession(conversation).policy);
        useRelation(() => retired.pipe(filter(id => id === conversation.identity.sessionId), tap(() => { throw retiredFailure; })), [conversation]);
        useLayoutEffect(() => { programSessions.set(conversation.identity.sessionId, { source: conversation, jobs }); }, [conversation, jobs]);
        return NativeTools({ names: toolNames.state.status === 'ready' ? toolNames.state.value : [] });
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
    assert.equal(capability.toolNames$, capability.toolNames.values$);
    assert.ok((await capability.toolNames.read()).includes('job_list'));
    assert.equal(await firstValueFrom(approval.forSession(programSession.source).policy$), 'never', 'Program首次提交即可读取同一原生审批政策');
    assert.equal(capability.forSession(programSession.source), programSession.jobs);
    assert.throws(() => capability.forSession({ identity: programSession.source.identity }), /REACTIVE_NATIVE_SESSION_REQUIRED/);
    let observedSession = capability.forSession(session);
    const absent = observedSession.job(JobId('native-lifecycle-0000000000000000'));
    assert.equal(observedSession.job(absent.id), absent);
    assert.throws(() => absent.snapshot(), /unknown job/u);
    await assert.rejects(absent.state.read(), /unknown job/u);
    assert.throws(() => absent.stop('不存在的目标不能进入执行'), /unknown job/u);
    assert.throws(() => absent.remove(), /unknown job/u);
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
    assert.equal(observedSession.job(first).state, reader.state);
    assert.equal((await reader.state.read()).status, 'running');
    assert.deepEqual(await reader.output.read(), observed, '资源读取同样不推进原生输出游标');
    assert.equal(reader.output.peek().status, 'pending', '最后一个资源消费者退出撤下有效快照');
    assert.deepEqual(secondObserved, observed, '两个SDK观察者读取同一个真实输出，均不消费模型cursor');
    const foreignReader = capability.forSession(foreign).job(first);
    assert.throws(() => foreignReader.readAt(0), '另一个真实owner不能读取输出');
    await assert.rejects(foreignReader.output.read());
    assert.throws(() => foreignReader.stop('外会话不能停止原进程'));
    assert.deepEqual(reader.readAt(0), observed, '重复SDK观察不消耗模型游标');
    assert.throws(() => reader.remove(), /still running/u, '移除记录不能暗中停止仍活跃的生产进程');
    assert.doesNotThrow(() => process.kill(children[0]!.child.pid!, 0));
    assert.throws(() => foreignReader.remove(), '另一个真实owner不能移除原Job');
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
    assert.throws(() => reader.remove(), /REACTIVE_SCOPE_DISPOSED/u, '旧SDK退出后不能借原目标移除记录');
    const preCancelled = new AbortController(), removalCancelled = new Error('JOB_REMOVAL_CANCELLED_BEFORE_CONTROL');
    preCancelled.abort(removalCancelled);
    assert.throws(() => controlling.remove({ signal: preCancelled.signal }), error => error === removalCancelled);
    assert.equal(jobs.get(first, owner).status, 'killed', '预取消不改变正式注册表');
    const removed = firstValueFrom(observedSession.events$.pipe(filter(event => event.type === 'removed' && event.job.id === first), timeout(3_000)));
    const removedList = firstValueFrom(observedSession.list$.pipe(filter(rows => !rows.some(row => row.id === first)), timeout(3_000)));
    const removedState = firstValueFrom(controlling.state.observations$.pipe(filter(value => value.status === 'error'), timeout(3_000)));
    const removedOutput = firstValueFrom(controlling.output.observations$.pipe(filter(value => value.status === 'error'), timeout(3_000)));
    controlling.remove();
    assert.equal((await removed).type, 'removed');
    assert.deepEqual(await removedList, []);
    for (const observation of await Promise.all([removedState, removedOutput])) {
      assert.equal(observation.status, 'error');
      if (observation.status === 'error') assert.match(String(observation.error), /unknown job/u);
    }
    assert.equal(observedSession.job(first), controlling, '移除后固定目标不重定向或伪造墓碑');
    assert.throws(() => controlling.snapshot(), /unknown job/u);
    assert.throws(() => controlling.remove(), /unknown job/u);

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
