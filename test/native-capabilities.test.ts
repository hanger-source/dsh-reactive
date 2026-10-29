import { test } from 'node:test';
import assert from 'node:assert/strict';
import { access, mkdir, mkdtemp, readFile, realpath, writeFile } from 'node:fs/promises';
import { spawn, type ChildProcess } from 'node:child_process';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate as immediate } from 'node:timers/promises';
import { Fragment, createElement as h, useLayoutEffect } from 'react';
import { BehaviorSubject, Observable, Subject, filter, firstValueFrom, from, tap, timeout } from 'rxjs';
import { LlmAdapter, ToolCallId, type GenerateOptions } from '@deepseek-ai/dsh-llm';
import { assembleContextFor } from '@deepseek-ai/dsh-agent';
import { Context, symbols } from '@deepseek-ai/cordis';
import Registry from '@deepseek-ai/dsh-typert-registry';
import SandboxPolicy from '@deepseek-ai/dsh-sandbox-policy';
import WorkspaceFiles from '@deepseek-ai/dsh-api-workspace-files';
import type { FileSystem } from '@deepseek-ai/dsh-fs';
import type { FileUploads } from '@deepseek-ai/dsh-client-file-upload';
import LocalJobs from '@deepseek-ai/dsh-jobs-local';
import * as jobTools from '@deepseek-ai/dsh-tool-jobs';
import type { JobEvent, JobOutcome } from '@deepseek-ai/dsh-jobs';
import { SessionId } from '@deepseek-ai/dsh-session';
import { WorkspaceId } from '@deepseek-ai/dsh-workspace';
import type { ToolDefinition, ToolExecutionResult } from '@deepseek-ai/dsh-tools';
import { collectionSurface, defineAction, implementAction, defineApplication, defineEnvironment, defineProgram, defineSurface, composeSurface, readActionPresentation, ConversationAccessRevokedError, SessionFileOutsideWorkspaceError, Scope, ScopeDisposedError, z, type Eligibility, type Observation, type NativeToolCall, type NativeToolAdmission, type ProgramSession, type SessionFiles } from '@hanger-source/dsh-reactive';
import { implementProgram, NativeTools, Offer, Expose, Module, useAction, useObservable, useResource, useRelation } from '@hanger-source/dsh-reactive/program';
import { boot, text, tool } from './native-support.ts';
import type { ConversationHandle } from '@hanger-source/dsh-reactive-dsh';
import { createRuntime, nativeModel, requireActivePlugin } from '@hanger-source/dsh-reactive-dsh';
import { createNativeJobs, JobId, type NativeJobSpec, type NativeSessionJobs } from '@hanger-source/dsh-reactive-dsh/jobs';
import { createNativeApproval } from '@hanger-source/dsh-reactive-dsh/approval';
import { createNativeFiles } from '@hanger-source/dsh-reactive-dsh/files';
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
    const eligibility = new BehaviorSubject<Observation<Eligibility>>({ status: 'ready', generation: 1, value: { allowed: true }, observedAt: Date.now() });
    const entered = Promise.withResolvers<NativeToolCall>(), release = Promise.withResolvers<void>();
    const sharedEntered = Promise.withResolvers<NativeToolCall>(), sharedRelease = Promise.withResolvers<void>();
    const gates = new Map(['cancelled', 'replaced', 'downstream', 'failed', 'eligibility-pending', 'eligibility-denied'].map(marker => [marker, {
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
              NativeTools({ names: ['native_tool'], enabled, admit: current, eligibility })))) : null;
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
      assert.deepEqual(Object.values(nativeInputs?.calls ?? {}).find(call => call.callId === written)?.inputs[0]?.scope, { merchantId: 'merchant-a' },
        '政策收到独立的输入快照，不能修改原生调用的原请求权限事实');
      assert.equal(agent.ctx.tools.schemas(agent).filter(value => value.name === 'native_tool').length, 1);
      assert.ok(!agent.ctx.tools.schemas(agent).some(value => value.name === 'undeclared_tool'), '同一registry可见性决定PTC SDK，不仅过滤模型列表');
      const unavailable: readonly [string, Observation<Eligibility>][] = [
        ['pending', { status: 'pending', generation: 2 }],
        ['denied', { status: 'ready', generation: 4, value: { allowed: false, reason: '业务资格撤销' }, observedAt: Date.now() }],
      ];
      for (const [reason, observation] of unavailable) {
        const marker = 'eligibility-' + reason, gate = gates.get(marker)!;
        await send('native_tool', marker);
        const waiting = await firstValueFrom(from(gate.entered.promise).pipe(timeout(3_000)));
        assert.equal(waiting.signal.aborted, false);
        // 同一tick恢复，React可能只提交恢复后的值；实际借用必须保留中间撤销事实。
        eligibility.next(observation);
        eligibility.next({ status: 'ready', generation: observation.generation + 1, value: { allowed: true }, observedAt: Date.now() });
        assert.equal(waiting.signal.aborted, true, '资格恢复不能复活已经撤销的原调用signal');
        await idle();
        assert.equal((await gate.settled.promise).isError, true, '不等待迟到admit也结算旧调用');
        gate.release.resolve();
        await immediate();
        await assert.rejects(access(join(root, marker + '.txt')), { code: 'ENOENT' });
        assert.equal(agent.ctx.tools.schemas(agent).filter(value => value.name === 'native_tool').length, 1,
          '两个同源贡献没有因局部通知时序冲突，恢复后保留同一原生入口');
        const recovered = 'recovered-' + reason;
        await send('native_tool', recovered); await idle();
        const written = await readFile(join(root, recovered + '.txt'), 'utf8');
        assert.equal(written, admissions.find(call => nativeArguments.parse(call.arguments).marker === recovered)?.source.callId);
        assert.equal(waiting.signal.aborted, true);
      }
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
      assert.deepEqual(executions, ['accepted', 'recovered-pending', 'recovered-denied', 'shared']);
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
      selected.complete(); enabled$.complete(); commits$.complete(); policy.complete(); failed.complete(); eligibility.complete(); await app.dispose();
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

test('同会话模块原生工具的enabled、admit或资格来源冲突在取得时硬失败，不进入准入或文件副作用', { timeout: 15_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-native-contribution-conflict-'));
  const empty = defineSurface({ views: {}, actions: {} });
  const composed = composeSurface({ dependencies: collectionSurface(empty) });
  const program = defineProgram({ input: z.null(), surface: composed.surface });
  let admissions = 0, executions = 0, models = 0;
  const admit = async () => { admissions++; }, otherAdmit = async () => { admissions++; };
  const allowed: Observation<Eligibility> = { status: 'ready', generation: 1, value: { allowed: true }, observedAt: Date.now() };
  const eligibility = new BehaviorSubject(allowed), otherEligibility = new BehaviorSubject(allowed);
  const definition = defineApplication({ name: 'native-contribution-conflict', admit: async () => {}, environments:
    (['enabled', 'admit', 'eligibility'] as const).map(conflict => defineEnvironment({ id: conflict, instructions: '相同名称必须有一致的实际参与政策。',
      program: implementProgram(program, { Component() {
        return h(Fragment, null,
          h(Module, { mount: composed.mounts.dependencies.instance('left') },
            NativeTools({ names: ['native_tool'], enabled: true, admit, eligibility })),
          h(Module, { mount: composed.mounts.dependencies.instance('right') },
            NativeTools({ names: ['native_tool'], enabled: conflict !== 'enabled', admit: conflict === 'admit' ? otherAdmit : admit,
              eligibility: conflict === 'eligibility' ? otherEligibility : eligibility })));
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
    for (const environment of ['enabled', 'admit', 'eligibility'] as const) {
      const sessionId = 'native-conflict-' + environment;
      await assert.rejects(app.host.create(environment, { workspaceId: app.workspace.workspaceId, sessionId }),
        /REACTIVE_NATIVE_TOOL_CONTRIBUTION_CONFLICT: native_tool/u);
      assert.equal(app.native.agents.get(SessionId(sessionId)), undefined);
    }
    assert.equal(admissions, 0); assert.equal(executions, 0); assert.equal(models, 0);
    await assert.rejects(access(join(root, 'unexpected.txt')), { code: 'ENOENT' });
    assert.deepEqual(app.errors, []);
  } finally { remove(); eligibility.complete(); otherEligibility.complete(); await app.dispose(); }
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

test('本地Files惰性借入精确会话，真实文件变化与取消不越过范围，Host退出等待原生I/O和unwatch', { timeout: 20_000 }, async () => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'dsh-reactive-local-files-')));
  const directory = join(root, 'workspace'), subdirectory = join(directory, 'sub');
  await mkdir(subdirectory, { recursive: true });
  const artifact = join(directory, 'artifact.txt');
  await writeFile(artifact, 'title\ninitial\n');
  await writeFile(join(subdirectory, 'base.txt'), 'base');
  await writeFile(join(subdirectory, 'artifact.bin'), new Uint8Array([7, 0, 255, 9]));
  await writeFile(join(root, 'outside.txt'), '工作区外的真实文件');
  const capability = createNativeFiles({ service: 'provided' });
  const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
  const programFiles = new Map<string, SessionFiles>();
  const errors: unknown[] = [];
  let models = 0;
  const definition = defineApplication({ name: 'local-files', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '文件读取和观察不启动模型', program: implementProgram(program, { Component({ conversation }) {
      const files = conversation.files;
      useLayoutEffect(() => { programFiles.set(conversation.identity.sessionId, files); }, [conversation, files]);
      return null;
    } }),
  })] });
  const releaseResolve = Promise.withResolvers<void>(), resolveEntered = Promise.withResolvers<void>();
  const releaseRead = Promise.withResolvers<void>(), readEntered = Promise.withResolvers<void>();
  const releaseUnwatch = Promise.withResolvers<void>(), unwatchEntered = Promise.withResolvers<void>();
  const releaseUpload = Promise.withResolvers<void>(), uploadEntered = Promise.withResolvers<void>();
  const subscriptions: Array<{ unsubscribe(): void }> = [];
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), files: capability,
    actorId: 'local-file-reader', onError: event => errors.push(event.error),
    async installNative(ctx, home) {
      await requireActivePlugin(ctx.plugin(Registry));
      await requireActivePlugin(ctx.plugin(SandboxPolicy, { mode: 'read-only', workspaceRoot: directory }));
      await requireActivePlugin(ctx.plugin(WorkspaceFiles, { maxLines: 8, maxBytes: 4096, maxFileBytes: 4096, maxEntries: 16 }));
      await capability.install(ctx, home);
    },
    model: nativeModel({ provider: 'local-files', model: 'unused', create: () => new class extends LlmAdapter {
      stream() { models++; return text('文件读取无需模型'); }
    }() }),
  });
  try {
    const fs: FileSystem = Reflect.get(runtime.native.fs, symbols.original);
    const service: WorkspaceFiles = Reflect.get(runtime.native.workspaceFiles, symbols.original);
    const uploads: FileUploads = Reflect.get(runtime.native.fileUploads, symbols.original);
    const resolve = fs.resolve.bind(fs), watch = fs.watch.bind(fs), read = service.read.bind(service), bytes = service.readBytes.bind(service);
    const upload = uploads.upload.bind(uploads);
    uploads.upload = async (...args) => {
      const receipt = await upload(...args);
      uploadEntered.resolve(); await releaseUpload.promise;
      return receipt;
    };
    const resolved: string[] = [], watches: Array<{ path: string; active: boolean; closed: PromiseWithResolvers<void> }> = [];
    let holdResolve = false, holdRead = false, holdUnwatch = false, reads = 0, requireWatch = false;
    fs.resolve = async (path, options) => {
      resolved.push(path);
      const target = await resolve(path, options);
      if (holdResolve && path === directory && options?.cwd === undefined) {
        holdResolve = false; resolveEntered.resolve(); await releaseResolve.promise;
      }
      return target;
    };
    fs.watch = async (target, notify, signal) => {
      const unwatch = await watch(target, notify, signal);
      const current = { path: fs.processPath(target), active: true, closed: Promise.withResolvers<void>() };
      watches.push(current);
      return async () => {
        await unwatch();
        if (holdUnwatch) { unwatchEntered.resolve(); await releaseUnwatch.promise; }
        current.active = false; current.closed.resolve();
      };
    };
    service.read = async (scope, path, range, signal) => {
      reads++;
      if (requireWatch && path === 'artifact.txt')
        assert.ok(watches.some(current => current.active && current.path === directory), '正式命名空间watch ready前不得读取资源切面');
      return read(scope, path, range, signal);
    };
    service.readBytes = async (...args) => {
      const value = await bytes(...args);
      if (holdRead) { holdRead = false; readEntered.resolve(); await releaseRead.promise; }
      return value;
    };
    const workspace = await runtime.application.createWorkspace({ directory });
    const session = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    const baseline = { resolves: resolved.length, watches: watches.length, reads };
    const files = session.files, nativeFiles = capability.forSession(session);
    const range = { offset: 2, limit: 1 }, page = files.text('artifact.txt', range), metadata = files.metadata('artifact.txt');
    range.offset = 1;
    assert.equal(session.files, files); assert.equal(files, nativeFiles);
    assert.equal(files.text('artifact.txt', { offset: 2, limit: 1 }), page);
    assert.deepEqual({ resolves: resolved.length, watches: watches.length, reads }, baseline, '取得Files与资源对象不开始I/O或watch');
    assert.throws(() => capability.forSession({ id: session.id }), /REACTIVE_NATIVE_SESSION_REQUIRED/u);
    assert.deepEqual(await files.readBytes('artifact.bin', { baseFile: 'sub/base.txt', range: { offset: 1, length: 2 } }),
      { ...(await files.stat('sub/artifact.bin')), offset: 1, data: new Uint8Array([0, 255]), eof: false });
    for (const [path, baseFile, denied] of [
      ['../outside.txt', undefined, '../outside.txt'],
      ['artifact.txt', '../outside.txt', '../outside.txt'],
      ['../../outside.txt', 'sub/base.txt', '../../outside.txt'],
    ] as const) {
      await assert.rejects(files.readBytes(path, { ...(baseFile === undefined ? {} : { baseFile }) }),
        error => error instanceof SessionFileOutsideWorkspaceError && error.path === denied);
    }
    const beforeWatch = watches.length;
    requireWatch = true;
    subscriptions.push(page.values$.subscribe(), metadata.values$.subscribe());
    assert.equal((await page.read()).text, 'initial'); await metadata.read();
    assert.equal(watches.length, beforeWatch + 1, '同路径文本和元数据共享一个真实watch');
    const changed = firstValueFrom(page.values$.pipe(filter(value => value.text === 'changed'), timeout(5_000)));
    await writeFile(artifact, 'title\nchanged\n'); await changed;
    requireWatch = false;
    const sharedWatch = watches.at(-1)!;
    subscriptions.splice(0).forEach(subscription => subscription.unsubscribe());
    await sharedWatch.closed.promise;
    assert.equal(page.peek().status, 'pending');
    await runtime.application.close(session.id);
    assert.equal(runtime.native.agents.get(SessionId(session.id)), undefined);
    assert.equal((await programFiles.get(session.id)!.readText('artifact.txt')).text, 'title\nchanged');
    assert.equal(runtime.native.agents.get(SessionId(session.id)), undefined, '冷文件读取不激活Agent');

    const cancelled = new AbortController(), reason = new Error('LOCAL_FILE_RESOLVE_CONSUMER_LEFT');
    const beforeResolve = resolved.length, beforeRead = reads;
    holdResolve = true;
    const pending = files.readText('artifact.txt', {}, { signal: cancelled.signal });
    let rejected = false;
    const rejection = assert.rejects(pending, error => error === reason).then(() => { rejected = true; });
    await resolveEntered.promise; cancelled.abort(reason); await immediate();
    assert.equal(rejected, true, '不合作的真实resolve回执不能拖住消费者取消'); await rejection;
    releaseResolve.resolve(); await immediate();
    assert.deepEqual(resolved.slice(beforeResolve), [directory], '迟到路径解析不能继续目标解析或启动文件读取');
    assert.equal(reads, beforeRead);

    subscriptions.push(page.values$.subscribe()); await page.read();
    const watchCaller = new AbortController(), watchReason = new Error('LOCAL_FILE_WATCH_CONSUMER_LEFT');
    const explicitWatch = files.watch('artifact.txt', { signal: watchCaller.signal });
    await firstValueFrom(explicitWatch.changes$.pipe(filter(frame => frame.kind === 'ready'), timeout(5_000)));
    let watchFailure: unknown, watchClosed = false;
    subscriptions.push(explicitWatch.changes$.subscribe({ error: error => { watchFailure = error; } }));
    void explicitWatch.closed.then(() => { watchClosed = true; }, error => { errors.push(error); });
    holdUnwatch = true;
    watchCaller.abort(watchReason); await immediate();
    assert.equal(watchFailure, watchReason, 'raw watch取消立即交原reason，不等正式unwatch');
    await unwatchEntered.promise;
    assert.equal(watchClosed, false, '观察退出不冒充实际watch已经释放');
    const uploading = session.uploadFile(new Uint8Array([4, 5, 6]), { name: 'host-shutdown.bin' });
    let uploadSettled = false;
    void uploading.result.then(() => { uploadSettled = true; }, () => { uploadSettled = true; });
    await uploadEntered.promise;
    holdRead = true;
    let readCancelled = false;
    const reading = files.readBytes('sub/artifact.bin');
    const stopped = assert.rejects(reading, error => error instanceof ScopeDisposedError).then(() => { readCancelled = true; });
    await readEntered.promise;
    const closing = runtime.application.dispose();
    let disposed = false; void closing.then(() => { disposed = true; }, () => { disposed = true; });
    await immediate();
    assert.equal(uploadSettled, false, 'Host仍在等待已经持久化但尚未交付的原生上传回执');
    assert.equal(readCancelled, true, 'Host退出先撤Files等待，不能拖到原生上传和Agent结算后');
    assert.throws(() => files.text('artifact.txt'), error => error instanceof ScopeDisposedError, '保留的旧Files读侧不能越过Host退出继续读取');
    assert.throws(() => page.peek(), error => error instanceof ScopeDisposedError);
    releaseUpload.resolve(); await uploading.result; await stopped;
    await unwatchEntered.promise; await immediate();
    assert.equal(disposed, false, 'Host退出等待已借文件读侧的真实unwatch');
    releaseUnwatch.resolve(); await Promise.all(watches.map(current => current.closed.promise)); await immediate();
    assert.equal(disposed, false, 'unwatch完成后仍须等不合作的实际读取回执');
    releaseRead.resolve(); await closing;
    assert.equal(watchClosed, true);
    assert.equal(nativeFiles.dispose(), nativeFiles.dispose(), '重复释放加入同一真实清理回执');
    assert.equal((await service.stat({ sessionId: SessionId(session.id), workspaceRoot: directory }, 'artifact.txt', new AbortController().signal)).absolutePath,
      artifact, 'Host只退出借入读侧，正式外部WorkspaceFiles仍由原宿主持有');
    assert.throws(() => session.files);
    assert.equal(models, 0); assert.deepEqual(errors, []);
  } finally {
    releaseResolve.resolve(); releaseRead.resolve(); releaseUnwatch.resolve(); releaseUpload.resolve();
    subscriptions.forEach(subscription => subscription.unsubscribe());
    await runtime.dispose(); await capability.dispose();
  }
});

test('opt-in原生安装拒绝并释放PENDING与失败插件；官方Jobs围绕实际Node进程持有观察、停止及owner释放', { timeout: 15_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-native-installation-'));
  const directory = join(root, 'workspace'); await mkdir(directory);
  const errors: unknown[] = [], children: Array<{ child: ChildProcess; closed: Promise<JobOutcome> }> = [];
  const capability = createNativeJobs({ registry: { kind: 'provided' }, tools: { completionDelivery: 'quiet' } });
  const sharedCapability = createNativeJobs({ registry: { kind: 'provided' } });
  const observations: Array<{ unsubscribe(): void }> = [];
  const nativeEvents: JobEvent[] = [], observedEvents: JobEvent[] = [];
  let releaseNativeEvents: (() => void) | undefined;
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
      await requireActivePlugin(ctx.plugin(LocalJobs, { retainBytes: 128, settledRetainBytes: 8 }));
      await pending;
      assert.equal(ctx.tools.get('job_list'), undefined, '依赖后来装入不能使已拒绝的旧控制器复活');
      await capability.install(ctx, home);
      await sharedCapability.install(ctx, home);
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
    let programSession = programSessions.get(session.id)!;
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
    assert.throws(() => absent.collect(), /unknown job/u);
    assert.throws(() => absent.invalidate(), /unknown job/u);
    assert.throws(() => absent.stop('不存在的目标不能进入执行'), /unknown job/u);
    assert.throws(() => absent.remove(), /unknown job/u);
    const launch = (ownership: 'session' | 'unowned' = 'session') => {
      const spec: NativeJobSpec = { kind: 'native-lifecycle', label: '隔离Node进程，显式停止前保持运行',
        run(job) {
          // 此生产方只持有实际ChildProcess；id、游标、事件与终态均由官方JobRegistry拥有。
          const child = spawn(process.execPath, ['-e', "process.stdout.write('native-job-ready\\n');process.stdin.on('data',data=>process.stdout.write(data));setInterval(()=>{},1000);"],
            { cwd: directory, env: {}, stdio: ['pipe', 'pipe', 'pipe'] });
          let cancelled = false, failure: Error | undefined;
          child.stdout.on('data', (data: Buffer) => job.append(data.toString('utf8'), { channel: 'stdout' }));
          child.stderr.on('data', (data: Buffer) => job.append(data.toString('utf8'), { channel: 'stderr' }));
          child.once('error', error => { failure = error; });
          const closed = new Promise<JobOutcome>(resolve => child.once('close', (code, signal) => {
            const detail = failure?.message ?? `exit=${code};signal=${signal}`;
            resolve({ status: cancelled ? 'killed' : failure || code !== 0 ? 'failed' : 'completed',
              detail, result: `native-process ${job.id}: ${detail}` });
          }));
          children.push({ child, closed });
          return { cancel() { cancelled = true; child.kill('SIGTERM'); }, done: closed };
        },
      };
      return ownership === 'session' ? programSession.jobs.start(spec) : jobs.start(spec);
    };
    const ready = (job: ReturnType<typeof launch>) => firstValueFrom(observedSession.job(job).output$
      .pipe(filter(value => value.chunks.some(chunk => chunk.text.includes('native-job-ready')))));
    const first = launch(), reader = observedSession.job(first);
    observations.push(reader.state$.subscribe());
    const firstObserver = firstValueFrom(reader.output$.pipe(filter(value => value.chunks.some(chunk => chunk.text.includes('native-job-ready')))));
    const secondObserver = firstValueFrom(reader.output$.pipe(filter(value => value.chunks.some(chunk => chunk.text.includes('native-job-ready')))));
    const [observed, secondObserved] = await Promise.all([firstObserver, secondObserver]);
    assert.equal(observedSession.job(first).state, reader.state);
    assert.deepEqual(await reader.state.read(), jobs.get(first, owner), '已连接state随真实stdout追加更新输出元数据');
    assert.deepEqual(await reader.output.read(), observed, '资源读取同样不推进原生输出游标');
    assert.equal(reader.output.peek().status, 'pending', '最后一个资源消费者退出撤下有效快照');
    assert.deepEqual(secondObserved, observed, '两个SDK观察者读取同一个真实输出，均不消费模型cursor');
    const foreignJobs = capability.forSession(foreign), foreignReader = foreignJobs.job(first);
    assert.throws(() => foreignReader.readAt(0), '另一个真实owner不能读取输出');
    assert.throws(() => foreignReader.collect(), '另一个真实owner不能接管模型的输出游标');
    assert.throws(() => foreignReader.invalidate(), '另一个真实owner不能为外来目标发布失效');
    await assert.rejects(foreignReader.output.read());
    assert.throws(() => foreignReader.stop('外会话不能停止原进程'));
    assert.deepEqual(reader.readAt(0), observed, '重复SDK观察不消耗模型游标');
    assert.throws(() => reader.remove(), /still running/u, '移除记录不能暗中停止仍活跃的生产进程');
    assert.doesNotThrow(() => process.kill(children[0]!.child.pid!, 0));
    assert.throws(() => foreignReader.remove(), '另一个真实owner不能移除原Job');
    assert.deepEqual(programSession.jobs.job(first).readAt(0), observed, 'Program与外部SDK消费同一原生Job，不复制输出状态');
    assert.equal(reader.collect().chunks.map(chunk => chunk.text).join(''), 'native-job-ready\n');
    assert.equal(jobs.read(first, owner).chunks.length, 0, '显式collect推进正式模型游标，不另建SDK消费位置');
    assert.deepEqual(reader.readAt(0), observed, '模型读取也不破坏SDK观察者的绝对offset');
    assert.equal((await reader.wait(1)).status, 'running', '观察等待超时返回真实运行快照，不停止进程');
    await assert.rejects(reader.wait(0), /invalid wait timeout/u);
    const retainedOutput = 'retained-before-stop\n'.repeat(12);
    const expectedTotal = observed.next + Buffer.byteLength(retainedOutput);
    const advanced = firstValueFrom(reader.state$.pipe(filter(job => job.output.total === expectedTotal), timeout(3_000)));
    children[0]!.child.stdin!.write(retainedOutput);
    const appended = await advanced;
    assert.deepEqual(appended, jobs.get(first, owner));
    assert.ok(appended.output.earliest > 0, '真实进程输出越过正式live retention，已连接state也更新earliest');
    const sdkWait = assert.rejects(reader.wait(5_000), /REACTIVE_SCOPE_DISPOSED/u);
    observedSession.dispose();
    await sdkWait;
    assert.equal(jobs.get(first, owner).status, 'running', '释放SDK观察不调用原生kill');
    assert.doesNotThrow(() => process.kill(children[0]!.child.pid!, 0), '真实Node进程仍存活');
    observedSession = capability.forSession(session);
    const controlling = observedSession.job(first);
    const sharedSession = sharedCapability.forSession(session);
    const connectedSessions = [observedSession, programSession.jobs, sharedSession];
    const connectedReaders = connectedSessions.map(source => source.job(first));
    for (const source of connectedSessions) observations.push(source.list$.subscribe());
    for (const target of connectedReaders) observations.push(target.state.observations$.subscribe(), target.output.observations$.subscribe());
    observations.push(observedSession.events$.subscribe(event => observedEvents.push(event)));
    releaseNativeEvents = jobs.events.subscribe({ owner }, event => nativeEvents.push(event));
    const agent = runtime.native.agents.get(owner)!;
    assert.equal(controlling.stop('明确停止本次Node进程'), 'requested');
    const [stopped, programStopped] = await Promise.all([controlling.wait(5_000), programSession.jobs.job(first).wait(5_000)]);
    const producerOutcome = await children[0]!.closed;
    assert.equal(stopped.status, 'killed'); assert.deepEqual(programStopped, stopped);
    assert.equal(agent.inbox.nextStep.filter(message => message.source.kind === 'tool-jobs'
      && message.content.some(part => part.type === 'text' && part.text.includes(first))).length, 1,
    '两个SDK的非消费等待仍让正式ToolJobs向真实Agent投递一次quiet完成通知');
    const beforeCollect = jobs.get(first, owner);
    const pendingOutput = jobs.readAt(first, observed.next, owner);
    const eventCount = nativeEvents.length;
    const collected = controlling.collect();
    assert.equal(collected.job.status, 'killed');
    assert.equal(collected.result, producerOutcome.result, '非消费等待保留终态result，collect沿正式cursor首次交付');
    assert.deepEqual(collected.chunks, pendingOutput.chunks);
    const afterCollect = jobs.get(first, owner);
    assert.ok(afterCollect.output.earliest > beforeCollect.output.earliest, '终态消费真实trim，而非碰巧读取相同快照');
    for (const target of connectedReaders) {
      assert.deepEqual(await target.state.read(), afterCollect, '同Registry不同source和capability的已连接state共同重读');
      assert.deepEqual(await target.output.read(), jobs.readAt(first, 0, owner), '消费trim后输出观察读取真实保留窗口');
    }
    for (const source of connectedSessions) assert.deepEqual(await source.state.read(), jobs.list(owner));
    assert.equal(nativeEvents.length, eventCount, '消费trim没有原生JobEvent');
    assert.deepEqual(observedEvents, nativeEvents, 'SDK events$只转交正式原生事件');
    assert.equal(jobs.read(first, owner).result, undefined, '原生cursor不会重放SDK已收取的终态result');
    assert.equal(controlling.collect().result, undefined, '再次collect仍沿正式单次交付');
    assert.throws(() => process.kill(children[0]!.child.pid!, 0), { code: 'ESRCH' });
    assert.throws(() => reader.remove(), /REACTIVE_SCOPE_DISPOSED/u, '旧SDK退出后不能借原目标移除记录');
    assert.throws(() => reader.invalidate(), /REACTIVE_SCOPE_DISPOSED/u, '旧SDK退出后不能向共享注册表发布失效');
    const preCancelled = new AbortController(), removalCancelled = new Error('JOB_REMOVAL_CANCELLED_BEFORE_CONTROL');
    preCancelled.abort(removalCancelled);
    assert.throws(() => controlling.collect({ signal: preCancelled.signal }), error => error === removalCancelled);
    assert.throws(() => controlling.invalidate({ signal: preCancelled.signal }), error => error === removalCancelled);
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

    const consumedOwnedJobs: Array<ReturnType<typeof launch>> = [];
    for (const consuming of ['tool', 'native'] as const) {
      const id = launch(consuming === 'tool' ? 'unowned' : 'session');
      if (consuming === 'native') consumedOwnedJobs.push(id);
      const producer = children.at(-1)!;
      await ready(id);
      const sources = [observedSession, programSession.jobs, sharedSession,
        ...(consuming === 'tool' ? [foreignJobs] : [])];
      const targets = sources.map(source => source.job(id));
      for (const source of sources) observations.push(source.list$.subscribe());
      for (const target of targets) observations.push(target.state.observations$.subscribe(), target.output.observations$.subscribe());
      const target = targets[0]!;
      const total = jobs.get(id, owner).output.total + Buffer.byteLength(retainedOutput);
      const outputReady = firstValueFrom(target.output$.pipe(filter(output => output.next === total), timeout(3_000)));
      producer.child.stdin!.write(retainedOutput);
      await outputReady;
      target.stop(`正式${consuming}消费试验结束生产进程`);
      await target.wait(5_000);
      const outcome = await producer.closed;
      const beforeRead = jobs.get(id, owner), eventsBeforeRead = nativeEvents.length;
      if (consuming === 'tool') {
        assert.equal(beforeRead.owner, undefined, '后台工具消费由正式注册表持有的unowned进程');
        const journalBefore = agent.session.snapshotEvents();
        const result = await runtime.native.tools.execute({ name: 'run_code', callId: ToolCallId('native-jobs-output-consumption'),
          arguments: { description: '消费已结束的真实进程输出', code: `return await tools.job_output(${JSON.stringify({ job_id: id })});` },
          signal: t.signal });
        assert.equal(result.isError, false, '无Agent的正式后台PTC工具链消费unowned Job');
        assert.deepEqual(agent.session.snapshotEvents(), journalBefore, '无会话归属的工具执行不向原会话写入turn或工具日志');
        assert.equal(jobs.read(id, owner).result, undefined, '正式job_output已经消费终态result');
      } else {
        const read = jobs.read(id, owner);
        assert.equal(read.result, outcome.result);
        assert.ok(read.job.output.earliest > beforeRead.output.earliest, '裸原生消费确实改变了保留窗口');
        for (const observedTarget of targets) assert.deepEqual(await observedTarget.state.read(), beforeRead,
          '公开原生消费不发JobEvent，持续观察不会假称已经更新');
        target.invalidate();
      }
      const afterRead = jobs.get(id, owner);
      assert.ok(afterRead.output.earliest > beforeRead.output.earliest, '正式消费后的trim必须在观察侧可见');
      for (const observedTarget of targets) {
        assert.deepEqual(await observedTarget.state.read(), afterRead);
        assert.deepEqual(await observedTarget.output.read(), jobs.readAt(id, 0, owner));
      }
      for (const source of sources) assert.deepEqual(await source.state.read(), jobs.list(source === foreignJobs ? SessionId(foreign.id) : owner));
      assert.equal(nativeEvents.length, eventsBeforeRead, '消费与显式invalidate均不制造原生事件');
      assert.deepEqual(observedEvents, nativeEvents);
      target.remove();
      assert.throws(() => process.kill(producer.child.pid!, 0), { code: 'ESRCH' });
    }

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
    const secondProducer = children.at(-1)!;
    const secondClosed = secondProducer.closed;
    const retainedSource = programSession.source, secondReader = observedSession.job(second);
    const stateSubscription = secondReader.state.observations$.subscribe();
    const outputSubscription = secondReader.output.observations$.subscribe();
    const caller = new AbortController(), callerReason = new Error('JOB_WAIT_CALLER_LEFT');
    const callerWait = assert.rejects(secondReader.wait(5_000, { signal: caller.signal }), error => error === callerReason);
    caller.abort(callerReason); await callerWait;
    assert.equal(jobs.get(second, owner).status, 'running', '取消等待保原caller reason，不成为Job停止命令');
    const cancelledWait = assert.rejects(secondReader.wait(5_000), error => error === session.signal.reason);
    const nativeWorkspace = runtime.native.workspaceRegistry.get(WorkspaceId(workspace.workspaceId))!;
    await nativeWorkspace.detachSession(owner); await cancelledWait;
    const reason: unknown = session.signal.reason;
    assert.ok(reason instanceof ConversationAccessRevokedError && reason.reason === 'membership-removed');
    assert.throws(() => secondReader.state.peek(), error => error === reason);
    assert.throws(() => secondReader.output.peek(), error => error === reason);
    assert.throws(() => secondReader.invalidate(), error => error === reason);
    assert.throws(() => programSession.jobs.list(), error => error === reason);
    assert.equal(jobs.get(second, owner).status, 'running', '成员撤销只退出旧SDK资格，原生Job仍由exact Agent持有');
    assert.doesNotThrow(() => process.kill(secondProducer.child.pid!, 0));
    await nativeWorkspace.attachSession(owner);
    const renewed = await runtime.application.open(session.id, program);
    assert.equal(runtime.native.agents.get(owner), agent, '重新取得不启动另一Agent或Node进程');
    programSession = programSessions.get(session.id)!;
    assert.notEqual(programSession.source, retainedSource);
    assert.throws(() => capability.forSession(retainedSource), error => error === reason);
    observedSession = capability.forSession(renewed);
    assert.equal(observedSession.job(second).snapshot().status, 'running');
    stateSubscription.unsubscribe(); outputSubscription.unsubscribe();
    const registrations = agent.session.snapshotEvents().filter(event => event.type === 'job/registered');
    assert.deepEqual(registrations.map(event => event.data.owner), [owner, owner, owner]);
    assert.deepEqual(registrations.map(event => event.data.jobId), [first, ...consumedOwnedJobs, second]);
    foreign.release(); session.release(); renewed.release();
    assert.throws(() => capability.forSession(session), error => error === reason);
    assert.equal(programSession.jobs.job(second).snapshot().status, 'running', '外部SDK退出不释放Program来源');
    retired.next(session.id);
    assert.throws(() => capability.forSession(programSession.source), error => error === retiredFailure);
    assert.throws(() => programSession.jobs.list(), error => error === retiredFailure);
    assert.equal(jobs.get(second, owner).status, 'running', 'Program失败只撤观察，不能停止已受理的原生进程');
    assert.doesNotThrow(() => process.kill(secondProducer.child.pid!, 0));
    await runtime.dispose();
    assert.equal((await secondClosed).status, 'killed', 'Runtime释放等待真实owner的生产进程退出');
    assert.throws(() => process.kill(secondProducer.child.pid!, 0), { code: 'ESRCH' });
    assert.equal(modelCalls, 0, 'quiet Job观察与控制不制造业务输入或模型轮次');
    assert.deepEqual(errors, []);
  } finally {
    observations.forEach(subscription => subscription.unsubscribe());
    releaseNativeEvents?.();
    try { await runtime.dispose(); }
    finally {
      // 失败试验也释放自己创建的真实进程；这不把Runtime未释放进程伪装成成功。
      forceStop();
      await Promise.all(children.map(item => item.closed));
      t.signal.removeEventListener('abort', forceStop);
    }
  }
});
