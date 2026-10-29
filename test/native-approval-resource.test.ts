import assert from 'node:assert/strict';
import { test } from 'node:test';
import { access, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { useLayoutEffect } from 'react';
import { BehaviorSubject, filter, firstValueFrom, from, timeout, type Subscription } from 'rxjs';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { SessionId } from '@deepseek-ai/dsh-session';
import { combineResources, defineApplication, defineEnvironment, defineProgram, defineSurface, ScopeDisposedError, z, type Eligibility, type NativeToolCall, type Observation } from '@hanger-source/dsh-reactive';
import { implementProgram, NativeTools, useResource } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeCapabilities, nativeModel, type ConversationHandle } from '@hanger-source/dsh-reactive-dsh';
import { createNativeApproval, type ApprovalOutcome, type ApprovalPolicy, type ApprovalRequestEvent, type NativeApprovalEvent, type NativeSessionApproval } from '@hanger-source/dsh-reactive-dsh/approval';
import { createNativeTools } from '@hanger-source/dsh-reactive-dsh/tools';
import { text, tool } from './native-support.ts';

test('Approval政策资源与原生日志、hooks和Tool组合同源，冷读恢复及释放不产生批准或模型调用', { timeout: 20_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-approval-resource-'));
  let models = 0, answers = 0;
  const approval = createNativeApproval({ policy: 'ask', require: () => undefined,
    async answer(_request, next) { answers++; return next(); } });
  const tools = createNativeTools({ service: 'provided' });
  const captured = new Map<string, NativeSessionApproval>();
  const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
  const definition = defineApplication({ name: 'approval-resource', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '读取政策不请求批准', program: implementProgram(program, { Component({ conversation }) {
      const current = approval.forSession(conversation);
      useResource(current.policy);
      useLayoutEffect(() => { captured.set(conversation.identity.sessionId, current); }, [conversation, current]);
      return null;
    } }),
  })] });
  const errors: unknown[] = [];
  const runtime = await createRuntime(() => definition, { home: root, actorId: 'approval-owner',
    installNative: nativeCapabilities(approval, tools), onError: event => errors.push(event.error),
    model: nativeModel({ provider: 'approval-resource', model: 'unused', create: () => new class extends LlmAdapter {
      async *stream(): AsyncGenerator<never> { models++; throw new Error('POLICY_READ_MUST_NOT_START_MODEL'); }
    }() }),
  });
  let one: ConversationHandle<typeof program> | undefined, two: ConversationHandle<typeof program> | undefined;
  let resumed: ConversationHandle<typeof program> | undefined, subscription: Subscription | undefined;
  const states: Observation<ApprovalPolicy>[] = [];
  try {
    const workspace = await runtime.application.createWorkspace({ directory: root });
    one = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    two = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    const first = approval.forSession(one), second = approval.forSession(two);
    assert.equal(first, approval.forSession(one));
    assert.equal(first.policy$, first.policy.values$);
    subscription = first.policy.observations$.subscribe(state => states.push(state));
    assert.equal(await first.policy.read(), 'ask');
    assert.equal(await captured.get(one.id)!.policy.read(), 'ask');
    assert.equal(await second.policy.read(), 'ask');
    const before = states.length;
    first.setPolicy('never');
    assert.ok(states.slice(before).some(state => state.status === 'pending'));
    assert.equal(await first.policy.read(), 'never');
    assert.equal(await captured.get(one.id)!.policy.read(), 'never');
    assert.equal(await second.policy.read(), 'ask');
    const combined = await firstValueFrom(combineResources({ policy: first.policy.observations$,
      tools: tools.forSession(one).catalog.observations$ }).pipe(filter(state => state.status === 'ready'), timeout(5_000)));
    if (combined.status !== 'ready') assert.fail('policy与工具目录应从同一正式资源组合取得');
    assert.equal(combined.value.policy, 'never');
    const agent = runtime.native.get('agents')!.get(SessionId(one.id));
    await runtime.application.close(one.id);
    assert.equal(await first.policy.read(), 'never', '冷会话从持久切面读取原政策');
    assert.equal(runtime.native.get('agents')!.get(SessionId(one.id)), undefined);
    assert.throws(() => first.setPolicy('ask'), /REACTIVE_CONVERSATION_NOT_RUNNING/);
    resumed = await runtime.application.resume(one.id, program);
    assert.notEqual(runtime.native.get('agents')!.get(SessionId(one.id)), agent);
    assert.equal(await first.policy.read(), 'never');
    first.setPolicy('ask'); assert.equal(await first.policy.read(), 'ask');
    const audit = await first.readAudit();
    assert.deepEqual(audit.filter(event => event.type === 'approval/policy').map(event => event.data.policy), ['never', 'ask']);
    assert.equal(audit.some(event => event.type === 'approval/asked' || event.type === 'approval/decided'), false);

    const store = runtime.native.get('sessions')!, persistence = runtime.native.get('sessionPersistence')!;
    const flush = store.flush, open = persistence.open;
    type AuditOutcome = { readonly ok: true; readonly value: readonly NativeApprovalEvent[] }
      | { readonly ok: false; readonly error: unknown };
    let gate: { entered: ReturnType<typeof Promise.withResolvers<void>>; release: ReturnType<typeof Promise.withResolvers<void>>;
      returned: ReturnType<typeof Promise.withResolvers<void>> } | undefined;
    let reads = 0;
    const wait = <T>(work: Promise<T>) => firstValueFrom(from(work).pipe(timeout(3_000)));
    store.flush = new Proxy(flush, { async apply(target, receiver, args) {
      const flushed: Awaited<ReturnType<typeof flush>> = await Reflect.apply(target, receiver, args);
      if (args[0]?.id === one!.id && gate) {
        const current = gate; gate = undefined;
        current.entered.resolve(); await current.release.promise; current.returned.resolve();
      }
      return flushed;
    } });
    persistence.open = new Proxy(open, { apply(target, receiver, args) {
      if (args[0] === one!.id && args[1] === 'read') reads++;
      return Reflect.apply(target, receiver, args);
    } });
    try {
      for (const exit of ['caller', 'source', 'sdk'] as const) {
        const sdk: ConversationHandle<typeof program> | undefined = exit === 'sdk' ? await runtime.application.open(one.id, program) : undefined;
        const source: NativeSessionApproval = sdk ? approval.forSession(sdk) : exit === 'caller' ? captured.get(one.id)! : first;
        const current = { entered: Promise.withResolvers<void>(), release: Promise.withResolvers<void>(), returned: Promise.withResolvers<void>() };
        const controller = new AbortController(), reason = new Error('APPROVAL_AUDIT_CALLER_LEFT');
        gate = current;
        const outcome: Promise<AuditOutcome> = source.readAudit({ signal: controller.signal })
          .then<AuditOutcome, AuditOutcome>(value => ({ ok: true, value }), (error: unknown) => ({ ok: false, error }));
        try {
          await wait(Promise.race([current.entered.promise, outcome.then(value => {
            throw new Error('AUDIT_READ_DID_NOT_REACH_REAL_FLUSH', { cause: value });
          })]));
          const beforeReads = reads;
          if (exit === 'caller') controller.abort(reason);
          else if (exit === 'source') source.dispose();
          else sdk!.release();
          const completed: AuditOutcome = await wait(outcome);
          assert.equal(completed.ok, false, '真实flush回执未放行时，审计消费者已经退出');
          if (completed.ok) assert.fail('已退出审计读取不应交付结果');
          if (exit === 'caller') assert.equal(completed.error, reason);
          else if (exit === 'sdk') assert.equal(completed.error, sdk!.signal.reason);
          else {
            assert.ok(completed.error instanceof ScopeDisposedError);
            assert.throws(() => source.policy.peek(), error => error === completed.error, '保留同一读侧owner的原退出reason');
          }
          assert.deepEqual(await second.readAudit(), [], '一个来源退等不阻塞其他Session的正式审计读取');
          current.release.resolve(); await wait(current.returned.promise);
          await new Promise<void>(resolve => setImmediate(resolve));
          assert.equal(reads, beforeReads, '迟到flush回执复核原signal，不再继续打开持久读取');
          assert.equal(models, 0); assert.equal(answers, 0);
        } finally { current.release.resolve(); sdk?.release(); }
      }
    } finally { gate?.release.resolve(); store.flush = flush; persistence.open = open; }

    first.dispose();
    assert.throws(() => first.policy.peek(), /REACTIVE_SCOPE_DISPOSED/);
    const replacement = approval.forSession(one);
    assert.notEqual(replacement, first); assert.equal(await replacement.policy.read(), 'ask');
    assert.equal(await second.policy.read(), 'ask');
    assert.equal(models, 0); assert.equal(answers, 0); assert.deepEqual(errors, []);
  } finally { subscription?.unsubscribe(); resumed?.release(); one?.release(); two?.release(); await runtime.dispose(); }
});

test('原生工具资格瞬态撤销保留已受理Approval审计，晚批准不能复活旧调用，恢复后新调用可执行', { timeout: 20_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-approval-eligibility-'));
  const eligibility = new BehaviorSubject<Observation<Eligibility>>({ status: 'ready', generation: 1, value: { allowed: true }, observedAt: Date.now() });
  const entered = Promise.withResolvers<ApprovalRequestEvent>();
  const late = Promise.withResolvers<ApprovalOutcome>(), answered = Promise.withResolvers<void>();
  t.signal.addEventListener('abort', () => late.resolve('allowed-once'), { once: true });
  let answers = 0;
  const approval = createNativeApproval({ policy: 'ask',
    require: call => call.name === 'approval_eligibility' ? { reason: '写入文件前批准本次调用' } : undefined,
    async answer(request) {
      if (++answers !== 1) return 'allowed-once';
      entered.resolve(request);
      try { return await late.promise; }
      finally { answered.resolve(); }
    },
  });
  const admissions: NativeToolCall[] = [];
  const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
  const definition = defineApplication({ name: 'approval-eligibility', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '只执行资格仍有效且取得本次批准的工具',
    program: implementProgram(program, { Component: () => NativeTools({ names: ['approval_eligibility'], eligibility,
      admit: call => { admissions.push(call); } }) }),
  })] });
  const errors: unknown[] = [], audit: NativeApprovalEvent[] = [], results: string[] = [];
  let planned: 'withdrawn' | 'recovered' | undefined;
  const runtime = await createRuntime(() => definition, { home: root, actorId: 'approval-owner', tools: 'native',
    onError: event => errors.push(event.error), installNative: nativeCapabilities(approval),
    model: nativeModel({ provider: 'approval-eligibility', model: 'scripted', create: () => new class extends LlmAdapter {
      async *stream() {
        const current = planned; planned = undefined;
        if (current) yield* tool('approval_eligibility', 'eligibility-' + current, { marker: current });
        else yield* text('已取得本次工具结果');
      }
    }() }),
  });
  let session: ConversationHandle<typeof program> | undefined;
  const removeAudit = runtime.native.on('session/event', (current, event) => {
    if (current.id === session?.id && (event.type === 'approval/asked' || event.type === 'approval/decided')) audit.push(event);
  });
  const removeResults = runtime.native.on('tools/result', execution => {
    if (execution.name === 'approval_eligibility') results.push(execution.callId);
  });
  const removeTool = runtime.native.get('tools')!.register({ name: 'approval_eligibility', description: '批准后写入实际调用身份',
    parameters: { type: 'object', properties: { marker: { type: 'string' } }, required: ['marker'], additionalProperties: false },
    output: { schema: { type: 'null' }, render: () => [{ type: 'text', text: 'null' }] },
    async execute(raw, execution) {
      const { marker } = z.object({ marker: z.enum(['withdrawn', 'recovered']) }).parse(raw);
      await writeFile(join(root, marker + '.txt'), execution.callId);
      return null;
    },
  });
  try {
    const workspace = await runtime.application.createWorkspace({ directory: root });
    session = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    planned = 'withdrawn';
    await session.submit({ requestId: 'withdrawn', content: '等待本次文件写入批准', intent: 'question', delivery: 'followup', scope: null });
    const request = await firstValueFrom(from(entered.promise).pipe(timeout(5_000)));
    assert.equal(request.callId, 'eligibility-withdrawn');
    assert.equal(admissions[0]?.signal.aborted, false);
    eligibility.next({ status: 'pending', generation: 2 });
    eligibility.next({ status: 'ready', generation: 3, value: { allowed: true }, observedAt: Date.now() });
    assert.equal(admissions[0]?.signal.aborted, true, '同tick恢复也永久撤销旧准入');
    await new Promise<void>(resolve => setImmediate(resolve));
    assert.equal(request.signal?.aborted, false, '资格退出不取消由正式Approval持有的已受理请求');
    assert.equal(audit.filter(event => event.type === 'approval/asked').length, 1);
    assert.equal(audit.filter(event => event.type === 'approval/decided').length, 0, '回答尚未交付，不伪造cancelled审计');
    assert.deepEqual(results, [], '原生管线仍等待原审批结算');
    await assert.rejects(access(join(root, 'withdrawn.txt')), { code: 'ENOENT' });

    late.resolve('allowed-once'); await answered.promise;
    await session.whenIdle();
    assert.deepEqual(results, ['eligibility-withdrawn']);
    await assert.rejects(access(join(root, 'withdrawn.txt')), { code: 'ENOENT' });
    assert.equal(admissions[0]?.signal.aborted, true, '晚批准不恢复旧准入');
    planned = 'recovered';
    await session.submit({ requestId: 'recovered', content: '恢复后重新请求写入', intent: 'question', delivery: 'followup', scope: null });
    await session.whenIdle();
    assert.equal(await readFile(join(root, 'recovered.txt'), 'utf8'), 'eligibility-recovered');
    const asked = audit.filter(event => event.type === 'approval/asked');
    const decided = audit.filter(event => event.type === 'approval/decided');
    assert.deepEqual(asked.map(event => event.data.callId), ['eligibility-withdrawn', 'eligibility-recovered']);
    assert.deepEqual(decided.map(event => event.data.id), asked.map(event => event.data.id));
    assert.deepEqual(decided.map(event => event.data.outcome), ['allowed-once', 'allowed-once'], '实际批准与工具资格分别结算，不篡改审批事实');
    assert.equal(answers, 2); assert.deepEqual(errors, []);
  } finally {
    late.resolve('allowed-once'); session?.release(); removeAudit(); removeResults(); removeTool(); eligibility.complete();
    await runtime.dispose();
  }
});

for (const answerer of ['owned', 'provided'] as const) {
  test(`Approval退出撤销尚未决策的${answerer}回答，晚批准不能执行原生工具`, { timeout: 20_000 }, async t => {
    const root = await mkdtemp(join(tmpdir(), `dsh-reactive-approval-${answerer}-exit-`));
    const entered = Promise.withResolvers<ApprovalRequestEvent>();
    const late = Promise.withResolvers<ApprovalOutcome>(), answered = Promise.withResolvers<void>();
    const cancelled = Promise.withResolvers<void>();
    t.signal.addEventListener('abort', () => late.resolve('allowed-once'), { once: true });
    let answers = 0;
    const answer = async (request: ApprovalRequestEvent): Promise<ApprovalOutcome> => {
      if (++answers === 1) return 'allowed-once';
      entered.resolve(request);
      try { return await late.promise; }
      finally { answered.resolve(); }
    };
    const approval = createNativeApproval({ policy: 'ask',
      require: call => call.name === 'approval_effect' ? { reason: '写入前需要本次批准' } : undefined,
      answer: answerer === 'owned' ? answer : 'provided',
    });
    const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
    const definition = defineApplication({ name: `approval-${answerer}-exit`, admit: async () => {}, environments: [defineEnvironment({
      id: 'work', instructions: '仅按本次批准执行工具',
      program: implementProgram(program, { Component: () => NativeTools({ names: ['approval_effect'] }) }),
    })] });
    const errors: unknown[] = [], audit: NativeApprovalEvent[] = [];
    let planned: 'accepted' | 'withdrawn' | undefined;
    let removeAnswer: (() => void) | undefined;
    const runtime = await createRuntime(() => definition, { home: root, actorId: 'approval-owner', tools: 'native',
      onError: event => errors.push(event.error),
      async installNative(ctx, home) {
        await approval.install(ctx, home);
        // 外部回答者后注册且抢先处理；请求寿命不能依赖本能力的listener顺序。
        if (answerer === 'provided') removeAnswer = ctx.on('approval/request', answer, { prepend: true });
      },
      // 脚本适配器只驱动可控工具请求；证据覆盖原生审批与执行边界，不代表外部provider。
      model: nativeModel({ provider: 'approval-script', model: 'scripted', create: () => new class extends LlmAdapter {
        async *stream() {
          const current = planned; planned = undefined;
          if (current) yield* tool('approval_effect', `approval-${answerer}-${current}`, { marker: current });
          else yield* text('已取得本次工具结果');
        }
      }() }),
    });
    let session: ConversationHandle<typeof program> | undefined;
    const removeAudit = runtime.native.on('session/event', (current, event) => {
      if (current.id !== session?.id || event.type !== 'approval/asked' && event.type !== 'approval/decided') return;
      audit.push(event);
      if (event.type === 'approval/decided' && event.data.outcome === 'cancelled') cancelled.resolve();
    });
    const removeTool = runtime.native.get('tools')!.register({ name: 'approval_effect', description: '批准后写入本次调用身份',
      parameters: { type: 'object', properties: { marker: { type: 'string' } }, required: ['marker'], additionalProperties: false },
      output: { schema: { type: 'null' }, render: () => [{ type: 'text', text: 'null' }] },
      async execute(raw, execution) {
        const { marker } = z.object({ marker: z.enum(['accepted', 'withdrawn']) }).parse(raw);
        await writeFile(join(root, marker + '.txt'), execution.callId);
        return null;
      },
    });
    try {
      const workspace = await runtime.application.createWorkspace({ directory: root });
      session = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
      planned = 'accepted';
      await session.submit({ requestId: 'accepted', content: '批准后执行', intent: 'question', delivery: 'followup', scope: null });
      await session.whenIdle();
      assert.equal(await readFile(join(root, 'accepted.txt'), 'utf8'), `approval-${answerer}-accepted`);

      planned = 'withdrawn';
      await session.submit({ requestId: 'withdrawn', content: '等待本次批准', intent: 'question', delivery: 'followup', scope: null });
      const request = await firstValueFrom(from(entered.promise).pipe(timeout(5_000)));
      assert.equal(request.agent, runtime.native.get('agents')!.get(SessionId(session.id)));
      assert.equal(request.callId, `approval-${answerer}-withdrawn`);
      assert.equal(request.signal?.aborted, false);
      await approval.dispose();
      assert.equal(request.signal?.aborted, true, '退出只撤销本安装尚未决策的请求');
      await firstValueFrom(from(cancelled.promise).pipe(timeout(5_000)));
      await session.whenIdle();
      await assert.rejects(access(join(root, 'withdrawn.txt')), { code: 'ENOENT' });

      late.resolve('allowed-once');
      await answered.promise;
      await new Promise<void>(resolve => setImmediate(resolve));
      await assert.rejects(access(join(root, 'withdrawn.txt')), { code: 'ENOENT' });
      const asked = audit.filter(event => event.type === 'approval/asked');
      const decided = audit.filter(event => event.type === 'approval/decided');
      assert.deepEqual(asked.map(event => event.data.callId), [`approval-${answerer}-accepted`, `approval-${answerer}-withdrawn`]);
      assert.deepEqual(decided.map(event => event.data.id), asked.map(event => event.data.id));
      assert.deepEqual(decided.map(event => event.data.outcome), ['allowed-once', 'cancelled'], '官方审计保留已决策grant，待决策请求只结算一次取消');
      assert.equal(answers, 2); assert.deepEqual(errors, []);
    } finally {
      late.resolve('allowed-once');
      session?.release(); removeAudit(); removeTool(); removeAnswer?.();
      await runtime.dispose();
    }
  });
}
