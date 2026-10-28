import assert from 'node:assert/strict';
import { test } from 'node:test';
import { access, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { useLayoutEffect } from 'react';
import { filter, firstValueFrom, from, timeout, type Subscription } from 'rxjs';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { SessionId } from '@deepseek-ai/dsh-session';
import { combineResources, defineApplication, defineEnvironment, defineProgram, defineSurface, z, type Observation } from '@hanger-source/dsh-reactive';
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
    first.dispose();
    assert.throws(() => first.policy.peek(), /REACTIVE_SCOPE_DISPOSED/);
    const replacement = approval.forSession(one);
    assert.notEqual(replacement, first); assert.equal(await replacement.policy.read(), 'ask');
    assert.equal(await second.policy.read(), 'ask');
    assert.equal(models, 0); assert.equal(answers, 0); assert.deepEqual(errors, []);
  } finally { subscription?.unsubscribe(); resumed?.release(); one?.release(); two?.release(); await runtime.dispose(); }
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
