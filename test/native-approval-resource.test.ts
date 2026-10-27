import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { useLayoutEffect } from 'react';
import { filter, firstValueFrom, timeout, type Subscription } from 'rxjs';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { SessionId } from '@deepseek-ai/dsh-session';
import { combineResources, defineApplication, defineEnvironment, defineProgram, defineSurface, z, type Observation } from '@hanger-source/dsh-reactive';
import { implementProgram, useResource } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeCapabilities, nativeModel, type ConversationHandle } from '@hanger-source/dsh-reactive-dsh';
import { createNativeApproval, type ApprovalPolicy, type NativeSessionApproval } from '@hanger-source/dsh-reactive-dsh/approval';
import { createNativeTools } from '@hanger-source/dsh-reactive-dsh/tools';

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
