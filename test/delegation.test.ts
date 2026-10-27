import { test } from 'node:test';
import assert from 'node:assert/strict';
import { setImmediate } from 'node:timers/promises';
import { filter, firstValueFrom } from 'rxjs';
import { pluginRecordOf } from '@deepseek-ai/dsh-session';
import type { GenerateOptions } from '@deepseek-ai/dsh-llm';
import { z } from '@hanger-source/dsh-reactive';
import { delegationRecordSchema } from '../packages/dsh/src/delegations.ts';
import { bootDelegationFixture, delegationDate, delegationScope, inspectDelegationSession } from './delegation-support.ts';
import { text, tool } from './native-support.ts';

const content = (messages: GenerateOptions['messages']) => messages.flatMap(message => message.content)
  .flatMap(block => block.type === 'text' ? [block.text] : []).join('\n');
// 原生 RuntimeContextProjection 创建的来源；当前 package exports 未公开该类型增强文件。
const runtimeContextSource = z.object({ kind: z.literal('runtime-context'), form: z.literal('snapshot'),
  sections: z.object({ name: z.string(), text: z.string() }).array() });

for (const mode of ['spawn', 'fork'] as const) test(`${mode} 子程序隔离当前声明，以独立Goal完成反馈续接原父工作，SDK调用保留独立身份`, async () => {
  const releaseChild = Promise.withResolvers<void>();
  const firstChildTurn = Promise.withResolvers<string>();
  const parentFeedback = Promise.withResolvers<void>();
  let parentCalls = 0, aiChildCalls = 0, sdkChildCalls = 0, totalCalls = 0;
  let fixture!: Awaited<ReturnType<typeof bootDelegationFixture>>;
  try {
    fixture = await bootDelegationFixture(mode, {
      installNative(native) {
        native.on('session/event', (session, event) => {
          if (event.type === 'turn/end' && event.data.reason.kind === 'completed' && session.header.parentSession) firstChildTurn.resolve(session.id);
        });
      },
      async *respond(request) {
        assert.ok(++totalCalls <= 16, '确定性模型不能无限调用工具');
        const names = request.tools?.map(value => value.name) ?? [];
        if (names.includes('child_complete')) {
          assert.equal(names.includes('parent_only_tool'), false);
          assert.equal(names.includes('delegate_work'), false);
          const leading = content(request.messages.filter(message => message.role === 'system'));
          assert.equal(leading.includes('PARENT_ONLY_SECRET'), false);
          // 原生语义是最后一份现场取代旧快照；fork 的历史快照仍是合法 seed 历史。
          const contexts = request.messages.flatMap(message => {
            const source = runtimeContextSource.safeParse(message.source);
            return source.success ? [{ message, source: source.data }] : [];
          });
          const current = contexts.at(-1);
          assert.ok(current, 'child模型请求必须包含原生当前现场快照');
          const currentText = content([current.message]);
          assert.equal(currentText.includes('PARENT_ONLY_SECRET'), false);
          assert.ok(currentText.includes('CHILD_ONLY_CONTEXT'));
          assert.ok(current.source.sections.some(section => section.name === 'child-only' && section.text.includes('CHILD_ONLY_CONTEXT')));
          if (content(request.messages).includes('sdk-job')) {
            assert.ok(++sdkChildCalls <= 8);
            if (sdkChildCalls === 1) yield* tool('child_complete', mode + '-sdk-complete', {});
            else yield* text('SDK 子工作完成');
          } else {
            assert.ok(++aiChildCalls <= 8);
            if (aiChildCalls === 1) yield* text('本轮已调查，子目标尚未完成');
            else if (aiChildCalls === 2) { await releaseChild.promise; yield* tool('child_complete', mode + '-child-complete', {}); }
            else yield* text('AI 子工作完成');
          }
          return;
        }
        parentCalls++;
        assert.ok(names.includes('parent_only_tool'));
        if (parentCalls === 1) yield* tool('delegate_work', mode + '-delegate-call', fixture.rawInput);
        else if (parentCalls === 2) {
          const receipt = await fixture.invocations[0]!.result;
          const child = await fixture.runtime.application.open(receipt.childSessionId, fixture.childProgram);
          try { assert.equal(child.snapshot().goal?.phase, 'complete'); }
          finally { child.release(); }
          parentFeedback.resolve(); yield* tool('parent_complete', mode + '-parent-complete', {});
        } else yield* text('原父目标完成');
      },
      authorize(admission) {
        assert.ok(admission.input.at instanceof Date);
        assert.equal(admission.input.at.toISOString(), delegationDate);
        assert.deepEqual(admission.target, { parentId: delegationScope });
        if (admission.executor.kind === 'tool') for (const cause of admission.causes) {
          assert.equal(cause.sourceSessionId, fixture.parent.id);
          assert.equal(cause.actorId, 'original-human');
          assert.deepEqual(cause.scope, { parentId: delegationScope });
        }
        if (admission.phase === 'create' && admission.executor.kind === 'participant') {
          assert.equal(admission.executor.actorId, 'sdk-human');
          assert.equal(admission.executor.participant, 'human');
          assert.deepEqual(admission.causes, []);
        }
      },
    });
    const { parent, runtime } = fixture;
    await parent.submit({ requestId: mode + '-original-goal', content: '委托调查后继续完成原父工作',
      intent: 'goal', delivery: 'followup' as const, scope: { parentId: delegationScope } });
    const firstChildId = await firstChildTurn.promise;
    const invocation = fixture.invocations[0]!;
    const receipt = await invocation.result;
    assert.equal(firstChildId, receipt.childSessionId);
    const operation = await invocation.operation;
    const active = await firstValueFrom(operation.observations$.pipe(filter(value => value.status === 'ready')));
    assert.equal(active.value.phase, 'active');
    let settled = false;
    const settlement = operation.settled().then(value => { settled = true; return value; });
    void settlement.catch(() => {});
    await setImmediate();
    assert.equal(settled, false, '普通child turn不代表正式子目标完成');
    assert.equal(parentCalls, 1, '普通child turn不自动产生父协作反馈');
    const originalGoal = parent.snapshot().goal;
    assert.equal(originalGoal?.phase, 'active');
    assert.ok(originalGoal);
    assert.ok(parent.snapshot().waits.some(wait => wait.continuation.goal?.id === originalGoal.id
      && wait.continuation.goal.revision === originalGoal.revision));
    releaseChild.resolve();
    await parentFeedback.promise;
    await parent.whenIdle();
    assert.equal((await settlement).phase, 'complete');
    assert.equal(parent.snapshot().goal?.phase, 'complete');
    assert.equal(parent.snapshot().goal?.id, originalGoal.id);
    const localInputs = fixture.childInputs.get(receipt.childSessionId);
    assert.ok(localInputs?.length);
    for (const input of localInputs) {
      assert.equal(input.sourceSessionId, receipt.childSessionId);
      assert.notEqual(input.sourceSessionId, parent.id);
      assert.equal(input.scope.taskId, 'ai-job');
      assert.ok(input.scope.issuedAt instanceof Date);
      assert.equal(input.scope.issuedAt.toISOString(), delegationDate);
    }
    const persisted = await inspectDelegationSession(runtime.native, parent.id);
    const records = persisted.events.flatMap(event => { const record = pluginRecordOf(event);
      return record?.type === 'plugin:reactive/delegation' ? [delegationRecordSchema.parse(record.data)] : []; });
    assert.deepEqual(records[0]!.input, fixture.rawInput);
    assert.equal(records[0]!.target, delegationScope);
    assert.equal(records[0]!.causes[0]!.sourceSessionId, parent.id);
    const sdk = parent.resolveAction('delegate', delegationScope)({ at: delegationDate, job: 'sdk-job' }, { actorId: 'sdk-human' });
    assert.ok(sdk.source.kind === 'participant');
    assert.equal(sdk.source.actorId, 'sdk-human');
    const sdkReceipt = await sdk.result;
    const sdkChild = await runtime.application.open(sdkReceipt.childSessionId, fixture.childProgram);
    try { await firstValueFrom(sdkChild.activity$.pipe(filter(activity => activity.goal?.phase === 'complete'))); }
    finally { sdkChild.release(); }
    assert.equal(parent.snapshot().goal?.id, originalGoal.id);
    assert.equal(parent.snapshot().goal?.phase, 'complete');
    assert.deepEqual(fixture.errors, []);
    parent.release();
  } finally { releaseChild.resolve(); if (fixture) await fixture.dispose(); }
});
