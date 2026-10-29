import { test } from 'node:test';
import assert from 'node:assert/strict';
import { setImmediate } from 'node:timers/promises';
import { BehaviorSubject, filter, firstValueFrom, from, timeout, type Subscription } from 'rxjs';
import { SessionId, SessionLogOffset, appendPluginRecord, pluginRecordOf } from '@deepseek-ai/dsh-session';
import type { GenerateOptions } from '@deepseek-ai/dsh-llm';
import { abortable, ActionBindingUnavailableError, defineProgram, programIdentity, ProgramContractMismatchError, z } from '@hanger-source/dsh-reactive';
import type { ApplicationService, ConversationHandle } from '@hanger-source/dsh-reactive-dsh';
import { createNativeSubagents, SubagentError } from '@hanger-source/dsh-reactive-dsh/subagents';
import { mountProgramGateway } from '@hanger-source/dsh-reactive-remote';
import { delegationRecordSchema } from '../packages/dsh/src/delegations.ts';
import { bootDelegationFixture, delegationDate, delegationScope, inspectDelegationSession } from './delegation-support.ts';
import { text, tool } from './native-support.ts';

declare module '@deepseek-ai/dsh-session/types' {
  interface PluginRecordMap { 'plugin:journal-fixture/attachment': { readonly phase: 'created' | 'tail' }; }
}

const content = (messages: GenerateOptions['messages']) => messages.flatMap(message => message.content)
  .flatMap(block => block.type === 'text' ? [block.text] : []).join('\n');
// 原生 RuntimeContextProjection 创建的来源；当前 package exports 未公开该类型增强文件。
const runtimeContextSource = z.object({ kind: z.literal('runtime-context'), form: z.literal('snapshot'),
  sections: z.object({ name: z.string(), text: z.string() }).array() });

for (const mode of ['spawn', 'fork'] as const) test(`${mode} 后代资源沿真实委托更新，exact祖先只中断选定执行并保FIFO和兄弟分支`, { timeout: 20_000 }, async () => {
  const capability = createNativeSubagents({ service: 'provided' });
  const gates = new Map(['branch-a', 'branch-b'].map(job => [job, {
    entered: Promise.withResolvers<AbortSignal>(), release: Promise.withResolvers<void>(),
  }]));
  const calls = new Map<string, number>();
  const fixture = await bootDelegationFixture(mode, {
    installNative: (native, home) => capability.install(native, home),
    async *respond(request) {
      assert.ok(request.tools?.some(tool => tool.name === 'child_complete'), '本试验只执行正式子Program');
      const job = [...content(request.messages).matchAll(/完成独立任务 (branch-[ab])/g)].at(-1)?.[1];
      assert.ok(job === 'branch-a' || job === 'branch-b', '沿正式委托objective识别本次子工作');
      const count = (calls.get(job) ?? 0) + 1; calls.set(job, count);
      assert.ok(count <= 3, '被中断分支不能自动重发原委托');
      if (count === 1) {
        assert.ok(request.signal);
        const gate = gates.get(job)!;
        gate.entered.resolve(request.signal);
        await firstValueFrom(abortable(() => gate.release.promise, request.signal));
        yield* tool('child_complete', mode + '-' + job + '-complete', {});
      } else yield* text('所选子工作完成');
    },
  });
  const { parent, runtime } = fixture;
  const children = capability.forSession(parent);
  const observed = children.descendants$.subscribe();
  let first: ConversationHandle<typeof fixture.childProgram> | undefined;
  let second: typeof first;
  try {
    assert.deepEqual(await children.descendants.read(), []);
    const accepted = async (job: string) => parent.resolveAction('delegate', delegationScope)(
      { at: delegationDate, job }, { actorId: 'sdk-human' }).result;
    const a = await accepted('branch-a'), b = await accepted('branch-b');
    const signalA = await gates.get('branch-a')!.entered.promise, signalB = await gates.get('branch-b')!.entered.promise;
    const descendants = await firstValueFrom(children.descendants$.pipe(filter(rows => rows.some(row => row.id === a.childSessionId)
      && rows.some(row => row.id === b.childSessionId)), timeout(3_000)));
    assert.deepEqual(descendants, await children.readDescendants());
    first = await runtime.application.open(a.childSessionId, fixture.childProgram);
    second = await runtime.application.open(b.childSessionId, fixture.childProgram);
    const agentA = runtime.native.agents.get(SessionId(first.id))!, agentB = runtime.native.agents.get(SessionId(second.id))!;
    const parentAgent = runtime.native.agents.get(SessionId(parent.id))!;
    await first.submit({ requestId: 'preserved-child-facts', intent: 'question', delivery: 'inject', content: '中断后仍待消费的资料',
      scope: { taskId: 'branch-a', issuedAt: delegationDate } });
    const queued = agentA.inbox.nextStep.find(message => message.source.kind === 'user'
      && 'rpcId' in message.source && message.source.rpcId === 'preserved-child-facts');
    assert.ok(queued);

    const cancellation = new AbortController(), reason = new Error('SUBAGENT_INTERRUPT_NOT_ENTERED');
    cancellation.abort(reason);
    assert.throws(() => children.interrupt(agentB.id, { signal: cancellation.signal }), error => error === reason);
    assert.equal(signalB.aborted, false);
    assert.throws(() => capability.forSession(first!).interrupt(agentB.id), error => error instanceof SubagentError && error.code === 'UNAUTHORIZED');
    assert.equal(signalA.aborted, false); assert.equal(signalB.aborted, false);

    assert.equal(children.interrupt(agentA.id), undefined, '正式同步控制不伪称静止回执');
    assert.equal(signalA.aborted, true);
    assert.equal(signalB.aborted, false, '选定子分支中断不连带兄弟执行');
    await firstValueFrom(from(agentA.whenIdle()).pipe(timeout(3_000)));
    assert.ok(agentA.inbox.nextStep.some(message => message.id === queued.id), '保留真实已受理但尚未claim的FIFO输入');
    assert.equal(runtime.native.agents.get(agentA.id), agentA, '中断不撤销原生子Agent与Activation');
    assert.equal(runtime.native.agents.get(parentAgent.id), parentAgent);
    assert.equal(calls.get('branch-a'), 1, '中断不自动重传或重启委托');

    children.dispose(); await capability.dispose();
    assert.equal(signalB.aborted, false, 'SDK读侧退出不冒充原生执行取消');
    assert.equal(runtime.native.agents.get(agentB.id), agentB);
    gates.get('branch-b')!.release.resolve();
    await second.whenIdle();
    assert.equal(second.snapshot().goal?.phase, 'complete');
    assert.deepEqual(fixture.errors, []);
  } finally {
    for (const gate of gates.values()) gate.release.resolve();
    observed.unsubscribe(); first?.release(); second?.release(); parent.release();
    await capability.dispose(); await fixture.dispose();
  }
});

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
    await runtime.application.close(parent.id);
    const callsBeforeRead = totalCalls;
    const rootSelection = { contract: programIdentity(fixture.parentProgram) };
    const childSelection = { contract: programIdentity(fixture.childProgram) };
    const childReference = { sessionId: sdkReceipt.childSessionId, workspaceId: fixture.workspace.workspaceId,
      environmentId: parent.identity.environmentId };
    const sameChildIdentity = defineProgram({ ...programIdentity(fixture.childProgram),
      input: fixture.childProgram.input, surface: fixture.childProgram.surface });
    const ports: readonly Pick<ApplicationService, 'open' | 'resume' | 'completeCreation'>[] =
      [runtime.application, runtime.application.service()];
    for (const port of ports) {
      await assert.rejects(port.open(sdkReceipt.childSessionId, rootSelection), ProgramContractMismatchError);
      await assert.rejects(port.resume(sdkReceipt.childSessionId, rootSelection), ProgramContractMismatchError);
      await assert.rejects(port.completeCreation(childReference, rootSelection), ProgramContractMismatchError);
      await assert.rejects(port.open(sdkReceipt.childSessionId, sameChildIdentity), /REACTIVE_PROGRAM_CONTRACT_MISMATCH/);
      const child = await port.open(sdkReceipt.childSessionId, childSelection);
      try { assert.equal(child.definition, fixture.childProgram); }
      finally { child.release(); }
      assert.equal(runtime.native.agents.get(SessionId(parent.id)), undefined, '拒绝root合同与读取child均不能恢复父Agent');
      assert.equal(runtime.native.agents.get(SessionId(sdkReceipt.childSessionId)), undefined, '正确child合同只打开冷程序图');
    }
    assert.equal(totalCalls, callsBeforeRead, '冷合同核验和只读打开不启动父或子模型');
    assert.deepEqual(fixture.errors, []);
  } finally { releaseChild.resolve(); if (fixture) await fixture.dispose(); }
});

test('冷Action读回执与委托授权不合作时，取消或撤声明及时结束准入，迟到返回不激活或启动child', { timeout: 20_000 }, async () => {
  const declaration$ = new BehaviorSubject(true);
  const readEntered = Promise.withResolvers<void>(), readRelease = Promise.withResolvers<void>();
  const gates = new Map(['caller', 'declaration'].map(job => [job, {
    entered: Promise.withResolvers<AbortSignal>(), release: Promise.withResolvers<void>(),
  }]));
  let models = 0;
  const fixture = await bootDelegationFixture('spawn', {
    declaration$,
    async *respond() { models++; throw new Error('REJECTED_ADMISSION_MUST_NOT_START_MODEL'); },
    authorize(admission) {
      if (admission.phase !== 'create') return;
      const gate = gates.get(admission.input.job);
      assert.ok(gate);
      gate.entered.resolve(admission.signal);
      return gate.release.promise;
    },
  });
  const { parent, runtime } = fixture;
  const open = runtime.native.sessionPersistence.open;
  const resume = runtime.native.agents.resume;
  let holdRead = false, resumed = 0;
  const wait = <T>(work: Promise<T>) => firstValueFrom(from(work).pipe(timeout(3_000)));
  runtime.native.sessionPersistence.open = new Proxy(open, { async apply(target, receiver, args) {
    const handle: Awaited<ReturnType<typeof open>> = await Reflect.apply(target, receiver, args);
    if (holdRead && args[0] === parent.id && args[1] === 'read') {
      holdRead = false;
      const read = handle.read;
      handle.read = new Proxy(read, { async apply(target, receiver, args) {
        const snapshot = await Reflect.apply(target, receiver, args);
        readEntered.resolve(); await readRelease.promise;
        return snapshot;
      } });
    }
    return handle;
  } });
  runtime.native.agents.resume = new Proxy(resume, { async apply(target, receiver, args) {
    const handle: Awaited<ReturnType<typeof resume>> = await Reflect.apply(target, receiver, args);
    if (handle.agent.id === parent.id) resumed++;
    return handle;
  } });
  try {
    await runtime.application.close(parent.id);
    await parent.actionManifest.read();
    const coldAction = parent.resolveAction('delegate', delegationScope);
    const controller = new AbortController(), reason = new Error('COLD_ACTION_CALLER_LEFT');
    holdRead = true;
    const reading = coldAction({ at: delegationDate, job: 'cold' }, { signal: controller.signal });
    await wait(readEntered.promise);
    controller.abort(reason);
    await assert.rejects(wait(reading.result), error => error === reason);
    assert.equal(runtime.native.agents.get(SessionId(parent.id)), undefined);
    assert.equal(resumed, 0, '真实冷日志回执挂起时尚未激活Agent');
    readRelease.resolve(); await setImmediate();
    assert.equal(runtime.native.agents.get(SessionId(parent.id)), undefined, '迟到读回执不再继续activate');
    assert.equal(resumed, 0);
    assert.equal(fixture.admissions.length, 0);

    for (const exit of ['caller', 'declaration'] as const) {
      const controller = new AbortController(), reason = new Error('DELEGATION_CALLER_LEFT');
      const invocation = parent.resolveAction('delegate', delegationScope)({ at: delegationDate, job: exit }, { signal: controller.signal });
      const gate = gates.get(exit)!;
      const admissionSignal = await wait(gate.entered.promise);
      assert.equal(admissionSignal.aborted, false);
      if (exit === 'caller') {
        controller.abort(reason);
        await assert.rejects(wait(invocation.result), error => error === reason);
      } else {
        const withdrawn = firstValueFrom(parent.actionManifest$.pipe(
          filter(manifest => manifest.some(action => action.toolName === 'delegate_work' && !action.enabled)), timeout(3_000)));
        declaration$.next(false); await withdrawn;
        await assert.rejects(wait(invocation.result), error => error instanceof ActionBindingUnavailableError
          && error.reason === 'not-exposed');
        assert.equal(controller.signal.aborted, false, '声明退出没有取消原调用者或已受理业务的执行signal');
      }
      assert.equal(admissionSignal.aborted, true, '不合作authorize在门闩释放前收到本次准入退出');
      assert.equal((await runtime.native.subagents.listDescendants(SessionId(parent.id))).length, 0);
      if (exit === 'caller') gate.release.reject(new Error('RETIRED_DELEGATION_POLICY_FAILED_LATE'));
      else gate.release.resolve();
      await setImmediate();
      assert.equal((await runtime.native.subagents.listDescendants(SessionId(parent.id))).length, 0, '迟到authorize不能启动child');
      assert.equal(models, 0);
    }
    const agent = runtime.native.agents.get(SessionId(parent.id));
    assert.ok(agent);
    assert.equal(await runtime.native.sessions.flush(agent.session), true);
    const persisted = await inspectDelegationSession(runtime.native, parent.id);
    assert.equal(persisted.events.filter(event => pluginRecordOf(event)?.type === 'plugin:reactive/delegation').length, 0,
      '本次准入失败没有持久受理委托或自动重发');
    assert.deepEqual(fixture.errors, []);
  } finally {
    readRelease.resolve(); for (const gate of gates.values()) gate.release.resolve();
    runtime.native.sessionPersistence.open = open;
    runtime.native.agents.resume = resume;
    declaration$.complete(); parent.release(); await fixture.dispose();
  }
});

test('委托观察最后退订撤销本次授权与读取资格，迟到回执不续读且原子工作仍可完成', { timeout: 20_000 }, async () => {
  const childEntered = Promise.withResolvers<AbortSignal>(), childRelease = Promise.withResolvers<void>();
  const authorizationEntered = Promise.withResolvers<AbortSignal>(), authorizationRelease = Promise.withResolvers<void>();
  const readEntered = Promise.withResolvers<AbortSignal>(), readRelease = Promise.withResolvers<void>();
  let holdAuthorization = false, holdRead = false, models = 0;
  let journalAttachment: string | undefined;
  const fixture = await bootDelegationFixture('spawn', {
    installNative(native) {
      native.on('session/created', session => {
        if (session.id === journalAttachment) appendPluginRecord(session, 'plugin:journal-fixture/attachment', { phase: 'created' });
      }, { global: true });
    },
    async *respond(request) {
      assert.ok(request.tools?.some(tool => tool.name === 'child_complete'));
      assert.ok(++models <= 2, '退出观察不能重发委托或重复启动子工作');
      if (models === 1) {
        assert.ok(request.signal);
        childEntered.resolve(request.signal);
        await firstValueFrom(abortable(() => childRelease.promise, request.signal));
        yield* tool('child_complete', 'observation-child-complete', {});
      } else yield* text('原子工作完成');
    },
    authorize(admission) {
      if (admission.phase !== 'read' || !holdAuthorization) return;
      holdAuthorization = false;
      authorizationEntered.resolve(admission.signal);
      return authorizationRelease.promise;
    },
  });
  const { parent, runtime } = fixture;
  const open = runtime.native.sessionPersistence.open;
  const reads: string[] = [], observationErrors: unknown[] = [], subscriptions: Subscription[] = [];
  let childSessionId: string | undefined;
  let gateway: Awaited<ReturnType<typeof mountProgramGateway>> | undefined;
  const wait = <T>(work: Promise<T>) => firstValueFrom(from(work).pipe(timeout(3_000)));
  runtime.native.sessionPersistence.open = new Proxy(open, { async apply(target, receiver, args) {
    if (args[1] === 'read' && (args[0] === parent.id || args[0] === childSessionId)) reads.push(args[0]);
    const handle: Awaited<ReturnType<typeof open>> = await Reflect.apply(target, receiver, args);
    if (holdRead && args[0] === childSessionId && args[1] === 'read') {
      holdRead = false;
      const signal: AbortSignal | undefined = args[2]?.signal;
      assert.ok(signal);
      const read = handle.read;
      handle.read = new Proxy(read, { async apply(target, receiver, args) {
        const snapshot = await Reflect.apply(target, receiver, args);
        readEntered.resolve(signal); await readRelease.promise;
        return snapshot;
      } });
    }
    return handle;
  } });
  try {
    gateway = await mountProgramGateway(runtime.native, { application: runtime.application,
      model: { provider: 'delegation-spawn', model: 'deterministic' }, access() {},
      participant() { throw new Error('LOCAL_CHILD_JOURNAL_HAS_NO_REMOTE_PRINCIPAL'); },
      carrier: { async authenticate() { throw new Error('LOCAL_CHILD_JOURNAL_HAS_NO_REMOTE_PRINCIPAL'); } } });
    const invocation = parent.resolveAction('delegate', delegationScope)(
      { at: delegationDate, job: 'observation-owner' }, { actorId: 'sdk-human' });
    const receipt = await wait(invocation.result);
    childSessionId = receipt.childSessionId;
    const childSignal = await wait(childEntered.promise);
    const child = runtime.native.agents.get(SessionId(childSessionId));
    assert.ok(child);
    const operation = await invocation.operation;
    const observe = () => {
      const subscription = operation.observations$.subscribe({ error: error => observationErrors.push(error) });
      subscriptions.push(subscription); return subscription;
    };

    holdAuthorization = true;
    const first = observe(), second = observe();
    const authorizationSignal = await wait(authorizationEntered.promise);
    first.unsubscribe();
    assert.equal(authorizationSignal.aborted, false, '同源资源仍有观察者时保留本次读取资格');
    second.unsubscribe();
    assert.equal(authorizationSignal.aborted, true, '不合作策略尚未返回时，最后退订已撤销本次准入');
    assert.equal(parent.signal.aborted, false);
    assert.equal(childSignal.aborted, false, '观察退出不取消子工作原生执行');
    const beforeLateAuthorization = [...reads];
    authorizationRelease.resolve(); await setImmediate();
    assert.deepEqual(reads, beforeLateAuthorization, '迟到allow不能再进入state或双边回执核验');
    assert.equal(await invocation.result, receipt);
    assert.equal(runtime.native.agents.get(child.id), child);

    holdRead = true;
    const reading = observe();
    const readSignal = await wait(readEntered.promise);
    assert.notEqual(readSignal, authorizationSignal, '重新显式观察签发本连接自己的读取资格');
    assert.equal(readSignal.aborted, false);
    reading.unsubscribe();
    assert.equal(readSignal.aborted, true);
    assert.equal(childSignal.aborted, false);
    const beforeLateRead = [...reads];
    readRelease.resolve(); await setImmediate();
    assert.deepEqual(reads, beforeLateRead, '已开始的真实read迟到返回后不再起后续parent或child读取');
    assert.equal((await wait(operation.state.read())).phase, 'active');
    assert.equal(runtime.native.agents.get(child.id), child);

    childRelease.resolve();
    await wait(child.whenIdle());
    assert.equal((await wait(operation.settled())).phase, 'complete', '原业务终态不受观察退出影响');
    assert.equal(await invocation.result, receipt);

    const parentAgent = runtime.native.agents.get(SessionId(parent.id));
    assert.ok(parentAgent);
    await runtime.native.subagents.drainChildren(parentAgent, [child.id]);
    assert.equal(runtime.native.agents.get(child.id), undefined);
    assert.equal(runtime.native.sessions.get(child.id), undefined, 'Activation已释放原执行句柄和live Session');
    assert.equal(runtime.native.agents.get(parentAgent.id), parentAgent, '只退出选定child，原parent仍持有其真实owner');
    const beforeJournal = models, stored = await inspectDelegationSession(runtime.native, child.id);
    const coldChild = await runtime.application.open(child.id, fixture.childProgram);
    const serviceChild = await runtime.application.service().open(child.id, fixture.childProgram);
    const following = coldChild.journal.follow({ maxMessages: 1_000 })[Symbol.asyncIterator]();
    let journalFailure: { readonly error: unknown } | undefined;
    try {
      const opening = await wait(following.next());
      assert.ok(!opening.done && opening.value.type === 'opened');
      const { cursor, page } = opening.value;
      assert.deepEqual(page.records.map(record => record.event), stored.events, '子会话开场保留实际持久日志');
      assert.deepEqual(await serviceChild.journal.page({ throughSeq: cursor, maxMessages: 1_000 }), page,
        '本地人工与服务句柄沿同一已核验child地址读页');
      assert.equal(runtime.native.agents.get(child.id), undefined, 'cold child journal不启动Agent');

      const restored = runtime.native.sessions.prepare(child.id, { seed: stored.events, meta: stored.header,
        inheritedEventCount: stored.inheritedEventCount });
      journalAttachment = child.id;
      const detach = runtime.native.sessions.enter(restored);
      try {
        runtime.native.sessions.announce(restored);
        const suffix = restored.snapshotEvents(SessionLogOffset(cursor + 1));
        assert.ok(suffix.length > 1, '实际seed marker先于created listener追加的事件');
        assert.equal(pluginRecordOf(suffix.at(-1)!)?.type, 'plugin:journal-fixture/attachment');
        for (const expected of suffix) {
          const frame = await wait(following.next());
          assert.ok(!frame.done && frame.value.type === 'entry');
          assert.deepEqual(frame.value.entry.event, expected, '首次event先补齐原生seed，created不能将旧suffix放到队尾');
        }
        const nextSequence = appendPluginRecord(restored, 'plugin:journal-fixture/attachment', { phase: 'tail' });
        const next = await wait(following.next());
        assert.ok(!next.done && next.value.type === 'entry');
        assert.equal(next.value.entry.event.seq, nextSequence, 'created与event的重叠不能重复交付');
        assert.equal(models, beforeJournal, 'cold到显式Session attach只观察原日志，不启动模型');
        assert.ok(following.return);
        const pending = following.next(), closing = following.return();
        const [last, closed] = await wait(Promise.all([pending, closing]));
        assert.equal(last.done, true, '正常return立即结束本连接尚未取得下一帧的等待');
        assert.equal(closed.done, true, '已读entry后的正常return不拒绝自身取消');
      } finally { journalAttachment = undefined; detach(); }
    } catch (error) { journalFailure = { error }; throw error; }
    finally {
      try { await following.return?.(); }
      catch (cleanup) {
        if (journalFailure && cleanup !== journalFailure.error)
          throw new AggregateError([journalFailure.error, cleanup], 'CHILD_JOURNAL_TEST_AND_CLEANUP_FAILED');
        throw cleanup;
      }
      finally { coldChild.release(); serviceChild.release(); }
    }
    assert.deepEqual(observationErrors, []);
    assert.deepEqual(fixture.errors, []);
  } finally {
    authorizationRelease.resolve(); readRelease.resolve(); childRelease.resolve();
    for (const subscription of subscriptions) subscription.unsubscribe();
    runtime.native.sessionPersistence.open = open;
    parent.release();
    try { await gateway?.close(); } finally { await fixture.dispose(); }
  }
});
