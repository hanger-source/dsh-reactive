import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate } from 'node:timers/promises';
import { BehaviorSubject, Observable, UnsubscriptionError } from 'rxjs';
import { bindAction, createWorkSelector, defineAction, defineApplication, defineEnvironment, defineProgram,
  defineSurface, implementAction, ResourceReader, z, type Observation, type ResourceReadable,
  type ResourceReaderOptions, type WorkInvocationInput, type WorkSnapshot, type WorkSource } from '@hanger-source/dsh-reactive';
import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { boot, text } from './native-support.ts';

const selector = createWorkSelector({ identify: origin => {
  const scope = origin.kind === 'invocation' ? origin.origin.scope : origin.scope;
  return typeof scope === 'string' ? scope : undefined;
} });

function containsError(actual: unknown, expected: Error): boolean {
  return actual === expected || ((actual instanceof UnsubscriptionError || actual instanceof AggregateError)
    && actual.errors.some((error: unknown) => containsError(error, expected)));
}

// 只在实际资源订阅的 teardown 注入故障；值、代次和业务生命周期全部沿原来源传播。
function cleanupFault<T>(source: ResourceReadable<T>, failure?: Error, options?: ResourceReaderOptions) {
  let active = 0, releases = 0;
  const reader = new ResourceReader({ peek: () => source.peek(),
    observations$: new Observable<Observation<T>>(subscriber => {
      active++;
      source.observations$.subscribe(subscriber);
      return () => { active--; releases++; if (failure) throw failure; };
    }),
  }, options);
  return { reader, active: () => active, releases: () => releases };
}

async function executingWork() {
  const entered = Promise.withResolvers<void>(), finish = Promise.withResolvers<void>();
  let executions = 0;
  const action = bindAction(implementAction(defineAction({ name: 'cleanup_work', description: '观察退出后继续完成原调用',
    scope: z.string(), input: z.null(), output: z.boolean() }), {
    async execute() { executions++; entered.resolve(); await finish.promise; return true; },
  }), 'cleanup', { admit() {} });
  const invocation = action(null, { invocationId: 'cleanup-invocation', sessionId: 'cleanup-sdk',
    source: { kind: 'participant', actorId: 'cli', participant: 'human', entrypoint: 'cli' },
    inputs: [], signal: new AbortController().signal, continuation: null });
  await entered.promise;
  return { action, invocation, finish, executions: () => executions };
}

for (const exit of ['unsubscribe', 'remove', 'pending'] as const) {
  for (const sourceOwnsError of [false, true]) {
    test(`实际Invocation借用${exit}：清理故障只交付${sourceOwnsError ? '来源owner' : 'Work owner'}一次`, async () => {
      const f = await executingWork();
      const sourceOwner = new AbortController(), workOwner = new AbortController();
      const failure = new Error('ACTIVITY_TEARDOWN_FAILED');
      const sourceErrors: unknown[] = [], workErrors: unknown[] = [], streamErrors: unknown[] = [];
      const activity = cleanupFault(f.invocation.activity, failure, sourceOwnsError
        ? { signal: sourceOwner.signal, onCleanupError: error => sourceErrors.push(error) } : undefined);
      const member = { id: f.invocation.id, origin: f.invocation.origin, activity: activity.reader };
      const members = new BehaviorSubject<Observation<readonly WorkInvocationInput<string>[]>>({ status: 'ready',
        generation: 1, observedAt: 0, value: [member] });
      const work = selector.observe('cleanup', { invocations: members }, {
        signal: workOwner.signal, onCleanupError: error => workErrors.push(error),
      });
      const subscription = work.state$.subscribe({ error: error => streamErrors.push(error) });
      try {
        assert.equal((await work.state.read()).invocations[0]!.activity.status, 'ready');
        assert.equal(activity.active(), 1);
        if (exit === 'unsubscribe') subscription.unsubscribe();
        else if (exit === 'remove') members.next({ status: 'ready', generation: 2, observedAt: 0, value: [] });
        else members.next({ status: 'pending', generation: 2 });
        await setImmediate();
        assert.equal(activity.active(), 0); assert.equal(activity.releases(), 1);
        assert.equal(sourceErrors.length, sourceOwnsError ? 1 : 0);
        assert.equal(workErrors.length, sourceOwnsError ? 0 : 1);
        assert.ok(containsError((sourceOwnsError ? sourceErrors : workErrors)[0], failure));
        assert.deepEqual(streamErrors, []);
        if (exit === 'remove') assert.equal(work.state.peek().status, 'ready');
        if (exit === 'pending') assert.equal(work.state.peek().status, 'pending');
        f.finish.resolve();
        assert.equal(await f.invocation.result, true);
        assert.equal(f.executions(), 1, '退出观察没有重发或取消已经受理的业务');
      } finally {
        subscription.unsubscribe(); sourceOwner.abort(); workOwner.abort();
        f.finish.resolve(); await f.invocation.result; f.action.release();
      }
    });
  }
}

test('同步首ready的read退出在实际ResourceReader返回teardown后仍将清理失败交给Work', async () => {
  const f = await executingWork();
  const failure = new Error('LATE_ACTIVITY_TEARDOWN_FAILED'), errors: unknown[] = [];
  const activity = cleanupFault(f.invocation.activity, failure);
  const owner = new AbortController();
  const work = selector.observe('cleanup', { invocations: [{ id: f.invocation.id,
    origin: f.invocation.origin, activity: activity.reader }] }, {
    signal: owner.signal, onCleanupError: error => errors.push(error),
  });
  try {
    const value = await work.state.read();
    assert.ok(value.invocations[0]!.activity.status === 'ready'
      && value.invocations[0]!.activity.value.state === 'running');
    await setImmediate();
    assert.equal(activity.active(), 0); assert.equal(activity.releases(), 1);
    assert.equal(errors.length, 1, '不能把inner Subscriber晚add执行的teardown故障吞进closed.error');
    assert.ok(containsError(errors[0], failure));
    f.finish.resolve(); assert.equal(await f.invocation.result, true); assert.equal(f.executions(), 1);
  } finally { owner.abort(); f.finish.resolve(); await f.invocation.result; f.action.release(); }
});

for (const exit of ['unsubscribe', 'remove', 'pending'] as const) {
  test(`没有清理callback的${exit}保留原故障，首来源失败仍释放全部借用且名单可恢复`, async () => {
    const f = await executingWork();
    const failure = new Error('UNOWNED_ACTIVITY_TEARDOWN_FAILED');
    const first = cleanupFault(f.invocation.activity, failure), second = cleanupFault(f.invocation.activity);
    // 并列连接来自第二次真实受理，不伪造另一份Invocation身份或状态。
    const peer = f.action(null, { invocationId: 'cleanup-peer', sessionId: 'cleanup-sdk',
      source: { kind: 'participant', actorId: 'cli', participant: 'human', entrypoint: 'cli' },
      inputs: [], signal: new AbortController().signal, continuation: null });
    const peerActivity = cleanupFault(peer.activity);
    const members = new BehaviorSubject<Observation<readonly WorkInvocationInput<string>[]>>({ status: 'ready', generation: 1,
      observedAt: 0, value: [{ id: f.invocation.id, origin: f.invocation.origin, activity: first.reader },
        { id: peer.id, origin: peer.origin, activity: peerActivity.reader }] });
    const work = selector.observe('cleanup', { invocations: members });
    const seen: Observation<WorkSnapshot<string>>[] = [], streamErrors: unknown[] = [];
    const subscription = work.state$.subscribe({ next: value => seen.push(value), error: error => streamErrors.push(error) });
    try {
      assert.equal((await work.state.read()).invocations.length, 2);
      assert.equal(first.active(), 1); assert.equal(peerActivity.active(), 1);
      if (exit === 'unsubscribe') assert.throws(() => subscription.unsubscribe(), error => containsError(error, failure));
      else {
        if (exit === 'remove') members.next({ status: 'ready', generation: 2, observedAt: 0, value: [] });
        else members.next({ status: 'pending', generation: 2 });
        const current = seen.at(-1)!;
        assert.equal(current.status, 'error');
        assert.ok(current.status === 'error' && containsError(current.error, failure));
        assert.deepEqual(streamErrors, [], '名单清理错误应为可恢复的error观察');
        members.next({ status: 'ready', generation: 3, observedAt: 0,
          value: [{ id: f.invocation.id, origin: f.invocation.origin, activity: second.reader }] });
        assert.equal((await work.state.read()).invocations.length, 1);
        assert.equal(second.active(), 1);
      }
      assert.equal(first.active(), 0); assert.equal(peerActivity.active(), 0);
      assert.equal(first.releases(), 1); assert.equal(peerActivity.releases(), 1);
      f.finish.resolve(); assert.deepEqual(await Promise.all([f.invocation.result, peer.result]), [true, true]);
      assert.equal(f.executions(), 2, '每次真实受理只execute一次');
    } finally { subscription.unsubscribe(); f.finish.resolve(); await Promise.all([f.invocation.result, peer.result]); f.action.release(); }
  });
}

test('实际Session SDK的progress清理失败只报告一次并释放并列human与其他会话', { timeout: 30_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-work-cleanup-'));
  const definition = defineApplication({ name: 'work-cleanup', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '处理输入', program: implementProgram(defineProgram({ input: z.string(),
      surface: defineSurface({ views: {}, actions: {} }) }), { Component() { return null; } }),
  })] });
  const app = await boot(join(root, 'home'), definition, async function* () { yield* text('真实会话仍能继续工作'); });
  const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
  const peer = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
  try {
    for (const exit of ['unsubscribe', 'remove', 'pending'] as const) {
      const owner = new AbortController(), errors: unknown[] = [];
      const failure = new Error('SESSION_PROGRESS_TEARDOWN_FAILED');
      const progress = cleanupFault(session.progress, failure), human = cleanupFault(session.human);
      const peerProgress = cleanupFault(peer.progress), peerHuman = cleanupFault(peer.human);
      const wrap = (source: WorkSource, p: typeof progress, h: typeof human): WorkSource => ({
        identity: source.identity, progress: p.reader, human: h.reader,
        rawEvents$: source.rawEvents$, progressFrames$: source.progressFrames$,
      });
      const members = new BehaviorSubject<Observation<readonly WorkSource[]>>({ status: 'ready', generation: 1, observedAt: 0,
        value: [wrap(session, progress, human), wrap(peer, peerProgress, peerHuman)] });
      const work = selector.observe('cleanup', { sessions: members }, { signal: owner.signal,
        onCleanupError: error => errors.push(error) });
      const subscription = work.state$.subscribe();
      try {
        assert.equal((await work.state.read()).sessions.length, 2);
        assert.ok([progress, human, peerProgress, peerHuman].every(resource => resource.active() === 1));
        if (exit === 'unsubscribe') subscription.unsubscribe();
        else if (exit === 'remove') members.next({ status: 'ready', generation: 2, observedAt: 0, value: [] });
        else members.next({ status: 'pending', generation: 2 });
        await setImmediate();
        assert.ok([progress, human, peerProgress, peerHuman].every(resource => resource.active() === 0));
        assert.equal(errors.length, 1); assert.ok(containsError(errors[0], failure));
      } finally { subscription.unsubscribe(); owner.abort(); }
    }
    await session.submit({ requestId: 'after-cleanup', content: '继续原会话', intent: 'question', delivery: 'followup', scope: 'cleanup' });
    await session.whenIdle();
    const snapshot = selector.select('cleanup', { sessions: [{ identity: session.identity,
      progress: await session.progress.read(), human: await session.human.read() }] });
    assert.ok(snapshot.sessions[0]!.replies.some(reply => reply.value.text === '真实会话仍能继续工作'));
    assert.deepEqual(app.errors, []);
  } finally { session.release(); peer.release(); await app.dispose(); }
});
