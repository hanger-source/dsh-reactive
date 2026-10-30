import assert from 'node:assert/strict';
import { test } from 'node:test';
import { appendFile, mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Observable, Subject, concatMap, firstValueFrom, of } from 'rxjs';
import { SessionId } from '@deepseek-ai/dsh-session';
import { abortable, createReaction, defineApplication, defineEnvironment, defineProgram, defineSurface, view, z,
  type Scope } from '@hanger-source/dsh-reactive';
import { Expose, implementProgram, useScope } from '@hanger-source/dsh-reactive/program';
import { boot, text, tool } from './native-support.ts';
import { bootDelegationFixture } from './delegation-support.ts';

test('冷句柄释放同步收集图且保留真实结算，宿主退出等待已收集图和独立工作区反应', { timeout: 20_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-host-scope-settlement-'));
  const writer = (name: string) => {
    const commands = new Subject<number>();
    const entered = Promise.withResolvers<void>(), proceed = Promise.withResolvers<void>();
    const stopped = Promise.withResolvers<void>();
    const started: number[] = [];
    const path = join(directory, name);
    const options = { event: z.number(), result: z.number(), operator: concatMap,
      observe: () => new Observable<number>(subscriber => {
        const connection = commands.subscribe(subscriber);
        return () => { connection.unsubscribe(); stopped.resolve(); };
      }),
      async execute(value: number) {
        started.push(value);
        await writeFile(path, `started:${value}`);
        entered.resolve();
        await proceed.promise;
        await appendFile(path, '\nsettled');
        return value;
      },
    };
    return { commands, entered, proceed, stopped, started, path, options };
  };
  const sessionOwner = (scope: Scope, id: string) => {
    const writing = writer(id);
    const run = createReaction(scope, writing.options).start();
    const readable = scope.state('available');
    const cleanupStarted = Promise.withResolvers<void>(), cleanupProceed = Promise.withResolvers<void>();
    const cleanupPath = join(directory, id + '-cleanup');
    return { scope, writing, run, readable, cleanupStarted, cleanupProceed, cleanupPath,
      async cleanup() {
        cleanupStarted.resolve();
        await cleanupProceed.promise;
        await writeFile(cleanupPath, 'cleanup-settled');
      },
    };
  };
  type SessionOwner = ReturnType<typeof sessionOwner>;
  const ids = ['cold-close', 'cold-collect'] as const;
  const ready = new Map(ids.map(id => [id as string, Promise.withResolvers<SessionOwner>()]));
  const owners: SessionOwner[] = [];
  let cold = false, models = 0;
  const surface = defineSurface({ actions: {}, views: { state: view(z.string()) } });
  const program = defineProgram({ input: z.null(), surface });
  const definition = defineApplication({ name: 'host-scope-settlement', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '只运行显式关系，不启动模型。',
    program: implementProgram(program, { Component({ conversation }) {
      useScope(scope => {
        if (!cold) return;
        const owner = sessionOwner(scope, conversation.identity.sessionId);
        owners.push(owner);
        ready.get(conversation.identity.sessionId)!.resolve(owner);
        return () => owner.cleanup();
      }, [conversation]);
      return Expose({ surface, value: { views: { state: of('available') }, actions: {} } });
    } }),
  })] });
  const app = await boot(join(directory, 'home'), definition, async function* () {
    models++; throw new Error('SCOPE_SETTLEMENT_MUST_NOT_START_MODEL');
  });
  const handles: Array<{ release(): void }> = [];
  let workspaceWriter: ReturnType<typeof writer> | undefined;
  try {
    for (const id of ids) {
      const seed = await app.host.create('work', { workspaceId: app.workspace.workspaceId, sessionId: id });
      handles.push(seed);
      await app.host.close(id);
      seed.release();
    }
    cold = true;
    const first = await app.host.open(ids[0], program);
    handles.push(first);
    const firstOwner = await ready.get(first.id)!.promise;
    assert.equal(app.native.agents.get(SessionId(first.id)), undefined, '本次图没有原生Agent');
    assert.equal(await first.readView('state'), 'available');
    firstOwner.writing.commands.next(1);
    firstOwner.writing.commands.next(2);
    await firstOwner.writing.entered.promise;
    let closeFinished = false;
    first.release();
    const closing = firstOwner.scope.closed.then(() => { closeFinished = true; });
    await Promise.all([firstOwner.writing.stopped.promise, firstOwner.cleanupStarted.promise]);
    assert.equal(firstOwner.scope.disposed, true);
    assert.throws(() => firstOwner.readable.peek(), /REACTIVE_SCOPE_DISPOSED/);
    await assert.rejects(first.readView('state'));
    assert.deepEqual(firstOwner.run.snapshot(), { status: 'draining', reason: 'stopped', pending: 1, emitted: 0 });
    firstOwner.writing.commands.next(3);
    assert.deepEqual(firstOwner.writing.started, [1]);
    assert.equal(closeFinished, false, '最后句柄释放同步关闭图，关闭回执等待已开始写入');
    firstOwner.writing.proceed.resolve();
    assert.equal((await firstOwner.run.settled).status, 'stopped');
    assert.equal(closeFinished, false, '写入结束后仍等待组件返回的异步清理');
    firstOwner.cleanupProceed.resolve();
    await closing;
    await firstOwner.scope.closed;
    assert.equal(await readFile(firstOwner.writing.path, 'utf8'), 'started:1\nsettled');
    assert.equal(await readFile(firstOwner.cleanupPath, 'utf8'), 'cleanup-settled');
    first.release();

    const second = await app.host.open(ids[1], program);
    handles.push(second);
    const secondOwner = await ready.get(second.id)!.promise;
    workspaceWriter = writer('workspace');
    const workspaceRun = app.host.workspace(app.workspace.workspaceId).reaction(workspaceWriter.options).start();
    workspaceWriter.commands.next(1);
    workspaceWriter.commands.next(2);
    secondOwner.writing.commands.next(1);
    secondOwner.writing.commands.next(2);
    await Promise.all([secondOwner.writing.entered.promise, workspaceWriter.entered.promise]);
    second.release();
    assert.equal(secondOwner.scope.disposed, true, '最后句柄release同步收集冷图');
    assert.equal(secondOwner.run.snapshot().status, 'draining');
    assert.equal(workspaceRun.snapshot().status, 'running', 'Session收集不停止Workspace关系');
    assert.deepEqual(workspaceWriter.started, [1]);
    let disposed = false;
    const disposal = app.dispose().then(() => { disposed = true; });
    await Promise.all([secondOwner.cleanupStarted.promise, workspaceWriter.stopped.promise]);
    assert.deepEqual(workspaceRun.snapshot(), { status: 'draining', reason: 'stopped', pending: 1, emitted: 0 });
    workspaceWriter.commands.next(3);
    secondOwner.writing.commands.next(3);
    assert.deepEqual(secondOwner.writing.started, [1]);
    assert.deepEqual(workspaceWriter.started, [1]);
    assert.equal(disposed, false);
    workspaceWriter.proceed.resolve();
    await workspaceRun.settled;
    assert.equal(disposed, false, 'Workspace完成不能掩盖已从Session表收集的图');
    secondOwner.writing.proceed.resolve();
    await secondOwner.run.settled;
    assert.equal(disposed, false, '已收集的图仍保留异步组件清理归属');
    secondOwner.cleanupProceed.resolve();
    await disposal;
    await secondOwner.scope.closed;
    assert.equal(await readFile(secondOwner.writing.path, 'utf8'), 'started:1\nsettled');
    assert.equal(await readFile(secondOwner.cleanupPath, 'utf8'), 'cleanup-settled');
    assert.equal(await readFile(workspaceWriter.path, 'utf8'), 'started:1\nsettled');
    assert.equal(models, 0);
    assert.deepEqual(app.errors, []);
  } finally {
    for (const owner of owners) { owner.writing.proceed.resolve(); owner.cleanupProceed.resolve(); owner.writing.commands.complete(); }
    workspaceWriter?.proceed.resolve();
    workspaceWriter?.commands.complete();
    for (const handle of handles) handle.release();
    await app.dispose();
  }
});

for (const mixed of [false, true]) test(mixed
  ? '宿主取消在途子工作Follow恢复时，聚合中的真实拒权仍交付给dispose'
  : '宿主直接dispose取消在途子工作Follow恢复，无需调用方先whenIdle', { timeout: 20_000 }, async () => {
  const jobs = mixed ? ['denied-child', 'cancelled-child'] : ['cancelled-child'];
  const entered = new Map(jobs.map(job => [job, Promise.withResolvers<AbortSignal>()]));
  const childRelease = Promise.withResolvers<void>();
  const readRelease = Promise.withResolvers<void>();
  const businessFailure = new Error('CHILD_FOLLOW_RECOVERY_PERMISSION_REVOKED');
  let recovering = false;
  let fixture!: Awaited<ReturnType<typeof bootDelegationFixture>>;
  let invocationEntered = Promise.withResolvers<(typeof fixture.invocations)[number]>();
  fixture = await bootDelegationFixture('spawn', {
    onInvocation(invocation) { invocationEntered.resolve(invocation); },
    async authorize(admission) {
      if (!recovering || admission.phase !== 'follow' || admission.requester.kind !== 'participant'
        || admission.requester.entrypoint !== 'operation-recovery') return;
      entered.get(admission.input.job)!.resolve(admission.signal);
      if (admission.input.job === 'denied-child') throw businessFailure;
      await firstValueFrom(abortable(() => readRelease.promise, admission.signal));
    },
    async *respond(request) {
      if (request.tools?.some(item => item.name === 'child_complete')) {
        assert.ok(request.signal);
        await firstValueFrom(abortable(() => childRelease.promise, request.signal));
        yield* text('子工作结束');
        return;
      }
      const submitted = request.messages.flatMap(message => message.content)
        .flatMap(block => block.type === 'text' && jobs.includes(block.text) ? [block.text] : []);
      const job = submitted.at(-1);
      assert.ok(job);
      const called = request.messages.some(message => message.content.some(block => block.type === 'tool-call'
        && block.name === 'delegate_work' && block.id === 'host-recovery-' + job));
      if (!called) yield* tool('delegate_work', 'host-recovery-' + job, { ...fixture.rawInput, job });
      else yield* text('保留已经受理的子工作');
    },
  });
  let disposing: Promise<void> | undefined;
  try {
    const receipts = [];
    for (const job of jobs) {
      invocationEntered = Promise.withResolvers<(typeof fixture.invocations)[number]>();
      await fixture.parent.submit({ requestId: 'host-recovery-' + job, content: job,
        intent: 'question', delivery: 'followup', scope: { parentId: fixture.rawScope } });
      const invocation = await invocationEntered.promise;
      receipts.push(await invocation.result);
      await fixture.runtime.native.agents.get(SessionId(fixture.parent.id))!.whenIdle();
    }
    recovering = true;
    const child = fixture.runtime.native.agents.get(SessionId(receipts.at(-1)!.childSessionId))!;
    fixture.runtime.native.goals.edit(child, fixture.runtime.native.goals.get(child)!, { objective: '触发已受理子工作的权威更新' });
    await Promise.all([...entered.values()].map(value => value.promise));
    const pendingSignal = await entered.get('cancelled-child')!.promise;
    const agents = fixture.runtime.native.agents;
    disposing = fixture.dispose();
    if (mixed) {
      const containsBusinessFailure = (error: unknown): boolean => error === businessFailure
        || (error instanceof AggregateError && error.errors.some(containsBusinessFailure));
      await assert.rejects(disposing, containsBusinessFailure);
    } else {
      await disposing;
      assert.deepEqual(fixture.errors, []);
    }
    assert.equal(pendingSignal.aborted, true, '宿主退出取消仍在途恢复实际持有的signal');
    for (const receipt of receipts) assert.equal(agents.get(SessionId(receipt.childSessionId)), undefined);
  } finally {
    readRelease.resolve(); childRelease.resolve(); fixture.parent.release();
    if (disposing) await disposing.catch(() => {});
    else await fixture.dispose();
  }
});
