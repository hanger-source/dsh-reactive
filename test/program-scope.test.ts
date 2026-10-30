import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { Fragment, StrictMode, createElement as h, type ComponentType } from 'react';
import { BehaviorSubject, Observable, Subject, UnsubscriptionError, concatMap, filter, firstValueFrom } from 'rxjs';
import { createReaction, defineReaction, implementReaction, defineProgram, defineSurface, z, type ProgramProps, type ReactionInstance, type ReactionRun } from '@hanger-source/dsh-reactive';
import { implementProgram, useObservable, useRelation, useScope } from '@hanger-source/dsh-reactive/program';
import { Conversation, ProgramInstance, Scope, createWorkspace } from '@hanger-source/dsh-reactive/kernel';

const contract = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
function createProgram(Component: ComponentType<ProgramProps<typeof contract>>, directory: string) {
  const owner = new Scope(), workspace = createWorkspace(owner, { id: 'scope-workspace', directory });
  const scope = owner.child(), sessionId = crypto.randomUUID();
  const conversation = new Conversation(scope, { sessionId, workspaceId: workspace.id, environmentId: 'work', directory, parentSessionId: null },
    workspace, contract, { sessionId, sequence: -1, source: 'live', execution: 'idle', inputs: [], calls: [], replies: [], goal: null, requests: {}, waits: [], operationFollows: [], failures: [] });
  const program = new ProgramInstance(scope, implementProgram(contract, { Component }), conversation, workspace, {
    execution() { throw new Error('SCOPE_TEST_HAS_NO_ACTION'); },
  });
  return { owner, scope, workspace, program };
}

test('useRelation来源自然终止后组件真实卸载，父关闭仍保留已经释放来源的清理失败', async () => {
  const selected = new BehaviorSubject(true), events = new Subject<number>();
  const issue = new Error('COMPONENT_RELATION_CLEANUP_FAILED');
  let connected = 0, released = 0;
  const source = new Observable<number>(subscriber => {
    connected++;
    const connection = events.subscribe(subscriber);
    return () => { connection.unsubscribe(); connected--; released++; throw issue; };
  });
  function Worker() { useRelation(source); return null; }
  function Root() {
    const current = useObservable(selected);
    return current.status === 'ready' && current.value ? h(Worker) : null;
  }
  const { owner, program } = createProgram(Root, '/workspace');
  try {
    program.mount(); assert.equal(connected, 1);
    events.complete(); assert.equal(connected, 0); assert.equal(released, 1);
    selected.next(false); program.flush();
    assert.equal(program.signal.aborted, false, '自然完成的cleanup失败不冒充来源或程序error');
    owner.dispose();
    const contains = (error: unknown): boolean => error === issue
      || (error instanceof AggregateError || error instanceof UnsubscriptionError) && error.errors.some(contains);
    await assert.rejects(owner.closed, contains);
    assert.equal(released, 1, '组件卸载和父关闭不重复释放已结束来源');
  } finally { selected.complete(); events.complete(); owner.dispose(); }
});

test('组件借用Workspace反应实例，切换目标只结束原关系，Program关闭等待原写入且不收走Workspace', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-borrowed-reaction-'));
  const selected = new BehaviorSubject('a'), events = new Subject<{ target: string; sequence: number }>();
  const entered = Promise.withResolvers<void>(), finish = Promise.withResolvers<void>();
  const changes = defineReaction({ name: 'shared-worker', params: z.object({ target: z.string() }),
    event: z.object({ target: z.string(), sequence: z.number() }), result: z.string() });
  const runs = new Map<string, ReactionRun<typeof changes>>(), instances = new Map<string, ReactionInstance<typeof changes>>();
  const executions: string[] = [];
  let connected = 0;
  const implementation = implementReaction(changes, {
    observe: params => new Observable(subscriber => {
      connected++;
      const subscription = events.pipe(filter(event => event.target === params.target)).subscribe(subscriber);
      return () => { connected--; subscription.unsubscribe(); };
    }), operator: concatMap,
    async execute(event, { params }) {
      executions.push(params.target + ':' + event.sequence);
      if (params.target === 'a') { entered.resolve(); await finish.promise; }
      await writeFile(join(directory, params.target), String(event.sequence));
      return params.target + ':' + event.sequence;
    },
  });
  function Worker({ reaction }: { reaction: ReactionInstance<typeof changes> }) {
    useScope(scope => { runs.set(reaction.params.target, reaction.start({ scope })); }, [reaction]);
    return null;
  }
  function Root({ workspace }: ProgramProps<typeof contract>) {
    const current = useObservable(selected);
    if (current.status !== 'ready') return null;
    const reaction = workspace.reaction(implementation, { target: current.value });
    instances.set(current.value, reaction);
    return h(StrictMode, null, h(Worker, { reaction }));
  }
  const { owner, scope, workspace, program } = createProgram(Root, directory);
  try {
    program.mount();
    const original = runs.get('a')!;
    assert.equal(connected, 1);
    assert.equal(instances.get('a'), workspace.reaction(implementation, { target: 'a' }));
    events.next({ target: 'a', sequence: 1 }); events.next({ target: 'a', sequence: 2 }); await entered.promise;
    selected.next('b'); program.flush();
    assert.equal(original.snapshot().status, 'draining');
    assert.equal(connected, 1, '共享实例保留，原组件关系的来源仍须退出');
    assert.throws(() => instances.get('a')!.start(), /REACTIVE_REACTION_ALREADY_RUNNING/);
    const b = runs.get('b')!;
    const updated = firstValueFrom(b.events$.pipe(filter(event => event.result === 'b:3')));
    events.next({ target: 'b', sequence: 3 }); await updated;
    assert.equal(await readFile(join(directory, 'b'), 'utf8'), '3');
    let closed = false; void scope.closed.then(() => { closed = true; });
    scope.dispose(); await b.settled;
    assert.equal(connected, 0);
    assert.equal(closed, false, 'Program仍拥有已退出组件发起的真实写入结算');
    const independent = workspace.reaction(implementation, { target: 'b' }).start();
    const continued = firstValueFrom(independent.events$.pipe(filter(event => event.result === 'b:4')));
    events.next({ target: 'b', sequence: 4 }); await continued;
    assert.equal(await readFile(join(directory, 'b'), 'utf8'), '4');
    assert.equal(closed, false, '另一个Workspace运行不替代原Program的结算责任');
    finish.resolve(); await original.settled; await scope.closed;
    assert.equal(await readFile(join(directory, 'a'), 'utf8'), '1');
    assert.deepEqual(executions, ['a:1', 'b:3', 'b:4']);
    independent.stop(); await independent.settled;
    assert.equal(owner.disposed, false);
  } finally { finish.resolve(); selected.complete(); events.complete(); owner.dispose(); await owner.closed; }
});

test('组件关系代际独立，退出停止来源与排队，父关闭仍等待退出组件的真实写入和异步清理', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-program-scope-'));
  const selected = new BehaviorSubject({ first: true, revision: 0 }), events = new Subject<{ target: string; sequence: number }>();
  const entered = Promise.withResolvers<void>(), finish = Promise.withResolvers<void>(), cleanup = Promise.withResolvers<void>();
  const scopes: Scope[] = [], runs = new Map<string, ReturnType<ReturnType<typeof createReaction>['start']>>();
  const executed: string[] = [];
  let observing = 0;
  function Worker({ target, revision }: { target: string; revision: number }) {
    useScope(scope => {
      scopes.push(scope);
      const run = createReaction(scope, { event: z.object({ target: z.string(), sequence: z.number() }), result: z.string(),
        observe: () => new Observable<{ target: string; sequence: number }>(subscriber => {
          observing++;
          const connection = events.pipe(filter(event => event.target === target)).subscribe(subscriber);
          return () => { observing--; connection.unsubscribe(); };
        }), operator: concatMap,
        async execute(event) {
          executed.push(target + ':' + event.sequence);
          if (target === 'a') { entered.resolve(); await finish.promise; }
          await writeFile(join(directory, target), String(event.sequence));
          return target + ':' + event.sequence;
        },
      }).start();
      runs.set(target, run);
      return async () => {
        if (target === 'a') await cleanup.promise;
        await writeFile(join(directory, target + '-released'), 'released');
      };
    }, [target, revision]);
    return null;
  }
  function Root() {
    const current = useObservable(selected);
    if (current.status !== 'ready') return null;
    return h(StrictMode, null, h(Fragment, null,
      current.value.first ? h(Worker, { key: 'a', target: 'a', revision: current.value.revision }) : null,
      h(Worker, { key: 'b', target: 'b', revision: current.value.revision })));
  }
  const { owner, program } = createProgram(Root, directory);
  try {
    program.mount();
    assert.equal(observing, 2);
    const initial = [...scopes];
    selected.next({ first: true, revision: 1 }); program.flush();
    assert.equal(observing, 2, 'StrictMode中的关系重建不能累积来源连接');
    assert.ok(initial.every(scope => scope.disposed));
    assert.ok(scopes.length > initial.length, '重新setup必须创建新Scope，不复活旧代际');
    const original = runs.get('a')!, peer = runs.get('b')!;
    events.next({ target: 'a', sequence: 1 }); events.next({ target: 'a', sequence: 2 }); await entered.promise;
    selected.next({ first: false, revision: 1 }); program.flush();
    assert.equal(observing, 1);
    assert.equal(original.snapshot().status, 'draining');
    assert.equal(peer.snapshot().status, 'running');
    const delivered = firstValueFrom(peer.events$.pipe(filter(event => event.result === 'b:3')));
    events.next({ target: 'a', sequence: 3 }); events.next({ target: 'b', sequence: 3 });
    await delivered;
    assert.equal(await readFile(join(directory, 'b'), 'utf8'), '3');
    let closed = false;
    void owner.closed.then(() => { closed = true; });
    owner.dispose();
    assert.equal(observing, 0);
    finish.resolve(); await original.settled;
    assert.equal(await readFile(join(directory, 'a'), 'utf8'), '1');
    assert.equal(closed, false, '业务写入结算后，父owner仍等待已经退出子关系的异步清理');
    cleanup.resolve(); await owner.closed;
    assert.equal(await readFile(join(directory, 'a-released'), 'utf8'), 'released');
    assert.deepEqual(executed, ['a:1', 'b:3'], '模块退出清掉尚未开始的队列，不重执行已经受理的命令');
  } finally { finish.resolve(); cleanup.resolve(); selected.complete(); events.complete(); owner.dispose(); await owner.closed; }
});

test('被放弃的组件求值不会建立关系或执行清理', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-abandoned-scope-'));
  const failure = new Error('COMPONENT_RENDER_REJECTED');
  let setups = 0;
  function Rejected(): never {
    useScope(() => { setups++; return () => { throw new Error('UNCOMMITTED_CLEANUP_RAN'); }; }, []);
    throw failure;
  }
  const { owner, program } = createProgram(Rejected, directory);
  assert.throws(() => program.mount(), error => error === failure);
  owner.dispose(); await owner.closed;
  assert.equal(setups, 0);
});

test('提前释放中同步退出子Scope仍保留异步文件清理，父关闭汇总真实I/O失败并排空其他子owner', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-scope-close-failure-'));
  const parent = new Scope(), child = parent.child(), sibling = parent.child();
  const proceed = Promise.withResolvers<void>();
  sibling.own(() => writeFile(join(directory, 'other-owner'), 'released'));
  const release = child.own(() => {
    child.dispose();
    return proceed.promise.then(() => writeFile(join(directory, 'absent-directory', 'cleanup'), 'released'));
  });
  let closed = false;
  void parent.closed.then(() => { closed = true; }, () => { closed = true; });
  release(); parent.dispose();
  assert.equal(child.disposed, true);
  assert.equal(closed, false, '子owner重入dispose时，尚未返回的清理Promise不能丢失');
  proceed.resolve();
  const ioFailure = (error: unknown): boolean => error instanceof Error && (
    ('code' in error && error.code === 'ENOENT') || (error instanceof AggregateError && error.errors.some(ioFailure)));
  await assert.rejects(parent.closed, ioFailure);
  assert.equal(await readFile(join(directory, 'other-owner'), 'utf8'), 'released');
});
