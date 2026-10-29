import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { Fragment, StrictMode, createElement as h, type ComponentType } from 'react';
import { BehaviorSubject, Observable, Subject, concatMap, filter, firstValueFrom } from 'rxjs';
import { createReaction, defineProgram, defineSurface, z, type ProgramProps } from '@hanger-source/dsh-reactive';
import { implementProgram, useObservable, useScope } from '@hanger-source/dsh-reactive/program';
import { Conversation, ProgramInstance, Scope, createWorkspace } from '@hanger-source/dsh-reactive/kernel';

const contract = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
function createProgram(Component: ComponentType<ProgramProps<typeof contract>>, directory: string) {
  const owner = new Scope(), workspace = createWorkspace(owner, { id: 'scope-workspace', directory });
  const scope = owner.child(), sessionId = crypto.randomUUID();
  const conversation = new Conversation(scope, { sessionId, workspaceId: workspace.id, environmentId: 'work', directory, parentSessionId: null },
    workspace, contract, { sessionId, sequence: -1, source: 'live', execution: 'idle', inputs: [], calls: [], replies: [], goal: null, requests: {}, waits: [], operationFollows: [] });
  const program = new ProgramInstance(scope, implementProgram(contract, { Component }), conversation, workspace, {
    execution() { throw new Error('SCOPE_TEST_HAS_NO_ACTION'); },
  });
  return { owner, program };
}

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
