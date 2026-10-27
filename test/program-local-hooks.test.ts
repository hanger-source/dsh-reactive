import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Fragment, StrictMode, createElement, useLayoutEffect } from 'react';
import { BehaviorSubject, Observable, Subject, concatMap } from 'rxjs';
import { defineProgram, defineSurface, z, type LocalResource, type LocalReaction, type WorkspaceContext } from '@hanger-source/dsh-reactive';
import { implementProgram, useObservable, useResource, useResourceInstance, useReaction } from '@hanger-source/dsh-reactive/program';
import { Conversation, ProgramInstance, Scope } from '@hanger-source/dsh-reactive/kernel';

test('局部复合hook用同一Resource和Reaction跨函数消费；组件身份、目标切换及Program失败各守原owner', async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-local-hooks-'));
  await writeFile(join(root, 'a'), '11'); await writeFile(join(root, 'b'), '22');
  const scope = new Scope();
  const workspace: WorkspaceContext = { id: 'local-hooks', directory: root, scope, share: source => source };
  const selected = new BehaviorSubject({ target: 'a', visible: true, tick: 0 }), changes = new Subject<number>();
  const value = z.string().transform(Number), event = z.number(), result = z.string();
  type Capability = { resource: LocalResource<typeof value>; reaction: LocalReaction<typeof event, typeof result> };
  const committed = new Map<string, Capability>();
  const entered = Promise.withResolvers<void>(), release = Promise.withResolvers<void>(), written = Promise.withResolvers<void>();
  let rendering = false, observing = 0;
  const executions: number[] = [];

  function useBusiness(target: string): Capability {
    const resource = useResourceInstance(() => ({ value, read: ({ signal }) => {
      assert.equal(rendering, false, 'factory和render不得读取文件');
      return readFile(join(root, target), { encoding: 'utf8', signal });
    } }), [target]);
    const reaction = useReaction(() => ({ event, result,
      observe: () => new Observable<number>(subscriber => {
        assert.equal(rendering, false); observing++;
        const connection = changes.subscribe(subscriber);
        return () => { observing--; connection.unsubscribe(); };
      }), operator: concatMap,
      async execute(revision) {
        executions.push(revision);
        if (revision === 2) { entered.resolve(); await release.promise; }
        const record = target + ':' + revision;
        await writeFile(join(root, 'business-effect'), record);
        if (revision === 2) written.resolve();
        return record;
      },
    }), [target]);
    return { resource, reaction };
  }
  function Consumer({ session, side, target }: { session: string; side: string; target: string }) {
    rendering = true;
    const capability = useBusiness(target);
    const current = useResource(capability.resource);
    useLayoutEffect(() => { committed.set(session + ':' + side, capability); }, [session, side, capability.resource, capability.reaction]);
    if (current.state.status === 'ready') assert.equal(typeof current.state.value, 'number');
    rendering = false;
    return null;
  }
  const definition = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
  const implementation = implementProgram(definition, { Component({ conversation }) {
    const current = useObservable(() => selected, []);
    if (current.status !== 'ready' || !current.value.visible) return null;
    return createElement(StrictMode, null, createElement(Fragment, null, ...['left', 'right'].map(side => createElement(Consumer,
      { key: side, session: conversation.identity.sessionId, side, target: current.value.target }))));
  } });
  const programs = ['one', 'two'].map(sessionId => {
    const conversation = new Conversation({ sessionId, workspaceId: workspace.id, environmentId: 'work', directory: root, parentSessionId: null },
      workspace, definition, { sessionId, sequence: -1, source: 'live', execution: 'idle', goal: null, requests: {}, waits: [] });
    return new ProgramInstance(conversation.scope, implementation, conversation, workspace, {
      execution() { throw new Error('UNEXPECTED_MODEL_OR_ACTION_EXECUTION'); },
    });
  });
  try {
    for (const program of programs) program.mount();
    const first = committed.get('one:left')!, sibling = committed.get('one:right')!, other = committed.get('two:left')!;
    assert.notEqual(first.resource, sibling.resource); assert.notEqual(first.reaction, sibling.reaction);
    assert.notEqual(first.resource, other.resource); assert.notEqual(first.reaction, other.reaction);
    assert.equal(await first.resource.read(), 11);
    assert.equal(observing, 0, '构造和观察资源都不隐式start反应');
    selected.next({ target: 'a', visible: true, tick: 1 });
    for (const program of programs) program.flush();
    assert.equal(committed.get('one:left')!.resource, first.resource);
    assert.equal(committed.get('one:left')!.reaction, first.reaction);
    const run = first.reaction.start();
    assert.equal(observing, 1);
    const delivered = new Promise<string>(resolve => run.events$.subscribe(value => resolve(value.result)));
    changes.next(1);
    assert.equal(await delivered, 'a:1');

    selected.next({ target: 'b', visible: true, tick: 2 });
    for (const program of programs) program.flush();
    const next = committed.get('one:left')!, remaining = committed.get('two:left')!;
    assert.notEqual(next.resource, first.resource); assert.notEqual(next.reaction, first.reaction);
    assert.equal(await next.resource.read(), 22); assert.equal(await first.resource.read(), 11);
    await writeFile(join(root, 'b'), '33'); next.resource.invalidate();
    assert.equal(await next.resource.read(), 33);
    selected.next({ target: 'b', visible: false, tick: 3 });
    for (const program of programs) program.flush();
    assert.equal(observing, 1, '组件退出不停止Program持有的显式运行');
    assert.equal(await next.resource.read(), 33, '普通函数仍可消费捕获的局部资源');

    changes.next(2); changes.next(3); await entered.promise;
    const failure = new Error('LOCAL_PROGRAM_FAILED');
    programs[0]!.fail(failure);
    assert.equal((await run.settled).status, 'stopped'); assert.equal(observing, 0);
    assert.throws(() => first.resource.read(), /REACTIVE_SCOPE_DISPOSED/);
    assert.throws(() => next.reaction.start(), /REACTIVE_SCOPE_DISPOSED/);
    assert.equal(await remaining.resource.read(), 33, '另一个Program的对象继续有效');
    release.resolve(); await written.promise;
    assert.equal(await readFile(join(root, 'business-effect'), 'utf8'), 'a:2', '已进入业务函数的工作沿原目标完成');
    assert.deepEqual(executions, [1, 2], '失败后尚未开始的队列项不再执行');
  } finally { release.resolve(); selected.complete(); changes.complete(); scope.dispose(); }
});
