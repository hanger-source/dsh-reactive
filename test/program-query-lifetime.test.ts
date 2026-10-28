import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { useLayoutEffect } from 'react';
import { EmptyError, Observable, Subject, defer, filter, firstValueFrom, from, lastValueFrom, map, timeout } from 'rxjs';
import { Scope, createResource, defineProgram, defineSurface, z, type Observation, type WorkspaceContext } from '@hanger-source/dsh-reactive';
import { Conversation, ProgramInstance, connect } from '@hanger-source/dsh-reactive/kernel';
import { implementProgram, useResource, useShared } from '@hanger-source/dsh-reactive/program';

test('复合hook的自建查询共享连接和终止缓存，Program退出不重放或重新读取，借用资源保留原owner', async t => {
  for (const end of ['dispose', 'fail'] as const) await t.test(end, async () => {
    const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-program-query-' + end + '-'));
    const paths = { label: join(root, 'label'), settings: join(root, 'settings'), denied: join(root, 'denied'),
      missing: join(root, 'missing'), cold: join(root, 'cold'), borrowed: join(root, 'borrowed') };
    await Promise.all([writeFile(paths.label, 'label-first'), writeFile(paths.settings, 'settings-first'),
      writeFile(paths.cold, 'never-read'), writeFile(paths.borrowed, 'workspace-first')]);
    const reads = { label: 0, settings: 0, denied: 0, missing: 0, cold: 0, borrowed: 0 };
    const file = (name: keyof typeof paths) => defer(() => { reads[name]++; return from(readFile(paths[name], 'utf8')); });
    const observations = (name: keyof typeof paths) => file(name).pipe(map(value => ({
      status: 'ready' as const, generation: 1, observedAt: Date.now(), value,
    })));
    const scope = new Scope();
    const workspace: WorkspaceContext = { id: end, directory: root, scope, share: source => connect(scope, source) };
    const borrowed = createResource(scope, { value: z.string(), read: () => { reads.borrowed++; return readFile(paths.borrowed, 'utf8'); } });
    const events = new Subject<number>();
    let starts = 0, stops = 0;
    function useCapability() {
      const label = useShared(() => file('label'), []);
      const denied = useShared(() => file('denied'), []);
      const cold = useShared(() => file('cold'), []);
      const live = useShared(() => new Observable<number>(subscriber => {
        starts++; const subscription = events.subscribe(subscriber);
        return () => { stops++; subscription.unsubscribe(); };
      }), []);
      const settings = useResource(() => observations('settings'), []);
      const missing = useResource(() => observations('missing'), []);
      const shared = useResource(borrowed);
      return { label, denied, cold, live, settings: settings.observations$, missing: missing.observations$, borrowed: shared.observations$ };
    }
    let capability!: ReturnType<typeof useCapability>;
    const definition = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
    const conversation = new Conversation({ sessionId: end, workspaceId: workspace.id, environmentId: 'work',
      directory: root, parentSessionId: null }, workspace, definition, {
      sessionId: end, sequence: -1, source: 'live', execution: 'idle', goal: null, requests: {}, waits: [],
    });
    const implementation = implementProgram(definition, { Component() {
      const current = useCapability();
      useLayoutEffect(() => { capability = current; });
      return null;
    } });
    const program = new ProgramInstance(conversation.scope, implementation, conversation, workspace, {
      execution() { throw new Error('QUERY_MUST_NOT_EXECUTE_ACTION'); },
    });
    const subscriptions: { unsubscribe(): void }[] = [];
    const read = <T>(source: Observable<Observation<T>>) => firstValueFrom(source.pipe(
      filter(value => value.status !== 'pending'), timeout(3_000),
    ));
    try {
      program.mount();
      assert.equal(reads.label, 0); assert.equal(reads.cold, 0, '共享构造仍不启动读取');
      assert.equal(await lastValueFrom(capability.label), 'label-first');
      assert.equal(await firstValueFrom(capability.label), 'label-first');
      assert.equal((await read(capability.settings)).status, 'ready');
      const failed = await read(capability.missing);
      assert.equal(failed.status, 'error');
      if (failed.status === 'error') assert.equal((failed.error as NodeJS.ErrnoException).code, 'ENOENT');
      let queryError: unknown;
      await assert.rejects(firstValueFrom(capability.denied), error => { queryError = error; return (error as NodeJS.ErrnoException).code === 'ENOENT'; });
      await assert.rejects(firstValueFrom(capability.denied), error => error === queryError);
      assert.deepEqual(reads, { label: 1, settings: 1, denied: 1, missing: 1, cold: 0, borrowed: 1 });
      assert.equal(capability.borrowed, borrowed.observations$, '直接借入资源继续使用原对象及其共享机制');
      const first: number[] = [], second: number[] = [], late: unknown[] = [];
      let completed = 0;
      const one = capability.live.subscribe(value => first.push(value));
      const two = capability.live.subscribe({ next: value => second.push(value), complete() {
        completed++;
        // complete清理期间重入，同样不能借旧hook重新打开查询或取得旧终止缓存。
        for (const source of [capability.label, capability.denied, capability.cold])
          subscriptions.push(source.subscribe({ next: value => late.push(value), error: error => late.push(error) }));
      } });
      subscriptions.push(one, two, borrowed.observations$.subscribe());
      assert.equal(starts, 1); events.next(11); assert.deepEqual(first, [11]); assert.deepEqual(second, [11]);
      one.unsubscribe(); assert.equal(stops, 0);
      if (end === 'fail') program.fail(new Error('EXPLICIT_PROGRAM_FAILURE'));
      else conversation.scope.dispose();
      assert.equal(completed, 1); assert.equal(stops, 1); assert.equal(two.closed, true);
      events.next(22); assert.deepEqual(second, [11]); assert.deepEqual(late, []);
      for (const source of [capability.label, capability.denied, capability.cold, capability.live, capability.settings, capability.missing]) {
        const delivered: unknown[] = [], errors: unknown[] = [];
        let ended = false;
        const subscriber = (source as Observable<unknown>).subscribe({ next: value => delivered.push(value),
          error: error => errors.push(error), complete() { ended = true; } });
        subscriptions.push(subscriber);
        assert.equal(ended, true); assert.equal(subscriber.closed, true); assert.deepEqual(delivered, []); assert.deepEqual(errors, []);
        await assert.rejects(firstValueFrom(source as Observable<unknown>), EmptyError);
      }
      assert.deepEqual(reads, { label: 1, settings: 1, denied: 1, missing: 1, cold: 0, borrowed: 1 });
      await writeFile(paths.borrowed, 'workspace-second');
      const changed = firstValueFrom(borrowed.values$.pipe(filter(value => value === 'workspace-second'), timeout(3_000)));
      borrowed.invalidate(); assert.equal(await changed, 'workspace-second');
      assert.equal(scope.disposed, false, 'Program退出不销毁共享事实的业务owner');
    } finally {
      subscriptions.forEach(subscription => subscription.unsubscribe());
      conversation.scope.dispose(); scope.dispose(); events.complete();
    }
  });
});
