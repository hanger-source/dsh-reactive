import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Fragment, createElement, useLayoutEffect } from 'react';
import { BehaviorSubject, EmptyError, Observable, Subject, UnsubscriptionError, defer, filter, firstValueFrom, from, lastValueFrom, map, of, timeout } from 'rxjs';
import { Scope, ResourceReader, createResource, defineProgram, defineSurface, resourceView, mapResource, z, type Observation, type Workspace } from '@hanger-source/dsh-reactive';
import { Conversation, ProgramInstance, createWorkspace } from '@hanger-source/dsh-reactive/kernel';
import { Context, Expose, implementProgram, useObservable, useResource, useShared, useSharedResource } from '@hanger-source/dsh-reactive/program';

const containsCleanup = (error: unknown, issue: Error): boolean => error === issue
  || (error instanceof AggregateError || error instanceof UnsubscriptionError)
    && error.errors.some((child: unknown) => containsCleanup(child, issue));

test('Program共享来源的真实释放失败归父退出或程序失败owner，退出回执不早于来源清理', async () => {
  for (const end of ['dispose', 'fail'] as const) {
    const issue = new Error('SHARED_QUERY_TEARDOWN_FAILED'), reports: unknown[] = [];
    const root = new Scope({ onObserverError: error => reports.push(error) }), sessionScope = root.child();
    const workspace = createWorkspace(root, { id: end, directory: '/workspace' });
    const definition = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
    const conversation = new Conversation(sessionScope, { sessionId: end, workspaceId: end, environmentId: 'work',
      directory: '/workspace', parentSessionId: null }, workspace, definition,
    { sessionId: end, sequence: -1, source: 'live', execution: 'idle', inputs: [], calls: [], replies: [], goal: null, requests: {}, waits: [], operationFollows: [], failures: [] });
    let released = 0, reader!: ResourceReader<number>;
    const source = new Observable<Observation<number>>(subscriber => {
      subscriber.next({ status: 'ready', generation: 1, value: 42, observedAt: Date.now() });
      return () => { released++; throw issue; };
    });
    const implementation = implementProgram(definition, { Component() {
      reader = useSharedResource(source);
      return null;
    } });
    const program = new ProgramInstance(sessionScope, implementation, conversation, workspace, {
      execution() { throw new Error('READING_MUST_NOT_EXECUTE_ACTION'); },
    });
    program.mount();
    const observations: Observation<number>[] = [];
    const subscription = reader.observations$.subscribe(value => observations.push(value));
    try {
      assert.equal(reader.peek().status, 'ready');
      if (end === 'fail') {
        program.fail(new Error('PROGRAM_FAILED'));
      } else {
        sessionScope.dispose();
      }
      assert.equal(released, 1); assert.equal(subscription.closed, true);
      const invalidated = observations.at(-1)!;
      assert.equal(invalidated.status, 'error');
      if (invalidated.status === 'error') assert.equal(invalidated.error, program.signal.reason);
      sessionScope.dispose(); root.dispose();
      await assert.rejects(root.closed, error => containsCleanup(error, issue));
    } finally { subscription.unsubscribe(); sessionScope.dispose(); root.dispose(); }
  }
});

test('useShared实际查询在最后消费者离开或工厂内Program失败时释放，父关闭保留错误且旧查询不重连', async t => {
  for (const end of ['last-consumer', 'factory-fail'] as const) await t.test(end, async () => {
    const reports: unknown[] = [], root = new Scope({ onObserverError: error => reports.push(error) });
    const sessionScope = root.child(), workspace = createWorkspace(root, { id: end, directory: '/workspace' });
    const definition = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
    const conversation = new Conversation(sessionScope, { sessionId: end, workspaceId: end, environmentId: 'work',
      directory: '/workspace', parentSessionId: null }, workspace, definition,
    { sessionId: end, sequence: -1, source: 'live', execution: 'idle', inputs: [], calls: [], replies: [], goal: null, requests: {}, waits: [], operationFollows: [], failures: [] });
    const issue = new Error('SHARED_RAW_QUERY_CLEANUP_FAILED'), failure = new Error('PROGRAM_QUERY_FAILED');
    let starts = 0, released = 0, query!: Observable<number>, program!: ProgramInstance<typeof definition>;
    const source = new Observable<number>(subscriber => {
      starts++; subscriber.next(42);
      if (end === 'factory-fail') program.fail(failure);
      return () => { released++; throw issue; };
    });
    program = new ProgramInstance(sessionScope, implementProgram(definition, { Component() {
      query = useShared(source); return null;
    } }), conversation, workspace, { execution() { throw new Error('QUERY_MUST_NOT_START_BUSINESS_ACTION'); } });
    program.mount();
    assert.equal(starts, 0);
    const seen: number[] = [], subscription = query.subscribe(value => seen.push(value));
    try {
      assert.deepEqual(seen, [42]);
      if (end === 'last-consumer') {
        subscription.unsubscribe();
        assert.equal(released, 1, 'refCount归零真正退订，不能等Program退出才释放');
        program.fail(failure);
      }
      assert.equal(released, 1);
      const late: number[] = [];
      assert.equal(query.subscribe(value => late.push(value)).closed, true);
      assert.deepEqual(late, []);
      assert.equal(starts, 1, '旧查询在Program失败后不能重新打开来源');
      sessionScope.dispose(); root.dispose();
      await assert.rejects(root.closed, error => containsCleanup(error, issue));
      assert.equal(released, 1);
    } finally { subscription.unsubscribe(); sessionScope.dispose(); root.dispose(); }
  });
});

test('Program的资源hook在框架normalize前接管真实来源，有限ready与工厂退出均保存迟到清理失败', async t => {
  for (const hook of ['shared-reader', 'resource-hook'] as const) {
    for (const end of ['complete', 'factory-fail'] as const) await t.test(hook + ':' + end, async () => {
      const reports: unknown[] = [], root = new Scope({ onObserverError: error => reports.push(error) });
      const sessionScope = root.child(), workspace = createWorkspace(root, { id: end, directory: '/workspace' });
      const definition = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
      const conversation = new Conversation(sessionScope, { sessionId: end, workspaceId: end, environmentId: 'work',
        directory: '/workspace', parentSessionId: null }, workspace, definition,
      { sessionId: end, sequence: -1, source: 'live', execution: 'idle', inputs: [], calls: [], replies: [], goal: null, requests: {}, waits: [], operationFollows: [], failures: [] });
      const issue = new Error('PROGRAM_NORMALIZED_CLEANUP_FAILED'), failure = new Error('NORMALIZED_QUERY_PROGRAM_FAILED');
      let starts = 0, released = 0, closedBeforeReturn = false, reader: ResourceReader<number> | undefined;
      let state!: Observation<number>, observations$!: Observable<Observation<number>>, program!: ProgramInstance<typeof definition>;
      const source = new Observable<Observation<number>>(subscriber => {
        starts++;
        subscriber.next({ status: 'ready', generation: 1, value: 42, observedAt: Date.now() });
        if (end === 'complete') subscriber.complete();
        else program.fail(failure);
        closedBeforeReturn = subscriber.closed;
        return () => { released++; throw issue; };
      });
      program = new ProgramInstance(sessionScope, implementProgram(definition, { Component() {
        if (hook === 'shared-reader') {
          reader = useSharedResource(source); observations$ = reader.observations$;
        } else {
          const observed = useResource(source); state = observed.state; observations$ = observed.observations$;
        }
        return null;
      } }), conversation, workspace, { execution() { throw new Error('RESOURCE_QUERY_MUST_NOT_EXECUTE_ACTION'); } });
      let subscription: { unsubscribe(): void } | undefined;
      try {
        if (hook === 'resource-hook' && end === 'factory-fail') assert.throws(() => program.mount(), error => error === failure);
        else program.mount();
        if (hook === 'shared-reader') {
          assert.equal(starts, 0, '共享Reader构造不能启动来源');
          subscription = observations$.subscribe();
        }
        assert.equal(starts, 1); assert.equal(released, 1);
        assert.equal(closedBeforeReturn, true, '框架normalize不能隔断真实来源的即时退订');
        if (end === 'complete') {
          program.flush();
          const current = reader ? reader.peek() : state;
          assert.equal(current.status, 'ready');
          if (current.status === 'ready') assert.equal(current.value, 42, '有限来源清理失败不覆盖已完成ready');
        } else assert.equal(program.signal.reason, failure);
        sessionScope.dispose(); root.dispose();
        await assert.rejects(root.closed, error => containsCleanup(error, issue) && !containsCleanup(error, failure));
        observations$.subscribe();
        assert.equal(starts, 1, '退出后借用旧观察流不能重连已经结束的查询');
        assert.equal(released, 1);
      } finally { subscription?.unsubscribe(); sessionScope.dispose(); root.dispose(); }
    });
  }
});

test('同一外部资源在对象、原流和hook之间切换不重读，原owner退出撤下Context与公开视图', async () => {
  const scope = new Scope(), owner = new AbortController(), reason = new Error('BORROWED_FINITE_RESOURCE_ENDED');
  const workspace = createWorkspace(scope, { id: 'finite-reader', directory: '/workspace' });
  const sessionScope = scope.child();
  const definition = defineProgram({ input: z.null(), surface: defineSurface({ actions: {}, views: { current: resourceView(z.number()) } }) });
  const frame: Observation<number> = { status: 'ready', generation: 1, value: 42, observedAt: Date.now() };
  let reads = 0;
  const selected = new BehaviorSubject(0);
  const resource = new ResourceReader({ peek: () => frame, observations$: defer(() => { reads++; return of(frame); }) }, { signal: owner.signal });
  const identity = { sessionId: 'finite-reader', workspaceId: workspace.id, environmentId: 'work', directory: workspace.directory, parentSessionId: null };
  const conversation = new Conversation(sessionScope, identity, workspace, definition, { sessionId: identity.sessionId,
    sequence: -1, source: 'live', execution: 'idle', inputs: [], calls: [], replies: [], goal: null, requests: {}, waits: [], operationFollows: [], failures: [] });
  const implementation = implementProgram(definition, { Component() {
    const mode = useObservable(selected);
    const observed = useResource(() => resource, []);
    const source = mode.status === 'ready' && mode.value === 1 ? resource.observations$
      : mode.status === 'ready' && mode.value === 2 ? observed : resource;
    return createElement(Fragment, null,
      Context({ name: 'current', source: observed, render: value => ({ value }) }),
      Expose({ surface: definition.surface, value: { views: { current: source }, actions: {} } }));
  } });
  const program = new ProgramInstance(sessionScope, implementation, conversation, workspace, {
    execution() { throw new Error('RESOURCE_OBSERVATION_MUST_NOT_EXECUTE_ACTION'); },
  });
  try {
    program.mount();
    assert.equal(await program.outputs.view('current').read(), 42);
    assert.deepEqual(JSON.parse(await program.contexts.get('current')!.read()), { value: 42 });
    const initialReads = reads;
    for (const mode of [1, 2, 0]) {
      selected.next(mode); program.flush();
      assert.equal(await program.outputs.view('current').read(), 42);
      assert.equal(reads, initialReads, '同一原观察流的声明形态变化不能重新执行有限来源');
    }
    owner.abort(reason);
    program.flush();
    await assert.rejects(program.outputs.view('current').read(), error => error === reason);
    await assert.rejects(program.contexts.get('current')!.read(), error => error === reason);
    assert.equal(sessionScope.disposed, false, '来源退出没有结束借用它的Program');
  } finally { owner.abort(); scope.dispose(); }
});

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
    const workspace: Workspace = createWorkspace(scope, { id: end, directory: root });
    const sessionScope = scope.child();
    const borrowed = createResource(scope, { value: z.string(), read: () => { reads.borrowed++; return readFile(paths.borrowed, 'utf8'); } });
    const events = new Subject<number>();
    const labelSource = file('label'), deniedSource = file('denied'), coldSource = file('cold');
    const settingsSource = observations('settings');
    let starts = 0, stops = 0;
    function useCapability() {
      const label = useShared(labelSource);
      const denied = useShared(deniedSource);
      const cold = useShared(coldSource);
      const live = useShared(() => new Observable<number>(subscriber => {
        starts++; const subscription = events.subscribe(subscriber);
        return () => { stops++; subscription.unsubscribe(); };
      }), []);
      const settings = useSharedResource(settingsSource);
      const sameSettings = useSharedResource(() => ({ observations$: settingsSource }), []);
      const derived = useSharedResource(() => settings.observations$.pipe(mapResource(value => value.toUpperCase())), [settings]);
      const current = useResource(derived);
      const missing = useResource(() => observations('missing'), []);
      const shared = useResource(() => borrowed, []);
      const borrowedReader = useSharedResource(() => borrowed, []);
      const coldReader = useSharedResource(() => observations('cold'), []);
      return { label, denied, cold, live, settings, sameSettings, derived, current, borrowedReader, coldReader,
        missing: missing.observations$, borrowed: shared.observations$ };
    }
    let capability!: ReturnType<typeof useCapability>;
    const definition = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
    const conversation = new Conversation(sessionScope, { sessionId: end, workspaceId: workspace.id, environmentId: 'work',
      directory: root, parentSessionId: null }, workspace, definition, {
      sessionId: end, sequence: -1, source: 'live', execution: 'idle', inputs: [], calls: [], replies: [], goal: null, requests: {}, waits: [], operationFollows: [], failures: [],
    });
    const implementation = implementProgram(definition, { Component() {
      const current = useCapability();
      useLayoutEffect(() => { capability = current; });
      return null;
    } });
    const program = new ProgramInstance(sessionScope, implementation, conversation, workspace, {
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
      assert.equal(await capability.settings.read(), 'settings-first');
      assert.equal(await capability.sameSettings.read(), 'settings-first');
      assert.equal(await capability.derived.read(), 'SETTINGS-FIRST');
      program.flush();
      assert.equal(capability.derived.peek().status, 'ready');
      assert.deepEqual(capability.current.state.status === 'ready' ? capability.current.state.value : undefined, 'SETTINGS-FIRST');
      assert.equal(await capability.borrowedReader.read(), 'workspace-first');
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
      else sessionScope.dispose();
      assert.equal(completed, 1); assert.equal(stops, 1); assert.equal(two.closed, true);
      events.next(22); assert.deepEqual(second, [11]); assert.deepEqual(late, []);
      for (const source of [capability.label, capability.denied, capability.cold, capability.live, capability.missing]) {
        const delivered: unknown[] = [], errors: unknown[] = [];
        let ended = false;
        const subscriber = (source as Observable<unknown>).subscribe({ next: value => delivered.push(value),
          error: error => errors.push(error), complete() { ended = true; } });
        subscriptions.push(subscriber);
        assert.equal(ended, true); assert.equal(subscriber.closed, true); assert.deepEqual(delivered, []); assert.deepEqual(errors, []);
        await assert.rejects(firstValueFrom(source as Observable<unknown>), EmptyError);
      }
      for (const reader of [capability.settings, capability.sameSettings, capability.derived, capability.borrowedReader, capability.coldReader]) {
        assert.throws(() => reader.peek(), error => error === program.signal.reason);
        assert.throws(() => reader.read(), error => error === program.signal.reason);
        const ended = await firstValueFrom(reader.observations$);
        assert.equal(ended.status, 'error');
        if (ended.status === 'error') assert.equal(ended.error, program.signal.reason);
      }
      assert.deepEqual(reads, { label: 1, settings: 1, denied: 1, missing: 1, cold: 0, borrowed: 1 });
      await writeFile(paths.borrowed, 'workspace-second');
      const changed = firstValueFrom(borrowed.values$.pipe(filter(value => value === 'workspace-second'), timeout(3_000)));
      borrowed.invalidate(); assert.equal(await changed, 'workspace-second');
      assert.equal(scope.disposed, false, 'Program退出不销毁共享事实的业务owner');
    } finally {
      subscriptions.forEach(subscription => subscription.unsubscribe());
      sessionScope.dispose(); scope.dispose(); events.complete();
    }
  });
});
