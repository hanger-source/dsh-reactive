import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Observable, Subject, UnsubscriptionError, firstValueFrom, type Subscriber } from 'rxjs';
import { Scope, connect, createWorkspace } from '@hanger-source/dsh-reactive/kernel';
import type { Observation } from '@hanger-source/dsh-reactive';

const entries = {
  connect: (scope: Scope, source: Observable<number>) => connect(scope, source),
  workspace: (scope: Scope, source: Observable<number>) => createWorkspace(scope, {
    id: 'source-cleanup', directory: '/workspace',
  }).share(source),
  observe: (scope: Scope, source: Observable<number>) => scope.observe(source).values$,
};
const contains = (error: unknown, issue: Error): boolean => error === issue
  || (error instanceof AggregateError || error instanceof UnsubscriptionError)
    && error.errors.some((child: unknown) => contains(child, issue));

test('来源装配内退出或有限完成，三种owner入口均等待迟到真实teardown并保留失败', async t => {
  for (const [name, attach] of Object.entries(entries)) {
    for (const end of ['dispose', 'next-dispose', 'complete'] as const) await t.test(name + ':' + end, async () => {
      const scope = new Scope(), issue = new Error(name + ':' + end + ':CLEANUP_FAILED');
      let released = 0, continued = 0, actual!: Subscriber<number>;
      const source = new Observable<number>(subscriber => {
        actual = subscriber;
        if (end !== 'dispose') subscriber.next(42);
        if (end === 'complete') subscriber.complete();
        else scope.dispose();
        if (!subscriber.closed) continued++;
        return () => { released++; throw issue; };
      });
      const observed = attach(scope, source);
      assert.equal(actual.closed, true, '来源工厂继续执行时实际订阅已经关闭');
      assert.equal(continued, 0);
      assert.equal(released, 1);
      if (end === 'complete') {
        assert.equal(await firstValueFrom(observed), 42, '有限完成保留最后值');
        scope.dispose();
      }
      await assert.rejects(scope.closed, error => contains(error, issue));
      scope.dispose();
      assert.equal(released, 1, 'owner再次退出不能重复释放来源');
    });
  }
});

test('稳定订阅退出仍同步抛清理失败，并排空同owner下的另一真实来源', async t => {
  for (const [name, attach] of Object.entries(entries)) await t.test(name, async () => {
    const scope = new Scope(), issue = new Error(name + ':STABLE_CLEANUP_FAILED');
    const released: string[] = [];
    attach(scope, new Observable<number>(() => () => { released.push('peer'); }));
    attach(scope, new Observable<number>(() => () => { released.push('failed'); throw issue; }));
    assert.throws(() => scope.dispose(), error => contains(error, issue));
    await assert.rejects(scope.closed, error => contains(error, issue));
    assert.deepEqual(released.sort(), ['failed', 'peer']);
  });
});

test('稳定来源自然完成与error独立于清理失败，关闭回执持有失败且数据保留原终态', async t => {
  for (const [name, attach] of Object.entries(entries)) {
    for (const end of ['complete', 'error'] as const) await t.test(name + ':' + end, async () => {
      const scope = new Scope(), issue = new Error(name + ':NATURAL_CLEANUP_FAILED');
      const failure = new Error(name + ':SOURCE_FAILED'), events = new Subject<number>();
      let released = 0;
      const source = new Observable<number>(subscriber => {
        const connection = events.subscribe(subscriber);
        return () => { connection.unsubscribe(); released++; throw issue; };
      });
      const observed = attach(scope, source), seen: number[] = [], errors: unknown[] = [];
      const subscription = observed.subscribe({ next: value => seen.push(value), error: error => errors.push(error) });
      events.next(42);
      if (end === 'complete') events.complete();
      else events.error(failure);
      assert.equal(released, 1, '自然终止立即释放真实来源');
      assert.deepEqual(seen, [42]);
      assert.deepEqual(errors, end === 'error' ? [failure] : []);
      if (end === 'complete') assert.equal(await firstValueFrom(observed), 42);
      else {
        const lateErrors: unknown[] = [];
        observed.subscribe({ error: error => lateErrors.push(error) });
        assert.deepEqual(lateErrors, [failure], '完整晚订阅仍收到原error，不把ReplaySubject先回放的值当成终态');
      }
      assert.doesNotThrow(() => scope.dispose(), '历史自然终止失败不扩大本次dispose同步抛错');
      await assert.rejects(scope.closed, error => contains(error, issue) && !contains(error, failure));
      subscription.unsubscribe();
      assert.equal(released, 1);
    });
  }
});

test('Workspace共享资源在框架normalize前持有真实来源，工厂退出或complete的迟到清理失败进入原owner回执', async t => {
  for (const end of ['dispose', 'complete'] as const) await t.test(end, async () => {
    const scope = new Scope(), workspace = createWorkspace(scope, { id: end, directory: '/workspace' });
    const issue = new Error('WORKSPACE_NORMALIZED_CLEANUP_FAILED'), reason = new Error('WORKSPACE_RESOURCE_OWNER_ENDED');
    const frames: Observation<number>[] = [];
    let starts = 0, released = 0, closedBeforeReturn = false;
    const source = new Observable<Observation<number>>(subscriber => {
      starts++;
      subscriber.next({ status: 'ready', generation: 1, value: 42, observedAt: Date.now() });
      if (end === 'dispose') scope.dispose(reason);
      else subscriber.complete();
      closedBeforeReturn = subscriber.closed;
      return () => { released++; throw issue; };
    });
    const reader = workspace.shareResource({ observations$: source });
    assert.equal(workspace.shareResource(source), reader, '同一原观察流在normalize前后只有一个工作区读侧');
    assert.equal(starts, 0);
    const subscription = reader.observations$.subscribe(frame => frames.push(frame));
    try {
      assert.equal(closedBeforeReturn, true);
      assert.equal(released, 1);
      if (end === 'complete') {
        const current = reader.peek();
        assert.equal(current.status, 'ready');
        if (current.status === 'ready') assert.equal(current.value, 42);
        scope.dispose(reason);
      }
      const terminal = frames.at(-1)!;
      assert.equal(terminal.status, 'error');
      if (terminal.status === 'error') assert.equal(terminal.error, reason, '资源失效交付原owner原因，cleanup不是数据错误');
      await assert.rejects(scope.closed, error => contains(error, issue));
      reader.observations$.subscribe();
      assert.equal(starts, 1, 'owner退出后旧共享读侧不得重开真实来源');
      assert.equal(released, 1);
    } finally { subscription.unsubscribe(); scope.dispose(); }
  });
});
