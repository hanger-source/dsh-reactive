import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Subject, concatMap, map, of } from 'rxjs';
import { Scope, defineResource, implementResource, bindResource, defineReaction, implementReaction, bindReaction, z } from '@hanger-source/dsh-reactive';

test('两个资源消费者共享读取；失效取消旧读且迟到旧结果不能覆盖新现场', async () => {
  const scope = new Scope();
  const invalidations = new Subject<void>();
  const reads: { signal: AbortSignal; value: ReturnType<typeof Promise.withResolvers<number>> }[] = [];
  const resource = defineResource({ name: 'document.revision', params: z.object({ id: z.string() }), value: z.number() });
  const implementation = implementResource(resource, {
    read(_params, { signal }) {
      const value = Promise.withResolvers<number>(); reads.push({ signal, value }); return value.promise;
    },
    invalidations: () => invalidations,
  });
  const binding = bindResource(implementation, { scope });
  const instance = binding.instance({ id: 'merchant-a' });
  assert.equal(binding.instance({ id: 'merchant-a' }), instance);
  const received: number[] = [];
  const first = instance.values$.subscribe(value => received.push(value));
  const second = instance.observations$.subscribe();
  assert.equal(reads.length, 1);
  invalidations.next();
  assert.equal(reads[0].signal.aborted, true);
  assert.equal(reads.length, 2);
  reads[1].value.resolve(2);
  await Promise.resolve(); await Promise.resolve();
  reads[0].value.resolve(1);
  await Promise.resolve(); await Promise.resolve();
  assert.deepEqual(received, [2]);
  first.unsubscribe();
  assert.equal(instance.peek().status, 'ready');
  invalidations.next();
  assert.equal(reads.length, 3);
  second.unsubscribe();
  assert.equal(instance.peek().status, 'pending');
  assert.equal(reads[2].signal.aborted, true);
  scope.dispose();
  assert.throws(() => instance.peek(), /REACTIVE_SCOPE_DISPOSED/);
});

test('停止持续反应撤掉排队关系，已经受理的业务仍可按原 owner 完成', async () => {
  const scope = new Scope();
  const source = new Subject<{ commandId: string }>();
  const accepted = Promise.withResolvers<string>();
  const started: string[] = [];
  const committed: string[] = [];
  const reaction = defineReaction({ name: 'accepted-delivery', params: z.object({ workspaceId: z.string() }),
    event: z.object({ commandId: z.string() }), result: z.string() });
  const implementation = implementReaction(reaction, { observe: () => source, operator: concatMap,
    execute(event) {
      started.push(event.commandId);
      return accepted.promise.then(value => { committed.push(event.commandId); return value; });
    },
  });
  const instance = bindReaction(implementation, { scope }).instance({ workspaceId: 'one' });
  const run = instance.start();
  const one = run.events$.subscribe(), two = run.state$.subscribe();
  assert.throws(() => instance.start(), /ALREADY_RUNNING/);
  source.next({ commandId: 'accepted-one' }); source.next({ commandId: 'queued-two' });
  run.stop();
  assert.deepEqual(started, ['accepted-one']);
  assert.equal((await run.settled).status, 'stopped');
  accepted.resolve('official-result');
  await accepted.promise; await Promise.resolve();
  assert.deepEqual(committed, ['accepted-one']);
  assert.deepEqual(started, ['accepted-one']);
  assert.equal(run.snapshot().emitted, 0);
  one.unsubscribe(); two.unsubscribe(); scope.dispose();
});

test('来源失败成为可观察结算，Scope 退出释放另一项持续感知', async () => {
  const scope = new Scope();
  const failures = new Subject<number>();
  const definition = defineReaction({ name: 'watch-revisions', params: z.number(), event: z.number(), result: z.number() });
  let signal: AbortSignal | undefined;
  const implementation = implementReaction(definition, { observe(_params, context) { signal = context.signal; return failures; },
    operator: concatMap, execute: event => event });
  const binding = bindReaction(implementation, { scope });
  const failed = binding.instance(1).start();
  const error = new Error('business notification connection lost'); failures.error(error);
  const settlement = await failed.settled;
  assert.equal(settlement.status, 'failed');
  if (settlement.status === 'failed') assert.equal(settlement.error, error);
  assert.equal(signal?.aborted, true);
  const never = new Subject<number>();
  let released = false;
  const second = implementReaction(definition, { observe(_params, context) {
    context.signal.addEventListener('abort', () => { released = true; }, { once: true }); return never;
  }, operator: concatMap, execute: value => value });
  const run = bindReaction(second, { scope }).instance(2).start();
  scope.dispose();
  assert.equal((await run.settled).status, 'stopped');
  assert.equal(released, true);
});

test('同步来源首次业务处理释放Scope后，不再开始下一项处理或资源读取', async () => {
  const scope = new Scope();
  const effects: number[] = [];
  const reaction = defineReaction({ name: 'synchronous-delivery', params: z.null(), event: z.number(), result: z.number() });
  const implementation = implementReaction(reaction, { observe: () => of(1, 2), operator: concatMap,
    execute(event) {
      effects.push(event);
      scope.dispose();
      return event;
    },
  });
  const run = bindReaction(implementation, { scope }).instance(null).start();
  await run.settled;
  assert.deepEqual(effects, [1]);

  const resourceScope = new Scope();
  const reads: number[] = [];
  const resource = defineResource({ name: 'synchronous-facts', params: z.null(), value: z.number() });
  const facts = bindResource(implementResource(resource, { observe: () => of(1, 2).pipe(map(value => {
    reads.push(value);
    return { status: 'ready' as const, generation: value, value, observedAt: Date.now() };
  })) }), { scope: resourceScope }).instance(null);
  facts.observations$.subscribe(observation => { if (observation.status === 'ready') resourceScope.dispose(); });
  assert.deepEqual(reads, [1]);
});
