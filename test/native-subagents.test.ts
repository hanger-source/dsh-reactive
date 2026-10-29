import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate as immediate } from 'node:timers/promises';
import { useMemo } from 'react';
import { filter, firstValueFrom, timeout, type Subscription } from 'rxjs';
import { symbols } from '@deepseek-ai/cordis';
import { SessionId } from '@deepseek-ai/dsh-session';
import type { SubagentRuntime } from '@deepseek-ai/dsh-subagent';
import { defineApplication, defineEnvironment, defineProgram, defineSurface, mapResource, resourceView, z } from '@hanger-source/dsh-reactive';
import { Expose, implementProgram, useResource } from '@hanger-source/dsh-reactive/program';
import { createNativeSubagents } from '@hanger-source/dsh-reactive-dsh/subagents';
import { boot, text } from './native-support.ts';

test('原生子目录三种模式沿真实Session持久事实组合，缺失分支保诊断，冷读与取消不启动Agent或Goal', async () => {
  const home = await mkdtemp(join(tmpdir(), 'dsh-reactive-native-subagents-'));
  const capability = createNativeSubagents({ service: 'provided' });
  const program = defineProgram({ input: z.null(), surface: defineSurface({ actions: {}, views: { modes: resourceView(z.string().array()) } }) });
  const definition = defineApplication({ name: 'native-subagents', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '子目录观察不执行任务', program: implementProgram(program, { Component({ conversation }) {
      const children = useMemo(() => capability.forSession(conversation), [conversation]);
      const observed = useResource(children.catalog);
      return Expose({ surface: program.surface, value: { views: {
        modes: observed.observations$.pipe(mapResource(rows => rows.map(row => row.mode))),
      }, actions: {} } });
    } }),
  })] });
  let models = 0;
  const app = await boot(home, definition, async function* () { models++; yield* text('观察不应唤醒模型'); });
  await capability.install(app.native, home);
  const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
  const peer = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
  const children = capability.forSession(session);
  const observer = children.catalog.observations$.subscribe();
  let descendantObserver: Subscription | undefined;
  const service = Reflect.get(app.native.subagents, symbols.original) as SubagentRuntime;
  const original = service.listDescendants;
  const release = Promise.withResolvers<void>();
  try {
    assert.deepEqual(await children.catalog.read(), []);
    assert.equal(children.descendants.peek().status, 'pending', '构造与peek不触发递归查询');
    descendantObserver = children.descendants$.subscribe();
    assert.deepEqual(await children.descendants.read(), []);
    const parent = app.native.sessions.get(SessionId(session.id))!;
    const nativePeer = app.native.sessions.get(SessionId(peer.id))!;
    // 正式持久目录可保留已归档或已不可读的历史子项；不伪造Activation、Agent或执行结果。
    const entries = [
      { version: 0 as const, mode: 'one-shot' as const, childId: SessionId('archived-one-shot'), childCreatedAt: Date.now() },
      { version: 0 as const, mode: 'continuable' as const, label: '历史子会话', childId: SessionId('archived-continuable'), childCreatedAt: Date.now() },
      { version: 2 as const, mode: 'external' as const, label: '外部执行', childId: SessionId('external-child'), childCreatedAt: Date.now() },
    ];
    const updated = firstValueFrom(children.catalog.observations$.pipe(filter(value => value.status === 'ready' && value.value.length === 3), timeout(3_000)));
    const modesUpdated = firstValueFrom(session.viewResource('modes').observations$.pipe(filter(value => value.status === 'ready' && value.value.length === 3), timeout(3_000)));
    for (const entry of entries) parent.append('subagent/catalog', entry);
    await Promise.all([updated, modesUpdated]);
    await children.descendants.read();
    assert.deepEqual(await session.readView('modes'), ['one-shot', 'continuable', 'external']);
    assert.equal(app.native.goals.get(app.native.agents.get(SessionId(session.id))!), undefined);
    const before = children.catalog.peek();
    const beforeDescendants = children.descendants.peek();
    nativePeer.append('subagent/catalog', { version: 2, mode: 'external', childId: SessionId('peer-child'), childCreatedAt: Date.now() });
    await immediate();
    assert.equal(children.catalog.peek(), before, '其他父会话的目录变更不撤销此资源');
    assert.equal(children.descendants.peek(), beforeDescendants, '不在正式可达切面的父会话变化不撤销后代资源');

    const nested = firstValueFrom(children.descendants$.pipe(filter(rows => rows.some(row => row.id === 'peer-child')), timeout(3_000)));
    parent.append('subagent/catalog', { version: 0, mode: 'continuable', label: '可读取的分支',
      childId: nativePeer.id, childCreatedAt: nativePeer.header.createdAt });
    await nested;
    const branchUpdate = firstValueFrom(children.descendants$.pipe(filter(rows => rows.some(row => row.id === 'nested-external')), timeout(3_000)));
    nativePeer.append('subagent/catalog', { version: 2, mode: 'external', childId: SessionId('nested-external'), childCreatedAt: Date.now() });
    await branchUpdate;
    const recursive = await children.descendants.read();
    assert.deepEqual(recursive, await children.readDescendants(), 'Promise与Resource均保留正式递归切面');
    assert.ok(recursive.some(row => row.kind === 'child' && row.id === nativePeer.id && row.activity === 'running'));
    const branchCold = firstValueFrom(children.descendants$.pipe(filter(rows => rows.some(row =>
      row.kind === 'child' && row.id === nativePeer.id && row.activity === 'inactive')), timeout(3_000)));
    await app.host.close(peer.id);
    await branchCold;
    assert.equal(app.native.agents.get(nativePeer.id), undefined, '后代资源沿驻留事件失效，不把观察变成恢复');

    await app.host.close(session.id);
    assert.equal(app.native.agents.get(SessionId(session.id)), undefined);
    assert.deepEqual((await children.catalog.read()).map(row => row.mode), ['one-shot', 'continuable', 'external', 'continuable']);
    const descendants = await children.readDescendants();
    assert.ok(descendants.some(row => row.kind === 'child' && row.mode === 'external' && row.id === 'external-child'));
    assert.ok(descendants.some(row => row.kind === 'diagnostic' && row.reason === 'unavailable'), '缺失历史分支不是空子树或业务完成');
    assert.equal(app.native.agents.get(SessionId(session.id)), undefined, '递归冷读不恢复Agent');
    assert.deepEqual(await children.descendants.read(), descendants);
    assert.throws(() => children.interrupt(SessionId('external-child')), /REACTIVE_NATIVE_SUBAGENTS_LIVE_ANCESTOR_REQUIRED/);
    assert.equal(app.native.agents.get(SessionId(session.id)), undefined, '冷祖先控制拒绝不激活Agent');

    const entered = Promise.withResolvers<AbortSignal>();
    service.listDescendants = async function (id, signal) {
      const rows = await original.call(this, id, signal);
      assert.ok(signal); entered.resolve(signal); await release.promise; return rows;
    };
    const cancellation = new AbortController(), reason = new Error('CANCEL_ONE_SUBAGENT_READ');
    const reading = children.readDescendants({ signal: cancellation.signal });
    const rejected = assert.rejects(reading, error => error === reason);
    const signal = await entered.promise;
    cancellation.abort(reason); await rejected;
    assert.equal(signal.aborted, true);
    release.resolve(); service.listDescendants = original;
    const sessionId = session.id;
    session.release();
    assert.throws(() => children.catalog.peek(), /REACTIVE_SCOPE_DISPOSED/);
    assert.throws(() => children.descendants.peek(), /REACTIVE_SCOPE_DISPOSED/);
    const resumed = await app.host.resume(sessionId, program);
    try {
      assert.deepEqual(await resumed.readView('modes'), ['one-shot', 'continuable', 'external', 'continuable']);
      await capability.dispose();
      assert.throws(() => capability.forSession(resumed), /REACTIVE_SCOPE_DISPOSED/);
    } finally { resumed.release(); }
    assert.equal(models, 0); assert.deepEqual(app.errors, []);
  } finally {
    release.resolve(); service.listDescendants = original; observer.unsubscribe(); descendantObserver?.unsubscribe(); session.release(); peer.release();
    await capability.dispose(); await app.dispose();
  }
});
