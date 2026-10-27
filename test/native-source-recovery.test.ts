import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate as immediate } from 'node:timers/promises';
import { ReplaySubject, filter, firstValueFrom, timeout, type Subscription } from 'rxjs';
import type { SessionEvent } from '@deepseek-ai/dsh-session';
import type { SessionInspection } from '@deepseek-ai/dsh-session-persistence';
import { defineApplication, defineEnvironment, defineProgram, defineSurface, z } from '@hanger-source/dsh-reactive';
import { implementProgram } from '@hanger-source/dsh-reactive/program';
import type { ConversationHandle } from '@hanger-source/dsh-reactive-dsh';
import { nativeSessionAccess, nativeSessionEvents, nativeSessionInspections } from '../packages/dsh/src/conversations/session-access.ts';
import { boot, text } from './native-support.ts';

test('实际原生owner关闭并恢复时取消旧切面，长期观察接上新历史且不重复日志或启动模型', async () => {
  const home = await mkdtemp(join(tmpdir(), 'dsh-reactive-native-owner-recovery-'));
  const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
  const definition = defineApplication({ name: 'native-owner-recovery', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '原生观察不启动模型', program: implementProgram(program, { Component: () => null }),
  })] });
  let modelCalls = 0;
  const app = await boot(home, definition, async function* () { modelCalls++; yield* text('没有模型工作'); });
  const entered = Promise.withResolvers<{ signal: AbortSignal; snapshot: SessionInspection }>();
  const release = Promise.withResolvers<void>();
  const snapshots = new ReplaySubject<SessionInspection>(1), events = new ReplaySubject<SessionEvent>(1);
  const deliveredSnapshots: SessionInspection[] = [], deliveredEvents: SessionEvent[] = [], errors: unknown[] = [];
  let session: ConversationHandle<typeof program> | undefined, resumed: ConversationHandle<typeof program> | undefined;
  let snapshotSubscription: Subscription | undefined, eventSubscription: Subscription | undefined;
  try {
    session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const access = nativeSessionAccess(session), originalAgent = access.liveAgent();
    let first = true;
    const delayed = { ...access, async inspect(signal?: AbortSignal) {
      const snapshot = await access.inspect(signal);
      if (first) {
        first = false; assert.ok(signal);
        // 延后已经取得的真实切面，模拟忽略取消的迟到交付；不另造Agent、历史或持久状态。
        entered.resolve({ signal, snapshot }); await release.promise;
      }
      return snapshot;
    } };
    snapshotSubscription = nativeSessionInspections(delayed, () => false).subscribe({
      next: snapshot => { deliveredSnapshots.push(snapshot); snapshots.next(snapshot); },
      error: error => { errors.push(error); snapshots.error(error); },
    });
    const old = await entered.promise;
    // 原生日志读取也在真正的owner切换期间进行，而非预先完成全部历史再关闭。
    eventSubscription = nativeSessionEvents(access).subscribe({
      next: event => { deliveredEvents.push(event); events.next(event); },
      error: error => { errors.push(error); events.error(error); },
    });
    await app.host.close(session.id);
    assert.equal(old.signal.aborted, true);
    assert.equal(access.liveAgent(), undefined);
    resumed = await app.host.resume(session.id, program);
    const currentAgent = access.liveAgent()!;
    assert.notEqual(currentAgent, originalAgent);
    const expected = currentAgent.session.snapshotEvents().map(event => event.seq);
    const last = expected.at(-1)!;
    await firstValueFrom(snapshots.pipe(filter(snapshot => snapshot.events.at(-1)?.seq === last), timeout(8_000)));
    await firstValueFrom(events.pipe(filter(event => event.seq === last), timeout(8_000)));
    release.resolve(); await immediate();
    assert.equal(deliveredSnapshots.includes(old.snapshot), false, '旧读取迟到不能回写当前owner的现场');
    assert.equal(deliveredSnapshots.at(-1)!.events.at(-1)!.seq, last);
    assert.deepEqual(deliveredEvents.map(event => event.seq), expected, '完整历史和恢复通知只按原始日志顺序交付一次');
    assert.equal(modelCalls, 0);
    assert.deepEqual(errors, []); assert.deepEqual(app.errors, []);
  } finally {
    release.resolve(); snapshotSubscription?.unsubscribe(); eventSubscription?.unsubscribe();
    snapshots.complete(); events.complete(); resumed?.release(); session?.release(); await app.dispose();
  }
});
