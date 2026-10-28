import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate as immediate } from 'node:timers/promises';
import { filter, firstValueFrom, timeout } from 'rxjs';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import ScheduleService, { ScheduleId, scheduleDomain } from '@deepseek-ai/dsh-schedule';
import { SessionId } from '@deepseek-ai/dsh-session';
import { defineApplication, defineEnvironment, defineProgram, defineSurface, mapResource, resourceView, z } from '@hanger-source/dsh-reactive';
import { Expose, implementProgram, useResource } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel, requireActivePlugin } from '@hanger-source/dsh-reactive-dsh';
import { createNativeSchedule } from '@hanger-source/dsh-reactive-dsh/schedule';
import { mountProgramGateway, RemoteAuthenticationError } from '@hanger-source/dsh-reactive-remote';

const first = (source, predicate) => firstValueFrom(source.pipe(filter(predicate), timeout(8_000)));

test('Schedule同步目标沿真实CAS读取，退出等待保留原生结果，迟到写入失败不污染能力释放', { timeout: 25_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-schedule-resources-'));
  const directory = join(root, 'workspace'); await mkdir(directory);
  const capability = createNativeSchedule({ service: { kind: 'install' } });
  const surface = defineSurface({ actions: {}, views: { reminders: resourceView(z.array(z.object({ id: z.string(), title: z.string(), status: z.string() }))) } });
  const program = defineProgram({ name: 'schedule_resources_test', input: z.null(), surface });
  let models = 0;
  const errors = [], subscriptions = [], entered = Promise.withResolvers(), release = Promise.withResolvers();
  const definition = defineApplication({ name: 'schedule-resources', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '只观察和管理提醒，不启动模型', program: implementProgram(program, { Component({ conversation }) {
      const observed = useResource(capability.forSession(conversation).state);
      return Expose({ surface, value: { views: { reminders: observed.observations$.pipe(mapResource(rows => rows.map(({ id, title, status }) => ({ id, title, status })))) } } });
    } }),
  })] });
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'schedule-owner', onError: event => errors.push(event.error),
    model: nativeModel({ provider: 'schedule-resources', model: 'unused', create: () => new class extends LlmAdapter {
      async *stream() { models++; throw new Error('SCHEDULE_READING_MUST_NOT_START_MODEL'); }
    }() }),
  });
  let gateway, session, foreign, sdk;
  try {
    // 正式Controller/Gateway提供Schedule公开依赖；不构造同名空服务或调用私有安装接点。
    gateway = await mountProgramGateway(runtime.native, { application: runtime.application,
      model: { provider: 'schedule-resources', model: 'unused' }, participant: principal => ({ participant: 'human', actorId: principal.id }),
      access: () => {}, carrier: { authenticate: async () => { throw new RemoteAuthenticationError(401); } },
    });
    await capability.install(runtime.native, join(root, 'home'));
    assert.ok(runtime.native.schedule instanceof ScheduleService);
    const workspace = await runtime.application.createWorkspace({ directory });
    session = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    foreign = await runtime.application.create('work', { workspaceId: workspace.workspaceId });
    sdk = capability.forSession(session);
    assert.equal(sdk.state.peek().status, 'pending', '构造SDK不读取');
    const catalog = sdk.catalog.bind(sdk), failure = new Error('SCHEDULE_CATALOG_READ_FAILED');
    let held = false, failNext = false, calls = 0;
    sdk.catalog = async options => {
      const rows = await catalog(options); calls++;
      if (held) { held = false; entered.resolve({ rows, signal: options.signal }); await release.promise; }
      if (failNext) { failNext = false; throw failure; }
      return rows;
    };
    const states = [];
    subscriptions.push(sdk.state.observations$.subscribe(value => states.push(value)));
    await sdk.state.read(t.signal);
    // 测试准备只经正式原生create写入一个小时后的记录；框架管理API并未获得创建/投递授权。
    const original = await runtime.native.schedule.create(SessionId(session.id), { title: '原提醒', prompt: '未来提醒，不在本次试验执行', after_seconds: 3600 });
    await first(sdk.state.observations$, value => value.status === 'ready' && value.value.some(row => row.id === original.id));
    const task = sdk.task(original.id), before = calls;
    assert.equal(sdk.task(original.id), task);
    subscriptions.push(task.state.observations$.subscribe());
    await task.state.read(t.signal);
    assert.equal(calls, before, '绑定不读取，任务资源复用会话观察而不再查询catalog');
    const foreignTask = capability.forSession(foreign).task(original.id);
    assert.equal(await foreignTask.read(), undefined);
    assert.equal(await foreignTask.state.read(t.signal), undefined);
    assert.equal((await foreignTask.update({ expected: original, title: '不得修改其他会话提醒' }).result).code, 'schedule_not_found');
    assert.equal((await foreignTask.history({ limit: 10 })).code, 'schedule_not_found');
    const absent = sdk.task(ScheduleId('missing-reminder'));
    assert.equal(sdk.task(absent.id), absent);
    assert.equal(await absent.read(), undefined);
    assert.equal(await absent.state.read(t.signal), undefined);
    assert.deepEqual(await absent.delete().result, { id: absent.id, deleted: false, code: 'schedule_not_found' });
    assert.throws(() => absent.update({ expected: original, title: '不得改变固定目标' }), /REACTIVE_NATIVE_SCHEDULE_EXPECTED_MISMATCH/u);

    held = true;
    const changed = await task.update({ expected: original, title: '中间提醒' }).result; assert.equal(changed.updated, true);
    const old = await entered.promise;
    assert.equal(sdk.state.peek().status, 'pending'); assert.equal(task.state.peek().status, 'pending');
    const newer = first(task.state.observations$, value => value.status === 'ready' && value.value?.title === '当前提醒');
    const current = await task.update({ expected: changed.record, title: '当前提醒' }).result; assert.equal(current.updated, true);
    await newer; assert.equal(old.signal.aborted, true);
    release.resolve(); await immediate();
    assert.equal(task.state.peek().value.title, '当前提醒');
    assert.equal((await session.readView('reminders')).find(row => row.id === task.id).title, '当前提醒');

    failNext = true;
    const failed = first(task.state.observations$, value => value.status === 'error');
    const updated = await task.update({ expected: current.record, title: '恢复目标' }).result; assert.equal(updated.updated, true);
    assert.equal((await failed).error, failure);
    const domain = runtime.native.storageDomain.get(scheduleDomain.name); assert.ok(domain);
    const table = domain.table('tasks'), nativePut = table.put, nativeDelete = table.delete;
    const updateAtWrite = Promise.withResolvers(), deleteAtWrite = Promise.withResolvers();
    const releaseUpdate = Promise.withResolvers(), releaseDelete = Promise.withResolvers();
    let putCalls = 0, deleteCalls = 0, putReceipt, deleteReceipt, updating, deleting, writer, writerHandle;
    // 同一正式KV表完成真实落盘，只延后交回Schedule事务的put/delete回执。
    table.put = function (...args) {
      putReceipt = (async () => {
        const result = await nativePut.apply(this, args); putCalls++;
        updateAtWrite.resolve(); await releaseUpdate.promise; return result;
      })();
      return putReceipt;
    };
    table.delete = function (...args) {
      deleteReceipt = (async () => {
        const result = await nativeDelete.apply(this, args); deleteCalls++;
        deleteAtWrite.resolve(); await releaseDelete.promise; return result;
      })();
      return deleteReceipt;
    };
    try {
      const preCancelled = new AbortController(), preCancelledReason = new Error('SCHEDULE_MUTATION_CANCELLED_BEFORE_WRITE');
      preCancelled.abort(preCancelledReason);
      assert.throws(() => task.update({ expected: updated.record, title: '不得写入的修订' }, { signal: preCancelled.signal }),
        error => error === preCancelledReason);
      assert.throws(() => task.delete({ signal: preCancelled.signal }), error => error === preCancelledReason);
      assert.equal(putCalls, 0); assert.equal(deleteCalls, 0);

      const recovered = first(task.state.observations$, value => value.status === 'ready' && value.value?.title === '已恢复');
      const cancellation = new AbortController(), reason = new Error('CANCEL_SCHEDULE_UPDATE_RECEIPT');
      let updateOutcome;
      const updateCall = task.update({ expected: updated.record, title: '已恢复' }, { signal: cancellation.signal });
      updating = updateCall.wait().then(value => { updateOutcome = value; }, error => { updateOutcome = error; });
      await Promise.race([updateAtWrite.promise, updating.then(() => { throw new Error('SCHEDULE_UPDATE_DID_NOT_REACH_WRITE'); })]);
      cancellation.abort(reason); await immediate();
      assert.equal(updateOutcome, reason, '真实put尚未交回时，调用取消已经结束更新回执等待');
      releaseUpdate.resolve(); await recovered; await updating;
      const updateResult = await updateCall.result;
      assert.equal(updateResult.updated, true); assert.equal(updateResult.record.title, '已恢复');
      assert.equal((await first(updateCall.activity$, value => value.state === 'returned')).state, 'returned');
      assert.equal(table.get(original.id).record.title, '已恢复');
      assert.equal(putCalls, 1, '取消更新等待不会重新执行已受理写入');
      assert.ok(states.some(value => value.status === 'error' && value.error === failure));
      assert.equal((await task.history({ limit: 10 })).records.length, 0, '观察不会生成投递回执');
      await runtime.application.close(session.id);
      assert.equal(runtime.native.agents.get(SessionId(session.id)), undefined);
      assert.equal((await task.read()).title, '已恢复');
      assert.equal((await task.state.read(t.signal)).title, '已恢复');

      writerHandle = await runtime.application.open(session.id, program);
      writer = capability.forSession(writerHandle);
      const ended = first(task.state.observations$, value => value.status === 'ready' && value.value === undefined);
      let deleteOutcome;
      const deleteCall = writer.task(original.id).delete();
      deleting = deleteCall.wait().then(value => { deleteOutcome = value; }, error => { deleteOutcome = error; });
      await Promise.race([deleteAtWrite.promise, deleting.then(() => { throw new Error('SCHEDULE_DELETE_DID_NOT_REACH_WRITE'); })]);
      writer.dispose(); await immediate();
      assert.ok(deleteOutcome instanceof Error); assert.match(deleteOutcome.message, /REACTIVE_SCOPE_DISPOSED/u,
        '独立SDK退出不等待原生delete回执');
      releaseDelete.resolve(); await ended; await deleting;
      assert.deepEqual(await deleteCall.result, { id: original.id, deleted: true });
      assert.equal((await first(deleteCall.activity$, value => value.state === 'returned')).state, 'returned');
      assert.equal(table.get(original.id), undefined);
      assert.equal(putCalls, 1); assert.equal(deleteCalls, 1, 'SDK退出不会重新删除或补写已移除记录');
      assert.equal(sdk.task(original.id), task);
      assert.equal(await task.read(), undefined);
    } finally {
      releaseUpdate.resolve(); releaseDelete.resolve();
      await Promise.allSettled([putReceipt, deleteReceipt, updating, deleting]);
      table.put = nativePut; table.delete = nativeDelete;
      writer?.dispose(); writerHandle?.release();
    }
    assert.equal((await sdk.active()).length, 0);
    subscriptions.forEach(subscription => subscription.unsubscribe());
    assert.equal(sdk.state.peek().status, 'pending');
    assert.equal(task.state.peek().status, 'pending');
    await immediate();
    const nativeList = ScheduleService.prototype.list;
    for (const mode of ['caller', 'sdk']) {
      const read = Promise.withResolvers(), finish = Promise.withResolvers();
      ScheduleService.prototype.list = async function (...args) {
        const rows = await nativeList.apply(this, args);
        read.resolve(); await finish.promise; return rows;
      };
      let waiting;
      try {
        const cancellation = new AbortController(), reason = new Error('CANCEL_ONLY_THIS_SCHEDULE_READ');
        let outcome;
        waiting = sdk.active({ signal: cancellation.signal }).then(value => { outcome = value; }, error => { outcome = error; });
        await read.promise;
        if (mode === 'caller') cancellation.abort(reason); else sdk.dispose();
        await immediate();
        if (mode === 'caller') assert.equal(outcome, reason, '调用者取消不等原生list回执');
        else { assert.ok(outcome instanceof Error); assert.match(outcome.message, /REACTIVE_SCOPE_DISPOSED/); }
      } finally { finish.resolve(); await waiting; ScheduleService.prototype.list = nativeList; }
    }
    assert.throws(() => task.state.peek(), /REACTIVE_SCOPE_DISPOSED/);

    const lateRecord = await runtime.native.schedule.create(SessionId(session.id), {
      title: '迟到结算的原提醒', prompt: '只验证实际写入与调用结算，不执行提醒', after_seconds: 3600,
    });
    const lateSdk = capability.forSession(session), lateTask = lateSdk.task(lateRecord.id);
    const lateUpdateAtWrite = Promise.withResolvers(), lateDeleteAtWrite = Promise.withResolvers();
    const releaseLateUpdate = Promise.withResolvers(), releaseLateDelete = Promise.withResolvers();
    const lateUpdateFailure = new Error('SCHEDULE_PUT_RECEIPT_FAILED_AFTER_COMMIT');
    const lateDeleteFailure = new Error('SCHEDULE_DELETE_RECEIPT_FAILED_AFTER_COMMIT');
    let latePutCalls = 0, lateDeleteCalls = 0, latePutReceipt, lateDeleteReceipt, lateUpdateCall, lateDeleteCall, disposing;
    table.put = function (...args) {
      latePutReceipt = (async () => {
        await nativePut.apply(this, args); latePutCalls++;
        lateUpdateAtWrite.resolve(); await releaseLateUpdate.promise; throw lateUpdateFailure;
      })();
      return latePutReceipt;
    };
    table.delete = function (...args) {
      lateDeleteReceipt = (async () => {
        await nativeDelete.apply(this, args); lateDeleteCalls++;
        lateDeleteAtWrite.resolve(); await releaseLateDelete.promise; throw lateDeleteFailure;
      })();
      return lateDeleteReceipt;
    };
    try {
      const updateCancellation = new AbortController(), updateReason = new Error('LEAVE_LATE_SCHEDULE_UPDATE_WAIT');
      lateUpdateCall = lateTask.update({ expected: lateRecord, title: '已经持久化但回执失败' });
      const updateWait = assert.rejects(lateUpdateCall.wait({ signal: updateCancellation.signal }), error => error === updateReason);
      await lateUpdateAtWrite.promise;
      updateCancellation.abort(updateReason); await updateWait;
      assert.equal(table.get(lateRecord.id).record.title, '已经持久化但回执失败');
      releaseLateUpdate.resolve();
      await assert.rejects(lateUpdateCall.result, error => error === lateUpdateFailure);
      assert.equal(table.get(lateRecord.id).record.title, '已经持久化但回执失败', '原回执失败不抹掉实际持久化事实');
      const updateActivity = await first(lateUpdateCall.activity$, value => value.state === 'failed');
      assert.equal(updateActivity.error, lateUpdateFailure); assert.equal(updateActivity.phase, 'execute');
      assert.equal(latePutCalls, 1, '退出等待和失败结算均不重发已落盘put');

      const deleteCancellation = new AbortController(), deleteReason = new Error('LEAVE_LATE_SCHEDULE_DELETE_WAIT');
      lateDeleteCall = lateTask.delete();
      const deleteWait = assert.rejects(lateDeleteCall.wait({ signal: deleteCancellation.signal }), error => error === deleteReason);
      await lateDeleteAtWrite.promise;
      deleteCancellation.abort(deleteReason); await deleteWait;
      assert.equal(table.get(lateRecord.id), undefined, '正式KV删除已落盘，迟到异常不恢复原记录');
      let disposed = false;
      disposing = capability.dispose().then(() => { disposed = true; });
      await immediate();
      assert.equal(disposed, false, '能力退出等待尚未交回的真实delete写入');
      assert.equal((await lateDeleteCall.activity.read(t.signal)).state, 'running', '能力退出期间仍可观察同一次在途调用');
      releaseLateDelete.resolve();
      await assert.rejects(lateDeleteCall.result, error => error === lateDeleteFailure);
      const deleteActivity = await first(lateDeleteCall.activity$, value => value.state === 'failed');
      assert.equal(deleteActivity.error, lateDeleteFailure); assert.equal(deleteActivity.phase, 'execute');
      await disposing;
      assert.equal(disposed, true, '原生写入拒绝是调用事实，能力清理仍正常完成');
      assert.equal((await lateDeleteCall.activity.read(t.signal)).error, lateDeleteFailure, '退出完成后保留有限的原调用终态');
      assert.equal(latePutCalls, 1); assert.equal(lateDeleteCalls, 1, '迟到失败不自动重发或补写已删除提醒');
    } finally {
      releaseLateUpdate.resolve(); releaseLateDelete.resolve();
      await Promise.allSettled([latePutReceipt, lateDeleteReceipt, lateUpdateCall?.result, lateDeleteCall?.result, disposing]);
      table.put = nativePut; table.delete = nativeDelete;
      lateSdk.dispose();
    }
    assert.equal(models, 0); assert.deepEqual(errors, []);
  } finally {
    release.resolve(); subscriptions.forEach(subscription => subscription.unsubscribe());
    sdk?.dispose(); foreign?.release(); session?.release(); await capability.dispose(); await gateway?.close(); await runtime.dispose();
  }
});
