import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate as immediate } from 'node:timers/promises';
import { firstValueFrom, from, timeout } from 'rxjs';
import { foldGoal } from '@deepseek-ai/dsh-goal';
import { SessionId, SessionLogOffset } from '@deepseek-ai/dsh-session';
import { defineApplication, defineEnvironment, defineProgram, defineSurface, z, type HumanProgramSessionClient } from '@hanger-source/dsh-reactive';
import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { boot } from './native-support.ts';

const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
const wait = <T>(work: Promise<T>) => firstValueFrom(from(work).pipe(timeout(3_000)));
function definition() {
  return defineApplication({ name: 'session-client-control', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '控制与只读查询均不启动模型。',
    program: implementProgram(program, { Component: () => null }),
  })] });
}

for (const control of ['pause', 'cancel'] as const) for (const cancellation of ['call', 'handle'] as const) {
  test(`${control}已进入原生checkpoint后${cancellation}取消只结束回执等待，实际控制继续持久化`, { timeout: 15_000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-session-control-'));
    let models = 0;
    const app = await boot(join(directory, 'home'), definition(), async function* () {
      models++; throw new Error('SESSION_CONTROL_MUST_NOT_CALL_MODEL');
    });
    const entered = Promise.withResolvers<void>(), proceed = Promise.withResolvers<void>();
    const completed = Promise.withResolvers<void>();
    let stop = () => {};
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const peer = await app.host.open(session.id, program);
    const common: HumanProgramSessionClient<typeof program> = session;
    try {
      const agent = app.native.agents.get(SessionId(session.id))!;
      assert.ok(agent);
      if (control === 'cancel') {
        await common.submit({ requestId: 'queued', intent: 'question', delivery: 'inject', content: '保持在队列中', scope: null });
        assert.deepEqual(await common.requestStatus('queued'), { requestId: 'queued', state: 'accepted' });
      }
      let armed = true;
      stop = app.native.on('session/flush', async native => {
        if (!armed || native !== agent.session || control === 'pause' && app.native.goals.get(agent)?.phase !== 'paused') return;
        armed = false;
        entered.resolve();
        await proceed.promise;
        completed.resolve();
      });
      if (control === 'pause') app.native.goals.create(agent, { objective: '由本次控制暂停', maxGoalRounds: 1 });
      const abort = new AbortController(), reason = new Error('CALLER_STOPPED_WAITING_FOR_' + control.toUpperCase());
      const pending = common[control]({ signal: abort.signal });
      const rejected = assert.rejects(pending, cancellation === 'call'
        ? error => error === reason
        : /REACTIVE_SCOPE_DISPOSED/u);
      await entered.promise;
      if (control === 'pause') assert.equal(app.native.goals.get(agent)?.phase, 'paused');
      else assert.equal(agent.inbox.nextStep.length, 0);
      if (cancellation === 'call') abort.abort(reason);
      else session.release();
      await rejected;
      proceed.resolve();
      await completed.promise;
      assert.equal(await app.native.sessions.flush(agent.session), true);
      const stored = await app.native.sessionPersistence.open(SessionId(session.id), 'read');
      try {
        const { events } = await stored.read(SessionLogOffset(0));
        if (control === 'pause') assert.equal(foldGoal(events).goal?.phase, 'paused');
        else {
          assert.ok(events.some(event => event.type === 'agent/inbox/spliced' && event.data.outcome === 'canceled'),
            '已取消调用者等待后，原生队列取消事实仍到达真实持久日志');
          assert.deepEqual(await peer.requestStatus('queued'), { requestId: 'queued', state: 'settled' });
        }
      } finally { await stored.close(); }
      assert.equal(models, 0);
      assert.deepEqual(app.errors, []);
    } finally { proceed.resolve(); stop(); peer.release(); session.release(); await app.dispose(); }
  });
}

test('预取消不进入Goal或队列控制，服务身份可读权威请求但不能取得人工控制', { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-session-control-admission-'));
  let models = 0;
  const app = await boot(join(directory, 'home'), definition(), async function* () {
    models++; throw new Error('SESSION_CONTROL_ADMISSION_MUST_NOT_CALL_MODEL');
  });
  const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
  const service = await app.host.service().open(session.id, program);
  try {
    const common: HumanProgramSessionClient<typeof program> = session;
    const agent = app.native.agents.get(SessionId(session.id))!;
    await common.submit({ requestId: 'preserved', intent: 'question', delivery: 'inject', content: '显式取消前保留', scope: null });
    const abort = new AbortController(), reason = new Error('CONTROL_CANCELLED_BEFORE_ADMISSION');
    abort.abort(reason);
    const goal = app.native.goals.create(agent, { objective: '预取消不得暂停此Goal', maxGoalRounds: 1 });
    const paused = common.pause({ signal: abort.signal });
    app.native.goals.disarm(agent);
    await assert.rejects(paused, error => error === reason);
    await assert.rejects(common.cancel({ signal: abort.signal }), error => error === reason);
    await assert.rejects(common.requestStatus('preserved', { signal: abort.signal }), error => error === reason);
    assert.equal(app.native.goals.get(agent)?.phase, 'active');
    assert.equal(app.native.goals.get(agent)?.revision, goal.revision);
    assert.ok(agent.inbox.nextStep.some(message => 'rpcId' in message.source && message.source.rpcId === 'preserved'));
    assert.deepEqual(await service.requestStatus('preserved'), { requestId: 'preserved', state: 'accepted' });
    const untyped = service as unknown as typeof session;
    await assert.rejects(untyped.pause(), /REACTIVE_HUMAN_PARTICIPANT_REQUIRED/u);
    await assert.rejects(untyped.cancel(), /REACTIVE_HUMAN_PARTICIPANT_REQUIRED/u);
    assert.deepEqual(await common.cancel(), { canceled: true });
    assert.deepEqual(await common.requestStatus('preserved'), { requestId: 'preserved', state: 'settled' });
    await app.host.close(session.id);
    assert.equal(app.native.agents.get(SessionId(session.id)), undefined);
    assert.deepEqual(await service.requestStatus('preserved'), { requestId: 'preserved', state: 'settled' });
    assert.deepEqual(await common.requestStatus('not-submitted'), { requestId: 'not-submitted', state: 'absent' });
    assert.equal(app.native.agents.get(SessionId(session.id)), undefined, '权威冷查询不得提升为原生Agent');
    assert.equal(models, 0);
    assert.deepEqual(app.errors, []);
  } finally { service.release(); session.release(); await app.dispose(); }
});

test('权威请求查询的调用者或句柄退出结束等待，不影响同会话其他只读消费者', { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-session-status-owner-'));
  let models = 0;
  const app = await boot(join(directory, 'home'), definition(), async function* () {
    models++; throw new Error('SESSION_STATUS_MUST_NOT_CALL_MODEL');
  });
  const peer = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
  try {
    await peer.submit({ requestId: 'visible', intent: 'question', delivery: 'inject', content: '仅观察原受理事实', scope: null });
    const agent = app.native.agents.get(SessionId(peer.id))!;
    for (const cancellation of ['call', 'handle'] as const) {
      const session = await app.host.open(peer.id, program);
      const entered = Promise.withResolvers<void>(), proceed = Promise.withResolvers<void>();
      let armed = true;
      const stop = app.native.on('session/flush', async native => {
        if (!armed || native !== agent.session) return;
        armed = false; entered.resolve(); await proceed.promise;
      });
      try {
        const abort = new AbortController(), reason = new Error('REQUEST_STATUS_WAIT_CANCELLED');
        const pending = session.requestStatus('visible', { signal: abort.signal });
        const rejected = assert.rejects(pending, cancellation === 'call'
          ? error => error === reason
          : /REACTIVE_SCOPE_DISPOSED/u);
        await entered.promise;
        if (cancellation === 'call') abort.abort(reason);
        else session.release();
        await rejected;
      } finally { proceed.resolve(); stop(); session.release(); }
      assert.deepEqual(await peer.requestStatus('visible'), { requestId: 'visible', state: 'accepted' });
    }
    assert.equal(models, 0);
    assert.deepEqual(app.errors, []);
  } finally { peer.release(); await app.dispose(); }
});

test('同一输入共享受理，加入者退出不清除事务，最后回执退出后仍由原Agent完成持久化', { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-shared-input-wait-'));
  const admitting = Promise.withResolvers<void>(), admit = Promise.withResolvers<void>();
  const accepted = Promise.withResolvers<void>(), commit = Promise.withResolvers<void>();
  let admissions = 0, models = 0;
  const app = await boot(join(directory, 'home'), defineApplication({ name: 'shared-input-wait', admit: async () => {
    admissions++; admitting.resolve(); await admit.promise;
  }, environments: [defineEnvironment({ id: 'work', instructions: '受理不启动模型。',
    program: implementProgram(program, { Component: () => null }) })] }), async function* () {
    models++; throw new Error('SHARED_INPUT_WAIT_MUST_NOT_CALL_MODEL');
  });
  const first = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
  const second = await app.host.open(first.id, program), departing = await app.host.open(first.id, program);
  const third = await app.host.open(first.id, program), peer = await app.host.open(first.id, program);
  const input = { requestId: 'shared-input', intent: 'question' as const, delivery: 'inject' as const,
    content: '原始调用者与重试共享同一份资料', scope: null };
  let stop = () => {};
  try {
    const agent = app.native.agents.get(SessionId(first.id))!;
    let armed = true;
    stop = app.native.on('session/flush', async native => {
      if (!armed || native !== agent.session || !agent.inbox.nextStep.some(message =>
        message.source.kind === 'user' && 'rpcId' in message.source && message.source.rpcId === input.requestId)) return;
      armed = false; accepted.resolve(); await commit.promise;
    });
    const original = first.submit(input);
    const originalReleased = assert.rejects(original, /REACTIVE_SCOPE_DISPOSED/u);
    await wait(admitting.promise);

    const abort = new AbortController(), reason = new Error('JOINED_INPUT_WAIT_CANCELLED');
    const joined = second.submit(input, { signal: abort.signal });
    const cancelled = assert.rejects(joined, error => error === reason);
    abort.abort(reason); await wait(cancelled);
    const abandoned = departing.submit(input);
    const released = assert.rejects(abandoned, /REACTIVE_SCOPE_DISPOSED/u);
    departing.release(); await wait(released);

    const finalCaller = new AbortController(), finalReason = new Error('ACCEPTED_INPUT_RECEIPT_ABANDONED');
    const retried = third.submit(input, { signal: finalCaller.signal });
    const retryCancelled = assert.rejects(retried, error => error === finalReason);
    first.release(); await wait(originalReleased);
    await immediate();
    assert.equal(admissions, 1, '退出的加入者不能清掉共享事务，让第三次调用再次进入真实准入');
    admit.resolve(); await wait(accepted.promise);
    assert.equal(agent.inbox.nextStep.filter(message => message.source.kind === 'user'
      && 'rpcId' in message.source && message.source.rpcId === input.requestId).length, 1);
    finalCaller.abort(finalReason); await wait(retryCancelled);

    const idle = third.whenIdle(), idleReleased = assert.rejects(idle, /REACTIVE_SCOPE_DISPOSED/u);
    third.release(); await wait(idleReleased);
    const settled = peer.whenIdle();
    commit.resolve(); await wait(settled);
    await app.native.sessionPersistence.flush();
    const stored = await app.native.sessionPersistence.open(agent.id, 'read');
    try {
      const { events } = await stored.read(SessionLogOffset(0));
      assert.equal(events.flatMap(event => event.type === 'agent/inbox/spliced' ? event.data.inserted : [])
        .filter(message => message.source.kind === 'user' && 'rpcId' in message.source
          && message.source.rpcId === input.requestId).length, 1, '所有回执退出后，唯一原生入队仍完成真实持久化');
    } finally { await stored.close(); }
    assert.deepEqual(await peer.requestStatus(input.requestId), { requestId: input.requestId, state: 'accepted' });
    assert.equal(admissions, 1); assert.equal(models, 0); assert.deepEqual(app.errors, []);
  } finally {
    admit.resolve(); commit.resolve(); stop();
    first.release(); second.release(); departing.release(); third.release(); peer.release();
    await app.dispose();
  }
});

test('输入已入队后调用者取消等待，迟到的持久化失败仍由同一Agent的whenIdle报告', { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-input-commit-failure-'));
  let models = 0;
  const app = await boot(join(directory, 'home'), definition(), async function* () {
    models++; throw new Error('INPUT_COMMIT_FAILURE_MUST_NOT_CALL_MODEL');
  });
  const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
  const peer = await app.host.open(session.id, program);
  const departing = await app.host.open(session.id, program);
  const entered = Promise.withResolvers<void>(), proceed = Promise.withResolvers<void>();
  const failure = new Error('ACCEPTED_INPUT_CHECKPOINT_FAILED_LATE');
  let stop = () => {};
  try {
    const agent = app.native.agents.get(SessionId(session.id))!;
    const input = { requestId: 'late-input-checkpoint', intent: 'question' as const,
      delivery: 'inject' as const, content: '已进入原生队列的资料', scope: null };
    let armed = true;
    stop = app.native.on('session/flush', async native => {
      if (!armed || native !== agent.session || !agent.inbox.nextStep.some(message =>
        message.source.kind === 'user' && 'rpcId' in message.source && message.source.rpcId === input.requestId)) return;
      armed = false; entered.resolve(); await proceed.promise; throw failure;
    });
    const abort = new AbortController(), reason = new Error('INPUT_CALLER_LEFT_BEFORE_CHECKPOINT_FAILURE');
    const pending = session.submit(input, { signal: abort.signal });
    const cancelled = assert.rejects(pending, error => error === reason);
    await wait(entered.promise);
    abort.abort(reason); await wait(cancelled);
    const idleReleased = assert.rejects(departing.whenIdle(), /REACTIVE_SCOPE_DISPOSED/u);
    departing.release(); await wait(idleReleased);
    const failed = assert.rejects(peer.whenIdle(), error => error instanceof AggregateError && error.errors.includes(failure));
    proceed.resolve(); await wait(failed);
    assert.deepEqual(await peer.requestStatus(input.requestId), { requestId: input.requestId, state: 'accepted' });
    assert.equal(models, 0); assert.deepEqual(app.errors, []);
  } finally { proceed.resolve(); stop(); session.release(); peer.release(); departing.release(); await app.dispose(); }
});

test('Blob来源取消结束当前上传等待，迟到字节不激活冷Agent，正常上传仍保留首次字节快照', { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-blob-upload-wait-'));
  let models = 0;
  const app = await boot(join(directory, 'home'), definition(), async function* () {
    models++; throw new Error('BLOB_UPLOAD_WAIT_MUST_NOT_CALL_MODEL');
  });
  const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
  const peer = await app.host.open(session.id, program);
  const entered = Promise.withResolvers<void>(), bytes = Promise.withResolvers<ArrayBuffer>();
  class DelayedBlob extends Blob {
    override arrayBuffer(): Promise<ArrayBuffer> { entered.resolve(); return bytes.promise; }
  }
  try {
    await app.host.close(session.id);
    assert.equal(app.native.agents.get(SessionId(session.id)), undefined);
    const preCancelled = new AbortController(), preReason = new Error('UPLOAD_CANCELLED_BEFORE_CALL');
    preCancelled.abort(preReason);
    assert.throws(() => session.uploadFile(new Uint8Array([1]), { signal: preCancelled.signal }), error => error === preReason);
    const abort = new AbortController(), reason = new Error('BLOB_SOURCE_WAIT_CANCELLED');
    const pending = session.uploadFile(new DelayedBlob(['等待外部字节']), { name: 'cancelled.txt', signal: abort.signal });
    const cancelled = assert.rejects(pending.wait(), error => error === reason);
    const failed = assert.rejects(pending.result, error => error === reason);
    await wait(entered.promise);
    abort.abort(reason); await wait(cancelled); await wait(failed);
    bytes.resolve(await new Blob(['不应进入上传的迟到字节']).arrayBuffer());
    await immediate();
    assert.equal(app.native.agents.get(SessionId(session.id)), undefined, '取消后的Blob完成不得继续调用原生upload并恢复Agent');
    const activeReading = Promise.withResolvers<void>(), activeBytes = Promise.withResolvers<ArrayBuffer>();
    const retained = peer.uploadFile(new class extends Blob {
      override arrayBuffer(): Promise<ArrayBuffer> { activeReading.resolve(); return activeBytes.promise; }
    }(['单独读者等待']), { name: 'retained-source.txt' });
    const readAbort = new AbortController(), readReason = new Error('ONLY_THIS_UPLOAD_READER_LEFT');
    const readerLeft = assert.rejects(retained.wait({ signal: readAbort.signal }), error => error === readReason);
    await wait(activeReading.promise);
    readAbort.abort(readReason); await wait(readerLeft);
    activeBytes.resolve(await new Blob(['读者退出后仍继续准备的实际字节']).arrayBuffer());
    const retainedValue = await wait(retained.result), retainedPath = app.native.attachments.fileHostPath(retainedValue.file);
    assert.ok(retainedPath);
    assert.equal(await readFile(retainedPath, 'utf8'), '读者退出后仍继续准备的实际字节');
    const original = new TextEncoder().encode('首次取得的正式文件字节');
    const options = { name: 'snapshot.txt' };
    const uploading = peer.uploadFile(original, options);
    original.fill(0);
    options.name = 'changed-after-call.txt';
    const file = await wait(uploading.result);
    assert.equal(file.file.name, 'snapshot.txt');
    const path = app.native.attachments.fileHostPath(file.file);
    assert.ok(path);
    assert.equal(await readFile(path, 'utf8'), '首次取得的正式文件字节');
    const agent = app.native.agents.get(SessionId(session.id))!;
    assert.ok(agent); assert.ok(app.native.fileUploads.resolve(agent, file.receiptId));
    assert.equal(models, 0); assert.deepEqual(app.errors, []);
  } finally { bytes.resolve(new ArrayBuffer(0)); session.release(); peer.release(); await app.dispose(); }
});

for (const cancellation of ['call', 'handle', 'activity'] as const) {
  test(`真实文件存储开始后${cancellation}退出只结束等待，同次调用仍取得原生receipt`, { timeout: 15_000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-upload-result-'));
    let models = 0, stores = 0;
    const app = await boot(join(directory, 'home'), definition(), async function* () {
      models++; throw new Error('UPLOAD_RESULT_MUST_NOT_CALL_MODEL');
    });
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const entered = Promise.withResolvers<void>(), proceed = Promise.withResolvers<void>();
    const attachments = app.native.attachments, admit = attachments.admitEncodedFile;
    let stop = () => {};
    attachments.admitEncodedFile = async input => {
      stores++;
      const file = await admit.call(attachments, input);
      entered.resolve(); await proceed.promise;
      return file;
    };
    try {
      const agent = app.native.agents.get(SessionId(session.id))!;
      const abort = new AbortController(), reason = new Error('UPLOAD_CALLER_LEFT');
      const invocation = session.uploadFile(new TextEncoder().encode('迟到但仍属于同次调用的原始资料'),
        { name: 'late-receipt.txt', signal: abort.signal });
      if (cancellation === 'activity') {
        const subscription = invocation.activity$.subscribe(value => {
          if (value.state === 'running') abort.abort(reason);
        });
        stop = () => subscription.unsubscribe();
      }
      const cancelled = assert.rejects(invocation.wait(), cancellation === 'handle'
        ? /REACTIVE_SCOPE_DISPOSED/u : error => error === reason);
      await wait(entered.promise);
      if (cancellation === 'call') abort.abort(reason);
      else if (cancellation === 'handle') session.release();
      await wait(cancelled);
      proceed.resolve();
      const value = await wait(invocation.result);
      assert.deepEqual(app.native.fileUploads.resolve(agent, value.receiptId), value.file);
      const path = app.native.attachments.fileHostPath(value.file);
      assert.ok(path);
      assert.equal(await readFile(path, 'utf8'), '迟到但仍属于同次调用的原始资料');
      assert.deepEqual(invocation.source, { kind: 'file-upload', sessionId: session.id });
      assert.equal(invocation.activity.peek().status, 'ready');
      assert.deepEqual(await invocation.activity.read(), { state: 'returned' });
      assert.equal(stores, 1); assert.equal(models, 0); assert.deepEqual(app.errors, []);
    } finally {
      proceed.resolve(); stop(); attachments.admitEncodedFile = admit;
      session.release(); await app.dispose();
    }
  });
}

test('上传退等后的真实存储异常归同次result和failed事实，不变成Host清理失败', { timeout: 15_000 }, async () => {
  const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-upload-late-failure-'));
  let models = 0, stores = 0;
  const app = await boot(join(directory, 'home'), definition(), async function* () {
    models++; throw new Error('UPLOAD_FAILURE_MUST_NOT_CALL_MODEL');
  });
  const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
  const entered = Promise.withResolvers<void>(), proceed = Promise.withResolvers<void>();
  const failure = new Error('ATTACHMENT_STORAGE_FAILED_AFTER_CALLER_LEFT');
  const attachments = app.native.attachments, admit = attachments.admitEncodedFile;
  attachments.admitEncodedFile = async input => {
    stores++;
    await admit.call(attachments, input);
    entered.resolve(); await proceed.promise;
    throw failure;
  };
  try {
    const abort = new AbortController(), reason = new Error('STOP_WAITING_FOR_FILE');
    const invocation = session.uploadFile(new TextEncoder().encode('真实存储错误的资料'), { signal: abort.signal });
    const cancelled = assert.rejects(invocation.wait(), error => error === reason);
    let actual: unknown;
    const failed = assert.rejects(invocation.result, error => {
      actual = error;
      return error instanceof Error && error.cause === failure;
    });
    await wait(entered.promise);
    abort.abort(reason); await wait(cancelled);
    session.release();
    const disposing = app.dispose();
    proceed.resolve(); await wait(failed); await wait(disposing);
    assert.deepEqual(await invocation.activity.read(), { state: 'failed', phase: 'execute', error: actual });
    assert.equal(stores, 1); assert.equal(models, 0); assert.deepEqual(app.errors, []);
  } finally {
    proceed.resolve(); attachments.admitEncodedFile = admit;
    session.release(); await app.dispose();
  }
});

for (const owner of ['session', 'host'] as const) {
  test(`${owner}关闭取消Blob准备并等真实存储签发receipt后才释放Agent`, { timeout: 15_000 }, async () => {
    const directory = await mkdtemp(join(tmpdir(), 'dsh-reactive-upload-drain-'));
    let models = 0, stores = 0;
    const app = await boot(join(directory, 'home'), definition(), async function* () {
      models++; throw new Error('UPLOAD_DRAIN_MUST_NOT_CALL_MODEL');
    });
    const session = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const entered = Promise.withResolvers<void>(), proceed = Promise.withResolvers<void>();
    const reading = Promise.withResolvers<void>(), bytes = Promise.withResolvers<ArrayBuffer>();
    class DelayedBlob extends Blob {
      override arrayBuffer(): Promise<ArrayBuffer> { reading.resolve(); return bytes.promise; }
    }
    const attachments = app.native.attachments, admit = attachments.admitEncodedFile;
    attachments.admitEncodedFile = async input => {
      stores++;
      const file = await admit.call(attachments, input);
      entered.resolve(); await proceed.promise;
      return file;
    };
    try {
      const agents = app.native.agents, agent = agents.get(SessionId(session.id))!;
      const preparing = session.uploadFile(new DelayedBlob(['不能分派的迟到来源']), { name: 'preparing.txt' });
      const preparationFailed = assert.rejects(preparing.result, /REACTIVE_(?:FILE_UPLOAD_CLOSING|HOST_DISPOSED)/u);
      await wait(reading.promise);
      const writing = session.uploadFile(new TextEncoder().encode('关闭前已开始的实际文件'), { name: 'drained.txt' });
      const receipted = writing.result.then(value => {
        assert.deepEqual(app.native.fileUploads.resolve(agent, value.receiptId), value.file);
        return { value, path: attachments.fileHostPath(value.file) };
      });
      await wait(entered.promise);
      let closed = false;
      const closing = (owner === 'session' ? app.host.close(session.id) : app.dispose()).then(() => { closed = true; });
      await wait(preparationFailed); await immediate();
      assert.equal(closed, false);
      assert.equal(agents.get(SessionId(session.id)), agent);
      assert.throws(() => session.uploadFile(new Uint8Array([1])), /REACTIVE_(?:CONVERSATION_CLOSING|HOST_DISPOSED)/u);
      bytes.resolve(await new Blob(['关闭后才取得的字节']).arrayBuffer());
      proceed.resolve();
      const { value, path } = await wait(receipted);
      assert.ok(value.receiptId); assert.ok(path);
      await wait(closing);
      assert.equal(agents.get(SessionId(session.id)), undefined);
      assert.equal(await readFile(path, 'utf8'), '关闭前已开始的实际文件');
      assert.deepEqual(await writing.activity.read(), { state: 'returned' });
      assert.equal(stores, 1); assert.equal(models, 0); assert.deepEqual(app.errors, []);
    } finally {
      bytes.resolve(new ArrayBuffer(0)); proceed.resolve(); attachments.admitEncodedFile = admit;
      session.release(); await app.dispose();
    }
  });
}
