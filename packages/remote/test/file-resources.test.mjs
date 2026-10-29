import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, mkdir, rename, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { setImmediate as immediate, setTimeout as delay } from 'node:timers/promises';
import { symbols } from '@deepseek-ai/cordis';
import Registry from '@deepseek-ai/dsh-typert-registry';
import WorkspaceFiles from '@deepseek-ai/dsh-api-workspace-files';
import SandboxPolicy from '@deepseek-ai/dsh-sandbox-policy';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { SessionId } from '@deepseek-ai/dsh-session';
import { WorkspaceId } from '@deepseek-ai/dsh-workspace';
import { firstValueFrom, filter, from, timeout } from 'rxjs';
import WebSocket from 'ws';
import { defineApplication, defineEnvironment, defineProgram, defineSurface, programIdentity,
  ConversationAccessRevokedError, SessionFileOutsideWorkspaceError, z } from '@hanger-source/dsh-reactive';
import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel, requireActivePlugin } from '@hanger-source/dsh-reactive-dsh';
import { createNativeFiles } from '@hanger-source/dsh-reactive-dsh/files';
import { createProgramClient } from '@hanger-source/dsh-reactive-client';
import { mountProgramGateway, RemoteAuthenticationError } from '@hanger-source/dsh-reactive-remote';

test('Session Files直接经过真实HTTP与watch：范围、权限、重连、成员资格和两端清理保各自owner', { timeout: 35_000 }, async t => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-file-resources-'));
  const directory = join(root, 'workspace'); await mkdir(directory);
  const path = join(directory, 'artifact.txt'); await writeFile(path, 'title\nv1\n');
  await writeFile(join(root, 'outside.txt'), '不属于工作区');
  const capability = createNativeFiles({ service: 'provided' });
  const program = defineProgram({ name: 'file_resources_test', input: z.null(), surface: defineSurface({ actions: {}, views: {} }) });
  let programFiles;
  const definition = defineApplication({ name: 'file-resources', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '文件观察不启动模型', program: implementProgram(program, {
      Component({ conversation }) { programFiles = conversation.files; return null; },
    }),
  })] });
  let models = 0, flushes = 0, reads = 0, delayed = false, watchGate, closeGate, bytesGate;
  const errors = [], watches = [], sockets = [], subscriptions = [], gates = [];
  const gate = () => {
    const value = { entered: Promise.withResolvers(), release: Promise.withResolvers() };
    gates.push(value); return value;
  };
  const delayedRead = gate();
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'file-owner', files: capability,
    onError: event => errors.push(event.error), async installNative(ctx, home) {
      await requireActivePlugin(ctx.plugin(Registry));
      await requireActivePlugin(ctx.plugin(SandboxPolicy, { mode: 'read-only', workspaceRoot: directory }));
      await requireActivePlugin(ctx.plugin(WorkspaceFiles, { maxLines: 8, maxBytes: 4096, maxFileBytes: 4096, maxEntries: 16 }));
      await capability.install(ctx, home);
    }, model: nativeModel({ provider: 'file-resources', model: 'unused', create: () => new class extends LlmAdapter {
      async *stream() { models++; yield { type: 'finish', reason: { kind: 'stop' } }; }
    }() }),
  });
  let gateway, client, deniedClient, local, remote, denied, files, access;
  try {
    const visible = new Set();
    const actions = new Set(['reactivePrograms/open', 'reactivePrograms/fileRead', 'reactivePrograms/fileReadBytes',
      'reactivePrograms/fileStat', 'reactivePrograms/fileList', 'reactivePrograms/fileChanges']);
    const registry = runtime.native.typert[symbols.original];
    gateway = await mountProgramGateway(runtime.native, { application: runtime.application, registry: 'provided',
      model: { provider: 'file-resources', model: 'unused' }, participant: principal => ({ participant: 'human', actorId: principal.id }),
      access(service) {
        access = service;
        service.registerPolicy('files', { bindings: principal => [...visible].map(id => ({
          role: { name: 'file-artifacts', resource: 'session',
            actions: principal.id === 'file-reader' ? [...actions] : ['reactivePrograms/open'] },
          resource: { kind: 'session', id }, condition: ({ args }) => args?.request?.path === 'blocked.txt'
            ? { kind: 'deny', reason: 'private-artifact' } : { kind: 'allow' },
        })) });
      }, carrier: { async authenticate(request) {
        const id = request.headers.get('authorization')?.slice('Bearer '.length);
        if (id !== 'file-reader' && id !== 'file-denied') throw new RemoteAuthenticationError(401);
        return { kind: 'files', id };
      }, onError: error => errors.push(error) },
    });
    assert.equal(runtime.native.typert[symbols.original], registry, 'Gateway借同一Registry，不替换已安装文件服务的lookups');
    const transport = (actor = 'file-reader') => ({ baseUrl: gateway.url,
      fetch: (url, init) => fetch(url, { ...init,
        headers: { ...Object.fromEntries(new Headers(init?.headers)), authorization: 'Bearer ' + actor } }),
      createWebSocket(url) {
        const socket = new WebSocket(url, { headers: { authorization: 'Bearer ' + actor } }), send = socket.send.bind(socket);
        socket.send = (data, ...args) => {
          const frame = JSON.parse(data);
          if (frame.type === 'open' && frame.endpoint === 'reactivePrograms/fileChanges') socket.filePath = frame.payload.args.request.path;
          return send(data, ...args);
        };
        socket.on('message', data => {
          if (socket.filePath) t.diagnostic('files.stream ' + JSON.stringify({ path: socket.filePath, frame: JSON.parse(data.toString()) }));
        });
        sockets.push(socket); return socket;
      },
    });
    const rpc = async (method, request, actor = 'file-reader') => {
      const response = await fetch(new URL('api/reactivePrograms/' + method, gateway.url), { method: 'POST',
        headers: { authorization: 'Bearer ' + actor, 'content-type': 'application/json' },
        body: JSON.stringify({ type: 'client-request', method: 'reactivePrograms/' + method,
          rpcId: crypto.randomUUID(), payload: { args: { request } } }) });
      return (await response.json()).result;
    };
    const fs = runtime.native.fs[symbols.original], native = runtime.native.workspaceFiles[symbols.original];
    const watch = fs.watch, read = native.read, readBytes = native.readBytes, sessions = runtime.native.sessions[symbols.original], flush = sessions.flush;
    sessions.flush = function (...args) { flushes++; return flush.apply(this, args); };
    fs.watch = async function (target, notify, signal) {
      const absolutePath = this.processPath(target);
      const unwatch = await watch.call(this, target, error => {
        t.diagnostic('files.fs.invalidated ' + JSON.stringify({ path: absolutePath, error: error?.message }));
        notify(error);
      }, signal);
      const current = { path: this.processPath(target), closed: Promise.withResolvers(), active: true };
      watches.push(current);
      t.diagnostic('files.fs.ready ' + absolutePath);
      const opening = watchGate; watchGate = undefined;
      if (opening) { opening.entered.resolve(current); await opening.release.promise; }
      return async () => {
        await unwatch();
        const closing = closeGate; closeGate = undefined;
        if (closing) { closing.entered.resolve(current); await closing.release.promise; }
        current.active = false; current.closed.resolve();
      };
    };
    native.read = async function (scope, target, range, signal) {
      assert.ok(watches.some(watch => watch.active && watch.path === dirname(resolve(scope.workspaceRoot, target))), '目标所在目录watch ready前不能读取文件切面');
      let result;
      try { result = await read.call(this, scope, target, range, signal); reads++; }
      catch (error) {
        t.diagnostic('files.read.rejected ' + JSON.stringify({ path: target, range, code: error.code, message: error.message, aborted: signal.aborted }));
        throw error;
      }
      t.diagnostic('files.read.fulfilled ' + JSON.stringify({ path: target, range, text: result.text, aborted: signal.aborted }));
      if (delayed && range.offset === 2) {
        delayed = false; delayedRead.entered.resolve({ result, signal }); await delayedRead.release.promise;
      }
      return result;
    };
    native.readBytes = async function (...args) {
      const result = await readBytes.apply(this, args);
      const reading = bytesGate; bytesGate = undefined;
      if (reading) { reading.entered.resolve({ result, signal: args.at(-1) }); await reading.release.promise; }
      return result;
    };
    const workspace = await runtime.application.createWorkspace({ directory });
    local = await runtime.application.create('work', { workspaceId: workspace.workspaceId }); visible.add(local.id);
    files = capability.forSession(local);
    assert.equal(local.files, files, '本地SDK沿同一已安装能力取得借用接口');
    assert.equal((await programFiles.readBytes('artifact.txt')).data[0], 't'.charCodeAt(0), 'Program直接读取同一真实文件，无需Expose中转');
    client = await createProgramClient(program, transport());
    remote = await client.open(local.id);
    const qualified = { sessionId: local.id, program: programIdentity(program), accessId: remote.identity.accessId };
    const range = { offset: 2, limit: 1 }, page = remote.files.text('artifact.txt', range), metadata = remote.files.metadata('artifact.txt');
    range.offset = 1;
    assert.equal(remote.files.text('artifact.txt', { offset: 2, limit: 1 }), page);
    const started = watches.length, beforeFlush = flushes, delivered = [];
    subscriptions.push(page.observations$.subscribe(value => { if (value.status === 'ready') delivered.push(value.value.text); }));
    subscriptions.push(page.values$.subscribe({ error() {} })); subscriptions.push(metadata.values$.subscribe({ error() {} }));
    assert.equal((await page.read()).text, 'v1'); await metadata.read();
    assert.equal(flushes, beforeFlush, '直接文件读取不flush或激活会话');
    assert.equal(watches.length, started + 1, '同路径文本窗口和元数据共用一个真实Host watch');
    const beforeRead = reads;
    assert.equal((await page.read()).text, 'v1'); assert.equal(reads, beforeRead);
    const listing = remote.files.directory('.');
    subscriptions.push(listing.values$.subscribe()); await listing.read();

    delayed = true; await writeFile(path, 'title\nv2\n');
    const old = await delayedRead.entered.promise;
    await delay(150);
    const next = firstValueFrom(page.values$.pipe(filter(value => value.text === 'version-three'), timeout(8_000)));
    const listed = firstValueFrom(listing.values$.pipe(filter(value => value.entries.some(entry =>
      entry.name === 'artifact.txt' && entry.size === Buffer.byteLength('title\nversion-three\n'))), timeout(8_000)));
    await writeFile(path, 'title\nversion-three\n'); await Promise.all([next, listed]);
    assert.equal(old.signal.aborted, true);
    const beforeLate = delivered.length;
    delayedRead.release.resolve(); await immediate();
    assert.equal(delivered.slice(beforeLate).includes(old.result.text), false, '迟到HTTP旧页不能覆盖新代次');
    assert.equal((await metadata.read()).version, (await remote.files.stat('artifact.txt')).version);
    t.diagnostic('files.phase HTTP二进制与范围');
    const bytes = await remote.files.readBytes('artifact.txt');
    assert.ok(bytes.data instanceof Uint8Array, 'generated codec通过正式HTTP二进制附件恢复原生字节');
    assert.equal(new TextDecoder().decode(bytes.data), 'title\nversion-three\n');
    bytes.data[0] = 0;
    assert.equal((await remote.files.readBytes('artifact.txt')).data[0], 't'.charCodeAt(0));
    assert.equal(new TextDecoder().decode((await remote.files.readBytes('./artifact.txt', { baseFile: 'artifact.txt', range: { offset: 6, length: 7 } })).data), 'version');
    await assert.rejects(files.readBytes('../outside.txt'), error => error instanceof SessionFileOutsideWorkspaceError);
    for (const readOutside of [() => remote.files.readBytes('../outside.txt'), () => remote.files.stat(join(root, 'outside.txt')),
      () => remote.files.readBytes('artifact.txt', { baseFile: '../outside.txt' }), () => remote.files.list('..')]) {
      await assert.rejects(readOutside(), error => error.code === 'workspace-file/outside-workspace');
    }
    await assert.rejects(remote.files.readText('artifact.txt', { limit: 9 }), error => error.code === 'gateway/bad-request');
    const wrongProgram = await rpc('fileStat', { ...qualified, program: { ...qualified.program, version: program.version + 1 }, path: 'artifact.txt' });
    assert.equal(wrongProgram.ok, false); assert.equal(wrongProgram.error.code, 'reactive/program-contract-mismatch');
    t.diagnostic('files.phase 非owner权限');
    deniedClient = await createProgramClient(program, transport('file-denied'));
    denied = await deniedClient.open(local.id);
    await assert.rejects(denied.files.readBytes('artifact.txt'), error => error.code === 'remote-access/denied');
    await assert.rejects(remote.files.stat('blocked.txt'), error => error.code === 'remote-access/denied' && error.details.reason === 'private-artifact');
    assert.equal((await remote.files.stat('artifact.txt')).absolutePath, path, '普通文件拒绝不撤本Session资格');

    t.diagnostic('files.phase 连接失效与raw晚订阅');
    const reopening = gate(); watchGate = reopening;
    const pending = firstValueFrom(page.observations$.pipe(filter(value => value.status === 'pending'), timeout(8_000)));
    sockets.findLast(socket => socket.filePath === 'artifact.txt' && socket.readyState === WebSocket.OPEN).terminate();
    await pending; await reopening.entered.promise;
    const latePage = remote.files.text('artifact.txt', { offset: 1, limit: 1 }), lateValues = [];
    const lateSubscription = latePage.observations$.subscribe(value => lateValues.push(value));
    await immediate();
    assert.equal(lateValues.some(value => value.status === 'ready'), false, '重连中的晚查询不能借旧ready读取');
    await writeFile(path, 'title\nreconnected\n');
    const reconnected = firstValueFrom(page.values$.pipe(filter(value => value.text === 'reconnected'), timeout(8_000)));
    reopening.release.resolve(); await reconnected; lateSubscription.unsubscribe();

    const raw = remote.files.watch('artifact.txt');
    await firstValueFrom(raw.changes$.pipe(filter(frame => frame.kind === 'ready'), timeout(8_000)));
    const rawOpening = gate(); watchGate = rawOpening;
    sockets.findLast(socket => socket.filePath === 'artifact.txt' && socket.readyState === WebSocket.OPEN).terminate();
    await rawOpening.entered.promise;
    const rawLate = [], rawSubscription = raw.changes$.subscribe(frame => rawLate.push(frame));
    await immediate(); assert.deepEqual(rawLate, [], 'raw watch在载体失效后也撤旧Replay帧');
    const rawReady = firstValueFrom(raw.changes$.pipe(filter(frame => frame.kind === 'ready'), timeout(8_000)));
    rawOpening.release.resolve(); await rawReady; rawSubscription.unsubscribe(); await raw.dispose();

    t.diagnostic('files.phase 缺失恢复与冷读取');
    const nested = remote.files.text('created-directory/artifact.txt'), nestedStates = [];
    const nestedMonitor = nested.observations$.subscribe(value => nestedStates.push(value));
    subscriptions.push(nestedMonitor);
    const missingParent = await firstValueFrom(nested.observations$.pipe(filter(value => value.status === 'error'), timeout(8_000)));
    assert.equal(missingParent.error.message, 'REACTIVE_NATIVE_FILE_WATCH_DIRECTORY_REQUIRED');
    assert.equal(nestedStates.some(value => value.status === 'ready'), false, '父目录缺失不能签发可观察的ready');
    await mkdir(join(directory, 'created-directory'));
    await writeFile(join(directory, 'created-directory', 'artifact.txt'), '目录成立后取得正式观察');
    const nestedReady = firstValueFrom(nested.observations$.pipe(filter(value => value.status === 'ready'), timeout(8_000)));
    nested.invalidate();
    assert.equal((await nestedReady).value.text, '目录成立后取得正式观察');
    nestedMonitor.unsubscribe();
    const late = remote.files.text('later.txt');
    const absent = firstValueFrom(late.observations$.pipe(filter(value => value.status === 'error'), timeout(8_000)));
    const monitor = late.observations$.subscribe(value => t.diagnostic('files.later ' + JSON.stringify({
      status: value.status, generation: value.generation, error: value.error?.code ?? value.error?.message, text: value.value?.text,
    }))); subscriptions.push(monitor); await absent;
    t.diagnostic('files.phase 缺失路径已观察，开始创建later.txt');
    const recovered = firstValueFrom(late.observations$.pipe(filter(value => value.status === 'ready'), timeout(8_000)));
    await writeFile(join(directory, 'later.txt'), '真实创建后恢复');
    assert.equal((await recovered).value.text, '真实创建后恢复'); monitor.unsubscribe();
    t.diagnostic('files.phase later.txt已恢复，开始关闭Agent');
    const coldMonitor = page.observations$.subscribe(value => t.diagnostic('files.cold ' + JSON.stringify({
      status: value.status, generation: value.generation, error: value.error?.code ?? value.error?.message, text: value.value?.text,
    }))); subscriptions.push(coldMonitor);
    await runtime.application.close(local.id);
    t.diagnostic('files.phase Agent已关闭，开始冷文件更新');
    const cold = firstValueFrom(page.values$.pipe(filter(value => value.text === 'cold-file'), timeout(8_000)));
    await writeFile(path, 'title\ncold-file\n'); page.invalidate(); await cold;
    assert.equal(runtime.native.agents.get(SessionId(local.id)), undefined, '冷Files不恢复Agent');
    t.diagnostic('files.phase 已知正文连续覆盖使用显式失效，不依赖原生短窗口change尾通知');
    const coldTail = firstValueFrom(page.values$.pipe(filter(value => value.text === 'cold-file-tail'), timeout(8_000)));
    await writeFile(path, 'title\ncold-file-tail\n');
    assert.equal((await remote.files.readText('artifact.txt', { offset: 2, limit: 1 })).text, 'cold-file-tail',
      '显式读取以真实持久正文为准，不拿资源旧快照当权威');
    page.invalidate(); await coldTail;
    for (let cycle = 1; cycle <= 6; cycle++) {
      t.diagnostic('files.phase 冷文件移走 round=' + cycle);
      const absentAgain = firstValueFrom(page.observations$.pipe(filter(value => value.status === 'error'), timeout(8_000)));
      await rename(path, join(directory, 'previous-artifact-' + cycle + '.txt')); await absentAgain;
      t.diagnostic('files.phase 冷文件缺失已观察，重新创建 round=' + cycle);
      const text = 'restored-' + cycle;
      const restored = firstValueFrom(page.observations$.pipe(filter(value => value.status === 'ready' && value.value.text === text), timeout(8_000)));
      await writeFile(path, 'title\n' + text + '\n'); await restored;
    }
    coldMonitor.unsubscribe();

    t.diagnostic('files.phase caller取消真实读取');
    const canceledRead = gate(); bytesGate = canceledRead;
    const cancellation = new AbortController(), reason = new Error('CANCEL_ONLY_THIS_HTTP_FILE_READ');
    const reading = remote.files.readBytes('artifact.txt', {}, { signal: cancellation.signal });
    const rejected = assert.rejects(reading, error => error === reason);
    await canceledRead.entered.promise; cancellation.abort(reason);
    await firstValueFrom(from(rejected).pipe(timeout(2_000)));
    canceledRead.release.resolve();
    t.diagnostic('files.phase 已开始读取的ACL撤销');
    const revokedRead = gate(); bytesGate = revokedRead;
    const revoked = remote.files.readBytes('artifact.txt');
    const refused = assert.rejects(revoked, error => error.code === 'remote-access/denied');
    await revokedRead.entered.promise;
    actions.delete('reactivePrograms/fileReadBytes'); access.invalidate();
    revokedRead.release.resolve(); await refused;
    actions.add('reactivePrograms/fileReadBytes'); access.invalidate();
    assert.equal((await remote.files.stat('artifact.txt')).absolutePath, path);

    t.diagnostic('files.phase 成员资格撤销与显式重新取得');
    const memberRead = gate(); bytesGate = memberRead;
    const memberPending = remote.files.readBytes('artifact.txt');
    const memberOutcome = memberPending.then(value => ({ value }), error => ({ error }));
    await memberRead.entered.promise;
    const nativeWorkspace = runtime.native.workspaceRegistry.get(WorkspaceId(workspace.workspaceId));
    await nativeWorkspace.detachSession(SessionId(local.id));
    const memberResult = await firstValueFrom(from(memberOutcome).pipe(timeout(2_000)));
    if (!(memberResult.error instanceof ConversationAccessRevokedError))
      t.diagnostic('files.member-read 非资格错误: ' + (memberResult.error?.stack ?? JSON.stringify(memberResult)));
    assert.ok(memberResult.error instanceof ConversationAccessRevokedError, '未结算的HTTP文件读取保留正式成员资格错误');
    assert.equal(memberResult.error.reason, 'membership-removed');
    assert.throws(() => local.files, error => error === local.signal.reason, '本地Files getter保留同一资格退出原因');
    memberRead.release.resolve();
    await nativeWorkspace.attachSession(SessionId(local.id));
    const stale = await rpc('fileStat', { ...qualified, path: 'artifact.txt' });
    assert.equal(stale.ok, false); assert.equal(stale.error.code, 'reactive/conversation-access-revoked');
    assert.equal(stale.error.details.reason, 'access-expired');
    await remote.release(); local.release();
    remote = await client.open(qualified.sessionId);
    local = await runtime.application.open(qualified.sessionId); files = capability.forSession(local);
    assert.notEqual(remote.identity.accessId, qualified.accessId);
    assert.equal((await remote.files.readBytes('artifact.txt')).data[0], 't'.charCodeAt(0));

    t.diagnostic('files.phase 客户端释放与远端unwatch');
    const remoteWatch = remote.files.watch('artifact.txt');
    await firstValueFrom(remoteWatch.changes$.pipe(filter(frame => frame.kind === 'ready'), timeout(8_000)));
    const remoteClosing = gate(); closeGate = remoteClosing;
    await remote.release(); await remoteWatch.closed;
    const unfinished = await remoteClosing.entered.promise;
    assert.equal(unfinished.active, true, '客户端关闭只证明本端清理，没有远端unwatch完成ACK');
    assert.equal((await local.files.readBytes('artifact.txt')).data[0], 't'.charCodeAt(0), '另一借用仍能读取');
    remoteClosing.release.resolve();
    remote = await client.open(qualified.sessionId);

    t.diagnostic('files.phase 本地owner实际清理');
    const finalPage = files.text('artifact.txt');
    subscriptions.push(finalPage.observations$.subscribe()); await finalPage.read();
    const finalRead = gate(); bytesGate = finalRead;
    const localCancellation = new AbortController(), localReason = new Error('CANCEL_ONLY_LOCAL_FILE_WAIT');
    const localPending = files.readBytes('artifact.txt', {}, { signal: localCancellation.signal });
    const localRejected = assert.rejects(localPending, error => error === localReason);
    await finalRead.entered.promise; localCancellation.abort(localReason); await localRejected;
    const localClosing = gate(); closeGate = localClosing;
    let disposed = false;
    const disposing = files.dispose().then(() => { disposed = true; });
    assert.throws(() => finalPage.peek(), 'SDK退出立即撤ready，不等不合作I/O');
    await localClosing.entered.promise; await immediate(); assert.equal(disposed, false);
    localClosing.release.resolve(); await immediate();
    assert.equal(disposed, false, 'unwatch结束后仍等真实I/O');
    finalRead.release.resolve(); await disposing;
    assert.equal((await remote.files.stat('artifact.txt')).absolutePath, path, '本地借用退出不销毁全域能力');
    assert.equal(models, 0); assert.deepEqual(errors, []);
  } finally {
    for (const current of gates) current.release.resolve();
    subscriptions.forEach(subscription => subscription.unsubscribe());
    await remote?.release(); await denied?.release(); await client?.dispose(); await deniedClient?.dispose();
    local?.release(); await gateway?.close(); await capability.dispose(); await runtime.dispose();
  }
});
