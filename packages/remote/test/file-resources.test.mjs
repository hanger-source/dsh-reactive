import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, mkdir, rename, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setImmediate as immediate, setTimeout as delay } from 'node:timers/promises';
import { symbols } from '@deepseek-ai/cordis';
import WorkspaceFiles from '@deepseek-ai/dsh-api-workspace-files';
import SandboxPolicy from '@deepseek-ai/dsh-sandbox-policy';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { SessionId } from '@deepseek-ai/dsh-session';
import { firstValueFrom, filter, timeout } from 'rxjs';
import WebSocket from 'ws';
import { defineApplication, defineEnvironment, defineProgram, defineSurface, resourceView, mapResource, z } from '@hanger-source/dsh-reactive';
import { implementProgram, useResource, Expose } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel, requireActivePlugin } from '@hanger-source/dsh-reactive-dsh';
import { createNativeFiles } from '@hanger-source/dsh-reactive-dsh/files';
import { createProgramClient } from '@hanger-source/dsh-reactive-client';
import { mountProgramGateway, RemoteAuthenticationError } from '@hanger-source/dsh-reactive-remote';

test('真实文件资源共享watch并先监听后读，失效取消旧页，冷文件与远程产物沿同一Program资源且SDK等待unwatch', { timeout: 25_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'dsh-reactive-file-resources-'));
  const directory = join(root, 'workspace'); await mkdir(directory);
  const path = join(directory, 'artifact.txt'); await writeFile(path, 'title\nv1\n');
  await writeFile(join(root, 'outside.txt'), '不属于工作区');
  const capability = createNativeFiles({ service: 'provided' });
  const surface = defineSurface({ actions: {}, views: { artifact: resourceView(z.object({ text: z.string(), version: z.string() })) } });
  const program = defineProgram({ input: z.null(), surface });
  const definition = defineApplication({ name: 'file-resources', admit: async () => {}, environments: [defineEnvironment({
    id: 'work', instructions: '文件观察不启动模型', program: implementProgram(program, { Component({ conversation }) {
      const page = capability.forSession(conversation).text('artifact.txt');
      const observed = useResource(page);
      return Expose({ surface, value: { views: { artifact: observed.observations$.pipe(mapResource(value => ({ text: value.text, version: value.version }))) }, actions: {} } });
    } }),
  })] });
  let models = 0, flushes = 0, reads = 0, delayed = false, holdClose = false;
  const errors = [], watches = [], subscriptions = [];
  const entered = Promise.withResolvers(), releaseRead = Promise.withResolvers(), closing = Promise.withResolvers(), releaseClose = Promise.withResolvers();
  const releaseBytes = Promise.withResolvers();
  const runtime = await createRuntime(() => definition, { home: join(root, 'home'), actorId: 'file-owner', onError: event => errors.push(event.error),
    installNative: ctx => requireActivePlugin(ctx.plugin(SandboxPolicy, { mode: 'read-only', workspaceRoot: directory })).then(() => {}),
    model: nativeModel({ provider: 'file-resources', model: 'unused', create: () => new class extends LlmAdapter {
      async *stream() { models++; yield { type: 'finish', reason: { kind: 'stop' } }; }
    }() }),
  });
  let gateway, client, local, remote, files;
  try {
    const visible = new Set();
    const role = { name: 'file-artifacts', resource: 'session', actions: ['reactivePrograms/open', 'reactivePrograms/questions', 'reactivePrograms/view'] };
    gateway = await mountProgramGateway(runtime.native, { application: runtime.application,
      model: { provider: 'file-resources', model: 'unused' }, participant: principal => ({ participant: 'human', actorId: principal.id }),
      access: service => service.registerPolicy('files', { bindings: () => [...visible].map(id => ({ role, resource: { kind: 'session', id } })) }),
      carrier: { async authenticate(request) {
        if (request.headers.get('authorization') !== 'Bearer file-reader') throw new RemoteAuthenticationError(401);
        return { kind: 'files', id: 'file-reader' };
      }, onError: error => errors.push(error) },
    });
    await requireActivePlugin(runtime.native.plugin(WorkspaceFiles, { maxLines: 8, maxBytes: 4096, maxFileBytes: 4096, maxEntries: 16 }));
    await capability.install(runtime.native, join(root, 'home'));
    const fs = runtime.native.fs[symbols.original], native = runtime.native.workspaceFiles[symbols.original];
    const watch = fs.watch, read = native.read, sessions = runtime.native.sessions[symbols.original], flush = sessions.flush;
    sessions.flush = function (...args) { flushes++; return flush.apply(this, args); };
    fs.watch = async function (target, notify, signal) {
      const unwatch = await watch.call(this, target, notify, signal);
      const current = { path: this.processPath(target), closed: Promise.withResolvers(), active: true };
      watches.push(current);
      return async () => {
        await unwatch();
        if (holdClose) { closing.resolve(); await releaseClose.promise; }
        current.active = false; current.closed.resolve();
      };
    };
    native.read = async function (scope, target, range, signal) {
      assert.ok(watches.some(watch => watch.active && watch.path === resolve(scope.workspaceRoot, target)), '实际watch ready前不能读取文件切面');
      const result = await read.call(this, scope, target, range, signal);
      reads++;
      if (delayed && range.offset === 2) {
        delayed = false; entered.resolve({ result, signal }); await releaseRead.promise;
      }
      return result;
    };
    const workspace = await runtime.application.createWorkspace({ directory });
    local = await runtime.application.create('work', { workspaceId: workspace.workspaceId }); visible.add(local.id);
    client = await createProgramClient(program, { baseUrl: gateway.url,
      fetch: (url, init) => fetch(url, { ...init, headers: { ...Object.fromEntries(new Headers(init?.headers)), authorization: 'Bearer file-reader' } }),
      createWebSocket: url => new WebSocket(url, { headers: { authorization: 'Bearer file-reader' } }),
    });
    remote = await client.open(local.id);
    assert.equal((await remote.readView('artifact')).text, 'title\nv1');
    files = capability.forSession(local);
    const range = { offset: 2, limit: 1 }, page = files.text('artifact.txt', range), metadata = files.metadata('artifact.txt');
    range.offset = 1;
    assert.equal(files.text('artifact.txt', { offset: 2, limit: 1 }), page);
    const started = watches.length, beforeFlush = flushes;
    const delivered = [];
    subscriptions.push(page.observations$.subscribe(value => { if (value.status === 'ready') delivered.push(value.value.text); }));
    subscriptions.push(page.values$.subscribe()); subscriptions.push(metadata.values$.subscribe());
    assert.equal((await page.read()).text, 'v1'); await metadata.read();
    assert.equal(flushes, beforeFlush, '文件I/O不冲刷会话日志或依赖Agent执行');
    assert.equal(watches.length, started + 1, '两个文本消费者和元数据查询共享同路径的实际watch');
    const localWatch = watches.at(-1), beforeRead = reads;
    assert.equal((await page.read()).text, 'v1'); assert.equal(reads, beforeRead);
    const listing = files.directory('.');
    subscriptions.push(listing.values$.subscribe()); await listing.read();
    delayed = true; await writeFile(path, 'title\nv2\n');
    const old = await entered.promise;
    // 原生FS会合并短窗口内的同路径通知；独立写入分开，读取竞态由真实切面的延迟交付制造。
    await delay(150);
    const next = firstValueFrom(page.values$.pipe(filter(value => value.text === 'version-three'), timeout(8_000)));
    const listed = firstValueFrom(listing.values$.pipe(filter(value => value.entries.some(entry => entry.name === 'artifact.txt' && entry.size === Buffer.byteLength('title\nversion-three\n'))), timeout(8_000)));
    await writeFile(path, 'title\nversion-three\n'); await Promise.all([next, listed]);
    assert.equal(old.signal.aborted, true);
    const deliveredBeforeLateRead = delivered.length;
    releaseRead.resolve(); await immediate();
    assert.equal(delivered.slice(deliveredBeforeLateRead).includes(old.result.text), false, '迟到的真实旧页不能覆盖新资源');
    const exposed = await firstValueFrom(remote.observeView('artifact').pipe(filter(value => value.status === 'ready' && value.value.text.includes('version-three')), timeout(8_000)));
    assert.equal(exposed.value.version, (await files.metadata('artifact.txt').read()).version);
    await assert.rejects(files.text('../outside.txt').read(), /REACTIVE_NATIVE_FILE_OUTSIDE_WORKSPACE/);
    await assert.rejects(files.text('artifact.txt', { limit: 9 }).read());
    const late = files.text('later.txt');
    const failed = firstValueFrom(late.observations$.pipe(filter(value => value.status === 'error'), timeout(8_000)));
    const monitor = late.observations$.subscribe(); subscriptions.push(monitor);
    await failed;
    const recovered = firstValueFrom(late.observations$.pipe(filter(value => value.status === 'ready'), timeout(8_000)));
    await writeFile(join(directory, 'later.txt'), '真实创建后恢复');
    assert.equal((await recovered).value.text, '真实创建后恢复'); monitor.unsubscribe();
    const bytes = await files.readBytes('artifact.txt'); bytes.data[0] = 0;
    assert.equal((await files.readBytes('artifact.txt')).data[0], 't'.charCodeAt(0));
    await runtime.application.close(local.id);
    await delay(150);
    await writeFile(path, 'title\ncold-file\n');
    await firstValueFrom(remote.observeView('artifact').pipe(filter(value => value.status === 'ready' && value.value.text === 'title\ncold-file'), timeout(8_000)));
    assert.equal(runtime.native.agents.get(SessionId(local.id)), undefined);
    subscriptions.splice(0, 3).forEach(subscription => subscription.unsubscribe());
    await localWatch.closed.promise;
    assert.equal(page.peek().status, 'pending');
    await delay(150);
    const unavailable = firstValueFrom(remote.observeView('artifact').pipe(filter(value => value.status === 'error'), timeout(8_000)));
    await rename(path, join(directory, 'previous-artifact.txt')); await unavailable;
    const restored = firstValueFrom(remote.observeView('artifact').pipe(filter(value => value.status === 'ready' && value.value.text === 'title\nrestored'), timeout(8_000)));
    await writeFile(path, 'title\nrestored\n'); await restored;
    const readBytes = native.readBytes, bytesEntered = Promise.withResolvers();
    native.readBytes = async function (...args) {
      const result = await readBytes.apply(this, args);
      bytesEntered.resolve(); await releaseBytes.promise; return result;
    };
    const cancellation = new AbortController(), reason = new Error('CANCEL_ONLY_THIS_FILE_READ');
    const pending = files.readBytes('artifact.txt', {}, { signal: cancellation.signal });
    let stopped = false;
    const rejected = assert.rejects(pending, error => error === reason).then(() => { stopped = true; });
    await bytesEntered.promise; cancellation.abort(reason); await immediate();
    assert.equal(stopped, true, '取消读取先结束消费者等待，实际提供者仍归SDK清理');
    await rejected;
    holdClose = true;
    let disposed = false;
    const disposing = files.dispose().then(() => { disposed = true; });
    await closing.promise; await immediate(); assert.equal(disposed, false, 'SDK释放等待原生异步unwatch');
    releaseClose.resolve(); await immediate();
    assert.equal(disposed, false, 'unwatch完成后仍等待真实读取提供者退出');
    releaseBytes.resolve(); await disposing;
    assert.equal(models, 0); assert.deepEqual(errors, []);
  } finally {
    releaseRead.resolve(); releaseClose.resolve(); releaseBytes.resolve(); subscriptions.forEach(subscription => subscription.unsubscribe());
    await files?.dispose(); await remote?.release(); await client?.dispose(); local?.release();
    await gateway?.close(); await runtime.dispose();
  }
});
