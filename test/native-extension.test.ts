import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setImmediate as immediate } from 'node:timers/promises';
import { Observable, defer, from, map, type Subscription } from 'rxjs';
import { symbols, type Context, type Fiber } from '@deepseek-ai/cordis';
import LocalFileSystem from '@deepseek-ai/dsh-fs-local';
import type { FileSystem } from '@deepseek-ai/dsh-fs';
import { defineApplication, defineEnvironment, defineProgram, defineSurface, z, type Observation } from '@hanger-source/dsh-reactive';
import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { requireActivePlugin } from '@hanger-source/dsh-reactive-dsh';
import { createNativeSessionCapability } from '@hanger-source/dsh-reactive-dsh/native';
import { boot, text } from './native-support.ts';

const program = defineProgram({ input: z.null(), surface: defineSurface({ views: {}, actions: {} }) });
const definition = defineApplication({ name: 'public-native-extension', admit: async () => {}, environments: [defineEnvironment({
  id: 'work', instructions: '原生扩展观察不启动模型', program: implementProgram(program, { Component: () => null }),
})] });

test('公共扩展冷构造后读取真实文件和Agent；exact来源缓存与同会话独立句柄各有观察寿命', async () => {
  const home = await mkdtemp(join(tmpdir(), 'dsh-reactive-extension-source-'));
  const path = join(home, 'fact.txt');
  await writeFile(path, '来自真实文件');
  let models = 0, queries = 0;
  const app = await boot(home, definition, async function* () { models++; yield* text('unused'); });
  const capability = createNativeSessionCapability({ name: 'extension-source', services: ['tools', 'fs'], session(context) {
    const file = context.resource(() => defer(() => {
      queries++;
      const fs = context.context().get('fs')!;
      return from(fs.resolve(path).then(target => fs.readText(target))).pipe(map(value =>
        ({ status: 'ready' as const, generation: 1, observedAt: Date.now(), value })));
    }));
    const catalog = context.agentResource({
      changes: agent => new Observable(subscriber => agent.ctx.on('tools/change', () => subscriber.next(undefined))),
      read: agent => { queries++; return agent.ctx.get('tools')!.schemas(agent).map(tool => tool.name).sort(); },
    });
    return { file, catalog, agents: context.liveAgent$ };
  } });
  const subscriptions: Subscription[] = [];
  try {
    await capability.install(app.native, home);
    const one = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const two = await app.host.open(one.id, program);
    const first = capability.forSession(one), second = capability.forSession(two);
    assert.equal(capability.forSession(one), first);
    assert.notEqual(first, second);
    assert.throws(() => capability.forSession({ id: one.id }), /REACTIVE_NATIVE_SESSION_REQUIRED/u);
    assert.equal(queries, 0, '构造两份资源不读取原生服务');
    let firstCompleted = false, secondCompleted = false;
    const firstAgents: unknown[] = [], secondAgents: unknown[] = [];
    subscriptions.push(first.agents.subscribe({ next: agent => firstAgents.push(agent), complete: () => { firstCompleted = true; } }),
      second.agents.subscribe({ next: agent => secondAgents.push(agent), complete: () => { secondCompleted = true; } }));
    assert.ok(firstAgents.at(-1));
    assert.equal(firstAgents.at(-1), secondAgents.at(-1), '独立句柄借入同一个真实Agent');
    assert.equal(await first.file.read(), '来自真实文件');
    assert.deepEqual(await first.catalog.read(), await second.catalog.read());
    one.release();
    assert.equal(firstCompleted, true, '已连接liveAgent$随本来源退出完成');
    assert.equal(secondCompleted, false, '同会话另一来源继续观察真实Agent');
    assert.throws(() => first.file.peek());
    assert.throws(() => capability.forSession(one));
    assert.equal(await second.file.read(), '来自真实文件');
    await app.host.close(two.id);
    assert.equal(second.catalog.peek().status, 'pending');
    const resumed = await app.host.resume(two.id, program);
    assert.deepEqual(await second.catalog.read(), await capability.forSession(resumed).catalog.read());
    assert.notEqual(secondAgents.at(-1), firstAgents.at(-1), '正式恢复切换Agent，已退出来源不复活');
    assert.equal(firstAgents.length, 1);
    resumed.release(); two.release();
    assert.equal(secondCompleted, true);
    assert.equal(models, 0); assert.deepEqual(app.errors, []);
  } finally {
    subscriptions.forEach(subscription => subscription.unsubscribe());
    await capability.dispose(); await app.dispose();
  }
});

test('构造期观察和调用均拒绝；失败不缓存，来源释放后Host仍等待该次异步文件清理', async () => {
  const home = await mkdtemp(join(tmpdir(), 'dsh-reactive-extension-construction-'));
  const path = join(home, 'cleanup.txt');
  const app = await boot(home, definition, async function* () { yield* text('unused'); });
  const cleanupEntered = Promise.withResolvers<void>(), releaseCleanup = Promise.withResolvers<void>();
  let constructions = 0, executions = 0, queries = 0;
  let reenter: (() => unknown) | undefined;
  const forbidden: Observation<number>[] = [];
  const capability = createNativeSessionCapability({ name: 'extension-construction', services: ['fs'], session(context) {
    constructions++;
    if (constructions === 1) {
      context.own(async () => { cleanupEntered.resolve(); await releaseCleanup.promise; await writeFile(path, '清理已落盘'); });
      const resource = context.resource(() => { queries++; return from([{
        status: 'ready' as const, generation: 1, observedAt: Date.now(), value: 1,
      }]); });
      resource.observations$.subscribe(value => forbidden.push(value)).unsubscribe();
      context.invoke('forbidden-construction', () => { executions++; });
    }
    if (constructions === 2) assert.throws(() => reenter!(), /REACTIVE_NATIVE_SESSION_REENTRANT/u,
      '同exact来源尚在构造时不能重入创建另一份共享面');
    return { current: () => { context.assertCurrent(); return context.sessionId; } };
  } });
  let closing: Promise<void> | undefined;
  try {
    await capability.install(app.native, home);
    const source = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    reenter = () => capability.forSession(source);
    assert.throws(() => capability.forSession(source), /REACTIVE_NATIVE_SESSION_CONSTRUCTING/u);
    await cleanupEntered.promise;
    assert.equal(queries, 0); assert.equal(executions, 0);
    const denied = forbidden.find(value => value.status === 'error');
    assert.ok(denied?.status === 'error');
    assert.match(String(denied.error), /REACTIVE_NATIVE_SESSION_CONSTRUCTING/u);
    const retried = capability.forSession(source);
    assert.equal(constructions, 2, '失败的factory不成为exact来源缓存');
    assert.equal(capability.forSession(source), retried, '拒绝构造重入后exact来源仍只共享一份完成对象');
    assert.equal(retried.current(), source.id);
    source.release();
    assert.throws(retried.current);
    let closed = false;
    closing = app.host.dispose().then(() => { closed = true; });
    await immediate();
    assert.equal(closed, false, 'Host加入已经从来源退出的异步清理');
    await assert.rejects(readFile(path), { code: 'ENOENT' });
    releaseCleanup.resolve(); await closing;
    assert.equal(await readFile(path, 'utf8'), '清理已落盘');
    assert.deepEqual(app.errors, []);
  } finally {
    releaseCleanup.resolve(); await closing;
    await capability.dispose(); await app.dispose();
  }
});

test('真实文件写入的Invocation等待可取消；来源退出和能力dispose仍保留结果并等待实际写入', async () => {
  const home = await mkdtemp(join(tmpdir(), 'dsh-reactive-extension-invocation-'));
  const path = join(home, 'result.txt');
  const app = await boot(home, definition, async function* () { yield* text('unused'); });
  const entered = Promise.withResolvers<void>(), release = Promise.withResolvers<void>();
  const capability = createNativeSessionCapability({ name: 'extension-write', services: ['fs'], session(context) {
    return { write: () => {
      const fs = context.context().get('fs')!;
      return context.invoke('write-file', async () => {
        const target = await fs.resolve(path);
        entered.resolve(); await release.promise;
        return fs.writeText(target, '实际原生写入');
      });
    } };
  } });
  let closing: Promise<void> | undefined;
  try {
    await capability.install(app.native, home);
    const source = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    const extension = capability.forSession(source), invocation = extension.write();
    await entered.promise;
    const caller = new AbortController(), reason = new Error('CALLER_STOPPED_WAITING');
    const callerWait = assert.rejects(invocation.wait({ signal: caller.signal }), error => error === reason);
    caller.abort(reason); await callerWait;
    let returned = false;
    void invocation.result.then(() => { returned = true; });
    const sourceWait = assert.rejects(invocation.wait(), error => error === source.signal.reason);
    source.release(); await sourceWait;
    let disposed = false;
    closing = capability.dispose().then(() => { disposed = true; });
    await immediate();
    assert.equal(disposed, false); assert.equal(returned, false);
    assert.throws(extension.write);
    await assert.rejects(readFile(path), { code: 'ENOENT' });
    release.resolve();
    const result = await invocation.result;
    await closing;
    assert.equal(await readFile(path, 'utf8'), '实际原生写入');
    assert.equal(await invocation.result, result, '等待取消不改写原生执行结果');
    assert.equal(invocation.activity.peek().status, 'ready');
    assert.deepEqual(app.errors, []);
  } finally {
    release.resolve(); await closing;
    await capability.dispose(); await app.dispose();
  }
});

test('所有服务均核provider身份；真实Cordis依赖退出后旧扩展不能由替换provider复活', async () => {
  const home = await mkdtemp(join(tmpdir(), 'dsh-reactive-extension-provider-'));
  const path = join(home, 'provider.txt');
  const foreignPath = join(home, 'foreign-provider.txt');
  await writeFile(path, '正式provider文件');
  await writeFile(foreignPath, '隔离provider文件');
  const app = await boot(home, definition, async function* () { yield* text('unused'); });
  const isolated = app.native.isolate('fs');
  const foreignProvider = isolated.plugin(LocalFileSystem, { cwd: home });
  const wrong = createNativeSessionCapability({ name: 'extension-wrong-second-provider', services: ['tools', 'fs'],
    session: () => ({}) });
  const fs = app.native.get('fs')!;
  const sharedMarker = {};
  let foreignReads = 0;
  const plainProvider = app.native.plugin({ name: 'extension-plain-files-provider', apply(ctx: Context) {
    ctx.provide('extensionPlainFiles', {
      [symbols.original]: sharedMarker,
      async read() { return fs.readText(await fs.resolve(path)); },
    });
  } });
  const plainIsolated = app.native.isolate('extensionPlainFiles');
  const foreignPlainProvider = plainIsolated.plugin({ name: 'extension-foreign-plain-files-provider', apply(ctx: Context) {
    ctx.provide('extensionPlainFiles', {
      [symbols.original]: sharedMarker,
      async read() { foreignReads++; return fs.readText(await fs.resolve(foreignPath)); },
    });
  } });
  const plain = createNativeSessionCapability({ name: 'extension-plain-files', services: ['tools', 'extensionPlainFiles'],
    session: context => ({ read: () => {
      const files: { read(): Promise<string> } = context.context().get('extensionPlainFiles');
      return files.read();
    } }),
  });
  const foreignPlain = createNativeSessionCapability({ name: 'extension-foreign-plain-files', services: ['tools', 'extensionPlainFiles'],
    session: context => ({ read: () => {
      const files: { read(): Promise<string> } = context.context().get('extensionPlainFiles');
      return files.read();
    } }),
  });
  let provider: (Fiber & PromiseLike<Fiber>) | undefined;
  const capability = createNativeSessionCapability({ name: 'extension-provider', services: ['tools', 'extensionFiles'],
    *setup(context) {
      provider = context.plugin({ name: 'extension-files-provider', apply(ctx: Context) {
        ctx.provide('extensionFiles', ctx.get('fs')!);
      } });
      yield provider;
    },
    session(context) {
      return { file: context.resource(() => defer(() => {
        const fs: FileSystem = context.context().get('extensionFiles');
        return from(fs.resolve(path).then(target => fs.readText(target))).pipe(map(value =>
          ({ status: 'ready' as const, generation: 1, observedAt: Date.now(), value })));
      })) };
    },
  });
  let replacement: (Fiber & PromiseLike<Fiber>) | undefined;
  try {
    await requireActivePlugin(foreignProvider);
    await requireActivePlugin(plainProvider); await requireActivePlugin(foreignPlainProvider);
    await wrong.install(isolated, home); await capability.install(app.native, home);
    await plain.install(app.native, home); await foreignPlain.install(plainIsolated, home);
    const source = await app.host.create('work', { workspaceId: app.workspace.workspaceId });
    assert.throws(() => wrong.forSession(source), /REACTIVE_NATIVE_CAPABILITY_HOST_MISMATCH/u,
      '第一依赖tools相同仍须核第二依赖fs');
    assert.equal(await plain.forSession(source).read(), '正式provider文件', '普通对象provider的匹配来源仍实际读取官方fs');
    assert.throws(() => foreignPlain.forSession(source), /REACTIVE_NATIVE_CAPABILITY_HOST_MISMATCH/u,
      '第一依赖tools相同，两份没有trace original的普通对象provider仍须按exact身份隔离');
    assert.equal(foreignReads, 0, '来源不能借隔离的普通对象provider读取另一份真实文件');
    const extension = capability.forSession(source);
    assert.equal(await extension.file.read(), '正式provider文件');
    await provider!.dispose(); await capability.dispose();
    assert.throws(() => extension.file.peek());
    replacement = app.native.plugin({ name: 'extension-files-replacement', apply(ctx: Context) {
      ctx.provide('extensionFiles', ctx.get('fs')!);
    } });
    await requireActivePlugin(replacement);
    assert.throws(() => capability.forSession(source));
    assert.throws(() => extension.file.peek(), '后来的正式provider不能恢复旧实例读取权');
    assert.equal(await fs.readText(await fs.resolve(path)), '正式provider文件', '借用能力退出不卸载外部官方fs');
    source.release(); assert.deepEqual(app.errors, []);
  } finally {
    await plain.dispose(); await foreignPlain.dispose();
    await plainProvider.dispose(); await foreignPlainProvider.dispose();
    await replacement?.dispose(); await wrong.dispose(); await capability.dispose();
    await foreignProvider.dispose(); await app.dispose();
  }
});
