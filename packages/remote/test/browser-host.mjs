import { createServer, request as forward } from 'node:http';
import { connect } from 'node:net';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';
import { BehaviorSubject, of } from 'rxjs';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { RemoteError } from '@deepseek-ai/dsh-typert-protocol';
import { implementAction, implementOperation, defineApplication, defineEnvironment } from '@hanger-source/dsh-reactive';
import { implementProgram, useAction, Expose } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import { mountProgramGateway, RemoteAuthenticationError } from '@hanger-source/dsh-reactive-remote';
import { bootInjections } from '@deepseek-ai/dsh-client-modules';
import { operation, check, surface, program } from './browser-contracts.mjs';

const assets = await mkdtemp(join(tmpdir(), 'dsh-browser-assets-'));
const coreEntry = fileURLToPath(import.meta.resolve('@hanger-source/dsh-reactive'));
const clientBrowserEntry = fileURLToPath(new URL('./lib/client.browser.js', import.meta.resolve('@hanger-source/dsh-reactive-client/package.json')));
let browserClient;
const browserAssets = new Set();
const reactivityEntry = fileURLToPath(import.meta.resolve('@hanger-source/dsh-reactive/reactivity'));
const coreArtifact = await build({ entryPoints: { core: coreEntry, reactivity: reactivityEntry, zod: fileURLToPath(import.meta.resolve('zod')) }, outdir: assets,
  bundle: true, splitting: true, format: 'esm', platform: 'browser', target: 'es2024', metafile: true,
  define: { 'process.env.NODE_ENV': '"production"' } });
for (const file of Object.keys(coreArtifact.metafile.outputs)) browserAssets.add('/' + relative(assets, resolve(file)));
for (const [entry, name, external] of [
  [fileURLToPath(new URL('./browser-consumer.mjs', import.meta.url)), 'consumer.js', ['@hanger-source/dsh-reactive']],
  [fileURLToPath(new URL('./browser-native-consumer.mjs', import.meta.url)), 'native-consumer.js', ['@hanger-source/dsh-reactive', 'zod']]]) {
  const artifact = await build({ entryPoints: [entry], outfile: join(assets, name), bundle: true,
    format: 'esm', platform: 'browser', target: 'es2024', external, metafile: true, define: { 'process.env.NODE_ENV': '"production"' },
    logOverride: { 'import-is-undefined': 'error' } });
  browserAssets.add('/' + name);
  for (const output of Object.values(artifact.metafile.outputs)) for (const dependency of output.imports) {
    if (dependency.path !== '@hanger-source/dsh-reactive' && dependency.path !== '@hanger-source/dsh-reactive/reactivity' && dependency.path !== 'zod')
      throw new Error('BROWSER_NODE_OR_EXTERNAL_IMPORT:' + dependency.path);
  }
  if (name === 'consumer.js') {
    browserClient = Object.keys(artifact.metafile.inputs).find(input => resolve(input) === clientBrowserEntry);
    if (!browserClient) throw new Error('BROWSER_PUBLIC_CLIENT_FACE_REQUIRED');
  }
}
const nativeModules = ['@deepseek-ai/dsh-client-modules', '@deepseek-ai/dsh-client-connection', '@deepseek-ai/dsh-typert-registry',
  '@deepseek-ai/dsh-api-gateway', '@deepseek-ai/dsh-client-file-upload', '@deepseek-ai/dsh-api-session-controller'];
const nativeBootstrap = (await Promise.all(nativeModules.map(async name => {
  const manifest = import.meta.resolve(name + '/package.json');
  const pkg = JSON.parse(await readFile(fileURLToPath(manifest), 'utf8'));
  const condition = ['@deepseek-ai/dsh-client-connection', '@deepseek-ai/dsh-typert-registry', '@deepseek-ai/dsh-api-gateway'].includes(name) ? 'browser' : 'default';
  const face = pkg.exports['./client'][condition];
  if (typeof face !== 'string') throw new Error('NATIVE_PUBLIC_CLIENT_FACE_REQUIRED:' + name);
  return readFile(new URL(face, manifest), 'utf8');
}))).join('\n');
const nativeGraph = { rev: 'native-session-reference-rc3', entries: nativeModules.map(id => ({ id, url: '/native-bootstrap.js', rev: 'rc3' })),
  batches: [{ phase: 'bootstrap', url: '/native-bootstrap.js', rev: 'rc3', entries: nativeModules }] };
const nativeHead = bootInjections(nativeGraph).map(row => {
  if (row.kind === 'script') return '<script>' + row.text + '</script>';
  if (row.kind === 'script-src') return '<script src="' + row.src + '"></script>';
  if (row.kind === 'global') return '<script>window[' + JSON.stringify(row.name) + ']=' + JSON.stringify(row.value) + '</script>';
  throw new Error('UNEXPECTED_NATIVE_BOOT_INJECTION:' + row.kind);
}).join('');
const home = await mkdtemp(join(tmpdir(), 'dsh-reactive-browser-'));
const current = new BehaviorSubject(new Date('2026-10-26T00:00:00Z'));
let executions = 0, queries = 0, models = 0, parsedAmount;
const owner = implementOperation(operation, { locate: () => ({ status: 'unknown' }), observe() {
  queries++; return of({ status: 'ready', generation: 0, observedAt: Date.now(), value: { phase: 'done', at: '2026-10-26T00:00:08Z' } });
} });
const implementation = implementAction(check, { operationOwner: owner, execute(input, call) {
  if (call.source.actorId !== 'browser-participant') throw new Error('WRONG_AUTHENTICATED_ACTOR');
  executions++; parsedAmount = input.amount;
  if (input.amount === 13) throw new RemoteError('test/revision-conflict', '订单版本已变化', { orderId: 'order-7', expectedRevision: 7, actualRevision: 8 });
  current.next(new Date('2026-10-26T00:00:07Z'));
  return { at: '2026-10-26T00:00:07Z', operation: 'operation-7' };
} });
const definition = defineApplication({ name: 'browser-proof', admit: async () => {}, environments: [defineEnvironment({
  id: 'work', instructions: '显式SDK业务核验',
  program: implementProgram(program, { Component() {
    const action = useAction(implementation, () => null, []);
    return Expose({ surface, value: { views: { current }, actions: { check: action } } });
  } }),
})] });
const runtime = await createRuntime(() => definition, { home: join(home, 'home'), operations: [owner], tools: 'native',
  model: nativeModel({ provider: 'browser-proof', model: 'unused', create: () => new class extends LlmAdapter {
    async *stream() { models++; throw new Error('SDK_MUST_NOT_WAKE_MODEL'); }
  }() }) });
const workspace = await runtime.application.createWorkspace({ directory: home, title: '浏览器共享工作目录' });
const sessionId = randomUUID();
const credential = randomUUID();
let origin;
const gateway = await mountProgramGateway(runtime.native, { application: runtime.application,
  model: { provider: 'browser-proof', model: 'unused' }, actor: principal => principal.id,
  access: service => service.registerPolicy('browser-participant', { bindings() {
    return [{ role: { name: 'create', resource: 'workspace', actions: ['reactivePrograms/create'] }, resource: { kind: 'workspace', id: workspace.workspaceId } },
      { role: { name: 'native-client-catalog', resource: 'global', actions: ['session/control', 'session/list'] }, resource: { kind: 'global' } },
      { role: {
        name: 'participate', resource: 'session', actions: ['reactivePrograms/open', 'reactivePrograms/view', 'reactivePrograms/questions', 'reactivePrograms/resolveAction', 'reactivePrograms/startAction',
          'reactivePrograms/actionResult', 'reactivePrograms/observeAction', 'reactivePrograms/observeOperation', 'session/list', 'session/control', 'session/follow', 'session/projections', 'session/page', 'commands/list'],
      }, resource: { kind: 'session', id: sessionId } }];
  } }), carrier: { async authenticate(request) {
    if (!request.headers.get('cookie')?.split(';').some(part => part.trim() === 'remote-proof=' + credential)
      || (request.headers.has('origin') && request.headers.get('origin') !== origin)) throw new RemoteAuthenticationError(401);
    return { kind: 'browser-participant', id: 'browser-participant' };
  } },
});
const target = new URL(gateway.url);
const app = createServer(async (incoming, outgoing) => {
  if (incoming.url?.startsWith('/api/')) {
    const upstream = forward(new URL(incoming.url, target), { method: incoming.method, headers: incoming.headers }, response => {
      outgoing.writeHead(response.statusCode, response.headers); response.pipe(outgoing);
    });
    upstream.on('error', error => outgoing.destroy(error)); incoming.once('aborted', () => upstream.destroy()); incoming.pipe(upstream); return;
  }
  if (incoming.url === '/setup' || incoming.url === '/proof') { outgoing.setHeader('content-type', 'application/json'); outgoing.end(JSON.stringify({ workspaceId: workspace.workspaceId, sessionId, executions, queries, parsedAmount, models })); return; }
  if (incoming.url === '/native-bootstrap.js') { outgoing.setHeader('content-type', 'text/javascript'); outgoing.end(nativeBootstrap); return; }
  if (browserAssets.has(incoming.url)) {
    outgoing.setHeader('content-type', 'text/javascript'); outgoing.end(await readFile(join(assets, incoming.url.slice(1)))); return;
  }
  outgoing.setHeader('content-type', 'text/html; charset=utf-8');
  outgoing.setHeader('set-cookie', 'remote-proof=' + credential + '; HttpOnly; SameSite=Strict; Path=/');
  if (incoming.url === '/native') {
    outgoing.end('<!doctype html><html lang="zh"><meta charset="utf-8"><title>原生会话引用试验</title>' + nativeHead
      + '<script type="importmap">{"imports":{"@hanger-source/dsh-reactive":"/core.js","@hanger-source/dsh-reactive/reactivity":"/reactivity.js","zod":"/zod.js"}}</script><button>验证原生会话引用归属</button><output>加载中</output><script type="module" src="/native-consumer.js"></script></html>'); return;
  }
  outgoing.end('<!doctype html><html lang="zh"><meta charset="utf-8"><title>远程 SDK 浏览器试验</title><script type="importmap">{"imports":{"@hanger-source/dsh-reactive":"/core.js"}}</script><button>执行真实远程调用</button><output>加载中</output><script type="module" src="/consumer.js"></script></html>');
});
app.on('upgrade', (incoming, downstream, head) => {
  const upstream = connect(Number(target.port), target.hostname, () => {
    upstream.write(incoming.method + ' ' + incoming.url + ' HTTP/' + incoming.httpVersion + '\r\n'
      + incoming.rawHeaders.reduce((lines, value, index, headers) => index % 2 ? lines : lines + value + ': ' + headers[index + 1] + '\r\n', '') + '\r\n');
    if (head.length) upstream.write(head);
    upstream.pipe(downstream); downstream.pipe(upstream);
  });
  upstream.on('error', () => downstream.destroy()); downstream.on('error', () => upstream.destroy());
});
app.listen(0, '127.0.0.1'); await once(app, 'listening'); origin = 'http://127.0.0.1:' + app.address().port;
console.log(JSON.stringify({ url: origin, nativeUrl: origin + '/native', coreEntry, browserClient, assets }));
let closing;
const close = () => closing ??= (async () => { await gateway.close(); await runtime.dispose(); app.closeAllConnections(); app.close(); })();
process.once('SIGINT', () => void close()); process.once('SIGTERM', () => void close());
