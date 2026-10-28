import { createServer, request as forward } from 'node:http';
import { connect } from 'node:net';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { extname, isAbsolute, join, relative, resolve } from 'node:path';
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
const clientManifest = new URL(import.meta.resolve('@hanger-source/dsh-reactive-client/package.json'));
const clientPackage = JSON.parse(await readFile(clientManifest, 'utf8'));
const clientBrowserEntry = fileURLToPath(new URL(clientPackage.exports['.'].browser, clientManifest));
let browserClient;
const browserAssets = new Set();
const coreArtifact = await build({ entryPoints: { core: coreEntry }, outdir: assets,
  bundle: true, splitting: true, format: 'esm', platform: 'browser', target: 'es2024', metafile: true,
  define: { 'process.env.NODE_ENV': '"production"' } });
for (const file of Object.keys(coreArtifact.metafile.outputs)) browserAssets.add('/' + relative(assets, resolve(file)));
const artifact = await build({ entryPoints: [fileURLToPath(new URL('./browser-consumer.mjs', import.meta.url))], outfile: join(assets, 'consumer.js'), bundle: true,
  format: 'esm', platform: 'browser', target: 'es2024', external: ['@hanger-source/dsh-reactive'], metafile: true, define: { 'process.env.NODE_ENV': '"production"' },
  logOverride: { 'import-is-undefined': 'error' } });
browserAssets.add('/consumer.js');
for (const output of Object.values(artifact.metafile.outputs)) for (const dependency of output.imports) {
  if (dependency.path !== '@hanger-source/dsh-reactive') throw new Error('BROWSER_NODE_OR_EXTERNAL_IMPORT:' + dependency.path);
}
browserClient = Object.keys(artifact.metafile.inputs).find(input => resolve(input) === clientBrowserEntry);
if (!browserClient) throw new Error('BROWSER_PUBLIC_CLIENT_FACE_REQUIRED');
const nativeAssetsArgument = process.argv.slice(2).find(value => value.startsWith('--native-assets='));
let nativeAssets, nativeManifest, nativeHead = '';
if (nativeAssetsArgument) {
  nativeAssets = nativeAssetsArgument.slice('--native-assets='.length);
  if (!isAbsolute(nativeAssets)) throw new Error('NATIVE_ASSETS_ABSOLUTE_DIRECTORY_REQUIRED');
  nativeAssets = resolve(nativeAssets);
  nativeManifest = JSON.parse(await readFile(join(nativeAssets, 'manifest.json'), 'utf8'));
  const { entry, bootstrap, modules, files } = nativeManifest;
  if (typeof entry !== 'string' || typeof bootstrap !== 'string' || !Array.isArray(modules) || !modules.length
    || !modules.every(id => typeof id === 'string') || !modules.includes('@deepseek-ai/dsh-client-ui-renderer')
    || !Array.isArray(files) || !files.includes(entry) || !files.includes(bootstrap)) throw new Error('NATIVE_ASSETS_MANIFEST_INVALID');
  for (const file of files) {
    if (typeof file !== 'string' || !file || isAbsolute(file) || file.includes('\\') || file.split('/').some(part => part === '..' || !part))
      throw new Error('NATIVE_ASSETS_FILE_INVALID:' + file);
  }
  const bootstrapUrl = '/native-assets/' + bootstrap;
  const graph = { rev: 'native-ui-renderer-rc3', entries: modules.map(id => ({ id, url: bootstrapUrl, rev: 'rc3' })),
    batches: [{ phase: 'bootstrap', url: bootstrapUrl, rev: 'rc3', entries: modules }] };
  nativeHead = bootInjections(graph).map(row => {
    if (row.kind === 'script') return '<script>' + row.text + '</script>';
    if (row.kind === 'script-src') return '<script src="' + row.src + '"></script>';
    if (row.kind === 'global') return '<script>window[' + JSON.stringify(row.name) + ']=' + JSON.stringify(row.value) + '</script>';
    throw new Error('UNEXPECTED_NATIVE_BOOT_INJECTION:' + row.kind);
  }).join('');
}
const home = await mkdtemp(join(tmpdir(), 'dsh-reactive-browser-'));
const current = new BehaviorSubject(new Date('2026-10-26T00:00:00Z'));
let executions = 0, queries = 0, models = 0, parsedAmount, nativeActionGate;
const owner = implementOperation(operation, { locate: () => ({ status: 'unknown' }), observe() {
  queries++; return of({ status: 'ready', generation: 0, observedAt: Date.now(), value: { phase: 'done', at: '2026-10-26T00:00:08Z' } });
} });
const implementation = implementAction(check, { operationOwner: owner, async execute(input, call) {
  if (call.source.actorId !== 'browser-participant') throw new Error('WRONG_AUTHENTICATED_ACTOR');
  executions++; parsedAmount = input.amount;
  if (input.amount === 13) throw new RemoteError('test/revision-conflict', '订单版本已变化', { orderId: 'order-7', expectedRevision: 7, actualRevision: 8 });
  const gate = nativeActionGate;
  if (gate) {
    gate.entered.resolve();
    await gate.release.promise;
    call.signal.throwIfAborted();
    if (nativeActionGate === gate) nativeActionGate = undefined;
  }
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
  model: { provider: 'browser-proof', model: 'unused' }, participant: principal => ({ participant: 'human', actorId: principal.id }),
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
  if (incoming.url?.startsWith('/native-action/')) {
    if (!nativeAssets) { outgoing.writeHead(503); outgoing.end('NATIVE_ASSETS_REQUIRED'); return; }
    outgoing.setHeader('content-type', 'application/json');
    if (incoming.url === '/native-action/hold' && incoming.method === 'POST' && !nativeActionGate) {
      nativeActionGate = { entered: Promise.withResolvers(), release: Promise.withResolvers() };
    } else if (incoming.url === '/native-action/entered' && incoming.method === 'GET' && nativeActionGate) {
      await nativeActionGate.entered.promise;
    } else if (incoming.url === '/native-action/release' && incoming.method === 'POST' && nativeActionGate) {
      nativeActionGate.release.resolve();
    } else { outgoing.writeHead(409); outgoing.end(JSON.stringify({ error: 'NATIVE_ACTION_GATE_INVALID' })); return; }
    outgoing.end(JSON.stringify({ ok: true })); return;
  }
  if (incoming.url?.startsWith('/native-assets/')) {
    const file = incoming.url.slice('/native-assets/'.length);
    if (!nativeAssets || !nativeManifest.files.includes(file)) { outgoing.writeHead(404); outgoing.end('NATIVE_ASSET_NOT_FOUND'); return; }
    outgoing.setHeader('content-type', ({ '.js': 'text/javascript', '.css': 'text/css', '.json': 'application/json',
      '.woff': 'font/woff', '.woff2': 'font/woff2', '.ttf': 'font/ttf', '.svg': 'image/svg+xml' })[extname(file)] ?? 'application/octet-stream');
    outgoing.end(await readFile(join(nativeAssets, file))); return;
  }
  if (browserAssets.has(incoming.url)) {
    outgoing.setHeader('content-type', 'text/javascript'); outgoing.end(await readFile(join(assets, incoming.url.slice(1)))); return;
  }
  outgoing.setHeader('content-type', 'text/html; charset=utf-8');
  outgoing.setHeader('set-cookie', 'remote-proof=' + credential + '; HttpOnly; SameSite=Strict; Path=/');
  if (incoming.url === '/native') {
    if (!nativeAssets) { outgoing.writeHead(503); outgoing.end('<!doctype html><html lang="zh"><meta charset="utf-8"><title>缺少原生 UI 产物</title><output>NATIVE_ASSETS_REQUIRED：请通过 --native-assets=/绝对路径/UIprofile/browser/native 传入正式 React18 浏览器产物。</output></html>'); return; }
    outgoing.end('<!doctype html><html lang="zh"><meta charset="utf-8"><title>原生 Slot 与会话引用试验</title>' + nativeHead
      + nativeManifest.files.filter(file => extname(file) === '.css').map(file => '<link rel="stylesheet" href="/native-assets/' + file + '">').join('')
      + '<button>验证原生 Slot 与会话引用</button><output>加载中</output><main id="native-renderer"></main><script type="module" src="/native-assets/' + nativeManifest.entry + '"></script></html>'); return;
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
console.log(JSON.stringify({ url: origin, nativeUrl: origin + '/native', coreEntry, browserClient, assets, nativeAssets }));
let closing;
const close = () => closing ??= (async () => { nativeActionGate?.release.resolve(); await gateway.close(); await runtime.dispose(); app.closeAllConnections(); app.close(); })();
process.once('SIGINT', () => void close()); process.once('SIGTERM', () => void close());
