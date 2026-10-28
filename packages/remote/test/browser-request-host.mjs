import { createServer, request as forward } from 'node:http';
import { connect } from 'node:net';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { mkdtemp, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';
import { LlmAdapter } from '@deepseek-ai/dsh-llm';
import { SessionId } from '@deepseek-ai/dsh-session';
import { defineApplication, defineEnvironment } from '@hanger-source/dsh-reactive';
import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { createRuntime, nativeModel } from '@hanger-source/dsh-reactive-dsh';
import { mountProgramGateway, RemoteAuthenticationError } from '@hanger-source/dsh-reactive-remote';
import { program } from './browser-request-contracts.mjs';

// 从当前消费者安装的公开包入口构建；不读取仓库 dist 或源码。
const assets = await mkdtemp(join(tmpdir(), 'dsh-browser-request-assets-'));
const coreEntry = fileURLToPath(import.meta.resolve('@hanger-source/dsh-reactive'));
const clientManifest = new URL(import.meta.resolve('@hanger-source/dsh-reactive-client/package.json'));
const clientPackage = JSON.parse(await readFile(clientManifest, 'utf8'));
const clientBrowserEntry = fileURLToPath(new URL(clientPackage.exports['.'].browser, clientManifest));
let browserClient;
for (const [entry, name, external] of [[coreEntry, 'core.js', []], [fileURLToPath(new URL('./browser-request-consumer.mjs', import.meta.url)), 'consumer.js', ['@hanger-source/dsh-reactive']]]) {
  const artifact = await build({ entryPoints: [entry], outfile: join(assets, name), bundle: true, format: 'esm', platform: 'browser', target: 'es2024',
    external, metafile: true, define: { 'process.env.NODE_ENV': '"production"' } });
  for (const output of Object.values(artifact.metafile.outputs)) for (const dependency of output.imports) {
    if (dependency.path !== '@hanger-source/dsh-reactive') throw new Error('BROWSER_NODE_OR_EXTERNAL_IMPORT:' + dependency.path);
  }
  if (name === 'consumer.js') {
    browserClient = Object.keys(artifact.metafile.inputs).find(input => resolve(input) === clientBrowserEntry);
    if (!browserClient) throw new Error('BROWSER_PUBLIC_CLIENT_FACE_REQUIRED');
  }
}
const home = await mkdtemp(join(tmpdir(), 'dsh-browser-request-'));
const nextChunk = Promise.withResolvers(), finish = Promise.withResolvers();
const errors = [];
let models = 0;
const definition = defineApplication({ name: 'browser-requests', admit: async (_input, _identity, access) => {
  if (access.actorId !== 'browser-request-participant') throw new Error('BROWSER_REQUEST_ACTOR_DENIED');
}, environments: [defineEnvironment({ id: 'work',
  instructions: '核验本次真实输入', program: implementProgram(program, { Component: () => null }) })] });
const runtime = await createRuntime(() => definition, { home: join(home, 'home'), tools: 'native', onError: event => errors.push(String(event.error)),
  model: nativeModel({ provider: 'browser-requests', model: 'test', create: () => new class extends LlmAdapter {
    async *stream() {
      models++;
      yield { type: 'block-start', index: 0, blockType: 'text' };
      yield { type: 'text-delta', index: 0, text: '浏览器当前前缀' };
      await nextChunk.promise;
      yield { type: 'text-delta', index: 0, text: '与真实后续增量' };
      await finish.promise;
      yield { type: 'block-end', index: 0, block: { type: 'text', text: '浏览器当前前缀与真实后续增量' } };
      yield { type: 'finish', reason: { kind: 'stop' } };
    }
  }() }),
});
const workspace = await runtime.application.createWorkspace({ directory: home, title: '浏览器输入工作目录' });
const local = await runtime.application.create('work', { workspaceId: workspace.workspaceId }), sessionId = local.id;
const facts = { local: [], raw: [] };
const subscriptions = Object.entries({ local: local.events$, raw: local.rawEvents$ }).map(([kind, source]) => source.subscribe({
  next: event => {
    if (event.kind !== 'assistant.message') return;
    facts[kind].push(...event.inputs.map(input => ({ requestId: input.requestId, sourceSessionId: input.sourceSessionId,
      actorId: input.actorId, domainDate: input.scope.at instanceof Date, at: input.scope.at })));
  }, error: error => errors.push(String(error)),
}));
await runtime.application.close(sessionId);
const credential = randomUUID();
let origin;
const gateway = await mountProgramGateway(runtime.native, { application: runtime.application,
  model: { provider: 'browser-requests', model: 'test' }, participant: principal => ({ participant: 'human', actorId: principal.id }),
  access: service => service.registerPolicy('browser-request-peer', { bindings: () => [{ role: { name: 'request-reader', resource: 'session',
    actions: ['reactivePrograms/open', 'reactivePrograms/questions', 'reactivePrograms/progress', 'reactivePrograms/journal',
      'reactivePrograms/journalPage', 'reactivePrograms/resume', 'reactivePrograms/submit'] }, resource: { kind: 'session', id: sessionId } }] }),
  carrier: { async authenticate(request) {
    if (!request.headers.get('cookie')?.split(';').some(part => part.trim() === 'request-proof=' + credential)
      || (request.headers.has('origin') && request.headers.get('origin') !== origin)) throw new RemoteAuthenticationError(401);
    return { kind: 'browser-request-peer', id: 'browser-request-participant' };
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
  if (incoming.url === '/request-info' || incoming.url === '/proof' || incoming.url === '/request-next' || incoming.url === '/request-finish') {
    if (incoming.url === '/request-next') nextChunk.resolve();
    if (incoming.url === '/request-finish') finish.resolve();
    outgoing.setHeader('content-type', 'application/json');
    outgoing.end(JSON.stringify({ workspaceId: workspace.workspaceId, sessionId, models, agentPresent: runtime.native.agents.get(SessionId(sessionId)) !== undefined, facts, errors })); return;
  }
  if (incoming.url === '/core.js' || incoming.url === '/consumer.js') {
    outgoing.setHeader('content-type', 'text/javascript'); outgoing.end(await readFile(join(assets, incoming.url.slice(1)))); return;
  }
  outgoing.setHeader('content-type', 'text/html; charset=utf-8');
  outgoing.setHeader('set-cookie', 'request-proof=' + credential + '; HttpOnly; SameSite=Strict; Path=/');
  outgoing.end('<!doctype html><html lang="zh"><meta charset="utf-8"><title>请求进展浏览器试验</title><script type="importmap">{"imports":{"@hanger-source/dsh-reactive":"/core.js"}}</script><button id="requests">验证真实请求进展</button><output id="status">加载中</output><script type="module" src="/consumer.js"></script></html>');
});
app.on('upgrade', (incoming, downstream, head) => {
  const upstream = connect(Number(target.port), target.hostname, () => {
    upstream.write(incoming.method + ' ' + incoming.url + ' HTTP/' + incoming.httpVersion + '\r\n'
      + incoming.rawHeaders.reduce((lines, value, index, headers) => index % 2 ? lines : lines + value + ': ' + headers[index + 1] + '\r\n', '') + '\r\n');
    if (head.length) upstream.write(head); upstream.pipe(downstream); downstream.pipe(upstream);
  });
  upstream.on('error', () => downstream.destroy()); downstream.on('error', () => upstream.destroy());
});
app.listen(0, '127.0.0.1'); await once(app, 'listening'); origin = 'http://127.0.0.1:' + app.address().port;
console.log(JSON.stringify({ url: origin, coreEntry, browserClient, assets }));
let closing;
const close = () => closing ??= (async () => {
  nextChunk.resolve(); finish.resolve(); subscriptions.forEach(subscription => subscription.unsubscribe()); local.release();
  await gateway.close(); await runtime.dispose(); app.closeAllConnections(); app.close();
})();
process.once('SIGINT', () => void close()); process.once('SIGTERM', () => void close());
