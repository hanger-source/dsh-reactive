import assert from 'node:assert/strict';
import { readFile, realpath, lstat } from 'node:fs/promises';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { createRequire } from 'node:module';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { WebSocketServer } from 'ws';
import { checkDeveloperSkill } from './consumer-skill.mjs';

/** 同一物理安装的 Node 或 browser 入口共同面对实际邮箱、HTTP 和 WebSocket。 */
export async function checkClientEntries(directory, entries) {
  if (!entries) {
    const require = createRequire(join(directory, 'package.json'));
    const load = async suffix => import(pathToFileURL(require.resolve('@hanger-source/dsh-reactive-client' + suffix)).href);
    const [client, errors, inbox, transport, types, remote] = await Promise.all(['', '/errors', '/inbox', '/transport', '/types', '/remote'].map(load));
    entries = { client, errors, inbox, transport, types, remote };
  }
  const { client, errors, inbox, transport, remote } = entries;
  assert.equal(client.createAuthenticatedTransport, transport.createAuthenticatedTransport);
  assert.equal(client.Inbox, inbox.Inbox); assert.equal(transport.Inbox, inbox.Inbox);
  assert.equal(client.decodeProgramFailure, errors.decodeProgramFailure);
  const cause = new Error('receipt connection ended');
  const unknown = new client.FileUploadReceiptUnknownError('client-entries', cause);
  assert.ok(unknown instanceof errors.FileUploadReceiptUnknownError); assert.equal(unknown.cause, cause);

  const queue = new inbox.Inbox(8), values = queue[Symbol.asyncIterator]();
  assert.ok(queue instanceof client.Inbox && queue instanceof transport.Inbox);
  const waiting = values.next(); queue.push('first', 5);
  assert.deepEqual(await waiting, { value: 'first', done: false });
  queue.push('next', 4); queue.end();
  assert.deepEqual(await values.next(), { value: 'next', done: false });
  assert.equal((await values.next()).done, true);
  const bounded = new client.Inbox(1), failed = bounded[Symbol.asyncIterator]().next();
  let overflow;
  assert.throws(() => bounded.push('too large', 2), error => { overflow = error; return true; });
  await assert.rejects(failed, error => error === overflow);

  const method = remote.TYPERT_REMOTE.methods['reactivePrograms/open'];
  const schema = method.input().create();
  const request = { request: { sessionId: 'client-entries', program: { name: 'entry-contract', version: 1 } } };
  assert.deepEqual(schema.parse(request), request);
  assert.equal(method.input().create(), schema);
  assert.equal(schema.safeParse({ request: { sessionId: 1, program: request.request.program } }).success, false);

  const frames = [], failures = [];
  const server = createServer((request, response) => {
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify({ path: request.url, authorization: request.headers.authorization }));
  });
  const sockets = new WebSocketServer({ server, path: '/entries/api/remote.mux' });
  sockets.on('connection', socket => socket.on('message', bytes => {
    try {
      const frame = JSON.parse(bytes.toString()); frames.push(frame);
      if (frame.type !== 'open') return;
      if (frame.endpoint === 'consumer/disconnect') { socket.close(); return; }
      socket.send(JSON.stringify({ type: 'item', streamId: frame.streamId, value: frame.payload }));
      if (frame.endpoint === 'consumer/echo') socket.send(JSON.stringify({ type: 'end', streamId: frame.streamId }));
    } catch (error) { failures.push(error); socket.close(); }
  }));
  server.listen(0, '127.0.0.1');
  try {
    await once(server, 'listening');
    const address = server.address(); assert.ok(address && typeof address !== 'string');
    const options = { baseUrl: `http://127.0.0.1:${address.port}/entries/`,
      fetch: (url, init) => fetch(url, { ...init, headers: { ...Object.fromEntries(new Headers(init?.headers)), authorization: 'Bearer entry-consumer' } }),
    };
    const selected = transport.createAuthenticatedTransport(options);
    const response = await selected.fetch('/echo', { signal: AbortSignal.timeout(5000) });
    assert.deepEqual(await response.json(), { path: '/entries/echo', authorization: 'Bearer entry-consumer' });
    const echo = [];
    for await (const value of selected.openStream('consumer/echo', { value: 'through-real-socket' }, AbortSignal.timeout(5000))) echo.push(value);
    assert.deepEqual(echo, [{ value: 'through-real-socket' }]);

    const controller = new AbortController(), reason = new Error('entry-consumer stopped waiting');
    const held = selected.openStream('consumer/hold', { value: 'still-open' }, controller.signal)[Symbol.asyncIterator]();
    assert.deepEqual(await held.next(), { value: { value: 'still-open' }, done: false });
    controller.abort(reason);
    await assert.rejects(held.next(), error => error === reason);
    const disconnected = selected.openStream('consumer/disconnect', {}, AbortSignal.timeout(5000))[Symbol.asyncIterator]();
    await assert.rejects(disconnected.next(), error => error?.name === 'RemoteStreamCarrierError');
    assert.equal(frames.filter(frame => frame.type === 'open').length, 3);
    assert.deepEqual(failures, []);
    return { inbox: true, socket: true, identities: true, schema: true };
  } finally {
    for (const socket of sockets.clients) socket.terminate();
    await new Promise((done, reject) => sockets.close(error => error ? reject(error) : done()));
    server.closeAllConnections();
    await new Promise((done, reject) => server.close(error => error ? reject(error) : done()));
  }
}

async function main() {
  const root = dirname(fileURLToPath(import.meta.url));
  const clientEntries = await checkClientEntries(root);
  if (process.argv.includes('--client-entries')) { console.log(JSON.stringify({ clientEntries })); return; }
  const expected = JSON.parse(await readFile(new URL('./expected.json', import.meta.url), 'utf8'));
  for (const name of expected.packages) {
    const directory = join(root, 'node_modules', name);
    assert.equal((await lstat(directory)).isSymbolicLink(), false, name + ' 必须为物理安装');
    assert.equal(await realpath(directory), directory);
    const pkg = JSON.parse(await readFile(join(directory, 'package.json'), 'utf8'));
    const receipt = JSON.parse(await readFile(join(directory, 'release.json'), 'utf8'));
    assert.equal(pkg.version, expected.version);
    assert.equal(receipt.package, name); assert.equal(receipt.version, expected.version);
    assert.equal(receipt.commit, expected.commit); assert.equal(receipt.tag, expected.tag);
    assert.equal(receipt.registry, expected.registry);
    const imported = await import(name);
    assert.ok(Object.keys(imported).length > 0, name + ' 公开入口必须实际加载');
  }
  const developerSkill = await checkDeveloperSkill(expected.version);
  console.log(JSON.stringify({ kind: expected.kind, source: expected.source, commit: expected.commit,
    version: expected.version, packages: expected.packages, developerSkill, clientEntries }));
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await main();
