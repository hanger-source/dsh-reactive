import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { once } from 'node:events';
import { Readable } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import type { Context } from '@deepseek-ai/cordis';
import type {} from '@deepseek-ai/dsh-client-connection';
import type {} from '@deepseek-ai/dsh-api-gateway';
import type { Principal } from '@deepseek-ai/dsh-typert-protocol';
import { parseRemoteStreamClientMessage, REMOTE_STREAM_MUX_PATH, type RemoteStreamServerMessage } from '@deepseek-ai/dsh-api-gateway/stream-protocol';
import { WebSocketServer, WebSocket } from 'ws';
import { Inbox } from '@hanger-source/dsh-reactive-client/inbox';

/** 应用认证及 Host/Origin trust 边界；返回身份来自宿主，不来自 Remote 参数。 */
export interface RemoteCarrierOptions {
  authenticate(request: Request): Promise<Principal>;
  readonly host?: string;
  readonly port?: number;
  readonly maxMessageBytes?: number;
  readonly streamInboxBytes?: number;
  onError?(error: unknown): void;
}
export interface RemoteCarrier {
  readonly url: string;
  close(): Promise<void>;
}
/** 只有认证 owner 可以将一次拒绝标记为 HTTP 401 或 403。 */
export class RemoteAuthenticationError extends Error {
  constructor(readonly status: 401 | 403) { super('REACTIVE_REMOTE_AUTHENTICATION_FAILED'); }
}

/** 已认证 unary 直接复用 Connection 的 envelope、参数和错误处理。 */
export function createAuthenticatedHandler(ctx: Context, authenticate: RemoteCarrierOptions['authenticate']): (request: Request) => Promise<Response> {
  const handler = ctx.connection.createSharedFetchHandler('/api');
  return async request => {
    const principal = await authenticate(request);
    request.signal.throwIfAborted();
    const peer = ctx.connection.createPeer({ principal, interactions: 'answer' });
    try { return await handler.fetch(request, peer); }
    finally { await peer.dispose(); }
  };
}

function headers(incoming: IncomingMessage): Headers {
  const result = new Headers();
  for (let index = 0; index < incoming.rawHeaders.length; index += 2) result.append(incoming.rawHeaders[index]!, incoming.rawHeaders[index + 1]!);
  return result;
}
function requestOf(incoming: IncomingMessage, signal: AbortSignal, body: boolean, maxBytes: number): Request {
  const init: RequestInit & { duplex?: 'half' } = { method: incoming.method, headers: headers(incoming), signal };
  if (body && incoming.method !== 'GET' && incoming.method !== 'HEAD') {
    let received = 0;
    init.body = (Readable.toWeb(incoming) as ReadableStream<Uint8Array>).pipeThrough(new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller): void {
        received += chunk.byteLength;
        if (received > maxBytes) throw new Error('REACTIVE_REMOTE_REQUEST_TOO_LARGE');
        controller.enqueue(chunk);
      },
    }));
    init.duplex = 'half';
  }
  if (!incoming.headers.host) throw new Error('REACTIVE_REMOTE_HOST_REQUIRED');
  return new Request(new URL(incoming.url ?? '/', 'http://' + incoming.headers.host), init);
}
function send(socket: WebSocket, frame: RemoteStreamServerMessage): Promise<void> {
  return new Promise<void>((resolve, reject) => socket.send(JSON.stringify(frame), error => error ? reject(error) : resolve()));
}
async function respond(outgoing: ServerResponse, response: Response, signal: AbortSignal): Promise<void> {
  outgoing.statusCode = response.status;
  response.headers.forEach((value, key) => outgoing.setHeader(key, value));
  if (response.body) await pipeline(Readable.fromWeb(response.body as import('node:stream/web').ReadableStream<Uint8Array>), outgoing, { signal });
  else outgoing.end();
}

/**
 * 已认证物理 carrier。每条 socket 承载一个可取消逻辑流；Gateway 仍拥有业务
 * 校验和 uplink 解码，可靠 snapshot/journal 仍归公开 Client stream 类。
 */
export async function serveRemoteCarrier(ctx: Context, options: RemoteCarrierOptions): Promise<RemoteCarrier> {
  const maxMessageBytes = options.maxMessageBytes ?? 1024 * 1024;
  const streamInboxBytes = options.streamInboxBytes ?? 262144;
  if (!Number.isSafeInteger(maxMessageBytes) || maxMessageBytes < 1
    || !Number.isSafeInteger(streamInboxBytes) || streamInboxBytes < 1) throw new Error('REACTIVE_REMOTE_CARRIER_LIMIT_INVALID');
  const report = options.onError ?? ((error: unknown): void => console.error('reactive.remote.carrier.failed', error));
  const handler = ctx.connection.createSharedFetchHandler('/api');
  const lifetime = new AbortController();
  const pending = new Set<Promise<void>>();
  const server = createServer((incoming, outgoing) => {
    const controller = new AbortController();
    const abort = (): void => controller.abort();
    incoming.once('aborted', abort); outgoing.once('close', abort);
    const operation = (async (): Promise<void> => {
      let peer: ReturnType<Context['connection']['createPeer']> | undefined;
      try {
        const signal = AbortSignal.any([lifetime.signal, controller.signal]);
        const admission = requestOf(incoming, signal, false, maxMessageBytes);
        const path = new URL(admission.url).pathname;
        if (path !== '/api' && !path.startsWith('/api/')) { outgoing.writeHead(404); outgoing.end(); return; }
        const principal = await options.authenticate(admission);
        signal.throwIfAborted();
        peer = ctx.connection.createPeer({ principal, interactions: 'answer' });
        const request = requestOf(incoming, signal, true, maxMessageBytes);
        await respond(outgoing, await handler.fetch(request, peer), signal);
      } catch (error) {
        if (!controller.signal.aborted && !lifetime.signal.aborted) {
          if (!(error instanceof RemoteAuthenticationError)) report(error);
          if (!outgoing.headersSent) { outgoing.writeHead(error instanceof RemoteAuthenticationError ? error.status : 500); outgoing.end(); }
          else outgoing.destroy(error instanceof Error ? error : undefined);
        }
      } finally {
        controller.abort(); incoming.removeListener('aborted', abort); outgoing.removeListener('close', abort); await peer?.dispose();
      }
    })();
    pending.add(operation); void operation.then(() => pending.delete(operation), error => { pending.delete(operation); report(error); });
  });
  const sockets = new WebSocketServer({ noServer: true, maxPayload: maxMessageBytes });
  server.on('upgrade', (incoming, socket, head) => {
    const operation = (async (): Promise<void> => {
      let peer: ReturnType<Context['connection']['createPeer']> | undefined;
      try {
        const request = requestOf(incoming, lifetime.signal, false, maxMessageBytes);
        if (new URL(request.url).pathname !== REMOTE_STREAM_MUX_PATH) throw new Error('REACTIVE_REMOTE_ROUTE_NOT_FOUND');
        const principal = await options.authenticate(request);
        lifetime.signal.throwIfAborted();
        if (socket.destroyed) return;
        peer = ctx.connection.createPeer({ principal, interactions: 'answer' });
        const admitted = peer;
        sockets.handleUpgrade(incoming, socket, head, connected => {
          const serving = serveSocket(ctx, connected, admitted, lifetime.signal, streamInboxBytes).catch(report);
          pending.add(serving); void serving.then(() => pending.delete(serving), error => { pending.delete(serving); report(error); });
        });
      } catch (error) {
        await peer?.dispose();
        if (!lifetime.signal.aborted && !(error instanceof RemoteAuthenticationError)) report(error);
        const status = error instanceof RemoteAuthenticationError ? error.status : 500;
        socket.end('HTTP/1.1 ' + status + ' Rejected\r\nConnection: close\r\nContent-Length: 0\r\n\r\n');
      }
    })();
    pending.add(operation); void operation.then(() => pending.delete(operation), error => { pending.delete(operation); report(error); });
  });
  server.listen(options.port ?? 0, options.host ?? '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('REACTIVE_REMOTE_ADDRESS_INVALID');
  const hostname = address.address.includes(':') ? '[' + address.address + ']' : address.address;
  let closing: Promise<void> | undefined;
  const carrier: RemoteCarrier = {
    url: 'http://' + hostname + ':' + address.port,
    close(): Promise<void> {
      return closing ??= (async (): Promise<void> => {
        lifetime.abort();
        for (const socket of sockets.clients) socket.terminate();
        sockets.close();
        await new Promise<void>((resolve, reject) => { server.close(error => error ? reject(error) : resolve()); server.closeAllConnections(); });
        await Promise.all([...pending]);
      })();
    },
  };
  ctx.effect(() => () => carrier.close());
  return carrier;
}

async function serveSocket(ctx: Context, socket: WebSocket, peer: ReturnType<Context['connection']['createPeer']>, lifetime: AbortSignal,
  streamInboxBytes: number): Promise<void> {
  const controller = new AbortController();
  const signal = AbortSignal.any([lifetime, controller.signal]);
  const inbox = new Inbox<unknown>(streamInboxBytes);
  const opened = Promise.withResolvers<{ streamId: string; endpoint: string; payload: unknown }>();
  let streamId: string | undefined;
  let ended = false;
  const abort = (): void => { inbox.fail(signal.reason); opened.reject(signal.reason); socket.close(); };
  signal.addEventListener('abort', abort, { once: true });
  socket.on('close', () => controller.abort());
  socket.on('error', error => controller.abort(error));
  socket.on('message', (bytes, binary) => {
    try {
      if (binary) throw new Error('REACTIVE_REMOTE_TEXT_FRAME_REQUIRED');
      const data = Array.isArray(bytes) ? Buffer.concat(bytes) : Buffer.isBuffer(bytes) ? bytes : Buffer.from(bytes);
      const message = parseRemoteStreamClientMessage(data.toString('utf8'));
      if (message.type === 'open') {
        if (streamId !== undefined) throw new Error('REACTIVE_REMOTE_DUPLICATE_OPEN');
        streamId = message.streamId; opened.resolve(message); return;
      }
      if (message.streamId !== streamId) throw new Error('REACTIVE_REMOTE_STREAM_ID_INVALID');
      if (message.type === 'cancel') { controller.abort(); return; }
      if (message.type === 'end') { ended = true; inbox.end(); return; }
      if (ended) throw new Error('REACTIVE_REMOTE_UPLINK_ENDED');
      inbox.push(message.value, data.byteLength);
    } catch (error) {
      if (streamId !== undefined && socket.readyState === WebSocket.OPEN) {
        void send(socket, { type: 'error', streamId, error: ctx.typertGateway.wireStream.failure(error) })
          .finally(() => controller.abort(error)).catch(() => {});
      } else controller.abort(error);
    }
  });
  try {
    signal.throwIfAborted();
    const request = await opened.promise;
    const downlink = await ctx.typertGateway.wireStream.open(request.endpoint, request.payload, inbox, peer, signal);
    for await (const value of downlink) { signal.throwIfAborted(); await send(socket, { type: 'item', streamId: request.streamId, value }); }
    if (socket.readyState === WebSocket.OPEN) await send(socket, { type: 'end', streamId: request.streamId });
  } catch (error) {
    if (streamId !== undefined && socket.readyState === WebSocket.OPEN && !signal.aborted)
      await send(socket, { type: 'error', streamId, error: ctx.typertGateway.wireStream.failure(error) });
  } finally {
    signal.removeEventListener('abort', abort); inbox.end(); socket.close(); await peer.dispose();
  }
}
