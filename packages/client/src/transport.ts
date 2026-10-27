import { RemoteError, type RemoteErrorCode } from '@deepseek-ai/dsh-typert-protocol';
import { RemoteStreamCarrierError } from '@deepseek-ai/dsh-api-gateway/client';
import { parseRemoteStreamServerMessage, REMOTE_STREAM_MUX_PATH, type RemoteStreamClientMessage } from '@deepseek-ai/dsh-api-gateway/stream-protocol';
import type { ClientTransportHooks, ConnectionWebSocketFactory } from '@deepseek-ai/dsh-client-connection/client';
import { Inbox } from './inbox.ts';
export { Inbox } from './inbox.ts';

/** fetch/socket factory 由应用接入认证；此层不产生或保管 owner token。 */
export interface AuthenticatedTransportOptions {
  readonly baseUrl: string;
  readonly fetch?: typeof fetch;
  readonly createWebSocket?: ConnectionWebSocketFactory;
  readonly inboxBytes?: number;
}

/** 将应用已认证 HTTP 与 socket 接到 Connection 的正式 transport hooks。 */
export function createAuthenticatedTransport(options: AuthenticatedTransportOptions): ClientTransportHooks {
  const base = new URL(options.baseUrl);
  if (!['http:', 'https:'].includes(base.protocol) || base.username || base.password || base.search || base.hash) throw new Error('REACTIVE_REMOTE_URL_INVALID');
  base.pathname = base.pathname.replace(/\/?$/u, '/');
  const request = options.fetch ?? globalThis.fetch;
  const socketUrl = new URL(REMOTE_STREAM_MUX_PATH.slice(1), base);
  socketUrl.protocol = socketUrl.protocol === 'https:' ? 'wss:' : 'ws:';
  const openSocket: ConnectionWebSocketFactory = options.createWebSocket ?? (url => new WebSocket(url));
  const inboxBytes = options.inboxBytes ?? 1024 * 1024;
  if (!Number.isSafeInteger(inboxBytes) || inboxBytes < 1) throw new Error('REACTIVE_REMOTE_INBOX_LIMIT_INVALID');
  return {
    fetch: (input, init) => request(new URL(new URL(input, 'http://logical.invalid').pathname.slice(1), base), init),
    openStream: (endpoint, payload, signal, uplink) => socketStream(openSocket, socketUrl.href, endpoint, payload, signal, inboxBytes, uplink),
  };
}

async function* socketStream(openSocket: ConnectionWebSocketFactory, url: string, endpoint: string, payload: unknown,
  signal: AbortSignal, inboxBytes: number, uplink?: AsyncIterable<unknown>): AsyncGenerator<unknown> {
  signal.throwIfAborted();
  const socket = openSocket(url);
  const inbox = new Inbox<unknown>(inboxBytes);
  const opened = Promise.withResolvers<void>();
  const streamId = '1';
  let terminal = false, stopped = false;
  const send = (message: RemoteStreamClientMessage): void => { socket.send(JSON.stringify(message)); };
  const abort = (): void => { stopped = true; opened.reject(signal.reason); inbox.fail(signal.reason); socket.close(); };
  const onOpen = (): void => opened.resolve();
  const detach = (): void => { socket.removeEventListener('close', onClose); socket.removeEventListener('error', onError); };
  const disconnected = (): void => {
    stopped = true;
    if (!terminal && !signal.aborted) {
      const error = new RemoteStreamCarrierError('Remote physical stream disconnected');
      opened.reject(error); inbox.fail(error);
    }
  };
  const onClose = (): void => { disconnected(); detach(); };
  const onError = (): void => disconnected();
  const onMessage = (event: { readonly data: unknown }): void => {
    try {
      if (typeof event.data !== 'string') throw new Error('REACTIVE_REMOTE_TEXT_FRAME_REQUIRED');
      const frame = parseRemoteStreamServerMessage(event.data);
      if (frame.streamId !== streamId) throw new Error('REACTIVE_REMOTE_STREAM_ID_INVALID');
      if (frame.type === 'item') inbox.push(frame.value, new TextEncoder().encode(event.data).byteLength);
      else if (frame.type === 'end') { stopped = true; terminal = true; inbox.end(); }
      else {
        stopped = true; terminal = true;
        // Gateway 保留新 Host 的扩展错误码；wire 已由官方 parser 验证，重建正式 RemoteError。
        inbox.fail(new RemoteError(frame.error.code as RemoteErrorCode, frame.error.message, frame.error.details));
      }
    } catch (error) { stopped = true; terminal = true; inbox.fail(error); }
  };
  socket.addEventListener('open', onOpen);
  socket.addEventListener('close', onClose);
  socket.addEventListener('error', onError);
  socket.addEventListener('message', onMessage);
  signal.addEventListener('abort', abort, { once: true });
  const iterator = uplink?.[Symbol.asyncIterator]();
  let pumping: Promise<void> | undefined;
  try {
    signal.throwIfAborted(); await opened.promise; signal.throwIfAborted();
    send({ type: 'open', streamId, endpoint, payload });
    pumping = (async (): Promise<void> => {
      try {
        if (iterator) for (;;) {
          const item = await iterator.next();
          if (stopped || item.done) break;
          send({ type: 'item', streamId, value: item.value });
        }
        if (!stopped) send({ type: 'end', streamId });
      } catch (error) { if (!stopped) inbox.fail(error); }
    })();
    yield* inbox;
  } finally {
    stopped = true;
    if (!terminal && socket.readyState === 1) send({ type: 'cancel', streamId });
    if (iterator?.return) void iterator.return().catch(() => {});
    void pumping?.catch(() => {});
    signal.removeEventListener('abort', abort);
    socket.removeEventListener('open', onOpen); socket.removeEventListener('message', onMessage);
    inbox.end();
    // ws 在 CONNECTING 关闭时异步发出 error；一直持有错误监听到真实 close。
    if (socket.readyState === 3) detach(); else socket.close();
  }
}
