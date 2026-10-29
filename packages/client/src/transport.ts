/// <reference lib="esnext.disposable" preserve="true" />

import { RemoteError, type RemoteErrorCode } from '@deepseek-ai/dsh-typert-protocol';
import { RemoteStreamCarrierError } from '@deepseek-ai/dsh-api-gateway/client';
import { parseRemoteStreamServerMessage, REMOTE_STREAM_MUX_PATH, type RemoteStreamClientMessage } from '@deepseek-ai/dsh-api-gateway/stream-protocol';
import type { ClientTransportHooks, ConnectionWebSocketFactory } from '@deepseek-ai/dsh-client-connection/client';
import { Inbox } from './contracts/inbox.ts';
export { Inbox } from './contracts/inbox.ts';

/** fetch/socket factory 由应用接入认证；此层不产生或保管 owner token。 */
export interface AuthenticatedTransportOptions {
  readonly baseUrl: string;
  readonly fetch?: typeof fetch;
  readonly createWebSocket?: ConnectionWebSocketFactory;
  readonly inboxBytes?: number;
}
/**
 * 本实例拥有所创建的 socket/uplink；退出禁止新 HTTP/流，等待已启动物理流的实际清理。
 * 已分派 HTTP 仍由原请求 signal 和消费者拥有；dispose 不取消它，也不接管其响应体。
 * 活跃请求的业务错误归该流；实际退出中的迭代、发送与释放失败保留至 dispose。
 */
export interface AuthenticatedTransport extends ClientTransportHooks, AsyncDisposable {
  dispose(): Promise<void>;
}

/** 将应用已认证 HTTP 与 socket 接到 Connection 的正式 transport hooks。 */
export function createAuthenticatedTransport(options: AuthenticatedTransportOptions): AuthenticatedTransport {
  const base = new URL(options.baseUrl);
  if (!['http:', 'https:'].includes(base.protocol) || base.username || base.password || base.search || base.hash) throw new Error('REACTIVE_REMOTE_URL_INVALID');
  base.pathname = base.pathname.replace(/\/?$/u, '/');
  const request = options.fetch ?? globalThis.fetch;
  const socketUrl = new URL(REMOTE_STREAM_MUX_PATH.slice(1), base);
  socketUrl.protocol = socketUrl.protocol === 'https:' ? 'wss:' : 'ws:';
  const openSocket: ConnectionWebSocketFactory = options.createWebSocket ?? (url => new WebSocket(url));
  const inboxBytes = options.inboxBytes ?? 1024 * 1024;
  if (!Number.isSafeInteger(inboxBytes) || inboxBytes < 1) throw new Error('REACTIVE_REMOTE_INBOX_LIMIT_INVALID');
  const lifetime = new AbortController();
  const pending = new Set<{ readonly closed: Promise<void>; stop(): Promise<unknown> }>();
  const failures = new Set<unknown>();
  let closing: Promise<void> | undefined;
  const dispose = (): Promise<void> => {
    if (closing) return closing;
    const completion = Promise.withResolvers<void>();
    closing = completion.promise;
    lifetime.abort(new Error('REACTIVE_AUTHENTICATED_TRANSPORT_DISPOSED'));
    void (async () => {
      const active = [...pending];
      // return与pending next并列推进；真正的工作失败由每条closed统一保存，不能由官方吞错丢失。
      await Promise.allSettled(active.map(stream => stream.stop()));
      await Promise.allSettled(active.map(stream => stream.closed));
      if (failures.size === 1) throw [...failures][0];
      if (failures.size) throw new AggregateError([...failures], 'REACTIVE_AUTHENTICATED_TRANSPORT_DISPOSAL_FAILED');
    })().then(completion.resolve, completion.reject);
    return closing;
  };
  return {
    fetch: (input, init) => {
      lifetime.signal.throwIfAborted();
      return request(new URL(new URL(input, 'http://logical.invalid').pathname.slice(1), base), init);
    },
    openStream: (endpoint, payload, signal, uplink) => {
      lifetime.signal.throwIfAborted();
      const current = AbortSignal.any([lifetime.signal, signal]);
      const stream = socketStream(openSocket, socketUrl.href, endpoint, payload, current, inboxBytes, uplink, closed => {
        const entry = { closed, stop: () => stream.return(undefined) };
        pending.add(entry);
        void closed.then(() => pending.delete(entry), error => { failures.add(error); pending.delete(entry); });
      });
      return stream;
    },
    dispose, [Symbol.asyncDispose]: dispose,
  };
}

async function* socketStream(openSocket: ConnectionWebSocketFactory, url: string, endpoint: string, payload: unknown,
  signal: AbortSignal, inboxBytes: number, uplink: AsyncIterable<unknown> | undefined,
  started: (closed: Promise<void>) => void): AsyncGenerator<unknown> {
  signal.throwIfAborted();
  const completion = Promise.withResolvers<void>();
  started(completion.promise);
  let socket: ReturnType<ConnectionWebSocketFactory> | undefined;
  const inbox = new Inbox<unknown>(inboxBytes);
  const opened = Promise.withResolvers<void>();
  void opened.promise.catch(() => {});
  const closed = Promise.withResolvers<void>();
  const failures = new Set<unknown>();
  const streamId = '1';
  let terminal = false, stopped = false, retiring = false;
  let escaped: { readonly error: unknown } | undefined;
  const record = (error: unknown): void => { if (!signal.aborted || error !== signal.reason) failures.add(error); };
  const send = (message: RemoteStreamClientMessage): void => { socket!.send(JSON.stringify(message)); };
  const abort = (): void => {
    stopped = true; opened.reject(signal.reason); inbox.fail(signal.reason);
    try { socket?.close(); } catch (error) { record(error); }
  };
  const onOpen = (): void => opened.resolve();
  const detach = (): void => { socket?.removeEventListener('close', onClose); socket?.removeEventListener('error', onError); };
  const disconnected = (): void => {
    stopped = true;
    if (!terminal && !signal.aborted && !retiring) {
      const error = new RemoteStreamCarrierError('Remote physical stream disconnected');
      opened.reject(error); inbox.fail(error);
    }
  };
  const onClose = (): void => { disconnected(); closed.resolve(); detach(); };
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
  let iterator: AsyncIterator<unknown> | undefined;
  let pumping: Promise<void> | undefined;
  try {
    socket = openSocket(url);
    socket.addEventListener('open', onOpen);
    socket.addEventListener('close', onClose);
    socket.addEventListener('error', onError);
    socket.addEventListener('message', onMessage);
    signal.addEventListener('abort', abort, { once: true });
    if (socket.readyState === 3) onClose();
    if (signal.aborted) abort();
    signal.throwIfAborted();
    iterator = uplink?.[Symbol.asyncIterator]();
    signal.throwIfAborted(); await opened.promise; signal.throwIfAborted();
    send({ type: 'open', streamId, endpoint, payload });
    signal.throwIfAborted();
    pumping = (async (): Promise<void> => {
      try {
        if (iterator) while (!stopped) {
          const item = await iterator.next();
          if (stopped || item.done) break;
          send({ type: 'item', streamId, value: item.value });
        }
        if (!stopped) send({ type: 'end', streamId });
      } catch (error) {
        // 活跃来源失败交本条流；退出以后才迟到的迭代/发送失败属于实际清理。
        if (!stopped) inbox.fail(error); else record(error);
      }
    })();
    yield* inbox;
  } catch (error) { escaped = { error }; throw error;
  } finally {
    stopped = true; retiring = true;
    const cleanup: PromiseLike<unknown>[] = [];
    // 先要求来源退出，再等待pump，return可以唤醒其已有next；不能反过来等待。
    try { if (iterator?.return) cleanup.push(Promise.resolve(iterator.return())); } catch (error) { record(error); }
    if (pumping) cleanup.push(pumping);
    try { if (!terminal && socket?.readyState === 1) send({ type: 'cancel', streamId }); } catch (error) { record(error); }
    signal.removeEventListener('abort', abort);
    socket?.removeEventListener('open', onOpen); socket?.removeEventListener('message', onMessage);
    inbox.end();
    if (socket) {
      let requested = false;
      try { if (socket.readyState !== 3) socket.close(); requested = true; } catch (error) { record(error); }
      // 正式close事件确认物理退出；CONNECTING取消产生的error监听一直保留到该事件。
      if (socket.readyState === 3) { closed.resolve(); detach(); }
      else if (requested || socket.readyState === 2) cleanup.push(closed.promise);
    }
    for (const result of await Promise.allSettled(cleanup)) if (result.status === 'rejected') record(result.reason);
    if (!failures.size) completion.resolve();
    else {
      const failure = failures.size === 1 ? [...failures][0] : new AggregateError([...failures], 'REACTIVE_REMOTE_STREAM_CLEANUP_FAILED');
      completion.reject(failure);
      if (escaped && !failures.has(escaped.error)) throw new AggregateError([escaped.error, failure], 'REACTIVE_REMOTE_STREAM_FAILED');
      throw failure;
    }
  }
}
