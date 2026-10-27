import { RemoteSnapshotStream, type ClientRemote } from '@deepseek-ai/dsh-api-gateway/client';
import { RemoteError } from '@deepseek-ai/dsh-typert-protocol';
import type { JsonValue } from '@deepseek-ai/dsh-util-values';
import { z, type CancellationOptions } from '@hanger-source/dsh-reactive';
import type { ProgramActionBinding } from './types.ts';
import './errors.ts';

export interface RemoteActionCapture {
  readonly descriptor: ProgramActionBinding;
  /** 查询当前槽持有连接；连接移交不改写这个 capture 的原始绑定身份。 */
  assertActive(): void;
}

export interface RemoteActionBindings {
  resolve(name: string, scope: JsonValue, options?: CancellationOptions): Promise<RemoteActionCapture>;
  close(): Promise<void>;
}

type BindingConnection = {
  readonly ready: Promise<ProgramActionBinding>;
  start(): void;
  assertActive(): void;
  close(reason: unknown): Promise<void>;
};

function unavailable(name: string, reason: 'connection-lost' | 'binding-changed', cause?: unknown): RemoteError {
  return new RemoteError('reactive/action-binding-unavailable', 'Action binding is unavailable: ' + name,
    { name, reason }, cause === undefined ? undefined : { cause });
}

function freezeJson(value: JsonValue): void {
  if (value === null || typeof value !== 'object') return;
  Object.values(value).forEach(freezeJson);
  Object.freeze(value);
}

/** Gateway 的真实流持有远端 Handle；首帧以后只核身份，不自动改指另一个绑定。 */
function connectBinding(remote: ClientRemote, sessionId: string, name: string, scope: JsonValue,
  lifetime: AbortSignal): BindingConnection {
  const first = Promise.withResolvers<ProgramActionBinding>();
  void first.promise.catch(() => {});
  const controller = new AbortController();
  let descriptor: ProgramActionBinding | undefined;
  let active = false, closed = false;
  let failure: unknown = unavailable(name, 'connection-lost');
  let closing: Promise<void> | undefined;
  const stream = new RemoteSnapshotStream<ProgramActionBinding, never>(remote.$stream<ProgramActionBinding>({
    name: 'program-action-binding:' + sessionId + ':' + name,
    open: physical => remote.reactivePrograms.resolveAction({ sessionId, name, scope },
      AbortSignal.any([lifetime, controller.signal, physical])),
    ended: () => unavailable(name, 'connection-lost'),
    carrierFailed: error => {
      if (closed) return;
      active = false; failure = unavailable(name, 'connection-lost', error);
    },
  }), {
    name: 'program-action-binding:' + name,
    isSnapshot: (value): value is ProgramActionBinding => true,
    replace: value => {
      if (closed) return;
      if (descriptor && descriptor.bindingId !== value.bindingId) throw unavailable(name, 'binding-changed');
      if (!descriptor) {
        const scope = z.json().parse(value.scope);
        freezeJson(scope);
        descriptor = Object.freeze({ bindingId: value.bindingId, scope });
      }
      active = true;
      first.resolve(descriptor);
    },
    update: () => { throw new RemoteError('gateway/internal', 'Action binding stream emitted an unexpected update', {}); },
    failed: error => {
      if (closed) return;
      active = false; failure = error; first.reject(error);
    },
  });
  return {
    ready: first.promise,
    start() { if (!closed) stream.start(); },
    assertActive() { if (!active || closed) throw failure; },
    close(reason) {
      if (closing) return closing;
      closed = true; active = false; failure = reason;
      first.reject(reason); controller.abort(reason);
      return closing = stream.dispose();
    },
  };
}

/** 一次客户端 Session 只为每个槽保留一个已就绪连接；在途解析各自拥有自己的取消。 */
export function createActionBindings(remote: ClientRemote, sessionId: string, lifetime: AbortSignal): RemoteActionBindings {
  const current = new Map<string, BindingConnection>();
  const opening = new Set<BindingConnection>();
  const disposing = new Set<Promise<void>>();
  let closed = false;
  let closing: Promise<void> | undefined;
  const assertActive = () => {
    lifetime.throwIfAborted();
    if (closed) throw new Error('REACTIVE_REMOTE_ACTION_BINDINGS_CLOSED');
  };
  const dispose = (connection: BindingConnection, reason: unknown): Promise<void> => {
    const work = connection.close(reason);
    disposing.add(work);
    void work.then(() => disposing.delete(work), () => disposing.delete(work));
    return work;
  };
  return {
    async resolve(name, scope, options = {}) {
      const signal = options.signal;
      assertActive(); signal?.throwIfAborted();
      // 取得自己的 raw JSON，避免调用方在官方物理重连前改写这次解析请求。
      const connection = connectBinding(remote, sessionId, name, z.json().parse(scope), lifetime);
      opening.add(connection);
      const aborted = () => { void dispose(connection, signal?.reason); };
      signal?.addEventListener('abort', aborted, { once: true });
      try {
        signal?.throwIfAborted();
        connection.start();
        const descriptor = await connection.ready;
        assertActive(); signal?.throwIfAborted();
        // 首帧 ready 之后转归 Session，解析请求的取消不销毁已经移交的槽连接。
        signal?.removeEventListener('abort', aborted);
        const previous = current.get(name);
        current.set(name, connection);
        if (previous) await dispose(previous, new Error('REACTIVE_REMOTE_ACTION_BINDING_CONNECTION_REPLACED'));
        assertActive();
        return Object.freeze({ descriptor,
          assertActive() {
            assertActive();
            const held = current.get(name);
            if (!held) throw unavailable(name, 'connection-lost');
            held.assertActive();
          },
        });
      } catch (error) {
        if (current.get(name) !== connection) await dispose(connection, error);
        throw error;
      } finally {
        signal?.removeEventListener('abort', aborted); opening.delete(connection);
      }
    },
    close() {
      if (closing) return closing;
      closed = true;
      const connections = new Set([...current.values(), ...opening]);
      current.clear(); opening.clear();
      const reason = lifetime.aborted ? lifetime.reason : new Error('REACTIVE_REMOTE_ACTION_BINDINGS_CLOSED');
      const work = [...connections].map(connection => dispose(connection, reason));
      return closing = Promise.allSettled([...new Set([...work, ...disposing])]).then(results => {
        const errors = results.flatMap(result => result.status === 'rejected' ? [result.reason] : []);
        if (errors.length) throw new AggregateError(errors, 'REACTIVE_REMOTE_ACTION_BINDINGS_CLOSE_FAILED');
      });
    },
  };
}
