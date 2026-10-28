import { RemoteSnapshotStream, type ClientRemote } from '@deepseek-ai/dsh-api-gateway/client';
import { RemoteError } from '@deepseek-ai/dsh-typert-protocol';
import type { JsonValue } from '@deepseek-ai/dsh-util-values';
import { z, actionIdentity, assertActionIdentity, type ActionIdentity, type CancellationOptions } from '@hanger-source/dsh-reactive';
import type { ProgramActionBinding } from './types.ts';
import './errors.ts';
import type { ClientScope, ClientLease } from './scope.ts';

export interface RemoteActionCapture {
  readonly descriptor: ProgramActionBinding;
  /** 查询当前槽持有连接；连接移交不改写这个 capture 的原始绑定身份。 */
  assertActive(): void;
}

export interface RemoteActionBindings {
  resolve(name: string, contract: ActionIdentity, scope: JsonValue, options?: CancellationOptions): Promise<RemoteActionCapture>;
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
function connectBinding(remote: ClientRemote, sessionId: string, name: string, contract: ActionIdentity, scope: JsonValue,
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
    open: physical => remote.reactivePrograms.resolveAction({ sessionId, name, contract, scope },
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
      assertActionIdentity(contract, value.contract);
      if (descriptor && descriptor.bindingId !== value.bindingId) throw unavailable(name, 'binding-changed');
      if (!descriptor) {
        const scope = z.json().parse(value.scope);
        freezeJson(scope);
        descriptor = Object.freeze({ bindingId: value.bindingId, contract: actionIdentity(value.contract), scope });
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
export function createActionBindings(owner: ClientScope, sessionId: string): RemoteActionBindings {
  const current = new Map<string, ClientLease<BindingConnection>>();
  owner.acquire(() => current, current => { current.clear(); }, 'program-action-bindings:' + sessionId);
  const assertActive = () => owner.assertActive();
  const dispose = (connection: ClientLease<BindingConnection>, reason: unknown): Promise<void> => {
    // 先撤准入；实际清理始终由原生effect持有，手动退休也能被Session退出加入。
    void connection.value.close(reason).catch(() => {});
    return connection.release();
  };
  return {
    async resolve(name, contract, scope, options = {}) {
      const signal = options.signal;
      assertActive(); signal?.throwIfAborted();
      // 取得自己的 raw JSON，避免调用方在官方物理重连前改写这次解析请求。
      const connection = owner.acquire(() => connectBinding(owner.context.remote, sessionId, name, actionIdentity(contract), z.json().parse(scope), owner.signal),
        connection => connection.close(owner.signal.aborted ? owner.signal.reason : new Error('REACTIVE_REMOTE_ACTION_BINDINGS_CLOSED')),
        'program-action-binding:' + name);
      const aborted = () => { void dispose(connection, signal?.reason); };
      signal?.addEventListener('abort', aborted, { once: true });
      try {
        signal?.throwIfAborted();
        connection.value.start();
        const descriptor = await connection.value.ready;
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
            held.value.assertActive();
          },
        });
      } catch (error) {
        if (current.get(name) !== connection) await dispose(connection, error);
        throw error;
      } finally {
        signal?.removeEventListener('abort', aborted);
      }
    },
  };
}
