import { RemoteSnapshotStream, type ClientRemote } from '@deepseek-ai/dsh-api-gateway/client';
import { RemoteError } from '@deepseek-ai/dsh-typert-protocol';
import type { JsonValue } from '@deepseek-ai/dsh-util-values';
import { BehaviorSubject } from 'rxjs';
import { z, ResourceReader, actionIdentity, assertActionIdentity, type ActionEligibility, type Observation, type ActionIdentity, type CancellationOptions } from '@hanger-source/dsh-reactive';
import type { ProgramSessionAddress, ProgramActionBinding } from './contracts/types.ts';
import { decodeProgramFailure } from './contracts/errors.ts';
import { revokeConversationAccess, type ClientScope, type ClientLease } from './scope.ts';

export interface RemoteActionCapture {
  readonly descriptor: ProgramActionBinding;
  readonly eligibility?: ResourceReader<ActionEligibility>;
  /** 查询当前槽持有连接；连接移交不改写这个 capture 的原始绑定身份。 */
  assertActive(): void;
}

export interface RemoteActionBindings {
  resolve(name: string, contract: ActionIdentity, scope: JsonValue, options?: CancellationOptions): Promise<RemoteActionCapture>;
}

type BindingConnection = {
  readonly ready: Promise<ProgramActionBinding>;
  readonly bindingId: string | undefined;
  readonly reusable: boolean;
  readonly eligibility: ResourceReader<ActionEligibility> | undefined;
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
function connectBinding(remote: ClientRemote, address: ProgramSessionAddress, name: string, contract: ActionIdentity, scope: JsonValue,
  lifetime: AbortSignal, failed: (error: unknown) => unknown): BindingConnection {
  const { sessionId } = address;
  const first = Promise.withResolvers<ProgramActionBinding>();
  void first.promise.catch(() => {});
  const controller = new AbortController();
  const qualification = new BehaviorSubject<Observation<ActionEligibility>>({ status: 'pending', generation: 0 });
  let eligibility: ResourceReader<ActionEligibility> | undefined;
  let generation = 0;
  let descriptor: ProgramActionBinding | undefined;
  let active = false, closed = false, terminal = false, opening = true;
  let failure: unknown = unavailable(name, 'connection-lost');
  let closing: Promise<void> | undefined;
  const acceptBinding = (value: ProgramActionBinding): void => {
    if (closed) return;
    assertActionIdentity(contract, value.contract);
    if (descriptor && descriptor.bindingId !== value.bindingId) throw unavailable(name, 'binding-changed');
    if (!descriptor) {
      const scope = z.json().parse(value.scope);
      freezeJson(scope);
      descriptor = Object.freeze({ bindingId: value.bindingId, contract: actionIdentity(value.contract), scope });
      if (value.eligibility) eligibility = new ResourceReader({ observations$: qualification.asObservable(), peek: () => qualification.value },
        { signal: AbortSignal.any([lifetime, controller.signal]) });
    }
    if (Boolean(eligibility) !== Boolean(value.eligibility)) throw unavailable(name, 'binding-changed');
    if (value.eligibility) {
      const observation = value.eligibility;
      qualification.next(observation.status === 'ready'
        ? { ...observation, generation: ++generation, value: z.discriminatedUnion('allowed', [
          z.object({ allowed: z.literal(true) }).strict(), z.object({ allowed: z.literal(false), reason: z.string() }).strict(),
        ]).parse(observation.value) }
        : observation.status === 'error' ? { ...observation, generation: ++generation, error: decodeProgramFailure(observation.error) }
        : { ...observation, generation: ++generation });
    }
    active = true;
    first.resolve(descriptor);
  };
  const stream = new RemoteSnapshotStream<ProgramActionBinding, ProgramActionBinding>(remote.$stream<ProgramActionBinding>({
    name: 'program-action-binding:' + sessionId + ':' + name,
    open: physical => {
      opening = true;
      return remote.reactivePrograms.resolveAction({ ...address, name, contract, scope },
        AbortSignal.any([lifetime, controller.signal, physical]));
    },
    ended: () => unavailable(name, 'connection-lost'),
    carrierFailed: error => {
      if (closed) return;
      active = false; failure = unavailable(name, 'connection-lost', error);
      qualification.next({ status: 'pending', generation: ++generation });
    },
  }), {
    name: 'program-action-binding:' + name,
    isSnapshot: (value): value is ProgramActionBinding => {
      const first = opening; opening = false; return first;
    },
    replace: acceptBinding,
    update: acceptBinding,
    failed: error => {
      if (closed) return;
      active = false; terminal = true; failure = failed(error); first.reject(failure);
      controller.abort(failure);
      qualification.complete();
    },
  });
  return {
    ready: first.promise,
    get bindingId() { return descriptor?.bindingId; },
    get reusable() { return !closed && !terminal; },
    get eligibility() { return eligibility; },
    start() { if (!closed) stream.start(); },
    assertActive() { if (!active || closed) throw failure; },
    close(reason) {
      if (closing) return closing;
      closed = true; active = false; failure = reason;
      first.reject(reason); controller.abort(reason);
      qualification.complete();
      return closing = stream.dispose();
    },
  };
}

/** 一次客户端 Session 只为每个槽保留一个已就绪连接；在途解析各自拥有自己的取消。 */
export function createActionBindings(owner: ClientScope, address: ProgramSessionAddress): RemoteActionBindings {
  const current = new Map<string, ClientLease<BindingConnection>>();
  owner.acquire(() => current, current => { current.clear(); }, 'program-action-bindings:' + address.sessionId);
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
      const connection = owner.acquire(() => connectBinding(owner.context.remote, address, name, actionIdentity(contract), z.json().parse(scope), owner.signal,
        error => revokeConversationAccess(owner, error instanceof RemoteError ? decodeProgramFailure(error) : error)),
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
        const heldConnection = previous?.value.bindingId === descriptor.bindingId && previous.value.reusable ? previous : connection;
        if (heldConnection !== connection) {
          // 新解析只用于宿主核对本次原始scope与合同；同一绑定继续借用原连接及reader。
          await dispose(connection, new Error('REACTIVE_REMOTE_ACTION_BINDING_ALREADY_HELD'));
        } else {
          current.set(name, connection);
          if (previous) await dispose(previous, unavailable(name,
            previous.value.bindingId === descriptor.bindingId ? 'connection-lost' : 'binding-changed'));
        }
        assertActive();
        return Object.freeze({ descriptor, eligibility: heldConnection.value.eligibility,
          assertActive() {
            assertActive();
            const held = current.get(name);
            if (!held) throw unavailable(name, 'connection-lost');
            if (held.value.bindingId !== descriptor.bindingId) throw unavailable(name, 'binding-changed');
            heldConnection.value.assertActive();
            held.value.assertActive();
          },
        });
      } catch (error) {
        error = revokeConversationAccess(owner, error);
        if (current.get(name) !== connection) await dispose(connection, error);
        throw error;
      } finally {
        signal?.removeEventListener('abort', aborted);
      }
    },
  };
}
