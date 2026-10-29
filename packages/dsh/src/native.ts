import type { Context } from '@deepseek-ai/cordis';
import type { Agent } from '@deepseek-ai/dsh-agent';
import type { SessionId } from '@deepseek-ai/dsh-session';
import { Observable } from 'rxjs';
import type { CancellationOptions, Observation, ResourceReader } from '@hanger-source/dsh-reactive';
import { NativeCapabilityOwner, nativeObservation, nativeObservationScope, nativeResource,
  type NativeCapability, type NativeInvocation, type NativeSessionSource, type NativeSetupResource } from './native-capability.ts';
import { nativeSessionAccess } from './conversations/session-access.ts';
import { nativeAgentResource, type NativeAgentResource, type NativeAgentResourceOptions } from './conversations/agent-resource.ts';

export type { NativeCapability, NativeInvocation, NativeInvocationSource, NativeSessionHandle,
  NativeSessionSource, NativeSetupResource } from './native-capability.ts';
export type { NativeAgentResource, NativeAgentResourceOptions } from './conversations/agent-resource.ts';

/** exact 会话来源的扩展接点；读取与清理由宿主拥有，不提供共享 owner 的销毁权。 */
export interface NativeSessionContext {
  readonly sessionId: SessionId;
  readonly signal: AbortSignal;
  context(): Context;
  assertCurrent(): void;
  liveAgent(): Agent | undefined;
  readonly liveAgent$: Observable<Agent | undefined>;
  /** 冷构造可在 session 工厂中完成；实际观察才核验并连接来源。 */
  resource<T>(observe: () => Observable<Observation<T>>): ResourceReader<T>;
  agentResource<T>(options: NativeAgentResourceOptions<T>): NativeAgentResource<T>;
  /** 调用立即开始；来源退出仅结束默认等待，已开始的原生工作沿原 owner 结算。 */
  invoke<T>(operation: string, run: () => T | PromiseLike<T>, options?: CancellationOptions): NativeInvocation<T>;
  own(cleanup: () => void | PromiseLike<void>): () => void;
}

export interface NativeSessionCapability<T extends object> extends NativeCapability {
  forSession(source: NativeSessionSource): Readonly<T>;
}

export interface NativeSessionCapabilityOptions<T extends object> {
  readonly name: string;
  readonly services: readonly [string, ...string[]];
  readonly setup?: (context: Context, home: string) => Iterable<NativeSetupResource>;
  /** 同步构造共享业务面；不读取来源、不执行原生调用。 */
  readonly session: (source: NativeSessionContext) => T;
}

/** 安装、exact 来源核验、冷资源与原生调用共同复用正式宿主 owner。 */
export function createNativeSessionCapability<T extends object>(options: NativeSessionCapabilityOptions<T>): NativeSessionCapability<T> {
  const { name, setup, session: createSession } = options;
  if (typeof name !== 'string' || !name.trim()) throw new Error('REACTIVE_NATIVE_CAPABILITY_NAME_REQUIRED');
  if (!Array.isArray(options.services) || options.services.length === 0
    || options.services.some(service => typeof service !== 'string' || !service.trim())) {
    throw new Error('REACTIVE_NATIVE_CAPABILITY_SERVICES_REQUIRED');
  }
  if (typeof createSession !== 'function' || (setup !== undefined && typeof setup !== 'function')) {
    throw new TypeError('REACTIVE_NATIVE_CAPABILITY_FACTORY_REQUIRED');
  }
  const services = Object.freeze([...options.services]);
  const owner = new NativeCapabilityOwner(name, services, function* (context, home) {
    if (setup) yield* setup(context, home);
  });
  const sessions = new WeakMap<object, Readonly<T>>();
  const constructing = new WeakSet<object>();
  return Object.freeze({ install: owner.install, dispose: () => owner.dispose(),
    forSession(source: NativeSessionSource): Readonly<T> {
      const access = nativeSessionAccess(source);
      const check = (): Context => {
        for (const service of services) owner.session(access, service);
        return owner.context();
      };
      check();
      const previous = sessions.get(source);
      if (previous) return previous;
      if (constructing.has(source)) throw new Error('REACTIVE_NATIVE_SESSION_REENTRANT');
      constructing.add(source);
      try {
        const scope = nativeObservationScope(owner, access);
        try {
          const liveAgent$ = nativeObservation(scope, access.liveAgent$);
          // 同一派生 Scope 的真实清理同时进入能力和 Host 的退休图。
          access.ownReading(async () => {
            try { scope.dispose(access.signal.reason); }
            finally { await scope.closed; }
          });
          let constructed = false;
          const assertCurrent = (): void => {
            if (!constructed) throw new Error('REACTIVE_NATIVE_SESSION_CONSTRUCTING');
            scope.assertActive(); check();
          };
          const context: NativeSessionContext = Object.freeze({ sessionId: access.sessionId, signal: scope.signal,
            context() { assertCurrent(); return check(); },
            assertCurrent,
            liveAgent() { assertCurrent(); return access.liveAgent(); },
            liveAgent$: new Observable<Agent | undefined>(subscriber => {
              assertCurrent();
              return liveAgent$.subscribe(subscriber);
            }),
            resource<U>(observe: () => Observable<Observation<U>>): ResourceReader<U> {
              return nativeResource(scope, () => { assertCurrent(); return observe(); });
            },
            agentResource<U>(selected: NativeAgentResourceOptions<U>): NativeAgentResource<U> {
              return nativeAgentResource(scope, context, selected);
            },
            invoke<U>(operation: string, run: () => U | PromiseLike<U>, selected: CancellationOptions = {}): NativeInvocation<U> {
              assertCurrent(); selected.signal?.throwIfAborted();
              const signal = selected.signal ? AbortSignal.any([scope.signal, selected.signal]) : scope.signal;
              return owner.invoke({ sessionId: access.sessionId, operation }, run, signal);
            },
            own(cleanup: () => void | PromiseLike<void>): () => void { return scope.own(cleanup); },
          });
          const value = createSession(context);
          if (value !== null && (typeof value === 'object' || typeof value === 'function')
            && typeof Reflect.get(value, 'then') === 'function') {
            // 已经返回的异步工作不再有消费者，仍消费其拒绝以免产生未处理异常。
            void Promise.resolve(value).catch(() => {});
            throw new TypeError('REACTIVE_NATIVE_SESSION_MUST_BE_SYNCHRONOUS');
          }
          if (value === null || typeof value !== 'object') {
            throw new TypeError('REACTIVE_NATIVE_SESSION_OBJECT_REQUIRED');
          }
          const current = Object.freeze(value);
          scope.assertActive(); check();
          constructed = true;
          sessions.set(source, current);
          return current;
        } catch (error) {
          try { scope.dispose(error); }
          catch (cleanup) { throw new AggregateError([error, cleanup], 'REACTIVE_NATIVE_SESSION_CONSTRUCTION_FAILED'); }
          throw error;
        }
      } finally { constructing.delete(source); }
    },
  });
}
