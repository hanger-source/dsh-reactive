import { symbols, type Context, type Fiber } from '@deepseek-ai/cordis';
import type { ToolRuntime } from '@deepseek-ai/dsh-tools';
import { BehaviorSubject, Observable, distinctUntilChanged, firstValueFrom, map, takeWhile } from 'rxjs';
import { abortable, createResource, fromSnapshot, ResourceReader, z, type CancellationOptions, type Invocation, type InvocationActivity, type Json, type Observation, type ConversationIdentity } from '@hanger-source/dsh-reactive';
import { Scope, connect } from '@hanger-source/dsh-reactive/kernel';
import { requireActivePlugin, type NativeInstallation } from './native-installation.ts';
import type { NativeSessionAccess } from './conversations/session-access.ts';

/** 官方 trace 代理使用原对象身份；普通对象及函数 provider 使用自身身份。 */
function nativeServiceIdentity(service: unknown): object {
  if (service === null || (typeof service !== 'object' && typeof service !== 'function')) {
    throw new TypeError('REACTIVE_NATIVE_SERVICE_OBJECT_REQUIRED');
  }
  if (!Reflect.get(service, symbols.tracker)) return service;
  const original: unknown = Reflect.get(service, symbols.original);
  if (original === null || (typeof original !== 'object' && typeof original !== 'function')) {
    throw new TypeError('REACTIVE_NATIVE_SERVICE_OBJECT_REQUIRED');
  }
  return original;
}

/** 宿主选择的一次能力安装；必需服务退出会撤销实例，不转向替换后的provider。 */
export interface NativeCapability {
  readonly install: NativeInstallation;
  /** 撤销本实例观察，等待已开始调用结算并释放安装范围；调用拒绝保留在各自result中，借入服务仍由外部宿主持有。 */
  dispose(): Promise<void>;
}

/** 一次真实原生调用的来源事实；不替代原生成员、权限与事务校验。 */
export interface NativeInvocationSource {
  readonly kind: 'native';
  readonly sessionId: string;
  readonly capability: string;
  readonly operation: string;
}

/** 原生调用立即开始一次；result与有限调用事实独立于SDK等待，不能据running推断事务已受理。
 * wait默认沿原SDK等待资格；退出不改变result，也不回滚已经开始的原生写入。 */
export interface NativeInvocation<T> extends Invocation<T, Json, NativeInvocationSource> {}

export type NativeSetupResource = (Fiber & PromiseLike<Fiber>) | (() => unknown);

/** 仅用于编程时选择句柄；真正身份由 Host 的 exact 对象登记核验。 */
export interface NativeSessionHandle { readonly id: string; }
/** 外部本地句柄或 Program 的原 conversation；字段形状不授予访问权，仍核 exact 对象。 */
export type NativeSessionSource = NativeSessionHandle | { readonly identity: ConversationIdentity };

/** 一次安装同时用于独立 Runtime 与嵌入宿主，部分失败释放这个安装范围。 */
export function nativeCapabilities(...selected: readonly NativeCapability[]): NativeInstallation {
  return async (parent, home) => {
    await requireActivePlugin(parent.plugin({ name: 'reactive-native-capabilities', async apply(ctx: Context) {
      for (const capability of selected) await capability.install(ctx, home);
    } }));
  };
}

/** @internal 只持有一次正式安装及其观察者；不拥有 Job/MCP/Approval 状态。 */
export class NativeCapabilityOwner {
  readonly scope = new Scope();
  readonly install: NativeInstallation;
  #installation?: Fiber & PromiseLike<Fiber>;
  #reader?: Fiber;
  #parent?: Context;
  #disposing?: Promise<void>;
  #draining?: Promise<void>;
  #pending = new Set<Promise<void>>();
  #failures = new Set<unknown>();

  constructor(private readonly name: string, services: readonly string[], setup: (ctx: Context, home: string) => Iterable<NativeSetupResource>) {
    this.install = async (parent, home) => {
      this.scope.assertActive();
      if (this.#installation) throw new Error('REACTIVE_NATIVE_CAPABILITY_ALREADY_INSTALLED');
      const stop = () => this.stopObservations();
      const stopAndDrain = () => this.stopAndDrain();
      let reader: Fiber | undefined;
      this.#parent = parent;
      const owner = this;
      const installation = parent.plugin({ name, async apply(ctx: Context) {
        const resources = (function* (): Generator<NativeSetupResource, void> {
          yield stopAndDrain;
          yield* setup(ctx, home);
          const queries = ctx.plugin({ name: name + ':queries', inject: [...services], apply(readContext: Context) {
            owner.scope.assertActive();
            readContext.effect(() => () => {
              try { stop(); }
              finally {
                // 读依赖退出也释放本实例安装的资源；不等待自己的父Fiber，避免卸载自等待。
                if (owner.#installation?.state === 1 || owner.#installation?.state === 2) {
                  void owner.dispose().catch(() => {});
                }
              }
            });
          } });
          reader = queries;
          yield queries;
          yield stopAndDrain;
        })();
        // IteratorResult保留原thenable disposer身份；下一项只在前一Fiber活跃后构造。
        // async generator会自动await yield值，丢失Cordis收拢同级disposer所需的函数身份。
        await ctx.effect(() => ({
          [Symbol.asyncIterator]() {
            let previous: Fiber & PromiseLike<Fiber> | undefined;
            return { async next(): Promise<IteratorResult<() => unknown, void>> {
              if (previous) await requireActivePlugin(previous);
              const next = resources.next();
              if (next.done) return next;
              const resource = next.value;
              previous = typeof resource === 'function' ? undefined : resource;
              return { done: false, value: typeof resource === 'function' ? resource : resource.dispose };
            } };
          },
        }));
      } });
      this.#installation = installation;
      await requireActivePlugin(installation);
      this.scope.assertActive();
      this.#reader = reader;
    };
  }

  context(): Context {
    this.scope.assertActive();
    const ended = (state: number | undefined) => state === 3 || state === 4 || state === 5;
    if (!this.#reader || this.#reader.state !== 2 || this.#installation?.state !== 2
      || ended(this.#parent?.fiber.state) || ended(this.#parent?.root.fiber.state)) {
      throw new Error('REACTIVE_NATIVE_CAPABILITY_NOT_ACTIVE');
    }
    return this.#reader.ctx;
  }

  /** 只接受本能力实际服务所在的 Host；同进程其他隔离服务不能借此访问。 */
  session(access: NativeSessionAccess, service: string): Context {
    const context = this.context();
    access.signal.throwIfAborted();
    access.assertCurrent();
    const installed: unknown = context.get(service);
    const current: unknown = access.context.get(service);
    if (installed === undefined || current === undefined) {
      throw new Error('REACTIVE_NATIVE_CAPABILITY_HOST_MISMATCH');
    }
    if (nativeServiceIdentity(installed) !== nativeServiceIdentity(current)) {
      throw new Error('REACTIVE_NATIVE_CAPABILITY_HOST_MISMATCH');
    }
    return context;
  }

  recordCleanupFailure(error: unknown): void {
    this.#failures.add(error);
  }

  /** 自有清理失败交由最终dispose报告，正式effect仍继续释放其余资源。 */
  cleanup(release: () => void | PromiseLike<void>): () => Promise<void> {
    return async () => {
      try { await release(); }
      catch (error) { this.recordCleanupFailure(error); }
    };
  }

  invoke<T>(source: Pick<NativeInvocationSource, 'sessionId' | 'operation'>, run: () => PromiseLike<T> | T, signal: AbortSignal): NativeInvocation<T> {
    signal.throwIfAborted(); this.context();
    const id = globalThis.crypto.randomUUID();
    const completion = Promise.withResolvers<T>();
    const state = new BehaviorSubject<Observation<InvocationActivity>>({ status: 'ready', generation: 1,
      observedAt: Date.now(), value: { state: 'running' } });
    const activity = new ResourceReader<InvocationActivity>({ peek: () => state.value,
      observations$: state.pipe(takeWhile(value => value.status === 'ready' && value.value.state === 'running', true)) });
    const publish = (value: InvocationActivity) => state.next({ status: 'ready', generation: state.value.generation + 1,
      observedAt: Date.now(), value });
    const settled = completion.promise.then(() => { publish({ state: 'returned' }); }, error => {
      publish({ state: 'failed', phase: 'execute', error });
    });
    // 进入原生方法之前登记；同步重入的释放也必须加入同一次真实结算。
    this.#pending.add(settled);
    void settled.then(() => this.#pending.delete(settled), error => {
      this.#failures.add(error); this.#pending.delete(settled);
    });
    const invocation: NativeInvocation<T> = Object.freeze({ id,
      source: Object.freeze({ kind: 'native' as const, sessionId: source.sessionId, capability: this.name, operation: source.operation }),
      result: completion.promise, activity, activity$: activity.values$,
      wait(options: CancellationOptions = {}) {
        const waiting = options.signal ? AbortSignal.any([signal, options.signal]) : signal;
        return firstValueFrom(abortable(() => completion.promise, waiting));
      },
    });
    try { completion.resolve(run()); } catch (error) { completion.reject(error); }
    return invocation;
  }

  private stopObservations(): void {
    try { this.scope.dispose(); }
    catch (error) { this.#failures.add(error); throw error; }
  }

  private stopAndDrain(): Promise<void> {
    if (this.#draining) return this.#draining;
    const completion = Promise.withResolvers<void>();
    this.#draining = completion.promise;
    void (async () => {
      try { this.stopObservations(); } catch { /* 已保存清理失败，仍等待每个原生调用。 */ }
      try { await this.scope.closed; } catch (error) { this.#failures.add(error); }
      while (this.#pending.size) {
        const results = await Promise.allSettled(this.#pending);
        for (const result of results) if (result.status === 'rejected') this.#failures.add(result.reason);
      }
    })().then(completion.resolve, completion.reject);
    return this.#draining;
  }

  dispose(): Promise<void> {
    if (this.#disposing) return this.#disposing;
    const completion = Promise.withResolvers<void>();
    this.#disposing = completion.promise;
    void (async () => {
      try { await this.stopAndDrain(); } catch (error) { this.#failures.add(error); }
      if (this.#installation) {
        try { await this.#installation.dispose(); } catch (error) { this.#failures.add(error); }
        // 原生公开disposer是single-shot；外层已经开始卸载时仍加入原Fiber。
        try { await this.#installation; } catch (error) { this.#failures.add(error); }
      }
      if (this.#failures.size === 1) throw [...this.#failures][0];
      if (this.#failures.size) throw new AggregateError([...this.#failures], 'REACTIVE_NATIVE_CAPABILITY_DISPOSAL_FAILED');
    })().then(completion.resolve, completion.reject);
    return this.#disposing;
  }
}

/** @internal tools/change 也包含临时 agent mask；内容稳定时不制造新声明代际。 */
export function nativeToolNames(owner: NativeCapabilityOwner, accept: (name: string) => boolean): ResourceReader<readonly string[]> {
  return nativeResource(owner.scope, () => {
    const context = owner.context();
    return fromSnapshot({
      getSnapshot: () => {
        const tools: ToolRuntime | undefined = owner.context().get('tools');
        if (!tools) throw new Error('REACTIVE_NATIVE_TOOLS_REQUIRED');
        return Object.freeze(tools.schemas().map(tool => tool.name).filter(accept).sort());
      },
      subscribe: notify => context.on('tools/change', notify),
    }).pipe(
      distinctUntilChanged((left, right) => left.length === right.length && left.every((name, index) => name === right[index])),
      map(value => ({ status: 'ready', generation: 0, observedAt: Date.now(), value })),
    );
  });
}

/** @internal 读取对象的作用域；dispose 不作用于原生执行 owner。 */
export function nativeObservationScope(owner: NativeCapabilityOwner, access: NativeSessionAccess): Scope {
  owner.context(); access.assertCurrent(); access.signal.throwIfAborted();
  const scope = owner.scope.child();
  const stop = () => {
    try { scope.dispose(access.signal.reason); }
    catch (error) { owner.recordCleanupFailure(error); }
  };
  access.signal.addEventListener('abort', stop, { once: true });
  scope.own(() => access.signal.removeEventListener('abort', stop));
  if (access.signal.aborted) stop();
  scope.assertActive();
  return scope;
}

/** @internal 首次观察才连接，所有读取者共享 owner 持有的订阅；scope 退出后不重放旧快照。 */
export function nativeObservation<T>(scope: Scope, source: Observable<T>): Observable<T> {
  let shared: Observable<T> | undefined;
  return new Observable(subscriber => {
    if (scope.disposed) { subscriber.complete(); return; }
    shared ??= connect(scope, source);
    return shared.subscribe(subscriber);
  });
}

/** @internal 原生事实沿正式资源持有当前观察；构造不连接，最后消费者退出取消读取。 */
export function nativeResource<T>(scope: Scope, observe: () => Observable<Observation<T>>): ResourceReader<T> {
  // 值由正式原生服务决定；不复制原生结果schema或建立另一套领域状态。
  const resource = createResource(scope, { value: z.custom<T>(), observe });
  return new ResourceReader(resource, { signal: scope.signal, onObserverError: error => scope.observerError(error) });
}

/** @internal 自有快照保护共享观察，原生注册表对象不暴露为可变应用状态。 */
export function ownNativeValue<T>(value: T): T {
  const copy = structuredClone(value);
  const freeze = (item: unknown): void => {
    if (item === null || typeof item !== 'object' || Object.isFrozen(item)) return;
    for (const child of Object.values(item)) freeze(child);
    Object.freeze(item);
  };
  freeze(copy);
  return copy;
}
