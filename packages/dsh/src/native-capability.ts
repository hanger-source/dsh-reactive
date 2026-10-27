import { symbols, type Context, type Fiber } from '@deepseek-ai/cordis';
import type { ToolRuntime } from '@deepseek-ai/dsh-tools';
import { Observable, distinctUntilChanged } from 'rxjs';
import { fromSnapshot } from '@hanger-source/dsh-reactive';
import { Scope, connect } from '@hanger-source/dsh-reactive/kernel';
import { requireActivePlugin, type NativeInstallation } from './native-installation.ts';
import type { NativeSessionAccess } from './conversations/session-access.ts';

/** 宿主选择的能力实例；正式插件与执行资源仍由原生 Fiber 持有。 */
export interface NativeCapability {
  readonly install: NativeInstallation;
  dispose(): Promise<void>;
}

/** 仅用于编程时选择句柄；真正身份由 Host 的 exact 对象登记核验。 */
export interface NativeSessionHandle { readonly id: string; }

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
  #ready?: Promise<Fiber>;
  #fiber?: Fiber;
  #context?: Context;
  #disposing?: Promise<void>;

  constructor(name: string, setup: (ctx: Context) => void | PromiseLike<void>) {
    this.install = async parent => {
      this.scope.assertActive();
      if (this.#installation) throw new Error('REACTIVE_NATIVE_CAPABILITY_ALREADY_INSTALLED');
      const scope = this.scope;
      const installation = parent.plugin({ name, async apply(ctx: Context) {
        // 启动失败也结束观察；成功时另登记在子插件之后，退出先撤观察再等原生释放。
        ctx.effect(() => () => scope.dispose());
        await setup(ctx);
        ctx.effect(() => () => scope.dispose());
      } });
      this.#installation = installation;
      this.#context = installation.ctx;
      this.#ready = requireActivePlugin(installation);
      this.#fiber = await this.#ready;
      this.scope.assertActive();
    };
  }

  context(): Context {
    this.scope.assertActive();
    if (!this.#fiber || this.#fiber.state !== 2 || !this.#context) throw new Error('REACTIVE_NATIVE_CAPABILITY_NOT_ACTIVE');
    return this.#context;
  }

  /** 只接受本能力实际服务所在的 Host；同进程其他隔离服务不能借此访问。 */
  session(access: NativeSessionAccess, service: string): Context {
    const context = this.context();
    access.signal.throwIfAborted();
    access.assertCurrent();
    const installed: object | undefined = context.get(service);
    const current: object | undefined = access.context.get(service);
    if (!installed || !current || Reflect.get(installed, symbols.original) !== Reflect.get(current, symbols.original)) {
      throw new Error('REACTIVE_NATIVE_CAPABILITY_HOST_MISMATCH');
    }
    return context;
  }

  dispose(): Promise<void> {
    return this.#disposing ??= (async () => {
      const failures: unknown[] = [];
      try { this.scope.dispose(); } catch (error) { failures.push(error); }
      try { if (this.#installation) await this.#installation.dispose(); } catch (error) { failures.push(error); }
      if (failures.length === 1) throw failures[0];
      if (failures.length) throw new AggregateError(failures, 'REACTIVE_NATIVE_CAPABILITY_DISPOSAL_FAILED');
    })();
  }
}

/** @internal 声明可先于 install；首次真实观察才建立这个实例共有的连接。 */
export function nativeCapabilityObservation<T>(owner: NativeCapabilityOwner, source: () => Observable<T>): Observable<T> {
  let shared: Observable<T> | undefined;
  return new Observable(subscriber => {
    if (owner.scope.disposed) { subscriber.complete(); return; }
    try {
      owner.context();
      shared ??= nativeObservation(owner.scope, source());
      return shared.subscribe(subscriber);
    } catch (error) { subscriber.error(error); }
  });
}

/** @internal tools/change 也包含临时 agent mask；内容稳定时不制造新声明代际。 */
export function nativeToolNames(owner: NativeCapabilityOwner, accept: (name: string) => boolean): Observable<readonly string[]> {
  return nativeCapabilityObservation(owner, () => {
    const context = owner.context();
    return fromSnapshot({
      getSnapshot: () => {
        const tools: ToolRuntime | undefined = owner.context().get('tools');
        if (!tools) throw new Error('REACTIVE_NATIVE_TOOLS_REQUIRED');
        return Object.freeze(tools.schemas().map(tool => tool.name).filter(accept).sort());
      },
      subscribe: notify => context.on('tools/change', notify),
    }).pipe(distinctUntilChanged((left, right) => left.length === right.length && left.every((name, index) => name === right[index])));
  });
}

/** @internal 读取对象的作用域；dispose 不作用于原生执行 owner。 */
export function nativeObservationScope(owner: NativeCapabilityOwner, access: NativeSessionAccess): Scope {
  owner.context(); access.assertCurrent(); access.signal.throwIfAborted();
  const scope = owner.scope.child();
  const stop = () => scope.dispose();
  access.signal.addEventListener('abort', stop, { once: true });
  scope.own(() => access.signal.removeEventListener('abort', stop));
  if (access.signal.aborted) scope.dispose();
  scope.assertActive();
  return scope;
}

/** @internal 所有读取者共享一份订阅；scope 退出后不重放旧快照。 */
export function nativeObservation<T>(scope: Scope, source: Observable<T>): Observable<T> {
  const shared = connect(scope, source);
  return new Observable(subscriber => {
    if (scope.disposed) { subscriber.complete(); return; }
    return shared.subscribe(subscriber);
  });
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
