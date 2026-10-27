import { Observable, Subject, Subscriber, Subscription, defer, mergeMap, of, EMPTY, throwError, startWith } from 'rxjs';
import { z } from 'zod';
import { ScopeDisposedError, type Scope } from '../reactivity/scope.ts';
import { normalizeResource, switchLatest } from '../reactivity/observable.ts';
import { ownParameters, parameterIdentity } from '../reactivity/parameters.ts';
import type { Observation } from './observation.ts';
import { ResourceValue } from './value.ts';
import type { ResourceReadable } from './reader.ts';
import { mapResource } from './operators.ts';
import { observeResource } from './query.ts';
import type { ResourceContract, ResourceData, ResourceParameters, ResourceTarget } from './definition.ts';
import { resourceImplementation, type ResourceImplementation, type ResourceImplementationOptions } from './implementation.ts';

/** 实例身份属于绑定 scope；消费者退订不销毁身份，也不保留未经观察的旧 ready。 */
export class ResourceInstance<D extends ResourceContract> implements ResourceReadable<ResourceData<D>> {
  #connection: Subscription | undefined;
  #observers = 0;
  #generation = 0;
  #closed = false;
  #invalidations = new Subject<void>();
  #value: ResourceValue<ResourceData<D>>;
  readonly observations$: Observable<Observation<ResourceData<D>>>;
  readonly values$: Observable<ResourceData<D>>;
  readonly definition: D;
  readonly params: ResourceTarget<D>;

  /** @internal 只由绑定按正式实现创建。 */
  constructor(readonly scope: Scope, definition: D, params: ResourceTarget<D>, implementation: ResourceImplementationOptions<D>) {
    const state = scope.state<Observation<ResourceData<D>>>({ status: 'pending', generation: 0 });
    this.#value = new ResourceValue(scope, state);
    this.definition = definition;
    this.params = params;
    const snapshots$ = this.#value.observations$;
    const pending = () => state.set({ status: 'pending', generation: ++this.#generation });
    const disconnect = () => {
      const connection = this.#connection;
      this.#connection = undefined;
      // 先撤旧事实；来源清理可能同步重订阅，新连接的 ready 不属于这次退出。
      if (!this.#closed && !scope.disposed) pending();
      connection?.unsubscribe();
    };
    const start = () => {
      if (this.#connection || this.#closed) return;
      const connection = new Subscription();
      this.#connection = connection;
      const source$ = this.#invalidations.pipe(startWith(undefined), switchLatest(() => new Observable<Observation<ResourceData<D>>>(subscriber => {
        const controller = new AbortController();
        // observe/invalidations 工厂尚未返回时也可能退出 owner，不能延后登记取消。
        subscriber.add(() => controller.abort());
        if (subscriber.closed) return;
        const context = Object.freeze({ signal: controller.signal });
        normalizeResource(defer(() => implementation.observe
          ? implementation.observe(params, context)
          : observeResource<ResourceTarget<D>, z.input<D['value']>>({ key$: of(params), read: (current, signal) => implementation.read(current, { signal }),
            ...implementation.invalidations ? { invalidations: current => implementation.invalidations!(current, context) } : {} })))
          .pipe(mapResource(value => z.parse<D['value']>(definition.value, value))).subscribe(subscriber);
      }), pending));
      const subscriber = new Subscriber<Observation<ResourceData<D>>>({
        next: observation => {
          if (!this.#closed && !scope.disposed) state.set(Object.freeze({ ...observation, generation: ++this.#generation }));
        },
        error: (error: unknown) => scope.observerError(error),
        complete: () => {},
      });
      // 首次同步交付可能释放 scope；来源执行前先登记实际订阅。
      connection.add(subscriber);
      normalizeResource(source$).subscribe(subscriber);
    };
    this.observations$ = new Observable(subscriber => {
      if (this.#closed || scope.disposed) { subscriber.error(new ScopeDisposedError()); return; }
      this.#observers++;
      // 基线和来源均可同步通知；先登记退出，take/firstValueFrom 才能立即撤销最后一个连接。
      subscriber.add(() => {
        if (--this.#observers === 0) disconnect();
      });
      snapshots$.subscribe(subscriber);
      // 同步基线可以接入另一个观察者并关闭当前消费者；仍有人观察时须建立同一条连接。
      if (this.#observers > 0) start();
    });
    this.values$ = this.observations$.pipe(mergeMap(observation => observation.status === 'pending' ? EMPTY
      : observation.status === 'error' ? throwError(() => observation.error) : of(observation.value)));
    scope.own(() => {
      this.#closed = true;
      this.#connection?.unsubscribe();
      this.#connection = undefined;
      this.#invalidations.complete();
    });
  }

  get value(): Observation<ResourceData<D>> { return this.#value.value; }
  peek(): Observation<ResourceData<D>> { return this.#value.peek(); }

  read(signal?: AbortSignal): Promise<ResourceData<D>> {
    this.scope.assertActive();
    if (signal?.aborted) return Promise.reject(signal.reason);
    const stop = this.subscribe(() => {});
    return this.#value.read(signal).finally(stop);
  }

  subscribe(accept: (observation: Observation<ResourceData<D>>) => void): () => void {
    this.scope.assertActive();
    const subscription = this.observations$.subscribe(observation => {
      try { accept(observation); } catch (error) { this.scope.observerError(error); }
    });
    return () => subscription.unsubscribe();
  }

  /** 显式撤销当前代次并重连权威来源；没有观察者时保持 pending，不偷偷启动查询。 */
  invalidate(): void { this.scope.assertActive(); this.#invalidations.next(); }
}

export interface ResourceBinding<D extends ResourceContract = ResourceContract> {
  readonly scope: Scope;
  readonly definition: D;
  instance(params: ResourceParameters<D>): ResourceInstance<D>;
}

const bindings = new WeakMap<Scope, WeakMap<object, unknown>>();

/** 同一 scope 和实现复用参数化实例；此处只登记身份，不创建查询连接。 */
export function bindResource<D extends ResourceContract>(implementation: ResourceImplementation<D>,
  options: { readonly scope: Scope }): ResourceBinding<D> {
  const { scope } = options;
  scope.assertActive();
  const configured = resourceImplementation(implementation);
  let owned = bindings.get(scope);
  if (!owned) { owned = new WeakMap(); bindings.set(scope, owned); }
  const previous = owned.get(implementation);
  // 精确 scope、实现对象只对应一种 D；不按名字或调用者泛型恢复定义。
  if (previous) return previous as ResourceBinding<D>;
  const instances = new Map<string, ResourceInstance<D>>();
  const binding: ResourceBinding<D> = Object.freeze({ scope, definition: implementation.definition,
    instance(raw: ResourceParameters<D>) {
      scope.assertActive();
      const params = ownParameters(z.parse<D['params']>(implementation.definition.params, raw));
      const key = parameterIdentity(params);
      const previous = instances.get(key);
      if (previous) return previous;
      const instance = new ResourceInstance<D>(scope, implementation.definition, params, configured);
      instances.set(key, instance);
      return instance;
    },
  });
  owned.set(implementation, binding);
  scope.own(() => instances.clear());
  return binding;
}
