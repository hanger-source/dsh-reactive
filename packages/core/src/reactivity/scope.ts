import { signal, computed, effect, batch, type ReadonlySignal, type Signal } from '@preact/signals-core';
import { Observable, Subscriber, EmptyError } from 'rxjs';
import { ResourceValue } from '../resources/value.ts';
import type { Observation } from '../resources/observation.ts';
export { batch };
type Node<T> = Pick<ReadonlySignal<T>, 'value' | 'peek' | 'subscribe'>;

/** 同步依赖图不能接纳 Promise；异步工作通过资源或标准 Observable 表达。 */
function synchronous<T>(value: T, operation: string): T {
  if (value !== null && (typeof value === 'object' || typeof value === 'function')
    && typeof (value as { then?: unknown }).then === 'function') {
    // 回调已经返回的异步工作不再有消费者，消费其拒绝，避免制造未处理异常。
    void Promise.resolve(value).catch(() => {});
    throw new TypeError('REACTIVE_' + operation + '_MUST_BE_SYNCHRONOUS');
  }
  return value;
}

export class ScopeDisposedError extends Error {
  constructor() { super('REACTIVE_SCOPE_DISPOSED'); }
}
export interface ScopeOptions { onObserverError?: (error: unknown) => void; }

/** 内存依赖的 owner；与页面订阅、原生会话持久化分别拥有生命周期。 */
export class Scope {
  #released = false;
  #controller = new AbortController();
  #signal = this.#controller.signal;
  #releases = new Set<() => void>();
  constructor(private readonly options: ScopeOptions = {}) {}
  get disposed() { return this.#signal.aborted; }
  get signal() { return this.#signal; }
  assertActive() { this.#signal.throwIfAborted(); }
  own(release: () => void): () => void {
    this.assertActive();
    const stop = () => { if (this.#releases.delete(stop)) release(); };
    this.#releases.add(stop);
    return stop;
  }
  child() {
    this.assertActive();
    const child = new Scope(this.options);
    child.#signal = AbortSignal.any([this.signal, child.signal]);
    const release = this.own(() => child.dispose());
    child.own(() => release());
    return child;
  }
  state<T>(initial: T): Cell<T> { this.assertActive(); return new Cell(this, signal(initial)); }
  computed<T>(read: () => T): Value<T> {
    this.assertActive(); return new Value(this, computed(() => synchronous(read(), 'COMPUTED')));
  }
  observerError(error: unknown) {
    if (this.options.onObserverError) this.options.onObserverError(error);
    else console.error('reactive.observer.failed', error);
  }
  effect(run: () => void | (() => void)) {
    this.assertActive();
    const release = effect(() => this.disposed ? undefined : synchronous(run(), 'EFFECT'));
    if (this.disposed) { release(); this.assertActive(); }
    return this.own(release);
  }
  /** 来源只执行一次，公开读取和观察共享同一份当前状态，不随消费者退订而停止。 */
  observe<T>(source$: Observable<T>): ResourceValue<T> {
    this.assertActive();
    const state = this.state<Observation<T>>({ status: 'pending', generation: 0 });
    let generation = 0;
    let received = false;
    const subscription = new Subscriber<T>({
      next: value => {
        if (this.disposed) return;
        received = true;
        state.set({ status: 'ready', generation: ++generation, value, observedAt: Date.now() });
      },
      error: (error: unknown) => {
        if (!this.disposed) state.set({ status: 'error', generation: ++generation, error });
        release();
      },
      complete: () => {
        if (!received && !this.disposed) state.set({ status: 'error', generation: ++generation, error: new EmptyError() });
        release();
      },
    });
    const release = this.own(() => subscription.unsubscribe());
    source$.subscribe(subscription);
    return new ResourceValue(this, state);
  }
  dispose() {
    if (this.#released) return;
    this.#released = true;
    this.#controller.abort(new ScopeDisposedError());
    const failures: unknown[] = [];
    for (const release of [...this.#releases].reverse()) {
      try { release(); } catch (error) { failures.push(error); }
    }
    if (failures.length) throw new AggregateError(failures, 'REACTIVE_SCOPE_DISPOSAL_FAILED');
  }
}

/** 同步依赖使用 value 读取，map/computed 自动跟踪实际读取的分支。 */
export class Value<T> {
  readonly #scope: Scope;
  readonly changes$: Observable<T>;
  constructor(scope: Scope, protected readonly node: Node<T>) {
    this.#scope = scope;
    this.changes$ = new Observable(subscriber => {
      if (scope.disposed) { subscriber.complete(); return; }
      const release = scope.own(() => subscriber.complete());
      let stop: (() => void) | undefined;
      try { stop = node.subscribe(value => { if (!scope.disposed) subscriber.next(value); }); }
      catch (error) { subscriber.error(error); }
      return () => { stop?.(); release(); };
    });
  }
  protected assertActive(): void { this.#scope.assertActive(); }
  get value(): T { this.assertActive(); return this.node.value; }
  get(): T { return this.value; }
  peek(): T { this.assertActive(); return this.node.peek(); }
  read(signal?: AbortSignal): Promise<T> {
    signal?.throwIfAborted();
    return Promise.resolve(this.peek());
  }
  map<U>(project: (value: T) => U): Value<U> { return this.#scope.computed(() => project(this.value)); }
  subscribe(accept: (value: T) => void): () => void {
    this.assertActive();
    // signals 的同步订阅先登记依赖，再交付基线；调用者无需自己先读再订阅。
    const stop = this.node.subscribe(value => {
      if (this.#scope.disposed) return;
      try { accept(value); } catch (error) { this.#scope.observerError(error); }
    });
    if (this.#scope.disposed) { stop(); return () => {}; }
    return this.#scope.own(stop);
  }
}

export class Cell<T> extends Value<T> {
  declare protected readonly node: Signal<T>;
  set(value: T) { this.assertActive(); this.node.value = value; }
  update(change: (value: T) => T) { this.set(change(this.peek())); }
}
