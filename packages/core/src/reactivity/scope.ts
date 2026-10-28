import { signal, computed, effect, batch, untracked, type ReadonlySignal, type Signal } from '@preact/signals-core';
import { Observable, Subscriber, EmptyError } from 'rxjs';
import { ResourceValue } from '../resources/value.ts';
import type { Observation } from '../resources/observation.ts';
import { synchronous } from './synchronous.ts';
export { batch };
type Node<T> = Pick<ReadonlySignal<T>, 'value' | 'peek' | 'subscribe'>;

export class ScopeDisposedError extends Error {
  constructor() { super('REACTIVE_SCOPE_DISPOSED'); }
}
export interface ScopeOptions { onObserverError?: (error: unknown) => void; }

/** 内存依赖的 owner；与页面订阅、原生会话持久化分别拥有生命周期。 */
export class Scope {
  #released = false;
  #controller = new AbortController();
  #signal = this.#controller.signal;
  #availability = signal(true);
  #parent?: Scope;
  #releases = new Set<() => void>();
  constructor(private readonly options: ScopeOptions = {}) {}
  get disposed() { return this.#signal.aborted; }
  get signal() { return this.#signal; }
  private trackAvailability(): void {
    // 只把读取资格接入现有signals图；取消原因仍由原AbortSignal持有。
    this.#availability.value;
    this.#parent?.trackAvailability();
  }
  assertActive() { this.trackAvailability(); this.#signal.throwIfAborted(); }
  own(release: () => void): () => void {
    this.assertActive();
    const stop = () => { if (this.#releases.delete(stop)) release(); };
    this.#releases.add(stop);
    return stop;
  }
  child() {
    this.assertActive();
    const child = new Scope(this.options);
    child.#parent = this;
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
  dispose(reason: unknown = new ScopeDisposedError()) {
    if (this.#released) return;
    this.#released = true;
    const failures: unknown[] = [];
    try {
      batch(() => {
        // 派生缓存先失效；abort回调和子Scope清理重入时不能再读取旧值。
        this.#availability.value = false;
        this.#controller.abort(reason);
        for (const release of [...this.#releases].reverse()) {
          try { release(); } catch (error) { failures.push(error); }
        }
      });
    } catch (error) { failures.push(error); }
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
      try { stop = this.watch(value => subscriber.next(value), error => subscriber.error(error)); }
      catch (error) { subscriber.error(error); }
      return () => { stop?.(); release(); };
    });
  }
  protected assertActive(): void { this.#scope.assertActive(); }
  get value(): T { this.assertActive(); const value = this.node.value; this.assertActive(); return value; }
  get(): T { return this.value; }
  /** 不跟踪业务值；借用读取的owner资格仍参与当前依赖。 */
  peek(): T { this.assertActive(); const value = this.node.peek(); this.assertActive(); return value; }
  read(signal?: AbortSignal): Promise<T> {
    signal?.throwIfAborted();
    return Promise.resolve(this.peek());
  }
  map<U>(project: (value: T) => U): Value<U> { return this.#scope.computed(() => project(this.value)); }
  private watch(accept: (value: T) => void, onError: (error: unknown) => void): () => void {
    return effect(() => {
      if (this.#scope.disposed) return;
      let value: T;
      try { value = this.value; }
      catch (error) {
        if (!this.#scope.disposed) untracked(() => onError(error));
        return;
      }
      untracked(() => accept(value));
    });
  }
  /** 读取失败交给onError或observerError，保留依赖以等待当前分支恢复。 */
  subscribe(accept: (value: T) => void, onError?: (error: unknown) => void): () => void {
    this.assertActive();
    const stop = this.watch(value => {
      try { accept(value); } catch (error) { this.#scope.observerError(error); }
    }, onError ? error => {
      try { onError(error); } catch (callbackError) { this.#scope.observerError(callbackError); }
    } : error => this.#scope.observerError(error));
    if (this.#scope.disposed) { stop(); return () => {}; }
    return this.#scope.own(stop);
  }
}

export class Cell<T> extends Value<T> {
  declare protected readonly node: Signal<T>;
  set(value: T) { this.assertActive(); this.node.value = value; }
  update(change: (value: T) => T) { this.set(change(this.peek())); }
}
