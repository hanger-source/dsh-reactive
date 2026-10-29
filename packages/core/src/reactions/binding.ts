import { Observable, Subject, Subscriber, Subscription, UnsubscriptionError, defer, from, isObservable, map, of } from 'rxjs';
import { z } from 'zod';
import type { Scope } from '../reactivity/scope.ts';
import type { Observation } from '../resources/observation.ts';
import { ResourceReader } from '../resources/reader.ts';
import { ownParameters, parameterIdentity } from '../reactivity/parameters.ts';
import type { ReactionContract, ReactionInput, ReactionParameters, ReactionResult, ReactionTarget } from './definition.ts';
import { reactionImplementation, type ReactionImplementation, type ReactionImplementationOptions } from './implementation.ts';

export type ReactionState =
  | { readonly status: 'running'; readonly emitted: number }
  | { readonly status: 'draining'; readonly reason: 'completed' | 'stopped'; readonly pending: number; readonly emitted: number }
  | { readonly status: 'draining'; readonly reason: 'failed'; readonly pending: number; readonly emitted: number; readonly error: unknown }
  | { readonly status: 'completed' | 'stopped'; readonly emitted: number }
  | { readonly status: 'failed'; readonly emitted: number; readonly error: unknown };
export type ReactionSettlement = Extract<ReactionState, { readonly status: 'completed' | 'stopped' | 'failed' }>;
export interface ReactionEvent<D extends ReactionContract> {
  /** 当前运行内的事件序号；没有持久去重或授权含义。 */
  readonly sequence: number;
  readonly event: ReactionInput<D>;
  readonly result: ReactionResult<D>;
}

function isPromiseLike<T>(value: T | PromiseLike<T>): value is PromiseLike<T> {
  return value !== null && (typeof value === 'object' || typeof value === 'function') && typeof Reflect.get(value, 'then') === 'function';
}

/** 一个运行持有唯一实际订阅；观察运行不会再次执行处理器。 */
export class ReactionRun<D extends ReactionContract> {
  readonly #scope: Scope;
  #current: Extract<Observation<ReactionState>, { status: 'ready' }> = Object.freeze({
    status: 'ready', generation: 1, observedAt: Date.now(), value: Object.freeze({ status: 'running', emitted: 0 }),
  });
  #states = new Subject<void>();
  #events = new Subject<ReactionEvent<D>>();
  #connection = new Subscription();
  #controller = new AbortController();
  #completion = Promise.withResolvers<ReactionSettlement>();
  #release: (() => void) | undefined;
  #activated = false;
  #ending: 'completed' | 'stopped' | 'failed' | undefined;
  #finished = false;
  #depth = 0;
  #pending = 0;
  #failures: unknown[] = [];
  #failure: unknown;
  #knownFailures = new Set<unknown>();
  /** 当前运行的同源资源；退出感知后仍交付draining和真实终态，不随原Scope撤销。 */
  readonly state = new ResourceReader<ReactionState>({ peek: () => this.#current,
    observations$: new Observable<Observation<ReactionState>>(subscriber => {
      if (this.#finished) { subscriber.next(this.#current); subscriber.complete(); return; }
      // 先连接通知再交当前事实；事件回调内重入读取也不能重放通知前的旧计数。
      subscriber.add(this.#states.subscribe({ next: () => subscriber.next(this.#current), complete: () => subscriber.complete() }));
      if (!subscriber.closed) subscriber.next(this.#current);
    }),
  });
  /** 回放当前运行状态；来源、执行与释放失败均通过 failed 状态交付。 */
  readonly state$ = this.state.values$;
  /** RxJS 选中的热成功结果流；关系终止时 complete，失选的 Promise 不补发结果。 */
  readonly events$ = this.#events.asObservable();
  /** 等已开始的裸 Promise 及结果解析完成后 resolve 终态；失败不 reject。 */
  readonly settled = this.#completion.promise;

  /** @internal 运行构造与实际订阅分开，避免同步完成跨过实例的 active 身份登记。 */
  constructor(readonly definition: D, readonly params: ReactionTarget<D>, scope: Scope,
    private readonly implementation: ReactionImplementationOptions<D>, private readonly ended: () => void) {
    this.#scope = scope;
  }
  snapshot(): ReactionState { return this.#current.value; }

  private updateState(state: ReactionState): void {
    this.#current = Object.freeze({ status: 'ready', generation: this.#current.generation + 1, observedAt: Date.now(), value: state });
  }

  /** @internal 只有 instance.start 在登记 active 后调用。 */
  activate(): void {
    this.#scope.assertActive();
    if (this.#activated || this.#current.value.status !== 'running') throw new Error('REACTIVE_REACTION_ALREADY_STARTED');
    this.#activated = true;
    this.#release = this.#scope.own(() => { this.stop(); return this.settled; });
    let sequence = 0;
    const subscriber = new Subscriber<ReactionEvent<D>>({
      next: event => {
        if (!this.accepting()) return;
        this.updateState(Object.freeze({ status: 'running', emitted: this.#current.value.emitted + 1 }));
        this.#events.next(event);
        if (this.#current.value.status === 'running') this.#states.next();
      },
      error: (error: unknown) => this.fail(error),
      complete: () => this.end(this.#scope.disposed ? 'stopped' : 'completed'),
    });
    // 同步来源也必须在首个业务回调之前取得 owner 的取消连接。
    this.#connection.add(subscriber);
    this.enter();
    try { defer(() => this.source(() => this.implementation.observe(this.params, { signal: this.#controller.signal })).pipe(
      map(raw => {
        try { return { sequence: ++sequence, event: z.parse<D['event']>(this.definition.event, raw) }; }
        catch (error) { this.fail(error); throw error; }
      }),
      this.implementation.operator((occurrence: { readonly sequence: number; readonly event: ReactionInput<D> }) => defer(() => {
        let parsed: { readonly value: ReactionResult<D> } | undefined;
        return this.source(() => this.execute(occurrence.event, value => { parsed = { value }; }))
          .pipe(map(raw => Object.freeze({ ...occurrence, result: parsed ? parsed.value : this.parse(raw) })));
      })),
    )).subscribe(subscriber); }
    catch (error) { this.fail(error); }
    finally { this.leave(); }
  }

  /** 立即停止来源、排队及 Observable 订阅；已开始的裸 Promise 仍归本运行结算。 */
  stop(): void { this.end('stopped'); }

  private enter(): void { this.#depth++; }
  private leave(): void { this.#depth--; this.settle(); }
  private accepting(): boolean { return this.#ending === undefined && !this.#scope.disposed; }

  /** 同步 subscribe 返回前不交付终止，才能接收随后登记的 teardown。 */
  private source<T>(factory: () => Observable<T>): Observable<T> {
    return new Observable(subscriber => {
      if (subscriber.closed || !this.accepting()) return;
      this.enter();
      let connecting = true, delivered = false;
      let terminal: { readonly kind: 'complete' } | { readonly kind: 'error'; readonly error: unknown } | undefined;
      const close = () => {
        this.enter();
        try { connection.unsubscribe(); }
        catch (error) {
          if (terminal?.kind === 'error') this.fail(terminal.error);
          this.fail(error);
        } finally { this.leave(); }
      };
      const deliver = () => {
        if (connecting || delivered || !terminal) return;
        delivered = true;
        this.enter();
        try {
          close();
          if (terminal.kind === 'error') {
            if (subscriber.closed) this.fail(terminal.error);
            else subscriber.error(terminal.error);
          } else if (!subscriber.closed) subscriber.complete();
        } finally { this.leave(); }
      };
      const connection = new Subscriber<T>({
        next: value => {
          this.enter();
          try { subscriber.next(value); }
          finally { this.leave(); }
        },
        error: (error: unknown) => { terminal = { kind: 'error', error }; deliver(); },
        complete: () => { terminal = { kind: 'complete' }; deliver(); },
      });
      // 先登记连接；execute/producer 内同步 stop 也能立即关闭它。
      subscriber.add(close);
      try {
        const source = factory();
        // factory 可以同步释放 Scope；已关闭的冷来源不能因此开始副作用。
        if (!subscriber.closed && !connection.closed && this.accepting()) source.subscribe(connection);
      } catch (error) {
        if (terminal?.kind === 'error') this.fail(terminal.error);
        this.fail(error);
      } finally {
        connecting = false;
        try { deliver(); }
        finally { this.leave(); }
      }
    });
  }

  private parse(raw: z.input<D['result']>): ReactionResult<D> {
    try { return z.parse<D['result']>(this.definition.result, raw); }
    catch (error) { this.fail(error); throw error; }
  }

  private execute(event: ReactionInput<D>, parsed: (value: ReactionResult<D>) => void): Observable<z.input<D['result']>> {
    const value = this.implementation.execute(event, { params: this.params });
    // 屏障直接订阅业务 Observable，避免中间已关闭的 Subscriber 吞掉迟登记 teardown 的异常。
    if (isObservable(value)) return value;
    if (!isPromiseLike(value)) { parsed(this.parse(value)); return of(value); }
    // 只登记 RxJS 真正进入过的 execute；切换或退出不拥有已开始 Promise 的结果。
    this.#pending++;
    const result = Promise.resolve(value).then(raw => { parsed(this.parse(raw)); return raw; });
    void result.then(
      () => { this.#pending--; this.settle(); },
      error => {
        this.enter();
        try { this.fail(error); this.#pending--; }
        finally { this.leave(); }
      },
    );
    return from(result);
  }

  private record(error: unknown): void {
    // RxJS 用 UnsubscriptionError 汇集释放错误；业务 AggregateError 保持原对象。
    if (error instanceof UnsubscriptionError && error.errors.length) { for (const failure of error.errors) this.record(failure); return; }
    if (this.#knownFailures.has(error)) return;
    this.#knownFailures.add(error);
    this.#failures.push(error);
    this.#failure = this.#failures.length === 1 ? error
      : new AggregateError(this.#failures, 'REACTIVE_REACTION_FAILED');
  }

  private fail(error: unknown): void {
    if (this.#finished) return;
    this.record(error);
    this.end('failed');
  }

  private end(reason: 'completed' | 'stopped' | 'failed'): void {
    if (this.#finished) return;
    if (this.#ending) {
      if (reason === 'failed') this.#ending = 'failed';
      this.settle();
      return;
    }
    this.#ending = reason;
    this.enter();
    try {
      try { this.#connection.unsubscribe(); } catch (error) { this.record(error); }
      try { this.#controller.abort(); } catch (error) { this.record(error); }
      const release = this.#release; this.#release = undefined;
      try { release?.(); } catch (error) { this.record(error); }
      this.#events.complete();
    } finally {
      if (this.#failures.length) this.#ending = 'failed';
      this.leave();
    }
  }

  private settle(): void {
    if (!this.#ending || this.#finished) return;
    const reason = this.#ending, emitted = this.#current.value.emitted;
    if (this.#depth || this.#pending) {
      const previous = this.#current.value;
      if (previous.status === 'draining' && previous.reason === reason && previous.pending === this.#pending
        && (previous.reason !== 'failed' || previous.error === this.#failure)) return;
      this.updateState(Object.freeze(reason === 'failed'
        ? { status: 'draining', reason, pending: this.#pending, emitted, error: this.#failure }
        : { status: 'draining', reason, pending: this.#pending, emitted }));
      this.#states.next();
      return;
    }
    this.#finished = true;
    const state: ReactionSettlement = Object.freeze(reason === 'failed'
      ? { status: 'failed', emitted, error: this.#failure } : { status: reason, emitted });
    this.updateState(state);
    this.ended();
    try { this.#states.next(); this.#states.complete(); }
    finally { this.#completion.resolve(state); }
  }
}

export class ReactionInstance<D extends ReactionContract> {
  readonly #scope: Scope;
  #active: ReactionRun<D> | undefined;
  /** @internal 绑定提供解析过的自有参数和正式实现。 */
  constructor(scope: Scope, readonly definition: D, readonly params: ReactionTarget<D>,
    private readonly implementation: ReactionImplementationOptions<D>) { this.#scope = scope; }

  /** 副作用从此处显式发生；同实例不能无意建立第二条并行关系。 */
  start(): ReactionRun<D> {
    this.#scope.assertActive();
    if (this.#active) throw new Error('REACTIVE_REACTION_ALREADY_RUNNING');
    const run = new ReactionRun<D>(this.definition, this.params, this.#scope, this.implementation,
      () => { if (this.#active === run) this.#active = undefined; });
    this.#active = run;
    run.activate();
    return run;
  }
}

export interface ReactionBinding<D extends ReactionContract = ReactionContract> {
  readonly definition: D;
  instance(params: ReactionParameters<D>): ReactionInstance<D>;
}
const bindings = new WeakMap<Scope, WeakMap<object, unknown>>();

export function bindReaction<D extends ReactionContract>(implementation: ReactionImplementation<D>,
  options: { readonly scope: Scope }): ReactionBinding<D> {
  const { scope } = options;
  scope.assertActive();
  const configured = reactionImplementation(implementation);
  let owned = bindings.get(scope);
  if (!owned) { owned = new WeakMap(); bindings.set(scope, owned); }
  const previous = owned.get(implementation);
  // 精确 scope、implementation 的私有登记只持有一种合同 D。
  if (previous) return previous as ReactionBinding<D>;
  const instances = new Map<string, ReactionInstance<D>>();
  const binding: ReactionBinding<D> = Object.freeze({ definition: implementation.definition,
    instance(raw: ReactionParameters<D>) {
      scope.assertActive();
      const params = ownParameters(z.parse<D['params']>(implementation.definition.params, raw));
      const key = parameterIdentity(params);
      const previous = instances.get(key);
      if (previous) return previous;
      const instance = new ReactionInstance<D>(scope, implementation.definition, params, configured);
      instances.set(key, instance);
      return instance;
    },
  });
  owned.set(implementation, binding);
  scope.own(() => instances.clear());
  return binding;
}
