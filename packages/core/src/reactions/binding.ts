import { Observable, ReplaySubject, Subject, Subscriber, Subscription, defer, from, isObservable, map, of } from 'rxjs';
import { z } from 'zod';
import type { Scope } from '../reactivity/scope.ts';
import { ownParameters, parameterIdentity } from '../reactivity/parameters.ts';
import type { ReactionContract, ReactionInput, ReactionParameters, ReactionResult, ReactionTarget } from './definition.ts';
import { reactionImplementation, type ReactionImplementation, type ReactionImplementationOptions } from './implementation.ts';

export type ReactionState =
  | { readonly status: 'running'; readonly emitted: number }
  | { readonly status: 'completed' | 'stopped'; readonly emitted: number }
  | { readonly status: 'failed'; readonly emitted: number; readonly error: unknown };
export type ReactionSettlement = Exclude<ReactionState, { readonly status: 'running' }>;
export interface ReactionEvent<D extends ReactionContract> {
  /** 当前运行内的事件序号；没有持久去重或授权含义。 */
  readonly sequence: number;
  readonly event: ReactionInput<D>;
  readonly result: ReactionResult<D>;
}

function isPromiseLike<T>(value: T | PromiseLike<T>): value is PromiseLike<T> {
  return value !== null && (typeof value === 'object' || typeof value === 'function') && typeof Reflect.get(value, 'then') === 'function';
}

function resultSource<T>(value: T | PromiseLike<T> | Observable<T>): Observable<T> {
  if (isObservable(value)) return value;
  if (isPromiseLike(value)) return from(Promise.resolve(value));
  return of(value);
}

/** 一个运行持有唯一实际订阅；观察运行不会再次执行处理器。 */
export class ReactionRun<D extends ReactionContract> {
  #state: ReactionState = Object.freeze({ status: 'running', emitted: 0 });
  #states = new ReplaySubject<ReactionState>(1);
  #events = new Subject<ReactionEvent<D>>();
  #connection = new Subscription();
  #controller = new AbortController();
  #completion = Promise.withResolvers<ReactionSettlement>();
  #release: (() => void) | undefined;
  #activated = false;
  /** 回放当前运行状态；来源、执行与释放失败均通过 failed 状态交付。 */
  readonly state$ = this.#states.asObservable();
  /** 热成功结果流；运行终止时正常 complete，失败通过 state$ 与 settled 观察。 */
  readonly events$ = this.#events.asObservable();
  /** 始终 resolve 本次运行的终态；失败为带 error 的 failed 结算，不 reject。 */
  readonly settled = this.#completion.promise;

  /** @internal 运行构造与实际订阅分开，避免同步完成跨过实例的 active 身份登记。 */
  constructor(readonly definition: D, readonly params: ReactionTarget<D>, private readonly scope: Scope,
    private readonly implementation: ReactionImplementationOptions<D>, private readonly ended: () => void) {
    this.#states.next(this.#state);
  }
  snapshot(): ReactionState { return this.#state; }

  /** @internal 只有 instance.start 在登记 active 后调用。 */
  activate(): void {
    this.scope.assertActive();
    if (this.#activated || this.#state.status !== 'running') throw new Error('REACTIVE_REACTION_ALREADY_STARTED');
    this.#activated = true;
    this.#release = this.scope.own(() => this.stop());
    let sequence = 0;
    const subscriber = new Subscriber<ReactionEvent<D>>({
      next: event => {
        if (this.#state.status !== 'running') return;
        this.#state = Object.freeze({ status: 'running', emitted: this.#state.emitted + 1 });
        this.#states.next(this.#state);
        this.#events.next(event);
      },
      error: (error: unknown) => this.finish({ status: 'failed', emitted: this.#state.emitted, error }),
      complete: () => this.finish({ status: 'completed', emitted: this.#state.emitted }),
    });
    // 同步来源也必须在首个业务回调之前取得 owner 的取消连接。
    this.#connection.add(subscriber);
    defer(() => defer(() => this.implementation.observe(this.params, { signal: this.#controller.signal })).pipe(
      map(raw => ({ sequence: ++sequence, event: z.parse<D['event']>(this.definition.event, raw) })),
      this.implementation.operator((occurrence: { readonly sequence: number; readonly event: ReactionInput<D> }) =>
        defer(() => resultSource(this.implementation.execute(occurrence.event, { params: this.params })))
          .pipe(map(raw => Object.freeze({ ...occurrence, result: z.parse<D['result']>(this.definition.result, raw) })))),
    )).subscribe(subscriber);
  }

  /** 停止感知及尚未运行的 RxJS 工作，不向已经受理的 Action/Job 追加业务 cancel。 */
  stop(): void { this.finish({ status: 'stopped', emitted: this.#state.emitted }); }

  private finish(state: ReactionSettlement) {
    if (this.#state.status !== 'running') return;
    this.#state = Object.freeze(state);
    const failures: unknown[] = [];
    try { this.#connection.unsubscribe(); } catch (error) { failures.push(error); }
    try { this.#controller.abort(); } catch (error) { failures.push(error); }
    try { this.#release?.(); } catch (error) { failures.push(error); }
    this.#release = undefined;
    if (failures.length) this.#state = Object.freeze({ status: 'failed', emitted: state.emitted,
      error: new AggregateError(state.status === 'failed' ? [state.error, ...failures] : failures, 'REACTIVE_REACTION_DISPOSAL_FAILED') });
    this.ended();
    this.#states.next(this.#state);
    this.#states.complete(); this.#events.complete();
    this.#completion.resolve(this.#state);
  }
}

export class ReactionInstance<D extends ReactionContract> {
  #active: ReactionRun<D> | undefined;
  /** @internal 绑定提供解析过的自有参数和正式实现。 */
  constructor(readonly scope: Scope, readonly definition: D, readonly params: ReactionTarget<D>,
    private readonly implementation: ReactionImplementationOptions<D>) {}

  /** 副作用从此处显式发生；同实例不能无意建立第二条并行关系。 */
  start(): ReactionRun<D> {
    this.scope.assertActive();
    if (this.#active) throw new Error('REACTIVE_REACTION_ALREADY_RUNNING');
    const run = new ReactionRun<D>(this.definition, this.params, this.scope, this.implementation,
      () => { if (this.#active === run) this.#active = undefined; });
    this.#active = run;
    run.activate();
    return run;
  }
}

export interface ReactionBinding<D extends ReactionContract = ReactionContract> {
  readonly scope: Scope;
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
  const binding: ReactionBinding<D> = Object.freeze({ scope, definition: implementation.definition,
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
