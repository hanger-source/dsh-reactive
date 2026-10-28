import { createContext, createElement, useContext, useEffect, useLayoutEffect, useMemo, useSyncExternalStore, type DependencyList, type ReactNode } from 'react';
import { EmptyError, ReplaySubject, Subscriber, defer, fromEvent, isObservable, of, share, takeUntil, type Observable } from 'rxjs';
import type { z } from 'zod';
import { bindAction, implementAction, type LocalActionOptions, type ActionCallContext, type ActionDefinition, type ActionExecutionContext, type ActionImplementation, type ActionOperationEvent, type ActionReference, type BoundAction } from '../actions/contract.ts';
import type { Json } from '../conversations/protocol.ts';
import { bindOperation, type BoundOperation, type OperationAccessContext, type OperationBindingReference, type OperationCommand, type OperationContract, type OperationFeedbackPayload, type OperationFeedbackRead, type OperationOwner, type OperationReference } from '../operations/contract.ts';
import { Scope } from '../reactivity/scope.ts';
import { type Observation } from '../resources/observation.ts';
import { bindResource, type ResourceInstance } from '../resources/binding.ts';
import type { ResourceReadable } from '../resources/reader.ts';
import { isResourceImplementation, type ResourceImplementation } from '../resources/implementation.ts';
import type { ResourceContract, ResourceParameters, ResourceData } from '../resources/definition.ts';
import { createResource, type LocalResource, type LocalResourceOptions } from '../resources/local.ts';
import { bindReaction, type ReactionInstance } from '../reactions/binding.ts';
import type { ReactionContract, ReactionParameters } from '../reactions/definition.ts';
import type { ReactionImplementation } from '../reactions/implementation.ts';
import { createReaction, type LocalReaction, type LocalReactionOptions } from '../reactions/local.ts';
import { connect, normalizeResource, type SnapshotSource } from '../reactivity/observable.ts';

/** 宿主拥有程序寿命、真实执行来源与步骤准入；组件不自行授予业务权限。 */
export interface ProgramHookOwner {
  readonly scope: Scope;
  /** 参数化资源与反应共享此程序作用域；程序失败即释放，不取消已受理业务。 */
  readonly reactivityScope: Scope;
  /** 程序失败或释放的查询/关系寿命；已受理业务执行保留自己的 signal。 */
  readonly signal: AbortSignal;
  execution(action: ActionReference): ActionExecutionContext;
  admit(action: ActionReference, input: unknown, call: ActionCallContext<unknown>, execution: ActionExecutionContext): void | PromiseLike<void>;
  operation?(action: ActionReference, event: ActionOperationEvent, execution: ActionExecutionContext): void | PromiseLike<void>;
  operationAccess(binding: OperationBindingReference): OperationAccessContext;
  readOperationFeedback<D extends OperationContract>(binding: OperationBindingReference<D>, id: string, access: OperationAccessContext):
    Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>>;
  fail(error: unknown): void;
  beforeStep(check: (signal: AbortSignal) => void | Promise<void>): () => void;
}

const ownerContext = createContext<ProgramHookOwner | null>(null);

/** @internal 每个程序根注入实际 owner；多个会话不能共用执行身份。 */
export function ProgramHookProvider(props: { readonly owner: ProgramHookOwner; readonly children?: ReactNode }) {
  return createElement(ownerContext.Provider, { value: props.owner }, props.children);
}

function useOwner(): ProgramHookOwner {
  const owner = useContext(ownerContext);
  if (!owner) throw new Error('REACTIVE_PROGRAM_HOOK_OWNER_REQUIRED');
  owner.scope.assertActive();
  owner.signal.throwIfAborted();
  return owner;
}

/**
 * target 只纯构造目标；deps 与 React useMemo 一样列出该构造依赖。
 * 相同 owner、implementation 与 deps 保留绑定；改变目标必须改变 deps。
 * 局部 factory 同时返回实现选项与 target，沿用同一绑定和可信来源，不隐式执行。
 * 普通调用由宿主逐次建立来源，原生调用仍显式传入其真实执行关联。
 */
export function useAction<I extends z.ZodType, O extends z.ZodType, S extends z.ZodType<Json>, Observed extends Json = never,
  P extends OperationContract = never>(
  implementation: ActionImplementation<ActionDefinition<I, O, S, Observed, P>>, target: () => z.input<NoInfer<S>>, dependencies: DependencyList,
): BoundAction<ActionDefinition<I, O, S, Observed, P>, true>;
export function useAction<I extends z.ZodType, O extends z.ZodType, S extends z.ZodType<Json>, Observed extends Json = never,
  P extends OperationContract = never>(
  factory: () => LocalActionOptions<I, O, S, Observed, P> & { readonly target: z.input<NoInfer<S>> }, dependencies: DependencyList,
): BoundAction<ActionDefinition<I, O, S, Observed, P>, true>;
export function useAction<I extends z.ZodType, O extends z.ZodType, S extends z.ZodType<Json>, Observed extends Json = never,
  P extends OperationContract = never>(
  source: ActionImplementation<ActionDefinition<I, O, S, Observed, P>>
    | (() => LocalActionOptions<I, O, S, Observed, P> & { readonly target: z.input<NoInfer<S>> }),
  targetOrDependencies: (() => z.input<S>) | DependencyList, dependencies?: DependencyList,
): BoundAction<ActionDefinition<I, O, S, Observed, P>, true> {
  const owner = useOwner();
  const local = typeof source === 'function';
  const currentDependencies = local ? targetOrDependencies as DependencyList : dependencies!;
  const binding = useMemo(() => {
    const localOptions = typeof source === 'function' ? source() : undefined;
    const implementation = typeof source === 'function' ? implementAction(localOptions!) : source;
    const target = localOptions === undefined ? (targetOrDependencies as () => z.input<S>)() : localOptions.target;
    const state = { active: false, owned: false };
    const action: BoundAction<ActionDefinition<I, O, S, Observed, P>, true> = bindAction(implementation, target, {
      active: () => state.active,
      execution: reference => owner.execution(reference),
      admit: (input, call, execution) => owner.admit(action, input, call, execution),
      operation: (event, execution) => owner.operation?.(action, event, execution),
    });
    return { action, state };
  }, [owner, local ? true : source, ...currentDependencies]);
  useLayoutEffect(() => {
    if (!binding.state.owned) {
      owner.scope.own(() => binding.action.release());
      binding.state.owned = true;
    }
    binding.state.active = true;
    // StrictMode 的 setup-cleanup-setup 只改变当前准入，不永久撤销同一个绑定。
    return () => { binding.state.active = false; };
  }, [owner, binding]);
  return binding.action;
}

/**
 * 纯构造原命令绑定；deps 改变才更换目标，旧绑定及已取得的 participation 不会重定向。
 * 读取归会话 owner，不因组件查询退出取消业务或已登记事实；原生新参与由 Follow 单独准入。
 * factory 明确返回 null 时当前没有绑定，不查询、不登记等待，也不取消先前受理的业务。
 */
export function useOperation<D extends OperationContract>(operationOwner: OperationOwner<D>,
  command: () => Exclude<OperationCommand<NoInfer<D>>, null>, dependencies: DependencyList): BoundOperation<D>;
export function useOperation<D extends OperationContract>(operationOwner: OperationOwner<D>,
  command: () => OperationCommand<NoInfer<D>> | null, dependencies: DependencyList): BoundOperation<D> | null;
export function useOperation<D extends OperationContract>(operationOwner: OperationOwner<D>,
  command: () => OperationCommand<NoInfer<D>> | null, dependencies: DependencyList): BoundOperation<D> | null {
  const owner = useOwner();
  return useMemo(() => {
    const current = command();
    return current === null ? null : bindOperation(operationOwner, current, {
      access: binding => owner.operationAccess(binding),
      readFeedback: (binding, id, access) => owner.readOperationFeedback(binding, id, access),
    });
  }, [owner, operationOwner, ...dependencies]);
}

function useSource<T>(factory: () => Observable<T>, dependencies: DependencyList): Observable<T> {
  const source$ = useMemo(factory, dependencies);
  if (!isObservable(source$)) throw new Error('REACTIVE_HOOK_OBSERVABLE_REQUIRED');
  return source$;
}

/** hook只借管查询连接与终止缓存；RxJS持有唯一重放值，Program退出后不能重新连接。 */
function shareQuery<T>(owner: ProgramHookOwner, source$: Observable<T>): Observable<T> {
  const signal = AbortSignal.any([owner.signal, owner.reactivityScope.signal]);
  const ended$ = defer(() => signal.aborted ? of(undefined) : fromEvent(signal, 'abort'));
  return source$.pipe(share({ connector: () => new ReplaySubject<T>(1),
    resetOnError: () => ended$, resetOnComplete: () => ended$, resetOnRefCountZero: true,
  }), takeUntil(ended$));
}

/**
 * factory 纯构造 Observable，不订阅、不启动业务。deps 决定 source identity。
 * 每个已提交观察持有自己的查询连接；退出只停止查询，不取消已受理 Action。
 * pending、空完成及错误均保留为现有 Observation，不冒充空业务结果。
 */
export function useObservable<T>(factory: () => Observable<T>, dependencies: DependencyList): Observation<T> {
  const owner = useOwner();
  const source$ = useSource(factory, dependencies);
  const store = useMemo(() => observableStore(owner.scope, owner.signal, source$), [owner, source$]);
  return useSyncExternalStore(store.subscribe, store.getSnapshot);
}

/**
 * 同一查询同时供 Context 与 Expose 使用时显式共享此标准 Observable。
 * 最后一个查询观察退出后断开来源；完成与错误仅在Program活跃时按source identity保留。
 * Program退出完成既有观察并清空终止缓存；迟到订阅不交付旧值、不重新连接。
 * 长期业务执行应由 Action 或明确的业务/工作区 owner 持有。
 */
export function useShared<T>(factory: () => Observable<T>, dependencies: DependencyList): Observable<T> {
  const owner = useOwner();
  const source$ = useSource(factory, dependencies);
  return useMemo(() => shareQuery(owner, source$), [owner, source$]);
}

export interface ObservedResource<T> {
  readonly state: Observation<T>;
  readonly observations$: Observable<Observation<T>>;
}

/**
 * 取得程序拥有的正式资源实例，构造不读取；read、失效、流及其他 hook 共用同一对象。
 * 相同实现与解析参数跨组件共享；替换目标不改写已取得的实例。
 * 局部 factory 纯构造 createResource 的选项，依赖数组决定本组件的实例身份。
 */
export function useResourceInstance<D extends ResourceContract>(implementation: ResourceImplementation<D>,
  params: ResourceParameters<NoInfer<D>>): ResourceInstance<D>;
export function useResourceInstance<V extends z.ZodType>(factory: () => LocalResourceOptions<V>,
  dependencies: DependencyList): LocalResource<V>;
export function useResourceInstance(source: ResourceImplementation | (() => LocalResourceOptions<z.ZodType>),
  paramsOrDependencies: unknown): ResourceInstance<ResourceContract> {
  const owner = useOwner();
  const local = useMemo(() => typeof source === 'function'
    ? createResource(owner.reactivityScope, source())
    : undefined,
  [owner, typeof source === 'function', ...(typeof source === 'function' ? paramsOrDependencies as DependencyList : [])]);
  return typeof source === 'function' ? local!
    : bindResource(source, { scope: owner.reactivityScope }).instance(paramsOrDependencies);
}

/**
 * 取得程序拥有的持续反应，不在 render 或 commit 隐式 start。
 * 运行属于程序，组件退出不冒充停止；调用 run.stop 或程序退出才释放感知和排队关系。
 * 局部 factory 使用 createReaction 的同一选项，不要求声明参数合同或另传 Scope。
 */
export function useReaction<D extends ReactionContract>(implementation: ReactionImplementation<D>,
  params: ReactionParameters<NoInfer<D>>): ReactionInstance<D>;
export function useReaction<E extends z.ZodType, R extends z.ZodType>(factory: () => LocalReactionOptions<E, R>,
  dependencies: DependencyList): LocalReaction<E, R>;
export function useReaction(source: ReactionImplementation | (() => LocalReactionOptions<z.ZodType, z.ZodType>),
  paramsOrDependencies: unknown): ReactionInstance<ReactionContract> {
  const owner = useOwner();
  const local = useMemo(() => typeof source === 'function'
    ? createReaction(owner.reactivityScope, source())
    : undefined,
  [owner, typeof source === 'function', ...(typeof source === 'function' ? paramsOrDependencies as DependencyList : [])]);
  return typeof source === 'function' ? local!
    : bindReaction(source, { scope: owner.reactivityScope }).instance(paramsOrDependencies);
}

/**
 * factory 纯构造已有资源观察流；deps 决定来源身份，订阅随已提交组件的查询连接存在。
 * 原资源状态直接供 Context 和资源 View 共享，不嵌套 Observation，也不丢弃失效事件。
 * 来源终止错误与未就绪完成进入同一条 error 观察；不隐式重试。
 */
export function useResource<T>(resource: ResourceReadable<T>): ObservedResource<T>;
export function useResource<T>(factory: () => Observable<Observation<T>>, dependencies: DependencyList): ObservedResource<T>;
export function useResource<D extends ResourceContract>(implementation: ResourceImplementation<D>, params: ResourceParameters<NoInfer<D>>): ObservedResource<ResourceData<D>>;
export function useResource(source: ResourceReadable<unknown> | ResourceImplementation | (() => Observable<Observation<unknown>>),
  paramsOrDependencies: unknown = []): ObservedResource<unknown> {
  const owner = useOwner();
  const current = isResourceImplementation(source)
    ? bindResource(source, { scope: owner.reactivityScope }).instance(paramsOrDependencies) : source;
  const original$ = useSource(typeof current === 'function' ? current : () => current.observations$,
    typeof current === 'function' ? paramsOrDependencies as DependencyList : [current]);
  const observations$ = useMemo(() => typeof current === 'function'
    ? shareQuery(owner, normalizeResource(original$)) : original$, [owner, original$, typeof current]);
  const observed = useObservable(() => observations$, [observations$]);
  const state = observed.status === 'ready' ? observed.value : observed;
  return useMemo(() => Object.freeze({ state, observations$ }), [state, observations$]);
}

/** 提交后建立长期 RxJS 关系；未在流内恢复的错误使程序 owner 失败。 */
export function useRelation<T>(factory: () => Observable<T>, dependencies: DependencyList): void {
  const owner = useOwner();
  const source$ = useSource(factory, dependencies);
  useEffect(() => {
    const scope = owner.scope.child();
    const subscriber = new Subscriber<T>({ next() {}, complete() {},
      error: (error: unknown) => { if (!scope.disposed) owner.fail(error); } });
    scope.own(() => subscriber.unsubscribe());
    const stop = () => scope.dispose();
    scope.own(() => owner.signal.removeEventListener('abort', stop));
    owner.signal.addEventListener('abort', stop, { once: true });
    // fatal 可以发生于 React commit；关系退订不能等待 renderer 的延后卸载。
    if (owner.signal.aborted) stop();
    else source$.subscribe(subscriber);
    return () => scope.dispose();
  }, [owner, source$]);
}

/** 声明幂等的步骤前检查；依赖替换或组件退出撤销本登记的信号和等待，真实失败仍交宿主处理。 */
export function useBeforeStep(check: (signal: AbortSignal) => void | Promise<void>, dependencies: DependencyList): void {
  const owner = useOwner();
  const current = useMemo(() => check, dependencies);
  useLayoutEffect(() => owner.beforeStep(current), [owner, current]);
}

/** useSyncExternalStore 的同步快照接点；不定义流组合或另一个任务 owner。 */
function observableStore<T>(owner: Scope, signal: AbortSignal, source$: Observable<T>): SnapshotSource<Observation<T>> {
  let snapshot: Observation<T> = { status: 'pending', generation: 0 };
  let generation = 0;
  let scope: Scope | undefined;
  const listeners = new Set<() => void>();
  const publish = (next: Observation<T>) => {
    snapshot = next;
    for (const notify of [...listeners]) notify();
  };
  const start = () => {
    const connection = owner.child();
    scope = connection;
    const stop = () => connection.dispose();
    connection.own(() => signal.removeEventListener('abort', stop));
    signal.addEventListener('abort', stop, { once: true });
    if (signal.aborted) { stop(); return; }
    if (snapshot.status !== 'pending') publish({ status: 'pending', generation: ++generation });
    if (signal.aborted) { stop(); return; }
    let received = false;
    const subscriber = new Subscriber<T>({
      next(value) {
        if (connection.disposed) return;
        received = true;
        publish({ status: 'ready', generation: ++generation, value, observedAt: Date.now() });
      },
      error(error: unknown) {
        if (!connection.disposed) publish({ status: 'error', generation: ++generation, error });
      },
      complete() {
        if (!received && !connection.disposed) publish({ status: 'error', generation: ++generation, error: new EmptyError() });
      },
    });
    connection.own(() => subscriber.unsubscribe());
    if (signal.aborted) stop();
    else connect(connection, source$).subscribe(subscriber);
  };
  return {
    getSnapshot: () => snapshot,
    subscribe(notify) {
      listeners.add(notify);
      if (listeners.size === 1) start();
      return () => {
        listeners.delete(notify);
        if (listeners.size === 0) { scope?.dispose(); scope = undefined; }
      };
    },
  };
}
