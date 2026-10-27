import { Observable, Subject, ReplaySubject, Subscription, connectable, takeUntil, defer, from, finalize, tap, distinctUntilChanged, EmptyError, filter, isObservable, map, materialize, scan, startWith } from 'rxjs';
import type { Observation } from '../resources/observation.ts';
import type { Scope } from './scope.ts';

const connections = new WeakMap<Scope, WeakMap<Observable<unknown>, Observable<unknown>>>();

/** 作用域立即持有来源连接；消费者只观察，不决定来源启停或错误后的重试。 */
export function connect<T>(scope: Scope, source$: Observable<T>): Observable<T> {
  scope.assertActive();
  let owned = connections.get(scope);
  if (!owned) { owned = new WeakMap(); connections.set(scope, owned); }
  const previous = owned.get(source$);
  if (previous) return previous as Observable<T>;
  const ended$ = new Subject<void>();
  const connection = new Subscription();
  const shared$ = connectable(source$.pipe(takeUntil(ended$)), {
    connector: () => new ReplaySubject<T>(1), resetOnDisconnect: false,
  });
  // 先登记共享身份与清理，再建立连接；同步来源也可以安全地重入或释放 owner。
  owned.set(source$, shared$);
  scope.own(() => {
    try { ended$.next(); ended$.complete(); }
    finally { connection.unsubscribe(); }
  });
  connection.add(shared$.connect());
  return shared$;
}

export interface SnapshotSource<T> {
  getSnapshot(): T;
  subscribe(notify: () => void): () => void;
}

/** 先监听再读取，消除初次读取与登记监听之间的变化窗口。 */
export function fromSnapshot<T>(source: SnapshotSource<T>): Observable<T> {
  return new Observable<T>(subscriber => {
    const read = () => {
      if (subscriber.closed) return;
      try { subscriber.next(source.getSnapshot()); }
      catch (error) { subscriber.error(error); }
    };
    const release = source.subscribe(read);
    read();
    return release;
  }).pipe(distinctUntilChanged());
}

/** Promise API 与标准退订的取消互操作；组合、并发和重试均使用 RxJS 本身。 */
export function abortable<T>(run: (signal: AbortSignal) => PromiseLike<T> | T): Observable<T> {
  return defer(() => {
    const controller = new AbortController();
    let settled = false;
    return defer(() => from(Promise.resolve(run(controller.signal)))).pipe(
      tap({ next: () => { settled = true; }, error: () => { settled = true; } }),
      finalize(() => { if (!settled) controller.abort(); }),
    );
  });
}

/** @internal 显式资源边界的终止归一；保留同一个 pending/ready/error 来源，不猜普通业务值。 */
export function normalizeResource<T>(source$: Observable<Observation<T>>): Observable<Observation<T>> {
  if (!isObservable(source$)) throw new Error('REACTIVE_RESOURCE_OBSERVABLE_REQUIRED');
  return defer(() => {
    const initial: Observation<T> = { status: 'pending', generation: 0 };
    return source$.pipe(materialize(), scan((current: { state: Observation<T>; emit: boolean }, notification) => {
      if (notification.kind === 'N') return { state: notification.value, emit: true };
      if (notification.kind === 'E') return {
        state: { status: 'error', generation: current.state.generation + 1, error: notification.error } satisfies Observation<T>, emit: true,
      };
      if (current.state.status === 'pending') return {
        state: { status: 'error', generation: current.state.generation + 1, error: new EmptyError() } satisfies Observation<T>, emit: true,
      };
      return { state: current.state, emit: false };
    }, { state: initial, emit: false }), filter(current => current.emit), map(current => current.state), startWith(initial));
  });
}
