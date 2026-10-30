import { Observable, ReplaySubject, takeUntil, defer, from, tap, distinctUntilChanged, EmptyError, filter, isObservable, map, materialize, scan, startWith, share, observeOn, queueScheduler, switchMap, EMPTY, type OperatorFunction } from 'rxjs';
import { assertObservation, type Observation } from '../resources/observation.ts';
import type { Scope } from './scope.ts';
import { subscribeOwned } from './subscribe-owned.ts';

const connections = new WeakMap<Scope, WeakMap<Observable<unknown>, Observable<unknown>>>();

/** 作用域立即持有来源连接；owner退出后不向迟到消费者重放旧值或错误。 */
export function connect<T>(scope: Scope, source$: Observable<T>): Observable<T> {
  scope.assertActive();
  let owned = connections.get(scope);
  if (!owned) { owned = new WeakMap(); connections.set(scope, owned); }
  const previous = owned.get(source$);
  if (previous) return previous as Observable<T>;
  let replay: ReplaySubject<T> | undefined = new ReplaySubject<T>(1);
  const observed$ = new Observable<T>(subscriber => {
    if (scope.disposed) { subscriber.complete(); return; }
    return replay!.subscribe(subscriber);
  });
  // 先登记共享身份与清理，再建立连接；同步来源也可以安全地重入或释放 owner。
  owned.set(source$, observed$);
  scope.own(() => {
    // 先撤掉可重放对象；complete/teardown中的重入也不能重新取得已退出owner的事实。
    const subject = replay!;
    replay = undefined;
    subject.complete();
  });
  const subject = replay;
  subscribeOwned(scope, source$, {
    next: value => subject.next(value), error: error => subject.error(error), complete: () => subject.complete(),
  });
  return observed$;
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

/** Promise API、外部signal与标准退订的取消互操作；取消等待不宣称提供者已经静止。 */
export function abortable<T>(run: (signal: AbortSignal) => PromiseLike<T> | T, signal?: AbortSignal): Observable<T> {
  return new Observable(subscriber => {
    const controller = new AbortController();
    let settled = false;
    // run 可以同步失效当前读取；进入业务代码前就把取消交给真实订阅。
    subscriber.add(() => { if (!settled) controller.abort(); });
    const cancel = () => {
      controller.abort(signal!.reason);
      subscriber.error(signal!.reason);
    };
    signal?.addEventListener('abort', cancel, { once: true });
    subscriber.add(() => signal?.removeEventListener('abort', cancel));
    if (signal?.aborted) { cancel(); return; }
    if (subscriber.closed) return;
    defer(() => from(Promise.resolve(run(controller.signal)))).pipe(
      tap({ next: () => { settled = true; }, error: () => { settled = true; } }),
    ).subscribe(subscriber);
  });
}

/** @internal 立即撤销旧读，RxJS同步队列串行切换；清理中再次通知不能留下失管连接。 */
export function switchLatest<T, U>(project: (value: T) => Observable<U>, beforeSwitch: (value: T) => void): OperatorFunction<T, U> {
  return source => defer(() => {
    let latest = 0;
    const changes = source.pipe(
      map(value => ({ value, version: ++latest })),
      tap(change => beforeSwitch(change.value)),
      // 取消监听不能重启已经结束的有限来源；同次观察共享同一个终止事实。
      share({ resetOnComplete: false, resetOnError: false }),
    );
    return changes.pipe(
      observeOn(queueScheduler),
      filter(change => change.version === latest),
      switchMap(change => defer(() => change.version === latest ? project(change.value) : EMPTY)
        // 通知先登记；工厂尚未返回时到达的新事实也立即取消旧读。
        .pipe(takeUntil(changes.pipe(filter(next => next.version > change.version))))),
    );
  });
}

/** @internal 显式资源边界的终止归一；保留同一个 pending/ready/error 来源，不猜普通业务值。 */
export function normalizeResource<T>(source$: Observable<Observation<T>>): Observable<Observation<T>> {
  if (!isObservable(source$)) throw new Error('REACTIVE_RESOURCE_OBSERVABLE_REQUIRED');
  return defer(() => {
    const initial: Observation<T> = { status: 'pending', generation: 0 };
    return source$.pipe(materialize(), scan((current: { state: Observation<T>; emit: boolean }, notification) => {
      if (notification.kind === 'N') {
        try {
          assertObservation<T>(notification.value);
          return { state: notification.value, emit: true };
        } catch (error) {
          return { state: { status: 'error', generation: current.state.generation + 1, error } satisfies Observation<T>, emit: true };
        }
      }
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
