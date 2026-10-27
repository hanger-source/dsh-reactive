import { Observable, EMPTY, of, merge, map, catchError } from 'rxjs';
import { abortable, switchLatest } from '../reactivity/observable.ts';
import type { Observation } from './observation.ts';

export interface ResourceObservationOptions<K, T> {
  key$: Observable<K>;
  read(key: K, signal: AbortSignal): PromiseLike<T> | T;
  /** 通知只表示当前 key 的事实需要重新读取，不携带替代事实。 */
  invalidations?(key: K): Observable<unknown>;
}

/** 标准冷流；是否共享以及共享多久由调用者的 owner 连接决定。 */
export function observeResource<K, T>(options: ResourceObservationOptions<K, T>): Observable<Observation<T>> {
  return new Observable<Observation<T>>(subscriber => {
    let generation = 0;
    const pending = () => subscriber.next({ status: 'pending', generation: ++generation });
    options.key$.pipe(switchLatest(key => {
      // 先订阅 owner 通知再触发初读，读取期间同步到达的失效不会遗失。
      const invalidations$ = options.invalidations?.(key) ?? EMPTY;
      return merge(invalidations$, of(undefined)).pipe(switchLatest(() => {
        const current = generation;
        return abortable(signal => options.read(key, signal)).pipe(
          map((value): Observation<T> => ({ status: 'ready', generation: current, value, observedAt: Date.now() })),
          catchError((error: unknown) => of<Observation<T>>({ status: 'error', generation: current, error })),
        );
      }, pending));
    }, pending)).subscribe(subscriber);
  });
}
