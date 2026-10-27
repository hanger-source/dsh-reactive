import type { HostObservable } from '@deepseek-ai/dsh-client-ui-slots';
import { Scope, normalizeResource } from '@hanger-source/dsh-reactive/reactivity';
import type { Observation } from '@hanger-source/dsh-reactive';
import { Subscriber, type Observable } from 'rxjs';

/** Value、ResourceInstance 和 ResourceValue 的同一个同步读侧。 */
export interface NativeReadable<T> {
  peek(): T;
  subscribe(accept: (value: T) => void): () => void;
}
export type NativeSourceInput<T> = NativeReadable<T> | HostObservable<T>;
export type NativeSourceValue<S> = S extends NativeReadable<infer T> ? T
  : S extends HostObservable<infer T> ? T : never;

/** 不复制资源状态；原生 renderer 直接读取并订阅原对象。 */
export function nativeSource<T>(source: NativeSourceInput<T>): HostObservable<T> {
  if ('getSnapshot' in source) return source;
  return Object.freeze({ getSnapshot: () => source.peek(), subscribe: (notify: () => void) => source.subscribe(() => notify()) });
}

/** 普通事件的“尚未交付 / 当前值 / 观察失败”由既有 Scope.observe 表达。 */
export function observeNativeSource<T>(scope: Scope, source$: Observable<T>): HostObservable<Observation<T>> {
  return nativeSource(scope.observe(source$));
}

/** 保留来源完整 Observation，不把失效或读取失败当作旧 ready。 */
export function observeNativeResource<T>(scope: Scope, source$: Observable<Observation<T>>): HostObservable<Observation<T>> {
  scope.assertActive();
  const state = scope.state<Observation<T>>({ status: 'pending', generation: 0 });
  const subscriber = new Subscriber<Observation<T>>({
    next: value => { if (!scope.disposed) state.set(value); },
    error: (error: unknown) => scope.observerError(error),
    complete: () => {},
  });
  // 来源可以同步交付并释放 owner，先把真实订阅交给 scope。
  scope.own(() => subscriber.unsubscribe());
  normalizeResource(source$).subscribe(subscriber);
  return nativeSource(state);
}
