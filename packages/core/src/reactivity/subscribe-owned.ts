import { Observable, Subscriber, type Observer, type TeardownLogic } from 'rxjs';
import type { Scope } from './scope.ts';

/** 来源装配与真实退订共用既有 owner 回执；不把来源 error 当作清理错误。 */
export function subscribeOwned<T>(scope: Scope, source$: Observable<T>, observer: Partial<Observer<T>>): () => void {
  let settingUp = true;
  const finished = Promise.withResolvers<void>();
  const failures: unknown[] = [];
  const subscription = new class extends Subscriber<T> {
    override add(teardown: TeardownLogic): void {
      try { super.add(teardown); }
      catch (error) {
        // 已关闭订阅的 add 正在执行实际迟到 teardown，此时不能先抛掉装配回执。
        if (settingUp && this.closed) failures.push(error);
        else throw error;
      }
    }
  }({
    next: (value: T) => observer.next?.(value),
    error: (error: unknown) => { releaseObserver(); observer.error?.(error); },
    complete: () => { releaseObserver(); observer.complete?.(); },
  });
  const release = scope.own(() => {
    if (!settingUp) { subscription.unsubscribe(); return; }
    try { subscription.unsubscribe(); }
    catch (error) { failures.push(error); }
    return finished.promise;
  });
  const releaseObserver = () => {
    try { release(); }
    catch {
      // own 已将实际清理失败保存到 Scope.closed；RxJS 退出和自然终止共用该回执，
      // 不重复向消费者抛出或用清理错误覆盖原来源 error。
    }
  };
  // 下游同步 next 内退出也必须经同一个 owner release，不能绕过其清理回执。
  if (observer instanceof Subscriber) observer.add(releaseObserver);
  try {
    if (!subscription.closed) source$.subscribe(subscription);
  } finally {
    try { if (subscription.closed) releaseObserver(); }
    finally {
      settingUp = false;
      if (failures.length === 1) finished.reject(failures[0]);
      else if (failures.length) finished.reject(new AggregateError(failures, 'REACTIVE_SOURCE_CLEANUP_FAILED'));
      else finished.resolve();
    }
  }
  return releaseObserver;
}

/** 无缓存的原来源 owner 边界；共享和资源归一继续由调用方组合。 */
export function ownedObservable<T>(scope: Scope, source$: Observable<T>): Observable<T> {
  return new Observable<T>(subscriber => subscribeOwned(scope, source$, subscriber));
}
