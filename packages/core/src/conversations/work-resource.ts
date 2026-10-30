import { Observable, ReplaySubject, Subscriber, type TeardownLogic } from 'rxjs';
import type { Observation } from '../resources/observation.ts';
import type { ResourceReaderOptions } from '../resources/reader.ts';

/** 本次 Work 观察持有的借用连接；短暂退出组合观察不释放来源。 */
export interface WorkResourceConnection<T> {
  readonly observations$: Observable<Observation<T>>;
  unsubscribe(): void;
}

/** 惰性连接必须先登记到成员表；退出直接退订来源，再结束唯一重放缓存。 */
export function borrowWorkResource<T>(source$: Observable<Observation<T>>,
  onCleanupError?: ResourceReaderOptions['onCleanupError']): WorkResourceConnection<T> {
  const replay = new ReplaySubject<Observation<T>>(1);
  let started = false, subscribing = false, released = false;
  const reportLateCleanup = (error: unknown) => {
    if (onCleanupError) onCleanupError(error);
    else console.error('reactive.resource.cleanup.failed', error);
  };
  const releaseSource = () => {
    try { connection.unsubscribe(); }
    catch (error) {
      if (onCleanupError) onCleanupError(error);
      else throw error;
    }
  };
  const finish = (terminal: { error: unknown } | undefined) => {
    if (released || connection.closed) return;
    try { releaseSource(); }
    catch (error) {
      replay.error(terminal ? new AggregateError([terminal.error, error], 'REACTIVE_WORK_RESOURCE_CLOSE_FAILED') : error);
      return;
    }
    if (terminal) replay.error(terminal.error);
    else replay.complete();
  };
  const connection = new class extends Subscriber<Observation<T>> {
    override add(teardown: TeardownLogic): void {
      try { super.add(teardown); }
      catch (error) {
        // 同步 next 内退出后，add 会立即执行刚返回的真实 teardown。
        // 只在这个确定的清理边界报告；晚到的来源 error 仍遵循 RxJS 语义。
        if (this.closed && subscribing) reportLateCleanup(error);
        else throw error;
      }
    }
  }({ next: observation => replay.next(observation), error: (error: unknown) => finish({ error }), complete: () => finish(undefined) });
  const observations$ = new Observable<Observation<T>>(subscriber => {
    if (released) { subscriber.complete(); return; }
    replay.subscribe(subscriber);
    if (started || released || subscriber.closed) return;
    started = true;
    subscribing = true;
    try { source$.subscribe(connection); }
    finally { subscribing = false; }
  });
  return { observations$, unsubscribe() {
    if (released) return;
    released = true;
    try { releaseSource(); }
    finally { replay.complete(); }
  } };
}
