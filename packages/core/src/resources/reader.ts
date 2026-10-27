import { Observable, Subscriber, EMPTY, EmptyError, of, throwError, mergeMap } from 'rxjs';
import type { Observation } from './observation.ts';

/** hooks、服务和公开 View 共用此读侧，不要求 React 或宿主会话。 */
export interface ResourceReadable<T> {
  readonly observations$: Observable<Observation<T>>;
  peek(): Observation<T>;
  read(signal?: AbortSignal): Promise<T>;
  subscribe(accept: (observation: Observation<T>) => void): () => void;
}

export interface ResourceReaderOptions {
  readonly signal: AbortSignal;
  readonly onObserverError?: (error: unknown) => void;
}

/** 借用已有事实；options限定借用寿命，省略时沿用来源寿命，不缓存或重试。 */
export class ResourceReader<T> implements ResourceReadable<T> {
  readonly observations$: Observable<Observation<T>>;
  readonly values$: Observable<T>;
  private readonly signal: AbortSignal | undefined;
  private readonly onObserverError: ResourceReaderOptions['onObserverError'];
  constructor(private readonly source: Pick<ResourceReadable<T>, 'observations$' | 'peek'>,
    options?: ResourceReaderOptions) {
    const signal = options?.signal;
    this.signal = signal;
    this.onObserverError = options?.onObserverError;
    this.observations$ = new Observable(subscriber => {
      if (signal?.aborted) { subscriber.complete(); return; }
      const close = () => subscriber.complete();
      signal?.addEventListener('abort', close, { once: true });
      subscriber.add(() => signal?.removeEventListener('abort', close));
      source.observations$.subscribe(subscriber);
    });
    this.values$ = this.observations$.pipe(mergeMap(observation => observation.status === 'pending' ? EMPTY
      : observation.status === 'error' ? throwError(() => observation.error) : of(observation.value)));
  }
  peek(): Observation<T> { this.signal?.throwIfAborted(); return this.source.peek(); }
  subscribe(accept: (observation: Observation<T>) => void): () => void {
    this.signal?.throwIfAborted();
    const report = (error: unknown) => {
      if (this.onObserverError) this.onObserverError(error);
      else console.error('reactive.observer.failed', error);
    };
    const subscription = this.observations$.subscribe({
      next: observation => { try { accept(observation); } catch (error) { report(error); } },
      error: report,
    });
    return () => subscription.unsubscribe();
  }
  /** 等待当前有效值；失效跟随新代次，退出和空完成都必须结算等待。 */
  read(signal?: AbortSignal): Promise<T> {
    this.signal?.throwIfAborted();
    if (signal?.aborted) return Promise.reject(signal.reason);
    return new Promise<T>((resolve, reject) => {
      let settled = false;
      const finish = (complete: () => void) => {
        if (settled) return;
        settled = true; subscriber.unsubscribe(); complete();
      };
      const subscriber = new Subscriber<Observation<T>>({
        next: observation => {
          if (observation.status === 'ready') finish(() => resolve(observation.value));
          else if (observation.status === 'error') finish(() => reject(observation.error));
        },
        error: (error: unknown) => finish(() => reject(error)),
        complete: () => finish(() => reject(this.signal?.aborted ? this.signal.reason : new EmptyError())),
      });
      const abort = () => finish(() => reject(signal!.reason));
      signal?.addEventListener('abort', abort, { once: true });
      subscriber.add(() => signal?.removeEventListener('abort', abort));
      this.observations$.subscribe(subscriber);
    });
  }
}
