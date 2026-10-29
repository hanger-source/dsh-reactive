import { Observable, Subscriber, EMPTY, EmptyError, of, throwError, mergeMap } from 'rxjs';
import { assertObservation, type Observation } from './observation.ts';

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
  /** 来源连接的真实释放失败；由实际owner汇总，不接收消费者回调错误。 */
  readonly onCleanupError?: (error: unknown) => void;
}

/**
 * 借用已有事实；options限定借用寿命，省略时沿用来源寿命，不缓存或重试。
 * owner撤销向仍在观察的消费者交付原reason的error观察后完成；迟到观察只交付该撤销。
 * 来源正常完成不结束显式owner的有效性观察；无owner时保留来源自己的有限语义。
 * values$在自身owner退出时正常完成，普通查询错误仍沿值流报错。
 */
export class ResourceReader<T> implements ResourceReadable<T> {
  readonly observations$: Observable<Observation<T>>;
  readonly values$: Observable<T>;
  private readonly signal: AbortSignal | undefined;
  private readonly onObserverError: ResourceReaderOptions['onObserverError'];
  constructor(private readonly source: Pick<ResourceReadable<T>, 'observations$' | 'peek'>,
    options?: ResourceReaderOptions) {
    const signal = options?.signal;
    const onCleanupError = options?.onCleanupError;
    this.signal = signal;
    this.onObserverError = options?.onObserverError;
    this.observations$ = new Observable(subscriber => {
      let generation = 0, ended = false;
      let status: Observation<T>['status'] | undefined;
      let connection: Subscriber<Observation<T>> | undefined;
      const releaseSource = (aborting: boolean) => {
        try { connection?.unsubscribe(); }
        catch (error) {
          if (onCleanupError) onCleanupError(error);
          else if (aborting) console.error('reactive.resource.cleanup.failed', error);
          else throw error;
        }
      };
      const close = () => {
        if (ended || subscriber.closed) return;
        ended = true;
        try { releaseSource(true); }
        finally {
          subscriber.next({ status: 'error', generation: ++generation, error: signal!.reason });
          subscriber.complete();
        }
      };
      if (signal?.aborted) { close(); return; }
      signal?.addEventListener('abort', close, { once: true });
      subscriber.add(() => signal?.removeEventListener('abort', close));
      connection = new Subscriber<Observation<T>>({
        next: observation => {
          if (signal?.aborted) { close(); return; }
          try {
            assertObservation<T>(observation);
            const nextGeneration = observation.generation, nextStatus = observation.status;
            generation = nextGeneration;
            status = nextStatus;
          } catch (error) {
            if (signal?.aborted) { close(); return; }
            status = 'error';
            subscriber.next({ status, generation: ++generation, error });
            return;
          }
          if (signal?.aborted) { close(); return; }
          subscriber.next(observation);
        },
        error: (error: unknown) => signal?.aborted ? close() : subscriber.error(error),
        complete: () => {
          if (signal?.aborted) { close(); return; }
          if (!signal) { subscriber.complete(); return; }
          // 业务来源已静止，资源的读取资格仍由owner拥有；空完成必须结算当前等待。
          if (status === undefined || status === 'pending') {
            subscriber.next({ status: 'error', generation: ++generation, error: new EmptyError() });
          }
        },
      });
      subscriber.add(() => releaseSource(false));
      source.observations$.subscribe(connection);
    });
    this.values$ = this.observations$.pipe(mergeMap(observation => signal?.aborted || observation.status === 'pending' ? EMPTY
      : observation.status === 'error' ? throwError(() => observation.error) : of(observation.value)));
  }
  peek(): Observation<T> {
    this.signal?.throwIfAborted();
    try {
      const observation = this.source.peek();
      assertObservation<T>(observation);
      this.signal?.throwIfAborted();
      return observation;
    } catch (error) {
      this.signal?.throwIfAborted();
      throw error;
    }
  }
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
        settled = true;
        // read自己结束借用，没有显式unsubscribe调用者可接收来源清理失败；读取事实仍必须结算。
        try { subscriber.unsubscribe(); }
        catch (error) { console.error('reactive.resource.cleanup.failed', error); }
        finally { complete(); }
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
