import { Observable, EMPTY, of, throwError, mergeMap } from 'rxjs';
import type { ReadonlySignal } from '@preact/signals-core';
import { Value, ScopeDisposedError, type Scope } from '../reactivity/scope.ts';
import type { Observation } from './observation.ts';

type Node<T> = Pick<ReadonlySignal<T>, 'value' | 'peek' | 'subscribe'>;

export class ResourceValue<T> {
  #observation: Value<Observation<T>>;
  readonly observations$: Observable<Observation<T>>;
  readonly values$: Observable<T>;
  constructor(readonly scope: Scope, node: Node<Observation<T>>) {
    this.#observation = new Value(scope, node);
    this.observations$ = this.#observation.changes$;
    this.values$ = this.observations$.pipe(mergeMap(observation => observation.status === 'pending' ? EMPTY
      : observation.status === 'error' ? throwError(() => observation.error) : of(observation.value)));
  }
  get value() { return this.#observation.value; }
  peek() { return this.#observation.peek(); }
  subscribe(accept: (observation: Observation<T>) => void) { return this.#observation.subscribe(accept); }
  /** 等待当前代次；等待中失效会跟随新代次，而不是交付迟到的旧结果。 */
  read(signal?: AbortSignal): Promise<T> {
    this.scope.assertActive();
    if (signal?.aborted) return Promise.reject(signal.reason);
    return new Promise<T>((resolve, reject) => {
      let stop: (() => void) | undefined;
      let release: (() => void) | undefined;
      let settled = false;
      const finish = (complete: () => void) => {
        if (settled) return;
        settled = true;
        stop?.(); release?.(); signal?.removeEventListener('abort', abort);
        complete();
      };
      const abort = () => finish(() => reject(signal!.reason));
      release = this.scope.own(() => finish(() => reject(new ScopeDisposedError())));
      signal?.addEventListener('abort', abort, { once: true });
      stop = this.subscribe(observation => {
        if (observation.status === 'ready') finish(() => resolve(observation.value));
        if (observation.status === 'error') finish(() => reject(observation.error));
      });
      if (settled) { stop(); release(); }
    });
  }
}
