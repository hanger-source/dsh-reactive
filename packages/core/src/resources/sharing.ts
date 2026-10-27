import { BehaviorSubject, NEVER, concatWith, of, share, skip, type Observable } from 'rxjs';
import { normalizeResource } from '../reactivity/observable.ts';
import type { Observation } from './observation.ts';
import { ResourceReader, type ResourceReaderOptions } from './reader.ts';

/**
 * 已有Observation流的共享读侧；RxJS持有唯一快照和来源连接，不重新解析领域值。
 * 构造不订阅；有限来源的最终观察保留至最后消费者退出，之后显式读取才重新连接。
 */
export function shareResource<T>(source: Observable<Observation<T>>, options: ResourceReaderOptions): ResourceReader<T> {
  const pending: Observation<T> = Object.freeze({ status: 'pending', generation: 0 });
  let current: BehaviorSubject<Observation<T>> | undefined;
  const reset = () => { current = undefined; return of(undefined); };
  // connector交付唯一初始pending；跳过normalizeResource自己的合成基线，保留来源全部真实观察。
  const observations$ = normalizeResource(source).pipe(skip(1), concatWith(NEVER), share({
    connector: () => current = new BehaviorSubject<Observation<T>>(pending),
    resetOnRefCountZero: reset, resetOnComplete: false, resetOnError: reset,
  }));
  return new ResourceReader({ observations$, peek: () => current === undefined ? pending : current.value }, options);
}
