import { BehaviorSubject, NEVER, Observable, concatWith, share, skip } from 'rxjs';
import { normalizeResource } from '../reactivity/observable.ts';
import type { Observation } from './observation.ts';
import { ResourceReader, type ResourceReaderOptions } from './reader.ts';
import { resourceObservations, type ResourceSource } from './source.ts';

/**
 * 已有资源来源的共享读侧；RxJS持有唯一快照和来源连接，不重新解析领域值。
 * 构造不订阅；有限来源的最终观察保留至最后消费者退出，之后显式读取才重新连接。
 */
export function shareResource<T>(source: ResourceSource<T>, options: ResourceReaderOptions): ResourceReader<T> {
  const pending: Observation<T> = Object.freeze({ status: 'pending', generation: 0 });
  let current: BehaviorSubject<Observation<T>> | undefined;
  // connector交付唯一初始pending；跳过normalizeResource自己的合成基线，保留来源全部真实观察。
  const upstream$ = normalizeResource(resourceObservations(source)).pipe(skip(1), concatWith(NEVER));
  const observations$ = new Observable<Observation<T>>(subscriber => {
    const connected = current;
    // 先撤销本连接的快照，再释放真实来源；旧teardown重订阅不能清掉新connector。
    subscriber.add(() => { if (current === connected) current = undefined; });
    upstream$.subscribe(subscriber);
  }).pipe(share({
    connector: () => current = new BehaviorSubject<Observation<T>>(pending),
    resetOnRefCountZero: true, resetOnComplete: false, resetOnError: true,
  }));
  return new ResourceReader({ observations$, peek: () => current === undefined ? pending : current.value }, options);
}
