import { isObservable, type Observable } from 'rxjs';
import type { Observation } from './observation.ts';
import type { ResourceReadable } from './reader.ts';

/** 资源对象或其原观察流；消费来源不要求取得读取方法或业务 owner。 */
export type ResourceSource<T> = Pick<ResourceReadable<T>, 'observations$'> | Observable<Observation<T>>;

/** @internal 仅投射已有观察流，不订阅、包装、缓存或转移来源所有权。 */
export function resourceObservations<T>(source: ResourceSource<T>): Observable<Observation<T>> {
  if (isObservable(source)) return source;
  const observations$ = source?.observations$;
  if (!isObservable(observations$)) throw new Error('REACTIVE_RESOURCE_SOURCE_REQUIRED');
  return observations$;
}
