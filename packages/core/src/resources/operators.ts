import { catchError, combineLatest, defer, filter, map, of, EMPTY, Observable, type OperatorFunction } from 'rxjs';
import type { Observation } from './observation.ts';
import { resourceObservations, type ResourceSource } from './source.ts';
import { normalizeResource, switchLatest } from '../reactivity/observable.ts';
import { synchronous } from '../reactivity/synchronous.ts';

export type ResourceSources = Readonly<Record<string, ResourceSource<unknown>>>;
export type ResourceValues<S extends ResourceSources> = {
  readonly [K in keyof S]: S[K] extends ResourceSource<infer T> ? T : never;
};

export interface ResourceSwitchOptions<T> {
  /** 只有连续 ready 的这组依赖相同时才保留当前查询；元素按 Object.is 比较。 */
  readonly dependencies?: (value: T) => readonly unknown[];
}

/** 同步派生当前 ready 值；失效和错误仍是资源状态，投影失败不终止后续代次。 */
export function mapResource<T, U>(project: (value: T) => U): OperatorFunction<Observation<T>, Observation<U>> {
  return source => normalizeResource(source).pipe(map((observation): Observation<U> => {
    if (observation.status !== 'ready') return Object.freeze({ ...observation });
    try { return Object.freeze({ ...observation, value: synchronous(project(observation.value), 'RESOURCE_MAP') }); }
    catch (error) { return Object.freeze({ status: 'error', generation: observation.generation, error }); }
  }));
}

/**
 * ready 依赖选择实际下游资源；上游失效或改变会退订旧查询，不能继续交付旧 ready。
 * 显式依赖相同的连续 ready 保留同一查询；失效清除身份，同依赖恢复也必须重新连接。
 * project 选择资源对象或构造查询流，不受理业务；共享与重试仍由现有 owner 和 RxJS 组合决定。
 */
export function switchResource<T, U>(project: (value: T) => ResourceSource<U>,
  options: ResourceSwitchOptions<T> = {}): OperatorFunction<Observation<T>, Observation<U>> {
  const selectDependencies = options.dependencies;
  return source => defer(() => {
    let generation = 0;
    let previous: readonly unknown[] | undefined;
    const upstream$ = normalizeResource(source).pipe(
      map((upstream): Observation<T> | null => {
        if (upstream.status !== 'ready') { previous = undefined; return upstream; }
        if (selectDependencies === undefined) return upstream;
        try {
          const selected = selectDependencies(upstream.value);
          if (!Array.isArray(selected)) throw new Error('REACTIVE_RESOURCE_DEPENDENCIES_ARRAY_REQUIRED');
          // 捕获 tuple 本身；业务值的相等关系只按显式选中的标量或稳定引用判断。
          const current = Object.freeze([...selected]);
          if (previous && previous.length === current.length && previous.every((value, index) => Object.is(value, current[index]))) return null;
          previous = current;
          return upstream;
        } catch (error) {
          previous = undefined;
          return Object.freeze({ status: 'error', generation: upstream.generation, error });
        }
      }),
      filter((upstream): upstream is Observation<T> => upstream !== null),
    );
    return new Observable<Observation<U>>(subscriber => upstream$.pipe(
      switchLatest((upstream): Observable<Observation<U>> => {
        if (upstream.status !== 'ready') return EMPTY;
        return defer(() => normalizeResource(resourceObservations(project(upstream.value)))).pipe(
          map(observation => Object.freeze({ ...observation, generation: ++generation })),
          catchError((error: unknown) => of(Object.freeze({ status: 'error' as const, generation: ++generation, error }))),
        );
      }, upstream => subscriber.next(Object.freeze(upstream.status === 'ready'
        ? { status: 'pending', generation: ++generation }
        : { ...upstream, generation: ++generation }))),
    ).subscribe(subscriber));
  });
}

/**
 * 组合具名资源对象或观察流的当前观察，不过滤失效或借用上一代 ready。
 * error 优先于 pending；全部就绪后才交付冻结的值对象。
 * generation 属于这次组合订阅，不把独立来源的代次当成共同事务版本。
 * 来源连接、共享和业务执行的寿命仍由现有 RxJS owner 决定。
 */
export function combineResources<const S extends ResourceSources>(sources: S): Observable<Observation<ResourceValues<S>>> {
  const entries = Object.entries<ResourceSource<unknown>>(sources);
  if (!entries.length) throw new Error('REACTIVE_RESOURCE_SOURCES_REQUIRED');
  const normalized = entries.map(([, source]) => normalizeResource(resourceObservations(source)));
  return defer(() => {
    let generation = 0;
    return combineLatest(normalized).pipe(map((observations): Observation<ResourceValues<S>> => {
      const current = ++generation;
      const failed = observations.find(observation => observation.status === 'error');
      if (failed?.status === 'error') return Object.freeze({ status: 'error', generation: current, error: failed.error });
      if (observations.some(observation => observation.status === 'pending'))
        return Object.freeze({ status: 'pending', generation: current });
      const values: Array<readonly [string, unknown]> = [];
      observations.forEach((observation, index) => {
        if (observation.status === 'ready') values.push([entries[index]![0], observation.value]);
      });
      // keys 与来源在构造时一起捕获；fromEntries 将 __proto__ 等名字保留为自有数据属性。
      const value = Object.freeze(Object.fromEntries(values)) as ResourceValues<S>;
      return Object.freeze({ status: 'ready', generation: current, value, observedAt: Date.now() });
    }));
  });
}
