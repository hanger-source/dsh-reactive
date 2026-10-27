import type { Observable } from 'rxjs';
import type { z } from 'zod';
import type { Observation } from './observation.ts';
import { assertResourceDefinition, type ResourceContract, type ResourceTarget } from './definition.ts';

export interface ResourceReadContext { readonly signal: AbortSignal; }

export type ResourceImplementationOptions<D extends ResourceContract> =
  | {
    /** 连接业务 owner 的持续事实；pending/error 是同一资源的有效性。 */
    observe(params: ResourceTarget<D>, context: ResourceReadContext): Observable<Observation<z.input<D['value']>>>;
    readonly read?: never;
    readonly invalidations?: never;
  }
  | {
    /** 一次权威读取；失效通知会取消旧读取并重新读取，不携带替代事实。 */
    read(params: ResourceTarget<D>, context: ResourceReadContext): z.input<D['value']> | PromiseLike<z.input<D['value']>>;
    invalidations?(params: ResourceTarget<D>, context: ResourceReadContext): Observable<unknown>;
    readonly observe?: never;
  };

export interface ResourceImplementation<D extends ResourceContract = ResourceContract> { readonly definition: D; }
const implementations = new WeakMap<object, unknown>();

/** @internal hook 按实际实现身份区分参数绑定与已有资源，不猜结构或名字。 */
export function isResourceImplementation(value: unknown): value is ResourceImplementation {
  return typeof value === 'object' && value !== null && implementations.has(value);
}

/** 依赖在此闭包中固定；替换依赖须创建新实现，而非改写已有实例的来源。 */
export function implementResource<D extends ResourceContract>(definition: D,
  options: ResourceImplementationOptions<D>): ResourceImplementation<D> {
  assertResourceDefinition(definition);
  if (typeof options.observe === 'function' ? options.read !== undefined || options.invalidations !== undefined
    : typeof options.read !== 'function') throw new Error('REACTIVE_RESOURCE_IMPLEMENTATION_INVALID');
  const implementation = Object.freeze({ definition });
  implementations.set(implementation, Object.freeze({ ...options }));
  return implementation;
}

/** @internal 精确实现身份对应构造时登记的同一合同；类型擦除只跨此私有存储。 */
export function resourceImplementation<D extends ResourceContract>(implementation: ResourceImplementation<D>): ResourceImplementationOptions<D> {
  const options = implementations.get(implementation);
  if (!options) throw new Error('REACTIVE_RESOURCE_IMPLEMENTATION_REQUIRED');
  return options as ResourceImplementationOptions<D>;
}
