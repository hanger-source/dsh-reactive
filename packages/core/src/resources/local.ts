import { z } from 'zod';
import type { Observable } from 'rxjs';
import type { Scope } from '../reactivity/scope.ts';
import type { Observation } from './observation.ts';
import { defineResource, type ResourceDefinition } from './definition.ts';
import { implementResource, type ResourceReadContext } from './implementation.ts';
import { bindResource, type ResourceInstance } from './binding.ts';

export type LocalResourceOptions<V extends z.ZodType> = {
  readonly name: string;
  readonly value: V;
} & (
  | { observe(context: ResourceReadContext): Observable<Observation<z.input<V>>>; readonly read?: never; readonly invalidations?: never }
  | { read(context: ResourceReadContext): z.input<V> | PromiseLike<z.input<V>>;
      invalidations?(context: ResourceReadContext): Observable<unknown>; readonly observe?: never }
);
export type LocalResource<V extends z.ZodType> = ResourceInstance<ResourceDefinition<z.ZodNull, V>>;

/** 局部事实直接归给明确 scope；构造不读取，观察和 read 共用正式 Resource 实例。 */
export function createResource<V extends z.ZodType>(scope: Scope, options: LocalResourceOptions<V>): LocalResource<V> {
  scope.assertActive();
  if (options.observe !== undefined
    ? typeof options.observe !== 'function' || options.read !== undefined || options.invalidations !== undefined
    : typeof options.read !== 'function') throw new Error('REACTIVE_RESOURCE_IMPLEMENTATION_INVALID');
  const definition = defineResource({ name: options.name, params: z.null(), value: options.value });
  if (options.observe !== undefined) {
    const observe = options.observe;
    return bindResource(implementResource(definition, { observe: (_params, context) => observe(context) }), { scope }).instance(null);
  }
  const { read, invalidations } = options;
  return bindResource(implementResource(definition, {
    read: (_params, context) => read(context),
    ...(invalidations === undefined ? {} : { invalidations: (_params: null, context: ResourceReadContext) => invalidations(context) }),
  }), { scope }).instance(null);
}
