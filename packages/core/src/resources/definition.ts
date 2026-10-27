import { z } from 'zod';
import type { Json } from '../conversations/protocol.ts';

/** 参数的解析结果决定实例身份；资源值可以是 Date 等领域值。 */
export interface ResourceContract {
  readonly name: string;
  readonly params: z.ZodType<Json>;
  readonly value: z.ZodType;
}

export interface ResourceDefinition<P extends z.ZodType<Json>, V extends z.ZodType> extends ResourceContract {
  readonly params: P;
  readonly value: V;
}

export type ResourceParameters<D extends ResourceContract> = z.input<D['params']>;
export type ResourceTarget<D extends ResourceContract> = z.output<D['params']>;
export type ResourceData<D extends ResourceContract> = z.output<D['value']>;

const definitions = new WeakSet<object>();

/** 只定义可复用的资源合同，不读取、订阅或登记全局查询。 */
export function defineResource<P extends z.ZodType<Json>, V extends z.ZodType>(
  options: { readonly name: string; readonly params: P; readonly value: V },
): ResourceDefinition<P, V> {
  if (!options.name.trim()) throw new Error('REACTIVE_RESOURCE_NAME_REQUIRED');
  const definition = Object.freeze({ name: options.name, params: options.params, value: options.value });
  definitions.add(definition);
  return definition;
}

/** @internal 定义身份不能由同形对象或同名字符串代替。 */
export function assertResourceDefinition(definition: ResourceContract): void {
  if (!definitions.has(definition)) throw new Error('REACTIVE_RESOURCE_DEFINITION_REQUIRED');
}
