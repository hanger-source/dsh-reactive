import { z } from 'zod';
import type { Json } from '../conversations/protocol.ts';

/** 事件保留来源给出的因果事实；这份合同本身不签发 Actor、Goal 或工具权限。 */
export interface ReactionContract {
  readonly name: string;
  readonly params: z.ZodType<Json>;
  readonly event: z.ZodType;
  readonly result: z.ZodType;
}
export interface ReactionDefinition<P extends z.ZodType<Json>, E extends z.ZodType, R extends z.ZodType>
  extends ReactionContract { readonly params: P; readonly event: E; readonly result: R; }

export type ReactionParameters<D extends ReactionContract> = z.input<D['params']>;
export type ReactionTarget<D extends ReactionContract> = z.output<D['params']>;
export type ReactionInput<D extends ReactionContract> = z.output<D['event']>;
export type ReactionResult<D extends ReactionContract> = z.output<D['result']>;

const definitions = new WeakSet<object>();

export function defineReaction<P extends z.ZodType<Json>, E extends z.ZodType, R extends z.ZodType>(
  options: { readonly name: string; readonly params: P; readonly event: E; readonly result: R },
): ReactionDefinition<P, E, R> {
  if (!options.name.trim()) throw new Error('REACTIVE_REACTION_NAME_REQUIRED');
  const definition = Object.freeze({ name: options.name, params: options.params, event: options.event, result: options.result });
  definitions.add(definition);
  return definition;
}

/** @internal 同名或同形对象不能冒充原事件合同。 */
export function assertReactionDefinition(definition: ReactionContract): void {
  if (!definitions.has(definition)) throw new Error('REACTIVE_REACTION_DEFINITION_REQUIRED');
}
