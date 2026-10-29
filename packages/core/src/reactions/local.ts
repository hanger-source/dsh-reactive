import { z } from 'zod';
import type { Observable } from 'rxjs';
import type { Scope } from '../reactivity/scope.ts';
import { defineReaction, type ReactionDefinition } from './definition.ts';
import { implementReaction, reactionImplementation, type ReactionOperator, type ReactionSourceContext } from './implementation.ts';
import { ReactionInstance } from './binding.ts';

export interface LocalReactionOptions<E extends z.ZodType, R extends z.ZodType> {
  /** 可选诊断标签；不参与运行身份、去重或业务授权。 */
  readonly name?: string;
  readonly event: E;
  readonly result: R;
  observe(context: ReactionSourceContext): Observable<z.input<E>>;
  readonly operator: ReactionOperator;
  execute(event: z.output<E>): z.input<R> | PromiseLike<z.input<R>> | Observable<z.input<R>>;
}
export type LocalReaction<E extends z.ZodType, R extends z.ZodType> = ReactionInstance<ReactionDefinition<z.ZodNull, E, R>>;

/** 局部关系无需发布参数合同；start 仍是唯一开始执行的命令。 */
export function createReaction<E extends z.ZodType, R extends z.ZodType>(
  scope: Scope, options: LocalReactionOptions<E, R>,
): LocalReaction<E, R> {
  scope.assertActive();
  const { name = 'local.reaction', event, result, observe, operator, execute } = options;
  if (typeof observe !== 'function') throw new Error('REACTIVE_REACTION_IMPLEMENTATION_INVALID');
  const definition = defineReaction({ name, params: z.null(), event, result });
  const implementation = implementReaction(definition, {
    observe: (_params, context) => observe(context), operator, execute,
  });
  return new ReactionInstance(scope, definition, null, reactionImplementation(implementation));
}
