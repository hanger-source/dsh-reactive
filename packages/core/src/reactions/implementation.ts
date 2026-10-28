import type { Observable, OperatorFunction } from 'rxjs';
import type { z } from 'zod';
import { assertReactionDefinition, type ReactionContract, type ReactionInput, type ReactionTarget } from './definition.ts';

/** 直接接收 RxJS concatMap、mergeMap、switchMap 或 exhaustMap，不另定义并发策略名。 */
export interface ReactionOperator {
  <Input, Output>(project: (event: Input, index: number) => Observable<Output>): OperatorFunction<Input, Output>;
}
export interface ReactionSourceContext { readonly signal: AbortSignal; }
export interface ReactionExecutionContext<D extends ReactionContract> { readonly params: ReactionTarget<D>; }

export interface ReactionImplementationOptions<D extends ReactionContract> {
  /** 条件、去重与事件选择直接使用标准 RxJS；来源 signal 只拥有这次持续感知。 */
  observe(params: ReactionTarget<D>, context: ReactionSourceContext): Observable<z.input<D['event']>>;
  readonly operator: ReactionOperator;
  /** 直接调用真实业务函数、Action、Job 或明确会话；框架不把事件隐式转成 prompt。 */
  execute(event: ReactionInput<D>, context: ReactionExecutionContext<D>):
    z.input<D['result']> | PromiseLike<z.input<D['result']>> | Observable<z.input<D['result']>>;
}
export interface ReactionImplementation<D extends ReactionContract = ReactionContract> { readonly definition: D; }
const implementations = new WeakMap<object, unknown>();

/** @internal 按实际实现身份区分正式绑定与局部选项，不猜结构或名字。 */
export function isReactionImplementation(value: unknown): value is ReactionImplementation {
  return typeof value === 'object' && value !== null && implementations.has(value);
}

export function implementReaction<D extends ReactionContract>(definition: D,
  options: ReactionImplementationOptions<D>): ReactionImplementation<D> {
  assertReactionDefinition(definition);
  if (typeof options.observe !== 'function' || typeof options.execute !== 'function' || typeof options.operator !== 'function')
    throw new Error('REACTIVE_REACTION_IMPLEMENTATION_INVALID');
  const implementation = Object.freeze({ definition });
  implementations.set(implementation, Object.freeze({ ...options }));
  return implementation;
}

/** @internal 精确实现对象恢复同一合同的处理函数，不按名字查找执行者。 */
export function reactionImplementation<D extends ReactionContract>(implementation: ReactionImplementation<D>): ReactionImplementationOptions<D> {
  const options = implementations.get(implementation);
  if (!options) throw new Error('REACTIVE_REACTION_IMPLEMENTATION_REQUIRED');
  return options as ReactionImplementationOptions<D>;
}
