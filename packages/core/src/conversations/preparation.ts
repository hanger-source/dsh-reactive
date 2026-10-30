import { z } from 'zod';
import { continuationSchema, goalReferenceSchema, inputReferenceSchema, type Continuation, type GoalReference, type InputReference } from './protocol.ts';

/** 当前真实模型装配的瞬态读取现场；不提供执行资格。 */
export type ModelPreparationWait =
  | { readonly kind: 'delegation' }
  | { readonly kind: 'inputs' }
  | { readonly kind: 'step-admission' }
  | { readonly kind: 'skill-services' }
  | { readonly kind: 'working-directory' }
  | { readonly kind: 'skill-release' }
  | { readonly kind: 'assembly' }
  | { readonly kind: 'check'; readonly id: string }
  | { readonly kind: 'context'; readonly name: string }
  | { readonly kind: 'skill-directory'; readonly directory: string }
  | { readonly kind: 'skill'; readonly name: string };

/** 本轮实际承接的反馈及精确 Goal 修订；只是准备事实，不签发续接或调用权限。 */
export interface ModelPreparationAdmission {
  readonly source:
    | { readonly kind: 'result'; readonly requestId: string; readonly messageId: string }
    | { readonly kind: 'human-reply'; readonly messageId: string; readonly questionCallId: string; readonly callSequence: number };
  readonly continuation: Continuation;
  readonly goal: GoalReference | null;
}

export interface ModelPreparation {
  readonly id: string;
  readonly inputs: readonly InputReference[];
  readonly waiting: readonly ModelPreparationWait[];
  readonly admissions: readonly ModelPreparationAdmission[];
}

const identity = z.string().min(1);
export const modelPreparationSchema = z.object({
  id: identity,
  inputs: inputReferenceSchema.array().readonly(),
  admissions: z.object({
    source: z.discriminatedUnion('kind', [
      z.object({ kind: z.literal('result'), requestId: identity, messageId: identity }).strict(),
      z.object({ kind: z.literal('human-reply'), messageId: identity, questionCallId: identity,
        callSequence: z.number().int().nonnegative() }).strict(),
    ]),
    continuation: continuationSchema,
    goal: goalReferenceSchema.nullable(),
  }).strict().array().readonly(),
  waiting: z.discriminatedUnion('kind', [
    z.object({ kind: z.literal('delegation') }).strict(),
    z.object({ kind: z.literal('inputs') }).strict(),
    z.object({ kind: z.literal('step-admission') }).strict(),
    z.object({ kind: z.literal('check'), id: identity }).strict(),
    z.object({ kind: z.literal('context'), name: identity }).strict(),
    z.object({ kind: z.literal('skill-services') }).strict(),
    z.object({ kind: z.literal('skill-directory'), directory: identity }).strict(),
    z.object({ kind: z.literal('skill'), name: identity }).strict(),
    z.object({ kind: z.literal('working-directory') }).strict(),
    z.object({ kind: z.literal('skill-release') }).strict(),
    z.object({ kind: z.literal('assembly') }).strict(),
  ]).array().readonly(),
}).strict();

/** @internal 每次调用返回只属于本次真实等待的退出令牌。 */
export type PreparationObserver = (wait: ModelPreparationWait) => () => void;

/** @internal 通知可同步撤销 owner；进入真实读取前重新核验取消。 */
export async function observePreparation<T>(observer: PreparationObserver | undefined, wait: ModelPreparationWait,
  signal: AbortSignal, read: () => T | PromiseLike<T>): Promise<T> {
  const release = observer?.(wait);
  let active = true;
  const stop = () => {
    if (!active) return;
    active = false;
    signal.removeEventListener('abort', stop);
    release?.();
  };
  // 撤销本轮读取时立即撤下令牌，不等待旧 Promise 的 finally 排入微任务。
  signal.addEventListener('abort', stop, { once: true });
  let result: T;
  try { signal.throwIfAborted(); result = await read(); }
  finally { stop(); }
  signal.throwIfAborted();
  return result;
}

/** @internal 拥有完整 detached JSON，防止生产者或消费者改写同源现场。 */
export function ownModelPreparation(raw: unknown): ModelPreparation {
  const value = modelPreparationSchema.parse(raw);
  const freeze = (item: unknown): void => {
    if (item === null || typeof item !== 'object') return;
    for (const child of Object.values(item)) freeze(child);
    Object.freeze(item);
  };
  freeze(value);
  return value;
}
