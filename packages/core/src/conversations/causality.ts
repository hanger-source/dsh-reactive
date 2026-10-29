import { z } from 'zod';

/** 输入和关联接受不可变 JSON；协议 parse 负责取得自有 wire 快照。 */
export type Json = string | number | boolean | null | readonly Json[] | { readonly [key: string]: Json };
/** 原生通道不推断人或服务身份；框架签发的参与来源随持久输入保留。 */
export type InputParticipant = 'human' | 'service' | 'native';
export const inputReferenceSchema = z.object({ sourceSessionId: z.string().min(1), messageId: z.string().min(1), requestId: z.string().nullable(), source: z.string(),
  intent: z.enum(['question', 'goal', 'followup', 'result']), scope: z.json(), actorId: z.string().min(1).nullable(),
  participant: z.enum(['human', 'service', 'native']) }).strict();
export type InputReference<T = Json> = Readonly<Omit<z.infer<typeof inputReferenceSchema>, 'scope'>> & { readonly scope: T };
export const goalReferenceSchema = z.object({ id: z.string().min(1), revision: z.number().int().positive() }).strict();
export type GoalReference = Readonly<z.infer<typeof goalReferenceSchema>>;
export const continuationSchema = z.object({ sessionId: z.string().min(1), inputs: inputReferenceSchema.array().readonly(),
  goal: goalReferenceSchema.nullable() }).strict();
export interface Continuation {
  readonly sessionId: string;
  readonly inputs: readonly InputReference[];
  readonly goal: GoalReference | null;
}
export const waitSchema = z.object({ id: z.string().min(1), operation: z.json() }).strict();
export interface WaitReference { readonly id: string; readonly operation: Json; }
export interface InputResult {
  readonly continuation: Continuation;
  readonly wait: WaitReference;
  readonly ready: boolean;
}
