import { z } from 'zod';
import type { ActionSource } from '../actions/contract.ts';
import { continuationSchema, waitSchema, type Continuation, type Json, type WaitReference } from '../conversations/causality.ts';

/** wait 暂缓原工作；background 保留结果续接关系，让当前工作继续。 */
export type OperationFollowMode = 'wait' | 'background';

/** 持久化的是业务命令与精确原工作的关联，不是订阅或 JavaScript 调用栈。 */
interface OperationFollowBase {
  readonly id: string;
  readonly operation: { readonly kind: string; readonly version: number };
  readonly command: Json;
  readonly continuation: Continuation;
  readonly wait: WaitReference;
  readonly scope: Json;
  readonly target: Json;
  readonly source: Extract<ActionSource, { readonly kind: 'tool' }>;
  readonly mode: OperationFollowMode;
}
export type OperationFollowLink = OperationFollowBase & (
  | { readonly when: 'settled'; readonly after: null }
  | { readonly when: 'feedback'; readonly after: string | null }
);

const identity = z.string().min(1);
const followBaseSchema = z.object({
  id: identity,
  operation: z.object({ kind: identity, version: z.number().int().positive().max(Number.MAX_SAFE_INTEGER) }).strict(),
  command: z.json(), continuation: continuationSchema, wait: waitSchema, scope: z.json(), target: z.json(),
  mode: z.enum(['wait', 'background']),
  source: z.object({ kind: z.literal('tool'), callId: identity, rootCallId: identity, parentCallId: identity.nullable() }).strict(),
}).strict();
export const operationFollowLinkSchema = z.discriminatedUnion('when', [
  followBaseSchema.extend({ when: z.literal('settled'), after: z.null() }),
  followBaseSchema.extend({ when: z.literal('feedback'), after: identity.nullable() }),
]);

/** 同一会话切面中的持久关联事实；准备正文不表示原请求已经受理或消费。 */
export interface OperationFollowSnapshot {
  readonly link: OperationFollowLink;
  readonly receipt: { readonly reference: Json } | null;
  readonly preparedDelivery: {
    readonly requestId: string;
    readonly actorId: string;
    readonly notifierId: string;
    readonly reference?: Json;
    readonly node?: { readonly id: string; readonly payload: Json };
  } | null;
  readonly notStarted: {
    readonly id: string;
    readonly continuation: Continuation;
    readonly wait: WaitReference;
  } | null;
  /** 本次 Follow 未进入 owner；原业务操作仍可能已经运行。 */
  readonly unregistered: {
    readonly id: string;
    readonly continuation: Continuation;
    readonly wait: WaitReference;
  } | null;
}

export const operationFollowSnapshotSchema = z.object({
  link: operationFollowLinkSchema,
  receipt: z.object({ reference: z.json() }).strict().nullable(),
  preparedDelivery: z.object({
    requestId: identity, actorId: identity, notifierId: identity,
    reference: z.json().optional(),
    node: z.object({ id: identity, payload: z.json() }).strict().optional(),
  }).strict().nullable(),
  notStarted: z.object({ id: identity, continuation: continuationSchema, wait: waitSchema }).strict().nullable(),
  unregistered: z.object({ id: identity, continuation: continuationSchema, wait: waitSchema }).strict().nullable(),
}).strict();
