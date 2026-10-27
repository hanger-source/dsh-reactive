import { z } from 'zod';
import { defineOperation } from '../operations/contract.ts';
import type { ActionCommandOrigin } from '../actions/contract.ts';
import { goalReferenceSchema } from '../conversations/protocol.ts';

export const nativeChildCommandSchema = z.object({ parentSessionId: z.string().min(1), invocationId: z.string().min(1) }).strict();
export const nativeChildReferenceSchema = z.object({ parentSessionId: z.string().min(1), childSessionId: z.string().min(1) }).strict();
export const nativeChildReceiptSchema = nativeChildReferenceSchema.extend({ messageId: z.string().min(1), goal: goalReferenceSchema }).strict();
export const nativeChildPhaseSchema = z.enum(['reserved', 'creation-incomplete', 'active', 'paused', 'blocked', 'complete', 'error', 'cancelled', 'interrupted']);
export const nativeChildStateSchema = z.object({ childSessionId: z.string().min(1), phase: nativeChildPhaseSchema,
  goal: goalReferenceSchema.nullable(), sequence: z.number().int().nonnegative(), reason: z.string().nullable() }).strict();
export const nativeChildFeedbackSchema = z.object({ childSessionId: z.string().min(1), phase: nativeChildPhaseSchema,
  goal: goalReferenceSchema.nullable(), sequence: z.number().int().nonnegative(), reason: z.string().nullable() }).strict();

/** 原生子工作的正式完成条件属于子会话自己的 Goal，运行结束仅是一次协作反馈。 */
export const NativeChildWork = defineOperation({ kind: 'reactive.native-child-work', version: 1,
  command: nativeChildCommandSchema, reference: nativeChildReferenceSchema, state: nativeChildStateSchema,
  settled: state => state.phase === 'complete' ? state : undefined, feedback: nativeChildFeedbackSchema });

export type NativeChildCommand = z.infer<typeof nativeChildCommandSchema>;
export type NativeChildReference = z.infer<typeof nativeChildReferenceSchema>;
export type NativeChildReceipt = z.infer<typeof nativeChildReceiptSchema>;
export type NativeChildState = z.infer<typeof nativeChildStateSchema>;
export type NativeChildFeedback = z.infer<typeof nativeChildFeedbackSchema>;

/** 供 defineAction 组合标准回执与 Operation 映射；业务仍自行命名、定义输入和目标。 */
export const nativeChildWorkAction = Object.freeze({ output: nativeChildReceiptSchema,
  operation: { definition: NativeChildWork,
    command(_input: unknown, _scope: unknown, origin: ActionCommandOrigin): NativeChildCommand {
      return { parentSessionId: origin.sessionId, invocationId: origin.invocationId };
    },
    reference(receipt: NativeChildReceipt): NativeChildReference {
      return { parentSessionId: receipt.parentSessionId, childSessionId: receipt.childSessionId };
    },
  },
});
