import { z } from 'zod';
import type { AdmittedPromptContentPart, PromptContentPart as NativePromptContentPart } from '@deepseek-ai/dsh-attachment/types';
import type { FileUploadReceiptId } from '../files/contract.ts';

export type { AdmittedPromptContentPart } from '@deepseek-ai/dsh-attachment/types';
export type { FileUploadValue, FileUploadReceiptId, EncodedFileUploadRequest } from '../files/contract.ts';

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
/** 原始文件输入只接受同会话上传签发的 receipt，不接受客户端提供持久文件引用。 */
export type PromptContentPart = NativePromptContentPart | { readonly type: 'file'; readonly receiptId: FileUploadReceiptId };
export type InputDelivery = 'followup' | 'steer' | 'inject';
const imageMediaTypeSchema = z.enum(['image/png', 'image/jpeg', 'image/webp', 'image/gif']);
const textPartSchema = z.object({ type: z.literal('text'), text: z.string() }).strict();
const promptPartSchema = z.discriminatedUnion('type', [textPartSchema,
  z.object({ type: z.literal('image'), mediaType: imageMediaTypeSchema, data: z.string().min(1), name: z.string().optional() }).strict(),
  z.object({ type: z.literal('file'), receiptId: z.custom<FileUploadReceiptId>(value => typeof value === 'string' && value.length > 0) }).strict(),
]);
const contentPresent = (parts: readonly { readonly type: string; readonly text?: string }[]) =>
  parts.some(part => part.type !== 'text' || (part.text !== undefined && part.text.trim().length > 0));
const rawContentSchema = z.union([z.string(), promptPartSchema.array().readonly()])
  .transform(value => typeof value === 'string' ? [{ type: 'text' as const, text: value }] : value)
  .refine(contentPresent, '输入必须包含非空文本或附件');
// 这里读取宿主持久事实的形状；原始提交使用上方 receipt schema，不能走此分支。
const attachmentIdSchema = z.custom<Extract<AdmittedPromptContentPart, { type: 'file' }>['attachment']['attachmentId']>(
  value => typeof value === 'string' && value.length > 0);
const dimensionsSchema = z.object({ width: z.number().int().positive(), height: z.number().int().positive() }).strict();
const admittedPartSchema = z.discriminatedUnion('type', [textPartSchema,
  z.object({ type: z.literal('image'), attachment: z.object({ attachmentId: attachmentIdSchema, mediaType: imageMediaTypeSchema,
    bytes: z.number().int().nonnegative(), width: z.number().int().positive(), height: z.number().int().positive(),
    name: z.string().optional(), originalDimensions: dimensionsSchema.optional() }).strict() }).strict(),
  z.object({ type: z.literal('file'), attachment: z.object({ attachmentId: attachmentIdSchema,
    name: z.string(), bytes: z.number().int().nonnegative() }).strict() }).strict(),
]);
const admittedContentSchema = admittedPartSchema.array().readonly().refine(contentPresent, '输入必须包含非空文本或附件');
const submissionFields = { requestId: z.string().min(1).max(200), scope: z.json() };
const ordinaryFields = { intent: z.enum(['question', 'goal', 'followup']), delivery: z.enum(['followup', 'steer', 'inject']),
  result: z.never().optional() };
const resultFields = { intent: z.literal('result'), delivery: z.never().optional(),
  result: z.object({ continuation: continuationSchema, wait: waitSchema, ready: z.boolean() }).strict() };
const goalHasText = (value: { readonly intent: string; readonly content: readonly { readonly type: string; readonly text?: string }[] }, ctx: z.RefinementCtx) => {
  if (value.intent === 'goal' && !value.content.some(part => part.type === 'text' && part.text !== undefined && part.text.trim().length > 0))
    ctx.addIssue({ code: 'custom', path: ['content'], message: 'goal 输入必须提供真实的非空文本目标' });
};
/** string 只是单个 text part 的语法糖；验证非空不会改写正文。 */
export const submissionSchema = z.discriminatedUnion('intent', [
  z.object({ ...submissionFields, ...ordinaryFields, content: rawContentSchema }).strict().superRefine(goalHasText),
  z.object({ ...submissionFields, ...resultFields, content: rawContentSchema }).strict(),
]);
export const admittedSubmissionSchema = z.discriminatedUnion('intent', [
  z.object({ ...submissionFields, ...ordinaryFields, content: admittedContentSchema }).strict().superRefine(goalHasText),
  z.object({ ...submissionFields, ...resultFields, content: admittedContentSchema }).strict(),
]);
type WithInputScope<Value, T> = Value extends { intent: 'result' }
  ? Readonly<Omit<Value, 'scope' | 'result'>> & { readonly scope: T;
      readonly result: { readonly continuation: Continuation; readonly wait: WaitReference; readonly ready: boolean } }
  : Value extends unknown ? Readonly<Omit<Value, 'scope' | 'result'>> & { readonly scope: T; readonly result?: never } : never;
export type Submission<T = Json> = WithInputScope<z.input<typeof submissionSchema>, T>;
export type NormalizedSubmission<T = Json> = WithInputScope<z.output<typeof submissionSchema>, T>;
export type AdmittedSubmission<T = Json> = WithInputScope<z.output<typeof admittedSubmissionSchema>, T>;
export interface Admission { actorId: string; participant: Exclude<InputParticipant, 'native'>; signal: AbortSignal; }
export interface CancellationOptions { readonly signal?: AbortSignal; }
export interface SubmitOptions extends CancellationOptions { actorId?: string; }
export type FileUploadSource = Uint8Array | Blob;
export interface UploadFileOptions extends SubmitOptions { name?: string; }

export const toolActivitySchema = z.object({ callId: z.string(), rootCallId: z.string(), parentCallId: z.string().nullable(), name: z.string(),
  state: z.enum(['running', 'returned', 'failed']), inputs: inputReferenceSchema.array().readonly(),
  observedAt: z.number(), result: z.json().optional() }).strict();
export type ToolActivity = Readonly<Omit<z.infer<typeof toolActivitySchema>, 'inputs' | 'result'>> & {
  readonly inputs: readonly InputReference[];
  readonly result?: Json;
};
export const requestActivitySchema = z.object({ requestId: z.string(), state: z.enum(['declared', 'accepted', 'consumed', 'settled']),
  reply: z.string().nullable(), outcome: z.string().nullable(), tools: toolActivitySchema.array().readonly() }).strict();
export type RequestActivity = Readonly<Omit<z.infer<typeof requestActivitySchema>, 'tools'>> & {
  readonly tools: readonly ToolActivity[];
};
export const goalActivitySchema = z.object({ id: z.string(), revision: z.number().int().positive(), objective: z.string(),
  phase: z.enum(['active', 'paused', 'blocked', 'complete']), blockedReason: z.object({ code: z.string(), message: z.string() }).optional(),
  maxGoalRounds: z.number().int().positive(), roundsStarted: z.number().int().nonnegative(),
  activation: z.enum(['armed', 'disarmed']).nullable() }).strict();
export type GoalActivity = Readonly<Omit<z.infer<typeof goalActivitySchema>, 'blockedReason'>> & {
  readonly blockedReason?: { readonly code: string; readonly message: string };
};
export const conversationActivitySchema = z.object({ sessionId: z.string(), sequence: z.number().int(),
  source: z.enum(['live', 'durable']), execution: z.enum(['idle', 'running', 'unavailable']),
  goal: goalActivitySchema.nullable(),
  requests: z.record(z.string(), requestActivitySchema),
  waits: z.object({ continuation: continuationSchema, wait: waitSchema }).array().readonly() }).strict();
export type ConversationActivity = Readonly<Omit<z.infer<typeof conversationActivitySchema>, 'goal' | 'requests' | 'waits'>> & {
  readonly goal: GoalActivity | null;
  readonly requests: Readonly<Record<string, RequestActivity>>;
  readonly waits: readonly { readonly continuation: Continuation; readonly wait: WaitReference }[];
};

/** @internal 公开观察持有自己的协议快照，不能通过共享引用改写原生投影。 */
export function ownConversationActivity(raw: unknown): ConversationActivity {
  const value = conversationActivitySchema.parse(raw);
  const freeze = (item: unknown): void => {
    if (item === null || typeof item !== 'object') return;
    for (const child of Object.values(item)) freeze(child);
    Object.freeze(item);
  };
  freeze(value);
  return value;
}

export const workspaceInfoSchema = z.object({ workspaceId: z.string(), directory: z.string(), title: z.string(), sessionIds: z.string().array().readonly() }).strict();
export type WorkspaceInfo = Readonly<z.infer<typeof workspaceInfoSchema>>;
export const workspaceRegistrationSchema = z.object({ directory: z.string(), title: z.string().optional() }).strict();
export type WorkspaceRegistration = Readonly<z.infer<typeof workspaceRegistrationSchema>>;
export const conversationCreateOptionsSchema = z.object({ workspaceId: z.string(), sessionId: z.string().optional() }).strict();
export type ConversationCreateOptions = Readonly<z.infer<typeof conversationCreateOptionsSchema>>;
export const creationReferenceSchema = z.object({ environmentId: z.string(), workspaceId: z.string(), sessionId: z.string() }).strict();
export type CreationReference = Readonly<z.infer<typeof creationReferenceSchema>>;
