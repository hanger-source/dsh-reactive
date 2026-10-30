import { z } from 'zod';
import type { LlmFailure } from '@deepseek-ai/dsh-llm';
import type { PromptContentPart as NativePromptContentPart } from '@deepseek-ai/dsh-attachment/types';
import type { FileUploadReceiptId } from '../files/contract.ts';
import type { AttachmentIdType } from '../attachments/index.ts';
import { continuationSchema, inputReferenceSchema, waitSchema, type Continuation, type InputParticipant, type InputReference, type InputResult, type Json, type WaitReference } from './causality.ts';
import { operationFollowSnapshotSchema, type OperationFollowSnapshot } from '../operations/follows.ts';

export type { AdmittedPromptContentPart } from '@deepseek-ai/dsh-attachment/types';
export type { FileUploadValue, FileUploadReceiptId, EncodedFileUploadRequest } from '../files/contract.ts';

export { continuationSchema, goalReferenceSchema, inputReferenceSchema, waitSchema, type Continuation, type GoalReference, type InputParticipant, type InputReference, type InputResult, type Json, type WaitReference } from './causality.ts';

/** 原始文件输入只接受同会话上传签发的 receipt，不接受客户端提供持久文件引用。 */
export type PromptContentPart = NativePromptContentPart | { readonly type: 'file'; readonly receiptId: FileUploadReceiptId };
export type InputDelivery = 'followup' | 'steer' | 'inject';
/** 原生 turn/end:error 的同源投影，不另造故障记录或请求生命周期。 */
export interface ExecutionFailure {
  readonly sequence: number;
  readonly observedAt: number;
  readonly turn: number;
  readonly error: LlmFailure;
  readonly inputs: readonly InputReference[];
  readonly unentered: readonly {
    readonly messageId: string;
    readonly questionCallId: string | null;
    readonly target: 'next-turn' | 'next-step';
    readonly inputs: readonly InputReference[];
  }[];
}
const llmFailureSchema: z.ZodType<LlmFailure> = z.object({ message: z.string(), code: z.string(),
  status: z.number().optional(), providerRetryAfterMs: z.number().optional(),
  requestId: z.custom<NonNullable<LlmFailure['requestId']>>(value => typeof value === 'string').optional(),
  offloadImages: z.number().optional() }).strict();
export const executionFailureSchema: z.ZodType<ExecutionFailure> = z.object({
  sequence: z.number().int().nonnegative(), observedAt: z.number(), turn: z.number().int().nonnegative(),
  error: llmFailureSchema, inputs: inputReferenceSchema.array().readonly(),
  unentered: z.object({ messageId: z.string(), questionCallId: z.string().nullable(),
    target: z.enum(['next-turn', 'next-step']), inputs: inputReferenceSchema.array().readonly() }).strict().array().readonly(),
}).strict();
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
const attachmentIdSchema: z.ZodType<AttachmentIdType, AttachmentIdType> = z.custom<AttachmentIdType>(
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
/** 调用取消；读取、受理前检查和回执等待按各自边界响应，不撤回已经受理的工作。 */
export interface CancellationOptions { readonly signal?: AbortSignal; }
export interface SubmitOptions extends CancellationOptions { actorId?: string; }
export type FileUploadSource = Uint8Array | Blob;
export interface UploadFileOptions extends SubmitOptions { name?: string; }

export const toolActivitySchema = z.object({ callId: z.string(), callSequence: z.number().int().nonnegative(), rootCallId: z.string(), parentCallId: z.string().nullable(), name: z.string(),
  /** null 只表示没有 Invocation 关联事实，不判定业务是否执行。 */
  invocationId: z.string().min(1).nullable(),
  state: z.enum(['running', 'returned', 'failed']), inputs: inputReferenceSchema.array().readonly(),
  observedAt: z.number(), result: z.json().optional() }).strict();
export type ToolActivity = Readonly<Omit<z.infer<typeof toolActivitySchema>, 'inputs' | 'result'>> & {
  readonly inputs: readonly InputReference[];
  readonly result?: Json;
};
/** 持久声明的原请求；尚无原生 messageId，不等于已进入模型的 InputReference。 */
export const requestDeclarationSchema = z.object({ scope: z.json(), intent: z.enum(['question', 'goal', 'followup', 'result']),
  actorId: z.string().min(1), participant: z.enum(['human', 'service']), result: resultFields.result.nullable() }).strict();
export type RequestDeclaration = Readonly<Omit<z.infer<typeof requestDeclarationSchema>, 'scope' | 'result'>> & {
  readonly scope: Json;
  readonly result: InputResult | null;
};
/** 原生回复发生身份；null 表示未取得该步骤的输入关联证据。 */
export const replyActivitySchema = z.object({ messageId: z.string().min(1), turn: z.number().int().nonnegative(),
  step: z.number().int().nonnegative(), sequence: z.number().int().nonnegative(), observedAt: z.number(),
  text: z.string(), interrupted: z.boolean(), inputs: inputReferenceSchema.array().readonly().nullable() }).strict();
export type ReplyActivity = Readonly<Omit<z.infer<typeof replyActivitySchema>, 'inputs'>> & {
  readonly inputs: readonly InputReference[] | null;
};
export const requestActivitySchema = z.object({ requestId: z.string(), state: z.enum(['declared', 'accepted', 'consumed', 'settled']),
  declaration: requestDeclarationSchema.nullable(), inputs: inputReferenceSchema.array().readonly(),
  reply: z.string().nullable(), outcome: z.string().nullable(), tools: toolActivitySchema.array().readonly(),
  failures: executionFailureSchema.array().readonly() }).strict();
export type RequestActivity = Readonly<Omit<z.infer<typeof requestActivitySchema>, 'tools' | 'declaration' | 'inputs'>> & {
  readonly declaration: RequestDeclaration | null;
  readonly inputs: readonly InputReference[];
  readonly tools: readonly ToolActivity[];
};
export const goalActivitySchema = z.object({ id: z.string(), revision: z.number().int().positive(), objective: z.string(),
  inputs: inputReferenceSchema.array().readonly(),
  phase: z.enum(['active', 'paused', 'blocked', 'complete']), blockedReason: z.object({ code: z.string(), message: z.string() }).optional(),
  maxGoalRounds: z.number().int().positive(), roundsStarted: z.number().int().nonnegative(),
  activation: z.enum(['armed', 'disarmed']).nullable() }).strict();
export type GoalActivity = Readonly<Omit<z.infer<typeof goalActivitySchema>, 'blockedReason' | 'inputs'>> & {
  readonly inputs: readonly InputReference[];
  readonly blockedReason?: { readonly code: string; readonly message: string };
};
export const conversationActivitySchema = z.object({ sessionId: z.string(), sequence: z.number().int(),
  inputs: inputReferenceSchema.array().readonly(), calls: toolActivitySchema.array().readonly(), replies: replyActivitySchema.array().readonly(),
  source: z.enum(['live', 'durable']), execution: z.enum(['idle', 'running', 'unavailable']),
  goal: goalActivitySchema.nullable(),
  operationFollows: operationFollowSnapshotSchema.array().readonly(),
  requests: z.record(z.string(), requestActivitySchema),
  failures: executionFailureSchema.array().readonly(),
  waits: z.object({ continuation: continuationSchema, wait: waitSchema }).array().readonly() }).strict();
export type ConversationActivity = Readonly<Omit<z.infer<typeof conversationActivitySchema>, 'goal' | 'requests' | 'waits' | 'inputs' | 'calls' | 'replies' | 'operationFollows'>> & {
  readonly inputs: readonly InputReference[];
  readonly calls: readonly ToolActivity[];
  readonly operationFollows: readonly OperationFollowSnapshot[];
  readonly replies: readonly ReplyActivity[];
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
