import { createHash } from 'node:crypto';
import { z } from 'zod';
import type { UserMessage } from '@deepseek-ai/dsh-session';
import type { GoalProjection } from '@deepseek-ai/dsh-goal';
import type { ToolOccurrence } from '@hanger-source/dsh-reactive/kernel';
import type { InputReference, NormalizedSubmission, Continuation, WaitReference, GoalReference } from '@hanger-source/dsh-reactive/kernel';
import type { Admission } from '@hanger-source/dsh-reactive';
import type { ReactiveProjectionState } from './projections.ts';
import { inputReferenceSchema as inputReference, continuationSchema, waitSchema, admittedSubmissionSchema, type ConversationActivity } from '@hanger-source/dsh-reactive/kernel';
export { continuationSchema, waitSchema, submissionSchema } from '@hanger-source/dsh-reactive/kernel';
export type { ToolActivity, RequestActivity, ConversationActivity } from '@hanger-source/dsh-reactive';
const digestSchema = z.string().regex(/^sha256:[a-f0-9]{64}$/u);
const recordFields = { actorId: z.string().min(1).max(200).refine(value => value.trim().length > 0, '输入身份不能为空'),
  participant: z.enum(['human', 'service']),
  sourceDigest: digestSchema, digest: digestSchema };
/** 准备好的原生正文与原请求身份属于同一持久事实，不依赖上传 receipt 在进程中存活。 */
export const recordSchema = z.discriminatedUnion('intent', [
  admittedSubmissionSchema.options[0].safeExtend(recordFields),
  admittedSubmissionSchema.options[1].safeExtend(recordFields),
]).superRefine((record, ctx) => {
  if (record.digest !== contentDigest(record.content))
    ctx.addIssue({ code: 'custom', path: ['digest'], message: 'REACTIVE_INPUT_CONTENT_CONFLICT' });
});
export type SubmissionRecord = z.output<typeof recordSchema>;
/** 编辑准备只有在原生同一队列消息真正替换后生效；持久草稿不改写已受理正文。 */
export const inputEditRecordSchema = z.object({ messageId: z.string().min(1), previousDigest: digestSchema,
  record: recordSchema }).strict();
export type InputEditRecord = z.output<typeof inputEditRecordSchema>;
/** 编辑只改变正文；原请求、操作者、目标、投递与首次提交身份均保持不变。 */
export function inputRecordIdentity(record: SubmissionRecord) {
  const { content: _content, digest: _digest, ...identity } = record;
  return canonical(identity);
}
export const goalBindingSchema = z.object({ goalId: z.string(), inputs: inputReference.array().readonly() }).strict();
export const waitRecordSchema = z.object({ continuation: continuationSchema, wait: waitSchema }).strict();
/** 只结案框架确定未开始的那次调用，不判定或取消原业务操作。 */
export const operationNotStartedSchema = waitRecordSchema.extend({ id: z.string().min(1) }).strict();
export const childInputRecordSchema = z.object({ messageId: z.string().min(1), input: inputReference }).strict();
declare module '@deepseek-ai/dsh-session/types' {
  interface PluginRecordMap {
    'plugin:reactive/input': SubmissionRecord;
    'plugin:reactive/input-edit': InputEditRecord;
    'plugin:reactive/goal-inputs': { goalId: string; inputs: readonly InputReference[] };
    'plugin:reactive/wait': { continuation: Continuation; wait: WaitReference };
    'plugin:reactive/operation-not-started': z.infer<typeof operationNotStartedSchema>;
    'plugin:reactive/child-input': z.infer<typeof childInputRecordSchema>;
  }
}
/** 原生投影切面的只读业务关联视图；不订阅、不推进、不缓存会话日志。 */
export class NativeState {
  readonly current: readonly InputReference[];
  readonly activity: ConversationActivity;
  constructor(readonly value: ReactiveProjectionState, goal: GoalProjection | null, sequence: number) {
    this.current = value.current;
    this.activity = { sessionId: value.sessionId, sequence, source: 'durable', execution: 'unavailable',
      goal: goal ? { ...goal.goal, roundsStarted: goal.roundsStarted, activation: null } : null,
      requests: value.requests, waits: value.waits };
  }
  record(id: string) { return Object.hasOwn(this.value.records, id) ? this.value.records[id] : undefined; }
  hasGoalInput(requestId: string) { return Object.values(this.value.goals).some(inputs => inputs.some(input => input.requestId === requestId)); }
  /** 等待只约束原 Goal 的精确授权版本，不阻断人明确修改后的另一版本。 */
  hasWaits(goal: GoalReference) {
    return this.value.waits.some(item => item.continuation.goal?.id === goal.id && item.continuation.goal.revision === goal.revision);
  }
  call(id: string): ToolOccurrence | undefined {
    return Object.hasOwn(this.value.calls, id) ? this.value.calls[id] : undefined;
  }
  inputs(messages: readonly UserMessage[]) { return unique([...this.current, ...messages.flatMap(message => this.reference(message))]); }
  reference(message: UserMessage) { return referenceOf(this.value, message); }
}

export function referenceOf(state: Pick<ReactiveProjectionState, 'sessionId' | 'delegated' | 'records' | 'goals' | 'calls' | 'nativeInputs'>, message: UserMessage): readonly z.output<typeof inputReference>[] {
  // manager 的结算通知是控制触发；它没有人的业务授权，也不是子 Program 输入。
  if (message.source.kind === 'subagent-settled') return [];
  const native = state.nativeInputs[message.id];
  if (native) return [native];
  if (message.source.kind === 'goal') return state.goals[message.source.goalId] ?? [];
  if (message.source.kind === 'user-question-reply') {
    const call = Object.hasOwn(state.calls, message.source.callId) ? state.calls[message.source.callId] : undefined;
    if (!call) throw new Error('REACTIVE_HUMAN_QUESTION_CALL_NOT_FOUND');
    return call.inputs;
  }
  if (!['user', 'agent-message'].includes(message.source.kind)) return [];
  const requestId = 'rpcId' in message.source && typeof message.source.rpcId === 'string' ? message.source.rpcId : null;
  // 子原生初始消息须等真实回执关联后才成为其 Program 的本地输入。
  if (state.delegated && requestId === null) return [];
  const declared = requestId ? Object.hasOwn(state.records, requestId) ? state.records[requestId] : undefined : undefined;
  if (declared && declared.digest !== contentDigest(message.content)) throw new Error('REACTIVE_INPUT_CONTENT_CONFLICT');
  return unique<z.output<typeof inputReference>>([{ sourceSessionId: state.sessionId, messageId: message.id, requestId, source: message.source.kind,
    intent: declared?.intent ?? 'question', scope: declared?.scope ?? null,
    actorId: declared?.actorId ?? null, participant: declared?.participant ?? 'native' }, ...declared?.result?.continuation.inputs ?? []]);
}
export function contentDigest(content: unknown) { return 'sha256:' + createHash('sha256').update(canonical(content)).digest('hex'); }
/** 原始正文先作无损 string→text 归一；附件提升产生的持久引用不参与原请求身份。 */
export function submissionSourceDigest(input: NormalizedSubmission, actorId: string, participant: Admission['participant']) {
  return 'sha256:' + createHash('sha256').update(canonical({ submission: input, actorId, participant })).digest('hex');
}
/** 关联合同按 JSON 值核对；对象字段排列不改变同一个业务引用的身份。 */
export function canonical(value: unknown): string {
  return JSON.stringify(value, (_key, item: unknown) => {
    if (item === null || typeof item !== 'object' || Array.isArray(item)) return item;
    return Object.fromEntries(Object.keys(item).sort().map(key => [key, (item as Record<string, unknown>)[key]]));
  });
}
export function unique<T extends InputReference>(inputs: readonly T[]): readonly T[] {
  return [...new Map(inputs.map(input => [canonical([input.sourceSessionId, input.messageId]), input])).values()];
}
