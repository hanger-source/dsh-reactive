import { createHash } from 'node:crypto';
import { z } from 'zod';
import type { SessionEvent, UserMessage } from '@deepseek-ai/dsh-session';
import type { GoalProjection } from '@deepseek-ai/dsh-goal';
import type { ToolOccurrence } from '@hanger-source/dsh-reactive/kernel';
import type { InputReference, NormalizedSubmission, Continuation, WaitReference, GoalReference } from '@hanger-source/dsh-reactive/kernel';
import type { Admission } from '@hanger-source/dsh-reactive';
import type { ToolActivity } from '@hanger-source/dsh-reactive';
import type { OperationFollowSnapshot } from '@hanger-source/dsh-reactive';
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
  constructor(readonly value: ReactiveProjectionState, goal: GoalProjection | null, sequence: number,
    operationFollows: readonly OperationFollowSnapshot[]) {
    this.current = value.current;
    const calls: ToolActivity[] = [];
    const related = new Map<string, { inputs: InputReference[]; tools: ToolActivity[]; reply: string | null }>();
    for (const id of Object.keys(value.requests)) related.set(id, { inputs: [], tools: [], reply: null });
    const requestIds = (inputs: readonly InputReference[]) => new Set(inputs.flatMap(input =>
      input.sourceSessionId === value.sessionId && input.requestId !== null ? [input.requestId] : []));
    for (const input of value.inputs) {
      if (input.sourceSessionId === value.sessionId && input.requestId !== null) related.get(input.requestId)?.inputs.push(input);
    }
    for (const occurrence of Object.values(value.calls)) {
      const call = toolActivityOf(occurrence);
      calls.push(call);
      for (const id of requestIds(call.inputs)) related.get(id)?.tools.push(call);
    }
    for (const reply of value.replies) if (reply.text.trim().length > 0 && reply.inputs !== null) {
      for (const id of requestIds(reply.inputs)) {
        const request = related.get(id);
        if (request) request.reply = reply.text;
      }
    }
    const requests = Object.fromEntries(Object.entries(value.requests).map(([requestId, lifecycle]) => {
      const record = this.record(requestId);
      return [requestId, { ...lifecycle,
        declaration: record ? { scope: record.scope, intent: record.intent, actorId: record.actorId, participant: record.participant,
          result: record.result ?? null } : null,
        ...related.get(requestId)! }];
    }));
    this.activity = { sessionId: value.sessionId, sequence, source: 'durable', execution: 'unavailable',
      goal: goal ? { ...goal.goal, roundsStarted: goal.roundsStarted, activation: null, inputs: this.goalInputs(goal.goal.id) } : null,
      inputs: value.inputs, calls, replies: value.replies, requests, waits: value.waits, operationFollows };
  }
  record(id: string) { return Object.hasOwn(this.value.records, id) ? this.value.records[id] : undefined; }
  goalInputs(id: string): readonly InputReference[] { return Object.hasOwn(this.value.goals, id) ? this.value.goals[id]! : []; }
  hasGoalInput(requestId: string) { return Object.values(this.value.goals).some(inputs => inputs.some(input =>
    input.sourceSessionId === this.value.sessionId && input.requestId === requestId)); }
  /** 等待只约束原 Goal 的精确授权版本，不阻断人明确修改后的另一版本。 */
  hasWaits(goal: GoalReference) {
    return this.value.waits.some(item => item.continuation.goal?.id === goal.id && item.continuation.goal.revision === goal.revision);
  }
  call(id: string): ToolOccurrence | undefined {
    const call = latestCall(this.value, id);
    return call ? occurrenceOf(call) : undefined;
  }
  callAtSequence(sequence: number): ToolOccurrence | undefined {
    const call = Object.hasOwn(this.value.calls, String(sequence)) ? this.value.calls[String(sequence)] : undefined;
    return call ? occurrenceOf(call) : undefined;
  }
  resultCall(event: SessionEvent<'tool/result' | 'tool/ptc-dispatch'>): ToolOccurrence | undefined {
    const call = callForResult(this.value, event);
    return call ? occurrenceOf(call) : undefined;
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
    return humanQuestionCall(state, message.source.callId).inputs;
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

type CallOccurrence = ReactiveProjectionState['calls'][string];
type CallSource = Pick<ReactiveProjectionState, 'calls'>;
type EnclosedCallSource = Pick<ReactiveProjectionState, 'calls' | 'openTurn' | 'openStep'>;

/** 即时 ToolRunContext 按 callId 寻址；当前执行取得最新真实发生，历史不被覆盖。 */
export function latestCall(source: CallSource, callId: string): CallOccurrence | undefined {
  let latest: CallOccurrence | undefined;
  for (const call of Object.values(source.calls)) if (call.callId === callId && (!latest || call.callSequence > latest.callSequence)) latest = call;
  return latest;
}

/** 原生问题只提供 callId；只有唯一真实提问能签发来源，不能拿后来的同名工具或提问替代。 */
export function humanQuestionCall(source: CallSource, callId: string): CallOccurrence {
  const matches = Object.values(source.calls).filter(call => call.name === 'ask_user_question' && call.callId === callId);
  if (matches.length === 0) throw new Error('REACTIVE_HUMAN_QUESTION_CALL_NOT_FOUND');
  if (matches.length > 1) throw new Error('REACTIVE_HUMAN_QUESTION_ORIGIN_AMBIGUOUS');
  return matches[0]!;
}

export function occurrenceOf(call: CallOccurrence): ToolOccurrence {
  return { callId: call.callId, callSequence: call.callSequence, rootCallId: call.rootCallId,
    parentCallId: call.parentCallId, name: call.name, inputs: call.inputs };
}

function toolActivityOf(call: CallOccurrence): ToolActivity {
  return { ...occurrenceOf(call), state: call.state, observedAt: call.observedAt,
    ...(call.result === undefined ? {} : { result: call.result }) };
}

/** PTC 在父执行结束前排空；日志没有坐标或 sourceSeq，真实 open step 与完整配对身份必须唯一。 */
export function ptcParent(source: EnclosedCallSource,
  data: SessionEvent<'tool/ptc-dispatch-start'>['data']): CallOccurrence {
  const step = source.openStep;
  if (step === null || source.openTurn !== step.turn) throw new Error('REACTIVE_PTC_OPEN_STEP_REQUIRED');
  const parents = Object.values(source.calls).filter(call => call.turn === step.turn && call.step === step.step
    && call.callId === data.parentCallId && call.rootCallId === data.rootCallId);
  if (parents.length !== 1) throw new Error('REACTIVE_PTC_PARENT_OCCURRENCE_REQUIRED');
  return parents[0]!;
}

/** 普通结果引用正式 call seq；未开始的修复结果没有发生，不按同名 ID 拼接。 */
export function callForResult(source: EnclosedCallSource,
  event: SessionEvent<'tool/result' | 'tool/ptc-dispatch'>): CallOccurrence | undefined {
  if (event.type === 'tool/result') {
    if (event.sourceEventSeqs === undefined) return undefined;
    const matches = event.sourceEventSeqs.flatMap(sequence => {
      const call = Object.hasOwn(source.calls, String(sequence)) ? source.calls[String(sequence)] : undefined;
      return call && call.callId === event.data.message.source.callId && call.parentCallId === null
        && call.turn === event.data.turn && call.step === event.data.step ? [call] : [];
    });
    if (matches.length > 1) throw new Error('REACTIVE_TOOL_RESULT_OCCURRENCE_AMBIGUOUS');
    return matches[0];
  }
  const parent = ptcParent(source, event.data);
  const matches = Object.values(source.calls).filter(call => call.turn === parent.turn && call.step === parent.step
    && call.callId === event.data.subCallId && call.rootCallId === event.data.rootCallId
    && call.parentCallId === event.data.parentCallId && call.name === event.data.name);
  if (matches.length !== 1) throw new Error('REACTIVE_PTC_RESULT_OCCURRENCE_REQUIRED');
  return matches[0];
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
