import type { Continuation, GoalActivity, InputReference, Json, WaitReference } from './protocol.ts';
import type { StreamChunk } from '@deepseek-ai/dsh-llm';
import type { AssistantStreamRecord } from '@deepseek-ai/dsh-llm/assistant-stream';
import type { OperationFollowSnapshot } from '../operations/follows.ts';

/** sequence 只对已追加的会话事实有值；瞬态流和驱动状态没有持久序号。 */
export interface EventOrigin { sessionId: string; time: number; sequence: number | null; }
export interface ToolOccurrence<T = Json> {
  callId: string; callSequence: number; rootCallId: string; parentCallId: string | null; name: string;
  inputs: readonly InputReference<T>[];
}
export type AssistantStream =
  | { phase: 'start'; attemptId: string; revision: number; turn: number; step: number }
  | { phase: 'text' | 'reasoning'; attemptId: string; revision: number; index: number; blockIndex: number; text: string }
  | { phase: 'tool'; attemptId: string; revision: number; index: number; blockIndex: number; callId: string; name?: string; argumentsDelta: string }
  | { phase: 'end'; attemptId: string; revision: number; index: number;
      outcome: { kind: 'committed'; eventType: 'assistant/message' | 'assistant/attempt'; seq: number } | { kind: 'abandoned' } };

/** 当前真实 attempt 的 lossless 前缀；开场快照不是一次新的流式事件。 */
export interface AssistantAttemptSnapshot {
  readonly attemptId: string;
  readonly revision: number;
  readonly turn: number;
  readonly step: number;
  readonly inputs: readonly InputReference[];
  readonly nextIndex: number;
  readonly stream: readonly AssistantStreamRecord[];
}
/** 原生流的无品牌传输形态；保留 block、usage 等便利事件未投影的 chunk。 */
export interface AssistantProgressFrame {
  readonly inputs: readonly InputReference[];
  readonly frame:
    | { readonly type: 'start'; readonly attemptId: string; readonly revision: number; readonly turn: number; readonly step: number }
    | { readonly type: 'chunk'; readonly attemptId: string; readonly revision: number; readonly index: number; readonly time: number; readonly chunk: StreamChunk }
    | { readonly type: 'end'; readonly attemptId: string; readonly revision: number; readonly index: number;
        readonly outcome: { readonly kind: 'committed'; readonly eventType: 'assistant/message' | 'assistant/attempt'; readonly seq: number } | { readonly kind: 'abandoned' } };
}

/** 新发生的对话事实。订阅不交付基线，重建会话也不重放历史事件。 */
export type ConversationEvent<T = Json> = EventOrigin & (
  | { kind: 'input.accepted' | 'input.entered'; messageId: string; inputs: readonly InputReference<T>[] }
  | { kind: 'input.progress'; requestId: string; inputs: readonly InputReference<T>[] }
  | { kind: 'operation.follow.changed'; change: 'intent' | 'call-associated' | 'receipt' | 'delivery-prepared' | 'not-started' | 'unregistered'; follow: OperationFollowSnapshot; inputs: readonly InputReference<T>[] }
  | ({ kind: 'tool.started' } & ToolOccurrence<T>)
  | ({ kind: 'tool.invocation'; invocationId: string } & ToolOccurrence<T>)
  | ({ kind: 'tool.settled'; invocationId: string | null; outcome: 'returned' | 'failed'; result?: Json; meta?: Json } & ToolOccurrence<T>)
  | { kind: 'assistant.message'; messageId: string; turn: number; step: number; inputs?: readonly InputReference<T>[]; text: string; interrupted: boolean }
  | { kind: 'assistant.stream'; inputs: readonly InputReference<T>[]; frame: AssistantStream }
  | { kind: 'goal.changed'; goal: GoalActivity | null }
  | { kind: 'goal.activation'; goalId: string; revision: number; activation: 'armed' | 'disarmed'; inputs: readonly InputReference<T>[] }
  | { kind: 'execution.changed'; execution: 'idle' | 'running' }
  | { kind: 'execution.error'; error: unknown; turn: number | null; step: number | null; inputs?: readonly InputReference<T>[] }
  | { kind: 'wait.opened' | 'wait.resolved'; continuation: Continuation; wait: WaitReference }
);

/** 只包含确切关联的事实；会话级运行/Goal 变化不冒充请求独占的工作。 */
export type RequestEvent<T = Json> =
  | Extract<ConversationEvent<T>, { inputs: readonly InputReference<T>[] }>
  | Extract<ConversationEvent<T>, { kind: 'execution.error' | 'assistant.message' }> & { inputs: readonly InputReference<T>[] }
  | Extract<ConversationEvent<T>, { kind: 'wait.opened' | 'wait.resolved' }>;

/** @internal 原始观察取得自有不可变快照，真实 Error 仍保留原失败身份。 */
export function ownConversationEvent(raw: ConversationEvent): ConversationEvent {
  const freeze = (value: unknown): void => {
    if (value === null || typeof value !== 'object') return;
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  };
  if (raw.kind === 'execution.error') {
    const { error, ...metadata } = raw;
    const owned = structuredClone(metadata);
    freeze(owned);
    return Object.freeze({ ...owned, error });
  }
  const owned = structuredClone(raw);
  freeze(owned);
  return owned;
}
