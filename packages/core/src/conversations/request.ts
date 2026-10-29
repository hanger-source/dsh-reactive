import { defer, EMPTY, filter, fromEvent, map, takeUntil, type Observable } from 'rxjs';
import { z } from 'zod';
import { AssistantStreamAccumulator, expandAssistantStream, type AssistantStreamRecord } from '@deepseek-ai/dsh-llm/assistant-stream';
import { inputReferenceSchema, ownConversationActivity, type ConversationActivity, type Json, type RequestActivity } from './protocol.ts';
import type { AssistantAttemptSnapshot, AssistantProgressFrame, ConversationEvent, RequestEvent } from './events.ts';
import type { Observation } from '../resources/observation.ts';
import { ResourceReader, type ResourceReadable, type ResourceReaderOptions } from '../resources/reader.ts';
import { ownModelPreparation, type ModelPreparation } from './preparation.ts';

/** 当前持久事实与真实活跃前缀；冷读取的 attempts 为空，不复活草稿。 */
export interface ConversationProgressSnapshot {
  readonly preparation: ModelPreparation | null;
  readonly activity: ConversationActivity;
  readonly attempts: readonly AssistantAttemptSnapshot[];
}
export interface RequestSnapshot {
  readonly preparation: ModelPreparation | null;
  readonly sessionId: string;
  readonly requestId: string;
  readonly source: ConversationActivity['source'];
  /** 会话的运行状态，不表示这个请求独占一次执行。 */
  readonly execution: ConversationActivity['execution'];
  readonly sequence: number;
  readonly activity: RequestActivity | null;
  readonly attempts: readonly AssistantAttemptSnapshot[];
}
/** 观察不投递输入；settled 是本轮事实，不会结束这个读取来源。 */
export interface RequestHandle<T = Json> {
  readonly sessionId: string;
  readonly requestId: string;
  readonly state: ResourceReader<RequestSnapshot>;
  readonly state$: Observable<Observation<RequestSnapshot>>;
  readonly events$: Observable<RequestEvent<T>>;
}

/** 同一选择器供本地和远程使用；借用会话来源，读取 owner 不投递或控制工作。 */
export function createRequestHandle<T>(sessionId: string, requestId: string,
  progress: ResourceReadable<ConversationProgressSnapshot>, events$: Observable<ConversationEvent<T>>,
  options: ResourceReaderOptions): RequestHandle<T> {
  if (!sessionId.trim() || !requestId.trim()) throw new Error('REACTIVE_REQUEST_ID_REQUIRED');
  const { signal } = options;
  // 父资源已经拥有当前观察；只按该观察身份物化一次选择结果，不另存请求阶段或连接状态。
  let cached: { source: Observation<ConversationProgressSnapshot>; snapshot: Observation<RequestSnapshot> } | undefined;
  const select = (source: Observation<ConversationProgressSnapshot>): Observation<RequestSnapshot> => {
    if (source.status !== 'ready') return source;
    if (cached?.source === source) return cached.snapshot;
    let snapshot: Observation<RequestSnapshot>;
    try {
      const { activity, attempts, preparation } = source.value;
      if (activity.sessionId !== sessionId) throw new Error('REACTIVE_REQUEST_SESSION_MISMATCH');
      snapshot = Object.freeze({ ...source, value: Object.freeze({ sessionId, requestId, source: activity.source,
        execution: activity.execution, sequence: activity.sequence,
        activity: Object.hasOwn(activity.requests, requestId) ? activity.requests[requestId]! : null,
        preparation: preparation?.inputs.some(input => input.sourceSessionId === sessionId && input.requestId === requestId) ? preparation : null,
        attempts: Object.freeze(attempts.filter(attempt => attempt.inputs.some(input =>
          input.sourceSessionId === sessionId && input.requestId === requestId))),
      }) });
    } catch (error) { snapshot = Object.freeze({ status: 'error', generation: source.generation, error }); }
    cached = { source, snapshot };
    return snapshot;
  };
  const state = new ResourceReader({ observations$: progress.observations$.pipe(map(select)), peek: () => select(progress.peek()) }, options);
  return Object.freeze({ sessionId, requestId, state, state$: state.observations$,
    events$: defer(() => signal.aborted ? EMPTY : events$.pipe(filter((event): event is RequestEvent<T> => {
      if (event.sessionId !== sessionId) return false;
      if ('inputs' in event && event.inputs?.some(input => input.sourceSessionId === sessionId && input.requestId === requestId)) return true;
      if (event.kind === 'input.progress') return event.requestId === requestId;
      return (event.kind === 'wait.opened' || event.kind === 'wait.resolved')
        && event.continuation.inputs.some(input => input.sourceSessionId === sessionId && input.requestId === requestId);
    }), takeUntil(fromEvent(signal, 'abort')))),
  });
}

const integer = z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER);
const identity = z.string().min(1);
const inputs = inputReferenceSchema.array().readonly();
const outcome = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('committed'), eventType: z.enum(['assistant/message', 'assistant/attempt']), seq: integer }).strict(),
  z.object({ kind: z.literal('abandoned') }).strict(),
]);
const attemptSchema = z.object({ attemptId: identity, revision: integer, turn: integer, step: integer,
  inputs, nextIndex: integer, stream: z.json().array() }).strict();
const frameSchema = z.object({ inputs, frame: z.discriminatedUnion('type', [
  z.object({ type: z.literal('start'), attemptId: identity, revision: integer, turn: integer, step: integer }).strict(),
  z.object({ type: z.literal('chunk'), attemptId: identity, revision: integer, index: integer,
    time: z.number().int().min(Number.MIN_SAFE_INTEGER).max(Number.MAX_SAFE_INTEGER), chunk: z.json() }).strict(),
  z.object({ type: z.literal('end'), attemptId: identity, revision: integer, index: integer, outcome }).strict(),
]) }).strict();

/** wire 边界委托原生 reader 校验 lossless compact 合同，不复制 chunk schema。 */
export function parseAssistantAttempt(raw: unknown): AssistantAttemptSnapshot {
  const value = attemptSchema.parse(raw);
  const candidate: unknown = value.stream;
  // 原生 expand 是针对不可信持久记录的验证入口；这个断言只恢复它要求的静态输入类型。
  const members = expandAssistantStream(candidate as readonly AssistantStreamRecord[]);
  if (members.length !== value.nextIndex) throw new Error('REACTIVE_ASSISTANT_PREFIX_INDEX_MISMATCH');
  const accumulator = new AssistantStreamAccumulator();
  for (const member of members) accumulator.push(member);
  return Object.freeze({ ...value, inputs: ownInputs(value.inputs), stream: accumulator.snapshot() });
}
export function parseConversationProgress(raw: unknown): ConversationProgressSnapshot {
  const value = z.object({ activity: z.unknown(), attempts: z.unknown().array(), preparation: z.unknown().refine(value => value !== undefined) }).strict().parse(raw);
  return Object.freeze({ activity: ownConversationActivity(value.activity), attempts: Object.freeze(value.attempts.map(parseAssistantAttempt)),
    preparation: value.preparation === null ? null : ownModelPreparation(value.preparation) });
}
export function parseAssistantProgressFrame(raw: unknown): AssistantProgressFrame {
  const value = frameSchema.parse(raw), frame = value.frame;
  if (frame.type !== 'chunk') return Object.freeze({ inputs: ownInputs(value.inputs), frame: Object.freeze(frame) });
  const candidate: unknown = [{ type: 'chunk', time: frame.time, chunk: frame.chunk }];
  // 与 compact 开场相同，由官方 reader 验证并取得 detached 原生 chunk。
  const [member] = expandAssistantStream(candidate as readonly AssistantStreamRecord[]);
  if (!member) throw new Error('REACTIVE_ASSISTANT_CHUNK_REQUIRED');
  const accumulator = new AssistantStreamAccumulator();
  const owned = accumulator.push(member);
  return Object.freeze({ inputs: ownInputs(value.inputs), frame: Object.freeze({ ...frame, chunk: owned.chunk }) });
}

function ownInputs(raw: unknown) {
  const value = inputs.parse(raw);
  const freezeJson = (item: Json): void => {
    if (item === null || typeof item !== 'object') return;
    for (const child of Object.values<Json>(item)) freezeJson(child);
    Object.freeze(item);
  };
  for (const input of value) { freezeJson(input.scope); Object.freeze(input); }
  return Object.freeze(value);
}

function canonical(value: unknown): string {
  return JSON.stringify(value, (_key, item: unknown) => item !== null && typeof item === 'object' && !Array.isArray(item)
    ? Object.fromEntries(Object.entries(item).sort(([left], [right]) => left.localeCompare(right))) : item);
}

export interface AssistantProgress {
  replace(attempts: readonly AssistantAttemptSnapshot[]): void;
  accept(value: AssistantProgressFrame): AssistantProgressFrame;
  snapshot(): readonly AssistantAttemptSnapshot[];
}
type ActiveAttempt = {
  readonly started: Omit<AssistantAttemptSnapshot, 'revision' | 'nextIndex' | 'stream'>;
  readonly accumulator: AssistantStreamAccumulator;
  revision: number;
  nextIndex: number;
  snapshot?: AssistantAttemptSnapshot;
};

/** 原生 accumulator 的身份接点；只接收已按顺序交付的帧，不重放、修 gap 或驱动模型。 */
export function createAssistantProgress(initial: readonly AssistantAttemptSnapshot[] = []): AssistantProgress {
  let active = new Map<string, ActiveAttempt>();
  let cached: readonly AssistantAttemptSnapshot[] | undefined;
  const replace = (attempts: readonly AssistantAttemptSnapshot[]) => {
    const next = new Map<string, ActiveAttempt>();
    for (const attempt of attempts) {
      if (next.has(attempt.attemptId)) throw new Error('REACTIVE_ASSISTANT_ATTEMPT_DUPLICATE');
      const accumulator = new AssistantStreamAccumulator();
      const members = expandAssistantStream(attempt.stream);
      if (members.length !== attempt.nextIndex) throw new Error('REACTIVE_ASSISTANT_PREFIX_INDEX_MISMATCH');
      for (const member of members) accumulator.push(member);
      next.set(attempt.attemptId, { started: { attemptId: attempt.attemptId, turn: attempt.turn, step: attempt.step, inputs: ownInputs(attempt.inputs) },
        accumulator, revision: attempt.revision, nextIndex: attempt.nextIndex });
    }
    active = next; cached = undefined;
  };
  replace(initial);
  return Object.freeze({ replace,
    accept(value: AssistantProgressFrame) {
      const frame = value.frame;
      let owned: AssistantProgressFrame;
      if (frame.type === 'start') {
        if (active.has(frame.attemptId)) throw new Error('REACTIVE_ASSISTANT_ATTEMPT_DUPLICATE');
        const captured = ownInputs(value.inputs);
        active.set(frame.attemptId, { started: { attemptId: frame.attemptId, turn: frame.turn, step: frame.step, inputs: captured },
          accumulator: new AssistantStreamAccumulator(), revision: frame.revision, nextIndex: 0 });
        owned = Object.freeze({ inputs: captured, frame: Object.freeze({ ...frame }) });
      } else {
        const current = active.get(frame.attemptId);
        if (!current) throw new Error('REACTIVE_ASSISTANT_ATTEMPT_NOT_FOUND');
        if (frame.revision <= current.revision || frame.index !== current.nextIndex)
          throw new Error('REACTIVE_ASSISTANT_FRAME_ORDER_INVALID');
        if (current.started.inputs !== value.inputs && canonical(current.started.inputs) !== canonical(value.inputs))
          throw new Error('REACTIVE_ASSISTANT_INPUTS_CONFLICT');
        if (frame.type === 'chunk') {
          const member = current.accumulator.push({ time: frame.time, chunk: frame.chunk });
          current.nextIndex++; current.revision = frame.revision; current.snapshot = undefined;
          owned = Object.freeze({ inputs: current.started.inputs, frame: Object.freeze({ ...frame, chunk: member.chunk }) });
        } else {
          active.delete(frame.attemptId);
          owned = Object.freeze({ inputs: current.started.inputs,
            frame: Object.freeze({ ...frame, outcome: Object.freeze({ ...frame.outcome }) }) });
        }
      }
      cached = undefined;
      return owned;
    },
    snapshot() {
      return cached ??= Object.freeze([...active.values()].map(value => value.snapshot ??= Object.freeze({ ...value.started,
        revision: value.revision, nextIndex: value.nextIndex, stream: value.accumulator.snapshot() })));
    },
  });
}
