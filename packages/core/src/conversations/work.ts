import { EMPTY, defer, fromEvent, map, merge, takeUntil, filter, type Observable } from 'rxjs';
import type { ConversationIdentity } from './conversation.ts';
import type { Continuation, ConversationActivity, ExecutionFailure, GoalActivity, InputReference, Json, ReplyActivity, RequestActivity, RequestDeclaration, ToolActivity } from './protocol.ts';
import type { AssistantAttemptSnapshot, AssistantProgressFrame, ConversationEvent } from './events.ts';
import type { ConversationProgressSnapshot } from './request.ts';
import type { ModelPreparation } from './preparation.ts';
import type { HumanForegroundQuestion, HumanQuestion, HumanReplyDelivery, HumanSettledQuestion, HumanSnapshot } from '../participation/contract.ts';
import type { Observation } from '../resources/observation.ts';
import { matchesOperationFollowResult, type OperationFollowSnapshot } from '../operations/follows.ts';
import type { ResourceReader, ResourceReadable, ResourceReaderOptions } from '../resources/reader.ts';
import { combineResources, mapResource } from '../resources/operators.ts';
import { shareResource } from '../resources/sharing.ts';

/** 选择依据保留原始输入；声明没有原生输入身份，不能伪造 messageId 或 source。 */
export type WorkOrigin =
  | (InputReference & { readonly kind: 'input'; readonly input: InputReference })
  | (RequestDeclaration & { readonly kind: 'declaration'; readonly sourceSessionId: string;
      readonly requestId: string; readonly messageId: null; readonly source: null; readonly declaration: RequestDeclaration });

export type WorkSourceIdentity = Pick<ConversationIdentity, 'sessionId' | 'workspaceId' | 'environmentId' | 'parentSessionId'>;

/** 只借用已经取得的原始观察能力，不打开会话、解码其他 Program 或取得控制资格。 */
export interface WorkSource {
  readonly identity: WorkSourceIdentity;
  readonly progress: ResourceReadable<ConversationProgressSnapshot>;
  readonly human: ResourceReadable<HumanSnapshot>;
  readonly rawEvents$: Observable<ConversationEvent>;
  readonly progressFrames$: Observable<AssistantProgressFrame>;
}

export interface WorkSourceSnapshot {
  readonly identity: WorkSourceIdentity;
  readonly progress: ConversationProgressSnapshot;
  readonly human: HumanSnapshot;
}

/** value 始终是完整 occurrence；matchedInputs 仅解释哪些输入命中了本次业务选择。 */
export interface WorkOccurrence<T> {
  readonly value: T;
  readonly matchedInputs: readonly InputReference[];
}

export interface WorkRequestOccurrence extends WorkOccurrence<RequestActivity> {
  readonly matchedDeclaration: Extract<WorkOrigin, { kind: 'declaration' }> | null;
}

export type WorkWait = ConversationActivity['waits'][number];

/** 关联准备、受理与消费各自保留原事实；观察不查询业务owner或签发续接权。 */
export interface WorkOperationFollow extends WorkOccurrence<OperationFollowSnapshot> {
  readonly resultRequest: RequestActivity | null;
}

/** 条件只描述原事实，不判定整个业务是否可以继续，也不把执行 busy 当作阻塞。 */
export type WorkCondition =
  | ({ readonly kind: 'preparation.waiting' } & WorkOccurrence<ModelPreparation>)
  | ({ readonly kind: 'execution.failed' } & WorkOccurrence<ExecutionFailure>)
  | ({ readonly kind: 'goal.paused' } & WorkOccurrence<GoalActivity>)
  | ({ readonly kind: 'goal.blocked'; readonly blockedReason: GoalActivity['blockedReason'] } & WorkOccurrence<GoalActivity>)
  | ({ readonly kind: 'goal.round-limit' } & WorkOccurrence<GoalActivity>)
  | ({ readonly kind: 'wait.pending' } & WorkOccurrence<WorkWait>)
  | ({ readonly kind: 'call.failed' } & WorkOccurrence<ToolActivity>)
  | ({ readonly kind: 'question.reply-pending' } & WorkOccurrence<HumanReplyDelivery>)
  | { readonly kind: 'continuation.unconsumed' | 'continuation.superseded' | 'continuation.inactive';
      readonly request: WorkRequestOccurrence; readonly continuation: Continuation;
      readonly currentGoal: GoalActivity | null; readonly matchedInputs: readonly InputReference[] }
  | ({ readonly kind: 'operation.not-started' | 'operation.follow-unregistered' | 'operation.delivery-prepared' } & WorkOperationFollow)
  | { readonly kind: 'question.pending'; readonly callId: HumanQuestion['callId'];
      readonly active: WorkOccurrence<HumanQuestion> | null;
      readonly foreground: WorkOccurrence<HumanForegroundQuestion> | null;
      readonly matchedInputs: readonly InputReference[] };

export interface WorkSessionSnapshot {
  readonly preparation: WorkOccurrence<ModelPreparation> | null;
  readonly identity: WorkSourceIdentity;
  readonly sessionId: string;
  /** 会话背景，不表示该业务独占了整个会话的执行。 */
  readonly source: ConversationActivity['source'];
  readonly live: boolean;
  readonly execution: ConversationActivity['execution'];
  readonly sequence: number;
  readonly requests: readonly WorkRequestOccurrence[];
  readonly inputs: readonly WorkOccurrence<InputReference>[];
  readonly calls: readonly WorkOccurrence<ToolActivity>[];
  readonly replies: readonly WorkOccurrence<ReplyActivity>[];
  readonly failures: readonly WorkOccurrence<ExecutionFailure>[];
  readonly attempts: readonly WorkOccurrence<AssistantAttemptSnapshot>[];
  readonly questions: {
    readonly active: readonly WorkOccurrence<HumanQuestion>[];
    readonly settled: readonly WorkOccurrence<HumanSettledQuestion>[];
    readonly replies: readonly WorkOccurrence<HumanReplyDelivery>[];
  };
  readonly foreground: readonly WorkOccurrence<HumanForegroundQuestion>[];
  readonly waits: readonly WorkOccurrence<WorkWait>[];
  readonly operationFollows: readonly WorkOperationFollow[];
  /** 只包含当前权威 Goal 且其 inputs 明确命中的事实；不沿历史 wait 猜当前 Goal。 */
  readonly goals: readonly WorkOccurrence<GoalActivity>[];
  readonly conditions: readonly WorkCondition[];
}

export interface WorkSnapshot<K extends Json = Json> {
  readonly key: K;
  readonly sessions: readonly WorkSessionSnapshot[];
}

export interface WorkEvent<K extends Json = Json> {
  readonly key: K;
  readonly source: WorkSourceIdentity;
  readonly event: ConversationEvent;
  readonly matchedInputs: readonly InputReference[];
}

export interface WorkProgressFrame<K extends Json = Json> {
  readonly key: K;
  readonly source: WorkSourceIdentity;
  readonly frame: AssistantProgressFrame;
  readonly matchedInputs: readonly InputReference[];
}

export interface WorkHandle<K extends Json = Json> {
  /** 显式来源各有自己的有效性；完整state仍要求全部来源可读。 */
  readonly sessions: readonly WorkSessionHandle<K>[];
  readonly state: ResourceReader<WorkSnapshot<K>>;
  readonly state$: Observable<Observation<WorkSnapshot<K>>>;
  readonly events$: Observable<WorkEvent<K>>;
  readonly progressFrames$: Observable<WorkProgressFrame<K>>;
}

export interface WorkSessionHandle<K extends Json = Json> {
  readonly identity: WorkSourceIdentity;
  readonly state: ResourceReader<WorkSessionSnapshot>;
  readonly state$: Observable<Observation<WorkSessionSnapshot>>;
  readonly events$: Observable<WorkEvent<K>>;
  readonly progressFrames$: Observable<WorkProgressFrame<K>>;
}

export interface WorkSelector<K extends Json> {
  select(key: K, sources: readonly WorkSourceSnapshot[]): WorkSnapshot<K>;
  observe(key: K, sources: readonly WorkSource[], options?: ResourceReaderOptions): WorkHandle<K>;
}

/** 一个业务定义自己的 JSON 身份；undefined 明确表示该原始输入或声明不属于业务。 */
export function createWorkSelector<K extends Json>(options: { readonly identify: (origin: WorkOrigin) => K | undefined }): WorkSelector<K> {
  const identify = options.identify;
  const matcher = (key: K) => {
    const expected = canonicalJson(key);
    const matches = (origin: WorkOrigin): boolean => {
      const selected = identify(origin);
      return selected !== undefined && canonicalJson(selected) === expected;
    };
    const inputs = (values: readonly InputReference[]): readonly InputReference[] => Object.freeze(values.filter(input =>
      matches(Object.freeze({ ...input, kind: 'input' as const, input }))));
    return { matches, inputs };
  };

  const project = (key: K, sources: readonly WorkSourceSnapshot[]): WorkSnapshot<K> => {
    assertDistinctSources(sources);
    const match = matcher(key);
    const sessions = sources.map(({ identity, progress, human }): WorkSessionSnapshot => {
      const { activity, attempts } = progress;
      if (activity.sessionId !== identity.sessionId) throw new Error('REACTIVE_WORK_SESSION_MISMATCH');
      for (const question of [...human.questions.active, ...human.questions.settled, ...human.foreground, ...human.replies]) {
        if (question.origin.sessionId !== identity.sessionId) throw new Error('REACTIVE_WORK_QUESTION_SESSION_MISMATCH');
      }
      for (const wait of activity.waits) {
        if (wait.continuation.sessionId !== identity.sessionId) throw new Error('REACTIVE_WORK_WAIT_SESSION_MISMATCH');
      }
      for (const follow of activity.operationFollows) {
        if (follow.link.continuation.sessionId !== identity.sessionId) throw new Error('REACTIVE_WORK_OPERATION_SESSION_MISMATCH');
      }
      const occurrences = <T>(values: readonly T[], inputOf: (value: T) => readonly InputReference[] | null): readonly WorkOccurrence<T>[] =>
        Object.freeze(values.flatMap(value => {
          const matchedInputs = match.inputs(inputOf(value) ?? []);
          return matchedInputs.length ? [Object.freeze({ value, matchedInputs })] : [];
        }));
      const requests: readonly WorkRequestOccurrence[] = Object.freeze(Object.values(activity.requests).flatMap(value => {
        const declaration = value.declaration === null ? null : Object.freeze({ ...value.declaration,
          kind: 'declaration' as const, sourceSessionId: identity.sessionId, requestId: value.requestId,
          messageId: null, source: null, declaration: value.declaration });
        const matchedDeclaration = declaration !== null && match.matches(declaration) ? declaration : null;
        const matchedInputs = uniqueInputs([...match.inputs(value.inputs), ...match.inputs(value.declaration?.result?.continuation.inputs ?? [])]);
        return matchedDeclaration !== null || matchedInputs.length ? [Object.freeze({ value, matchedInputs, matchedDeclaration })] : [];
      }));
      const inputs = occurrences(activity.inputs, value => [value]);
      const calls = occurrences(activity.calls, value => value.inputs);
      const replies = occurrences(activity.replies, value => value.inputs);
      const failures = occurrences(activity.failures, value => value.inputs);
      const activeAttempts = occurrences(attempts, value => value.inputs);
      const preparation = occurrences(progress.preparation === null ? [] : [progress.preparation], value => value.inputs)[0] ?? null;
      const active = occurrences(human.questions.active, value => value.origin.inputs);
      const settled = occurrences(human.questions.settled, value => value.origin.inputs);
      const humanReplies = occurrences(human.replies, value => value.origin.inputs);
      const foreground = occurrences(human.foreground, value => value.origin.inputs);
      const waits = occurrences(activity.waits, value => value.continuation.inputs);
      const goals = occurrences(activity.goal === null ? [] : [activity.goal], value => value.inputs);
      const operationFollows: readonly WorkOperationFollow[] = Object.freeze(activity.operationFollows.flatMap(value => {
        const matchedInputs = match.inputs(value.link.continuation.inputs);
        if (!matchedInputs.length) return [];
        const resultRequestId = value.preparedDelivery?.requestId ?? 'operation:' + value.link.id;
        const candidate = Object.hasOwn(activity.requests, resultRequestId) ? activity.requests[resultRequestId]! : null;
        const resultRequest = matchesOperationFollowResult(value.link, candidate?.declaration) ? candidate : null;
        return [Object.freeze({ value, matchedInputs, resultRequest })];
      }));
      const conditions: WorkCondition[] = [];
      if (preparation && preparation.value.waiting.length) conditions.push(Object.freeze({ kind: 'preparation.waiting', ...preparation }));
      for (const failure of failures) conditions.push(Object.freeze({ kind: 'execution.failed', ...failure }));
      for (const reply of humanReplies) conditions.push(Object.freeze({ kind: 'question.reply-pending', ...reply }));
      for (const goal of goals) {
        if (goal.value.phase === 'paused') conditions.push(Object.freeze({ kind: 'goal.paused', ...goal }));
        if (goal.value.phase === 'blocked') conditions.push(Object.freeze({ kind: 'goal.blocked', ...goal, blockedReason: goal.value.blockedReason }));
        if (goal.value.phase === 'active' && goal.value.roundsStarted >= goal.value.maxGoalRounds)
          conditions.push(Object.freeze({ kind: 'goal.round-limit', ...goal }));
      }
      for (const request of requests) {
        const result = request.value.declaration?.result;
        if (request.value.state !== 'accepted' || result?.ready !== true) continue;
        const continuation = result.continuation, expectedGoal = continuation.goal;
        conditions.push(Object.freeze({ kind: 'continuation.unconsumed', request, continuation,
          currentGoal: activity.goal, matchedInputs: request.matchedInputs }));
        if (expectedGoal === null) continue;
        const currentGoal = activity.goal;
        if (currentGoal === null || currentGoal.id !== expectedGoal.id || currentGoal.revision !== expectedGoal.revision) {
          conditions.push(Object.freeze({ kind: 'continuation.superseded', request, continuation,
            currentGoal, matchedInputs: request.matchedInputs }));
        } else if (currentGoal.phase !== 'active') {
          conditions.push(Object.freeze({ kind: 'continuation.inactive', request, continuation,
            currentGoal, matchedInputs: request.matchedInputs }));
        }
      }
      for (const wait of waits) conditions.push(Object.freeze({ kind: 'wait.pending', ...wait }));
      for (const follow of operationFollows) {
        if (follow.value.unregistered !== null) conditions.push(Object.freeze({ kind: 'operation.follow-unregistered', ...follow }));
        else if (follow.value.notStarted !== null) conditions.push(Object.freeze({ kind: 'operation.not-started', ...follow }));
        else if (follow.value.preparedDelivery !== null
          && (follow.resultRequest === null || follow.resultRequest.state === 'declared'))
          conditions.push(Object.freeze({ kind: 'operation.delivery-prepared', ...follow }));
      }
      for (const call of calls) if (call.value.state === 'failed') conditions.push(Object.freeze({ kind: 'call.failed', ...call }));
      const unanswered = new Map<HumanQuestion['callId'], { active: WorkOccurrence<HumanQuestion> | null; foreground: WorkOccurrence<HumanForegroundQuestion> | null }>();
      for (const question of active) unanswered.set(question.value.callId, { active: question, foreground: null });
      for (const question of foreground) {
        const current = unanswered.get(question.value.callId);
        unanswered.set(question.value.callId, { active: current?.active ?? null, foreground: question });
      }
      for (const [callId, question] of unanswered) conditions.push(Object.freeze({ kind: 'question.pending', callId, ...question,
        matchedInputs: uniqueInputs([...(question.active?.matchedInputs ?? []), ...(question.foreground?.matchedInputs ?? [])]) }));
      return Object.freeze({ identity: captureIdentity(identity), sessionId: identity.sessionId,
        source: activity.source, live: activity.source === 'live', execution: activity.execution, sequence: activity.sequence,
        requests, inputs, calls, replies, failures, preparation, attempts: activeAttempts, questions: Object.freeze({ active, settled, replies: humanReplies }),
        foreground, waits, operationFollows, goals, conditions: Object.freeze(conditions) });
    });
    return Object.freeze({ key, sessions: Object.freeze(sessions) });
  };

  return Object.freeze({
    select(key: K, sources: readonly WorkSourceSnapshot[]): WorkSnapshot<K> {
      return project(captureKey(key), [...sources]);
    },
    observe(key: K, sources: readonly WorkSource[], readerOptions?: ResourceReaderOptions): WorkHandle<K> {
      const capturedKey = captureKey(key);
      const captured = Object.freeze(sources.map(source => Object.freeze({ identity: captureIdentity(source.identity),
        progress: source.progress, human: source.human, rawEvents$: source.rawEvents$, progressFrames$: source.progressFrames$ })));
      assertDistinctSources(captured);
      const match = matcher(capturedKey);
      const sessions: readonly WorkSessionHandle<K>[] = Object.freeze(captured.map(source => {
        const state = shareResource(combineResources({ progress: source.progress, human: source.human }).pipe(
          mapResource(value => project(capturedKey, [{ identity: source.identity, ...value }]).sessions[0]!),
        ), readerOptions);
        const events$ = source.rawEvents$.pipe(map(event => {
          if (event.sessionId !== source.identity.sessionId) throw new Error('REACTIVE_WORK_EVENT_SESSION_MISMATCH');
          const matchedInputs = match.inputs(eventInputs(event));
          return matchedInputs.length ? Object.freeze({ key: capturedKey, source: source.identity, event, matchedInputs }) : null;
        }), filter((value): value is WorkEvent<K> => value !== null));
        const progressFrames$ = source.progressFrames$.pipe(map(frame => {
          const matchedInputs = match.inputs(frame.inputs);
          return matchedInputs.length ? Object.freeze({ key: capturedKey, source: source.identity, frame, matchedInputs }) : null;
        }), filter((value): value is WorkProgressFrame<K> => value !== null));
        return Object.freeze({ identity: source.identity, state, state$: state.observations$,
          events$: borrowEvents(events$, readerOptions?.signal), progressFrames$: borrowEvents(progressFrames$, readerOptions?.signal) });
      }));
      const state = shareResource(combineResources(Object.fromEntries(sessions.map((source, index) => [String(index), source.state]))).pipe(
        mapResource(value => Object.freeze({ key: capturedKey, sessions: Object.freeze(Object.values(value)) })),
      ), readerOptions);
      return Object.freeze({ sessions, state, state$: state.observations$,
        events$: merge(...sessions.map(session => session.events$)), progressFrames$: merge(...sessions.map(session => session.progressFrames$)) });
    },
  });
}

function eventInputs(event: ConversationEvent): readonly InputReference[] {
  if (event.kind === 'execution.changed') return [];
  if (event.kind === 'goal.changed') return event.goal?.inputs ?? [];
  if (event.kind === 'wait.opened' || event.kind === 'wait.resolved') return event.continuation.inputs;
  return 'inputs' in event ? event.inputs ?? [] : [];
}

function borrowEvents<T>(source: Observable<T>, signal: AbortSignal | undefined): Observable<T> {
  return defer(() => signal?.aborted ? EMPTY : signal === undefined ? source : source.pipe(takeUntil(fromEvent(signal, 'abort'))));
}

function assertDistinctSources(sources: readonly { readonly identity: WorkSourceIdentity }[]): void {
  if (!sources.length) throw new Error('REACTIVE_WORK_SOURCES_REQUIRED');
  const ids = new Set<string>();
  for (const source of sources) {
    const { sessionId } = source.identity;
    if (!sessionId.trim()) throw new Error('REACTIVE_WORK_SESSION_ID_REQUIRED');
    if (ids.has(sessionId)) throw new Error('REACTIVE_WORK_SOURCE_DUPLICATE');
    ids.add(sessionId);
  }
}

function captureIdentity(identity: WorkSourceIdentity): WorkSourceIdentity {
  return Object.freeze({ sessionId: identity.sessionId, workspaceId: identity.workspaceId,
    environmentId: identity.environmentId, parentSessionId: identity.parentSessionId });
}

function uniqueInputs(inputs: readonly InputReference[]): readonly InputReference[] {
  const seen = new Set<string>();
  return Object.freeze(inputs.filter(input => {
    const identity = canonicalJson(input);
    if (seen.has(identity)) return false;
    seen.add(identity);
    return true;
  }));
}

/** JSON 值语义比较；拒绝非 JSON、环、稀疏数组和被 JSON.stringify 静默丢弃的成员。 */
function canonicalJson(value: unknown): string {
  const ancestors = new Set<object>();
  const encode = (item: unknown): string => {
    if (item === null || typeof item === 'string' || typeof item === 'boolean') return JSON.stringify(item);
    if (typeof item === 'number' && Number.isFinite(item)) return JSON.stringify(item);
    if (typeof item !== 'object' || item === null || ancestors.has(item)) throw new Error('REACTIVE_WORK_JSON_KEY_REQUIRED');
    ancestors.add(item);
    try {
      if (Array.isArray(item)) {
        if (Reflect.ownKeys(item).length !== item.length + 1) throw new Error('REACTIVE_WORK_JSON_KEY_REQUIRED');
        const values: string[] = [];
        for (let index = 0; index < item.length; index++) {
          if (!Object.hasOwn(item, index)) throw new Error('REACTIVE_WORK_JSON_KEY_REQUIRED');
          values.push(encode(item[index]));
        }
        return `[${values.join(',')}]`;
      }
      const prototype: unknown = Object.getPrototypeOf(item);
      if (prototype !== Object.prototype && prototype !== null) throw new Error('REACTIVE_WORK_JSON_KEY_REQUIRED');
      const keys = Object.keys(item).sort();
      if (Reflect.ownKeys(item).length !== keys.length) throw new Error('REACTIVE_WORK_JSON_KEY_REQUIRED');
      return `{${keys.map(key => {
        const descriptor = Object.getOwnPropertyDescriptor(item, key)!;
        if (!('value' in descriptor)) throw new Error('REACTIVE_WORK_JSON_KEY_REQUIRED');
        return `${JSON.stringify(key)}:${encode(descriptor.value)}`;
      }).join(',')}}`;
    } finally { ancestors.delete(item); }
  };
  return encode(value);
}

function captureKey<K extends Json>(key: K): K {
  const owned = JSON.parse(canonicalJson(key)) as K;
  const freeze = (value: Json): void => {
    if (value === null || typeof value !== 'object') return;
    for (const child of Object.values<Json>(value)) freeze(child);
    Object.freeze(value);
  };
  freeze(owned);
  return owned;
}
