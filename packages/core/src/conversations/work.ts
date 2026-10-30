import { EMPTY, defer, fromEvent, map, merge, takeUntil, filter, of, combineLatest, catchError, tap, finalize, ReplaySubject, shareReplay, observeOn, queueScheduler, throwIfEmpty, type Observable } from 'rxjs';
import { actionInvocationOriginSchema, type ActionInvocationOrigin, type InvocationActivity } from '../actions/contract.ts';
import { resourceObservations, type ResourceSource } from '../resources/source.ts';
import type { ConversationIdentity } from './conversation.ts';
import type { Continuation, ConversationActivity, ExecutionFailure, GoalActivity, InputReference, Json, ReplyActivity, RequestActivity, RequestDeclaration, ToolActivity } from './protocol.ts';
import type { AssistantAttemptSnapshot, AssistantProgressFrame, ConversationEvent } from './events.ts';
import type { ConversationProgressSnapshot } from './request.ts';
import type { ModelPreparation, ModelPreparationAdmission } from './preparation.ts';
import type { HumanForegroundQuestion, HumanQuestion, HumanReplyDelivery, HumanSettledQuestion, HumanSnapshot } from '../participation/contract.ts';
import type { Observation } from '../resources/observation.ts';
import { matchesOperationFollowResult, type OperationFollowSnapshot } from '../operations/follows.ts';
import type { ResourceReader, ResourceReadable, ResourceReaderOptions } from '../resources/reader.ts';
import { combineResources, mapResource, switchResource } from '../resources/operators.ts';
import { shareResource } from '../resources/sharing.ts';

/** 选择依据保留原始输入；声明没有原生输入身份，不能伪造 messageId 或 source。 */
export type WorkOrigin =
  | { readonly kind: 'invocation'; readonly id: string; readonly origin: ActionInvocationOrigin }
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
  | ({ readonly kind: 'question.reply-pending'; readonly admission: ModelPreparationAdmission | null } & WorkOccurrence<HumanReplyDelivery>)
  | ({ readonly kind: 'question.reply-blocked'; readonly reason: 'unavailable' | 'superseded' | 'inactive';
      readonly currentGoal: GoalActivity | null; readonly admission: ModelPreparationAdmission | null } & WorkOccurrence<HumanReplyDelivery>)
  | { readonly kind: 'continuation.unconsumed' | 'continuation.superseded' | 'continuation.inactive';
      readonly request: WorkRequestOccurrence; readonly continuation: Continuation;
      readonly admission: ModelPreparationAdmission | null;
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
  readonly invocations: readonly WorkInvocationSnapshot<K>[];
}

/** 已持有调用的纯观察能力，不要求结果、控制权或业务 owner。 */
export interface WorkInvocationSource {
  readonly id: string;
  readonly origin: ActionInvocationOrigin;
  readonly activity: ResourceReadable<InvocationActivity>;
}
export interface WorkInvocationObservation {
  readonly id: string;
  readonly origin: ActionInvocationOrigin;
  readonly activity: Observation<InvocationActivity>;
}
export type WorkInvocationInput<K extends Json, T = WorkInvocationSource> = T | { readonly invocation: T; readonly key: K };
export interface WorkInvocationSnapshot<K extends Json = Json> extends WorkInvocationObservation {
  readonly matchedInputs: readonly InputReference[];
  readonly keys: readonly K[];
}
export interface WorkSources<K extends Json> {
  readonly sessions?: readonly WorkSource[];
  readonly invocations?: readonly WorkInvocationInput<K>[] | ResourceSource<readonly WorkInvocationInput<K>[]>;
}
export interface WorkSelectionSources<K extends Json> {
  readonly sessions?: readonly WorkSourceSnapshot[];
  readonly invocations?: readonly WorkInvocationInput<K, WorkInvocationObservation>[];
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
  select(key: K, sources: WorkSelectionSources<K>): WorkSnapshot<K>;
  observe(key: K, sources: WorkSources<K>, options?: ResourceReaderOptions): WorkHandle<K>;
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
      const admissions = progress.preparation?.admissions ?? [];
      for (const admission of admissions) {
        if (admission.continuation.sessionId !== identity.sessionId) throw new Error('REACTIVE_WORK_PREPARATION_SESSION_MISMATCH');
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
      for (const reply of humanReplies) {
        const continuation = reply.value.origin.continuation;
        const admission = continuation === null ? null : admissions.find(value => value.source.kind === 'human-reply'
          && value.source.messageId === reply.value.messageId && value.source.questionCallId === reply.value.questionCallId
          && value.source.callSequence === reply.value.origin.callSequence
          && canonicalJson(value.continuation) === canonicalJson(continuation)) ?? null;
        conditions.push(Object.freeze({ kind: 'question.reply-pending', ...reply, admission }));
        const currentGoal = activity.goal;
        if (continuation === null) {
          conditions.push(Object.freeze({ kind: 'question.reply-blocked', ...reply, reason: 'unavailable', currentGoal, admission }));
        } else {
          const expectedGoal = admission === null ? continuation.goal : admission.goal;
          if (expectedGoal === null) continue;
          if (currentGoal === null || currentGoal.id !== expectedGoal.id || currentGoal.revision !== expectedGoal.revision) {
            conditions.push(Object.freeze({ kind: 'question.reply-blocked', ...reply, reason: 'superseded', currentGoal, admission }));
          } else if (currentGoal.phase !== 'active') {
            conditions.push(Object.freeze({ kind: 'question.reply-blocked', ...reply, reason: 'inactive', currentGoal, admission }));
          }
        }
      }
      for (const goal of goals) {
        if (goal.value.phase === 'paused') conditions.push(Object.freeze({ kind: 'goal.paused', ...goal }));
        if (goal.value.phase === 'blocked') conditions.push(Object.freeze({ kind: 'goal.blocked', ...goal, blockedReason: goal.value.blockedReason }));
        if (goal.value.phase === 'active' && goal.value.roundsStarted >= goal.value.maxGoalRounds)
          conditions.push(Object.freeze({ kind: 'goal.round-limit', ...goal }));
      }
      for (const request of requests) {
        const result = request.value.declaration?.result;
        if (request.value.state !== 'accepted' || result?.ready !== true) continue;
        const continuation = result.continuation;
        const admission = admissions.find(value => {
          const source = value.source;
          return source.kind === 'result' && source.requestId === request.value.requestId
            && request.value.inputs.some(input => input.sourceSessionId === identity.sessionId
              && input.requestId === source.requestId && input.messageId === source.messageId)
            && canonicalJson(value.continuation) === canonicalJson(continuation);
        }) ?? null;
        const expectedGoal = admission === null ? continuation.goal : admission.goal;
        conditions.push(Object.freeze({ kind: 'continuation.unconsumed', request, continuation,
          currentGoal: activity.goal, matchedInputs: request.matchedInputs, admission }));
        if (expectedGoal === null) continue;
        const currentGoal = activity.goal;
        if (currentGoal === null || currentGoal.id !== expectedGoal.id || currentGoal.revision !== expectedGoal.revision) {
          conditions.push(Object.freeze({ kind: 'continuation.superseded', request, continuation,
            currentGoal, matchedInputs: request.matchedInputs, admission }));
        } else if (currentGoal.phase !== 'active') {
          conditions.push(Object.freeze({ kind: 'continuation.inactive', request, continuation,
            currentGoal, matchedInputs: request.matchedInputs, admission }));
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
    return Object.freeze({ key, sessions: Object.freeze(sessions), invocations: Object.freeze([]) });
  };

  return Object.freeze({
    select(key: K, sources: WorkSelectionSources<K>): WorkSnapshot<K> {
      const capturedKey = captureKey(key);
      const groups = invocationGroups<K, Observation<InvocationActivity>>(capturedKey, sources.invocations ?? [], matcher(capturedKey));
      return Object.freeze({ ...project(capturedKey, sources.sessions ?? []), invocations: Object.freeze(groups.map(({ reader, ...group }) =>
        Object.freeze({ ...group, activity: captureObservation(reader.activity) }))) });
    },
    observe(key: K, sources: WorkSources<K>, readerOptions?: ResourceReaderOptions): WorkHandle<K> {
      const capturedKey = captureKey(key);
      const captured = Object.freeze((sources.sessions ?? []).map(source => Object.freeze({ identity: captureIdentity(source.identity),
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
      const sessionState = sessions.length ? combineResources(Object.fromEntries(sessions.map((source, index) => [String(index), source.state]))).pipe(
        mapResource(value => Object.freeze(Object.values(value)))) : readyResource<readonly WorkSessionSnapshot[]>(Object.freeze([]));
      const invocationInputs = sources.invocations;
      const members = invocationInputs === undefined || Array.isArray(invocationInputs)
        ? readyResource(captureInvocations<K, ResourceReadable<InvocationActivity>>((invocationInputs ?? []) as readonly WorkInvocationInput<K>[]))
        : resourceObservations(invocationInputs as ResourceSource<readonly WorkInvocationInput<K>[]>).pipe(mapResource(value => captureInvocations<K, ResourceReadable<InvocationActivity>>(value)));
      const invocationState = defer(() => {
        // 仅在本次观察连接内持有选择和连接；调用事实仍来自原 activity。
        const selections = new Map<string, unknown>();
        const connections = new Map<string, { activity: ResourceReadable<InvocationActivity>;
          stop: ReplaySubject<void>; observations$: Observable<Observation<InvocationActivity>> }>();
        const release = (identity: string) => {
          const connection = connections.get(identity);
          connections.delete(identity);
          connection?.stop.next();
          connection?.stop.complete();
        };
        const releaseAll = () => {
          const released = [...connections.values()];
          connections.clear();
          for (const connection of released) {
            connection.stop.next();
            connection.stop.complete();
          }
        };
        return members.pipe(
          // 成员更新和清理属于同一同步队列；teardown 重入的新事实在当前更新之后处理。
          observeOn(queueScheduler),
          mapResource(values => invocationGroups<K, ResourceReadable<InvocationActivity>>(capturedKey, values, match, selections)),
          tap(observation => { if (observation.status !== 'ready') releaseAll(); }),
          switchResource(groups => {
            const identities = new Set(groups.map(group => invocationIdentity(group)));
            for (const identity of [...connections.keys()]) if (!identities.has(identity)) release(identity);
            selections.clear();
            for (const group of groups) selections.set(invocationIdentity(group), memberIdentity(group.reader));
            if (!groups.length) return readyResource<readonly WorkInvocationSnapshot<K>[]>(Object.freeze([]));
            const observations = groups.map(({ reader, ...group }) => {
              const identity = invocationIdentity(group);
              let connection = connections.get(identity);
              if (connection && connection.activity !== reader.activity) { release(identity); connection = undefined; }
              if (!connection) {
                const stop = new ReplaySubject<void>(1);
                const observations$ = reader.activity.observations$.pipe(
                  throwIfEmpty(),
                  catchError((error: unknown) => of<Observation<InvocationActivity>>(Object.freeze({ status: 'error', generation: 0, error }))),
                  takeUntil(stop), shareReplay({ bufferSize: 1, refCount: false }));
                connection = { activity: reader.activity, stop, observations$ };
                connections.set(identity, connection);
              }
              return connection.observations$.pipe(map(activity => Object.freeze({ ...group, activity: captureObservation(activity) })));
            });
            return combineLatest(observations).pipe(map(value => Object.freeze({ status: 'ready' as const, generation: 0,
              observedAt: Date.now(), value: Object.freeze(value) })));
          }),
          finalize(() => { releaseAll(); selections.clear(); }),
        );
      });
      const state = shareResource(combineResources({ sessions: sessionState, invocations: invocationState }).pipe(
        mapResource(value => Object.freeze({ key: capturedKey, ...value })),
      ), readerOptions);
      return Object.freeze({ sessions, state, state$: state.observations$,
        events$: merge(...sessions.map(session => session.events$)), progressFrames$: merge(...sessions.map(session => session.progressFrames$)) });
    },
  });
}

function readyResource<T>(value: T): Observable<Observation<T>> {
  return of(Object.freeze({ status: 'ready' as const, generation: 0, observedAt: Date.now(), value }));
}

function captureObservation<T>(value: Observation<T>): Observation<T> {
  return Object.freeze({ ...value });
}

const invocationMemberIdentity = Symbol('work.invocation.member');

interface CapturedInvocation<Activity> {
  readonly [invocationMemberIdentity]: unknown;
  readonly id: string;
  readonly origin: ActionInvocationOrigin;
  readonly activity: Activity;
}

function memberIdentity(source: object): unknown {
  return (source as { readonly [invocationMemberIdentity]?: unknown })[invocationMemberIdentity] ?? source;
}

function invocationIdentity(source: { readonly id: string; readonly origin: ActionInvocationOrigin }): string {
  return canonicalJson([source.origin.sessionId, source.origin.bindingId, source.id]);
}

function captureInvocations<K extends Json, Activity>(
  values: readonly WorkInvocationInput<K, Pick<CapturedInvocation<Activity>, 'id' | 'origin' | 'activity'>>[],
): readonly WorkInvocationInput<K, CapturedInvocation<Activity>>[] {
  return Object.freeze(values.map(value => {
    const explicit = 'invocation' in value;
    const source = explicit ? value.invocation : value;
    const invocation = Object.freeze({ [invocationMemberIdentity]: memberIdentity(source), id: source.id, activity: source.activity,
      origin: actionInvocationOriginSchema.parse(source.origin) });
    return explicit ? Object.freeze({ invocation, key: captureKey(value.key) }) : invocation;
  }));
}

/** 先验证全部别名的事实身份，再为当前业务合并命中；状态只读取首个命中成员。 */
function invocationGroups<K extends Json, Activity>(
  key: K, values: readonly WorkInvocationInput<K, Pick<CapturedInvocation<Activity>, 'id' | 'origin' | 'activity'>>[],
  match: { matches(origin: WorkOrigin): boolean; inputs(values: readonly InputReference[]): readonly InputReference[] },
  previousSelections?: ReadonlyMap<string, unknown>,
): readonly { readonly id: string; readonly origin: ActionInvocationOrigin; readonly reader: CapturedInvocation<Activity>;
  readonly matchedInputs: readonly InputReference[]; readonly keys: readonly K[] }[] {
  const captured = captureInvocations<K, Activity>(values);
  const groups = new Map<string, { origin: ActionInvocationOrigin; members: WorkInvocationInput<K, CapturedInvocation<Activity>>[] }>();
  for (const value of captured) {
    const source = 'invocation' in value ? value.invocation : value;
    if (!source.id.trim()) throw new Error('REACTIVE_WORK_INVOCATION_ID_REQUIRED');
    const identity = canonicalJson([source.origin.sessionId, source.origin.bindingId, source.id]);
    const previous = groups.get(identity);
    if (previous) {
      if (canonicalJson(previous.origin) !== canonicalJson(source.origin)) throw new Error('REACTIVE_WORK_INVOCATION_ORIGIN_CONFLICT');
      previous.members.push(value);
    } else groups.set(identity, { origin: source.origin, members: [value] });
  }
  const selected = [];
  for (const [identity, { origin, members }] of groups) {
    const matchedInputs = match.inputs(origin.inputs);
    let reader: CapturedInvocation<Activity> | undefined;
    const keys: K[] = [];
    for (const value of members) {
      const explicit = 'invocation' in value;
      const source = explicit ? value.invocation : value;
      const keyMatches = explicit && canonicalJson(value.key) === canonicalJson(key);
      const matches = matchedInputs.length > 0 || keyMatches || (!explicit && match.matches({ kind: 'invocation', id: source.id, origin }));
      if (!matches) continue;
      reader ??= source;
      if (previousSelections?.get(identity) === memberIdentity(source)) reader = source;
      if (keyMatches && !keys.some(current => canonicalJson(current) === canonicalJson(value.key))) keys.push(value.key);
    }
    if (reader) selected.push(Object.freeze({ id: reader.id, origin, reader,
      matchedInputs: uniqueInputs(matchedInputs), keys: Object.freeze(keys) }));
  }
  return Object.freeze(selected);
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
