import { revokeConversationAccess, type ClientScope } from './scope.ts';
import { RemoteSnapshotStream } from '@deepseek-ai/dsh-api-gateway/client';
import { BehaviorSubject, Subject, defer, EMPTY, fromEvent, map, mergeAll, of, shareReplay, takeUntil, type Observable } from 'rxjs';
import { ResourceReader, createAssistantProgress, createRequestHandle, decodeProgramEvent, ownConversationEvent, parseAssistantProgressFrame, parseConversationProgress, type AssistantProgressFrame, type ConversationActivity, type ConversationEvent, type ConversationProgressSnapshot, type Observation, type RequestHandle, type ProgramContract, type ProgramParsedInput } from '@hanger-source/dsh-reactive';
import type { ProgramSessionAddress, ProgramProgressFrame } from './contracts/types.ts';
import { decodeProgramFailure } from './contracts/errors.ts';

export interface RemoteProgress<P extends ProgramContract> {
  readonly progress: ResourceReader<ConversationProgressSnapshot>;
  readonly progress$: Observable<Observation<ConversationProgressSnapshot>>;
  readonly progressFrames$: Observable<AssistantProgressFrame>;
  readonly events$: Observable<ConversationEvent<ProgramParsedInput<P>>>;
  readonly rawEvents$: Observable<ConversationEvent>;
  request(id: string): RequestHandle<ProgramParsedInput<P>>;
}

/** 会话拥有一个连接；request 只选择 core 的当前事实，不重折日志或重放历史。 */
export function connectProgress<P extends ProgramContract>(owner: ClientScope, address: ProgramSessionAddress, program: P): RemoteProgress<P> {
  const { sessionId } = address;
  const { context: ctx, signal: lifetime } = owner;
  const assistant = createAssistantProgress();
  const current = new BehaviorSubject<Observation<ConversationActivity>>({ status: 'pending', generation: 0 });
  const facts = new Subject<{ raw$: Observable<ConversationEvent>; typed$: Observable<ConversationEvent<ProgramParsedInput<P>>> }>();
  const rawEvents = facts.pipe(map(fact => fact.raw$), mergeAll());
  const events = facts.pipe(map(fact => fact.typed$), mergeAll());
  const progressFrames = new Subject<AssistantProgressFrame>();
  const requests = new Map<string, RequestHandle<ProgramParsedInput<P>>>();
  let terminated = false;
  // 累计 prefix 只在真正的 progress/state 消费者读取时物化，helper 缓存该 revision。
  let cached: { source: Observation<ConversationActivity>; snapshot: Observation<ConversationProgressSnapshot> } | undefined;
  const snapshot = (): Observation<ConversationProgressSnapshot> => {
    const source = current.value;
    if (source.status !== 'ready') return source;
    if (cached?.source === source) return cached.snapshot;
    const snapshot = Object.freeze({ ...source, value: Object.freeze({ activity: source.value, attempts: assistant.snapshot() }) });
    cached = { source, snapshot };
    return snapshot;
  };
  const progress = new ResourceReader({ observations$: current.pipe(map(snapshot)), peek: snapshot }, { signal: lifetime });
  const invalidate = (): void => {
    if (lifetime.aborted || terminated) return;
    assistant.replace([]);
    current.next({ status: 'pending', generation: current.value.generation + 1 });
  };
  const ready = (activity: ConversationActivity): void => {
    if (activity.sessionId !== sessionId) throw new Error('REACTIVE_REQUEST_SESSION_MISMATCH');
    current.next({ status: 'ready', generation: current.value.generation + 1, observedAt: Date.now(), value: activity });
  };
  const lease = owner.acquire(() => {
    const stream = new RemoteSnapshotStream<Extract<ProgramProgressFrame, { type: 'baseline' }>, Exclude<ProgramProgressFrame, { type: 'baseline' }>>(ctx.remote.$stream<ProgramProgressFrame>({
      name: 'program-progress:' + sessionId,
      open: signal => ctx.remote.reactivePrograms.progress(address, AbortSignal.any([lifetime, signal])),
      ended: () => new Error('REACTIVE_REMOTE_PROGRESS_ENDED'), carrierFailed: invalidate,
    }), {
      name: 'program-progress:' + sessionId,
      isSnapshot: (frame): frame is Extract<ProgramProgressFrame, { type: 'baseline' }> => frame.type === 'baseline',
      replace: frame => {
        if (lifetime.aborted) return;
        const observation = frame.observation;
        if (observation.status === 'ready') {
          const value = parseConversationProgress(observation.value);
          assistant.replace(value.attempts); ready(value.activity);
        } else {
          assistant.replace([]);
          if (observation.status === 'error') {
            const error = revokeConversationAccess(owner, decodeProgramFailure(observation.error));
            if (lifetime.aborted) return;
            current.next({ status: 'error', generation: current.value.generation + 1, error });
            return;
          }
          current.next({ status: 'pending', generation: current.value.generation + 1 });
        }
      },
      update: frame => {
        if (lifetime.aborted) return;
        if (frame.type === 'activity') ready(parseConversationProgress({ activity: frame.activity, attempts: [] }).activity);
        else if (frame.type === 'assistant') {
          const observed = current.value;
          if (observed.status !== 'ready') throw new Error('REACTIVE_REMOTE_ASSISTANT_WITHOUT_BASELINE');
          const value = parseAssistantProgressFrame(frame.progress);
          assistant.accept(value); ready(observed.value); progressFrames.next(value);
        } else {
          const event = frame.event;
          if (event.sessionId !== sessionId) throw new Error('REACTIVE_REQUEST_SESSION_MISMATCH');
          // 同一事实 cut 固定当前订阅者；raw-only 不执行业务 input transform。
          // 此处已经离开宿主 append，消费者共享本次自有快照和领域解码，不回放历史。
          const raw$ = defer(() => of(ownConversationEvent(event.kind === 'execution.error'
            ? { ...event, error: decodeProgramFailure(event.error) } : event))).pipe(shareReplay({ bufferSize: 1, refCount: true }));
          const typed$ = raw$.pipe(map(value => decodeProgramEvent(program, value)), shareReplay({ bufferSize: 1, refCount: true }));
          facts.next({ raw$, typed$ });
        }
      },
      failed: error => {
        if (lifetime.aborted || terminated) return;
        error = revokeConversationAccess(owner, error);
        if (lifetime.aborted) return;
        // 官方snapshot consumer已经终止；先固定事实，通知中的重入reset也不能假装重新观察。
        terminated = true;
        assistant.replace([]); current.next({ status: 'error', generation: current.value.generation + 1, error });
        facts.error(error); progressFrames.error(error);
      },
    });
    const reset = ctx.on('connection/reset', () => { if (!terminated) { invalidate(); stream.restart(); } });
    return { stream, reset };
  }, async ({ stream, reset }) => {
    reset();
    try { await stream.dispose(); }
    finally { assistant.replace([]); current.complete(); facts.complete(); progressFrames.complete(); requests.clear(); cached = undefined; }
  }, 'program-progress:' + sessionId);
  if (owner.active) lease.value.stream.start();
  return {
    progress, progress$: progress.observations$,
    progressFrames$: defer(() => lifetime.aborted ? EMPTY : progressFrames.pipe(takeUntil(fromEvent(lifetime, 'abort')))),
    events$: defer(() => lifetime.aborted ? EMPTY : events.pipe(takeUntil(fromEvent(lifetime, 'abort')))),
    rawEvents$: defer(() => lifetime.aborted ? EMPTY : rawEvents.pipe(takeUntil(fromEvent(lifetime, 'abort')))),
    request(id) {
      lifetime.throwIfAborted();
      let request = requests.get(id);
      if (!request) { request = createRequestHandle(sessionId, id, progress, events, { signal: lifetime }); requests.set(id, request); }
      return request;
    },
  };
}
