import type { Context } from '@deepseek-ai/cordis';
import { RemoteSnapshotStream } from '@deepseek-ai/dsh-api-gateway/client';
import { BehaviorSubject, Subject, map, type Observable } from 'rxjs';
import { ResourceReader, createAssistantProgress, createRequestHandle, decodeProgramEvent, parseAssistantProgressFrame, parseConversationProgress, type AssistantProgressFrame, type ConversationActivity, type ConversationEvent, type ConversationProgressSnapshot, type Observation, type RequestHandle, type ProgramContract, type ProgramParsedInput } from '@hanger-source/dsh-reactive';
import type { ProgramProgressFrame } from './types.ts';
import { decodeProgramFailure } from './errors.ts';

export interface RemoteProgress<P extends ProgramContract> {
  readonly progress: ResourceReader<ConversationProgressSnapshot>;
  readonly progress$: Observable<Observation<ConversationProgressSnapshot>>;
  readonly progressFrames$: Observable<AssistantProgressFrame>;
  readonly events$: Observable<ConversationEvent<ProgramParsedInput<P>>>;
  request(id: string): RequestHandle<ProgramParsedInput<P>>;
  close(): Promise<void>;
}

/** 会话拥有一个连接；request 只选择 core 的当前事实，不重折日志或重放历史。 */
export function connectProgress<P extends ProgramContract>(ctx: Context, sessionId: string, program: P, lifetime: AbortSignal): RemoteProgress<P> {
  const assistant = createAssistantProgress();
  const current = new BehaviorSubject<Observation<ConversationActivity>>({ status: 'pending', generation: 0 });
  const events = new Subject<ConversationEvent<ProgramParsedInput<P>>>();
  const progressFrames = new Subject<AssistantProgressFrame>();
  const requests = new Map<string, RequestHandle<ProgramParsedInput<P>>>();
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
    if (lifetime.aborted) return;
    assistant.replace([]);
    current.next({ status: 'pending', generation: current.value.generation + 1 });
  };
  const ready = (activity: ConversationActivity): void => {
    if (activity.sessionId !== sessionId) throw new Error('REACTIVE_REQUEST_SESSION_MISMATCH');
    current.next({ status: 'ready', generation: current.value.generation + 1, observedAt: Date.now(), value: activity });
  };
  const stream = new RemoteSnapshotStream<Extract<ProgramProgressFrame, { type: 'baseline' }>, Exclude<ProgramProgressFrame, { type: 'baseline' }>>(ctx.remote.$stream<ProgramProgressFrame>({
    name: 'program-progress:' + sessionId,
    open: signal => ctx.remote.reactivePrograms.progress({ sessionId }, AbortSignal.any([lifetime, signal])),
    ended: () => new Error('REACTIVE_REMOTE_PROGRESS_ENDED'), carrierFailed: invalidate,
  }), {
    name: 'program-progress:' + sessionId,
    isSnapshot: (frame): frame is Extract<ProgramProgressFrame, { type: 'baseline' }> => frame.type === 'baseline',
    replace: frame => {
      const observation = frame.observation;
      if (observation.status === 'ready') {
        const value = parseConversationProgress(observation.value);
        assistant.replace(value.attempts); ready(value.activity);
      } else {
        assistant.replace([]);
        current.next(observation.status === 'error'
          ? { status: 'error', generation: current.value.generation + 1, error: decodeProgramFailure(observation.error) }
          : { status: 'pending', generation: current.value.generation + 1 });
      }
    },
    update: frame => {
      if (frame.type === 'activity') ready(parseConversationProgress({ activity: frame.activity, attempts: [] }).activity);
      else if (frame.type === 'assistant') {
        const observed = current.value;
        if (observed.status !== 'ready') throw new Error('REACTIVE_REMOTE_ASSISTANT_WITHOUT_BASELINE');
        const value = parseAssistantProgressFrame(frame.progress);
        assistant.accept(value); ready(observed.value); progressFrames.next(value);
      } else {
        const event = frame.event;
        if (event.sessionId !== sessionId) throw new Error('REACTIVE_REQUEST_SESSION_MISMATCH');
        events.next(decodeProgramEvent(program, event.kind === 'execution.error' ? { ...event, error: decodeProgramFailure(event.error) } : event));
      }
    },
    failed: error => {
      if (lifetime.aborted) return;
      assistant.replace([]); current.next({ status: 'error', generation: current.value.generation + 1, error });
      events.error(error); progressFrames.error(error);
    },
  });
  const reset = ctx.on('connection/reset', () => { invalidate(); stream.restart(); });
  stream.start();
  return {
    progress, progress$: progress.observations$, progressFrames$: progressFrames.asObservable(), events$: events.asObservable(),
    request(id) {
      lifetime.throwIfAborted();
      let request = requests.get(id);
      if (!request) { request = createRequestHandle(sessionId, id, progress, events, { signal: lifetime }); requests.set(id, request); }
      return request;
    },
    async close() {
      reset(); await stream.dispose(); assistant.replace([]); current.complete(); events.complete(); progressFrames.complete(); requests.clear(); cached = undefined;
    },
  };
}
