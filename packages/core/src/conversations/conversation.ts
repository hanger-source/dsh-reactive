import { Observable, Subject, asapScheduler, of, observeOn, shareReplay, mergeAll, map, filter, distinctUntilChanged, combineLatest } from 'rxjs';
import { z } from 'zod';
import { Scope, batch, type Cell } from '../reactivity/scope.ts';
import type { ProgramDefinition } from '../program/definition.ts';
import { decodeProgramEvent, decodeProgramInputs } from '../program/decode.ts';
import type { Surface } from '../surfaces/contract.ts';
import type { Json, InputReference, Submission, CancellationOptions, ConversationActivity } from './protocol.ts';
import { ownConversationActivity } from './protocol.ts';
import { ownConversationEvent, type AssistantProgressFrame, type ConversationEvent } from './events.ts';
import type { HumanInputState, HumanForegroundQuestion, HumanSnapshot } from '../participation/contract.ts';
import { shareResource } from '../resources/sharing.ts';
import { ResourceReader } from '../resources/reader.ts';
import { createAssistantProgress, createRequestHandle, type ConversationProgressSnapshot, type RequestHandle } from './request.ts';
import type { Observation } from '../resources/observation.ts';

export { z };
export type { Json, InputReference, GoalReference, Continuation, WaitReference, Submission, NormalizedSubmission, AdmittedSubmission, InputDelivery, PromptContentPart, AdmittedPromptContentPart, FileUploadValue, FileUploadReceiptId, EncodedFileUploadRequest, FileUploadSource, UploadFileOptions, Admission, CancellationOptions, SubmitOptions, WorkspaceInfo, WorkspaceRegistration, ConversationCreateOptions } from './protocol.ts';
export interface WorkspaceContext {
  readonly id: string;
  readonly directory: string;
  /** @internal 工作区持有共享来源，独立于会话观察者。 */
  readonly scope: Scope;
  share<T>(source$: Observable<T>): Observable<T>;
}
export interface ConversationIdentity {
  readonly sessionId: string;
  readonly workspaceId: string;
  readonly environmentId: string;
  readonly directory: string;
  readonly parentSessionId: string | null;
}
export type Views = Readonly<Record<string, Observable<unknown>>>;
export type ObservedViews<V extends Views> = { readonly [K in keyof V]: V[K] };
export interface InputReceipt { readonly sessionId: string; readonly requestId: string; readonly accepted: true; }
/** @internal 真实会话事实与宿主控制连接，不承担组件声明。 */
export const conversationBindings = Symbol('conversation-bindings');

/** 对话事实和显式输入；组件、普通程序与外部观察消费同一原生会话。 */
export class Conversation<T = Json, Raw = T> {
  /** @internal 会话程序的 owner；释放页面句柄不释放此 scope。 */
  readonly scope: Scope;
  readonly inputs$: Observable<readonly InputReference<T>[]>;
  readonly activity$: Observable<ConversationActivity>;
  readonly goal$: Observable<ConversationActivity['goal']>;
  readonly waits$: Observable<ConversationActivity['waits']>;
  readonly events$: Observable<ConversationEvent<T>>;
  /** 原始事实读侧；跨进程传输直接使用它，不反向编码领域输入。 */
  readonly rawEvents$: Observable<ConversationEvent>;
  readonly progress$: Observable<Observation<ConversationProgressSnapshot>>;
  readonly progress: ResourceReader<ConversationProgressSnapshot>;
  readonly progressFrames$: Observable<AssistantProgressFrame>;
  readonly human: ResourceReader<HumanSnapshot>;
  readonly questions$: Observable<HumanInputState>;
  readonly foregroundQuestions$: Observable<readonly HumanForegroundQuestion[]>;
  #inputs: Cell<readonly InputReference<T>[]>;
  #activity: Cell<ConversationActivity>;
  #events = new Subject<{ readonly raw$: Observable<ConversationEvent>; readonly typed$: Observable<ConversationEvent<T>> }>();
  #frames = new Subject<AssistantProgressFrame>();
  #assistant = createAssistantProgress();
  #progressVersion: Cell<number>;
  #progressSnapshot?: Observation<ConversationProgressSnapshot>;
  #requests = new Map<string, RequestHandle<T>>();
  #questions: Cell<HumanInputState | undefined>;
  #foregroundQuestions: Cell<readonly HumanForegroundQuestion[]>;
  #submit?: (input: Submission<Raw>, options: CancellationOptions) => Promise<InputReceipt>;

  constructor(
    readonly identity: ConversationIdentity,
    readonly workspace: WorkspaceContext,
    private readonly program: ProgramDefinition<z.ZodType<T, Raw>, Surface>,
    initial: ConversationActivity,
  ) {
    this.scope = workspace.scope.child();
    this.#inputs = this.scope.state([]);
    this.#activity = this.scope.state(ownConversationActivity(initial));
    this.#progressVersion = this.scope.state(0);
    this.#questions = this.scope.state<HumanInputState | undefined>(undefined);
    this.#foregroundQuestions = this.scope.state<readonly HumanForegroundQuestion[]>([]);
    this.inputs$ = this.#inputs.changes$;
    this.activity$ = this.#activity.changes$;
    this.goal$ = this.activity$.pipe(map(activity => activity.goal), distinctUntilChanged());
    this.waits$ = this.activity$.pipe(map(activity => activity.waits), distinctUntilChanged());
    this.human = shareResource(combineLatest([
      this.#questions.changes$.pipe(filter((value): value is HumanInputState => value !== undefined)),
      this.#foregroundQuestions.changes$,
    ]).pipe(map(([questions, foreground], index): Observation<HumanSnapshot> => ({
      status: 'ready', generation: index + 1, observedAt: Date.now(), value: Object.freeze({ questions, foreground }),
    }))), { signal: this.scope.signal, onObserverError: error => this.scope.observerError(error) });
    this.questions$ = this.human.values$.pipe(map(snapshot => snapshot.questions), distinctUntilChanged());
    this.foregroundQuestions$ = this.human.values$.pipe(map(snapshot => snapshot.foreground), distinctUntilChanged());
    this.rawEvents$ = this.observeFacts(this.#events.pipe(map(event => event.raw$), mergeAll()));
    this.events$ = this.observeFacts(this.#events.pipe(map(event => event.typed$), mergeAll()));
    this.progressFrames$ = this.observeFacts(this.#frames);
    this.progress = new ResourceReader({
      observations$: this.#progressVersion.changes$.pipe(map(() => this.progressSnapshot())),
      peek: () => this.progressSnapshot(),
    }, { signal: this.scope.signal, onObserverError: error => this.scope.observerError(error) });
    this.progress$ = this.progress.observations$;
    this.scope.own(() => { this.#events.complete(); this.#frames.complete(); this.#requests.clear(); });
  }
  private observeFacts<U>(source: Observable<U>): Observable<U> {
    return new Observable(subscriber => {
      if (this.scope.disposed) { subscriber.complete(); return; }
      const subscription = source.subscribe(subscriber);
      const release = this.scope.own(() => { subscription.unsubscribe(); subscriber.complete(); });
      return () => { subscription.unsubscribe(); release(); };
    });
  }
  /** 同一版本按需物化一次前缀；无人读取时每个 token 不展开完整 compact stream。 */
  progressSnapshot(): Observation<ConversationProgressSnapshot> {
    this.scope.assertActive();
    if (this.#progressSnapshot) return this.#progressSnapshot;
    const generation = this.#progressVersion.peek();
    try {
      return this.#progressSnapshot = Object.freeze({ status: 'ready', generation, observedAt: Date.now(),
        value: Object.freeze({ activity: this.#activity.peek(), attempts: this.#assistant.snapshot() }) });
    } catch (error) {
      return this.#progressSnapshot = Object.freeze({ status: 'error', generation, error });
    }
  }
  request(requestId: string): RequestHandle<T> {
    this.scope.assertActive();
    let request = this.#requests.get(requestId);
    if (!request) {
      request = createRequestHandle(this.identity.sessionId, requestId, this.progress, this.events$,
        { signal: this.scope.signal, onObserverError: error => this.scope.observerError(error) });
      this.#requests.set(requestId, request);
    }
    return request;
  }
  private notifyProgress() {
    this.#progressSnapshot = undefined;
    this.#progressVersion.update(value => value + 1);
  }
  /** 显式投递到原生输入协议；订阅和组件求值不会重新投递。 */
  submit(input: Submission<Raw>, options: CancellationOptions = {}): Promise<InputReceipt> {
    this.scope.assertActive();
    if (!this.#submit) return Promise.reject(new Error('REACTIVE_PROGRAM_NOT_ATTACHED'));
    return this.#submit(input, options);
  }
  /** @internal 只由原生宿主更新事实和建立显式输入连接。 */
  [conversationBindings]() {
    return {
      activity: this.#activity,
      setSubmit: (submit: (input: Submission<Raw>, options: CancellationOptions) => Promise<InputReceipt>) => { this.#submit = submit; },
      accept: (inputs: readonly InputReference[]) => this.#inputs.set(decodeProgramInputs(this.program, this.identity.sessionId, inputs)),
      emit: (event: ConversationEvent) => {
        if (this.scope.disposed) return;
        // 当前订阅者在事实发生时取得此 inner；后来订阅者不会收到已经排队的旧通知。
        // 原始观察与领域观察共享此事实 cut；raw-only 不求值业务 transform。
        // schema 求值与观察通知都离开原生 append，现有观察者共享一次领域解码。
        const raw$ = of(event).pipe(observeOn(asapScheduler), map(ownConversationEvent),
          shareReplay({ bufferSize: 1, refCount: true }));
        const typed$ = raw$.pipe(map(value => decodeProgramEvent(this.program, value)),
          shareReplay({ bufferSize: 1, refCount: true }));
        this.#events.next({ raw$, typed$ });
      },
      publish: (activity: ConversationActivity) => {
        const owned = ownConversationActivity(activity);
        batch(() => { this.#activity.set(owned); this.notifyProgress(); });
      },
      emitProgressFrame: (value: AssistantProgressFrame) => {
        if (this.scope.disposed) return;
        const owned = this.#assistant.accept(value);
        this.notifyProgress();
        this.#frames.next(owned);
      },
      resetProgress: () => { this.#assistant.replace([]); this.notifyProgress(); },
      publishQuestions: (questions: HumanInputState) => this.#questions.set(questions),
      publishForegroundQuestions: (questions: readonly HumanForegroundQuestion[]) => this.#foregroundQuestions.set(questions),
    };
  }
}
