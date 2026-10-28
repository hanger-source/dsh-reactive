import { bytesToBase64 } from "@deepseek-ai/dsh-util-crypto";
import { type z } from "zod";
import { Observable, map, distinctUntilChanged, firstValueFrom } from "rxjs";
import { conversationBindings, type Submission, type CancellationOptions, type SubmitOptions, type ObservedViews } from "@hanger-source/dsh-reactive/kernel";
import { type AnyEnvironment, type EnvironmentOperations } from "@hanger-source/dsh-reactive/kernel";
import { type Scope } from "@hanger-source/dsh-reactive/kernel";
import { type Observation } from "@hanger-source/dsh-reactive/kernel";
import { ResourceReader } from "@hanger-source/dsh-reactive/kernel";
import { createHumanQuestionResolver, type HumanQuestionResolver, type HumanQuestionHandle, type HumanSnapshot } from "@hanger-source/dsh-reactive/kernel";
import { type ActionContract, type ActionInput, type ActionScope } from "@hanger-source/dsh-reactive/kernel";
import { type OperationContract, type OperationHandle, type OperationReference, type OperationCommand, type OperationLocation, type OperationState, type BoundOperation, type OperationFeedbackRead, type OperationFeedbackPayload } from "@hanger-source/dsh-reactive/kernel";
import { type ActionInvocation } from "@hanger-source/dsh-reactive/kernel";
import { type HumanAnswer, type HumanForegroundQuestion, type HumanInputState, type HumanWaitFrame } from "@hanger-source/dsh-reactive/kernel";
import { type ConversationActivity } from "@hanger-source/dsh-reactive/kernel";
import { type FileUploadSource, type FileUploadValue, type EncodedFileUploadRequest, type UploadFileOptions, type Json } from "@hanger-source/dsh-reactive/kernel";
import type { AssistantProgressFrame, ConversationEvent } from "@hanger-source/dsh-reactive/kernel";
import { createRequestHandle, type ConversationProgressSnapshot, type RequestHandle } from "@hanger-source/dsh-reactive/kernel";
import type { ProgramContract, ProgramInput, ProgramParsedInput } from "@hanger-source/dsh-reactive/kernel";
import { surfaceSlotDefinition, type SurfaceViews } from "@hanger-source/dsh-reactive/kernel";
import { createSurfaceCollectionClient, createSurfaceModuleClient, type SurfaceCollectionClient } from "@hanger-source/dsh-reactive/kernel";
import type { Surface, SurfaceMount } from '@hanger-source/dsh-reactive';
import { actionManifestSlot, type ToolActionBinding } from "@hanger-source/dsh-reactive/kernel";
import { abortable, type InputReceipt, type CollectionResolvedAction, type SurfaceClient, type ProgramSessionClient, type ViewValue, type ProgramCancelResult, type ProgramRequestStatus } from "@hanger-source/dsh-reactive";
import { assertServiceInputOptions } from './inputs.ts';
import type { ConversationSession } from './session.ts';
import type { ConversationSessions } from './sessions.ts';
import type { ConversationInputs } from './inputs.ts';
import type { ConversationObservations } from './observations.ts';
import type { ActionAuthority } from '../execution/authority.ts';
import type { OperationParticipation } from '../operations/participation.ts';
import type { ConversationQuestions } from '../participation/questions.ts';
import { nativeSessionAccess } from './session-access.ts';
export type ResolvedAction<D extends ActionContract> = CollectionResolvedAction<D, SubmitOptions, false>;
export type EnvironmentOperationReaders<E extends AnyEnvironment> = {
    readonly [K in keyof EnvironmentOperations<E>]: (options?: SubmitOptions) => BoundOperation<EnvironmentOperations<E>[K]>;
};
export type ProgramOperationReaders<P extends ProgramContract> = SurfaceClient<P['surface'], SubmitOptions, false>['operations'];
type ProgramViews<P extends ProgramContract> = SurfaceViews<P['surface']>;
export type ServiceConversation<P extends ProgramContract> = Pick<ConversationHandle<P>, 'id' | 'definition' | 'identity' | 'surface' | 'activity$' | 'progress' | 'progress$' | 'progressFrames$' | 'events$' | 'rawEvents$' | 'actionManifest' | 'actionManifest$' | 'views' | 'view' | 'viewResource' | 'observeView' | 'readView' | 'collection' | 'module' | 'request' | 'requestStatus' | 'resolveAction' | 'invokeAction' | 'operations' | 'resolveOperation' | 'attachOperation' | 'locateOperation' | 'readOperationFeedback' | 'snapshot' | 'progressSnapshot' | 'release'> & {
    submit(input: Submission<ProgramInput<P>>, options?: CancellationOptions): Promise<InputReceipt>;
    editQueuedInput(requestId: string, content: Submission['content'], options?: CancellationOptions): Promise<InputReceipt>;
};
export class ConversationHandle<P extends ProgramContract = ProgramContract> implements ProgramSessionClient<P, SubmitOptions, false> {
    readonly activity$: Observable<ConversationActivity>;
    readonly progress$: Observable<Observation<ConversationProgressSnapshot>>;
    readonly progress: ResourceReader<ConversationProgressSnapshot>;
    readonly progressFrames$: Observable<AssistantProgressFrame>;
    readonly events$: Observable<ConversationEvent<ProgramParsedInput<P>>>;
    readonly rawEvents$: Observable<ConversationEvent>;
    readonly questions$: Observable<HumanInputState>;
    readonly foregroundQuestions$: Observable<readonly HumanForegroundQuestion[]>;
    readonly human: ResourceReader<HumanSnapshot>;
    get human$(): Observable<Observation<HumanSnapshot>> { return this.human.observations$; }
    #question: HumanQuestionResolver<SubmitOptions>;
    #released = false;
    #scope: Scope;
    #views?: ObservedViews<ProgramViews<P>>;
    #viewResources = new Map<string, ResourceReader<unknown>>();
    #operations?: ProgramOperationReaders<P>;
    #requests = new Map<string, RequestHandle<ProgramParsedInput<P>>>();
    private constructor(private readonly entry: ConversationSession, private readonly participant: 'human' | 'service', private readonly sessions: ConversationSessions, private readonly inputs: ConversationInputs, private readonly actions: ActionAuthority, private readonly participation: OperationParticipation, private readonly questions: ConversationQuestions, private readonly facts: ConversationObservations) {
        this.#scope = entry.conversation.scope.child();
        facts.registerSessionAccess(this, entry, this.#scope.signal, () => this.assertActive());
        entry.pins++;
        this.activity$ = this.observe(entry.conversation.activity$);
        this.progress = new ResourceReader(entry.conversation.progress,
            { signal: this.#scope.signal, onObserverError: error => this.#scope.observerError(error) });
        this.progress$ = this.progress.observations$;
        this.progressFrames$ = this.observe(entry.conversation.progressFrames$);
        this.events$ = this.observe(entry.conversation.events$);
        this.rawEvents$ = this.observe(entry.conversation.rawEvents$);
        this.human = new ResourceReader(entry.conversation.human,
            { signal: this.#scope.signal, onObserverError: error => this.#scope.observerError(error) });
        this.questions$ = this.human.values$.pipe(map(snapshot => snapshot.questions), distinctUntilChanged());
        this.foregroundQuestions$ = this.human.values$.pipe(map(snapshot => snapshot.foreground), distinctUntilChanged());
        this.#question = createHumanQuestionResolver(this.human, {
            answerQuestion: (id, answer, options) => this.answerQuestion(id, answer, options),
            attachWait: (id, options) => this.attachWait(id, options),
        }, { signal: this.#scope.signal, onObserverError: error => this.#scope.observerError(error) });
    }
    /** @internal 只有宿主能建立带观察生存期的句柄。 */
    static create<P extends ProgramContract = ProgramContract>(entry: ConversationSession, participant: 'human' | 'service', sessions: ConversationSessions, inputs: ConversationInputs, actions: ActionAuthority, participation: OperationParticipation, questions: ConversationQuestions, facts: ConversationObservations): ConversationHandle<P> {
        return new ConversationHandle<P>(entry, participant, sessions, inputs, actions, participation, questions, facts);
    }
    private observe<T>(source$: Observable<T>): Observable<T> {
        return new Observable(subscriber => {
            if (this.#scope.disposed) {
                subscriber.complete();
                return;
            }
            const subscription = source$.subscribe(subscriber);
            if (this.#scope.disposed) {
                subscription.unsubscribe();
                subscriber.complete();
                return;
            }
            const release = this.#scope.own(() => { subscription.unsubscribe(); subscriber.complete(); });
            return () => { subscription.unsubscribe(); release(); };
        });
    }
    private assertActive() {
        if (this.#released)
            throw new Error('REACTIVE_CONVERSATION_HANDLE_RELEASED');
        this.#scope.assertActive();
    }
    private assertHuman() {
        this.assertActive();
        if (this.participant !== 'human') throw new Error('REACTIVE_HUMAN_PARTICIPANT_REQUIRED');
    }
    get id() { return this.entry.header.id as string; }
    /** P 由宿主创建句柄时的实际 Program 绑定确定；返回原对象，不重建合同。 */
    get definition(): P { this.assertActive(); return this.entry.implementation.definition as P; }
    get identity() { return this.entry.conversation.identity; }
    /** 当前环境的纯 Surface 合同；远程宿主复用其 schema，不读取业务实现。 */
    get surface(): P['surface'] { this.assertActive(); return this.entry.implementation.definition.surface; }
    get actionManifest(): ResourceReader<readonly ToolActionBinding[]> {
        this.assertActive();
        return this.resource(actionManifestSlot) as ResourceReader<readonly ToolActionBinding[]>;
    }
    get actionManifest$(): Observable<readonly ToolActionBinding[]> { return this.actionManifest.values$; }
    collection<K extends keyof P['surface']['collections'] & string>(name: K): SurfaceCollectionClient<P['surface']['collections'][K], SubmitOptions, false> {
        this.assertActive();
        // factory 已由同一根 Surface 查得叶合同；此处恢复擦除后的统一传输签名。
        const untyped = this as ConversationHandle;
        return createSurfaceCollectionClient(this.surface, name, { asynchronous: false,
            viewResource: address => this.resource(address),
            resolveAction: (address, _definition, expectedScope, options) => untyped.resolveAction(address, expectedScope, options),
            resolveOperation: (address, _definition, options) => untyped.resolveOperation(address, options),
        });
    }
    module<Source extends Surface, Selected extends Surface>(mount: SurfaceMount<Source, Selected>): SurfaceClient<Selected, SubmitOptions, false> {
        this.assertActive();
        const untyped = this as ConversationHandle;
        return createSurfaceModuleClient(this.surface, mount, { asynchronous: false,
            viewResource: address => this.resource(address),
            resolveAction: (address, _definition, expectedScope, options) => untyped.resolveAction(address, expectedScope, options),
            resolveOperation: (address, _definition, options) => untyped.resolveOperation(address, options),
        });
    }
    get views(): ObservedViews<ProgramViews<P>> {
        this.assertActive();
        return this.#views ??= Object.freeze(Object.fromEntries(Object.entries(this.entry.views)
            .map(([name]) => [name, this.resource(name).values$]))) as ObservedViews<ProgramViews<P>>;
    }
    view<K extends keyof ProgramViews<P> & string>(name: K): ObservedViews<ProgramViews<P>>[K] {
        this.assertActive();
        const value = this.views[name];
        if (!value)
            throw new Error('REACTIVE_VIEW_NOT_FOUND');
        return value;
    }
    /** 只读解析当前绑定并核验明确的原始目标；不激活 Agent，也不受理业务。 */
    resolveAction<K extends keyof P['surface']['actions'] & string>(name: K, expectedScope: z.input<P['surface']['actions'][K]['scope']>, options: CancellationOptions = {}): ResolvedAction<P['surface']['actions'][K]> {
        this.assertActive();
        options.signal?.throwIfAborted();
        const binding = this.actions.resolveSessionAction(this.entry, name, expectedScope);
        // Surface 已核验同一合同；scope 来自实际绑定的唯一解析，不再 parse 领域输出。
        return Object.freeze(Object.assign((input: ActionInput<P['surface']['actions'][K]>, options: SubmitOptions = {}) => this.invokeAction(name, binding.bindingId, input, options), {
            definition: surfaceSlotDefinition(this.surface, 'actions', name) as P['surface']['actions'][K], bindingId: binding.bindingId,
            scope: binding.scope as ActionScope<P['surface']['actions'][K]>,
        }));
    }
    /** 已认证 transport 的第二阶段受理；必填 bindingId 只核对原绑定，不授予权限。 */
    invokeAction<K extends keyof P['surface']['actions'] & string>(name: K, bindingId: string, input: ActionInput<P['surface']['actions'][K]>, options: SubmitOptions = {}): ActionInvocation<P['surface']['actions'][K]> {
        this.assertActive();
        return this.actions.invokeSession(this.entry, name, bindingId, input, options, this.participant) as ActionInvocation<P['surface']['actions'][K]>;
    }
    /** 解析时捕获当前操作绑定；取得的 reader 不随组件后来更换槽而改指目标。 */
    get operations(): ProgramOperationReaders<P> {
        this.assertActive();
        return this.#operations ??= Object.freeze(Object.fromEntries(Object.keys(this.surface.operations).map(name => [name,
            (options: SubmitOptions = {}) => {
                this.assertActive();
                const signal = options.signal ? AbortSignal.any([this.#scope.signal, options.signal]) : this.#scope.signal;
                return this.participation.resolveOperation(this.entry, name, { ...options, signal }, this.participant);
            },
        ]))) as ProgramOperationReaders<P>;
    }
    /** transport 和 collection 使用同一合同查找；动态地址由框架 factory 生成。 */
    resolveOperation<K extends keyof P['surface']['operations'] & string>(name: K, options: SubmitOptions = {}): BoundOperation<P['surface']['operations'][K]> {
        this.assertActive();
        const signal = options.signal ? AbortSignal.any([this.#scope.signal, options.signal]) : this.#scope.signal;
        return this.participation.resolveOperation(this.entry, name, { ...options, signal }, this.participant) as BoundOperation<P['surface']['operations'][K]>;
    }
    snapshot() { this.assertActive(); return this.entry.conversation[conversationBindings]().activity.peek(); }
    progressSnapshot() { this.assertActive(); return this.progress.peek(); }
    request(requestId: string): RequestHandle<ProgramParsedInput<P>> {
        this.assertActive();
        let request = this.#requests.get(requestId);
        if (!request) {
            request = createRequestHandle(this.id, requestId, this.progress, this.events$,
                { signal: this.#scope.signal, onObserverError: error => this.#scope.observerError(error) });
            this.#requests.set(requestId, request);
        }
        return request;
    }
    private resource(name: string): ResourceReader<unknown> {
        this.assertActive();
        let resource = this.#viewResources.get(name);
        if (!resource) {
            resource = new ResourceReader(this.entry.program.outputs.view(name),
                { signal: this.#scope.signal, onObserverError: error => this.#scope.observerError(error) });
            this.#viewResources.set(name, resource);
        }
        return resource;
    }
    viewResource<K extends keyof P['surface']['views'] & string>(name: K): ResourceReader<ViewValue<P['surface']['views'][K]>> {
        return this.resource(name) as ResourceReader<ViewValue<P['surface']['views'][K]>>;
    }
    observeView<K extends keyof P['surface']['views'] & string>(name: K): Observable<Observation<ViewValue<P['surface']['views'][K]>>> {
        return this.viewResource(name).observations$;
    }
    /** HTTP/Gateway 使用 Surface 已编码的 wire 值，本地 SDK 保留领域值。 */
    observeWireView(name: string): Observable<Observation<Json>> {
        this.assertActive();
        const value = this.entry.program.outputs.wireView(name);
        return this.observe(value.observations$);
    }
    async readView<K extends keyof P['surface']['views'] & string>(name: K, options: CancellationOptions = {}): Promise<ViewValue<P['surface']['views'][K]>> {
        return this.viewResource(name).read(options.signal);
    }
    submit(input: Submission<ProgramInput<P>>, options: SubmitOptions = {}) {
        this.assertActive();
        if (this.participant === 'service')
            assertServiceInputOptions(options);
        return this.inputs.submitSession(this.entry, input, options, this.participant);
    }
    /** 保留本请求来源与业务范围，重新准入仍未消费的正文；编辑本身不唤醒 Agent。 */
    editQueuedInput(requestId: string, content: Submission['content'], options: SubmitOptions = {}) {
        this.assertActive();
        if (this.participant === 'service')
            assertServiceInputOptions(options);
        return this.inputs.editSessionInput(this.entry, requestId, content, options, this.participant);
    }
    /** 显式上传至本会话；receipt 由原生 exact Session 持有，提交正文时使用它。 */
    async uploadFile(data: FileUploadSource, options: UploadFileOptions = {}): Promise<FileUploadValue> {
        this.assertActive();
        const { name, actorId, signal } = options;
        signal?.throwIfAborted();
        const bytes = data instanceof Uint8Array ? data : new Uint8Array(await data.arrayBuffer());
        this.assertActive();
        signal?.throwIfAborted();
        return this.uploadEncodedFile({ data: bytesToBase64(bytes), ...(name === undefined ? {} : { name }) }, { actorId, signal });
    }
    /** 已认证 transport 接入官方 canonical base64；不解码重编码而洗掉非法输入。 */
    uploadEncodedFile(request: EncodedFileUploadRequest, options: SubmitOptions = {}): Promise<FileUploadValue> {
        this.assertActive();
        return this.inputs.uploadSessionFile(this.entry, request, options);
    }
    whenIdle() { this.assertActive(); return this.sessions.settle(this.sessions.live(this.entry).agent); }
    attachOperation<D extends OperationContract>(definition: D, reference: OperationReference<D>, options: SubmitOptions = {}): Promise<OperationHandle<D>> {
        this.assertActive();
        const signal = options.signal ? AbortSignal.any([this.#scope.signal, options.signal]) : this.#scope.signal;
        return this.participation.attachOperation(this.id, definition, reference, { ...options, signal }, this.participant);
    }
    locateOperation<D extends OperationContract>(definition: D, command: OperationCommand<D>, options: SubmitOptions = {}): Promise<OperationLocation<OperationReference<D>, OperationState<D>>> {
        this.assertActive();
        const signal = options.signal ? AbortSignal.any([this.#scope.signal, options.signal]) : this.#scope.signal;
        return this.participation.locateOperation(this.id, definition, command, { ...options, signal }, this.participant);
    }
    readOperationFeedback<D extends OperationContract>(definition: D, command: OperationCommand<D>, id: string, options: SubmitOptions = {}): Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>> {
        this.assertActive();
        const signal = options.signal ? AbortSignal.any([this.#scope.signal, options.signal]) : this.#scope.signal;
        return this.participation.readOperationFeedback(this.id, definition, command, id, { ...options, signal }, this.participant);
    }
    async question(callId: string, options: SubmitOptions = {}): Promise<HumanQuestionHandle<SubmitOptions>> {
        this.assertHuman();
        return this.#question(callId, options);
    }
    answerQuestion(callId: string, answer: HumanAnswer, options: SubmitOptions = {}): Promise<boolean> {
        this.assertHuman();
        const signal = options.signal ? AbortSignal.any([this.#scope.signal, options.signal]) : this.#scope.signal;
        return this.questions.answerQuestion(this.entry, callId, answer, { ...options, signal });
    }
    async *attachWait(callId: string, options: SubmitOptions = {}): AsyncIterable<HumanWaitFrame> {
        this.assertHuman();
        const signal = options.signal ? AbortSignal.any([this.#scope.signal, options.signal]) : this.#scope.signal;
        yield* this.questions.attachWait(this.entry, callId, { ...options, signal });
    }
    async pause(options: CancellationOptions = {}) {
        this.assertHuman();
        const signal = options.signal ? AbortSignal.any([this.#scope.signal, options.signal]) : this.#scope.signal;
        return firstValueFrom(abortable(() => this.sessions.pauseGoal(this.sessions.live(this.entry).agent), signal));
    }
    /** 停止模型处理；是否取消外部业务操作仍由业务能力决定。 */
    async cancel(options: CancellationOptions = {}): Promise<ProgramCancelResult> {
        this.assertHuman();
        const signal = options.signal ? AbortSignal.any([this.#scope.signal, options.signal]) : this.#scope.signal;
        return firstValueFrom(abortable(async (): Promise<ProgramCancelResult> => {
            await this.sessions.cancelAgent(this.sessions.live(this.entry).agent);
            return { canceled: true };
        }, signal));
    }
    /** 权威请求切面来自原生日志与投影；冷读取不激活 Agent，也不改写观察缓存。 */
    async requestStatus(requestId: string, options: CancellationOptions = {}): Promise<ProgramRequestStatus> {
        this.assertActive();
        const signal = options.signal ? AbortSignal.any([this.#scope.signal, options.signal]) : this.#scope.signal;
        return firstValueFrom(abortable(async (readSignal): Promise<ProgramRequestStatus> => {
            const inspection = await nativeSessionAccess(this).inspect(readSignal);
            readSignal.throwIfAborted();
            const current = this.facts.stateOf(inspection).state.activity;
            return { requestId, state: current.requests[requestId]?.state ?? 'absent' };
        }, signal));
    }
    /** 释放调用者的观察作用域；原生执行与已提交业务操作继续由各自 owner 管理。 */
    release() {
        if (this.#released)
            return;
        this.#released = true;
        const failures: unknown[] = [];
        try {
            this.#scope.dispose();
        }
        catch (error) {
            failures.push(error);
        }
        try {
            this.sessions.release(this.entry);
        }
        catch (error) {
            failures.push(error);
        }
        if (failures.length)
            throw new AggregateError(failures, 'REACTIVE_OBSERVATION_RELEASE_FAILED');
    }
    [Symbol.dispose]() { this.release(); }
}
