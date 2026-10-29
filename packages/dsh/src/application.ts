import type { Context } from "@deepseek-ai/cordis";
import { type SubmitOptions, type CancellationOptions, type Workspace, type WorkspaceInfo, type WorkspaceRegistration, type ConversationCreateOptions } from "@hanger-source/dsh-reactive/kernel";
import { type ApplicationDefinition } from "@hanger-source/dsh-reactive/kernel";
import { type AnyEnvironment, type EnvironmentAt } from "@hanger-source/dsh-reactive/kernel";
import { type OperationContract, type OperationHandle, type OperationReference, type OperationCommand, type OperationLocation, type OperationState, type OperationNotificationResult, type OperationFeedbackRead, type OperationFeedbackPayload } from "@hanger-source/dsh-reactive/kernel";
import { type Workspaces } from "./workspaces/workspaces.ts";
import { type CreationReference } from "@hanger-source/dsh-reactive/kernel";
import { installReactiveProjection } from "./conversations/projections.ts";
import { captureProgramExpectation, type ProgramContract, type ProgramExpectation } from "@hanger-source/dsh-reactive/kernel";
import { type NativeDelegations } from "./delegations.ts";
import type { HostOptions } from './applications/options.ts';
import { ConversationHandle, type ServiceConversation } from './conversations/handle.ts';
import type { ConversationSession } from './conversations/session.ts';
import { ConversationSessions } from './conversations/sessions.ts';
import { ConversationObservations } from './conversations/observations.ts';
import { ConversationInputs } from './conversations/inputs.ts';
import { ActionAuthority } from './execution/authority.ts';
import type { RetainedActionInvocation } from './execution/invocation.ts';
import { ProgramTools } from './execution/tools.ts';
import { OperationParticipation } from './operations/participation.ts';
import { OperationNotifications } from './operations/notifications.ts';
import { ConversationPrograms } from './programs/preparation.ts';
import { ConversationQuestions } from './participation/questions.ts';
import { humanParticipant, serviceParticipant, type ParticipantIdentity } from './participation/identity.ts';

/** 可信宿主签发的固定服务身份；会话、工作区与执行仍由同一应用持有。 */
export interface ApplicationService<E extends readonly AnyEnvironment[] = readonly AnyEnvironment[]> {
    create<Id extends E[number]['id']>(environmentId: Id, options: ConversationCreateOptions, expected?: ProgramExpectation): Promise<ServiceConversation<EnvironmentAt<E[number], Id>['program']['definition']>>;
    open(sessionId: string, expected?: undefined, expectedAccessId?: string): Promise<ServiceConversation>;
    open<P extends ProgramContract>(sessionId: string, expected: P, expectedAccessId?: string): Promise<ServiceConversation<P>>;
    open(sessionId: string, expected: Exclude<ProgramExpectation, ProgramContract>, expectedAccessId?: string): Promise<ServiceConversation>;
    resume(sessionId: string, expected?: undefined, expectedAccessId?: string): Promise<ServiceConversation>;
    resume<P extends ProgramContract>(sessionId: string, expected: P, expectedAccessId?: string): Promise<ServiceConversation<P>>;
    resume(sessionId: string, expected: Exclude<ProgramExpectation, ProgramContract>, expectedAccessId?: string): Promise<ServiceConversation>;
    completeCreation(reference: CreationReference, expected?: ProgramExpectation): Promise<ServiceConversation>;
    createWorkspace(input: WorkspaceRegistration): Promise<WorkspaceInfo>;
    describe(): Promise<readonly WorkspaceInfo[]>;
    readInvocation(sessionId: string, id: string, expected: ProgramExpectation, options?: CancellationOptions): RetainedActionInvocation;
    notifyOperation(sessionId: string, followId: string, options?: CancellationOptions): Promise<OperationNotificationResult>;
}
/** 应用入口只组合能力域，执行和事实由对应owner持有。 */
export class ApplicationHost<E extends readonly AnyEnvironment[] = readonly AnyEnvironment[]> {
    #lifetime = new AbortController();
    #closing?: Promise<void>;
    #disposing?: Promise<void>;
    #initializing?: Promise<void>;
    #sessionsStarting?: Promise<void>;
    private readonly sessions: ConversationSessions<E>;
    private readonly facts: ConversationObservations;
    private readonly inputs: ConversationInputs;
    private readonly actions: ActionAuthority;
    private readonly operations: OperationParticipation;
    private readonly operationNotifications: OperationNotifications;
    private readonly questions: ConversationQuestions;
    private readonly programs: ConversationPrograms;
    private readonly stopDelegationUpdates: () => void;
    private constructor(ctx: Context, definition: ApplicationDefinition<E>, private readonly workspaces: Workspaces, private readonly options: HostOptions, private readonly delegations: NativeDelegations, private readonly releaseInstallation: () => Promise<void>) {
        installReactiveProjection(ctx);
        const signal = this.#lifetime.signal;
        this.facts = new ConversationObservations(ctx, signal);
        this.sessions = new ConversationSessions(ctx, definition, workspaces, options, signal, this.facts, delegations, () => this.programs, () => this.inputs);
        this.inputs = new ConversationInputs(ctx, definition, workspaces, options, signal, this.sessions, this.facts, delegations, () => this.operations);
        this.actions = new ActionAuthority(ctx, options, signal, workspaces, this.sessions, this.facts, this.inputs, delegations);
        this.operations = new OperationParticipation(ctx, definition, options, signal, this.sessions, this.facts, this.inputs, this.actions, delegations);
        this.operationNotifications = new OperationNotifications(options.operations ?? [], options.serviceActorId, this.operations);
        const tools = new ProgramTools(ctx, workspaces, this.sessions, this.actions, this.operations);
        this.programs = new ConversationPrograms(ctx, this.sessions, this.inputs, this.actions, this.operations, tools, delegations);
        this.questions = new ConversationQuestions(ctx, options, signal, this.sessions, this.facts);
        delegations.install();
        this.stopDelegationUpdates = this.operations.installDelegationUpdates();
        this.sessions.installDisposed();
        this.facts.install(this.sessions, entry => this.inputs.retainPausedResults(entry));
        this.inputs.installClaimed();
        this.programs.installAssembly();
        this.inputs.installPreStep();
        tools.installPolicies();
        // 原生卸载只排空业务；公开dispose在排空后退出安装，避免Fiber等待自己。
        ctx.effect(() => () => this.drain());
    }
    /** @internal 正式mount完成依赖后构造同一应用。 */
    static create<E extends readonly AnyEnvironment[]>(ctx: Context, definition: ApplicationDefinition<E>, workspaces: Workspaces, options: HostOptions, delegations: NativeDelegations, releaseInstallation: () => Promise<void>): ApplicationHost<E> {
        return new ApplicationHost(ctx, definition, workspaces, options, delegations, releaseInstallation);
    }
    initialize(): Promise<void> {
        if (this.#initializing) return this.#initializing;
        const completion = Promise.withResolvers<void>();
        this.#initializing = completion.promise;
        void (async () => {
            this.#lifetime.signal.throwIfAborted();
            this.#sessionsStarting = this.sessions.initialize();
            await this.#sessionsStarting;
            this.#lifetime.signal.throwIfAborted();
            await this.operationNotifications.initialize();
        })().then(completion.resolve, completion.reject);
        return this.#initializing;
    }
    private handle<P extends ProgramContract = ProgramContract>(session: ConversationSession, participant: ParticipantIdentity = humanParticipant): ConversationHandle<P> {
        return ConversationHandle.create<P>(session, participant, this.sessions, this.inputs, this.actions, this.operations, this.questions, this.facts);
    }
    async create<Id extends E[number]['id']>(environmentId: Id, options: ConversationCreateOptions, expected?: ProgramExpectation): Promise<ConversationHandle<EnvironmentAt<E[number], Id>['program']['definition']>> {
        return this.createAs(environmentId, options, humanParticipant, expected);
    }
    private async createAs<Id extends E[number]['id']>(environmentId: Id, options: ConversationCreateOptions, participant: ParticipantIdentity, expected?: ProgramExpectation): Promise<ConversationHandle<EnvironmentAt<E[number], Id>['program']['definition']>> {
        expected = captureProgramExpectation(expected);
        return this.handle<EnvironmentAt<E[number], Id>['program']['definition']>(await this.sessions.create(environmentId, options, expected), participant);
    }
    createWorkspace(input: WorkspaceRegistration): Promise<WorkspaceInfo> { return this.sessions.createWorkspace(input); }
    /** 取得已有本地工作区的编程接点，不创建或恢复Session。 */
    workspace(workspaceId: string): Workspace { this.#lifetime.signal.throwIfAborted(); return this.workspaces.get(workspaceId).context; }
    describe(): Promise<readonly WorkspaceInfo[]> { return this.sessions.describe(); }
    /** 原受理记录属于Application，读取不open会话或重新装配Program。 */
    readInvocation(sessionId: string, id: string, expected: ProgramExpectation, options: SubmitOptions = {}): RetainedActionInvocation {
        return this.actions.readInvocation(sessionId, id, expected, options);
    }
    open(sessionId: string, expected?: undefined, expectedAccessId?: string): Promise<ConversationHandle>;
    open<P extends ProgramContract>(sessionId: string, expected: P, expectedAccessId?: string): Promise<ConversationHandle<P>>;
    open(sessionId: string, expected: Exclude<ProgramExpectation, ProgramContract>, expectedAccessId?: string): Promise<ConversationHandle>;
    async open(sessionId: string, expected?: ProgramExpectation, expectedAccessId?: string): Promise<ConversationHandle> {
        return this.openAs(sessionId, expected, humanParticipant, expectedAccessId);
    }
    private async openAs<P extends ProgramContract = ProgramContract>(sessionId: string, expected: ProgramExpectation | undefined, participant: ParticipantIdentity, expectedAccessId?: string): Promise<ConversationHandle<P>> {
        expected = captureProgramExpectation(expected);
        const session = await this.sessions.readSession(sessionId, expected, expectedAccessId);
        return this.handle<P>(session, participant);
    }
    async completeCreation(reference: CreationReference, expected?: ProgramExpectation): Promise<ConversationHandle> { return this.completeCreationAs(reference, humanParticipant, expected); }
    private async completeCreationAs(reference: CreationReference, participant: ParticipantIdentity, expected?: ProgramExpectation): Promise<ConversationHandle> {
        expected = captureProgramExpectation(expected);
        return this.handle(await this.sessions.completeCreation(reference, expected), participant);
    }
    resume(sessionId: string, expected?: undefined, expectedAccessId?: string): Promise<ConversationHandle>;
    resume<P extends ProgramContract>(sessionId: string, expected: P, expectedAccessId?: string): Promise<ConversationHandle<P>>;
    resume(sessionId: string, expected: Exclude<ProgramExpectation, ProgramContract>, expectedAccessId?: string): Promise<ConversationHandle>;
    async resume(sessionId: string, expected?: ProgramExpectation, expectedAccessId?: string): Promise<ConversationHandle> {
        return this.resumeAs(sessionId, expected, humanParticipant, expectedAccessId);
    }
    private async resumeAs<P extends ProgramContract = ProgramContract>(sessionId: string, expected: ProgramExpectation | undefined, participant: ParticipantIdentity, expectedAccessId?: string): Promise<ConversationHandle<P>> {
        expected = captureProgramExpectation(expected);
        const session = await this.sessions.readSession(sessionId, expected, expectedAccessId);
        this.sessions.assertSessionWorkspace(session);
        const acquired = await this.sessions.activate(sessionId, session.accessId);
        try {
            this.sessions.assertSessionWorkspace(session);
            if (session.child) {
                await this.sessions.recoverChild(session);
                this.sessions.assertSessionWorkspace(session);
            }
            await this.operations.reconcileOperations(this.sessions.live(session));
            this.sessions.assertSessionWorkspace(session);
            return this.handle<P>(session, participant);
        }
        catch (error) {
            // 原生恢复已经完成；读侧资格退出只结束这次取得，不回滚真实Agent或覆盖原退出原因。
            session.scope.signal.throwIfAborted();
            if (acquired) {
                try { await this.sessions.rollbackActivation(acquired); }
                catch (cleanup) { throw new AggregateError([error, cleanup], 'REACTIVE_CONVERSATION_RESUME_FAILED'); }
            }
            throw error;
        }
    }
    conversation(sessionId: string): ConversationHandle;
    conversation<P extends ProgramContract>(sessionId: string, expected: P): ConversationHandle<P>;
    conversation(sessionId: string, expected?: ProgramContract): ConversationHandle {
        const session = this.sessions.require(sessionId);
        this.sessions.expectProgram(session, expected);
        return this.handle(session);
    }
    service(actorId?: string): ApplicationService<E> {
        this.#lifetime.signal.throwIfAborted();
        const participant = serviceParticipant(actorId === undefined ? this.options.serviceActorId : actorId), application = this;
        const service: ApplicationService<E> = {
            create<Id extends E[number]['id']>(environmentId: Id, options: ConversationCreateOptions, expected?: ProgramExpectation) {
                return application.createAs(environmentId, options, participant, expected);
            },
            open<P extends ProgramContract = ProgramContract>(sessionId: string, expected?: ProgramExpectation, expectedAccessId?: string) {
                return application.openAs<P>(sessionId, expected, participant, expectedAccessId);
            },
            resume<P extends ProgramContract = ProgramContract>(sessionId: string, expected?: ProgramExpectation, expectedAccessId?: string) {
                return application.resumeAs<P>(sessionId, expected, participant, expectedAccessId);
            },
            completeCreation: (reference, expected) => application.completeCreationAs(reference, participant, expected),
            createWorkspace: input => application.createWorkspace(input),
            describe: () => application.describe(),
            readInvocation: (sessionId, id, expected, options = {}) => application.actions.readInvocation(sessionId, id, expected, options, participant),
            notifyOperation: (sessionId, followId, options = {}) => application.operations.notifyOperation(sessionId, followId, options, participant),
        };
        return Object.freeze(service);
    }
    operationDefinition(kind: string, version: number): OperationContract { return this.operations.operationDefinition(kind, version); }
    attachOperation<D extends OperationContract>(sessionId: string, definition: D, reference: OperationReference<D>, options: SubmitOptions = {}): Promise<OperationHandle<D>> {
        return this.operations.attachOperation(sessionId, definition, reference, options);
    }
    locateOperation<D extends OperationContract>(sessionId: string, definition: D, command: OperationCommand<D>, options: SubmitOptions = {}): Promise<OperationLocation<OperationReference<D>, OperationState<D>>> {
        return this.operations.locateOperation(sessionId, definition, command, options);
    }
    readOperationFeedback<D extends OperationContract>(sessionId: string, definition: D, command: OperationCommand<D>, id: string, options: SubmitOptions = {}): Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>> {
        return this.operations.readOperationFeedback(sessionId, definition, command, id, options);
    }
    notifyOperation(sessionId: string, followId: string, options: SubmitOptions = {}): Promise<OperationNotificationResult> {
        return this.operations.notifyOperation(sessionId, followId, { signal: options.signal }, serviceParticipant(options.actorId ?? this.options.serviceActorId));
    }
    close(sessionId: string): Promise<void> { return this.sessions.close(sessionId); }
    dispose(): Promise<void> {
        if (this.#disposing) return this.#disposing;
        const completion = Promise.withResolvers<void>();
        this.#disposing = completion.promise;
        void (async () => {
            const failures = new Set<unknown>();
            try { await this.drain(); } catch (error) { failures.add(error); }
            try { await this.releaseInstallation(); } catch (error) { failures.add(error); }
            if (failures.size) throw new AggregateError([...failures], 'REACTIVE_APPLICATION_DISPOSAL_FAILED');
        })().then(completion.resolve, completion.reject);
        return this.#disposing;
    }
    /** @internal 父安装退出与显式dispose共用排空回执；这里不卸载正在等待它的Fiber。 */
    drain(): Promise<void> {
        if (this.#closing) return this.#closing;
        const completion = Promise.withResolvers<void>();
        this.#closing = completion.promise;
        void (async () => {
            const stoppingNotifications = this.operationNotifications.close();
            this.stopDelegationUpdates();
            this.#lifetime.abort(new Error('REACTIVE_HOST_DISPOSED'));
            const failures: unknown[] = [];
            try { await stoppingNotifications; }
            catch (error) { failures.push(error); }
            await Promise.allSettled(this.#sessionsStarting ? [this.#sessionsStarting] : []);
            failures.push(...await this.sessions.shutdownNative());
            try {
                await this.sessions.stopPresets();
            }
            catch (error) {
                failures.push(error);
            }
            failures.push(...await this.sessions.releasePrograms());
            this.delegations.dispose();
            try {
                this.workspaces.dispose();
            }
            catch (error) {
                failures.push(error);
            }
            if (failures.length)
                throw new AggregateError(failures, 'REACTIVE_HOST_SHUTDOWN_FAILED');
        })().then(completion.resolve, completion.reject);
        return this.#closing;
    }
}
