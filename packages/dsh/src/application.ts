import type { Context } from "@deepseek-ai/cordis";
import { type SubmitOptions, type WorkspaceInfo, type WorkspaceRegistration, type ConversationCreateOptions } from "@hanger-source/dsh-reactive/kernel";
import { type ApplicationDefinition } from "@hanger-source/dsh-reactive/kernel";
import { type AnyEnvironment, type EnvironmentAt } from "@hanger-source/dsh-reactive/kernel";
import { type OperationContract, type OperationHandle, type OperationReference, type OperationCommand, type OperationLocation, type OperationState, type OperationNotificationResult, type OperationFeedbackRead, type OperationFeedbackPayload } from "@hanger-source/dsh-reactive/kernel";
import { type Workspaces } from "./workspaces/workspaces.ts";
import { type CreationReference } from "@hanger-source/dsh-reactive/kernel";
import { installReactiveProjection } from "./conversations/projections.ts";
import type { ProgramContract } from "@hanger-source/dsh-reactive/kernel";
import { type NativeDelegations } from "./delegations.ts";
import type { HostOptions } from './applications/options.ts';
import { ConversationHandle, type ServiceConversation } from './conversations/handle.ts';
import type { ConversationSession } from './conversations/session.ts';
import { ConversationSessions } from './conversations/sessions.ts';
import { ConversationObservations } from './conversations/observations.ts';
import { ConversationInputs } from './conversations/inputs.ts';
import { ActionAuthority } from './execution/authority.ts';
import { ProgramTools } from './execution/tools.ts';
import { OperationParticipation } from './operations/participation.ts';
import { ConversationPrograms } from './programs/preparation.ts';
import { ConversationQuestions } from './participation/questions.ts';
/** 应用入口只组合能力域，执行和事实由对应owner持有。 */
export class ApplicationHost<E extends readonly AnyEnvironment[] = readonly AnyEnvironment[]> {
    #lifetime = new AbortController();
    #closing?: Promise<void>;
    private readonly sessions: ConversationSessions<E>;
    private readonly facts: ConversationObservations;
    private readonly inputs: ConversationInputs;
    private readonly actions: ActionAuthority;
    private readonly operations: OperationParticipation;
    private readonly questions: ConversationQuestions;
    private readonly programs: ConversationPrograms;
    private readonly stopDelegationUpdates: () => void;
    private constructor(ctx: Context, definition: ApplicationDefinition<E>, private readonly workspaces: Workspaces, options: HostOptions, private readonly delegations: NativeDelegations) {
        installReactiveProjection(ctx);
        const signal = this.#lifetime.signal;
        this.facts = new ConversationObservations(ctx, workspaces, signal, () => this.sessions);
        this.sessions = new ConversationSessions(ctx, definition, workspaces, options, signal, this.facts, delegations, () => this.programs, () => this.inputs, () => this.operations);
        this.inputs = new ConversationInputs(ctx, definition, workspaces, options, signal, this.sessions, this.facts, delegations, () => this.operations);
        this.actions = new ActionAuthority(ctx, options, signal, workspaces, this.sessions, this.facts, this.inputs, delegations, (agent, continuation, wait) => this.operations.registerWait(agent, continuation, wait));
        this.operations = new OperationParticipation(ctx, definition, options, signal, this.sessions, this.facts, this.inputs, this.actions, delegations, agent => this.programs.scope(agent));
        const tools = new ProgramTools(ctx, workspaces, this.sessions, this.actions, this.operations);
        this.programs = new ConversationPrograms(ctx, this.sessions, this.inputs, this.actions, this.operations, tools, delegations);
        this.questions = new ConversationQuestions(ctx, options, signal, this.sessions, this.facts);
        delegations.install();
        this.stopDelegationUpdates = this.operations.installDelegationUpdates();
        this.sessions.installDisposed();
        this.facts.install(entry => this.inputs.retainPausedResults(entry));
        this.inputs.installClaimed();
        this.programs.installAssembly();
        this.inputs.installPreStep();
        tools.installPolicies();
        ctx.effect(() => () => this.dispose());
    }
    /** @internal 正式mount完成依赖后构造同一应用。 */
    static create<E extends readonly AnyEnvironment[]>(ctx: Context, definition: ApplicationDefinition<E>, workspaces: Workspaces, options: HostOptions, delegations: NativeDelegations): ApplicationHost<E> {
        return new ApplicationHost(ctx, definition, workspaces, options, delegations);
    }
    initialize(): Promise<void> { return this.sessions.initialize(); }
    private handle<P extends ProgramContract = ProgramContract>(session: ConversationSession, participant: 'human' | 'service' = 'human'): ConversationHandle<P> {
        return ConversationHandle.create<P>(session, participant, this.sessions, this.inputs, this.actions, this.operations, this.questions, this.facts);
    }
    async create<Id extends E[number]['id']>(environmentId: Id, options: ConversationCreateOptions): Promise<ConversationHandle<EnvironmentAt<E[number], Id>['program']['definition']>> {
        return this.handle<EnvironmentAt<E[number], Id>['program']['definition']>(await this.sessions.create(environmentId, options));
    }
    createWorkspace(input: WorkspaceRegistration): Promise<WorkspaceInfo> { return this.sessions.createWorkspace(input); }
    describe(): Promise<readonly WorkspaceInfo[]> { return this.sessions.describe(); }
    open(sessionId: string): Promise<ConversationHandle>;
    open<P extends ProgramContract>(sessionId: string, expected: P): Promise<ConversationHandle<P>>;
    async open(sessionId: string, expected?: ProgramContract): Promise<ConversationHandle> {
        const session = await this.sessions.readSession(sessionId);
        this.sessions.expectProgram(session, expected);
        return this.handle(session);
    }
    async completeCreation(reference: CreationReference): Promise<ConversationHandle> { return this.handle(await this.sessions.completeCreation(reference)); }
    resume(sessionId: string): Promise<ConversationHandle>;
    resume<P extends ProgramContract>(sessionId: string, expected: P): Promise<ConversationHandle<P>>;
    async resume(sessionId: string, expected?: ProgramContract): Promise<ConversationHandle> {
        const session = await this.sessions.readSession(sessionId);
        this.sessions.expectProgram(session, expected);
        await this.sessions.activate(sessionId);
        if (session.child)
            await this.sessions.recoverChild(session);
        await this.operations.reconcileOperations(this.sessions.live(this.sessions.require(sessionId)));
        return this.handle(session);
    }
    conversation(sessionId: string): ConversationHandle;
    conversation<P extends ProgramContract>(sessionId: string, expected: P): ConversationHandle<P>;
    conversation(sessionId: string, expected?: ProgramContract): ConversationHandle {
        const session = this.sessions.require(sessionId);
        this.sessions.expectProgram(session, expected);
        return this.handle(session);
    }
    async service<P extends ProgramContract>(sessionId: string, expected: P): Promise<ServiceConversation<P>> {
        this.sessions.assertServiceActor();
        const session = await this.sessions.readSession(sessionId);
        this.sessions.expectProgram(session, expected);
        return this.handle<P>(session, 'service');
    }
    operationDefinition(kind: string, version: number): OperationContract { return this.operations.operationDefinition(kind, version); }
    attachOperation<D extends OperationContract>(sessionId: string, definition: D, reference: OperationReference<D>, options: SubmitOptions = {}, participant: 'human' | 'service' = 'human'): Promise<OperationHandle<D>> {
        return this.operations.attachOperation(sessionId, definition, reference, options, participant);
    }
    locateOperation<D extends OperationContract>(sessionId: string, definition: D, command: OperationCommand<D>, options: SubmitOptions = {}, participant: 'human' | 'service' = 'human'): Promise<OperationLocation<OperationReference<D>, OperationState<D>>> {
        return this.operations.locateOperation(sessionId, definition, command, options, participant);
    }
    readOperationFeedback<D extends OperationContract>(sessionId: string, definition: D, command: OperationCommand<D>, id: string, options: SubmitOptions = {}, participant: 'human' | 'service' = 'human'): Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>> {
        return this.operations.readOperationFeedback(sessionId, definition, command, id, options, participant);
    }
    notifyOperation(sessionId: string, followId: string, options: SubmitOptions = {}): Promise<OperationNotificationResult> { return this.operations.notifyOperation(sessionId, followId, options); }
    close(sessionId: string): Promise<void> { return this.sessions.close(sessionId); }
    dispose(): Promise<void> {
        if (this.#closing) return this.#closing;
        const completion = Promise.withResolvers<void>();
        this.#closing = completion.promise;
        void (async () => {
            this.stopDelegationUpdates();
            this.#lifetime.abort(new Error('REACTIVE_HOST_DISPOSED'));
            const failures = await this.sessions.shutdownNative();
            try {
                await this.sessions.stopPresets();
            }
            catch (error) {
                failures.push(error);
            }
            failures.push(...this.sessions.releasePrograms());
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
