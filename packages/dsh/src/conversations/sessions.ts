import { randomUUID } from "node:crypto";
import { BehaviorSubject, map, distinctUntilChanged } from 'rxjs';
import { isAbsolute } from "node:path";
import type { Context } from "@deepseek-ai/cordis";
import type { Agent, AgentHandle } from "@deepseek-ai/dsh-agent";
import { SessionId, pluginRecordOf } from "@deepseek-ai/dsh-session";
import { SessionAlreadyExistsError, type SessionInspection } from "@deepseek-ai/dsh-session-persistence";
import { Conversation, conversationBindings, type WorkspaceInfo, type WorkspaceRegistration, type ConversationCreateOptions } from "@hanger-source/dsh-reactive/kernel";
import { type ApplicationDefinition } from "@hanger-source/dsh-reactive/kernel";
import { type AnyEnvironment } from "@hanger-source/dsh-reactive/kernel";
import { EnvironmentPresets } from "../programs/presets.ts";
import { canonical } from "./records.ts";
import { type Workspaces, type ManagedWorkspace } from "../workspaces/workspaces.ts";
import { conversationCreateOptionsSchema, creationReferenceSchema, workspaceRegistrationSchema, type CreationReference } from "@hanger-source/dsh-reactive/kernel";
import type { ProgramContract } from "@hanger-source/dsh-reactive/kernel";
import type { ProgramImplementation } from "@hanger-source/dsh-reactive/kernel";
import { type NativeDelegations, type DelegationRecord } from "../delegations.ts";
import type { HostOptions } from '../applications/options.ts';
import type { ConversationSession, LiveConversation } from './session.ts';
import type { ConversationObservations } from './observations.ts';
import type { ConversationInputs } from './inputs.ts';
import type { ConversationPrograms } from '../programs/preparation.ts';
import { registerNativeSession } from './session-access.ts';
export class CreationIncompleteError extends Error {
    constructor(readonly reference: CreationReference, cause: unknown) {
        super('REACTIVE_WORKSPACE_CREATION_INCOMPLETE', { cause });
    }
}
/** @internal 冷程序图和真实Agent挂载分别保留身份；只持有本应用实际取得的原生句柄。 */
export class ConversationSessions<E extends readonly AnyEnvironment[] = readonly AnyEnvironment[]> {
    #entries = new Map<Agent, LiveConversation>();
    #sessions = new Map<string, ConversationSession>();
    #starting = new Map<string, { readonly kind: 'create' | 'activate'; readonly completion: Promise<void> }>();
    #owned = new Map<string, AgentHandle>();
    #cleanupFailures = new Set<unknown>();
    #rootAdmissions = new WeakMap<Agent, {
        workspace: ManagedWorkspace;
        environment: AnyEnvironment;
    }>();
    #presets: EnvironmentPresets;
    constructor(private readonly ctx: Context, private readonly definition: ApplicationDefinition<E>, private readonly workspaces: Workspaces, private readonly options: HostOptions, private readonly signal: AbortSignal, private readonly facts: ConversationObservations, private readonly delegations: NativeDelegations, private readonly programs: () => ConversationPrograms, private readonly inputs: () => ConversationInputs) {
        this.#presets = new EnvironmentPresets(ctx, definition.name);
    }
    private assertActive() { if (this.signal.aborted)
        throw new Error('REACTIVE_HOST_DISPOSED'); }
    get(id: string) { return this.#sessions.get(id); }
    forAgent(agent: Agent) { return this.#entries.get(agent); }
    async recoverChild(session: ConversationSession): Promise<void> {
        if (!session.child)
            return;
        const parent = this.ctx.agents.get(SessionId(session.child.command.parentSessionId));
        if (!parent)
            throw new Error('REACTIVE_CHILD_PARENT_NOT_RUNNING');
        await this.delegations.recoverLive(this.live(session).agent, parent, this.signal);
    }
    require(id: string): ConversationSession {
        this.assertActive();
        const session = this.#sessions.get(id);
        if (!session)
            throw new Error('REACTIVE_CONVERSATION_NOT_OPEN');
        if (session.live.value) {
            const native = session.live.value.agent.session;
            this.assertCreated({ meta: native.header, events: native.snapshotEvents(), inheritedEventCount: native.inheritedEventCount }, session.workspace, session.environment);
        }
        return session;
    }
    private assertCreated(inspection: SessionInspection, workspace: ManagedWorkspace, environment: AnyEnvironment): void {
        if (this.#starting.get(inspection.meta.id)?.kind === 'create' || !this.workspaces.completed(inspection, workspace, environment)) {
            throw new CreationIncompleteError({ environmentId: environment.id, workspaceId: workspace.native.id, sessionId: inspection.meta.id },
                new Error('REACTIVE_CREATION_RECEIPT_REQUIRED'));
        }
    }
    async initialize(): Promise<void> {
        await this.#presets.initialize(this.workspaces.environments(), (agent, environment, signal) => this.attach(agent, environment, signal));
    }
    installDisposed(): void {
        this.ctx.on('agent/disposed', ({ agent }) => {
            const entry = this.#entries.get(agent);
            this.#entries.delete(agent);
            if (!entry || entry.session.live.value !== entry)
                return;
            entry.session.live.next(undefined);
            entry.session.conversation[conversationBindings]().resetProgress();
            entry.session.conversation[conversationBindings]().publishForegroundQuestions([]);
            this.facts.publish(entry.session);
            try { this.collect(entry.session); }
            catch (error) {
                // 原生disposed通知不等待监听者；图已退出，错误仍由Host释放时交付。
                this.#cleanupFailures.add(error);
                throw error;
            }
        });
    }
    private async releaseNative(handle: AgentHandle, entry: LiveConversation | undefined): Promise<void> {
        const failures: unknown[] = [];
        let released = false;
        try { await handle.dispose(); released = true; } catch (error) { failures.push(error); }
        try { await entry?.dispose(); } catch (error) { failures.push(error); }
        if (entry) {
            try { this.collect(entry.session); } catch (error) { failures.push(error); }
        }
        if (released && this.#owned.get(handle.agent.id) === handle) this.#owned.delete(handle.agent.id);
        if (failures.length === 1) throw failures[0];
        if (failures.length) throw new AggregateError(failures, 'REACTIVE_NATIVE_RELEASE_FAILED');
    }
    async shutdownNative(): Promise<unknown[]> {
        await Promise.allSettled([...this.#starting.values()].map(starting => starting.completion));
        const owned = [...this.#owned.values()].map(handle => ({ handle, entry: this.#entries.get(handle.agent) })), failures: unknown[] = [];
        try {
            await this.ctx.get('subagents')?.drainDescendants(owned.map(({ handle }) => handle.agent));
        }
        catch (error) {
            failures.push(error);
        }
        for (const { handle } of owned)
            handle.agent.cancel({ kind: 'disposed' });
        const results = await Promise.allSettled(owned.map(async ({ handle, entry }) => {
            const failures: unknown[] = [];
            try { await this.settle(handle.agent); } catch (error) { failures.push(error); }
            try { await this.releaseNative(handle, entry); } catch (error) { failures.push(error); }
            if (failures.length === 1) throw failures[0];
            if (failures.length) throw new AggregateError(failures, 'REACTIVE_NATIVE_SHUTDOWN_FAILED');
        }));
        this.#owned.clear();
        failures.push(...results.flatMap(result => result.status === 'rejected' ? [result.reason] : []));
        return failures;
    }
    stopPresets(): Promise<void> { return this.#presets.dispose(); }
    releasePrograms(): unknown[] {
        const failures: unknown[] = [...this.#cleanupFailures];
        this.#cleanupFailures.clear();
        for (const entry of this.#sessions.values()) {
            try {
                entry.scope.dispose();
            }
            catch (error) {
                failures.push(error);
            }
        }
        this.#sessions.clear();
        return failures;
    }
    collect(entry: ConversationSession) {
        if (entry.live.value || entry.pins !== 0)
            return;
        if (this.#sessions.get(entry.header.id) === entry)
            this.#sessions.delete(entry.header.id);
        entry.scope.dispose();
    }
    release(entry: ConversationSession) { entry.pins--; this.collect(entry); }
    live(entry: ConversationSession) {
        this.assertActive();
        if (!entry.live.value)
            throw new Error('REACTIVE_CONVERSATION_NOT_RUNNING');
        return entry.live.value;
    }
    private bindAgent(session: ConversationSession, agent: Agent): LiveConversation {
        if (session.live.value) throw new Error('REACTIVE_CONVERSATION_ALREADY_RUNNING');
        const entry = this.programs().createLive(agent, session);
        this.#entries.set(agent, entry);
        session.live.next(entry);
        return entry;
    }
    async compose(inspection: SessionInspection, workspace: ManagedWorkspace, environment: AnyEnvironment, implementation: ProgramImplementation = environment.program, child?: DelegationRecord, agent?: Agent): Promise<ConversationSession> {
        this.workspaces.verifyInspection(inspection, workspace, environment);
        if (!child && (inspection.meta.origin || inspection.meta.parentSession || inspection.meta.isSeeded))
            throw new Error('REACTIVE_ROOT_CONVERSATION_REQUIRED');
        const existing = this.#sessions.get(inspection.meta.id);
        if (existing) {
            if (existing.workspace !== workspace || existing.environment !== environment)
                throw new Error('REACTIVE_WORKSPACE_BINDING_MISMATCH');
            if (existing.implementation !== implementation)
                throw new Error('REACTIVE_PROGRAM_IMPLEMENTATION_MISMATCH');
            if (!existing.live.value) {
                if (agent) this.bindAgent(existing, agent);
                const { state, questions } = this.facts.stateOf(inspection);
                existing.conversation[conversationBindings]().accept(state.current);
                existing.conversation[conversationBindings]().publishQuestions(questions);
                existing.state = state;
                this.facts.publish(existing);
            } else if (agent) throw new Error('REACTIVE_CONVERSATION_ALREADY_RUNNING');
            return existing;
        }
        const { state, questions } = this.facts.stateOf(inspection);
        const scope = workspace.scope.child();
        let entry!: ConversationSession;
        try {
            const conversation = new Conversation(scope, { sessionId: inspection.meta.id, workspaceId: workspace.native.id,
                environmentId: environment.id, directory: workspace.native.path, parentSessionId: inspection.meta.parentSession ?? null }, workspace.context, implementation.definition, state.activity);
            conversation[conversationBindings]().accept(state.current);
            conversation[conversationBindings]().publishQuestions(questions);
            const program = this.programs().create(scope, conversation, implementation, workspace, () => entry);
            const views = Object.freeze(Object.fromEntries(Object.entries(program.outputs.views)
                .map(([name, value]) => [name, value.values$])));
            const live = new BehaviorSubject<LiveConversation | undefined>(undefined);
            scope.own(() => live.complete());
            entry = { scope, header: inspection.meta, workspace, environment, implementation, child, conversation, state, program, views, pins: 0, live };
            this.inputs().bindConversation(entry);
            this.registerSessionAccess(conversation, entry, program.signal, () => program.signal.throwIfAborted());
            if (agent) this.bindAgent(entry, agent);
            program.mount();
            this.#sessions.set(inspection.meta.id, entry);
            return entry;
        }
        catch (error) {
            const failures = [error];
            if (entry) {
                if (this.#sessions.get(inspection.meta.id) === entry) this.#sessions.delete(inspection.meta.id);
                const live = entry.live.value;
                if (live) {
                    if (this.#entries.get(live.agent) === live) this.#entries.delete(live.agent);
                    entry.live.next(undefined);
                }
            }
            try { scope.dispose(); } catch (cleanup) { failures.push(cleanup); }
            if (agent) {
                try { await this.programs().release(agent); } catch (cleanup) { failures.push(cleanup); }
            }
            if (failures.length > 1) throw new AggregateError(failures, 'REACTIVE_CONVERSATION_COMPOSE_FAILED');
            throw error;
        }
    }
    async attach(agent: Agent, environment: AnyEnvironment, nativeSignal: AbortSignal = this.signal) {
        this.assertActive();
        if (this.#entries.has(agent))
            return this.#entries.get(agent)!;
        const parentWorkspace = (record: DelegationRecord, parent: Agent) => {
            const parentLiveConversation = this.#entries.get(parent);
            if (this.ctx.agents.get(parent.id) !== parent || !parentLiveConversation || parentLiveConversation.session.environment !== environment
                || canonical(parentLiveConversation.session.conversation.identity) !== canonical(record.parent))
                throw new Error('REACTIVE_CHILD_PARENT_WORKSPACE_MISMATCH');
            this.workspaces.assertMember(parent.session, parentLiveConversation.session.workspace, environment);
            return parentLiveConversation.session.workspace;
        };
        const child = await this.delegations.created(agent, async (record, parent) => {
            parentWorkspace(record, parent);
            await this.inputs().revalidateCauses(record.causes, nativeSignal);
        }, nativeSignal);
        let workspace: ManagedWorkspace;
        if (child) {
            const parent = this.ctx.agents.get(SessionId(child.record.command.parentSessionId));
            if (!parent)
                throw new Error('REACTIVE_CHILD_PARENT_NOT_RUNNING');
            workspace = parentWorkspace(child.record, parent);
            const own = agent.session.snapshotEvents().slice(agent.session.inheritedEventCount);
            if (!own.some(event => pluginRecordOf(event)?.type === 'plugin:reactive/workspace')) {
                this.workspaces.bind(agent.session, workspace, environment);
                await this.workspaces.attach(agent.session, workspace, environment);
                this.workspaces.complete(agent.session, workspace, environment);
                await this.facts.flush(agent);
            }
            else {
                const inspection = { meta: agent.session.header, events: agent.session.snapshotEvents(), inheritedEventCount: agent.session.inheritedEventCount };
                if (!this.workspaces.completed(inspection, workspace, environment))
                    throw new Error('REACTIVE_CHILD_WORKSPACE_CREATION_INCOMPLETE');
                this.workspaces.assertMember(agent.session, workspace, environment);
            }
        }
        else {
            const admission = this.#rootAdmissions.get(agent);
            this.#rootAdmissions.delete(agent);
            if (!admission || admission.environment !== environment)
                throw new Error('REACTIVE_ROOT_CREATION_AUTHORITY_REQUIRED');
            const selected = this.workspaces.resolve({ meta: agent.session.header, events: agent.session.snapshotEvents(), inheritedEventCount: agent.session.inheritedEventCount });
            if (selected.environment !== environment || selected.workspace !== admission.workspace)
                throw new Error('REACTIVE_WORKSPACE_BINDING_MISMATCH');
            workspace = selected.workspace;
            const inspection = { meta: agent.session.header, events: agent.session.snapshotEvents(), inheritedEventCount: agent.session.inheritedEventCount };
            if (this.workspaces.completed(inspection, workspace, environment)) {
                // 恢复只核已有成员，不能重新授予已被原生 registry 撤销的关系。
                this.workspaces.assertMember(agent.session, workspace, environment);
            } else {
                // 首次 Program 求值前先持久登记归属；最终创建回执仍在实际 Agent setup 完成后提交。
                try {
                    await this.facts.flush(agent);
                    await this.workspaces.attach(agent.session, workspace, environment);
                } catch (cause) {
                    throw new CreationIncompleteError({ environmentId: environment.id, workspaceId: workspace.native.id, sessionId: agent.id }, cause);
                }
            }
        }
        try {
            const session = await this.compose({ meta: agent.session.header, events: agent.session.snapshotEvents(),
                inheritedEventCount: agent.session.inheritedEventCount }, workspace, environment, child?.implementation ?? environment.program, child?.record, agent);
            const entry = session.live.value;
            if (!entry || entry.agent !== agent) throw new Error('REACTIVE_AGENT_NOT_BOUND');
            await this.programs().attach(entry);
            this.facts.publish(session);
            return entry;
        }
        catch (error) {
            const entry = this.#entries.get(agent);
            this.#entries.delete(agent);
            if (entry) entry.session.live.next(undefined);
            const failures = [error];
            try { await this.programs().release(agent); } catch (cleanup) { failures.push(cleanup); }
            if (entry) {
                try { this.collect(entry.session); } catch (cleanup) { failures.push(cleanup); }
            }
            if (failures.length > 1) throw new AggregateError(failures, 'REACTIVE_CONVERSATION_ATTACH_FAILED');
            throw error;
        }
    }
    assertSessionWorkspace(session: ConversationSession) {
        this.assertActive();
        session.scope.assertActive();
        if (session.live.value)
            this.workspaces.assertMember(session.live.value.agent.session, session.workspace, session.environment);
        else
            this.workspaces.assertMemberHeader(session.header, session.workspace, session.environment);
    }
    registerSessionAccess(handle: object, session: ConversationSession, signal: AbortSignal, assertHandle: () => void) {
        const assertCurrent = () => {
            signal.throwIfAborted();
            assertHandle();
            this.assertSessionWorkspace(session);
        };
        registerNativeSession(handle, { context: this.ctx, sessionId: session.header.id, signal, assertCurrent,
            workspaceRoot: () => {
                assertCurrent();
                const directory = session.header.cwd;
                if (!directory) throw new Error('REACTIVE_NATIVE_FILE_WORKSPACE_REQUIRED');
                return directory;
            },
            liveAgent$: session.live.pipe(map(entry => entry?.agent), distinctUntilChanged()),
            liveAgent: () => session.live.value?.agent,
            inspect: async (callerSignal?: AbortSignal) => {
                const readSignal = callerSignal ? AbortSignal.any([signal, callerSignal]) : signal;
                readSignal.throwIfAborted();
                assertCurrent();
                const live = session.live.value;
                if (live) {
                    await this.facts.flush(live.agent);
                    readSignal.throwIfAborted();
                    assertCurrent();
                    if (session.live.value !== live || this.ctx.agents.get(live.agent.id) !== live.agent)
                        throw new Error('REACTIVE_CONVERSATION_NOT_RUNNING');
                }
                const inspection = await this.facts.inspect(session.header.id, readSignal);
                readSignal.throwIfAborted();
                assertCurrent();
                if (session.live.value !== live || live && this.ctx.agents.get(live.agent.id) !== live.agent)
                    throw new Error('REACTIVE_CONVERSATION_NOT_RUNNING');
                this.workspaces.verifyInspection(inspection, session.workspace, session.environment);
                return inspection;
            },
        });
    }
    async createWorkspace(input: WorkspaceRegistration): Promise<WorkspaceInfo> {
        this.assertActive();
        const registration = workspaceRegistrationSchema.parse(input);
        if (!isAbsolute(registration.directory))
            throw new Error('REACTIVE_WORKSPACE_PATH_NOT_ABSOLUTE');
        return this.workspaces.register(registration, id => this.facts.inspect(id));
    }
    async describe(): Promise<readonly WorkspaceInfo[]> {
        this.assertActive();
        return this.workspaces.describe(id => this.facts.inspect(id));
    }
    async mountRoot(native: Context, agent: Agent, workspace: ManagedWorkspace, environment: AnyEnvironment) {
        this.#rootAdmissions.set(agent, { workspace, environment });
        try {
            await this.ctx.agentPresets.mount(native, this.workspaces.presetId(environment));
        }
        catch (error) {
            this.#rootAdmissions.delete(agent);
            throw error;
        }
    }
    async createRegistered(workspace: ManagedWorkspace, environment: AnyEnvironment, sessionId: string, create: () => Promise<AgentHandle>) {
        this.assertActive();
        if (this.#starting.has(sessionId))
            throw new Error('REACTIVE_CONVERSATION_ACTIVATION_IN_PROGRESS');
        const starting = (async () => {
            let handle: AgentHandle | undefined;
            try {
                handle = await create();
                this.#owned.set(sessionId, handle);
                this.assertActive();
                this.workspaces.assertMember(handle.agent.session, workspace, environment);
                this.workspaces.complete(handle.agent.session, workspace, environment);
                await this.facts.flush(handle.agent);
            }
            catch (cause) {
                if (!handle) {
                    if (cause instanceof SessionAlreadyExistsError)
                        throw new Error('REACTIVE_CONVERSATION_EXISTS', { cause });
                    throw cause;
                }
                let failure = cause;
                try {
                    await this.releaseNative(handle, this.#entries.get(handle.agent));
                }
                catch (cleanup) {
                    failure = new AggregateError([cause, cleanup], 'REACTIVE_CREATION_CLEANUP_FAILED');
                }
                throw new CreationIncompleteError({ environmentId: environment.id, workspaceId: workspace.native.id, sessionId }, failure);
            }
        })();
        this.#starting.set(sessionId, { kind: 'create', completion: starting });
        try {
            await starting;
        }
        finally {
            this.#starting.delete(sessionId);
        }
    }
    async create<Id extends E[number]['id']>(environmentId: Id, raw: ConversationCreateOptions): Promise<ConversationSession> {
        this.assertActive();
        const options = conversationCreateOptionsSchema.parse(raw), sessionId = options.sessionId ?? randomUUID();
        const environment = this.workspaces.environment(environmentId), workspace = this.workspaces.get(options.workspaceId);
        const presetId = this.workspaces.presetId(environment);
        await this.createRegistered(workspace, environment, sessionId, () => this.ctx.agents.create({ sessionId: SessionId(sessionId),
            meta: { cwd: workspace.native.path, agentPreset: presetId }, agentOptions: this.options.model,
            setup: async (native, agent) => {
                this.workspaces.bind(agent.session, workspace, environment);
                await this.mountRoot(native, agent, workspace, environment);
            } }));
        return this.#sessions.get(sessionId)!;
    }
    async readSession(sessionId: string, expected?: ProgramContract) {
        this.assertActive();
        const live = this.#sessions.get(sessionId);
        if (live) this.expectProgram(live, expected);
        if (live?.live.value) {
            this.require(sessionId);
            this.workspaces.assertMember(live.live.value.agent.session, live.workspace, live.environment);
            return live;
        }
        const inspection = await this.facts.inspect(sessionId);
        this.assertActive();
        const { workspace, environment } = this.workspaces.resolve(inspection);
        if (!workspace.native.sessionIds.includes(SessionId(sessionId)))
            throw new Error('REACTIVE_WORKSPACE_MEMBERSHIP_REQUIRED');
        this.assertCreated(inspection, workspace, environment);
        const child = inspection.meta.parentSession ? await this.delegations.describeChild(inspection) : undefined;
        this.assertActive();
        const implementation = child?.implementation ?? environment.program;
        if (expected && implementation.definition !== expected) throw new Error('REACTIVE_PROGRAM_CONTRACT_MISMATCH');
        return this.compose(inspection, workspace, environment, implementation, child?.record);
    }
    async completeCreation(raw: CreationReference): Promise<ConversationSession> {
        this.assertActive();
        const reference = creationReferenceSchema.parse(raw);
        const workspace = this.workspaces.get(reference.workspaceId), environment = this.workspaces.environment(reference.environmentId);
        const inspection = await this.facts.inspect(reference.sessionId);
        this.workspaces.verifyInspection(inspection, workspace, environment);
        if (this.workspaces.completed(inspection, workspace, environment)) {
            if (!workspace.native.sessionIds.includes(SessionId(reference.sessionId)))
                throw new Error('REACTIVE_WORKSPACE_MEMBERSHIP_REQUIRED');
            return this.readSession(reference.sessionId);
        }
        this.workspaces.assertCreation(inspection, workspace, environment);
        await this.createRegistered(workspace, environment, reference.sessionId, () => this.ctx.agents.resume({ resumeSessionId: SessionId(reference.sessionId),
            agentOptions: this.options.model, setup: async (native, agent) => {
                this.workspaces.assertCreation({ meta: agent.session.header, events: agent.session.snapshotEvents(),
                    inheritedEventCount: agent.session.inheritedEventCount }, workspace, environment);
                await this.mountRoot(native, agent, workspace, environment);
            } }));
        return this.require(reference.sessionId);
    }
    async activate(sessionId: string): Promise<AgentHandle | undefined> {
        const pending = this.#starting.get(sessionId);
        if (pending) {
            await pending.completion;
            return;
        }
        if (this.#sessions.get(sessionId)?.live.value)
            return;
        let acquired: AgentHandle | undefined;
        const starting = (async () => {
            const session = await this.readSession(sessionId);
            let handle: AgentHandle | undefined;
            try {
                if (session.child) {
                    await this.activate(session.child.command.parentSessionId);
                    if (session.live.value)
                        return;
                    const parent = this.live(this.#sessions.get(session.child.command.parentSessionId)!).agent;
                    await this.inputs().revalidateCauses(session.child.causes, this.signal);
                    await this.delegations.resume(session.child, parent, this.signal);
                    if (!session.live.value)
                        throw new Error('REACTIVE_CHILD_NOT_RUNNING');
                    return;
                }
                if (this.#owned.has(sessionId)) throw new Error('REACTIVE_CONVERSATION_OWNER_NOT_RELEASED');
                handle = await this.ctx.agents.resume({ resumeSessionId: SessionId(sessionId), agentOptions: this.options.model,
                    setup: async (native, agent) => {
                        this.workspaces.assertMember(agent.session, session.workspace, session.environment);
                        await this.mountRoot(native, agent, session.workspace, session.environment);
                    } });
                this.#owned.set(sessionId, handle);
                this.assertActive();
                await this.delegations.restoreChildren(handle.agent, this.signal);
                acquired = handle;
            }
            catch (error) {
                const failures = [error];
                if (handle) {
                    try { await this.rollbackActivation(handle); } catch (cleanup) { failures.push(cleanup); }
                }
                try { this.collect(session); } catch (cleanup) { failures.push(cleanup); }
                if (failures.length > 1) throw new AggregateError(failures, 'REACTIVE_CONVERSATION_ACTIVATION_FAILED');
                throw error;
            }
        })();
        this.#starting.set(sessionId, { kind: 'activate', completion: starting });
        try {
            await starting;
            return acquired;
        }
        finally {
            this.#starting.delete(sessionId);
        }
    }
    /** 只归还本次启动者实际取得的引用，不能按sessionId关闭别人已有的Agent。 */
    async rollbackActivation(handle: AgentHandle): Promise<void> {
        const sessionId = handle.agent.id;
        if (this.#owned.get(sessionId) !== handle) throw new Error('REACTIVE_CONVERSATION_NOT_OWNED');
        const entry = this.#entries.get(handle.agent);
        const failures: unknown[] = [];
        handle.agent.cancel({ kind: 'disposed' });
        try { await this.ctx.get('subagents')?.drainDescendants([handle.agent]); } catch (error) { failures.push(error); }
        try { await this.releaseNative(handle, entry); } catch (error) { failures.push(error); }
        if (failures.length === 1) throw failures[0];
        if (failures.length) throw new AggregateError(failures, 'REACTIVE_CONVERSATION_ACTIVATION_ROLLBACK_FAILED');
    }
    expectProgram(session: ConversationSession, expected?: ProgramContract) {
        if (expected && session.implementation.definition !== expected)
            throw new Error('REACTIVE_PROGRAM_CONTRACT_MISMATCH');
    }
    checkpoint(agent: Agent) { return this.facts.flush(agent); }
    async pauseGoal(agent: Agent) {
        const goal = this.ctx.goals.get(agent);
        if (!goal)
            throw new Error('REACTIVE_GOAL_ABSENT');
        const paused = this.ctx.goals.pause(agent, goal);
        await this.facts.flush(agent);
        return paused;
    }
    async cancelAgent(agent: Agent) {
        // 本次取消先同步截断当前 step；后续 pause 不能把它误作仍可保留的处理中输入。
        const prompts = [...agent.inbox.nextTurn, ...agent.inbox.nextStep, ...(this.#entries.get(agent)?.claimed ?? [])]
            .flatMap(message => message.source.kind === 'user'
            && 'rpcId' in message.source && typeof message.source.rpcId === 'string' ? [message.source.rpcId] : []);
        agent.cancel({ kind: 'user' });
        for (const requestId of new Set(prompts))
            this.ctx.fileUploads.retirePrompt(agent, requestId);
        const subagents = this.ctx.get('subagents');
        if (subagents) {
            const descendants = await subagents.listDescendants(agent.id);
            for (const child of descendants)
                if ('id' in child && typeof child.id === 'string')
                    subagents.interrupt(SessionId(child.id), { kind: 'ancestor', agent });
        }
        await agent.whenIdle();
        await this.facts.flush(agent);
    }
    async settle(agent: Agent, signal?: AbortSignal) {
        signal?.throwIfAborted();
        const entry = this.#entries.get(agent);
        const failures: unknown[] = [];
        try {
            for (;;) {
                await agent.whenIdle();
                signal?.throwIfAborted();
                const children = await this.ctx.get('subagents')?.waitForChildren(agent);
                signal?.throwIfAborted();
                if (!children)
                    break;
            }
            if (entry) {
                while (entry.recovery.size) {
                    await Promise.all(entry.recovery);
                    signal?.throwIfAborted();
                }
                signal?.throwIfAborted();
                if (!entry.recoveryErrors.length && this.ctx.agents.get(agent.id) === agent) {
                    await this.facts.flush(agent);
                    signal?.throwIfAborted();
                }
            }
        }
        catch (error) { failures.push(error); }
        signal?.throwIfAborted();
        if (entry?.scope.disposed) {
            try { await entry.closed; } catch (error) { failures.push(error); }
            signal?.throwIfAborted();
        }
        // 最后一个异步阶段之后才领取owner错误，退出的观察者不会消费仍需交付的失败。
        signal?.throwIfAborted();
        if (entry?.recoveryErrors.length)
            failures.unshift(new AggregateError(entry.recoveryErrors.splice(0), 'REACTIVE_INPUT_RECOVERY_FAILED'));
        if (failures.length === 1) throw failures[0];
        if (failures.length) throw new AggregateError(failures, 'REACTIVE_CONVERSATION_SETTLE_FAILED');
    }
    async close(sessionId: string) {
        const session = this.#sessions.get(sessionId);
        if (session?.child) {
            const entry = session.live.value;
            const parent = this.ctx.agents.get(SessionId(session.child.command.parentSessionId));
            if (!parent)
                throw new Error('REACTIVE_CHILD_PARENT_NOT_RUNNING');
            const failures: unknown[] = [];
            try { await this.ctx.subagents.drainChildren(parent, [SessionId(sessionId)]); } catch (error) { failures.push(error); }
            try { await entry?.dispose(); } catch (error) { failures.push(error); }
            if (failures.length === 1) throw failures[0];
            if (failures.length) throw new AggregateError(failures, 'REACTIVE_CHILD_CLOSE_FAILED');
            return;
        }
        const handle = this.#owned.get(sessionId);
        if (!handle)
            throw new Error('REACTIVE_CONVERSATION_NOT_OWNED');
        const entry = this.#entries.get(handle.agent);
        const failures: unknown[] = [];
        try { await this.ctx.get('subagents')?.drainDescendants([handle.agent]); } catch (error) { failures.push(error); }
        try { await this.settle(handle.agent); } catch (error) { failures.push(error); }
        try { await this.releaseNative(handle, entry); } catch (error) { failures.push(error); }
        if (failures.length === 1) throw failures[0];
        if (failures.length) throw new AggregateError(failures, 'REACTIVE_CONVERSATION_CLOSE_FAILED');
    }
}
