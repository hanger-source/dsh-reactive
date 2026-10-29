import type { Context } from "@deepseek-ai/cordis";
import type { Agent } from "@deepseek-ai/dsh-agent";
import { firstValueFrom } from 'rxjs';
import { appendPluginRecord } from "@deepseek-ai/dsh-session";
import { z } from "zod";
import { type Continuation, type WaitReference, type SubmitOptions, type GoalReference } from "@hanger-source/dsh-reactive/kernel";
import { type ApplicationDefinition } from "@hanger-source/dsh-reactive/kernel";
import { type ActionExecutionContext, type ActionReference } from "@hanger-source/dsh-reactive/kernel";
import { type BoundOffer, type BoundFollow } from "@hanger-source/dsh-reactive/kernel";
import { OperationFollows, operationFollowId } from "./follows.ts";
import type { ActionOperationEvent } from "@hanger-source/dsh-reactive/kernel";
import { attachOperation as attachOwnedOperation, locateOperation as locateOwnedOperation, operationBindingOwner, operationBindingCommand, bindOperation, type OperationAccessContext as OperationAccess, type OperationContract, type OperationHandle, type OperationReference, type OperationCommand, type OperationLocation, type OperationState, type OperationNotificationResult, type OperationBindingReference, type BoundOperation, type OperationFeedbackRead, type OperationFeedbackPayload } from "@hanger-source/dsh-reactive/kernel";
import { actionOperationOwner, matchesOperationFollowResult } from "@hanger-source/dsh-reactive/kernel";
import { abortable } from '@hanger-source/dsh-reactive';
import { canonical, waitSchema } from "../conversations/records.ts";
import { type AdmittedSubmission } from "@hanger-source/dsh-reactive/kernel";
import type { ProgramImplementation } from "@hanger-source/dsh-reactive/kernel";
import { surfaceSlotDefinition } from "@hanger-source/dsh-reactive/kernel";
import { type NativeDelegations } from "../delegations.ts";
import type { HostOptions } from '../applications/options.ts';
import { retainRecovery, type ConversationSession, type LiveConversation } from '../conversations/session.ts';
import type { ConversationSessions } from '../conversations/sessions.ts';
import type { ConversationObservations } from '../conversations/observations.ts';
import type { ConversationInputs } from '../conversations/inputs.ts';
import type { ActionAuthority } from '../execution/authority.ts';
import { humanParticipant, serviceParticipant, assertParticipantOptions, participantActor, type ParticipantIdentity } from '../participation/identity.ts';
import { retainEligibility, withOperationAccessSettlement, type OperationOwnerReference } from '@hanger-source/dsh-reactive/kernel';
/** 可靠操作参与与精确 Goal 续接；Operation 事实仍由业务 owner 持有。 */
export class OperationParticipation {
    #follows: OperationFollows;
    #notifications = new Set<Promise<unknown>>();
    constructor(private readonly ctx: Context, private readonly definition: ApplicationDefinition, private readonly options: HostOptions, private readonly signal: AbortSignal, private readonly sessions: ConversationSessions, private readonly facts: ConversationObservations, private readonly inputs: ConversationInputs, private readonly authority: ActionAuthority, private readonly delegations: NativeDelegations) {
        this.#follows = new OperationFollows(ctx, [...this.options.operations ?? [], delegations.owner], {
            publisherId: () => {
                const actorId = this.options.serviceActorId;
                if (!actorId?.trim())
                    throw new Error('REACTIVE_OPERATION_PUBLISHER_REQUIRED');
                return actorId;
            },
            recoveryAccess: agent => {
                const entry = this.sessions.forAgent(agent);
                if (!entry) throw new Error('REACTIVE_AGENT_NOT_BOUND');
                return this.operationRecoveryAccess(entry);
            },
            retainRecovery: (agent, work, signal) => {
                const entry = this.sessions.forAgent(agent);
                if (!entry) throw new Error('REACTIVE_AGENT_NOT_BOUND');
                return retainRecovery(entry, work, 'reactive.operation.recovery.failed', {}, signal);
            },
            checkpoint: agent => this.facts.flush(agent),
            wait: (agent, continuation, wait) => this.registerWait(agent, continuation, wait),
            resolved: (agent, link) => {
                const record = this.facts.liveState(agent.session).record('operation:' + link.id);
                return matchesOperationFollowResult(link, record);
            },
            waiting: (agent, link) => this.facts.liveState(agent.session).activity.waits.some(item => canonical(item.wait) === canonical(link.wait)
                && canonical(item.continuation) === canonical(link.continuation)),
            admit: (link, source, signal) => {
                const work = this.definition.admit({
                    requestId: 'operation:' + link.id, content: [{ type: 'text', text: '核验原业务操作的结算状态' }], intent: 'result', scope: link.scope,
                    result: { continuation: link.continuation, wait: link.wait, ready: false },
                }, this.sessions.get(link.continuation.sessionId)!.conversation.identity, { actorId: source.actorId, participant: source.participant, signal });
                this.retainNotification(work);
                return work;
            },
            deliver: (agent, input, actorId, signal) => this.inputs.submitSession(this.sessions.forAgent(agent)!.session, input,
                { signal }, serviceParticipant(actorId), work => this.retainNotification(work)),
        });
    }
    private assertActive() { if (this.signal.aborted)
        throw new Error('REACTIVE_HOST_DISPOSED'); }
    /** Session调用保留原图；按id调用才是明确新取得，不能在旧句柄的await之后重签资格。 */
    private async acquireSession(target: string | ConversationSession, signal?: AbortSignal): Promise<ConversationSession> {
        this.assertActive();
        signal?.throwIfAborted();
        const session = typeof target === 'string' ? await this.sessions.readSession(target) : target;
        signal?.throwIfAborted();
        this.sessions.assertSessionWorkspace(session);
        return session;
    }
    installDelegationUpdates(): () => void {
        const changes = this.delegations.updates$.subscribe(({ parentSessionId }) => {
            const entry = this.sessions.get(parentSessionId)?.live.value;
            if (!entry)
                return;
            const recovery = this.reconcileOperations(entry).catch(error => {
                entry.recoveryErrors.push(error);
                console.error('reactive.child.follow-reconcile.failed', { sessionId: parentSessionId, error });
            }).finally(() => { entry.recovery.delete(recovery); });
            entry.recovery.add(recovery);
        });
        const stopDelegationUpdates = () => changes.unsubscribe();
        this.ctx.effect(() => stopDelegationUpdates);
        return stopDelegationUpdates;
    }
    hasPendingGoal(agent: Agent, goal: GoalReference) { return this.#follows.hasPendingGoal(agent, goal); }
    forDefinition<D extends OperationContract>(definition: D) { return this.#follows.forDefinition(definition); }
    validateFollow(follow: Pick<BoundFollow, 'scope' | 'operation' | 'when'>, implementation: ProgramImplementation): void {
        if (!this.options.serviceActorId?.trim())
            throw new Error('REACTIVE_OPERATION_PUBLISHER_REQUIRED');
        implementation.definition.input.parse(follow.scope);
        this.#follows.validate(operationBindingOwner(follow.operation), follow.when);
    }
    validateOffer(offer: Pick<BoundOffer, 'action' | 'follow'>, implementation: ProgramImplementation): void {
        if (!offer.follow)
            return;
        if (!this.options.serviceActorId?.trim())
            throw new Error('REACTIVE_OPERATION_PUBLISHER_REQUIRED');
        implementation.definition.input.parse(offer.follow.scope);
        const owner = actionOperationOwner(offer.action);
        if (!owner)
            throw new Error('REACTIVE_ACTION_OPERATION_REQUIRED');
        this.#follows.validate(owner, offer.follow.when);
    }
    programAccess(session: ConversationSession, binding: OperationBindingReference): OperationAccess {
        this.sessions.assertSessionWorkspace(session);
        if (this.#follows.forDefinition(binding.definition) !== operationBindingOwner(binding))
            throw new Error('REACTIVE_OPERATION_OWNER_MISMATCH');
        return this.authority.programExecution(session, 'program-operation');
    }
    async participate(entry: LiveConversation, follow: BoundFollow, execution: ActionExecutionContext & {
        readonly continuation: Continuation;
        readonly source: Extract<ActionExecutionContext['source'], {
            readonly kind: 'tool';
        }>;
    }, after: string | null, allowed: () => boolean): Promise<string> {
        const owner = operationBindingOwner(follow.operation), command = operationBindingCommand(follow.operation);
        const id = operationFollowId(entry.agent.id, this.authority.callSequence(execution), owner, command);
        const program = entry.session.program, changed = new AbortController();
        const signal = AbortSignal.any([execution.signal, this.signal, entry.scope.signal, entry.session.scope.signal,
            program.signal, changed.signal]);
        this.authority.assertFollowExecution(entry, execution, signal);
        let eligibility: ReturnType<typeof retainEligibility> | undefined, stop: (() => void) | undefined, released = false;
        const release = () => {
            if (released) return;
            released = true;
            try { stop?.(); } finally { eligibility?.release(); }
        };
        try {
            eligibility = follow.eligibility === undefined ? undefined : retainEligibility(follow.name, follow.eligibility, signal);
            const admissionSignal = eligibility === undefined ? signal : AbortSignal.any([signal, eligibility.signal]);
            stop = entry.scope.effect(() => {
                program.revision.get();
                try { if (!allowed()) changed.abort(new Error('REACTIVE_CAPABILITY_UNAVAILABLE: ' + follow.name)); }
                catch (error) { changed.abort(error); }
            });
            const check = () => {
                this.authority.assertFollowExecution(entry, execution, admissionSignal);
                if (!allowed()) throw new Error('REACTIVE_CAPABILITY_UNAVAILABLE: ' + follow.name);
                eligibility?.check();
            };
            const access = withOperationAccessSettlement({ sessionId: entry.agent.id, source: execution.source,
                signal: AbortSignal.any([execution.signal, this.signal]) }, work => this.retainNotification(work));
            await this.#follows.prepare(entry.agent, owner, {
                id, operation: { kind: owner.definition.kind, version: owner.definition.version }, command,
                continuation: execution.continuation, wait: { id: 'operation:' + id, operation: { kind: owner.definition.kind, version: owner.definition.version, command } },
                scope: follow.scope, target: command, source: execution.source, mode: follow.mode,
                ...follow.when === 'feedback' ? { when: 'feedback', after } : { when: 'settled', after: null },
            }, this.authority.callSequence(execution), access, {
                admit: async () => {
                    await firstValueFrom(abortable(() => eligibility?.ready, admissionSignal));
                    check();
                    await this.authority.admitFollow(entry, execution, admissionSignal);
                    check();
                },
                enter: () => { check(); release(); this.authority.assertFollowExecution(entry, execution, admissionSignal); },
            });
            return id;
        } finally { release(); }
    }
    registerWait(agent: Agent, continuation: Continuation, raw: WaitReference): Promise<void> {
        const wait = waitSchema.parse(raw);
        if (this.#follows.closedWait(agent, continuation, wait))
            return this.facts.flush(agent);
        const state = this.facts.liveState(agent.session);
        const previous = state.activity.waits.find(item => item.wait.id === wait.id);
        if (previous && (canonical(previous.wait) !== canonical(wait) || canonical(previous.continuation) !== canonical(continuation))) {
            throw new Error('REACTIVE_WAIT_ID_CONFLICT');
        }
        // 可靠完成通知可能先于 prepare 的 registerFollow ACK 返回；不能重新打开已经结算的同一等待。
        const resolved = Object.values(state.value.records).some(record => record.result?.ready
            && canonical(record.result.wait) === canonical(wait) && canonical(record.result.continuation) === canonical(continuation));
        if (resolved)
            return this.facts.flush(agent);
        if (!previous)
            appendPluginRecord(agent.session, 'plugin:reactive/wait', { continuation, wait });
        const current = this.ctx.goals.get(agent);
        if (continuation.goal?.id === current?.id && continuation.goal?.revision === current?.revision)
            this.ctx.goals.disarm(agent);
        return this.facts.flush(agent);
    }
    async operationEvent(action: ActionReference, event: ActionOperationEvent, execution: ActionExecutionContext) {
        const call = this.authority.nativeCall(execution);
        if (!call || event.phase === 'access')
            return;
        const { entry, offer, exec } = call;
        const id = operationFollowId(entry.agent.id, this.authority.callSequence(execution), event.owner, event.command);
        if (event.phase === 'not-started') {
            await this.#follows.notStarted(entry.agent, id);
            return;
        }
        if (event.phase === 'receipt') {
            if (this.#follows.links(entry.agent).some(link => link.id === id))
                await this.#follows.receipt(entry.agent, id, event.owner, event.reference);
            return;
        }
        const follow = offer.follow;
        if (!follow)
            return;
        if (!this.options.serviceActorId?.trim())
            throw new Error('REACTIVE_OPERATION_PUBLISHER_REQUIRED');
        if (execution.source.kind !== 'tool' || !execution.continuation)
            throw new Error('REACTIVE_OPERATION_FOLLOW_SOURCE_REQUIRED');
        const source = execution.source, continuation = execution.continuation;
        entry.session.implementation.definition.input.parse(follow.scope);
        await this.delegations.preparing(action, execution, event.command, () => this.#follows.prepare(entry.agent, event.owner, {
            id, when: follow.when, after: null, operation: { kind: event.owner.definition.kind, version: event.owner.definition.version },
            command: event.command, continuation,
            wait: { id: 'operation:' + id, operation: { kind: event.owner.definition.kind, version: event.owner.definition.version, command: event.command } },
            scope: z.json().parse(follow.scope), target: z.json().parse(action.scope), source, mode: follow.mode,
        }, this.authority.callSequence(execution), { sessionId: execution.sessionId, source: execution.source, signal: execution.signal }));
        if (follow.mode === 'wait')
            exec.concludeTurn();
    }
    operationDefinition(kind: string, version: number): OperationContract {
        this.assertActive();
        return this.#follows.definition(kind, version);
    }
    resolveOperation(session: ConversationSession, name: string, options: SubmitOptions = {}, identity: ParticipantIdentity = humanParticipant): BoundOperation<OperationContract> {
        this.assertActive();
        assertParticipantOptions(identity, options);
        session.program.flush();
        const binding = session.program.outputs.operations.peek()[name];
        const definition = surfaceSlotDefinition(session.implementation.definition.surface, 'operations', name);
        if (!binding || !definition)
            throw new Error('REACTIVE_OPERATION_NOT_EXPOSED');
        if (binding.definition !== definition || this.#follows.forDefinition(definition) !== operationBindingOwner(binding))
            throw new Error('REACTIVE_OPERATION_OWNER_MISMATCH');
        return bindOperation(this.#follows.forDefinition(definition), operationBindingCommand(binding), {
            access: () => {
                this.sessions.assertSessionWorkspace(session);
                return this.authority.participantExecution(session, options, identity, 'operation-query');
            },
            readFeedback: (operation, id, access) => this.readBoundOperationFeedback(session, operation, id, access),
        });
    }
    async readBoundOperationFeedback<D extends OperationContract>(session: ConversationSession, binding: OperationBindingReference<D>, id: string, access: OperationAccess): Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>> {
        this.assertActive();
        this.sessions.assertSessionWorkspace(session);
        access.signal.throwIfAborted();
        if (access.sessionId !== session.header.id || this.#follows.forDefinition(binding.definition) !== operationBindingOwner(binding))
            throw new Error('REACTIVE_OPERATION_OWNER_MISMATCH');
        const signal = AbortSignal.any([access.signal, session.scope.signal]);
        const reading: OperationAccess = { sessionId: access.sessionId, source: access.source, signal };
        return firstValueFrom(abortable(async () => {
            const live = session.live.value;
            const inspection = live ? undefined : await this.facts.inspect(session.header.id, signal);
            signal.throwIfAborted(); this.sessions.assertSessionWorkspace(session);
            const value = live ? await this.#follows.readFeedback(live.agent, binding.definition, operationBindingCommand(binding), id, reading)
                : await this.#follows.readInspectedFeedback(inspection!, binding.definition, operationBindingCommand(binding), id, reading);
            signal.throwIfAborted(); this.sessions.assertSessionWorkspace(session);
            return value;
        }, signal));
    }
    async readOperationFeedback<D extends OperationContract>(target: string | ConversationSession, definition: D, command: OperationCommand<D>, id: string, options: SubmitOptions = {}, identity: ParticipantIdentity = humanParticipant): Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>> {
        assertParticipantOptions(identity, options);
        const session = await this.acquireSession(target, options.signal);
        const signal = AbortSignal.any([this.signal, ...(typeof target === 'string' ? [] : [session.scope.signal]),
            ...(options.signal ? [options.signal] : [])]);
        const access = this.authority.participantExecution(session, { ...options, signal }, identity, 'operation-query');
        return firstValueFrom(abortable(async () => {
            const live = session.live.value;
            const inspection = live ? undefined : await this.facts.inspect(session.header.id, signal);
            signal.throwIfAborted();
            if (typeof target !== 'string') this.sessions.assertSessionWorkspace(session);
            const value = live ? await this.#follows.readFeedback(live.agent, definition, command, id, access)
                : await this.#follows.readInspectedFeedback(inspection!, definition, command, id, access);
            signal.throwIfAborted();
            if (typeof target !== 'string') this.sessions.assertSessionWorkspace(session);
            return value;
        }, signal));
    }
    async attachOperation<D extends OperationContract>(target: string | ConversationSession, definition: D, reference: OperationReference<D>, options: SubmitOptions = {}, identity: ParticipantIdentity = humanParticipant): Promise<OperationHandle<D>> {
        assertParticipantOptions(identity, options);
        const session = await this.acquireSession(target, options.signal);
        this.sessions.assertSessionWorkspace(session);
        const access = this.authority.participantExecution(session, options, identity, 'operation-query');
        return attachOwnedOperation(this.#follows.forDefinition(definition), reference, access);
    }
    async locateOperation<D extends OperationContract>(target: string | ConversationSession, definition: D, command: OperationCommand<D>, options: SubmitOptions = {}, identity: ParticipantIdentity = humanParticipant): Promise<OperationLocation<OperationReference<D>, OperationState<D>>> {
        assertParticipantOptions(identity, options);
        const session = await this.acquireSession(target, options.signal);
        this.sessions.assertSessionWorkspace(session);
        const signal = AbortSignal.any([this.signal, ...(typeof target === 'string' ? [] : [session.scope.signal]),
            ...(options.signal ? [options.signal] : [])]);
        const access = this.authority.participantExecution(session, { ...options, signal }, identity, 'operation-query');
        const result = await locateOwnedOperation(this.#follows.forDefinition(definition), command, access);
        signal.throwIfAborted();
        if (typeof target !== 'string') this.sessions.assertSessionWorkspace(session);
        return result;
    }
    async notifyOperation(target: string | ConversationSession, followId: string, options: SubmitOptions, identity: ParticipantIdentity, sourceOwner?: OperationOwnerReference): Promise<OperationNotificationResult> {
        this.assertActive();
        const actorId = participantActor(identity, options, this.options.actorId);
        const callerSignal = options.signal ? AbortSignal.any([this.signal, options.signal]) : this.signal;
        callerSignal.throwIfAborted();
        const completion = Promise.withResolvers<OperationNotificationResult>();
        const work = completion.promise;
        this.retainNotification(work);
        void (async (): Promise<OperationNotificationResult> => {
            const session = await this.acquireSession(target, callerSignal), sessionId = session.header.id;
            const live = session.live.value;
            const signal = AbortSignal.any([callerSignal, session.scope.signal, ...(live ? [live.scope.signal] : [])]);
            signal.throwIfAborted();
            const inspection = live ? undefined : await this.facts.inspect(sessionId, signal);
            signal.throwIfAborted(); this.sessions.assertSessionWorkspace(session);
            const link = live ? this.#follows.link(live.agent, followId) : this.#follows.inspectedLink(inspection!, followId);
            if (sourceOwner) this.#follows.assertNotificationOwner(link, sourceOwner);
            const notStarted = live ? this.#follows.isNotStarted(live.agent, followId) : this.#follows.inspectedNotStarted(inspection!, followId);
            const unregistered = live ? this.#follows.isUnregistered(live.agent, followId) : this.#follows.inspectedUnregistered(inspection!, followId);
            const access: OperationAccess = withOperationAccessSettlement({ sessionId, source: {
                    kind: 'participant', participant: identity.participant, actorId, entrypoint: 'operation-feedback',
                }, signal }, work => this.retainNotification(work));
            // 冷会话在加载原生 Agent 前也须取得业务 follow 权限；纯读权不能触发激活。
            const closed = await this.#follows.admit(link, access,
                notStarted ? { status: 'not-started' } : unregistered ? { status: 'unregistered' } : undefined);
            signal.throwIfAborted(); this.sessions.assertSessionWorkspace(session);
            if (closed) return closed;
            await this.sessions.activate(sessionId, session.accessId);
            signal.throwIfAborted(); this.sessions.assertSessionWorkspace(session);
            const entry = this.sessions.live(session);
            return this.#follows.complete(entry.agent, followId, { ...access,
                signal: AbortSignal.any([signal, entry.scope.signal]) });
        })().then(completion.resolve, completion.reject);
        return firstValueFrom(abortable(() => work, callerSignal));
    }
    private retainNotification(pending: PromiseLike<unknown>): void {
        const work = Promise.resolve(pending);
        this.#notifications.add(work);
        void work.then(() => this.#notifications.delete(work), () => this.#notifications.delete(work));
    }
    async settleNotifications(): Promise<void> {
        while (this.#notifications.size) await Promise.allSettled([...this.#notifications]);
    }
    async reconcileOperations(entry: LiveConversation) {
        const links = this.#follows.links(entry.agent);
        const pending = links.filter(link => {
            const request = entry.session.state.activity.requests['operation:' + link.id];
            return !matchesOperationFollowResult(link, request?.declaration)
                || (request?.state !== 'consumed' && request?.state !== 'settled');
        });
        if (!pending.length)
            return;
        await this.#follows.restore(entry.agent, pending, this.operationRecoveryAccess(entry));
    }
    private operationRecoveryAccess(entry: LiveConversation): OperationAccess {
        const actorId = this.options.serviceActorId;
        if (!actorId?.trim())
            throw new Error('REACTIVE_OPERATION_PUBLISHER_REQUIRED');
        return withOperationAccessSettlement({ sessionId: entry.agent.id, source: {
                kind: 'participant', participant: 'service', actorId, entrypoint: 'operation-recovery',
            }, signal: AbortSignal.any([this.signal, entry.scope.signal]) }, work => this.retainNotification(work));
    }
    shouldWake(entry: LiveConversation, input: AdmittedSubmission): boolean {
        const result = input.result;
        if (!result)
            return input.delivery !== 'inject';
        if (!result.continuation.goal)
            return this.resultReady(entry, input);
        const goal = this.ctx.goals.get(entry.agent);
        return goal?.phase === 'active' && goal.id === result.continuation.goal.id && goal.revision === result.continuation.goal.revision
            && this.resultReady(entry, input);
    }
    /** 只复核原结果和等待事实；Goal 准入身份由 shouldWake 或本次已准备的精确关联负责。 */
    resultReady(entry: LiveConversation, input: AdmittedSubmission): boolean {
        const result = input.result;
        if (!result?.ready)
            return false;
        if (!result.continuation.goal)
            return !this.#follows.hasPendingContinuation(entry.agent, result.continuation);
        const goal = this.ctx.goals.get(entry.agent);
        return goal?.phase === 'active' && goal.roundsStarted < goal.maxGoalRounds
            && !entry.session.state.hasWaits(result.continuation.goal) && !this.#follows.hasPendingGoal(entry.agent, result.continuation.goal)
            && !entry.session.state.hasWaits(goal) && !this.#follows.hasPendingGoal(entry.agent, goal);
    }
    validateContinuation(entry: LiveConversation, continuation: Continuation, wait: WaitReference) {
        if (continuation.sessionId !== entry.agent.id)
            throw new Error('REACTIVE_CONTINUATION_SESSION_MISMATCH');
        const messages = entry.agent.session.snapshotEvents().flatMap(event => event.type === 'user/message' ? entry.session.state.reference(event.data) : []);
        for (const input of continuation.inputs) {
            if (!messages.some(message => canonical(message) === canonical(input)))
                throw new Error('REACTIVE_CONTINUATION_INPUT_MISMATCH');
        }
        const previous = entry.session.state.activity.waits.find(item => item.wait.id === wait.id);
        if (!previous && this.#follows.background(entry.agent, continuation, wait))
            return;
        if (!previous)
            throw new Error('REACTIVE_WAIT_NOT_FOUND');
        if (canonical(previous.wait) !== canonical(wait)
            || canonical(previous.continuation) !== canonical(continuation))
            throw new Error('REACTIVE_WAIT_ID_CONFLICT');
    }
}
