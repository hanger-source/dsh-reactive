import type { Context } from "@deepseek-ai/cordis";
import type { Agent } from "@deepseek-ai/dsh-agent";
import { appendPluginRecord } from "@deepseek-ai/dsh-session";
import { z } from "zod";
import { type Continuation, type WaitReference, type SubmitOptions, type GoalReference } from "@hanger-source/dsh-reactive/kernel";
import { type ApplicationDefinition } from "@hanger-source/dsh-reactive/kernel";
import { type Scope } from "@hanger-source/dsh-reactive/kernel";
import { type ActionExecutionContext, type ActionReference } from "@hanger-source/dsh-reactive/kernel";
import { type BoundOffer, type BoundFollow } from "@hanger-source/dsh-reactive/kernel";
import { OperationFollows, operationFollowId } from "./follows.ts";
import type { ActionOperationEvent } from "@hanger-source/dsh-reactive/kernel";
import { attachOperation as attachOwnedOperation, locateOperation as locateOwnedOperation, operationBindingOwner, operationBindingCommand, bindOperation, type OperationAccessContext as OperationAccess, type OperationContract, type OperationHandle, type OperationReference, type OperationCommand, type OperationLocation, type OperationState, type OperationNotificationResult, type OperationBindingReference, type BoundOperation, type OperationFeedbackRead, type OperationFeedbackPayload } from "@hanger-source/dsh-reactive/kernel";
import { actionOperationOwner } from "@hanger-source/dsh-reactive/kernel";
import { canonical, waitSchema } from "../conversations/records.ts";
import { type AdmittedSubmission } from "@hanger-source/dsh-reactive/kernel";
import type { ProgramImplementation } from "@hanger-source/dsh-reactive/kernel";
import { surfaceSlotDefinition } from "@hanger-source/dsh-reactive/kernel";
import { type NativeDelegations } from "../delegations.ts";
import type { HostOptions } from '../applications/options.ts';
import type { ConversationSession, LiveConversation } from '../conversations/session.ts';
import type { ConversationSessions } from '../conversations/sessions.ts';
import type { ConversationObservations } from '../conversations/observations.ts';
import type { ConversationInputs } from '../conversations/inputs.ts';
import type { ActionAuthority } from '../execution/authority.ts';
/** 可靠操作参与与精确 Goal 续接；Operation 事实仍由业务 owner 持有。 */
export class OperationParticipation {
    #follows: OperationFollows;
    constructor(private readonly ctx: Context, private readonly definition: ApplicationDefinition, private readonly options: HostOptions, private readonly signal: AbortSignal, private readonly sessions: ConversationSessions, private readonly facts: ConversationObservations, private readonly inputs: ConversationInputs, private readonly authority: ActionAuthority, private readonly delegations: NativeDelegations, private readonly nativeScope: (agent: Agent) => Scope) {
        this.#follows = new OperationFollows(ctx, [...this.options.operations ?? [], delegations.owner], {
            publisherId: () => {
                const actorId = this.options.serviceActorId;
                if (!actorId?.trim())
                    throw new Error('REACTIVE_OPERATION_PUBLISHER_REQUIRED');
                return actorId;
            },
            recoveryAccess: agent => this.operationRecoveryAccess(agent),
            checkpoint: agent => this.facts.flush(agent),
            wait: (agent, continuation, wait) => this.registerWait(agent, continuation, wait),
            resolved: (agent, link) => {
                const record = this.facts.liveState(agent.session).record('operation:' + link.id);
                return record?.result?.ready === true && canonical(record.result.wait) === canonical(link.wait)
                    && canonical(record.result.continuation) === canonical(link.continuation);
            },
            waiting: (agent, link) => this.facts.liveState(agent.session).activity.waits.some(item => canonical(item.wait) === canonical(link.wait)
                && canonical(item.continuation) === canonical(link.continuation)),
            admit: (link, actorId, signal) => this.definition.admit({
                requestId: 'operation:' + link.id, content: [{ type: 'text', text: '核验原业务操作的结算状态' }], intent: 'result', scope: link.scope,
                result: { continuation: link.continuation, wait: link.wait, ready: false },
            }, this.sessions.get(link.continuation.sessionId)!.conversation.identity, { actorId, participant: 'service', signal }),
            deliver: (agent, input, actorId, signal) => this.inputs.submitSession(this.sessions.forAgent(agent)!.session, input, { actorId, signal }, 'service'),
        });
    }
    private assertActive() { if (this.signal.aborted)
        throw new Error('REACTIVE_HOST_DISPOSED'); }
    installDelegationUpdates(): () => void {
        const changes = this.delegations.updates$.subscribe(({ parentSessionId }) => {
            const entry = this.sessions.get(parentSessionId)?.live;
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
        if (this.#follows.forDefinition(binding.definition) !== operationBindingOwner(binding))
            throw new Error('REACTIVE_OPERATION_OWNER_MISMATCH');
        return this.authority.participantExecution(session, {}, 'service', 'program-operation');
    }
    async participate(entry: LiveConversation, follow: BoundFollow, execution: ActionExecutionContext & {
        readonly continuation: Continuation;
        readonly source: Extract<ActionExecutionContext['source'], {
            readonly kind: 'tool';
        }>;
    }, after: string | null): Promise<string> {
        const owner = operationBindingOwner(follow.operation), command = operationBindingCommand(follow.operation);
        const id = operationFollowId(entry.agent.id, execution.source.callId, owner, command);
        await this.#follows.prepare(entry.agent, owner, {
            id, operation: { kind: owner.definition.kind, version: owner.definition.version }, command,
            continuation: execution.continuation, wait: { id: 'operation:' + id, operation: { kind: owner.definition.kind, version: owner.definition.version, command } },
            scope: follow.scope, target: command, source: execution.source, mode: follow.mode,
            ...follow.when === 'feedback' ? { when: 'feedback', after } : { when: 'settled', after: null },
        }, { sessionId: entry.agent.id, source: execution.source, signal: execution.signal });
        return id;
    }
    registerWait(agent: Agent, continuation: Continuation, raw: WaitReference): Promise<void> {
        const wait = waitSchema.parse(raw);
        if (this.#follows.notStartedWait(agent, continuation, wait))
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
        const id = operationFollowId(entry.agent.id, exec.callId, event.owner, event.command);
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
        }, { sessionId: execution.sessionId, source: execution.source, signal: execution.signal }));
        if (follow.mode === 'wait')
            exec.concludeTurn();
    }
    operationDefinition(kind: string, version: number): OperationContract {
        this.assertActive();
        return this.#follows.definition(kind, version);
    }
    resolveOperation(session: ConversationSession, name: string, options: SubmitOptions = {}, participant: 'human' | 'service' = 'human'): BoundOperation<OperationContract> {
        this.assertActive();
        session.program.flush();
        const binding = session.program.outputs.operations.peek()[name];
        const definition = surfaceSlotDefinition(session.implementation.definition.surface, 'operations', name);
        if (!binding || !definition)
            throw new Error('REACTIVE_OPERATION_NOT_EXPOSED');
        if (binding.definition !== definition || this.#follows.forDefinition(definition) !== operationBindingOwner(binding))
            throw new Error('REACTIVE_OPERATION_OWNER_MISMATCH');
        return bindOperation(this.#follows.forDefinition(definition), operationBindingCommand(binding), {
            access: () => this.authority.participantExecution(session, options, participant, 'operation-query'),
            readFeedback: (operation, id, access) => this.readBoundOperationFeedback(session, operation, id, access),
        });
    }
    async readBoundOperationFeedback<D extends OperationContract>(session: ConversationSession, binding: OperationBindingReference<D>, id: string, access: OperationAccess): Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>> {
        this.assertActive();
        access.signal.throwIfAborted();
        if (access.sessionId !== session.header.id || this.#follows.forDefinition(binding.definition) !== operationBindingOwner(binding))
            throw new Error('REACTIVE_OPERATION_OWNER_MISMATCH');
        if (session.live)
            return this.#follows.readFeedback(session.live.agent, binding.definition, operationBindingCommand(binding), id, access);
        const inspection = await this.facts.inspect(session.header.id);
        access.signal.throwIfAborted();
        return this.#follows.readInspectedFeedback(inspection, binding.definition, operationBindingCommand(binding), id, access);
    }
    async readOperationFeedback<D extends OperationContract>(sessionId: string, definition: D, command: OperationCommand<D>, id: string, options: SubmitOptions = {}, participant: 'human' | 'service' = 'human'): Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>> {
        const session = await this.sessions.readSession(sessionId);
        const access = this.authority.participantExecution(session, options, participant, 'operation-query');
        if (session.live)
            return this.#follows.readFeedback(session.live.agent, definition, command, id, access);
        const inspection = await this.facts.inspect(sessionId);
        access.signal.throwIfAborted();
        return this.#follows.readInspectedFeedback(inspection, definition, command, id, access);
    }
    async attachOperation<D extends OperationContract>(sessionId: string, definition: D, reference: OperationReference<D>, options: SubmitOptions = {}, participant: 'human' | 'service' = 'human'): Promise<OperationHandle<D>> {
        const session = await this.sessions.readSession(sessionId);
        const access = this.authority.participantExecution(session, options, participant, 'operation-query');
        return attachOwnedOperation(this.#follows.forDefinition(definition), reference, access);
    }
    async locateOperation<D extends OperationContract>(sessionId: string, definition: D, command: OperationCommand<D>, options: SubmitOptions = {}, participant: 'human' | 'service' = 'human'): Promise<OperationLocation<OperationReference<D>, OperationState<D>>> {
        const session = await this.sessions.readSession(sessionId);
        const access = this.authority.participantExecution(session, options, participant, 'operation-query');
        return locateOwnedOperation(this.#follows.forDefinition(definition), command, access);
    }
    async notifyOperation(sessionId: string, followId: string, options: SubmitOptions = {}): Promise<OperationNotificationResult> {
        this.assertActive();
        const actorId = options.actorId ?? this.options.serviceActorId;
        if (!actorId?.trim())
            throw new Error('REACTIVE_OPERATION_PUBLISHER_REQUIRED');
        const callerSignal = options.signal ? AbortSignal.any([this.signal, options.signal]) : this.signal;
        callerSignal.throwIfAborted();
        const session = await this.sessions.readSession(sessionId);
        const signal = session.live ? AbortSignal.any([callerSignal, this.nativeScope(session.live.agent).signal]) : callerSignal;
        signal.throwIfAborted();
        const inspection = session.live ? undefined : await this.facts.inspect(sessionId);
        const link = session.live ? this.#follows.link(session.live.agent, followId) : this.#follows.inspectedLink(inspection!, followId);
        const notStarted = session.live ? this.#follows.isNotStarted(session.live.agent, followId) : this.#follows.inspectedNotStarted(inspection!, followId);
        const access: OperationAccess = { sessionId, source: {
                kind: 'participant', participant: 'service', actorId, entrypoint: 'operation-feedback',
            }, signal };
        // 冷会话在加载原生 Agent 前也须取得业务 follow 权限；纯读权不能触发激活。
        await this.#follows.admit(link, access, notStarted);
        signal.throwIfAborted();
        if (notStarted)
            return { status: 'not-started' };
        await this.sessions.activate(sessionId);
        const agent = this.sessions.live(this.sessions.get(sessionId)!).agent;
        return this.#follows.complete(agent, followId, { ...access,
            signal: AbortSignal.any([signal, this.nativeScope(agent).signal]) });
    }
    async reconcileOperations(entry: LiveConversation) {
        const links = this.#follows.links(entry.agent);
        const pending = links.filter(link => !['consumed', 'settled'].includes(entry.session.state.activity.requests['operation:' + link.id]?.state ?? ''));
        if (!pending.length)
            return;
        await this.#follows.restore(entry.agent, pending, this.operationRecoveryAccess(entry.agent));
    }
    operationRecoveryAccess(agent: Agent): OperationAccess {
        const actorId = this.options.serviceActorId;
        if (!actorId?.trim())
            throw new Error('REACTIVE_OPERATION_PUBLISHER_REQUIRED');
        return { sessionId: agent.id, source: {
                kind: 'participant', participant: 'service', actorId, entrypoint: 'operation-recovery',
            }, signal: AbortSignal.any([this.signal, this.nativeScope(agent).signal]) };
    }
    shouldWake(entry: LiveConversation, input: AdmittedSubmission): boolean {
        const result = input.result;
        if (!result)
            return input.delivery !== 'inject';
        if (!result.ready)
            return false;
        if (!result.continuation.goal)
            return !this.#follows.hasPendingContinuation(entry.agent, result.continuation);
        const goal = this.ctx.goals.get(entry.agent);
        return goal?.phase === 'active' && goal.id === result.continuation.goal.id && goal.revision === result.continuation.goal.revision
            && goal.roundsStarted < goal.maxGoalRounds && !entry.session.state.hasWaits(result.continuation.goal)
            && !this.#follows.hasPendingGoal(entry.agent, result.continuation.goal);
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
