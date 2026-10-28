import { randomUUID } from "node:crypto";
import type { Context } from "@deepseek-ai/cordis";
import type { ToolRunContext } from "@deepseek-ai/dsh-tools";
import { firstValueFrom } from 'rxjs';
import { abortable } from '@hanger-source/dsh-reactive';
import { z } from "zod";
import { type Continuation, type WaitReference, type SubmitOptions } from "@hanger-source/dsh-reactive/kernel";
import { ActionBindingUnavailableError, ActionTargetMismatchError, type ActionCallContext, type ActionExecutionContext, type ActionReference, type Invocation } from "@hanger-source/dsh-reactive/kernel";
import { type BoundOffer } from "@hanger-source/dsh-reactive/kernel";
import { guardActionExecution } from "@hanger-source/dsh-reactive/kernel";
import { dispatchHumanQuestion } from "../participation/human-dispatch.ts";
import { canonical } from "../conversations/records.ts";
import { type Json } from "@hanger-source/dsh-reactive/kernel";
import { type NativeDelegations } from "../delegations.ts";
import type { HostOptions } from '../applications/options.ts';
import type { ConversationSession, LiveConversation } from '../conversations/session.ts';
import type { ConversationSessions } from '../conversations/sessions.ts';
import type { ConversationObservations } from '../conversations/observations.ts';
import type { ConversationInputs } from '../conversations/inputs.ts';
import type { Workspaces } from '../workspaces/workspaces.ts';
import { humanParticipant, serviceParticipant, participantActor, type ParticipantIdentity } from '../participation/identity.ts';
type ToolExecution = ActionExecutionContext & {
    readonly source: Extract<ActionExecutionContext['source'], {
        readonly kind: 'tool';
    }>;
    readonly continuation: Continuation;
};
/** 三种真实入口共用的签发者；绑定身份和来源证明不由调用参数声明。 */
export class ActionAuthority {
    #toolCalls = new WeakMap<ActionExecutionContext, {
        entry: LiveConversation;
        exec: ToolRunContext;
        offer: BoundOffer;
    }>();
    #executions = new WeakMap<ActionExecutionContext, ConversationSession>();
    constructor(private readonly ctx: Context, private readonly options: HostOptions, private readonly signal: AbortSignal, private readonly workspaces: Workspaces, private readonly sessions: ConversationSessions, private readonly facts: ConversationObservations, private readonly inputs: ConversationInputs, private readonly delegations: NativeDelegations) { }
    private assertActive() { if (this.signal.aborted)
        throw new Error('REACTIVE_HOST_DISPOSED'); }
    nativeCall(execution: ActionExecutionContext) { return this.#toolCalls.get(execution); }
    assertActionExecution(session: ConversationSession, execution: ActionExecutionContext) {
        if (this.#executions.get(execution) !== session)
            throw new Error('REACTIVE_ACTION_EXECUTION_UNTRUSTED');
        const native = this.#toolCalls.get(execution);
        if (execution.source.kind === 'tool' && (!native || native.entry.session !== session
            || this.ctx.agents.get(native.entry.agent.id) !== native.entry.agent))
            throw new Error('REACTIVE_ACTION_TOOL_AUTHORITY_REQUIRED');
    }
    signExecution<T extends ActionExecutionContext>(session: ConversationSession, execution: T, beforeExecute?: () => void): T {
        this.#executions.set(execution, session);
        guardActionExecution(execution, () => {
            beforeExecute?.();
            this.assertActionExecution(session, execution);
            this.sessions.assertSessionWorkspace(session);
        });
        return execution;
    }
    async admitAction(session: ConversationSession, action: ActionReference, input: unknown, call: ActionCallContext<unknown>, execution: ActionExecutionContext, admissionSignal: AbortSignal) {
        const native = this.#toolCalls.get(execution);
        const signal = AbortSignal.any([admissionSignal, this.signal, session.program.signal,
            ...(native ? [native.entry.scope.signal] : [])]);
        const check = () => {
            signal.throwIfAborted();
            this.assertActionExecution(session, execution);
            session.program.flush();
            signal.throwIfAborted();
            this.sessions.assertSessionWorkspace(session);
        };
        check();
        if (!session.live.value) {
            const inspection = await firstValueFrom(abortable(() => this.facts.inspect(session.header.id, signal), signal));
            check();
            this.workspaces.verifyInspection(inspection, session.workspace, session.environment);
            check();
        }
        if (!this.delegations.owns(action))
            return;
        await firstValueFrom(abortable(() => this.inputs.revalidateCauses(execution.inputs, signal), signal));
        check();
        // 已开始的原生activate仍归其真实owner；本准入退等后不再继续授权或准备child。
        await firstValueFrom(abortable(() => this.sessions.activate(session.header.id), signal));
        check();
        const live = this.sessions.live(session);
        const authorizationSignal = AbortSignal.any([signal, live.scope.signal]);
        await this.delegations.admit(action, input, call, execution, live.agent, session.conversation.identity, authorizationSignal);
        check();
        authorizationSignal.throwIfAborted();
    }
    call(entry: LiveConversation, exec: ToolRunContext, offer: BoundOffer,
        registerWait: (continuation: Continuation, wait: WaitReference) => Promise<void>): ActionExecutionContext {
        const execution = this.toolExecution(entry, exec, registerWait);
        this.#toolCalls.set(execution, { entry, exec, offer });
        return execution;
    }
    toolExecution(entry: LiveConversation, exec: ToolRunContext,
        registerWait: (continuation: Continuation, wait: WaitReference) => Promise<void>): ToolExecution {
        const occurrence = entry.session.state.call(exec.callId);
        if (!occurrence || occurrence.rootCallId !== exec.rootCallId)
            throw new Error('REACTIVE_CALL_INPUT_UNAVAILABLE');
        const inputs = occurrence.inputs;
        const goal = this.ctx.goals.get(entry.agent);
        const continuation: Continuation = { sessionId: entry.agent.id, inputs,
            goal: goal?.phase === 'active' ? { id: goal.id, revision: goal.revision } : null };
        const execution: ToolExecution = { invocationId: exec.callId, sessionId: entry.agent.id,
            source: { kind: 'tool', callId: exec.callId, rootCallId: exec.rootCallId, parentCallId: occurrence.parentCallId },
            inputs, signal: exec.signal, continuation,
            askHuman: (questions, options, signal) => {
                this.assertActionExecution(entry.session, execution);
                this.sessions.assertSessionWorkspace(entry.session);
                entry.session.program.flush();
                if (!entry.session.program.humanInput)
                    throw new Error('REACTIVE_HUMAN_INPUT_NOT_DECLARED');
                return dispatchHumanQuestion(this.ctx, exec, questions, options, signal);
            },
            completeGoal: () => {
                if (!goal)
                    throw new Error('REACTIVE_GOAL_ABSENT');
                this.ctx.goals.complete(entry.agent, goal);
            },
            wait: raw => {
                const accepted = registerWait(continuation, raw);
                exec.concludeTurn();
                return accepted;
            },
        };
        return this.signExecution(entry.session, execution);
    }
    participantExecution(session: ConversationSession, options: SubmitOptions, identity: ParticipantIdentity, entrypoint: string, beforeExecute?: () => void): ActionExecutionContext {
        this.assertActive();
        const actorId = participantActor(identity, options, this.options.actorId), participant = identity.participant;
        const signal = options.signal ? AbortSignal.any([this.signal, options.signal]) : this.signal;
        signal.throwIfAborted();
        // 独立参与调用没有继承当前模型输入或 Goal 续接权限。
        const execution: ActionExecutionContext = { invocationId: randomUUID(), sessionId: session.header.id,
            source: { kind: 'participant', actorId, participant, entrypoint }, inputs: [], continuation: null, signal };
        return this.signExecution(session, execution, beforeExecute);
    }
    programExecution(session: ConversationSession, entrypoint: string): ActionExecutionContext {
        return this.participantExecution(session, {}, serviceParticipant(this.options.serviceActorId), entrypoint);
    }
    currentAction(session: ConversationSession, name: string): ActionReference {
        const actions = session.program.outputs.actions.peek();
        const action: ActionReference | undefined = Object.hasOwn(actions, name) ? actions[name] : undefined;
        if (!action)
            throw new ActionBindingUnavailableError(name, 'not-exposed');
        return action;
    }
    resolveSessionAction(session: ConversationSession, name: string, expectedScope: unknown): ActionReference {
        this.assertActive();
        session.program.flush();
        const action = this.currentAction(session, name);
        const expected = z.json().parse(action.definition.scope.parse(expectedScope));
        session.program.flush();
        if (this.currentAction(session, name) !== action)
            throw new ActionBindingUnavailableError(name, 'binding-changed');
        if (canonical(expected) !== canonical(action.scope))
            throw new ActionTargetMismatchError(name);
        return action;
    }
    invokeSession(session: ConversationSession, name: string, bindingId: string, input: unknown, options: SubmitOptions = {}, identity: ParticipantIdentity = humanParticipant): Invocation<unknown, Json> {
        this.assertActive();
        const program = session.program;
        program.flush();
        const action = this.currentAction(session, name);
        const check = () => {
            const current = this.currentAction(session, name);
            if (current !== action || current.bindingId !== bindingId)
                throw new ActionBindingUnavailableError(name, 'binding-changed');
            return true;
        };
        check();
        const execution = this.participantExecution(session, options, identity, identity.participant === 'service' ? 'service-sdk' : 'sdk', () => { program.flush(); check(); });
        return program.invoke(action, input, execution, check);
    }
}
