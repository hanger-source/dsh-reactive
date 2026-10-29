import { randomUUID } from "node:crypto";
import type { Context } from "@deepseek-ai/cordis";
import type { ToolRunContext } from "@deepseek-ai/dsh-tools";
import { firstValueFrom } from 'rxjs';
import { abortable } from '@hanger-source/dsh-reactive';
import { z } from "zod";
import { type Continuation, type WaitReference, type SubmitOptions } from "@hanger-source/dsh-reactive/kernel";
import { ActionBindingUnavailableError, ActionTargetMismatchError, assertExpectedProgram, type ActionContract, type ActionCallContext, type ActionExecutionContext, type ActionReference, type Invocation, type ProgramContract, type ProgramExpectation } from "@hanger-source/dsh-reactive/kernel";
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
import { InvocationUnavailableError, type RetainedActionInvocation } from './invocation.ts';
type ToolExecution = ActionExecutionContext & {
    readonly source: Extract<ActionExecutionContext['source'], {
        readonly kind: 'tool';
    }>;
    readonly continuation: Continuation;
};
type ParticipantExecution = ActionExecutionContext & {
    readonly source: Extract<ActionExecutionContext['source'], { readonly kind: 'participant' }>;
};
type RetainedEntry = {
    readonly sessionId: string;
    readonly program: ProgramContract;
    readonly participant: ParticipantIdentity['participant'];
    readonly actorId: string;
    readonly value: RetainedActionInvocation;
    completed?: ReturnType<typeof setTimeout>;
};
/** 三种真实入口共用的签发者；绑定身份和来源证明不由调用参数声明。 */
export class ActionAuthority {
    #invocations = new Map<string, RetainedEntry>();
    #completed = new Set<string>();
    readonly #retention: number;
    readonly #maxCompleted: number;
    #toolCalls = new WeakMap<ActionExecutionContext, {
        entry: LiveConversation;
        exec: ToolRunContext;
        offer: BoundOffer;
    }>();
    #executions = new WeakMap<ActionExecutionContext, {
        session: ConversationSession;
        callSequence: number | null;
    }>();
    constructor(private readonly ctx: Context, private readonly options: HostOptions, private readonly signal: AbortSignal, private readonly workspaces: Workspaces, private readonly sessions: ConversationSessions, private readonly facts: ConversationObservations, private readonly inputs: ConversationInputs, private readonly delegations: NativeDelegations) {
        this.#retention = options.completedRetentionMs ?? 300_000;
        this.#maxCompleted = options.maxCompleted ?? 1000;
        if (!Number.isSafeInteger(this.#retention) || this.#retention < 1 || this.#retention > 2_147_483_647
            || !Number.isSafeInteger(this.#maxCompleted) || this.#maxCompleted < 1) throw new Error('REACTIVE_ACTION_RETENTION_INVALID');
        const release = () => { for (const id of this.#invocations.keys()) this.evictInvocation(id); };
        signal.addEventListener('abort', release, { once: true });
        if (signal.aborted) release();
    }
    private evictInvocation(id: string): void {
        const entry = this.#invocations.get(id);
        if (entry?.completed) clearTimeout(entry.completed);
        this.#invocations.delete(id); this.#completed.delete(id);
    }
    /** 只核原受理事实，不读取会话或当前Expose，不取得新的成员资格。 */
    readInvocation(sessionId: string, id: string, expected: ProgramExpectation, options: SubmitOptions = {}, identity: ParticipantIdentity = humanParticipant): RetainedActionInvocation {
        this.assertActive(); options.signal?.throwIfAborted();
        if (expected === undefined) throw new Error('REACTIVE_PROGRAM_EXPECTATION_REQUIRED');
        const actorId = participantActor(identity, options, this.options.actorId);
        const entry = this.#invocations.get(id);
        if (!entry || entry.sessionId !== sessionId || entry.actorId !== actorId || entry.participant !== identity.participant) throw new InvocationUnavailableError();
        assertExpectedProgram(entry.program, expected);
        return entry.value;
    }
    private retainInvocation(session: ConversationSession, name: string, definition: ActionContract, invocation: RetainedActionInvocation['invocation'], identity: ParticipantIdentity, actorId: string): void {
        if (this.signal.aborted) return;
        const entry: RetainedEntry = { sessionId: session.header.id, program: session.implementation.definition,
            participant: identity.participant, actorId, value: Object.freeze({ name, definition, invocation }) };
        this.#invocations.set(invocation.id, entry);
        const settle = () => {
            if (this.signal.aborted || this.#invocations.get(invocation.id) !== entry) return;
            this.#completed.add(invocation.id);
            entry.completed = setTimeout(() => this.evictInvocation(invocation.id), this.#retention);
            entry.completed.unref();
            while (this.#completed.size > this.#maxCompleted) {
                const oldest = this.#completed.values().next().value;
                if (oldest !== undefined) this.evictInvocation(oldest);
            }
        };
        invocation.result.then(settle, settle);
    }
    private assertActive() { if (this.signal.aborted)
        throw new Error('REACTIVE_HOST_DISPOSED'); }
    nativeCall(execution: ActionExecutionContext) { return this.#toolCalls.get(execution); }
    /** 发生身份只来自签发时捕获的原生事实，不接受调用者声明，也不重查同名调用。 */
    callSequence(execution: ActionExecutionContext): number {
        const signed = this.#executions.get(execution);
        if (!signed || signed.callSequence === null || execution.source.kind !== 'tool')
            throw new Error('REACTIVE_ACTION_TOOL_EXECUTION_UNTRUSTED');
        return signed.callSequence;
    }
    assertActionExecution(session: ConversationSession, execution: ActionExecutionContext) {
        if (this.#executions.get(execution)?.session !== session)
            throw new Error('REACTIVE_ACTION_EXECUTION_UNTRUSTED');
        const native = this.#toolCalls.get(execution);
        if (execution.source.kind === 'tool' && (!native || native.entry.session !== session
            || this.ctx.agents.get(native.entry.agent.id) !== native.entry.agent))
            throw new Error('REACTIVE_ACTION_TOOL_AUTHORITY_REQUIRED');
    }
    private signExecution<T extends ActionExecutionContext>(session: ConversationSession, execution: T, callSequence: number | null, beforeExecute?: () => void): T {
        this.#executions.set(execution, { session, callSequence });
        guardActionExecution(execution, () => {
            beforeExecute?.();
            this.assertActionExecution(session, execution);
            this.sessions.assertSessionWorkspace(session);
        });
        return execution;
    }
    async admitAction(session: ConversationSession, action: ActionReference, input: unknown, call: ActionCallContext<unknown>, execution: ActionExecutionContext, admissionSignal: AbortSignal) {
        const native = this.#toolCalls.get(execution);
        const signal = AbortSignal.any([admissionSignal, this.signal, session.scope.signal, session.program.signal,
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
        await firstValueFrom(abortable(() => this.sessions.activate(session.header.id, session.accessId), signal));
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
        const execution: ToolExecution = { invocationId: JSON.stringify([entry.agent.id, occurrence.callSequence]), sessionId: entry.agent.id,
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
        return this.signExecution(entry.session, execution, occurrence.callSequence);
    }
    participantExecution(session: ConversationSession, options: SubmitOptions, identity: ParticipantIdentity, entrypoint: string, beforeExecute?: () => void): ParticipantExecution {
        this.assertActive();
        const actorId = participantActor(identity, options, this.options.actorId), participant = identity.participant;
        const signal = options.signal ? AbortSignal.any([this.signal, options.signal]) : this.signal;
        signal.throwIfAborted();
        // 独立参与调用没有继承当前模型输入或 Goal 续接权限。
        const execution: ParticipantExecution = { invocationId: randomUUID(), sessionId: session.header.id,
            source: { kind: 'participant', actorId, participant, entrypoint }, inputs: [], continuation: null, signal };
        return this.signExecution(session, execution, null, beforeExecute);
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
        // SDK调用者只撤销本次准入；真正执行权限仍归Host，Operation查询继续保原读侧signal。
        const { signal: admissionSignal, ...executionOptions } = options;
        const execution = this.participantExecution(session, executionOptions, identity, identity.participant === 'service' ? 'service-sdk' : 'sdk', () => { program.flush(); check(); });
        admissionSignal?.throwIfAborted();
        const invocation = program.invoke(action, input, execution, check, admissionSignal);
        this.retainInvocation(session, name, action.definition, invocation, identity, execution.source.actorId);
        return invocation;
    }
}
