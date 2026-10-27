import type { Context } from "@deepseek-ai/cordis";
import type { Agent } from "@deepseek-ai/dsh-agent";
import { appendPluginRecord } from "@deepseek-ai/dsh-session";
import { createUserMessage } from "@deepseek-ai/dsh-llm";
import { conversationBindings, type Submission, type CancellationOptions, type SubmitOptions } from "@hanger-source/dsh-reactive/kernel";
import { type ApplicationDefinition } from "@hanger-source/dsh-reactive/kernel";
import { contentDigest, submissionSourceDigest, canonical, recordSchema, submissionSchema, type SubmissionRecord } from "./records.ts";
import { admitPendingRecovery, followAdmittedInput, isRecoveryControl, resumePendingInput, retainClaimedInputs } from "./pending-input.ts";
import { type Workspaces } from "../workspaces/workspaces.ts";
import { admittedSubmissionSchema, type AdmittedSubmission, type NormalizedSubmission, type FileUploadValue, type EncodedFileUploadRequest, type FileUploadReceiptId, type InputReference } from "@hanger-source/dsh-reactive/kernel";
import { admitInputContent, InputFileNotStagedError } from "../files/input-content.ts";
import { editQueuedInput } from "./queued-input.ts";
import type { Admission } from "@hanger-source/dsh-reactive";
import { type NativeDelegations } from "../delegations.ts";
import type { HostOptions } from '../applications/options.ts';
import type { ConversationSession, LiveConversation } from './session.ts';
import type { ConversationSessions } from './sessions.ts';
import type { ConversationObservations } from './observations.ts';
import type { OperationParticipation } from '../operations/participation.ts';
export function assertServiceInputOptions(options: CancellationOptions) {
    if ('actorId' in options)
        throw new Error('REACTIVE_SERVICE_INPUT_ACTOR_FIXED');
}
/** 受理正文与来源的唯一事务入口；实际队列和附件仍由原生服务持有。 */
export class ConversationInputs {
    #pending = new Map<string, {
        sourceDigest: string;
        promise: Promise<{
            sessionId: string;
            requestId: string;
            accepted: true;
        }>;
    }>();
    constructor(private readonly ctx: Context, private readonly definition: ApplicationDefinition, private readonly workspaces: Workspaces, private readonly options: HostOptions, private readonly signal: AbortSignal, private readonly sessions: ConversationSessions, private readonly facts: ConversationObservations, private readonly delegations: NativeDelegations, private readonly operations: () => OperationParticipation) { }
    bindConversation(session: ConversationSession): void {
        session.conversation[conversationBindings]().setSubmit((input, options) => {
            assertServiceInputOptions(options);
            return this.submitSession(session, input, options, 'service');
        });
    }
    async prepare(entry: LiveConversation, signal: AbortSignal) {
        const { agent } = entry;
        this.workspaces.assertMember(agent.session, entry.session.workspace, entry.session.environment);
        await this.delegations.settlementNotices(agent, entry.claimed, signal);
        const currentGoal = this.ctx.goals.get(agent);
        if (currentGoal?.activation === 'armed' && (entry.session.state.hasWaits(currentGoal)
            || this.operations().hasPendingGoal(agent, currentGoal)))
            this.ctx.goals.disarm(agent);
        const inputs = entry.session.state.inputs(entry.claimed);
        if (entry.session.child && entry.claimed.some(message => ['user', 'agent-message'].includes(message.source.kind)
            && entry.session.state.reference(message).length === 0))
            throw new Error('REACTIVE_CHILD_INPUT_ASSOCIATION_REQUIRED');
        if (inputs.some(input => input.actorId === null && input.source !== 'delegation'))
            throw new Error('REACTIVE_INPUT_ACTOR_REQUIRED');
        entry.session.conversation[conversationBindings]().accept(inputs);
        for (const message of entry.claimed) {
            const rpcId = 'rpcId' in message.source && typeof message.source.rpcId === 'string' ? message.source.rpcId : null;
            const record = rpcId ? entry.session.state.record(rpcId) : undefined;
            if (!record)
                continue;
            const instruction = message.content.flatMap(block => block.type === 'text' ? [block.text] : []).join('\n');
            const { digest: _digest, sourceDigest: _sourceDigest, actorId, participant, ...source } = record;
            await this.definition.admit(admittedSubmissionSchema.parse(source), entry.session.conversation.identity, { actorId, participant, signal });
            if (record.intent === 'goal') {
                const goal = this.ctx.goals.get(agent);
                const bound = entry.session.state.hasGoalInput(record.requestId);
                if (!bound && (!goal || goal.phase === 'complete')) {
                    const created = this.ctx.goals.create(agent, { objective: instruction });
                    appendPluginRecord(agent.session, 'plugin:reactive/goal-inputs', { goalId: created.id, inputs });
                }
            }
        }
    }
    async revalidateCauses(causes: readonly InputReference[], signal: AbortSignal, seen = new Set<string>()) {
        for (const cause of causes) {
            signal.throwIfAborted();
            const key = canonical([cause.sourceSessionId, cause.messageId]);
            if (seen.has(key))
                continue;
            seen.add(key);
            const inspection = await this.facts.inspect(cause.sourceSessionId);
            const { workspace, environment } = this.workspaces.resolve(inspection);
            if (!workspace.native.sessionIds.includes(inspection.meta.id))
                throw new Error('REACTIVE_WORKSPACE_MEMBERSHIP_REQUIRED');
            const messages = inspection.events.slice(inspection.inheritedEventCount).flatMap(event => event.type === 'user/message' ? [event.data] : event.type === 'agent/inbox/spliced' ? event.data.inserted : []);
            const message = messages.findLast(item => item.id === cause.messageId);
            const state = this.facts.stateOf(inspection).state;
            if (!message || !state.reference(message).some(item => canonical(item) === canonical(cause)))
                throw new Error('REACTIVE_DELEGATION_CAUSE_MISMATCH');
            if (cause.source === 'delegation') {
                const child = await this.delegations.describeChild(inspection);
                await this.revalidateCauses(child.record.causes, signal, seen);
                continue;
            }
            const record = cause.requestId ? state.record(cause.requestId) : undefined;
            if (!record || !cause.actorId || record.actorId !== cause.actorId || record.participant !== cause.participant)
                throw new Error('REACTIVE_DELEGATION_INPUT_ORIGIN_REQUIRED');
            const { digest: _digest, sourceDigest: _sourceDigest, actorId, participant, ...submission } = record;
            await this.definition.admit(admittedSubmissionSchema.parse(submission), { sessionId: inspection.meta.id, workspaceId: workspace.native.id, environmentId: environment.id,
                directory: workspace.native.path, parentSessionId: inspection.meta.parentSession ?? null }, { actorId, participant, signal });
        }
    }
    async editSessionInput(session: ConversationSession, requestId: string, content: Submission['content'], options: SubmitOptions = {}, participant: Admission['participant'] = 'human') {
        this.sessions.assertSessionWorkspace(session);
        const actorId = options.actorId ?? (participant === 'service' ? this.options.serviceActorId : this.options.actorId);
        if (!actorId?.trim())
            throw new Error('REACTIVE_INPUT_ACTOR_REQUIRED');
        const signal = options.signal ? AbortSignal.any([this.signal, options.signal]) : this.signal;
        signal.throwIfAborted();
        await this.sessions.activate(session.header.id);
        const entry = this.sessions.live(session);
        return editQueuedInput({ ctx: this.ctx, agent: entry.agent, requestId, content, actorId, participant, signal,
            state: () => this.facts.liveState(entry.agent.session),
            assertCurrent: () => { this.sessions.assertSessionWorkspace(session); if (session.live.value !== entry)
                throw new Error('REACTIVE_CONVERSATION_NOT_RUNNING'); },
            admit: input => Promise.resolve(this.definition.admit(input, session.conversation.identity, { actorId, participant, signal })),
            persist: () => this.facts.flush(entry.agent),
        });
    }
    async submitSession(session: ConversationSession, raw: unknown, options: SubmitOptions, participant: Admission['participant'] = 'human'): Promise<{
        sessionId: string;
        requestId: string;
        accepted: true;
    }> {
        const signal = options.signal ?? new AbortController().signal;
        const actorId = options.actorId ?? (participant === 'service' ? this.options.serviceActorId : this.options.actorId);
        if (!actorId?.trim())
            throw new Error('REACTIVE_INPUT_ACTOR_REQUIRED');
        signal.throwIfAborted();
        this.sessions.assertSessionWorkspace(session);
        const input = submissionSchema.parse(raw);
        session.implementation.definition.input.parse(input.scope);
        const sourceDigest = submissionSourceDigest(input, actorId, participant);
        const key = JSON.stringify([session.header.id, input.requestId]);
        const pending = this.#pending.get(key);
        if (pending) {
            if (pending.sourceDigest !== sourceDigest)
                throw new Error('REACTIVE_REQUEST_ID_CONFLICT');
            return pending.promise;
        }
        // 在任何附件存储或准入回调之前占住同一请求；并发重试不重复提升图片。
        const promise = Promise.resolve().then(() => this.prepareSubmission(session, input, actorId, participant, sourceDigest, signal));
        this.#pending.set(key, { sourceDigest, promise });
        try {
            return await promise;
        }
        finally {
            if (this.#pending.get(key)?.promise === promise)
                this.#pending.delete(key);
        }
    }
    async uploadSessionFile(session: ConversationSession, request: EncodedFileUploadRequest, options: SubmitOptions): Promise<FileUploadValue> {
        const actorId = options.actorId ?? this.options.actorId;
        if (!actorId?.trim())
            throw new Error('REACTIVE_INPUT_ACTOR_REQUIRED');
        const signal = options.signal ?? new AbortController().signal;
        const encoded = { data: request.data, ...(request.name === undefined ? {} : { name: request.name }) };
        signal.throwIfAborted();
        this.sessions.assertSessionWorkspace(session);
        await this.sessions.activate(session.header.id);
        signal.throwIfAborted();
        this.sessions.assertSessionWorkspace(session);
        const value = await this.ctx.fileUploads.upload(this.sessions.live(session).agent, encoded, signal);
        signal.throwIfAborted();
        this.sessions.assertSessionWorkspace(session);
        return structuredClone(value);
    }
    async prepareSubmission(session: ConversationSession, input: NormalizedSubmission, actorId: string, participant: Admission['participant'], sourceDigest: string, signal: AbortSignal) {
        signal.throwIfAborted();
        this.sessions.assertSessionWorkspace(session);
        const previous = session.state.record(input.requestId);
        if (previous && (previous.sourceDigest !== sourceDigest || previous.actorId !== actorId || previous.participant !== participant))
            throw new Error('REACTIVE_REQUEST_ID_CONFLICT');
        let receiptIds: readonly FileUploadReceiptId[] = [];
        let receiptOwner: Agent | undefined;
        let record: SubmissionRecord;
        if (previous)
            record = recordSchema.parse(previous);
        else {
            const agent = session.live.value?.agent;
            const admitted = await admitInputContent(this.ctx, agent, input.content);
            signal.throwIfAborted();
            this.sessions.assertSessionWorkspace(session);
            receiptIds = admitted.receiptIds;
            if (receiptIds.length)
                receiptOwner = agent;
            record = recordSchema.parse({ ...input, content: admitted.content, actorId, participant, sourceDigest, digest: contentDigest(admitted.content) });
        }
        const { digest: _digest, sourceDigest: _sourceDigest, actorId: _actor, participant: _participant, ...submission } = record;
        // 策略得到独立正文快照；即便策略修改参数，也不能改写已固定的投递和持久事实。
        await this.definition.admit(admittedSubmissionSchema.parse(submission), session.conversation.identity, { actorId, participant, signal });
        signal.throwIfAborted();
        this.sessions.assertSessionWorkspace(session);
        if (receiptOwner && session.live.value?.agent !== receiptOwner)
            throw new InputFileNotStagedError();
        await this.sessions.activate(session.header.id);
        signal.throwIfAborted();
        this.sessions.assertSessionWorkspace(session);
        const entry = this.sessions.live(session);
        if (receiptOwner && entry.agent !== receiptOwner)
            throw new InputFileNotStagedError();
        if (previous) {
            // 持久正文已拥有准入事实；仍存活的原 receipt 只参与本次绑定与消费退役。
            receiptIds = [...new Set(input.content.flatMap(part => part.type === 'file'
                    && this.ctx.fileUploads.resolve(entry.agent, part.receiptId) ? [part.receiptId] : []))];
        }
        return this.accept(entry, record, receiptIds, signal);
    }
    async accept(entry: LiveConversation, record: SubmissionRecord, receiptIds: readonly FileUploadReceiptId[], signal: AbortSignal) {
        signal.throwIfAborted();
        this.sessions.assertSessionWorkspace(entry.session);
        if (entry.session.live.value !== entry)
            throw new Error('REACTIVE_CONVERSATION_NOT_RUNNING');
        const input: AdmittedSubmission = record;
        const previous = entry.session.state.record(input.requestId);
        if (previous && canonical(previous) !== canonical(record))
            throw new Error('REACTIVE_REQUEST_ID_CONFLICT');
        const accepted = entry.session.state.activity.requests[input.requestId]?.state;
        if (!accepted || accepted === 'declared') {
            if (input.result && !previous)
                this.operations().validateContinuation(entry, input.result.continuation, input.result.wait);
            if (!previous)
                appendPluginRecord(entry.agent.session, 'plugin:reactive/input', record);
            // 关联先持久化；重试核对原生 inbox，不把关联记录当作已经受理。
            await this.facts.flush(entry.agent);
            signal.throwIfAborted();
            this.sessions.assertSessionWorkspace(entry.session);
            if (entry.session.live.value !== entry)
                throw new Error('REACTIVE_CONVERSATION_NOT_RUNNING');
            // 未就绪是已受理的业务进度；它既不是模型输入，也不占用另一条待执行队列。
            if (input.result?.ready === false)
                return { sessionId: entry.agent.id as string, requestId: input.requestId, accepted: true as const };
            const wake = this.operations().shouldWake(entry, input);
            signal.throwIfAborted();
            const target = input.intent === 'result' || input.delivery !== 'followup' ? 'next-step' : 'next-turn';
            const binding = receiptIds.length ? this.ctx.fileUploads.bindPrompt(entry.agent, receiptIds, input.requestId) : undefined;
            try {
                entry.agent.send(createUserMessage({ source: { kind: 'user', rpcId: input.requestId }, content: structuredClone([...record.content]) }), target, wake);
                binding?.commit();
            }
            finally {
                binding?.[Symbol.dispose]();
            }
            if (wake) {
                const recovery = followAdmittedInput(entry.agent, input.requestId, () => this.operations().shouldWake(entry, input)).catch(error => {
                    entry.recoveryErrors.push(error);
                    console.error('reactive.input.recovery.failed', { sessionId: entry.agent.id, requestId: input.requestId, error });
                }).finally(() => { entry.recovery.delete(recovery); });
                entry.recovery.add(recovery);
            }
        }
        else if (accepted === 'accepted' && this.operations().shouldWake(entry, input)) {
            signal.throwIfAborted();
            await resumePendingInput(entry.agent, input.requestId, () => this.operations().shouldWake(entry, input));
            signal.throwIfAborted();
        }
        signal.throwIfAborted();
        await this.facts.flush(entry.agent);
        signal.throwIfAborted();
        return { sessionId: entry.agent.id as string, requestId: input.requestId, accepted: true as const };
    }
    retainPausedResults(entry: LiveConversation): void {
        // 原生 pause 会取消活动 turn 并保留 inbox；已 claim 的结果须先回到同一 inbox。
        if (entry.stepSignal && !entry.stepSignal.aborted)
            retainClaimedInputs(entry.agent, entry.claimed.filter(message => {
                const requestId = 'rpcId' in message.source && typeof message.source.rpcId === 'string' ? message.source.rpcId : undefined;
                return requestId !== undefined && entry.session.state.record(requestId)?.result?.ready === true
                    && entry.session.state.activity.requests[requestId]?.state === 'accepted';
            }));
    }
    installClaimed(): void {
        this.ctx.on('agent/inbox/claimed', ({ agent, message }) => {
            const entry = this.sessions.forAgent(agent);
            if (entry)
                entry.claimed.push(message);
        });
    }
    installPreStep(): void {
        this.ctx.on('agent/pre-step', async ({ agent, messages, signal, turn, step }, next) => {
            const entry = this.sessions.forAgent(agent);
            if (!entry)
                return next();
            entry.stepSignal = signal;
            return admitPendingRecovery(agent, messages, signal, async () => {
                const decision = await next();
                if (decision.kind !== 'enter')
                    return decision;
                signal.throwIfAborted();
                const independent = messages.some(message => {
                    if (isRecoveryControl(message))
                        return false;
                    const requestId = 'rpcId' in message.source && typeof message.source.rpcId === 'string' ? message.source.rpcId : undefined;
                    return requestId === undefined || !entry.session.state.record(requestId)?.result;
                });
                const results = messages.flatMap(message => {
                    const requestId = 'rpcId' in message.source && typeof message.source.rpcId === 'string' ? message.source.rpcId : undefined;
                    const record = requestId === undefined ? undefined : entry.session.state.record(requestId);
                    return record?.result ? [record] : [];
                });
                if (!independent && results.length && !results.some(record => this.operations().shouldWake(entry, record))) {
                    retainClaimedInputs(agent, messages);
                    return { kind: 'reject' };
                }
                // 人的新要求可以处理其他工作；同行旧结果不借此取得 Goal 续接权限。
                if (!independent) {
                    const result = results.find(record => this.operations().shouldWake(entry, record))?.result;
                    const goal = this.ctx.goals.get(agent);
                    if (result?.continuation.goal && goal?.phase === 'active' && goal.activation === 'disarmed'
                        && goal.id === result.continuation.goal.id && goal.revision === result.continuation.goal.revision) {
                        this.ctx.goals.resume(agent, goal);
                    }
                }
                const entered = { ...decision, messages: decision.messages.filter(message => message.source.kind !== 'reactive-resume-signal') };
                entry.progress.admitted(turn, step, entry.session.state.inputs(entered.messages));
                return entered;
            });
        }, { prepend: true });
    }
}
