import type { Context } from "@deepseek-ai/cordis";
import { firstValueFrom } from 'rxjs';
import type { Agent } from "@deepseek-ai/dsh-agent";
import type { GoalRef } from '@deepseek-ai/dsh-goal';
import type { UserMessage } from '@deepseek-ai/dsh-llm';
import type { ExecutionFailure } from '@hanger-source/dsh-reactive/kernel';
import { humanQuestionCall, humanQuestionOrigin } from './records.ts';
import { appendPluginRecord } from "@deepseek-ai/dsh-session";
import { createUserMessage } from "@deepseek-ai/dsh-llm";
import { batch, conversationBindings, type ConversationIdentity, type Continuation, type ModelPreparationAdmission, type Submission, type SubmitOptions } from "@hanger-source/dsh-reactive/kernel";
import { type ApplicationDefinition } from "@hanger-source/dsh-reactive/kernel";
import { contentDigest, submissionSourceDigest, canonical, recordSchema, submissionSchema, type SubmissionRecord } from "./records.ts";
import { admitPendingRecovery, followAdmittedInput, isRecoveryControl, resumePendingInput, retainClaimedInputs } from "./pending-input.ts";
import { type Workspaces } from "../workspaces/workspaces.ts";
import { admittedSubmissionSchema, createFileUploadInvocation, type AdmittedSubmission, type NormalizedSubmission, type FileUploadInvocation, type EncodedFileUploadRequest, type FileUploadReceiptId, type InputReference } from "@hanger-source/dsh-reactive/kernel";
import { admitInputContent, InputFileNotStagedError } from "../files/input-content.ts";
import { editQueuedInput } from "./queued-input.ts";
import { abortable, type Admission } from "@hanger-source/dsh-reactive";
import { type NativeDelegations } from "../delegations.ts";
import type { HostOptions } from '../applications/options.ts';
import { retainRecovery, type ConversationSession, type LiveConversation } from './session.ts';
import type { ConversationSessions } from './sessions.ts';
import type { ConversationObservations } from './observations.ts';
import type { OperationParticipation } from '../operations/participation.ts';
import { humanParticipant, serviceParticipant, participantActor, type ParticipantIdentity } from '../participation/identity.ts';
import { prepareGoalRound, admitGoalRound, requireGoalRoundDriver } from './goal-round.ts';
/** 受理正文与来源的唯一事务入口；实际队列和附件仍由原生服务持有。 */
export class ConversationInputs {
    // 每项只拥有准备取消与真实结算；存储与 receipt 仍归原生 exact Session。
    #uploads = new Map<ConversationSession, Map<AbortController, Promise<void>>>();
    #pending = new Map<string, {
        sourceDigest: string;
        signal: AbortSignal;
        promise: Promise<{
            sessionId: string;
            requestId: string;
            accepted: true;
        }>;
    }>();
    constructor(private readonly ctx: Context, private readonly definition: ApplicationDefinition, private readonly workspaces: Workspaces, private readonly options: HostOptions, private readonly signal: AbortSignal, private readonly sessions: ConversationSessions, private readonly facts: ConversationObservations, private readonly delegations: NativeDelegations, private readonly operations: () => OperationParticipation) { }
    private admit(input: AdmittedSubmission, conversation: ConversationIdentity, admission: Admission,
        retain?: (work: PromiseLike<unknown>) => void): Promise<void> {
        return firstValueFrom(abortable(() => {
            const work = this.definition.admit(input, conversation, admission);
            retain?.(work);
            return work;
        }, admission.signal));
    }
    bindConversation(session: ConversationSession): void {
        session.conversation[conversationBindings]().setSubmit((input, options) => {
            const signal = AbortSignal.any([session.program.signal, ...(options.signal ? [options.signal] : [])]);
            return this.submitSession(session, input, { ...options, signal }, serviceParticipant(this.options.serviceActorId));
        });
    }
    async prepare(entry: LiveConversation, signal: AbortSignal, onAdmissions: (admissions: readonly ModelPreparationAdmission[]) => void) {
        const { agent } = entry;
        signal.throwIfAborted();
        this.sessions.assertSessionWorkspace(entry.session);
        this.workspaces.assertMember(agent.session, entry.session.workspace, entry.session.environment);
        await this.delegations.settlementNotices(agent, entry.claimed, signal);
        signal.throwIfAborted();
        this.sessions.assertSessionWorkspace(entry.session);
        const currentGoal = this.ctx.goals.get(agent);
        if (currentGoal?.activation === 'armed' && (entry.session.state.hasWaits(currentGoal)
            || this.operations().hasPendingGoal(agent, currentGoal)))
            this.ctx.goals.disarm(agent);
        const inputs = entry.session.state.inputs(entry.claimed);
        const goalRound = prepareGoalRound(this.ctx, entry);
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
            await this.admit(admittedSubmissionSchema.parse(source), entry.session.conversation.identity, { actorId, participant, signal });
            signal.throwIfAborted();
            this.sessions.assertSessionWorkspace(entry.session);
            if (record.intent === 'goal') {
                const goal = this.ctx.goals.get(agent);
                const bound = entry.session.state.hasGoalInput(record.requestId);
                if (!bound && (!goal || goal.phase === 'complete')) {
                    const created = this.ctx.goals.create(agent, { objective: instruction });
                    appendPluginRecord(agent.session, 'plugin:reactive/goal-inputs', { goalId: created.id, inputs });
                }
            }
        }
        const independent = entry.claimed.some(message => {
            if (isRecoveryControl(message) || message.source.kind === 'user-question-reply') return false;
            const requestId = 'rpcId' in message.source && typeof message.source.rpcId === 'string' ? message.source.rpcId : undefined;
            return requestId === undefined || !entry.session.state.record(requestId)?.result;
        });
        const results = entry.claimed.flatMap(message => {
            const requestId = 'rpcId' in message.source && typeof message.source.rpcId === 'string' ? message.source.rpcId : undefined;
            const record = requestId === undefined ? undefined : entry.session.state.record(requestId);
            return record?.result ? [{ record, message }] : [];
        });
        const replies = entry.claimed.flatMap(message => message.source.kind === 'user-question-reply'
            ? [{ message, origin: humanQuestionOrigin(entry.session.state.value, message.source.callId) }] : []);
        const hasNewInput = entry.claimed.some(message => !isRecoveryControl(message)
            && message.source.kind !== 'user-question-reply'
            && (message.source.kind === 'goal' || entry.session.state.reference(message).length > 0));
        // resume 同步发布 Goal；同批签出精确来源，观察者不读取到中间修订。
        return batch(() => {
            // 结果先以原 continuation 取得准入，再由本次原生 resume 明确签出新 revision；prompt 使用同一 revision。
            const preparingGoal = this.ctx.goals.get(agent);
            let admittedGoal: GoalRef | null = preparingGoal ? Object.freeze({ id: preparingGoal.id, revision: preparingGoal.revision }) : null;
            const admitted = independent ? [] : results.filter(({ record }) => this.operations().shouldWake(entry, record));
            const goalResults = admitted.filter(({ record }) => Boolean(record.result?.continuation.goal));
            const humanReady = (continuation: Continuation | null) => {
                if (!continuation) return false;
                const expected = continuation.goal;
                if (!expected) return true;
                const current = this.ctx.goals.get(agent);
                return current?.phase === 'active' && current.id === expected.id && current.revision === expected.revision;
            };
            const admittedReplies = independent ? [] : replies.filter(({ origin }) => humanReady(origin.continuation));
            const goalReplies = admittedReplies.filter(({ origin }) => Boolean(origin.continuation?.goal));
            const hasGoal = goalResults.length > 0 || goalReplies.length > 0;
            let resumed: GoalRef | undefined;
            if (!independent) {
                const result = goalResults[0]?.record.result;
                const goal = this.ctx.goals.get(agent);
                if (result?.continuation.goal && goal?.phase === 'active' && goal.activation === 'disarmed'
                    && goal.id === result.continuation.goal.id && goal.revision === result.continuation.goal.revision) {
                    const next = this.ctx.goals.resume(agent, goal);
                    resumed = admittedGoal = Object.freeze({ id: next.id, revision: next.revision });
                }
            }
            const ready = () => {
                if (!independent && hasGoal) {
                    const current = this.ctx.goals.get(agent);
                    if (current ? admittedGoal?.id !== current.id || admittedGoal.revision !== current.revision : admittedGoal !== null) return false;
                }
                if (independent || (!results.length && !replies.length)) return true;
                return (hasGoal ? goalResults : admitted).some(({ record }) => this.operations().resultReady(entry, record))
                    || (hasGoal ? goalReplies : admittedReplies).some(({ origin }) => humanReady(origin.continuation));
            };
            let released = false;
            const release = () => {
                if (released) return;
                released = true;
                if (!resumed || this.ctx.agents.get(agent.id) !== agent) return;
                const current = this.ctx.goals.get(agent);
                if (current?.id === resumed.id && current.revision === resumed.revision && current.activation === 'armed')
                    this.ctx.goals.disarm(agent);
            };
            const feedbackOnly = !independent && (results.length > 0 || replies.length > 0);
            let goal: GoalRef | null | undefined = hasGoal ? admittedGoal : null;
            if (!hasNewInput && !feedbackOnly) goal = entry.progress.continuedGoal();
            const admissions: ModelPreparationAdmission[] = [];
            for (const { record, message } of hasGoal ? goalResults : admitted) {
                if (!record.result || !this.operations().resultReady(entry, record)) continue;
                admissions.push({ source: { kind: 'result', requestId: record.requestId, messageId: message.id },
                    continuation: record.result.continuation, goal: record.result.continuation.goal ? admittedGoal : null });
            }
            for (const { message, origin } of hasGoal ? goalReplies : admittedReplies) {
                if (!origin.continuation || !humanReady(origin.continuation)) continue;
                admissions.push({ source: { kind: 'human-reply', messageId: message.id, questionCallId: origin.callId, callSequence: origin.callSequence },
                    continuation: origin.continuation, goal: origin.continuation.goal });
            }
            onAdmissions(admissions);
            return { ready, release, fixedGoal: Boolean(goalRound) || !hasNewInput || feedbackOnly,
                goal: goalRound ? undefined : goal, goalRound };
        });
    }
    async revalidateCauses(causes: readonly InputReference[], signal: AbortSignal, seen = new Set<string>()) {
        for (const cause of causes) {
            signal.throwIfAborted();
            const key = canonical([cause.sourceSessionId, cause.messageId]);
            if (seen.has(key))
                continue;
            seen.add(key);
            const inspection = await this.facts.inspect(cause.sourceSessionId);
            signal.throwIfAborted();
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
            await this.admit(admittedSubmissionSchema.parse(submission), { sessionId: inspection.meta.id, workspaceId: workspace.native.id, environmentId: environment.id,
                directory: workspace.native.path, parentSessionId: inspection.meta.parentSession ?? null }, { actorId, participant, signal });
            signal.throwIfAborted();
            this.workspaces.assertMemberHeader(inspection.meta, workspace, environment);
        }
    }
    async editSessionInput(session: ConversationSession, requestId: string, content: Submission['content'], options: SubmitOptions = {}, identity: ParticipantIdentity = humanParticipant) {
        this.sessions.assertSessionWorkspace(session);
        const actorId = participantActor(identity, options, this.options.actorId), participant = identity.participant;
        const signal = AbortSignal.any([this.signal, session.scope.signal, ...(options.signal ? [options.signal] : [])]);
        signal.throwIfAborted();
        return firstValueFrom(abortable(async () => {
            await this.sessions.activate(session.header.id, session.accessId);
            signal.throwIfAborted();
            const entry = this.sessions.live(session);
            return editQueuedInput({ ctx: this.ctx, agent: entry.agent, requestId, content, actorId, participant, signal,
                state: () => this.facts.liveState(entry.agent.session),
                assertCurrent: () => this.assertCurrent(entry),
                admit: input => this.admit(input, session.conversation.identity, { actorId, participant, signal }),
                persist: () => this.facts.flush(entry.agent),
                persistAccepted: () => this.finishAccepted(entry, requestId),
            });
        }, signal));
    }
    async submitSession(session: ConversationSession, raw: unknown, options: SubmitOptions, identity: ParticipantIdentity = humanParticipant,
        retain?: (work: PromiseLike<unknown>) => void): Promise<{
        sessionId: string;
        requestId: string;
        accepted: true;
    }> {
        const signal = AbortSignal.any([this.signal, session.scope.signal, ...(options.signal ? [options.signal] : [])]);
        const actorId = participantActor(identity, options, this.options.actorId), participant = identity.participant;
        signal.throwIfAborted();
        this.sessions.assertSessionWorkspace(session);
        const input = submissionSchema.parse(raw);
        session.implementation.definition.input.parse(input.scope);
        const sourceDigest = submissionSourceDigest(input, actorId, participant);
        const key = JSON.stringify([session.header.id, input.requestId]);
        for (;;) {
            const pending = this.#pending.get(key);
            if (!pending) break;
            if (pending.sourceDigest !== sourceDigest)
                throw new Error('REACTIVE_REQUEST_ID_CONFLICT');
            retain?.(pending.promise);
            if (!pending.signal.aborted)
                return firstValueFrom(abortable(() => pending.promise, signal));
            // 本次显式重提等待已取消发布者真正退出；不提前清表或重发仍在执行的命令。
            await firstValueFrom(abortable(() => pending.promise.then(() => {}, () => {}), signal));
            signal.throwIfAborted();
            this.sessions.assertSessionWorkspace(session);
        }
        // 在任何附件存储或准入回调之前占住同一请求；并发重试不重复提升图片。
        const promise = Promise.resolve().then(() => this.prepareSubmission(session, input, actorId, participant, sourceDigest, signal, retain));
        this.#pending.set(key, { sourceDigest, signal, promise });
        const clear = () => {
            if (this.#pending.get(key)?.promise === promise)
                this.#pending.delete(key);
        };
        // 清表跟随真实事务；即便首个等待在订阅前已取消，worker 拒绝仍有观察者。
        void promise.then(clear, clear);
        retain?.(promise);
        return firstValueFrom(abortable(() => promise, signal));
    }
    uploadSessionFile(session: ConversationSession, prepare: (signal: AbortSignal) => EncodedFileUploadRequest | PromiseLike<EncodedFileUploadRequest>, options: SubmitOptions, identity: ParticipantIdentity = humanParticipant, waitSignal: AbortSignal = this.signal): FileUploadInvocation {
        participantActor(identity, options, this.options.actorId);
        this.sessions.assertUploadOpen(session);
        const preparation = new AbortController(), callerSignal = options.signal;
        const preparationSignal = AbortSignal.any([this.signal, session.scope.signal, waitSignal, preparation.signal,
            ...(callerSignal ? [callerSignal] : [])]);
        preparationSignal.throwIfAborted();
        const invocation = createFileUploadInvocation({ sessionId: session.header.id, preparationSignal,
            waitSignal: AbortSignal.any([this.signal, waitSignal, ...(callerSignal ? [callerSignal] : [])]),
            prepare: async signal => {
                signal.throwIfAborted();
                const encoded = await prepare(signal);
                signal.throwIfAborted();
                this.sessions.assertUploadOpen(session);
                await this.sessions.activate(session.header.id, session.accessId);
                signal.throwIfAborted();
                this.sessions.assertUploadOpen(session);
                return { entry: this.sessions.live(session), encoded };
            },
            execute: async ({ entry, encoded }) => {
                // 原生入口之后保留实际结算；调用者退等不能覆盖已签发的 receipt。
                return structuredClone(await this.ctx.fileUploads.upload(entry.agent, encoded, preparationSignal));
            },
        });
        let uploads = this.#uploads.get(session);
        if (!uploads) this.#uploads.set(session, uploads = new Map());
        const pending = uploads;
        const settled = invocation.result.then(() => {}, () => {}).finally(() => {
            pending.delete(preparation);
            if (!pending.size && this.#uploads.get(session) === pending) this.#uploads.delete(session);
        });
        pending.set(preparation, settled);
        return invocation;
    }
    /** 调用方先截新准入；准备立即退出，已分派存储按真实 Promise 结算。 */
    async drainUploads(session?: ConversationSession): Promise<void> {
        const pending = session ? [this.#uploads.get(session)].filter(value => value !== undefined) : [...this.#uploads.values()];
        for (const uploads of pending)
            for (const preparation of uploads.keys()) preparation.abort(new Error('REACTIVE_FILE_UPLOAD_CLOSING'));
        await Promise.all(pending.flatMap(uploads => [...uploads.values()]));
    }
    async prepareSubmission(session: ConversationSession, input: NormalizedSubmission, actorId: string, participant: Admission['participant'], sourceDigest: string, signal: AbortSignal,
        retain?: (work: PromiseLike<unknown>) => void) {
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
        await this.admit(admittedSubmissionSchema.parse(submission), session.conversation.identity, { actorId, participant, signal }, retain);
        signal.throwIfAborted();
        this.sessions.assertSessionWorkspace(session);
        if (receiptOwner && session.live.value?.agent !== receiptOwner)
            throw new InputFileNotStagedError();
        await this.sessions.activate(session.header.id, session.accessId);
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
        this.assertCurrent(entry);
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
            // not-ready 在投影中已经结算，不经历 inbox；该事实的持久化同样不归等待者取消。
            if (input.result?.ready === false) {
                await this.finishAccepted(entry, input.requestId);
                return { sessionId: entry.agent.id as string, requestId: input.requestId, accepted: true as const };
            }
            // 关联先持久化；重试核对原生 inbox，不把关联记录当作已经受理。
            await this.facts.flush(entry.agent);
            signal.throwIfAborted();
            this.assertCurrent(entry);
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
                retainRecovery(entry, followAdmittedInput(entry.agent, input.requestId, () => this.operations().shouldWake(entry, input)),
                    'reactive.input.recovery.failed', { requestId: input.requestId });
            }
        }
        await this.finishAccepted(entry, input.requestId, accepted === 'accepted' && this.operations().shouldWake(entry, input)
            ? () => resumePendingInput(entry.agent, input.requestId, () => this.operations().shouldWake(entry, input)) : undefined);
        return { sessionId: entry.agent.id as string, requestId: input.requestId, accepted: true as const };
    }
    private assertCurrent(entry: LiveConversation) {
        this.sessions.assertSessionWorkspace(entry.session);
        if (entry.session.live.value !== entry || this.ctx.agents.get(entry.agent.id) !== entry.agent)
            throw new Error('REACTIVE_CONVERSATION_NOT_RUNNING');
    }
    private finishAccepted(entry: LiveConversation, requestId: string, resume?: () => Promise<void>): Promise<void> {
        return retainRecovery(entry, (async () => {
            // 真实受理已发生；只核原生执行owner，不用已退出的读取图覆盖这份持久回执。
            const assertOwner = () => {
                if (this.ctx.agents.get(entry.agent.id) !== entry.agent) throw new Error('REACTIVE_CONVERSATION_NOT_RUNNING');
            };
            if (resume) {
                assertOwner();
                await resume();
            }
            await this.facts.flush(entry.agent);
            assertOwner();
        })(), 'reactive.input.commit.failed', { requestId });
    }
    /** 失败事实先发布；离开原生 append 后仅接回本次未入历史的原晚答，不重答或唤醒。 */
    retainFailedReplies(entry: LiveConversation, failure: ExecutionFailure, signal: AbortSignal | undefined): void {
        if (!failure.unentered.some(item => item.questionCallId !== null)) return;
        void retainRecovery(entry, Promise.resolve().then(async () => {
            if (!signal) throw new Error('REACTIVE_REPLY_FAILURE_SIGNAL_REQUIRED');
            await this.restoreFailedReplies(entry, [failure], signal);
        }), 'reactive.human.reply-retention.failed', { failureSequence: failure.sequence, turn: failure.turn });
    }
    /** 实际 Agent 准入与 live 交接共用；failure 仅限定原消息，内容及终止事实仍由原生日志决定。 */
    async restoreFailedReplies(entry: LiveConversation, failures: readonly ExecutionFailure[], signal: AbortSignal): Promise<void> {
        const selected = new Map<string, ExecutionFailure['unentered'][number]>();
        for (const failure of failures) for (const cause of failure.unentered) if (cause.questionCallId !== null) {
            const previous = selected.get(cause.messageId);
            if (previous && (previous.questionCallId !== cause.questionCallId || canonical(previous.inputs) !== canonical(cause.inputs)))
                throw new Error('REACTIVE_REPLY_FAILURE_CONFLICT');
            selected.set(cause.messageId, cause);
        }
        const causes = [...selected.values()];
        if (!causes.length) return;
        const failureSequence = Math.max(...failures.map(failure => failure.sequence));
        const stillOwned = () => !signal.aborted && !this.signal.aborted && !entry.scope.disposed && !entry.session.scope.disposed
            && entry.session.live.value === entry && this.ctx.agents.get(entry.agent.id) === entry.agent;
        if (!stillOwned()) return;
        this.sessions.assertSessionWorkspace(entry.session);
        const ids = new Set(causes.map(item => item.messageId));
        const originals = new Map<string, UserMessage>();
        const consumed = new Set<string>();
        const removed = new Map<string, 'claimed' | 'cancelled'>();
        const claimOrder = new Map<string, number>();
        const claimTurns = new Map<string, number | null>();
        let nextOrder = 0;
        let openTurn: number | null = null;
        const queues: Record<'next-turn' | 'next-step', string[]> = { 'next-turn': [], 'next-step': [] };
        // failure 的精确 ID 限定查找范围；日志只提供原内容和后续消费/取消事实，不签发重放权限。
        for (const event of entry.agent.session.snapshotEvents()) {
            if (event.type === 'turn/start') openTurn = event.data.turn;
            if (event.type === 'turn/end') {
                if (event.data.reason.kind === 'aborted') for (const [id, turn] of claimTurns)
                    if (turn === event.data.turn && removed.get(id) === 'claimed') removed.set(id, 'cancelled');
                openTurn = null;
            }
            if (event.type === 'user/message' && ids.has(event.data.id)) consumed.add(event.data.id);
            if (event.type !== 'agent/inbox/spliced') continue;
            const { target, start, removedCount = 0, inserted, outcome } = event.data;
            const taken = queues[target].splice(start, removedCount, ...inserted.map(message => message.id));
            for (const message of inserted) if (ids.has(message.id)) {
                const original = originals.get(message.id);
                if (original && canonical(original) !== canonical(message)) throw new Error('REACTIVE_REPLY_MESSAGE_CONFLICT');
                if (event.seq >= entry.agent.session.inheritedEventCount && event.seq <= failureSequence) originals.set(message.id, message);
                removed.delete(message.id);
                claimTurns.delete(message.id);
            }
            for (const id of taken) if (ids.has(id)) {
                removed.set(id, outcome === 'canceled' ? 'cancelled' : 'claimed');
                claimOrder.set(id, nextOrder++);
                claimTurns.set(id, openTurn);
            }
        }
        const questions = this.ctx.sessionProjections.snapshot(entry.agent.session, ['userQuestions']).values.userQuestions;
        if (!questions) throw new Error('REACTIVE_HUMAN_QUESTIONS_PROJECTION_REQUIRED');
        // 统一按真实 claim 顺序逆序 prepend，避免跨 failure 分批恢复翻转原队列。
        const ordered = causes.filter(cause => removed.get(cause.messageId) === 'claimed')
            .sort((left, right) => claimOrder.get(right.messageId)! - claimOrder.get(left.messageId)!);
        let discarded = false;
        const stopDiscarded = this.ctx.on('agent/inbox/discarded', ({ agent, message }) => {
            if (agent === entry.agent && ids.has(message.id)) discarded = true;
        });
        try {
            for (const cause of ordered) {
                const original = originals.get(cause.messageId);
                if (!original || original.source.kind !== 'user-question-reply' || original.source.callId !== cause.questionCallId)
                    throw new Error('REACTIVE_REPLY_ORIGINAL_REQUIRED');
                const questionCallId = original.source.callId;
                const call = humanQuestionCall(entry.session.state.value, questionCallId);
                if (canonical(call.inputs) !== canonical(cause.inputs)) throw new Error('REACTIVE_REPLY_INPUT_CONFLICT');
                if (consumed.has(cause.messageId) || removed.get(cause.messageId) === 'cancelled') continue;
                if (!questions.active.some(question => question.callId === questionCallId && question.state === 'continued')) continue;
                const pending = [...entry.agent.inbox.nextStep, ...entry.agent.inbox.nextTurn];
                if (pending.some(message => message.source.kind === 'user-question-reply'
                    && message.source.callId === questionCallId && message.id !== original.id)
                    || Object.values(entry.session.state.value.proposed).some(item => item.questionCallId === cause.questionCallId && item.messageId !== original.id))
                    throw new Error('REACTIVE_REPLY_DELIVERY_CONFLICT');
                if (pending.some(message => message.id === original.id)) continue;
                if (removed.get(original.id) !== 'claimed') throw new Error('REACTIVE_REPLY_CLAIM_REQUIRED');
                if (!stillOwned() || discarded) return;
                retainClaimedInputs(entry.agent, [original], cause.target);
                if (!stillOwned() || discarded) return;
            }
        } finally { stopDiscarded(); await this.facts.flush(entry.agent); }
    }
    retainPausedResults(entry: LiveConversation): void {
        // 原生 pause 会取消活动 turn 并保留 inbox；已 claim 的结果须先回到同一 inbox。
        const signal = entry.stepSignal;
        if (!signal) return;
        const stillOwned = () => !signal.aborted && !this.signal.aborted && !entry.scope.disposed && !entry.session.scope.disposed
            && entry.session.live.value === entry && this.ctx.agents.get(entry.agent.id) === entry.agent;
        if (!stillOwned()) return;
        const retained = entry.claimed.filter(message => {
            if (message.source.kind === 'user-question-reply') {
                if ([...entry.agent.inbox.nextStep, ...entry.agent.inbox.nextTurn].some(pending => pending.id === message.id)) return false;
                const call = humanQuestionCall(entry.session.state.value, message.source.callId);
                const proposal = entry.session.state.value.proposed[message.id];
                if (!proposal || proposal.questionCallId !== message.source.callId || canonical(call.inputs) !== canonical(proposal.inputs))
                    throw new Error('REACTIVE_REPLY_INPUT_CONFLICT');
                return true;
            }
            const requestId = 'rpcId' in message.source && typeof message.source.rpcId === 'string' ? message.source.rpcId : undefined;
            return requestId !== undefined && entry.session.state.record(requestId)?.result?.ready === true
                && entry.session.state.activity.requests[requestId]?.state === 'accepted';
        });
        for (const message of retained.toReversed()) {
            if (!stillOwned()) return;
            retainClaimedInputs(entry.agent, [message], 'next-step');
            if (!stillOwned()) return;
        }
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
            const prepared = entry.progress.preparedFor(signal);
            const admissionSignal = AbortSignal.any([signal, entry.scope.signal, entry.session.scope.signal]);
            try {
                if (prepared.goalRound) requireGoalRoundDriver(this.ctx, prepared.goalRound.driver);
                return await admitPendingRecovery(agent, messages, signal, async () => {
                    const decision = await firstValueFrom(abortable(() => next(), admissionSignal));
                    if (decision.kind !== 'enter') {
                        if (!admissionSignal.aborted) retainClaimedInputs(agent, messages, 'next-step');
                        return decision;
                    }
                    admissionSignal.throwIfAborted();
                    this.sessions.assertSessionWorkspace(entry.session);
                    const goal = prepared.goalRound ? admitGoalRound(this.ctx, entry, prepared.goalRound, decision.messages) : prepared.goal;
                    if (prepared.goalRound && goal === undefined) {
                        retainClaimedInputs(agent, messages, 'next-step');
                        return { kind: 'reject' };
                    }
                    if (!prepared.ready()) {
                        retainClaimedInputs(agent, messages, 'next-step');
                        return { kind: 'reject' };
                    }
                    if (!prepared.current()) throw new Error('REACTIVE_STEP_GOAL_CHANGED');
                    const entered = { ...decision, messages: decision.messages.filter(message => message.source.kind !== 'reactive-resume-signal') };
                    const inputs = entry.session.state.inputs(entered.messages);
                    if (canonical(inputs) !== canonical(prepared.inputs)) throw new Error('REACTIVE_STEP_INPUTS_CHANGED');
                    // 只等待本轮真实业务输入关联落入消费事实；无关联的原生上下文不延长准备现场。
                    entry.progress.admitted(turn, step, inputs, prepared,
                        entered.messages.filter(message => entry.session.state.reference(message).length > 0).map(message => message.id), goal);
                    return entered;
                });
            } finally { entry.progress.discardPreparation(prepared); }
        }, { prepend: true });
    }
}
