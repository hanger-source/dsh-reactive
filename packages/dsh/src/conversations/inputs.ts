import type { Context } from "@deepseek-ai/cordis";
import { firstValueFrom } from 'rxjs';
import type { Agent } from "@deepseek-ai/dsh-agent";
import { appendPluginRecord } from "@deepseek-ai/dsh-session";
import { createUserMessage } from "@deepseek-ai/dsh-llm";
import { conversationBindings, type ConversationIdentity, type Submission, type SubmitOptions } from "@hanger-source/dsh-reactive/kernel";
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
    private admit(input: AdmittedSubmission, conversation: ConversationIdentity, admission: Admission): Promise<void> {
        return firstValueFrom(abortable(() => this.definition.admit(input, conversation, admission), admission.signal));
    }
    bindConversation(session: ConversationSession): void {
        session.conversation[conversationBindings]().setSubmit((input, options) => {
            const signal = AbortSignal.any([session.program.signal, ...(options.signal ? [options.signal] : [])]);
            return this.submitSession(session, input, { ...options, signal }, serviceParticipant(this.options.serviceActorId));
        });
    }
    async prepare(entry: LiveConversation, signal: AbortSignal) {
        const { agent } = entry;
        signal.throwIfAborted();
        this.sessions.assertSessionWorkspace(entry.session);
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
    async submitSession(session: ConversationSession, raw: unknown, options: SubmitOptions, identity: ParticipantIdentity = humanParticipant): Promise<{
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
            if (!pending.signal.aborted)
                return firstValueFrom(abortable(() => pending.promise, signal));
            // 本次显式重提等待已取消发布者真正退出；不提前清表或重发仍在执行的命令。
            await firstValueFrom(abortable(() => pending.promise.then(() => {}, () => {}), signal));
            signal.throwIfAborted();
            this.sessions.assertSessionWorkspace(session);
        }
        // 在任何附件存储或准入回调之前占住同一请求；并发重试不重复提升图片。
        const promise = Promise.resolve().then(() => this.prepareSubmission(session, input, actorId, participant, sourceDigest, signal));
        this.#pending.set(key, { sourceDigest, signal, promise });
        const clear = () => {
            if (this.#pending.get(key)?.promise === promise)
                this.#pending.delete(key);
        };
        // 清表跟随真实事务；即便首个等待在订阅前已取消，worker 拒绝仍有观察者。
        void promise.then(clear, clear);
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
        await this.admit(admittedSubmissionSchema.parse(submission), session.conversation.identity, { actorId, participant, signal });
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
            const admissionSignal = AbortSignal.any([signal, entry.scope.signal, entry.session.scope.signal]);
            return admitPendingRecovery(agent, messages, signal, async () => {
                const decision = await firstValueFrom(abortable(() => next(), admissionSignal));
                if (decision.kind !== 'enter')
                    return decision;
                admissionSignal.throwIfAborted();
                this.sessions.assertSessionWorkspace(entry.session);
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
