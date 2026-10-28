import type { Context } from "@deepseek-ai/cordis";
import type { Agent } from "@deepseek-ai/dsh-agent";
import { SessionLogOffset, type Session } from "@deepseek-ai/dsh-session";
import { SessionPersistenceNotFoundError, type SessionInspection } from "@deepseek-ai/dsh-session-persistence";
import { goalProjectionDefinition } from "@deepseek-ai/dsh-goal";
import { conversationBindings } from "@hanger-source/dsh-reactive/kernel";
import { humanInputSnapshot } from "../participation/human-input.ts";
import { NativeState, type ConversationActivity } from "./records.ts";
import type { ConversationEvent } from "@hanger-source/dsh-reactive/kernel";
import { reactiveProjectionSchema } from "./projections.ts";
import { sessionOccurrences, assistantFrame } from "./events.ts";
import { readNativeInspection } from "../delegations.ts";
import type { ConversationSession, LiveConversation } from './session.ts';
import type { ConversationSessions } from './sessions.ts';
/** 官方投影、流式前缀与精确日志读取；不启动或持有 Agent。 */
export class ConversationObservations {
    constructor(private readonly ctx: Context, private readonly signal: AbortSignal) { }
    private assertActive() { if (this.signal.aborted)
        throw new Error('REACTIVE_HOST_DISPOSED'); }
    emit(entry: ConversationSession, event: ConversationEvent) {
        // Conversation 在发生边界固定当前订阅者，并通过标准调度延迟解码与观察通知。
        if (this.signal.aborted || entry.conversation.scope.disposed)
            return;
        try {
            entry.conversation[conversationBindings]().emit(event);
        }
        catch (error) {
            entry.conversation.scope.observerError(error);
        }
    }
    publish(entry: ConversationSession) {
        const native = entry.live.value;
        const goal = native ? this.ctx.goals.get(native.agent) : undefined;
        const activity: ConversationActivity = native ? { ...entry.state.activity, source: 'live', execution: native.agent.status,
            goal: goal ? { id: goal.id, revision: goal.revision, objective: goal.objective, phase: goal.phase,
                ...(goal.blockedReason ? { blockedReason: goal.blockedReason } : {}), maxGoalRounds: goal.maxGoalRounds, roundsStarted: goal.roundsStarted, activation: goal.activation } : null }
            : entry.state.activity;
        entry.conversation[conversationBindings]().publish(activity);
    }
    liveState(session: Session) {
        const state = this.ctx.sessionProjections.stateOf(session, 'reactiveInputs');
        const goal = this.ctx.sessionProjections.stateOf(session, 'goal');
        if (!state || !goal)
            throw new Error('REACTIVE_DSH_PROJECTION_REQUIRED');
        if (goal.failure)
            throw new Error('REACTIVE_GOAL_PROJECTION_INVALID: ' + goal.failure);
        const cursor = this.ctx.sessionProjections.snapshot(session, []).asOfSeq;
        return new NativeState(state, goal.current, cursor);
    }
    stateOf(inspection: SessionInspection) {
        const live = this.ctx.sessions.get(inspection.meta.id);
        if (live) {
            const state = this.liveState(live);
            return { state, questions: humanInputSnapshot(this.ctx.sessionProjections.snapshot(live, ['userQuestions']), state.value) };
        }
        const restored = this.ctx.sessionProjections.restore({}, inspection.events, SessionLogOffset(0), inspection.meta, inspection.inheritedEventCount);
        const state = reactiveProjectionSchema.parse(restored.checkpoint.reactiveInputs?.val);
        const goal = goalProjectionDefinition.stateSchema.parse(restored.checkpoint.goal?.val);
        if (goal.failure)
            throw new Error('REACTIVE_GOAL_PROJECTION_INVALID: ' + goal.failure);
        return { state: new NativeState(state, goal.current, restored.snapshot.asOfSeq),
            questions: humanInputSnapshot(restored.snapshot, state) };
    }
    async inspect(sessionId: string, signal?: AbortSignal): Promise<SessionInspection> {
        try {
            return await readNativeInspection(this.ctx, sessionId, signal);
        }
        catch (cause) {
            if (cause instanceof SessionPersistenceNotFoundError)
                throw new Error('REACTIVE_CONVERSATION_NOT_FOUND', { cause });
            throw cause;
        }
    }
    async flush(agent: Agent) {
        if (!await this.ctx.sessions.flush(agent.session))
            throw new Error('REACTIVE_PERSISTENCE_UNAVAILABLE');
    }
    install(sessions: Pick<ConversationSessions, 'forAgent' | 'get'>, onGoalPause: (entry: LiveConversation) => void): void {
        this.ctx.on('session/event', (session, event) => {
            const agent = this.ctx.agents.get(session.id);
            const entry = agent && sessions.forAgent(agent);
            if (!entry)
                return;
            const previous = entry.session.state;
            if (event.type === 'turn/start' || event.type === 'turn/end') {
                entry.claimed = [];
                entry.stepSignal = undefined;
                entry.progress.endedTurn();
            }
            else if (event.type === 'user/message') {
                const messageId = event.data.id;
                entry.claimed = entry.claimed.filter(message => message.id !== messageId);
            }
            entry.session.state = this.liveState(session);
            this.publish(entry.session);
            for (const occurrence of sessionOccurrences(session.id, event, previous, entry.session.state, (turn, step) => entry.progress.inputs(turn, step)))
                this.emit(entry.session, occurrence);
        });
        this.ctx.on('agent/status', ({ agent, status }) => {
            const entry = sessions.forAgent(agent);
            if (!entry)
                return;
            this.publish(entry.session);
            this.emit(entry.session, { sessionId: agent.id, time: Date.now(), sequence: null, kind: 'execution.changed', execution: status });
        });
        this.ctx.on('goal/changed', ({ agent, change }) => {
            const entry = sessions.forAgent(agent);
            if (!entry)
                return;
            if (change.operation === 'pause')
                onGoalPause(entry);
            this.publish(entry.session);
        }, { prepend: true });
        this.ctx.on('goal/activation-changed', ({ sessionId, goal }) => {
            const entry = sessions.get(sessionId);
            if (!entry?.live.value)
                return;
            this.publish(entry);
            if (goal)
                this.emit(entry, { sessionId, time: Date.now(), sequence: null, kind: 'goal.activation',
                    goalId: goal.id, revision: goal.revision, activation: goal.activation });
        });
        this.ctx.on('agent/assistant-stream', ({ agent, frame }) => {
            const entry = sessions.forAgent(agent);
            if (!entry)
                return;
            const inputs = entry.progress.accept(frame, entry.session.state.current);
            entry.session.conversation[conversationBindings]().emitProgressFrame({ inputs, frame });
            const value = assistantFrame(frame);
            if (value)
                this.emit(entry.session, { sessionId: agent.id, time: frame.type === 'chunk' ? frame.time : Date.now(), sequence: null,
                    kind: 'assistant.stream', inputs, frame: value });
        });
        this.ctx.on('agent/error', ({ agent, turn, step, error }) => {
            const entry = sessions.forAgent(agent);
            if (!entry)
                return;
            this.publish(entry.session);
            const inputs = entry.progress.inputs(turn, step);
            this.emit(entry.session, { sessionId: agent.id, time: Date.now(), sequence: null, kind: 'execution.error', error, turn, step,
                ...(inputs === undefined ? {} : { inputs }) });
        });
    }
}
