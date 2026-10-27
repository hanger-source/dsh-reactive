import type { Context } from "@deepseek-ai/cordis";
import { type SubmitOptions } from "@hanger-source/dsh-reactive/kernel";
import { humanAnswerSchema, type HumanWaitFrame } from "@hanger-source/dsh-reactive/kernel";
import { canonical } from "../conversations/records.ts";
import type { HostOptions } from '../applications/options.ts';
import type { ConversationSession } from '../conversations/session.ts';
import type { ConversationSessions } from '../conversations/sessions.ts';
import type { ConversationObservations } from '../conversations/observations.ts';
/** 人工答复保留原问题与响应者准入；冷读不重建前台等待。 */
export class ConversationQuestions {
    constructor(private readonly ctx: Context, private readonly options: HostOptions, private readonly signal: AbortSignal, private readonly sessions: ConversationSessions, private readonly facts: ConversationObservations) { }
    private assertActive() { if (this.signal.aborted)
        throw new Error('REACTIVE_HOST_DISPOSED'); }
    async answerQuestion(session: ConversationSession, id: string, raw: unknown, options: SubmitOptions = {}): Promise<boolean> {
        this.assertActive();
        const actorId = options.actorId ?? this.options.actorId;
        if (!actorId?.trim())
            throw new Error('REACTIVE_HUMAN_RESPONDER_REQUIRED');
        const signal = options.signal ? AbortSignal.any([this.signal, options.signal]) : this.signal;
        signal.throwIfAborted();
        if (!session.live) {
            session.program.flush();
            const policy = session.program.humanInput;
            if (!policy)
                throw new Error('REACTIVE_HUMAN_INPUT_NOT_DECLARED');
            const { questions: state } = this.facts.stateOf(await this.facts.inspect(session.header.id));
            const question = state.active.find(item => item.callId === id);
            if (!question || question.state !== 'continued')
                throw new Error('REACTIVE_HUMAN_FOREGROUND_UNAVAILABLE');
            const answer = humanAnswerSchema.parse(raw);
            await policy.admitResponder({ channel: 'continued', question: structuredClone(question), answer: structuredClone(answer), actorId, signal });
            const current = this.facts.stateOf(await this.facts.inspect(session.header.id)).questions.active.find(item => item.callId === id);
            signal.throwIfAborted();
            session.program.flush();
            if (session.program.humanInput !== policy)
                throw new Error('REACTIVE_HUMAN_INPUT_CHANGED');
            if (!current || canonical(current) !== canonical(question))
                throw new Error('REACTIVE_HUMAN_QUESTION_CHANGED');
            await this.sessions.activate(session.header.id);
        }
        return this.sessions.live(session).human.answerQuestion(id, raw, { actorId, signal });
    }
    async *attachWait(session: ConversationSession, id: string, options: SubmitOptions = {}): AsyncIterable<HumanWaitFrame> {
        this.assertActive();
        const actorId = options.actorId ?? this.options.actorId;
        if (!actorId?.trim())
            throw new Error('REACTIVE_HUMAN_RESPONDER_REQUIRED');
        if (!session.live)
            throw new Error('REACTIVE_HUMAN_FOREGROUND_UNAVAILABLE');
        const signal = options.signal ? AbortSignal.any([this.signal, options.signal]) : this.signal;
        yield* session.live.human.attachWait(id, { actorId, signal });
    }
}
