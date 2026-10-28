import type { Context } from "@deepseek-ai/cordis";
import { firstValueFrom } from 'rxjs';
import { abortable } from '@hanger-source/dsh-reactive';
import { type SubmitOptions } from "@hanger-source/dsh-reactive/kernel";
import { humanAnswerSchema, type HumanWaitFrame } from "@hanger-source/dsh-reactive/kernel";
import { canonical } from "../conversations/records.ts";
import type { HostOptions } from '../applications/options.ts';
import type { ConversationSession } from '../conversations/session.ts';
import type { ConversationSessions } from '../conversations/sessions.ts';
import type { ConversationObservations } from '../conversations/observations.ts';
import { admitHumanResponder, assertHumanAnswerBatch } from './human-input.ts';
/** 人工答复保留原问题与响应者准入；冷读不重建前台等待。 */
export class ConversationQuestions {
    constructor(private readonly ctx: Context, private readonly options: HostOptions, private readonly signal: AbortSignal, private readonly sessions: ConversationSessions, private readonly facts: ConversationObservations) { }
    private assertActive() { if (this.signal.aborted)
        throw new Error('REACTIVE_HOST_DISPOSED'); }
    async answerQuestion(session: ConversationSession, id: string, raw: unknown, options: SubmitOptions = {}): Promise<boolean> {
        this.assertActive();
        if (typeof id !== 'string' || !id.trim())
            throw new Error('REACTIVE_HUMAN_QUESTION_CALL_REQUIRED');
        const actorId = options.actorId ?? this.options.actorId;
        if (!actorId?.trim())
            throw new Error('REACTIVE_HUMAN_RESPONDER_REQUIRED');
        const signal = AbortSignal.any([this.signal, session.program.signal, ...options.signal ? [options.signal] : []]);
        signal.throwIfAborted();
        // 在任何异步读取或准入之前取得解析后的自有输入；恢复后不再消费调用方的可变 raw。
        const answer = humanAnswerSchema.parse(raw);
        if (!session.live.value) {
            session.program.flush();
            const policy = session.program.humanInput;
            if (!policy)
                throw new Error('REACTIVE_HUMAN_INPUT_NOT_DECLARED');
            const assertPolicy = () => {
                signal.throwIfAborted();
                this.assertActive();
                session.program.flush();
                if (session.program.humanInput !== policy)
                    throw new Error('REACTIVE_HUMAN_INPUT_CHANGED');
            };
            const readQuestion = async () => {
                const inspection = await firstValueFrom(abortable(reading => this.facts.inspect(session.header.id, reading), signal));
                signal.throwIfAborted();
                return this.facts.stateOf(inspection).questions.active.find(item => item.callId === id);
            };
            const question = await readQuestion();
            assertPolicy();
            if (!question || question.state !== 'continued')
                throw new Error('REACTIVE_HUMAN_FOREGROUND_UNAVAILABLE');
            assertHumanAnswerBatch(question, answer);
            await admitHumanResponder(session.program, session.scope, policy, { channel: 'continued',
                question: structuredClone(question), answer: structuredClone(answer), actorId, signal });
            assertPolicy();
            const current = await readQuestion();
            assertPolicy();
            if (!current || canonical(current) !== canonical(question))
                throw new Error('REACTIVE_HUMAN_QUESTION_CHANGED');
            // 取消结束当前阶段的等待；迟到的读取或准入没有继续激活 Agent 的续体。
            await firstValueFrom(abortable(() => this.sessions.activate(session.header.id), signal));
            assertPolicy();
            const restored = this.sessions.live(session).human.snapshot().active.find(item => item.callId === id);
            if (!restored || canonical(restored) !== canonical(question))
                throw new Error('REACTIVE_HUMAN_QUESTION_CHANGED');
        }
        signal.throwIfAborted();
        return this.sessions.live(session).human.answerQuestion(id, answer, { actorId, signal });
    }
    async *attachWait(session: ConversationSession, id: string, options: SubmitOptions = {}): AsyncIterable<HumanWaitFrame> {
        this.assertActive();
        const actorId = options.actorId ?? this.options.actorId;
        if (!actorId?.trim())
            throw new Error('REACTIVE_HUMAN_RESPONDER_REQUIRED');
        if (!session.live.value)
            throw new Error('REACTIVE_HUMAN_FOREGROUND_UNAVAILABLE');
        const signal = options.signal ? AbortSignal.any([this.signal, options.signal]) : this.signal;
        yield* session.live.value.human.attachWait(id, { actorId, signal });
    }
}
