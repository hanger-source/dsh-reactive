import type { Context } from '@deepseek-ai/cordis';
import type { Agent } from '@deepseek-ai/dsh-agent';
import { ToolCallId } from '@deepseek-ai/dsh-llm';
import type { ProjectionSnapshot } from '@deepseek-ai/dsh-session-projection';
import { UserQuestionError, type AskUserQuestionRequest } from '@deepseek-ai/dsh-user-questions';
import { Observable, Subject, firstValueFrom } from 'rxjs';
import { abortable, type HumanResponderAdmission } from '@hanger-source/dsh-reactive';
import { HumanQuestionSourceConflictError, captureHumanQuestionReference, humanAnswerSchema, type HumanQuestionReference, type HumanQuestionSourceConflict, type HumanAnswer, type HumanSnapshot, type HumanReplyDelivery, type HumanQuestion, type HumanQuestionOrigin, type HumanForegroundQuestion, type HumanResponderOptions, type HumanWaitFrame } from '@hanger-source/dsh-reactive/kernel';
import type { HumanInputDeclaration } from '@hanger-source/dsh-reactive/kernel';
import { Scope, type Value } from '@hanger-source/dsh-reactive/kernel';
import type { ReactiveProjectionState } from '../conversations/projections.ts';
import { canonical, humanQuestionOrigin, occurrenceOf } from '../conversations/records.ts';

interface HumanInputProgram {
  readonly humanInput: HumanInputDeclaration | undefined;
  readonly revision: Value<number>;
  readonly signal: AbortSignal;
  flush(): void;
}
type ForegroundResponse = { readonly signal: AbortSignal; readonly question: HumanQuestion;
  readonly timed: boolean; answer(answer: HumanAnswer): void };
type Claim = { readonly policy: HumanInputDeclaration; readonly origin: HumanQuestionOrigin; readonly controller: AbortController };
type QuestionAddress = { readonly channel: 'foreground' | 'continued'; readonly question: HumanQuestion; readonly response?: ForegroundResponse };
type ResponseAdmission = QuestionAddress & { readonly policy: HumanInputDeclaration; readonly signal: AbortSignal };

function ownHumanFact<T>(value: T): T {
  const owned = structuredClone(value);
  const freeze = (item: unknown): void => {
    if (item === null || typeof item !== 'object') return;
    for (const child of Object.values(item)) freeze(child);
    Object.freeze(item);
  };
  freeze(owned);
  return owned;
}

function questionOrigin(source: Pick<ReactiveProjectionState, 'sessionId' | 'calls' | 'questionOrigins'>, callId: string): HumanQuestionOrigin {
  return ownHumanFact(humanQuestionOrigin(source, callId));
}

/** live 与 cold 以同一原生投影切面关联真实调用；不从当前输入推测旧问题来源。 */
export function humanInputSnapshot(snapshot: ProjectionSnapshot, source: Pick<ReactiveProjectionState, 'sessionId' | 'calls' | 'proposed' | 'questionOrigins' | 'questionEvidence'>,
  foreground: readonly HumanForegroundQuestion[]): HumanSnapshot {
  const questions = snapshot.values.userQuestions;
  if (!questions) throw new Error('REACTIVE_HUMAN_QUESTIONS_PROJECTION_REQUIRED');
  const inbox = snapshot.values.inbox;
  if (!inbox) throw new Error('REACTIVE_HUMAN_INBOX_PROJECTION_REQUIRED');
  const sourceConflicts: HumanQuestionSourceConflict[] = [];
  for (const [key, evidence] of Object.entries(source.questionEvidence)) {
    const call = source.calls[key];
    if (!call) throw new Error('REACTIVE_HUMAN_QUESTION_CALL_NOT_FOUND');
    const origin = questionOrigin(source, call.callId);
    for (const conflict of evidence.conflicts) sourceConflicts.push({ origin, reason: conflict.reason,
      eventSequence: conflict.sequence, occurrence: conflict.callSequence === null ? null : occurrenceOf(source.calls[String(conflict.callSequence)]!) });
  }
  for (const settled of questions.settled) {
    const origin = questionOrigin(source, settled.callId);
    if (sourceConflicts.some(conflict => conflict.origin.callSequence === origin.callSequence)) continue;
    const evidence = source.questionEvidence[String(origin.callSequence)];
    const candidates = evidence ? [
      ...evidence.results.filter(result => result.kind === 'answer').map(result => result.answer),
      ...evidence.consumedReplies.filter(reply => reply.answer !== null).map(reply => reply.answer),
    ] : [];
    if (candidates.length !== 1 || canonical(candidates[0]?.answers) !== canonical(settled.answers)) {
      sourceConflicts.push({ origin, reason: 'unverified-answer', eventSequence: null, occurrence: null });
    }
  }
  const trusted = (origin: HumanQuestionOrigin) => !sourceConflicts.some(conflict => conflict.origin.sessionId === origin.sessionId
    && conflict.origin.callSequence === origin.callSequence);
  const replies: HumanReplyDelivery[] = [];
  for (const target of ['next-turn', 'next-step'] as const) for (const message of inbox[target]) {
    if (message === null || typeof message !== 'object' || Array.isArray(message) || !('source' in message)) throw new Error('REACTIVE_HUMAN_INBOX_MESSAGE_INVALID');
    const replySource = message.source;
    if (replySource === null || typeof replySource !== 'object' || Array.isArray(replySource) || !('kind' in replySource)) throw new Error('REACTIVE_HUMAN_INBOX_SOURCE_INVALID');
    if (replySource.kind !== 'user-question-reply') continue;
    if (!('id' in message) || typeof message.id !== 'string' || !('callId' in replySource) || typeof replySource.callId !== 'string') throw new Error('REACTIVE_HUMAN_REPLY_ID_REQUIRED');
    const origin = questionOrigin(source, replySource.callId);
    replies.push({ messageId: message.id, questionCallId: ToolCallId(replySource.callId), origin, state: 'queued', target });
  }
  for (const proposed of Object.values(source.proposed)) if (proposed.questionCallId !== null && proposed.turn !== null) {
    if (replies.some(reply => reply.messageId === proposed.messageId)) throw new Error('REACTIVE_HUMAN_REPLY_LOCATION_CONFLICT');
    const origin = questionOrigin(source, proposed.questionCallId);
    replies.push({ messageId: proposed.messageId, questionCallId: ToolCallId(proposed.questionCallId), origin, state: 'proposed', target: proposed.target });
  }
  return ownHumanFact({
    questions: {
      active: questions.active.map(question => ({ ...question, origin: questionOrigin(source, question.callId) })).filter(question => trusted(question.origin)),
      settled: questions.settled.map(question => ({ ...question, origin: questionOrigin(source, question.callId) })).filter(question => trusted(question.origin)),
    }, foreground: foreground.filter(question => trusted(question.origin)), replies: replies.filter(reply => trusted(reply.origin)), sourceConflicts,
  });
}

/** @internal 冷预审与 live 受理核验同一完整答案批次，不以恢复 Agent 代替输入验证。 */
export function assertHumanAnswerBatch(question: HumanQuestion, answer: HumanAnswer): void {
  const ids = new Set(answer.answers.map(item => item.id));
  if (ids.size !== answer.answers.length || question.questions.length !== answer.answers.length
    || question.questions.some(item => !ids.has(item.id))) {
    throw new UserQuestionError('the answer batch must name each question exactly once', 'BAD_ANSWER');
  }
}

export function assertHumanQuestionTarget(expected: HumanQuestionReference | undefined, sessionId: string, callId: string): void {
  if (expected && (expected.sessionId !== sessionId || expected.callId !== callId))
    throw new Error('REACTIVE_HUMAN_QUESTION_REFERENCE_MISMATCH');
}
export function assertHumanQuestionSource(snapshot: HumanSnapshot, callId: string): void {
  const conflicts = snapshot.sourceConflicts.filter(conflict => conflict.origin.callId === callId);
  if (conflicts.length) throw new HumanQuestionSourceConflictError(conflicts);
}
export function assertHumanQuestionReference(origin: HumanQuestionOrigin, expected: HumanQuestionReference | undefined): void {
  if (expected && (origin.sessionId !== expected.sessionId || origin.callId !== expected.callId || origin.callSequence !== expected.callSequence))
    throw new Error('REACTIVE_HUMAN_QUESTION_CHANGED');
}

/** @internal 本次准入只借用当前声明；声明撤销或等待退出立即退等，结算后释放 revision 依赖。 */
export async function admitHumanResponder(program: HumanInputProgram, scope: Scope,
  policy: HumanInputDeclaration, admission: HumanResponderAdmission): Promise<void> {
  const changed = new AbortController();
  const signal = AbortSignal.any([scope.signal, program.signal, admission.signal, changed.signal]);
  signal.throwIfAborted();
  const stop = scope.effect(() => {
    program.revision.get();
    if (program.humanInput !== policy) changed.abort(new Error('REACTIVE_HUMAN_INPUT_CHANGED'));
  });
  try {
    await firstValueFrom(abortable(() => policy.admitResponder({ ...admission, signal }), signal));
  } finally { stop(); }
}

/** 一个真实 Agent 的前台 answerer 接点；持久问题及晚答输入始终归原生服务所有。 */
export class HumanInputs {
  readonly snapshot$: Observable<HumanSnapshot>;
  readonly scope: Scope;
  // 这里只保留原生 waterfall 的未返回 continuation，不存问题状态或待执行输入。
  #foreground = new Map<ToolCallId, ForegroundResponse>();
  #claims = new Set<Claim>();
  #foregroundChanged = new Subject<void>();

  constructor(private readonly ctx: Context, readonly agent: Agent,
    private readonly program: HumanInputProgram, parent: Scope) {
    this.scope = parent.child();
    this.scope.own(() => this.#foregroundChanged.complete());
    this.scope.own(agent.ctx.on('user-questions/request', request => this.request(request)));
    this.scope.effect(() => {
      program.revision.get();
      for (const claim of this.#claims) if (claim.policy !== program.humanInput) {
        claim.controller.abort(new Error('REACTIVE_HUMAN_INPUT_CHANGED'));
      }
    });
    this.scope.own(ctx.on('session/event', session => {
      if (session !== agent.session || !this.#claims.size) return;
      const snapshot = this.snapshot();
      for (const claim of this.#claims) {
        const conflicts = snapshot.sourceConflicts.filter(conflict => conflict.origin.sessionId === claim.origin.sessionId
          && conflict.origin.callSequence === claim.origin.callSequence);
        if (conflicts.length) claim.controller.abort(new HumanQuestionSourceConflictError(conflicts));
      }
    }));
    this.snapshot$ = new Observable(subscriber => {
      if (this.scope.disposed) { subscriber.complete(); return; }
      const release = this.scope.own(() => subscriber.complete());
      const emit = () => {
        if (this.scope.disposed) { subscriber.complete(); return; }
        try { subscriber.next(this.snapshot()); }
        catch (error) { subscriber.error(error); }
      };
      // session/event 在注册表完成同一事件的全部同步投影后交付。
      const stop = ctx.on('session/event', (session, event) => {
        if (session === agent.session && ['agent/inbox/spliced', 'turn/start', 'turn/end', 'user/message',
          'tool/call', 'tool/result', 'tool/ptc-dispatch-start', 'tool/ptc-dispatch'].includes(event.type)) emit();
      });
      const subscription = this.#foregroundChanged.subscribe({ next: emit, complete: () => subscriber.complete() });
      emit();
      return () => { stop(); subscription.unsubscribe(); release(); };
    });
  }

  snapshot(): HumanSnapshot {
    this.scope.assertActive();
    const foreground = [...this.#foreground.entries()].map(([callId, response]) => ({
      callId, questions: response.question.questions, timed: response.timed, origin: response.question.origin,
    }));
    // snapshot/stateOf 都同步 materialize 全部注册 unit 到同一 Session cursor；两次读取之间不允许 await。
    const snapshot = this.ctx.sessionProjections.snapshot(this.agent.session, ['userQuestions', 'inbox']);
    return humanInputSnapshot(snapshot, this.origins(), foreground);
  }

  private origins(): ReactiveProjectionState {
    const state = this.ctx.sessionProjections.stateOf(this.agent.session, 'reactiveInputs');
    if (!state || state.sessionId !== this.agent.id) throw new Error('REACTIVE_DSH_PROJECTION_REQUIRED');
    return state;
  }

  private assertRoot() {
    this.scope.assertActive();
    if (this.ctx.agents.get(this.agent.id) !== this.agent)
      throw new UserQuestionError('human interaction requires the exact live calling agent', 'CALLER_NOT_LIVE');
    if (!this.ctx.agents.roots().includes(this.agent))
      throw new UserQuestionError('human interaction is unavailable to a delegated Agent', 'DELEGATED_CALLER');
  }
  private question(callId: ToolCallId): QuestionAddress {
    const snapshot = this.snapshot();
    assertHumanQuestionSource(snapshot, callId);
    const response = this.#foreground.get(callId);
    if (response && !response.signal.aborted) return { channel: 'foreground', question: response.question, response };
    const question = snapshot.questions.active.find(question => question.callId === callId);
    if (!question) throw new Error('REACTIVE_HUMAN_QUESTION_NOT_OPEN');
    if (question.state !== 'continued') throw new Error('REACTIVE_HUMAN_FOREGROUND_UNAVAILABLE');
    return { channel: 'continued', question };
  }

  private request(request: AskUserQuestionRequest): Promise<HumanAnswer> {
    this.program.flush();
    if (!this.program.humanInput) throw new Error('REACTIVE_HUMAN_INPUT_NOT_DECLARED');
    this.assertRoot();
    if (request.agent !== this.agent) throw new Error('REACTIVE_HUMAN_QUESTION_OWNER_MISMATCH');
    const callId = request.wait?.callId;
    if (!callId) throw new Error('REACTIVE_HUMAN_QUESTION_CALL_REQUIRED');
    if (this.#foreground.has(callId)) throw new Error('REACTIVE_HUMAN_QUESTION_DUPLICATE_REQUEST');
    const signal = request.signal ? AbortSignal.any([request.signal, this.scope.signal]) : this.scope.signal;
    signal.throwIfAborted();
    const origin = questionOrigin(this.origins(), callId);
    return new Promise<HumanAnswer>((resolve, reject) => {
      const finish = (settle: () => void) => {
        if (this.#foreground.get(callId) !== response) return;
        this.#foreground.delete(callId);
        this.#foregroundChanged.next();
        signal.removeEventListener('abort', aborted);
        settle();
      };
      const aborted = () => finish(() => reject(signal.reason));
      const response: ForegroundResponse = { signal, timed: request.wait?.timed === true,
        question: ownHumanFact<HumanQuestion>({ callId, questions: request.questions, state: 'open', origin }),
        answer: answer => finish(() => resolve(answer)) };
      this.#foreground.set(callId, response);
      signal.addEventListener('abort', aborted, { once: true });
      this.#foregroundChanged.next();
    });
  }

  private async admit(callId: ToolCallId, answer: HumanAnswer | null, options: HumanResponderOptions, expected?: HumanQuestionReference) {
    const actorId = options.actorId;
    this.assertRoot();
    this.program.flush();
    const policy = this.program.humanInput;
    if (!policy) throw new Error('REACTIVE_HUMAN_INPUT_NOT_DECLARED');
    if (typeof actorId !== 'string' || !actorId.trim()) throw new Error('REACTIVE_HUMAN_RESPONDER_REQUIRED');
    const signal = AbortSignal.any([this.scope.signal, this.program.signal, ...options.signal ? [options.signal] : []]);
    signal.throwIfAborted();
    const address = this.question(callId);
    const { question, response } = address;
    assertHumanQuestionReference(question.origin, expected);
    const responding = response ? AbortSignal.any([signal, response.signal]) : signal;
    if (answer) assertHumanAnswerBatch(question, answer);
    await admitHumanResponder(this.program, this.scope, policy, { channel: address.channel,
      question: structuredClone(question), answer: answer ? structuredClone(answer) : null, actorId, signal: responding });
    return { ...address, policy, signal: responding };
  }

  /** 在最后一个 await 之后同步复核；准入返回和原生受理之间不留权限窗口。 */
  private recheck({ channel, response, question, policy, signal }: ResponseAdmission): QuestionAddress {
    signal.throwIfAborted();
    this.assertRoot();
    this.program.flush();
    if (this.program.humanInput !== policy) throw new Error('REACTIVE_HUMAN_INPUT_CHANGED');
    const current = this.question(question.callId);
    if (current.channel !== channel || current.response !== response || current.question.state !== question.state
      || JSON.stringify(current.question.questions) !== JSON.stringify(question.questions)
      || canonical(current.question.origin) !== canonical(question.origin)
      || canonical(questionOrigin(this.origins(), question.callId)) !== canonical(question.origin))
      throw new Error('REACTIVE_HUMAN_QUESTION_CHANGED');
    return current;
  }

  /** 返回实际受理事实；前台答案随后由原生 tool/result 落盘，晚答由原生 inbox 持有。 */
  async answerQuestion(id: string, raw: unknown, options: HumanResponderOptions, expected?: HumanQuestionReference): Promise<boolean> {
    if (typeof id !== 'string' || !id.trim()) throw new Error('REACTIVE_HUMAN_QUESTION_CALL_REQUIRED');
    const callId = ToolCallId(id);
    const reference = expected === undefined ? undefined : captureHumanQuestionReference(expected);
    assertHumanQuestionTarget(reference, this.agent.id, id);
    const answer = humanAnswerSchema.parse(raw);
    const admitted = await this.admit(callId, answer, options, reference);
    const question = this.recheck(admitted);
    if (question.channel === 'continued') return this.ctx.userQuestions.answer(this.agent, callId, answer);
    const foreground = question.response;
    if (!foreground || foreground.signal.aborted) throw new Error('REACTIVE_HUMAN_FOREGROUND_UNAVAILABLE');
    foreground.answer(answer);
    return true;
  }

  /** 原生 stream 持有倒计时 claim；断开或声明退出只释放 claim，不提交答案或取消问题。 */
  async *attachWait(id: string, options: HumanResponderOptions, expected?: HumanQuestionReference): AsyncIterable<HumanWaitFrame> {
    if (typeof id !== 'string' || !id.trim()) throw new Error('REACTIVE_HUMAN_QUESTION_CALL_REQUIRED');
    const callId = ToolCallId(id);
    const reference = expected === undefined ? undefined : captureHumanQuestionReference(expected);
    assertHumanQuestionTarget(reference, this.agent.id, id);
    const admitted = await this.admit(callId, null, options, reference);
    this.recheck(admitted);
    const { policy, signal } = admitted;
    const controller = new AbortController();
    const claim: Claim = { policy, origin: admitted.question.origin, controller };
    this.#claims.add(claim);
    const observing = AbortSignal.any([signal, controller.signal]);
    const release = () => { this.#claims.delete(claim); controller.abort(); };
    observing.addEventListener('abort', release, { once: true });
    try {
      observing.throwIfAborted();
      yield* this.ctx.userQuestions.attachWait(this.agent, callId, observing);
    } finally { observing.removeEventListener('abort', release); release(); }
  }

  /** 原生 Agent 的观察 owner 结束；组件退出仅改变当前声明，不调用此方法。 */
  dispose() { this.scope.dispose(); }
}
