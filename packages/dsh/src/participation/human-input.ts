import type { Context } from '@deepseek-ai/cordis';
import type { Agent } from '@deepseek-ai/dsh-agent';
import { ToolCallId } from '@deepseek-ai/dsh-llm';
import type { ProjectionSnapshot } from '@deepseek-ai/dsh-session-projection';
import { UserQuestionError, type AskUserQuestionRequest } from '@deepseek-ai/dsh-user-questions';
import { Observable, Subject } from 'rxjs';
import { humanAnswerSchema, type HumanAnswer, type HumanInputState, type HumanQuestion, type HumanQuestionOrigin, type HumanForegroundQuestion, type HumanResponderOptions, type HumanWaitFrame } from '@hanger-source/dsh-reactive/kernel';
import type { HumanInputDeclaration } from '@hanger-source/dsh-reactive/kernel';
import { Scope, type Value } from '@hanger-source/dsh-reactive/kernel';
import type { ReactiveProjectionState } from '../conversations/projections.ts';
import { canonical } from '../conversations/records.ts';

interface HumanInputProgram {
  readonly humanInput: HumanInputDeclaration | undefined;
  readonly revision: Value<number>;
  readonly signal: AbortSignal;
  flush(): void;
}
type ForegroundResponse = { readonly signal: AbortSignal; readonly question: HumanQuestion;
  readonly timed: boolean; answer(answer: HumanAnswer): void };
type Claim = { readonly policy: HumanInputDeclaration; readonly controller: AbortController };
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

function questionOrigin(source: Pick<ReactiveProjectionState, 'sessionId' | 'calls'>, callId: string): HumanQuestionOrigin {
  const call = Object.hasOwn(source.calls, callId) ? source.calls[callId] : undefined;
  if (!call || call.callId !== callId) throw new Error('REACTIVE_HUMAN_QUESTION_CALL_NOT_FOUND');
  return ownHumanFact({ sessionId: source.sessionId, ...call });
}

/** live 与 cold 以同一原生投影切面关联真实调用；不从当前输入推测旧问题来源。 */
export function humanInputSnapshot(snapshot: ProjectionSnapshot, source: Pick<ReactiveProjectionState, 'sessionId' | 'calls'>): HumanInputState {
  const questions = snapshot.values.userQuestions;
  if (!questions) throw new Error('REACTIVE_HUMAN_QUESTIONS_PROJECTION_REQUIRED');
  return ownHumanFact({
    active: questions.active.map(question => ({ ...question, origin: questionOrigin(source, question.callId) })),
    settled: questions.settled.map(question => ({ ...question, origin: questionOrigin(source, question.callId) })),
  });
}

/** 一个真实 Agent 的前台 answerer 接点；持久问题及晚答输入始终归原生服务所有。 */
export class HumanInputs {
  readonly questions$: Observable<HumanInputState>;
  readonly foreground$: Observable<readonly HumanForegroundQuestion[]>;
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
    this.questions$ = new Observable(subscriber => {
      if (this.scope.disposed) { subscriber.complete(); return; }
      const release = this.scope.own(() => subscriber.complete());
      const emit = () => {
        try { subscriber.next(this.snapshot()); }
        catch (error) { subscriber.error(error); }
      };
      const stop = ctx.sessionProjections.onChanged((session, key) => {
        if (session === agent.session && key === 'userQuestions') emit();
      });
      emit();
      return () => { stop(); release(); };
    });
    this.foreground$ = new Observable(subscriber => {
      if (this.scope.disposed) { subscriber.complete(); return; }
      const emit = () => subscriber.next(ownHumanFact([...this.#foreground.entries()].map(([callId, response]) => ({
        callId, questions: response.question.questions, timed: response.timed, origin: response.question.origin,
      }))));
      const subscription = this.#foregroundChanged.subscribe({ next: emit, complete: () => subscriber.complete() });
      emit();
      return () => subscription.unsubscribe();
    });
  }

  snapshot(): HumanInputState {
    this.scope.assertActive();
    return humanInputSnapshot(this.ctx.sessionProjections.snapshot(this.agent.session, ['userQuestions']), this.origins());
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
    const response = this.#foreground.get(callId);
    if (response && !response.signal.aborted) return { channel: 'foreground', question: response.question, response };
    const question = this.snapshot().active.find(question => question.callId === callId);
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

  private async admit(callId: ToolCallId, answer: HumanAnswer | null, options: HumanResponderOptions) {
    this.assertRoot();
    this.program.flush();
    const policy = this.program.humanInput;
    if (!policy) throw new Error('REACTIVE_HUMAN_INPUT_NOT_DECLARED');
    if (typeof options.actorId !== 'string' || !options.actorId.trim()) throw new Error('REACTIVE_HUMAN_RESPONDER_REQUIRED');
    const signal = AbortSignal.any([this.scope.signal, this.program.signal, ...options.signal ? [options.signal] : []]);
    signal.throwIfAborted();
    const address = this.question(callId);
    const { question } = address;
    if (answer) {
      const ids = new Set(answer.answers.map(item => item.id));
      if (ids.size !== answer.answers.length || question.questions.length !== answer.answers.length
        || question.questions.some(item => !ids.has(item.id))) {
        throw new UserQuestionError('the answer batch must name each question exactly once', 'BAD_ANSWER');
      }
    }
    await policy.admitResponder({ channel: address.channel, question: structuredClone(question), answer: answer ? structuredClone(answer) : null,
      actorId: options.actorId, signal });
    return { ...address, policy, signal };
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
  async answerQuestion(id: string, raw: unknown, options: HumanResponderOptions): Promise<boolean> {
    if (typeof id !== 'string' || !id.trim()) throw new Error('REACTIVE_HUMAN_QUESTION_CALL_REQUIRED');
    const callId = ToolCallId(id);
    const answer = humanAnswerSchema.parse(raw);
    const admitted = await this.admit(callId, answer, options);
    const question = this.recheck(admitted);
    if (question.channel === 'continued') return this.ctx.userQuestions.answer(this.agent, callId, answer);
    const foreground = question.response;
    if (!foreground || foreground.signal.aborted) throw new Error('REACTIVE_HUMAN_FOREGROUND_UNAVAILABLE');
    foreground.answer(answer);
    return true;
  }

  /** 原生 stream 持有倒计时 claim；断开或声明退出只释放 claim，不提交答案或取消问题。 */
  async *attachWait(id: string, options: HumanResponderOptions): AsyncIterable<HumanWaitFrame> {
    if (typeof id !== 'string' || !id.trim()) throw new Error('REACTIVE_HUMAN_QUESTION_CALL_REQUIRED');
    const callId = ToolCallId(id);
    const admitted = await this.admit(callId, null, options);
    this.recheck(admitted);
    const { policy, signal } = admitted;
    const controller = new AbortController();
    const claim: Claim = { policy, controller };
    this.#claims.add(claim);
    try {
      yield* this.ctx.userQuestions.attachWait(this.agent, callId, AbortSignal.any([signal, controller.signal]));
    } finally { this.#claims.delete(claim); controller.abort(); }
  }

  /** 原生 Agent 的观察 owner 结束；组件退出仅改变当前声明，不调用此方法。 */
  dispose() { this.scope.dispose(); }
}
