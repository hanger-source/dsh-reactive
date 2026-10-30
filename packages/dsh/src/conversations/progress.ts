import type { AssistantStreamFrame } from '@deepseek-ai/dsh-agent';
import type { GoalRef } from '@deepseek-ai/dsh-goal';
import type { InputReference } from '@hanger-source/dsh-reactive/kernel';

type StepInputs = {
  turn: number;
  step: number;
  inputs: readonly InputReference[];
  handoff?: { readonly close: () => void; messageIds: readonly string[] };
};
type StepPreparation = {
  readonly signal: AbortSignal;
  readonly inputs: readonly InputReference[];
  readonly goal: GoalRef | null | undefined;
  readonly current: () => boolean;
  readonly ready: () => boolean;
  readonly release: () => void;
  readonly close: () => void;
};

/** 原生准备、已准入步骤与 attempt 的因果关联；不持有 Goal 状态、消息正文、前缀或持久历史。 */
export class NativeProgress {
  #step?: StepInputs;
  #active?: StepInputs & { attemptId: string };
  #prepared?: StepPreparation;
  #goals = new Map<number, Map<number, GoalRef | null>>();
  #continuation?: { turn: number; step: number; goal: GoalRef | null };

  /** 下一次装配尚未进入原生 step，不能把它的错误归给上一 step。 */
  beginPreparation() {
    this.#step?.handoff?.close();
    this.#step = undefined;
    if (this.#prepared) this.discardPreparation(this.#prepared);
  }

  prepared(preparation: StepPreparation) {
    if (this.#prepared) this.discardPreparation(this.#prepared);
    this.#prepared = Object.freeze({ ...preparation, inputs: Object.freeze([...preparation.inputs]) });
  }

  discardPreparation(preparation: StepPreparation) {
    if (this.#prepared !== preparation) return;
    this.#prepared = undefined;
    try { preparation.release(); } finally { preparation.close(); }
  }

  preparedFor(signal: AbortSignal): StepPreparation {
    const prepared = this.#prepared;
    if (!prepared || prepared.signal !== signal) throw new Error('REACTIVE_STEP_PREPARATION_REQUIRED');
    return prepared;
  }

  admitted(turn: number, step: number, inputs: readonly InputReference[], preparation: StepPreparation, messageIds: readonly string[]) {
    if (this.#prepared !== preparation) throw new Error('REACTIVE_STEP_PREPARATION_CHANGED');
    if (!preparation.current()) throw new Error('REACTIVE_STEP_GOAL_CHANGED');
    if (preparation.goal === undefined) throw new Error('REACTIVE_STEP_CONTINUATION_REQUIRED');
    let goals = this.#goals.get(turn);
    if (!goals) { goals = new Map(); this.#goals.set(turn, goals); }
    if (goals.has(step)) throw new Error('REACTIVE_STEP_ALREADY_ADMITTED');
    goals.set(step, preparation.goal);
    this.#continuation = { turn, step, goal: preparation.goal };
    this.#prepared = undefined;
    this.#step?.handoff?.close();
    this.#step = { turn, step, inputs: Object.freeze([...inputs]),
      handoff: { close: preparation.close, messageIds: Object.freeze([...messageIds]) } };
    if (!messageIds.length) this.consume();
  }

  /** pre-step 后原生仍在 prepareRequest；真实 user/message 落下才结束本次准备现场。 */
  consume(messageId?: string) {
    const handoff = this.#step?.handoff;
    if (!handoff) return;
    if (messageId !== undefined) handoff.messageIds = handoff.messageIds.filter(id => id !== messageId);
    if (handoff.messageIds.length) return;
    this.#step!.handoff = undefined;
    handoff.close();
  }

  /** null 是本步明确无 Goal 权限；undefined 表示从未准入，不能据此签发工具控制。 */
  goal(turn: number, step: number): GoalRef | null | undefined { return this.#goals.get(turn)?.get(step); }

  /** 没有新输入的自然步骤沿原关联继续；空输入不是重新取得当前 Goal 的权限。 */
  continuedGoal(): GoalRef | null | undefined { return this.#continuation?.goal; }

  /** 已准入步骤中的原生显式 Goal 控制只改变后续步骤，保留原模型请求的固定身份。 */
  controlledGoal(turn: number, step: number, goal: GoalRef | null) {
    if (this.goal(turn, step) === undefined || this.#continuation?.turn !== turn || this.#continuation.step !== step)
      throw new Error('REACTIVE_GOAL_CONTROL_STEP_NOT_ADMITTED');
    this.#continuation = { turn, step, goal };
  }

  inputs(turn: number | null, step: number | null) {
    return this.#step?.turn === turn && this.#step.step === step ? this.#step.inputs : undefined;
  }

  accept(frame: AssistantStreamFrame, inputs: readonly InputReference[]): readonly InputReference[] {
    if (frame.type === 'start') {
      if (this.#active) throw new Error('REACTIVE_ASSISTANT_ATTEMPT_ALREADY_ACTIVE');
      this.#step?.handoff?.close();
      const fixed = Object.freeze([...inputs]);
      this.#step = { turn: frame.turn, step: frame.step, inputs: fixed };
      this.#active = { ...this.#step, attemptId: frame.attemptId };
    } else {
      const active = this.#active;
      if (!active || active.attemptId !== frame.attemptId) throw new Error('REACTIVE_ASSISTANT_ATTEMPT_NOT_ACTIVE');
      if (frame.type === 'end') this.#active = undefined;
      return active.inputs;
    }
    return this.#active.inputs;
  }

  endedTurn() {
    this.#step?.handoff?.close();
    this.#step = undefined; this.#active = undefined;
    if (this.#prepared) this.discardPreparation(this.#prepared);
    this.#goals.clear(); this.#continuation = undefined;
  }
}
