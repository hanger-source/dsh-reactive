import type { AssistantStreamFrame } from '@deepseek-ai/dsh-agent';
import type { InputReference } from '@hanger-source/dsh-reactive/kernel';

type StepInputs = { turn: number; step: number; inputs: readonly InputReference[] };

/** 原生步骤与 attempt 的输入身份视图；不持有消息、前缀或持久历史。 */
export class NativeProgress {
  #step?: StepInputs;
  #active?: StepInputs & { attemptId: string };

  /** 下一次装配尚未进入原生 step，不能把它的错误归给上一 step。 */
  beginPreparation() { this.#step = undefined; }

  admitted(turn: number, step: number, inputs: readonly InputReference[]) {
    this.#step = { turn, step, inputs: Object.freeze([...inputs]) };
  }

  inputs(turn: number | null, step: number | null) {
    return this.#step?.turn === turn && this.#step.step === step ? this.#step.inputs : undefined;
  }

  accept(frame: AssistantStreamFrame, inputs: readonly InputReference[]): readonly InputReference[] {
    if (frame.type === 'start') {
      if (this.#active) throw new Error('REACTIVE_ASSISTANT_ATTEMPT_ALREADY_ACTIVE');
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

  endedTurn() { this.#step = undefined; this.#active = undefined; }
}
