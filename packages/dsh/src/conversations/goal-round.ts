import { getTraceable, type Context, type Fiber } from '@deepseek-ai/cordis';
import { GoalId, type GoalRef } from '@deepseek-ai/dsh-goal';
import * as goalDriver from '@deepseek-ai/dsh-goal-round-driver';
import type { UserMessage } from '@deepseek-ai/dsh-llm';
import type { InputReference } from '@hanger-source/dsh-reactive';
import { assertNativeContextActive } from '../native-installation.ts';
import { nativeServiceMatches } from '../native-capability.ts';
import { canonical } from './records.ts';
import type { LiveConversation } from './session.ts';

export class GoalRoundDriverUnavailableError extends Error {
  constructor(readonly reason: 'unavailable' | 'ambiguous' | 'changed') {
    super(`REACTIVE_GOAL_ROUND_DRIVER_${reason.toUpperCase()}`);
  }
}

/** 只承认实际宿主服务下唯一、仍然活跃的官方 Driver 安装。 */
export function requireGoalRoundDriver(ctx: Context, expected?: Fiber): Fiber {
  assertNativeContextActive(ctx);
  const fibers = [...ctx.registry.get(goalDriver)?.fibers ?? []].filter(fiber => {
    if (fiber.state !== 2) return false;
    try { assertNativeContextActive(fiber.ctx); } catch { return false; }
    return goalDriver.inject.every(service => nativeServiceMatches(
      getTraceable(fiber.ctx, fiber.store?.[service]?.value), ctx.get(service)));
  });
  if (expected && (fibers.length !== 1 || fibers[0] !== expected)) throw new GoalRoundDriverUnavailableError('changed');
  if (!fibers.length) throw new GoalRoundDriverUnavailableError('unavailable');
  if (fibers.length !== 1) throw new GoalRoundDriverUnavailableError('ambiguous');
  return fibers[0]!;
}

export interface GoalRoundCandidate {
  readonly driver: Fiber;
  readonly messageId: string;
  readonly goal: GoalRef;
  readonly round: number;
  readonly inputs: readonly InputReference[];
}

/** 候选只保存准备现场；真正的 reservation/claimed/content 准入仍由官方 pre-step 执行。 */
export function prepareGoalRound(ctx: Context, entry: LiveConversation): GoalRoundCandidate | undefined {
  const messages = entry.claimed.filter(message => message.source.kind === 'goal' && message.source.round > 0);
  if (!messages.length) return undefined;
  if (messages.length !== 1) throw new Error('REACTIVE_GOAL_ROUND_INVALID');
  const message = messages[0]!;
  if (message.source.kind !== 'goal') throw new Error('REACTIVE_GOAL_ROUND_INVALID');
  const source = message.source;
  if (!Number.isSafeInteger(source.round) || typeof source.goalId !== 'string' || !source.goalId
    || !Number.isSafeInteger(source.revision) || source.revision <= 0 || typeof message.id !== 'string' || !message.id) {
    throw new Error('REACTIVE_GOAL_ROUND_INVALID');
  }
  return Object.freeze({ driver: requireGoalRoundDriver(ctx), messageId: message.id,
    goal: Object.freeze({ id: GoalId(source.goalId), revision: source.revision }), round: source.round,
    inputs: Object.freeze(structuredClone([...entry.session.state.reference(message)])) });
}

/** next() 的 enter 已经过官方双检；此处只核验返回的仍是原候选与权威输入。 */
export function admitGoalRound(ctx: Context, entry: LiveConversation, candidate: GoalRoundCandidate,
  messages: readonly UserMessage[]): GoalRef | undefined {
  requireGoalRoundDriver(ctx, candidate.driver);
  const submitted = messages.filter(message => message.source.kind === 'goal' && message.source.round > 0);
  const message = submitted[0];
  const goal = ctx.goals.get(entry.agent);
  if (submitted.length !== 1 || !message || message.id !== candidate.messageId || message.source.kind !== 'goal'
    || message.source.goalId !== candidate.goal.id || message.source.revision !== candidate.goal.revision
    || message.source.round !== candidate.round
    || canonical(entry.session.state.reference(message)) !== canonical(candidate.inputs)) {
    throw new Error('REACTIVE_GOAL_ROUND_CHANGED');
  }
  if (!goal || goal.phase !== 'active' || goal.activation !== 'armed' || goal.id !== candidate.goal.id
    || goal.revision !== candidate.goal.revision || candidate.round !== goal.roundsStarted + 1) return undefined;
  return candidate.goal;
}
