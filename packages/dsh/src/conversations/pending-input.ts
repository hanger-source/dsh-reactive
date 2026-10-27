import type { Agent, PreStepDecision } from '@deepseek-ai/dsh-agent';
import { createUserMessage, type MessageId, type UserMessage } from '@deepseek-ai/dsh-llm';

declare module '@deepseek-ai/dsh-llm' {
  interface MessageSourceMap {
    'reactive-resume-signal': { kind: 'reactive-resume-signal'; requestId: string; messageId: MessageId; nextTurnId: MessageId | null };
  }
}
export const isRecoveryControl = (message: UserMessage) => message.source.kind === 'reactive-resume-signal'
  || message.source.kind === 'reactive-child-resume' || message.source.kind === 'subagent-settled';

/** 只恢复已取得真实回执、被 claim 却未进入模型历史的同一原生消息。 */
export function restoreChildInput(agent: Agent, messageId: string): 'pending' | 'consumed' | 'restored' | 'cancelled' {
  const events = agent.session.snapshotEvents().slice(agent.session.inheritedEventCount);
  if (events.some(event => event.type === 'user/message' && event.data.id === messageId)) return 'consumed';
  if ([...agent.inbox.nextTurn, ...agent.inbox.nextStep].some(message => message.id === messageId)) return 'pending';
  const queues: Record<'next-turn' | 'next-step', UserMessage[]> = { 'next-turn': [], 'next-step': [] };
  let original: UserMessage | undefined;
  let removed: 'claimed' | 'cancelled' | undefined;
  for (const event of events) {
    if (event.type !== 'agent/inbox/spliced') continue;
    const { target, start, removedCount = 0, inserted, outcome } = event.data;
    const taken = queues[target].splice(start, removedCount, ...inserted);
    for (const message of inserted) if (message.id === messageId) {
      if (original && JSON.stringify(original) !== JSON.stringify(message)) throw new Error('REACTIVE_CHILD_INITIAL_MESSAGE_CONFLICT');
      original = message; removed = undefined;
    }
    if (taken.some(message => message.id === messageId)) removed = outcome === 'canceled' ? 'cancelled' : 'claimed';
  }
  if (!original || !removed) throw new Error('REACTIVE_CHILD_INITIAL_MESSAGE_NOT_FOUND');
  if (removed === 'cancelled') return 'cancelled';
  agent.inbox.prepend('next-turn', original);
  console.info('reactive.child.claim-restored', { sessionId: agent.id, messageId });
  return 'restored';
}

/** Follow one new native admission through its first quiescent boundary. A stale
 * Goal may reject that turn while leaving the newly admitted user input queued. */
export async function followAdmittedInput(agent: Agent, requestId: string, canWake: () => boolean) {
  await agent.whenIdle();
  if (agent.ctx.agents.get(agent.id) !== agent) return;
  if (canWake()) await resumePendingInput(agent, requestId, canWake);
  if (agent.ctx.agents.get(agent.id) === agent && !await agent.ctx.sessions.flush(agent.session)) {
    throw new Error('REACTIVE_PERSISTENCE_UNAVAILABLE');
  }
}

/** Wake the native driver without resubmitting, removing or changing an admitted request. */
export async function resumePendingInput(agent: Agent, requestId: string, canWake: () => boolean) {
  if (agent.status !== 'idle' || agent.ctx.agents.get(agent.id) !== agent) return;
  if (!canWake()) return;
  const pending = [...agent.inbox.nextTurn, ...agent.inbox.nextStep];
  const original = pending.find(message => message.source.kind === 'user' && 'rpcId' in message.source && message.source.rpcId === requestId);
  if (!original) return; // Already consumed or canceled is not permission to replay history.
  const pendingGoal = pending.some(message => message.source.kind === 'goal' && message.source.round > 0);
  const boundary = [...agent.session.snapshotEvents()].reverse().find(event => event.type === 'turn/end');
  if (boundary?.type === 'turn/end' && boundary.data.reason.kind === 'aborted' && boundary.data.reason.reason.kind === 'user') {
    const queued = [...agent.session.snapshotEvents()].reverse().find(event => event.type === 'agent/inbox/spliced'
      && event.data.inserted.some(message => message.id === original.id));
    if (!queued || queued.seq < boundary.seq) return;
  }
  // next-step does not compete with or reorder the original next-turn queue.
  agent.send(createUserMessage({ source: { kind: 'reactive-resume-signal', requestId, messageId: original.id,
    nextTurnId: agent.inbox.nextTurn[0]?.id ?? null }, content: [] }), 'next-step', true);
  console.info('reactive.native.pending-wake-requested', { sessionId: agent.id, requestId, messageId: original.id });
  if (!pendingGoal) return;
  // A restored Goal round has lost its in-memory reservation. Let the native
  // Goal driver reject that round and preserve other claimed input itself.
  await agent.whenIdle();
  if (agent.ctx.agents.get(agent.id) !== agent) return;
  if (!canWake()) return;
  const remaining = [...agent.inbox.nextTurn, ...agent.inbox.nextStep];
  if (remaining.some(message => message.source.kind === 'goal' && message.source.round > 0)) {
    throw new Error('REACTIVE_PENDING_GOAL_REQUIRES_RESUME');
  }
  if (remaining.some(message => message.id === original.id)) {
    console.info('reactive.native.pending-goal-reconciled', { sessionId: agent.id, requestId, messageId: original.id });
    await resumePendingInput(agent, requestId, canWake);
  }
}

/** 保留原生尚未消费的业务消息；原生 Goal round 与恢复控制仍由各自 owner 结算。 */
export function retainClaimedInputs(agent: Agent, messages: readonly UserMessage[]) {
  const queued = new Set([...agent.inbox.nextStep, ...agent.inbox.nextTurn].map(message => message.id));
  for (const message of [...messages].reverse()) {
    if (isRecoveryControl(message) || message.source.kind === 'goal') continue;
    if (queued.has(message.id)) continue;
    agent.inbox.prepend('next-step', message);
    queued.add(message.id);
  }
}

/** Recovery is transport control, never model input, a new request or execution authority. */
export async function admitPendingRecovery(
  agent: Agent, claimed: UserMessage[], signal: AbortSignal,
  next: (recovered: boolean) => Promise<PreStepDecision>,
): Promise<PreStepDecision> {
  const signals = claimed.filter(isRecoveryControl);
  if (!signals.length) return next(false);
  const originals = claimed.filter(message => !isRecoveryControl(message));
  // Calling the default next() here would add runtime-context and run an otherwise empty model request.
  if (!originals.length) return { kind: 'reject' };
  const restore = () => {
    if (signal.aborted) return; // Native cancel/disposal owns the aborted turn; never revive it.
    const queued = new Set([...agent.inbox.nextStep, ...agent.inbox.nextTurn].map(message => message.id));
    const turnIds = new Set(signals.flatMap(marker => marker.source.kind === 'reactive-resume-signal' && marker.source.nextTurnId ? [marker.source.nextTurnId] : []));
    for (const message of [...originals].reverse()) {
      // Goal round validity and reservation belong to the native driver. Never
      // put its rejected input back, even when it was claimed with our signal.
      if (message.source.kind === 'goal' && message.source.round > 0) continue;
      if (!queued.has(message.id)) agent.inbox.prepend(turnIds.has(message.id) ? 'next-turn' : 'next-step', message);
    }
  };
  try {
    const decision = await next(true);
    if (decision.kind === 'reject') { restore(); return decision; }
    return { ...decision, messages: decision.messages.filter(message => !isRecoveryControl(message)) };
  } catch (error) { restore(); throw error; }
}
