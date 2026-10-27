import type { Context } from '@deepseek-ai/cordis';
import type { Agent } from '@deepseek-ai/dsh-agent';
import { appendPluginRecord, type UserMessage } from '@deepseek-ai/dsh-session';
import { admittedSubmissionSchema, submissionSchema, type AdmittedSubmission, type Submission } from '@hanger-source/dsh-reactive/kernel';
import { InputEditRejectedError } from '@hanger-source/dsh-reactive/kernel';
import type { Admission } from '@hanger-source/dsh-reactive';
import { admitInputContent } from '../files/input-content.ts';
import { canonical, contentDigest, inputEditRecordSchema, recordSchema, type NativeState } from './records.ts';

/** 宿主提供同一受理策略、持久边界及最终工作区核验，不另持有输入或 Agent 生命周期。 */
export interface QueuedInputEdit {
  readonly ctx: Context;
  readonly agent: Agent;
  readonly requestId: string;
  readonly content: Submission['content'];
  readonly actorId: string;
  readonly participant: Admission['participant'];
  readonly signal: AbortSignal;
  readonly state: () => NativeState;
  readonly assertCurrent: () => void;
  readonly admit: (input: AdmittedSubmission) => Promise<void>;
  readonly persist: () => Promise<void>;
}

/** 替换仍在原生队列中的同一请求正文；不改变身份、目标、顺序或唤醒状态。 */
export async function editQueuedInput(edit: QueuedInputEdit): Promise<{
  readonly sessionId: string; readonly requestId: string; readonly accepted: true;
}> {
  const { ctx, agent, signal, requestId } = edit;
  const assertCurrent = () => {
    signal.throwIfAborted();
    edit.assertCurrent();
    if (ctx.agents.get(agent.id) !== agent) throw new Error('REACTIVE_CONVERSATION_NOT_RUNNING');
  };
  const locate = () => {
    const matches: { target: 'next-turn' | 'next-step'; message: UserMessage }[] = [];
    for (const target of ['next-turn', 'next-step'] as const) for (const message of agent.inbox[target === 'next-turn' ? 'nextTurn' : 'nextStep']) {
      if (message.source.kind === 'user' && 'rpcId' in message.source && message.source.rpcId === requestId)
        matches.push({ target, message });
    }
    if (matches.length !== 1) throw new InputEditRejectedError(requestId, 'unavailable');
    return matches[0]!;
  };
  assertCurrent();
  const state = edit.state(), previous = state.record(requestId), initial = locate();
  if (!previous || previous.intent === 'result' || state.activity.requests[requestId]?.state !== 'accepted')
    throw new InputEditRejectedError(requestId, 'unavailable');
  if (!edit.actorId.trim() || previous.actorId !== edit.actorId || previous.participant !== edit.participant)
    throw new InputEditRejectedError(requestId, 'actor-mismatch');
  if (previous.digest !== contentDigest(initial.message.content)) throw new InputEditRejectedError(requestId, 'content-conflict');
  const check = () => {
    assertCurrent();
    const current = edit.state(), located = locate();
    if (current.activity.requests[requestId]?.state !== 'accepted') throw new InputEditRejectedError(requestId, 'unavailable');
    if (canonical(current.record(requestId)) !== canonical(previous) || located.target !== initial.target
      || canonical(located.message) !== canonical(initial.message)) throw new InputEditRejectedError(requestId, 'changed');
  };
  const { actorId: _actor, participant: _participant, digest: _digest, sourceDigest: _sourceDigest, ...submission } = previous;
  const normalized = submissionSchema.parse({ ...submission, content: edit.content });
  const admitted = await admitInputContent(ctx, agent, normalized.content);
  check();
  const next = recordSchema.parse({ ...previous, content: admitted.content, digest: contentDigest(admitted.content) });
  const nextSubmission = admittedSubmissionSchema.parse({ ...submission, content: admitted.content });
  await edit.admit(nextSubmission);
  check();
  const prepared = inputEditRecordSchema.parse({ messageId: initial.message.id, previousDigest: previous.digest, record: next });
  if (next.digest !== previous.digest) {
    appendPluginRecord(agent.session, 'plugin:reactive/input-edit', prepared);
    await edit.persist();
    check();
    if (canonical(edit.state().value.pendingEdits[requestId]) !== canonical(prepared)) throw new InputEditRejectedError(requestId, 'changed');
  }
  const binding = admitted.receiptIds.length ? ctx.fileUploads.bindPrompt(agent, admitted.receiptIds, requestId) : undefined;
  try {
    if (next.digest !== previous.digest && !agent.inbox.replace(initial.message.id, {
      ...initial.message, content: structuredClone([...next.content]),
    })) throw new InputEditRejectedError(requestId, 'unavailable');
    binding?.commit();
  } finally { binding?.[Symbol.dispose](); }
  // ACK 与初次提交一致，只确认持久受理；取消或观察失败不撤销已发生的替换。
  await edit.persist();
  assertCurrent();
  return { sessionId: agent.id as string, requestId, accepted: true };
}
