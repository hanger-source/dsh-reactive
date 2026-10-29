import { z } from 'zod';
import type { AssistantStreamFrame } from '@deepseek-ai/dsh-agent';
import { pluginRecordOf, type SessionEvent } from '@deepseek-ai/dsh-session';
import type { AssistantStream, ConversationEvent } from '@hanger-source/dsh-reactive/kernel';
import { childInputRecordSchema, recordSchema, waitRecordSchema, type NativeState } from './records.ts';

/** 只转换新的 append。历史读取只消费原生投影，不调用此入口。 */
export function sessionOccurrences(sessionId: string, event: SessionEvent, previous: NativeState, current: NativeState): ConversationEvent[] {
  const origin = { sessionId, time: event.time, sequence: event.seq };
  const events: ConversationEvent[] = [];
  const record = pluginRecordOf(event);
  if (record?.type === 'plugin:reactive/child-input') {
    const association = childInputRecordSchema.parse(record.data);
    if (!Object.hasOwn(previous.value.nativeInputs, association.messageId)) events.push({ ...origin,
      kind: 'input.accepted', messageId: association.messageId, inputs: [association.input] });
  }
  if (record?.type === 'plugin:reactive/input') {
    const input = recordSchema.parse(record.data);
    if (input.result?.ready === false && !previous.record(input.requestId)) {
      events.push({ ...origin, kind: 'input.progress', requestId: input.requestId, inputs: input.result.continuation.inputs });
    }
  }
  if (event.type === 'agent/inbox/spliced') for (const message of event.data.inserted) {
    // Goal 与恢复控制负责原生推进，不是一次新的外部输入受理。
    if (!['user', 'agent-message', 'subagent-settled'].includes(message.source.kind)) continue;
    const inputs = current.reference(message);
    const original = inputs.find(input => input.sourceSessionId === sessionId && input.messageId === message.id);
    const before = original?.requestId && Object.hasOwn(previous.activity.requests, original.requestId)
      ? previous.activity.requests[original.requestId] : undefined;
    const restored = Object.hasOwn(previous.value.proposed, message.id);
    if (inputs.length && !restored && (!before || before.state === 'declared')) {
      events.push({ ...origin, kind: 'input.accepted', messageId: message.id, inputs });
    }
  }
  if (event.type === 'user/message' && ['user', 'agent-message', 'subagent-settled'].includes(event.data.source.kind)) {
    const inputs = current.reference(event.data);
    if (inputs.length) events.push({ ...origin, kind: 'input.entered', messageId: event.data.id, inputs });
  }
  if (event.type === 'tool/call' || event.type === 'tool/ptc-dispatch-start') {
    const call = current.callAtSequence(event.seq);
    if (call) events.push({ ...origin, kind: 'tool.started', ...call });
  }
  if (event.type === 'tool/result' || event.type === 'tool/ptc-dispatch') {
    const call = current.resultCall(event);
    const failed = event.type === 'tool/result' ? event.data.message.isError : event.data.isError;
    const meta = z.object({ reactive: z.json() }).safeParse(event.data.meta);
    if (call) events.push({ ...origin, kind: 'tool.settled', ...call, outcome: failed ? 'failed' : 'returned',
      ...(event.data.meta === undefined ? {} : { meta: z.json().parse(event.data.meta) }),
      ...(meta.success ? { result: meta.data.reactive } : {}) });
  }
  if (event.type === 'assistant/message' && event.surfaceOp === 'append') {
    const reply = current.value.replies.find(reply => reply.messageId === event.data.message.id);
    if (!reply) throw new Error('REACTIVE_ASSISTANT_REPLY_NOT_FOUND');
    const inputs = reply.inputs;
    events.push({ ...origin, kind: 'assistant.message', messageId: event.data.message.id,
      // 公开事件与冷恢复读取同一原生 step 关联；未知关联仍是会话事实。
      turn: event.data.turn, step: event.data.step, ...(inputs === null ? {} : { inputs }),
      text: event.data.message.content.flatMap(block => block.type === 'text' ? [block.text] : []).join('\n'),
      interrupted: event.data.interrupted === true });
  }
  if (event.type === 'goal/change') events.push({ ...origin, kind: 'goal.changed', goal: current.activity.goal });
  if (record?.type === 'plugin:reactive/wait') {
    const waiting = waitRecordSchema.parse(record.data);
    if (!previous.activity.waits.some(item => item.wait.id === waiting.wait.id)) events.push({ ...origin, kind: 'wait.opened', ...waiting });
  }
  for (const waiting of previous.activity.waits) if (!current.activity.waits.some(item => item.wait.id === waiting.wait.id)) {
    events.push({ ...origin, kind: 'wait.resolved', ...waiting });
  }
  return events;
}

/** 投影可组合的流式片段，不暴露原生 replay/ContentBlock 内部合同。 */
export function assistantFrame(frame: AssistantStreamFrame): AssistantStream | undefined {
  const common = { attemptId: frame.attemptId as string, revision: frame.revision };
  if (frame.type === 'start') return { ...common, phase: 'start', turn: frame.turn, step: frame.step };
  if (frame.type === 'end') return { ...common, phase: 'end', index: frame.index, outcome: frame.outcome };
  const chunk = frame.chunk;
  if (chunk.type === 'text-delta' || chunk.type === 'reasoning-delta') return { ...common,
    phase: chunk.type === 'text-delta' ? 'text' : 'reasoning', index: frame.index, blockIndex: chunk.index, text: chunk.text };
  if (chunk.type === 'tool-call-delta') return { ...common, phase: 'tool', index: frame.index, blockIndex: chunk.index,
    callId: chunk.id, ...(chunk.name === undefined ? {} : { name: chunk.name }), argumentsDelta: chunk.argumentsDelta };
  return undefined;
}
