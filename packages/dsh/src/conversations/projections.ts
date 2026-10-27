import { z } from 'zod';
import type { Context } from '@deepseek-ai/cordis';
import { pluginRecordOf, type SessionEvent } from '@deepseek-ai/dsh-session';
import type { ProjectionDefinition } from '@deepseek-ai/dsh-session-projection';
import { inputReferenceSchema, requestActivitySchema } from '@hanger-source/dsh-reactive/kernel';
import { canonical, childInputRecordSchema, goalBindingSchema, inputEditRecordSchema, inputRecordIdentity, operationNotStartedSchema, recordSchema, referenceOf, contentDigest, unique, waitRecordSchema } from './records.ts';

const inputsSchema = inputReferenceSchema.array().readonly();
const pendingOccurrenceSchema = z.object({ messageId: z.string(), inputs: inputsSchema }).strict();
const callSchema = z.object({ callId: z.string(), rootCallId: z.string(), parentCallId: z.string().nullable(),
  name: z.string(), inputs: inputsSchema }).strict();
export const reactiveProjectionSchema = z.object({ sessionId: z.string(), delegated: z.boolean(), inheritedEventCount: z.number().int().nonnegative(),
  records: z.record(z.string(), recordSchema), pendingEdits: z.record(z.string(), inputEditRecordSchema), goals: z.record(z.string(), inputsSchema),
  nativeInputs: z.record(z.string(), inputReferenceSchema),
  calls: z.record(z.string(), callSchema), current: inputsSchema,
  pending: z.object({ 'next-turn': pendingOccurrenceSchema.array(), 'next-step': pendingOccurrenceSchema.array() }).strict(),
  proposed: z.record(z.string(), inputsSchema), requests: z.record(z.string(), requestActivitySchema),
  waits: waitRecordSchema.array().readonly(),
}).strict();
export type ReactiveProjectionState = z.infer<typeof reactiveProjectionSchema>;
declare module '@deepseek-ai/dsh-session-projection/types' {
  interface SessionProjectionStateMap { reactiveInputs: ReactiveProjectionState; }
}

/** 仅负责业务请求、原生输入和调用的关联；驱动、水位、冷恢复由原生注册表拥有。 */
export const reactiveProjectionDefinition: ProjectionDefinition<'reactiveInputs'> = {
  key: 'reactiveInputs', stateVersion: 7, stateSchema: reactiveProjectionSchema,
  init: (header, inheritedEventCount) => ({ sessionId: header.id, delegated: header.parentSession !== undefined, inheritedEventCount,
    records: {}, pendingEdits: {}, goals: {}, nativeInputs: {}, calls: {}, current: [], pending: { 'next-turn': [], 'next-step': [] },
    proposed: {}, requests: {}, waits: [] }),
  apply: applyReactiveProjection,
};

function own<T>(values: Readonly<Record<string, T>>, key: string): T | undefined {
  return Object.hasOwn(values, key) ? values[key] : undefined;
}

function applyReactiveProjection(previous: ReactiveProjectionState, event: SessionEvent): ReactiveProjectionState {
  if (event.seq < previous.inheritedEventCount) return previous;
  const record = pluginRecordOf(event);
  if (!['turn/start', 'turn/end', 'agent/inbox/spliced', 'user/message', 'tool/call',
    'tool/ptc-dispatch-start', 'tool/result', 'tool/ptc-dispatch', 'assistant/message'].includes(event.type)
    && record?.type !== 'plugin:reactive/input' && record?.type !== 'plugin:reactive/goal-inputs' && record?.type !== 'plugin:reactive/wait'
    && record?.type !== 'plugin:reactive/child-input' && record?.type !== 'plugin:reactive/operation-not-started'
    && record?.type !== 'plugin:reactive/input-edit') return previous;
  if (event.type === 'assistant/message' && event.surfaceOp !== 'append') return previous;
  let state = { ...previous };
  const update = (id: string, change: Partial<z.output<typeof requestActivitySchema>>) => {
    const before = own(state.requests, id);
    state.requests = { ...state.requests, [id]: before ? { ...before, ...change }
      : { requestId: id, state: 'declared', reply: null, outcome: null, tools: [], ...change } };
  };
  if (record?.type === 'plugin:reactive/child-input') {
    const bound = childInputRecordSchema.parse(record.data);
    if (bound.input.sourceSessionId !== state.sessionId || bound.input.messageId !== bound.messageId) throw new Error('REACTIVE_CHILD_INPUT_SESSION_MISMATCH');
    const before = own(state.nativeInputs, bound.messageId);
    if (before && canonical(before) !== canonical(bound.input)) throw new Error('REACTIVE_CHILD_INPUT_CONFLICT');
    state.nativeInputs = { ...state.nativeInputs, [bound.messageId]: bound.input };
    state.pending = Object.fromEntries(Object.entries(state.pending).map(([target, values]) => [target,
      values.map(item => item.messageId === bound.messageId ? { ...item, inputs: [bound.input] } : item)])) as ReactiveProjectionState['pending'];
    if (own(state.proposed, bound.messageId)) state.proposed = { ...state.proposed, [bound.messageId]: [bound.input] };
  }
  if (record?.type === 'plugin:reactive/input') {
    const input = recordSchema.parse(record.data);
    const before = own(state.records, input.requestId);
    if (before && canonical(before) !== canonical(input)) throw new Error('REACTIVE_REQUEST_ID_CONFLICT');
    state.records = { ...state.records, [input.requestId]: input };
    // 完成事实独立于模型消费；只解除经过受理核验的同一操作和原续接条件。
    const result = input.result;
    if (result?.ready) state.waits = state.waits.filter(item =>
      canonical(item.wait) !== canonical(result.wait)
      || canonical(item.continuation) !== canonical(result.continuation));
    if (!own(state.requests, input.requestId)) update(input.requestId, input.result?.ready === false
      ? { state: 'settled', outcome: 'not-ready' } : {});
  }
  if (record?.type === 'plugin:reactive/input-edit') {
    const edit = inputEditRecordSchema.parse(record.data), id = edit.record.requestId;
    const before = own(state.records, id);
    if (!before || before.intent === 'result' || inputRecordIdentity(before) !== inputRecordIdentity(edit.record)
      || before.digest !== edit.previousDigest) throw new Error('REACTIVE_INPUT_EDIT_CONFLICT');
    const pending = [...state.pending['next-turn'], ...state.pending['next-step']]
      .find(item => item.messageId === edit.messageId && item.inputs.some(input => input.requestId === id));
    if (!pending || own(state.requests, id)?.state !== 'accepted') throw new Error('REACTIVE_INPUT_EDIT_UNAVAILABLE');
    state.pendingEdits = { ...state.pendingEdits, [id]: edit };
  }
  if (record?.type === 'plugin:reactive/goal-inputs') {
    const binding = goalBindingSchema.parse(record.data);
    state.goals = { ...state.goals, [binding.goalId]: binding.inputs };
  }
  if (record?.type === 'plugin:reactive/wait') {
    const wait = waitRecordSchema.parse(record.data);
    state.waits = [...state.waits.filter(item => item.wait.id !== wait.wait.id), wait];
  }
  if (record?.type === 'plugin:reactive/operation-not-started') {
    const closed = operationNotStartedSchema.parse(record.data);
    state.waits = state.waits.filter(item => canonical(item.wait) !== canonical(closed.wait)
      || canonical(item.continuation) !== canonical(closed.continuation));
  }
  if (event.type === 'turn/start') { state.current = []; state.proposed = {}; }
  if (event.type === 'agent/inbox/spliced') {
    const { target, start, removedCount = 0, outcome } = event.data;
    const removed = state.pending[target].slice(start, start + removedCount);
    // 只有同一个原生 replace 才提交编辑。准备记录本身不授权重发、移位或消费新正文。
    for (const message of event.data.inserted) {
      const id = message.source.kind === 'user' && 'rpcId' in message.source && typeof message.source.rpcId === 'string'
        ? message.source.rpcId : undefined;
      const edit = id ? own(state.pendingEdits, id) : undefined;
      const before = id ? own(state.records, id) : undefined;
      if (!id || !edit || !before || before.digest === contentDigest(message.content)) continue;
      if (removedCount !== 1 || event.data.inserted.length !== 1 || removed[0]?.messageId !== message.id
        || edit.messageId !== message.id || edit.previousDigest !== before.digest
        || edit.record.digest !== contentDigest(message.content) || own(state.requests, id)?.state !== 'accepted')
        throw new Error('REACTIVE_INPUT_EDIT_CONFLICT');
      state.records = { ...state.records, [id]: edit.record };
    }
    const inserted = event.data.inserted.map(message => ({ messageId: message.id, inputs: referenceOf(state, message) }));
    state.pending = { ...state.pending, [target]: state.pending[target].toSpliced(start, removedCount, ...inserted) };
    state.proposed = { ...state.proposed };
    state.pendingEdits = { ...state.pendingEdits };
    for (const item of removed) {
      for (const input of item.inputs) if (input.requestId) delete state.pendingEdits[input.requestId];
      if (inserted.some(next => next.messageId === item.messageId)) continue;
      if (outcome !== 'canceled') state.proposed = { ...state.proposed, [item.messageId]: item.inputs };
      else for (const input of item.inputs) if (input.messageId === item.messageId && input.requestId) {
        update(input.requestId, { state: 'settled', outcome: 'cancelled' });
      }
    }
    // Goal 的拒绝可能放回同行输入；它仍属于原生队列，不由本 turn 结算。
    for (const item of inserted) {
      delete state.proposed[item.messageId];
      for (const input of item.inputs) if (input.requestId && !['consumed', 'settled'].includes(own(state.requests, input.requestId)?.state ?? '')) {
        update(input.requestId, { state: 'accepted' });
      }
    }
  }
  if (event.type === 'user/message') {
    const inputs = referenceOf(state, event.data);
    state.current = unique([...state.current, ...inputs]);
    for (const input of inputs) if (input.requestId) update(input.requestId, { state: 'consumed', outcome: null });
  }
  const started = (call: z.output<typeof callSchema>) => {
    state.calls = { ...state.calls, [call.callId]: call };
    const tool = { callId: call.callId, rootCallId: call.rootCallId, parentCallId: call.parentCallId, name: call.name,
      state: 'running' as const, inputs: call.inputs, observedAt: event.time };
    for (const input of call.inputs) if (input.requestId) update(input.requestId, { tools: [...(own(state.requests, input.requestId)?.tools ?? []), tool] });
  };
  if (event.type === 'tool/call') started({ callId: event.data.callId, rootCallId: event.data.callId,
    parentCallId: null, name: event.data.name, inputs: state.current });
  if (event.type === 'tool/ptc-dispatch-start') started({ callId: event.data.subCallId, rootCallId: event.data.rootCallId,
    parentCallId: event.data.parentCallId, name: event.data.name, inputs: own(state.calls, event.data.rootCallId)?.inputs ?? [] });
  if (event.type === 'tool/result' || event.type === 'tool/ptc-dispatch') {
    const callId = event.type === 'tool/result' ? event.data.message.source.callId : event.data.subCallId;
    const failed = event.type === 'tool/result' ? event.data.message.isError : event.data.isError;
    const meta = z.object({ reactive: z.json() }).safeParse(event.data.meta);
    for (const input of own(state.calls, callId)?.inputs ?? []) if (input.requestId) update(input.requestId, {
      tools: (own(state.requests, input.requestId)?.tools ?? []).map(tool => tool.callId === callId
        ? { ...tool, state: failed ? 'failed' : 'returned', observedAt: event.time, ...(meta.success ? { result: meta.data.reactive } : {}) } : tool),
    });
  }
  if (event.type === 'assistant/message') {
    const text = event.data.message.content.flatMap(block => block.type === 'text' ? [block.text] : []).join('\n');
    if (text.trim()) for (const input of state.current) if (input.requestId) update(input.requestId, { reply: text });
  }
  if (event.type === 'turn/end') {
    for (const input of unique([...state.current, ...Object.values(state.proposed).flat()])) if (input.requestId) {
      update(input.requestId, { state: 'settled', outcome: event.data.reason.kind });
    }
    state.proposed = {};
  }
  return state;
}

/** 注册一次关联 unit；声明本身与具体应用、操作者、观察者无关。 */
export function installReactiveProjection(ctx: Context) {
  return ctx.sessionProjections.register(reactiveProjectionDefinition);
}
