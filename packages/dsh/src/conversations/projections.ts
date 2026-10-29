import { z } from 'zod';
import type { Context } from '@deepseek-ai/cordis';
import { pluginRecordOf, type SessionEvent } from '@deepseek-ai/dsh-session';
import type { ProjectionDefinition } from '@deepseek-ai/dsh-session-projection';
import { inputReferenceSchema, requestActivitySchema, toolActivitySchema, replyActivitySchema, executionFailureSchema } from '@hanger-source/dsh-reactive/kernel';
import { canonical, childInputRecordSchema, goalBindingSchema, inputEditRecordSchema, inputRecordIdentity, operationNotStartedSchema, operationFollowUnregisteredSchema, questionOriginRecordSchema, recordSchema, referenceOf, contentDigest, unique, waitRecordSchema, callForResult, ptcParent } from './records.ts';

const inputsSchema = inputReferenceSchema.array().readonly();
const pendingOccurrenceSchema = z.object({ messageId: z.string(), inputs: inputsSchema, questionCallId: z.string().nullable() }).strict();
const proposedOccurrenceSchema = pendingOccurrenceSchema.extend({ target: z.enum(['next-turn', 'next-step']), turn: z.number().int().nonnegative().nullable() });
const requestLifecycleSchema = requestActivitySchema.pick({ requestId: true, state: true, outcome: true });
const stepSchema = z.object({ turn: z.number().int().nonnegative(), step: z.number().int().nonnegative() }).strict();
const callOccurrenceSchema = toolActivitySchema.extend({ turn: z.number().int().nonnegative(), step: z.number().int().nonnegative() });
export const reactiveProjectionSchema = z.object({ sessionId: z.string(), delegated: z.boolean(), inheritedEventCount: z.number().int().nonnegative(),
  records: z.record(z.string(), recordSchema), pendingEdits: z.record(z.string(), inputEditRecordSchema), goals: z.record(z.string(), inputsSchema),
  nativeInputs: z.record(z.string(), inputReferenceSchema),
  questionOrigins: z.record(z.string(), questionOriginRecordSchema.shape.goal),
  inputs: inputsSchema, calls: z.record(z.string(), callOccurrenceSchema), replies: replyActivitySchema.array().readonly(), current: inputsSchema,
  openTurn: z.number().int().nonnegative().nullable(), openStep: stepSchema.nullable(),
  pending: z.object({ 'next-turn': pendingOccurrenceSchema.array(), 'next-step': pendingOccurrenceSchema.array() }).strict(),
  proposed: z.record(z.string(), proposedOccurrenceSchema), requests: z.record(z.string(), requestLifecycleSchema),
  failures: executionFailureSchema.array().readonly(),
  waits: waitRecordSchema.array().readonly(),
}).strict();
export type ReactiveProjectionState = z.infer<typeof reactiveProjectionSchema>;
declare module '@deepseek-ai/dsh-session-projection/types' {
  interface SessionProjectionStateMap { reactiveInputs: ReactiveProjectionState; }
}

/** 仅负责业务请求、原生输入和调用的关联；驱动、水位、冷恢复由原生注册表拥有。 */
export const reactiveProjectionDefinition: ProjectionDefinition<'reactiveInputs'> = {
  key: 'reactiveInputs', stateVersion: 14, stateSchema: reactiveProjectionSchema,
  init: (header, inheritedEventCount) => ({ sessionId: header.id, delegated: header.parentSession !== undefined, inheritedEventCount,
    records: {}, pendingEdits: {}, goals: {}, nativeInputs: {}, questionOrigins: {}, inputs: [], calls: {}, replies: [], current: [], openTurn: null, openStep: null,
    pending: { 'next-turn': [], 'next-step': [] },
    proposed: {}, requests: {}, waits: [], failures: [] }),
  apply: applyReactiveProjection,
};

function own<T>(values: Readonly<Record<string, T>>, key: string): T | undefined {
  return Object.hasOwn(values, key) ? values[key] : undefined;
}

function applyReactiveProjection(previous: ReactiveProjectionState, event: SessionEvent): ReactiveProjectionState {
  if (event.seq < previous.inheritedEventCount) return previous;
  const record = pluginRecordOf(event);
  if (!['turn/start', 'turn/end', 'step/start', 'step/end', 'agent/inbox/spliced', 'user/message', 'tool/call',
    'tool/ptc-dispatch-start', 'tool/result', 'tool/ptc-dispatch', 'assistant/message'].includes(event.type)
    && record?.type !== 'plugin:reactive/input' && record?.type !== 'plugin:reactive/goal-inputs' && record?.type !== 'plugin:reactive/wait'
    && record?.type !== 'plugin:reactive/child-input' && record?.type !== 'plugin:reactive/operation-not-started'
    && record?.type !== 'plugin:reactive/operation-follow-unregistered'
    && record?.type !== 'plugin:reactive/input-edit' && record?.type !== 'plugin:reactive/question-origin') return previous;
  let state = { ...previous };
  type RequestLifecycle = z.output<typeof requestLifecycleSchema>;
  const update = (id: string, change: Pick<RequestLifecycle, 'state'> & Partial<RequestLifecycle>) => {
    const before = own(state.requests, id);
    state.requests = { ...state.requests, [id]: before ? { ...before, ...change }
      : { requestId: id, outcome: null, ...change } };
  };
  const localRequest = (input: z.output<typeof inputReferenceSchema>): input is z.output<typeof inputReferenceSchema> & { requestId: string } =>
    input.sourceSessionId === state.sessionId && input.requestId !== null;
  const inOpenStep = (turn: number, step: number) => state.openTurn === turn
    && state.openStep?.turn === turn && state.openStep.step === step;
  if (record?.type === 'plugin:reactive/question-origin') {
    const origin = questionOriginRecordSchema.parse(record.data), key = String(origin.callSequence);
    const call = own(state.calls, key);
    if (!call || call.name !== 'ask_user_question' || call.state !== 'running' || !inOpenStep(call.turn, call.step))
      throw new Error('REACTIVE_HUMAN_QUESTION_ORIGIN_CALL_REQUIRED');
    if (Object.hasOwn(state.questionOrigins, key) && canonical(state.questionOrigins[key]) !== canonical(origin.goal))
      throw new Error('REACTIVE_HUMAN_QUESTION_ORIGIN_CONFLICT');
    state.questionOrigins = { ...state.questionOrigins, [key]: origin.goal };
  }
  if (record?.type === 'plugin:reactive/child-input') {
    const bound = childInputRecordSchema.parse(record.data);
    if (bound.input.sourceSessionId !== state.sessionId || bound.input.messageId !== bound.messageId) throw new Error('REACTIVE_CHILD_INPUT_SESSION_MISMATCH');
    const before = own(state.nativeInputs, bound.messageId);
    if (before && canonical(before) !== canonical(bound.input)) throw new Error('REACTIVE_CHILD_INPUT_CONFLICT');
    state.nativeInputs = { ...state.nativeInputs, [bound.messageId]: bound.input };
    state.inputs = unique([...state.inputs, bound.input]);
    state.pending = Object.fromEntries(Object.entries(state.pending).map(([target, values]) => [target,
      values.map(item => item.messageId === bound.messageId ? { ...item, inputs: [bound.input] } : item)])) as ReactiveProjectionState['pending'];
    const proposed = own(state.proposed, bound.messageId);
    if (proposed) state.proposed = { ...state.proposed, [bound.messageId]: { ...proposed, inputs: [bound.input] } };
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
      ? { state: 'settled', outcome: 'not-ready' } : { state: 'declared' });
  }
  if (record?.type === 'plugin:reactive/input-edit') {
    const edit = inputEditRecordSchema.parse(record.data), id = edit.record.requestId;
    const before = own(state.records, id);
    if (!before || before.intent === 'result' || inputRecordIdentity(before) !== inputRecordIdentity(edit.record)
      || before.digest !== edit.previousDigest) throw new Error('REACTIVE_INPUT_EDIT_CONFLICT');
    const pending = [...state.pending['next-turn'], ...state.pending['next-step']]
      .find(item => item.messageId === edit.messageId && item.inputs.some(input => localRequest(input) && input.requestId === id));
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
  if (record?.type === 'plugin:reactive/operation-follow-unregistered') {
    const closed = operationFollowUnregisteredSchema.parse(record.data);
    state.waits = state.waits.filter(item => canonical(item.wait) !== canonical(closed.wait)
      || canonical(item.continuation) !== canonical(closed.continuation));
  }
  if (event.type === 'turn/start') {
    state.current = []; state.proposed = {}; state.openTurn = event.data.turn; state.openStep = null;
  }
  if (event.type === 'step/start') state.openStep = state.openTurn === event.data.turn ? event.data : null;
  if (event.type === 'step/end' && inOpenStep(event.data.turn, event.data.step)) state.openStep = null;
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
    const inserted = event.data.inserted.map(message => ({ messageId: message.id, inputs: referenceOf(state, message),
      questionCallId: message.source.kind === 'user-question-reply' ? message.source.callId : null }));
    state.inputs = unique([...state.inputs, ...inserted.flatMap(item => item.inputs)]);
    state.pending = { ...state.pending, [target]: state.pending[target].toSpliced(start, removedCount, ...inserted) };
    state.proposed = { ...state.proposed };
    state.pendingEdits = { ...state.pendingEdits };
    for (const item of removed) {
      for (const input of item.inputs) if (localRequest(input)) delete state.pendingEdits[input.requestId];
      if (inserted.some(next => next.messageId === item.messageId)) continue;
      if (outcome !== 'canceled') {
        if (item.questionCallId === null || state.openTurn !== null)
          state.proposed[item.messageId] = { ...item, target, turn: state.openTurn };
      } else {
        delete state.proposed[item.messageId];
        for (const input of item.inputs) if (input.messageId === item.messageId && localRequest(input)) {
          update(input.requestId, { state: 'settled', outcome: 'cancelled' });
        }
      }
    }
    // Goal 的拒绝可能放回同行输入；它仍属于原生队列，不由本 turn 结算。
    for (const item of inserted) {
      delete state.proposed[item.messageId];
      for (const input of item.inputs) if (localRequest(input) && !['consumed', 'settled'].includes(own(state.requests, input.requestId)?.state ?? '')) {
        update(input.requestId, { state: 'accepted' });
      }
    }
  }
  if (event.type === 'user/message') {
    const inputs = referenceOf(state, event.data);
    state.inputs = unique([...state.inputs, ...inputs]);
    state.current = unique([...state.current, ...inputs]);
    state.proposed = { ...state.proposed };
    delete state.proposed[event.data.id];
    for (const input of inputs) if (localRequest(input)) update(input.requestId, { state: 'consumed', outcome: null });
  }
  const started = (call: Pick<z.output<typeof callOccurrenceSchema>, 'callId' | 'rootCallId' | 'parentCallId' | 'name' | 'inputs' | 'turn' | 'step'>) => {
    state.calls = { ...state.calls, [String(event.seq)]: { ...call, callSequence: event.seq, state: 'running', observedAt: event.time } };
  };
  if (event.type === 'tool/call') started({ callId: event.data.callId, rootCallId: event.data.callId,
    parentCallId: null, name: event.data.name, turn: event.data.turn, step: event.data.step,
    inputs: inOpenStep(event.data.turn, event.data.step) ? state.current : [] });
  if (event.type === 'tool/ptc-dispatch-start') {
    const parent = ptcParent(state, event.data);
    started({ callId: event.data.subCallId, rootCallId: event.data.rootCallId, parentCallId: event.data.parentCallId,
      name: event.data.name, turn: parent.turn, step: parent.step, inputs: parent.inputs });
  }
  if (event.type === 'tool/result' || event.type === 'tool/ptc-dispatch') {
    const failed = event.type === 'tool/result' ? event.data.message.isError : event.data.isError;
    const meta = z.object({ reactive: z.json() }).safeParse(event.data.meta);
    const call = callForResult(state, event);
    if (call) state.calls = { ...state.calls, [String(call.callSequence)]: { ...call, state: failed ? 'failed' : 'returned', observedAt: event.time,
      ...(meta.success ? { result: meta.data.reactive } : {}) } };
  }
  if (event.type === 'assistant/message') {
    const text = event.data.message.content.flatMap(block => block.type === 'text' ? [block.text] : []).join('\n');
    const previousReply = state.replies.find(reply => reply.messageId === event.data.message.id);
    const inputs = event.surfaceOp === 'append'
      ? inOpenStep(event.data.turn, event.data.step) ? state.current : null
      : previousReply ? previousReply.inputs : null;
    const reply = { messageId: event.data.message.id, turn: event.data.turn, step: event.data.step,
      sequence: event.seq, observedAt: event.time, text, interrupted: event.data.interrupted === true, inputs };
    state.replies = previousReply ? state.replies.map(before => before.messageId === reply.messageId ? reply : before) : [...state.replies, reply];
  }
  if (event.type === 'turn/end') {
    if (event.data.reason.kind === 'error') {
      const unentered = Object.values(state.proposed).filter(item => item.turn === event.data.turn).map(item => ({
        messageId: item.messageId, questionCallId: item.questionCallId, target: item.target, inputs: item.inputs,
      }));
      state.failures = [...state.failures, { sequence: event.seq, observedAt: event.time, turn: event.data.turn,
        error: event.data.reason.error, inputs: unique([...state.current, ...unentered.flatMap(item => item.inputs)]), unentered }];
    }
    // 未进入 user/message 的晚答只引用原问题来源，不能替原请求结算本 turn。
    const proposedInputs = Object.values(state.proposed).flatMap(item => item.questionCallId === null ? item.inputs : []);
    for (const input of unique([...state.current, ...proposedInputs])) if (localRequest(input)) {
      update(input.requestId, { state: 'settled', outcome: event.data.reason.kind });
    }
    state.proposed = Object.fromEntries(Object.entries(state.proposed).filter(([, item]) => item.questionCallId !== null && item.turn !== event.data.turn));
    state.openTurn = null; state.openStep = null;
  }
  return state;
}

/** 注册一次关联 unit；声明本身与具体应用、操作者、观察者无关。 */
export function installReactiveProjection(ctx: Context) {
  return ctx.sessionProjections.register(reactiveProjectionDefinition);
}
