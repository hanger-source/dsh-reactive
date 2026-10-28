import { randomUUID } from 'node:crypto';
import { abortable, actionIdentityKey } from '@hanger-source/dsh-reactive';
import type { Context } from '@deepseek-ai/cordis';
import type { Agent } from '@deepseek-ai/dsh-agent';
import { SessionId, appendPluginRecord, pluginRecordOf, type Session, type UserMessage } from '@deepseek-ai/dsh-session';
import { SessionPersistenceNotFoundError, type SessionInspection } from '@deepseek-ai/dsh-session-persistence';
import { foldGoal } from '@deepseek-ai/dsh-goal';
import { steerHostSubagentPrompt } from '@deepseek-ai/dsh-subagent/internal';
import type {} from '@deepseek-ai/dsh-subagent';
import { firstValueFrom, Observable, Subject } from 'rxjs';
import { z } from 'zod';
import { implementAction, actionOperationOwner, actionInvocationArguments, captureActionArguments, type ActionContract, type ActionImplementation, type ActionParsedInput, type ActionScope, type ActionCallContext, type ActionExecutionContext, type ActionReference, type ActionSource } from '@hanger-source/dsh-reactive/kernel';
import { implementOperation, operationFollowLinkSchema, type OperationOwner, type OperationAccessContext, type OperationFeedbackNode } from '@hanger-source/dsh-reactive/kernel';
import { NativeChildWork, nativeChildCommandSchema, nativeChildReceiptSchema, type NativeChildCommand, type NativeChildReceipt, type NativeChildReference, type NativeChildState, type NativeChildFeedback } from '@hanger-source/dsh-reactive/kernel';
import { inputReferenceSchema, type InputReference, type Json } from '@hanger-source/dsh-reactive/kernel';
import type { ConversationIdentity } from '@hanger-source/dsh-reactive/kernel';
import type { ProgramContract, ProgramInput } from '@hanger-source/dsh-reactive/kernel';
import type { ProgramImplementation } from '@hanger-source/dsh-reactive/kernel';
import { canonical, goalBindingSchema } from './conversations/records.ts';
import { restoreChildInput } from './conversations/pending-input.ts';
import { sessionProgramBindingSchema, captureProgramBinding, assertSessionProgram } from './conversations/program-binding.ts';

const sourceSchema = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('tool'), callId: z.string(), rootCallId: z.string(), parentCallId: z.string().nullable() }).strict(),
  z.object({ kind: z.literal('participant'), actorId: z.string().min(1), participant: z.enum(['human', 'service']), entrypoint: z.string() }).strict(),
]);
const parentIdentitySchema = z.object({ sessionId: z.string(), workspaceId: z.string(), environmentId: z.string(), directory: z.string(), parentSessionId: z.string().nullable() }).strict();
export const delegationRecordSchema = z.object({ command: nativeChildCommandSchema, childSessionId: z.string(), action: z.string(), actionVersion: z.number().int().positive().safe(),
  mode: z.enum(['spawn', 'fork']), program: sessionProgramBindingSchema, input: z.json(), target: z.json(), childInput: z.json(), objective: z.string().trim().min(1),
  parent: parentIdentitySchema, executor: sourceSchema, causes: inputReferenceSchema.array().readonly() }).strict();
const childBindingSchema = z.object({ command: nativeChildCommandSchema, childSessionId: z.string(), action: z.string(), actionVersion: z.number().int().positive().safe() }).strict();
const childReceiptRecordSchema = z.object({ command: nativeChildCommandSchema, receipt: nativeChildReceiptSchema }).strict();
const failureSchema = z.object({ command: nativeChildCommandSchema, childSessionId: z.string(), reason: z.string() }).strict();
export type DelegationRecord = z.infer<typeof delegationRecordSchema>;

declare module '@deepseek-ai/dsh-session/types' {
  interface PluginRecordMap {
    'plugin:reactive/delegation': DelegationRecord;
    'plugin:reactive/child': z.infer<typeof childBindingSchema>;
    'plugin:reactive/child-receipt': z.infer<typeof childReceiptRecordSchema>;
    'plugin:reactive/child-creation-error': z.infer<typeof failureSchema>;
  }
}
declare module '@deepseek-ai/dsh-llm' {
  interface MessageSourceMap {
    'reactive-child-resume': { kind: 'reactive-child-resume'; childSessionId: string; messageId: string };
  }
}

export type DelegationRequester = ActionSource | { readonly kind: 'parent'; readonly sessionId: string };
export interface DelegationAdmission<D extends ActionContract = ActionContract> {
  readonly phase: 'create' | 'read' | 'follow' | 'resume';
  readonly input: ActionParsedInput<D>;
  readonly target: ActionScope<D>;
  readonly parent: ConversationIdentity;
  readonly executor: ActionSource;
  readonly requester: DelegationRequester;
  readonly causes: readonly InputReference[];
  readonly childInput: Json;
  readonly objective: string;
  readonly signal: AbortSignal;
}
export interface DelegationOptions<D extends ActionContract, P extends ProgramContract> {
  readonly program: ProgramImplementation<P>;
  readonly mode: 'spawn' | 'fork';
  childInput(input: ActionParsedInput<D>, call: ActionCallContext<ActionScope<D>>): ProgramInput<NoInfer<P>>;
  objective(input: ActionParsedInput<D>, call: ActionCallContext<ActionScope<D>>): string;
  authorize(admission: DelegationAdmission<D>): void | PromiseLike<void>;
}
type Descriptor = { readonly action: ActionContract; readonly program: ProgramImplementation;
  readonly mode: 'spawn' | 'fork'; authorize(admission: DelegationAdmission): void | PromiseLike<void>;
  childInput(input: unknown, call: ActionCallContext<unknown>): unknown; objective(input: unknown, call: ActionCallContext<unknown>): string };
type Prepared = { readonly record: DelegationRecord; readonly parent: Agent; readonly input: unknown; readonly target: unknown };
type Gate = { record: DelegationRecord; promise: Promise<void>; resolve(): void; reject(error: unknown): void;
  acceptedMessageId?: string; goal?: NativeChildReceipt['goal']; confirmed?: boolean };

export class ChildCreationIncompleteError extends Error {
  constructor(readonly command: NativeChildCommand, readonly reference: NativeChildReference, cause?: unknown) {
    super('REACTIVE_CHILD_CREATION_INCOMPLETE', { cause });
  }
}

/** @internal 原始日志读取不合成 interrupted 终态，也不取得写 ownership 或启动 Agent。 */
export async function readNativeInspection(ctx: Context, id: string, signal?: AbortSignal): Promise<SessionInspection> {
  const handle = await ctx.sessionPersistence.open(SessionId(id), 'read', { signal });
  try { return { meta: handle.header, inheritedEventCount: handle.inheritedEventCount, events: (await handle.read(0, undefined, { signal })).events }; }
  finally { await handle.close(); }
}
function records<T>(inspection: SessionInspection, type: string, schema: z.ZodType<T>): T[] {
  return inspection.events.slice(inspection.inheritedEventCount).flatMap(event => {
    const record = pluginRecordOf(event);
    return record?.type === type ? [schema.parse(record.data)] : [];
  });
}
function one<T>(values: readonly T[], description: string): T | undefined {
  if (values.length > 1 && values.some(value => canonical(value) !== canonical(values[0]))) throw new Error(description + '_CONFLICT');
  return values[0];
}
function snapshot(session: Session): SessionInspection {
  return { meta: session.header, inheritedEventCount: session.inheritedEventCount, events: session.snapshotEvents() };
}

/** @internal 每个 assembly 独有的接入 owner。原生 manager 拥有执行、容量、取消与资源树。 */
export class NativeDelegations {
  readonly owner: OperationOwner<typeof NativeChildWork>;
  readonly updates$ = new Subject<{ parentSessionId: string; childSessionId: string }>();
  #descriptors = new Map<string, Descriptor>();
  #definitions = new Map<ActionContract, Descriptor>();
  #prepared = new WeakMap<ActionCallContext<unknown>, Prepared>();
  #preparedExecutions = new WeakMap<ActionExecutionContext, Prepared>();
  #preparingCommands = new Map<string, Prepared>();
  #gates = new WeakMap<Agent, Gate>();
  #listeners: (() => void)[] = [];
  constructor(private readonly ctx: Context) {
    this.owner = implementOperation(NativeChildWork, {
      locate: async (command, access) => {
        const record = await this.locateRecord(command, access.signal);
        if (!record) return { status: 'unknown' };
        await this.authorize(record, 'read', access.source, access.signal);
        this.assertAccess(record, access);
        return { status: 'accepted', reference: this.reference(record), state: await this.state(record, access.signal) };
      },
      observe: (reference, access) => new Observable(subscriber => {
        let disposed = false;
        let running = false;
        let changed = false;
        const read = async () => {
          changed = true;
          if (running) return;
          running = true;
          try {
            while (changed && !disposed) {
              changed = false;
              const record = await this.referenceRecord(reference, access.signal);
              await this.authorize(record, 'read', access.source, access.signal);
              this.assertAccess(record, access);
              const value = await this.state(record, access.signal);
              if (!disposed) subscriber.next({ status: 'ready', generation: value.sequence, observedAt: Date.now(), value });
            }
          } catch (error) { if (!disposed) subscriber.error(error); }
          finally { running = false; }
        };
        const subscription = this.updates$.subscribe(event => {
          if (event.parentSessionId === reference.parentSessionId && event.childSessionId === reference.childSessionId) void read();
        });
        void read();
        return () => { disposed = true; subscription.unsubscribe(); };
      }),
      registerFollow: async (command, link, access) => {
        if (command.parentSessionId !== access.sessionId || link.continuation.sessionId !== access.sessionId
          || canonical(command) !== canonical(link.command)) throw new Error('REACTIVE_CHILD_FOLLOW_MISMATCH');
        const inspection = await readNativeInspection(ctx, command.parentSessionId, access.signal);
        const found = inspection.events.some(event => {
          const record = pluginRecordOf(event);
          if (record?.type !== 'plugin:reactive/operation-follow') return false;
          const { publisherId: _publisherId, ...raw } = z.object({ publisherId: z.string() }).passthrough().parse(record.data);
          const parsed = operationFollowLinkSchema.safeParse(raw);
          return parsed.success && canonical(parsed.data) === canonical(link);
        });
        if (!found) throw new Error('REACTIVE_CHILD_FOLLOW_NOT_PERSISTED');
        const persisted = await this.locateRecord(command, access.signal);
        const prepared = this.#preparingCommands.get(canonical(command));
        if (!persisted && (!prepared || this.ctx.agents.get(prepared.parent.id) !== prepared.parent
          || canonical(prepared.record.executor) !== canonical(access.source))) throw new Error('REACTIVE_CHILD_COMMAND_NOT_ISSUED');
        const record = persisted ?? prepared!.record;
        this.assertAccess(record, access);
        await this.authorize(record, 'follow', access.source, access.signal, prepared ? { input: prepared.input, target: prepared.target } : undefined);
      },
      readFeedback: async (command, link, access) => {
        const record = await this.locateRecord(command, access.signal);
        if (!record) return { status: 'unknown' };
        this.assertAccess(record, access);
        await this.authorize(record, 'follow', access.source, access.signal);
        const nodes = await this.feedback(record, access.signal);
        const after = link.after === null ? -1 : nodes.findIndex(node => node.id === link.after);
        if (link.after !== null && after < 0) throw new Error('REACTIVE_CHILD_FEEDBACK_BOUNDARY_NOT_FOUND');
        const node = nodes[after + 1];
        return node ? { status: 'ready', reference: this.reference(record), node }
          : { status: 'pending', reference: this.reference(record) };
      },
    });
  }
  implementation<D extends ActionContract, P extends ProgramContract>(action: D, options: DelegationOptions<D, P>): ActionImplementation<D> {
    if (action.operation?.definition !== NativeChildWork || action.output !== nativeChildReceiptSchema) throw new Error('REACTIVE_CHILD_ACTION_CONTRACT_REQUIRED');
    const key = actionIdentityKey(action);
    if (this.#descriptors.has(key)) throw new Error('REACTIVE_DELEGATION_ACTION_DUPLICATE');
    const descriptor = { action, ...options } as unknown as Descriptor;
    this.#descriptors.set(key, descriptor); this.#definitions.set(action, descriptor);
    const implementation = implementAction(action as never, { operationOwner: this.owner, execute: async (_input: unknown, call: ActionCallContext<unknown>) => {
      const prepared = this.#prepared.get(call);
      if (!prepared) throw new Error('REACTIVE_DELEGATION_EXECUTION_UNTRUSTED');
      this.#prepared.delete(call);
      return await this.start(prepared, call.signal);
    } } as never) as ActionImplementation<D>;
    captureActionArguments(implementation, { input: raw => z.json().parse(raw), scope: raw => z.json().parse(raw) });
    return implementation;
  }
  install() {
    const notify = (session: Session) => {
      const binding = this.childBinding(snapshot(session));
      if (!binding) return;
      void this.ctx.sessions.flush(session).then(flushed => {
        if (!flushed) throw new Error('REACTIVE_PERSISTENCE_UNAVAILABLE');
        this.updates$.next({ parentSessionId: binding.command.parentSessionId, childSessionId: binding.childSessionId });
      }).catch(error => console.error('reactive.child.observation.failed', { childSessionId: session.id, error }));
    };
    this.#listeners.push(this.ctx.on('session/event', (session, event) => {
      if (event.type === 'goal/change' || event.type === 'turn/end' || event.type === 'assistant/message') notify(session);
    }));
    this.#listeners.push(this.ctx.on('subagent/end', event => {
      const session = this.ctx.sessions.get(event.id);
      if (session) notify(session);
      else void readNativeInspection(this.ctx, event.id).then(inspection => {
        const binding = this.childBinding(inspection);
        if (binding) this.updates$.next({ parentSessionId: binding.command.parentSessionId, childSessionId: binding.childSessionId });
      }).catch(error => console.error('reactive.child.terminal-observation.failed', { childSessionId: event.id, error }));
    }));
  }
  dispose() { for (const release of this.#listeners.splice(0)) release(); this.updates$.complete(); }
  owns(action: ActionReference) { return this.#definitions.has(action.definition) && actionOperationOwner(action) === this.owner; }
  programs() { return [...this.#descriptors.values()].map(descriptor => descriptor.program); }
  private descriptor(record: DelegationRecord): Descriptor {
    const descriptor = this.#descriptors.get(actionIdentityKey({ name: record.action, version: record.actionVersion }));
    if (!descriptor || descriptor.mode !== record.mode) throw new Error('REACTIVE_DELEGATION_IMPLEMENTATION_REQUIRED');
    assertSessionProgram(record.childSessionId, record.program, descriptor.program.definition);
    descriptor.program.definition.input.parse(record.childInput);
    return descriptor;
  }
  private async authorize(record: DelegationRecord, phase: DelegationAdmission['phase'], requester: DelegationRequester, signal: AbortSignal, parsed?: { input: unknown; target: unknown }) {
    signal.throwIfAborted();
    const descriptor = this.descriptor(record);
    await firstValueFrom(abortable(admissionSignal => descriptor.authorize({ phase, input: parsed ? parsed.input : descriptor.action.input.parse(record.input), target: parsed ? parsed.target as never : descriptor.action.scope.parse(record.target),
      parent: record.parent, executor: record.executor, requester, causes: record.causes,
      childInput: record.childInput, objective: record.objective, signal: admissionSignal }), signal));
    signal.throwIfAborted();
  }
  private assertAccess(record: DelegationRecord, access: OperationAccessContext) {
    if (access.sessionId !== record.command.parentSessionId) throw new Error('REACTIVE_CHILD_PARENT_ACCESS_REQUIRED');
  }
  async admit(action: ActionReference, input: unknown, call: ActionCallContext<unknown>, execution: ActionExecutionContext,
    parent: Agent, identity: ConversationIdentity, signal: AbortSignal) {
    signal.throwIfAborted();
    const descriptor = this.#definitions.get(action.definition);
    if (!descriptor || actionOperationOwner(action) !== this.owner) throw new Error('REACTIVE_DELEGATION_IMPLEMENTATION_REQUIRED');
    if (this.ctx.agents.get(parent.id) !== parent || parent.id !== identity.sessionId || execution.sessionId !== parent.id) throw new Error('REACTIVE_DELEGATION_PARENT_NOT_LIVE');
    const childInput = z.json().parse(descriptor.childInput(input, call));
    descriptor.program.definition.input.parse(childInput);
    const original = actionInvocationArguments(call);
    const record = delegationRecordSchema.parse({ command: { parentSessionId: parent.id, invocationId: execution.invocationId },
      childSessionId: randomUUID(), action: descriptor.action.name, actionVersion: descriptor.action.version, mode: descriptor.mode,
      program: captureProgramBinding(descriptor.program.definition), input: original.input, target: original.scope, childInput,
      objective: descriptor.objective(input, call), parent: identity, executor: execution.source, causes: execution.inputs });
    await this.authorize(record, 'create', execution.source, signal, { input, target: call.scope });
    signal.throwIfAborted();
    if (this.ctx.agents.get(parent.id) !== parent) throw new Error('REACTIVE_DELEGATION_PARENT_NOT_LIVE');
    const prepared = { record, parent, input, target: call.scope };
    this.#prepared.set(call, prepared);
    this.#preparedExecutions.set(execution, prepared);
  }
  /** 首次 Follow 的未持久命令事实只在本次可靠 prepare 边界内可见。 */
  async preparing<T>(action: ActionReference, execution: ActionExecutionContext, command: Json, prepare: () => Promise<T>): Promise<T> {
    if (!this.owns(action)) return prepare();
    const prepared = this.#preparedExecutions.get(execution);
    if (!prepared || canonical(prepared.record.command) !== canonical(command)
      || this.ctx.agents.get(prepared.parent.id) !== prepared.parent) throw new Error('REACTIVE_DELEGATION_EXECUTION_UNTRUSTED');
    const key = canonical(prepared.record.command);
    if (this.#preparingCommands.has(key)) throw new Error('REACTIVE_CHILD_PREPARE_CONCURRENT');
    this.#preparingCommands.set(key, prepared);
    try { return await prepare(); }
    finally {
      this.#preparedExecutions.delete(execution);
      if (this.#preparingCommands.get(key) === prepared) this.#preparingCommands.delete(key);
    }
  }
  async locateRecord(command: NativeChildCommand, signal?: AbortSignal): Promise<DelegationRecord | undefined> {
    const parent = await readNativeInspection(this.ctx, command.parentSessionId, signal);
    return one(records(parent, 'plugin:reactive/delegation', delegationRecordSchema)
      .filter(record => canonical(record.command) === canonical(command)), 'REACTIVE_DELEGATION_COMMAND');
  }
  private reference(record: DelegationRecord): NativeChildReference { return { parentSessionId: record.command.parentSessionId, childSessionId: record.childSessionId }; }
  private async referenceRecord(reference: NativeChildReference, signal?: AbortSignal) {
    const parent = await readNativeInspection(this.ctx, reference.parentSessionId, signal);
    const record = one(records(parent, 'plugin:reactive/delegation', delegationRecordSchema)
      .filter(record => record.childSessionId === reference.childSessionId), 'REACTIVE_CHILD_REFERENCE');
    if (!record) throw new Error('REACTIVE_CHILD_NOT_FOUND');
    return record;
  }
  childBinding(inspection: SessionInspection) { return one(records(inspection, 'plugin:reactive/child', childBindingSchema), 'REACTIVE_CHILD_BINDING'); }
  childReceipt(inspection: SessionInspection) {
    const receipt = one(records(inspection, 'plugin:reactive/child-receipt', childReceiptRecordSchema)
      .filter(item => item.receipt.childSessionId === inspection.meta.id), 'REACTIVE_CHILD_RECEIPT');
    if (receipt) {
      const binding = this.childBinding(inspection);
      if (!binding || receipt.receipt.parentSessionId !== inspection.meta.parentSession
        || receipt.command.parentSessionId !== inspection.meta.parentSession || canonical(receipt.command) !== canonical(binding.command))
        throw new Error('REACTIVE_CHILD_RECEIPT_LINEAGE_MISMATCH');
    }
    return receipt;
  }
  async describeChild(inspection: SessionInspection) {
    const binding = this.childBinding(inspection);
    if (!binding || binding.childSessionId !== inspection.meta.id || binding.command.parentSessionId !== inspection.meta.parentSession
      || inspection.meta.origin !== 'subagent') throw new Error('REACTIVE_CHILD_BINDING_REQUIRED');
    const record = await this.locateRecord(binding.command);
    if (!record || canonical(binding) !== canonical({ command: record.command, childSessionId: record.childSessionId, action: record.action, actionVersion: record.actionVersion }))
      throw new Error('REACTIVE_CHILD_BINDING_MISMATCH');
    return { record, implementation: this.descriptor(record).program };
  }
  async created(agent: Agent, validate: (record: DelegationRecord, parent: Agent) => Promise<void>, signal: AbortSignal) {
    const parentId = agent.session.header.parentSession;
    if (!parentId) return undefined;
    const parent = this.ctx.agents.get(parentId);
    if (!parent || !this.ctx.agents.isOwnedBy(agent.id, parent)) throw new Error('REACTIVE_CHILD_RUNTIME_OWNER_REQUIRED');
    const parentInspection = await readNativeInspection(this.ctx, parentId);
    const record = one(records(parentInspection, 'plugin:reactive/delegation', delegationRecordSchema)
      .filter(item => item.childSessionId === agent.id), 'REACTIVE_CHILD_RESERVED_ID');
    if (!record) throw new Error('REACTIVE_CHILD_RESERVATION_REQUIRED');
    await validate(record, parent);
    const descriptor = this.descriptor(record);
    const inspection = snapshot(agent.session);
    const existing = this.childBinding(inspection);
    if (existing) await this.authorize(record, 'resume', { kind: 'parent', sessionId: parent.id }, signal);
    const binding = { command: record.command, childSessionId: record.childSessionId, action: record.action, actionVersion: record.actionVersion };
    if (existing && canonical(existing) !== canonical(binding)) throw new Error('REACTIVE_CHILD_BINDING_MISMATCH');
    const receipt = this.childReceipt(inspection);
    if (!existing) {
      appendPluginRecord(agent.session, 'plugin:reactive/child', binding);
      // fork 的 Goal 属于继承历史。独立目标由初始真实回执建立，不能借用父目标。
      const inherited = this.ctx.goals.get(agent);
      if (inherited) this.ctx.goals.clear(agent, inherited);
    }
    const completion = Promise.withResolvers<void>();
    completion.promise.catch(() => {});
    const gate: Gate = { record, ...completion };
    this.#gates.set(agent, gate);
    if (existing) {
      if (!receipt) throw new ChildCreationIncompleteError(record.command, this.reference(record));
      await this.verifyReceipt(record, receipt.receipt, parent);
      const goal = this.ctx.goals.get(agent);
      if (!goal || goal.id !== receipt.receipt.goal.id) throw new Error('REACTIVE_CHILD_GOAL_MISMATCH');
      const input = restoreChildInput(agent, receipt.receipt.messageId);
      if (input !== 'cancelled' && goal.phase === 'active' && goal.activation === 'disarmed') this.ctx.goals.resume(agent, goal);
      gate.confirmed = true;
      completion.resolve();
    }
    return { record, implementation: descriptor.program };
  }
  async beforeStep(agent: Agent, signal: AbortSignal) {
    const gate = this.#gates.get(agent);
    if (!gate || gate.confirmed) return;
    signal.throwIfAborted();
    let aborted: (() => void) | undefined;
    try {
      await Promise.race([gate.promise, new Promise<never>((_resolve, reject) => {
        aborted = () => reject(signal.reason);
        signal.addEventListener('abort', aborted, { once: true });
      })]);
    } finally { if (aborted) signal.removeEventListener('abort', aborted); }
    signal.throwIfAborted();
  }
  /** 冷恢复的 manager 会重新投递原生结算 notice；只核关联并触发权威日志读取。 */
  async settlementNotices(parent: Agent, messages: readonly UserMessage[], signal: AbortSignal) {
    const notices = messages.filter(message => message.source.kind === 'subagent-settled');
    if (!notices.length) return;
    if (this.ctx.agents.get(parent.id) !== parent) throw new Error('REACTIVE_CHILD_RUNTIME_OWNER_REQUIRED');
    const inspection = await readNativeInspection(this.ctx, parent.id, signal);
    const issued = records(inspection, 'plugin:reactive/delegation', delegationRecordSchema);
    for (const message of notices) {
      if (message.source.kind !== 'subagent-settled') continue;
      const senderSessionId = message.source.senderSessionId;
      const record = one(issued.filter(item => item.childSessionId === senderSessionId), 'REACTIVE_CHILD_SETTLEMENT');
      if (!record || record.command.parentSessionId !== parent.id) throw new Error('REACTIVE_CHILD_SETTLEMENT_ASSOCIATION_REQUIRED');
      const child = await readNativeInspection(this.ctx, record.childSessionId, signal), confirmation = this.childReceipt(child);
      if (!confirmation) throw new ChildCreationIncompleteError(record.command, this.reference(record));
      if (canonical(this.childBinding(child)) !== canonical({ command: record.command, childSessionId: record.childSessionId, action: record.action, actionVersion: record.actionVersion }))
        throw new Error('REACTIVE_CHILD_BINDING_MISMATCH');
      await this.verifyReceipt(record, confirmation.receipt);
      signal.throwIfAborted();
      this.updates$.next({ parentSessionId: parent.id, childSessionId: record.childSessionId });
    }
  }
  private async start(prepared: Prepared, signal: AbortSignal): Promise<NativeChildReceipt> {
    const { record, parent } = prepared;
    const previous = await this.locateRecord(record.command, signal);
    if (previous) throw new ChildCreationIncompleteError(previous.command, this.reference(previous));
    appendPluginRecord(parent.session, 'plugin:reactive/delegation', record);
    await this.flush(parent);
    try {
      const activation = await this.ctx.subagents.startActivation({ provider: record.mode, childId: SessionId(record.childSessionId),
        label: record.action, request: { parent, prompt: [{ type: 'text', text: record.objective }],
          maxDepth: this.ctx.subagents.resolveMaxDepth() }, signal, delivery: 'caller' });
      activation.result.catch(error => console.error('reactive.child.activation.failed', { childSessionId: record.childSessionId, error }));
      const child = this.ctx.agents.get(SessionId(record.childSessionId));
      if (!child || !this.ctx.agents.isOwnedBy(child.id, parent)) throw new Error('REACTIVE_CHILD_RUNTIME_OWNER_REQUIRED');
      const gate = this.#gates.get(child);
      if (!gate) throw new Error('REACTIVE_CHILD_GATE_REQUIRED');
      gate.acceptedMessageId = activation.messageId;
      return await this.confirmCreation(child, parent, gate);
    } catch (cause) {
      const reason = cause instanceof Error ? cause.message : String(cause);
      let failure = cause;
      try {
        appendPluginRecord(parent.session, 'plugin:reactive/child-creation-error', { command: record.command, childSessionId: record.childSessionId, reason });
        await this.flush(parent);
      } catch (persist) { failure = new AggregateError([cause, persist], 'REACTIVE_CHILD_CREATION_FAILURE_NOT_PERSISTED'); }
      // 原生受理前由 manager 回滚；受理后的回执故障不取消已经接受的子工作。
      this.updates$.next({ parentSessionId: parent.id, childSessionId: record.childSessionId });
      throw new ChildCreationIncompleteError(record.command, this.reference(record), failure);
    }
  }
  private async confirmCreation(child: Agent, parent: Agent, gate: Gate): Promise<NativeChildReceipt> {
    const { record } = gate;
    const persisted = this.childReceipt(snapshot(child.session));
    const messageId = persisted?.receipt.messageId ?? gate.acceptedMessageId;
    if (!messageId) throw new ChildCreationIncompleteError(record.command, this.reference(record));
    const accepted = child.session.snapshotEvents().slice(child.session.inheritedEventCount).some(event => event.type === 'agent/inbox/spliced'
      && event.data.inserted.some(message => message.id === messageId));
    if (!accepted) throw new Error('REACTIVE_CHILD_INITIAL_MESSAGE_NOT_ACCEPTED');
    if (!parent.session.snapshotEvents().some(event => event.type === 'subagent/catalog' && event.data.childId === child.id))
      throw new ChildCreationIncompleteError(record.command, this.reference(record));
    // 原生 manager 已登记的 catalog 必须先持久，子确认不能依赖稍后的父 ACK 顺带刷入它。
    await this.flush(parent);
    if (!gate.goal) {
      if (persisted) gate.goal = persisted.receipt.goal;
      else {
        if (this.ctx.goals.get(child)) throw new ChildCreationIncompleteError(record.command, this.reference(record));
        const created = this.ctx.goals.create(child, { objective: record.objective });
        gate.goal = { id: created.id, revision: created.revision };
      }
    }
    const receipt = nativeChildReceiptSchema.parse({ ...this.reference(record), messageId, goal: gate.goal });
    const input = { sourceSessionId: child.id, messageId, requestId: null, source: 'delegation', intent: 'goal' as const, scope: record.childInput, actorId: null, participant: 'native' as const };
    const own = child.session.snapshotEvents().slice(child.session.inheritedEventCount);
    if (!own.some(event => pluginRecordOf(event)?.type === 'plugin:reactive/child-input'))
      appendPluginRecord(child.session, 'plugin:reactive/child-input', { messageId, input });
    if (!own.some(event => { const item = pluginRecordOf(event); return item?.type === 'plugin:reactive/goal-inputs' && goalBindingSchema.parse(item.data).goalId === receipt.goal.id; }))
      appendPluginRecord(child.session, 'plugin:reactive/goal-inputs', { goalId: receipt.goal.id, inputs: [input] });
    const confirmation = { command: record.command, receipt };
    if (!persisted) appendPluginRecord(child.session, 'plugin:reactive/child-receipt', confirmation);
    else if (canonical(persisted) !== canonical(confirmation)) throw new Error('REACTIVE_CHILD_RECEIPT_CONFLICT');
    await this.flush(child);
    const parentReceipt = one(records(snapshot(parent.session), 'plugin:reactive/child-receipt', childReceiptRecordSchema)
      .filter(item => canonical(item.command) === canonical(record.command)), 'REACTIVE_CHILD_RECEIPT');
    if (!parentReceipt) appendPluginRecord(parent.session, 'plugin:reactive/child-receipt', confirmation);
    else if (canonical(parentReceipt) !== canonical(confirmation)) throw new Error('REACTIVE_CHILD_RECEIPT_CONFLICT');
    await this.flush(parent);
    await this.verifyReceipt(record, receipt);
    gate.confirmed = true; gate.resolve();
    this.updates$.next({ parentSessionId: parent.id, childSessionId: child.id });
    return receipt;
  }
  async recoverLive(child: Agent, parent: Agent, signal: AbortSignal) {
    const gate = this.#gates.get(child);
    if (!gate || gate.confirmed) return;
    if (this.ctx.agents.get(parent.id) !== parent || !this.ctx.agents.isOwnedBy(child.id, parent)) throw new Error('REACTIVE_CHILD_RUNTIME_OWNER_REQUIRED');
    await this.authorize(gate.record, 'resume', { kind: 'parent', sessionId: parent.id }, signal);
    await this.confirmCreation(child, parent, gate);
  }
  private async flush(agent: Agent) { if (!await this.ctx.sessions.flush(agent.session)) throw new Error('REACTIVE_PERSISTENCE_UNAVAILABLE'); }
  private async verifyReceipt(record: DelegationRecord, receipt: NativeChildReceipt, repairParent?: Agent) {
    const parent = await readNativeInspection(this.ctx, record.command.parentSessionId);
    const child = await readNativeInspection(this.ctx, record.childSessionId);
    const confirmations = records(parent, 'plugin:reactive/child-receipt', childReceiptRecordSchema)
      .filter(item => canonical(item.command) === canonical(record.command));
    const childConfirmation = this.childReceipt(child);
    const expected = { command: record.command, receipt };
    if (confirmations.some(item => canonical(item) !== canonical(expected))
      || canonical(childConfirmation) !== canonical(expected)) throw new ChildCreationIncompleteError(record.command, this.reference(record));
    const catalog = parent.events.some(event => event.type === 'subagent/catalog' && event.data.childId === record.childSessionId);
    if (!catalog) throw new ChildCreationIncompleteError(record.command, this.reference(record));
    if (child.meta.parentSession !== record.command.parentSessionId || child.meta.origin !== 'subagent') throw new Error('REACTIVE_CHILD_LINEAGE_MISMATCH');
    const own = child.events.slice(child.inheritedEventCount);
    if (!own.some(event => event.type === 'agent/inbox/spliced' && event.data.inserted.some(message => message.id === receipt.messageId)))
      throw new Error('REACTIVE_CHILD_INITIAL_MESSAGE_NOT_ACCEPTED');
    const input = one(own.flatMap(event => {
      const item = pluginRecordOf(event);
      return item?.type === 'plugin:reactive/child-input' ? [item.data] : [];
    }), 'REACTIVE_CHILD_INPUT');
    const expectedInput = { messageId: receipt.messageId, input: { sourceSessionId: record.childSessionId, messageId: receipt.messageId,
      requestId: null, source: 'delegation', intent: 'goal', scope: record.childInput, actorId: null, participant: 'native' } };
    if (canonical(input) !== canonical(expectedInput)) throw new Error('REACTIVE_CHILD_INPUT_MISMATCH');
    if (!confirmations.length) {
      if (!repairParent || repairParent.id !== record.command.parentSessionId || this.ctx.agents.get(repairParent.id) !== repairParent)
        throw new ChildCreationIncompleteError(record.command, this.reference(record));
      appendPluginRecord(repairParent.session, 'plugin:reactive/child-receipt', expected);
      await this.flush(repairParent);
    }
  }
  async resume(record: DelegationRecord, parent: Agent, signal: AbortSignal) {
    signal.throwIfAborted();
    this.descriptor(record);
    const child = await readNativeInspection(this.ctx, record.childSessionId, signal);
    const confirmed = this.childReceipt(child);
    if (!confirmed) throw new ChildCreationIncompleteError(record.command, this.reference(record));
    if (this.ctx.agents.get(parent.id) !== parent || child.meta.parentSession !== parent.id || record.command.parentSessionId !== parent.id)
      throw new Error('REACTIVE_CHILD_RUNTIME_OWNER_REQUIRED');
    // authorizer 在 serial agent/created 中取得 native isOwnedBy 的精确实例证据后核验。
    await steerHostSubagentPrompt(this.ctx.subagents, parent, SessionId(record.childSessionId), [], {
      kind: 'reactive-child-resume', childSessionId: record.childSessionId, messageId: confirmed.receipt.messageId }, signal);
  }
  async restoreChildren(parent: Agent, signal: AbortSignal) {
    const inspection = await readNativeInspection(this.ctx, parent.id, signal);
    for (const record of records(inspection, 'plugin:reactive/delegation', delegationRecordSchema)) {
      signal.throwIfAborted();
      if (this.ctx.agents.get(SessionId(record.childSessionId))) continue;
      const child = await this.childInspection(record, signal);
      if (!child || !this.childReceipt(child)) continue;
      const goal = foldGoal(child.events).goal;
      if (!goal || goal.id !== this.childReceipt(child)!.receipt.goal.id || goal.phase !== 'active') continue;
      const ended = child.events.slice(child.inheritedEventCount).findLast(event => event.type === 'turn/end');
      if (ended?.type === 'turn/end' && ['error', 'aborted', 'max-tokens'].includes(ended.data.reason.kind)) continue;
      await this.resume(record, parent, signal);
    }
  }
  private async childInspection(record: DelegationRecord, signal?: AbortSignal) {
    try { return await readNativeInspection(this.ctx, record.childSessionId, signal); }
    catch (error) { if (error instanceof SessionPersistenceNotFoundError) return undefined; throw error; }
  }
  private async state(record: DelegationRecord, signal: AbortSignal): Promise<NativeChildState> {
    const inspection = await this.childInspection(record, signal);
    if (!inspection) {
      const failure = (await this.creationFailures(record, signal)).at(-1)?.payload;
      return failure ?? { childSessionId: record.childSessionId, phase: 'reserved', goal: null, sequence: 0, reason: null };
    }
    const confirmation = this.childReceipt(inspection);
    if (!confirmation) return { childSessionId: record.childSessionId, phase: 'creation-incomplete', goal: null, sequence: inspection.events.length, reason: 'initial-message-receipt-unavailable' };
    const failures = await this.creationFailures(record, signal);
    if (failures.length && !inspection.events.slice(inspection.inheritedEventCount)
      .some(event => event.type === 'user/message' && event.data.id === confirmation.receipt.messageId)) {
      return { childSessionId: record.childSessionId, phase: 'creation-incomplete', goal: confirmation.receipt.goal,
        sequence: inspection.events.length, reason: failures.at(-1)!.payload.reason };
    }
    try { await this.verifyReceipt(record, confirmation.receipt); }
    catch (error) {
      if (!(error instanceof ChildCreationIncompleteError)) throw error;
      return { childSessionId: record.childSessionId, phase: 'creation-incomplete', goal: confirmation.receipt.goal,
        sequence: inspection.events.length, reason: 'bilateral-receipt-unconfirmed' };
    }
    const goal = foldGoal(inspection.events).goal;
    if (!goal || goal.id !== confirmation.receipt.goal.id) return { childSessionId: record.childSessionId, phase: 'error',
      goal: goal ? { id: goal.id, revision: goal.revision } : null, sequence: inspection.events.length, reason: 'child-goal-replaced-or-cleared' };
    const nodes = await this.feedback(record, signal, inspection);
    const latest = nodes.at(-1)?.payload;
    const goalSeq = inspection.events.findLast(event => event.type === 'goal/change' && event.data.operation !== 'clear' && event.data.goal.id === goal.id)?.seq ?? -1;
    return { childSessionId: record.childSessionId, phase: goal.phase === 'active' && latest && ['error', 'cancelled', 'interrupted'].includes(latest.phase)
      && latest.sequence > goalSeq ? latest.phase : goal.phase, goal: { id: goal.id, revision: goal.revision }, sequence: inspection.events.length,
      reason: goal.blockedReason?.message ?? latest?.reason ?? null };
  }
  private async creationFailures(record: DelegationRecord, signal: AbortSignal): Promise<OperationFeedbackNode<NativeChildFeedback>[]> {
    const parent = await readNativeInspection(this.ctx, record.command.parentSessionId, signal);
    return parent.events.slice(parent.inheritedEventCount).flatMap(event => {
      const item = pluginRecordOf(event);
      if (item?.type !== 'plugin:reactive/child-creation-error') return [];
      const failure = failureSchema.parse(item.data);
      return canonical(failure.command) !== canonical(record.command) ? [] : [{ id: 'parent:' + event.seq,
        payload: { childSessionId: record.childSessionId, phase: 'creation-incomplete' as const, goal: null, sequence: event.seq, reason: failure.reason } }];
    });
  }
  private async feedback(record: DelegationRecord, signal: AbortSignal, existing?: SessionInspection): Promise<OperationFeedbackNode<NativeChildFeedback>[]> {
    const failures = await this.creationFailures(record, signal);
    const inspection = existing ?? await this.childInspection(record, signal);
    if (!inspection) return failures;
    const confirmation = this.childReceipt(inspection);
    if (!confirmation) return failures;
    try { await this.verifyReceipt(record, confirmation.receipt); }
    catch (error) { if (error instanceof ChildCreationIncompleteError) return failures; throw error; }
    const nodes: OperationFeedbackNode<NativeChildFeedback>[] = inspection.events.slice(inspection.inheritedEventCount).flatMap(event => {
      let phase: NativeChildFeedback['phase'] | undefined;
      let reason: string | null = null;
      let goal: NativeChildFeedback['goal'] = null;
      if (event.type === 'goal/change' && event.data.kind === 'goal/change' && event.data.operation !== 'clear'
        && event.data.goal.id === confirmation.receipt.goal.id && event.data.goal.phase !== 'active') {
        phase = event.data.goal.phase; goal = { id: event.data.goal.id, revision: event.data.goal.revision };
        reason = event.data.goal.blockedReason?.message ?? null;
      }
      if (event.type === 'turn/end') {
        if (event.data.reason.kind === 'error') { phase = 'error'; reason = event.data.reason.error.message; }
        else if (event.data.reason.kind === 'aborted') { phase = 'cancelled'; reason = event.data.reason.reason.kind; }
        else if (event.data.reason.kind === 'interrupted') phase = 'interrupted';
      }
      return phase ? [{ id: 'child:' + event.seq, payload: { childSessionId: record.childSessionId, phase, goal, sequence: event.seq, reason } }] : [];
    });
    return [...failures, ...nodes];
  }
}
