import { createHash } from 'node:crypto';
import type { Context } from '@deepseek-ai/cordis';
import type { Agent } from '@deepseek-ai/dsh-agent';
import { appendPluginRecord, pluginRecordOf, SessionLogOffset } from '@deepseek-ai/dsh-session';
import type { SessionInspection } from '@deepseek-ai/dsh-session-persistence';
import type { ProjectionDefinition } from '@deepseek-ai/dsh-session-projection';
import { firstValueFrom } from 'rxjs';
import { z } from 'zod';
import { locateOperation, registerOperationFollow, settledOperation, readOperationFeedback, encodeOperationFeedback, assertOperationFollowSupported, isOperationOwner, operationFollowLinkSchema, type OperationAccessContext as OperationAccess, type OperationFollowLink, type OperationOwnerReference, type OperationOwner, type OperationContract, type OperationNotificationResult, type OperationFeedbackRead, type OperationReference, type OperationFeedbackPayload } from '@hanger-source/dsh-reactive/kernel';
import type { Continuation, InputReceipt, Json, Submission, WaitReference } from '@hanger-source/dsh-reactive/kernel';
import { abortable } from '@hanger-source/dsh-reactive';
import { canonical, operationNotStartedSchema } from '../conversations/records.ts';

const receiptSchema = z.object({ id: z.string().min(1), reference: z.json() }).strict();
const publisher = { publisherId: z.string().min(1) };
const intentSchema = z.discriminatedUnion('when', [operationFollowLinkSchema.options[0].extend(publisher), operationFollowLinkSchema.options[1].extend(publisher)]);
const deliverySchema = z.object({ id: z.string().min(1), actorId: z.string().min(1), notifierId: z.string().min(1), reference: z.json().optional(),
  node: z.object({ id: z.string().min(1), payload: z.json() }).strict().optional(), instruction: z.string().min(1).max(32_000) }).strict();
const followsSchema = z.object({
  inheritedEventCount: z.number().int().nonnegative(),
  intents: z.record(z.string(), intentSchema),
  receipts: z.record(z.string(), receiptSchema),
  deliveries: z.record(z.string(), deliverySchema),
  notStarted: z.record(z.string(), operationNotStartedSchema),
}).strict();
type FollowState = z.infer<typeof followsSchema>;
type FollowIntent = z.infer<typeof intentSchema>;
type Delivery = z.infer<typeof deliverySchema>;

declare module '@deepseek-ai/dsh-session/types' {
  interface PluginRecordMap {
    'plugin:reactive/operation-follow': FollowIntent;
    'plugin:reactive/operation-receipt': z.infer<typeof receiptSchema>;
    'plugin:reactive/operation-delivery': Delivery;
  }
}
declare module '@deepseek-ai/dsh-session-projection/types' {
  interface SessionProjectionStateMap { reactiveOperations: FollowState; }
}

/** 这里只持久保存调用、原工作与业务 owner 的关联；不保存或执行业务任务。 */
const followsProjection: ProjectionDefinition<'reactiveOperations'> = {
  key: 'reactiveOperations', stateVersion: 4, stateSchema: followsSchema,
  init: (_header, inheritedEventCount) => ({ inheritedEventCount, intents: {}, receipts: {}, deliveries: {}, notStarted: {} }),
  apply(previous, event) {
    if (event.seq < previous.inheritedEventCount) return previous;
    const record = pluginRecordOf(event);
    if (record?.type === 'plugin:reactive/operation-not-started') {
      const closed = operationNotStartedSchema.parse(record.data);
      const intent = Object.hasOwn(previous.intents, closed.id) ? previous.intents[closed.id] : undefined;
      if (!intent) throw new Error('REACTIVE_OPERATION_FOLLOW_NOT_FOUND');
      if (canonical({ id: intent.id, continuation: intent.continuation, wait: intent.wait }) !== canonical(closed))
        throw new Error('REACTIVE_OPERATION_FOLLOW_MISMATCH');
      if (Object.hasOwn(previous.receipts, closed.id)) throw new Error('REACTIVE_OPERATION_ALREADY_STARTED');
      return { ...previous, notStarted: { ...previous.notStarted, [closed.id]: closed } };
    }
    if (record?.type === 'plugin:reactive/operation-follow') {
      const link = intentSchema.parse(record.data);
      const before = Object.hasOwn(previous.intents, link.id) ? previous.intents[link.id] : undefined;
      if (before && canonical(before) !== canonical(link)) throw new Error('REACTIVE_OPERATION_FOLLOW_CONFLICT');
      return { ...previous, intents: { ...previous.intents, [link.id]: link } };
    }
    if (record?.type === 'plugin:reactive/operation-receipt') {
      const receipt = receiptSchema.parse(record.data);
      if (!Object.hasOwn(previous.intents, receipt.id)) throw new Error('REACTIVE_OPERATION_FOLLOW_NOT_FOUND');
      const before = Object.hasOwn(previous.receipts, receipt.id) ? previous.receipts[receipt.id] : undefined;
      if (before && canonical(before) !== canonical(receipt)) throw new Error('REACTIVE_OPERATION_REFERENCE_CONFLICT');
      return { ...previous, receipts: { ...previous.receipts, [receipt.id]: receipt } };
    }
    if (record?.type === 'plugin:reactive/operation-delivery') {
      const delivery = deliverySchema.parse(record.data);
      if (!Object.hasOwn(previous.intents, delivery.id)) throw new Error('REACTIVE_OPERATION_FOLLOW_NOT_FOUND');
      const before = Object.hasOwn(previous.deliveries, delivery.id) ? previous.deliveries[delivery.id] : undefined;
      if (before && canonical(before) !== canonical(delivery)) throw new Error('REACTIVE_OPERATION_DELIVERY_CONFLICT');
      return { ...previous, deliveries: { ...previous.deliveries, [delivery.id]: delivery } };
    }
    return previous;
  },
};

export interface FollowHost {
  publisherId(): string;
  recoveryAccess(agent: Agent): OperationAccess;
  retainRecovery<T>(agent: Agent, work: Promise<T>, signal: AbortSignal): Promise<T>;
  checkpoint(agent: Agent): Promise<void>;
  wait(agent: Agent, continuation: Continuation, wait: WaitReference): Promise<void>;
  resolved(agent: Agent, link: OperationFollowLink): boolean;
  waiting(agent: Agent, link: OperationFollowLink): boolean;
  admit(link: OperationFollowLink, source: Extract<OperationAccess['source'], { readonly kind: 'participant' }>, signal: AbortSignal): Promise<void>;
  deliver(agent: Agent, input: Submission, actorId: string, signal: AbortSignal): Promise<InputReceipt>;
}

function key(owner: OperationOwnerReference) {
  return JSON.stringify([owner.definition.kind, owner.definition.version]);
}
export function operationFollowId(sessionId: string, callId: string, owner: OperationOwnerReference, command: Json) {
  return createHash('sha256').update(canonical([sessionId, callId, owner.definition.kind, owner.definition.version, command])).digest('hex');
}

/** 持久关联、原生等待与可靠通知的接合；业务受理事实始终从配置的 owner 读取。 */
export class OperationFollows {
  #owners = new Map<string, OperationOwnerReference>();
  #barriers = new WeakMap<Agent, Map<string, Promise<ReadonlySet<string>>>>();
  constructor(private readonly ctx: Context, owners: readonly OperationOwnerReference[], private readonly host: FollowHost) {
    ctx.sessionProjections.register(followsProjection);
    for (const owner of owners) {
      if (!isOperationOwner(owner)) throw new Error('REACTIVE_OPERATION_OWNER_INVALID');
      const identity = key(owner);
      if (this.#owners.has(identity) && this.#owners.get(identity) !== owner) throw new Error('REACTIVE_OPERATION_OWNER_DUPLICATE');
      this.#owners.set(identity, owner);
    }
  }
  private state(agent: Agent): FollowState {
    const state = this.ctx.sessionProjections.stateOf(agent.session, 'reactiveOperations');
    if (!state) throw new Error('REACTIVE_OPERATION_PROJECTION_REQUIRED');
    return state;
  }
  private owner(link: OperationFollowLink) {
    const owner = this.#owners.get(JSON.stringify([link.operation.kind, link.operation.version]));
    if (!owner) throw new Error('REACTIVE_OPERATION_OWNER_REQUIRED');
    return owner;
  }
  private publisher(intent: FollowIntent) {
    const publisherId = this.host.publisherId();
    if (!publisherId.trim()) throw new Error('REACTIVE_OPERATION_PUBLISHER_REQUIRED');
    if (intent.publisherId !== publisherId) throw new Error('REACTIVE_OPERATION_PUBLISHER_CONFLICT');
    return publisherId;
  }
  private publicLink(intent: FollowIntent): OperationFollowLink {
    const { publisherId: _publisherId, ...link } = intent;
    return link;
  }
  private group(link: OperationFollowLink) {
    // 无 Goal 的工作由原 continuation 区分，不借用另一组输入的等待。
    return canonical(link.continuation.goal ?? link.continuation);
  }
  private pending(agent: Agent, group: string) {
    return this.links(agent).filter(link => link.mode === 'wait' && this.group(link) === group && !this.host.resolved(agent, link));
  }
  /** 后台关联不登记 Goal wait；投递仍须命中持久保存的确切原工作。 */
  background(agent: Agent, continuation: Continuation, wait: WaitReference) {
    return this.links(agent).some(link => link.mode === 'background' && canonical(link.wait) === canonical(wait)
      && canonical(link.continuation) === canonical(continuation));
  }
  definition(kind: string, version: number): OperationContract {
    const owner = this.#owners.get(JSON.stringify([kind, version]));
    if (!owner) throw new Error('REACTIVE_OPERATION_OWNER_REQUIRED');
    return owner.definition;
  }
  forDefinition<D extends OperationContract>(definition: D): OperationOwner<D> {
    const owner = this.#owners.get(JSON.stringify([definition.kind, definition.version]));
    if (!owner || owner.definition !== definition) throw new Error('REACTIVE_OPERATION_OWNER_MISMATCH');
    // 已核验实际登记 owner 的纯合同 identity；此处只恢复同一合同的泛型。
    return owner as OperationOwner<D>;
  }
  validate(owner: OperationOwnerReference, when: 'settled' | 'feedback') {
    if (this.#owners.get(key(owner)) !== owner) throw new Error('REACTIVE_OPERATION_OWNER_MISMATCH');
    assertOperationFollowSupported(owner, when);
  }
  assertNotificationOwner(link: OperationFollowLink, owner: OperationOwnerReference) {
    if (this.owner(link) !== owner) throw new Error('REACTIVE_OPERATION_OWNER_MISMATCH');
  }
  inspectedLink(inspection: SessionInspection, id: string): OperationFollowLink {
    const state = this.inspectedState(inspection);
    if (!Object.hasOwn(state.intents, id)) throw new Error('REACTIVE_OPERATION_FOLLOW_NOT_FOUND');
    const intent = state.intents[id]!;
    this.publisher(intent);
    return this.publicLink(intent);
  }
  private inspectedState(inspection: SessionInspection): FollowState {
    const restored = this.ctx.sessionProjections.restore({}, inspection.events, SessionLogOffset(0), inspection.meta, inspection.inheritedEventCount);
    return followsSchema.parse(restored.checkpoint.reactiveOperations?.val);
  }
  readFeedback<D extends OperationContract>(agent: Agent, definition: D, command: unknown, id: string, access: OperationAccess): Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>> {
    return this.readRegisteredFeedback(this.state(agent), definition, command, id, access);
  }
  readInspectedFeedback<D extends OperationContract>(inspection: SessionInspection, definition: D, command: unknown, id: string, access: OperationAccess): Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>> {
    return this.readRegisteredFeedback(this.inspectedState(inspection), definition, command, id, access);
  }
  private async readRegisteredFeedback<D extends OperationContract>(state: FollowState, definition: D, command: unknown, id: string, access: OperationAccess): Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>> {
    const owner = this.forDefinition(definition);
    const intent = Object.hasOwn(state.intents, id) ? state.intents[id] : undefined;
    if (!intent) throw new Error('REACTIVE_OPERATION_FOLLOW_NOT_FOUND');
    this.publisher(intent);
    const link = this.publicLink(intent);
    if (link.continuation.sessionId !== access.sessionId || this.owner(link) !== owner) throw new Error('REACTIVE_OPERATION_FOLLOW_MISMATCH');
    if (link.when !== 'feedback') throw new Error('REACTIVE_OPERATION_FEEDBACK_PARTICIPATION_REQUIRED');
    if (Object.hasOwn(state.notStarted, id)) throw new Error('REACTIVE_OPERATION_NOT_STARTED');
    const read = await readOperationFeedback(owner, command, link, access);
    access.signal.throwIfAborted();
    const delivered = state.deliveries[id];
    if (read.status === 'ready' && delivered?.node) {
      const node = encodeOperationFeedback(definition, read.node);
      if (canonical(node) !== canonical(delivered.node) || canonical(read.reference) !== canonical(delivered.reference))
        throw new Error('REACTIVE_OPERATION_FEEDBACK_CONFLICT');
    }
    return read;
  }
  links(agent: Agent): readonly OperationFollowLink[] {
    const state = this.state(agent);
    return Object.values(state.intents).filter(intent => !Object.hasOwn(state.notStarted, intent.id)).map(intent => this.publicLink(intent));
  }
  isNotStarted(agent: Agent, id: string) { return Object.hasOwn(this.state(agent).notStarted, id); }
  inspectedNotStarted(inspection: SessionInspection, id: string) { return Object.hasOwn(this.inspectedState(inspection).notStarted, id); }
  notStartedWait(agent: Agent, continuation: Continuation, wait: WaitReference) {
    return Object.values(this.state(agent).notStarted).some(closed => canonical(closed.wait) === canonical(wait)
      && canonical(closed.continuation) === canonical(continuation));
  }
  async notStarted(agent: Agent, id: string) {
    const state = this.state(agent);
    const intent = Object.hasOwn(state.intents, id) ? state.intents[id] : undefined;
    // prepare 可能在写入 intent 前即失败；没有关联就没有待结案的 native wait。
    if (!intent) return;
    this.publisher(intent);
    if (Object.hasOwn(state.receipts, id)) throw new Error('REACTIVE_OPERATION_ALREADY_STARTED');
    if (!Object.hasOwn(state.notStarted, id)) appendPluginRecord(agent.session, 'plugin:reactive/operation-not-started', {
      id, continuation: intent.continuation, wait: intent.wait,
    });
    // 不使用已经撤销的 tool signal：退出本次准入不能撤销已知未执行事实的持久结案。
    await this.host.checkpoint(agent);
  }
  link(agent: Agent, id: string): OperationFollowLink {
    const intents = this.state(agent).intents;
    if (!Object.hasOwn(intents, id)) throw new Error('REACTIVE_OPERATION_FOLLOW_NOT_FOUND');
    const intent = intents[id]!;
    this.publisher(intent);
    return this.publicLink(intent);
  }
  /** intent 只能阻止提前唤醒；它不代表业务 owner 已授权关联或 native wait 已受理。 */
  hasPendingGoal(agent: Agent, goal: { readonly id: string; readonly revision: number }) {
    return this.pending(agent, canonical({ id: goal.id, revision: goal.revision })).length > 0;
  }
  hasPendingContinuation(agent: Agent, continuation: Continuation) {
    return this.pending(agent, canonical(continuation.goal ?? continuation)).length > 0;
  }
  async prepare(agent: Agent, owner: OperationOwnerReference, raw: OperationFollowLink, access: OperationAccess) {
    const link = operationFollowLinkSchema.parse(raw);
    this.validate(owner, link.when);
    const publisherId = this.host.publisherId();
    if (!publisherId.trim()) throw new Error('REACTIVE_OPERATION_PUBLISHER_REQUIRED');
    const intent = intentSchema.parse({ ...link, publisherId });
    const previous = this.state(agent).intents[link.id];
    if (previous) this.publisher(previous);
    if (previous && canonical(previous) !== canonical(intent)) throw new Error('REACTIVE_OPERATION_FOLLOW_CONFLICT');
    if (this.isNotStarted(agent, link.id)) throw new Error('REACTIVE_OPERATION_NOT_STARTED');
    if (!previous) appendPluginRecord(agent.session, 'plugin:reactive/operation-follow', intent);
    // intent 在业务 side 的可靠关联之前落盘；恢复绝不重放 execute。
    await this.host.checkpoint(agent);
    await registerOperationFollow(owner, link.command, link, access);
    if (link.mode === 'wait') await this.host.wait(agent, link.continuation, link.wait);
  }
  /** 冷入口同样先验证真实通知者的关联权限，不能以 locator 的读取权限代替。 */
  async admit(link: OperationFollowLink, access: OperationAccess, notStarted = false) {
    const source = access.source;
    if (source.kind !== 'participant') throw new Error('REACTIVE_OPERATION_NOTIFIER_REQUIRED');
    access.signal.throwIfAborted();
    const owner = this.owner(link);
    this.validate(owner, link.when);
    await firstValueFrom(abortable(() => this.host.admit(link, source, access.signal), access.signal));
    access.signal.throwIfAborted();
    // 已结案的框架关联只核本次通知准入；不能让旧 outbox 重新登记业务观察。
    if (notStarted) return;
    await registerOperationFollow(owner, link.command, link, access);
    access.signal.throwIfAborted();
  }
  private async ensureWaits(agent: Agent, link: OperationFollowLink, access: OperationAccess) {
    const group = this.group(link);
    let barriers = this.#barriers.get(agent);
    if (!barriers) { barriers = new Map(); this.#barriers.set(agent, barriers); }
    while (true) {
      let barrier = barriers.get(group);
      if (!barrier) {
        // 外部 outbox 只授权自己的 link；跨模块等待恢复由配置的框架 publisher 独立准入。
        const recovery = this.host.recoveryAccess(agent);
        const registered = new Set<string>();
        barrier = this.host.retainRecovery(agent, Promise.resolve().then(async () => {
          while (true) {
            const pending = this.pending(agent, group);
            // 整组先取得可靠关联 ACK；一个未授权 intent 不能被伪装成等待登记成功。
            for (const item of pending) {
              this.publisher(this.state(agent).intents[item.id]!);
              if (registered.has(item.id) || this.isNotStarted(agent, item.id)) continue;
              await this.admit(item, recovery);
              registered.add(item.id);
            }
            for (const item of pending) {
              recovery.signal.throwIfAborted();
              if (this.isNotStarted(agent, item.id)) continue;
              await this.host.wait(agent, item.continuation, item.wait);
            }
            // register/flush 的 await 期间可能出现同组的新 native 调用。
            if (this.pending(agent, group).every(item => registered.has(item.id) && this.host.waiting(agent, item))) return registered;
          }
        }), recovery.signal);
        barriers.set(group, barrier);
        void barrier.finally(() => { if (barriers.get(group) === barrier) barriers.delete(group); }).catch(() => {});
      }
      const established = await firstValueFrom(abortable(() => barrier!, access.signal));
      access.signal.throwIfAborted();
      // 另一个完成回调也可能刚加入 intent；返回前再次核验当前组。
      if (this.pending(agent, group).every(item => established.has(item.id) && this.host.waiting(agent, item))) return;
    }
  }
  async receipt(agent: Agent, id: string, owner: OperationOwnerReference, reference: Json) {
    if (this.isNotStarted(agent, id)) throw new Error('REACTIVE_OPERATION_NOT_STARTED');
    const link = this.link(agent, id);
    if (this.owner(link) !== owner) throw new Error('REACTIVE_OPERATION_OWNER_MISMATCH');
    const receipt = receiptSchema.parse({ id, reference });
    const delivered = this.state(agent).deliveries[id];
    if (delivered?.reference !== undefined && canonical(delivered.reference) !== canonical(reference)) throw new Error('REACTIVE_OPERATION_REFERENCE_CONFLICT');
    const previous = this.state(agent).receipts[id];
    if (previous && canonical(previous) !== canonical(receipt)) throw new Error('REACTIVE_OPERATION_REFERENCE_CONFLICT');
    if (!previous) appendPluginRecord(agent.session, 'plugin:reactive/operation-receipt', receipt);
    await this.host.checkpoint(agent);
  }
  async restore(agent: Agent, links: readonly OperationFollowLink[], access: OperationAccess) {
    const groups = new Map<string, OperationFollowLink[]>();
    for (const link of links) {
      const group = this.group(link), members = groups.get(group) ?? [];
      members.push(link); groups.set(group, members);
    }
    // 旧 Goal 的拒绝仍明确报告，但不能阻止另一授权版本建立等待及投递。
    const restored = await Promise.allSettled([...groups.values()].map(async members => {
      for (const link of members) await this.complete(agent, link.id, access);
    }));
    const failures = restored.flatMap(result => result.status === 'rejected' ? [result.reason] : []);
    if (failures.length) throw new AggregateError(failures, 'REACTIVE_OPERATION_RECOVERY_FAILED');
  }
  async complete(agent: Agent, id: string, access: OperationAccess): Promise<OperationNotificationResult> {
    const link = this.link(agent, id);
    if (access.source.kind !== 'participant') throw new Error('REACTIVE_OPERATION_PUBLISHER_REQUIRED');
    const actorId = this.publisher(this.state(agent).intents[id]!);
    const notifierId = access.source.actorId;
    access.signal.throwIfAborted();
    await this.admit(link, access, this.isNotStarted(agent, id));
    if (this.isNotStarted(agent, id)) return { status: 'not-started' };
    await this.ensureWaits(agent, link, access);
    if (this.isNotStarted(agent, id)) return { status: 'not-started' };
    let delivery = this.state(agent).deliveries[id];
    if (delivery && delivery.actorId !== actorId) throw new Error('REACTIVE_OPERATION_PUBLISHER_CONFLICT');
    if (delivery?.node) await this.readRegisteredFeedback(this.state(agent), this.owner(link).definition, link.command, id, access);
    if (!delivery) {
      const owner = this.owner(link);
      const located = link.when === 'feedback'
        ? await this.readRegisteredFeedback(this.state(agent), owner.definition, link.command, id, access)
        : await locateOperation(owner, link.command, access);
      access.signal.throwIfAborted();
      if (this.isNotStarted(agent, id)) return { status: 'not-started' };
      if (located.status === 'unknown') return { status: 'unknown' };
      let payload: Json;
      let reference: Json | undefined;
      let node: z.infer<typeof deliverySchema>['node'];
      if (located.status === 'rejected') payload = { status: 'rejected', reason: located.reason };
      else {
        const receipt = this.state(agent).receipts[id];
        if (receipt && canonical(receipt.reference) !== canonical(located.reference)) throw new Error('REACTIVE_OPERATION_REFERENCE_CONFLICT');
        reference = located.reference;
        if (link.when === 'feedback') {
          if (located.status !== 'ready') return { status: 'pending' };
          const encoded = encodeOperationFeedback(owner.definition, located.node);
          const deliveredNode = deliverySchema.shape.node.unwrap().parse(encoded);
          node = deliveredNode;
          payload = { status: 'feedback', reference, node: deliveredNode };
        } else {
          if (located.status !== 'accepted') throw new Error('REACTIVE_OPERATION_SETTLEMENT_INVALID');
          if (settledOperation(owner, located.state) === undefined) return { status: 'pending' };
          payload = { status: 'settled', reference, state: z.json().parse(z.encode(owner.definition.state, located.state)) };
        }
      }
      delivery = deliverySchema.parse({ id, actorId, notifierId, ...(reference === undefined ? {} : { reference }), ...(node === undefined ? {} : { node }),
        instruction: '业务操作有新的反馈，请沿原操作和原工作核验后继续。\n' + canonical({
          operation: link.operation, command: link.command, outcome: payload,
        }),
      });
      // 固定首次终态的投递正文；业务 owner 后续资料更新不会改写同 requestId 的内容。
      const existing = this.state(agent).deliveries[id];
      if (existing?.node && node && (canonical(existing.node) !== canonical(node)
        || canonical(existing.reference) !== canonical(reference))) throw new Error('REACTIVE_OPERATION_FEEDBACK_CONFLICT');
      if (existing && canonical(existing) !== canonical(delivery)) {
        if (existing.actorId !== actorId) throw new Error('REACTIVE_OPERATION_PUBLISHER_CONFLICT');
        delivery = existing;
      } else if (!existing) appendPluginRecord(agent.session, 'plugin:reactive/operation-delivery', delivery);
      await this.host.checkpoint(agent);
    }
    await this.ensureWaits(agent, link, access);
    if (this.isNotStarted(agent, id)) return { status: 'not-started' };
    return { status: 'accepted', receipt: await this.host.deliver(agent, {
      requestId: 'operation:' + link.id, content: delivery.instruction, intent: 'result', scope: link.scope,
      result: { continuation: link.continuation, wait: link.wait, ready: true },
    }, actorId, access.signal) };
  }
}
