import { createHash } from 'node:crypto';
import type { Context } from '@deepseek-ai/cordis';
import type { Agent } from '@deepseek-ai/dsh-agent';
import { appendPluginRecord, SessionLogOffset } from '@deepseek-ai/dsh-session';
import type { SessionInspection } from '@deepseek-ai/dsh-session-persistence';
import { firstValueFrom } from 'rxjs';
import { z } from 'zod';
import { locateOperation, registerOperationFollow, settledOperation, readOperationFeedback, encodeOperationFeedback, assertOperationFollowSupported, isOperationOwner, operationFollowLinkSchema, type OperationAccessContext as OperationAccess, type OperationFollowLink, type OperationOwnerReference, type OperationOwner, type OperationContract, type OperationNotificationResult, type OperationFeedbackRead, type OperationReference, type OperationFeedbackPayload } from '@hanger-source/dsh-reactive/kernel';
import type { Continuation, InputReceipt, Json, Submission, WaitReference } from '@hanger-source/dsh-reactive/kernel';
import { abortable } from '@hanger-source/dsh-reactive';
import { canonical } from '../conversations/records.ts';
import { receiptSchema, intentSchema, deliverySchema, followsSchema, followsProjection, type FollowState, type FollowIntent } from './projection.ts';

type FollowClosure = { readonly status: 'not-started' } | { readonly status: 'unregistered' };
interface FollowEntry {
  readonly outcome: Promise<boolean>;
  readonly resolve: (closed: boolean) => void;
  readonly reject: (reason: unknown) => void;
  entered: boolean;
  failure?: { readonly error: unknown };
}

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
export function operationFollowId(sessionId: string, callSequence: number, owner: OperationOwnerReference, command: Json) {
  return createHash('sha256').update(canonical([sessionId, callSequence, owner.definition.kind, owner.definition.version, command])).digest('hex');
}

/** 持久关联、原生等待与可靠通知的接合；业务受理事实始终从配置的 owner 读取。 */
export class OperationFollows {
  #owners = new Map<string, OperationOwnerReference>();
  #barriers = new WeakMap<Agent, Map<string, Promise<ReadonlySet<string>>>>();
  // 进入门只覆盖本进程首次登记的接点；resolve 后不等待 owner 的可靠 ACK。
  #entries = new Map<string, FollowEntry>();
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
  private assertEntryCheckpoint(link: OperationFollowLink) {
    const failure = this.#entries.get(this.entryKey(link))?.failure;
    if (failure) throw failure.error;
  }
  private assertTargetCheckpoint(state: FollowState, id: string) {
    const intent = Object.hasOwn(state.intents, id) ? state.intents[id] : undefined;
    if (intent) this.assertEntryCheckpoint(intent);
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
  private entryKey(link: OperationFollowLink) { return canonical([link.continuation.sessionId, link.id]); }
  private async awaitEntry(link: OperationFollowLink, signal: AbortSignal): Promise<boolean> {
    const entry = this.#entries.get(this.entryKey(link));
    if (!entry) return false;
    return firstValueFrom(abortable(() => entry.outcome, signal));
  }
  private pending(agent: Agent, group: string) {
    for (const intent of Object.values(this.state(agent).intents)) {
      if (intent.mode === 'wait' && this.group(intent) === group) this.assertEntryCheckpoint(intent);
    }
    return this.links(agent).filter(link => link.mode === 'wait' && this.group(link) === group && !this.host.resolved(agent, link));
  }
  /** 后台关联不登记 Goal wait；投递仍须命中持久保存的确切原工作。 */
  background(agent: Agent, continuation: Continuation, wait: WaitReference) {
    const links = this.links(agent).filter(link => link.mode === 'background' && canonical(link.wait) === canonical(wait)
      && canonical(link.continuation) === canonical(continuation));
    for (const link of links) this.assertEntryCheckpoint(link);
    return links.length > 0;
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
    this.assertEntryCheckpoint(intent);
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
    this.assertEntryCheckpoint(intent);
    this.publisher(intent);
    const link = this.publicLink(intent);
    if (link.continuation.sessionId !== access.sessionId || this.owner(link) !== owner) throw new Error('REACTIVE_OPERATION_FOLLOW_MISMATCH');
    if (link.when !== 'feedback') throw new Error('REACTIVE_OPERATION_FEEDBACK_PARTICIPATION_REQUIRED');
    if (await this.awaitEntry(link, access.signal) || Object.hasOwn(state.unregistered, id))
      throw new Error('REACTIVE_OPERATION_FOLLOW_UNREGISTERED');
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
    // 未落盘结案的失败门仍交给各组恢复报告；不能当作权威关闭，也不能封锁其他组。
    return Object.values(state.intents).filter(intent => this.#entries.get(this.entryKey(intent))?.failure
      || (!Object.hasOwn(state.notStarted, intent.id) && !Object.hasOwn(state.unregistered, intent.id)))
      .map(intent => this.publicLink(intent));
  }
  isNotStarted(agent: Agent, id: string) {
    const state = this.state(agent);
    this.assertTargetCheckpoint(state, id);
    return Object.hasOwn(state.notStarted, id);
  }
  inspectedNotStarted(inspection: SessionInspection, id: string) {
    const state = this.inspectedState(inspection);
    this.assertTargetCheckpoint(state, id);
    return Object.hasOwn(state.notStarted, id);
  }
  isUnregistered(agent: Agent, id: string) {
    const state = this.state(agent);
    this.assertTargetCheckpoint(state, id);
    return Object.hasOwn(state.unregistered, id);
  }
  inspectedUnregistered(inspection: SessionInspection, id: string) {
    const state = this.inspectedState(inspection);
    this.assertTargetCheckpoint(state, id);
    return Object.hasOwn(state.unregistered, id);
  }
  closedWait(agent: Agent, continuation: Continuation, wait: WaitReference) {
    const state = this.state(agent);
    for (const intent of Object.values(state.intents)) {
      if (canonical(intent.wait) === canonical(wait) && canonical(intent.continuation) === canonical(continuation))
        this.assertEntryCheckpoint(intent);
    }
    return [...Object.values(state.notStarted), ...Object.values(state.unregistered)].some(closed => canonical(closed.wait) === canonical(wait)
      && canonical(closed.continuation) === canonical(continuation));
  }
  async notStarted(agent: Agent, id: string) {
    const state = this.state(agent);
    const intent = Object.hasOwn(state.intents, id) ? state.intents[id] : undefined;
    // prepare 可能在写入 intent 前即失败；没有关联就没有待结案的 native wait。
    if (!intent) return;
    if (this.isUnregistered(agent, id)) throw new Error('REACTIVE_OPERATION_FOLLOW_UNREGISTERED');
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
    this.assertEntryCheckpoint(intent);
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
  async prepare(agent: Agent, owner: OperationOwnerReference, raw: OperationFollowLink, callSequence: number, access: OperationAccess,
    admission?: { admit(): void | PromiseLike<void>; enter(): void }) {
    const link = operationFollowLinkSchema.parse(raw);
    this.assertEntryCheckpoint(link);
    this.validate(owner, link.when);
    const publisherId = this.host.publisherId();
    if (!publisherId.trim()) throw new Error('REACTIVE_OPERATION_PUBLISHER_REQUIRED');
    const intent = intentSchema.parse({ ...link, publisherId });
    const previous = this.state(agent).intents[link.id];
    if (previous) this.publisher(previous);
    if (previous && canonical(previous) !== canonical(intent)) throw new Error('REACTIVE_OPERATION_FOLLOW_CONFLICT');
    if (this.isNotStarted(agent, link.id)) throw new Error('REACTIVE_OPERATION_NOT_STARTED');
    if (this.isUnregistered(agent, link.id)) throw new Error('REACTIVE_OPERATION_FOLLOW_UNREGISTERED');
    const entryKey = this.entryKey(link);
    if (this.#entries.has(entryKey)) throw new Error('REACTIVE_OPERATION_FOLLOW_IN_PROGRESS');
    const completion = admission ? Promise.withResolvers<boolean>() : undefined;
    const entry: FollowEntry | undefined = completion ? { outcome: completion.promise, resolve: completion.resolve, reject: completion.reject,
      entered: false } : undefined;
    if (entry) {
      this.#entries.set(entryKey, entry);
      void entry.outcome.catch(() => {});
    }
    let appended = false;
    try {
      if (!previous) { appendPluginRecord(agent.session, 'plugin:reactive/operation-follow', intent); appended = true; }
      appendPluginRecord(agent.session, 'plugin:reactive/operation-follow-call', { id: link.id, callSequence });
      // intent 在业务 side 的可靠关联之前落盘；恢复绝不重放 execute。
      await this.host.checkpoint(agent);
      if (admission) await admission.admit();
      await registerOperationFollow(owner, link.command, link, access, entry ? () => {
        admission!.enter();
        entry.entered = true;
        entry.resolve(false);
      } : undefined);
      if (link.mode === 'wait') await this.host.wait(agent, link.continuation, link.wait);
    } catch (error) {
      if (entry && !entry.entered) {
        try {
          // 已有 intent 可能来自崩溃前已经进入的 owner，不能据此声明从未登记。
          if (!appended) throw error;
          appendPluginRecord(agent.session, 'plugin:reactive/operation-follow-unregistered', {
            id: link.id, continuation: link.continuation, wait: link.wait,
          });
          await this.host.checkpoint(agent);
          entry.resolve(true);
        } catch (checkpointError) {
          entry.failure = { error: checkpointError };
          entry.reject(checkpointError);
          if (checkpointError !== error) throw new AggregateError([error, checkpointError], 'REACTIVE_OPERATION_FOLLOW_CHECKPOINT_FAILED');
        }
      }
      throw error;
    } finally {
      if (entry?.entered) this.#entries.delete(entryKey);
    }
  }
  /** 冷入口同样先验证真实通知者的关联权限，不能以 locator 的读取权限代替。 */
  async admit(link: OperationFollowLink, access: OperationAccess, closed?: FollowClosure): Promise<FollowClosure | undefined> {
    const source = access.source;
    if (source.kind !== 'participant') throw new Error('REACTIVE_OPERATION_NOTIFIER_REQUIRED');
    access.signal.throwIfAborted();
    const owner = this.owner(link);
    this.validate(owner, link.when);
    const unregistered = await this.awaitEntry(link, access.signal);
    await firstValueFrom(abortable(() => this.host.admit(link, source, access.signal), access.signal));
    access.signal.throwIfAborted();
    // 已结案的框架关联只核本次通知准入；不能让旧 outbox 重新登记业务观察。
    if (unregistered) return { status: 'unregistered' };
    if (closed) return closed;
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
              if (registered.has(item.id) || this.isNotStarted(agent, item.id) || this.isUnregistered(agent, item.id)) continue;
              await this.admit(item, recovery);
              registered.add(item.id);
            }
            for (const item of pending) {
              recovery.signal.throwIfAborted();
              if (this.isNotStarted(agent, item.id) || this.isUnregistered(agent, item.id)) continue;
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
    if (this.isUnregistered(agent, id)) throw new Error('REACTIVE_OPERATION_FOLLOW_UNREGISTERED');
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
    const closed = await this.admit(link, access, this.isNotStarted(agent, id) ? { status: 'not-started' }
      : this.isUnregistered(agent, id) ? { status: 'unregistered' } : undefined);
    if (closed) return closed;
    if (this.isUnregistered(agent, id)) return { status: 'unregistered' };
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
