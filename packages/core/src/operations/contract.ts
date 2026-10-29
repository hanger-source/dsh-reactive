import { Observable, EMPTY, EmptyError, concatWith, defer, filter, firstValueFrom, from, fromEvent, isObservable, map, mergeMap, takeUntil, tap, throwError, type MonoTypeOperatorFunction } from 'rxjs';
import { z } from 'zod';
import type { ActionSource, ActionTarget } from '../actions/contract.ts';
import type { Json } from '../conversations/protocol.ts';
import { operationFollowLinkSchema, type OperationFollowLink } from './follows.ts';
export { operationFollowLinkSchema, type OperationFollowLink, type OperationFollowMode } from './follows.ts';
import type { InputReceipt } from '../conversations/conversation.ts';
import type { Observation } from '../resources/observation.ts';
import { shareResource } from '../resources/sharing.ts';
import type { ResourceReader } from '../resources/reader.ts';

/** 纯业务操作合同；身份与事实独立于某次函数、工具和组件的生存期。 */
export interface OperationContract {
  readonly kind: string;
  readonly version: number;
  readonly command: z.ZodType<Json>;
  readonly reference: z.ZodType<Json>;
  readonly state: z.ZodType;
  settled(state: never): unknown;
  readonly feedback?: z.ZodType;
}

export interface OperationDefinition<C extends z.ZodType<Json>, R extends z.ZodType<Json>, S extends z.ZodType, Terminal extends z.output<S>, F extends z.ZodType | undefined = undefined>
extends OperationContract {
  readonly command: C;
  readonly reference: R;
  readonly state: S;
  /** undefined 表示仍在执行；业务失败与取消也应返回明确的领域终态。 */
  settled(state: z.output<S>): Terminal | undefined;
  /** 节点 payload 的纯编解码合同；节点选择与持久事实由 owner 负责。 */
  readonly feedback: F | undefined;
}

export interface OperationOptions<C extends z.ZodType<Json>, R extends z.ZodType<Json>, S extends z.ZodType, Terminal extends z.output<S>, F extends z.ZodType | undefined = undefined> {
  readonly kind: string;
  readonly version: number;
  readonly command: C;
  readonly reference: R;
  readonly state: S;
  settled(state: z.output<NoInfer<S>>): Terminal | undefined;
  readonly feedback?: F;
}

/** 公开与持久关联使用同一 JSON wire，owner 消费解析后的冻结领域引用。 */
export type OperationCommand<D extends OperationContract> = z.input<D['command']> & Json;
export type OperationReference<D extends OperationContract> = z.input<D['reference']> & Json;
export type OperationParsedCommand<D extends OperationContract> = ActionTarget<z.output<D['command']>>;
export type OperationParsedReference<D extends OperationContract> = ActionTarget<z.output<D['reference']>>;
export type OperationState<D extends OperationContract> = z.output<D['state']>;
export type OperationSettled<D extends OperationContract> = Exclude<ReturnType<D['settled']>, undefined>;
export type OperationFeedbackPayload<D extends OperationContract> = z.output<NonNullable<D['feedback']>>;
type RawFeedbackPayload<D extends OperationContract> = z.input<NonNullable<D['feedback']>>;

/** id 来自 owner 的持久观察事实；相同 payload 不代表同一个节点。 */
export interface OperationFeedbackNode<Payload> { readonly id: string; readonly payload: Payload; }
export type OperationFeedbackRead<Reference, Payload> =
  | { readonly status: 'unknown' }
  | { readonly status: 'rejected'; readonly reason: Json }
  | { readonly status: 'pending'; readonly reference: Reference }
  | { readonly status: 'ready'; readonly reference: Reference; readonly node: OperationFeedbackNode<Payload> };

/** 读取有自己的可信参与来源，不取得原 Goal 或工具结束权限。 */
export interface OperationAccessContext {
  readonly sessionId: string;
  readonly source: ActionSource;
  readonly signal: AbortSignal;
}

export type OperationLocation<Reference, State> =
  | { readonly status: 'accepted'; readonly reference: Reference; readonly state: State }
  | { readonly status: 'rejected'; readonly reason: Json }
  | { readonly status: 'unknown' };

/** 通知回执只表示原生持久受理，不表示操作结算或模型已经消费。 */
export type OperationNotificationAcknowledgement =
  | { readonly status: 'not-started' }
  | { readonly status: 'unregistered' }
  | { readonly status: 'accepted'; readonly receipt: InputReceipt };
export type OperationNotificationResult = OperationNotificationAcknowledgement
  | { readonly status: 'unknown' | 'pending' };

/** 只传递原登记地址；通知确认只消费业务来源的待办。 */
export interface OperationNotificationDelivery {
  readonly sessionId: string;
  readonly followId: string;
  acknowledge(result: OperationNotificationAcknowledgement): void | PromiseLike<void>;
}
export interface OperationNotificationConnection {
  /** 停止来源继续读取和交接；已经开始的 ACK 仍结算。 */
  readonly signal: AbortSignal;
  deliver(delivery: OperationNotificationDelivery): Promise<OperationNotificationResult>;
}
/** Host 等本次交接结算后调用；返回前必须停止来源，不清除 follow/feedback 事实。 */
export type OperationNotificationCleanup = () => void | PromiseLike<void>;
export interface OperationNotificationSource {
  /** 应用原有设施继续负责持久、租约和重投；异步连接也必须在 signal 退出后返回清理。 */
  connect(connection: OperationNotificationConnection): OperationNotificationCleanup | PromiseLike<OperationNotificationCleanup>;
}

const identity = z.string().min(1);

export interface OperationOwnerReference { readonly definition: OperationContract; }
export interface OperationOwner<D extends OperationContract = OperationContract> extends OperationOwnerReference { readonly definition: D; }

export interface OperationOwnerOptions<D extends OperationContract> {
  readonly notifications?: OperationNotificationSource;
  locate(command: OperationParsedCommand<D>, access: OperationAccessContext):
    OperationLocation<z.input<D['reference']>, z.input<D['state']>> | PromiseLike<OperationLocation<z.input<D['reference']>, z.input<D['state']>>>;
  observe(reference: OperationParsedReference<D>, access: OperationAccessContext): Observable<Observation<z.input<D['state']>>>;
  /** 必须可靠保存关联并使用业务既有投递设施；没有该能力就不能提供可靠 follow。 */
  registerFollow?(command: OperationParsedCommand<D>, link: OperationFollowLink, access: OperationAccessContext): void | PromiseLike<void>;
  /** 只读该真实登记已选定的后继节点；不建立观察工作，不把授权或读取异常改成领域拒绝。 */
  readFeedback?(command: OperationParsedCommand<D>, link: OperationFollowLink, access: OperationAccessContext):
    OperationFeedbackRead<z.input<D['reference']>, RawFeedbackPayload<D>>
    | PromiseLike<OperationFeedbackRead<z.input<D['reference']>, RawFeedbackPayload<D>>>;
}

export interface OperationHandle<D extends OperationContract> {
  readonly definition: D;
  readonly reference: OperationReference<D>;
  /** 当前查询的完整资源读侧；read读取当前值，settled另行等待权威业务终态。 */
  readonly state: ResourceReader<OperationState<D>>;
  /** 冷查询共享来源；订阅不会启动、重试或取消业务操作。 */
  readonly observations$: Observable<Observation<OperationState<D>>>;
  /** 只等待权威终态；query error/abort 会拒绝，业务失败和取消则作为领域值返回。 */
  settled(options?: { readonly signal?: AbortSignal }): Promise<OperationSettled<D>>;
}

/** 异构声明只保留合同与原命令；真正 owner 及读取分派保存在私有 WeakMap。 */
export interface OperationBindingReference<D extends OperationContract = OperationContract> {
  readonly definition: D;
  readonly command: OperationCommand<D>;
}
export interface OperationParticipation<D extends OperationContract> {
  readonly id: string;
  readonly definition: D;
  readonly command: OperationCommand<D>;
  /** 单次权威快照；pending/unknown 不冒充未来节点已经返回。 */
  readFeedback(): Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>>;
}
export interface BoundOperation<D extends OperationContract> extends OperationBindingReference<D> {
  locate(): Promise<OperationLocation<OperationReference<D>, OperationState<D>>>;
  attach(reference: OperationReference<D>): OperationHandle<D>;
  participation(id: string): OperationParticipation<D>;
}
export interface OperationBindingOptions<D extends OperationContract> {
  /** 宿主为每次读取签发真实访问来源；不是 Goal 或原生调用控制。 */
  access(binding: OperationBindingReference<D>): OperationAccessContext;
  /** 由实际 Host 找到持久 link 并核验 definition/command，不接收调用者拼造的登记。 */
  readFeedback(binding: OperationBindingReference<D>, id: string, access: OperationAccessContext):
    Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>>;
}

type AccessAdmission = () => void | PromiseLike<void>;
const accessSettlement = Symbol('operation-access-settlement');
type SettlingAccess = OperationAccessContext & { readonly [accessSettlement]?: (work: Promise<unknown>) => void };
type OwnerBindings = {
  readonly notifications?: OperationNotificationSource;
  command(raw: unknown): Json;
  reference(raw: unknown): Json;
  attach(raw: unknown, access: OperationAccessContext, admit?: AccessAdmission): OperationHandle<OperationContract>;
  locate(raw: unknown, access: OperationAccessContext, admit?: AccessAdmission): Promise<OperationLocation<Json, unknown>>;
  registerFollow?: (raw: unknown, link: OperationFollowLink, access: OperationAccessContext, beforeRegister?: () => void) => Promise<void>;
  readFeedback?: (raw: unknown, link: OperationFollowLink, access: OperationAccessContext) => Promise<OperationFeedbackRead<Json, unknown>>;
  settled(state: unknown): unknown;
};
const definitions = new WeakSet<object>();
const owners = new WeakMap<object, OwnerBindings>();
const operationBindings = new WeakMap<object, { readonly owner: OperationOwnerReference; readonly command: Json }>();
const rawLocation = z.discriminatedUnion('status', [
  z.object({ status: z.literal('accepted'), reference: z.unknown(), state: z.unknown() }).strict(),
  z.object({ status: z.literal('rejected'), reason: z.json() }).strict(),
  z.object({ status: z.literal('unknown') }).strict(),
]);
const generation = z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER);
const rawObservation = z.discriminatedUnion('status', [
  z.object({ status: z.literal('pending'), generation }).strict(),
  z.object({ status: z.literal('ready'), generation, value: z.unknown(), observedAt: z.number() }).strict(),
  z.object({ status: z.literal('error'), generation, error: z.unknown() }).strict(),
]);
const rawNode = z.object({ id: identity, payload: z.unknown() }).strict();
const rawFeedback = z.discriminatedUnion('status', [
  z.object({ status: z.literal('unknown') }).strict(),
  z.object({ status: z.literal('rejected'), reason: z.json() }).strict(),
  z.object({ status: z.literal('pending'), reference: z.unknown() }).strict(),
  z.object({ status: z.literal('ready'), reference: z.unknown(), node: rawNode }).strict(),
]);

export function defineOperation<C extends z.ZodType<Json>, R extends z.ZodType<Json>, S extends z.ZodType, Terminal extends z.output<S>, F extends z.ZodType | undefined = undefined>(
  options: OperationOptions<C, R, S, Terminal, F>,
): OperationDefinition<C, R, S, Terminal, F> {
  if (!options.kind.trim() || options.kind !== options.kind.trim() || !Number.isSafeInteger(options.version) || options.version < 1) {
    throw new Error('REACTIVE_OPERATION_DEFINITION_INVALID');
  }
  if (options.feedback !== undefined && !(options.feedback instanceof z.ZodType)) throw new Error('REACTIVE_OPERATION_FEEDBACK_SCHEMA_REQUIRED');
  const definition: OperationDefinition<C, R, S, Terminal, F> = Object.freeze({ kind: options.kind, version: options.version,
    command: options.command, reference: options.reference, state: options.state, settled: options.settled, feedback: options.feedback });
  definitions.add(definition);
  return definition;
}

/** owner 负责真实受理事实、查询权限和可靠关联；创建 owner 不开始查询或业务。 */
export function implementOperation<D extends OperationContract>(definition: D,
  options: NoInfer<OperationOwnerOptions<D>>,
): OperationOwner<D> {
  if (!definitions.has(definition)) throw new Error('REACTIVE_OPERATION_DEFINITION_INVALID');
  const owner = Object.freeze({ definition });
  const locate = options.locate, observe = options.observe;
  const source = options.notifications, connect = source?.connect;
  if (source !== undefined && typeof connect !== 'function') throw new Error('REACTIVE_OPERATION_NOTIFICATION_SOURCE_INVALID');
  // D 来自已登记的纯合同；解析和终态投影均由该合同决定。
  const parseState = (raw: unknown): OperationState<D> => definition.state.parse(raw) as OperationState<D>;
  const settled = (state: OperationState<D>): OperationSettled<D> | undefined =>
    definition.settled(state as never) as OperationSettled<D> | undefined;
  const bindings: OwnerBindings = {
    ...(connect ? { notifications: Object.freeze({ connect: (connection: OperationNotificationConnection) => connect.call(source, connection) }) } : {}),
    command: raw => parseIdentity(definition.command, raw).wire,
    reference: raw => parseIdentity(definition.reference, raw).wire,
    attach(raw, initialAccess, admit) {
      const reference = parseIdentity<D['reference']>(definition.reference, raw);
      const access = ownAccess(initialAccess);
      const state = shareResource(defer(() => {
        access.signal.throwIfAborted();
        let last: Observation<OperationState<D>> | undefined;
        return from(acknowledgement(admit?.())).pipe(mergeMap(() => {
          access.signal.throwIfAborted();
          const source = observe(reference.value, access);
          if (!isObservable(source)) throw new Error('REACTIVE_OPERATION_OBSERVABLE_REQUIRED');
          return source;
        }), map(value => {
          const observation = rawObservation.parse(value);
          return observation.status === 'ready'
            ? { ...observation, value: parseState(observation.value) }
            : observation;
        }), tap(observation => { last = observation; }), concatWith(defer(() =>
          // 操作来源结束仍未给出业务终态时，不能让settled永远等待一个已结束的查询。
          last?.status === 'ready' && settled(last.value) === undefined ? throwError(() => new EmptyError()) : EMPTY)), untilAbort(access.signal));
      }), { signal: access.signal });
      const observations$ = state.observations$;
      const handle: OperationHandle<D> = Object.freeze({
        definition,
        reference: reference.wire,
        state,
        observations$,
        async settled(readOptions: { readonly signal?: AbortSignal } = {}) {
          const signal = readOptions.signal ? AbortSignal.any([access.signal, readOptions.signal]) : access.signal;
          signal.throwIfAborted();
          const terminal$ = observations$.pipe(map(observation => {
            if (observation.status === 'error') throw observation.error;
            return observation.status === 'ready' ? settled(observation.value) : undefined;
          }), filter((state): state is OperationSettled<D> => state !== undefined));
          return firstValueFrom(terminal$.pipe(untilAbort(signal)));
        },
      });
      return handle;
    },
    async locate(raw, initialAccess, admit) {
      const command = parseIdentity<D['command']>(definition.command, raw);
      const access = ownAccess(initialAccess);
      return firstValueFrom(defer(() => {
        access.signal.throwIfAborted();
        return from(acknowledgement(admit?.())).pipe(mergeMap(() => {
          access.signal.throwIfAborted();
          return from(retainAccessWork(access, locate(command.value, access)));
        }), map(value => {
          const location = rawLocation.parse(value);
          if (location.status === 'accepted') return { status: location.status,
            reference: parseIdentity<D['reference']>(definition.reference, location.reference).wire,
            state: parseState(location.state) };
          return location.status === 'rejected' ? { ...location, reason: ownJson<Json>(location.reason) } : location;
        }), untilAbort(access.signal));
      }));
    },
    settled: state => settled(state as OperationState<D>),
  };
  const register = options.registerFollow;
  if (register) bindings.registerFollow = async (raw, rawLink, initialAccess, beforeRegister) => {
    const command = parseIdentity<D['command']>(definition.command, raw);
    const link = verifiedLink(definition, command.wire, rawLink);
    if (link.when === 'feedback' && !definition.feedback) throw new Error('REACTIVE_OPERATION_FEEDBACK_UNSUPPORTED');
    const access = ownAccess(initialAccess);
    await firstValueFrom(defer(() => {
      access.signal.throwIfAborted();
      beforeRegister?.();
      return from(retainAccessWork(access, acknowledgement(register(command.value, link, access)))).pipe(untilAbort(access.signal));
    }));
  };
  const readFeedback = options.readFeedback;
  if (readFeedback) {
    if (!definition.feedback) throw new Error('REACTIVE_OPERATION_FEEDBACK_SCHEMA_REQUIRED');
    bindings.readFeedback = async (raw, rawLink, initialAccess) => {
      const command = parseIdentity<D['command']>(definition.command, raw);
      const link = verifiedLink(definition, command.wire, rawLink);
      if (link.when !== 'feedback') throw new Error('REACTIVE_OPERATION_FEEDBACK_REQUIRED');
      const access = ownAccess(initialAccess);
      return firstValueFrom(defer(() => {
        access.signal.throwIfAborted();
        return from(retainAccessWork(access, readFeedback(command.value, link, access))).pipe(map(rawResult => {
          const result = rawFeedback.parse(rawResult);
          if (result.status === 'unknown') return result;
          if (result.status === 'rejected') return { ...result, reason: ownJson<Json>(result.reason) };
          const reference = parseIdentity<D['reference']>(definition.reference, result.reference).wire;
          if (result.status === 'pending') return { status: result.status, reference };
          const node = decodeOperationFeedback(definition, result.node);
          if (node.id === link.after) throw new Error('REACTIVE_OPERATION_FEEDBACK_NOT_SUCCESSOR');
          return { status: result.status, reference, node };
        }), untilAbort(access.signal));
      }));
    };
  }
  owners.set(owner, bindings);
  return owner;
}

/** 纯绑定原命令；构造不 locate、订阅、登记观察或执行任何业务。 */
export function bindOperation<D extends OperationContract>(owner: OperationOwner<D>, originalCommand: OperationCommand<D>,
  options: OperationBindingOptions<NoInfer<D>>): BoundOperation<D> {
  const command = operationCommand(owner, originalCommand);
  const definition = owner.definition;
  const access = options.access, read = options.readFeedback;
  const binding: BoundOperation<D> = Object.freeze({
    definition, command,
    locate: () => locateOperation(owner, command, access(binding)),
    attach: (reference: OperationReference<D>) => attachOperation(owner, reference, access(binding)),
    participation(id: string): OperationParticipation<D> {
      const participationId = identity.parse(id);
      return Object.freeze({ id: participationId, definition, command,
        readFeedback: () => read(binding, participationId, ownAccess(access(binding))),
      });
    },
  });
  operationBindings.set(binding, { owner, command });
  return binding;
}

/** @internal 公开字段不构成绑定身份或授权。 */
export function isOperationBinding(value: unknown): value is OperationBindingReference {
  return typeof value === 'object' && value !== null && operationBindings.has(value);
}
function bindingOf(binding: OperationBindingReference) {
  const bound = operationBindings.get(binding);
  if (!bound) throw new Error('REACTIVE_OPERATION_BINDING_INVALID');
  return bound;
}
/** @internal 真实绑定捕获的 owner，不读可伪造的结构字段。 */
export function operationBindingOwner(binding: OperationBindingReference): OperationOwnerReference { return bindingOf(binding).owner; }
/** @internal 原命令是绑定时自有、冻结的 canonical wire。 */
export function operationBindingCommand(binding: OperationBindingReference): Json { return bindingOf(binding).command; }

/** @internal 实际 owner 身份由本模块登记，公开字段不能伪造。 */
export function isOperationOwner(value: unknown): value is OperationOwnerReference {
  return typeof value === 'object' && value !== null && owners.has(value);
}

function ownerBindings(owner: OperationOwnerReference): OwnerBindings {
  const bindings = owners.get(owner);
  if (!bindings) throw new Error('REACTIVE_OPERATION_OWNER_INVALID');
  return bindings;
}

/** @internal 只取得签发 owner 时捕获的来源，不读取外部可变配置。 */
export function operationNotificationSource(owner: OperationOwnerReference): OperationNotificationSource | undefined {
  return ownerBindings(owner).notifications;
}
/** @internal 仅为本次交接持有真实调用；不会改变业务 owner 或其他宿主的寿命。 */
export function withOperationAccessSettlement(access: OperationAccessContext, retain: (work: Promise<unknown>) => void): OperationAccessContext {
  return Object.freeze({ ...access, [accessSettlement]: retain });
}

/** @internal pure mapping 后的 raw 身份在此验证、编码并保存 owned JSON。 */
export function operationCommand<D extends OperationContract>(owner: OperationOwner<D>, raw: unknown): OperationCommand<D> {
  // 该 owner 已按自己的 command schema 验证并反向编码为 wire。
  return ownerBindings(owner).command(raw) as OperationCommand<D>;
}
/** @internal receipt 已解析；这里只解析另一份明确的 operation reference 合同。 */
export function operationReference(owner: OperationOwnerReference, raw: unknown): Json { return ownerBindings(owner).reference(raw); }

export function attachOperation<D extends OperationContract>(owner: OperationOwner<D>, raw: z.input<D['reference']>,
  access: OperationAccessContext, admit?: AccessAdmission): OperationHandle<D> {
  // owner identity 捕获了唯一合同；泛型擦除只跨越此实际 owner 的存储边界。
  return ownerBindings(owner).attach(raw, access, admit) as OperationHandle<D>;
}

export function locateOperation<D extends OperationContract>(owner: OperationOwner<D>, raw: z.input<D['command']>,
  access: OperationAccessContext, admit?: AccessAdmission): Promise<OperationLocation<OperationReference<D>, OperationState<D>>> {
  return ownerBindings(owner).locate(raw, access, admit) as Promise<OperationLocation<OperationReference<D>, OperationState<D>>>;
}

/** @internal 可靠 follow 不能退化成临时 Observable 订阅。 */
export async function registerOperationFollow(owner: OperationOwnerReference, raw: unknown, link: OperationFollowLink,
  access: OperationAccessContext, beforeRegister?: () => void): Promise<void> {
  const register = ownerBindings(owner).registerFollow;
  if (!register) throw new Error('REACTIVE_OPERATION_FOLLOW_UNSUPPORTED');
  await register(raw, link, access, beforeRegister);
}

/** @internal feedback 有独立权威读取；不会先 locate，也不会以读建立登记。 */
export function readOperationFeedback<D extends OperationContract>(owner: OperationOwner<D>, rawCommand: unknown,
  link: OperationFollowLink, access: OperationAccessContext): Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>> {
  const read = ownerBindings(owner).readFeedback;
  if (!read) throw new Error('REACTIVE_OPERATION_FEEDBACK_READ_UNSUPPORTED');
  // 实际 owner 闭包捕获唯一 schema；跨异构存储恢复同一合同的读取类型。
  return read(rawCommand, link, access) as Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>>;
}

/** @internal 声明装配的纯能力校验；不写 intent、不查询、不订阅来源。 */
export function assertOperationFollowSupported(owner: OperationOwnerReference, when: 'settled' | 'feedback'): void {
  const bindings = ownerBindings(owner);
  if (!bindings.registerFollow) throw new Error('REACTIVE_OPERATION_FOLLOW_UNSUPPORTED');
  if (when === 'feedback' && (!owner.definition.feedback || !bindings.readFeedback)) throw new Error('REACTIVE_OPERATION_FEEDBACK_UNSUPPORTED');
}

/** @internal 输入只来自同一 owner 已解析的 state；不重复运行 state schema。 */
export function settledOperation(owner: OperationOwnerReference, state: unknown): unknown { return ownerBindings(owner).settled(state); }
/** 反馈 payload 只在 raw owner/wire 进入领域时解析；节点身份仍由 owner 验证。 */
export function decodeOperationFeedback<D extends OperationContract>(definition: D, raw: unknown): OperationFeedbackNode<OperationFeedbackPayload<D>> {
  const schema = feedbackSchema(definition);
  const node = rawNode.parse(raw);
  // schema 来自该纯 definition；此处恢复其具体输出类型，不复制或猜测 codec。
  return Object.freeze({ id: node.id, payload: schema.parse(node.payload) }) as OperationFeedbackNode<OperationFeedbackPayload<D>>;
}
/** 已解析反馈反向编码；单向 transform 在 wire 出口明确失败，不再 parse 输出。 */
export function encodeOperationFeedback<D extends OperationContract>(definition: D,
  node: OperationFeedbackNode<OperationFeedbackPayload<D>>): OperationFeedbackNode<Json> {
  const schema = feedbackSchema(definition);
  return Object.freeze({ id: identity.parse(node.id), payload: ownJson<Json>(z.json().parse(z.encode(schema, node.payload))) });
}
function feedbackSchema(definition: OperationContract): z.ZodType {
  if (!definitions.has(definition)) throw new Error('REACTIVE_OPERATION_DEFINITION_INVALID');
  if (!definition.feedback) throw new Error('REACTIVE_OPERATION_FEEDBACK_SCHEMA_REQUIRED');
  return definition.feedback;
}

function verifiedLink(definition: OperationContract, command: Json, raw: OperationFollowLink): OperationFollowLink {
  const link = ownJson<OperationFollowLink>(operationFollowLinkSchema.parse(raw));
  if (link.operation.kind !== definition.kind || link.operation.version !== definition.version
    || canonical(link.command) !== canonical(command)) throw new Error('REACTIVE_OPERATION_FOLLOW_MISMATCH');
  return link;
}

function parseIdentity<S extends z.ZodType<Json>>(schema: S, raw: unknown) {
  const value = schema.parse(raw);
  const wire = ownJson<Json>(z.json().parse(z.encode(schema, value))) as z.input<S> & Json;
  return { value: ownJson(value), wire };
}

function ownAccess(access: OperationAccessContext): OperationAccessContext {
  const retain = (access as SettlingAccess)[accessSettlement];
  return Object.freeze({ sessionId: access.sessionId, source: ownJson(access.source), signal: access.signal,
    ...(retain ? { [accessSettlement]: retain } : {}) });
}

function retainAccessWork<T>(access: OperationAccessContext, value: T | PromiseLike<T>): Promise<T> {
  const work = Promise.resolve(value);
  (access as SettlingAccess)[accessSettlement]?.(work);
  return work;
}

function untilAbort<T>(signal: AbortSignal): MonoTypeOperatorFunction<T> {
  return takeUntil(defer(() => {
    signal.throwIfAborted();
    return fromEvent(signal, 'abort').pipe(mergeMap(() => throwError(() => signal.reason)));
  }));
}

function acknowledgement(value: void | PromiseLike<void>): Promise<void> {
  if (value !== undefined && (value === null || typeof value !== 'object' && typeof value !== 'function' || typeof value.then !== 'function')) {
    throw new Error('REACTIVE_OPERATION_ACK_INVALID');
  }
  return Promise.resolve(value).then(result => {
    if (result !== undefined) throw new Error('REACTIVE_OPERATION_ACK_INVALID');
  });
}

function ownJson<T>(value: T): ActionTarget<T> { return copyJson(value) as ActionTarget<T>; }
function copyJson(value: unknown, ancestors = new Set<object>()): Json {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value !== 'object' || ancestors.has(value)) throw new Error('REACTIVE_OPERATION_IDENTITY_NOT_JSON');
  ancestors.add(value);
  try {
    if (Array.isArray(value)) return Object.freeze(Array.from(value, item => copyJson(item, ancestors)));
    const prototype: unknown = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null || Object.getOwnPropertySymbols(value).length) {
      throw new Error('REACTIVE_OPERATION_IDENTITY_NOT_JSON');
    }
    return Object.freeze(Object.fromEntries(Object.entries(value).map(([key, item]) => [key, copyJson(item, ancestors)])));
  } finally { ancestors.delete(value); }
}

function canonical(value: Json): string {
  return JSON.stringify(value, (_key, item: unknown) => {
    if (item === null || typeof item !== 'object' || Array.isArray(item)) return item;
    return Object.fromEntries(Object.entries(item).sort(([left], [right]) => left.localeCompare(right)));
  });
}
