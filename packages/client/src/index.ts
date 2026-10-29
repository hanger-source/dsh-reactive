import { Context } from '@deepseek-ai/cordis';
import * as Registry from '@deepseek-ai/dsh-typert-registry/client';
import * as Gateway from '@deepseek-ai/dsh-api-gateway/client';
import { type ClientRemote } from '@deepseek-ai/dsh-api-gateway/client';
import { installConnection, type ConnectionHandle, type ConnectionInstallOptions } from '@deepseek-ai/dsh-client-connection/client';
import { type RemoteResult } from '@deepseek-ai/dsh-typert-protocol';
import type { JsonValue } from '@deepseek-ai/dsh-util-values';
import { bytesToBase64 } from '@deepseek-ai/dsh-util-crypto';
import { BehaviorSubject, firstValueFrom, map, takeWhile, type Observable } from 'rxjs';
import type { ActionInvocationHandle, ActionInvocationOrigin } from '@hanger-source/dsh-reactive';
import { z, actionInvocationOriginSchema, assertSharedProgramDefinition, programIdentity, assertProgramIdentity, surfaceSlotDefinition, createSurfaceCollectionClient, actionManifestSlot, createFileUploadInvocation, type ToolActionBinding, type ActionInput, type ActionInvocation, type ActionContract, type ActionScope, type InvocationActivity, type Observation, type Surface, type SurfaceViews, type ViewValue, type OperationContract, type OperationReference, type OperationCommand, type OperationHandle, type BoundOperation, type ProgramContract, type SharedProgramContract, type ProgramSurface, type WorkspaceRegistration, type ConversationCreateOptions, type FileUploadSource, type FileUploadInvocation, type CancellationOptions } from '@hanger-source/dsh-reactive';
import { decodeProgramFailure, ActionAdmissionUnknownError, CreationUnknownError, CreationIncompleteError,
  ProgramParticipantMismatchError, SubmissionAdmissionUnknownError, InputEditAdmissionUnknownError, FileUploadReceiptUnknownError } from './contracts/errors.ts';
import contribution from '@hanger-source/dsh-reactive-client/remote';
import type { ProgramActionStart, ProgramCreationReference, ProgramIdentity, ProgramInputReceipt, ProgramInvocationFrame, ProgramOperationNotificationResult, ProgramWorkspace, ProgramSubmission } from './contracts/types.ts';
import { createAuthenticatedTransport, type AuthenticatedTransport, type AuthenticatedTransportOptions } from './transport.ts';
import { createOperationQueries, createInvocationOperationQueries, attachRemoteOperation, attachInvocationRemoteOperation, locateRemoteOperation, bindRemoteOperation, readRemoteOperationFeedback, type RemoteOperationObservation } from './operations.ts';
import { ProgramJournal, remoteJournal, type ProgramJournalOptions, type ProgramJournalRequest } from './journal.ts';
import { connectProgress, type RemoteProgress } from './progress.ts';
import { createActionBindings } from './action-bindings.ts';
import { remoteView } from './views.ts';
import { remoteAttachments } from './attachments.ts';
import type { SessionAttachments } from '@hanger-source/dsh-reactive';
import { remoteFiles } from './files.ts';
import { createHumanWaits, remoteHuman } from './human.ts';
import { createClientScope, revokeConversationAccess, type ClientScope } from './scope.ts';
import { ResourceReader } from '@hanger-source/dsh-reactive';
import { actionIdentity, assertActionIdentity } from '@hanger-source/dsh-reactive';
import { createHumanQuestionResolver } from '@hanger-source/dsh-reactive';
import type { CollectionResolvedAction, SurfaceClient, ProgramSessionClient, HumanProgramSessionClient } from '@hanger-source/dsh-reactive';
import { createSurfaceModuleClient } from '@hanger-source/dsh-reactive';
import { abortable } from '@hanger-source/dsh-reactive';

export { createAuthenticatedTransport } from './transport.ts';
export type { AuthenticatedTransport, AuthenticatedTransportOptions } from './transport.ts';
export { Inbox } from './contracts/inbox.ts';
export { decodeProgramFailure, ActionAdmissionUnknownError, CreationUnknownError, CreationIncompleteError,
  ProgramParticipantMismatchError, SubmissionAdmissionUnknownError, InputEditAdmissionUnknownError, FileUploadReceiptUnknownError, ConversationAccessRevokedError, SessionProgramMismatchError, SessionFilesUnavailableError, SessionJournalUnavailableError, SessionAttachmentsUnavailableError } from './contracts/errors.ts';
export type { AttachmentIdType, ImageAttachmentRef, SessionAttachments, SessionImageAttachment } from '@hanger-source/dsh-reactive';
export { ProgramJournal } from './journal.ts';
export type { ProgramJournalOptions, ProgramJournalRequest } from './journal.ts';

/** 明确目标的已捕获绑定；调用时仍核验当前声明及真实参与者权限。 */
export type RemoteResolvedAction<D extends ActionContract> = CollectionResolvedAction<D, CancellationOptions, true>;
/** resolver 只捕获当前公开的只读绑定，网络边界允许异步，不受理业务或登记等待。 */
export type RemoteOperations<S extends Surface> = SurfaceClient<S, CancellationOptions, true>['operations'];
export interface ProgramClientOptions extends AuthenticatedTransportOptions {
  readonly recovery?: ConnectionInstallOptions['recovery'];
}
export interface RemoteUploadFileOptions extends CancellationOptions { readonly name?: string; }
interface RemoteSession<P extends ProgramContract> extends ProgramSessionClient<P, CancellationOptions, true> {
  /** 实际创建这个 SDK 的纯合同；UI 或模块组合不得只按相同 sessionId 猜合同。 */
  readonly definition: P;
  readonly identity: ProgramIdentity;
  uploadFile(data: FileUploadSource, options?: RemoteUploadFileOptions): FileUploadInvocation;
  attachOperation<D extends OperationContract>(definition: D, reference: OperationReference<D>, options?: CancellationOptions): OperationHandle<D>;
  notifyOperation(followId: string, options?: CancellationOptions): Promise<ProgramOperationNotificationResult>;
  /** 原生可靠 journal；调用者消费完整 change，不另造事件 cursor 或重放引擎。 */
  openEvents(options: ProgramJournalOptions, request?: ProgramJournalRequest, cancellation?: CancellationOptions): Promise<ProgramJournal>;
  release(): Promise<void>;
}
/** 人工端口在共同观察和业务能力上提供人工参与及控制。 */
export interface RemoteProgramSession<P extends ProgramContract> extends HumanProgramSessionClient<P, CancellationOptions, true> {
  readonly definition: P;
  readonly identity: ProgramIdentity & { readonly participant: 'human' };
  uploadFile(data: FileUploadSource, options?: RemoteUploadFileOptions): FileUploadInvocation;
  attachOperation<D extends OperationContract>(definition: D, reference: OperationReference<D>, options?: CancellationOptions): OperationHandle<D>;
  notifyOperation(followId: string, options?: CancellationOptions): Promise<ProgramOperationNotificationResult>;
  openEvents(options: ProgramJournalOptions, request?: ProgramJournalRequest, cancellation?: CancellationOptions): Promise<ProgramJournal>;
  release(): Promise<void>;
}
/** 服务端口只借用共同资源与业务入口，运行对象没有人工回答或Goal控制方法。 */
export interface RemoteServiceProgramSession<P extends ProgramContract> extends RemoteSession<P> {
  readonly identity: ProgramIdentity & { readonly participant: 'service' };
}
interface ProgramClientBase<P extends ProgramContract, Session extends ProgramSessionClient<P, CancellationOptions, true>> {
  createWorkspace(registration: WorkspaceRegistration): Promise<ProgramWorkspace>;
  create(environmentId: string, options: ConversationCreateOptions): Promise<Session>;
  completeCreation(reference: ProgramCreationReference): Promise<Session>;
  open(sessionId: string): Promise<Session>;
  resume(sessionId: string): Promise<Session>;
  describe(): Promise<readonly ProgramWorkspace[]>;
  dispose(): Promise<void>;
}
export interface ProgramClient<P extends ProgramContract> extends ProgramClientBase<P, RemoteProgramSession<P>> {}
export interface ServiceProgramClient<P extends ProgramContract> extends ProgramClientBase<P, RemoteServiceProgramSession<P>> {}

type ParticipantSession<P extends ProgramContract, Participant extends ProgramIdentity['participant']> =
  Participant extends 'human' ? RemoteProgramSession<P> : RemoteServiceProgramSession<P>;
type ParticipantClient<P extends ProgramContract, Participant extends ProgramIdentity['participant']> =
  Participant extends 'human' ? ProgramClient<P> : ServiceProgramClient<P>;

function result<T>(value: RemoteResult<T>): T { if (!value.ok) throw decodeProgramFailure(value.error); return value.value; }
function json(value: unknown): JsonValue { return z.json().parse(value); }
function jsonIdentity(value: unknown): string {
  return JSON.stringify(value, (_key, item: unknown) => item !== null && typeof item === 'object' && !Array.isArray(item)
    ? Object.fromEntries(Object.entries(item).sort(([left], [right]) => left.localeCompare(right))) : item);
}
function creation<T>(reply: RemoteResult<T>): T {
  if (!reply.ok && reply.error.code === 'reactive/creation-incomplete') {
    const details = z.object({ reference: z.object({ environmentId: z.string(), workspaceId: z.string(), sessionId: z.string() }) }).parse(reply.error.details);
    throw new CreationIncompleteError(details.reference, reply.error);
  }
  return result(reply);
}

const sessionConnections = new WeakMap<object, ConnectionHandle>();

/** 核验SDK对象的真实连接归属；身份由构造入口签发，不接受同形对象或URL推断。 */
export function assertProgramSessionConnection<P extends ProgramContract>(session: RemoteProgramSession<P>, connection: ConnectionHandle | undefined): void {
  if (sessionConnections.get(session) !== connection || connection === undefined)
    throw new Error('REACTIVE_REMOTE_SESSION_CONNECTION_MISMATCH');
}

/** 同一纯 Program 合同声明输入与公开出口；构造客户端不携带宿主组件或业务执行。 */
export async function createProgramClient<const P extends SharedProgramContract>(program: P, options: ProgramClientOptions): Promise<ProgramClient<P>> {
  return createParticipantProgramClient(program, options, 'human');
}

/**
 * 服务权限由宿主principal签发。正式rc.3的流恢复复用answer连接，
 * transport的interaction声明不赋予此服务端口人工回答或Goal控制能力。
 */
export async function createServiceProgramClient<const P extends SharedProgramContract>(program: P, options: ProgramClientOptions): Promise<ServiceProgramClient<P>> {
  return createParticipantProgramClient(program, options, 'service');
}

async function createParticipantProgramClient<const P extends SharedProgramContract, Participant extends ProgramIdentity['participant']>(
  program: P, options: ProgramClientOptions, participant: Participant,
): Promise<ParticipantClient<P, Participant>> {
  assertSharedProgramDefinition(program);
  const ctx = new Context();
  let transport: AuthenticatedTransport | undefined;
  try {
    await ctx.plugin(Registry);
    const physical = transport = createAuthenticatedTransport(options);
    ctx.effect(() => () => physical.dispose(), 'reactive authenticated transport');
    installConnection(ctx, { interactions: 'answer', transport, recovery: options.recovery });
    await ctx.plugin(Gateway);
    await ctx.remote.$mount(contribution);
    const client = await bindParticipantProgramClient(program, ctx, participant);
    let closing: Promise<void> | undefined;
    return { ...client, dispose() {
      if (closing) return closing;
      const completion = Promise.withResolvers<void>();
      closing = completion.promise;
      void (async () => {
        const failures = new Set<unknown>();
        try { await client.dispose(); } catch (error) { failures.add(error); }
        try { await ctx.fiber.dispose(); } catch (error) { failures.add(error); }
        // Cordis自动卸载会记录并吸收effect失败；创建者仍join同一物理清理回执。
        try { await physical.dispose(); } catch (error) { failures.add(error); }
        if (failures.size === 1) throw [...failures][0];
        if (failures.size) throw new AggregateError([...failures], 'REACTIVE_PROGRAM_CLIENT_DISPOSAL_FAILED');
      })().then(completion.resolve, completion.reject);
      return closing;
    } };
  } catch (error) {
    const failures = new Set<unknown>([error]);
    try { await ctx.fiber.dispose(); } catch (cleanup) { failures.add(cleanup); }
    try { await transport?.dispose(); } catch (cleanup) { failures.add(cleanup); }
    if (failures.size === 1) throw error;
    throw new AggregateError([...failures], 'REACTIVE_PROGRAM_CLIENT_CONSTRUCTION_FAILED');
  }
}

/** 借用已安装Connection、Gateway和Program远程合同的客户端上下文；只释放本SDK的使用。 */
export async function bindProgramClient<const P extends SharedProgramContract>(program: P, parent: Context): Promise<ProgramClient<P>> {
  return bindParticipantProgramClient(program, parent, 'human');
}

/** 正式rc.3要求已有answer流恢复连接；此transport声明不改变宿主签发的服务身份。 */
export async function bindServiceProgramClient<const P extends SharedProgramContract>(program: P, parent: Context): Promise<ServiceProgramClient<P>> {
  return bindParticipantProgramClient(program, parent, 'service');
}

async function bindParticipantProgramClient<const P extends SharedProgramContract, Participant extends ProgramIdentity['participant']>(
  program: P, parent: Context, participant: Participant,
): Promise<ParticipantClient<P, Participant>> {
  assertSharedProgramDefinition(program);
  type S = ProgramSurface<P>;
  const sharedIdentity = programIdentity(program);
  const surface: S = program.surface;
  const connection: ConnectionHandle | undefined = parent.get('connection');
  if (!connection || !parent.get('remote') || !parent.get('remote.reactivePrograms')) throw new Error('REACTIVE_PROGRAM_CLIENT_CONTEXT_REQUIRED');
  if (connection.interactions !== 'answer') throw new Error('REACTIVE_PROGRAM_CLIENT_INTERACTIONS_REQUIRED');
  const lifetime = createClientScope(parent, 'reactive-program-client', new Error('REACTIVE_REMOTE_CLIENT_DISPOSED'));
  await lifetime.ready;
  try {
    const ctx = lifetime.context;
    // 已受理Invocation的Operation借用SDK寿命，不能由原Session句柄退出撤销。
    const invocationQueries = createInvocationOperationQueries(lifetime, sharedIdentity);
    const remote = ctx.remote.reactivePrograms;
    const active = (): void => {
      lifetime.assertActive();
      if (ctx.get('connection') !== connection) throw new Error('REACTIVE_REMOTE_CLIENT_CONNECTION_REPLACED');
    };
    active();
    const callRemote = <T>(call: (signal: AbortSignal) => Promise<T>, signal = lifetime.signal): Promise<T> => {
      active();
      return lifetime.run(call, signal);
    };

    const session = async (identity: ProgramIdentity): Promise<ParticipantSession<P, Participant>> => {
      assertProgramIdentity(sharedIdentity, identity.program);
      if (identity.participant !== participant) throw new ProgramParticipantMismatchError(participant, identity);
      identity = Object.freeze({ ...identity, program: sharedIdentity });
      const address = Object.freeze({ sessionId: identity.sessionId, program: sharedIdentity, accessId: identity.accessId });
      const lease = lifetime.acquire(() => createClientScope(ctx, 'reactive-program-session:' + identity.sessionId,
        new Error('REACTIVE_CONVERSATION_HANDLE_RELEASED'), lifetime), owner => owner.close(), 'reactive Program session');
      const owner = lease.value;
      try {
        await owner.ready;
        const scopeSignal = owner.signal;
        const slots = new Map<string, ResourceReader<unknown>>();
        owner.acquire(() => slots, slots => { slots.clear(); }, 'program-view-slots');
        const assertActive = (): void => { active(); scopeSignal.throwIfAborted(); };
        const sessionResult = <T>(reply: RemoteResult<T>): T => {
          if (!reply.ok) throw revokeConversationAccess(owner, reply.error);
          return reply.value;
        };
        const actionBindings = createActionBindings(owner, address);
        const operationQueries = createOperationQueries(owner, address);
        const journal = remoteJournal(owner, address);
        const accessSource = { kind: 'participant', participant: identity.participant, actorId: identity.actorId, entrypoint: 'operation-query' } as const;
        let progress: RemoteProgress<P> | undefined;
        let files: ReturnType<typeof remoteFiles> | undefined;
        let attachments: SessionAttachments | undefined;
        const progressOwner = (): RemoteProgress<P> => {
          assertActive(); return progress ??= connectProgress(owner, address, program);
        };
        const human = remoteHuman(owner, address);
        const viewSlot = (name: string): ResourceReader<unknown> => {
          assertActive();
          const existing = slots.get(name);
          if (existing) return existing;
          const slot = remoteView(owner, address, surface, name);
          slots.set(name, slot);
          return slot;
        };
        for (const name of Object.keys(surface.views)) viewSlot(name);
        // 每个槽来自原 Surface，并在交付前由同一 schema decode；动态映射在此恢复泛型。
        const views = Object.freeze(Object.fromEntries([...slots].map(([name, slot]) => [name, slot.values$]))) as SurfaceViews<S>;
        const resolveOperation = async (name: string, options: CancellationOptions = {}): Promise<BoundOperation<OperationContract>> => {
            assertActive();
            const definition = surfaceSlotDefinition(surface, 'operations', name);
            if (!definition) throw new Error('REACTIVE_OPERATION_SLOT_UNKNOWN');
            const cancelled = options.signal;
            const signal = cancelled ? AbortSignal.any([scopeSignal, cancelled]) : scopeSignal;
            signal.throwIfAborted();
            const binding = sessionResult(await remote.resolveOperation({ ...address, name }, signal));
            signal.throwIfAborted();
            if (binding.operation.kind !== definition.kind || binding.operation.version !== definition.version) {
              throw new Error('REACTIVE_OPERATION_SLOT_CONTRACT_MISMATCH');
            }
            return bindRemoteOperation(operationQueries, definition, binding.command as OperationCommand<typeof definition>, accessSource, signal);
          };
        const operations = Object.freeze(Object.fromEntries(Object.keys(surface.operations).map(name => [name,
          (options: CancellationOptions = {}) => resolveOperation(name, options),
        ]))) as RemoteOperations<S>;
        const resolveActionAt = async <D extends ActionContract>(name: string, contract: D, expectedScope: z.input<D['scope']>,
          options: CancellationOptions = {}): Promise<RemoteResolvedAction<D>> => {
          assertActive();
          if (!Object.is(surfaceSlotDefinition(surface, 'actions', name), contract)) throw new Error('REACTIVE_SURFACE_ACTION_CONTRACT_MISMATCH');
          const captured = await actionBindings.resolve(name, actionIdentity(contract), json(expectedScope), options);
          const binding = captured.descriptor;
          // 宿主已经用该合同解析原始目标并核对真实绑定。scope 是其 JSON 输出，不能再次运行单向 transform。
          const invoke = async (input: ActionInput<D>, options: CancellationOptions = {}): Promise<ActionInvocation<D>> => {
            assertActive(); captured.assertActive();
            const signal = options.signal;
            const starting = signal ? AbortSignal.any([lifetime.signal, signal]) : lifetime.signal;
            starting.throwIfAborted();
            // input 本来就是 z.input；客户端仅核验 JSON，唯一业务解析仍在 Host。
            const capturedInput = json(input);
            starting.throwIfAborted();
            const invocationId = globalThis.crypto.randomUUID();
            let started: Awaited<ReturnType<typeof remote.startAction>>;
            try {
              started = await remote.startAction({ ...address, name, contract: binding.contract, bindingId: binding.bindingId,
                invocationId, input: capturedInput }, starting);
            } catch (cause) { throw new ActionAdmissionUnknownError(identity.sessionId, name, invocationId, cause); }
            if (!started.ok) {
              if (started.error.code === 'gateway/internal' || started.error.code === 'gateway/cancelled')
                throw new ActionAdmissionUnknownError(identity.sessionId, name, invocationId, started.error);
              throw revokeConversationAccess(owner, started.error);
            }
            // ACK 已可能受理业务；矛盾回执不能被描述成确定未执行，也不能自动重发。
            let origin: ActionInvocationOrigin;
            try {
              if (started.value.id !== invocationId) throw new Error('REACTIVE_ACTION_INVOCATION_ID_MISMATCH');
              if (started.value.name !== name) throw new Error('REACTIVE_ACTION_INVOCATION_SLOT_MISMATCH');
              origin = actionInvocationOriginSchema.parse(started.value.origin);
              assertActionIdentity(contract, origin.action);
              if (origin.sessionId !== identity.sessionId) throw new Error('REACTIVE_ACTION_INVOCATION_SESSION_MISMATCH');
              if (origin.bindingId !== binding.bindingId) throw new Error('REACTIVE_ACTION_INVOCATION_BINDING_MISMATCH');
              if (jsonIdentity(origin.scope) !== jsonIdentity(binding.scope)) throw new Error('REACTIVE_ACTION_INVOCATION_TARGET_MISMATCH');
            } catch (error) { throw new ActionAdmissionUnknownError(identity.sessionId, name, invocationId, error); }
            return invocation(ctx.remote, identity.sessionId, { ...started.value, origin }, contract, lifetime, invocationQueries, callRemote);
          };
          // schema 输出只在明确合同的传输边界恢复类型；不要求 scope 支持反向 encode。
          return Object.freeze(Object.assign(invoke, { definition: contract, bindingId: binding.bindingId,
            scope: binding.scope as ActionScope<D>, ...captured.eligibility ? { eligibility: captured.eligibility } : {} }));
        };
        const invocationAt = async <D extends ActionContract>(name: string, contract: D, id: string,
          options: CancellationOptions = {}): Promise<ActionInvocation<D>> => {
          assertActive();
          if (!Object.is(surfaceSlotDefinition(surface, 'actions', name), contract)) throw new Error('REACTIVE_SURFACE_ACTION_CONTRACT_MISMATCH');
          const stored = result(await callRemote(signal => remote.actionInvocation({ sessionId: identity.sessionId, program: sharedIdentity,
            invocationId: id, name, contract: actionIdentity(contract) }, signal), options.signal ?? lifetime.signal));
          if (stored.id !== id) throw new Error('REACTIVE_ACTION_INVOCATION_ID_MISMATCH');
          if (stored.name !== name) throw new Error('REACTIVE_ACTION_INVOCATION_SLOT_MISMATCH');
          const origin = actionInvocationOriginSchema.parse(stored.origin);
          assertActionIdentity(contract, origin.action);
          if (origin.sessionId !== identity.sessionId) throw new Error('REACTIVE_ACTION_INVOCATION_SESSION_MISMATCH');
          return invocation(ctx.remote, identity.sessionId, { ...stored, origin }, contract, lifetime, invocationQueries, callRemote);
        };
        const handle: RemoteSession<P> = {
          get definition() { assertActive(); return program; },
          id: identity.sessionId, identity, surface, views, operations,
          get actionManifest() { return viewSlot(actionManifestSlot) as ResourceReader<readonly ToolActionBinding[]>; },
          get actionManifest$() { return this.actionManifest.values$; },
          collection(name) {
            assertActive();
            return createSurfaceCollectionClient(surface, name, { asynchronous: true,
              viewResource: address => viewSlot(address),
              resolveAction: resolveActionAt,
              invocation: invocationAt,
              resolveOperation: (address, _definition, options) => resolveOperation(address, options),
            });
          },
          module(mount) {
            assertActive();
            return createSurfaceModuleClient(surface, mount, { asynchronous: true,
              viewResource: address => viewSlot(address),
              resolveAction: resolveActionAt,
              invocation: invocationAt,
              resolveOperation: (address, _definition, options) => resolveOperation(address, options),
            });
          },
          async resolveAction<K extends keyof S['actions'] & string>(name: K, expectedScope: z.input<S['actions'][K]['scope']>,
            options: CancellationOptions = {}): Promise<RemoteResolvedAction<S['actions'][K]>> {
            assertActive();
            const contract = surfaceSlotDefinition(surface, 'actions', name);
            if (!contract) throw new Error('REACTIVE_ACTION_SLOT_UNKNOWN');
            return resolveActionAt(name, contract, expectedScope, options);
          },
          get progress() { return progressOwner().progress; },
          get attachments() { assertActive(); return attachments ??= remoteAttachments(owner, address); },
          get files() { assertActive(); return files ??= remoteFiles(owner, address); },
          get journal() { assertActive(); return journal; },
          get progress$() { return progressOwner().progress$; },
          get progressFrames$() { return progressOwner().progressFrames$; },
          get events$() { return progressOwner().events$; },
          get rawEvents$() { return progressOwner().rawEvents$; },
          request: requestId => progressOwner().request(requestId),
          async invocation<K extends keyof S['actions'] & string>(name: K, id: string, options: CancellationOptions = {}): Promise<ActionInvocation<S['actions'][K]>> {
            assertActive();
            const contract = surfaceSlotDefinition(surface, 'actions', name);
            if (!contract) throw new Error('REACTIVE_ACTION_SLOT_UNKNOWN');
            return invocationAt(name, contract, id, options);
          },
          human,
          human$: human.observations$,
          questions$: human.values$.pipe(map(value => value.questions)),
          foregroundQuestions$: human.values$.pipe(map(value => value.foreground)),
          observeView(name) {
            assertActive();
            return handle.viewResource(name).observations$;
          },
          viewResource(name) { return viewSlot(name) as ResourceReader<ViewValue<S['views'][typeof name]>>; },
          readView(name, options = {}) { return handle.viewResource(name).read(options.signal); },
          async submit(input, options = {}): Promise<ProgramInputReceipt> {
            assertActive();
            const cancelled = options.signal;
            // Session退出只结束本消费者等待；已启动提交仍使用SDK与caller的原HTTP signal。
            const signal = cancelled ? AbortSignal.any([lifetime.signal, cancelled]) : lifetime.signal;
            const waiting = cancelled ? AbortSignal.any([scopeSignal, cancelled]) : scopeSignal;
            signal.throwIfAborted();
            waiting.throwIfAborted();
            json(input);
            const submitted: ProgramSubmission = input.intent === 'result'
              ? { ...input, scope: json(input.scope), result: { ...input.result,
                continuation: { ...input.result.continuation, inputs: input.result.continuation.inputs.map(reference => ({ ...reference, scope: json(reference.scope) })) },
                wait: { ...input.result.wait, operation: json(input.result.wait.operation) } } }
              : { ...input, scope: json(input.scope) };
            let started = false;
            const reply = await owner.run(() => {
              signal.throwIfAborted();
              started = true;
              return remote.submit({ ...address, submission: submitted }, signal);
            }, waiting).catch(error => {
              if (started && waiting.aborted && error === waiting.reason)
                throw new SubmissionAdmissionUnknownError(identity.sessionId, submitted.requestId, error);
              throw error;
            });
            if (!reply.ok && (reply.error.code === 'gateway/internal' || reply.error.code === 'gateway/cancelled')) {
              throw new SubmissionAdmissionUnknownError(identity.sessionId, submitted.requestId, reply.error);
            }
            return sessionResult(reply);
          },
          async editQueuedInput(requestId, content, options = {}): Promise<ProgramInputReceipt> {
            assertActive();
            const cancelled = options.signal;
            const signal = cancelled ? AbortSignal.any([lifetime.signal, cancelled]) : lifetime.signal;
            const waiting = cancelled ? AbortSignal.any([scopeSignal, cancelled]) : scopeSignal;
            signal.throwIfAborted();
            waiting.throwIfAborted();
            let started = false;
            const reply = await owner.run(() => {
              signal.throwIfAborted();
              started = true;
              return remote.editQueuedInput({ ...address, requestId, content }, signal);
            }, waiting).catch(error => {
              if (started && waiting.aborted && error === waiting.reason)
                throw new InputEditAdmissionUnknownError(identity.sessionId, requestId, error);
              throw error;
            });
            if (!reply.ok && (reply.error.code === 'gateway/internal' || reply.error.code === 'gateway/cancelled'))
              throw new InputEditAdmissionUnknownError(identity.sessionId, requestId, reply.error);
            return sessionResult(reply);
          },
          uploadFile(data, options = {}): FileUploadInvocation {
            assertActive();
            const cancelled = options.signal;
            const preparationSignal = cancelled ? AbortSignal.any([scopeSignal, cancelled]) : scopeSignal;
            preparationSignal.throwIfAborted();
            const name = options.name;
            const source = data instanceof Uint8Array ? new Uint8Array(data) : data;
            const descriptor = contribution.methods['reactivePrograms/uploadFile'];
            return createFileUploadInvocation({
              sessionId: identity.sessionId, preparationSignal, waitSignal: preparationSignal,
              async prepare() {
                const bytes = source instanceof Uint8Array ? source : new Uint8Array(await source.arrayBuffer()).slice();
                return descriptor.input({ strict: true }).create().parse({ request: {
                  ...address, data: bytesToBase64(bytes), ...(name === undefined ? {} : { name }),
                } });
              },
              async execute(input) {
                // 沿原SDK依赖准入；已派发的HTTP只归SDK/Connection，不再随Session或caller退等取消。
                active();
                preparationSignal.throwIfAborted();
                // Typert unary会把transport throw与明确Host internal折成同一RemoteResult。
                // 公开RPC保留此区别，参数与成功值仍使用同一生成贡献的正式codec。
                const reply = await connection.rpc.call('/api', descriptor.namespace + '/' + descriptor.method,
                  { args: input }, lifetime.signal).catch((cause: unknown) => {
                  throw new FileUploadReceiptUnknownError(identity.sessionId, cause);
                });
                if (!reply.ok) throw revokeConversationAccess(owner, decodeProgramFailure(reply.error));
                try { return descriptor.result.create().parse(reply.value); }
                catch (cause) { throw new FileUploadReceiptUnknownError(identity.sessionId, cause); }
              },
            });
          },
          async requestStatus(requestId, options = {}) {
            assertActive();
            const signal = options.signal ? AbortSignal.any([scopeSignal, options.signal]) : scopeSignal;
            return sessionResult(await callRemote(current => remote.requestStatus({ ...address, requestId }, current), signal));
          },
          attachOperation(definition, reference, options = {}) {
            assertActive();
            const signal = options.signal;
            return attachRemoteOperation(operationQueries, definition, reference, accessSource,
              signal ? AbortSignal.any([scopeSignal, signal]) : scopeSignal);
          },
          locateOperation(definition, command, options = {}) {
            assertActive();
            const signal = options.signal;
            return locateRemoteOperation(operationQueries, definition, command, accessSource,
              signal ? AbortSignal.any([scopeSignal, signal]) : scopeSignal);
          },
          readOperationFeedback(definition, command, participationId, options = {}) {
            const signal = options.signal;
            assertActive(); return readRemoteOperationFeedback(operationQueries, definition, command, participationId,
              signal ? AbortSignal.any([scopeSignal, signal]) : scopeSignal);
          },
          async notifyOperation(followId, options = {}) {
            assertActive();
            const cancelled = options.signal;
            const signal = cancelled ? AbortSignal.any([scopeSignal, cancelled]) : scopeSignal;
            signal.throwIfAborted();
            return sessionResult(await remote.notifyOperation({ ...address, followId }, signal));
          },
          async openEvents(eventOptions, request = {}, cancellation = {}) {
            assertActive();
            return ProgramJournal.create(owner, journal, eventOptions, request, cancellation);
          },
          release(): Promise<void> {
            return lease.release();
          },
        };
        if (participant === 'human') {
          const waits = createHumanWaits(owner, address);
          const controls: Pick<RemoteProgramSession<P>, 'question' | 'answerQuestion' | 'attachWait' | 'pause' | 'cancel'> = {
            question: createHumanQuestionResolver(human, {
              answerQuestion: (id, answer, options) => controls.answerQuestion(id, answer, options),
              attachWait: (id, options) => controls.attachWait(id, options),
            }, { signal: scopeSignal }),
            async pause(options = {}) {
              assertActive();
              const signal = options.signal ? AbortSignal.any([scopeSignal, options.signal]) : scopeSignal;
              return sessionResult(await callRemote(current => remote.pause(address, current), signal));
            },
            async cancel(options = {}) {
              assertActive();
              const signal = options.signal ? AbortSignal.any([scopeSignal, options.signal]) : scopeSignal;
              return sessionResult(await callRemote(current => remote.cancel(address, current), signal));
            },
            async answerQuestion(callId, answer, options = {}) {
              assertActive();
              const signal = options.signal ? AbortSignal.any([scopeSignal, options.signal]) : scopeSignal;
              signal.throwIfAborted();
              return sessionResult(await callRemote(current => remote.answerQuestion({ ...address, callId, answer }, current), signal));
            },
            async *attachWait(callId, options = {}) {
              assertActive();
              yield* waits.attach(callId, options);
            },
          };
          Object.assign(handle, controls);
        }
        sessionConnections.set(handle, connection);
        // 参与模式已核宿主身份，人工能力也只在对应分支实际安装。
        assertActive(); return handle as ParticipantSession<P, Participant>;
      } catch (error) {
        try { await lease.release(); }
        catch (cleanup) { throw new AggregateError([error, cleanup], 'REACTIVE_PROGRAM_SESSION_STARTUP_FAILED'); }
        throw error;
      }
    };
    const client: ProgramClientBase<P, ParticipantSession<P, Participant>> = {
      async createWorkspace(registration) {
        return result(await callRemote(signal => remote.createWorkspace(registration, signal)));
      },
      async create(environmentId, options) {
        active();
        const reference = { environmentId, workspaceId: options.workspaceId, sessionId: options.sessionId ?? crypto.randomUUID() };
        let dispatched = false;
        try {
          const reply = await callRemote(signal => {
            dispatched = true;
            return remote.create({ ...reference, program: sharedIdentity }, signal);
          });
          if (!reply.ok && (reply.error.code === 'gateway/internal' || reply.error.code === 'gateway/cancelled')) throw new CreationUnknownError(reference, reply.error);
          const identity = creation(reply);
          try { assertProgramIdentity(sharedIdentity, identity.program); }
          catch (cause) { throw new CreationUnknownError(reference, cause); }
          active(); return await session(identity);
        } catch (error) {
          if (dispatched && lifetime.signal.aborted && error === lifetime.signal.reason) throw new CreationUnknownError(reference, error);
          throw error;
        }
      },
      async completeCreation(reference) {
        const captured = Object.freeze({ environmentId: reference.environmentId, workspaceId: reference.workspaceId, sessionId: reference.sessionId });
        const identity = creation(await callRemote(signal => remote.completeCreation({ ...captured, program: sharedIdentity }, signal)));
        try { assertProgramIdentity(sharedIdentity, identity.program); }
        catch (cause) { throw new CreationUnknownError(captured, cause); }
        active(); return session(identity);
      },
      async open(sessionId) {
        const identity = creation(await callRemote(signal => remote.open({ sessionId, program: sharedIdentity }, signal))); active(); return session(identity);
      },
      async resume(sessionId) {
        const identity = creation(await callRemote(signal => remote.resume({ sessionId, program: sharedIdentity }, signal))); active(); return session(identity);
      },
      async describe() { return result(await callRemote(signal => remote.describe(signal))); },
      dispose: () => lifetime.close(),
    };
    return client as ParticipantClient<P, Participant>;
  } catch (error) {
    try { await lifetime.close(); }
    catch (cleanup) { throw new AggregateError([error, cleanup], 'REACTIVE_PROGRAM_CLIENT_STARTUP_FAILED'); }
    throw error;
  }
}

function invocation<D extends ActionContract>(remote: ClientRemote, sessionId: string,
  start: Omit<ProgramActionStart, 'origin'> & { readonly origin: ActionInvocationOrigin }, contract: D, lifetime: ClientScope,
  queries: RemoteOperationObservation, callRemote: <T>(call: (signal: AbortSignal) => Promise<T>, signal?: AbortSignal) => Promise<T>): ActionInvocation<D> {
  const origin = start.origin;
  assertActionIdentity(contract, origin.action);
  if (origin.sessionId !== sessionId) throw new Error('REACTIVE_ACTION_INVOCATION_SESSION_MISMATCH');
  const signal = lifetime.signal;
  const activityState = new BehaviorSubject<Observation<InvocationActivity>>({ status: 'pending', generation: 0 });
  const ongoing = (observation: Observation<InvocationActivity>): boolean => observation.status === 'pending'
    || (observation.status === 'ready' && (observation.value.state === 'pending' || observation.value.state === 'running'));
  const activity = new ResourceReader<InvocationActivity>({ peek: () => activityState.value,
    observations$: activityState.pipe(takeWhile(ongoing, true)),
  }, { signal });
  const address = { sessionId, program: queries.program, invocationId: start.id };
  // 受理ACK是业务事实；观察owner已退出也保留原id，调用者可在另一SDK恢复它。
  if (lifetime.active) {
    const watch = lifetime.acquire(() => (async (): Promise<void> => {
      try {
        for await (const item of remote.$stream<ProgramInvocationFrame>({ name: 'program-invocation:' + start.id,
          open: physicalSignal => remote.reactivePrograms.observeAction(address, AbortSignal.any([signal, physicalSignal])),
          ended: () => new Error('REACTIVE_REMOTE_INVOCATION_OBSERVATION_ENDED'),
          carrierFailed: () => {
            if (!signal.aborted && ongoing(activityState.value))
              activityState.next({ status: 'pending', generation: activityState.value.generation + 1 });
          },
        })) {
          item.accept();
          const value = item.value;
          const decoded = value.state === 'failed' ? { ...value, error: decodeProgramFailure(value.error) }
            : value.state === 'returned' ? { ...value, ...(value.observationError ? { observationError: decodeProgramFailure(value.observationError) } : {}) } : value;
          activityState.next({ status: 'ready', generation: activityState.value.generation + 1, observedAt: Date.now(), value: decoded });
          if (value.state === 'returned' || value.state === 'failed') return;
        }
      } catch (error) {
        if (!signal.aborted) {
          if (ongoing(activityState.value)) activityState.next({ status: 'error', generation: activityState.value.generation + 1, error });
          else console.error('reactive.invocation.observation.cleanup.failed', error);
        }
      }
    })(), watch => watch, 'program-invocation:' + start.id);
    void watch.value.then(() => watch.release(), () => watch.release()).catch(() => {});
  }
  const settled = Promise.resolve().then(() => callRemote(signal => remote.reactivePrograms.actionResult(address, signal)))
    .then(reply => z.decode(contract.output, result(reply)));
  void settled.catch(() => {});
  const mapping = contract.operation;
  const operation = mapping ? settled.then(receipt => attachInvocationRemoteOperation(queries, address, mapping.definition,
    z.json().parse(mapping.reference(receipt as never)), origin.source, signal)) : undefined;
  void operation?.catch(() => {});
  // 动态 Surface 映射经对应原合同 decode；operation 也只在原合同声明时构造。
  const handle: ActionInvocationHandle = Object.freeze({ id: start.id, origin, source: origin.source, result: settled, activity, activity$: activity.values$,
    wait(options: CancellationOptions = {}) {
      const waiting = options.signal ? AbortSignal.any([signal, options.signal]) : signal;
      return firstValueFrom(abortable(() => settled, waiting));
    },
    ...(operation ? { operation } : {}) });
  return handle as ActionInvocation<D>;
}
