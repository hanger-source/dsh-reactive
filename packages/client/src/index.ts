import { Context } from '@deepseek-ai/cordis';
import * as Registry from '@deepseek-ai/dsh-typert-registry/client';
import * as Gateway from '@deepseek-ai/dsh-api-gateway/client';
import { RemoteSnapshotStream, type ClientRemote } from '@deepseek-ai/dsh-api-gateway/client';
import { installConnection, type ConnectionHandle, type ConnectionInstallOptions } from '@deepseek-ai/dsh-client-connection/client';
import { type RemoteResult } from '@deepseek-ai/dsh-typert-protocol';
import type { JsonValue } from '@deepseek-ai/dsh-util-values';
import { bytesToBase64 } from '@deepseek-ai/dsh-util-crypto';
import { BehaviorSubject, defer, firstValueFrom, from, fromEvent, map, mergeMap, raceWith, takeWhile, throwError, type Observable } from 'rxjs';
import { z, assertProgramDefinition, surfaceSlotDefinition, createSurfaceCollectionClient, actionManifestSlot, type ToolActionBinding, type SurfaceCollectionClient, type ActionInput, type ActionInvocation, type ActionContract, type ActionScope, type Invocation, type InvocationActivity, type Observation, type Submission, type Surface, type SurfaceViews, type ViewValue, type OperationContract, type OperationReference, type OperationCommand, type OperationHandle, type OperationLocation, type OperationState, type HumanInputState, type HumanForegroundQuestion, type HumanAnswer, type HumanWaitFrame, type BoundOperation, type OperationFeedbackRead, type OperationFeedbackPayload, type ConversationProgressSnapshot, type ConversationEvent, type RequestHandle, type AssistantProgressFrame, type ProgramContract, type ProgramInput, type ProgramParsedInput, type ProgramSurface, type WorkspaceRegistration, type ConversationCreateOptions, type FileUploadSource, type FileUploadValue, type CancellationOptions } from '@hanger-source/dsh-reactive';
import { decodeProgramFailure } from './errors.ts';
import './errors.ts';
import contribution from '@hanger-source/dsh-reactive-client/remote';
import type { ProgramActionStart, ProgramCreationReference, ProgramIdentity, ProgramInputReceipt, ProgramInvocationFrame, ProgramQuestions, ProgramOperationNotificationResult, ProgramRequestStatus, ProgramWorkspace, ProgramGoalControl, ProgramCancelResult, ProgramSubmission } from './types.ts';
import { createAuthenticatedTransport, type AuthenticatedTransportOptions } from './transport.ts';
import { attachRemoteOperation, locateRemoteOperation, bindRemoteOperation, readRemoteOperationFeedback } from './operations.ts';
import { ProgramJournal, type ProgramJournalOptions, type ProgramJournalRequest } from './journal.ts';
import { connectProgress, type RemoteProgress } from './progress.ts';
import { createActionBindings } from './action-bindings.ts';
import { remoteView } from './views.ts';
import { remoteHuman } from './human.ts';
import type { RemoteResourceConnection } from './resource.ts';
import { ResourceReader } from '@hanger-source/dsh-reactive';
import { actionIdentity, assertActionIdentity } from '@hanger-source/dsh-reactive';
import { createHumanQuestionResolver, type HumanQuestionHandle } from '@hanger-source/dsh-reactive';

export { createAuthenticatedTransport } from './transport.ts';
export type { AuthenticatedTransportOptions } from './transport.ts';
export { ProgramJournal } from './journal.ts';
export type { ProgramJournalOptions, ProgramJournalRequest } from './journal.ts';

/** 明确目标的已捕获绑定；调用时仍核验当前声明及真实参与者权限。 */
export interface RemoteResolvedAction<D extends ActionContract> {
  (input: ActionInput<D>, options?: CancellationOptions): Promise<ActionInvocation<D>>;
  readonly definition: D;
  readonly bindingId: string;
  readonly scope: ActionScope<D>;
}
/** resolver 只捕获当前公开的只读绑定，网络边界允许异步，不受理业务或登记等待。 */
export type RemoteOperations<S extends Surface> = {
  readonly [K in keyof S['operations']]: (options?: CancellationOptions) => Promise<BoundOperation<S['operations'][K]>>;
};
export interface ProgramClientOptions extends AuthenticatedTransportOptions {
  readonly recovery?: ConnectionInstallOptions['recovery'];
}
export interface RemoteUploadFileOptions extends CancellationOptions { readonly name?: string; }
export interface RemoteProgramSession<P extends ProgramContract> {
  readonly id: string;
  /** 实际创建这个 SDK 的纯合同；UI 或模块组合不得只按相同 sessionId 猜合同。 */
  readonly definition: P;
  readonly identity: ProgramIdentity;
  readonly views: SurfaceViews<ProgramSurface<P>>;
  viewResource<K extends keyof ProgramSurface<P>['views'] & string>(name: K): ResourceReader<ViewValue<ProgramSurface<P>['views'][K]>>;
  readonly actionManifest$: Observable<readonly ToolActionBinding[]>;
  collection<K extends keyof ProgramSurface<P>['collections'] & string>(name: K): SurfaceCollectionClient<ProgramSurface<P>['collections'][K], CancellationOptions, true>;
  resolveAction<K extends keyof ProgramSurface<P>['actions'] & string>(name: K,
    expectedScope: z.input<ProgramSurface<P>['actions'][K]['scope']>, options?: CancellationOptions): Promise<RemoteResolvedAction<ProgramSurface<P>['actions'][K]>>;
  readonly operations: RemoteOperations<ProgramSurface<P>>;
  readonly progress: ResourceReader<ConversationProgressSnapshot>;
  readonly progress$: Observable<Observation<ConversationProgressSnapshot>>;
  readonly progressFrames$: Observable<AssistantProgressFrame>;
  readonly events$: Observable<ConversationEvent<ProgramParsedInput<P>>>;
  request(requestId: string): RequestHandle<ProgramParsedInput<P>>;
  invocation<K extends keyof ProgramSurface<P>['actions'] & string>(name: K, id: string): Promise<ActionInvocation<ProgramSurface<P>['actions'][K]>>;
  readonly human$: Observable<Observation<ProgramQuestions>>;
  readonly human: ResourceReader<ProgramQuestions>;
  question(callId: string, options?: CancellationOptions): Promise<HumanQuestionHandle>;
  readonly questions$: Observable<HumanInputState>;
  readonly foregroundQuestions$: Observable<readonly HumanForegroundQuestion[]>;
  observeView<K extends keyof ProgramSurface<P>['views'] & string>(name: K): Observable<Observation<ViewValue<ProgramSurface<P>['views'][K]>>>;
  readView<K extends keyof ProgramSurface<P>['views'] & string>(name: K, options?: CancellationOptions): Promise<ViewValue<ProgramSurface<P>['views'][K]>>;
  submit(input: Submission<ProgramInput<P>>, options?: CancellationOptions): Promise<ProgramInputReceipt>;
  editQueuedInput(requestId: string, content: Submission['content'], options?: CancellationOptions): Promise<ProgramInputReceipt>;
  uploadFile(data: FileUploadSource, options?: RemoteUploadFileOptions): Promise<FileUploadValue>;
  /** 等待归本句柄；退出结束等待，已进入原生暂停的控制仍由Host结算。 */
  pause(): Promise<ProgramGoalControl>;
  /** 取消原生处理；退出只结束回执等待，不回滚已经进入的控制。 */
  cancel(): Promise<ProgramCancelResult>;
  /** 只读原请求事实，句柄或SDK退出立即结束尚未返回的查询。 */
  requestStatus(requestId: string): Promise<ProgramRequestStatus>;
  answerQuestion(callId: string, answer: HumanAnswer, options?: CancellationOptions): Promise<boolean>;
  attachWait(callId: string, options?: CancellationOptions): AsyncIterable<HumanWaitFrame>;
  attachOperation<D extends OperationContract>(definition: D, reference: OperationReference<D>, options?: CancellationOptions): OperationHandle<D>;
  locateOperation<D extends OperationContract>(definition: D, command: OperationCommand<D>, options?: CancellationOptions): Promise<OperationLocation<OperationReference<D>, OperationState<D>>>;
  readOperationFeedback<D extends OperationContract>(definition: D, command: OperationCommand<D>, participationId: string,
    options?: CancellationOptions): Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>>;
  notifyOperation(followId: string, options?: CancellationOptions): Promise<ProgramOperationNotificationResult>;
  /** 原生可靠 journal；调用者消费完整 change，不另造事件 cursor 或重放引擎。 */
  openEvents(options: ProgramJournalOptions, request?: ProgramJournalRequest): Promise<ProgramJournal>;
  release(): Promise<void>;
}
export interface ProgramClient<P extends ProgramContract> {
  createWorkspace(registration: WorkspaceRegistration): Promise<ProgramWorkspace>;
  create(environmentId: string, options: ConversationCreateOptions): Promise<RemoteProgramSession<P>>;
  completeCreation(reference: ProgramCreationReference): Promise<RemoteProgramSession<P>>;
  open(sessionId: string): Promise<RemoteProgramSession<P>>;
  resume(sessionId: string): Promise<RemoteProgramSession<P>>;
  describe(): Promise<readonly ProgramWorkspace[]>;
  dispose(): Promise<void>;
}

/** 接受状态未被确认；此错误从不触发自动重发 Action。 */
export class ActionAdmissionUnknownError extends Error {
  constructor(readonly sessionId: string, readonly name: string, cause: unknown) { super('REACTIVE_ACTION_ADMISSION_UNKNOWN', { cause }); }
}
/** 创建结果未知时保留准确引用，调用者可显式恢复，不悄悄创建另一会话。 */
export class CreationUnknownError extends Error {
  constructor(readonly reference: ProgramCreationReference, cause: unknown) { super('REACTIVE_CREATION_UNKNOWN', { cause }); }
}
/** 宿主明确报告已部分持久化；沿同一引用完成登记，不归类为未知受理。 */
export class CreationIncompleteError extends Error {
  constructor(readonly reference: ProgramCreationReference, cause: unknown) { super('REACTIVE_CREATION_INCOMPLETE', { cause }); }
}
/** HTTP 回执未证明受理结果；保留原请求地址供只读核对和应用显式确定性重投。 */
export class SubmissionAdmissionUnknownError extends Error {
  constructor(readonly sessionId: string, readonly requestId: string, cause: unknown) { super('REACTIVE_SUBMISSION_ADMISSION_UNKNOWN', { cause }); }
}
/** 编辑可能已经持久生效；保留同一请求身份供显式读取核对，不自动重投。 */
export class InputEditAdmissionUnknownError extends Error {
  constructor(readonly sessionId: string, readonly requestId: string, cause: unknown) { super('REACTIVE_INPUT_EDIT_ADMISSION_UNKNOWN', { cause }); }
}

function result<T>(value: RemoteResult<T>): T { if (!value.ok) throw value.error; return value.value; }
function json(value: unknown): JsonValue { return z.json().parse(value); }
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
export async function createProgramClient<const P extends ProgramContract>(program: P, options: ProgramClientOptions): Promise<ProgramClient<P>> {
  assertProgramDefinition(program);
  const ctx = new Context();
  try {
    await ctx.plugin(Registry);
    installConnection(ctx, { interactions: 'answer', transport: createAuthenticatedTransport(options), recovery: options.recovery });
    await ctx.plugin(Gateway);
    await ctx.remote.$mount(contribution);
    const client = bindProgramClient(program, ctx);
    return { ...client, async dispose() { await client.dispose(); await ctx.fiber.dispose(); } };
  } catch (error) { await ctx.fiber.dispose(); throw error; }
}

/** 借用已安装Connection、Gateway和Program远程合同的客户端上下文；只释放本SDK的使用。 */
export function bindProgramClient<const P extends ProgramContract>(program: P, ctx: Context): ProgramClient<P> {
  assertProgramDefinition(program);
  type S = ProgramSurface<P>;
  const surface: S = program.surface;
  const connection = ctx.get('connection');
  const gateway = ctx.get('remote');
  if (!connection || !gateway?.reactivePrograms) throw new Error('REACTIVE_PROGRAM_CLIENT_CONTEXT_REQUIRED');
  if (connection.interactions !== 'answer') throw new Error('REACTIVE_PROGRAM_CLIENT_INTERACTIONS_REQUIRED');
  const lifetime = new AbortController();
  const sessions = new Set<RemoteProgramSession<P>>();
  const observations = new Set<AbortController>();
  let closing: Promise<void> | undefined;
  const close = (): Promise<void> => {
    lifetime.abort(new Error('REACTIVE_REMOTE_CLIENT_DISPOSED'));
    for (const observation of observations) observation.abort();
    return closing ??= Promise.all([...sessions].map(handle => handle.release())).then(() => {});
  };
  const releaseContext = ctx.effect(() => close, 'reactive Program client');
  const remote = gateway.reactivePrograms;
  const active = (): void => {
    lifetime.signal.throwIfAborted();
    if (ctx.get('connection') !== connection) throw new Error('REACTIVE_REMOTE_CLIENT_CONNECTION_REPLACED');
  };
  active();
  const callRemote = <T>(call: (signal: AbortSignal) => Promise<T>, signal = lifetime.signal): Promise<T> => {
    active();
    signal.throwIfAborted();
    return firstValueFrom(fromEvent(signal, 'abort').pipe(
      mergeMap(() => throwError(() => signal.reason)),
      raceWith(defer(() => from(call(signal)))),
    ));
  };

  const session = async (identity: ProgramIdentity): Promise<RemoteProgramSession<P>> => {
    const controller = new AbortController();
    const scopeSignal = AbortSignal.any([lifetime.signal, controller.signal]);
    const slots = new Map<string, RemoteResourceConnection<unknown>>();
    const assertActive = (): void => { active(); scopeSignal.throwIfAborted(); };
    const actionBindings = createActionBindings(ctx.remote, identity.sessionId, scopeSignal);
    const accessSource = { kind: 'participant', participant: 'human', actorId: identity.actorId, entrypoint: 'operation-query' } as const;
    const journals = new Set<ProgramJournal>();
    let progress: RemoteProgress<P> | undefined;
    const progressOwner = (): RemoteProgress<P> => {
      assertActive(); return progress ??= connectProgress(ctx, identity.sessionId, program, scopeSignal);
    };
    const humanConnection = remoteHuman(ctx, identity.sessionId, scopeSignal);
    const human = humanConnection.reader;
    const question = createHumanQuestionResolver(human, {
      answerQuestion: (id, answer, options) => handle.answerQuestion(id, answer, options),
      attachWait: (id, options) => handle.attachWait(id, options),
    }, { signal: scopeSignal });
    const viewSlot = (name: string): RemoteResourceConnection<unknown> => {
      assertActive();
      const existing = slots.get(name);
      if (existing) return existing;
      const slot = remoteView(ctx, identity.sessionId, surface, name, scopeSignal);
      slots.set(name, slot);
      return slot;
    };
    for (const name of Object.keys(surface.views)) viewSlot(name);
    // 每个槽来自原 Surface，并在交付前由同一 schema decode；动态映射在此恢复泛型。
    const views = Object.freeze(Object.fromEntries([...slots].map(([name, slot]) => [name, slot.reader.values$]))) as SurfaceViews<S>;
    const resolveOperation = async (name: string, options: CancellationOptions = {}): Promise<BoundOperation<OperationContract>> => {
        assertActive();
        const definition = surfaceSlotDefinition(surface, 'operations', name);
        if (!definition) throw new Error('REACTIVE_OPERATION_SLOT_UNKNOWN');
        const cancelled = options.signal;
        const signal = cancelled ? AbortSignal.any([scopeSignal, cancelled]) : scopeSignal;
        signal.throwIfAborted();
        const binding = result(await remote.resolveOperation({ sessionId: identity.sessionId, name }, signal));
        signal.throwIfAborted();
        if (binding.operation.kind !== definition.kind || binding.operation.version !== definition.version) {
          throw new Error('REACTIVE_OPERATION_SLOT_CONTRACT_MISMATCH');
        }
        return bindRemoteOperation(ctx.remote, identity.sessionId, definition, binding.command as OperationCommand<typeof definition>, accessSource, signal);
      };
    const operations = Object.freeze(Object.fromEntries(Object.keys(surface.operations).map(name => [name,
      (options: CancellationOptions = {}) => resolveOperation(name, options),
    ]))) as RemoteOperations<S>;
    let released: Promise<void> | undefined;
    const handle: RemoteProgramSession<P> = {
      get definition() { assertActive(); return program; },
      id: identity.sessionId, identity, views, operations,
      get actionManifest$() { return viewSlot(actionManifestSlot).reader.values$ as Observable<readonly ToolActionBinding[]>; },
      collection(name) {
        assertActive();
        const untyped = handle as RemoteProgramSession<ProgramContract>;
        return createSurfaceCollectionClient(surface, name, { asynchronous: true,
          viewResource: address => viewSlot(address).reader,
          resolveAction: (address, _definition, scope, options) => untyped.resolveAction(address, scope, options),
          resolveOperation: (address, _definition, options) => resolveOperation(address, options),
        });
      },
      async resolveAction<K extends keyof S['actions'] & string>(name: K, expectedScope: z.input<S['actions'][K]['scope']>,
        options: CancellationOptions = {}): Promise<RemoteResolvedAction<S['actions'][K]>> {
        assertActive();
        const contract = surfaceSlotDefinition(surface, 'actions', name) as S['actions'][K];
        if (!contract) throw new Error('REACTIVE_ACTION_SLOT_UNKNOWN');
        const captured = await actionBindings.resolve(name, actionIdentity(contract), json(expectedScope), options);
        const binding = captured.descriptor;
        // 宿主已经用该合同解析原始目标并核对真实绑定。scope 是其 JSON 输出，不能再次运行单向 transform。
        const invoke = async (input: ActionInput<S['actions'][K]>, options: CancellationOptions = {}): Promise<ActionInvocation<S['actions'][K]>> => {
          assertActive(); captured.assertActive();
          const signal = options.signal;
          const starting = signal ? AbortSignal.any([lifetime.signal, signal]) : lifetime.signal;
          starting.throwIfAborted();
          // input 本来就是 z.input；客户端仅核验 JSON，唯一业务解析仍在 Host。
          const started = await remote.startAction({ sessionId: identity.sessionId, name, contract: binding.contract, bindingId: binding.bindingId,
            input: json(input) }, starting);
          if (!started.ok) {
            if (started.error.code === 'gateway/internal' || started.error.code === 'gateway/cancelled')
              throw new ActionAdmissionUnknownError(identity.sessionId, name, started.error);
            throw started.error;
          }
          // ACK 已可能受理业务；矛盾回执不能被描述成确定未执行，也不能自动重发。
          try {
            if (started.value.name !== name) throw new Error('REACTIVE_ACTION_INVOCATION_SLOT_MISMATCH');
            assertActionIdentity(contract, started.value.contract);
          } catch (error) { throw new ActionAdmissionUnknownError(identity.sessionId, name, error); }
          return invocation(ctx.remote, identity.sessionId, started.value, contract, lifetime.signal, observations);
        };
        // schema 输出只在明确合同的传输边界恢复类型；不要求 scope 支持反向 encode。
        return Object.freeze(Object.assign(invoke, { definition: contract, bindingId: binding.bindingId,
          scope: binding.scope as ActionScope<S['actions'][K]> }));
      },
      get progress() { return progressOwner().progress; },
      get progress$() { return progressOwner().progress$; },
      get progressFrames$() { return progressOwner().progressFrames$; },
      get events$() { return progressOwner().events$; },
      request: requestId => progressOwner().request(requestId),
      async invocation<K extends keyof S['actions'] & string>(name: K, id: string): Promise<ActionInvocation<S['actions'][K]>> {
        assertActive();
        const contract = surfaceSlotDefinition(surface, 'actions', name);
        if (!contract) throw new Error('REACTIVE_ACTION_SLOT_UNKNOWN');
        const stored = result(await remote.actionInvocation({ sessionId: identity.sessionId, invocationId: id }, lifetime.signal));
        if (stored.name !== name) throw new Error('REACTIVE_ACTION_INVOCATION_SLOT_MISMATCH');
        return invocation(ctx.remote, identity.sessionId, stored, contract as S['actions'][K], lifetime.signal, observations);
      },
      human, question,
      human$: human.observations$,
      questions$: human.values$.pipe(map(value => value.questions)),
      foregroundQuestions$: human.values$.pipe(map(value => value.foreground)),
      observeView(name) {
        assertActive();
        return handle.viewResource(name).observations$;
      },
      viewResource(name) { return viewSlot(name).reader as ResourceReader<ViewValue<S['views'][typeof name]>>; },
      readView(name, options = {}) { return handle.viewResource(name).read(options.signal); },
      async submit(input, options = {}): Promise<ProgramInputReceipt> {
        assertActive();
        const cancelled = options.signal;
        const signal = cancelled ? AbortSignal.any([lifetime.signal, cancelled]) : lifetime.signal;
        signal.throwIfAborted();
        json(input);
        const submitted: ProgramSubmission = input.intent === 'result'
          ? { ...input, scope: json(input.scope), result: { ...input.result,
            continuation: { ...input.result.continuation, inputs: input.result.continuation.inputs.map(reference => ({ ...reference, scope: json(reference.scope) })) },
            wait: { ...input.result.wait, operation: json(input.result.wait.operation) } } }
          : { ...input, scope: json(input.scope) };
        const reply = await remote.submit({ sessionId: identity.sessionId, submission: submitted }, signal);
        if (!reply.ok && (reply.error.code === 'gateway/internal' || reply.error.code === 'gateway/cancelled')) {
          throw new SubmissionAdmissionUnknownError(identity.sessionId, submitted.requestId, reply.error);
        }
        return result(reply);
      },
      async editQueuedInput(requestId, content, options = {}): Promise<ProgramInputReceipt> {
        assertActive();
        const signal = options.signal ? AbortSignal.any([scopeSignal, options.signal]) : scopeSignal;
        signal.throwIfAborted();
        const reply = await remote.editQueuedInput({ sessionId: identity.sessionId, requestId, content }, signal);
        if (!reply.ok && (reply.error.code === 'gateway/internal' || reply.error.code === 'gateway/cancelled'))
          throw new InputEditAdmissionUnknownError(identity.sessionId, requestId, reply.error);
        return result(reply);
      },
      async uploadFile(data, options = {}): Promise<FileUploadValue> {
        assertActive();
        const cancelled = options.signal;
        const signal = cancelled ? AbortSignal.any([scopeSignal, cancelled]) : scopeSignal;
        signal.throwIfAborted();
        const name = options.name;
        const bytes = data instanceof Uint8Array ? data : new Uint8Array(await data.arrayBuffer());
        signal.throwIfAborted();
        const uploaded = await remote.uploadFile({ sessionId: identity.sessionId, data: bytesToBase64(bytes),
          ...(name === undefined ? {} : { name }) }, signal);
        signal.throwIfAborted();
        return result(uploaded);
      },
      async requestStatus(requestId) {
        assertActive();
        return result(await callRemote(signal => remote.requestStatus({ sessionId: identity.sessionId, requestId }, signal), scopeSignal));
      },
      async pause() {
        assertActive(); return result(await callRemote(signal => remote.pause({ sessionId: identity.sessionId }, signal), scopeSignal));
      },
      async cancel() {
        assertActive(); return result(await callRemote(signal => remote.cancel({ sessionId: identity.sessionId }, signal), scopeSignal));
      },
      async answerQuestion(callId, answer, options = {}) {
        assertActive();
        const cancelled = options.signal;
        const signal = cancelled ? AbortSignal.any([scopeSignal, cancelled]) : scopeSignal;
        signal.throwIfAborted();
        return result(await remote.answerQuestion({ sessionId: identity.sessionId, callId, answer }, signal));
      },
      async *attachWait(callId, options = {}) {
        assertActive();
        const signal = options.signal;
        const observing = signal ? AbortSignal.any([scopeSignal, signal]) : scopeSignal;
        // claim 不跨断线自动重领；原生 owner 明确接收这一物理流的取消。
        const opened = await remote.attachWait({ sessionId: identity.sessionId, callId }, observing);
        yield* opened;
      },
      attachOperation(definition, reference, options = {}) {
        assertActive();
        const signal = options.signal;
        return attachRemoteOperation(ctx.remote, identity.sessionId, definition, reference, accessSource,
          signal ? AbortSignal.any([scopeSignal, signal]) : scopeSignal);
      },
      locateOperation(definition, command, options = {}) {
        assertActive();
        const signal = options.signal;
        return locateRemoteOperation(ctx.remote, identity.sessionId, definition, command, accessSource,
          signal ? AbortSignal.any([scopeSignal, signal]) : scopeSignal);
      },
      readOperationFeedback(definition, command, participationId, options = {}) {
        const signal = options.signal;
        assertActive(); return readRemoteOperationFeedback(ctx.remote, identity.sessionId, definition, command, participationId,
          signal ? AbortSignal.any([scopeSignal, signal]) : scopeSignal);
      },
      async notifyOperation(followId, options = {}) {
        assertActive();
        const cancelled = options.signal;
        const signal = cancelled ? AbortSignal.any([scopeSignal, cancelled]) : scopeSignal;
        signal.throwIfAborted();
        return result(await remote.notifyOperation({ sessionId: identity.sessionId, followId }, signal));
      },
      async openEvents(eventOptions, request = {}) {
        assertActive();
        const journal = new ProgramJournal(ctx.remote, identity.sessionId, eventOptions);
        journals.add(journal);
        try { await journal.open(request); assertActive(); return journal; }
        catch (error) { journals.delete(journal); await journal.dispose(); throw error; }
      },
      release(): Promise<void> {
        return released ??= (async (): Promise<void> => {
          controller.abort(new Error('REACTIVE_CONVERSATION_HANDLE_RELEASED'));
          // 释放中的句柄仍归SDK持有；并发dispose必须等待同一批异步查询退役。
          try {
            await Promise.all([actionBindings.close(), humanConnection.close(), progress?.close(), ...[...journals].map(journal => journal.dispose()), ...[...slots.values()].map(slot => slot.close())]);
          } finally { sessions.delete(handle); }
        })();
      },
    };
    sessionConnections.set(handle, connection);
    sessions.add(handle);
    try { assertActive(); return handle; }
    catch (error) { await handle.release(); throw error; }
  };
  return {
    async createWorkspace(registration) {
      return result(await callRemote(signal => remote.createWorkspace(registration, signal)));
    },
    async create(environmentId, options): Promise<RemoteProgramSession<P>> {
      active();
      const reference = { environmentId, workspaceId: options.workspaceId, sessionId: options.sessionId ?? crypto.randomUUID() };
      let dispatched = false;
      try {
        const reply = await callRemote(signal => {
          dispatched = true;
          return remote.create(reference, signal);
        });
        if (!reply.ok && (reply.error.code === 'gateway/internal' || reply.error.code === 'gateway/cancelled')) throw new CreationUnknownError(reference, reply.error);
        const identity = creation(reply); active(); return await session(identity);
      } catch (error) {
        if (dispatched && lifetime.signal.aborted && error === lifetime.signal.reason) throw new CreationUnknownError(reference, error);
        throw error;
      }
    },
    async completeCreation(reference): Promise<RemoteProgramSession<P>> {
      const identity = creation(await callRemote(signal => remote.completeCreation(reference, signal))); active(); return session(identity);
    },
    async open(sessionId): Promise<RemoteProgramSession<P>> {
      const identity = creation(await callRemote(signal => remote.open({ sessionId }, signal))); active(); return session(identity);
    },
    async resume(sessionId): Promise<RemoteProgramSession<P>> {
      const identity = creation(await callRemote(signal => remote.resume({ sessionId }, signal))); active(); return session(identity);
    },
    async describe() { return result(await callRemote(signal => remote.describe(signal))); },
    dispose: () => close().finally(() => releaseContext()),
  };
}

function invocation<D extends ActionContract>(remote: ClientRemote, sessionId: string, start: ProgramActionStart, contract: D, lifetime: AbortSignal,
  observations: Set<AbortController>): ActionInvocation<D> {
  assertActionIdentity(contract, start.contract);
  const controller = new AbortController();
  observations.add(controller);
  const signal = AbortSignal.any([lifetime, controller.signal]);
  const activityState = new BehaviorSubject<Observation<InvocationActivity>>({ status: 'pending', generation: 0 });
  const ongoing = (observation: Observation<InvocationActivity>): boolean => observation.status === 'pending'
    || (observation.status === 'ready' && (observation.value.state === 'pending' || observation.value.state === 'running'));
  const activity = new ResourceReader<InvocationActivity>({ peek: () => activityState.value,
    observations$: activityState.pipe(takeWhile(ongoing, true)),
  }, { signal: lifetime });
  const address = { sessionId, invocationId: start.id };
  const watch = (async (): Promise<void> => {
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
    finally { observations.delete(controller); }
  })();
  void watch.catch(() => {});
  const settled = remote.reactivePrograms.actionResult(address, lifetime).then(reply => z.decode(contract.output, result(reply)));
  void settled.catch(() => {});
  const mapping = contract.operation;
  const operation = mapping ? settled.then(receipt => attachRemoteOperation(remote, sessionId, mapping.definition,
    z.json().parse(mapping.reference(receipt as never)), start.source, lifetime)) : undefined;
  void operation?.catch(() => {});
  // 动态 Surface 映射经对应原合同 decode；operation 也只在原合同声明时构造。
  return Object.freeze({ id: start.id, source: start.source, result: settled, activity, activity$: activity.values$,
    ...(operation ? { operation } : {}) }) as ActionInvocation<D>;
}
