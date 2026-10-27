import type { Context } from '@deepseek-ai/cordis';
import { Remote, RemoteError, TypertRemoteService, type Principal } from '@deepseek-ai/dsh-typert-protocol';
import type { JsonValue } from '@deepseek-ai/dsh-util-values';
import { SessionId, SessionLogOffset, type Session, type SessionEvent } from '@deepseek-ai/dsh-session';
import type { ProgramJournalEntry, ProgramJournalPage } from '@hanger-source/dsh-reactive-client/types';
import { type ApplicationHost, type ConversationHandle } from '@hanger-source/dsh-reactive-dsh';
import { WorkspaceId } from '@deepseek-ai/dsh-workspace';
import { decodeProgramFailure } from '@hanger-source/dsh-reactive-client/errors';
import { createProgramFailureMapper } from './host-errors.ts';
import { z, encodeOperationFeedback, type AnyEnvironment, type InputReference, type ConversationEvent, type HumanQuestionOrigin } from '@hanger-source/dsh-reactive';
import { endWith, firstValueFrom, fromEvent, ignoreElements, merge, take, type Observable, type Subscription } from 'rxjs';
import type { ProgramActionRequest, ProgramActionStart, ProgramAddress, ProgramCreateRequest, ProgramCreationReference, ProgramIdentity, ProgramInputReceipt, ProgramInvocationAddress, ProgramInvocationFrame, ProgramSubmitRequest, ProgramWorkspaceRegistration, ProgramViewFrame, ProgramViewRequest, WireObservation, ProgramFailure, ProgramActionBindingRequest, ProgramActionBinding, ProgramUploadFileRequest, ProgramFileUploadValue, ProgramInputEditRequest } from '@hanger-source/dsh-reactive-client/types';
import type { ProgramOperationRequest, ProgramOperationLocateRequest, ProgramOperationLocation, ProgramOperationNotification, ProgramOperationNotificationResult, ProgramAnswerRequest, ProgramQuestionRequest, ProgramQuestionsFrame, ProgramWaitFrame, ProgramOperationBindingRequest, ProgramOperationBinding, ProgramOperationFeedbackRequest, ProgramOperationFeedbackRead } from '@hanger-source/dsh-reactive-client/types';
import type { ProgramRequestAddress, ProgramRequestStatus, ProgramWorkspace, ProgramGoalControl, ProgramCancelResult, ProgramJournalOpenRequest, ProgramJournalPageRequest, ProgramJournalFrame } from '@hanger-source/dsh-reactive-client/types';
import type { ProgramProgressFrame, ProgramConversationEvent } from '@hanger-source/dsh-reactive-client/types';
import { Inbox } from '@hanger-source/dsh-reactive-client/transport';
import { createProgramActionOwner, type ProgramActionOwner, type ProgramActionOwnerOptions } from './actions.ts';

declare module '@deepseek-ai/cordis' {
  interface Context { reactivePrograms: ProgramRemoteService; }
}
/** 身份映射来自宿主认证配置，wire 参数不能选择 actor。 */
export interface ProgramRemoteOptions<E extends readonly AnyEnvironment[] = readonly AnyEnvironment[]> extends ProgramActionOwnerOptions {
  readonly application: ApplicationHost<E>;
}

/** @typert service reactivePrograms */
export class ProgramRemoteService<E extends readonly AnyEnvironment[] = readonly AnyEnvironment[]> extends TypertRemoteService {
  static inject: string[] = ['typertGateway', 'remoteAccess', 'sessionController'];
  private readonly actions: ProgramActionOwner;
  private readonly failure: (error: unknown) => ProgramFailure;
  private readonly rethrow = (error: unknown): never => { throw decodeProgramFailure(this.failure(error)); };
  constructor(ctx: Context, private readonly options: ProgramRemoteOptions<E>) {
    super(ctx, 'reactivePrograms');
    this.failure = createProgramFailureMapper(ctx.typertGateway.wireStream);
    const owner = createProgramActionOwner(ctx, options.application, options);
    this.actions = owner;
    ctx.effect(() => () => owner.dispose());
  }

  private principal(): Principal {
    const invocation = this.ctx.invocation;
    if (!invocation) throw new RemoteError('gateway/internal', 'Remote invocation identity is required', {});
    return invocation.peer.principal;
  }
  private actor(): string {
    const actor = this.options.actor(this.principal());
    if (!actor.trim()) throw new RemoteError('gateway/internal', 'Host actor mapping is invalid', {});
    return actor;
  }
  /** 注册身份可以返回；成员读取仍按当前 endpoint 的 Workspace 与 Session grant 逐项核对。 */
  private visibleWorkspace(workspace: ProgramWorkspace): ProgramWorkspace {
    const visible = this.ctx.remoteAccess.allows({ kind: 'workspace', id: WorkspaceId(workspace.workspaceId) });
    return { ...workspace, sessionIds: visible
      ? workspace.sessionIds.filter(id => this.ctx.remoteAccess.allows({ kind: 'session', id: SessionId(id) })) : [] };
  }
  /** 原始范围只经过 JSON wire 边界，不运行 Program 输入转换。 */
  private wireInputs(references: readonly InputReference[]): readonly InputReference<JsonValue>[] {
    return references.map(reference => ({ ...reference, scope: z.json().parse(reference.scope) }));
  }
  /** 直接消费原始事实；只核验 JSON 字段，不逆编码或再次解析业务 input。 */
  private occurrence(value: ConversationEvent): ProgramConversationEvent {
    if (value.kind === 'execution.error') {
      const { inputs: references, ...event } = value;
      return { ...event, error: this.failure(value.error), ...(references ? { inputs: this.wireInputs(references) } : {}) };
    }
    if (value.kind === 'assistant.message') {
      const { inputs: references, ...event } = value;
      return { ...event, ...(references ? { inputs: this.wireInputs(references) } : {}) };
    }
    if (value.kind === 'tool.settled') {
      const { result, meta, ...event } = value;
      return { ...event, inputs: this.wireInputs(value.inputs), ...(result === undefined ? {} : { result: z.json().parse(result) }),
        ...(meta === undefined ? {} : { meta: z.json().parse(meta) }) };
    }
    if ('continuation' in value) return { ...value,
      continuation: { ...value.continuation, inputs: this.wireInputs(value.continuation.inputs) },
      wait: { ...value.wait, operation: z.json().parse(value.wait.operation) } };
    if ('inputs' in value) return { ...value, inputs: this.wireInputs(value.inputs) };
    return structuredClone(value);
  }

  /** 显式注册已有工作目录；不选择 Environment 或创建会话。 */
  @Remote
  async createWorkspace(request: ProgramWorkspaceRegistration): Promise<ProgramWorkspace> {
    this.principal();
    const workspace = await this.options.application.createWorkspace(request).catch(this.rethrow);
    return this.visibleWorkspace(workspace);
  }

  /** 在获准的既有 Workspace 中，以指定 Environment 创建工作会话。 */
  @Remote({ resources: { session: _request => undefined, workspace: request => WorkspaceId(request.workspaceId) } })
  async create(request: ProgramCreateRequest): Promise<ProgramIdentity> {
    const actorId = this.actor();
    const { environmentId, ...options } = request;
    const handle = await this.options.application.create(environmentId, options).catch(this.rethrow);
    try { return { sessionId: handle.id, environmentId: handle.identity.environmentId, workspaceId: handle.identity.workspaceId, actorId }; }
    finally { handle.release(); }
  }

  /** 凭明确引用补齐框架已持久化但尚未完成的创建。 */
  @Remote({ resources: { session: _request => undefined, workspace: request => WorkspaceId(request.workspaceId) } })
  async completeCreation(request: ProgramCreationReference): Promise<ProgramIdentity> {
    const actorId = this.actor();
    const handle = await this.options.application.completeCreation(request).catch(this.rethrow);
    try { return { sessionId: handle.id, environmentId: handle.identity.environmentId, workspaceId: handle.identity.workspaceId, actorId }; }
    finally { handle.release(); }
  }

  /** 只读打开当前程序，不把观察变成模型唤醒。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async open(request: ProgramAddress): Promise<ProgramIdentity> {
    const actorId = this.actor();
    const handle = await this.options.application.open(request.sessionId).catch(this.rethrow);
    try { return { sessionId: handle.id, environmentId: handle.identity.environmentId, workspaceId: handle.identity.workspaceId, actorId }; }
    finally { handle.release(); }
  }

  /** 显式恢复真实宿主；只读 open 不承担激活语义。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async resume(request: ProgramAddress): Promise<ProgramIdentity> {
    const actorId = this.actor();
    const handle = await this.options.application.resume(request.sessionId).catch(this.rethrow);
    try { return { sessionId: handle.id, environmentId: handle.identity.environmentId, workspaceId: handle.identity.workspaceId, actorId }; }
    finally { handle.release(); }
  }

  /** 发现仅返回获准的 Workspace 与其获准成员；global endpoint grant 不授予内容。 */
  @Remote
  async describe(): Promise<readonly ProgramWorkspace[]> {
    this.principal();
    const workspaces = await this.options.application.describe().catch(this.rethrow);
    return workspaces.filter(workspace => this.ctx.remoteAccess.allows({ kind: 'workspace', id: WorkspaceId(workspace.workspaceId) }))
      .map(workspace => this.visibleWorkspace(workspace));
  }

  /** 暂停原生 Goal；它不取消外部业务操作。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async pause(request: ProgramAddress): Promise<ProgramGoalControl> {
    this.principal();
    const handle = await this.options.application.open(request.sessionId).catch(this.rethrow);
    try { const goal = await handle.pause().catch(this.rethrow); return { id: goal.id, revision: goal.revision, phase: goal.phase }; }
    finally { handle.release(); }
  }

  /** 取消原生处理；真实业务 owner 的 body 不随网络调用结束。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async cancel(request: ProgramAddress): Promise<ProgramCancelResult> {
    this.principal();
    const handle = await this.options.application.open(request.sessionId).catch(this.rethrow);
    try { await handle.cancel().catch(this.rethrow); return { canceled: true }; }
    finally { handle.release(); }
  }

  /** 原始提交继续由 ApplicationHost 解析、准入、持久化及关联。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async submit(request: ProgramSubmitRequest, signal: AbortSignal): Promise<ProgramInputReceipt> {
    const actorId = this.actor();
    signal.throwIfAborted();
    const handle = await this.options.application.open(request.sessionId).catch(this.rethrow);
    // wire 只证明 JSON；动态环境的 scope 合同继续由 Host 唯一 parse，不在 remote 重复转换。
    try { return await handle.submit(request.submission, { actorId, signal }).catch(this.rethrow); }
    finally { handle.release(); }
  }

  /** 认证主体只上传到获准会话；正文编码及 staged receipt 由同一 Host 入口处理。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async uploadFile(request: ProgramUploadFileRequest, signal: AbortSignal): Promise<ProgramFileUploadValue> {
    const actorId = this.actor();
    signal.throwIfAborted();
    const handle = await this.options.application.open(request.sessionId).catch(this.rethrow);
    try {
      return await handle.uploadEncodedFile({ data: request.data, ...(request.name === undefined ? {} : { name: request.name }) },
        { actorId, signal }).catch(this.rethrow);
    } finally { handle.release(); }
  }

  /** 核对原 requestId 的真实受理/消费事实；查询不唤醒模型或重投输入。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async requestStatus(request: ProgramRequestAddress): Promise<ProgramRequestStatus> {
    this.principal();
    const handle = await this.options.application.open(request.sessionId).catch(this.rethrow);
    try { return { requestId: request.requestId, state: handle.snapshot().requests[request.requestId]?.state ?? 'absent' }; }
    finally { handle.release(); }
  }

  /** 一个 Session 连接承载当前状态与新事实；快照不伪造新的请求事件。 */
  @Remote({ mode: 'stream', resources: { session: request => SessionId(request.sessionId) } })
  async *progress(request: ProgramAddress, signal: AbortSignal): AsyncIterable<ProgramProgressFrame> {
    this.principal();
    const handle = await this.options.application.open(request.sessionId).catch(this.rethrow);
    const inbox = new Inbox<ProgramProgressFrame>(1024 * 1024);
    const subscriptions: Subscription[] = [];
    const abort = (): void => inbox.fail(signal.reason);
    signal.addEventListener('abort', abort, { once: true });
    try {
      signal.throwIfAborted();
      // open 等待期间发生的 start 已进入同一 Conversation owner。此同步 cut 到订阅完成没有 await。
      const initial = handle.progressSnapshot();
      const observation: WireObservation = initial.status === 'ready' ? { ...initial, value: z.json().parse(initial.value) }
        : initial.status === 'error' ? { ...initial, error: this.failure(initial.error) } : initial;
      inbox.push({ type: 'baseline', observation });
      let activity = initial.status === 'ready' ? initial.value.activity : undefined;
      // 不订 progress$：那会在每个 raw chunk 先物化整个累计 prefix。
      subscriptions.push(handle.activity$.subscribe({
        next: value => {
          if (value === activity) return;
          activity = value;
          try { inbox.push({ type: 'activity', activity: z.json().parse(value) }); }
          catch (error) { inbox.fail(error); }
        }, error: (error: unknown) => inbox.fail(error),
      }), handle.progressFrames$.subscribe({
        next: value => {
          try { inbox.push({ type: 'assistant', progress: z.json().parse(value) }); }
          catch (error) { inbox.fail(error); }
        }, error: (error: unknown) => inbox.fail(error),
      }), handle.rawEvents$.subscribe({
        next: value => {
          try { inbox.push({ type: 'event', event: this.occurrence(value) }); }
          catch (error) { inbox.fail(error); }
        }, error: (error: unknown) => inbox.fail(error), complete: () => inbox.end(),
      }));
      yield* inbox;
    } catch (error) { this.rethrow(error); }
    finally {
      signal.removeEventListener('abort', abort); subscriptions.forEach(subscription => subscription.unsubscribe());
      inbox.end(); handle.release();
    }
  }

  /** 监听先于 opening cut；观察不经过 Controller.follow 的后台 Agent 激活。 */
  @Remote({ mode: 'stream', resources: { session: request => SessionId(request.sessionId) } })
  async *journal(request: ProgramJournalOpenRequest, signal: AbortSignal): AsyncIterable<ProgramJournalFrame> {
    this.principal();
    const id = SessionId(request.sessionId);
    const inbox = new Inbox<ProgramJournalEntry>(1024 * 1024);
    let cut: number | undefined;
    let openingSession: Session | undefined;
    const append = (event: SessionEvent): void => {
      try { inbox.push({ type: 'event', event: { ...event, data: z.json().parse(event.data) } }); }
      catch (error) { inbox.fail(error); }
    };
    const stopEvent = this.ctx.on('session/event', (session, event) => {
      if (session.id === id) append(event);
    }, { global: true });
    const stopCreated = this.ctx.on('session/created', session => {
      if (session.id !== id) return;
      if (cut === undefined) { openingSession = session; return; }
      // 后续挂载可带此前未由本进程发布的 seed；重复项交给 Gateway 原生 cursor 处理。
      for (const event of session.snapshotEvents(SessionLogOffset(cut + 1))) append(event);
    }, { global: true });
    const abort = (): void => inbox.fail(signal.reason);
    signal.addEventListener('abort', abort, { once: true });
    let handle: ConversationHandle | undefined;
    try {
      signal.throwIfAborted();
      handle = await this.options.application.open(request.sessionId).catch(this.rethrow);
      cut = handle.snapshot().sequence;
      // await opening 期间只保留原生 Session，不把已持久历史塞入有界 carrier 邮箱。
      if (openingSession) {
        for (const event of openingSession.snapshotEvents(SessionLogOffset(cut + 1))) append(event);
        openingSession = undefined;
      }
      const { sessionId: _sessionId, ...window } = request;
      const page = await this.ctx.sessionController.page({ address: { kind: 'session', sessionId: id }, throughSeq: cut, ...window }, signal);
      signal.throwIfAborted();
      yield { type: 'opened', cursor: cut, page };
      for await (const entry of inbox) yield { type: 'entry', entry };
    } catch (error) { this.rethrow(error); }
    finally {
      signal.removeEventListener('abort', abort); stopEvent(); stopCreated(); inbox.end(); openingSession = undefined; handle?.release();
    }
  }

  /** 同一 Program 权限边界内的原生冷安全分页，不唤醒 Agent。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async journalPage(request: ProgramJournalPageRequest, signal: AbortSignal): Promise<ProgramJournalPage> {
    this.principal();
    const handle = await this.options.application.open(request.sessionId).catch(this.rethrow);
    try {
      const { sessionId, ...page } = request;
      return await this.ctx.sessionController.page({ address: { kind: 'session', sessionId: SessionId(sessionId) }, ...page }, signal);
    } finally { handle.release(); }
  }

  /** 订阅已编码的当前视图；业务 source 连接仍归 Program owner。 */
  @Remote({ mode: 'stream', resources: { session: request => SessionId(request.sessionId) } })
  async *view(request: ProgramViewRequest, signal: AbortSignal): AsyncIterable<ProgramViewFrame> {
    this.principal();
    const handle = await this.options.application.open(request.sessionId).catch(this.rethrow);
    const inbox = new Inbox<ProgramViewFrame>(1024 * 1024);
    let first = true;
    const subscription = handle.observeWireView(request.name).subscribe({
      next: value => {
        try {
          const observation: WireObservation = value.status === 'error'
            ? { status: 'error', generation: value.generation, error: this.failure(value.error) }
            : value.status === 'ready' ? { ...value, value: z.json().parse(value.value) } : value;
          inbox.push({ type: first ? 'baseline' : 'observation', observation }); first = false;
        } catch (error) { inbox.fail(error); }
      }, error: (error: unknown) => inbox.fail(error), complete: () => inbox.end(),
    });
    const abort = (): void => inbox.fail(signal.reason);
    signal.addEventListener('abort', abort, { once: true });
    try { signal.throwIfAborted(); yield* inbox; }
    catch (error) { this.rethrow(error); }
    finally { signal.removeEventListener('abort', abort); subscription.unsubscribe(); inbox.end(); handle.release(); }
  }

  /** 流拥有真实会话句柄；开场交付绑定，释放流才结束这次只读持有。 */
  @Remote({ mode: 'stream', resources: { session: request => SessionId(request.sessionId) } })
  async *resolveAction(request: ProgramActionBindingRequest, signal: AbortSignal): AsyncIterable<ProgramActionBinding> {
    this.principal();
    signal.throwIfAborted();
    const handle = await this.options.application.open(request.sessionId).catch(this.rethrow);
    try {
      signal.throwIfAborted();
      const binding = handle.resolveAction(request.name, request.scope);
      yield { bindingId: binding.bindingId, scope: z.json().parse(binding.scope) };
      if (!signal.aborted) await firstValueFrom(merge(
        handle.activity$.pipe(ignoreElements(), endWith(undefined)), fromEvent(signal, 'abort'),
      ).pipe(take(1)));
    } catch (error) { return this.rethrow(error); }
    finally { handle.release(); }
  }

  /** 显式开始一次确切绑定的调用；此方法不隐式重试，也不把 receipt 当成结果。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  startAction(request: ProgramActionRequest, signal: AbortSignal): Promise<ProgramActionStart> {
    return this.actions.start(request, this.principal(), signal);
  }

  /** 读取原 Invocation 的 wire 结果，不重新执行 Action。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  actionResult(request: ProgramInvocationAddress, signal: AbortSignal): Promise<JsonValue> {
    return this.actions.result(request, this.principal(), signal);
  }

  /** 只查询同一个进程 owner 持有的 Invocation；过期或未知不重放。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  actionInvocation(request: ProgramInvocationAddress, signal: AbortSignal): Promise<ProgramActionStart> {
    return this.actions.get(request, this.principal(), signal);
  }

  /** 观察原 Invocation 最近状态及结算，取消流只释放该观察。 */
  @Remote({ mode: 'stream', resources: { session: request => SessionId(request.sessionId) } })
  observeAction(request: ProgramInvocationAddress, signal: AbortSignal): AsyncIterable<ProgramInvocationFrame> {
    return this.actions.observe(request, this.principal(), signal);
  }

  /** 捕获当前 Expose 槽的真实 command；后续读取不再解析这个槽或登记原生等待。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async resolveOperation(request: ProgramOperationBindingRequest, signal: AbortSignal): Promise<ProgramOperationBinding> {
    const actorId = this.actor();
    signal.throwIfAborted();
    const handle = await this.options.application.open(request.sessionId).catch(this.rethrow);
    try {
      const binding = handle.resolveOperation(request.name, { actorId, signal });
      return { operation: { kind: binding.definition.kind, version: binding.definition.version }, command: z.json().parse(binding.command) };
    } catch (error) { return this.rethrow(error); }
    finally { handle.release(); }
  }

  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async editQueuedInput(request: ProgramInputEditRequest, signal: AbortSignal): Promise<ProgramInputReceipt> {
    const actorId = this.actor();
    signal.throwIfAborted();
    const handle = await this.options.application.open(request.sessionId).catch(this.rethrow);
    try { return await handle.editQueuedInput(request.requestId, request.content, { actorId, signal }).catch(this.rethrow); }
    finally { handle.release(); }
  }

  /** 只读原 command 的已登记参与；Host 取精确持久 link，不激活 Agent 或 register。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async readOperationFeedback(request: ProgramOperationFeedbackRequest, signal: AbortSignal): Promise<ProgramOperationFeedbackRead> {
    const definition = this.options.application.operationDefinition(request.operation.kind, request.operation.version);
    const value = await this.options.application.readOperationFeedback(request.sessionId, definition, request.command,
      request.participationId, { actorId: this.actor(), signal }).catch(this.rethrow);
    if (value.status === 'unknown') return value;
    if (value.status === 'rejected') return { ...value, reason: z.json().parse(value.reason) };
    const reference = z.json().parse(value.reference);
    if (value.status === 'pending') return { status: 'pending', reference };
    const node = encodeOperationFeedback(definition, value.node);
    return { status: 'ready', reference, node: { id: node.id, payload: z.json().parse(node.payload) } };
  }

  /** 只按既有业务 command 查受理事实；unknown 不重发 Action。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async locateOperation(request: ProgramOperationLocateRequest, signal: AbortSignal): Promise<ProgramOperationLocation> {
    const definition = this.options.application.operationDefinition(request.operation.kind, request.operation.version);
    const location = await this.options.application.locateOperation(request.sessionId, definition, request.command, { actorId: this.actor(), signal }).catch(this.rethrow);
    return location.status === 'accepted' ? { status: 'accepted', reference: z.json().parse(location.reference),
      state: z.json().parse(z.encode(definition.state, location.state)) }
      : location.status === 'rejected' ? { status: 'rejected', reason: z.json().parse(location.reason) } : location;
  }

  /** 连接实际 owner 的当前事实，只有 ready 才编码同一 Operation state schema。 */
  @Remote({ mode: 'stream', resources: { session: request => SessionId(request.sessionId) } })
  async *observeOperation(request: ProgramOperationRequest, signal: AbortSignal): AsyncIterable<ProgramViewFrame> {
    const definition = this.options.application.operationDefinition(request.operation.kind, request.operation.version);
    const operation = await this.options.application.attachOperation(request.sessionId, definition, request.reference, { actorId: this.actor(), signal }).catch(this.rethrow);
    let first = true;
    try { yield* frames(operation.observations$, signal, observed => {
      const observation: WireObservation = observed.status === 'ready'
        ? { ...observed, value: z.json().parse(z.encode(definition.state, observed.value)) }
        : observed.status === 'error' ? { ...observed, error: this.failure(observed.error) } : observed;
      const frame: ProgramViewFrame = { type: first ? 'baseline' : 'observation', observation }; first = false; return frame;
    }); } catch (error) { this.rethrow(error); }
  }

  /** 宿主重新核验原 follow 与业务事实；不接受网络端自报 ready。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  notifyOperation(request: ProgramOperationNotification, signal: AbortSignal): Promise<ProgramOperationNotificationResult> {
    return this.options.application.notifyOperation(request.sessionId, request.followId, { actorId: this.actor(), signal }).catch(this.rethrow);
  }

  /** 同一原生问题投影与实际前台请求，不维护第二份问题注册。 */
  @Remote({ mode: 'stream', resources: { session: request => SessionId(request.sessionId) } })
  async *questions(request: ProgramAddress, signal: AbortSignal): AsyncIterable<ProgramQuestionsFrame> {
    this.principal();
    const handle = await this.options.application.open(request.sessionId).catch(this.rethrow);
    let first = true;
    try {
      yield* frames(handle.human.values$, signal, ({ questions, foreground }) => {
        const wireQuestion = <Q extends { readonly origin: HumanQuestionOrigin }>(question: Q) => ({
          ...question, origin: { ...question.origin, inputs: this.wireInputs(question.origin.inputs) },
        });
        const frame: ProgramQuestionsFrame = { type: first ? 'baseline' : 'questions', value: {
          questions: { ...questions, active: questions.active.map(wireQuestion), settled: questions.settled.map(wireQuestion) },
          foreground: foreground.map(wireQuestion),
        } };
        first = false; return frame;
      });
    } finally { handle.release(); }
  }

  /** bool 只说明前台受理或 continued 排队，不是答案落盘和工具结算。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async answerQuestion(request: ProgramAnswerRequest, signal: AbortSignal): Promise<boolean> {
    const actorId = this.actor();
    const handle = await this.options.application.open(request.sessionId).catch(this.rethrow);
    try { return await handle.answerQuestion(request.callId, request.answer, { actorId, signal }).catch(this.rethrow); }
    finally { handle.release(); }
  }

  /** claim 真实前台等待；流退出由原生 Human owner 释放 claim。 */
  @Remote({ mode: 'stream', resources: { session: request => SessionId(request.sessionId) } })
  async *attachWait(request: ProgramQuestionRequest, signal: AbortSignal): AsyncIterable<ProgramWaitFrame> {
    const actorId = this.actor();
    const handle = await this.options.application.open(request.sessionId).catch(this.rethrow);
    try { yield* handle.attachWait(request.callId, { actorId, signal }); }
    catch (error) { this.rethrow(error); }
    finally { handle.release(); }
  }
}

/** 仅连接物理流与已有 Observable；历史、重连与游标继续由 Gateway 拥有。 */
async function* frames<T, F>(source: Observable<T>, signal: AbortSignal, project: (value: T) => F): AsyncIterable<F> {
  const inbox = new Inbox<F>(1024 * 1024);
  const subscription = source.subscribe({ next: value => { try { inbox.push(project(value)); } catch (error) { inbox.fail(error); } },
    error: (error: unknown) => inbox.fail(error), complete: () => inbox.end() });
  const abort = (): void => inbox.fail(signal.reason);
  signal.addEventListener('abort', abort, { once: true });
  try { signal.throwIfAborted(); yield* inbox; }
  finally { signal.removeEventListener('abort', abort); subscription.unsubscribe(); inbox.end(); }
}
