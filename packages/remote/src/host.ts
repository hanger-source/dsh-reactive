import type { ProgramImageAttachmentRequest, ProgramImageAttachmentValue } from '@hanger-source/dsh-reactive-client/types';
import type { Context } from '@deepseek-ai/cordis';
import { Remote, RemoteError, TypertRemoteService, type Principal } from '@deepseek-ai/dsh-typert-protocol';
import { deepFreeze, type JsonValue } from '@deepseek-ai/dsh-util-values';
import { SessionId } from '@deepseek-ai/dsh-session';
import type { ProgramJournalPage } from '@hanger-source/dsh-reactive-client/types';
import { type ApplicationHost, type ConversationHandle, type ServiceConversation } from '@hanger-source/dsh-reactive-dsh';
import { WorkspaceId } from '@deepseek-ai/dsh-workspace';
import { createProgramFailureMapper, throwProgramFailure } from './host-errors.ts';
import { z, abortable, ConversationAccessRevokedError, actionIdentity, assertActionIdentity, programIdentity, assertSharedProgramDefinition, surfaceSlotDefinition, encodeOperationFeedback, type AnyEnvironment, type InputReference, type ConversationEvent, type HumanQuestionOrigin } from '@hanger-source/dsh-reactive';
import { defer, endWith, firstValueFrom, fromEvent, ignoreElements, merge, take, type Observable, type Subscription } from 'rxjs';
import type { ProgramActionRequest, ProgramActionStart, ProgramAddress, ProgramSessionAddress, ProgramCreateRequest, ProgramCompleteCreationRequest, ProgramIdentity, ProgramInputReceipt, ProgramInvocationAddress, ProgramInvocationLookupRequest, ProgramInvocationFrame, ProgramSubmitRequest, ProgramWorkspaceRegistration, ProgramViewFrame, ProgramViewRequest, WireObservation, ProgramFailure, ProgramActionBindingRequest, ProgramActionBinding, ProgramUploadFileRequest, ProgramFileUploadValue, ProgramInputEditRequest } from '@hanger-source/dsh-reactive-client/types';
import type { ProgramOperationRequest, ProgramOperationLocateRequest, ProgramInvocationOperationRequest, ProgramOperationLocation, ProgramOperationNotification, ProgramOperationNotificationResult, ProgramAnswerRequest, ProgramQuestionRequest, ProgramQuestionsFrame, ProgramWaitFrame, ProgramOperationBindingRequest, ProgramOperationBinding, ProgramOperationFeedbackRequest, ProgramOperationFeedbackRead } from '@hanger-source/dsh-reactive-client/types';
import type { ProgramRequestAddress, ProgramRequestStatus, ProgramWorkspace, ProgramGoalControl, ProgramCancelResult, ProgramJournalOpenRequest, ProgramJournalPageRequest, ProgramJournalFrame } from '@hanger-source/dsh-reactive-client/types';
import type { ProgramProgressFrame, ProgramConversationEvent, ProgramExecutionFailure, ProgramModelPreparation } from '@hanger-source/dsh-reactive-client/types';
import type { ConversationActivity, ExecutionFailure, ModelPreparation } from '@hanger-source/dsh-reactive';
import { Inbox } from '@hanger-source/dsh-reactive-client/inbox';
import { createProgramActionOwner, type ProgramActionOwner, type ProgramActionOwnerOptions } from './actions.ts';
import { resolveProgramParticipant, type ProgramParticipant } from './participant.ts';
import type { WorkspaceFileText, WorkspaceFileBytes, WorkspaceFileStat, WorkspaceDirectoryListing, WorkspaceFileWatchFrame } from '@deepseek-ai/dsh-api-workspace-files/types';
import type { ProgramFileRequest, ProgramFileReadRequest, ProgramFileBytesRequest } from '@hanger-source/dsh-reactive-client/types';

declare module '@deepseek-ai/cordis' {
  interface Context { reactivePrograms: ProgramRemoteService; }
}
declare module '@deepseek-ai/dsh-typert-protocol' {
  interface RemoteErrorDetailsMap {
    'reactive/human-participant-required': Record<string, never>;
  }
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
  private readonly rethrow = (error: unknown): never => throwProgramFailure(this.failure(error));
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
  private participant(): ProgramParticipant {
    return resolveProgramParticipant(this.options.participant, this.principal());
  }
  private human(): ProgramParticipant {
    const identity = this.participant();
    if (identity.participant !== 'human') {
      throw new RemoteError('reactive/human-participant-required', 'This operation requires a human participant', {});
    }
    return identity;
  }
  private port(identity: ProgramParticipant) {
    return identity.participant === 'service' ? this.options.application.service(identity.actorId) : this.options.application;
  }
  private openReceipt(request: ProgramAddress, identity = this.participant()) {
    return this.port(identity).open(request.sessionId, { contract: request.program }).catch(this.rethrow);
  }
  private async openSession(request: ProgramSessionAddress, identity = this.participant()) {
    const handle = await this.port(identity).open(request.sessionId, { contract: request.program }, request.accessId).catch(this.rethrow);
    try {
      handle.signal.throwIfAborted();
      if (request.accessId !== handle.accessId)
        throw new ConversationAccessRevokedError(handle.id, handle.identity.workspaceId, 'access-expired');
      return handle;
    } catch (error) { handle.release(); return this.rethrow(error); }
  }
  private sessionIdentity(handle: ConversationHandle | ServiceConversation, identity: ProgramParticipant): ProgramIdentity {
    const definition = handle.definition;
    try { assertSharedProgramDefinition(definition); }
    catch (error) { return this.rethrow(error); }
    return { sessionId: handle.id, environmentId: handle.identity.environmentId, workspaceId: handle.identity.workspaceId,
      parentSessionId: handle.identity.parentSessionId, accessId: handle.accessId,
      program: programIdentity(definition), ...identity };
  }
  private callOptions(identity: ProgramParticipant, signal: AbortSignal) {
    return identity.participant === 'human' ? { actorId: identity.actorId, signal } : { signal };
  }
  /** 只限定本次读取；已经受理的执行和回执继续使用各自的原owner。 */
  private reading(handle: ServiceConversation, signal: AbortSignal): AbortSignal {
    return AbortSignal.any([signal, handle.signal]);
  }
  private async read<T>(handle: ServiceConversation, signal: AbortSignal, run: (signal: AbortSignal) => PromiseLike<T> | T): Promise<T> {
    const reading = this.reading(handle, signal);
    const value = await firstValueFrom(abortable(run, reading));
    reading.throwIfAborted();
    return value;
  }
  /** 每次内容读取和交付均复核原认证主体；不缓存授权或从wire接受actor。 */
  private contentAccess(method: 'fileRead' | 'fileReadBytes' | 'fileStat' | 'fileList' | 'fileChanges' | 'readImage', request: ProgramSessionAddress, principal: Principal): void {
    const action: `reactivePrograms/${typeof method}` = `reactivePrograms/${method}`;
    const decision = this.ctx.remoteAccess.decide({ principal, action,
      resources: [{ kind: 'session', id: SessionId(request.sessionId) }], args: { request } });
    if (decision.kind === 'deny') throw new RemoteError('remote-access/denied', `remote access: ${action}: access denied (${decision.reason})`, {
      action, reason: decision.reason,
    });
  }

  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async fileRead(request: ProgramFileReadRequest, signal: AbortSignal): Promise<WorkspaceFileText> {
    request = deepFreeze(structuredClone(request));
    const principal = this.principal();
    const handle = await this.openSession(request);
    try {
      return await this.read(handle, signal, async reading => {
        this.contentAccess('fileRead', request, principal);
        const value = await handle.files.readText(request.path, request.range, { signal: reading });
        reading.throwIfAborted(); this.contentAccess('fileRead', request, principal);
        return value;
      }).catch(this.rethrow);
    } finally { handle.release(); }
  }

  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async fileReadBytes(request: ProgramFileBytesRequest, signal: AbortSignal): Promise<WorkspaceFileBytes> {
    request = deepFreeze(structuredClone(request));
    const principal = this.principal();
    const handle = await this.openSession(request);
    try {
      return await this.read(handle, signal, async reading => {
        this.contentAccess('fileReadBytes', request, principal);
        const value = await handle.files.readBytes(request.path, request.range, { signal: reading });
        reading.throwIfAborted(); this.contentAccess('fileReadBytes', request, principal);
        return value;
      }).catch(this.rethrow);
    } finally { handle.release(); }
  }

  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async fileStat(request: ProgramFileRequest, signal: AbortSignal): Promise<WorkspaceFileStat> {
    request = deepFreeze(structuredClone(request));
    const principal = this.principal();
    const handle = await this.openSession(request);
    try {
      return await this.read(handle, signal, async reading => {
        this.contentAccess('fileStat', request, principal);
        const value = await handle.files.stat(request.path, { signal: reading });
        reading.throwIfAborted(); this.contentAccess('fileStat', request, principal);
        return value;
      }).catch(this.rethrow);
    } finally { handle.release(); }
  }

  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async fileList(request: ProgramFileRequest, signal: AbortSignal): Promise<WorkspaceDirectoryListing> {
    request = deepFreeze(structuredClone(request));
    const principal = this.principal();
    const handle = await this.openSession(request);
    try {
      return await this.read(handle, signal, async reading => {
        this.contentAccess('fileList', request, principal);
        const value = await handle.files.list(request.path, { signal: reading });
        reading.throwIfAborted(); this.contentAccess('fileList', request, principal);
        return value;
      }).catch(this.rethrow);
    } finally { handle.release(); }
  }

  /** ready只证明本代watch已登记；重连不声称补回上一代丢失的文件事件。 */
  @Remote({ mode: 'stream', resources: { session: request => SessionId(request.sessionId) } })
  async *fileChanges(request: ProgramFileRequest, signal: AbortSignal): AsyncIterable<WorkspaceFileWatchFrame> {
    request = deepFreeze(structuredClone(request));
    const principal = this.principal();
    const handle = await this.openSession(request);
    signal = this.reading(handle, signal);
    const inbox = new Inbox<WorkspaceFileWatchFrame>(1024 * 1024);
    const abort = (): void => inbox.fail(signal.reason);
    signal.addEventListener('abort', abort, { once: true });
    let subscription: Subscription | undefined;
    try {
      signal.throwIfAborted(); this.contentAccess('fileChanges', request, principal);
      const watch = handle.files.watch(request.path, { signal });
      subscription = watch.changes$.subscribe({
        next: frame => {
          try { signal.throwIfAborted(); this.contentAccess('fileChanges', request, principal); inbox.push(frame); }
          catch (error) { inbox.fail(error); }
        },
        error: (error: unknown) => inbox.fail(signal.aborted ? signal.reason : error),
        complete: () => signal.aborted ? inbox.fail(signal.reason) : inbox.end(),
      });
      for await (const frame of inbox) {
        signal.throwIfAborted(); this.contentAccess('fileChanges', request, principal);
        yield frame;
      }
      signal.throwIfAborted();
    } catch (error) { this.rethrow(error); }
    finally {
      signal.removeEventListener('abort', abort); subscription?.unsubscribe(); inbox.end();
      // Handle退出撤下本次观察；真实I/O与unwatch仍由NativeFiles的原owner drain。
      handle.release();
    }
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
  private wireExecutionFailure(value: ExecutionFailure): ProgramExecutionFailure {
    return { ...value, inputs: this.wireInputs(value.inputs),
      unentered: value.unentered.map(input => ({ ...input, inputs: this.wireInputs(input.inputs) })) };
  }
  private wirePreparation(value: ModelPreparation | null): ProgramModelPreparation | null {
    return value === null ? null : { ...value, inputs: this.wireInputs(value.inputs),
      admissions: value.admissions.map(admission => ({ ...admission, continuation: {
        ...admission.continuation, inputs: this.wireInputs(admission.continuation.inputs),
      } })) };
  }
  private wireActivity(value: ConversationActivity): JsonValue {
    return z.json().parse({ ...value,
      failures: value.failures.map(failure => this.wireExecutionFailure(failure)),
      requests: Object.fromEntries(Object.entries(value.requests).map(([id, request]) => [id, {
        ...request, failures: request.failures.map(failure => this.wireExecutionFailure(failure)),
      }])),
    });
  }
  /** 直接消费原始事实；只核验 JSON 字段，不逆编码或再次解析业务 input。 */
  private occurrence(value: ConversationEvent): ProgramConversationEvent {
    if (value.kind === 'operation.follow.changed') return { ...value,
      follow: z.json().parse(value.follow), inputs: this.wireInputs(value.inputs) };
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
    if (value.kind === 'goal.changed') return { ...value,
      goal: value.goal === null ? null : { ...value.goal, inputs: this.wireInputs(value.goal.inputs) } };
    return structuredClone(value);
  }

  /** 显式注册已有工作目录；不选择 Environment 或创建会话。 */
  @Remote
  async createWorkspace(request: ProgramWorkspaceRegistration, signal: AbortSignal): Promise<ProgramWorkspace> {
    const identity = this.participant();
    signal.throwIfAborted();
    const workspace = await this.port(identity).createWorkspace(request).catch(this.rethrow);
    return this.visibleWorkspace(workspace);
  }

  /** 在获准的既有 Workspace 中，以指定 Environment 创建工作会话。 */
  @Remote({ resources: { session: _request => undefined, workspace: request => WorkspaceId(request.workspaceId) } })
  async create(request: ProgramCreateRequest, signal: AbortSignal): Promise<ProgramIdentity> {
    const identity = this.participant();
    signal.throwIfAborted();
    const { environmentId, program, ...options } = request;
    const handle = await this.port(identity).create(environmentId, options, { contract: program }).catch(this.rethrow);
    try { return this.sessionIdentity(handle, identity); }
    finally { handle.release(); }
  }

  /** 凭明确引用补齐框架已持久化但尚未完成的创建。 */
  @Remote({ resources: { session: _request => undefined, workspace: request => WorkspaceId(request.workspaceId) } })
  async completeCreation(request: ProgramCompleteCreationRequest, signal: AbortSignal): Promise<ProgramIdentity> {
    const identity = this.participant();
    signal.throwIfAborted();
    const { program, ...reference } = request;
    const handle = await this.port(identity).completeCreation(reference, { contract: program }).catch(this.rethrow);
    try { return this.sessionIdentity(handle, identity); }
    finally { handle.release(); }
  }

  /** 只读打开当前程序，不把观察变成模型唤醒。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async open(request: ProgramAddress, signal: AbortSignal): Promise<ProgramIdentity> {
    const identity = this.participant();
    signal.throwIfAborted();
    const handle = await this.openReceipt(request, identity);
    try { return this.sessionIdentity(handle, identity); }
    finally { handle.release(); }
  }

  /** 显式恢复真实宿主；只读 open 不承担激活语义。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async resume(request: ProgramAddress, signal: AbortSignal): Promise<ProgramIdentity> {
    const identity = this.participant();
    signal.throwIfAborted();
    const handle = await this.port(identity).resume(request.sessionId, { contract: request.program }).catch(this.rethrow);
    try { return this.sessionIdentity(handle, identity); }
    finally { handle.release(); }
  }

  /** 发现仅返回获准的 Workspace 与其获准成员；global endpoint grant 不授予内容。 */
  @Remote
  async describe(signal: AbortSignal): Promise<readonly ProgramWorkspace[]> {
    const identity = this.participant();
    signal.throwIfAborted();
    const workspaces = await this.port(identity).describe().catch(this.rethrow);
    return workspaces.filter(workspace => this.ctx.remoteAccess.allows({ kind: 'workspace', id: WorkspaceId(workspace.workspaceId) }))
      .map(workspace => this.visibleWorkspace(workspace));
  }

  /** 暂停原生 Goal；它不取消外部业务操作。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async pause(request: ProgramSessionAddress, signal: AbortSignal): Promise<ProgramGoalControl> {
    this.human();
    signal.throwIfAborted();
    const handle = await this.options.application.open(request.sessionId, { contract: request.program }, request.accessId).catch(this.rethrow);
    try {
      signal.throwIfAborted();
      // 短命句柄持续持有到原生控制结算；transport 退出不能提前释放其回执 owner。
      const goal = await handle.pause().catch(this.rethrow);
      return { id: goal.id, revision: goal.revision, phase: goal.phase };
    }
    finally { handle.release(); }
  }

  /** 取消原生处理；真实业务 owner 的 body 不随网络调用结束。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async cancel(request: ProgramSessionAddress, signal: AbortSignal): Promise<ProgramCancelResult> {
    this.human();
    signal.throwIfAborted();
    const handle = await this.options.application.open(request.sessionId, { contract: request.program }, request.accessId).catch(this.rethrow);
    try { signal.throwIfAborted(); return await handle.cancel().catch(this.rethrow); }
    finally { handle.release(); }
  }

  /** 原始提交继续由 ApplicationHost 解析、准入、持久化及关联。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async submit(request: ProgramSubmitRequest, signal: AbortSignal): Promise<ProgramInputReceipt> {
    const identity = this.participant();
    signal.throwIfAborted();
    const handle = await this.openSession(request, identity);
    // wire 只证明 JSON；动态环境的 scope 合同继续由 Host 唯一 parse，不在 remote 重复转换。
    try { return await handle.submit(request.submission, this.callOptions(identity, signal)).catch(this.rethrow); }
    finally { handle.release(); }
  }

  /** 认证主体只上传到获准会话；正文编码及 staged receipt 由同一 Host 入口处理。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async uploadFile(request: ProgramUploadFileRequest, signal: AbortSignal): Promise<ProgramFileUploadValue> {
    const identity = this.participant();
    signal.throwIfAborted();
    const handle = await this.openSession(request, identity);
    try {
      return await handle.uploadEncodedFile({ data: request.data, ...(request.name === undefined ? {} : { name: request.name }) },
        this.callOptions(identity, signal)).result.catch(this.rethrow);
    } finally { handle.release(); }
  }

  /** 核对原 requestId 的真实受理/消费事实；查询不唤醒模型或重投输入。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async requestStatus(request: ProgramRequestAddress, signal: AbortSignal): Promise<ProgramRequestStatus> {
    signal.throwIfAborted();
    const handle = await this.openSession(request);
    try {
      signal.throwIfAborted();
      return await this.read(handle, signal, signal => handle.requestStatus(request.requestId, { signal })).catch(this.rethrow);
    }
    finally { handle.release(); }
  }

  /** 一个 Session 连接承载当前状态与新事实；快照不伪造新的请求事件。 */
  @Remote({ mode: 'stream', resources: { session: request => SessionId(request.sessionId) } })
  async *progress(request: ProgramSessionAddress, signal: AbortSignal): AsyncIterable<ProgramProgressFrame> {
    const handle = await this.openSession(request);
    signal = this.reading(handle, signal);
    const inbox = new Inbox<ProgramProgressFrame>(1024 * 1024);
    const subscriptions: Subscription[] = [];
    const abort = (): void => inbox.fail(signal.reason);
    signal.addEventListener('abort', abort, { once: true });
    try {
      signal.throwIfAborted();
      // open 等待期间发生的 start 已进入同一 Conversation owner。此同步 cut 到订阅完成没有 await。
      const initial = handle.progressSnapshot();
      const observation: WireObservation = initial.status === 'ready' ? { ...initial, value: z.json().parse({
        ...initial.value, activity: this.wireActivity(initial.value.activity), preparation: this.wirePreparation(initial.value.preparation),
      }) }
        : initial.status === 'error' ? { ...initial, error: this.failure(initial.error) } : initial;
      inbox.push({ type: 'baseline', observation });
      let activity = initial.status === 'ready' ? initial.value.activity : undefined;
      let preparation = initial.status === 'ready' ? initial.value.preparation : undefined;
      // 不订 progress$：那会在每个 raw chunk 先物化整个累计 prefix。
      subscriptions.push(handle.activity$.subscribe({
        next: value => {
          if (value === activity) return;
          activity = value;
          try { inbox.push({ type: 'activity', activity: this.wireActivity(value) }); }
          catch (error) { inbox.fail(error); }
        }, error: (error: unknown) => inbox.fail(signal.aborted ? signal.reason : error),
      }), handle.preparation$.subscribe({
        next: value => {
          if (value === preparation) return;
          preparation = value;
          try { inbox.push({ type: 'preparation', preparation: this.wirePreparation(value) }); }
          catch (error) { inbox.fail(error); }
        }, error: (error: unknown) => inbox.fail(signal.aborted ? signal.reason : error),
      }), handle.progressFrames$.subscribe({
        next: value => {
          try { inbox.push({ type: 'assistant', progress: z.json().parse(value) }); }
          catch (error) { inbox.fail(error); }
        }, error: (error: unknown) => inbox.fail(signal.aborted ? signal.reason : error),
      }), handle.rawEvents$.subscribe({
        next: value => {
          try { inbox.push({ type: 'event', event: this.occurrence(value) }); }
          catch (error) { inbox.fail(error); }
        }, error: (error: unknown) => inbox.fail(signal.aborted ? signal.reason : error),
        complete: () => signal.aborted ? inbox.fail(signal.reason) : inbox.end(),
      }));
      yield* inbox;
      signal.throwIfAborted();
    } catch (error) { this.rethrow(error); }
    finally {
      signal.removeEventListener('abort', abort); subscriptions.forEach(subscription => subscription.unsubscribe());
      inbox.end(); handle.release();
    }
  }

  /** 借用同一会话读侧；冷安全 cut、监听及原生分页均归 Host journal。 */
  @Remote({ mode: 'stream', resources: { session: request => SessionId(request.sessionId) } })
  async *journal(request: ProgramJournalOpenRequest, signal: AbortSignal): AsyncIterable<ProgramJournalFrame> {
    const handle = await this.openSession(request);
    try {
      const { sessionId: _sessionId, program: _program, accessId: _accessId, ...window } = request;
      const reading = this.reading(handle, signal);
      for await (const frame of handle.journal.follow(window, { signal: reading })) {
        reading.throwIfAborted();
        yield frame;
      }
      reading.throwIfAborted();
    } catch (error) { this.rethrow(error); }
    finally { handle.release(); }
  }

  /** 只读原会话内正式引用的图片，由 Controller 核验附件身份。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async readImage(request: ProgramImageAttachmentRequest, signal: AbortSignal): Promise<ProgramImageAttachmentValue> {
    request = deepFreeze(structuredClone(request));
    const principal = this.principal();
    const handle = await this.openSession(request);
    try {
      return await this.read(handle, signal, async reading => {
        this.contentAccess('readImage', request, principal);
        const image = await handle.attachments.readImage(request.attachmentId, { signal: reading });
        reading.throwIfAborted(); this.contentAccess('readImage', request, principal);
        return image;
      }).catch(this.rethrow);
    } finally { handle.release(); }
  }

  /** 同一 Program 权限边界内的原生冷安全分页，不唤醒 Agent。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async journalPage(request: ProgramJournalPageRequest, signal: AbortSignal): Promise<ProgramJournalPage> {
    const handle = await this.openSession(request);
    try {
      const { sessionId: _sessionId, program: _program, accessId: _accessId, ...page } = request;
      return await this.read(handle, signal, signal => handle.journal.page(page, { signal })).catch(this.rethrow);
    } finally { handle.release(); }
  }

  /** 订阅已编码的当前视图；业务 source 连接仍归 Program owner。 */
  @Remote({ mode: 'stream', resources: { session: request => SessionId(request.sessionId) } })
  async *view(request: ProgramViewRequest, signal: AbortSignal): AsyncIterable<ProgramViewFrame> {
    const handle = await this.openSession(request);
    let first = true;
    try {
      yield* frames(handle.observeWireView(request.name), this.reading(handle, signal), value => {
        const observation: WireObservation = value.status === 'error'
          ? { status: 'error', generation: value.generation, error: this.failure(value.error) }
          : value.status === 'ready' ? { ...value, value: z.json().parse(value.value) } : value;
        const frame: ProgramViewFrame = { type: first ? 'baseline' : 'observation', observation }; first = false; return frame;
      });
    }
    catch (error) { this.rethrow(error); }
    finally { handle.release(); }
  }

  /** 流拥有真实会话句柄；开场交付绑定，释放流才结束这次只读持有。 */
  @Remote({ mode: 'stream', resources: { session: request => SessionId(request.sessionId) } })
  async *resolveAction(request: ProgramActionBindingRequest, signal: AbortSignal): AsyncIterable<ProgramActionBinding> {
    signal.throwIfAborted();
    const handle = await this.openSession(request);
    signal = this.reading(handle, signal);
    try {
      signal.throwIfAborted();
      const definition = surfaceSlotDefinition(handle.surface, 'actions', request.name);
      if (!definition) throw new Error('REACTIVE_ACTION_SLOT_UNKNOWN');
      assertActionIdentity(request.contract, definition);
      const binding = handle.resolveAction(request.name, request.scope);
      if (binding.eligibility) {
        yield* frames(binding.eligibility.observations$, signal, value => ({
          bindingId: binding.bindingId, contract: actionIdentity(definition), scope: z.json().parse(binding.scope),
          eligibility: value.status === 'error'
            ? { status: 'error', generation: value.generation, error: this.failure(value.error) }
            : value.status === 'ready' ? { ...value, value: z.json().parse(value.value) } : value,
        }));
        signal.throwIfAborted();
        return;
      }
      yield { bindingId: binding.bindingId, contract: actionIdentity(definition), scope: z.json().parse(binding.scope) };
      if (!signal.aborted) await firstValueFrom(merge(
        defer(() => { signal.throwIfAborted(); return fromEvent(signal, 'abort'); }),
        handle.activity$.pipe(ignoreElements(), endWith(undefined)),
      ).pipe(take(1)));
      signal.throwIfAborted();
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
  actionInvocation(request: ProgramInvocationLookupRequest, signal: AbortSignal): Promise<ProgramActionStart> {
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
    const identity = this.participant();
    signal.throwIfAborted();
    const handle = await this.openSession(request, identity);
    try {
      signal = this.reading(handle, signal);
      signal.throwIfAborted();
      const binding = handle.resolveOperation(request.name, this.callOptions(identity, signal));
      signal.throwIfAborted();
      return { operation: { kind: binding.definition.kind, version: binding.definition.version }, command: z.json().parse(binding.command) };
    } catch (error) { return this.rethrow(error); }
    finally { handle.release(); }
  }

  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async editQueuedInput(request: ProgramInputEditRequest, signal: AbortSignal): Promise<ProgramInputReceipt> {
    const identity = this.participant();
    signal.throwIfAborted();
    const handle = await this.openSession(request, identity);
    try { return await handle.editQueuedInput(request.requestId, request.content, this.callOptions(identity, signal)).catch(this.rethrow); }
    finally { handle.release(); }
  }

  /** 只读原 command 的已登记参与；Host 取精确持久 link，不激活 Agent 或 register。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async readOperationFeedback(request: ProgramOperationFeedbackRequest, signal: AbortSignal): Promise<ProgramOperationFeedbackRead> {
    const identity = this.participant();
    const handle = await this.openSession(request, identity);
    try {
      const definition = this.options.application.operationDefinition(request.operation.kind, request.operation.version);
      const value = await this.read(handle, signal, signal => handle.readOperationFeedback(definition, request.command,
        request.participationId, this.callOptions(identity, signal))).catch(this.rethrow);
      if (value.status === 'unknown') return value;
      if (value.status === 'rejected') return { ...value, reason: z.json().parse(value.reason) };
      const reference = z.json().parse(value.reference);
      if (value.status === 'pending') return { status: 'pending', reference };
      const node = encodeOperationFeedback(definition, value.node);
      return { status: 'ready', reference, node: { id: node.id, payload: z.json().parse(node.payload) } };
    } finally { handle.release(); }
  }

  /** 只按既有业务 command 查受理事实；unknown 不重发 Action。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async locateOperation(request: ProgramOperationLocateRequest, signal: AbortSignal): Promise<ProgramOperationLocation> {
    const identity = this.participant();
    const handle = await this.openSession(request, identity);
    try {
      const definition = this.options.application.operationDefinition(request.operation.kind, request.operation.version);
      const location = await this.read(handle, signal, signal => handle.locateOperation(definition, request.command, this.callOptions(identity, signal))).catch(this.rethrow);
      return location.status === 'accepted' ? { status: 'accepted', reference: z.json().parse(location.reference),
        state: z.json().parse(z.encode(definition.state, location.state)) }
        : location.status === 'rejected' ? { status: 'rejected', reason: z.json().parse(location.reason) } : location;
    } finally { handle.release(); }
  }

  /** 连接实际 owner 的当前事实，只有 ready 才编码同一 Operation state schema。 */
  @Remote({ mode: 'stream', resources: { session: request => SessionId(request.sessionId) } })
  async *observeOperation(request: ProgramOperationRequest, signal: AbortSignal): AsyncIterable<ProgramViewFrame> {
    const identity = this.participant();
    const handle = await this.openSession(request, identity);
    signal = this.reading(handle, signal);
    try {
      const definition = this.options.application.operationDefinition(request.operation.kind, request.operation.version);
      const operation = await this.read(handle, signal, signal => handle.attachOperation(definition, request.reference, this.callOptions(identity, signal)));
      let first = true;
      yield* frames(operation.observations$, signal, observed => {
        const observation: WireObservation = observed.status === 'ready'
          ? { ...observed, value: z.json().parse(z.encode(definition.state, observed.value)) }
          : observed.status === 'error' ? { ...observed, error: this.failure(observed.error) } : observed;
        const frame: ProgramViewFrame = { type: first ? 'baseline' : 'observation', observation }; first = false; return frame;
      });
    } catch (error) { this.rethrow(error); }
    finally { handle.release(); }
  }

  /** 原调用已签发的实际Operation读侧；不重新open会话或按任意reference取得目标。 */
  @Remote({ mode: 'stream', resources: { session: request => SessionId(request.sessionId) } })
  async *observeInvocationOperation(request: ProgramInvocationOperationRequest, signal: AbortSignal): AsyncIterable<ProgramViewFrame> {
    signal = AbortSignal.any([signal, this.actions.signal]);
    const operation = await this.actions.operation(request, this.principal(), signal);
    let first = true;
    try {
      yield* frames(operation.observations$, signal, observed => {
        const observation: WireObservation = observed.status === 'ready'
          ? { ...observed, value: z.json().parse(z.encode(operation.definition.state, observed.value)) }
          : observed.status === 'error' ? { ...observed, error: this.failure(observed.error) } : observed;
        const frame: ProgramViewFrame = { type: first ? 'baseline' : 'observation', observation }; first = false; return frame;
      });
    } catch (error) { this.rethrow(error); }
  }

  /** 宿主重新核验原 follow 与业务事实；不接受网络端自报 ready。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async notifyOperation(request: ProgramOperationNotification, signal: AbortSignal): Promise<ProgramOperationNotificationResult> {
    const identity = this.participant();
    signal.throwIfAborted();
    const handle = await this.openSession(request, identity);
    try { return await handle.notifyOperation(request.followId, this.callOptions(identity, signal)).catch(this.rethrow); }
    finally { handle.release(); }
  }

  /** 同一原生问题投影与实际前台请求，不维护第二份问题注册。 */
  @Remote({ mode: 'stream', resources: { session: request => SessionId(request.sessionId) } })
  async *questions(request: ProgramSessionAddress, signal: AbortSignal): AsyncIterable<ProgramQuestionsFrame> {
    const handle = await this.openSession(request);
    let first = true;
    try {
      yield* frames(handle.human.values$, this.reading(handle, signal), ({ questions, foreground, replies }) => {
        const wireQuestion = <Q extends { readonly origin: HumanQuestionOrigin }>(question: Q) => ({
          ...question, origin: { ...question.origin, inputs: this.wireInputs(question.origin.inputs),
            continuation: question.origin.continuation === null ? null : { ...question.origin.continuation,
              inputs: this.wireInputs(question.origin.continuation.inputs) } },
        });
        const frame: ProgramQuestionsFrame = { type: first ? 'baseline' : 'questions', value: {
          questions: { ...questions, active: questions.active.map(wireQuestion), settled: questions.settled.map(wireQuestion) },
          foreground: foreground.map(wireQuestion),
          replies: replies.map(wireQuestion),
        } };
        first = false; return frame;
      });
    } catch (error) { this.rethrow(error); }
    finally { handle.release(); }
  }

  /** bool 只说明前台受理或 continued 排队，不是答案落盘和工具结算。 */
  @Remote({ resources: { session: request => SessionId(request.sessionId) } })
  async answerQuestion(request: ProgramAnswerRequest, signal: AbortSignal): Promise<boolean> {
    const { actorId } = this.human();
    const handle = await this.options.application.open(request.sessionId, { contract: request.program }, request.accessId).catch(this.rethrow);
    try { return await handle.answerQuestion(request.callId, request.answer, { actorId, signal }).catch(this.rethrow); }
    finally { handle.release(); }
  }

  /** claim 真实前台等待；流退出由原生 Human owner 释放 claim。 */
  @Remote({ mode: 'stream', resources: { session: request => SessionId(request.sessionId) } })
  async *attachWait(request: ProgramQuestionRequest, signal: AbortSignal): AsyncIterable<ProgramWaitFrame> {
    const { actorId } = this.human();
    const handle = await this.options.application.open(request.sessionId, { contract: request.program }, request.accessId).catch(this.rethrow);
    try { yield* handle.attachWait(request.callId, { actorId, signal: this.reading(handle, signal) }); }
    catch (error) { this.rethrow(error); }
    finally { handle.release(); }
  }
}

/** 仅连接物理流与已有 Observable；历史、重连与游标继续由 Gateway 拥有。 */
async function* frames<T, F>(source: Observable<T>, signal: AbortSignal, project: (value: T) => F): AsyncIterable<F> {
  const inbox = new Inbox<F>(1024 * 1024);
  const abort = (): void => inbox.fail(signal.reason);
  signal.addEventListener('abort', abort, { once: true });
  let subscription: Subscription | undefined;
  try {
    signal.throwIfAborted();
    subscription = source.subscribe({
      next: value => {
        try { signal.throwIfAborted(); inbox.push(project(value)); }
        catch (error) { inbox.fail(error); }
      },
      error: (error: unknown) => inbox.fail(signal.aborted ? signal.reason : error),
      complete: () => signal.aborted ? inbox.fail(signal.reason) : inbox.end(),
    });
    signal.throwIfAborted();
    yield* inbox;
    signal.throwIfAborted();
  } finally { signal.removeEventListener('abort', abort); subscription?.unsubscribe(); inbox.end(); }
}
