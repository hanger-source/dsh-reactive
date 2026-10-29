/// <reference lib="esnext.disposable" preserve="true" />

import { assertProgramDefinition, type HumanQuestion,
  type HumanForegroundQuestion, type HumanAnswer, type ProgramContract, type ProgramSurface,
  type OperationContract, type OperationHandle, type OperationState, type ResourceReadable,
  type CancellationOptions, type Invocation, type InvocationActivity, type Json,
  type ViewValue, type Observation, type HumanInputState, type HumanSnapshot, type HumanQuestionSnapshot, type HumanWaitFrame,
  type ConversationProgressSnapshot, type RequestHandle, type RequestSnapshot, type ProgramParsedInput,
  shareResource, mapResource, ResourceReader, type SessionFiles, type FileResource, type FileWatch,
  type SessionAttachments, type SessionImageAttachment,
  type ToolActionBinding } from '@hanger-source/dsh-reactive';
import { Scope } from '@hanger-source/dsh-reactive/reactivity';
import type { RemoteProgramSession } from '@hanger-source/dsh-reactive-client';
import { assertProgramSessionConnection } from '@hanger-source/dsh-reactive-client';
import type { SessionBinding } from '@deepseek-ai/dsh-api-session-controller/client';
import type { HostObservable } from '@deepseek-ai/dsh-client-ui-slots';
import { defer, fromEvent, takeUntil, type Observable } from 'rxjs';
import { nativeSource, type NativeReadable } from './sources.ts';

export interface NativeHumanQuestion {
  readonly callId: string;
  readonly state: HostObservable<Observation<HumanQuestionSnapshot>>;
  answer(answer: HumanAnswer, options?: CancellationOptions): Promise<boolean>;
  attachWait(options?: CancellationOptions): AsyncIterable<HumanWaitFrame>;
}

/** 固定原请求的只读消费；当前状态与后续新事件不授予执行控制权。 */
export interface NativeRequestHandle<T = Json> {
  readonly sessionId: string;
  readonly requestId: string;
  readonly state: HostObservable<Observation<RequestSnapshot>>;
  readonly events$: RequestHandle<T>['events$'];
}

interface NativeFileWatchLease {
  watch?: FileWatch;
  failure?: { readonly error: unknown };
  readonly closed: Promise<void>;
}

/**
 * 一个明确 Program 与真实 Client Session generation 的借用绑定。
 * SDK handle / SessionReference 由取得它们的 owner 释放；本对象只拥有 UI 观察。
 */
export class NativeSession<P extends ProgramContract> implements Disposable, AsyncDisposable {
  readonly scope = new Scope();
  readonly id: string;
  readonly questions: HostObservable<Observation<HumanInputState>>;
  readonly foregroundQuestions: HostObservable<Observation<readonly HumanForegroundQuestion[]>>;
  readonly human: HostObservable<Observation<HumanSnapshot>>;
  readonly actionManifest$: Observable<readonly ToolActionBinding[]>;
  #questions = new Map<string, NativeHumanQuestion>();
  #requests = new Map<string, NativeRequestHandle<ProgramParsedInput<P>>>();
  #progress?: HostObservable<Observation<ConversationProgressSnapshot>>;
  #files?: SessionFiles;
  #attachments?: SessionAttachments;
  #attachmentReads = new Set<Promise<void>>();
  #fileWatches = new Set<NativeFileWatchLease>();
  #cleanupFailures = new Set<unknown>();
  #closing?: Promise<void>;

  /** @internal 通过 bindNativeSession 建立并核验关联。 */
  constructor(readonly definition: P, readonly native: SessionBinding, private readonly sdk: RemoteProgramSession<P>) {
    assertProgramDefinition(definition);
    this.id = native.sessionId;
    this.assertActive();
    const release = native.ctx.effect(() => () => this.dispose(), 'reactive native Session binding');
    this.scope.own(() => { void Promise.resolve(release()).catch(error => this.scope.observerError(error)); });
    this.scope.own(() => { this.#questions.clear(); this.#requests.clear(); });
    this.human = this.resource(sdk.human);
    const options = { signal: this.scope.signal, onObserverError: (error: unknown) => this.scope.observerError(error) };
    this.questions = this.resource(shareResource(sdk.human.observations$.pipe(mapResource(snapshot => snapshot.questions)), options));
    this.foregroundQuestions = this.resource(shareResource(sdk.human.observations$.pipe(mapResource(snapshot => snapshot.foreground)), options));
    this.actionManifest$ = defer(() => {
      this.assertActive();
      return sdk.actionManifest$;
    }).pipe(takeUntil(fromEvent(this.scope.signal, 'abort')));
  }

  /** 原生 generation 与 SDK 句柄各自仍须有效，不按当前页面选择重新寻址。 */
  assertActive(): void {
    this.scope.assertActive();
    if (this.native.ctx.sessions.binding(this.native.sessionId) !== this.native)
      throw new Error('REACTIVE_NATIVE_SESSION_GENERATION_INACTIVE');
    if (this.sdk.id !== this.id || this.sdk.definition !== this.definition)
      throw new Error('REACTIVE_NATIVE_SESSION_CONTRACT_MISMATCH');
    if (this.sdk.identity.participant !== 'human')
      throw new Error('REACTIVE_NATIVE_HUMAN_PARTICIPANT_REQUIRED');
    assertProgramSessionConnection(this.sdk, this.native.ctx.get('connection'));
  }

  private options(options: CancellationOptions = {}): CancellationOptions {
    this.assertActive();
    options.signal?.throwIfAborted();
    return { signal: options.signal ? AbortSignal.any([this.scope.signal, options.signal]) : this.scope.signal };
  }

  /** 持有的原资源仍由原 scope 决定有效性，UI 不再发起一次查询。 */
  source<T>(source: NativeReadable<T>): HostObservable<T> {
    this.assertActive();
    return this.guard(nativeSource(source));
  }

  private guard<T>(source: HostObservable<T>): HostObservable<T> {
    return Object.freeze({
      getSnapshot: () => { this.assertActive(); const value = source.getSnapshot(); this.assertActive(); return value; },
      subscribe: (notify: () => void) => {
        this.assertActive();
        let stop: (() => void) | undefined;
        const release = this.scope.own(() => stop?.());
        try { stop = source.subscribe(() => { if (!this.scope.disposed) notify(); }); }
        catch (error) { release(); throw error; }
        if (this.scope.disposed) stop();
        return release;
      },
    });
  }

  resource<T>(resource: ResourceReadable<T>): HostObservable<Observation<T>> { return this.source(resource); }

  /** 显式取得进展入口才借入SDK连接；仅绑定原生UI不额外要求进展权限。 */
  get progress(): HostObservable<Observation<ConversationProgressSnapshot>> {
    this.assertActive();
    return this.#progress ??= this.resource(this.sdk.progress);
  }

  /** 图片读取借用同一 SDK；UI 只撤销并等待从本入口发起的读取。 */
  get attachments(): SessionAttachments {
    this.assertActive();
    return this.#attachments ??= Object.freeze({
      readImage: (attachmentId, options) => {
        const cancellation = this.options(options);
        const completion = Promise.withResolvers<void>();
        this.#attachmentReads.add(completion.promise);
        const finish = () => {
          this.#attachmentReads.delete(completion.promise);
          completion.resolve();
        };
        let pending: Promise<SessionImageAttachment>;
        try { pending = this.sdk.attachments.readImage(attachmentId, cancellation); }
        catch (error) { finish(); throw error; }
        return pending.then(value => {
          cancellation.signal?.throwIfAborted(); this.assertActive(); return value;
        }).finally(finish);
      },
    } satisfies SessionAttachments);
  }

  /** 借用原SDK的文件事实；UI只拥有本读侧以及从本入口创建的watch。 */
  get files(): SessionFiles {
    this.assertActive();
    if (this.#files) return this.#files;
    const files = this.sdk.files;
    const resources = new WeakMap<object, unknown>();
    const resource = <T>(source: FileResource<T>): FileResource<T> => {
      this.assertActive();
      const previous = resources.get(source);
      if (previous) return previous as FileResource<T>;
      const reader = new ResourceReader({
        observations$: defer(() => { this.assertActive(); return source.observations$; }),
        peek: () => { this.assertActive(); const value = source.peek(); this.assertActive(); return value; },
      }, { signal: this.scope.signal, onObserverError: error => this.scope.observerError(error),
        onCleanupError: error => this.#cleanupFailures.add(error) });
      const borrowed = Object.assign(reader, { invalidate: () => { this.assertActive(); source.invalidate(); } });
      resources.set(source, borrowed);
      return borrowed;
    };
    return this.#files = Object.freeze({
      text: (path, range) => { this.assertActive(); return resource(files.text(path, range)); },
      metadata: path => { this.assertActive(); return resource(files.metadata(path)); },
      directory: path => { this.assertActive(); return resource(files.directory(path)); },
      readText: async (path, range, options) => {
        const value = await files.readText(path, range, this.options(options)); this.assertActive(); return value;
      },
      readBytes: async (path, range, options) => {
        const value = await files.readBytes(path, range, this.options(options)); this.assertActive(); return value;
      },
      stat: async (path, options) => {
        const value = await files.stat(path, this.options(options)); this.assertActive(); return value;
      },
      list: async (path, options) => {
        const value = await files.list(path, this.options(options)); this.assertActive(); return value;
      },
      watch: (path, options) => this.fileWatch(files, path, options),
    } satisfies SessionFiles);
  }

  private fileWatch(files: SessionFiles, path: string, options?: CancellationOptions): FileWatch {
    const cancellation = this.options(options);
    const completion = Promise.withResolvers<void>();
    const lease: NativeFileWatchLease = { closed: completion.promise };
    // 原watch构造中可以重入退出；先登记这次实际取得，退出方才能加入它的关闭回执。
    this.#fileWatches.add(lease);
    void lease.closed.catch(() => {});
    try {
      const watch = files.watch(path, cancellation);
      lease.watch = watch;
      // 只记原watch的失败身份，不复制帧；业务流失败不能掩盖随后不同的实际释放错误。
      const observation = watch.changes$.subscribe({ error: error => { lease.failure = { error }; } });
      const finish = () => {
        try { observation.unsubscribe(); } catch (error) { this.#cleanupFailures.add(error); }
        this.#fileWatches.delete(lease);
      };
      void watch.closed.then(() => {
        finish(); completion.resolve();
      }, error => {
        if (!lease.failure || error !== lease.failure.error) this.#cleanupFailures.add(error);
        finish(); completion.reject(error);
      });
      if (this.scope.disposed) void watch.dispose().catch(error => {
        if (!lease.failure || error !== lease.failure.error) this.#cleanupFailures.add(error);
      });
      return watch;
    } catch (error) {
      this.#fileWatches.delete(lease); completion.resolve();
      throw error;
    }
  }

  request(requestId: string): NativeRequestHandle<ProgramParsedInput<P>> {
    this.assertActive();
    let bound = this.#requests.get(requestId);
    if (!bound) {
      const request = this.sdk.request(requestId);
      bound = Object.freeze({ sessionId: request.sessionId, requestId: request.requestId,
        state: this.resource(request.state),
        events$: defer(() => { this.assertActive(); return request.events$; })
          .pipe(takeUntil(fromEvent(this.scope.signal, 'abort'))),
      });
      this.#requests.set(requestId, bound);
    }
    return bound;
  }

  async question(callId: string, options: CancellationOptions = {}): Promise<NativeHumanQuestion> {
    const question = await this.sdk.question(callId, this.options(options));
    this.assertActive();
    let bound = this.#questions.get(question.callId);
    if (!bound) {
      const controlOptions = (options?: CancellationOptions) => this.options(options);
      bound = Object.freeze({ callId: question.callId, state: this.resource(question.state),
        answer: (answer: HumanAnswer, options?: CancellationOptions) => question.answer(answer, controlOptions(options)),
        async *attachWait(options?: CancellationOptions) { yield* question.attachWait(controlOptions(options)); },
      });
      this.#questions.set(question.callId, bound);
    }
    return bound;
  }

  view<K extends keyof ProgramSurface<P>['views'] & string>(name: K): HostObservable<Observation<ViewValue<ProgramSurface<P>['views'][K]>>> {
    this.assertActive();
    return this.resource(this.sdk.viewResource(name));
  }

  operation<D extends OperationContract>(operation: OperationHandle<D>): HostObservable<Observation<OperationState<D>>> {
    this.assertActive();
    return this.resource(operation.state);
  }

  invocation<T, O extends Json, Source>(invocation: Invocation<T, O, Source>): HostObservable<Observation<InvocationActivity<O>>> {
    this.assertActive();
    return this.resource(invocation.activity);
  }

  readonly resolveAction: RemoteProgramSession<P>['resolveAction'] = async (name, scope, options) => {
    const action = await this.sdk.resolveAction(name, scope, this.options(options));
    this.assertActive();
    // 保留 exact bindingId / definition / parsed scope；只加本 UI generation 的受理边界。
    return Object.assign((...args: Parameters<typeof action>) => {
      this.assertActive();
      return action(args[0], this.options(args[1]));
    }, { definition: action.definition, bindingId: action.bindingId, scope: action.scope });
  };

  readonly submit: RemoteProgramSession<P>['submit'] = (input, options) => this.sdk.submit(input, this.options(options));
  readonly editQueuedInput: RemoteProgramSession<P>['editQueuedInput'] = (id, content, options) => this.sdk.editQueuedInput(id, content, this.options(options));
  /** UI 退出可阻止尚未分派的准备；分派后只结束等待，返回 SDK 的原调用对象。 */
  readonly uploadFile: RemoteProgramSession<P>['uploadFile'] = (data, options) => this.sdk.uploadFile(data, { ...options, ...this.options(options) });
  readonly readView: RemoteProgramSession<P>['readView'] = (name, options) => this.sdk.readView(name, this.options(options));
  readonly attachOperation: RemoteProgramSession<P>['attachOperation'] = (definition, reference, options) => this.sdk.attachOperation(definition, reference, this.options(options));
  readonly locateOperation: RemoteProgramSession<P>['locateOperation'] = (definition, command, options) => this.sdk.locateOperation(definition, command, this.options(options));
  readonly readOperationFeedback: RemoteProgramSession<P>['readOperationFeedback'] = (definition, command, id, options) => this.sdk.readOperationFeedback(definition, command, id, this.options(options));

  /** 问题保留其原始归属；页面切换不把另一会话的问题改投本会话。 */
  answer(question: HumanQuestion | HumanForegroundQuestion, answer: HumanAnswer, options?: CancellationOptions): Promise<boolean> {
    this.assertQuestion(question);
    return this.sdk.answerQuestion(question.callId, answer, this.options(options));
  }

  async *claim(question: HumanQuestion | HumanForegroundQuestion, options?: CancellationOptions) {
    this.assertQuestion(question);
    yield* this.sdk.attachWait(question.callId, this.options(options));
  }

  private assertQuestion(question: HumanQuestion | HumanForegroundQuestion) {
    this.assertActive();
    if (question.origin.sessionId !== this.id) throw new Error('REACTIVE_NATIVE_QUESTION_SESSION_MISMATCH');
  }

  /** 立即撤 UI 资格，等待本 UI 的图片读取与 watch 退出。 */
  dispose(): Promise<void> {
    if (this.#closing) return this.#closing;
    const completion = Promise.withResolvers<void>();
    this.#closing = completion.promise;
    void this.#closing.catch(() => {});
    const watches = [...this.#fileWatches];
    const attachmentReads = [...this.#attachmentReads];
    try { this.scope.dispose(); } catch (error) { this.#cleanupFailures.add(error); }
    void Promise.resolve().then(async () => {
      await Promise.all(attachmentReads);
      await Promise.all(watches.map(async lease => {
        const stopped = await Promise.allSettled([Promise.resolve().then(() => lease.watch?.dispose()), lease.closed]);
        for (const result of stopped) if (result.status === 'rejected' && (!lease.failure || result.reason !== lease.failure.error))
          this.#cleanupFailures.add(result.reason);
      }));
      if (this.#cleanupFailures.size === 1) throw [...this.#cleanupFailures][0];
      if (this.#cleanupFailures.size) throw new AggregateError([...this.#cleanupFailures], 'REACTIVE_NATIVE_SESSION_DISPOSAL_FAILED');
    }).then(completion.resolve, completion.reject);
    return this.#closing;
  }
  [Symbol.dispose](): void { void this.dispose().catch(error => this.scope.observerError(error)); }
  [Symbol.asyncDispose](): Promise<void> { return this.dispose(); }
}

export function bindNativeSession<P extends ProgramContract>(program: P, native: SessionBinding, sdk: RemoteProgramSession<P>): NativeSession<P> {
  assertProgramDefinition(program);
  return new NativeSession(program, native, sdk);
}
