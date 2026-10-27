import { assertProgramDefinition, type HumanQuestion,
  type HumanForegroundQuestion, type HumanAnswer, type ProgramContract, type ProgramSurface,
  type OperationContract, type OperationHandle, type OperationState, type ResourceReadable,
  type CancellationOptions, type Invocation, type InvocationActivity, type Json,
  type ViewValue, type Observation, type HumanInputState, type ToolActionBinding } from '@hanger-source/dsh-reactive';
import { Scope } from '@hanger-source/dsh-reactive/reactivity';
import type { RemoteProgramSession } from '@hanger-source/dsh-reactive-client';
import type { SessionBinding } from '@deepseek-ai/dsh-api-session-controller/client';
import type { HostObservable } from '@deepseek-ai/dsh-client-ui-slots';
import { defer, fromEvent, takeUntil, type Observable } from 'rxjs';
import { nativeSource, observeNativeResource, observeNativeSource, type NativeReadable } from './sources.ts';

/**
 * 一个明确 Program 与真实 Client Session generation 的借用绑定。
 * SDK handle / SessionReference 由取得它们的 owner 释放；本对象只拥有 UI 观察。
 */
export class NativeSession<P extends ProgramContract> implements Disposable {
  readonly scope = new Scope();
  readonly id: string;
  readonly questions: HostObservable<Observation<HumanInputState>>;
  readonly foregroundQuestions: HostObservable<Observation<readonly HumanForegroundQuestion[]>>;
  readonly actionManifest$: Observable<readonly ToolActionBinding[]>;

  /** @internal 通过 bindNativeSession 建立并核验关联。 */
  constructor(readonly definition: P, readonly native: SessionBinding, private readonly sdk: RemoteProgramSession<P>) {
    assertProgramDefinition(definition);
    this.id = native.sessionId;
    this.assertActive();
    const release = native.ctx.effect(() => () => this.dispose(), 'reactive native Session binding');
    this.scope.own(() => release());
    this.questions = this.guard(observeNativeSource(this.scope, sdk.questions$));
    this.foregroundQuestions = this.guard(observeNativeSource(this.scope, sdk.foregroundQuestions$));
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
    return nativeSource({
      peek: () => { this.assertActive(); return source.getSnapshot(); },
      subscribe: (accept: (value: T) => void) => {
        this.assertActive();
        let stop: (() => void) | undefined;
        const release = this.scope.own(() => stop?.());
        try { stop = source.subscribe(() => { if (!this.scope.disposed) accept(source.getSnapshot()); }); }
        catch (error) { release(); throw error; }
        if (this.scope.disposed) stop();
        return release;
      },
    });
  }

  resource<T>(resource: ResourceReadable<T>): HostObservable<Observation<T>> { return this.source(resource); }

  view<K extends keyof ProgramSurface<P>['views'] & string>(name: K): HostObservable<Observation<ViewValue<ProgramSurface<P>['views'][K]>>> {
    this.assertActive();
    return this.resource(this.sdk.viewResource(name));
  }

  operation<D extends OperationContract>(operation: OperationHandle<D>): HostObservable<Observation<OperationState<D>>> {
    this.assertActive();
    return this.guard(observeNativeResource(this.scope, operation.observations$));
  }

  invocation<T, O extends Json>(invocation: Invocation<T, O>): HostObservable<Observation<InvocationActivity<O>>> {
    this.assertActive();
    return this.guard(observeNativeSource(this.scope, invocation.activity$));
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

  /** 只撤观察和 UI 贡献；不释放借入句柄，不取消 Action 或 Operation。 */
  dispose(): void { this.scope.dispose(); }
  [Symbol.dispose](): void { this.dispose(); }
}

export function bindNativeSession<P extends ProgramContract>(program: P, native: SessionBinding, sdk: RemoteProgramSession<P>): NativeSession<P> {
  assertProgramDefinition(program);
  return new NativeSession(program, native, sdk);
}
