import { createElement, type ReactElement } from 'react';
import { catchError, firstValueFrom, forkJoin, of, throwError } from 'rxjs';
import { invokeBoundAction, type ActionCallContext, type ActionExecutionContext, type ActionReference, type ActionOperationEvent, type Invocation } from '../actions/contract.ts';
import type { Json } from '../conversations/protocol.ts';
import type { OperationAccessContext, OperationBindingReference, OperationContract, OperationFeedbackRead, OperationFeedbackPayload, OperationReference, OperationFollowMode } from '../operations/contract.ts';
import { Scope, batch, type Cell } from '../reactivity/scope.ts';
import { abortable } from '../reactivity/observable.ts';
import { ResourceValue } from '../resources/value.ts';
import type { ResourceReader } from '../resources/reader.ts';
import type { ResourceSource } from '../resources/source.ts';
import type { Eligibility } from '../resources/eligibility.ts';
import { scopedResourceSharing } from '../resources/sharing.ts';
import { type Observation } from '../resources/observation.ts';
import { ProgramHookProvider, type ProgramHookOwner } from './hooks.ts';
import type { ProgramContract, ProgramSession, ProgramWorkspace } from './definition.ts';
import { programElement } from './implementation.ts';
import { type ProgramImplementation } from './definition.ts';
import { decodeProgramDeclaration, type OfferDeclaration, type FollowDeclaration, type SkillDeclaration, type HumanInputDeclaration, type NativeToolDeclaration } from './declarations.ts';
import { collectProgramFrame, type ProgramFrame } from './frame.ts';
import { createProgramOutputs } from '../surfaces/outputs.ts';
import { createDeclarationRenderer, type DeclarationRenderer } from './renderer.ts';

type ContextBinding = { readonly scope: Scope; readonly state: Cell<Observation<string>>; readonly value: ResourceValue<string> };
export interface BoundOffer {
  readonly name: string;
  readonly action: ActionReference;
  readonly enabled: Cell<boolean>;
  readonly modulePath: readonly string[];
  follow?: { readonly when: 'settled' | 'feedback'; readonly scope: Json; readonly mode: OperationFollowMode };
}
export interface BoundFollow {
  readonly name: string;
  readonly description: string;
  readonly operation: OperationBindingReference;
  readonly when: 'settled' | 'feedback';
  readonly mode: OperationFollowMode;
  readonly scope: Json;
  readonly enabled: Cell<boolean>;
  readonly eligibility?: ResourceReader<Eligibility>;
}
export interface ProgramInstanceOptions {
  /** 普通程序调用的真实服务身份与宿主信号；不由组件自报原生身份。 */
  execution(action: ActionReference): ActionExecutionContext;
  /** 核验绑定后把原始执行对象交还宿主；宿主可据其私有身份签认调用。 */
  admit?(action: ActionReference, input: unknown, call: ActionCallContext<unknown>, originalExecution: ActionExecutionContext,
    signal: AbortSignal):
    void | PromiseLike<void>;
  operation?(action: ActionReference, event: ActionOperationEvent, execution: ActionExecutionContext): void | PromiseLike<void>;
  validateOffer?(offer: OfferDeclaration): void;
  validateFollow?(follow: FollowDeclaration): void;
  validateNativeTool?(tool: NativeToolDeclaration): void;
  operationAccess?(operation: OperationBindingReference): OperationAccessContext;
  readOperationFeedback?<D extends OperationContract>(operation: OperationBindingReference<D>, id: string,
    access: OperationAccessContext): Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>>;
}
type TrustedExecution = { readonly action: ActionReference; readonly allowed: () => boolean };
type StepBoundary = { readonly check: (signal: AbortSignal) => void | Promise<void>; readonly signal: AbortSignal };

/** 会话拥有组件树、声明快照与查询；原生执行及已受理业务拥有各自生存期。 */
export class ProgramInstance<P extends ProgramContract> implements ProgramHookOwner {
  readonly reactivityScope: Scope;
  readonly shareResource: <T>(source: ResourceSource<T>) => ResourceReader<T>;
  readonly revision: Cell<number>;
  readonly contexts = new Map<string, ResourceValue<string>>();
  readonly offers = new Map<string, BoundOffer>();
  readonly follows = new Map<string, BoundFollow>();
  readonly outputs: ReturnType<typeof createProgramOutputs<P['surface']>>;
  readonly signal: AbortSignal;
  #contexts = new Map<string, ContextBinding>();
  #boundaries = new Set<StepBoundary>();
  #trusted = new WeakMap<ActionExecutionContext, TrustedExecution>();
  #failure?: { error: unknown };
  #failed = new AbortController();
  #renderer: DeclarationRenderer;
  #frame?: ProgramFrame;
  #humanInput?: HumanInputDeclaration;
  #element: ReactElement;
  #mounted = false;

  constructor(
    readonly scope: Scope,
    program: ProgramImplementation<P>,
    conversation: ProgramSession<P>,
    workspace: ProgramWorkspace,
    private readonly options: ProgramInstanceOptions,
  ) {
    this.revision = scope.state(0);
    this.signal = AbortSignal.any([scope.signal, this.#failed.signal]);
    this.reactivityScope = scope.child();
    this.shareResource = scopedResourceSharing(this.reactivityScope, 'REACTIVE_PROGRAM_RESOURCE_CLEANUP_FAILED');
    const releaseReactivity = () => {
      // 父撤销先交完子signal的资源释放，再由原child清理汇总；不能提前消费该回执。
      if (scope.disposed) return;
      try { this.reactivityScope.dispose(this.signal.reason); }
      catch (error) { scope.observerError(error); }
    };
    this.signal.addEventListener('abort', releaseReactivity, { once: true });
    this.reactivityScope.own(() => this.signal.removeEventListener('abort', releaseReactivity));
    this.outputs = createProgramOutputs(scope, program.definition.surface);
    this.#renderer = createDeclarationRenderer({
      decode: decodeProgramDeclaration,
      signal: this.signal,
      onCommit: frame => this.commit(collectProgramFrame(frame)),
      onError: error => this.fail(error),
    });
    scope.own(() => this.#renderer.dispose());
    this.#element = createElement(ProgramHookProvider, { owner: this }, programElement(program, conversation, workspace));
  }
  mount() {
    this.assertActive();
    if (this.#mounted) throw new Error('REACTIVE_PROGRAM_ALREADY_MOUNTED');
    this.#mounted = true;
    try { this.#renderer.render(this.#element); }
    catch (error) {
      this.fail(error);
      if (this.#failure) throw this.#failure.error;
      throw error;
    }
  }
  get skills(): readonly SkillDeclaration[] { return this.#frame?.skills ?? []; }
  get humanInput(): HumanInputDeclaration | undefined { return this.#humanInput; }
  get nativeTools(): Readonly<Record<string, NativeToolDeclaration>> { return this.#frame?.nativeTools ?? {}; }

  private commit(frame: ProgramFrame) {
    this.assertActive();
    this.outputs.validate(frame);
    for (const offer of Object.values(frame.offers)) this.options.validateOffer?.(offer);
    for (const follow of Object.values(frame.follows)) this.options.validateFollow?.(follow);
    for (const tool of Object.values(frame.nativeTools)) this.options.validateNativeTool?.(tool);
    batch(() => {
      for (const [name, binding] of this.#contexts) if (!Object.hasOwn(frame.contexts, name)) {
        this.#contexts.delete(name); this.contexts.delete(name); binding.scope.dispose();
      }
      for (const [name, declaration] of Object.entries(frame.contexts)) {
        let binding = this.#contexts.get(name);
        if (!binding) {
          const scope = this.scope.child();
          const state = scope.state<Observation<string>>({ status: 'pending', generation: frame.revision });
          binding = { scope, state, value: new ResourceValue(scope, state) };
          this.#contexts.set(name, binding); this.contexts.set(name, binding.value);
        }
        const observed: Observation<Json> = declaration.observation ??
          { status: 'ready', generation: frame.revision, value: declaration.value!, observedAt: Date.now() };
        binding.state.set(observed.status === 'ready' ? {
          ...observed, value: typeof observed.value === 'string' ? observed.value : JSON.stringify(observed.value),
        } : observed);
      }
      for (const name of this.offers.keys()) if (!Object.hasOwn(frame.offers, name)) this.offers.delete(name);
      for (const declaration of Object.values(frame.offers)) this.offer(declaration);
      for (const name of this.follows.keys()) if (!Object.hasOwn(frame.follows, name)) this.follows.delete(name);
      for (const declaration of Object.values(frame.follows)) this.follow(declaration);
      // leaf 每帧重建；只有准入函数变化才替换策略身份，避免无关帧释放原生 claim。
      if (this.#humanInput?.admitResponder !== frame.humanInput?.admitResponder) this.#humanInput = frame.humanInput;
      this.#frame = frame;
      this.revision.update(value => value + 1);
      // 来源订阅会运行用户 producer；它同步重入时必须取得当前声明与执行许可。
      this.outputs.apply(frame);
    });
  }
  private offer(declaration: OfferDeclaration) {
    const previous = this.offers.get(declaration.name);
    if (previous?.action === declaration.action) { previous.enabled.set(declaration.enabled); previous.follow = declaration.follow; }
    else this.offers.set(declaration.name, { name: declaration.name, action: declaration.action,
      enabled: this.scope.state(declaration.enabled), modulePath: declaration.modulePath ?? Object.freeze([]), follow: declaration.follow });
  }
  private follow(declaration: FollowDeclaration) {
    const previous = this.follows.get(declaration.name);
    if (previous?.operation === declaration.operation && previous.when === declaration.when && previous.mode === declaration.mode
      && previous.eligibility === declaration.eligibility
      && previous.description === declaration.description && scopeIdentity(previous.scope) === scopeIdentity(declaration.scope)) {
      previous.enabled.set(declaration.enabled);
    } else this.follows.set(declaration.name, { name: declaration.name, description: declaration.description,
      operation: declaration.operation, when: declaration.when, mode: declaration.mode, scope: declaration.scope,
      ...declaration.eligibility === undefined ? {} : { eligibility: declaration.eligibility },
      enabled: this.scope.state(declaration.enabled) });
  }
  private assertActive() {
    this.scope.assertActive();
    if (this.#failure) throw this.#failure.error;
  }
  /** native 步骤边界冲刷同步声明；资源准备继续由原生异步装配等待。 */
  flush() {
    this.assertActive();
    try { this.#renderer.flush(); }
    catch (error) {
      // fatal可以在commit中同步abort renderer；公开边界保留真正的程序失败原因。
      if (this.#failure) throw this.#failure.error;
      throw error;
    }
    this.assertActive();
  }
  fail(error: unknown): void {
    if (this.#failure || this.scope.disposed) return;
    this.#failure = { error };
    this.#failed.abort(error);
    batch(() => {
      for (const binding of this.#contexts.values())
        binding.state.set({ status: 'error', generation: binding.state.peek().generation + 1, error });
      this.outputs.fail(error);
      this.revision.update(value => value + 1);
    });
  }
  beforeStep(check: (signal: AbortSignal) => void | Promise<void>): () => void {
    this.assertActive();
    const controller = new AbortController();
    const boundary = { check, signal: controller.signal };
    const release = this.reactivityScope.own(() => {
      this.#boundaries.delete(boundary);
      controller.abort();
      if (this.scope.disposed || this.signal.aborted) return;
      this.revision.update(value => value + 1);
    });
    this.#boundaries.add(boundary); this.revision.update(value => value + 1);
    return release;
  }
  async prepare(signal: AbortSignal): Promise<void> {
    for (;;) {
      signal.throwIfAborted(); this.flush();
      const boundaries = [...this.#boundaries];
      if (!boundaries.length) return;
      const same = () => boundaries.length === this.#boundaries.size && boundaries.every(boundary => this.#boundaries.has(boundary));
      const changed = new AbortController();
      const stop = this.scope.effect(() => {
        this.revision.get();
        if (!same()) changed.abort();
      });
      try {
        // 同次检查并列连接；任一失败由forkJoin立即撤其余本次等待，不撤销登记身份。
        await firstValueFrom(forkJoin(boundaries.map(boundary => {
          const checking = AbortSignal.any([signal, this.signal, boundary.signal, changed.signal]);
          return abortable(current => boundary.check(AbortSignal.any([checking, current])), checking).pipe(catchError((error: unknown) => {
            signal.throwIfAborted(); this.assertActive();
            return this.#boundaries.has(boundary) ? throwError(() => error) : of(undefined);
          }));
        })));
      } catch (error) {
        signal.throwIfAborted(); this.assertActive();
        if (!changed.signal.aborted || error !== changed.signal.reason) throw error;
      } finally { stop(); }
      signal.throwIfAborted(); this.flush();
      if (same()) return;
    }
  }
  /** 只信任由本会话的宿主入口签发的原始执行对象。 */
  async admit(action: ActionReference, input: unknown, call: ActionCallContext<unknown>, originalExecution: ActionExecutionContext,
    admissionSignal: AbortSignal): Promise<void> {
    this.flush();
    const trusted = this.#trusted.get(originalExecution);
    if (trusted?.action !== action) throw new Error('REACTIVE_ACTION_EXECUTION_UNTRUSTED');
    originalExecution.signal.throwIfAborted();
    if (!trusted.allowed()) throw new Error('REACTIVE_ACTION_NOT_AVAILABLE');
    const changed = new AbortController();
    const signal = AbortSignal.any([this.signal, originalExecution.signal, admissionSignal, changed.signal]);
    signal.throwIfAborted();
    const stop = this.scope.effect(() => {
      this.revision.get();
      try {
        if (this.#trusted.get(originalExecution) !== trusted) throw new Error('REACTIVE_ACTION_EXECUTION_UNTRUSTED');
        if (!trusted.allowed()) throw new Error('REACTIVE_ACTION_NOT_AVAILABLE');
      } catch (error) { changed.abort(error); }
    });
    try {
      await firstValueFrom(abortable(() => this.options.admit?.(action, input, call, originalExecution, signal), signal));
    } finally { stop(); }
    signal.throwIfAborted();
    this.flush();
    originalExecution.signal.throwIfAborted();
    if (this.#trusted.get(originalExecution) !== trusted) throw new Error('REACTIVE_ACTION_EXECUTION_UNTRUSTED');
    if (!trusted.allowed()) throw new Error('REACTIVE_ACTION_NOT_AVAILABLE');
  }
  execution(action: ActionReference): ActionExecutionContext {
    this.assertActive();
    const execution = this.options.execution(action);
    this.#trusted.set(execution, { action, allowed: () => !this.scope.disposed && !this.#failure });
    return execution;
  }
  /** 查询使用宿主服务身份，不取得当前工具或 Goal 的执行权限。 */
  operationAccess(operation: OperationBindingReference): OperationAccessContext {
    this.assertActive();
    if (!this.options.operationAccess) throw new Error('REACTIVE_OPERATION_HOST_REQUIRED');
    return this.options.operationAccess(operation);
  }
  /** 已登记参与保留原绑定；当前组件不再公开它也不改写原关联。 */
  readOperationFeedback<D extends OperationContract>(operation: OperationBindingReference<D>, id: string,
    access: OperationAccessContext): Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>> {
    this.assertActive();
    if (!this.options.readOperationFeedback) throw new Error('REACTIVE_OPERATION_HOST_REQUIRED');
    return this.options.readOperationFeedback(operation, id, access);
  }
  /** 回执属于已经受理的执行；声明退出不抹去其业务操作关联。 */
  async operation(action: ActionReference, event: ActionOperationEvent, execution: ActionExecutionContext): Promise<void> {
    const trusted = this.#trusted.get(execution);
    if (trusted?.action !== action) throw new Error('REACTIVE_ACTION_EXECUTION_UNTRUSTED');
    if (event.phase !== 'receipt' && event.phase !== 'not-started') {
      this.flush();
      execution.signal.throwIfAborted();
      if (!trusted.allowed()) throw new Error('REACTIVE_ACTION_NOT_AVAILABLE');
    }
    if (!this.options.operation) throw new Error('REACTIVE_OPERATION_HOST_REQUIRED');
    await this.options.operation(action, event, execution);
    if (event.phase === 'prepare') {
      this.flush();
      execution.signal.throwIfAborted();
      if (!trusted.allowed()) throw new Error('REACTIVE_ACTION_NOT_AVAILABLE');
    }
  }
  /** 工具与认证参与入口复用同一绑定；订阅 Invocation 不会重新调用业务。 */
  invoke(action: ActionReference, input: unknown, execution: ActionExecutionContext, allowed: () => boolean,
    admissionSignal?: AbortSignal): Invocation<unknown, Json> {
    this.flush();
    this.#trusted.set(execution, { action, allowed });
    return invokeBoundAction(action, input, execution, admissionSignal);
  }
}

function scopeIdentity(scope: Json): string {
  return JSON.stringify(scope, (_key, item: unknown) => item !== null && typeof item === 'object' && !Array.isArray(item)
    ? Object.fromEntries(Object.keys(item).sort().map(key => [key, (item as Record<string, unknown>)[key]])) : item);
}
