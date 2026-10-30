import { useMemo } from 'react';
import { BehaviorSubject } from 'rxjs';
import { approvalSurface, approvalPendingRequestSchema, implementAction, ResourceReader,
  type ActionCallContext, type ApprovalPendingRequest, type ApprovalResponderAdmission,
  type Observation, type ProgramSessionObservation } from '@hanger-source/dsh-reactive';
import { Expose, useAction, useProgramSession, useScope, type ModuleImplementation } from '@hanger-source/dsh-reactive/program';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';
import { implementNativeModule } from '../programs/modules.ts';
import { nativeSessionAccess, type NativeSessionAccess } from '../conversations/session-access.ts';
import { ownNativeValue } from '../native-capability.ts';
import { createNativeApproval, type NativeApproval, type NativeApprovalOptions,
  type ApprovalOutcome, type ApprovalRequestEvent } from './native.ts';

export interface NativeApprovalModuleOptions extends Pick<NativeApprovalOptions, 'policy' | 'require'> {
  readonly admitResponder: (admission: ApprovalResponderAdmission) => void | Promise<void>;
}

interface PendingAnswer {
  readonly request: ApprovalRequestEvent;
  readonly summary: ApprovalPendingRequest;
  readonly signal: AbortSignal;
  settle(outcome: ApprovalOutcome): void;
}

/** 只保存当前 callback 的 Promise 关联；观察连接不拥有请求或模块寿命。 */
class ApprovalChannel {
  readonly #waiting = new Map<string, PendingAnswer>();
  readonly #state = new BehaviorSubject<Observation<readonly ApprovalPendingRequest[]>>({ status: 'pending', generation: 0 });
  readonly pending = new ResourceReader({ peek: () => this.#state.value, observations$: this.#state.asObservable() });
  #scope?: Scope;
  #access?: NativeSessionAccess;
  constructor(readonly source: ProgramSessionObservation, readonly capability: NativeApproval,
    readonly admitResponder: NativeApprovalModuleOptions['admitResponder']) {}

  get access(): NativeSessionAccess | undefined { return this.#access; }

  private assertCurrent(): NativeSessionAccess {
    if (!this.#scope || !this.#access) throw new Error('REACTIVE_APPROVAL_MODULE_UNAVAILABLE');
    this.#scope.assertActive();
    const access = nativeSessionAccess(this.source);
    if (access !== this.#access) throw new Error('REACTIVE_NATIVE_SESSION_MISMATCH');
    this.capability.forSession(this.source);
    return access;
  }

  open(scope: Scope, access: NativeSessionAccess): void {
    if (this.#scope) throw new Error('REACTIVE_APPROVAL_MODULE_ALREADY_MOUNTED');
    this.#scope = scope; this.#access = access;
    this.assertCurrent();
    this.publish();
  }

  close(outcome: 'cancelled' | 'unavailable'): void {
    this.#scope = undefined; this.#access = undefined;
    for (const pending of [...this.#waiting.values()]) pending.settle(outcome);
    this.#state.next({ status: 'error', generation: this.#state.value.generation + 1,
      error: new Error('REACTIVE_APPROVAL_MODULE_UNAVAILABLE') });
  }

  private publish(): void {
    if (!this.#scope) return;
    this.#state.next({ status: 'ready', generation: this.#state.value.generation + 1, observedAt: Date.now(),
      value: Object.freeze([...this.#waiting.values()].map(pending => pending.summary)) });
  }

  accepts(request: ApprovalRequestEvent): boolean {
    if (!this.#access || this.#access.liveAgent() !== request.agent) return false;
    const access = this.assertCurrent();
    return access.liveAgent() === request.agent;
  }

  receive(request: ApprovalRequestEvent): Promise<ApprovalOutcome> {
    const access = this.assertCurrent();
    if (access.liveAgent() !== request.agent) throw new Error('REACTIVE_APPROVAL_REQUEST_SOURCE_MISMATCH');
    const signal = request.signal ? AbortSignal.any([request.signal, access.signal]) : access.signal;
    if (signal.aborted) return Promise.resolve('cancelled');
    const completion = Promise.withResolvers<ApprovalOutcome>();
    const summary = ownNativeValue(approvalPendingRequestSchema.parse({ transportId: globalThis.crypto.randomUUID(),
      toolName: request.toolName, callId: request.callId ?? null, reason: request.reason ?? null,
      displayReason: request.displayReason ?? null }));
    const cancel = () => pending.settle('cancelled');
    const pending: PendingAnswer = { request, summary, signal, settle: outcome => {
      if (this.#waiting.get(summary.transportId) !== pending) return;
      // 原子取走等待者，先移除监听再交付；同步重入和并发答复都不能再次取得它。
      this.#waiting.delete(summary.transportId);
      signal.removeEventListener('abort', cancel);
      completion.resolve(outcome);
      this.publish();
    } };
    this.#waiting.set(summary.transportId, pending);
    signal.addEventListener('abort', cancel, { once: true });
    if (signal.aborted) cancel();
    // Promise、关联表与取消监听全部就位后才向消费者发布。
    this.publish();
    return completion.promise;
  }

  async answer(input: { readonly transportId: string; readonly decision: 'allowed-once' | 'rejected' },
    call: ActionCallContext<{ readonly sessionId: string }>): Promise<{ status: 'submitted' }> {
    const access = this.assertCurrent();
    if (call.source.kind !== 'participant' || call.source.participant !== 'human' || !call.source.actorId.trim()) {
      throw new Error('REACTIVE_APPROVAL_HUMAN_RESPONDER_REQUIRED');
    }
    if (call.sessionId !== access.sessionId || call.scope.sessionId !== access.sessionId) {
      throw new Error('REACTIVE_APPROVAL_SESSION_MISMATCH');
    }
    const pending = this.#waiting.get(input.transportId);
    if (!pending) throw new Error('REACTIVE_APPROVAL_REQUEST_UNAVAILABLE');
    const assertPending = () => {
      const current = this.assertCurrent();
      call.signal.throwIfAborted();
      if (this.#waiting.get(input.transportId) !== pending || pending.signal.aborted
        || current !== access || current.liveAgent() !== pending.request.agent) {
        throw new Error('REACTIVE_APPROVAL_REQUEST_UNAVAILABLE');
      }
    };
    assertPending();
    const signal = AbortSignal.any([call.signal, this.#scope!.signal, pending.signal]);
    signal.throwIfAborted();
    await this.admitResponder({ identity: ownNativeValue(this.source.identity), request: pending.summary,
      decision: input.decision, actorId: call.source.actorId, signal });
    signal.throwIfAborted();
    assertPending();
    pending.settle(input.decision);
    return { status: 'submitted' };
  }
}

/** 每个 App 工厂创建独立实例；同一原生 capability 静态安装，实际 Program 提交后才登记响应路由。 */
export function createNativeApprovalModule(options: NativeApprovalModuleOptions): ModuleImplementation<typeof approvalSurface> {
  if (typeof options.admitResponder !== 'function') throw new Error('REACTIVE_APPROVAL_RESPONDER_ADMISSION_REQUIRED');
  const admitResponder = options.admitResponder;
  const routes = new Set<ApprovalChannel>();
  const capability = createNativeApproval({ policy: options.policy, require: options.require,
    async answer(request) {
      for (const route of routes) if (route.accepts(request)) return route.receive(request);
      return 'unavailable';
    },
  });
  function Component() {
    const source = useProgramSession();
    const channel = useMemo(() => new ApprovalChannel(source, capability, admitResponder), [source]);
    const implementation = useMemo(() => implementAction(approvalSurface.actions.answer,
      { execute: (input, call) => channel.answer(input, call) }), [channel]);
    const answer = useAction(implementation, () => ({ sessionId: source.identity.sessionId }), [source]);
    useScope(scope => {
      capability.forSession(source);
      const access = nativeSessionAccess(source);
      if ([...routes].some(route => route.access?.sessionId === access.sessionId)) {
        throw new Error('REACTIVE_APPROVAL_SESSION_ALREADY_MOUNTED');
      }
      const close = (outcome: 'cancelled' | 'unavailable') => {
        routes.delete(channel);
        access.signal.removeEventListener('abort', cancel);
        channel.close(outcome);
      };
      const cancel = () => close('cancelled');
      routes.add(channel);
      try { channel.open(scope, access); }
      catch (error) { close('unavailable'); throw error; }
      access.signal.addEventListener('abort', cancel, { once: true });
      if (access.signal.aborted) cancel();
      return () => close('unavailable');
    }, [channel, source]);
    return Expose({ surface: approvalSurface,
      value: { views: { pending: channel.pending }, actions: { answer } } });
  }
  return implementNativeModule(approvalSurface, { native: [capability], Component });
}
