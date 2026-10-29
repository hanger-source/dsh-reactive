/// <reference lib="esnext.disposable" preserve="true" />

import type { ReactNode } from 'react';
import type { SlotCore, SlotComponent } from '@deepseek-ai/dsh-client-ui-slots';
import type { ToolCallViewProps } from '@deepseek-ai/dsh-client-ui-tool/client';
import { actionIdentityKey, readActionPresentation, type ActionContract, type ActionPresentation, type ToolActionBinding } from '@hanger-source/dsh-reactive';
import { Subject, type Observable } from 'rxjs';

export interface NativeActionViewProps<D extends ActionContract> {
  /** 原生 callId、阶段、文件导航、详情与 renderer 标准 props 原样保留。 */
  readonly native: ToolCallViewProps;
  /** 只从本次持久工具结果读取；不把当前模块绑定当作历史调用身份。 */
  readonly result: ActionPresentation<D> | undefined;
}
export interface NativeActionView<D extends ActionContract = ActionContract> {
  readonly action: D;
  readonly render: SlotComponent<NativeActionViewProps<D>>;
}
export function defineNativeActionView<D extends ActionContract>(
  action: D, render: SlotComponent<NativeActionViewProps<D>>,
): NativeActionView<D> { return Object.freeze({ action, render }); }

/** 本地或远程 SDK 的同一只读出口；无需原生宿主或业务 UI 依赖。 */
export interface NativePresentationSource {
  readonly id: string;
  readonly actionManifest$: Observable<readonly ToolActionBinding[]>;
}
export interface NativeActionViews {
  readonly errors$: Observable<{ readonly sessionId: string; readonly error: unknown }>;
  /** 多会话共享 keyed 注册；观察结束只撤下本消费者的贡献。 */
  attach(source: NativePresentationSource, options?: { readonly signal?: AbortSignal }): () => void;
  dispose(): void;
  [Symbol.dispose](): void;
}

/**
 * 按真实 manifest 将可复用 Action 视图装入 dsh 原生 Tool slot。
 * 原生 slots 继续拥有组件、props、错误监督、child slots 与渲染生命周期。
 */
export function createNativeActionViews(
  slots: Pick<SlotCore, 'register'>,
  definitions: readonly NativeActionView<any>[],
): NativeActionViews {
  const views = new Map<string, NativeActionView<any>>();
  for (const definition of definitions) {
    const key = actionIdentityKey(definition.action);
    if (views.has(key)) throw new Error('REACTIVE_NATIVE_VIEW_ACTION_DUPLICATE');
    views.set(key, definition);
  }
  type Registration = { action: ActionContract; users: Set<object>; release: (() => void) | undefined };
  const registrations = new Map<string, Registration>();
  const consumers = new Set<() => void>();
  const errors = new Subject<{ readonly sessionId: string; readonly error: unknown }>();
  let disposed = false;
  let updating = 0;
  const completeErrors = () => { if (disposed && updating === 0) errors.complete(); };
  // 只沿当前同步调用栈排空错误交付；不延后原生注册/释放或另建调度队列。
  const update = (run: () => void) => {
    updating++;
    try { run(); } finally { updating--; completeErrors(); }
  };
  const releaseUnused = (name: string, registration: Registration) => {
    // register尚未返回时保留同一entry，允许同步重入的另一消费者共享本次真实注册。
    if (registration.users.size || !registration.release || registrations.get(name) !== registration) return;
    registrations.delete(name);
    const release = registration.release;
    registration.release = undefined;
    release();
  };
  const acquire = (owner: object, binding: ToolActionBinding) => {
    const definition = views.get(actionIdentityKey({ name: binding.action, version: binding.actionVersion }));
    if (!definition) return;
    const existing = registrations.get(binding.toolName);
    if (existing) {
      if (existing.action !== definition.action) throw new Error('REACTIVE_NATIVE_VIEW_TOOL_CONFLICT');
      existing.users.add(owner);
      return;
    }
    function ActionView(native: ToolCallViewProps): ReactNode {
      const result = native.phase === 'result'
        ? readActionPresentation(definition!.action, native.block.meta) : undefined;
      return definition!.render({ native, result });
    }
    const registration: Registration = { action: definition.action, users: new Set([owner]), release: undefined };
    registrations.set(binding.toolName, registration);
    try { registration.release = slots.register({ name: 'tool.call.toolview', key: binding.toolName }, ActionView); }
    catch (error) {
      if (registrations.get(binding.toolName) === registration) registrations.delete(binding.toolName);
      throw error;
    }
    releaseUnused(binding.toolName, registration);
  };
  const releaseOwner = (owner: object, keep: ReadonlySet<string> = new Set()) => {
    const failures: unknown[] = [];
    for (const [name, registration] of [...registrations]) {
      if (keep.has(name)) continue;
      if (!registration.users.delete(owner)) continue;
      try { releaseUnused(name, registration); } catch (error) { failures.push(error); }
    }
    if (failures.length === 1) throw failures[0];
    if (failures.length) throw new AggregateError(failures, 'REACTIVE_NATIVE_VIEW_RELEASE_FAILED');
  };
  return {
    errors$: errors.asObservable(),
    attach(source, options = {}) {
      if (disposed) throw new Error('REACTIVE_NATIVE_VIEWS_DISPOSED');
      const signal = options.signal;
      signal?.throwIfAborted();
      const owner = {};
      let subscription: { unsubscribe(): void } | undefined;
      let stopped = false;
      const stop = () => {
        if (stopped) return;
        stopped = true;
        signal?.removeEventListener('abort', aborted);
        consumers.delete(stop);
        update(() => {
          const failures: unknown[] = [];
          try { subscription?.unsubscribe(); } catch (error) { failures.push(error); }
          try { releaseOwner(owner); } catch (error) { failures.push(error); }
          if (failures.length === 1) throw failures[0];
          if (failures.length) throw new AggregateError(failures, 'REACTIVE_NATIVE_VIEW_CONSUMER_RELEASE_FAILED');
        });
      };
      const failed = (error: unknown) => update(() => {
        try { stop(); }
        catch (cleanup) { error = new AggregateError([error, cleanup], 'REACTIVE_NATIVE_VIEW_OBSERVATION_FAILED'); }
        errors.next({ sessionId: source.id, error });
      });
      const aborted = () => update(() => { try { stop(); } catch (error) { errors.next({ sessionId: source.id, error }); } });
      consumers.add(stop);
      signal?.addEventListener('abort', aborted, { once: true });
      subscription = source.actionManifest$.subscribe({
        next: bindings => {
          if (stopped) return;
          // 已见工具的历史调用仍沿相同视图读取；声明撤下不改写历史工具的展示身份。
          update(() => {
            try {
              for (const binding of bindings) {
                if (stopped) break;
                acquire(owner, binding);
              }
            }
            catch (error) { failed(error); }
          });
        },
        error: failed,
        complete: aborted,
      });
      if (stopped) subscription.unsubscribe();
      return stop;
    },
    dispose() {
      if (disposed) return;
      disposed = true;
      update(() => {
        const failures: unknown[] = [];
        for (const stop of [...consumers]) { try { stop(); } catch (error) { failures.push(error); } }
        if (failures.length === 1) throw failures[0];
        if (failures.length) throw new AggregateError(failures, 'REACTIVE_NATIVE_VIEWS_DISPOSAL_FAILED');
      });
    },
    [Symbol.dispose]() { this.dispose(); },
  };
}
