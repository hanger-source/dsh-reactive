import type { ReactNode } from 'react';
import type { SlotCore, SlotComponent } from '@deepseek-ai/dsh-client-ui-slots';
import type { ToolCallViewProps } from '@deepseek-ai/dsh-client-ui-tool/client';
import { readActionPresentation, type ActionContract, type ActionPresentation, type ToolActionBinding } from '@hanger-source/dsh-reactive';
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
    if (views.has(definition.action.name)) throw new Error('REACTIVE_NATIVE_VIEW_ACTION_DUPLICATE');
    views.set(definition.action.name, definition);
  }
  const registrations = new Map<string, { action: ActionContract; users: Set<object>; release(): void }>();
  const consumers = new Set<() => void>();
  const errors = new Subject<{ readonly sessionId: string; readonly error: unknown }>();
  let disposed = false;
  const acquire = (owner: object, binding: ToolActionBinding) => {
    const definition = views.get(binding.action);
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
    const release = slots.register({ name: 'tool.call.toolview', key: binding.toolName }, ActionView);
    registrations.set(binding.toolName, { action: definition.action, users: new Set([owner]), release });
  };
  const releaseOwner = (owner: object, keep: ReadonlySet<string> = new Set()) => {
    for (const [name, registration] of registrations) {
      if (keep.has(name)) continue;
      registration.users.delete(owner);
      if (!registration.users.size) { registration.release(); registrations.delete(name); }
    }
  };
  return {
    errors$: errors.asObservable(),
    attach(source, options = {}) {
      if (disposed) throw new Error('REACTIVE_NATIVE_VIEWS_DISPOSED');
      options.signal?.throwIfAborted();
      const owner = {};
      let subscription: { unsubscribe(): void } | undefined;
      let stopped = false;
      const stop = () => {
        if (stopped) return;
        stopped = true;
        options.signal?.removeEventListener('abort', stop);
        subscription?.unsubscribe();
        releaseOwner(owner);
        consumers.delete(stop);
      };
      consumers.add(stop);
      options.signal?.addEventListener('abort', stop, { once: true });
      subscription = source.actionManifest$.subscribe({
        next: bindings => {
          if (stopped) return;
          // 已见工具的历史调用仍沿相同视图读取；声明撤下不改写历史工具的展示身份。
          try { for (const binding of bindings) acquire(owner, binding); }
          catch (error) { stop(); errors.next({ sessionId: source.id, error }); }
        },
        error: error => { stop(); errors.next({ sessionId: source.id, error }); },
        complete: stop,
      });
      if (stopped) subscription.unsubscribe();
      return stop;
    },
    dispose() {
      if (disposed) return;
      disposed = true;
      for (const stop of [...consumers]) stop();
      errors.complete();
    },
    [Symbol.dispose]() { this.dispose(); },
  };
}
