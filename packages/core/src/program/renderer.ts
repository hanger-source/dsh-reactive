import { createContext, createElement, type ReactElement, type ReactNode } from 'react';
import Reconciler from 'react-reconciler';
import { ConcurrentRoot, DefaultEventPriority, NoEventPriority } from 'react-reconciler/constants.js';

const declarationType = 'dsh:declaration';
interface DeclarationProps<T> { readonly value: T; }
interface Instance { value: unknown; children: Instance[]; }
interface Container { children: Instance[]; commit(values: readonly unknown[]): void; }

/** 帧和声明顺序不可变；声明引用的业务对象仍由调用者拥有。
 * revision 严格递增，冲刷中被替代的提交不向外发布。
 */
export interface Frame<T> { readonly revision: number; readonly declarations: readonly T[]; }
export interface DeclarationRendererOptions<T> {
  /** ReactNode 擦除泛型；程序层必须在这里核验声明品牌、种类与所属 Surface。 */
  decode(value: unknown): T;
  /** 冲刷同步工作和 Effect 后交付当前完整帧，revision 可以跳过被替代的提交。 */
  onCommit(frame: Frame<T>): void;
  onError(error: unknown): void;
  signal?: AbortSignal;
}
export interface DeclarationRenderer {
  render(element: ReactNode): void;
  /** 冲刷同步 React 工作、Effect 和已完成的 commit；不提升普通 setState/Transition 的优先级，也不等待业务资源。 */
  flush(): void;
  /** 撤下当前树；冲刷后发布当前帧，发布回调重入的后续 render 可以替代空帧。 */
  unmount(): void;
  /** 释放 React 树和 Effect，停止发布帧；可重复调用。 */
  dispose(): void;
}

/** 供类型化声明组件使用的叶子；宿主类型不暴露为业务协议。 */
export function declaration<T>(value: T): ReactElement<DeclarationProps<T>> {
  return createElement(declarationType, { value });
}

function unsupported(feature: string): never { throw new Error('REACTIVE_RENDERER_UNSUPPORTED: ' + feature); }
function append(parent: Container | Instance, child: Instance) {
  const previous = parent.children.indexOf(child);
  if (previous !== -1) parent.children.splice(previous, 1);
  parent.children.push(child);
}
function remove(parent: Container | Instance, child: Instance) {
  const index = parent.children.indexOf(child);
  if (index === -1) throw new Error('REACTIVE_RENDERER_CHILD_NOT_FOUND');
  parent.children.splice(index, 1);
}
function insert(parent: Container | Instance, child: Instance, before: Instance) {
  const previous = parent.children.indexOf(child);
  if (previous !== -1) parent.children.splice(previous, 1);
  const index = parent.children.indexOf(before);
  if (index === -1) throw new Error('REACTIVE_RENDERER_CHILD_NOT_FOUND');
  parent.children.splice(index, 0, child);
}

type HostConfig = Reconciler.HostConfig<typeof declarationType, DeclarationProps<unknown>, Container, Instance,
  never, never, never, never, never, Instance, Readonly<Record<string, never>>, never,
  ReturnType<typeof setTimeout>, -1, null, never, never, never, never, never>;
const hostContext = Object.freeze({});
let updatePriority = NoEventPriority;
const hostConfig: HostConfig = {
  rendererPackageName: '@hanger-source/dsh-reactive', rendererVersion: '0.1.0', extraDevToolsConfig: null,
  isPrimaryRenderer: true, supportsMutation: true, supportsPersistence: false, supportsHydration: false,
  supportsMicrotasks: true, scheduleMicrotask: queueMicrotask,
  scheduleTimeout: setTimeout, cancelTimeout: clearTimeout, noTimeout: -1,
  getCurrentUpdatePriority: () => updatePriority,
  setCurrentUpdatePriority: priority => { updatePriority = priority; },
  resolveUpdatePriority: () => updatePriority || DefaultEventPriority,
  trackSchedulerEvent() {}, resolveEventType: () => null, resolveEventTimeStamp: () => -1,
  shouldAttemptEagerTransition: () => false,
  getRootHostContext: () => hostContext, getChildHostContext: parent => parent,
  getPublicInstance: instance => instance,
  prepareForCommit: () => null,
  resetAfterCommit(container) {
    const values: unknown[] = [];
    const visit = (instance: Instance) => { values.push(instance.value); instance.children.forEach(visit); };
    container.children.forEach(visit);
    container.commit(Object.freeze(values));
  },
  createInstance(type, props) {
    if (type !== declarationType) throw new Error('REACTIVE_RENDERER_HOST_TYPE_INVALID: ' + type);
    if (!Object.hasOwn(props, 'value')) throw new Error('REACTIVE_RENDERER_DECLARATION_VALUE_REQUIRED');
    return { value: props.value, children: [] };
  },
  createTextInstance: () => unsupported('text'),
  shouldSetTextContent: () => false, finalizeInitialChildren: () => false,
  appendInitialChild: append, appendChild: append, appendChildToContainer: append,
  insertBefore: insert, insertInContainerBefore: insert,
  removeChild: remove, removeChildFromContainer: remove,
  commitUpdate(instance, _type, _previous, next) { instance.value = next.value; },
  clearContainer(container) { container.children.length = 0; },
  resetTextContent() {}, commitMount() {}, detachDeletedInstance() {},
  preparePortalMount: () => unsupported('portal'),
  getInstanceFromNode: () => null,
  beforeActiveInstanceBlur() {}, afterActiveInstanceBlur() {},
  prepareScopeUpdate: () => unsupported('scope'), getInstanceFromScope: () => null,
  NotPendingTransition: null, HostTransitionContext: createContext(null),
  resetFormInstance: () => unsupported('form'),
  requestPostPaintCallback: () => unsupported('post-paint'),
  bindToConsole: () => () => unsupported('server-console'),
  maySuspendCommit: () => false, maySuspendCommitOnUpdate: () => false, maySuspendCommitInSyncRender: () => false,
  preloadInstance: () => true,
  startSuspendingCommit: () => unsupported('suspended-commit'),
  suspendInstance: () => unsupported('suspended-commit'),
  suspendOnActiveViewTransition: () => unsupported('view-transition'),
  waitForCommitToBeReady: () => null, getSuspendedCommitReason: () => null,
  hideInstance: () => unsupported('hidden-declaration'), unhideInstance: () => unsupported('hidden-declaration'),
};
// 所有会话 root 共用同一个 React renderer，避免共享 Context 被识别成多个 renderer。
const reconciler = Reconciler(hostConfig);

/** 只提交声明；资源等待和业务 Action 属于程序及原生执行边界，不在 React commit 中运行。 */
export function createDeclarationRenderer<T>(options: DeclarationRendererOptions<T>): DeclarationRenderer {
  let revision = 0;
  let failure: { error: unknown } | undefined;
  let disposed = false;
  let queued = false;
  let publishing = false;
  let pending: Frame<T> | undefined;
  const fail = (error: unknown) => {
    if (failure) return;
    failure = { error }; pending = undefined;
    try { options.onError(error); }
    catch (notificationError) { failure.error = new AggregateError([error, notificationError], 'REACTIVE_RENDERER_ERROR_NOTIFICATION_FAILED'); }
  };
  const assertActive = () => {
    if (disposed) throw new Error('REACTIVE_RENDERER_DISPOSED');
    if (failure) throw failure.error;
  };
  const publish = () => {
    if (publishing || disposed || failure) return;
    publishing = true;
    try {
      while (!disposed && !failure && pending) {
        const frame = pending;
        pending = undefined;
        try { options.onCommit(frame); } catch (error) { fail(error); }
      }
    } finally { publishing = false; }
  };
  const settle = () => {
    try {
      reconciler.flushSyncWork();
      while (reconciler.flushPassiveEffects()) reconciler.flushSyncWork();
    } catch (error) { fail(error); }
    publish();
  };
  const container: Container = { children: [], commit(values) {
    if (disposed || failure) return;
    try {
      const declarations = Object.freeze(values.map(value => options.decode(value)));
      // Effect 可以同步提交下一棵树；退出代际的声明不能在冲刷后重新连接其来源。
      pending = Object.freeze({ revision: ++revision, declarations });
    } catch (error) { fail(error); return; }
    if (queued) return;
    queued = true;
    // onUncaughtError/onCaughtError 可以晚于 resetAfterCommit；不得先发布错误树的空帧。
    queueMicrotask(() => { queued = false; if (!disposed && !failure) settle(); });
  } };
  const root = reconciler.createContainer(container, ConcurrentRoot, null, false, null, '', fail, fail, fail, () => {}, null);
  const sync = (action: () => void) => {
    assertActive();
    if (reconciler.isAlreadyRendering()) throw new Error('REACTIVE_RENDERER_REENTRANT_OPERATION');
    try { reconciler.flushSyncFromReconciler(action); } catch (error) { fail(error); }
    settle(); assertActive();
  };
  const onAbort = () => dispose();
  function dispose() {
    if (disposed) return;
    disposed = true; pending = undefined;
    options.signal?.removeEventListener('abort', onAbort);
    const cleanup = () => {
      try {
        reconciler.flushSyncFromReconciler(() => { reconciler.updateContainerSync(null, root); });
        while (reconciler.flushPassiveEffects()) reconciler.flushSyncWork();
      } catch (error) { fail(error); }
    };
    // owner 可以在 React 的错误通知里 abort；清理应等当前 commit 结束。
    if (reconciler.isAlreadyRendering()) queueMicrotask(cleanup); else cleanup();
  }
  options.signal?.addEventListener('abort', onAbort, { once: true });
  if (options.signal?.aborted) dispose();
  return {
    render: element => sync(() => { reconciler.updateContainerSync(element, root); }),
    flush: () => sync(() => {}),
    unmount: () => sync(() => { reconciler.updateContainerSync(null, root); }),
    dispose,
  };
}
