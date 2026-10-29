import { posix, win32 } from 'node:path';
import type WorkspaceFiles from '@deepseek-ai/dsh-api-workspace-files';
import type { WorkspaceFileScope } from '@deepseek-ai/dsh-api-workspace-files';
import { FsError, type FileSystem } from '@deepseek-ai/dsh-fs';
import { Observable, ReplaySubject, firstValueFrom } from 'rxjs';
import { abortable, ScopeDisposedError, type CancellationOptions } from '@hanger-source/dsh-reactive';
import { SessionFileOutsideWorkspaceError, type SessionFiles as SessionFilesContract, type FileWatch,
  type WorkspaceFileRange, type WorkspaceByteReadOptions, type WorkspaceFileText, type WorkspaceFileBytes,
  type WorkspaceFileStat, type WorkspaceDirectoryListing, type WorkspaceFileWatchFrame } from '@hanger-source/dsh-reactive/files';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';
import { NativeCapabilityOwner, nativeObservationScope, ownNativeValue,
  type NativeCapability, type NativeSessionSource } from './native-capability.ts';
import { nativeSessionAccess, type NativeSessionAccess } from './conversations/session-access.ts';
import { createFileResources, type NativeFileResource } from './files/resources.ts';

export type { NativeFileResource } from './files/resources.ts';

export type { WorkspaceFileRange, WorkspaceByteReadOptions, WorkspaceFileText, WorkspaceFileBytes,
  WorkspaceFileStat, WorkspaceDirectoryListing, WorkspaceFileWatchFrame, WorkspaceFileChange } from '@hanger-source/dsh-reactive/files';

export interface NativeFilesOptions {
  /** 明确复用正式 WorkspaceFiles，不隐装 Gateway、Typert 或 sandbox policy。 */
  readonly service: 'provided';
}

/** 原生 FS 明确把取消映射为 FS_ABORTED；不能把同时发生的其他 I/O/释放错误吞成取消。 */
function cancelledRead(error: unknown, signal: AbortSignal): boolean {
  return signal.aborted && (error === signal.reason || error instanceof FsError && error.code === 'FS_ABORTED'
    || error instanceof Error && error.name === 'AbortError' && error.cause === signal.reason);
}

export type NativeFileWatch = FileWatch;

export interface NativeSessionFiles extends SessionFilesContract {
  /** 取消本实例读取并等待真实 watch 退出；不停止产生文件的 Action、Job 或 Operation。 */
  dispose(): Promise<void>;
}

class NativeWatch implements FileWatch {
  readonly changes$: Observable<WorkspaceFileWatchFrame>;
  readonly closed: Promise<void>;
  #scope: Scope;
  #disposedReason = new ScopeDisposedError();
  constructor(files: SessionFiles, path: string, options: CancellationOptions) {
    const watchScope = files.scope.child();
    this.#scope = watchScope;
    const subject = new ReplaySubject<WorkspaceFileWatchFrame>(1);
    const signal = options.signal ? AbortSignal.any([watchScope.signal, options.signal]) : watchScope.signal;
    this.changes$ = new Observable(subscriber => {
      if (signal.aborted) {
        if (signal.reason === this.#disposedReason) subscriber.complete();
        else subscriber.error(signal.reason);
        return;
      }
      return subject.subscribe(subscriber);
    });
    const cancel = () => {
      if (signal.reason === this.#disposedReason) subject.complete();
      else subject.error(signal.reason);
    };
    signal.addEventListener('abort', cancel, { once: true });
    if (signal.aborted) cancel();
    this.closed = (async () => {
      let reason: unknown = this.#disposedReason;
      try {
        const { service, scope, target } = await files.resolve(path, signal);
        signal.throwIfAborted(); files.assertCurrent();
        // for-await 的退出等待官方 generator.finally → follower.close → unwatch。
        for await (const frame of service.changes(scope, target, signal)) {
          signal.throwIfAborted(); files.assertCurrent();
          subject.next(ownNativeValue(frame));
        }
        subject.complete();
      } catch (error) {
        if (cancelledRead(error, signal)) subject.complete();
        else { reason = error; subject.error(error); throw error; }
      } finally {
        try { watchScope.dispose(reason); }
        finally { signal.removeEventListener('abort', cancel); }
      }
    })();
    // 错误同时交给 changes$ 和 closed；即使没有观察者也不制造未处理拒绝。
    void this.closed.catch(() => {});
  }
  dispose(): Promise<void> {
    this.#scope.dispose(this.#disposedReason);
    return this.closed;
  }
}

class SessionFiles implements NativeSessionFiles {
  readonly scope: Scope;
  #resources: ReturnType<typeof createFileResources>;
  #watches = new Set<NativeFileWatch>();
  #reads = new Set<Promise<unknown>>();
  #cleanupFailures = new Set<unknown>();
  #closing?: Promise<void>;
  get disposed(): boolean { return this.scope.disposed || this.#closing !== undefined; }
  constructor(private readonly owner: NativeCapabilityOwner, private readonly access: NativeSessionAccess,
    private readonly released: () => void) {
    this.scope = nativeObservationScope(owner, access);
    this.#resources = createFileResources(this, { signal: this.scope.signal,
      onObserverError: error => this.scope.observerError(error),
      onCleanupError: error => { this.#cleanupFailures.add(error); this.scope.observerError(error); },
      watch: path => this.watch(path),
    });
    const logger = owner.context().logger;
    this.scope.signal.addEventListener('abort', () => {
      void this.dispose().catch(error => {
        owner.recordCleanupFailure(error);
        logger.error(error);
      });
    }, { once: true });
  }
  assertCurrent(): void {
    this.scope.assertActive();
    if (this.#closing) throw new Error('REACTIVE_NATIVE_FILES_DISPOSED');
    this.owner.session(this.access, 'workspaceFiles');
  }
  private signal(options: CancellationOptions): AbortSignal {
    this.assertCurrent();
    const signal = options.signal ? AbortSignal.any([this.scope.signal, options.signal]) : this.scope.signal;
    signal.throwIfAborted();
    return signal;
  }
  /** @internal 仅从 exact 句柄的当前原生 header 构造 scope；路径不根据进程 cwd 猜测。 */
  async resolve(path: string, signal: AbortSignal, baseFile?: string) {
    signal.throwIfAborted(); this.assertCurrent();
    const workspaceRoot = this.access.workspaceRoot();
    const ctx = this.owner.session(this.access, 'workspaceFiles');
    const service: WorkspaceFiles | undefined = ctx.get('workspaceFiles');
    const fs: FileSystem | undefined = ctx.get('fs');
    if (!service || !fs) throw new Error('REACTIVE_NATIVE_FILES_REQUIRED');
    const root = await fs.resolve(workspaceRoot, { signal });
    signal.throwIfAborted(); this.assertCurrent();
    let selected = path;
    if (baseFile !== undefined) {
      const target = await fs.resolve(baseFile, { cwd: workspaceRoot, signal });
      signal.throwIfAborted(); this.assertCurrent();
      if (!fs.contains(root, target)) throw new SessionFileOutsideWorkspaceError(baseFile);
      const absolute = fs.processPath(target);
      const paths = absolute.startsWith('/') ? posix : win32;
      selected = paths.resolve(paths.dirname(absolute), path.replace(/\\/gu, '/'));
    }
    // 只计算范围；原始 path/baseFile 仍交给正式服务验证，不绕过它的符号链接/相对路径规则。
    const target = await fs.resolve(selected, { cwd: workspaceRoot, signal });
    signal.throwIfAborted(); this.assertCurrent();
    if (!fs.contains(root, target)) throw new SessionFileOutsideWorkspaceError(path);
    const scope: WorkspaceFileScope = { sessionId: this.access.sessionId, workspaceRoot };
    return { service, scope, target: path };
  }
  private read<T>(options: CancellationOptions, run: (signal: AbortSignal) => Promise<T>): Promise<T> {
    const signal = this.signal(options);
    const reading = run(signal).then(value => {
      signal.throwIfAborted(); this.assertCurrent(); return value;
    }, error => { throw cancelledRead(error, signal) ? signal.reason : error; });
    this.#reads.add(reading);
    void reading.then(() => this.#reads.delete(reading), () => this.#reads.delete(reading));
    // 只结束消费者的等待；dispose仍通过原reading等待真实I/O完成。
    return firstValueFrom(abortable(() => reading, signal));
  }
  text(path: string, range: WorkspaceFileRange = {}): NativeFileResource<WorkspaceFileText> {
    this.assertCurrent(); return this.#resources.text(path, range);
  }
  metadata(path: string): NativeFileResource<WorkspaceFileStat> {
    this.assertCurrent(); return this.#resources.metadata(path);
  }
  directory(path: string): NativeFileResource<WorkspaceDirectoryListing> {
    this.assertCurrent(); return this.#resources.directory(path);
  }
  readText(path: string, range: WorkspaceFileRange = {}, options: CancellationOptions = {}): Promise<WorkspaceFileText> {
    const selected = structuredClone(range);
    return this.read(options, async signal => {
      const { service, scope, target } = await this.resolve(path, signal);
      signal.throwIfAborted(); this.assertCurrent();
      return ownNativeValue(await service.read(scope, target, selected, signal));
    });
  }
  readBytes(path: string, range: WorkspaceByteReadOptions = {}, options: CancellationOptions = {}): Promise<WorkspaceFileBytes> {
    const selected = structuredClone(range);
    return this.read(options, async signal => {
      const { service, scope, target } = await this.resolve(path, signal, selected.baseFile);
      signal.throwIfAborted(); this.assertCurrent();
      const result = await service.readBytes(scope, target, selected, signal);
      // Uint8Array 不能 freeze；各次调用持有独立 bytes，记录本身不可变。
      return Object.freeze({ ...result, data: Uint8Array.from(result.data) });
    });
  }
  stat(path: string, options: CancellationOptions = {}): Promise<WorkspaceFileStat> {
    return this.read(options, async signal => {
      const { service, scope, target } = await this.resolve(path, signal);
      signal.throwIfAborted(); this.assertCurrent();
      return ownNativeValue(await service.stat(scope, target, signal));
    });
  }
  list(path: string, options: CancellationOptions = {}): Promise<WorkspaceDirectoryListing> {
    return this.read(options, async signal => {
      const { service, scope, target } = await this.resolve(path, signal);
      signal.throwIfAborted(); this.assertCurrent();
      return ownNativeValue(await service.list(scope, target, signal));
    });
  }
  watch(path: string, options: CancellationOptions = {}): NativeFileWatch {
    this.signal(options);
    const watch = new NativeWatch(this, path, options);
    this.#watches.add(watch);
    void watch.closed.then(() => this.#watches.delete(watch), () => this.#watches.delete(watch));
    return watch;
  }
  dispose(): Promise<void> {
    // 先固定同一次关闭 Promise，再触发 scope abort；abort 回调重入也只参与这次关闭。
    if (this.#closing) return this.#closing;
    const completion = Promise.withResolvers<void>();
    this.#closing = completion.promise;
    const failures = this.#cleanupFailures;
    try { this.scope.dispose(); } catch (error) { failures.add(error); }
    // 正在构造的watch/read可能同步触发退出；等其登记完毕再加入真实清理。
    void Promise.resolve().then(async () => {
      try {
        const results = await Promise.allSettled([...this.#watches].map(watch => watch.dispose()));
        await Promise.allSettled([...this.#reads]);
        for (const result of results) if (result.status === 'rejected') failures.add(result.reason);
        if (failures.size === 1) throw [...failures][0];
        if (failures.size) throw new AggregateError([...failures], 'REACTIVE_NATIVE_FILE_WATCH_DISPOSAL_FAILED');
      } finally { this.released(); }
    }).then(completion.resolve, completion.reject);
    return completion.promise;
  }
}

export interface NativeFiles extends NativeCapability {
  forSession(session: NativeSessionSource): NativeSessionFiles;
}

/** 只读文件接点；字节、版本、限额及 watch 仍由正式 FS/WorkspaceFiles 服务持有。 */
export function createNativeFiles(options: NativeFilesOptions): NativeFiles {
  if (options.service !== 'provided') throw new Error('REACTIVE_NATIVE_FILES_PROVIDED_REQUIRED');
  const active = new Set<SessionFiles>();
  const sessions = new WeakMap<object, SessionFiles>();
  const owner: NativeCapabilityOwner = new NativeCapabilityOwner('reactive-native-files', ['workspaceFiles', 'fs'], function* (ctx) {
    if (!ctx.get('workspaceFiles') || !ctx.get('fs')) throw new Error('REACTIVE_NATIVE_FILES_REQUIRED');
    yield owner.cleanup(async () => {
      const results = await Promise.allSettled([...active].map(session => session.dispose()));
      for (const result of results) if (result.status === 'rejected') owner.recordCleanupFailure(result.reason);
    });
  });
  return Object.freeze({ install: owner.install, dispose: () => owner.dispose(),
    forSession(session: NativeSessionSource) {
      const access = nativeSessionAccess(session); owner.session(access, 'workspaceFiles');
      let current = sessions.get(session);
      if (!current || current.disposed) {
        const created = new SessionFiles(owner, access, () => active.delete(created));
        current = created; sessions.set(session, current); active.add(current);
      }
      return current;
    },
  });
}
