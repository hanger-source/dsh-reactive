import { posix, win32 } from 'node:path';
import type WorkspaceFiles from '@deepseek-ai/dsh-api-workspace-files';
import type { WorkspaceFileScope, WorkspaceFileRange, WorkspaceByteReadOptions, WorkspaceFileText,
  WorkspaceFileBytes, WorkspaceFileStat, WorkspaceDirectoryListing, WorkspaceFileWatchFrame } from '@deepseek-ai/dsh-api-workspace-files';
import { FsError, type FileSystem } from '@deepseek-ai/dsh-fs';
import { Observable, ReplaySubject, firstValueFrom } from 'rxjs';
import { abortable, type CancellationOptions } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';
import { NativeCapabilityOwner, nativeObservationScope, ownNativeValue,
  type NativeCapability, type NativeSessionSource } from './native-capability.ts';
import { nativeSessionAccess, type NativeSessionAccess } from './conversations/session-access.ts';
import { createFileResources, type NativeFileResource } from './files/resources.ts';

export type { NativeFileResource } from './files/resources.ts';

export type { WorkspaceFileRange, WorkspaceByteReadOptions, WorkspaceFileText, WorkspaceFileBytes,
  WorkspaceFileStat, WorkspaceDirectoryListing, WorkspaceFileWatchFrame, WorkspaceFileChange } from '@deepseek-ai/dsh-api-workspace-files';

export interface NativeFilesOptions {
  /** 明确复用正式 WorkspaceFiles，不隐装 Gateway、Typert 或 sandbox policy。 */
  readonly service: 'provided';
}

/** 原生 FS 明确把取消映射为 FS_ABORTED；不能把同时发生的其他 I/O/释放错误吞成取消。 */
function cancelledRead(error: unknown, signal: AbortSignal): boolean {
  return signal.aborted && (error === signal.reason || error instanceof FsError && error.code === 'FS_ABORTED'
    || error instanceof Error && error.name === 'AbortError' && error.cause === signal.reason);
}

export interface NativeFileWatch {
  /** 同一个原生 watch 的共享观察；退订一个观察者不释放这个 watch。 */
  readonly changes$: Observable<WorkspaceFileWatchFrame>;
  /** 正式迭代与异步 unwatch 全部结束；真实初始化/读取/释放错误仍会拒绝。 */
  readonly closed: Promise<void>;
  dispose(): Promise<void>;
}

export interface NativeSessionFiles {
  /** 固定路径与原始行窗口的共享资源；构造不读，首次观察先建立原生watch。 */
  text(path: string, range?: WorkspaceFileRange): NativeFileResource<WorkspaceFileText>;
  metadata(path: string): NativeFileResource<WorkspaceFileStat>;
  directory(path: string): NativeFileResource<WorkspaceDirectoryListing>;
  readText(path: string, range?: WorkspaceFileRange, options?: CancellationOptions): Promise<WorkspaceFileText>;
  readBytes(path: string, range?: WorkspaceByteReadOptions, options?: CancellationOptions): Promise<WorkspaceFileBytes>;
  stat(path: string, options?: CancellationOptions): Promise<WorkspaceFileStat>;
  list(path: string, options?: CancellationOptions): Promise<WorkspaceDirectoryListing>;
  watch(path: string, options?: CancellationOptions): NativeFileWatch;
  /** 取消本实例读取并等待真实 watch 退出；不停止产生文件的 Action、Job 或 Operation。 */
  dispose(): Promise<void>;
}

class FileWatch implements NativeFileWatch {
  readonly changes$: Observable<WorkspaceFileWatchFrame>;
  readonly closed: Promise<void>;
  #scope: Scope;
  constructor(files: SessionFiles, path: string, options: CancellationOptions) {
    const watchScope = files.scope.child();
    this.#scope = watchScope;
    const subject = new ReplaySubject<WorkspaceFileWatchFrame>(1);
    this.changes$ = new Observable(subscriber => {
      if (watchScope.disposed) { subscriber.complete(); return; }
      return subject.subscribe(subscriber);
    });
    const signal = options.signal ? AbortSignal.any([watchScope.signal, options.signal]) : watchScope.signal;
    this.closed = (async () => {
      try {
        const { service, scope, target } = await files.resolve(path, signal);
        // for-await 的退出等待官方 generator.finally → follower.close → unwatch。
        for await (const frame of service.changes(scope, target, signal)) {
          signal.throwIfAborted(); files.assertCurrent();
          subject.next(ownNativeValue(frame));
        }
        subject.complete();
      } catch (error) {
        if (cancelledRead(error, signal)) subject.complete();
        else { subject.error(error); throw error; }
      } finally { watchScope.dispose(); }
    })();
    // 错误同时交给 changes$ 和 closed；即使没有观察者也不制造未处理拒绝。
    void this.closed.catch(() => {});
  }
  dispose(): Promise<void> {
    this.#scope.dispose();
    return this.closed;
  }
}

class SessionFiles implements NativeSessionFiles {
  readonly scope: Scope;
  #resources: ReturnType<typeof createFileResources>;
  #watches = new Set<NativeFileWatch>();
  #reads = new Set<Promise<unknown>>();
  #closing?: Promise<void>;
  get disposed(): boolean { return this.scope.disposed || this.#closing !== undefined; }
  constructor(private readonly owner: NativeCapabilityOwner, private readonly access: NativeSessionAccess,
    private readonly released: () => void) {
    this.scope = nativeObservationScope(owner, access);
    this.#resources = createFileResources(this, this.scope);
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
    let selected = path;
    if (baseFile !== undefined) {
      const target = await fs.resolve(baseFile, { cwd: workspaceRoot, signal });
      if (!fs.contains(root, target)) throw new Error('REACTIVE_NATIVE_FILE_OUTSIDE_WORKSPACE');
      const absolute = fs.processPath(target);
      const paths = absolute.startsWith('/') ? posix : win32;
      selected = paths.resolve(paths.dirname(absolute), path.replace(/\\/gu, '/'));
    }
    // 只计算范围；原始 path/baseFile 仍交给正式服务验证，不绕过它的符号链接/相对路径规则。
    const target = await fs.resolve(selected, { cwd: workspaceRoot, signal });
    signal.throwIfAborted(); this.assertCurrent();
    if (!fs.contains(root, target)) throw new Error('REACTIVE_NATIVE_FILE_OUTSIDE_WORKSPACE');
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
      return ownNativeValue(await service.read(scope, target, selected, signal));
    });
  }
  readBytes(path: string, range: WorkspaceByteReadOptions = {}, options: CancellationOptions = {}): Promise<WorkspaceFileBytes> {
    const selected = structuredClone(range);
    return this.read(options, async signal => {
      const { service, scope, target } = await this.resolve(path, signal, selected.baseFile);
      const result = await service.readBytes(scope, target, selected, signal);
      // Uint8Array 不能 freeze；各次调用持有独立 bytes，记录本身不可变。
      return Object.freeze({ ...result, data: Uint8Array.from(result.data) });
    });
  }
  stat(path: string, options: CancellationOptions = {}): Promise<WorkspaceFileStat> {
    return this.read(options, async signal => {
      const { service, scope, target } = await this.resolve(path, signal);
      return ownNativeValue(await service.stat(scope, target, signal));
    });
  }
  list(path: string, options: CancellationOptions = {}): Promise<WorkspaceDirectoryListing> {
    return this.read(options, async signal => {
      const { service, scope, target } = await this.resolve(path, signal);
      return ownNativeValue(await service.list(scope, target, signal));
    });
  }
  watch(path: string, options: CancellationOptions = {}): NativeFileWatch {
    this.signal(options);
    const watch = new FileWatch(this, path, options);
    this.#watches.add(watch);
    void watch.closed.then(() => this.#watches.delete(watch), () => this.#watches.delete(watch));
    return watch;
  }
  dispose(): Promise<void> {
    // 先固定同一次关闭 Promise，再触发 scope abort；abort 回调重入也只参与这次关闭。
    return this.#closing ??= Promise.resolve().then(async () => {
      try {
        this.scope.dispose();
        const results = await Promise.allSettled([...this.#watches].map(watch => watch.dispose()));
        await Promise.allSettled([...this.#reads]);
        const failures = results.flatMap(result => result.status === 'rejected' ? [result.reason] : []);
        if (failures.length === 1) throw failures[0];
        if (failures.length) throw new AggregateError(failures, 'REACTIVE_NATIVE_FILE_WATCH_DISPOSAL_FAILED');
      } finally { this.released(); }
    });
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
      const failures = results.flatMap(result => result.status === 'rejected' ? [result.reason] : []);
      if (failures.length) throw new AggregateError(failures, 'REACTIVE_NATIVE_FILES_DISPOSAL_FAILED');
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
