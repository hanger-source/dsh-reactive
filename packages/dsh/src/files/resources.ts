import { Observable, ReplaySubject, share } from 'rxjs';
import { bindResource, defineResource, implementResource, observeResource, z,
  type Observation, type ResourceReadable } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';
import type { NativeSessionFiles, WorkspaceFileRange, WorkspaceFileText, WorkspaceFileStat,
  WorkspaceDirectoryListing, WorkspaceFileWatchFrame } from '../files.ts';

/** 同一个正式Resource实例；字节缓冲区不进入共享快照。 */
export interface NativeFileResource<T> extends ResourceReadable<T> {
  readonly values$: Observable<T>;
  invalidate(): void;
}

// 参数决定本地查询身份；返回值已经来自正式WorkspaceFiles，不复制其结果schema。
const text = defineResource({ name: 'native.workspace.text', params: z.object({ path: z.string(),
  offset: z.number().optional(), limit: z.number().optional() }), value: z.custom<WorkspaceFileText>() });
const metadata = defineResource({ name: 'native.workspace.metadata', params: z.object({ path: z.string() }),
  value: z.custom<WorkspaceFileStat>() });
const directory = defineResource({ name: 'native.workspace.directory', params: z.object({ path: z.string() }),
  value: z.custom<WorkspaceDirectoryListing>() });

/** ready确认实际watch已登记，之后才取第一份切面；目录变化不能只按目录版本去重。 */
function fileChanges(files: NativeSessionFiles, scope: Scope, path: string): Observable<WorkspaceFileWatchFrame> {
  return new Observable<WorkspaceFileWatchFrame>(subscriber => {
    const watch = files.watch(path);
    let failed = false;
    subscriber.add(watch.changes$.subscribe({
      next: frame => subscriber.next(frame),
      error: error => { failed = true; subscriber.error(error); },
      complete: () => subscriber.complete(),
    }));
    return () => {
      // 原生异步unwatch仍由SessionFiles跟踪并等待；退出后的清理失败不能静默消失。
      void watch.dispose().catch(error => { if (!failed) scope.observerError(error); });
    };
  }).pipe(share({ connector: () => new ReplaySubject<WorkspaceFileWatchFrame>(1),
    resetOnError: true, resetOnComplete: true, resetOnRefCountZero: true }));
}

/** 只装配绑定，不启动读取或watch；生命周期统一属于原SDK的Scope。 */
export function createFileResources(files: NativeSessionFiles, scope: Scope) {
  const watches = new Map<string, Observable<WorkspaceFileWatchFrame>>();
  scope.own(() => watches.clear());
  const changes = (path: string) => {
    let source = watches.get(path);
    if (!source) { source = fileChanges(files, scope, path); watches.set(path, source); }
    return source;
  };
  const watchRead = <T>(path: string, signal: AbortSignal, read: (signal: AbortSignal) => Promise<T>): Observable<Observation<T>> =>
    observeResource({ key$: changes(path), read: (_frame, reading) => read(AbortSignal.any([signal, reading])) });
  const texts = bindResource(implementResource(text, { observe: ({ path, offset, limit }, { signal }) =>
    watchRead(path, signal, reading => files.readText(path, { offset, limit }, { signal: reading })) }), { scope });
  const stats = bindResource(implementResource(metadata, { observe: ({ path }, { signal }) =>
    watchRead(path, signal, reading => files.stat(path, { signal: reading })) }), { scope });
  const directories = bindResource(implementResource(directory, { observe: ({ path }, { signal }) =>
    watchRead(path, signal, reading => files.list(path, { signal: reading })) }), { scope });
  return {
    text(path: string, range: WorkspaceFileRange): NativeFileResource<WorkspaceFileText> {
      return texts.instance({ path, ...(range.offset === undefined ? {} : { offset: range.offset }),
        ...(range.limit === undefined ? {} : { limit: range.limit }) });
    },
    metadata: (path: string): NativeFileResource<WorkspaceFileStat> => stats.instance({ path }),
    directory: (path: string): NativeFileResource<WorkspaceDirectoryListing> => directories.instance({ path }),
  };
}
