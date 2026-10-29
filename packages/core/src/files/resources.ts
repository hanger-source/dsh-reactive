import { Observable, ReplaySubject, Subject, Subscriber, map, of, share, startWith } from 'rxjs';
import { switchResource } from '../resources/operators.ts';
import { observeResource } from '../resources/query.ts';
import { shareResource } from '../resources/sharing.ts';
import type { Observation } from '../resources/observation.ts';
import type { ResourceReaderOptions } from '../resources/reader.ts';
import type { FileResource, FileWatch, SessionFiles, WorkspaceFileRange, WorkspaceFileText,
  WorkspaceFileStat, WorkspaceDirectoryListing, WorkspaceFileWatchFrame } from './contract.ts';

export interface FileResourceOptions extends ResourceReaderOptions {
  /** 来源暂不可读时调用invalidate；下一次正式ready到达后才重新读取，不伪造watch帧。 */
  watch(path: string, invalidate: () => void): FileWatch;
}

/** 同一路径的所有查询共用实际watch；ReplaySubject只持有当前连接的资格和正式帧。 */
function fileChanges(path: string, options: FileResourceOptions): Observable<Observation<WorkspaceFileWatchFrame>> {
  return new Observable<Observation<WorkspaceFileWatchFrame>>(subscriber => {
    let generation = 0, releaseRequested = false, released = false;
    let failure: { readonly error: unknown } | undefined;
    let watch: FileWatch | undefined;
    const reportCleanup = (error: unknown) => {
      if (failure && error === failure.error) return;
      if (options.onCleanupError) options.onCleanupError(error);
      else console.error('reactive.files.cleanup.failed', error);
    };
    const release = () => {
      releaseRequested = true;
      if (!watch || released) return;
      released = true;
      try { void watch.dispose().catch(reportCleanup); }
      catch (error) { reportCleanup(error); }
    };
    // watch工厂可同步失效owner；退出先登记，返回的实际watch仍必须收拢。
    subscriber.add(release);
    watch = options.watch(path, () => {
      if (!subscriber.closed) subscriber.next({ status: 'pending', generation: ++generation });
    });
    if (releaseRequested) release();
    if (subscriber.closed) return;
    const connection = new Subscriber<WorkspaceFileWatchFrame>({
      next: frame => subscriber.next({ status: 'ready', generation: ++generation, value: frame, observedAt: Date.now() }),
      error: (error: unknown) => { failure = { error }; subscriber.error(error); },
      complete: () => subscriber.complete(),
    });
    subscriber.add(connection);
    watch.changes$.subscribe(connection);
  }).pipe(share({ connector: () => new ReplaySubject<Observation<WorkspaceFileWatchFrame>>(1),
    resetOnError: true, resetOnComplete: true, resetOnRefCountZero: true }));
}

/** 参数身份与共享连接属于原会话signal；构造不读取，也不创建额外Scope或执行队列。 */
export function createFileResources(reads: Pick<SessionFiles, 'readText' | 'stat' | 'list'>,
  options: FileResourceOptions): Pick<SessionFiles, 'text' | 'metadata' | 'directory'> {
  const { signal, onObserverError, onCleanupError, watch } = options;
  signal.throwIfAborted();
  const configured: FileResourceOptions = { signal, onObserverError, onCleanupError, watch };
  const watches = new Map<string, Observable<Observation<WorkspaceFileWatchFrame>>>();
  const texts = new Map<string, Map<number | undefined, Map<number | undefined, FileResource<WorkspaceFileText>>>>();
  const stats = new Map<string, FileResource<WorkspaceFileStat>>();
  const directories = new Map<string, FileResource<WorkspaceDirectoryListing>>();
  signal.addEventListener('abort', () => { watches.clear(); texts.clear(); stats.clear(); directories.clear(); }, { once: true });
  const changes = (path: string) => {
    let source = watches.get(path);
    if (!source) { source = fileChanges(path, configured); watches.set(path, source); }
    return source;
  };
  const resource = <T>(path: string, read: (signal: AbortSignal) => Promise<T>): FileResource<T> => {
    const invalidations = new Subject<void>();
    const source = invalidations.pipe(startWith(undefined),
      map((_, generation): Observation<void> => ({ status: 'ready', generation, value: undefined, observedAt: Date.now() })),
      // 显式刷新重连来源，也能从watch初始化失败后重新取得实际watch。
      switchResource(() => changes(path).pipe(switchResource(() => observeResource({
        key$: of(path), read: (_path, reading) => read(AbortSignal.any([signal, reading])),
      })))),
    );
    return Object.assign(shareResource(source, configured), {
      invalidate() { signal.throwIfAborted(); invalidations.next(); },
    });
  };
  return Object.freeze({
    text(path: string, range: WorkspaceFileRange = {}): FileResource<WorkspaceFileText> {
      signal.throwIfAborted();
      const { offset, limit } = range;
      let offsets = texts.get(path);
      if (!offsets) { offsets = new Map(); texts.set(path, offsets); }
      let limits = offsets.get(offset);
      if (!limits) { limits = new Map(); offsets.set(offset, limits); }
      let page = limits.get(limit);
      if (!page) {
        const captured = Object.freeze({ ...(offset === undefined ? {} : { offset }), ...(limit === undefined ? {} : { limit }) });
        page = resource(path, reading => reads.readText(path, captured, { signal: reading }));
        limits.set(limit, page);
      }
      return page;
    },
    metadata(path: string): FileResource<WorkspaceFileStat> {
      signal.throwIfAborted();
      let stat = stats.get(path);
      if (!stat) { stat = resource(path, reading => reads.stat(path, { signal: reading })); stats.set(path, stat); }
      return stat;
    },
    directory(path: string): FileResource<WorkspaceDirectoryListing> {
      signal.throwIfAborted();
      let directory = directories.get(path);
      if (!directory) { directory = resource(path, reading => reads.list(path, { signal: reading })); directories.set(path, directory); }
      return directory;
    },
  });
}
