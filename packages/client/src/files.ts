import { RemoteSnapshotStream } from '@deepseek-ai/dsh-api-gateway/client';
import { BehaviorSubject, Observable, ReplaySubject, switchAll } from 'rxjs';
import { createFileResources, type CancellationOptions, type FileWatch, type SessionFiles,
  type WorkspaceFileRange, type WorkspaceByteReadOptions, type WorkspaceFileText, type WorkspaceFileBytes,
  type WorkspaceFileStat, type WorkspaceDirectoryListing, type WorkspaceFileWatchFrame } from '@hanger-source/dsh-reactive';
import { revokeConversationAccess, type ClientScope } from './scope.ts';
import { decodeProgramFailure } from './contracts/errors.ts';
import type { ProgramSessionAddress } from './contracts/types.ts';
import { RemoteError, type RemoteResult } from '@deepseek-ai/dsh-typert-protocol';

function fileWatch(owner: ClientScope, address: ProgramSessionAddress, path: string,
  options: CancellationOptions = {}, invalidate: () => void = () => {}): FileWatch {
  owner.assertActive();
  const controller = new AbortController();
  const signal = AbortSignal.any([owner.signal, controller.signal, ...(options.signal ? [options.signal] : [])]);
  signal.throwIfAborted();
  let subject = new ReplaySubject<WorkspaceFileWatchFrame>(1);
  const sources = new BehaviorSubject<Observable<WorkspaceFileWatchFrame>>(subject);
  const changes = sources.pipe(switchAll());
  const completion = Promise.withResolvers<void>();
  let failed: { readonly error: unknown } | undefined;
  let manuallyDisposed = false;
  const closed = completion.promise.then(() => { if (failed) throw failed.error; });
  void closed.catch(() => {});
  let release: (() => Promise<void>) | undefined;
  const stop = () => {
    if (manuallyDisposed) { subject.complete(); sources.complete(); }
    else sources.error(failed ? failed.error : signal.reason);
    void release?.().catch(() => {});
  };
  const invalidateConnection = () => {
    if (signal.aborted || failed) return;
    // RxJS切换实际连接来源，先撤旧代Replay；同步重入或晚订阅都不能再拿旧ready。
    const previous = subject;
    subject = new ReplaySubject<WorkspaceFileWatchFrame>(1);
    sources.next(subject);
    previous.complete();
    invalidate();
  };
  const name = 'program-file-watch:' + address.sessionId + ':' + path;
  const lease = owner.acquire(() => {
    const stream = new RemoteSnapshotStream<Extract<WorkspaceFileWatchFrame, { kind: 'ready' }>, Exclude<WorkspaceFileWatchFrame, { kind: 'ready' }>>(
      owner.context.remote.$stream<WorkspaceFileWatchFrame>({ name,
        open: reading => owner.context.remote.reactivePrograms.fileChanges({ ...address, path }, AbortSignal.any([signal, reading])),
        ended: () => new Error('REACTIVE_REMOTE_FILE_WATCH_ENDED'), carrierFailed: invalidateConnection,
      }), { name,
        isSnapshot: (frame): frame is Extract<WorkspaceFileWatchFrame, { kind: 'ready' }> => frame.kind === 'ready',
        replace: frame => { if (!signal.aborted) subject.next(frame); },
        update: frame => { if (!signal.aborted) subject.next(frame); },
        failed(error) {
          if (signal.aborted) return;
          error = revokeConversationAccess(owner, error instanceof RemoteError ? decodeProgramFailure(error) : error);
          if (signal.aborted) return;
          failed = { error }; sources.error(error); stop();
        },
      });
    const reset = owner.context.on('connection/reset', () => {
      if (signal.aborted || failed) return;
      invalidateConnection(); stream.restart();
    });
    return { stream, reset };
  }, async ({ stream, reset }) => {
    signal.removeEventListener('abort', stop);
    controller.abort(new Error('REACTIVE_REMOTE_FILE_WATCH_DISPOSED'));
    const failures: unknown[] = [];
    try { reset(); } catch (error) { failures.push(error); }
    try { await stream.dispose(); } catch (error) { failures.push(error); }
    subject.complete(); sources.complete();
    if (failures.length) {
      const error = failures.length === 1 ? failures[0] : new AggregateError(failures, 'REACTIVE_REMOTE_FILE_WATCH_CLEANUP_FAILED');
      completion.reject(error); throw error;
    }
    completion.resolve();
  }, name);
  release = lease.release;
  signal.addEventListener('abort', stop, { once: true });
  if (signal.aborted) stop();
  else if (owner.active) lease.value.stream.start();
  return Object.freeze({
    changes$: new Observable<WorkspaceFileWatchFrame>(subscriber => {
      if (signal.aborted) {
        if (manuallyDisposed) subscriber.complete();
        else subscriber.error(failed ? failed.error : signal.reason);
        return;
      }
      return changes.subscribe(subscriber);
    }), closed,
    async dispose() {
      if (!signal.aborted) manuallyDisposed = true;
      subject.complete(); sources.complete();
      controller.abort(new Error('REACTIVE_REMOTE_FILE_WATCH_DISPOSED'));
      await lease.release(); await closed;
    },
  });
}

/** 同一Session的借用Files；只有读取或watch才调用Gateway，不另建连接或CoreScope。 */
export function remoteFiles(owner: ClientScope, address: ProgramSessionAddress): SessionFiles {
  const remote = owner.context.remote.reactivePrograms;
  const read = <T>(options: CancellationOptions, invoke: (signal: AbortSignal) => Promise<RemoteResult<T>>): Promise<T> => {
    owner.assertActive();
    const signal = options.signal ? AbortSignal.any([owner.signal, options.signal]) : owner.signal;
    signal.throwIfAborted();
    const lease = owner.acquire(() => invoke(signal), async pending => {
      // 请求失败仍归本次读取；owner只等待实际传输结算，不把业务拒绝当清理失败。
      await pending.then(() => {}, () => {});
    }, 'program-file-read:' + address.sessionId);
    const release = () => { void lease.release().catch(() => {}); };
    void lease.value.then(release, release);
    return owner.run(async () => {
      const reply = await lease.value;
      signal.throwIfAborted(); owner.assertActive();
      if (!reply.ok) throw revokeConversationAccess(owner, decodeProgramFailure(reply.error));
      return reply.value;
    }, signal);
  };
  const reads = {
    readText(path: string, range: WorkspaceFileRange = {}, options: CancellationOptions = {}): Promise<WorkspaceFileText> {
      const selected = structuredClone(range);
      return read(options, signal => remote.fileRead({ ...address, path, range: selected }, signal));
    },
    readBytes(path: string, range: WorkspaceByteReadOptions = {}, options: CancellationOptions = {}): Promise<WorkspaceFileBytes> {
      const selected = structuredClone(range);
      return read(options, signal => remote.fileReadBytes({ ...address, path, range: selected }, signal));
    },
    stat(path: string, options: CancellationOptions = {}): Promise<WorkspaceFileStat> {
      return read(options, signal => remote.fileStat({ ...address, path }, signal));
    },
    list(path: string, options: CancellationOptions = {}): Promise<WorkspaceDirectoryListing> {
      return read(options, signal => remote.fileList({ ...address, path }, signal));
    },
  };
  const resources = createFileResources(reads, { signal: owner.signal,
    watch: (path, invalidate) => fileWatch(owner, address, path, {}, invalidate),
    // 各watch的lease已在ClientScope登记；真实释放失败由同一owner.close汇总。
    onCleanupError: () => {},
  });
  return Object.freeze({ ...reads, ...resources,
    watch: (path: string, options: CancellationOptions = {}) => fileWatch(owner, address, path, options),
  });
}
