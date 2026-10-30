import { Observable, Subscriber, Subscription, defer, filter, finalize, ignoreElements, map, observeOn, of,
  queueScheduler, takeUntil, tap } from 'rxjs';
import type { HumanSnapshot } from '../participation/contract.ts';
import type { Observation } from '../resources/observation.ts';
import { combineResources, mapResource, switchResource } from '../resources/operators.ts';
import type { ResourceReader, ResourceReaderOptions } from '../resources/reader.ts';
import { shareResource } from '../resources/sharing.ts';
import { resourceObservations, type ResourceSource } from '../resources/source.ts';
import type { AssistantProgressFrame, ConversationEvent } from './events.ts';
import type { Json } from './protocol.ts';
import type { ConversationProgressSnapshot } from './request.ts';
import type { WorkEvent, WorkProgressFrame, WorkSessionHandle, WorkSessionSnapshot, WorkSource, WorkSourceIdentity } from './work.ts';
import { borrowWorkResource, type WorkResourceConnection } from './work-resource.ts';

/** 成员身份与借用连接；业务选择和原生会话寿命分别由投影者和真实来源负责。 */
export function createWorkSessions<K extends Json>(
  sources: readonly WorkSource[] | ResourceSource<readonly WorkSource[]> | undefined,
  project: {
    snapshot(source: WorkSourceIdentity, value: { progress: ConversationProgressSnapshot; human: HumanSnapshot }): WorkSessionSnapshot;
    event(source: WorkSourceIdentity, event: ConversationEvent): WorkEvent<K> | null;
    frame(source: WorkSourceIdentity, frame: AssistantProgressFrame): WorkProgressFrame<K> | null;
  },
  options?: ResourceReaderOptions,
): {
  sessions: ResourceReader<readonly WorkSessionHandle<K>[]>;
  state: ResourceSource<readonly WorkSessionSnapshot[]>;
  events$: Observable<WorkEvent<K>>;
  progressFrames$: Observable<WorkProgressFrame<K>>;
} {
  const onCleanupError = options?.onCleanupError;
  // 缓存只有纯句柄，没有常驻订阅；同 sessionId 的新对象不能改写旧句柄的来源。
  const handles = new WeakMap<WorkSource, WorkSessionHandle<K>>();
  const aborted$ = new Observable<void>(subscriber => {
    const signal = options?.signal;
    if (!signal) return;
    const abort = () => { subscriber.next(); subscriber.complete(); };
    if (signal.aborted) { abort(); return; }
    signal.addEventListener('abort', abort, { once: true });
    return () => signal.removeEventListener('abort', abort);
  });
  const capture = (values: readonly WorkSource[]): readonly WorkSessionHandle<K>[] => {
    const identities = new Set<string>();
    return Object.freeze(values.map(source => {
      let handle = handles.get(source);
      const sourceIdentity = handle?.identity ?? source.identity;
      const identity = handle?.identity ?? Object.freeze({ sessionId: sourceIdentity.sessionId,
        workspaceId: sourceIdentity.workspaceId, environmentId: sourceIdentity.environmentId,
        parentSessionId: sourceIdentity.parentSessionId });
      if (!identity.sessionId.trim()) throw new Error('REACTIVE_WORK_SESSION_ID_REQUIRED');
      if (identities.has(identity.sessionId)) throw new Error('REACTIVE_WORK_SOURCE_DUPLICATE');
      identities.add(identity.sessionId);
      if (!handle) {
        // 捕获能力本身，之后不回读可变的 source 属性；成员移除只结束群组借用。
        const progress = source.progress.observations$;
        const human = source.human.observations$;
        const rawEvents$ = source.rawEvents$;
        const progressFrames$ = source.progressFrames$;
        const state = shareResource(combineResources({ progress, human }).pipe(
          mapResource(value => project.snapshot(identity, value)),
        ), options);
        handle = Object.freeze({ identity, state, state$: state.observations$,
          events$: rawEvents$.pipe(takeUntil(aborted$), map(event => project.event(identity, event)),
            filter((event): event is WorkEvent<K> => event !== null)),
          progressFrames$: progressFrames$.pipe(takeUntil(aborted$), map(frame => project.frame(identity, frame)),
            filter((frame): frame is WorkProgressFrame<K> => frame !== null)),
        });
        handles.set(source, handle);
      }
      return handle;
    }));
  };
  // 静态名单立即捕获；动态名单每个 ready 都核验，错误不封死资源的后续代次。
  const membership: Observable<Observation<readonly WorkSessionHandle<K>[]>> = sources === undefined || Array.isArray(sources)
    ? ready(capture((sources ?? []) as readonly WorkSource[]))
    : resourceObservations(sources as ResourceSource<readonly WorkSource[]>).pipe(mapResource(capture));
  // 一次名单必须先交付全部观察面，再处理任一消费者 teardown 重入的新名单。
  const sessions = shareResource<readonly WorkSessionHandle<K>[]>(membership.pipe(
    observeOn<Observation<readonly WorkSessionHandle<K>[]>>(queueScheduler),
  ), options);
  const state = defer(() => {
    const connections = new Map<WorkSessionHandle<K>, WorkResourceConnection<WorkSessionSnapshot>>();
    const release = (handles: readonly WorkSessionHandle<K>[]) => {
      const cleanup = new Subscription();
      for (const handle of handles) {
        const connection = connections.get(handle);
        connections.delete(handle);
        if (connection) cleanup.add(connection);
      }
      cleanup.unsubscribe();
    };
    const releaseAll = () => release([...connections.keys()]);
    return sessions.observations$.pipe(
      // 名单事实和清理重入共用同步队列，旧 teardown 不能覆盖较新的名单。
      observeOn(queueScheduler),
      map(observation => {
        if (observation.status === 'ready') return observation;
        try { releaseAll(); }
        catch (error) { return Object.freeze({ ...observation, status: 'error' as const,
          error: observation.status === 'error'
            ? new AggregateError([observation.error, error], 'REACTIVE_WORK_MEMBERS_RELEASE_FAILED') : error }); }
        return observation;
      }),
      switchResource(values => {
        const current = new Set(values);
        release([...connections.keys()].filter(handle => !current.has(handle)));
        if (!values.length) return ready<readonly WorkSessionSnapshot[]>(Object.freeze([]));
        const members: Record<string, Observable<Observation<WorkSessionSnapshot>>> = {};
        values.forEach((handle, index) => {
          let connection = connections.get(handle);
          if (!connection) {
            connection = borrowWorkResource(handle.state.observations$, onCleanupError);
            connections.set(handle, connection);
          }
          members[index] = connection.observations$;
        });
        return combineResources(members).pipe(mapResource(value => Object.freeze(Object.values(value))));
      }),
      finalize(releaseAll),
    );
  });
  const events$ = observeMembers(sessions, handle => handle.events$).pipe(takeUntil(aborted$));
  const progressFrames$ = observeMembers(sessions, handle => handle.progressFrames$).pipe(takeUntil(aborted$));
  return Object.freeze({ sessions, state, events$, progressFrames$ });
}

/** 群组事件无重放；变更名单时只增减实际变化的连接。 */
function observeMembers<K extends Json, T>(
  sessions: ResourceReader<readonly WorkSessionHandle<K>[]>,
  select: (handle: WorkSessionHandle<K>) => Observable<T>,
): Observable<T> {
  return new Observable<T>(subscriber => {
    const connections = new Map<WorkSessionHandle<K>, Subscriber<T>>();
    const releaseAll = () => {
      const previous = [...connections.values()];
      connections.clear();
      const cleanup = new Subscription();
      for (const connection of previous) cleanup.add(connection);
      cleanup.unsubscribe();
    };
    subscriber.add(releaseAll);
    sessions.observations$.pipe(
      observeOn(queueScheduler),
      tap(observation => {
        if (observation.status !== 'ready') {
          releaseAll();
          if (observation.status === 'error') throw observation.error;
          return;
        }
        const current = new Set(observation.value);
        for (const [handle, connection] of connections) {
          if (current.has(handle)) continue;
          connections.delete(handle);
          connection.unsubscribe();
        }
        for (const handle of observation.value) {
          if (subscriber.closed) break;
          if (connections.has(handle)) continue;
          const connection = new Subscriber<T>({ next: value => subscriber.next(value),
            error: (error: unknown) => subscriber.error(error), complete: () => {} });
          // 同步来源也必须先进入连接表；下游在 next 内退出时即可一并释放。
          connections.set(handle, connection);
          select(handle).subscribe(connection);
        }
      }),
      ignoreElements(),
    ).subscribe(subscriber);
  });
}

function ready<T>(value: T): Observable<Observation<T>> {
  return of(Object.freeze({ status: 'ready' as const, generation: 0, observedAt: Date.now(), value }));
}
