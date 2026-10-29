import type { JobRegistry, JobId, JobEvent, JobSpec, JobView, JobRead, JobOutputRead } from '@deepseek-ai/dsh-jobs';
import { symbols } from '@deepseek-ai/cordis';
import LocalJobs, { type Config as LocalJobsConfig } from '@deepseek-ai/dsh-jobs-local';
import * as ToolJobs from '@deepseek-ai/dsh-tool-jobs';
import { Observable, Subject, defer, filter, firstValueFrom, fromEvent, map, of, takeUntil, timeout } from 'rxjs';
import { fromSnapshot, type ResourceReader, type CancellationOptions } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';
import { NativeCapabilityOwner, nativeObservation, nativeObservationScope, nativeResource, nativeToolNames, ownNativeValue, type NativeCapability, type NativeSessionSource } from '../native-capability.ts';
import { nativeSessionAccess, type NativeSessionAccess } from '../conversations/session-access.ts';

export { JobId } from './client.ts';
export type { JobView, JobKind, JobKindMap } from './client.ts';
export type { JobEvent, JobRead, JobOutputRead, JobSpec, JobHooks, JobOutcome } from '@deepseek-ai/dsh-jobs';

export interface NativeJobsOptions {
  /** local 安装正式进程内注册表；provided 使用宿主已经安装的同一服务。 */
  readonly registry: { readonly kind: 'local'; readonly config?: LocalJobsConfig } | { readonly kind: 'provided' };
  /** 模型工具和完成通知显式启用；不选择时只有 SDK 控制面，不暗中唤醒模型。 */
  readonly tools?: ToolJobs.Config & { readonly completionDelivery: ToolJobs.CompletionDelivery };
}

/** owner 与 toolCall 不从业务参数补写；这里接原生正式 producer 合同。 */
export type NativeJobSpec = Omit<JobSpec, 'owner' | 'toolCall'>;

type JobInvalidation = Pick<JobView, 'id' | 'owner'>;
/** 只共享失效通知；快照、输出、消费游标和执行始终属于正式注册表。 */
const invalidations = new WeakMap<object, Subject<JobInvalidation>>();
const observedToolResults = new WeakMap<object, WeakSet<object>>();
function registrySource(registry: JobRegistry): object {
  const source: unknown = Reflect.get(registry, symbols.original);
  if (typeof source !== 'object' || source === null) throw new Error('REACTIVE_NATIVE_JOBS_SOURCE_REQUIRED');
  return source;
}
function jobInvalidations(registry: JobRegistry): Subject<JobInvalidation> {
  const source = registrySource(registry);
  let changes = invalidations.get(source);
  if (!changes) { changes = new Subject<JobInvalidation>(); invalidations.set(source, changes); }
  return changes;
}
function invalidateJob(registry: JobRegistry, job: JobView): void {
  jobInvalidations(registry).next({ id: job.id, owner: job.owner });
}

/** 固定原生 Job 目标的非消费读取与显式控制；未知或外来目标由原生读写边界拒绝。 */
export interface NativeJob {
  readonly id: JobId;
  readonly state: ResourceReader<JobView>;
  readonly output: ResourceReader<JobOutputRead>;
  readonly state$: Observable<JobView>;
  readonly output$: Observable<JobOutputRead>;
  snapshot(): JobView;
  readAt(fromByte: number): JobOutputRead;
  /** 非消费状态等待；不取得模型完成通知或输出游标的交付权。 */
  wait(timeoutMs: number, options?: CancellationOptions): Promise<JobView>;
  /** 显式消费模型游标；终态 result 仅由原生首次消费读取交付一次。 */
  collect(options?: CancellationOptions): JobRead;
  /** 宿主直接消费原生注册表后显式重读；失效通知在同一注册表的可见读取对象间共享。 */
  invalidate(options?: CancellationOptions): void;
  stop(reason: string, options?: CancellationOptions): 'requested' | 'already-finished';
  /** 删除终态记录；调用方决定何时清理，不读取结果或确认模型已经收取。 */
  remove(options?: CancellationOptions): void;
}

class NativeJobReader implements NativeJob {
  readonly state: ResourceReader<JobView>;
  readonly output: ResourceReader<JobOutputRead>;
  readonly state$: Observable<JobView>;
  readonly output$: Observable<JobOutputRead>;
  constructor(private readonly jobs: SessionJobs, readonly id: JobId, scope: Scope) {
    const changes = (notify: () => void) => jobs.subscribeChanges(id, notify);
    this.state = nativeResource(scope, () => fromSnapshot({
      getSnapshot: () => this.snapshot(),
      subscribe: changes,
    }).pipe(map(value => ({ status: 'ready', generation: 0, observedAt: Date.now(), value }))));
    this.output = nativeResource(scope, () => fromSnapshot({ getSnapshot: () => this.readAt(0), subscribe: changes })
      .pipe(map(value => ({ status: 'ready', generation: 0, observedAt: Date.now(), value }))));
    this.state$ = this.state.values$;
    this.output$ = this.output.values$;
  }
  snapshot(): JobView { return ownNativeValue(this.jobs.registry().get(this.id, this.jobs.ownerId)); }
  /** 绝对字节偏移不推进模型游标；lossy/next 保留原生含义。 */
  readAt(fromByte: number): JobOutputRead { return ownNativeValue(this.jobs.registry().readAt(this.id, fromByte, this.jobs.ownerId)); }
  /** 只观察状态，不进入原生消费等待；超时返回当时快照，取消只退出本次等待。 */
  wait(timeoutMs: number, options: CancellationOptions = {}): Promise<JobView> {
    const signal = this.jobs.signal(options);
    signal.throwIfAborted();
    const cancelled = defer(() => {
      signal.throwIfAborted();
      return fromEvent(signal, 'abort').pipe(map((): never => { throw signal.reason; }));
    });
    return firstValueFrom(defer(() => {
      if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
        throw new Error(`invalid wait timeout: expected a positive number of milliseconds, got ${JSON.stringify(timeoutMs)}`);
      }
      return this.state.values$.pipe(
        filter(job => job.status !== 'running' && job.status !== 'stopping'),
        timeout({ first: timeoutMs, with: () => of(this.snapshot()) }),
      );
    }).pipe(takeUntil(cancelled))).then(ownNativeValue);
  }
  collect(options: CancellationOptions = {}): JobRead {
    this.jobs.signal(options).throwIfAborted();
    const registry = this.jobs.registry();
    const read = registry.read(this.id, this.jobs.ownerId);
    invalidateJob(registry, read.job);
    return ownNativeValue(read);
  }
  invalidate(options: CancellationOptions = {}): void {
    this.jobs.signal(options).throwIfAborted();
    const registry = this.jobs.registry();
    invalidateJob(registry, registry.get(this.id, this.jobs.ownerId));
  }
  /** 停止是显式命令，不等于已经停止；需要确认时另行 wait。 */
  stop(reason: string, options: CancellationOptions = {}): 'requested' | 'already-finished' {
    this.jobs.signal(options).throwIfAborted();
    return this.jobs.registry().kill(this.id, this.jobs.ownerId, reason);
  }
  remove(options: CancellationOptions = {}): void {
    this.jobs.signal(options).throwIfAborted();
    this.jobs.registry().remove(this.id, this.jobs.ownerId);
  }
}

/** 同一 exact 会话句柄共享读取连接，原生 Agent 持有已开始执行。 */
export interface NativeSessionJobs {
  readonly state: ResourceReader<readonly JobView[]>;
  readonly list$: Observable<readonly JobView[]>;
  readonly events$: Observable<JobEvent>;
  list(): readonly JobView[];
  /** 同步绑定固定目标，不读取注册表；读取与控制分别核验真实存在性和 owner。 */
  job(id: JobId): NativeJob;
  start(spec: NativeJobSpec, options?: CancellationOptions): JobId;
  dispose(): void;
}

class SessionJobs implements NativeSessionJobs {
  readonly state: ResourceReader<readonly JobView[]>;
  readonly list$: Observable<readonly JobView[]>;
  readonly events$: Observable<JobEvent>;
  readonly ownerId: NativeSessionAccess['sessionId'];
  #scope: Scope;
  #jobs = new Map<JobId, NativeJob>();
  get disposed(): boolean { return this.#scope.disposed; }

  constructor(private readonly native: NativeCapabilityOwner, private readonly access: NativeSessionAccess) {
    this.#scope = nativeObservationScope(native, access);
    this.ownerId = access.sessionId;
    this.events$ = nativeObservation(this.#scope, new Observable(subscriber =>
      this.subscribe(event => {
        try { this.registry(); subscriber.next(ownNativeValue(event)); }
        catch (error) { subscriber.error(error); }
      })));
    this.state = nativeResource(this.#scope, () => fromSnapshot({
      getSnapshot: () => this.list(), subscribe: notify => this.subscribeChanges(undefined, notify),
    }).pipe(map(value => ({ status: 'ready', generation: 0, observedAt: Date.now(), value }))));
    this.list$ = this.state.values$;
  }
  /** @internal 每次控制/读取都重新核 exact Host、成员及官方服务。 */
  registry(): JobRegistry {
    this.#scope.assertActive();
    const context = this.native.session(this.access, 'jobs');
    const registry: JobRegistry | undefined = context.get('jobs');
    if (!registry) throw new Error('REACTIVE_NATIVE_JOBS_REQUIRED');
    return registry;
  }
  /** @internal 原生事件订阅不拥有执行寿命。 */
  subscribe(accept: (event: JobEvent) => void): () => void {
    return this.registry().events.subscribe({ owner: this.ownerId }, event => {
      // Service 的事件回调是 contained；观察错误必须交给 RxJS，而不是静默留旧值。
      accept(event);
    });
  }
  /** @internal 原生事件与显式失效分别连接；每个观察 owner 只持有自己的监听。 */
  subscribeChanges(id: JobId | undefined, notify: () => void): () => void {
    const registry = this.registry();
    const changes = jobInvalidations(registry).subscribe(change => {
      if ((id === undefined || change.id === id) && (change.owner === undefined || change.owner === this.ownerId)) notify();
    });
    let release: () => void;
    try {
      release = this.subscribe(event => {
        if (id === undefined || (event.type === 'output' ? event.id : event.job.id) === id) notify();
      });
    } catch (error) { changes.unsubscribe(); throw error; }
    return () => { try { release(); } finally { changes.unsubscribe(); } };
  }
  /** @internal 取消观察/等待，不取消 Job body。 */
  signal(options: CancellationOptions): AbortSignal {
    this.registry();
    return options.signal ? AbortSignal.any([this.#scope.signal, options.signal]) : this.#scope.signal;
  }
  list(): readonly JobView[] { return ownNativeValue(this.registry().list(this.ownerId)); }
  job(id: JobId): NativeJob {
    this.#scope.assertActive();
    this.native.session(this.access, 'jobs');
    let job = this.#jobs.get(id);
    if (!job) { job = new NativeJobReader(this, id, this.#scope); this.#jobs.set(id, job); }
    return job;
  }
  start(spec: NativeJobSpec, options: CancellationOptions = {}): JobId {
    const signal = this.signal(options); signal.throwIfAborted();
    if (Object.hasOwn(spec, 'owner') || Object.hasOwn(spec, 'toolCall')) throw new Error('REACTIVE_NATIVE_JOB_SOURCE_RESERVED');
    const agent = this.access.liveAgent();
    if (!agent || agent.id !== this.ownerId) throw new Error('REACTIVE_CONVERSATION_NOT_RUNNING');
    // registry.start 自己核 exact Agent、容量、controller 并原子签发 id；不重跑 starter。
    return this.registry().start({ ...spec, owner: this.ownerId });
  }
  /** 只释放这个 SDK 读取对象；执行仍属于原生 Agent/JobRegistry。 */
  dispose(): void { this.#scope.dispose(); }
}

export interface NativeJobs extends NativeCapability {
  /** 本能力显式启用并实际注册的原生工具；读取不启动Job。 */
  readonly toolNames: ResourceReader<readonly string[]>;
  readonly toolNames$: Observable<readonly string[]>;
  forSession(session: NativeSessionSource): NativeSessionJobs;
}

/** 正式 Jobs 配置、安装 owner 与会话 SDK。没有安装选择时不会创建服务或工作。 */
export function createNativeJobs(options: NativeJobsOptions): NativeJobs {
  const selected = structuredClone(options);
  if (selected.registry.kind !== 'local' && selected.registry.kind !== 'provided') throw new Error('REACTIVE_NATIVE_JOBS_REGISTRY_INVALID');
  if (selected.tools && selected.tools.completionDelivery !== 'quiet' && selected.tools.completionDelivery !== 'wakeup') {
    throw new Error('REACTIVE_NATIVE_JOBS_DELIVERY_REQUIRED');
  }
  const sessions = new WeakMap<object, SessionJobs>();
  const owner = new NativeCapabilityOwner('reactive-native-jobs', ['jobs', ...(selected.tools ? ['tools'] : [])], function* (ctx) {
    if (selected.registry.kind === 'local') yield ctx.plugin(LocalJobs, selected.registry.config ?? {});
    else if (!ctx.get('jobs')) throw new Error('REACTIVE_NATIVE_JOBS_REQUIRED');
    yield ctx.inject(['jobs'], native => {
      native.jobs.attachController('reactive-native-jobs-sdk');
    });
    if (selected.tools) {
      yield ctx.plugin(ToolJobs, selected.tools);
      yield ctx.inject(['jobs', 'tools'], native => native.on('tools/result', execution => {
        if (owner.scope.disposed || execution.name !== 'job_output') return;
        const args = execution.arguments;
        if (typeof args !== 'object' || args === null || !('job_id' in args) || typeof args.job_id !== 'string') return;
        const registry = native.jobs;
        const source = registrySource(registry);
        let observed = observedToolResults.get(source);
        if (!observed) { observed = new WeakSet<object>(); observedToolResults.set(source, observed); }
        if (observed.has(execution)) return;
        // 最终失败也可能发生在消费之后；只重读正式可见目标，不从工具结果推断Job状态。
        const visible = registry.list(execution.agent?.id).find(job => job.id === args.job_id);
        if (!visible) return;
        const job = registry.get(visible.id, execution.agent?.id);
        observed.add(execution);
        invalidateJob(registry, job);
      }));
    }
  });
  const names = new Set(['job_list', 'job_output', 'job_kill']);
  const toolNames = selected.tools ? nativeToolNames(owner, name => names.has(name)) : nativeResource<readonly string[]>(owner.scope, () => {
    owner.context();
    return of({ status: 'ready', generation: 0, observedAt: Date.now(), value: Object.freeze([]) });
  });
  return Object.freeze({ install: owner.install, dispose: () => owner.dispose(), toolNames, toolNames$: toolNames.values$,
    forSession(session: NativeSessionSource) {
      const access = nativeSessionAccess(session);
      owner.session(access, 'jobs');
      let current = sessions.get(session);
      if (!current || current.disposed) { current = new SessionJobs(owner, access); sessions.set(session, current); }
      return current;
    },
  });
}
