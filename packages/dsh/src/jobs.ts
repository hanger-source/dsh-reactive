import type { JobRegistry, JobId, JobEvent, JobSpec, JobView, JobOutputRead } from '@deepseek-ai/dsh-jobs';
import LocalJobs, { type Config as LocalJobsConfig } from '@deepseek-ai/dsh-jobs-local';
import * as ToolJobs from '@deepseek-ai/dsh-tool-jobs';
import { Observable, map } from 'rxjs';
import { fromSnapshot, type ResourceReader, type CancellationOptions } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';
import { requireActivePlugin } from './native-installation.ts';
import { NativeCapabilityOwner, nativeObservation, nativeObservationScope, nativeResource, nativeToolNames, ownNativeValue, type NativeCapability, type NativeSessionSource } from './native-capability.ts';
import { nativeSessionAccess, type NativeSessionAccess } from './conversations/session-access.ts';

export { JobId } from '@deepseek-ai/dsh-jobs';
export type { JobEvent, JobView, JobOutputRead, JobSpec, JobHooks, JobOutcome, JobKind, JobKindMap } from '@deepseek-ai/dsh-jobs';

export interface NativeJobsOptions {
  /** local 安装正式进程内注册表；provided 使用宿主已经安装的同一服务。 */
  readonly registry: { readonly kind: 'local'; readonly config?: LocalJobsConfig } | { readonly kind: 'provided' };
  /** 模型工具和完成通知显式启用；不选择时只有 SDK 控制面，不暗中唤醒模型。 */
  readonly tools?: ToolJobs.Config & { readonly completionDelivery: ToolJobs.CompletionDelivery };
}

/** owner 与 toolCall 不从业务参数补写；这里接原生正式 producer 合同。 */
export type NativeJobSpec = Omit<JobSpec, 'owner' | 'toolCall'>;

/** 原生 Job 执行实例的非消费读取与显式控制；它不是持久业务 Operation。 */
export interface NativeJob {
  readonly id: JobId;
  readonly state: ResourceReader<JobView>;
  readonly output: ResourceReader<JobOutputRead>;
  readonly state$: Observable<JobView>;
  readonly output$: Observable<JobOutputRead>;
  snapshot(): JobView;
  readAt(fromByte: number): JobOutputRead;
  wait(timeoutMs: number, options?: CancellationOptions): Promise<JobView>;
  stop(reason: string, options?: CancellationOptions): 'requested' | 'already-finished';
}

class NativeJobReader implements NativeJob {
  readonly state: ResourceReader<JobView>;
  readonly output: ResourceReader<JobOutputRead>;
  readonly state$: Observable<JobView>;
  readonly output$: Observable<JobOutputRead>;
  constructor(private readonly jobs: SessionJobs, readonly id: JobId, scope: Scope) {
    const changes = (notify: () => void) => jobs.subscribe(event => {
      if ((event.type === 'output' ? event.id : event.job.id) === id) notify();
    });
    this.state = nativeResource(scope, () => fromSnapshot({
      getSnapshot: () => this.snapshot(),
      subscribe: notify => jobs.subscribe(event => {
        if (event.type !== 'output' && event.job.id === id) notify();
      }),
    }).pipe(map(value => ({ status: 'ready', generation: 0, observedAt: Date.now(), value }))));
    this.output = nativeResource(scope, () => fromSnapshot({ getSnapshot: () => this.readAt(0), subscribe: changes })
      .pipe(map(value => ({ status: 'ready', generation: 0, observedAt: Date.now(), value }))));
    this.state$ = this.state.values$;
    this.output$ = this.output.values$;
  }
  snapshot(): JobView { return ownNativeValue(this.jobs.registry().get(this.id, this.jobs.ownerId)); }
  /** 绝对字节偏移不推进模型游标；lossy/next 保留原生含义。 */
  readAt(fromByte: number): JobOutputRead { return ownNativeValue(this.jobs.registry().readAt(this.id, fromByte, this.jobs.ownerId)); }
  /** 只取消本次等待，不停止执行；超时仍可能返回 running/stopping。 */
  wait(timeoutMs: number, options: CancellationOptions = {}): Promise<JobView> {
    const signal = this.jobs.signal(options);
    signal.throwIfAborted();
    return this.jobs.registry().wait(this.id, timeoutMs, this.jobs.ownerId, signal).then(ownNativeValue);
  }
  /** 停止是显式命令，不等于已经停止；需要确认时另行 wait。 */
  stop(reason: string, options: CancellationOptions = {}): 'requested' | 'already-finished' {
    this.jobs.signal(options).throwIfAborted();
    return this.jobs.registry().kill(this.id, this.jobs.ownerId, reason);
  }
}

/** 同一 exact 会话句柄共享读取连接，原生 Agent 持有已开始执行。 */
export interface NativeSessionJobs {
  readonly state: ResourceReader<readonly JobView[]>;
  readonly list$: Observable<readonly JobView[]>;
  readonly events$: Observable<JobEvent>;
  list(): readonly JobView[];
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
      getSnapshot: () => this.list(), subscribe: notify => this.subscribe(notify),
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
  /** @internal 取消观察/等待，不取消 Job body。 */
  signal(options: CancellationOptions): AbortSignal {
    this.registry();
    return options.signal ? AbortSignal.any([this.#scope.signal, options.signal]) : this.#scope.signal;
  }
  list(): readonly JobView[] { return ownNativeValue(this.registry().list(this.ownerId)); }
  job(id: JobId): NativeJob {
    this.registry().get(id, this.ownerId);
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
  const owner = new NativeCapabilityOwner('reactive-native-jobs', async ctx => {
    if (selected.registry.kind === 'local') await requireActivePlugin(ctx.plugin(LocalJobs, selected.registry.config ?? {}));
    if (!ctx.get('jobs')) throw new Error('REACTIVE_NATIVE_JOBS_REQUIRED');
    await requireActivePlugin(ctx.inject(['jobs'], native => {
      native.jobs.attachController('reactive-native-jobs-sdk');
    }));
    if (selected.tools) await requireActivePlugin(ctx.plugin(ToolJobs, selected.tools));
  });
  const names = new Set(['job_list', 'job_output', 'job_kill']);
  const toolNames = nativeToolNames(owner, name => selected.tools !== undefined && names.has(name));
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
