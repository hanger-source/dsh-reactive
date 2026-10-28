import ScheduleService, { type Config, type ScheduleCatalogEntry, type ScheduleRecord,
  type ScheduleUpdateRequest, type ScheduleUpdateResult, type ScheduleDeleteResult,
  type ScheduleDeliveryHistoryRequest, type ScheduleDeliveryHistoryResult } from '@deepseek-ai/dsh-schedule';
import type { ScheduleId } from '@deepseek-ai/dsh-schedule/client';
import { Observable, firstValueFrom, map } from 'rxjs';
import { abortable, observeResource, mapResource, type ResourceReader, type CancellationOptions } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';
import { NativeCapabilityOwner, nativeResource, nativeObservationScope, ownNativeValue,
  type NativeCapability, type NativeInvocation, type NativeSessionSource } from './native-capability.ts';
import { nativeSessionAccess, type NativeSessionAccess } from './conversations/session-access.ts';

export type { ScheduleId, ScheduleRecord, ScheduleCatalogEntry, ScheduleDeliveryReceipt, ScheduleDeliveryRecord,
  ScheduleTimingChange, ScheduleUpdateResult, ScheduleDeleteResult, ScheduleDeliveryHistoryResult } from '@deepseek-ai/dsh-schedule/client';

export interface NativeScheduleOptions {
  /** install 仅安装正式管理 owner，不安装创建提醒的模型工具；宿主须提供它的全部依赖。 */
  readonly service: { readonly kind: 'install'; readonly config?: Config } | { readonly kind: 'provided' };
}
export type NativeScheduleUpdate = Omit<ScheduleUpdateRequest, 'sessionId' | 'id'>;
export type NativeScheduleHistory = Pick<ScheduleDeliveryHistoryRequest, 'limit' | 'before'>;

/** catalog 的会话/状态/投递元数据不是原生 CAS record 的字段。 */
function scheduleRecord(entry: ScheduleCatalogEntry): ScheduleRecord {
  const { sessionId, status, lastDelivery, ...record } = entry;
  return ownNativeValue(record);
}

/** 原会话内的固定提醒目标；读取、历史与变更各自核验归属，缺席读取为 undefined。 */
export interface NativeScheduleTask {
  readonly id: ScheduleId;
  readonly state: ResourceReader<ScheduleCatalogEntry | undefined>;
  readonly state$: Observable<ScheduleCatalogEntry | undefined>;
  /** 完整 ScheduleRecord，可原样用于 expected；不混入 catalog 的额外字段。 */
  read(options?: CancellationOptions): Promise<ScheduleRecord | undefined>;
  history(page: NativeScheduleHistory, options?: CancellationOptions): Promise<ScheduleDeliveryHistoryResult>;
  update(request: NativeScheduleUpdate, options?: CancellationOptions): NativeInvocation<ScheduleUpdateResult>;
  delete(options?: CancellationOptions): NativeInvocation<ScheduleDeleteResult>;
}

export interface NativeSessionSchedule {
  readonly state: ResourceReader<readonly ScheduleCatalogEntry[]>;
  readonly active$: Observable<readonly ScheduleRecord[]>;
  readonly catalog$: Observable<readonly ScheduleCatalogEntry[]>;
  active(options?: CancellationOptions): Promise<readonly ScheduleRecord[]>;
  catalog(options?: CancellationOptions): Promise<readonly ScheduleCatalogEntry[]>;
  /** 同步绑定固定目标，不读取 catalog；取得对象不表示提醒存在或已经获得修改资格。 */
  task(id: ScheduleId): NativeScheduleTask;
  /** 释放管理观察，不删除提醒或撤回已经投递的输入。 */
  dispose(): void;
}

class SessionSchedule implements NativeSessionSchedule {
  readonly state: ResourceReader<readonly ScheduleCatalogEntry[]>;
  readonly active$: Observable<readonly ScheduleRecord[]>;
  readonly catalog$: Observable<readonly ScheduleCatalogEntry[]>;
  readonly scope: Scope;
  #tasks = new Map<ScheduleId, NativeScheduleTask>();
  get disposed(): boolean { return this.scope.disposed; }

  constructor(private readonly owner: NativeCapabilityOwner, private readonly access: NativeSessionAccess) {
    this.scope = nativeObservationScope(owner, access);
    const changes$ = new Observable<void>(subscriber => {
      const ctx = this.context();
      const release = ctx.on('schedule/changed', () => subscriber.next());
      subscriber.next();
      return release;
    });
    this.state = nativeResource(this.scope, () => observeResource({ key$: changes$, read: (_key, signal) => this.catalog({ signal }) }));
    this.catalog$ = this.state.values$;
    // 同一提交切面同时含 active/inactive；不另起一条原生变更监听。
    this.active$ = this.catalog$.pipe(map(records => Object.freeze(records.filter(record => record.status === 'active').map(scheduleRecord))));
  }
  private context() {
    this.scope.assertActive();
    return this.owner.session(this.access, 'schedule');
  }
  service(): ScheduleService {
    const service: ScheduleService | undefined = this.context().get('schedule');
    if (!service) throw new Error('REACTIVE_NATIVE_SCHEDULE_REQUIRED');
    return service;
  }
  signal(options: CancellationOptions): AbortSignal {
    this.context();
    const signal = options.signal ? AbortSignal.any([this.scope.signal, options.signal]) : this.scope.signal;
    signal.throwIfAborted();
    return signal;
  }
  async active(options: CancellationOptions = {}): Promise<readonly ScheduleRecord[]> {
    const signal = this.signal(options);
    const records = await firstValueFrom(abortable(() => this.service().list({ sessionId: this.access.sessionId }), signal));
    signal.throwIfAborted(); this.context();
    return ownNativeValue(records);
  }
  async catalog(options: CancellationOptions = {}): Promise<readonly ScheduleCatalogEntry[]> {
    const signal = this.signal(options);
    const records = await firstValueFrom(abortable(() => this.service().catalog(), signal));
    signal.throwIfAborted(); this.context();
    return ownNativeValue(records.filter(record => record.sessionId === this.access.sessionId));
  }
  task(id: ScheduleId): NativeScheduleTask {
    this.context();
    let task = this.#tasks.get(id);
    if (!task) {
      const state = nativeResource(this.scope, () => this.state.observations$.pipe(mapResource(current => current.find(record => record.id === id))));
      task = Object.freeze({ id,
        state, state$: state.values$,
        read: async (opts: CancellationOptions = {}) => {
          const current = (await this.catalog(opts)).find(record => record.id === id);
          return current === undefined ? undefined : scheduleRecord(current);
        },
        history: async (page: NativeScheduleHistory, opts: CancellationOptions = {}) => {
          const signal = this.signal(opts);
          const result = await firstValueFrom(abortable(() => this.service().history({ ...structuredClone(page), id, sessionId: this.access.sessionId }), signal));
          signal.throwIfAborted(); this.context();
          return ownNativeValue(result);
        },
        update: (request: NativeScheduleUpdate, opts: CancellationOptions = {}) => {
          const signal = this.signal(opts);
          const selected = structuredClone(request);
          if (selected.expected.id !== id) throw new Error('REACTIVE_NATIVE_SCHEDULE_EXPECTED_MISMATCH');
          const service = this.service(), sessionId = this.access.sessionId;
          // 原生signal保留FIFO后的执行前取消；结果与独立回执等待分别结算。
          return this.owner.invoke({ sessionId, operation: 'update' }, async () =>
            ownNativeValue(await service.update({ ...selected, id, sessionId }, signal)), signal);
        },
        delete: (opts: CancellationOptions = {}) => {
          const signal = this.signal(opts);
          const service = this.service(), sessionId = this.access.sessionId;
          return this.owner.invoke({ sessionId, operation: 'delete' }, async () =>
            ownNativeValue(await service.delete({ id, sessionId }, signal)), signal);
        },
      });
      this.#tasks.set(id, task);
    }
    return task;
  }
  dispose(): void { this.scope.dispose(); }
}

export interface NativeSchedule extends NativeCapability {
  forSession(session: NativeSessionSource): NativeSessionSchedule;
}

/**
 * 正式提醒的管理面。rc.3 自动投递没有 Program 来源准入前门，因此本接口不创建提醒、
 * 不声明创建工具，也不把历史 messageId 或当前人的输入当成新的执行授权。
 */
export function createNativeSchedule(options: NativeScheduleOptions): NativeSchedule {
  const selected = structuredClone(options);
  if (selected.service.kind !== 'install' && selected.service.kind !== 'provided') throw new Error('REACTIVE_NATIVE_SCHEDULE_SERVICE_SELECTION_INVALID');
  const owner = new NativeCapabilityOwner('reactive-native-schedule', ['schedule'], function* (ctx) {
    if (selected.service.kind === 'install') yield ctx.plugin(ScheduleService, selected.service.config ?? {});
    else if (!ctx.get('schedule')) throw new Error('REACTIVE_NATIVE_SCHEDULE_REQUIRED');
  });
  const sessions = new WeakMap<object, SessionSchedule>();
  return Object.freeze({ install: owner.install, dispose: () => owner.dispose(),
    forSession(session: NativeSessionSource) {
      const access = nativeSessionAccess(session); owner.session(access, 'schedule');
      let current = sessions.get(session);
      if (!current || current.disposed) { current = new SessionSchedule(owner, access); sessions.set(session, current); }
      return current;
    },
  });
}
