import ScheduleService, { type Config, type ScheduleCatalogEntry, type ScheduleRecord,
  type ScheduleUpdateRequest, type ScheduleUpdateResult, type ScheduleDeleteResult,
  type ScheduleDeliveryHistoryRequest, type ScheduleDeliveryHistoryResult } from '@deepseek-ai/dsh-schedule';
import type { ScheduleId } from '@deepseek-ai/dsh-schedule/client';
import { Observable, map, switchMap } from 'rxjs';
import { abortable, type CancellationOptions } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';
import { requireActivePlugin } from './native-installation.ts';
import { NativeCapabilityOwner, nativeObservation, nativeObservationScope, ownNativeValue,
  type NativeCapability, type NativeSessionSource } from './native-capability.ts';
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

/** 已核验原会话归属的现有提醒；投递 ACK 与模型/业务完成分别表达。 */
export interface NativeScheduleTask {
  readonly id: ScheduleId;
  readonly state$: Observable<ScheduleCatalogEntry | undefined>;
  /** 完整 ScheduleRecord，可原样用于 expected；不混入 catalog 的额外字段。 */
  read(options?: CancellationOptions): Promise<ScheduleRecord | undefined>;
  history(page: NativeScheduleHistory, options?: CancellationOptions): Promise<ScheduleDeliveryHistoryResult>;
  update(request: NativeScheduleUpdate, options?: CancellationOptions): Promise<ScheduleUpdateResult>;
  delete(options?: CancellationOptions): Promise<ScheduleDeleteResult>;
}

export interface NativeSessionSchedule {
  readonly active$: Observable<readonly ScheduleRecord[]>;
  readonly catalog$: Observable<readonly ScheduleCatalogEntry[]>;
  active(options?: CancellationOptions): Promise<readonly ScheduleRecord[]>;
  catalog(options?: CancellationOptions): Promise<readonly ScheduleCatalogEntry[]>;
  task(id: ScheduleId, options?: CancellationOptions): Promise<NativeScheduleTask>;
  /** 释放管理观察，不删除提醒或撤回已经投递的输入。 */
  dispose(): void;
}

class SessionSchedule implements NativeSessionSchedule {
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
    this.catalog$ = nativeObservation(this.scope, changes$.pipe(switchMap(() => abortable(signal => this.catalog({ signal })))));
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
    const records = await this.service().list({ sessionId: this.access.sessionId });
    signal.throwIfAborted(); this.context();
    return ownNativeValue(records);
  }
  async catalog(options: CancellationOptions = {}): Promise<readonly ScheduleCatalogEntry[]> {
    const signal = this.signal(options);
    const records = await this.service().catalog();
    signal.throwIfAborted(); this.context();
    return ownNativeValue(records.filter(record => record.sessionId === this.access.sessionId));
  }
  async task(id: ScheduleId, options: CancellationOptions = {}): Promise<NativeScheduleTask> {
    const records = await this.catalog(options);
    if (!records.some(record => record.id === id)) throw new Error('REACTIVE_NATIVE_SCHEDULE_NOT_FOUND');
    let task = this.#tasks.get(id);
    if (!task) {
      task = Object.freeze({ id,
        state$: this.catalog$.pipe(map(current => current.find(record => record.id === id))),
        read: async (opts: CancellationOptions = {}) => {
          const current = (await this.catalog(opts)).find(record => record.id === id);
          return current === undefined ? undefined : scheduleRecord(current);
        },
        history: async (page: NativeScheduleHistory, opts: CancellationOptions = {}) => {
          const signal = this.signal(opts);
          const result = await this.service().history({ ...structuredClone(page), id, sessionId: this.access.sessionId });
          signal.throwIfAborted(); this.context();
          return ownNativeValue(result);
        },
        update: async (request: NativeScheduleUpdate, opts: CancellationOptions = {}) => {
          const signal = this.signal(opts);
          const selected = structuredClone(request);
          if (selected.expected.id !== id) throw new Error('REACTIVE_NATIVE_SCHEDULE_EXPECTED_MISMATCH');
          return ownNativeValue(await this.service().update({ ...selected, id, sessionId: this.access.sessionId }, signal));
        },
        delete: async (opts: CancellationOptions = {}) => ownNativeValue(await this.service().delete({ id, sessionId: this.access.sessionId }, this.signal(opts))),
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
  const owner = new NativeCapabilityOwner('reactive-native-schedule', async ctx => {
    if (selected.service.kind === 'install') await requireActivePlugin(ctx.plugin(ScheduleService, selected.service.config ?? {}));
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
