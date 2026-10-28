import TeamService, { TeamError, type Config, type TeamId, type TeamTaskId, type TeamMemberView, type TeamMemberProjection,
  type TeamTaskView, type TeamProjection, type CreateTeamTaskRequest, type UpdateTeamTaskRequest, type TeamWaitResult } from '@deepseek-ai/dsh-experimental-agent-team';
import { SessionLogOffset } from '@deepseek-ai/dsh-session/types';
import type { SessionId } from '@deepseek-ai/dsh-session';
import type { SessionProjectionRegistry } from '@deepseek-ai/dsh-session-projection';
import { foldSubagentDescriptor } from '@deepseek-ai/dsh-subagent';
import { Observable, firstValueFrom, map, merge } from 'rxjs';
import { abortable, observeResource, mapResource, type ResourceReader, type CancellationOptions } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';
import { requireActivePlugin } from './native-installation.ts';
import { NativeCapabilityOwner, nativeResource, nativeObservationScope, ownNativeValue,
  type NativeCapability, type NativeSessionSource } from './native-capability.ts';
import { nativeSessionAccess, type NativeSessionAccess } from './conversations/session-access.ts';

export { TeamId, TeamTaskId, TeamError } from '@deepseek-ai/dsh-experimental-agent-team';
export type { TeamMemberView, TeamMemberProjection, TeamTaskView, TeamProjection, CreateTeamTaskRequest,
  UpdateTeamTaskRequest, TeamTaskStatus, TeamWaitResult } from '@deepseek-ai/dsh-experimental-agent-team';

/** 剔除原生 Agent 凭据的角色快照，不授予其他会话的控制权。 */
export interface NativeTeamMembership {
  readonly teamId: TeamId;
  readonly rootSessionId: SessionId;
  readonly role: 'lead' | 'teammate';
  readonly name: string;
}
export interface NativeTeamOptions {
  readonly service: { readonly kind: 'install'; readonly config?: Config } | { readonly kind: 'provided' };
}

export type NativeTeamTaskUpdate = Omit<UpdateTeamTaskRequest, 'taskId'>;

/** 原 Team 内固定的一项任务；观察和读取来自同一原生投影，修改仍由原生 CAS 受理。 */
export interface NativeTeamTask {
  readonly id: TeamTaskId;
  readonly state: ResourceReader<TeamTaskView | undefined>;
  /** 当前任务板中的记录；删除后为 undefined，不把撤下当成一次读取失败。 */
  readonly state$: Observable<TeamTaskView | undefined>;
  read(options?: CancellationOptions): Promise<TeamTaskView | undefined>;
  update(request: NativeTeamTaskUpdate, options?: CancellationOptions): Promise<TeamTaskView>;
}

export interface NativeSessionTeam {
  readonly state: ResourceReader<TeamProjection>;
  readonly projection$: Observable<TeamProjection>;
  /** 持久成员阶段；运行中的 turn 状态另用 listMembers 读取。 */
  readonly members$: Observable<readonly TeamMemberProjection[]>;
  readonly tasks$: Observable<readonly TeamTaskView[]>;
  /** 冷 Lead 可读原投影；冷 teammate 不凭字符串恢复一个 live 成员身份。 */
  readProjection(options?: CancellationOptions): Promise<TeamProjection>;
  membership(): NativeTeamMembership;
  listMembers(): readonly TeamMemberView[];
  listTasks(): readonly TeamTaskView[];
  getTask(id: TeamTaskId): TeamTaskView;
  /** 先核现有任务的归属；冷 Lead 可绑定，读取不会激活 Agent。 */
  task(id: TeamTaskId, options?: CancellationOptions): Promise<NativeTeamTask>;
  createTask(request: CreateTeamTaskRequest, options?: CancellationOptions): Promise<TeamTaskView>;
  updateTask(request: UpdateTeamTaskRequest, options?: CancellationOptions): Promise<TeamTaskView>;
  waitForChange(timeoutMs: number, options?: CancellationOptions): Promise<TeamWaitResult>;
  interrupt(name: string, options?: CancellationOptions): { readonly previousStatus: 'running' | 'inactive' };
  /** 停止观察/等待，不中断成员；能力 owner 退出时仍由原生 Team drain。 */
  dispose(): void;
}

class SessionTeam implements NativeSessionTeam {
  readonly state: ResourceReader<TeamProjection>;
  readonly projection$: Observable<TeamProjection>;
  readonly members$: Observable<readonly TeamMemberProjection[]>;
  readonly tasks$: Observable<readonly TeamTaskView[]>;
  #scope: Scope;
  #tasks = new Map<TeamTaskId, NativeTeamTask>();
  get disposed(): boolean { return this.#scope.disposed; }
  constructor(private readonly owner: NativeCapabilityOwner, private readonly access: NativeSessionAccess) {
    this.#scope = nativeObservationScope(owner, access);
    const changes$ = new Observable<void>(subscriber => {
      const ctx = this.context();
      const release = ctx.on('session/event', (session, event) => {
        if (event.type !== 'team/member' && event.type !== 'team/task') return;
        try {
          const agent = this.access.liveAgent();
          const rootId = agent ? this.service().membership(agent).root.id : this.access.sessionId;
          if (session.id === rootId) subscriber.next();
        } catch (error) { subscriber.error(error); }
      });
      return release;
    });
    // 先监听 Team 日志，再由实际 owner 触发初读；关闭/恢复取消旧切面，不另存 Agent 状态。
    this.state = nativeResource(this.#scope, () => observeResource({ key$: merge(changes$, access.liveAgent$),
      read: (_owner, signal) => this.readProjection({ signal }) }));
    this.projection$ = this.state.values$;
    this.members$ = this.projection$.pipe(map(projection => projection.members));
    this.tasks$ = this.projection$.pipe(map(projection => projection.tasks));
  }
  private context() {
    this.#scope.assertActive();
    return this.owner.session(this.access, 'agentTeams');
  }
  private service(): TeamService {
    const service: TeamService | undefined = this.context().get('agentTeams');
    if (!service) throw new Error('REACTIVE_NATIVE_TEAM_REQUIRED');
    return service;
  }
  private agent() {
    this.context();
    const agent = this.access.liveAgent();
    if (!agent || agent.id !== this.access.sessionId) throw new Error('REACTIVE_NATIVE_TEAM_LIVE_MEMBER_REQUIRED');
    return agent;
  }
  private signal(options: CancellationOptions): AbortSignal {
    this.context();
    const signal = options.signal ? AbortSignal.any([this.#scope.signal, options.signal]) : this.#scope.signal;
    signal.throwIfAborted();
    return signal;
  }
  async readProjection(options: CancellationOptions = {}): Promise<TeamProjection> {
    const signal = this.signal(options);
    const ctx = this.context();
    const projections: SessionProjectionRegistry | undefined = ctx.get('sessionProjections');
    if (!projections) throw new Error('REACTIVE_NATIVE_TEAM_PROJECTION_REQUIRED');
    const agent = this.access.liveAgent();
    let projection: TeamProjection | undefined;
    if (agent) {
      const member = this.service().membership(agent);
      projection = projections.snapshot(member.root.session, ['agentTeam']).values.agentTeam;
    } else {
      const inspection = await firstValueFrom(abortable(signal => this.access.inspect(signal), signal));
      signal.throwIfAborted(); this.context();
      if (foldSubagentDescriptor(inspection.events.slice(inspection.inheritedEventCount)) !== undefined) {
        throw new Error('REACTIVE_NATIVE_TEAM_LIVE_MEMBER_REQUIRED');
      }
      // 官方 restore 负责 Team 事件校验、fork cut 和版本语义，不在 SDK 复制投影。
      projection = projections.restore({}, inspection.events, SessionLogOffset(0), inspection.meta,
        inspection.inheritedEventCount).snapshot.values.agentTeam;
    }
    signal.throwIfAborted(); this.context();
    if (!projection) throw new Error('REACTIVE_NATIVE_TEAM_PROJECTION_REQUIRED');
    if (projection.failure !== undefined) throw new Error('REACTIVE_NATIVE_TEAM_PROJECTION_FAILED: ' + projection.failure);
    return ownNativeValue(projection);
  }
  membership(): NativeTeamMembership {
    const member = this.service().membership(this.agent());
    return Object.freeze({ teamId: member.id, rootSessionId: member.root.id, role: member.role, name: member.name });
  }
  listMembers(): readonly TeamMemberView[] { return ownNativeValue(this.service().listMembers(this.agent())); }
  listTasks(): readonly TeamTaskView[] { return ownNativeValue(this.service().listTasks(this.agent())); }
  getTask(id: TeamTaskId): TeamTaskView { return ownNativeValue(this.service().getTask(this.agent(), id)); }
  async task(id: TeamTaskId, options: CancellationOptions = {}): Promise<NativeTeamTask> {
    const projection = await this.readProjection(options);
    if (!projection.tasks.some(task => task.id === id)) throw new TeamError(`team task "${id}" not found`, 'TEAM_TASK_NOT_FOUND');
    let task = this.#tasks.get(id);
    if (!task) {
      const state = nativeResource(this.#scope, () => this.state.observations$.pipe(mapResource(projection => projection.tasks.find(current => current.id === id))));
      task = Object.freeze({ id,
        state, state$: state.values$,
        read: async (options: CancellationOptions = {}) => (await this.readProjection(options)).tasks.find(current => current.id === id),
        update: async (request: NativeTeamTaskUpdate, options: CancellationOptions = {}) => {
          if (Object.hasOwn(request, 'taskId')) throw new Error('REACTIVE_NATIVE_TEAM_TASK_SOURCE_RESERVED');
          return this.updateTask({ ...request, taskId: id }, options);
        },
      });
      this.#tasks.set(id, task);
    }
    return task;
  }
  async createTask(request: CreateTeamTaskRequest, options: CancellationOptions = {}): Promise<TeamTaskView> {
    const signal = this.signal(options);
    // 原生事务受理后由 Team journal 结算；取消观察不能回滚已追加的 task。
    return firstValueFrom(abortable(async () => ownNativeValue(await this.service().createTask(this.agent(), structuredClone(request))), signal));
  }
  async updateTask(request: UpdateTeamTaskRequest, options: CancellationOptions = {}): Promise<TeamTaskView> {
    const signal = this.signal(options);
    return firstValueFrom(abortable(async () => ownNativeValue(await this.service().updateTask(this.agent(), structuredClone(request))), signal));
  }
  async waitForChange(timeoutMs: number, options: CancellationOptions = {}): Promise<TeamWaitResult> {
    const signal = this.signal(options);
    return ownNativeValue(await this.service().waitForChange(this.agent(), timeoutMs, signal));
  }
  interrupt(name: string, options: CancellationOptions = {}): { readonly previousStatus: 'running' | 'inactive' } {
    this.signal(options);
    return ownNativeValue(this.service().interrupt(this.agent(), name));
  }
  dispose(): void { this.#scope.dispose(); }
}

export interface NativeTeam extends NativeCapability {
  forSession(session: NativeSessionSource): NativeSessionTeam;
}

/** 具名成员和任务板仍归原生 Team；没有可信 Program 创建/输入前门时不包装 spawn/send。 */
export function createNativeTeam(options: NativeTeamOptions): NativeTeam {
  const selected = structuredClone(options);
  if (selected.service.kind !== 'install' && selected.service.kind !== 'provided') throw new Error('REACTIVE_NATIVE_TEAM_SERVICE_SELECTION_INVALID');
  const owner = new NativeCapabilityOwner('reactive-native-team', ['agentTeams'], async ctx => {
    if (selected.service.kind === 'install') await requireActivePlugin(ctx.plugin(TeamService, selected.service.config ?? {}));
    else if (!ctx.get('agentTeams')) throw new Error('REACTIVE_NATIVE_TEAM_REQUIRED');
  });
  const sessions = new WeakMap<object, SessionTeam>();
  return Object.freeze({ install: owner.install, dispose: () => owner.dispose(),
    forSession(session: NativeSessionSource) {
      const access = nativeSessionAccess(session); owner.session(access, 'agentTeams');
      let current = sessions.get(session);
      if (!current || current.disposed) { current = new SessionTeam(owner, access); sessions.set(session, current); }
      return current;
    },
  });
}
