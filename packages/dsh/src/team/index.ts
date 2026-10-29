import TeamService, { type Config, type TeamId, type TeamTaskId, type TeamMemberView, type TeamMemberProjection,
  type TeamTaskView, type TeamProjection, type CreateTeamTaskRequest, type UpdateTeamTaskRequest, type TeamWaitResult } from '@deepseek-ai/dsh-experimental-agent-team';
import { SessionLogOffset } from '@deepseek-ai/dsh-session/types';
import type { SessionId } from '@deepseek-ai/dsh-session';
import type { SessionProjectionRegistry } from '@deepseek-ai/dsh-session-projection';
import { foldSubagentDescriptor } from '@deepseek-ai/dsh-subagent';
import { Observable, firstValueFrom, map, merge, switchMap } from 'rxjs';
import { abortable, observeResource, mapResource, ResourceReader, type CancellationOptions } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';
import { NativeCapabilityOwner, nativeResource, nativeObservationScope, ownNativeValue,
  type NativeCapability, type NativeInvocation, type NativeSessionSource } from '../native-capability.ts';
import { nativeSessionAccess, type NativeSessionAccess } from '../conversations/session-access.ts';
import { nativeAgentResource } from '../conversations/agent-resource.ts';

export { TeamId, TeamTaskId } from './client.ts';
export type { TeamMemberView, TeamMemberProjection, TeamTaskView, TeamProjection, TeamTaskStatus } from './client.ts';
export { TeamError } from '@deepseek-ai/dsh-experimental-agent-team';
export type { CreateTeamTaskRequest, UpdateTeamTaskRequest, TeamWaitResult } from '@deepseek-ai/dsh-experimental-agent-team';

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

/** 原 Team 内的固定任务目标；观察和读取来自同一原生投影，缺席读取为 undefined。 */
export interface NativeTeamTask {
  readonly id: TeamTaskId;
  readonly state: ResourceReader<TeamTaskView | undefined>;
  /** 当前任务板中的记录；删除后为 undefined，不把撤下当成一次读取失败。 */
  readonly state$: Observable<TeamTaskView | undefined>;
  read(options?: CancellationOptions): Promise<TeamTaskView | undefined>;
  update(request: NativeTeamTaskUpdate, options?: CancellationOptions): NativeInvocation<TeamTaskView>;
}

export interface NativeSessionTeam {
  readonly state: ResourceReader<TeamProjection>;
  readonly projection$: Observable<TeamProjection>;
  /** 持久成员阶段；当前 turn 状态由 roster 表达。 */
  readonly members$: Observable<readonly TeamMemberProjection[]>;
  /** 正式运行态成员列表；冷会话保持 pending，恢复后沿 exact live 成员重新读取。 */
  readonly roster: ResourceReader<readonly TeamMemberView[]>;
  readonly roster$: Observable<readonly TeamMemberView[]>;
  readonly tasks$: Observable<readonly TeamTaskView[]>;
  /** 冷 Lead 可读原投影；冷 teammate 不凭字符串恢复一个 live 成员身份。 */
  readProjection(options?: CancellationOptions): Promise<TeamProjection>;
  membership(): NativeTeamMembership;
  listMembers(): readonly TeamMemberView[];
  listTasks(): readonly TeamTaskView[];
  getTask(id: TeamTaskId): TeamTaskView;
  /** 同步绑定固定目标，不读取投影；修改仍须由真实 live 成员通过原生 membership 与 CAS 受理。 */
  task(id: TeamTaskId): NativeTeamTask;
  /** 调用即进入原生事务；result 保留实际结算，wait 只结束当前调用方的等待。 */
  createTask(request: CreateTeamTaskRequest, options?: CancellationOptions): NativeInvocation<TeamTaskView>;
  updateTask(request: UpdateTeamTaskRequest, options?: CancellationOptions): NativeInvocation<TeamTaskView>;
  waitForChange(timeoutMs: number, options?: CancellationOptions): Promise<TeamWaitResult>;
  interrupt(name: string, options?: CancellationOptions): { readonly previousStatus: 'running' | 'inactive' };
  /** 停止观察/等待，不中断成员；能力 owner 退出时仍由原生 Team drain。 */
  dispose(): void;
}

class SessionTeam implements NativeSessionTeam {
  readonly state: ResourceReader<TeamProjection>;
  readonly projection$: Observable<TeamProjection>;
  readonly members$: Observable<readonly TeamMemberProjection[]>;
  readonly roster: ResourceReader<readonly TeamMemberView[]>;
  readonly roster$: Observable<readonly TeamMemberView[]>;
  readonly tasks$: Observable<readonly TeamTaskView[]>;
  #scope: Scope;
  #tasks = new Map<TeamTaskId, NativeTeamTask>();
  get disposed(): boolean { return this.#scope.disposed; }
  constructor(private readonly owner: NativeCapabilityOwner, private readonly access: NativeSessionAccess) {
    this.#scope = nativeObservationScope(owner, access);
    const changes$ = access.liveAgent$.pipe(switchMap(agent => new Observable<void>(subscriber => {
      const ctx = this.context();
      const projections: SessionProjectionRegistry | undefined = ctx.get('sessionProjections');
      if (!projections) throw new Error('REACTIVE_NATIVE_TEAM_PROJECTION_REQUIRED');
      const root = agent ? this.service().membership(agent).root.session : undefined;
      return projections.onChanged((session, key) => {
        if (key !== 'agentTeam' || (root ? session !== root : session.id !== access.sessionId)) return;
        subscriber.next();
      });
    })));
    // 先监听同源官方投影再初读；每个 Agent 代际固定原生 root，不复制 Team 事件分类。
    this.state = nativeResource(this.#scope, () => observeResource({ key$: merge(changes$, access.liveAgent$),
      read: (_owner, signal) => this.readProjection({ signal }) }));
    this.projection$ = this.state.values$;
    this.members$ = this.projection$.pipe(map(projection => projection.members));
    this.tasks$ = this.projection$.pipe(map(projection => projection.tasks));
    const roster = nativeAgentResource<readonly TeamMemberView[]>(owner, access, this.#scope, { service: 'agentTeams',
      changes: agent => merge(changes$, new Observable<void>(subscriber => {
        const ctx = this.context();
        const notify = (id: SessionId) => {
          if (subscriber.closed || access.liveAgent() !== agent) return;
          try {
            // disposed 已撤出原生 registry；由当前调用方的正式 roster 判断归属，不能向退出对象补授身份。
            if (this.service().listMembers(agent).some(member => member.id === id)) subscriber.next();
          } catch (error) { subscriber.error(error); }
        };
        const releases = [
          ctx.on('agent/status', ({ agent: changed }) => notify(changed.id)),
          ctx.on('agent/created', ({ agent: changed }) => { notify(changed.id); return undefined; }),
          ctx.on('agent/disposed', ({ agent: changed }) => notify(changed.id)),
        ];
        return () => { for (const release of releases) release(); };
      })),
      read: agent => this.service().listMembers(agent),
    });
    this.roster = new ResourceReader(roster, { signal: this.#scope.signal, onObserverError: error => this.#scope.observerError(error) });
    this.roster$ = this.roster.values$;
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
      if (inspection.meta.id !== this.access.sessionId) throw new Error('REACTIVE_NATIVE_SESSION_MISMATCH');
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
  task(id: TeamTaskId): NativeTeamTask {
    this.context();
    let task = this.#tasks.get(id);
    if (!task) {
      const state = nativeResource(this.#scope, () => this.state.observations$.pipe(mapResource(projection => projection.tasks.find(current => current.id === id))));
      task = Object.freeze({ id,
        state, state$: state.values$,
        read: async (options: CancellationOptions = {}) => (await this.readProjection(options)).tasks.find(current => current.id === id),
        update: (request: NativeTeamTaskUpdate, options: CancellationOptions = {}) => {
          if (Object.hasOwn(request, 'taskId')) throw new Error('REACTIVE_NATIVE_TEAM_TASK_SOURCE_RESERVED');
          return this.updateTask({ ...request, taskId: id }, options);
        },
      });
      this.#tasks.set(id, task);
    }
    return task;
  }
  createTask(request: CreateTeamTaskRequest, options: CancellationOptions = {}): NativeInvocation<TeamTaskView> {
    const signal = this.signal(options);
    const service = this.service(), agent = this.agent(), selected = structuredClone(request);
    // 原生事务受理后由 Team journal 结算；取消观察不能回滚已追加的 task。
    return this.owner.invoke({ sessionId: this.access.sessionId, operation: 'createTask' },
      async () => ownNativeValue(await service.createTask(agent, selected)), signal);
  }
  updateTask(request: UpdateTeamTaskRequest, options: CancellationOptions = {}): NativeInvocation<TeamTaskView> {
    const signal = this.signal(options);
    const service = this.service(), agent = this.agent(), selected = structuredClone(request);
    return this.owner.invoke({ sessionId: this.access.sessionId, operation: 'updateTask' },
      async () => ownNativeValue(await service.updateTask(agent, selected)), signal);
  }
  async waitForChange(timeoutMs: number, options: CancellationOptions = {}): Promise<TeamWaitResult> {
    const signal = this.signal(options);
    const service = this.service(), agent = this.agent();
    const result = await firstValueFrom(abortable(waiting => service.waitForChange(agent, timeoutMs, waiting), signal));
    // 原生通知可以先结算、同轮后续 listener 再退出；交付仍核验这次等待的原身份。
    signal.throwIfAborted(); this.context();
    if (this.agent() !== agent) throw new Error('REACTIVE_NATIVE_TEAM_LIVE_MEMBER_REQUIRED');
    return ownNativeValue(result);
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
  const owner = new NativeCapabilityOwner('reactive-native-team', ['agentTeams'], function* (ctx) {
    if (selected.service.kind === 'install') yield ctx.plugin(TeamService, selected.service.config ?? {});
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
