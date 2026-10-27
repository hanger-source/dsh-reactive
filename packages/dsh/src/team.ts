import TeamService, { type Config, type TeamId, type TeamTaskId, type TeamMemberView, type TeamMemberProjection,
  type TeamTaskView, type TeamProjection, type CreateTeamTaskRequest, type UpdateTeamTaskRequest, type TeamWaitResult } from '@deepseek-ai/dsh-experimental-agent-team';
import { SessionLogOffset } from '@deepseek-ai/dsh-session/types';
import type { SessionId } from '@deepseek-ai/dsh-session';
import type { SessionProjectionRegistry } from '@deepseek-ai/dsh-session-projection';
import { foldSubagentDescriptor } from '@deepseek-ai/dsh-subagent';
import { Observable, map, switchMap } from 'rxjs';
import { abortable, type CancellationOptions } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';
import { requireActivePlugin } from './native-installation.ts';
import { NativeCapabilityOwner, nativeObservation, nativeObservationScope, ownNativeValue,
  type NativeCapability, type NativeSessionHandle } from './native-capability.ts';
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
export interface NativeSessionTeam {
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
  createTask(request: CreateTeamTaskRequest, options?: CancellationOptions): Promise<TeamTaskView>;
  updateTask(request: UpdateTeamTaskRequest, options?: CancellationOptions): Promise<TeamTaskView>;
  waitForChange(timeoutMs: number, options?: CancellationOptions): Promise<TeamWaitResult>;
  interrupt(name: string, options?: CancellationOptions): { readonly previousStatus: 'running' | 'inactive' };
  /** 停止观察/等待，不中断成员；能力 owner 退出时仍由原生 Team drain。 */
  dispose(): void;
}

class SessionTeam implements NativeSessionTeam {
  readonly projection$: Observable<TeamProjection>;
  readonly members$: Observable<readonly TeamMemberProjection[]>;
  readonly tasks$: Observable<readonly TeamTaskView[]>;
  #scope: Scope;
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
      subscriber.next();
      return release;
    });
    this.projection$ = nativeObservation(this.#scope, changes$.pipe(switchMap(() => abortable(signal => this.readProjection({ signal })))));
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
      const inspection = await this.access.inspect(signal);
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
  async createTask(request: CreateTeamTaskRequest, options: CancellationOptions = {}): Promise<TeamTaskView> {
    this.signal(options);
    // 原生事务受理后由 Team journal 结算；取消观察不能回滚已追加的 task。
    return ownNativeValue(await this.service().createTask(this.agent(), structuredClone(request)));
  }
  async updateTask(request: UpdateTeamTaskRequest, options: CancellationOptions = {}): Promise<TeamTaskView> {
    this.signal(options);
    return ownNativeValue(await this.service().updateTask(this.agent(), structuredClone(request)));
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
  forSession(session: NativeSessionHandle): NativeSessionTeam;
}

/** 具名成员和任务板仍归原生 Team；没有可信 Program 创建/输入前门时不包装 spawn/send。 */
export function createNativeTeam(options: NativeTeamOptions): NativeTeam {
  const selected = structuredClone(options);
  if (selected.service.kind !== 'install' && selected.service.kind !== 'provided') throw new Error('REACTIVE_NATIVE_TEAM_SERVICE_SELECTION_INVALID');
  const owner = new NativeCapabilityOwner('reactive-native-team', async ctx => {
    if (selected.service.kind === 'install') await requireActivePlugin(ctx.plugin(TeamService, selected.service.config ?? {}));
    else if (!ctx.get('agentTeams')) throw new Error('REACTIVE_NATIVE_TEAM_REQUIRED');
  });
  const sessions = new WeakMap<object, SessionTeam>();
  return Object.freeze({ install: owner.install, dispose: () => owner.dispose(),
    forSession(session: NativeSessionHandle) {
      const access = nativeSessionAccess(session); owner.session(access, 'agentTeams');
      let current = sessions.get(session);
      if (!current || current.disposed) { current = new SessionTeam(owner, access); sessions.set(session, current); }
      return current;
    },
  });
}
