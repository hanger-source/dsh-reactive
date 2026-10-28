import type { Context } from '@deepseek-ai/cordis';
import { randomUUID } from 'node:crypto';
import type { Session, SessionHeader, SessionId } from '@deepseek-ai/dsh-session';
import type { SessionInspection } from '@deepseek-ai/dsh-session-persistence';
import { appendPluginRecord, pluginRecordOf } from '@deepseek-ai/dsh-session';
import { WorkspaceId, workspaceDomainSpec, workspaceRecord, type Workspace as NativeWorkspace } from '@deepseek-ai/dsh-workspace';
import type {} from '@deepseek-ai/dsh-storage-domain';
import { z } from 'zod';
import { Scope, ConversationAccessRevokedError, createWorkspace } from '@hanger-source/dsh-reactive/kernel';
import type { AnyEnvironment, ProgramContract } from '@hanger-source/dsh-reactive/kernel';
import type { Workspace, WorkspaceInfo, WorkspaceRegistration } from '@hanger-source/dsh-reactive/kernel';
import { environmentPresetId } from '../programs/presets.ts';
import { sessionProgramBindingSchema, captureProgramBinding, assertSessionProgram } from '../conversations/program-binding.ts';

const bindingSchema = z.object({ application: z.string(), environmentId: z.string(), workspaceId: z.string(),
  program: sessionProgramBindingSchema }).strict();
const creationSchema = z.object({ workspaceId: z.string() }).strict();
declare module '@deepseek-ai/dsh-session/types' {
  interface PluginRecordMap {
    'plugin:reactive/workspace': z.infer<typeof bindingSchema>;
    'plugin:reactive/created': z.infer<typeof creationSchema>;
  }
}
export interface ManagedWorkspace { native: NativeWorkspace; readonly scope: Scope; context: Workspace; }

/** 原生 registry 拥有身份和会话成员；框架只拥有工作环境关联与共享资源作用域。 */
export class Workspaces {
  #scope = new Scope();
  #failures = new Set<unknown>();
  #entries = new Map<string, ManagedWorkspace>();
  #membershipObservers = new Set<(workspaceId: string, members: readonly SessionId[] | undefined) => void>();
  // 仅登记本应用实际签过的资格；Session图收集不结束仍存在的持久成员关系。
  #accesses = new Map<string, Map<SessionId, string>>();
  #environments: ReadonlyMap<string, AnyEnvironment>;
  constructor(private readonly ctx: Context, private readonly application: string, environments: readonly AnyEnvironment[]) {
    this.#environments = new Map(environments.map(environment => [environment.id, environment]));
    this.#scope.own(ctx.on('domain/changed', change => {
      if (change.domain !== workspaceDomainSpec.name || change.table !== 'workspaces') return;
      // Entity在await table.update之后才交换record；这里用正式提交切面撤销资格，不重读旧实体缓存。
      const members = change.operation === 'put' ? workspaceRecord.parse(change.value).sessionIds : undefined;
      const issued = this.#accesses.get(change.key);
      if (issued) {
        for (const sessionId of issued.keys()) if (!members?.includes(sessionId)) issued.delete(sessionId);
        if (!issued.size) this.#accesses.delete(change.key);
      }
      for (const observer of this.#membershipObservers) {
        try { observer(change.key, members); }
        catch (error) {
          this.#failures.add(error);
          console.error('reactive.workspace.membership.observer.failed', { workspaceId: change.key, error });
        }
      }
      if (change.operation !== 'deleted') return;
      const workspace = this.#entries.get(change.key);
      if (!workspace) return;
      this.#entries.delete(change.key);
      try { workspace.scope.dispose(); }
      catch (error) {
        this.#failures.add(error);
        console.error('reactive.workspace.disposal.failed', { workspaceId: change.key, error });
      }
    }));
  }
  /** 正式成员提交先撤下相关Session图，再退出实体父Scope；不保留第二份成员状态。 */
  onMembershipChange(observer: (workspaceId: string, members: readonly SessionId[] | undefined) => void): () => void {
    this.#scope.assertActive();
    this.#membershipObservers.add(observer);
    return this.#scope.own(() => { this.#membershipObservers.delete(observer); });
  }
  /** 核验原资格必须早于Program装配；省略原id是显式新取得，而非复活旧token。 */
  assertAccess(header: SessionHeader, workspace: ManagedWorkspace, environment: AnyEnvironment, accessId?: string): void {
    if (accessId !== undefined && this.#accesses.get(workspace.native.id)?.get(header.id) !== accessId) {
      throw new ConversationAccessRevokedError(header.id, workspace.native.id, 'access-expired');
    }
    this.assertMemberHeader(header, workspace, environment);
  }
  issueAccess(header: SessionHeader, workspace: ManagedWorkspace, environment: AnyEnvironment): string {
    this.assertMemberHeader(header, workspace, environment);
    let issued = this.#accesses.get(workspace.native.id);
    if (!issued) { issued = new Map(); this.#accesses.set(workspace.native.id, issued); }
    const previous = issued.get(header.id);
    if (previous !== undefined) return previous;
    const id = randomUUID(); issued.set(header.id, id); return id;
  }
  environments() { return [...this.#environments.values()]; }
  environment(environmentId: string) {
    const environment = this.#environments.get(environmentId);
    if (!environment) throw new Error('REACTIVE_ENVIRONMENT_NOT_FOUND');
    return environment;
  }
  presetId(environment: AnyEnvironment) { return environmentPresetId(this.application, environment.id); }
  get(workspaceId: string): ManagedWorkspace {
    this.#scope.assertActive();
    const native = this.ctx.workspaceRegistry.get(WorkspaceId(workspaceId));
    // 原生delete先提交顺序，再删除表项；回调中不能从尚未撤下的实体缓存重建owner。
    if (!native || !this.ctx.workspaceRegistry.list().includes(native)) throw new Error('REACTIVE_WORKSPACE_NOT_FOUND');
    const existing = this.#entries.get(workspaceId);
    if (existing) {
      if (existing.native !== native) throw new Error('REACTIVE_WORKSPACE_IDENTITY_CHANGED');
      existing.scope.assertActive();
      return existing;
    }
    const scope = this.#scope.child();
    const workspace = { native, scope, context: createWorkspace(scope, { id: native.id, directory: native.path }) };
    this.#entries.set(workspaceId, workspace);
    return workspace;
  }
  private bindings(session: SessionInspection) {
    return session.events.slice(session.inheritedEventCount).flatMap(event => {
      const record = pluginRecordOf(event);
      return record?.type === 'plugin:reactive/workspace' ? [record.data] : [];
    });
  }
  resolve(session: SessionInspection) {
    const records = this.bindings(session);
    if (records.length !== 1) throw new Error('REACTIVE_WORKSPACE_BINDING_MISMATCH');
    const binding = bindingSchema.parse(records[0]);
    if (binding.application !== this.application) throw new Error('REACTIVE_WORKSPACE_BINDING_MISMATCH');
    const environment = this.environment(binding.environmentId), workspace = this.get(binding.workspaceId);
    this.verifyInspection(session, workspace, environment);
    return { environment, workspace };
  }
  async register(registration: WorkspaceRegistration, inspect: (id: string) => Promise<SessionInspection>): Promise<WorkspaceInfo> {
    const native = await this.ctx.workspaceRegistry.create(registration.directory, registration.title);
    return this.describeWorkspace(native, inspect);
  }
  async describe(inspect: (id: string) => Promise<SessionInspection>): Promise<readonly WorkspaceInfo[]> {
    return Promise.all(this.ctx.workspaceRegistry.list().map(workspace => this.describeWorkspace(workspace, inspect)));
  }
  private async describeWorkspace(native: NativeWorkspace, inspect: (id: string) => Promise<SessionInspection>): Promise<WorkspaceInfo> {
    const sessionIds: string[] = [];
    for (const id of native.sessionIds) {
      const session = await inspect(id), bindings = this.bindings(session);
      const own = bindings.some(value => z.object({ application: z.literal(this.application) }).safeParse(value).success);
      const claimsOurPreset = session.meta.agentPreset?.startsWith(environmentPresetId(this.application, ''));
      if (!own) {
        if (claimsOurPreset) throw new Error('REACTIVE_WORKSPACE_BINDING_MISMATCH');
        continue;
      }
      const resolved = this.resolve(session);
      if (resolved.workspace.native !== native) throw new Error('REACTIVE_WORKSPACE_BINDING_MISMATCH');
      if (!native.sessionIds.includes(id)) throw new Error('REACTIVE_WORKSPACE_MEMBERSHIP_REQUIRED');
      sessionIds.push(id);
    }
    return { workspaceId: native.id, directory: native.path, title: native.title, sessionIds };
  }
  bind(session: Session, workspace: ManagedWorkspace, environment: AnyEnvironment, program: ProgramContract) {
    this.assertPresent(workspace);
    appendPluginRecord(session, 'plugin:reactive/workspace', { application: this.application,
      environmentId: environment.id, workspaceId: workspace.native.id, program: captureProgramBinding(program) });
  }
  /** 实际Program由Session装配选择；child不能被父Environment的根Program替代。 */
  verifyProgram(session: Session | SessionInspection, definition: ProgramContract): void {
    const inspection = 'snapshotEvents' in session ? { meta: session.header, events: session.snapshotEvents(),
      inheritedEventCount: session.inheritedEventCount } : session;
    const records = this.bindings(inspection);
    if (records.length !== 1) throw new Error('REACTIVE_WORKSPACE_BINDING_MISMATCH');
    const binding = bindingSchema.parse(records[0]);
    if (binding.application !== this.application) throw new Error('REACTIVE_WORKSPACE_BINDING_MISMATCH');
    assertSessionProgram(inspection.meta.id, binding.program, definition);
  }
  complete(session: Session, workspace: ManagedWorkspace, environment: AnyEnvironment) {
    this.verify(session, workspace, environment);
    appendPluginRecord(session, 'plugin:reactive/created', { workspaceId: workspace.native.id });
  }
  completed(session: SessionInspection, workspace: ManagedWorkspace, environment: AnyEnvironment) {
    this.verifyInspection(session, workspace, environment);
    const receipts = session.events.slice(session.inheritedEventCount).flatMap(event => {
      const record = pluginRecordOf(event);
      return record?.type === 'plugin:reactive/created' ? [creationSchema.parse(record.data)] : [];
    });
    if (receipts.length > 1 || receipts.some(receipt => receipt.workspaceId !== workspace.native.id)) throw new Error('REACTIVE_CREATION_RECEIPT_INVALID');
    return receipts.length === 1;
  }
  verify(session: Session, workspace: ManagedWorkspace, environment: AnyEnvironment) {
    this.verifyInspection({ meta: session.header, events: session.snapshotEvents(), inheritedEventCount: session.inheritedEventCount }, workspace, environment);
  }
  private assertPresent(workspace: ManagedWorkspace) {
    if (this.ctx.workspaceRegistry.get(workspace.native.id) !== workspace.native) throw new Error('REACTIVE_WORKSPACE_NOT_FOUND');
  }
  verifyInspection(session: SessionInspection, workspace: ManagedWorkspace, environment: AnyEnvironment) {
    this.verifyHeader(session.meta, workspace, environment);
    const records = this.bindings(session).map(value => bindingSchema.parse(value));
    if (records.length !== 1 || records.some(record => record.application !== this.application || record.workspaceId !== workspace.native.id
      || record.environmentId !== environment.id)) throw new Error('REACTIVE_WORKSPACE_BINDING_MISMATCH');
  }
  assertCreation(session: SessionInspection, workspace: ManagedWorkspace, environment: AnyEnvironment) {
    this.verifyInspection(session, workspace, environment);
    if (session.meta.origin || session.meta.parentSession || session.meta.isSeeded || session.inheritedEventCount !== 0
      || session.events.length === 0 || pluginRecordOf(session.events[0]!)?.type !== 'plugin:reactive/workspace'
      // 原生 resume 在公布新 Agent 前追加恢复边界；它不是已开展的业务工作。
      || session.events.slice(1).some(event => event.type !== 'session/end-seed')) {
      throw new Error('REACTIVE_CREATION_RECOVERY_NOT_ADMITTED');
    }
  }
  assertMember(session: Session, workspace: ManagedWorkspace, environment: AnyEnvironment) {
    this.verify(session, workspace, environment);
    this.assertMemberHeader(session.header, workspace, environment);
  }
  /** 已核验 own binding 的冷会话也能同步复核当前原生成员，不加载模型驱动。 */
  assertMemberHeader(header: SessionHeader, workspace: ManagedWorkspace, environment: AnyEnvironment) {
    this.verifyHeader(header, workspace, environment);
    // 事件已发布而Entity尚未交换record时，资格仍须服从同一正式domain表的已提交切面。
    // 这里只借用registry已打开的domain，不重新open、关闭或保存成员镜像。
    const domain = this.ctx.storageDomain.get(workspaceDomainSpec.name);
    if (!domain) throw new Error('REACTIVE_WORKSPACE_DOMAIN_REQUIRED');
    const committed = domain.table('workspaces').get(workspace.native.id);
    if (committed === undefined) throw new Error('REACTIVE_WORKSPACE_NOT_FOUND');
    const record = workspaceRecord.parse(committed);
    if (record.path !== workspace.native.path) throw new Error('REACTIVE_WORKSPACE_MISMATCH');
    if (!record.sessionIds.includes(header.id)) throw new Error('REACTIVE_WORKSPACE_MEMBERSHIP_REQUIRED');
    if (!workspace.native.sessionIds.includes(header.id)) throw new Error('REACTIVE_WORKSPACE_MEMBERSHIP_REQUIRED');
  }
  private verifyHeader(header: SessionHeader, workspace: ManagedWorkspace, environment: AnyEnvironment) {
    this.assertPresent(workspace);
    if (this.environment(environment.id) !== environment) throw new Error('REACTIVE_ENVIRONMENT_MISMATCH');
    if (header.cwd !== workspace.native.path) throw new Error('REACTIVE_WORKSPACE_MISMATCH');
    if (header.agentPreset !== this.presetId(environment)) throw new Error('REACTIVE_PRESET_MISMATCH');
  }
  async attach(session: Session, workspace: ManagedWorkspace, environment: AnyEnvironment) {
    this.verify(session, workspace, environment);
    await workspace.native.attachSession(session.id);
  }
  dispose() {
    try { this.#scope.dispose(); } catch (error) { this.#failures.add(error); }
    this.#entries.clear();
    this.#accesses.clear();
    if (this.#failures.size) throw new AggregateError([...this.#failures], 'REACTIVE_WORKSPACES_DISPOSAL_FAILED');
  }
}
