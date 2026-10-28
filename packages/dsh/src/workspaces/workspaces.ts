import type { Context } from '@deepseek-ai/cordis';
import type { Session, SessionHeader } from '@deepseek-ai/dsh-session';
import type { SessionInspection } from '@deepseek-ai/dsh-session-persistence';
import { appendPluginRecord, pluginRecordOf } from '@deepseek-ai/dsh-session';
import { WorkspaceId, workspaceDomainSpec, type Workspace as NativeWorkspace } from '@deepseek-ai/dsh-workspace';
import type {} from '@deepseek-ai/dsh-storage-domain';
import { z } from 'zod';
import { Scope, createWorkspace } from '@hanger-source/dsh-reactive/kernel';
import type { AnyEnvironment } from '@hanger-source/dsh-reactive/kernel';
import type { Workspace, WorkspaceInfo, WorkspaceRegistration } from '@hanger-source/dsh-reactive/kernel';
import { environmentPresetId } from '../programs/presets.ts';

const bindingSchema = z.object({ application: z.string(), environmentId: z.string(), workspaceId: z.string() }).strict();
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
  #environments: ReadonlyMap<string, AnyEnvironment>;
  constructor(private readonly ctx: Context, private readonly application: string, environments: readonly AnyEnvironment[]) {
    this.#environments = new Map(environments.map(environment => [environment.id, environment]));
    this.#scope.own(ctx.on('domain/changed', change => {
      if (change.domain !== workspaceDomainSpec.name || change.table !== 'workspaces' || change.operation !== 'deleted') return;
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
  bind(session: Session, workspace: ManagedWorkspace, environment: AnyEnvironment) {
    this.assertPresent(workspace);
    appendPluginRecord(session, 'plugin:reactive/workspace', { application: this.application,
      environmentId: environment.id, workspaceId: workspace.native.id });
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
    if (this.#failures.size) throw new AggregateError([...this.#failures], 'REACTIVE_WORKSPACES_DISPOSAL_FAILED');
  }
}
