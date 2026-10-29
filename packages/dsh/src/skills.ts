import type { Agent } from '@deepseek-ai/dsh-agent';
import type {} from '@deepseek-ai/dsh-working-directory';
import { isSkillName, type SkillRegistry, type SkillCatalogSnapshot, type SkillDefinition } from '@deepseek-ai/dsh-skill';
import { Observable } from 'rxjs';
import type { ResourceReadable } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';
import { NativeCapabilityOwner, nativeObservationScope,
  type NativeCapability, type NativeSessionSource } from './native-capability.ts';
import { nativeSessionAccess, type NativeSessionAccess } from './conversations/session-access.ts';
import { nativeAgentResource } from './conversations/agent-resource.ts';

export type { SkillCatalogSnapshot, SkillDefinition, SkillSummary, SkillInvocationPolicy,
  SkillSource, SkillResourceBase } from '@deepseek-ai/dsh-skill';

export interface NativeSkillsOptions {
  /** 复用宿主正式 Registry；provider 与注册层级继续由原生 owner 管理。 */
  readonly service: 'provided';
}

/** 同一个正式资源可用于函数、Promise、Observable 和 hooks；invalidate 显式重取原生事实。 */
export interface NativeSkillResource<T> extends ResourceReadable<T> {
  readonly values$: Observable<T>;
  invalidate(): void;
}

export interface NativeSessionSkills {
  /** 原生完整性标记与 invocation policy 原样保留，不把发现结果当成执行授权。 */
  readonly catalog: NativeSkillResource<SkillCatalogSnapshot>;
  /** 固定名称的正文资源；不存在为 ready(undefined)，冷会话保持 pending。 */
  definition(name: string): NativeSkillResource<SkillDefinition | undefined>;
}

class SessionSkills implements NativeSessionSkills {
  readonly catalog: NativeSkillResource<SkillCatalogSnapshot>;
  readonly scope: Scope;
  #definitions = new Map<string, NativeSkillResource<SkillDefinition | undefined>>();

  constructor(private readonly owner: NativeCapabilityOwner, private readonly access: NativeSessionAccess) {
    this.scope = nativeObservationScope(owner, access);
    this.catalog = this.resource((registry, agent, cwd, signal) => registry.snapshot({ scope: agent, cwd, signal }));
    this.scope.own(() => this.#definitions.clear());
  }

  private resource<T>(read: (registry: SkillRegistry, agent: Agent, cwd: string, signal: AbortSignal) => Promise<T>): NativeSkillResource<T> {
    return nativeAgentResource(this.owner, this.access, this.scope, { service: 'skills',
      changes: agent => new Observable<void>(subscriber => {
        const releases = [
          agent.ctx.on('skills/change', () => subscriber.next()),
          agent.ctx.on('session/event', (session, event) => {
            if (session === agent.session && event.type === 'working-directory/change') subscriber.next();
          }),
        ];
        return () => { for (const release of releases) release(); };
      }),
      read: (agent, signal) => {
        const registry: SkillRegistry | undefined = agent.ctx.get('skills');
        if (!registry) throw new Error('REACTIVE_NATIVE_SKILLS_REQUIRED');
        const directory = this.owner.session(this.access, 'workingDirectory').get('workingDirectory');
        if (!directory) throw new Error('REACTIVE_NATIVE_WORKING_DIRECTORY_REQUIRED');
        // 观察只读取正式已提交的目录，不ensure、恢复目录或改变文件权限root。
        return read(registry, agent, directory.get(agent.session), signal);
      },
    });
  }

  definition(name: string): NativeSkillResource<SkillDefinition | undefined> {
    this.scope.assertActive();
    this.owner.session(this.access, 'skills');
    if (!isSkillName(name)) throw new Error('REACTIVE_SKILL_NAME_INVALID: ' + name);
    let resource = this.#definitions.get(name);
    if (!resource) {
      resource = this.resource((registry, agent, cwd, signal) => registry.get(name, { scope: agent, cwd, signal }));
      this.#definitions.set(name, resource);
    }
    return resource;
  }
}

export interface NativeSkills extends NativeCapability {
  forSession(session: NativeSessionSource): NativeSessionSkills;
}

/** 只读业务 Skill 接点，与独立发行的开发指导 Skill 无关，不安装另一套 Registry。 */
export function createNativeSkills(options: NativeSkillsOptions): NativeSkills {
  if (options.service !== 'provided') throw new Error('REACTIVE_NATIVE_SKILLS_SERVICE_SELECTION_INVALID');
  const owner = new NativeCapabilityOwner('reactive-native-skills', ['skills', 'workingDirectory'], function* (ctx) {
    if (!ctx.get('skills')) throw new Error('REACTIVE_NATIVE_SKILLS_REQUIRED');
    if (!ctx.get('workingDirectory')) throw new Error('REACTIVE_NATIVE_WORKING_DIRECTORY_REQUIRED');
  });
  const sessions = new WeakMap<object, NativeSessionSkills>();
  return Object.freeze({ install: owner.install, dispose: () => owner.dispose(),
    forSession(session: NativeSessionSource) {
      const access = nativeSessionAccess(session); owner.session(access, 'skills');
      let current = sessions.get(session);
      if (!current) {
        const reader = new SessionSkills(owner, access);
        current = Object.freeze({ catalog: reader.catalog, definition: reader.definition.bind(reader) });
        sessions.set(session, current);
      }
      return current;
    },
  });
}
