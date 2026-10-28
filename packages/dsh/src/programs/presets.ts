import type { Context } from '@deepseek-ai/cordis';
import type { Agent } from '@deepseek-ai/dsh-agent';
import type {} from '@deepseek-ai/cordis-plugin-loader';
import type {} from '@deepseek-ai/dsh-agent-preset-registry';
import * as skillFilesystem from '@deepseek-ai/dsh-skill-filesystem';
import type { AnyEnvironment } from '@hanger-source/dsh-reactive/kernel';

/** 应用与环境共同决定原生身份；不同应用的同名环境不会共享装配。 */
export function environmentPresetId(application: string, environmentId: string) {
  return `reactive:${encodeURIComponent(application)}:${encodeURIComponent(environmentId)}`;
}

type Registration = { key: string; plugin: object; retire?: () => Promise<void> };

/** 声明归宿主所有；已 mount 的 standing scope 与代际引用由原生 registry 所有。 */
export class EnvironmentPresets {
  #registrations: Registration[] = [];
  #closing?: Promise<void>;
  constructor(private readonly ctx: Context, private readonly application: string) {}

  async initialize(environments: readonly AnyEnvironment[], attach: (agent: Agent, environment: AnyEnvironment, signal?: AbortSignal) => Promise<unknown>) {
    for (const environment of environments) {
      const presetId = environmentPresetId(this.application, environment.id);
      const key = presetId;
      if (Object.hasOwn(this.ctx.loader.builtins, key)) throw new Error('REACTIVE_PRESET_BUILTIN_EXISTS');
      const plugin = { name: 'reactive-environment',
        inject: ['systemPrompt', ...environment.skills?.length ? ['skills', 'fs'] : []],
        async apply(native: Context) {
          native.systemPrompt.section({ name: presetId, order: 1000, text: environment.instructions, interpolate: false });
          if (environment.skills?.length) await native.plugin(skillFilesystem, {
            providerName: presetId, includeDefaultRoots: false, customSkillDirs: [...environment.skills],
            watch: true, watchFollowSymlinks: false,
          });
          native.on('agent/created', async ({ agent, signal }) => {
            if (agent.session.header.agentPreset !== presetId) throw new Error('REACTIVE_PRESET_MISMATCH');
            await attach(agent, environment, signal);
          });
        },
      };
      this.ctx.loader.builtins[key] = plugin;
      const registration: Registration = { key, plugin };
      this.#registrations.push(registration);
      registration.retire = await this.ctx.agentPresets.register({ id: presetId,
        name: this.application + ':' + environment.id, plugins: [{ name: `cordis:${key}` }] });
      // register 保留装配诊断供 roster 阅读；宿主启动必须进一步拒绝不可用声明。
      const resolved = await this.ctx.agentPresets.resolve(presetId);
      if (resolved.broken !== undefined) throw new Error('REACTIVE_PRESET_INVALID: ' + resolved.broken);
    }
  }

  /** 调用者先释放 owned Agent handles；unregister 只退休原生声明，不接管其代际寿命。 */
  dispose() {
    if (this.#closing) return this.#closing;
    const completion = Promise.withResolvers<void>();
    this.#closing = completion.promise;
    void (async () => {
      const failures: unknown[] = [];
      for (const registration of this.#registrations.reverse()) {
        try { await registration.retire?.(); } catch (error) { failures.push(error); }
        if (this.ctx.loader.builtins[registration.key] === registration.plugin) delete this.ctx.loader.builtins[registration.key];
        else failures.push(new Error('REACTIVE_PRESET_BUILTIN_OWNERSHIP_LOST'));
      }
      this.#registrations = [];
      if (failures.length) throw new AggregateError(failures, 'REACTIVE_PRESETS_DISPOSAL_FAILED');
    })().then(completion.resolve, completion.reject);
    return this.#closing;
  }
}
