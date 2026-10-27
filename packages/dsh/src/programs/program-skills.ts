import { isAbsolute, normalize } from 'node:path';
import type { Context, Fiber } from '@deepseek-ai/cordis';
import type { Agent } from '@deepseek-ai/dsh-agent';
import { isModelInvocable, isSkillName, renderSkillContent } from '@deepseek-ai/dsh-skill';
import * as skillFilesystem from '@deepseek-ai/dsh-skill-filesystem';
import type {} from '@deepseek-ai/dsh-system-prompt';
import type {} from '@deepseek-ai/dsh-working-directory';
import type {} from '@deepseek-ai/dsh-fs';
import type { SkillDeclaration } from '@hanger-source/dsh-reactive/kernel';

type NamedSkill = { text: string; release(): void };

/** 当前程序的原生 Skill 接点；目录与方法正文分别沿原生 provider 和 prompt context 生存。 */
export class ProgramSkills {
  #owner: Fiber;
  #ctx?: Context;
  #names: readonly string[] = [];
  #directories: readonly string[] = [];
  #revision = 0;
  #catalogRevision = 0;
  #providers = new Map<string, Fiber>();
  #contexts = new Map<string, NamedSkill>();
  #retiring = new Set<Promise<void>>();
  #failures: unknown[] = [];
  #lifetime = new AbortController();
  #disposed = false;
  #bindingsClosing?: Promise<void>;
  #closing?: Promise<void>;

  constructor(private readonly agent: Agent) {
    // 新的 plugin fiber 继承 Agent 的 scope；显式注入不依赖 agent-loop 的依赖清单。
    this.#owner = agent.ctx.inject(['skills', 'systemPrompt', 'workingDirectory', 'fs'], ctx => {
      this.#ctx = ctx;
      ctx.on('skills/change', () => { this.#catalogRevision++; });
      ctx.effect(() => () => this.closeBindings());
    });
  }

  /** React commit 只提交声明，文件读取、插件就绪和正文加载留在原生执行边界。 */
  commit(declarations: readonly SkillDeclaration[]) {
    this.assertActive();
    const names: string[] = [];
    const directories: string[] = [];
    for (const declaration of declarations) {
      if (declaration.name !== undefined) {
        if (!isSkillName(declaration.name)) throw new Error('REACTIVE_SKILL_NAME_INVALID: ' + declaration.name);
        names.push(declaration.name);
      } else {
        if (!isAbsolute(declaration.directory)) throw new Error('REACTIVE_SKILL_PATH_NOT_ABSOLUTE: ' + declaration.directory);
        directories.push(normalize(declaration.directory));
      }
    }
    if (new Set(names).size !== names.length || new Set(directories).size !== directories.length) {
      throw new Error('REACTIVE_SKILL_DUPLICATE');
    }
    const same = (left: readonly string[], right: readonly string[]) => left.length === right.length
      && left.every((value, index) => value === right[index]);
    if (same(names, this.#names) && same(directories, this.#directories)) return;
    this.#names = names;
    this.#directories = directories;
    this.#revision++;
  }

  /** 在原生 pre-step 的 Skill catalog 收集之前等待；相同目录与名称保留原生注册身份。 */
  async ready(signal: AbortSignal) {
    this.assertActive();
    signal.throwIfAborted();
    await this.#owner.await();
    this.assertActive();
    signal.throwIfAborted();
    const ctx = this.#ctx;
    if (!ctx) throw new Error('REACTIVE_SKILL_SERVICES_NOT_READY');
    for (;;) {
      const revision = this.#revision;
      const directories = new Set(this.#directories);
      const names = new Set(this.#names);
      for (const [directory, fiber] of this.#providers) if (!directories.has(directory)) {
        this.#providers.delete(directory);
        this.retire(fiber);
      }
      for (const directory of directories) if (!this.#providers.has(directory)) {
        this.#providers.set(directory, ctx.plugin(skillFilesystem, {
          providerName: 'reactive-program:' + encodeURIComponent(this.agent.id) + ':' + encodeURIComponent(directory),
          includeDefaultRoots: false, customSkillDirs: [directory], watch: true, watchFollowSymlinks: false,
        }));
      }
      for (const [name, binding] of this.#contexts) if (!names.has(name)) {
        binding.release();
        this.#contexts.delete(name);
      }
      for (const name of names) if (!this.#contexts.has(name)) {
        const binding = { text: '' };
        const release = ctx.systemPrompt.context({ name: 'reactive:skill:' + name, order: 1000,
          required: true, interpolate: false, text: () => binding.text });
        this.#contexts.set(name, Object.assign(binding, { release }));
      }
      const results = await Promise.allSettled([...this.#providers.values()].map(fiber => fiber.await()));
      await Promise.all([...this.#retiring]);
      this.assertActive();
      signal.throwIfAborted();
      const failures = [...this.#failures, ...results.flatMap(result => result.status === 'rejected' ? [result.reason] : [])];
      if (failures.length) throw new AggregateError(failures, 'REACTIVE_SKILL_PREPARATION_FAILED');
      if (revision === this.#revision) return;
    }
  }

  /** 选中的方法正文进入本次原生上下文；声明、目录或 catalog 改变时由调用者重新装配。 */
  async prepare(signal: AbortSignal): Promise<() => boolean> {
    const lifetime = AbortSignal.any([signal, this.#lifetime.signal]);
    for (;;) {
      await this.ready(lifetime);
      const ctx = this.#ctx!;
      const revision = this.#revision;
      const catalogRevision = this.#catalogRevision;
      const names = this.#names;
      const cwd = await ctx.workingDirectory.ensure(this.agent, lifetime);
      const stable = () => !this.#disposed && !lifetime.aborted && revision === this.#revision
        && catalogRevision === this.#catalogRevision && cwd === ctx.workingDirectory.get(this.agent.session);
      let definitions;
      try { definitions = await Promise.all(names.map(name => ctx.skills.get(name, { scope: this.agent, cwd, signal: lifetime }))); }
      catch (error) {
        lifetime.throwIfAborted();
        this.assertActive();
        if (!stable()) continue;
        throw error;
      }
      lifetime.throwIfAborted();
      this.assertActive();
      if (!stable()) continue;
      const texts = definitions.map((definition, index) => {
        if (!definition) throw new Error('REACTIVE_SKILL_NOT_FOUND: ' + names[index]);
        if (!isModelInvocable(definition)) throw new Error('REACTIVE_SKILL_NOT_MODEL_INVOCABLE: ' + names[index]);
        return renderSkillContent(definition);
      });
      names.forEach((name, index) => { this.#contexts.get(name)!.text = texts[index]!; });
      return stable;
    }
  }

  /** 外部显式释放与 Agent scope 卸载共享同一批原生资源；清理失败仍由此接口传播。 */
  dispose(): Promise<void> {
    return this.#closing ??= (async () => {
      this.#disposed = true;
      this.#lifetime.abort(new Error('REACTIVE_SKILLS_DISPOSED'));
      const failures: unknown[] = [];
      try { await this.#owner.dispose(); } catch (error) { failures.push(error); }
      try { await this.closeBindings(); } catch (error) { failures.push(error); }
      if (failures.length) throw new AggregateError(failures, 'REACTIVE_SKILLS_DISPOSAL_FAILED');
    })();
  }

  private assertActive() { if (this.#disposed) throw new Error('REACTIVE_SKILLS_DISPOSED'); }
  private retire(fiber: Fiber) {
    const retirement = Promise.resolve(fiber.dispose()).then(() => { this.#retiring.delete(retirement); }, error => {
      this.#retiring.delete(retirement);
      this.#failures.push(error);
    });
    this.#retiring.add(retirement);
  }
  private closeBindings(): Promise<void> {
    return this.#bindingsClosing ??= (async () => {
      this.#disposed = true;
      this.#lifetime.abort(new Error('REACTIVE_SKILLS_DISPOSED'));
      const failures: unknown[] = [];
      for (const binding of this.#contexts.values()) {
        try { binding.release(); } catch (error) { failures.push(error); }
      }
      this.#contexts.clear();
      for (const fiber of this.#providers.values()) this.retire(fiber);
      this.#providers.clear();
      await Promise.all([...this.#retiring]);
      failures.push(...this.#failures);
      if (failures.length) throw new AggregateError(failures, 'REACTIVE_SKILL_BINDINGS_DISPOSAL_FAILED');
    })();
  }
}
