import { isAbsolute, normalize } from 'node:path';
import type { Context, Fiber } from '@deepseek-ai/cordis';
import type { Agent } from '@deepseek-ai/dsh-agent';
import { isModelInvocable, isSkillName, renderSkillContent } from '@deepseek-ai/dsh-skill';
import * as skillFilesystem from '@deepseek-ai/dsh-skill-filesystem';
import type {} from '@deepseek-ai/dsh-system-prompt';
import type {} from '@deepseek-ai/dsh-working-directory';
import type {} from '@deepseek-ai/dsh-fs';
import type { SkillDeclaration } from '@hanger-source/dsh-reactive/kernel';

// 原生注册可同步发布通知；先登记身份，撤销后才返回的资源仍归原注册收拢。
type NamedSkill = { text: string; release?: () => void };
type DirectorySkill = { fiber?: Fiber };

/** 当前程序的原生 Skill 接点；目录与方法正文分别沿原生 provider 和 prompt context 生存。 */
export class ProgramSkills {
  #owner: Fiber;
  #ctx?: Context;
  #names: readonly string[] = [];
  #directories: readonly string[] = [];
  #revision = 0;
  #lookupRevision = 0;
  #preparing = new Set<AbortController>();
  #providers = new Map<string, DirectorySkill>();
  #contexts = new Map<string, NamedSkill>();
  #retiring = new Set<Promise<void>>();
  #failures: unknown[] = [];
  #lifetime = new AbortController();
  #disposed = false;
  #bindingsClosing?: Promise<void>;
  #closing?: Promise<void>;

  constructor(private readonly agent: Agent, installation: Context) {
    // 同一声明安装内的子Fiber保留Agent路由，并随Program结束释放目录watcher。
    this.#owner = installation.inject(['skills', 'systemPrompt', 'workingDirectory', 'fs'], ctx => {
      this.#ctx = ctx;
      const invalidate = () => { this.#lookupRevision++; this.invalidatePreparation(); };
      ctx.on('skills/change', invalidate);
      ctx.on('session/event', (session, event) => {
        if (session === agent.session && event.type === 'working-directory/change') invalidate();
      });
      ctx.effect(() => () => this.closeBindings());
    });
  }

  /** 当前整帧形成会话级依赖集合；等价目录共享原provider，文件与正文读取留在原生执行边界。 */
  commit(declarations: readonly SkillDeclaration[]) {
    this.assertActive();
    const names = new Set<string>();
    const directories = new Set<string>();
    for (const declaration of declarations) {
      if (declaration.name !== undefined) {
        if (!isSkillName(declaration.name)) throw new Error('REACTIVE_SKILL_NAME_INVALID: ' + declaration.name);
        names.add(declaration.name);
      } else {
        if (!isAbsolute(declaration.directory)) throw new Error('REACTIVE_SKILL_PATH_NOT_ABSOLUTE: ' + declaration.directory);
        directories.add(normalize(declaration.directory));
      }
    }
    const same = (left: ReadonlySet<string>, right: readonly string[]) => left.size === right.length
      && right.every(value => left.has(value));
    if (same(names, this.#names) && same(directories, this.#directories)) return;
    this.#names = [...names];
    this.#directories = [...directories];
    this.#revision++;
    this.withdraw(names, directories);
    this.invalidatePreparation();
  }

  /** 声明提交即退休失效贡献；先摘身份，再进入可能同步重入的原生释放。 */
  private withdraw(names: ReadonlySet<string>, directories: ReadonlySet<string>) {
    const contexts: NamedSkill[] = [];
    const providers: DirectorySkill[] = [];
    for (const [name, binding] of this.#contexts) if (!names.has(name)) {
      this.#contexts.delete(name); contexts.push(binding);
    }
    for (const [directory, binding] of this.#providers) if (!directories.has(directory)) {
      this.#providers.delete(directory); providers.push(binding);
    }
    for (const binding of contexts) this.releaseContext(binding);
    for (const binding of providers) if (binding.fiber) this.retire(binding.fiber);
  }

  private releaseContext(binding: NamedSkill) {
    try { binding.release?.(); }
    catch (error) { this.#failures.push(error); }
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
      for (const directory of directories) if (!this.#disposed && this.#directories.includes(directory) && !this.#providers.has(directory)) {
        const binding: DirectorySkill = {};
        this.#providers.set(directory, binding);
        try {
          const fiber = ctx.plugin(skillFilesystem, {
            providerName: 'reactive-program:' + encodeURIComponent(this.agent.id) + ':' + encodeURIComponent(directory),
            includeDefaultRoots: false, customSkillDirs: [directory], watch: true, watchFollowSymlinks: false,
          });
          binding.fiber = fiber;
          if (this.#disposed || this.#providers.get(directory) !== binding) this.retire(fiber);
        } catch (error) {
          if (this.#providers.get(directory) === binding) this.#providers.delete(directory);
          throw error;
        }
      }
      for (const name of names) if (!this.#disposed && this.#names.includes(name) && !this.#contexts.has(name)) {
        const binding: NamedSkill = { text: '' };
        this.#contexts.set(name, binding);
        try {
          binding.release = ctx.systemPrompt.context({ name: 'reactive:skill:' + name, order: 1000,
            required: true, interpolate: false, text: () => binding.text });
          if (this.#disposed || this.#contexts.get(name) !== binding) this.releaseContext(binding);
        } catch (error) {
          if (this.#contexts.get(name) === binding) this.#contexts.delete(name);
          throw error;
        }
      }
      const results = await Promise.allSettled([...this.#providers.values()].map(binding => binding.fiber?.await()));
      await Promise.all([...this.#retiring]);
      this.assertActive();
      signal.throwIfAborted();
      const failures = [...this.#failures, ...results.flatMap(result => result.status === 'rejected' ? [result.reason] : [])];
      if (failures.length) throw new AggregateError(failures, 'REACTIVE_SKILL_PREPARATION_FAILED');
      if (revision === this.#revision) return;
    }
  }

  /** 选中的方法正文进入本次原生上下文；依赖变化取消当前读取并准备新的集合。 */
  async prepare(signal: AbortSignal): Promise<() => boolean> {
    const lifetime = AbortSignal.any([signal, this.#lifetime.signal]);
    for (;;) {
      await this.ready(lifetime);
      const ctx = this.#ctx!;
      const revision = this.#revision;
      const lookupRevision = this.#lookupRevision;
      const names = this.#names;
      const current = () => revision === this.#revision && lookupRevision === this.#lookupRevision;
      const attempt = new AbortController();
      this.#preparing.add(attempt);
      const reading = AbortSignal.any([lifetime, attempt.signal]);
      try {
        const cwd = await ctx.workingDirectory.ensure(this.agent, reading);
        const stable = () => !this.#disposed && !lifetime.aborted && current()
          && cwd === ctx.workingDirectory.get(this.agent.session);
        const definitions = await Promise.all(names.map(name => ctx.skills.get(name, { scope: this.agent, cwd, signal: reading })));
        reading.throwIfAborted();
        this.assertActive();
        if (!stable()) continue;
        const texts = definitions.map((definition, index) => {
          if (!definition) throw new Error('REACTIVE_SKILL_NOT_FOUND: ' + names[index]);
          if (!isModelInvocable(definition)) throw new Error('REACTIVE_SKILL_NOT_MODEL_INVOCABLE: ' + names[index]);
          return renderSkillContent(definition);
        });
        names.forEach((name, index) => { this.#contexts.get(name)!.text = texts[index]!; });
        return stable;
      } catch (error) {
        lifetime.throwIfAborted();
        this.assertActive();
        if (!current()) continue;
        throw error;
      } finally {
        this.#preparing.delete(attempt);
        // 原生 Registry 取消本次查询；共享 provider 的注册和其他查询保持各自寿命。
        attempt.abort();
      }
    }
  }

  /** 外部显式释放与 Agent scope 卸载共享同一批原生资源；清理失败仍由此接口传播。 */
  dispose(): Promise<void> {
    if (this.#closing) return this.#closing;
    const completion = Promise.withResolvers<void>();
    this.#closing = completion.promise;
    void (async () => {
      this.#disposed = true;
      this.#lifetime.abort(new Error('REACTIVE_SKILLS_DISPOSED'));
      const failures: unknown[] = [];
      try { await this.#owner.dispose(); } catch (error) { failures.push(error); }
      try { await this.#owner.await(); } catch (error) { failures.push(error); }
      try { await this.closeBindings(); } catch (error) { failures.push(error); }
      if (failures.length) throw new AggregateError(failures, 'REACTIVE_SKILLS_DISPOSAL_FAILED');
    })().then(completion.resolve, completion.reject);
    return this.#closing;
  }

  private assertActive() { if (this.#disposed) throw new Error('REACTIVE_SKILLS_DISPOSED'); }
  private invalidatePreparation() {
    for (const attempt of this.#preparing) attempt.abort(new Error('REACTIVE_SKILL_PREPARATION_CHANGED'));
  }
  private retire(fiber: Fiber) {
    const retirement = (async () => { await fiber.dispose(); await fiber.await(); })().then(() => { this.#retiring.delete(retirement); }, error => {
      this.#retiring.delete(retirement);
      this.#failures.push(error);
    });
    this.#retiring.add(retirement);
  }
  private closeBindings(): Promise<void> {
    if (this.#bindingsClosing) return this.#bindingsClosing;
    const completion = Promise.withResolvers<void>();
    this.#bindingsClosing = completion.promise;
    void (async () => {
      this.#disposed = true;
      this.#lifetime.abort(new Error('REACTIVE_SKILLS_DISPOSED'));
      this.withdraw(new Set(), new Set());
      await Promise.all([...this.#retiring]);
      const failures = [...this.#failures];
      if (failures.length) throw new AggregateError(failures, 'REACTIVE_SKILL_BINDINGS_DISPOSAL_FAILED');
    })().then(completion.resolve, completion.reject);
    return this.#bindingsClosing;
  }
}
