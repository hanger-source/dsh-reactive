import type { Context, Fiber } from '@deepseek-ai/cordis';
import type { Agent } from '@deepseek-ai/dsh-agent';
import type { ToolDefinition } from '@deepseek-ai/dsh-tools';
import type {} from '@deepseek-ai/dsh-system-prompt';
import type {} from '@deepseek-ai/dsh-fs';
import { Scope, type Cell, batch } from '@hanger-source/dsh-reactive/kernel';
import { type ResourceValue } from '@hanger-source/dsh-reactive/kernel';
import { requireActivePlugin } from '../native-installation.ts';

type BoundContext = { source: ResourceValue<string>; committed: Cell<string>; release(): void };
interface Entry {
  readonly scope: Scope;
  readonly context: Context;
  readonly contexts: Map<string, BoundContext>;
  readonly closed: Promise<void>;
  ready(): Promise<void>;
  dispose(): Promise<void>;
}

/** dsh 的原生接点。业务派生值与读取逻辑位于普通响应式代码中。 */
export class AgentBindings {
  #entries = new WeakMap<Agent, Entry>();

  /** 一次声明安装同时借用Program图，并由实际Agent的原生scope持有。 */
  create(agent: Agent, sourceScope: Scope, signal: AbortSignal, release: () => void | Promise<void>): Entry {
    signal.throwIfAborted();
    if (this.#entries.has(agent)) throw new Error('REACTIVE_AGENT_ALREADY_BOUND');
    const scope = sourceScope.child();
    scope.own(release);
    const resources = Promise.withResolvers<void>();
    const ended = Promise.withResolvers<void>();
    let closing = false;
    let disposing: Promise<void> | undefined;
    let releaseOwner: (() => void) | undefined;
    let installation: (Fiber & PromiseLike<Fiber>) | undefined;
    const closeBindings = (): Promise<void> => {
      if (closing) return resources.promise;
      closing = true;
      signal.removeEventListener('abort', stop);
      const failures = new Set<unknown>();
      try { scope.dispose(); } catch (error) { failures.add(error); }
      void (async () => {
        try { await scope.closed; } catch (error) { failures.add(error); }
        if (failures.size === 1) throw [...failures][0];
        if (failures.size) throw new AggregateError([...failures], 'REACTIVE_AGENT_BINDINGS_DISPOSAL_FAILED');
      })().then(resources.resolve, resources.reject);
      return resources.promise;
    };
    const dispose = (): Promise<void> => {
      if (disposing) return disposing;
      disposing = ended.promise;
      const owned = releaseOwner; releaseOwner = undefined;
      owned?.();
      // 先同步撤销声明和读取；原生Fiber与Skill的异步退出保留同一回执。
      const closed = closeBindings();
      void (async () => {
        const failures = new Set<unknown>();
        try { await closed; } catch (error) { failures.add(error); }
        if (installation) {
          try { await installation.dispose(); } catch (error) { failures.add(error); }
          try { await installation; } catch (error) { failures.add(error); }
        }
        if (failures.size === 1) throw [...failures][0];
        if (failures.size) throw new AggregateError([...failures], 'REACTIVE_AGENT_INSTALLATION_DISPOSAL_FAILED');
      })().then(ended.resolve, ended.reject);
      return disposing;
    };
    const stop = () => {
      // Abort事件不能等待；错误回执仍由显式关闭及Agent清理加入，日志只保留现场。
      const closing = dispose();
      void closing.catch(error => agent.ctx.logger.error('reactive.program.disposal.failed', { sessionId: agent.id, error }));
    };
    // 原图保留已开始的原生卸载；子Scope只加入自己的资源，避免等待父图或当前Fiber。
    releaseOwner = sourceScope.own(dispose);
    try {
      installation = agent.ctx.plugin({ name: 'reactive-program-bindings', inject: ['tools', 'systemPrompt', 'agents', 'sessionProjections', 'userQuestions'], apply(native: Context) {
        if (closing) return;
        native.effect(() => () => {
          const closed = closeBindings();
          // provider退出也退休原实例；清理回调只等待资源，不反向等待自己的Fiber。
          if (!disposing) stop();
          return closed;
        });
      } });
      const installed = installation;
      const entry: Entry = { scope, context: installed.ctx, contexts: new Map(), closed: ended.promise,
        async ready() { signal.throwIfAborted(); await requireActivePlugin(installed); signal.throwIfAborted(); scope.assertActive(); },
        dispose,
      };
      this.#entries.set(agent, entry);
      signal.addEventListener('abort', stop, { once: true });
      if (signal.aborted) stop();
      return entry;
    } catch (error) {
      // 同步构造不能等待；原图的owned回执继续承担全部异步清理。
      stop();
      throw error;
    }
  }
  scope(agent: Agent): Scope {
    const entry = this.#entries.get(agent);
    if (!entry) throw new Error('REACTIVE_AGENT_NOT_BOUND');
    return entry.scope;
  }
  ready(agent: Agent): Promise<void> {
    const entry = this.#entries.get(agent);
    if (!entry) throw new Error('REACTIVE_AGENT_NOT_BOUND');
    return entry.ready();
  }
  contexts(agent: Agent, factory: () => ReadonlyMap<string, ResourceValue<string>>) {
    const scope = this.scope(agent);
    const definitions = scope.computed(factory);
    const { contexts, context } = this.#entries.get(agent)!;
    scope.own(() => {
      const previous = [...contexts.values()]; contexts.clear();
      const failures: unknown[] = [];
      for (const binding of previous) { try { binding.release(); } catch (error) { failures.push(error); } }
      if (failures.length) throw new AggregateError(failures, 'REACTIVE_CONTEXTS_DISPOSAL_FAILED');
    });
    scope.effect(() => {
      const current = definitions.get();
      for (const [name, binding] of contexts) if (current.get(name) !== binding.source) {
        binding.release(); contexts.delete(name);
      }
      for (const [name, source] of current) if (!contexts.has(name)) {
        // 原生预装配不会发往模型；正式装配必须经过 prepare 的异步读取边界。
        const committed = scope.state('');
        const release = context.systemPrompt.context({ name, order: 1000, required: true,
          interpolate: false, text: () => committed.peek() });
        contexts.set(name, { source, committed, release });
      }
    });
  }
  /** 在应用准入、输入关联之后，原生 prompt 装配之前调用一次。
   * prepare 固定本次上下文；之后的现场变化只影响下一次执行边界。 */
  async prepare(agent: Agent, signal: AbortSignal) {
    const entry = this.#entries.get(agent);
    if (!entry) throw new Error('REACTIVE_AGENT_NOT_BOUND');
    for (;;) {
      const bindings = [...entry.contexts.values()];
      const sameBindings = () => bindings.length === entry.contexts.size && bindings.every(binding => [...entry.contexts.values()].includes(binding));
      let texts: string[];
      try { texts = await Promise.all(bindings.map(binding => binding.source.read(signal))); }
      catch (error) {
        signal.throwIfAborted(); entry.scope.assertActive();
        // 旧模块已退出时，它的读取取消属于依赖切换，继续读取现在的声明集合。
        if (!sameBindings()) continue;
        throw error;
      }
      signal.throwIfAborted();
      entry.scope.assertActive();
      const stable = () => sameBindings()
        && !bindings.some((binding, index) => {
        const now = binding.source.peek();
        return now.status !== 'ready' || now.value !== texts[index];
      });
      if (!stable()) continue;
      batch(() => bindings.forEach((binding, index) => binding.committed.set(texts[index]!)));
      return stable;
    }
  }
  /** 能力表随派生值变化注册到原生 agent scope，移除的能力也同时退出执行准入。 */
  tools(agent: Agent, factory: () => readonly ToolDefinition[]) {
    const scope = this.scope(agent);
    const entry = this.#entries.get(agent)!;
    const definitions = scope.computed(factory);
    const registrations = new Map<string, { definition: ToolDefinition; release(): void }>();
    const clear = () => {
      const previous = [...registrations.values()]; registrations.clear();
      const failures: unknown[] = [];
      for (const registration of previous.reverse()) { try { registration.release(); } catch (error) { failures.push(error); } }
      if (failures.length) throw new AggregateError(failures, 'REACTIVE_TOOLS_DISPOSAL_FAILED');
    };
    scope.own(clear);
    scope.effect(() => {
      const current = definitions.value;
      const names = new Set(current.map(tool => tool.name));
      if (names.size !== current.length) throw new Error('REACTIVE_TOOL_NAME_DUPLICATE');
      const definitionsByName = new Map(current.map(definition => [definition.name, definition]));
      for (const [name, registration] of registrations) if (definitionsByName.get(name) !== registration.definition) {
        registration.release(); registrations.delete(name);
      }
      for (const definition of current) if (!registrations.has(definition.name)) {
        const release = entry.context.tools.register(definition);
        registrations.set(definition.name, { definition, release });
      }
    });
  }
  async release(agent: Agent): Promise<void> {
    const entry = this.#entries.get(agent);
    if (!entry) return;
    try { await entry.dispose(); }
    finally { if (this.#entries.get(agent) === entry) this.#entries.delete(agent); }
  }
}
