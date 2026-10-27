import type { Context } from '@deepseek-ai/cordis';
import type { Agent } from '@deepseek-ai/dsh-agent';
import type { ToolDefinition } from '@deepseek-ai/dsh-tools';
import type {} from '@deepseek-ai/dsh-system-prompt';
import type {} from '@deepseek-ai/dsh-fs';
import { Scope, type Cell, type Value, batch } from '@hanger-source/dsh-reactive/kernel';
import { type ResourceValue } from '@hanger-source/dsh-reactive/kernel';

type BoundContext = { source: ResourceValue<string>; committed: Cell<string>; release(): void };
type Entry = { scope: Scope; sourceScope: Scope; contexts: Map<string, BoundContext> };

/** dsh 的原生接点。业务派生值与读取逻辑位于普通响应式代码中。 */
export class AgentBindings {
  #entries = new Map<Agent, Entry>();
  constructor(ctx: Context) {
    const releaseAgent = ctx.on('agent/disposed', ({ agent }) => this.release(agent));
    ctx.effect(() => () => {
      releaseAgent();
      const failures: unknown[] = [];
      for (const agent of this.#entries.keys()) {
        try { this.release(agent); } catch (error) { failures.push(error); }
      }
      if (failures.length) throw new AggregateError(failures, 'REACTIVE_AGENTS_DISPOSAL_FAILED');
    });
  }
  scope(agent: Agent, provided?: Scope): Scope {
    let entry = this.#entries.get(agent);
    if (!entry) {
      if (!provided) throw new Error('REACTIVE_AGENT_SCOPE_REQUIRED');
      const sourceScope = provided;
      entry = { scope: sourceScope.child(), sourceScope, contexts: new Map() };
      this.#entries.set(agent, entry);
    }
    if (provided && entry.sourceScope !== provided) throw new Error('REACTIVE_AGENT_SCOPE_CONFLICT');
    return entry.scope;
  }
  contexts(agent: Agent, definitions: Value<ReadonlyMap<string, ResourceValue<string>>>) {
    const scope = this.scope(agent);
    const contexts = this.#entries.get(agent)!.contexts;
    scope.own(() => { for (const binding of contexts.values()) binding.release(); contexts.clear(); });
    scope.effect(() => {
      const current = definitions.get();
      for (const [name, binding] of contexts) if (current.get(name) !== binding.source) {
        binding.release(); contexts.delete(name);
      }
      for (const [name, source] of current) if (!contexts.has(name)) {
        // 原生预装配不会发往模型；正式装配必须经过 prepare 的异步读取边界。
        const committed = scope.state('');
        const release = agent.ctx.systemPrompt.context({ name, order: 1000, required: true,
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
  tools(agent: Agent, definitions: Value<readonly ToolDefinition[]>) {
    const scope = this.scope(agent);
    if (definitions.scope !== this.#entries.get(agent)!.sourceScope) throw new Error('REACTIVE_TOOLS_SCOPE_MISMATCH');
    const registrations = new Map<string, { definition: ToolDefinition; release(): void }>();
    const clear = () => {
      const previous = [...registrations.values()]; registrations.clear();
      for (const registration of previous.reverse()) registration.release();
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
        const release = agent.ctx.tools.register(definition);
        registrations.set(definition.name, { definition, release });
      }
    });
  }
  release(agent: Agent) {
    const entry = this.#entries.get(agent);
    this.#entries.delete(agent);
    entry?.scope.dispose();
  }
}
