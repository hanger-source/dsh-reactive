import type { Agent } from '@deepseek-ai/dsh-agent';
import { map, of, type Observable } from 'rxjs';
import { createResource, observeResource, switchResource, z, type Observation, type ResourceReadable } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';
import { ownNativeValue } from '../native-capability.ts';

export interface NativeAgentResource<T> extends ResourceReadable<T> {
  readonly values$: Observable<T>;
  invalidate(): void;
}

export interface NativeAgentResourceOptions<T> {
  changes(agent: Agent): Observable<unknown>;
  /** 原生事实须支持结构化复制；服务和执行句柄保留在扩展内部。 */
  read(agent: Agent, signal: AbortSignal): T | PromiseLike<T>;
}

/** @internal 只需真实来源的代际与核验，不取得其Scope、Host登记或原始权限对象。 */
export interface NativeAgentResourceSource {
  assertCurrent(): void;
  liveAgent(): Agent | undefined;
  readonly liveAgent$: Observable<Agent | undefined>;
}

/** @internal 当前真实 Agent 的只读事实；源句柄、能力 owner、查询与单次读取沿已有 Scope 生存。 */
export function nativeAgentResource<T>(scope: Scope, source: NativeAgentResourceSource,
  options: NativeAgentResourceOptions<T>): NativeAgentResource<T> {
  const current = (agent: Agent) => {
    scope.assertActive(); source.assertCurrent();
    if (source.liveAgent() !== agent) throw new Error('REACTIVE_NATIVE_AGENT_RESOURCE_CHANGED');
  };
  return createResource(scope, { value: z.custom<T>(), observe: () => source.liveAgent$.pipe(
    map((agent, generation): Observation<Agent> => agent
      ? { status: 'ready', generation, value: agent, observedAt: Date.now() }
      : { status: 'pending', generation }),
    switchResource(agent => observeResource({ key$: of(agent),
      // observeResource 先连接通知再初读；切换代际立即撤下旧 ready，退订并取消旧读取。
      invalidations: () => { current(agent); return options.changes(agent); },
      read: async (_agent, signal) => {
        current(agent);
        const value = await options.read(agent, signal);
        signal.throwIfAborted(); current(agent);
        return ownNativeValue(value);
      },
    })),
  ) });
}
