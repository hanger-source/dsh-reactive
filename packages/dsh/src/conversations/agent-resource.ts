import type { Agent } from '@deepseek-ai/dsh-agent';
import { map, of, type Observable } from 'rxjs';
import { createResource, observeResource, switchResource, z, type Observation } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';
import { ownNativeValue, type NativeCapabilityOwner } from '../native-capability.ts';
import type { NativeSessionAccess } from './session-access.ts';

/** 当前真实 Agent 的只读事实；源句柄、能力 owner、查询与单次读取沿已有 Scope 生存。 */
export function nativeAgentResource<T>(owner: NativeCapabilityOwner, access: NativeSessionAccess, scope: Scope, options: {
  readonly service: string;
  changes(agent: Agent): Observable<unknown>;
  read(agent: Agent, signal: AbortSignal): T | PromiseLike<T>;
}) {
  const current = (agent: Agent) => {
    scope.assertActive(); owner.session(access, options.service);
    if (access.liveAgent() !== agent) throw new Error('REACTIVE_NATIVE_AGENT_RESOURCE_CHANGED');
  };
  return createResource(scope, { value: z.custom<T>(), observe: () => access.liveAgent$.pipe(
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
