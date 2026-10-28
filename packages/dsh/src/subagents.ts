import type { SubagentRuntime, SubagentCatalogEntry, SubagentDescendantListEntry } from '@deepseek-ai/dsh-subagent';
import { Observable, firstValueFrom, merge } from 'rxjs';
import { abortable, observeResource, type CancellationOptions, type ResourceReader } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';
import { NativeCapabilityOwner, nativeObservationScope, nativeResource, ownNativeValue,
  type NativeCapability, type NativeSessionSource } from './native-capability.ts';
import { nativeSessionAccess, type NativeSessionAccess } from './conversations/session-access.ts';

export type { SubagentCatalogEntry, SubagentDescendantListEntry } from '@deepseek-ai/dsh-subagent';

export interface NativeSubagentsOptions { readonly service: 'provided'; }

/** 原生目录事实，不将one-shot、external或仍可继续的子会话解释为业务Goal完成。 */
export interface NativeSessionSubagents {
  readonly catalog: ResourceReader<readonly SubagentCatalogEntry[]>;
  readonly catalog$: Observable<readonly SubagentCatalogEntry[]>;
  /** 正式递归查询保留每一分支的诊断；读取不加载或恢复Agent。 */
  readDescendants(options?: CancellationOptions): Promise<readonly SubagentDescendantListEntry[]>;
  /** 只释放这次读侧，不中断子Agent或其原生Activation。 */
  dispose(): void;
}

class SessionSubagents implements NativeSessionSubagents {
  readonly catalog: ResourceReader<readonly SubagentCatalogEntry[]>;
  readonly catalog$: Observable<readonly SubagentCatalogEntry[]>;
  #scope: Scope;
  get disposed(): boolean { return this.#scope.disposed; }

  constructor(private readonly owner: NativeCapabilityOwner, private readonly access: NativeSessionAccess) {
    this.#scope = nativeObservationScope(owner, access);
    const changes = new Observable<void>(subscriber => this.context().on('session/event', (session, event) => {
      if (session.id === access.sessionId && event.type === 'subagent/catalog') subscriber.next();
    }));
    this.catalog = nativeResource(this.#scope, () => observeResource({ key$: merge(changes, access.liveAgent$),
      read: (_agent, signal) => this.read(signal, (service, reading) => service.listChildren(access.sessionId, reading)) }));
    this.catalog$ = this.catalog.values$;
  }

  private context() { this.#scope.assertActive(); return this.owner.session(this.access, 'subagents'); }

  private async read<T>(signal: AbortSignal | undefined, query: (service: SubagentRuntime, signal: AbortSignal) => Promise<T>): Promise<T> {
    const reading = signal ? AbortSignal.any([this.#scope.signal, signal]) : this.#scope.signal;
    reading.throwIfAborted();
    const service: SubagentRuntime | undefined = this.context().get('subagents');
    if (!service) throw new Error('REACTIVE_NATIVE_SUBAGENTS_REQUIRED');
    const result = await firstValueFrom(abortable(current => query(service, current), reading));
    reading.throwIfAborted(); this.context();
    return ownNativeValue(result);
  }

  readDescendants(options: CancellationOptions = {}): Promise<readonly SubagentDescendantListEntry[]> {
    return this.read(options.signal, (service, signal) => service.listDescendants(this.access.sessionId, signal));
  }

  dispose(): void { this.#scope.dispose(); }
}

export interface NativeSubagents extends NativeCapability {
  forSession(session: NativeSessionSource): NativeSessionSubagents;
}

/** 原生发现读侧。创建和投递仍须通过具有正式Program准入的执行入口。 */
export function createNativeSubagents(options: NativeSubagentsOptions): NativeSubagents {
  if (options.service !== 'provided') throw new Error('REACTIVE_NATIVE_SUBAGENTS_PROVIDED_REQUIRED');
  const owner = new NativeCapabilityOwner('reactive-native-subagents', ['subagents'], function* (ctx) {
    if (!ctx.get('subagents')) throw new Error('REACTIVE_NATIVE_SUBAGENTS_REQUIRED');
  });
  const sessions = new WeakMap<object, SessionSubagents>();
  return Object.freeze({ install: owner.install, dispose: () => owner.dispose(),
    forSession(session: NativeSessionSource) {
      const access = nativeSessionAccess(session); owner.session(access, 'subagents');
      let current = sessions.get(session);
      if (!current || current.disposed) { current = new SessionSubagents(owner, access); sessions.set(session, current); }
      return current;
    },
  });
}
