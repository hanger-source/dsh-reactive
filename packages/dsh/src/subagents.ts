import type { SubagentRuntime, SubagentCatalogEntry, SubagentDescendantListEntry } from '@deepseek-ai/dsh-subagent';
import type { SessionId } from '@deepseek-ai/dsh-session';
import { Observable, firstValueFrom, merge } from 'rxjs';
import { abortable, observeResource, type CancellationOptions, type ResourceReader } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';
import { NativeCapabilityOwner, nativeObservationScope, nativeResource, ownNativeValue,
  type NativeCapability, type NativeSessionSource } from './native-capability.ts';
import { nativeSessionAccess, type NativeSessionAccess } from './conversations/session-access.ts';

export type { SubagentCatalogEntry, SubagentDescendantListEntry } from '@deepseek-ai/dsh-subagent';
export { SubagentError } from '@deepseek-ai/dsh-subagent';

export interface NativeSubagentsOptions { readonly service: 'provided'; }

/** 原生目录事实，不将one-shot、external或仍可继续的子会话解释为业务Goal完成。 */
export interface NativeSessionSubagents {
  readonly catalog: ResourceReader<readonly SubagentCatalogEntry[]>;
  readonly catalog$: Observable<readonly SubagentCatalogEntry[]>;
  /** 正式递归切面；activity保留原生Session驻留语义，不表示当前turn或业务Goal阶段。 */
  readonly descendants: ResourceReader<readonly SubagentDescendantListEntry[]>;
  readonly descendants$: Observable<readonly SubagentDescendantListEntry[]>;
  /** 正式递归查询保留每一分支的诊断；读取不加载或恢复Agent。 */
  readDescendants(options?: CancellationOptions): Promise<readonly SubagentDescendantListEntry[]>;
  /** exact live祖先只中断选定分支的当前执行；不清待处理inbox，返回不保证已静止。 */
  interrupt(targetSessionId: SessionId, options?: CancellationOptions): void;
}

class SessionSubagents implements NativeSessionSubagents {
  readonly catalog: ResourceReader<readonly SubagentCatalogEntry[]>;
  readonly catalog$: Observable<readonly SubagentCatalogEntry[]>;
  readonly descendants: ResourceReader<readonly SubagentDescendantListEntry[]>;
  readonly descendants$: Observable<readonly SubagentDescendantListEntry[]>;
  #scope: Scope;

  constructor(private readonly owner: NativeCapabilityOwner, private readonly access: NativeSessionAccess) {
    this.#scope = nativeObservationScope(owner, access);
    const changes = new Observable<void>(subscriber => this.context().on('session/event', (session, event) => {
      if (session.id === access.sessionId && event.type === 'subagent/catalog') subscriber.next();
    }));
    this.catalog = nativeResource(this.#scope, () => observeResource({ key$: merge(changes, access.liveAgent$),
      read: (_agent, signal) => this.read(signal, (service, reading) => service.listChildren(access.sessionId, reading)) }));
    this.catalog$ = this.catalog.values$;
    const descendantChanges = new Observable<void>(subscriber => {
      const ctx = this.context();
      const notify = (id: SessionId) => {
        if (subscriber.closed) return;
        try {
          const current = this.descendants.peek();
          // pending/error尚无完整可达切面，不能漏掉读取期间的分支变化；ready只借正式结果判关联。
          if (id === access.sessionId || current.status !== 'ready'
            || current.value.some(row => row.id === id || row.parentId === id)) subscriber.next();
        } catch (error) { subscriber.error(error); }
      };
      const releases = [
        ctx.on('session/event', (session, event) => { if (event.type === 'subagent/catalog') notify(session.id); }),
        ctx.on('session/created', session => notify(session.id)),
        ctx.on('session/disposed', session => notify(session.id)),
      ];
      return () => { for (const release of releases) release(); };
    });
    this.descendants = nativeResource(this.#scope, () => observeResource({ key$: merge(descendantChanges, access.liveAgent$),
      read: (_agent, signal) => this.readDescendants({ signal }) }));
    this.descendants$ = this.descendants.values$;
  }

  private context() { this.#scope.assertActive(); return this.owner.session(this.access, 'subagents'); }

  private service(): SubagentRuntime {
    const service: SubagentRuntime | undefined = this.context().get('subagents');
    if (!service) throw new Error('REACTIVE_NATIVE_SUBAGENTS_REQUIRED');
    return service;
  }

  private async read<T>(signal: AbortSignal | undefined, query: (service: SubagentRuntime, signal: AbortSignal) => Promise<T>): Promise<T> {
    const reading = signal ? AbortSignal.any([this.#scope.signal, signal]) : this.#scope.signal;
    reading.throwIfAborted();
    const service = this.service();
    const result = await firstValueFrom(abortable(current => query(service, current), reading));
    reading.throwIfAborted(); this.context();
    return ownNativeValue(result);
  }

  readDescendants(options: CancellationOptions = {}): Promise<readonly SubagentDescendantListEntry[]> {
    return this.read(options.signal, (service, signal) => service.listDescendants(this.access.sessionId, signal));
  }

  interrupt(targetSessionId: SessionId, options: CancellationOptions = {}): void {
    const service = this.service();
    options.signal?.throwIfAborted();
    const agent = this.access.liveAgent();
    if (!agent || agent.id !== this.access.sessionId) throw new Error('REACTIVE_NATIVE_SUBAGENTS_LIVE_ANCESTOR_REQUIRED');
    // 真实服务核exact Agent与祖先关系；信号仅约束进入控制前，不接管既有Activation。
    service.interrupt(targetSessionId, { kind: 'ancestor', agent });
  }

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
  const sessions = new WeakMap<object, NativeSessionSubagents>();
  return Object.freeze({ install: owner.install, dispose: () => owner.dispose(),
    forSession(session: NativeSessionSource) {
      const access = nativeSessionAccess(session); owner.session(access, 'subagents');
      let current = sessions.get(session);
      if (!current) {
        const reader = new SessionSubagents(owner, access);
        current = Object.freeze({ catalog: reader.catalog, catalog$: reader.catalog$,
          descendants: reader.descendants, descendants$: reader.descendants$,
          readDescendants: reader.readDescendants.bind(reader), interrupt: reader.interrupt.bind(reader) });
        sessions.set(session, current);
      }
      return current;
    },
  });
}
