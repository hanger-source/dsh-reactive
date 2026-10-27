import type { SessionBinding, ISessions } from '@deepseek-ai/dsh-api-session-controller/client';
import type { SessionId } from '@deepseek-ai/dsh-session/types';
import type { UiSession } from '@deepseek-ai/dsh-client-ui-session/client';
import type { HostObservable, InjectFace } from '@deepseek-ai/dsh-client-ui-slots';
import type { ProgramContract } from '@hanger-source/dsh-reactive';
import type { NativeSession } from './session.ts';
import { nativeSource, type NativeSourceInput, type NativeSourceValue } from './sources.ts';

export type NativeSources = Readonly<Record<string, NativeSourceInput<unknown>>>;
export type NativeHooks<S extends NativeSources> = {
  readonly hooks: { readonly [K in keyof S]: HostObservable<NativeSourceValue<S[K]>> };
};
/** 完整组件 props 继续由原生 SlotCore.register 推导，这里只贡献 inject 的 hooks compartment。 */
export type NativeInjected<S extends NativeSources> = InjectFace<NativeHooks<S>>;

export function nativeHooks<S extends NativeSources>(sources: S): NativeHooks<S> {
  // 同一份 own keys 同时决定运行时字段和映射类型，不解析或复制业务值。
  const hooks = Object.fromEntries(Object.entries(sources).map(([key, source]) => [key, nativeSource(source)]));
  return { hooks: hooks as NativeHooks<S>['hooks'] };
}

type SessionFactories<P extends ProgramContract> = Readonly<Record<string, (session: NativeSession<P>) => NativeSourceInput<unknown>>>;
export type NativeSessionInject<P extends ProgramContract, S extends SessionFactories<P>> = {
  /** 没有关联本 Program 的原生会话时明确缺席，不借当前页面的另一个句柄。 */
  readonly program: NativeSession<P> | undefined;
  readonly hooks: { readonly [K in keyof S]: HostObservable<NativeSourceValue<ReturnType<S[K]>> | undefined> };
};
const absent: HostObservable<undefined> = Object.freeze({ getSnapshot: () => undefined, subscribe: () => () => {} });

/**
 * 可直接传给原生 register 的 inject。原生给定 SessionId，框架核 exact generation；
 * 应用只选择该 generation 已借入的 Program，SlotCore 保留所有标准 props 和释放责任。
 */
export function nativeSessionInject<P extends ProgramContract, const S extends SessionFactories<P>>(
  sessions: Pick<ISessions, 'binding'>,
  options: { readonly resolve: (native: SessionBinding) => NativeSession<P> | undefined; readonly sources: S },
): (sessionId: SessionId) => NativeSessionInject<P, S> {
  return id => {
    const native = sessions.binding(id);
    if (!native) throw new Error('REACTIVE_NATIVE_SESSION_GENERATION_INACTIVE');
    const session = options.resolve(native);
    if (session) {
      session.assertActive();
      if (session.native !== native) throw new Error('REACTIVE_NATIVE_SESSION_GENERATION_MISMATCH');
    }
    const hooks = Object.fromEntries(Object.entries(options.sources).map(([key, select]) => {
      if (!session) return [key, absent];
      const source = nativeSource(select(session));
      const guarded = session.source({
        peek: () => source.getSnapshot(),
        subscribe: accept => source.subscribe(() => accept(source.getSnapshot())),
      });
      return [key, guarded];
    }));
    return { program: session, hooks: hooks as NativeSessionInject<P, S>['hooks'] };
  };
}

/**
 * 为 native UiSession 的标准 source roster 提供 core 读对象。
 * 同一个字段名的唯一性、注册更新、无会话投影和退出均由 UiSession.provide 处理。
 */
export function provideNativeSources<const S extends Readonly<Record<string, (binding: SessionBinding) => NativeSourceInput<unknown>>>>(
  uiSession: Pick<UiSession, 'provide'>,
  sources: S,
): () => void {
  const names = Object.keys(sources);
  return uiSession.provide({ hooks: names, resolve: binding => ({
    hooks: Object.fromEntries(names.map(name => [name, nativeSource(sources[name]!(binding))])),
  }) });
}

/** @internal 只拥有这次原生贡献的释放，不维护另一份 slot / node registry。 */
export function installNativeContributions(installers: readonly (() => () => void)[]): () => void {
  const releases: (() => void)[] = [];
  let active = true;
  const release = () => {
    if (!active) return;
    active = false;
    const failures: unknown[] = [];
    for (const stop of releases.reverse()) { try { stop(); } catch (error) { failures.push(error); } }
    if (failures.length) throw new AggregateError(failures, 'REACTIVE_NATIVE_CONTRIBUTION_RELEASE_FAILED');
  };
  try { for (const install of installers) releases.push(install()); }
  catch (error) {
    try { release(); } catch (cleanup) { throw new AggregateError([error, cleanup], 'REACTIVE_NATIVE_CONTRIBUTION_INSTALL_FAILED'); }
    throw error;
  }
  return release;
}
