import type { Context } from '@deepseek-ai/cordis';
import type { SessionReference } from '@deepseek-ai/dsh-api-session-controller/client';
import { SessionId } from '@deepseek-ai/dsh-session/types';
import type { CancellationOptions, ProgramContract } from '@hanger-source/dsh-reactive';
import { bindProgramClient } from '@hanger-source/dsh-reactive-client';
import { defer, firstValueFrom, fromEvent, mergeMap, raceWith, throwError } from 'rxjs';
import { bindNativeSession, type NativeSession } from './session.ts';

declare module '@deepseek-ai/dsh-api-session-controller/client' {
  interface SessionReferenceSourceMap { reactiveProgram: unknown; }
}

/** 拥有一个原生代际引用、独立 SDK 及其 UI 观察；释放不取消 Host 工作。 */
export interface NativeSessionReference<P extends ProgramContract> extends AsyncDisposable {
  readonly session: NativeSession<P>;
  release(): Promise<void>;
}

/** 刷新原生目录并在已有客户端装配中取得明确会话；signal 只取消本次取得。 */
export async function retainNativeSession<P extends ProgramContract>(
  program: P, ctx: Context, sessionId: string, options: CancellationOptions = {},
): Promise<NativeSessionReference<P>> {
  options.signal?.throwIfAborted();
  const sessions = ctx.get('sessions');
  if (!sessions) throw new Error('REACTIVE_NATIVE_SESSIONS_REQUIRED');
  if (sessions.scopeOf(ctx) !== undefined) throw new Error('REACTIVE_NATIVE_SESSION_REFERENCE_OWNER_REQUIRED');
  const client = await bindProgramClient(program, ctx);
  const controller = new AbortController();
  let reference: SessionReference | undefined;
  let session: NativeSession<P> | undefined;
  let closing: Promise<void> | undefined;
  const close = (): Promise<void> => {
    if (closing) return closing;
    const completion = Promise.withResolvers<void>();
    closing = completion.promise;
    controller.abort(new Error('REACTIVE_NATIVE_SESSION_REFERENCE_RELEASED'));
    const failures: unknown[] = [];
    try { session?.dispose(); } catch (error) { failures.push(error); }
    try { reference?.release(); } catch (error) { failures.push(error); }
    void Promise.resolve().then(() => client.dispose()).then(() => {
      if (failures.length) throw new AggregateError(failures, 'REACTIVE_NATIVE_SESSION_REFERENCE_RELEASE_FAILED');
    }, error => {
      throw new AggregateError([...failures, error], 'REACTIVE_NATIVE_SESSION_REFERENCE_RELEASE_FAILED');
    }).then(completion.resolve, completion.reject);
    return closing;
  };
  let forget: (() => Promise<void>) | undefined;
  const release = (): Promise<void> => close().finally(() => forget?.());
  const abort = (): void => controller.abort(options.signal?.reason);
  try {
    forget = ctx.effect(() => close, 'reactive native Session reference');
    options.signal?.addEventListener('abort', abort, { once: true });
    if (options.signal?.aborted) abort();
    controller.signal.throwIfAborted();
    const [binding, sdk] = await firstValueFrom(fromEvent(controller.signal, 'abort').pipe(
      mergeMap(() => throwError(() => controller.signal.reason)),
      raceWith(defer(async () => {
        await sessions.refresh();
        controller.signal.throwIfAborted();
        reference = sessions.retain(SessionId(sessionId), { source: 'reactiveProgram', signal: controller.signal });
        return Promise.all([reference.ready, client.open(sessionId)]);
      })),
    ));
    controller.signal.throwIfAborted();
    session = bindNativeSession(program, binding, sdk);
    controller.signal.throwIfAborted();
    return Object.freeze({ session, release, [Symbol.asyncDispose]: release });
  } catch (error) {
    try { await release(); }
    catch (cleanup) { throw new AggregateError([error, cleanup], 'REACTIVE_NATIVE_SESSION_REFERENCE_ACQUISITION_FAILED'); }
    throw error;
  } finally { options.signal?.removeEventListener('abort', abort); }
}
