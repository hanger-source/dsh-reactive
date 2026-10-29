import type { Context } from '@deepseek-ai/cordis';
import type { Agent } from '@deepseek-ai/dsh-agent';
import type { SessionId, SessionEvent } from '@deepseek-ai/dsh-session';
import type { SessionInspection } from '@deepseek-ai/dsh-session-persistence';
import type { SessionAddress } from '@deepseek-ai/dsh-api-session-controller/types';
import { Observable, defer, filter, fromEvent, switchMap, takeUntil } from 'rxjs';
import { observeResource, type Observation } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';

/** @internal Host 对 exact 本地句柄或 Program 来源签发的权限；不从 sessionId 反查或启动 Agent。 */
export interface NativeSessionAccess {
  readonly context: Context;
  readonly sessionId: SessionId;
  /** Host 已核验的根或直接子会话地址；不从裸 id 推断原生归属。 */
  readonly journalAddress: SessionAddress;
  readonly signal: AbortSignal;
  readonly scope: Scope;
  assertCurrent(): void;
  /** 原会话header的工作目录；读取文件范围不需要冲刷或重读会话日志。 */
  workspaceRoot(): string;
  readonly liveAgent$: Observable<Agent | undefined>;
  liveAgent(): Agent | undefined;
  /** signal 结束本次借用读取的等待；原生 flush、持久句柄清理仍由原 owner 结算。 */
  inspect(signal?: AbortSignal): Promise<SessionInspection>;
}

const sessions = new WeakMap<object, NativeSessionAccess>();

/** @internal 仅由 Host 为原 Program conversation 和本地 SDK 句柄登记，不导出到包入口。 */
export function registerNativeSession(handle: object, access: NativeSessionAccess): void {
  if (sessions.has(handle)) throw new Error('REACTIVE_NATIVE_SESSION_ALREADY_REGISTERED');
  sessions.set(handle, access);
}

/** @internal 同一个句柄始终使用签发时的 Host，不信调用方提供的会话身份。 */
export function nativeSessionAccess(handle: object): NativeSessionAccess {
  const access = sessions.get(handle);
  if (!access) throw new Error('REACTIVE_NATIVE_SESSION_REQUIRED');
  access.signal.throwIfAborted();
  access.assertCurrent();
  return access;
}

/**
 * 订阅先于冷读取，按原生日志 seq 接合完整历史与实时事件；不创建第二个持久投影。
 * 句柄退出只停止观察，不取消 Job、模型调用或其他已经受理的执行。
 */
export function nativeSessionEvents(access: NativeSessionAccess): Observable<SessionEvent> {
  return defer(() => {
    access.signal.throwIfAborted(); access.assertCurrent();
    let last = -1;
    return access.liveAgent$.pipe(switchMap(() => nativeEventCut(access)), filter(event => {
      if (event.seq <= last) return false;
      last = event.seq;
      return true;
    }), takeUntil(fromEvent(access.signal, 'abort')));
  });
}

/** 一个实际 owner 的历史切面与实时通知；owner 变更由外层 switchMap 撤销整次连接。 */
function nativeEventCut(access: NativeSessionAccess): Observable<SessionEvent> {
  return new Observable(subscriber => {
    const controller = new AbortController();
    const signal = AbortSignal.any([access.signal, controller.signal]);
    let buffered: SessionEvent[] | undefined = [];
    const deliver = (event: SessionEvent) => {
      if (subscriber.closed) return;
      access.assertCurrent();
      subscriber.next(event);
    };
    let release: (() => void) | undefined;
    try {
      signal.throwIfAborted();
      access.assertCurrent();
      release = access.context.on('session/event', (session, event) => {
        if (session.id !== access.sessionId) return;
        try {
          if (buffered) buffered.push(event);
          else deliver(event);
        } catch (error) { subscriber.error(error); }
      });
      void access.inspect(signal).then(inspection => {
        if (subscriber.closed) return;
        signal.throwIfAborted();
        access.assertCurrent();
        if (inspection.meta.id !== access.sessionId) throw new Error('REACTIVE_NATIVE_SESSION_MISMATCH');
        for (const event of inspection.events) deliver(event);
        const pending = buffered!;
        // 交付期间观察者可以同步 append；先按顺序清空同一个 buffer 再切实时模式。
        for (let index = 0; index < pending.length; index++) deliver(pending[index]);
        buffered = undefined;
      }).catch(error => { if (!subscriber.closed) subscriber.error(error); });
    } catch (error) { subscriber.error(error); }
    return () => {
      release?.();
      controller.abort();
    };
  });
}

/** @internal 官方日志的当前切面及读取有效性；通知仅失效，不以通知内容充当权威。 */
export function nativeSessionInspections(access: NativeSessionAccess,
  relevant: (event: SessionEvent) => boolean): Observable<Observation<SessionInspection>> {
  return defer(() => {
    access.signal.throwIfAborted(); access.assertCurrent();
    const changes$ = new Observable<SessionEvent>(subscriber => access.context.on('session/event', (session, event) => {
      if (session.id === access.sessionId) subscriber.next(event);
    })).pipe(filter(relevant));
    // 先监听日志再初读；代际和相关日志变化都撤下旧ready，错误等待下一次正式失效恢复。
    return observeResource({ key$: access.liveAgent$, invalidations: () => changes$,
      read: async (_agent, reading) => {
        const signal = AbortSignal.any([reading, access.signal]);
        const inspection = await access.inspect(signal);
        signal.throwIfAborted(); access.assertCurrent();
        if (inspection.meta.id !== access.sessionId) throw new Error('REACTIVE_NATIVE_SESSION_MISMATCH');
        return inspection;
      },
    }).pipe(takeUntil(fromEvent(access.signal, 'abort')));
  });
}
