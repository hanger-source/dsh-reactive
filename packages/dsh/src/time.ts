import * as TimeContext from '@deepseek-ai/dsh-time-context';
import type { SessionEvent } from '@deepseek-ai/dsh-session';
import { filter, map, type Observable } from 'rxjs';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';
import { NativeCapabilityOwner, nativeObservation, nativeObservationScope, ownNativeValue, type NativeCapability, type NativeSessionSource } from './native-capability.ts';
import { nativeSessionAccess, nativeSessionEvents, type NativeSessionAccess } from './conversations/session-access.ts';

export interface NativeClockOptions extends TimeContext.Config {
  /** 明确的显示区域；不会自动成为商家业务命令的 timezone 参数或权限。 */
  readonly timeZone: string;
}
export type NativeClockReading = SessionEvent<'user/message'>;

export interface NativeSessionClock {
  readonly readings$: Observable<NativeClockReading>;
}

class SessionClock implements NativeSessionClock {
  /** 原生日志中的实际采样，保留 seq/time/完整 source 和正文；没有额外定时器。 */
  readonly readings$: Observable<NativeClockReading>;
  #scope: Scope;
  constructor(owner: NativeCapabilityOwner, access: NativeSessionAccess) {
    this.#scope = nativeObservationScope(owner, access);
    this.readings$ = nativeObservation(this.#scope, nativeSessionEvents(access).pipe(filter((event): event is NativeClockReading =>
      event.type === 'user/message' && event.data.source.kind === 'time-context'), map(ownNativeValue)));
  }
}

export interface NativeClock extends NativeCapability {
  readonly timeZone: string;
  /** 显式 zone 的普通格式化；不把格式化值冒充一次原生 pre-step 采样。 */
  format(instant: Date | number, options?: Omit<Intl.DateTimeFormatOptions, 'timeZone'>): string;
  forSession(session: NativeSessionSource): NativeSessionClock;
}

export function createNativeClock(options: NativeClockOptions): NativeClock {
  if (!options.timeZone.trim()) throw new Error('REACTIVE_TIME_ZONE_REQUIRED');
  const selected = structuredClone(options);
  const timeZone = new Intl.DateTimeFormat('en', { timeZone: selected.timeZone }).resolvedOptions().timeZone;
  const sessions = new WeakMap<object, NativeSessionClock>();
  const owner = new NativeCapabilityOwner('reactive-native-clock', ['sessionProjections'], function* (ctx) {
    yield ctx.plugin(TimeContext, { ...selected, timeZone });
  });
  return Object.freeze({ install: owner.install, dispose: () => owner.dispose(), timeZone,
    format(instant: Date | number, format: Omit<Intl.DateTimeFormatOptions, 'timeZone'> = {}) {
      return new Intl.DateTimeFormat('zh-CN', { ...format, timeZone }).format(instant);
    },
    forSession(session: NativeSessionSource) {
      const access = nativeSessionAccess(session); owner.session(access, 'sessionProjections');
      let current = sessions.get(session);
      if (!current) {
        const reader = new SessionClock(owner, access);
        current = Object.freeze({ readings$: reader.readings$ });
        sessions.set(session, current);
      }
      return current;
    },
  });
}
