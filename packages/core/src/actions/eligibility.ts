import { EmptyError, Subscriber } from 'rxjs';
import type { Observation } from '../resources/observation.ts';
import type { ResourceReader } from '../resources/reader.ts';

/** 当前业务前提，不替代本次调用的真实来源、原始输入和业务权限核验。 */
export type ActionEligibility = { readonly allowed: true } | { readonly allowed: false; readonly reason: string };

/** 事实尚不可用或业务前提不成立；已经撤销的准入不会随资格恢复而复活。 */
export class ActionEligibilityError extends Error {
  override readonly name = 'ActionEligibilityError';
  readonly reason: 'pending' | 'error' | 'denied';
  constructor(readonly actionName: string, readonly observation: Observation<ActionEligibility>) {
    const reason = observation.status === 'ready' ? 'denied' : observation.status;
    super('Action eligibility is unavailable: ' + actionName + (observation.status === 'ready' && !observation.value.allowed
      ? ': ' + observation.value.reason : ': ' + reason),
    observation.status === 'error' ? { cause: observation.error } : undefined);
    this.reason = reason;
  }
}

/** @internal 调用只借用事实连接，持续到真正进入业务函数；不拥有来源或业务取消权。 */
export function retainActionEligibility(actionName: string, source: ResourceReader<ActionEligibility>, signal: AbortSignal): {
  readonly ready: Promise<void>; readonly signal: AbortSignal; check(): void; release(): void;
} {
  const withdrawn = new AbortController();
  const ready = Promise.withResolvers<void>();
  // 无调用者 await 的同步失败也不产生未处理拒绝。
  void ready.promise.catch(() => {});
  let accepted = false, current: Observation<ActionEligibility> = { status: 'pending', generation: 0 };
  const refuse = (observation: Observation<ActionEligibility>) => {
    if (withdrawn.signal.aborted) return;
    const error = new ActionEligibilityError(actionName, observation);
    withdrawn.abort(error); ready.reject(error);
  };
  const subscriber = new Subscriber<Observation<ActionEligibility>>({
    next(observation) {
      current = observation;
      if (observation.status === 'ready' && observation.value.allowed) {
        if (!accepted) { accepted = true; ready.resolve(); }
      } else if (accepted || observation.status !== 'pending') refuse(observation);
    },
    error(error: unknown) { refuse({ status: 'error', generation: current.generation, error }); },
    complete() {
      if (!accepted) refuse({ status: 'error', generation: current.generation, error: new EmptyError() });
    },
  });
  const abort = () => { ready.reject(signal.reason); };
  signal.addEventListener('abort', abort, { once: true });
  try {
    signal.throwIfAborted();
    source.observations$.subscribe(subscriber);
  } catch (error) {
    subscriber.unsubscribe(); signal.removeEventListener('abort', abort); throw error;
  }
  return {
    ready: ready.promise, signal: withdrawn.signal,
    check() {
      signal.throwIfAborted(); withdrawn.signal.throwIfAborted();
      if (current.status !== 'ready' || !current.value.allowed) throw new ActionEligibilityError(actionName, current);
    },
    release() { signal.removeEventListener('abort', abort); subscriber.unsubscribe(); },
  };
}
