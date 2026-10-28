import { BehaviorSubject, firstValueFrom, takeWhile } from 'rxjs';
import { randomUUID } from '@deepseek-ai/dsh-util-crypto';
import type { InvocationActivity } from '../actions/contract.ts';
import type { CancellationOptions } from '../conversations/protocol.ts';
import { abortable } from '../reactivity/observable.ts';
import type { Observation } from '../resources/observation.ts';
import { ResourceReader } from '../resources/reader.ts';
import type { FileUploadInvocation, FileUploadValue } from './contract.ts';

/** @internal 宿主与远程 SDK 共享调用事实；具体权限、存储及 transport 仍由调用方拥有。 */
export function createFileUploadInvocation<Input>(options: {
  readonly sessionId: string;
  readonly prepare: (signal: AbortSignal) => Input | PromiseLike<Input>;
  readonly execute: (input: Input) => FileUploadValue | PromiseLike<FileUploadValue>;
  readonly preparationSignal: AbortSignal;
  readonly waitSignal: AbortSignal;
}): FileUploadInvocation {
  const { sessionId, prepare, execute, preparationSignal, waitSignal } = options;
  const id = randomUUID();
  const state = new BehaviorSubject<Observation<InvocationActivity<never>>>({
    status: 'ready', generation: 1, observedAt: Date.now(), value: { state: 'pending' },
  });
  const activity = new ResourceReader<InvocationActivity<never>>({
    peek: () => state.value,
    observations$: state.pipe(takeWhile(value => value.status === 'ready'
      && (value.value.state === 'pending' || value.value.state === 'running'), true)),
  });
  const publish = (value: InvocationActivity<never>) => state.next({
    status: 'ready', generation: state.value.generation + 1, observedAt: Date.now(), value,
  });
  let phase: 'input' | 'execute' = 'input';
  // 先交付调用对象，实际 owner 可在 prepare 或 execute 发生同步重入前登记结算。
  const result = Promise.resolve().then(async () => {
    const input = await firstValueFrom(abortable(() => prepare(preparationSignal), preparationSignal));
    preparationSignal.throwIfAborted();
    phase = 'execute';
    const dispatched = execute(input);
    // 派发之后才通知观察者；running 回调中的退等不能抢在真实入口之前取消它。
    publish({ state: 'running' });
    return dispatched;
  });
  // 观察同一个 Promise；消费者退等后仍保留真实结算与迟到活动。
  void result.then(() => publish({ state: 'returned' }), error => publish({ state: 'failed', phase, error }));
  return Object.freeze({
    id,
    source: Object.freeze({ kind: 'file-upload' as const, sessionId }),
    result, activity, activity$: activity.values$,
    wait(options: CancellationOptions = {}) {
      const signal = options.signal ? AbortSignal.any([waitSignal, options.signal]) : waitSignal;
      return firstValueFrom(abortable(() => result, signal));
    },
  });
}
