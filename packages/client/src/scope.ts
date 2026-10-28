import type { Context } from '@deepseek-ai/cordis';
import { firstValueFrom } from 'rxjs';
import { abortable } from '@hanger-source/dsh-reactive';

export interface ClientLease<T> {
  readonly value: T;
  release(): Promise<void>;
}

/** 原生Fiber拥有实际清理；signal表达立即撤销，close等待同一Fiber静止并交付退出错误。 */
export interface ClientScope {
  readonly context: Context;
  readonly signal: AbortSignal;
  readonly ready: Promise<void>;
  readonly active: boolean;
  assertActive(): void;
  run<T>(call: (signal: AbortSignal) => PromiseLike<T>, signal?: AbortSignal): Promise<T>;
  acquire<T>(create: () => T, cleanup: (value: T) => void | PromiseLike<void>, label: string): ClientLease<T>;
  close(): Promise<void>;
}

export function createClientScope(parent: Context, name: string, reason: Error, owner?: ClientScope): ClientScope {
  const controller = new AbortController();
  const signal = owner ? AbortSignal.any([owner.signal, controller.signal]) : controller.signal;
  const failures = new Set<unknown>();
  let closing: Promise<void> | undefined;
  const installation = parent.plugin({ name, inject: ['connection', 'remote', 'remote.reactivePrograms'], apply(ctx: Context) {
    ctx.effect(() => () => controller.abort(reason), name + ' revoke');
  } });
  const context = installation.ctx;
  const ready = (async () => {
    try {
      const fiber = await installation;
      if (fiber.state !== 2) throw new Error('REACTIVE_CLIENT_SCOPE_NOT_ACTIVE:' + name);
      signal.throwIfAborted();
    } catch (error) { await installation.dispose(); throw error; }
  })();
  const failure = () => {
    if (failures.size === 1) throw [...failures][0];
    if (failures.size) throw new AggregateError([...failures], 'REACTIVE_CLIENT_SCOPE_CLOSE_FAILED:' + name);
  };
  const scope: ClientScope = {
    context, signal, ready,
    get active() {
      if (owner) void owner.active;
      // FAILED、DISPOSED和UNLOADING均已撤销使用权；LOADING父插件可以安装自己的子scope。
      const ended = (state: number) => state === 3 || state === 4 || state === 5;
      if (context.fiber.state !== 2 || ended(parent.fiber.state) || ended(parent.root.fiber.state)) controller.abort(reason);
      return !signal.aborted;
    },
    assertActive() { void scope.active; signal.throwIfAborted(); },
    run(call, cancellation = signal) {
      scope.assertActive();
      const pending = cancellation === signal ? signal : AbortSignal.any([signal, cancellation]);
      pending.throwIfAborted();
      // 只结束消费者的等待，不将已受理业务纳入SDK的清理责任。
      return firstValueFrom(abortable(call, pending));
    },
    acquire(create, cleanup, label) {
      scope.assertActive();
      const completion = Promise.withResolvers<void>();
      // 调用方可以稍后等待；失败仍由本owner保存并在close时交付。
      void completion.promise.catch(() => {});
      let value!: ReturnType<typeof create>;
      // effect在进入create之前已登记，构造中的重入退出由原生setup barrier等待。
      const dispose = context.effect(() => {
        value = create();
        return async () => {
          // 原生父Fiber直接卸载时，先撤销本owner，再进入任一子资源的释放。
          if (context.fiber.state !== 2) controller.abort(reason);
          try { await cleanup(value); completion.resolve(); }
          catch (error) { failures.add(error); completion.reject(error); throw error; }
        };
      }, label);
      const release = (): Promise<void> => {
        // 原生公开disposer只调用一次；completion还允许加入由外层已经开始的退出。
        void Promise.resolve(dispose()).catch(error => { failures.add(error); completion.reject(error); });
        return completion.promise;
      };
      // 构造中撤销时仍交付已取得的对象；它已归原生effect释放，不丢失已受理的身份。
      return { value, release };
    },
    close() {
      if (closing) return closing;
      const completion = Promise.withResolvers<void>();
      closing = completion.promise;
      controller.abort(reason);
      void (async () => {
        // 公开disposer是single-shot；await Fiber还会加入原生owner已开始的卸载。
        try { await installation.dispose(); await installation; }
        catch (error) { failures.add(error); }
        failure();
      })().then(completion.resolve, completion.reject);
      return closing;
    },
  };
  return scope;
}
