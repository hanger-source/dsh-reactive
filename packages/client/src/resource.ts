import type { Context } from '@deepseek-ai/cordis';
import { RemoteSnapshotStream, type RemoteStreamOptions } from '@deepseek-ai/dsh-api-gateway/client';
import { Observable } from 'rxjs';
import { shareResource, type Observation, type ResourceReader } from '@hanger-source/dsh-reactive';

export interface RemoteResourceConnection<T> {
  readonly reader: ResourceReader<T>;
  close(): Promise<void>;
}

/** 查询消费者持有原生流；RxJS持有唯一快照，会话退出等待所有已退休连接真正释放。 */
export function remoteResource<T, Snapshot, Delta>(ctx: Context, owner: AbortSignal, options: {
  readonly name: string;
  readonly open: RemoteStreamOptions<Snapshot | Delta>['open'];
  readonly ended: RemoteStreamOptions<Snapshot | Delta>['ended'];
  isSnapshot(frame: Snapshot | Delta): frame is Snapshot;
  observation(frame: Snapshot | Delta, generation: number): Observation<T>;
}): RemoteResourceConnection<T> {
  const controller = new AbortController();
  const signal = AbortSignal.any([owner, controller.signal]);
  const retiring = new Set<Promise<void>>();
  const failures: unknown[] = [];
  let closing: Promise<void> | undefined;
  const reader = shareResource(new Observable<Observation<T>>(subscriber => {
    signal.throwIfAborted();
    let generation = 0;
    const invalidate = () => {
      if (!subscriber.closed) subscriber.next({ status: 'pending', generation: ++generation });
    };
    const accept = (frame: Snapshot | Delta) => {
      if (subscriber.closed) return;
      const observation = options.observation(frame, generation + 1);
      generation = observation.generation;
      subscriber.next(observation);
    };
    const stream = new RemoteSnapshotStream(ctx.remote.$stream<Snapshot | Delta>({
      name: options.name, open: options.open, ended: options.ended, carrierFailed: invalidate,
    }), { name: options.name, isSnapshot: options.isSnapshot, replace: accept, update: accept,
      failed(error) {
        if (subscriber.closed) return;
        subscriber.next({ status: 'error', generation: ++generation, error });
        subscriber.complete();
      },
    });
    const reset = ctx.on('connection/reset', invalidate);
    subscriber.add(() => {
      reset();
      const retirement = stream.dispose().then(() => { retiring.delete(retirement); }, error => {
        retiring.delete(retirement); failures.push(error);
      });
      retiring.add(retirement);
    });
    if (!subscriber.closed) stream.start();
  }), { signal });
  return { reader, close() {
    return closing ??= (async () => {
      controller.abort(new Error('REACTIVE_REMOTE_RESOURCE_DISPOSED'));
      await Promise.all([...retiring]);
      if (failures.length === 1) throw failures[0];
      if (failures.length) throw new AggregateError(failures, 'REACTIVE_REMOTE_RESOURCE_DISPOSAL_FAILED');
    })();
  } };
}
