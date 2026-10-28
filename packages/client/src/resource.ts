import { RemoteSnapshotStream, type RemoteStreamOptions } from '@deepseek-ai/dsh-api-gateway/client';
import { Observable } from 'rxjs';
import { shareResource, type Observation, type ResourceReader } from '@hanger-source/dsh-reactive';
import type { ClientScope } from './scope.ts';

/** 查询消费者持有原生流；RxJS持有唯一快照，会话退出等待所有已退休连接真正释放。 */
export function remoteResource<T, Snapshot, Delta>(owner: ClientScope, options: {
  readonly name: string;
  readonly open: RemoteStreamOptions<Snapshot | Delta>['open'];
  readonly ended: RemoteStreamOptions<Snapshot | Delta>['ended'];
  isSnapshot(frame: Snapshot | Delta): frame is Snapshot;
  observation(frame: Snapshot | Delta, generation: number): Observation<T>;
}): ResourceReader<T> {
  const { context: ctx, signal } = owner;
  return shareResource(new Observable<Observation<T>>(subscriber => {
    owner.assertActive();
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
    const connection = owner.acquire(() => {
      const stream = new RemoteSnapshotStream(ctx.remote.$stream<Snapshot | Delta>({
        name: options.name, open: options.open, ended: options.ended, carrierFailed: invalidate,
      }), { name: options.name, isSnapshot: options.isSnapshot, replace: accept, update: accept,
        failed(error) {
          if (subscriber.closed) return;
          subscriber.next({ status: 'error', generation: ++generation, error });
          subscriber.complete();
        },
      });
      const reset = ctx.on('connection/reset', () => { invalidate(); stream.restart(); });
      return { stream, reset };
    }, async ({ stream, reset }) => { reset(); await stream.dispose(); }, options.name);
    subscriber.add(() => { void connection.release().catch(() => {}); });
    if (owner.active && !subscriber.closed) connection.value.stream.start();
  }), { signal });
}
