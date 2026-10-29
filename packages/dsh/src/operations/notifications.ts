import { operationNotificationSource, type OperationNotificationCleanup, type OperationNotificationDelivery, type OperationNotificationResult, type OperationNotificationSource, type OperationOwnerReference } from '@hanger-source/dsh-reactive/kernel';
import { serviceParticipant, type ServiceParticipantIdentity } from '../participation/identity.ts';
import type { OperationParticipation } from './participation.ts';

/** 只连接应用既有可靠来源；不拥有业务任务、通知队列或重投调度。 */
export class OperationNotifications {
    #lifetime = new AbortController();
    #connections: Promise<void>[] = [];
    #cleanups: OperationNotificationCleanup[] = [];
    #deliveries = new Set<Promise<OperationNotificationResult>>();
    #starting?: Promise<void>;
    #closing?: Promise<void>;
    #stopped = false;
    readonly #sources: readonly { readonly owner: OperationOwnerReference; readonly source: OperationNotificationSource }[];
    readonly #participant?: ServiceParticipantIdentity;

    constructor(owners: readonly OperationOwnerReference[], actorId: string | undefined, private readonly operations: OperationParticipation) {
        this.#sources = [...new Set(owners)].flatMap(owner => {
            const source = operationNotificationSource(owner);
            return source ? [{ owner, source }] : [];
        });
        if (this.#sources.length) this.#participant = serviceParticipant(actorId);
    }
    initialize(): Promise<void> {
        if (this.#starting) return this.#starting;
        if (this.#stopped) return Promise.reject(this.#lifetime.signal.reason);
        const completion = Promise.withResolvers<void>();
        this.#starting = completion.promise;
        this.#connections = this.#sources.map(({ owner, source }) => Promise.resolve().then(async () => {
            this.#lifetime.signal.throwIfAborted();
            const cleanup = await source.connect(Object.freeze({ signal: this.#lifetime.signal,
                deliver: (delivery: OperationNotificationDelivery) => this.deliver(owner, delivery) }));
            if (typeof cleanup !== 'function') throw new Error('REACTIVE_OPERATION_NOTIFICATION_CLEANUP_REQUIRED');
            this.#cleanups.push(cleanup);
        }));
        void Promise.all(this.#connections).then(() => {
            this.#lifetime.signal.throwIfAborted();
        }).catch(async error => {
            try { await this.close(error); }
            catch (cleanup) { throw new AggregateError([error, cleanup], 'REACTIVE_OPERATION_NOTIFICATIONS_STARTUP_FAILED'); }
            throw error;
        }).then(completion.resolve, completion.reject);
        return this.#starting;
    }
    private deliver(owner: OperationOwnerReference, delivery: OperationNotificationDelivery): Promise<OperationNotificationResult> {
        if (this.#stopped) return Promise.reject(this.#lifetime.signal.reason);
        // 先持有完成回执；来源的同步回调或 ACK 可以重入 Host 退出。
        const completion = Promise.withResolvers<OperationNotificationResult>();
        const work = completion.promise;
        this.#deliveries.add(work);
        void work.then(() => this.#deliveries.delete(work), () => this.#deliveries.delete(work));
        void (async () => {
            const { sessionId, followId, acknowledge } = delivery;
            if (typeof sessionId !== 'string' || !sessionId.trim() || typeof followId !== 'string' || !followId.trim()
                || typeof acknowledge !== 'function') throw new Error('REACTIVE_OPERATION_NOTIFICATION_DELIVERY_INVALID');
            const signal = this.#lifetime.signal;
            signal.throwIfAborted();
            const result = await this.operations.notifyOperation(sessionId, followId, { signal }, this.#participant!, owner);
            signal.throwIfAborted();
            if (result.status === 'accepted' || result.status === 'not-started' || result.status === 'unregistered') await acknowledge.call(delivery, result);
            return result;
        })().then(completion.resolve, completion.reject);
        return work;
    }
    close(reason: unknown = new Error('REACTIVE_HOST_DISPOSED')): Promise<void> {
        if (this.#closing) return this.#closing;
        const completion = Promise.withResolvers<void>();
        this.#closing = completion.promise;
        this.#stopped = true;
        this.#lifetime.abort(reason);
        void (async () => {
            await Promise.allSettled(this.#connections);
            while (this.#deliveries.size) await Promise.allSettled([...this.#deliveries]);
            await this.operations.settleNotifications();
            const cleanups = this.#cleanups;
            this.#cleanups = [];
            const stopped = await Promise.allSettled(cleanups.map(cleanup => Promise.resolve().then(cleanup)));
            const failures = stopped.flatMap(result => result.status === 'rejected' ? [result.reason] : []);
            if (failures.length) throw new AggregateError(failures, 'REACTIVE_OPERATION_NOTIFICATIONS_CLEANUP_FAILED');
        })().then(completion.resolve, completion.reject);
        return this.#closing;
    }
}
