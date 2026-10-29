import { symbols, type Context } from '@deepseek-ai/cordis';
import type SessionController from '@deepseek-ai/dsh-api-session-controller';
import { firstValueFrom } from 'rxjs';
import { abortable, type CancellationOptions } from '@hanger-source/dsh-reactive';
import { Scope } from '@hanger-source/dsh-reactive/kernel';
import type { AttachmentIdType, SessionAttachments, SessionImageAttachment } from '@hanger-source/dsh-reactive/attachments';
import { NativeCapabilityOwner, ownNativeValue, type NativeCapability, type NativeSessionSource } from './native-capability.ts';
import { nativeSessionAccess, type NativeSessionAccess } from './conversations/session-access.ts';

export type { AttachmentIdType, ImageAttachmentRef, ImageMediaType, SessionAttachments, SessionImageAttachment } from '@hanger-source/dsh-reactive/attachments';
export { SessionAttachmentsUnavailableError } from '@hanger-source/dsh-reactive/attachments';

export interface NativeAttachmentsOptions { readonly service: 'provided'; }
export type NativeSessionAttachments = SessionAttachments;
export interface NativeAttachments extends NativeCapability {
  readonly install: (ctx: Context, home?: string) => Promise<void>;
  forSession(source: NativeSessionSource): NativeSessionAttachments;
}

const original = (value: object | undefined): unknown => value && Reflect.get(value, symbols.original);

class SessionAttachmentsReader implements NativeSessionAttachments {
  readonly reader: NativeSessionAttachments = Object.freeze({ readImage: this.readImage.bind(this) });
  readonly scope: Scope;
  #pending = new Set<Promise<unknown>>();
  #closing?: Promise<void>;
  #cleanupFailures = new Set<unknown>();
  constructor(private readonly owner: NativeCapabilityOwner, private readonly access: NativeSessionAccess,
    private readonly ready: Promise<void>, private readonly controller: SessionController, private readonly released: () => void) {
    this.scope = owner.scope.child();
    const stop = () => this.scope.dispose(access.signal.reason);
    access.signal.addEventListener('abort', stop, { once: true });
    this.scope.own(() => access.signal.removeEventListener('abort', stop));
    this.scope.signal.addEventListener('abort', () => {
      void this.dispose().catch(error => { owner.recordCleanupFailure(error); access.context.logger.error(error); });
    }, { once: true });
    if (access.signal.aborted) stop();
    access.ownReading(() => this.dispose());
  }
  private assertCurrent(): void {
    this.scope.assertActive(); this.access.signal.throwIfAborted(); this.access.assertCurrent();
    const controller: SessionController | undefined = this.access.context.get('sessionController');
    if (!controller || original(controller) !== original(this.controller)) {
      const error = new Error('REACTIVE_NATIVE_ATTACHMENTS_SOURCE_CHANGED');
      this.scope.dispose(error); throw error;
    }
  }
  private track<T>(run: () => PromiseLike<T> | T): Promise<T> {
    const completion = Promise.withResolvers<T>();
    this.#pending.add(completion.promise);
    void completion.promise.then(() => this.#pending.delete(completion.promise), () => this.#pending.delete(completion.promise));
    try { completion.resolve(run()); } catch (error) { completion.reject(error); }
    return completion.promise;
  }
  async readImage(attachmentId: AttachmentIdType, options: CancellationOptions = {}): Promise<SessionImageAttachment> {
    this.assertCurrent();
    const signal = options.signal ? AbortSignal.any([this.scope.signal, options.signal]) : this.scope.signal;
    signal.throwIfAborted();
    await firstValueFrom(abortable(() => this.ready, signal));
    signal.throwIfAborted(); this.assertCurrent();
    this.owner.session(this.access, 'sessionController');
    // Controller从当前会话正式日志核验引用；不借全局附件存储绕过会话授权。
    const reading = this.track(async () => {
      const value = await this.controller.attachment({ sessionId: this.access.sessionId, attachmentId });
      this.assertCurrent(); this.owner.session(this.access, 'sessionController');
      return Object.freeze({ attachment: ownNativeValue(value.attachment), data: Uint8Array.from(Buffer.from(value.data, 'base64')) });
    });
    const value = await firstValueFrom(abortable(() => reading, signal));
    signal.throwIfAborted(); this.assertCurrent();
    this.owner.session(this.access, 'sessionController');
    return value;
  }
  dispose(): Promise<void> {
    if (this.#closing) return this.#closing;
    const completion = Promise.withResolvers<void>(); this.#closing = completion.promise;
    try { this.scope.dispose(); } catch (error) { this.#cleanupFailures.add(error); }
    void Promise.resolve().then(async () => {
      try {
        while (this.#pending.size) await Promise.allSettled([...this.#pending]);
        if (this.#cleanupFailures.size === 1) throw [...this.#cleanupFailures][0];
        if (this.#cleanupFailures.size) throw new AggregateError([...this.#cleanupFailures], 'REACTIVE_NATIVE_ATTACHMENTS_DISPOSAL_FAILED');
      } finally { this.released(); }
    }).then(completion.resolve, completion.reject);
    return completion.promise;
  }
}

/** 只借已安装的正式Controller；不安装Agent、Gateway、模型或附件存储。 */
export function createNativeAttachments(options: NativeAttachmentsOptions): NativeAttachments {
  if (options.service !== 'provided') throw new Error('REACTIVE_NATIVE_ATTACHMENTS_PROVIDED_REQUIRED');
  const active = new Set<SessionAttachmentsReader>(), sessions = new WeakMap<object, SessionAttachmentsReader>();
  let controller: SessionController | undefined, ready: Promise<void> | undefined, closing: Promise<void> | undefined;
  const owner: NativeCapabilityOwner = new NativeCapabilityOwner('reactive-native-attachments', ['sessionController'], function* (ctx) {
    if (!ctx.get('sessionController')) throw new Error('REACTIVE_NATIVE_ATTACHMENTS_REQUIRED');
    yield owner.cleanup(async () => {
      const results = await Promise.allSettled([...active].map(reader => reader.dispose()));
      for (const result of results) if (result.status === 'rejected') owner.recordCleanupFailure(result.reason);
    });
  });
  return Object.freeze({
    install(ctx: Context): Promise<void> {
      if (ready) throw new Error('REACTIVE_NATIVE_CAPABILITY_ALREADY_INSTALLED');
      controller = ctx.get('sessionController');
      if (!controller) throw new Error('REACTIVE_NATIVE_ATTACHMENTS_REQUIRED');
      const completion = Promise.withResolvers<void>(); ready = completion.promise;
      void ready.catch(() => {});
      try { Promise.resolve(owner.install(ctx, '')).then(completion.resolve, completion.reject); }
      catch (error) { completion.reject(error); }
      return ready;
    },
    forSession(source: NativeSessionSource): NativeSessionAttachments {
      const access = nativeSessionAccess(source); owner.scope.assertActive();
      if (!controller || !ready) throw new Error('REACTIVE_NATIVE_CAPABILITY_NOT_ACTIVE');
      if (original(access.context.get('sessionController')) !== original(controller)) throw new Error('REACTIVE_NATIVE_CAPABILITY_HOST_MISMATCH');
      const cached = sessions.get(source);
      if (cached) return cached.reader;
      const reader = new SessionAttachmentsReader(owner, access, ready, controller, () => active.delete(reader));
      sessions.set(source, reader); active.add(reader);
      return reader.reader;
    },
    dispose(): Promise<void> {
      if (closing) return closing;
      const completion = Promise.withResolvers<void>(); closing = completion.promise;
      void (async () => {
        const result = await Promise.allSettled([owner.dispose(), ...(ready ? [ready] : [])]);
        if (result[0]!.status === 'rejected') throw result[0]!.reason;
      })().then(completion.resolve, completion.reject);
      return closing;
    },
  });
}
