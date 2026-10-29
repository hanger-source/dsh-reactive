import { symbols, type Context } from '@deepseek-ai/cordis';
import type SessionController from '@deepseek-ai/dsh-api-session-controller';
import type SessionQuery from '@deepseek-ai/dsh-session-query';
import { SessionLogOffset, type Session, type SessionEvent } from '@deepseek-ai/dsh-session';
import { ReadableStream, type ReadableStreamDefaultController } from 'node:stream/web';
import { firstValueFrom } from 'rxjs';
import { abortable, z, type CancellationOptions } from '@hanger-source/dsh-reactive';
import { Scope } from '@hanger-source/dsh-reactive/kernel';
import type { SessionJournal, ProgramJournalEntry, ProgramJournalFrame, ProgramJournalPage,
  ProgramJournalPageQuery, ProgramJournalWindow } from '@hanger-source/dsh-reactive/journal';
import { NativeCapabilityOwner, ownNativeValue, type NativeCapability, type NativeSessionSource } from './native-capability.ts';
import { nativeSessionAccess, type NativeSessionAccess } from './conversations/session-access.ts';

export type { SessionJournal, ProgramJournalEvent, ProgramJournalEntry, ProgramJournalPage,
  ProgramJournalWindow, ProgramJournalPageQuery, ProgramJournalFrame } from '@hanger-source/dsh-reactive/journal';

export interface NativeJournalOptions { readonly service: 'provided'; }
export type NativeSessionJournal = SessionJournal;
export interface NativeJournal extends NativeCapability {
  /** 日志读侧不使用home；仍可作为普通NativeCapability参与原装配。 */
  readonly install: (ctx: Context, home?: string) => Promise<void>;
  forSession(source: NativeSessionSource): NativeSessionJournal;
}

type Providers = { readonly controller: SessionController; readonly query: SessionQuery };
const original = (value: object | undefined): unknown => value && Reflect.get(value, symbols.original);
const tailLimit = 1024 * 1024;

class SessionJournalReader implements NativeSessionJournal {
  readonly reader: NativeSessionJournal = Object.freeze({ page: this.page.bind(this), follow: this.follow.bind(this) });
  readonly scope: Scope;
  #pending = new Set<Promise<unknown>>();
  #followers = new Set<() => void>();
  #cleanupFailures = new Set<unknown>();
  #closing?: Promise<void>;
  constructor(private readonly owner: NativeCapabilityOwner, private readonly access: NativeSessionAccess,
    private readonly ready: Promise<void>, private readonly providers: Providers, private readonly released: () => void) {
    this.scope = owner.scope.child();
    const stop = () => this.scope.dispose(access.signal.reason);
    access.signal.addEventListener('abort', stop, { once: true });
    this.scope.own(() => access.signal.removeEventListener('abort', stop));
    const logger = access.context.logger;
    this.scope.signal.addEventListener('abort', () => {
      void this.dispose().catch(error => { owner.recordCleanupFailure(error); logger.error(error); });
    }, { once: true });
    if (access.signal.aborted) stop();
    access.ownReading(() => this.dispose());
  }
  get disposed(): boolean { return this.scope.disposed || this.#closing !== undefined; }
  private assertCurrent(): void {
    this.scope.assertActive(); this.access.assertCurrent();
    const controller: SessionController | undefined = this.access.context.get('sessionController');
    const query: SessionQuery | undefined = this.access.context.get('sessionQuery');
    if (!controller || !query || original(controller) !== original(this.providers.controller) || original(query) !== original(this.providers.query)) {
      const error = new Error('REACTIVE_NATIVE_JOURNAL_SOURCE_CHANGED');
      this.scope.dispose(error); throw error;
    }
  }
  private signal(options: CancellationOptions): AbortSignal {
    this.assertCurrent();
    const signal = options.signal ? AbortSignal.any([this.scope.signal, options.signal]) : this.scope.signal;
    signal.throwIfAborted(); return signal;
  }
  private async available(signal: AbortSignal): Promise<void> {
    await firstValueFrom(abortable(() => this.ready, signal));
    signal.throwIfAborted(); this.assertCurrent();
    this.owner.session(this.access, 'sessionController'); this.owner.session(this.access, 'sessionQuery');
  }
  /** 真实工作先登记，消费者取消只退等，原page/observation仍可由dispose加入。 */
  private track<T>(run: () => PromiseLike<T> | T): Promise<T> {
    const completion = Promise.withResolvers<T>();
    this.#pending.add(completion.promise);
    void completion.promise.then(() => this.#pending.delete(completion.promise), () => this.#pending.delete(completion.promise));
    try { completion.resolve(run()); } catch (error) { completion.reject(error); }
    return completion.promise;
  }
  async page(query: ProgramJournalPageQuery, options: CancellationOptions = {}): Promise<ProgramJournalPage> {
    const selected = structuredClone(query), signal = this.signal(options);
    await this.available(signal);
    signal.throwIfAborted(); this.assertCurrent();
    const reading = this.track(() => this.providers.controller.page({ ...selected, address: this.access.journalAddress }, signal));
    const page = await firstValueFrom(abortable(() => reading, signal));
    signal.throwIfAborted(); this.assertCurrent();
    return ownNativeValue(page);
  }
  follow(window: ProgramJournalWindow = {}, options: CancellationOptions = {}): AsyncIterable<ProgramJournalFrame> {
    const selected = structuredClone(window), signal = this.signal(options), reader = this;
    const controller = new AbortController(), reading = AbortSignal.any([signal, controller.signal]);
    let returnReason: Error | undefined;
    const returned = (error: unknown) => returnReason !== undefined && error === returnReason && reading.reason === returnReason;
    const iterator = (async function* (): AsyncGenerator<ProgramJournalFrame, void> {
      try { await reader.available(reading); }
      catch (error) { if (returned(error)) return; throw error; }
      const cleanupFailures = new Set<unknown>();
      const cleanupFailed = (error: unknown) => { cleanupFailures.add(error); reader.#cleanupFailures.add(error); };
      let tailController!: ReadableStreamDefaultController<ProgramJournalEntry>;
      let cut: number | undefined, last = -1, lastEnqueued = -1, openingSession: Session | undefined, tailSession: Session | undefined;
      let ended = false;
      const tail = new ReadableStream<ProgramJournalEntry>({
        start(value) { tailController = value; },
        cancel() { stop(); },
      }, { highWaterMark: tailLimit, size: entry => Buffer.byteLength(JSON.stringify(entry)) });
      const fail = (error: unknown) => {
        if (ended) return;
        ended = true; tailController.error(error); controller.abort(error);
      };
      const append = (event: SessionEvent): void => {
        if (ended || cut === undefined || event.seq <= Math.max(cut, lastEnqueued)) return;
        try {
          reading.throwIfAborted(); reader.assertCurrent();
          const entry: ProgramJournalEntry = ownNativeValue({ type: 'event', event: { ...event, data: z.json().parse(event.data) } });
          if ((tailController.desiredSize ?? 0) < Buffer.byteLength(JSON.stringify(entry)))
            throw new Error('REACTIVE_JOURNAL_BUFFER_OVERFLOW');
          tailController.enqueue(entry);
          lastEnqueued = event.seq;
        } catch (error) { fail(error); }
      };
      const seed = (session: Session): void => {
        if (ended) return;
        if (cut === undefined) { openingSession = session; return; }
        if (tailSession === session) return;
        tailSession = session;
        // append在发布session/event前已经进入原生日志；首次事件也可补齐created前的seed。
        for (const event of session.snapshotEvents(SessionLogOffset(Math.max(cut, lastEnqueued) + 1))) append(event);
      };
      let stopEvent: (() => void) | undefined, stopCreated: (() => void) | undefined;
      const stop = () => {
        const event = stopEvent, created = stopCreated;
        stopEvent = undefined; stopCreated = undefined;
        try { event?.(); }
        catch (error) { cleanupFailed(error); fail(error); }
        try { created?.(); }
        catch (error) { cleanupFailed(error); fail(error); }
        controller.abort(); openingSession = undefined;
      };
      const aborted = () => { fail(reading.reason); stop(); };
      reader.#followers.add(stop);
      reading.addEventListener('abort', aborted, { once: true });
      let failure: { readonly error: unknown } | undefined;
      try {
        reading.throwIfAborted(); reader.assertCurrent();
        stopEvent = reader.access.context.on('session/event', (session, event) => {
          if (session.id !== reader.access.sessionId) return;
          seed(session); append(event);
        }, { global: true });
        stopCreated = reader.access.context.on('session/created', session => {
          if (session.id !== reader.access.sessionId || ended) return;
          seed(session);
        }, { global: true });
        reading.throwIfAborted(); reader.assertCurrent();
        const opening = reader.track(async () => {
          const observation = await reader.providers.query.observeSession(reader.access.sessionId, { signal: reading, projectionMode: 'none' });
          try {
            reading.throwIfAborted(); reader.assertCurrent();
            if (observation.header.id !== reader.access.sessionId) throw new Error('REACTIVE_NATIVE_SESSION_MISMATCH');
            cut = observation.cursor;
          } finally {
            try { observation[Symbol.dispose](); }
            catch (error) { cleanupFailed(error); throw error; }
          }
          reading.throwIfAborted(); reader.assertCurrent();
          if (openingSession) {
            seed(openingSession);
            openingSession = undefined;
          }
          reading.throwIfAborted(); reader.assertCurrent();
          return reader.providers.controller.page({ ...selected, address: reader.access.journalAddress, throughSeq: cut! }, reading);
        });
        // 关闭首先结束等待；opening仍在pending中保留真实服务的清理责任。
        const page = await firstValueFrom(abortable(() => opening, reading));
        reading.throwIfAborted(); reader.assertCurrent();
        last = cut!;
        yield { type: 'opened', cursor: cut!, page: ownNativeValue(page) };
        for await (const entry of tail) {
          reading.throwIfAborted(); reader.assertCurrent();
          if (entry.event.seq <= last) continue;
          if (entry.event.seq !== last + 1) throw new Error('REACTIVE_JOURNAL_EVENT_GAP');
          last = entry.event.seq;
          yield { type: 'entry', entry };
        }
        reading.throwIfAborted();
      } catch (error) {
        if (!returned(error)) failure = { error };
      } finally {
        stop(); reading.removeEventListener('abort', aborted); reader.#followers.delete(stop);
        const failures = new Set([...(failure ? [failure.error] : []), ...cleanupFailures]);
        if (failures.size === 1) throw [...failures][0];
        if (failures.size) throw new AggregateError([...failures], 'REACTIVE_NATIVE_JOURNAL_FOLLOW_FAILED');
      }
    })();
    // async generator 的return排在pending next之后；先撤标准来源才能及时结束开场或尾部等待。
    return { [Symbol.asyncIterator]() { return {
      next: () => iterator.next(),
      return: () => {
        returnReason ??= new Error('REACTIVE_NATIVE_JOURNAL_FOLLOW_RETURNED');
        controller.abort(returnReason);
        return iterator.return();
      },
      throw: (error: unknown) => { controller.abort(error); return iterator.throw(error); },
    }; } };
  }
  dispose(): Promise<void> {
    if (this.#closing) return this.#closing;
    const completion = Promise.withResolvers<void>(); this.#closing = completion.promise;
    try { this.scope.dispose(); } catch (error) { this.#cleanupFailures.add(error); }
    for (const stop of this.#followers) stop();
    void Promise.resolve().then(async () => {
      try {
        while (this.#pending.size) await Promise.allSettled([...this.#pending]);
        if (this.#cleanupFailures.size === 1) throw [...this.#cleanupFailures][0];
        if (this.#cleanupFailures.size) throw new AggregateError([...this.#cleanupFailures], 'REACTIVE_NATIVE_JOURNAL_DISPOSAL_FAILED');
      } finally { this.released(); }
    }).then(completion.resolve, completion.reject);
    return completion.promise;
  }
}

/** 只借正式Controller/Query；安装不创建Gateway、身份策略、模型或Agent。 */
export function createNativeJournal(options: NativeJournalOptions): NativeJournal {
  if (options.service !== 'provided') throw new Error('REACTIVE_NATIVE_JOURNAL_PROVIDED_REQUIRED');
  const active = new Set<SessionJournalReader>(), sessions = new WeakMap<object, SessionJournalReader>();
  let providers: Providers | undefined, ready: Promise<void> | undefined, closing: Promise<void> | undefined;
  const owner: NativeCapabilityOwner = new NativeCapabilityOwner('reactive-native-journal', ['sessionController', 'sessionQuery'], function* (ctx) {
    if (!ctx.get('sessionController') || !ctx.get('sessionQuery')) throw new Error('REACTIVE_NATIVE_JOURNAL_REQUIRED');
    yield owner.cleanup(async () => {
      const results = await Promise.allSettled([...active].map(reader => reader.dispose()));
      for (const result of results) if (result.status === 'rejected') owner.recordCleanupFailure(result.reason);
    });
  });
  return Object.freeze({
    install(ctx: Context): Promise<void> {
      if (ready) throw new Error('REACTIVE_NATIVE_CAPABILITY_ALREADY_INSTALLED');
      const controller: SessionController | undefined = ctx.get('sessionController'), query: SessionQuery | undefined = ctx.get('sessionQuery');
      if (!controller || !query) throw new Error('REACTIVE_NATIVE_JOURNAL_REQUIRED');
      providers = { controller, query };
      const completion = Promise.withResolvers<void>(); ready = completion.promise;
      void ready.catch(() => {});
      try { Promise.resolve(owner.install(ctx, '')).then(completion.resolve, completion.reject); }
      catch (error) { completion.reject(error); }
      return ready;
    },
    forSession(source: NativeSessionSource): NativeSessionJournal {
      const access = nativeSessionAccess(source); owner.scope.assertActive();
      if (!providers || !ready) throw new Error('REACTIVE_NATIVE_CAPABILITY_NOT_ACTIVE');
      if (original(access.context.get('sessionController')) !== original(providers.controller)
        || original(access.context.get('sessionQuery')) !== original(providers.query)) throw new Error('REACTIVE_NATIVE_CAPABILITY_HOST_MISMATCH');
      let current = sessions.get(source);
      if (!current || current.disposed) {
        const created = new SessionJournalReader(owner, access, ready, providers, () => active.delete(created));
        current = created; sessions.set(source, current); active.add(current);
      }
      return current.reader;
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
