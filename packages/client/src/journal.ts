import { RemoteJournalStream, type RemoteJournalFrame, type RemoteJournalStreamOptions } from '@deepseek-ai/dsh-api-gateway/client';
import { RemoteError } from '@deepseek-ai/dsh-typert-protocol';
import { ConversationAccessRevokedError, SessionJournalUnavailableError, type CancellationOptions, type ProgramJournalEntry, type ProgramJournalPage, type ProgramJournalPageQuery,
  type ProgramJournalWindow, type ProgramJournalFrame, type SessionJournal } from '@hanger-source/dsh-reactive';
import type { ProgramSessionAddress } from './contracts/types.ts';
import { decodeProgramFailure } from './contracts/errors.ts';
import { revokeConversationAccess, type ClientScope } from './scope.ts';

export type ProgramJournalRequest = Omit<ProgramJournalPageQuery, 'throughSeq'>;
export type ProgramJournalOptions = Pick<RemoteJournalStreamOptions<ProgramJournalPage, ProgramJournalEntry, number>, 'publish' | 'failed' | 'carrierFailed'>;

function journalFailure(owner: ClientScope, error: unknown): unknown {
  if (error instanceof RemoteError) {
    const decoded = decodeProgramFailure(error);
    // 已是正式RemoteError的普通流失败保原对象；明确的core领域错误沿canonical类型交付。
    if (!(decoded instanceof RemoteError)) error = decoded;
  }
  return revokeConversationAccess(owner, error);
}

/** 官方RemoteStream只透传正式协议错误；这一项明确领域拒绝不能在其中变成gateway/internal。 */
function journalProtocolFailure(error: unknown): unknown {
  return error instanceof SessionJournalUnavailableError
    ? new RemoteError('reactive/journal-unavailable', error.message, { sessionId: error.sessionId }, { cause: error })
    : error;
}

/** 当前SDK只拥有本端读取与流；原始窗口不重连、不修补，Host Controller仍决定历史事实。 */
export function remoteJournal(owner: ClientScope, address: ProgramSessionAddress): SessionJournal {
  const remote = owner.context.remote.reactivePrograms;
  return Object.freeze({
    page(query: ProgramJournalPageQuery, options: CancellationOptions = {}): Promise<ProgramJournalPage> {
      owner.assertActive();
      const signal = options.signal ? AbortSignal.any([owner.signal, options.signal]) : owner.signal;
      signal.throwIfAborted();
      const selected = structuredClone(query);
      const lease = owner.acquire(() => remote.journalPage({ ...selected, ...address }, signal), async pending => {
        // 普通读取拒绝归本次调用；SDK退出仅加入真实传输结算。
        await pending.then(() => {}, () => {});
      }, 'program-journal-page:' + address.sessionId);
      const release = () => { void lease.release().catch(() => {}); };
      void lease.value.then(release, release);
      return owner.run(async () => {
        const result = await lease.value;
        signal.throwIfAborted(); owner.assertActive();
        if (!result.ok) throw revokeConversationAccess(owner, decodeProgramFailure(result.error));
        return result.value;
      }, signal);
    },
    follow(window: ProgramJournalWindow = {}, options: CancellationOptions = {}): AsyncIterable<ProgramJournalFrame> {
      owner.assertActive();
      const signal = options.signal ? AbortSignal.any([owner.signal, options.signal]) : owner.signal;
      signal.throwIfAborted();
      const selected = structuredClone(window);
      return (async function* () {
        owner.assertActive(); signal.throwIfAborted();
        const lease = owner.acquire(() => remote.journal({ ...selected, ...address }, signal)[Symbol.asyncIterator](),
          async iterator => { await iterator.return?.(); }, 'program-journal-follow:' + address.sessionId);
        const stop = () => { void lease.release().catch(() => {}); };
        signal.addEventListener('abort', stop, { once: true });
        if (signal.aborted) stop();
        let failed: { readonly error: unknown } | undefined;
        try {
          for (;;) {
            const item = await owner.run(() => lease.value.next(), signal);
            signal.throwIfAborted(); owner.assertActive();
            if (item.done) return;
            yield item.value;
          }
        } catch (error) { failed = { error: signal.aborted && error === signal.reason ? error : journalFailure(owner, error) }; }
        finally {
          signal.removeEventListener('abort', stop);
          try { await lease.release(); }
          catch (cleanup) {
            if (failed && cleanup !== failed.error)
              throw new AggregateError([failed.error, cleanup], 'REACTIVE_PROGRAM_JOURNAL_FOLLOW_FAILED');
            throw failed ? failed.error : cleanup;
          }
          if (failed) throw failed.error;
        }
      })();
    },
  });
}

/** Controller的durable原生事件；顺序、重连、缺口修补和分页均沿官方Gateway journal。 */
export class ProgramJournal extends RemoteJournalStream<ProgramJournalPage, ProgramJournalEntry, number, ProgramJournalRequest> {
  #release!: () => Promise<void>;
  #notifyFailure: (error: unknown) => void;
  #stopOwner?: () => void;
  #notificationFailure?: { readonly error: unknown };
  private constructor(private readonly owner: ClientScope, private readonly journal: SessionJournal, options: ProgramJournalOptions) {
    let notified = false;
    const failed = (error: unknown): void => {
      if (notified) return;
      // 先占据同一次终态通知；journalFailure可以同步撤销owner并重入其listener。
      notified = true;
      options.failed(journalFailure(owner, error));
    };
    super(owner.context.remote, { ...options, failed,
      name: 'program-session-events', emptyCursor: -1,
      entries: page => page.records, hasMore: page => page.hasMore,
      first: record => record.event.seq, last: record => record.event.seq,
      compare: (left, right) => left - right, follows: (left, right) => right === left + 1 });
    this.#notifyFailure = failed;
  }
  private observeOwner(): void {
    const aborted = (): void => {
      const error = this.owner.signal.reason;
      if (!(error instanceof ConversationAccessRevokedError)) return;
      // 官方stream在dispose后不再通知failed；同owner的兄弟读取可以先触发此退出。
      try { this.#notifyFailure(error); }
      catch (failure) { this.#notificationFailure = { error: failure }; }
    };
    this.owner.signal.addEventListener('abort', aborted, { once: true });
    this.#stopOwner = () => this.owner.signal.removeEventListener('abort', aborted);
    if (this.owner.signal.aborted) aborted();
  }
  /** @internal 先登记本次高级流；取消opening只释放这个流，不结束会话或兄弟观察。 */
  static async create(owner: ClientScope, journal: SessionJournal, options: ProgramJournalOptions,
    request: ProgramJournalRequest, cancellation: CancellationOptions = {}): Promise<ProgramJournal> {
    owner.assertActive();
    const signal = cancellation.signal ? AbortSignal.any([owner.signal, cancellation.signal]) : owner.signal;
    signal.throwIfAborted();
    const selected = structuredClone(request);
    const lease = owner.acquire(() => new ProgramJournal(owner, journal, options),
      journal => journal.disposeNative(), 'program-journal');
    lease.value.#release = lease.release;
    try {
      await owner.run(() => lease.value.open(selected), signal);
      signal.throwIfAborted(); owner.assertActive();
      lease.value.observeOwner();
      return lease.value;
    } catch (error) {
      if (!signal.aborted || error !== signal.reason) error = journalFailure(owner, error);
      try { await lease.release(); }
      catch (cleanup) {
        if (cleanup !== error) throw new AggregateError([error, cleanup], 'REACTIVE_PROGRAM_JOURNAL_OPEN_FAILED');
      }
      throw error;
    }
  }
  private async disposeNative(): Promise<void> {
    this.#stopOwner?.(); this.#stopOwner = undefined;
    const failures: unknown[] = [];
    try { await super.dispose(); } catch (error) { failures.push(error); }
    const notification = this.#notificationFailure;
    if (notification && !failures.includes(notification.error)) failures.push(notification.error);
    if (failures.length === 1) throw failures[0];
    if (failures.length) throw new AggregateError(failures, 'REACTIVE_PROGRAM_JOURNAL_DISPOSAL_FAILED');
  }
  override dispose(): Promise<void> { return this.#release(); }
  override async prepend(request: ProgramJournalRequest): Promise<void> {
    this.owner.assertActive();
    try { await super.prepend(structuredClone(request)); }
    catch (error) { throw journalFailure(this.owner, error); }
  }
  override restart(): void { this.owner.assertActive(); super.restart(); }
  protected async *follow(request: ProgramJournalRequest, signal: AbortSignal): AsyncIterable<RemoteJournalFrame<ProgramJournalEntry, number, ProgramJournalPage>> {
    this.owner.assertActive();
    try { yield* this.journal.follow(this.repairRequest(request), { signal }); }
    catch (error) { throw journalProtocolFailure(error); }
  }
  protected async readPage(request: ProgramJournalRequest, throughSeq: number, signal: AbortSignal): Promise<ProgramJournalPage> {
    try { return await this.journal.page({ ...request, throughSeq }, { signal }); }
    catch (error) { throw journalProtocolFailure(error); }
  }
  protected repairRequest(request: ProgramJournalRequest): ProgramJournalRequest {
    return { ...(request.maxMessages === undefined ? {} : { maxMessages: request.maxMessages }),
      ...(request.turnWindow === undefined ? {} : { turnWindow: request.turnWindow }) };
  }
}
