import { RemoteJournalStream, type ClientRemote, type RemoteJournalFrame, type RemoteJournalStreamOptions } from '@deepseek-ai/dsh-api-gateway/client';
import type { ProgramSessionAddress, ProgramJournalEntry, ProgramJournalPage, ProgramJournalPageRequest } from './contracts/types.ts';
import { revokeConversationAccess, type ClientScope } from './scope.ts';

export type ProgramJournalRequest = Omit<ProgramJournalPageRequest, 'sessionId' | 'program' | 'accessId' | 'throughSeq'>;
export type ProgramJournalOptions = Pick<RemoteJournalStreamOptions<ProgramJournalPage, ProgramJournalEntry, number>, 'publish' | 'failed' | 'carrierFailed'>;

/** Controller 的 durable 原生事件，顺序、重连、缺口修补和分页均交给 Gateway journal。 */
export class ProgramJournal extends RemoteJournalStream<ProgramJournalPage, ProgramJournalEntry, number, ProgramJournalRequest> {
  #release!: () => Promise<void>;
  private constructor(private readonly owner: ClientScope, private readonly remote: ClientRemote, private readonly address: ProgramSessionAddress, options: ProgramJournalOptions) {
    super(remote, { ...options, failed: error => options.failed(revokeConversationAccess(owner, error)),
      name: 'program-session-events', emptyCursor: -1,
      entries: page => page.records, hasMore: page => page.hasMore,
      first: record => record.event.seq, last: record => record.event.seq,
      compare: (left, right) => left - right, follows: (left, right) => right === left + 1 });
  }
  /** @internal 会话scope在构造前取得所有权，调用者只通过openEvents取得此句柄。 */
  static async create(owner: ClientScope, address: ProgramSessionAddress, options: ProgramJournalOptions, request: ProgramJournalRequest): Promise<ProgramJournal> {
    const lease = owner.acquire(() => new ProgramJournal(owner, owner.context.remote, address, options),
      journal => journal.disposeNative(), 'program-journal:' + address.sessionId);
    lease.value.#release = lease.release;
    try { await lease.value.open(request); owner.assertActive(); return lease.value; }
    catch (error) { error = revokeConversationAccess(owner, error); await lease.release(); throw error; }
  }
  private disposeNative(): Promise<void> { return super.dispose(); }
  override dispose(): Promise<void> { return this.#release(); }
  override async prepend(request: ProgramJournalRequest): Promise<void> {
    this.owner.assertActive();
    await super.prepend(request);
  }
  override restart(): void { this.owner.assertActive(); super.restart(); }
  protected async *follow(request: ProgramJournalRequest, signal: AbortSignal): AsyncIterable<RemoteJournalFrame<ProgramJournalEntry, number, ProgramJournalPage>> {
    this.owner.assertActive();
    yield* this.remote.reactivePrograms.journal({ ...this.repairRequest(request), ...this.address }, AbortSignal.any([this.owner.signal, signal]));
  }
  protected async readPage(request: ProgramJournalRequest, throughSeq: number, signal: AbortSignal): Promise<ProgramJournalPage> {
    const result = await this.owner.run(signal => this.remote.reactivePrograms.journalPage({ ...request, ...this.address, throughSeq }, signal), signal);
    this.owner.assertActive();
    if (!result.ok) throw revokeConversationAccess(this.owner, result.error);
    return result.value;
  }
  protected repairRequest(request: ProgramJournalRequest): ProgramJournalRequest {
    return { ...(request.maxMessages === undefined ? {} : { maxMessages: request.maxMessages }),
      ...(request.turnWindow === undefined ? {} : { turnWindow: request.turnWindow }) };
  }
}
