import { RemoteJournalStream, type ClientRemote, type RemoteJournalFrame, type RemoteJournalStreamOptions } from '@deepseek-ai/dsh-api-gateway/client';
import type { ProgramJournalEntry, ProgramJournalPage, ProgramJournalPageRequest } from './types.ts';

export type ProgramJournalRequest = Omit<ProgramJournalPageRequest, 'sessionId' | 'throughSeq'>;
export type ProgramJournalOptions = Pick<RemoteJournalStreamOptions<ProgramJournalPage, ProgramJournalEntry, number>, 'publish' | 'failed' | 'carrierFailed'>;

/** Controller 的 durable 原生事件，顺序、重连、缺口修补和分页均交给 Gateway journal。 */
export class ProgramJournal extends RemoteJournalStream<ProgramJournalPage, ProgramJournalEntry, number, ProgramJournalRequest> {
  constructor(private readonly remote: ClientRemote, private readonly sessionId: string, options: ProgramJournalOptions) {
    super(remote, { ...options, name: 'program-session-events', emptyCursor: -1,
      entries: page => page.records, hasMore: page => page.hasMore,
      first: record => record.event.seq, last: record => record.event.seq,
      compare: (left, right) => left - right, follows: (left, right) => right === left + 1 });
  }
  protected async *follow(request: ProgramJournalRequest, signal: AbortSignal): AsyncIterable<RemoteJournalFrame<ProgramJournalEntry, number, ProgramJournalPage>> {
    yield* this.remote.reactivePrograms.journal({ sessionId: this.sessionId, ...this.repairRequest(request) }, signal);
  }
  protected async readPage(request: ProgramJournalRequest, throughSeq: number, signal: AbortSignal): Promise<ProgramJournalPage> {
    const result = await this.remote.reactivePrograms.journalPage({ sessionId: this.sessionId, throughSeq, ...request }, signal);
    if (!result.ok) throw result.error;
    return result.value;
  }
  protected repairRequest(request: ProgramJournalRequest): ProgramJournalRequest {
    return { ...(request.maxMessages === undefined ? {} : { maxMessages: request.maxMessages }),
      ...(request.turnWindow === undefined ? {} : { turnWindow: request.turnWindow }) };
  }
}
