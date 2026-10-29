export class SessionJournalUnavailableError extends Error {
  override readonly name = 'SessionJournalUnavailableError';
  constructor(readonly sessionId: string) { super('REACTIVE_SESSION_JOURNAL_UNAVAILABLE'); }
}
