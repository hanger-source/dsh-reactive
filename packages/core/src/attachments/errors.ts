export class SessionAttachmentsUnavailableError extends Error {
  override readonly name = 'SessionAttachmentsUnavailableError';
  constructor(readonly sessionId: string) { super('REACTIVE_SESSION_ATTACHMENTS_UNAVAILABLE'); }
}
