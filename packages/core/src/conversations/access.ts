/** 已签发的会话读取与准入资格退出；已受理业务仍由原执行 owner 结算。 */
export class ConversationAccessRevokedError extends Error {
  override readonly name = 'ConversationAccessRevokedError';
  constructor(
    readonly sessionId: string,
    readonly workspaceId: string,
    readonly reason: 'membership-removed' | 'workspace-deleted' | 'access-expired',
  ) { super('REACTIVE_CONVERSATION_ACCESS_REVOKED'); }
}
