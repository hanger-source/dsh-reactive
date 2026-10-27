/** 确定没有替换队列正文的编辑拒绝；不涵盖取消、持久化或受理结果未知。 */
export class InputEditRejectedError extends Error {
  override readonly name = 'InputEditRejectedError';
  constructor(readonly requestId: string, readonly reason: 'unavailable' | 'actor-mismatch' | 'content-conflict' | 'changed') {
    super('Queued input edit was rejected: ' + reason);
  }
}
