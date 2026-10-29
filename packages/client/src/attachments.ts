import type { AttachmentIdType, CancellationOptions, SessionAttachments, SessionImageAttachment } from '@hanger-source/dsh-reactive';
import type { ProgramSessionAddress } from './contracts/types.ts';
import { decodeProgramFailure } from './contracts/errors.ts';
import { revokeConversationAccess, type ClientScope } from './scope.ts';

/** 读取原会话日志授权的图片；真实传输结算归 SDK owner。 */
export function remoteAttachments(owner: ClientScope, address: ProgramSessionAddress): SessionAttachments {
  const remote = owner.context.remote.reactivePrograms;
  return Object.freeze({
    readImage(attachmentId: AttachmentIdType, options: CancellationOptions = {}): Promise<SessionImageAttachment> {
      owner.assertActive();
      const signal = options.signal ? AbortSignal.any([owner.signal, options.signal]) : owner.signal;
      signal.throwIfAborted();
      const lease = owner.acquire(() => remote.readImage({ ...address, attachmentId }, signal), async pending => {
        await pending.then(() => {}, () => {});
      }, 'program-image-attachment:' + address.sessionId);
      const release = () => { void lease.release().catch(() => {}); };
      void lease.value.then(release, release);
      return owner.run(async () => {
        const result = await lease.value;
        signal.throwIfAborted(); owner.assertActive();
        if (!result.ok) throw revokeConversationAccess(owner, decodeProgramFailure(result.error));
        return result.value;
      }, signal);
    },
  });
}
