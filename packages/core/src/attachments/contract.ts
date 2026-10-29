import type { AttachmentId as AttachmentIdType, ImageAttachmentRef } from '@deepseek-ai/dsh-attachment/types';
import type { CancellationOptions } from '../conversations/protocol.ts';

/** 由正式会话日志授权的图片引用及本次读取独占的字节。 */
export interface SessionImageAttachment {
  readonly attachment: ImageAttachmentRef;
  readonly data: Uint8Array;
}

/** 只读取当前会话正式引用的图片；不接受文件路径或全局存储键读取。 */
export interface SessionAttachments {
  /** 取消只退出等待，已经开始的原生读取仍由读侧结算。 */
  readImage(attachmentId: AttachmentIdType, options?: CancellationOptions): Promise<SessionImageAttachment>;
}
