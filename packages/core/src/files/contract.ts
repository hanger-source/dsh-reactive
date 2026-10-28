import type { Branded } from '@deepseek-ai/dsh-brand';
import type { FileAttachmentRef } from '@deepseek-ai/dsh-attachment/types';
import type { Invocation } from '../actions/contract.ts';
import type { CancellationOptions } from '../conversations/protocol.ts';

/** 只由实际接收会话的原生上传服务签发；字符串品牌不授予受理权限。 */
export type FileUploadReceiptId = Branded<'file-upload-receipt-id'>;
export interface FileUploadValue {
  readonly receiptId: FileUploadReceiptId;
  readonly file: FileAttachmentRef;
}
export interface EncodedFileUploadRequest {
  readonly data: string;
  readonly name?: string;
}

/** 仅关联本次内存调用与目标会话；不授予权限，也不是持久恢复或幂等键。 */
export interface FileUploadInvocationSource {
  readonly kind: 'file-upload';
  readonly sessionId: string;
}

/** 同一次上传的回执与有限调用事实；running 表示已分派，不证明存储已经受理。 */
export interface FileUploadInvocation extends Invocation<FileUploadValue, never, FileUploadInvocationSource> {
  /** 退出本消费者的等待；不撤回已分派上传，也不改写 result 或 activity。 */
  wait(options?: CancellationOptions): Promise<FileUploadValue>;
}
