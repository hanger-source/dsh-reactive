import type { Branded } from '@deepseek-ai/dsh-brand';
import type { FileAttachmentRef } from '@deepseek-ai/dsh-attachment/types';
import type { Invocation } from '../actions/contract.ts';

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

/** 同一次上传的回执与有限调用事实；running不证明存储受理，wait仍受原上传读取owner约束。 */
export interface FileUploadInvocation extends Invocation<FileUploadValue, never, FileUploadInvocationSource> {}
