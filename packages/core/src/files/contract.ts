import type { Branded } from '@deepseek-ai/dsh-brand';
import type { FileAttachmentRef } from '@deepseek-ai/dsh-attachment/types';

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
