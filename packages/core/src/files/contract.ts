import type { Branded } from '@deepseek-ai/dsh-brand';
import type { FileAttachmentRef } from '@deepseek-ai/dsh-attachment/types';
import type { WorkspaceFileRange, WorkspaceByteReadOptions, WorkspaceFileText, WorkspaceFileBytes,
  WorkspaceFileStat, WorkspaceDirectoryListing, WorkspaceFileWatchFrame } from '@deepseek-ai/dsh-api-workspace-files/types';
import type { Observable } from 'rxjs';
import type { Invocation } from '../actions/contract.ts';
import type { CancellationOptions } from '../conversations/conversation.ts';
import type { ResourceReadable } from '../resources/reader.ts';

export type { WorkspaceFileStat, WorkspaceFileRange, WorkspaceFileText, WorkspaceByteRange,
  WorkspaceByteReadOptions, WorkspaceFileBytes, WorkspaceDirectoryEntry, WorkspaceDirectoryListing,
  WorkspaceFileChange, WorkspaceFileWatchFrame } from '@deepseek-ai/dsh-api-workspace-files/types';

/** 同一文件查询供函数、Promise、Observable和hooks读取；字节缓冲区不进入共享快照。 */
export interface FileResource<T> extends ResourceReadable<T> {
  readonly values$: Observable<T>;
  invalidate(): void;
}

/** 观察退出与实际释放分开；closed等待本实现持有的watch，不代表远端释放确认。 */
export interface FileWatch {
  readonly changes$: Observable<WorkspaceFileWatchFrame>;
  readonly closed: Promise<void>;
  dispose(): Promise<void>;
}

/** 借用当前会话的文件资格；会话owner负责退出、实际I/O与watch释放。 */
export interface SessionFiles {
  text(path: string, range?: WorkspaceFileRange): FileResource<WorkspaceFileText>;
  metadata(path: string): FileResource<WorkspaceFileStat>;
  directory(path: string): FileResource<WorkspaceDirectoryListing>;
  readText(path: string, range?: WorkspaceFileRange, options?: CancellationOptions): Promise<WorkspaceFileText>;
  readBytes(path: string, range?: WorkspaceByteReadOptions, options?: CancellationOptions): Promise<WorkspaceFileBytes>;
  stat(path: string, options?: CancellationOptions): Promise<WorkspaceFileStat>;
  list(path: string, options?: CancellationOptions): Promise<WorkspaceDirectoryListing>;
  watch(path: string, options?: CancellationOptions): FileWatch;
}

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
