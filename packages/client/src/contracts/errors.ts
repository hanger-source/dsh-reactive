import { RemoteError, type RemoteErrorCode } from '@deepseek-ai/dsh-typert-protocol';
import { z } from 'zod';
import { programIdentity, type ActionIdentity, type NativeChildCommand, type NativeChildReference, type ProgramContractIdentity } from '@hanger-source/dsh-reactive';
import type { ProgramCreationReference, ProgramFailure, ProgramIdentity } from './types.ts';

/** 接受状态未被确认；此错误从不触发自动重发 Action。 */
export class ActionAdmissionUnknownError extends Error {
  constructor(readonly sessionId: string, readonly name: string, cause: unknown) { super('REACTIVE_ACTION_ADMISSION_UNKNOWN', { cause }); }
}
/** 创建结果未知时保留准确引用，调用者可显式恢复，不悄悄创建另一会话。 */
export class CreationUnknownError extends Error {
  constructor(readonly reference: ProgramCreationReference, cause: unknown) { super('REACTIVE_CREATION_UNKNOWN', { cause }); }
}
/** 宿主明确报告已部分持久化；沿同一引用完成登记，不归类为未知受理。 */
export class CreationIncompleteError extends Error {
  constructor(readonly reference: ProgramCreationReference, cause: unknown) { super('REACTIVE_CREATION_INCOMPLETE', { cause }); }
}
/** 宿主已确认实际身份；保留创建或恢复结果，调用者可换用匹配端口取得原会话。 */
export class ProgramParticipantMismatchError extends Error {
  readonly identity: ProgramIdentity;
  constructor(readonly expectedParticipant: ProgramIdentity['participant'], identity: ProgramIdentity) {
    super('REACTIVE_PROGRAM_CLIENT_PARTICIPANT_MISMATCH');
    this.identity = Object.freeze({ ...identity, program: programIdentity(identity.program) });
  }
}
/** HTTP 回执未证明受理结果；保留原请求地址供只读核对和应用显式确定性重投。 */
export class SubmissionAdmissionUnknownError extends Error {
  constructor(readonly sessionId: string, readonly requestId: string, cause: unknown) { super('REACTIVE_SUBMISSION_ADMISSION_UNKNOWN', { cause }); }
}
/** 编辑可能已经持久生效；保留同一请求身份供显式读取核对，不自动重投。 */
export class InputEditAdmissionUnknownError extends Error {
  constructor(readonly sessionId: string, readonly requestId: string, cause: unknown) { super('REACTIVE_INPUT_EDIT_ADMISSION_UNKNOWN', { cause }); }
}
/** 本次传输没有取得正式上传回执；内存调用身份不能用于恢复未知的原生 receipt。 */
export class FileUploadReceiptUnknownError extends Error {
  override readonly name = 'FileUploadReceiptUnknownError';
  constructor(readonly sessionId: string, cause: unknown) { super('REACTIVE_FILE_UPLOAD_RECEIPT_UNKNOWN', { cause }); }
}

declare module '@deepseek-ai/dsh-typert-protocol' {
  interface RemoteErrorDetailsMap {
    'reactive/creation-incomplete': { readonly reference: ProgramCreationReference };
    'reactive/child-creation-incomplete': { readonly command: NativeChildCommand; readonly reference: NativeChildReference };
    'reactive/action-binding-unavailable': { readonly name: string; readonly reason: 'not-exposed' | 'binding-changed' | 'connection-lost' };
    'reactive/action-target-mismatch': { readonly name: string; readonly reason: 'target-mismatch' };
    'reactive/action-contract-mismatch': { readonly expected: ActionIdentity; readonly actual: ActionIdentity };
    'reactive/program-contract-mismatch': { readonly expected: ProgramContractIdentity; readonly actual: ProgramContractIdentity };
    'reactive/shared-program-required': Record<string, never>;
    'reactive/input-edit-rejected': { readonly requestId: string; readonly reason: 'unavailable' | 'actor-mismatch' | 'content-conflict' | 'changed' };
  }
}
/** 正式 failure 已由 Gateway/贡献合同核验，客户端保留完整公开 details。 */
export function decodeProgramFailure(value: Omit<ProgramFailure, 'details'> & { readonly details: unknown }): RemoteError {
  const details = z.union([z.array(z.json()), z.record(z.string(), z.json())]).parse(value.details);
  return new RemoteError(value.code as RemoteErrorCode, value.message, details);
}
