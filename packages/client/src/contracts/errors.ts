import { RemoteError, type RemoteErrorCode } from '@deepseek-ai/dsh-typert-protocol';
import { z } from 'zod';
import { ActionContractMismatchError, ConversationAccessRevokedError, SessionProgramMismatchError, SessionFilesUnavailableError, SessionJournalUnavailableError, programIdentity, type ActionIdentity, type NativeChildCommand, type NativeChildReference, type ProgramContractIdentity } from '@hanger-source/dsh-reactive';
import type { ProgramCreationReference, ProgramFailure, ProgramIdentity } from './types.ts';
import { EligibilityError } from '@hanger-source/dsh-reactive';
export { ConversationAccessRevokedError, SessionProgramMismatchError } from '@hanger-source/dsh-reactive';
export { SessionFilesUnavailableError, SessionJournalUnavailableError } from '@hanger-source/dsh-reactive';

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
    'reactive/eligibility': { readonly capabilityName: string; readonly reason: 'pending' | 'error' | 'denied'; readonly observation: import('./types.ts').WireObservation };
    'reactive/creation-incomplete': { readonly reference: ProgramCreationReference };
    'reactive/child-creation-incomplete': { readonly command: NativeChildCommand; readonly reference: NativeChildReference };
    'reactive/action-binding-unavailable': { readonly name: string; readonly reason: 'not-exposed' | 'binding-changed' | 'connection-lost' };
    'reactive/action-target-mismatch': { readonly name: string; readonly reason: 'target-mismatch' };
    'reactive/action-contract-mismatch': { readonly expected: ActionIdentity; readonly actual: ActionIdentity };
    'reactive/program-contract-mismatch': { readonly expected: ProgramContractIdentity; readonly actual: ProgramContractIdentity };
    'reactive/session-program-mismatch': { readonly sessionId: string; readonly recorded: ProgramContractIdentity | null; readonly current: ProgramContractIdentity | null };
    'reactive/shared-program-required': Record<string, never>;
    'reactive/input-edit-rejected': { readonly requestId: string; readonly reason: 'unavailable' | 'actor-mismatch' | 'content-conflict' | 'changed' };
    'reactive/conversation-access-revoked': { readonly sessionId: string; readonly workspaceId: string; readonly reason: 'membership-removed' | 'workspace-deleted' | 'access-expired' };
    'reactive/invocation-operation-unavailable': Record<string, never>;
    'reactive/files-unavailable': { readonly sessionId: string };
    'reactive/journal-unavailable': { readonly sessionId: string };
  }
}
const actionIdentitySchema = z.object({ name: z.string(), version: z.number().int().positive().safe() }).strict();

/** 正式 failure 已由 Gateway/贡献合同核验，客户端保留完整公开 details。 */
export function decodeProgramFailure(value: Omit<ProgramFailure, 'details'> & { readonly details: unknown }): RemoteError | ConversationAccessRevokedError | SessionProgramMismatchError | ActionContractMismatchError | SessionFilesUnavailableError | SessionJournalUnavailableError | EligibilityError {
  if (value.code === 'reactive/eligibility') {
    const failure = z.object({ code: z.string(), message: z.string(), details: z.union([z.array(z.json()), z.record(z.string(), z.json())]) }).strict();
    const details = z.object({ capabilityName: z.string(), reason: z.enum(['pending', 'error', 'denied']), observation: z.discriminatedUnion('status', [
      z.object({ status: z.literal('pending'), generation: z.number() }).strict(),
      z.object({ status: z.literal('error'), generation: z.number(), error: failure }).strict(),
      z.object({ status: z.literal('ready'), generation: z.number(), observedAt: z.number(),
        value: z.object({ allowed: z.literal(false), reason: z.string() }).strict() }).strict(),
    ]) }).strict().parse(value.details);
    const observation = details.observation;
    const error = new EligibilityError(details.capabilityName, observation.status === 'error'
      ? { ...observation, error: decodeProgramFailure(observation.error) } : observation);
    if (error.reason !== details.reason) throw new Error('REACTIVE_ELIGIBILITY_FAILURE_INVALID');
    return error;
  }
  if (value.code === 'reactive/journal-unavailable') {
    const details = z.object({ sessionId: z.string() }).strict().parse(value.details);
    return new SessionJournalUnavailableError(details.sessionId);
  }
  if (value.code === 'reactive/files-unavailable') {
    const details = z.object({ sessionId: z.string() }).strict().parse(value.details);
    return new SessionFilesUnavailableError(details.sessionId);
  }
  if (value.code === 'reactive/action-contract-mismatch') {
    const details = z.object({ expected: actionIdentitySchema, actual: actionIdentitySchema }).strict().parse(value.details);
    return new ActionContractMismatchError(details.expected, details.actual);
  }
  if (value.code === 'reactive/conversation-access-revoked') {
    const details = z.object({ sessionId: z.string(), workspaceId: z.string(),
      reason: z.enum(['membership-removed', 'workspace-deleted', 'access-expired']) }).strict().parse(value.details);
    return new ConversationAccessRevokedError(details.sessionId, details.workspaceId, details.reason);
  }
  if (value.code === 'reactive/session-program-mismatch') {
    const identity = z.object({ name: z.string(), version: z.number().int().positive().safe() }).strict().nullable();
    const details = z.object({ sessionId: z.string(), recorded: identity, current: identity }).strict().parse(value.details);
    return new SessionProgramMismatchError(details.sessionId, details.recorded, details.current);
  }
  const details = z.union([z.array(z.json()), z.record(z.string(), z.json())]).parse(value.details);
  return new RemoteError(value.code as RemoteErrorCode, value.message, details);
}
