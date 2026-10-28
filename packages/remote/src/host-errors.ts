import { RemoteError, type RemoteErrorCode } from '@deepseek-ai/dsh-typert-protocol';
import type { TypertGatewayWireStream } from '@deepseek-ai/dsh-api-gateway';
import { ChildCreationIncompleteError, CreationIncompleteError, InputFileNotStagedError } from '@hanger-source/dsh-reactive-dsh';
import { isAttachmentError } from '@deepseek-ai/dsh-attachment';
import { z, ConversationAccessRevokedError, ActionBindingUnavailableError, ActionTargetMismatchError, ActionContractMismatchError, InputEditRejectedError, ProgramContractMismatchError, SharedProgramRequiredError } from '@hanger-source/dsh-reactive';
import '@hanger-source/dsh-reactive-client/errors';
import type { ProgramFailure } from '@hanger-source/dsh-reactive-client/types';

/** 只有框架明确拥有的可恢复事实做业务映射；其他错误继续由官方 Gateway 归类。 */
export function createProgramFailureMapper(wireStream: Pick<TypertGatewayWireStream, 'failure'>): (error: unknown) => ProgramFailure {
  return error => {
    const known = error instanceof ConversationAccessRevokedError
      ? new RemoteError('reactive/conversation-access-revoked', error.message, {
        sessionId: error.sessionId, workspaceId: error.workspaceId, reason: error.reason,
      })
      : error instanceof ProgramContractMismatchError
      ? new RemoteError('reactive/program-contract-mismatch', error.message, { expected: error.expected, actual: error.actual })
      : error instanceof SharedProgramRequiredError
      ? new RemoteError('reactive/shared-program-required', error.message, {})
      : error instanceof ActionContractMismatchError
      ? new RemoteError('reactive/action-contract-mismatch', error.message, { expected: error.expected, actual: error.actual })
      : error instanceof ChildCreationIncompleteError
      ? new RemoteError('reactive/child-creation-incomplete', 'Child session creation is not yet confirmed', {
        command: error.command, reference: error.reference,
      })
      : error instanceof CreationIncompleteError
        ? new RemoteError('reactive/creation-incomplete', 'Session creation is not yet confirmed', { reference: error.reference })
        : error instanceof ActionBindingUnavailableError
          ? new RemoteError('reactive/action-binding-unavailable', error.message, { name: error.actionName, reason: error.reason })
          : error instanceof ActionTargetMismatchError
            ? new RemoteError('reactive/action-target-mismatch', error.message, { name: error.actionName, reason: error.reason })
            : error instanceof InputEditRejectedError
              ? new RemoteError('reactive/input-edit-rejected', error.message, { requestId: error.requestId, reason: error.reason })
            : error instanceof InputFileNotStagedError
              ? new RemoteError('session/attachment-invalid', error.message, { reason: 'FILE_NOT_STAGED' })
              : isAttachmentError(error)
                ? new RemoteError('session/attachment-invalid', error.message, { reason: error.code })
                : error;
    const failure = wireStream.failure(known);
    const details = z.json().parse(failure.details);
    if (details === null || typeof details !== 'object') throw new Error('REACTIVE_REMOTE_FAILURE_DETAILS_INVALID');
    return { ...failure, details };
  };
}

/** Host仍抛正式RemoteError；领域错误的恢复只发生在客户端，不能丢掉wire分类。 */
export function throwProgramFailure(failure: ProgramFailure): never {
  throw new RemoteError(failure.code as RemoteErrorCode, failure.message, failure.details);
}
