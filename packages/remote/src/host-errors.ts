import { RemoteError, type RemoteErrorCode } from '@deepseek-ai/dsh-typert-protocol';
import type { TypertGatewayWireStream } from '@deepseek-ai/dsh-api-gateway';
import { ChildCreationIncompleteError, CreationIncompleteError, InputFileNotStagedError, InvocationUnavailableError, NativeGoalError, GoalRoundDriverUnavailableError } from '@hanger-source/dsh-reactive-dsh';
import { isAttachmentError } from '@deepseek-ai/dsh-attachment';
import { z, EligibilityError, ConversationAccessRevokedError, SessionProgramMismatchError, SessionFileOutsideWorkspaceError, SessionFilesUnavailableError, SessionJournalUnavailableError, SessionAttachmentsUnavailableError, ActionBindingUnavailableError, ActionTargetMismatchError, ActionContractMismatchError, ActionInvocationConflictError, ActionAdmissionInProgressError, InputEditRejectedError, ProgramContractMismatchError, SharedProgramRequiredError } from '@hanger-source/dsh-reactive';
import '@hanger-source/dsh-reactive-client/errors';
import type { ProgramFailure, ProgramQuestions } from '@hanger-source/dsh-reactive-client/types';
import { HumanQuestionSourceConflictError } from '@hanger-source/dsh-reactive';

function wireQuestionConflicts(error: HumanQuestionSourceConflictError): ProgramQuestions['sourceConflicts'] {
  const inputs = (values: import('@hanger-source/dsh-reactive').HumanQuestionOrigin['inputs']) =>
    values.map(value => ({ ...value, scope: z.json().parse(value.scope) }));
  return error.conflicts.map(value => ({ ...value, origin: { ...value.origin, inputs: inputs(value.origin.inputs),
    continuation: value.origin.continuation === null ? null : { ...value.origin.continuation, inputs: inputs(value.origin.continuation.inputs) } },
    occurrence: value.occurrence === null ? null : { ...value.occurrence, inputs: inputs(value.occurrence.inputs) } }));
}

/** 框架领域事实与正式接入的原生 Goal 分类保留公开含义；其他错误由官方 Gateway 归类。 */
export function createProgramFailureMapper(wireStream: Pick<TypertGatewayWireStream, 'failure'>): (error: unknown) => ProgramFailure {
  return error => {
    const known = error instanceof HumanQuestionSourceConflictError
      ? new RemoteError('reactive/human-question-source-conflict', error.message, { conflicts: wireQuestionConflicts(error) })
      : error instanceof NativeGoalError
      ? new RemoteError('reactive/native-goal', error.message, { code: error.code })
      : error instanceof GoalRoundDriverUnavailableError
        ? new RemoteError('reactive/goal-round-driver-unavailable', error.message, { reason: error.reason })
      : error instanceof ActionInvocationConflictError
      ? new RemoteError('reactive/action-invocation-conflict', error.message, { invocationId: error.invocationId })
      : error instanceof ActionAdmissionInProgressError
        ? new RemoteError('reactive/action-admission-in-progress', error.message, { invocationId: error.invocationId })
        : error instanceof EligibilityError
      ? new RemoteError("reactive/eligibility", error.message, { capabilityName: error.capabilityName, reason: error.reason,
        observation: error.observation.status === "error"
          ? { ...error.observation, error: createProgramFailureMapper(wireStream)(error.observation.error) }
          : error.observation })
      : error instanceof SessionAttachmentsUnavailableError
      ? new RemoteError('reactive/attachments-unavailable', error.message, { sessionId: error.sessionId })
      : error instanceof SessionJournalUnavailableError
      ? new RemoteError('reactive/journal-unavailable', error.message, { sessionId: error.sessionId })
      : error instanceof SessionFilesUnavailableError
      ? new RemoteError('reactive/files-unavailable', error.message, { sessionId: error.sessionId })
      : error instanceof SessionFileOutsideWorkspaceError
      ? new RemoteError('workspace-file/outside-workspace', error.message, { path: error.path })
      : error instanceof InvocationUnavailableError
      ? new RemoteError('reactive/invocation-unavailable', error.message, {})
      : error instanceof ConversationAccessRevokedError
      ? new RemoteError('reactive/conversation-access-revoked', error.message, {
        sessionId: error.sessionId, workspaceId: error.workspaceId, reason: error.reason,
      })
      : error instanceof SessionProgramMismatchError
      ? new RemoteError('reactive/session-program-mismatch', error.message, {
        sessionId: error.sessionId, recorded: error.recorded, current: error.current,
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
