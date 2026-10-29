export { fromSnapshot, abortable } from './reactivity/observable.ts';
export { Scope, ScopeDisposedError, Cell, Value, batch } from './reactivity/scope.ts';
export type { ScopeOptions } from './reactivity/scope.ts';
export * from './resources/index.ts';
export * from './reactions/index.ts';
export type { FileUploadInvocation, FileUploadInvocationSource } from './files/contract.ts';
export { createFileUploadInvocation } from './files/invocation.ts';
export type { SessionFiles, FileResource, FileWatch, WorkspaceFileStat, WorkspaceFileRange, WorkspaceFileText,
  WorkspaceByteRange, WorkspaceByteReadOptions, WorkspaceFileBytes, WorkspaceDirectoryEntry,
  WorkspaceDirectoryListing, WorkspaceFileChange, WorkspaceFileWatchFrame } from './files/contract.ts';
export { SessionFilesUnavailableError, SessionFileOutsideWorkspaceError } from './files/errors.ts';
export { createFileResources } from './files/resources.ts';
export type { FileResourceOptions } from './files/resources.ts';
export * from './journal/index.ts';
export type { Workspace } from './workspaces/context.ts';
export type { SnapshotSource } from './reactivity/observable.ts';
export type { ProgramSessionClient, HumanProgramSessionClient, ProgramGoalControl, ProgramCancelResult, ProgramRequestStatus } from './conversations/client.ts';
export { defineApplication } from './applications/definition.ts';
export { defineEnvironment } from './environments/definition.ts';
export { z } from 'zod';
export type { InputParticipant } from './conversations/protocol.ts';
export type { Json, InputReference, GoalReference, Continuation, WaitReference, Submission, NormalizedSubmission, AdmittedSubmission, InputDelivery, PromptContentPart, AdmittedPromptContentPart, FileUploadValue, FileUploadReceiptId, EncodedFileUploadRequest, FileUploadSource, UploadFileOptions, CancellationOptions, SubmitOptions, Admission, Conversation, ConversationIdentity, Views, ObservedViews, InputReceipt } from './conversations/conversation.ts';
export type { Environment, AnyEnvironment, EnvironmentInput, EnvironmentViews, EnvironmentActions, EnvironmentOperations, EnvironmentAt } from './environments/definition.ts';
export type { ApplicationDefinition } from './applications/definition.ts';
export { defineAction, implementAction, bindAction, ActionBindingUnavailableError, ActionTargetMismatchError } from './actions/contract.ts';
export { ActionEligibilityError } from './actions/eligibility.ts';
export type { ActionEligibility } from './actions/eligibility.ts';
export { actionIdentity, actionIdentityKey, assertActionIdentity, ActionContractMismatchError } from './actions/identity.ts';
export type { ActionIdentity } from './actions/contract.ts';
export type { LocalActionOptions } from './actions/contract.ts';
export { InputEditRejectedError } from './conversations/input-edit.ts';
export { ConversationAccessRevokedError } from './conversations/access.ts';
export { SessionProgramMismatchError } from './conversations/program-identity.ts';
export type { ActionContract, ActionDefinition, ActionOptions, ActionImplementation, ActionImplementationOptions, ActionInput, ActionParsedInput, ActionOutput, ActionScope, ActionObserved, ActionTarget, ActionSource, ActionExecutionContext, ActionCallContext, ActionBindingOptions, ActionReference, ActionCommandOrigin, ActionOperationContract, ActionOperation, ActionInvocation, BoundAction, Invocation, InvocationActivity, InvocationPhase } from './actions/contract.ts';
export { defineOperation, implementOperation, bindOperation, attachOperation, locateOperation, operationFollowLinkSchema, encodeOperationFeedback, decodeOperationFeedback } from './operations/contract.ts';
export type { OperationNotificationAcknowledgement, OperationNotificationDelivery, OperationNotificationConnection, OperationNotificationCleanup, OperationNotificationSource } from './operations/contract.ts';
export type { OperationContract, OperationDefinition, OperationOptions, OperationCommand, OperationReference, OperationParsedCommand, OperationParsedReference, OperationState, OperationSettled, OperationAccessContext, OperationLocation, OperationNotificationResult, OperationFollowLink, OperationFollowMode, OperationOwnerReference, OperationOwner, OperationOwnerOptions, OperationHandle, OperationFeedbackNode, OperationFeedbackRead, OperationFeedbackPayload, OperationBindingReference, OperationBindingOptions, OperationParticipation, BoundOperation } from './operations/contract.ts';
export { NativeChildWork, nativeChildWorkAction, nativeChildReceiptSchema, nativeChildReferenceSchema } from './collaboration/child.ts';
export type { NativeChildCommand, NativeChildReference, NativeChildReceipt, NativeChildState, NativeChildFeedback } from './collaboration/child.ts';
export { defineProgram, assertProgramDefinition, assertSharedProgramDefinition } from './program/definition.ts';
export type { ProgramContract, SharedProgramContract, ProgramExpectation, ProgramDefinition, SharedProgramDefinition, ProgramInput, ProgramParsedInput, ProgramSurface, ProgramSession, ProgramWorkspace, ProgramProps } from './program/definition.ts';
export { programIdentity, assertProgramIdentity, ProgramContractMismatchError, SharedProgramRequiredError } from './program/identity.ts';
export type { ProgramContractIdentity } from './program/identity.ts';

export type { ProgramImplementation } from './program/definition.ts';
export { decodeProgramInputs, decodeProgramEvent } from './program/decode.ts';
export { defineSurface, view, resourceView, encodeView, decodeView, surfaceSlotDefinition, surfaceInstanceSlot, surfaceCollectionKeysSlot } from './surfaces/contract.ts';
export type { Surface, SurfaceCollectionDefinition, SurfaceViews, SurfaceBindings, PartialSurfaceBindings, ViewDefinition, ViewMode, ViewSource, ViewValue, ViewWire } from './surfaces/contract.ts';
export { composeSurface, selectSurface, collectionSurface } from './surfaces/composition.ts';

export type { SurfaceMount, SurfaceCollectionMount, SurfaceMounts, SurfaceComposition, SurfaceTree, SurfaceSelection, ComposedSurface } from './surfaces/composition.ts';

export { createSurfaceCollectionClient, createSurfaceModuleClient } from './surfaces/collection-client.ts';
export type { SurfaceClient, SurfaceCollectionClient, SurfaceCollectionTransport, SurfaceInstanceClient, CollectionResolvedAction } from './surfaces/collection-client.ts';

export type { NativeToolCall, NativeToolAdmission } from './tools/admission.ts';
export { readActionPresentation, toolActionBindingSchema, actionManifestSlot } from './surfaces/presentation.ts';
export type { ActionPresentation, ActionPresentationMetadata, ToolActionBinding } from './surfaces/presentation.ts';

export type { ConversationActivity, GoalActivity, RequestActivity, ToolActivity, CreationReference, WorkspaceInfo, WorkspaceRegistration, ConversationCreateOptions } from './conversations/protocol.ts';
export type { RequestDeclaration, ReplyActivity, InputResult } from './conversations/protocol.ts';
export type { ConversationEvent, RequestEvent, ToolOccurrence, AssistantStream, AssistantAttemptSnapshot, AssistantProgressFrame } from './conversations/events.ts';
export { ownConversationEvent } from './conversations/events.ts';
export { createRequestHandle, createAssistantProgress, parseAssistantAttempt, parseConversationProgress, parseAssistantProgressFrame } from './conversations/request.ts';
export type { ConversationProgressSnapshot, RequestSnapshot, RequestHandle, AssistantProgress } from './conversations/request.ts';
export { createWorkSelector } from './conversations/work.ts';
export type { WorkOrigin, WorkSourceIdentity, WorkSource, WorkSourceSnapshot, WorkOccurrence, WorkRequestOccurrence, WorkWait, WorkCondition, WorkSessionSnapshot, WorkSnapshot, WorkEvent, WorkProgressFrame, WorkHandle, WorkSelector } from './conversations/work.ts';
export { AssistantStreamAccumulator, expandAssistantStream, assembleAssistantStream, joinAssistantStreamText, isTokenDelta, isVisibleChunk, chunkHasVisibleText, runFirstTokenTime, runFirstVisibleTime, assistantStreamFirstTokenTime, assistantStreamHasVisibleContent, assistantStreamHasVisibleText, lastAssistantStreamChunk, assistantStreamChunks } from '@deepseek-ai/dsh-llm/assistant-stream';
export type { AssistantStreamRecord, AssistantStreamRun, TimedStreamChunk, RawStreamChunkType } from '@deepseek-ai/dsh-llm/assistant-stream';
export type { StreamChunk } from '@deepseek-ai/dsh-llm/types';

export type { HumanQuestionOrigin, HumanQuestion, HumanSettledQuestion, HumanQuestionItem, HumanAnswer, HumanAnswerItem, HumanInputState, HumanForegroundQuestion, HumanPromptOption, HumanPrompt, HumanAskOptions, HumanPendingAnswer, HumanAskResult, HumanResponderAdmission, HumanInputPolicy, HumanResponderOptions, HumanWaitFrame } from './participation/contract.ts';
export type { HumanSnapshot, HumanQuestionSnapshot } from './participation/contract.ts';
export { createHumanQuestionResolver } from './participation/questions.ts';
export type { HumanQuestionHandle, HumanQuestionControls, HumanQuestionResolver } from './participation/questions.ts';
