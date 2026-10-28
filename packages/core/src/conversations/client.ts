import type { Observable } from 'rxjs';
import type { ProgramContract, ProgramInput, ProgramParsedInput, ProgramSurface } from '../program/definition.ts';
import type { SurfaceClient } from '../surfaces/collection-client.ts';
import type { ToolActionBinding } from '../surfaces/presentation.ts';
import type { ResourceReader } from '../resources/reader.ts';
import type { Observation } from '../resources/observation.ts';
import type { HumanSnapshot, HumanInputState, HumanForegroundQuestion } from '../participation/contract.ts';
import type { HumanQuestionControls, HumanQuestionHandle } from '../participation/questions.ts';
import type { OperationContract, OperationHandle, OperationReference, OperationCommand,
  OperationLocation, OperationState, OperationFeedbackRead, OperationFeedbackPayload } from '../operations/contract.ts';
import type { ConversationIdentity, InputReceipt } from './conversation.ts';
import type { CancellationOptions, Submission, FileUploadSource, FileUploadValue } from './protocol.ts';
import type { AssistantProgressFrame, ConversationEvent } from './events.ts';
import type { ConversationProgressSnapshot, RequestHandle } from './request.ts';

/**
 * 供业务模块借用的会话能力；实现保留自己的 owner、权限与同步/异步时序。
 * Async 仅限定 Surface 的解析与 Action 受理，不描述业务 Operation 的寿命。
 */
export interface ProgramSessionClient<P extends ProgramContract,
  Options extends CancellationOptions = CancellationOptions, Async extends boolean = boolean>
  extends SurfaceClient<ProgramSurface<P>, Options, Async>, HumanQuestionControls<Options> {
  readonly id: string;
  /** 原 Program 纯合同；保留模块身份和输入/出口的完整类型。 */
  readonly definition: P;
  readonly identity: Pick<ConversationIdentity, 'sessionId' | 'environmentId' | 'workspaceId'>;
  readonly actionManifest$: Observable<readonly ToolActionBinding[]>;
  readonly progress: ResourceReader<ConversationProgressSnapshot>;
  readonly progress$: Observable<Observation<ConversationProgressSnapshot>>;
  readonly progressFrames$: Observable<AssistantProgressFrame>;
  readonly events$: Observable<ConversationEvent<ProgramParsedInput<P>>>;
  request(requestId: string): RequestHandle<ProgramParsedInput<P>>;
  readonly human: ResourceReader<HumanSnapshot>;
  readonly human$: Observable<Observation<HumanSnapshot>>;
  readonly questions$: Observable<HumanInputState>;
  readonly foregroundQuestions$: Observable<readonly HumanForegroundQuestion[]>;
  question(callId: string, options?: Options): Promise<HumanQuestionHandle<Options>>;
  submit(input: Submission<ProgramInput<P>>, options?: Options): Promise<InputReceipt>;
  editQueuedInput(requestId: string, content: Submission['content'], options?: Options): Promise<InputReceipt>;
  uploadFile(data: FileUploadSource, options?: Options & { readonly name?: string }): Promise<FileUploadValue>;
  attachOperation<D extends OperationContract>(definition: D, reference: OperationReference<D>, options?: Options): OperationHandle<D> | Promise<OperationHandle<D>>;
  locateOperation<D extends OperationContract>(definition: D, command: OperationCommand<D>, options?: Options): Promise<OperationLocation<OperationReference<D>, OperationState<D>>>;
  readOperationFeedback<D extends OperationContract>(definition: D, command: OperationCommand<D>, participationId: string,
    options?: Options): Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>>;
  /** 只归还本消费者持有的观察；已受理业务仍归原 owner。 */
  release(): void | Promise<void>;
}
