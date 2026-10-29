import type { Observable } from 'rxjs';
import type { ProgramContract, ProgramInput, ProgramParsedInput, ProgramSurface } from '../program/definition.ts';
import type { SurfaceClient } from '../surfaces/collection-client.ts';
import type { ToolActionBinding } from '../surfaces/presentation.ts';
import type { ResourceReader } from '../resources/reader.ts';
import type { Observation } from '../resources/observation.ts';
import type { HumanSnapshot, HumanInputState, HumanForegroundQuestion } from '../participation/contract.ts';
import type { HumanQuestionControls, HumanQuestionHandle } from '../participation/questions.ts';
import type { OperationContract, OperationHandle, OperationReference, OperationCommand,
  OperationLocation, OperationState, OperationFeedbackRead, OperationFeedbackPayload, OperationNotificationResult } from '../operations/contract.ts';
import type { ConversationIdentity, InputReceipt } from './conversation.ts';
import type { CancellationOptions, Submission, FileUploadSource } from './protocol.ts';
import type { FileUploadInvocation, SessionFiles } from '../files/contract.ts';
import type { SessionJournal } from '../journal/contract.ts';
import type { AssistantProgressFrame, ConversationEvent } from './events.ts';
import type { ConversationProgressSnapshot, RequestHandle } from './request.ts';

/** 原生 Goal 控制完成后的最小共同切面；本地实现可以提供完整原生 GoalView。 */
export interface ProgramGoalControl {
  readonly id: string;
  readonly revision: number;
  readonly phase: 'active' | 'paused' | 'blocked' | 'complete';
}
/** 原生处理已静止并完成持久 flush；不表示外部业务 Operation 已取消。 */
export interface ProgramCancelResult { readonly canceled: true; }
/** 在宿主核对原请求的权威事实；不使用客户端缓存缺项推断 absent。 */
export interface ProgramRequestStatus {
  readonly requestId: string;
  readonly state: 'absent' | 'declared' | 'accepted' | 'consumed' | 'settled';
}

/**
 * 供业务模块借用的会话能力；实现保留自己的 owner、权限与同步/异步时序。
 * Async 仅限定 Surface 的解析与 Action 受理，不描述业务 Operation 的寿命。
 */
export interface ProgramSessionClient<P extends ProgramContract,
  Options extends CancellationOptions = CancellationOptions, Async extends boolean = boolean>
  extends SurfaceClient<ProgramSurface<P>, Options, Async> {
  readonly id: string;
  /** 原 Program 纯合同；保留模块身份和输入/出口的完整类型。 */
  readonly definition: P;
  readonly identity: Pick<ConversationIdentity, 'sessionId' | 'environmentId' | 'workspaceId'>;
  readonly files: SessionFiles;
  readonly journal: SessionJournal;
  readonly actionManifest: ResourceReader<readonly ToolActionBinding[]>;
  readonly actionManifest$: Observable<readonly ToolActionBinding[]>;
  readonly progress: ResourceReader<ConversationProgressSnapshot>;
  readonly progress$: Observable<Observation<ConversationProgressSnapshot>>;
  readonly progressFrames$: Observable<AssistantProgressFrame>;
  readonly events$: Observable<ConversationEvent<ProgramParsedInput<P>>>;
  request(requestId: string): RequestHandle<ProgramParsedInput<P>>;
  /** 只读核对当前宿主事实；读取取消不会投递输入或激活 Agent。 */
  requestStatus(requestId: string, options?: CancellationOptions): Promise<ProgramRequestStatus>;
  readonly human: ResourceReader<HumanSnapshot>;
  readonly human$: Observable<Observation<HumanSnapshot>>;
  readonly questions$: Observable<HumanInputState>;
  readonly foregroundQuestions$: Observable<readonly HumanForegroundQuestion[]>;
  submit(input: Submission<ProgramInput<P>>, options?: Options): Promise<InputReceipt>;
  editQueuedInput(requestId: string, content: Submission['content'], options?: Options): Promise<InputReceipt>;
  /** 同步取得同一次上传；signal 在分派前取消准备，分派后只退出默认 wait。 */
  uploadFile(data: FileUploadSource, options?: Options & { readonly name?: string }): FileUploadInvocation;
  attachOperation<D extends OperationContract>(definition: D, reference: OperationReference<D>, options?: Options): OperationHandle<D> | Promise<OperationHandle<D>>;
  locateOperation<D extends OperationContract>(definition: D, command: OperationCommand<D>, options?: Options): Promise<OperationLocation<OperationReference<D>, OperationState<D>>>;
  readOperationFeedback<D extends OperationContract>(definition: D, command: OperationCommand<D>, participationId: string,
    options?: Options): Promise<OperationFeedbackRead<OperationReference<D>, OperationFeedbackPayload<D>>>;
  /** 以当前参与身份核验原 follow 并通知；可靠投递仍使用原发布者身份。 */
  notifyOperation(followId: string, options?: Options): Promise<OperationNotificationResult>;
  /** 只归还本消费者持有的观察；已受理业务仍归原 owner。 */
  release(): void | Promise<void>;
}

/** 人工参与端口；服务参与者借用同一事实，但不能回答、占有前台等待或控制模型。 */
export interface HumanProgramSessionClient<P extends ProgramContract,
  Options extends CancellationOptions = CancellationOptions, Async extends boolean = boolean>
  extends ProgramSessionClient<P, Options, Async>, HumanQuestionControls<Options> {
  question(callId: string, options?: Options): Promise<HumanQuestionHandle<Options>>;
  /** 取消只结束回执等待，已开始的原生控制仍继续结算。 */
  pause(options?: CancellationOptions): Promise<ProgramGoalControl>;
  cancel(options?: CancellationOptions): Promise<ProgramCancelResult>;
}
