import type { ActionSource, Continuation, InputReference, Submission, HumanAnswer, HumanInputState, HumanForegroundQuestion, HumanQuestionOrigin, HumanWaitFrame, ConversationEvent, WorkspaceInfo, WorkspaceRegistration, ConversationCreateOptions, CreationReference, EncodedFileUploadRequest, FileUploadValue } from '@hanger-source/dsh-reactive';
import type { JsonValue } from '@deepseek-ai/dsh-util-values';
import type { RemoteStreamFailure } from '@deepseek-ai/dsh-api-gateway/stream-protocol';

/** 官方 failure 的 wire 形态；details 按 Typert 边界限定为 JSON 对象或数组。 */
export type ProgramFailure = Omit<RemoteStreamFailure, 'details'> & {
  readonly details: Exclude<JsonValue, null | boolean | number | string>;
};
/** Surface 的 schema input 形态；不把领域对象塞进 JSON 传输。 */
export type WireObservation =
  | { readonly status: 'pending'; readonly generation: number }
  | { readonly status: 'ready'; readonly generation: number; readonly observedAt: number; readonly value: JsonValue }
  | { readonly status: 'error'; readonly generation: number; readonly error: ProgramFailure };
/** 每条视图流以完整基线开始，之后仍交付完整当前观察。 */
export type ProgramViewFrame =
  | { readonly type: 'baseline'; readonly observation: WireObservation }
  | { readonly type: 'observation'; readonly observation: WireObservation };
export type ProgramWorkspaceRegistration = WorkspaceRegistration;
export interface ProgramCreateRequest extends ConversationCreateOptions { readonly environmentId: string; }
export type ProgramCreationReference = CreationReference;
export interface ProgramAddress { readonly sessionId: string; }
export interface ProgramIdentity { readonly sessionId: string; readonly environmentId: string; readonly workspaceId: string; readonly actorId: string; }
export type ProgramWorkspace = WorkspaceInfo;
export interface ProgramGoalControl { readonly id: string; readonly revision: number; readonly phase: 'active' | 'paused' | 'blocked' | 'complete'; }
export interface ProgramCancelResult { readonly canceled: true; }
/** 从核心合同派生结构，递归 JSON 只用 Typert 的公开 canonical JSON 符号。 */
type WireSubmission<S> = S extends { readonly result: unknown } ? Omit<S, 'result'> & {
  readonly result: {
    readonly continuation: Omit<Continuation, 'inputs'> & { readonly inputs: readonly InputReference<JsonValue>[] };
    readonly wait: { readonly id: string; readonly operation: JsonValue };
    readonly ready: boolean;
  };
} : S;
export type ProgramSubmission = WireSubmission<Submission<JsonValue>>;
export interface ProgramSubmitRequest extends ProgramAddress { readonly submission: ProgramSubmission; }
export interface ProgramInputEditRequest extends ProgramRequestAddress { readonly content: Submission['content']; }
/** 单次有界 unary 上传；canonical base64 与 exact-Session receipt 继续由原生 Host 核验。 */
export interface ProgramUploadFileRequest extends ProgramAddress, EncodedFileUploadRequest {}
export type ProgramFileUploadValue = FileUploadValue;
export interface ProgramInputReceipt { readonly sessionId: string; readonly requestId: string; readonly accepted: true; }
export interface ProgramRequestAddress extends ProgramAddress { readonly requestId: string; }
export interface ProgramRequestStatus { readonly requestId: string; readonly state: 'absent' | 'declared' | 'accepted' | 'consumed' | 'settled'; }
/** Program journal 的纯 wire 合同；Host 的 Controller 页直接按此结构核验。 */
export interface ProgramJournalEvent {
  readonly type: string;
  readonly seq: number;
  readonly time: number;
  readonly data: JsonValue;
  readonly ignorable?: true;
  readonly sourceEventSeqs?: JsonValue;
  readonly surfaceOp?: JsonValue;
}
export interface ProgramJournalEntry { readonly type: 'event'; readonly event: ProgramJournalEvent; }
export interface ProgramJournalPage { readonly records: readonly ProgramJournalEntry[]; readonly hasMore: boolean; }
export interface ProgramJournalWindow {
  readonly maxMessages?: number;
  readonly turnWindow?: { readonly minMessages: number; readonly minTurns: number };
}
export interface ProgramJournalOpenRequest extends ProgramAddress, ProgramJournalWindow {}
export interface ProgramJournalPageRequest extends ProgramJournalOpenRequest { readonly throughSeq: number; readonly beforeSeq?: number; }
/** 原生水位和原生事件的只读载体；历史与连续性仍由 Controller/Gateway 拥有。 */
export type ProgramJournalFrame = { readonly type: 'opened'; readonly cursor: number; readonly page: ProgramJournalPage }
  | { readonly type: 'entry'; readonly entry: ProgramJournalEntry };
/** 异常通过 Gateway 正式 error 投影，不能把本地 Error 当 JSON 对象发送。 */
type WireEvent = ConversationEvent<JsonValue>;
export type ProgramConversationEvent = Exclude<WireEvent, { kind: 'execution.error' | 'tool.settled' | 'wait.opened' | 'wait.resolved' }>
  | (Omit<Extract<WireEvent, { kind: 'execution.error' }>, 'error'> & { readonly error: ProgramFailure })
  | (Omit<Extract<WireEvent, { kind: 'tool.settled' }>, 'result' | 'meta'> & { readonly result?: JsonValue; readonly meta?: JsonValue })
  | (Omit<Extract<WireEvent, { kind: 'wait.opened' | 'wait.resolved' }>, 'continuation' | 'wait'> & {
    readonly continuation: Omit<Continuation, 'inputs'> & { readonly inputs: readonly InputReference<JsonValue>[] };
    readonly wait: { readonly id: string; readonly operation: JsonValue };
  });
/** 仅开场传完整 compact prefix；后续使用官方原始帧，不重复发送累计快照。 */
export type ProgramProgressFrame =
  | { readonly type: 'baseline'; readonly observation: WireObservation }
  | { readonly type: 'activity'; readonly activity: JsonValue }
  | { readonly type: 'assistant'; readonly progress: JsonValue }
  | { readonly type: 'event'; readonly event: ProgramConversationEvent };
export interface ProgramViewRequest extends ProgramAddress { readonly name: string; }
/** scope 是调用方明确期望的原始业务目标，只在宿主的合同边界解析。 */
export interface ProgramActionBindingRequest extends ProgramAddress { readonly name: string; readonly scope: JsonValue; }
/** 进程内真实绑定的身份；它不是权限凭据，也不承诺跨重启仍然有效。 */
export interface ProgramActionBinding { readonly bindingId: string; readonly scope: JsonValue; }
export interface ProgramActionRequest extends ProgramAddress { readonly name: string; readonly bindingId: string; readonly input: JsonValue; }
export interface ProgramInvocationAddress extends ProgramAddress { readonly invocationId: string; }
export interface ProgramOperationIdentity { readonly kind: string; readonly version: number; }
export interface ProgramOperationRequest extends ProgramAddress { readonly operation: ProgramOperationIdentity; readonly reference: JsonValue; }
export interface ProgramOperationLocateRequest extends ProgramAddress { readonly operation: ProgramOperationIdentity; readonly command: JsonValue; }
/** 读取当前公开槽的真实绑定，不登记参与或取得原生 Goal 续接权。 */
export interface ProgramOperationBindingRequest extends ProgramAddress { readonly name: string; }
export interface ProgramOperationBinding { readonly operation: ProgramOperationIdentity; readonly command: JsonValue; }
/** 显式恢复原 command 与已登记的参与身份，独立于当前组件选择。 */
export interface ProgramOperationFeedbackRequest extends ProgramOperationLocateRequest { readonly participationId: string; }
export type ProgramOperationFeedbackRead =
  | { readonly status: 'unknown' }
  | { readonly status: 'rejected'; readonly reason: JsonValue }
  | { readonly status: 'pending'; readonly reference: JsonValue }
  | { readonly status: 'ready'; readonly reference: JsonValue; readonly node: { readonly id: string; readonly payload: JsonValue } };
export type ProgramOperationLocation =
  | { readonly status: 'accepted'; readonly reference: JsonValue; readonly state: JsonValue }
  | { readonly status: 'rejected'; readonly reason: JsonValue }
  | { readonly status: 'unknown' };
export interface ProgramOperationNotification extends ProgramAddress { readonly followId: string; }
export type ProgramOperationNotificationResult = { readonly status: 'unknown' | 'pending' | 'not-started' }
  | { readonly status: 'accepted'; readonly receipt: ProgramInputReceipt };
type WireHumanQuestion<Q extends { readonly origin: HumanQuestionOrigin }> = Omit<Q, 'origin'> & {
  readonly origin: Omit<Q['origin'], 'inputs'> & { readonly inputs: readonly InputReference<JsonValue>[] };
};
export interface ProgramQuestions {
  readonly questions: Omit<HumanInputState, 'active' | 'settled'> & {
    readonly active: readonly WireHumanQuestion<HumanInputState['active'][number]>[];
    readonly settled: readonly WireHumanQuestion<HumanInputState['settled'][number]>[];
  };
  readonly foreground: readonly WireHumanQuestion<HumanForegroundQuestion>[];
}
export type ProgramQuestionsFrame = { readonly type: 'baseline'; readonly value: ProgramQuestions }
  | { readonly type: 'questions'; readonly value: ProgramQuestions };
export interface ProgramQuestionRequest extends ProgramAddress { readonly callId: string; }
export interface ProgramAnswerRequest extends ProgramQuestionRequest { readonly answer: HumanAnswer; }
export type ProgramWaitFrame = HumanWaitFrame;
/** id/source 由宿主真实受理入口签发，不是客户端生成的任务或幂等键。 */
export interface ProgramActionStart { readonly id: string; readonly name: string; readonly source: ActionSource; }
export type ProgramInvocationFrame =
  | { readonly state: 'pending' | 'running' }
  | { readonly state: 'returned'; readonly observation?: JsonValue; readonly observationError?: ProgramFailure }
  | { readonly state: 'failed'; readonly phase: 'input' | 'admission' | 'execute' | 'output' | 'operation'; readonly error: ProgramFailure };
