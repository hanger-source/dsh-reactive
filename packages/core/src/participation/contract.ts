import { z } from 'zod';
import type { ToolCallId } from '@deepseek-ai/dsh-llm/brand';
import type { ToolOccurrence } from '../conversations/events.ts';

/** 原生问题和答案的公开词汇；本层不保存另一份问题生命周期。 */
export interface HumanQuestionOrigin extends Readonly<ToolOccurrence> {
  readonly sessionId: string;
}
export interface HumanQuestion {
  readonly callId: ToolCallId;
  readonly questions: readonly HumanQuestionItem[];
  readonly state: 'open' | 'continued';
  readonly origin: HumanQuestionOrigin;
}
export interface HumanSettledQuestion {
  readonly callId: ToolCallId;
  readonly answers: readonly HumanAnswerItem[];
  readonly origin: HumanQuestionOrigin;
}
/** 原生投影的数据合同；不携带 Cordis 事件扩展或问答服务实现。 */
export interface HumanQuestionItem {
  id: string;
  question: string;
  detail?: string;
  header?: string;
  options?: { label: string; description?: string }[];
  multiSelect?: boolean;
  intent?: { kind: 'plan-review'; approve: string; callId?: ToolCallId };
}
export interface HumanAnswerItem { id: string; selected: string[]; custom?: string; }
export interface HumanAnswer { answers: HumanAnswerItem[]; }
/** 主动提问只接受标准工具实际传递的字段；不承诺 detail 或 intent 的展示语义。 */
export interface HumanPromptOption {
  readonly label: string;
  readonly description?: string;
}
export interface HumanPrompt {
  readonly id: string;
  readonly question: string;
  readonly header?: string;
  readonly options?: readonly HumanPromptOption[];
  readonly multiSelect?: boolean;
  readonly detail?: never;
  readonly intent?: never;
}
export interface HumanAskOptions { readonly timeoutSeconds: number; }
export interface HumanPendingAnswer { readonly pending: true; readonly callId: string; }
export type HumanAskResult = HumanAnswer | HumanPendingAnswer;
export interface HumanInputState {
  readonly active: readonly HumanQuestion[];
  readonly settled: readonly HumanSettledQuestion[];
}
/** 实际 waterfall 的前台请求；PTC 前台请求尚不是持久的 open projection。 */
export interface HumanForegroundQuestion {
  readonly callId: HumanQuestion['callId'];
  readonly questions: readonly HumanQuestionItem[];
  readonly timed: boolean;
  readonly origin: HumanQuestionOrigin;
}

/** claim 的 answer 为 null；提交答案时业务同时核验原问题、完整答案和真实响应者。 */
export interface HumanResponderAdmission {
  readonly channel: 'foreground' | 'continued';
  readonly question: HumanQuestion;
  readonly answer: HumanAnswer | null;
  readonly actorId: string;
  readonly signal: AbortSignal;
}
export interface HumanInputPolicy {
  admitResponder(admission: HumanResponderAdmission): void | Promise<void>;
}

/** 只在信任边界验证原生答案批次；问题事实仍由原生投影提供。 */
export const humanAnswerSchema: z.ZodType<HumanAnswer> = z.object({ answers: z.array(z.object({
  id: z.string(), selected: z.array(z.string()), custom: z.string().optional(),
}).strict()) }).strict();

/** @internal 调用开始即取得自有输入；未知字段不能在工具转换时被静默丢弃。 */
export const humanPromptsSchema = z.array(z.object({
  id: z.string().min(1), question: z.string().min(1), header: z.string().optional(),
  options: z.array(z.object({ label: z.string(), description: z.string().optional() }).strict()).optional(),
  multiSelect: z.boolean().optional(),
}).strict()).min(1).superRefine((questions, context) => {
  if (new Set(questions.map(question => question.id)).size !== questions.length)
    context.addIssue({ code: 'custom', message: 'question ids must be unique within this call' });
});
/** @internal 沿原生计时服务的正整数和平台上限，不用超时模拟答复。 */
export const humanAskOptionsSchema = z.object({ timeoutSeconds: z.number().int().positive().max(2_147_483) }).strict();
/** @internal 宿主能力返回的正式答案或已超时问题身份。 */
export const humanAskResultSchema: z.ZodType<HumanAskResult> = z.union([
  humanAnswerSchema, z.object({ pending: z.literal(true), callId: z.string().min(1) }).strict(),
]);

/** actorId 由宿主认证入口提供；存在身份本身不授予响应权限。 */
export interface HumanResponderOptions {
  readonly actorId: string;
  readonly signal?: AbortSignal;
}
export interface HumanWaitFrame { readonly remainingMs: number; }
