import { z } from 'zod';
import { defineAction } from '../actions/contract.ts';
import { defineSurface, resourceView } from '../surfaces/contract.ts';
import type { ConversationIdentity } from '../conversations/conversation.ts';

/** 仅标识当前真实等待；transportId 不是原生审计 requestId，也不是 grant。 */
export const approvalPendingRequestSchema = z.object({
  transportId: z.uuid(), toolName: z.string(), callId: z.string().nullable(),
  reason: z.string().nullable(), displayReason: z.record(z.string(), z.string()).nullable(),
}).strict();
export type ApprovalPendingRequest = z.infer<typeof approvalPendingRequestSchema>;
export const approvalDecisionSchema = z.enum(['allowed-once', 'rejected']);
export type ApprovalDecision = z.infer<typeof approvalDecisionSchema>;

/** 真实 human 来源经宿主签发后，业务仍须逐次核验响应权限。 */
export interface ApprovalResponderAdmission {
  readonly identity: ConversationIdentity;
  readonly request: Readonly<ApprovalPendingRequest>;
  readonly decision: ApprovalDecision;
  readonly actorId: string;
  readonly signal: AbortSignal;
}

const answerApproval = defineAction({
  name: 'answer_approval', description: '向当前原生审批等待者提交人工答复',
  scope: z.object({ sessionId: z.string().min(1) }).strict(),
  input: z.object({ transportId: z.uuid(), decision: approvalDecisionSchema }).strict(),
  // 交付回执不声明 grant 已生效；实际政策、审计和执行仍归原生服务。
  output: z.object({ status: z.literal('submitted') }).strict(),
  concurrency: 'parallel',
});

export const approvalSurface = defineSurface({
  views: { pending: resourceView(approvalPendingRequestSchema.array().readonly()) },
  actions: { answer: answerApproval },
});
