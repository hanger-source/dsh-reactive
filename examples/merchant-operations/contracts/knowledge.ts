import { defineAction, defineOperation, z } from '@hanger-source/dsh-reactive';
import { invocationIdentitySchema, onboardingTargetSchema, revisionReceiptSchema } from './identity.ts';

export const knowledgeTaskStateSchema = onboardingTargetSchema.extend({ submissionId: z.string().uuid(),
  activationId: z.string().min(1), taskId: z.string().min(1).nullable(), rejectionCode: z.string().nullable(),
  taskStatus: z.enum(['queued', 'running', 'completed', 'failed', 'cancelled']).nullable(), reasonCode: z.string().nullable(),
});
export const KnowledgeTask = defineOperation({ kind: 'merchant-operations.knowledge-task', version: 1,
  command: invocationIdentitySchema.extend(onboardingTargetSchema.shape),
  reference: onboardingTargetSchema.extend({ submissionId: z.string().uuid() }),
  state: knowledgeTaskStateSchema, feedback: knowledgeTaskStateSchema,
  settled: state => state.rejectionCode !== null || state.taskStatus === 'completed' || state.taskStatus === 'failed' || state.taskStatus === 'cancelled'
    ? state : undefined,
});
export const InitializeKnowledge = defineAction({ name: 'initialize_knowledge', description: '按明确历史窗口受理知识任务；未知受理只定位原提交，不重发 POST',
  scope: onboardingTargetSchema,
  input: z.object({ expectedRevision: z.number().int().positive(), request: z.object({ kind: z.literal('historical-consultations'),
    window: z.object({ startAt: z.iso.datetime(), endAt: z.iso.datetime() }).strict() }).strict() }).strict(),
  output: KnowledgeTask.reference,
  operation: { definition: KnowledgeTask, command: (_input, target, origin) => ({ ...target,
    sessionId: origin.sessionId, invocationId: origin.invocationId }), reference: receipt => receipt },
});
export const RetryKnowledge = defineAction({ name: 'retry_knowledge', description: '只重试已核实失败的原知识任务，保留原任务和当前来源身份',
  scope: onboardingTargetSchema,
  input: z.object({ expectedRevision: z.number().int().positive(), taskId: z.string().min(1), activationId: z.string().min(1) }).strict(),
  output: KnowledgeTask.reference,
  operation: { definition: KnowledgeTask, command: (_input, target, origin) => ({ ...target,
    sessionId: origin.sessionId, invocationId: origin.invocationId }), reference: receipt => receipt },
});
export const CommitKnowledge = defineAction({ name: 'commit_knowledge', description: '按当前完整知识 snapshot 固定接入单知识上下文，不表示商家认可或接待开通',
  scope: onboardingTargetSchema, input: z.object({ expectedRevision: z.number().int().positive() }).strict(), output: revisionReceiptSchema,
});
export const knowledgeFactsSchema = onboardingTargetSchema.extend({ revision: z.number().int().positive(),
  canInitialize: z.boolean(), canRetry: z.boolean(), canCommit: z.boolean(), sourceActivationId: z.string().min(1).nullable(),
  ready: z.boolean(), snapshotReasonCode: z.string().nullable(), tasks: knowledgeTaskStateSchema.array(),
  currentTask: KnowledgeTask.command.nullable(),
});
export type KnowledgeFacts = z.output<typeof knowledgeFactsSchema>;
