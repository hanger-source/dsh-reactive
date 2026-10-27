import { z } from '@hanger-source/dsh-reactive';
import { onboardingTargetSchema } from './identity.ts';
import { preparationStateSchema, type PreparationFacts } from './preparation.ts';
import { knowledgeTaskStateSchema, type KnowledgeFacts } from './knowledge.ts';
import { receptionStateSchema, type ReceptionFacts } from './reception.ts';
import { merchantCommandStateSchema } from './merchant-commands.ts';

export const knowledgePrerequisitesSchema = onboardingTargetSchema.extend({
  preparationReady: z.boolean(),
  knowledgeReady: z.boolean(),
  canInitializeKnowledge: z.boolean(),
  canRetryKnowledge: z.boolean(),
  canCommitKnowledge: z.boolean(),
});
export type KnowledgePrerequisites = z.output<typeof knowledgePrerequisitesSchema>;
export const collaborationFactsSchema = knowledgePrerequisitesSchema.extend({
  canEnableReception: z.boolean(),
  blockers: z.enum(['preparation-required', 'knowledge-required', 'reception-unavailable']).array(),
});
export type CollaborationFacts = z.output<typeof collaborationFactsSchema>;

const sourceStatus = z.enum(['pending', 'ready', 'error']);
const activeCalls = z.number().int().nonnegative();
/** 当前协作读侧；长期阶段来自各业务事实，调用数、人工和等待不合并成一个 busy。 */
export const deliveryActivitySchema = onboardingTargetSchema.extend({
  invocations: z.object({ preparation: activeCalls, knowledge: activeCalls, reception: activeCalls, commands: activeCalls }).strict(),
  sources: z.object({ preparation: sourceStatus, knowledge: sourceStatus, reception: sourceStatus, commands: sourceStatus }).strict(),
  operations: z.object({
    preparation: preparationStateSchema.shape.status.nullable(),
    knowledge: z.object({ submissionId: knowledgeTaskStateSchema.shape.submissionId, status: knowledgeTaskStateSchema.shape.taskStatus }).strict().array().nullable(),
    reception: receptionStateSchema.shape.status.nullable(),
    commands: z.object({ commandId: merchantCommandStateSchema.shape.commandId, state: merchantCommandStateSchema.shape.state }).strict().array().nullable(),
  }).strict(),
  foregroundQuestionIds: z.string().array(), pendingQuestionIds: z.string().array(), waitIds: z.string().array(),
  conversation: z.object({
    sessionId: z.string(), execution: z.enum(['idle', 'running', 'unavailable']),
    goal: z.object({ id: z.string(), revision: z.number().int().positive(), phase: z.enum(['active', 'paused', 'blocked', 'complete']),
      activation: z.enum(['armed', 'disarmed']).nullable() }).strict().nullable(),
  }).strict(),
}).strict();
export type DeliveryActivity = z.output<typeof deliveryActivitySchema>;

/** 知识动作只依赖准备和知识，不受下游接待查询的可用性影响。 */
export function deriveKnowledgePrerequisites({ preparation, knowledge }: {
  readonly preparation: PreparationFacts;
  readonly knowledge: KnowledgeFacts;
}): KnowledgePrerequisites {
  if (preparation.onboardingId !== knowledge.onboardingId) {
    throw new Error('MERCHANT_COLLABORATION_TARGET_MISMATCH');
  }
  const preparationReady = preparation.preparation?.status === 'prepared';
  const knowledgeReady = knowledge.ready && knowledge.snapshotReasonCode === null;
  return {
    onboardingId: preparation.onboardingId,
    preparationReady,
    knowledgeReady,
    canInitializeKnowledge: preparationReady && knowledge.canInitialize,
    canRetryKnowledge: preparationReady && knowledge.canRetry,
    canCommitKnowledge: preparationReady && knowledgeReady && knowledge.canCommit,
  };
}

/** 协作前提由当前事实交集派生；正式来源、授权与修订仍在业务写入边界核验。 */
export function deriveCollaboration({ preparation, knowledge, reception }: {
  readonly preparation: PreparationFacts;
  readonly knowledge: KnowledgeFacts;
  readonly reception: ReceptionFacts;
}): CollaborationFacts {
  const prerequisites = deriveKnowledgePrerequisites({ preparation, knowledge });
  if (prerequisites.onboardingId !== reception.onboardingId) throw new Error('MERCHANT_COLLABORATION_TARGET_MISMATCH');
  const blockers: CollaborationFacts['blockers'] = [];
  if (!prerequisites.preparationReady) blockers.push('preparation-required');
  if (!prerequisites.knowledgeReady) blockers.push('knowledge-required');
  if (!reception.canEnable) blockers.push('reception-unavailable');
  return {
    ...prerequisites,
    canEnableReception: prerequisites.preparationReady && prerequisites.knowledgeReady && reception.canEnable,
    blockers,
  };
}
