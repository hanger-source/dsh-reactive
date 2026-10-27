import { defineAction, defineOperation, z } from '@hanger-source/dsh-reactive';
import { invocationIdentitySchema, onboardingTargetSchema } from './identity.ts';

const reference = onboardingTargetSchema.extend({ operationId: z.string().uuid() });
export const receptionStateSchema = reference.extend({ preparationOperationId: z.string().uuid(), updatedAt: z.iso.datetime(),
  status: z.enum(['queued', 'running', 'observation-required', 'enabled']), errorCode: z.string().nullable(),
  result: z.object({ activationId: z.string().uuid(), servingAccountId: z.string(), assignmentRevision: z.number().int().positive(),
    canonicalEntryUrl: z.url(), evaluation: z.literal('not-run') }).strict().nullable(),
});
export const Reception = defineOperation({ kind: 'merchant-operations.reception', version: 1,
  command: invocationIdentitySchema.extend({ ...onboardingTargetSchema.shape, operationId: z.string().uuid().nullable(),
    expectedUpdatedAt: z.iso.datetime().nullable() }),
  reference, state: receptionStateSchema, feedback: receptionStateSchema,
  settled: state => state.status === 'enabled' ? state : undefined,
});
export const EnableReception = defineAction({ name: 'enable_reception', description: '在原明确开通要求内，按准备与知识上下文受理接待开通',
  scope: onboardingTargetSchema, input: z.object({ expectedRevision: z.number().int().positive(), evaluation: z.literal('skip') }).strict(),
  output: Reception.reference,
  operation: { definition: Reception, command: (_input, target, origin) => ({ ...target, operationId: null, expectedUpdatedAt: null,
    sessionId: origin.sessionId, invocationId: origin.invocationId }), reference: receipt => receipt },
});
export const ResumeReception = defineAction({ name: 'resume_reception', description: '核对原接待操作与修订后恢复，不另建未知开通操作',
  scope: onboardingTargetSchema, input: z.object({ operationId: z.string().uuid(), expectedUpdatedAt: z.iso.datetime() }).strict(),
  output: Reception.reference,
  operation: { definition: Reception, command: (input, target, origin) => ({ ...target, operationId: input.operationId, expectedUpdatedAt: input.expectedUpdatedAt,
    sessionId: origin.sessionId, invocationId: origin.invocationId }), reference: receipt => receipt },
});
export const receptionFactsSchema = onboardingTargetSchema.extend({ revision: z.number().int().positive(),
  canEnable: z.boolean(), canResume: z.boolean(), operation: receptionStateSchema.nullable(), currentReception: Reception.command.nullable(),
});
export type ReceptionFacts = z.output<typeof receptionFactsSchema>;
