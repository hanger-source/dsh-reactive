import { defineAction, defineOperation, z } from '@hanger-source/dsh-reactive';
import { invocationIdentitySchema, onboardingTargetSchema, providerCommandSchema } from './identity.ts';

export const preparationStateSchema = onboardingTargetSchema.extend({ operationId: z.string().uuid(),
  deliveryIdentity: z.string().regex(/^sha256:[a-f0-9]{64}$/u), updatedAt: z.iso.datetime(),
  status: z.enum(['queued', 'running', 'awaiting-command', 'observation-required', 'prepared']), errorCode: z.string().nullable(),
});
const preparationReferenceSchema = onboardingTargetSchema.extend({ operationId: z.string().uuid() });
export const Preparation = defineOperation({ kind: 'merchant-operations.preparation', version: 1,
  command: invocationIdentitySchema.extend(onboardingTargetSchema.shape), reference: preparationReferenceSchema,
  state: preparationStateSchema, feedback: preparationStateSchema,
  settled: state => state.status === 'prepared' ? state : undefined,
});
export const deliveryCommandStateSchema = preparationReferenceSchema.extend({ deliveryCommandId: z.string().uuid(),
  settled: z.boolean(), output: z.json().nullable(), reasonCode: z.string().nullable(),
});
export const DeliveryCommand = defineOperation({ kind: 'merchant-operations.preparation-command', version: 1,
  command: invocationIdentitySchema.extend(preparationReferenceSchema.shape),
  reference: preparationReferenceSchema.extend({ deliveryCommandId: z.string().uuid() }),
  state: deliveryCommandStateSchema, feedback: deliveryCommandStateSchema,
  settled: state => state.settled ? state : undefined,
});

const instance = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('kit-input'), commandId: z.string().uuid() }).strict(),
  z.object({ kind: z.literal('source'), operationId: z.string().uuid() }).strict(),
]);
export const preparationCommandSchema = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('inspect') }).strict(),
  z.object({ kind: z.literal('create-volume'), sizeGi: z.number().int().positive().max(100) }).strict(),
  z.object({ kind: z.literal('create-sandbox'), cpu: z.number().positive(), memoryMiB: z.number().int().positive() }).strict(),
  z.object({ kind: z.literal('publish'), artifact: z.enum(['application', 'kit', 'upgrade-kit']) }).strict(),
  z.object({ kind: z.literal('instance-input'), mode: z.enum(['build', 'upgrade']),
    definition: z.record(z.string(), z.json()).optional(), agentConfiguration: z.record(z.string(), z.json()).optional() }).strict(),
  z.object({ kind: z.literal('reuse-instance'), instance }).strict(),
  z.object({ kind: z.literal('source-stop-input') }).strict(),
  ...(['render-host', 'render-ingress', 'inspect-instance', 'verify-runtime', 'configure-ai', 'configure-humans', 'verify'] as const)
    .map(kind => z.object({ kind: z.literal(kind), instance }).strict()),
  ...providerCommandSchema.options,
]);
export const PrepareMerchant = defineAction({ name: 'prepare_merchant', description: '固定接入单、修订与交付身份，受理准备操作',
  scope: onboardingTargetSchema,
  input: z.object({ expectedRevision: z.number().int().positive(), deliveryIdentity: z.string().regex(/^sha256:[a-f0-9]{64}$/u) }).strict(),
  output: Preparation.reference,
  operation: { definition: Preparation, command: (_input, target, origin) => ({ ...target, sessionId: origin.sessionId, invocationId: origin.invocationId }), reference: receipt => receipt },
});
export const RunPreparationCommand = defineAction({ name: 'run_preparation_command', description: '执行原准备操作中明确选择的一个部署函数，不自动选择下一动作',
  scope: onboardingTargetSchema,
  input: z.object({ operationId: z.string().uuid(), expectedUpdatedAt: z.iso.datetime(), command: preparationCommandSchema }).strict(),
  output: DeliveryCommand.reference,
  operation: { definition: DeliveryCommand, command: (input, target, origin) => ({ ...target, operationId: input.operationId,
    sessionId: origin.sessionId, invocationId: origin.invocationId }), reference: receipt => receipt },
});
export const ObservePreparationCommand = defineAction({ name: 'observe_preparation_command', description: '沿原 operation 与 deliveryCommandId 观察或续行，不重发未知命令',
  scope: onboardingTargetSchema, input: z.object({ operationId: z.string().uuid(), deliveryCommandId: z.string().uuid() }).strict(),
  output: deliveryCommandStateSchema,
});
export const preparationFactsSchema = onboardingTargetSchema.extend({ revision: z.number().int().positive(),
  canPrepare: z.boolean(), canRunCommand: z.boolean(), canObserveCommand: z.boolean(),
  preparation: preparationStateSchema.nullable(), command: deliveryCommandStateSchema.nullable(),
  currentPreparation: Preparation.command.nullable(), currentDeliveryCommand: DeliveryCommand.command.nullable(),
});
export type PreparationFacts = z.output<typeof preparationFactsSchema>;
