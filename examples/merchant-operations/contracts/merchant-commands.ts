import { defineAction, defineOperation, z } from '@hanger-source/dsh-reactive';
import { invocationIdentitySchema, merchantTargetSchema, providerCommandSchema } from './identity.ts';

export const merchantCommandReferenceSchema = merchantTargetSchema.pick({ subject: true, sandboxId: true }).extend({ commandId: z.string().uuid() });
export const merchantCommandStateSchema = merchantCommandReferenceSchema.extend({ deliveryIdentity: z.string().regex(/^sha256:[a-f0-9]{64}$/u),
  sessionId: z.string().min(1), providerSessionId: z.string().nullable(), observedAt: z.iso.datetime(),
  state: z.enum(['claimed', 'running', 'completed', 'unknown']), exitCode: z.number().int().nullable(),
  output: z.string().nullable(), reasonCode: z.string().nullable(),
});
export const MerchantCommand = defineOperation({ kind: 'merchant-operations.merchant-command', version: 1,
  command: invocationIdentitySchema.extend(merchantTargetSchema.shape), reference: merchantCommandReferenceSchema,
  state: merchantCommandStateSchema, feedback: merchantCommandStateSchema,
  settled: state => state.state === 'completed' ? state : undefined,
});
export const merchantCommandSchema = z.discriminatedUnion('kind', [
  ...providerCommandSchema.options,
  z.object({ kind: z.literal('publish'), artifact: z.enum(['application', 'kit', 'upgrade-kit']) }).strict(),
]);
export const ExecuteMerchantCommand = defineAction({ name: 'execute_merchant_command', description: '对明确 merchantKey、Sandbox 与交付身份执行一次明确命令',
  scope: merchantTargetSchema, input: z.object({ command: merchantCommandSchema }).strict(), output: MerchantCommand.reference,
  operation: { definition: MerchantCommand, command: (_input, target, origin) => ({ ...target,
    sessionId: origin.sessionId, invocationId: origin.invocationId }), reference: receipt => receipt },
});
export const ObserveMerchantCommand = defineAction({ name: 'observe_merchant_command', description: '只观察原 commandId 和原 provider session，不重发未知命令',
  scope: merchantTargetSchema, input: z.object({ commandId: z.string().uuid() }).strict(), output: merchantCommandStateSchema,
});
export const ReconcileMerchantCommand = defineAction({ name: 'reconcile_merchant_command', description: '按既有服务或路由登记事实核对原命令，不能重新 execute',
  scope: merchantTargetSchema, input: z.object({ commandId: z.string().uuid() }).strict(), output: merchantCommandStateSchema,
});
export const merchantCommandFactsSchema = z.object({ target: merchantTargetSchema,
  canExecute: z.boolean(), canObserve: z.boolean(), canReconcile: z.boolean(), commands: merchantCommandStateSchema.array(),
  currentCommand: MerchantCommand.command.nullable(),
}).strict();
export type MerchantCommandFacts = z.output<typeof merchantCommandFactsSchema>;
