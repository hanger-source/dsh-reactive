import { z } from '@hanger-source/dsh-reactive';

export const onboardingTargetSchema = z.object({ onboardingId: z.string().uuid() }).strict();
export const merchantTargetSchema = z.object({
  subject: z.object({ kind: z.literal('merchant'), id: z.string().min(1) }).strict(),
  sandboxId: z.string().min(1), deliveryIdentity: z.string().regex(/^sha256:[a-f0-9]{64}$/u),
}).strict();
export const invocationIdentitySchema = z.object({ sessionId: z.string().min(1), invocationId: z.string().min(1) }).strict();
export const revisionReceiptSchema = onboardingTargetSchema.extend({ revision: z.number().int().positive() });
export type OnboardingTarget = z.output<typeof onboardingTargetSchema>;
export type MerchantTarget = z.output<typeof merchantTargetSchema>;

/** 既有 provider 的明确操作；不在 reference 中编排部署计划。 */
export const providerCommandSchema = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('shell'), command: z.string().min(1), inputCommandId: z.string().uuid().optional() }).strict(),
  z.object({ kind: z.literal('register-service'), service: z.object({ name: z.string().min(1), start: z.string().min(1),
    dir: z.string().startsWith('/'), source: z.string().min(1), port: z.number().int().min(1024).max(65535).optional() }).strict() }).strict(),
  z.object({ kind: z.literal('service-action'), name: z.string().min(1), action: z.enum(['start', 'stop', 'restart', 'unregister']) }).strict(),
  z.object({ kind: z.literal('register-route'), path: z.string().startsWith('/'), upstream: z.string().min(1), stripPrefix: z.boolean() }).strict(),
  z.object({ kind: z.literal('unregister-route'), path: z.string().startsWith('/') }).strict(),
]);
