import { defineAction, z } from '@hanger-source/dsh-reactive';

export const stockTargetSchema = z.object({ warehouseId: z.string().min(1), sku: z.string().min(1) }).strict();
export type StockTarget = z.output<typeof stockTargetSchema>;
export const RequestReplenishment = defineAction({ name: 'request_replenishment', description: '按明确仓库和 SKU 的原库存修订受理补货请求',
  scope: stockTargetSchema, input: z.object({ expectedRevision: z.number().int().positive(), quantity: z.number().int().positive() }).strict(),
  output: z.object({ requestId: z.string().min(1), accepted: z.literal(true) }).strict() });
