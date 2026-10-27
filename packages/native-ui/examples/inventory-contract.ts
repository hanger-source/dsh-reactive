import { defineAction, defineOperation, defineProgram, defineSurface, resourceView, z } from '@hanger-source/dsh-reactive';

/** 后台与 UI 消费同一份纯合同；本文件不加载原生 UI 或后台 renderer。 */
export const inventoryTarget = z.object({ warehouseId: z.string(), sku: z.string() });
export const stockSchema = inventoryTarget.extend({ revision: z.number().int(), available: z.number().int() });
const replenishmentState = z.discriminatedUnion('phase', [
  z.object({ phase: z.literal('queued') }),
  z.object({ phase: z.literal('ordered') }),
  z.object({ phase: z.literal('received'), quantity: z.number().int() }),
  z.object({ phase: z.literal('failed'), reason: z.string() }),
]);
export const Replenishment = defineOperation({
  kind: 'inventory.replenishment', version: 1,
  command: inventoryTarget.extend({ requestId: z.string() }), reference: z.object({ orderId: z.string() }),
  state: replenishmentState,
  settled: state => state.phase === 'received' || state.phase === 'failed' ? state : undefined,
});
export const RequestReplenishment = defineAction({
  name: 'request_replenishment', description: '向正式库存服务请求补货', scope: inventoryTarget,
  input: z.object({ requestId: z.string(), quantity: z.number().int().positive() }), output: z.object({ orderId: z.string() }),
  operation: { definition: Replenishment,
    command: (input, target) => ({ ...target, requestId: input.requestId }), reference: receipt => receipt },
});
export const InventoryProgram = defineProgram({ input: inventoryTarget,
  surface: defineSurface({ views: { stock: resourceView(stockSchema) }, actions: { replenish: RequestReplenishment } }) });
