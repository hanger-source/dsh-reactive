import { distinctUntilChanged, filter, map, concatMap } from 'rxjs';
import { bindReaction, defineReaction, implementReaction, z, type ResourceBinding } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/reactivity';
import type { ApplicationHost } from '@hanger-source/dsh-reactive-dsh';
import { InventoryProgram, stockEventSchema } from './reactions.ts';
import { Stock } from './resources.ts';
import { stockTargetSchema } from './inventory-contract.ts';

const StockNotice = defineReaction({ name: 'inventory.notify-conversation', params: stockTargetSchema,
  event: stockEventSchema,
  result: z.object({ sessionId: z.string().min(1), requestId: z.string().min(1), accepted: z.literal(true) }).strict() });

/** 会话由调用者明确选择；服务身份来自正式入口，不借页面参与者或原人的 Goal。 */
export async function createStockNotices(scope: Scope, host: ApplicationHost, sessionId: string,
  stocks: ResourceBinding<typeof Stock>) {
  const participant = await host.service(sessionId, InventoryProgram);
  if (scope.disposed) { participant.release(); scope.assertActive(); }
  scope.own(() => participant.release());
  return bindReaction(implementReaction(StockNotice, {
    observe: target => stocks.instance(target).observations$.pipe(
      filter(state => state.status === 'ready'), map(state => state.value),
      distinctUntilChanged((previous, current) => previous.revision === current.revision),
    ),
    operator: concatMap,
    execute: (event, { params }) => participant.submit({
      requestId: 'inventory:' + JSON.stringify([params.warehouseId, params.sku, event.revision]),
      content: '库存现场更新：' + JSON.stringify(event), intent: 'followup', delivery: 'inject', scope: params,
    }),
  }), { scope });
}
