import { useObservable, useResource } from '@hanger-source/dsh-reactive/program';
import { combineResources, mapResource, type ReactionRun, type ReactionContract } from '@hanger-source/dsh-reactive';
import type { createInventorySources } from './resources.ts';
import type { StockTarget } from './inventory-contract.ts';

/** 程序推导绑定与共享 owner；业务 hook 只选择目标并派生库存和报价的共同条件。 */
export function useInventory(resources: ReturnType<typeof createInventorySources>,
  target: StockTarget) {
  const stock = useResource(resources.stocks, target);
  const quote = useResource(resources.quotes, target);
  const { warehouseId, sku } = target;
  const availability = useResource(() => combineResources({ stock: stock.observations$, quote: quote.observations$ })
    .pipe(mapResource(({ stock, quote }) => ({ warehouseId, sku, stockRevision: stock.revision,
      quoteRevision: quote.revision, canOrder: stock.available > 0 && quote.amount > 0 }))),
  [stock.observations$, quote.observations$, warehouseId, sku]);
  return { stock, quote, availability };
}

/** 观察已经显式启动的反应；组件挂载、重排或多次观察均不重新启动它。 */
export function useReactionActivity<D extends ReactionContract>(run: ReactionRun<D>) {
  return useObservable(() => run.state$, [run]);
}
