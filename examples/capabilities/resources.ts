import { bindResource, combineResources, defineResource, implementResource, mapResource, z,
  type Observation, type ResourceBinding } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/reactivity';
import type { Observable } from 'rxjs';
import { stockTargetSchema, type StockTarget } from './inventory-contract.ts';


export const Stock = defineResource({ name: 'inventory.stock', params: stockTargetSchema,
  value: stockTargetSchema.extend({ revision: z.number().int().positive(), available: z.number().int().nonnegative() }).strict() });
export const Quote = defineResource({ name: 'inventory.quote', params: stockTargetSchema,
  value: z.object({ revision: z.number().int().positive(), currency: z.string().min(1), amount: z.number().nonnegative() }).strict() });
export const Availability = defineResource({ name: 'inventory.availability', params: stockTargetSchema,
  value: stockTargetSchema.extend({ stockRevision: z.number().int().positive(), quoteRevision: z.number().int().positive(), canOrder: z.boolean() }).strict() });

/** 现有业务 owner 的真实读接口；这里没有后端实现、默认事实或权限 ACK。 */
export interface InventoryServices {
  observeStock(target: StockTarget, signal: AbortSignal): Observable<Observation<z.input<typeof Stock.value>>>;
  readQuote(target: StockTarget, signal: AbortSignal): Promise<z.input<typeof Quote.value>>;
  quoteInvalidations(target: StockTarget): Observable<unknown>;
}

/** 服务依赖在实现闭包固定；scope 决定实例共享范围及查询寿命。 */
export function createInventorySources(services: InventoryServices) {
  const stocks = implementResource(Stock, {
    observe: (target, { signal }) => services.observeStock(target, signal).pipe(mapResource(value => {
      if (value.warehouseId !== target.warehouseId || value.sku !== target.sku) throw new Error('INVENTORY_STOCK_TARGET_MISMATCH');
      return value;
    })),
  });
  const quotes = implementResource(Quote, {
    read: (target, { signal }) => services.readQuote(target, signal),
    invalidations: target => services.quoteInvalidations(target),
  });
  return { stocks, quotes };
}

/** 明确交给工作区等共享 owner 时，普通 SDK 与组件可以继续共享这些绑定。 */
export function createInventoryResources(scope: Scope, services: InventoryServices) {
  const sources = createInventorySources(services);
  const stocks = bindResource(sources.stocks, { scope });
  const quotes = bindResource(sources.quotes, { scope });
  const availability = bindResource(implementResource(Availability, {
    observe: target => combineResources({ stock: stocks.instance(target).observations$, quote: quotes.instance(target).observations$ })
      .pipe(mapResource(({ stock, quote }) => ({ ...target, stockRevision: stock.revision, quoteRevision: quote.revision,
        canOrder: stock.available > 0 && quote.amount > 0 }))),
  }), { scope });
  return { stocks, quotes, availability };
}

/** 普通代码与 callback/Observable 共用同一实例；read 的 signal 只取消这次等待。 */
export async function inspectAvailability(resources: ResourceBinding<typeof Availability>, target: StockTarget,
  signal: AbortSignal, onState: (state: Observation<z.output<typeof Availability.value>>) => void) {
  const current = resources.instance(target);
  const stop = current.subscribe(onState);
  try {
    const value = await current.read(signal);
    return { current, value };
  } finally { stop(); }
}

/** 正式业务写入后的显式失效；不会重发该写入或自动启动模型。 */
export function invalidateInventory(resources: ReturnType<typeof createInventoryResources>, target: StockTarget) {
  resources.stocks.instance(target).invalidate();
  resources.quotes.instance(target).invalidate();
}
