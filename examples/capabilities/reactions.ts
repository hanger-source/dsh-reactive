import { concatMap, distinctUntilChanged, filter, map } from 'rxjs';
import { bindReaction, defineProgram, defineReaction, defineSurface, implementReaction, z,
  type ResourceBinding } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/reactivity';
import type { ApplicationHost } from '@hanger-source/dsh-reactive-dsh';
import { Stock } from './resources.ts';
import { RequestReplenishment, stockTargetSchema, type StockTarget } from './inventory-contract.ts';

export const InventoryProgram = defineProgram({ input: stockTargetSchema,
  surface: defineSurface({ views: {}, actions: { replenish: RequestReplenishment } }) });
export const stockEventSchema = Stock.value;
export type StockEvent = z.output<typeof stockEventSchema>;
export const Replenish = defineReaction({ name: 'inventory.low-stock', params: stockTargetSchema,
  event: stockEventSchema, result: RequestReplenishment.output });

/** 服务调用走固定 service-sdk 身份；观察不继承当前页面、原人的输入或 Goal。 */
export async function createReplenishmentReaction(scope: Scope, host: ApplicationHost, sessionId: string,
  stocks: ResourceBinding<typeof Stock>, quantity: number) {
  const participant = await host.service(sessionId, InventoryProgram);
  if (scope.disposed) { participant.release(); scope.assertActive(); }
  scope.own(() => participant.release());
  const implementation = implementReaction(Replenish, {
    observe: target => stocks.instance(target).observations$.pipe(
      filter(state => state.status === 'ready'),
      map(state => state.value),
      distinctUntilChanged((previous, current) => previous.revision === current.revision),
      filter(stock => stock.available === 0),
      map(stock => ({ ...stock })),
    ),
    operator: concatMap,
    execute(event, { params }) {
      const replenish = participant.resolveAction('replenish', params);
      // 原库存修订仍交正式 owner 核验。失败默认终止此反应，不自动重发未知命令。
      return replenish({ expectedRevision: event.revision, quantity }).result;
    },
  });
  return bindReaction(implementation, { scope });
}

/** start 是显式命令；多观察者共享此次运行，stop 不取消已受理补货。 */
export function startReplenishment(binding: Awaited<ReturnType<typeof createReplenishmentReaction>>, target: StockTarget) {
  const run = binding.instance(target).start();
  return { run, stop: () => run.stop(), settled: run.settled };
}
