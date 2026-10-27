import { exhaustMap, mergeMap, switchMap, filter, type Observable } from 'rxjs';
import { bindReaction, defineReaction, implementReaction, z } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/reactivity';
import type { NativeSessionJobs, NativeJobSpec } from '@hanger-source/dsh-reactive-dsh/jobs';
import { stockEventSchema, type StockEvent } from './reactions.ts';
import { stockTargetSchema, type StockTarget } from './inventory-contract.ts';

const StartInventoryJob = defineReaction({ name: 'inventory.start-native-job', params: stockTargetSchema,
  event: stockEventSchema, result: z.object({ jobId: z.string().min(1) }).strict() });

/** 会话 SDK 已绑定真实 owner；业务只提供 producer，不接触原生注册表或工具来源。 */
export function inventoryJobReaction(scope: Scope, events$: Observable<StockEvent>, jobs: NativeSessionJobs,
  job: (event: StockEvent, target: StockTarget) => NativeJobSpec) {
  return bindReaction(implementReaction(StartInventoryJob, {
    observe: target => events$.pipe(filter(event => event.warehouseId === target.warehouseId && event.sku === target.sku)),
    operator: mergeMap,
    execute: (event, { params }) => ({ jobId: jobs.start(job(event, params)) }),
  }), { scope });
}

/** 对纯读取使用 switchMap；对完成前忽略后继事件使用 exhaustMap，均为原生 RxJS 算子。 */
export function inventoryInspectionReactions(scope: Scope, events$: Observable<StockEvent>,
  inspect: (event: StockEvent) => Promise<string>) {
  const Inspect = defineReaction({ name: 'inventory.inspect', params: stockTargetSchema, event: stockEventSchema, result: z.string() });
  const observe = (target: StockTarget) => events$.pipe(filter(event => event.warehouseId === target.warehouseId && event.sku === target.sku));
  return {
    latest: bindReaction(implementReaction(Inspect, { observe, operator: switchMap, execute: inspect }), { scope }),
    untilSettled: bindReaction(implementReaction(Inspect, { observe, operator: exhaustMap, execute: inspect }), { scope }),
  };
}
