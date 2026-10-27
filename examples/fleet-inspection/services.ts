import { createResource, implementAction, implementResource, mapResource, z,
  type ActionCallContext, type ActionParsedInput, type HumanAnswer, type HumanInputPolicy, type Scope } from '@hanger-source/dsh-reactive';
import type { Observable } from 'rxjs';
import { DiscussReport, Inspect, InspectionReport, SiteFacts, SiteReading, type InspectionTarget } from './contracts.ts';

/** 真正的事实、修订、业务副作用和响应者身份继续由应用已有服务决定。 */
export interface InspectionServices {
  readAll(signal: AbortSignal): Promise<readonly z.input<typeof SiteReading>[]>;
  readonly changes$: Observable<unknown>;
  inspect(input: ActionParsedInput<typeof Inspect>, call: ActionCallContext<InspectionTarget>): Promise<z.input<typeof InspectionReport>>;
  recordDiscussion(input: { readonly inspectionId: string; readonly answer: HumanAnswer },
    call: ActionCallContext<InspectionTarget>): Promise<{ readonly inspectionId: string; readonly recorded: true }>;
  readonly human: HumanInputPolicy;
}

/** 应用明确选择跨工作区共享的来源；各会话只公开自己站点的派生现场。 */
export function createInspectionCapabilities(scope: Scope, services: InspectionServices) {
  const readings = createResource(scope, { name: 'inspection.shared-readings', value: z.array(SiteReading),
    read: async ({ signal }) => [...await services.readAll(signal)], invalidations: () => services.changes$ });
  const facts = implementResource(SiteFacts, { observe: target => readings.observations$.pipe(mapResource(current => {
    const reading = current.find(value => value.siteId === target.siteId);
    if (!reading) throw new Error('INSPECTION_SITE_NOT_FOUND');
    return reading;
  })) });
  const inspect = implementAction(Inspect, { async execute(input, call) {
    const report = await services.inspect(input, call);
    if (report.siteId !== call.scope.siteId) throw new Error('INSPECTION_REPORT_TARGET_MISMATCH');
    readings.invalidate();
    return report;
  } });
  const discuss = implementAction(DiscussReport, { async execute(input, call) {
    // 只有真实原生工具调用能进入这条问答链；服务巡检自身不伪造问题或模型身份。
    const answer = await call.askHuman([{ id: 'inspection-opinion', question: input.question }]);
    return services.recordDiscussion({ inspectionId: input.inspectionId, answer }, call);
  } });
  return { readings, facts, inspect, discuss, human: services.human };
}
export type InspectionCapabilities = ReturnType<typeof createInspectionCapabilities>;
