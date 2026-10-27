import { Context, Expose, HumanInput, Offer, implementProgram, useAction, useResource } from '@hanger-source/dsh-reactive/program';
import { defineEnvironment } from '@hanger-source/dsh-reactive';
import { InspectionProgram, InspectionSurface } from './contracts.ts';
import type { InspectionCapabilities } from './services.ts';

/** 同一组能力可组合为多个站点环境，工作区身份取自实际 ProgramWorkspace。 */
export function createInspectionEnvironment(id: string, siteId: string, capabilities: InspectionCapabilities) {
  return defineEnvironment({ id, instructions: [
    '只处理当前站点。巡检由服务显式执行，收到异常报告后沿原inspectionId调查。',
    '需要人的意见时调用discuss_inspection；回答是业务信息，不扩大原操作权限。',
  ].join('\n'), program: implementProgram(InspectionProgram, { Component({ workspace }) {
    const current = useResource(capabilities.facts, { siteId });
    const inspect = useAction(capabilities.inspect, () => ({ workspaceId: workspace.id, siteId }), [workspace.id, siteId]);
    const discuss = useAction(capabilities.discuss, () => ({ workspaceId: workspace.id, siteId }), [workspace.id, siteId]);
    return <>
      <Context name="site" observation={current.state} render={value => value} />
      <HumanInput admitResponder={capabilities.human.admitResponder} />
      <Offer action={discuss} enabled={current.state.status === 'ready'} />
      <Expose surface={InspectionSurface} value={{ views: { facts: current.observations$ }, actions: { inspect, discuss } }} />
    </>;
  } }) });
}
