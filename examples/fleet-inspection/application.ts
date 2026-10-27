import { defineApplication, type ApplicationDefinition } from '@hanger-source/dsh-reactive';
import { InspectionTarget } from './contracts.ts';
import type { InspectionCapabilities } from './services.ts';
import { createInspectionEnvironment } from './program.tsx';

/** 业务按部署选择站点环境；工作区创建、冷打开和远程参与直接使用正式宿主/SDK。 */
export function createFleetApplication(capabilities: InspectionCapabilities,
  sites: readonly { readonly environmentId: string; readonly siteId: string }[],
  admit: ApplicationDefinition['admit']) {
  const selected = new Map(sites.map(site => [site.environmentId, site.siteId]));
  return defineApplication({ name: 'fleet-inspection',
    environments: sites.map(site => createInspectionEnvironment(site.environmentId, site.siteId, capabilities)),
    async admit(input, conversation, admission) {
      const target = InspectionTarget.parse(input.scope);
      if (target.workspaceId !== conversation.workspaceId || target.siteId !== selected.get(conversation.environmentId)) {
        throw new Error('INSPECTION_TARGET_NOT_ADMITTED');
      }
      await admit(input, conversation, admission);
    },
  });
}
