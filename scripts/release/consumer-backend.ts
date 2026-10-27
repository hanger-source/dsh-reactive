import { defineApplication, defineEnvironment, defineProgram, defineSurface, defineResource, defineReaction,
  type ResourceImplementation, type ReactionImplementation, z } from '@hanger-source/dsh-reactive';
import { implementProgram, useResource, useResourceInstance, useReaction } from '@hanger-source/dsh-reactive/program';
import { createRuntime, type RuntimeOptions } from '@hanger-source/dsh-reactive-dsh';
import { createProgramClient } from '@hanger-source/dsh-reactive-client';
import { mountProgramGateway } from '@hanger-source/dsh-reactive-remote';
import { getDeveloperSkill } from '@hanger-source/dsh-reactive-skills';

const contract = defineProgram({ input: z.object({ subject: z.string() }).strict(), surface: defineSurface({ views: {}, actions: {} }) });
const program = implementProgram(contract, { Component: () => null });
const application = defineApplication({ name: 'registry-consumer', admit: async () => {}, environments: [
  defineEnvironment({ id: 'main', instructions: '发行边界消费，不运行模型', program }),
] });
export const backend = (options: RuntimeOptions) => createRuntime(() => application, options);
export const clientContract = () => contract;
export const publicEntries = [createProgramClient, mountProgramGateway, getDeveloperSkill] as const;
export const developerSkill = getDeveloperSkill();

const current = defineResource({ name: 'consumer-current', params: z.null(), value: z.object({ revision: z.number() }) });
const changes = defineReaction({ name: 'consumer-changes', params: z.null(), event: z.number(), result: z.string() });
export function useBackendBindings(resource: ResourceImplementation<typeof current>, reaction: ReactionImplementation<typeof changes>) {
  const instance = useResourceInstance(resource, null);
  return { resource: instance, observed: useResource(instance), reaction: useReaction(reaction, null) };
}
