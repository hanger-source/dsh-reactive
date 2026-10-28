import { defineApplication, defineEnvironment, defineProgram, defineSurface, defineResource, defineReaction,
  resourceView, shareResource, type Scope, type Observation, type OperationContract, type OperationHandle,
  type ResourceReadable, type ResourceImplementation, type ReactionImplementation, type ProgramSession, type Invocation, type Json, z } from '@hanger-source/dsh-reactive';
import { concatMap, type Observable } from 'rxjs';
import { implementProgram, useResource, useResourceInstance, useReaction, useBeforeStep } from '@hanger-source/dsh-reactive/program';
import { createRuntime, type ConversationHandle, type RuntimeOptions, type NativeSessionSource } from '@hanger-source/dsh-reactive-dsh';
import { createNativeSkills, type NativeSessionSkills } from '@hanger-source/dsh-reactive-dsh/skills';
import { createNativeTools, type NativeSessionTools } from '@hanger-source/dsh-reactive-dsh/tools';
import { createProgramClient, type RemoteProgramSession } from '@hanger-source/dsh-reactive-client';
import { mountProgramGateway } from '@hanger-source/dsh-reactive-remote';
import { getDeveloperSkill } from '@hanger-source/dsh-reactive-skills';
import { sessionResources, readSessionView } from './consumer-session.js';

const contract = defineProgram({ input: z.object({ subject: z.string() }).strict(), surface: defineSurface({ views: { revision: resourceView(z.number()) }, actions: {} }) });
const program = implementProgram(contract, { Component: () => null });
const application = defineApplication({ name: 'registry-consumer', admit: async () => {}, environments: [
  defineEnvironment({ id: 'main', instructions: '发行边界消费，不运行模型', program }),
] });
export const backend = (options: RuntimeOptions) => createRuntime(() => application, options);
export const clientContract = () => contract;
export const portableLocal = (session: ConversationHandle<typeof contract>) => sessionResources(session);
export const portableRemote = (session: RemoteProgramSession<typeof contract>) => sessionResources(session);
export const localRevision = (session: ConversationHandle<typeof contract>): Promise<number> => readSessionView(session, 'revision');
export const remoteRevision = (session: RemoteProgramSession<typeof contract>): Promise<number> => readSessionView(session, 'revision');
export const publicEntries = [createProgramClient, mountProgramGateway, getDeveloperSkill, createNativeSkills, createNativeTools] as const;
export const developerSkill = getDeveloperSkill();
export const nativeProgramSource = (conversation: ProgramSession<typeof contract>): NativeSessionSource => conversation;
export const invocationActivity = <T, O extends Json>(invocation: Invocation<T, O>) => invocation.activity;

const current = defineResource({ name: 'consumer-current', params: z.null(), value: z.object({ revision: z.number() }) });
const changes = defineReaction({ name: 'consumer-changes', params: z.null(), event: z.number(), result: z.string() });
export function useBackendBindings(resource: ResourceImplementation<typeof current>, reaction: ReactionImplementation<typeof changes>) {
  const instance = useResourceInstance(resource, null);
  return { resource: instance, observed: useResource(instance), reaction: useReaction(reaction, null) };
}
export function useLocalBackend(target: string, changes$: Observable<number>) {
  const resource = useResourceInstance(() => ({ value: z.string(), read: () => target }), [target]);
  useBeforeStep(async signal => { await resource.read(signal); }, [resource]);
  const reaction = useReaction(() => ({ event: z.number(), result: z.string(),
    observe: () => changes$, operator: concatMap, execute: revision => target + ':' + (revision + 1) }), [target, changes$]);
  return { resource, reaction, observed: useResource(resource) };
}
export function usePublicView(session: ConversationHandle<typeof contract> | RemoteProgramSession<typeof contract>) {
  const resource: ResourceReadable<number> = session.viewResource('revision');
  return useResource(resource);
}
export const publicResource = (scope: Scope, source: Observable<Observation<number>>) => shareResource(source, { signal: scope.signal });
export function useOperationState<D extends OperationContract>(operation: OperationHandle<D>) { return useResource(operation.state); }
export const useHumanState = (conversation: ProgramSession<typeof contract>) => useResource(conversation.human);
export function useRuntimeSkills(skills: NativeSessionSkills, name: string) {
  return { catalog: useResource(skills.catalog), definition: useResource(skills.definition(name)) };
}
export const useRuntimeTools = (tools: NativeSessionTools) => useResource(tools.catalog);
export const useExecutionProgress = (conversation: ProgramSession<typeof contract>) => useResource(conversation.progress);
export function useRequestState(session: ConversationHandle<typeof contract> | RemoteProgramSession<typeof contract>, requestId: string) {
  return useResource(session.request(requestId).state);
}
export async function readQuestion(session: ConversationHandle<typeof contract> | RemoteProgramSession<typeof contract>, callId: string) {
  const question = await session.question(callId);
  return question.state.read();
}
