import { defineApplication, defineEnvironment, defineProgram, defineSurface, defineResource, defineReaction, combineResources, mapResource, switchResource, abortable,
  resourceView, shareResource, type Scope, type Observation, type OperationContract, type OperationHandle,
  type ResourceReadable, type ResourceImplementation, type ReactionImplementation, type ProgramSession, type Invocation, type Json, defineOperation, implementAction, type OperationOwner, z } from '@hanger-source/dsh-reactive';
import { concatMap, firstValueFrom, type Observable } from 'rxjs';
import { implementProgram, useAction, useResource, useResourceInstance, useReaction, useBeforeStep } from '@hanger-source/dsh-reactive/program';
import { createRuntime, type ConversationHandle, type RuntimeOptions, type NativeSessionSource } from '@hanger-source/dsh-reactive-dsh';
import { createNativeSkills, type NativeSessionSkills } from '@hanger-source/dsh-reactive-dsh/skills';
import { createNativeTools, type NativeSessionTools } from '@hanger-source/dsh-reactive-dsh/tools';
import { createProgramClient, type RemoteProgramSession } from '@hanger-source/dsh-reactive-client';
import { mountProgramGateway } from '@hanger-source/dsh-reactive-remote';
import { getDeveloperSkill } from '@hanger-source/dsh-reactive-skills';
import { sessionResources, readSessionView } from './consumer-session.js';
import type { Workspace } from '@hanger-source/dsh-reactive/workspaces';

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
export const readWithCancellation = <T>(read: (signal: AbortSignal) => T | PromiseLike<T>, signal: AbortSignal): Promise<T> =>
  firstValueFrom(abortable(read, signal));

const current = defineResource({ name: 'consumer-current', params: z.null(), value: z.object({ revision: z.number() }) });
const changes = defineReaction({ name: 'consumer-changes', params: z.null(), event: z.number(), result: z.string() });
export const workspaceBindings = (workspace: Workspace, resource: ResourceImplementation<typeof current>, reaction: ReactionImplementation<typeof changes>) =>
  ({ resource: workspace.resource(resource, null), reaction: workspace.reaction(reaction, null) });
export const localWorkspace = (runtime: Awaited<ReturnType<typeof backend>>, id: string): Workspace => runtime.application.workspace(id);
export function useBackendBindings(resource: ResourceImplementation<typeof current>, reaction: ReactionImplementation<typeof changes>) {
  const instance = useResourceInstance(resource, null);
  const composed = combineResources({ current: instance, same: instance.observations$ }).pipe(
    mapResource(value => value.current.revision + value.same.revision), switchResource(() => instance),
    mapResource(value => value.revision.toFixed()));
  return { resource: instance, composed, observed: useResource(instance), reaction: useReaction(reaction, null) };
}
export function useLocalBackend(target: string, changes$: Observable<number>, save: (target: string, value: number) => Promise<number>) {
  const resource = useResourceInstance(() => ({ value: z.string(), read: () => target }), [target]);
  const action = useAction(() => ({ name: 'local_save', description: '保存明确目标',
    scope: z.object({ target: z.string() }), target: { target }, input: z.string().transform(Number), output: z.number(),
    execute: (value, call) => save(call.scope.target, value),
  }), [target, save]);
  useBeforeStep(async signal => { await resource.read(signal); }, [resource]);
  const reaction = useReaction(() => ({ event: z.number(), result: z.string(),
    observe: () => changes$, operator: concatMap, execute: revision => target + ':' + (revision + 1) }), [target, changes$]);
  return { resource, action, reaction, observed: useResource(resource) };
}
export const localImplementation = (save: (target: string, value: number) => Promise<number>) => implementAction({
  name: 'local_service_save', description: '直接提供局部实现', scope: z.object({ target: z.string() }),
  input: z.number(), output: z.number(), execute: (value, call) => save(call.scope.target, value),
});

const preparation = defineOperation({ kind: 'consumer.preparation', version: 1,
  command: z.object({ target: z.string(), commandId: z.string() }), reference: z.object({ id: z.string() }),
  state: z.enum(['running', 'complete']), settled: state => state === 'complete' ? state : undefined });
export function useLocalPreparation(owner: OperationOwner<typeof preparation>, target: string,
  submit: (command: { target: string; commandId: string }) => Promise<{ id: string }>) {
  return useAction(() => ({ name: 'local_prepare', description: '委托真实业务操作',
    scope: z.object({ target: z.string() }), target: { target }, input: z.null(), output: preparation.reference,
    operation: { definition: preparation, command: (_input, scope, origin) => ({ target: scope.target, commandId: origin.invocationId }), reference: output => output },
    operationOwner: owner, execute: (_input, call) => submit({ target: call.scope.target, commandId: call.invocationId }),
  }), [owner, target, submit]);
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
