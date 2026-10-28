import { defineApplication, defineEnvironment, defineProgram, defineSurface, defineResource, defineReaction, composeSurface, combineResources, mapResource, switchResource, abortable,
  resourceView, shareResource, type Scope, type ResourceSource, type OperationContract, type OperationHandle,
  type ResourceReadable, type ResourceImplementation, type ReactionImplementation, type ProgramSession, type Invocation, type Json, defineOperation, implementAction, type OperationOwner, type NativeToolAdmission, z } from '@hanger-source/dsh-reactive';
import { concatMap, firstValueFrom, map, type Observable } from 'rxjs';
import { createElement } from 'react';
import { Context, Expose, Module, Skill, NativeTools, implementProgram, useAction, useResource, useResourceInstance, useReaction, useBeforeStep,
  useObservable, useShared, useSharedResource, useRelation } from '@hanger-source/dsh-reactive/program';
import { createRuntime, mountApplication, type ApplicationAssembly, type ConversationHandle, type ServiceConversation, type RuntimeOptions, type NativeSessionSource } from '@hanger-source/dsh-reactive-dsh';
import { createNativeSkills, type NativeSessionSkills } from '@hanger-source/dsh-reactive-dsh/skills';
import { createNativeTools, type NativeSessionTools } from '@hanger-source/dsh-reactive-dsh/tools';
import { createProgramClient, createServiceProgramClient, type RemoteProgramSession, type RemoteServiceProgramSession } from '@hanger-source/dsh-reactive-client';
import { mountProgramGateway } from '@hanger-source/dsh-reactive-remote';
import { getDeveloperSkill } from '@hanger-source/dsh-reactive-skills';
import { ConsumerModuleProgram, sessionResources, sessionControls, sessionFileUpload, receiveFileUpload, readSessionView, readSelectedRevision, invocationReceipt } from './consumer-session.js';
import type { Workspace } from '@hanger-source/dsh-reactive/workspaces';

const contract = defineProgram({ input: z.object({ subject: z.string() }).strict(), surface: defineSurface({ views: { revision: resourceView(z.number()) }, actions: {} }) });
const program = implementProgram(contract, { Component: () => null });
const application = defineApplication({ name: 'registry-consumer', admit: async () => {}, environments: [
  defineEnvironment({ id: 'main', instructions: '发行边界消费，不运行模型', program }),
] });
export const backend = (options: RuntimeOptions) => createRuntime(() => application, options);
export const clientContract = () => contract;
export function sharedDependencyProgram(skillName: string, toolNames: readonly string[], admit: NativeToolAdmission) {
  const moduleSurface = defineSurface({ views: {}, actions: {} });
  const modules = composeSurface({ left: moduleSurface, right: moduleSurface });
  function Dependencies() {
    return [createElement(Skill, { key: 'skill', name: skillName }), createElement(NativeTools, { key: 'native-tools', names: toolNames, enabled: true, admit })];
  }
  return implementProgram(defineProgram({ input: z.null(), surface: modules.surface }), { Component: () => [
    createElement(Module, { key: 'left', mount: modules.mounts.left }, createElement(Dependencies)),
    createElement(Module, { key: 'right', mount: modules.mounts.right }, createElement(Dependencies)),
  ] });
}
export const portableLocal = (session: ConversationHandle<typeof contract>) => sessionResources(session);
export const portableRemote = (session: RemoteProgramSession<typeof contract>) => sessionResources(session);
export const portableLocalService = (session: ServiceConversation<typeof contract>) => sessionResources(session);
export const portableRemoteService = (session: RemoteServiceProgramSession<typeof contract>) => sessionResources(session);
export const localControls = (session: ConversationHandle<typeof contract>, signal: AbortSignal) => sessionControls(session, signal);
export const remoteControls = (session: RemoteProgramSession<typeof contract>, signal: AbortSignal) => sessionControls(session, signal);
export const localUpload = (session: ConversationHandle<typeof contract>, bytes: Uint8Array, signal: AbortSignal) => sessionFileUpload(session, bytes, { signal });
export const remoteUpload = (session: RemoteProgramSession<typeof contract>, bytes: Uint8Array, signal: AbortSignal) => sessionFileUpload(session, bytes, { signal });
export const uploadReceipt = receiveFileUpload;
export const localModuleRevision = (session: ConversationHandle<typeof ConsumerModuleProgram>, signal: AbortSignal) => readSelectedRevision(session, signal);
export const remoteModuleRevision = (session: RemoteProgramSession<typeof ConsumerModuleProgram>, signal: AbortSignal) => readSelectedRevision(session, signal);
export const localRevision = (session: ConversationHandle<typeof contract>): Promise<number> => readSessionView(session, 'revision');
export const remoteRevision = (session: RemoteProgramSession<typeof contract>): Promise<number> => readSessionView(session, 'revision');
export const publicEntries = [createProgramClient, createServiceProgramClient, mountProgramGateway, getDeveloperSkill, createNativeSkills, createNativeTools] as const;
export const developerSkill = getDeveloperSkill();
export const nativeProgramSource = (conversation: ProgramSession<typeof contract>): NativeSessionSource => conversation;
export const invocationActivity = <T, O extends Json, Source>(invocation: Invocation<T, O, Source>) => invocation.activity;
export const invocationResult = <T, O extends Json, Source>(invocation: Invocation<T, O, Source>, signal: AbortSignal) => invocationReceipt(invocation, signal);
export const useInvocationActivity = <T, O extends Json, Source>(invocation: Invocation<T, O, Source>) => useResource(invocation.activity);
export const readWithCancellation = <T>(read: (signal: AbortSignal) => T | PromiseLike<T>, signal: AbortSignal): Promise<T> =>
  firstValueFrom(abortable(read, signal));

const current = defineResource({ name: 'consumer-current', params: z.null(), value: z.object({ revision: z.number() }) });
const changes = defineReaction({ name: 'consumer-changes', params: z.null(), event: z.number(), result: z.string() });
const backendFacts = defineSurface({ views: { current: resourceView(z.string()) }, actions: {} });
export const workspaceBindings = (workspace: Workspace, resource: ResourceImplementation<typeof current>, reaction: ReactionImplementation<typeof changes>) =>
  ({ resource: workspace.resource(resource, null), reaction: workspace.reaction(reaction, null) });
export const localWorkspace = (runtime: Awaited<ReturnType<typeof backend>>, id: string): Workspace => runtime.application.workspace(id);
export function localCapabilityOptions(read: (signal: AbortSignal) => Promise<string>, changes$: Observable<string>, write: (value: number) => Promise<string>) {
  return { resource: { value: z.string().transform(Number), read: ({ signal }: { readonly signal: AbortSignal }) => read(signal) },
    reaction: { event: z.string().transform(Number), result: z.string().transform(Number),
      observe: () => changes$, operator: concatMap, execute: write } };
}
export function localWorkspaceCapabilities(workspace: Workspace, options: ReturnType<typeof localCapabilityOptions>) {
  const resource = workspace.resource(options.resource), reaction = workspace.reaction(options.reaction);
  const shared: ResourceReadable<number> = workspace.shareResource(resource);
  const values: Observable<number> = resource.values$;
  return { resource, shared, reaction, values, read: (): Promise<number> => resource.read(), start() {
    const run = reaction.start(), results: Observable<number> = run.events$.pipe(map(event => event.result));
    return { run, results, state: run.state };
  } };
}
export function useLocalCapabilities(options: ReturnType<typeof localCapabilityOptions>) {
  const resource = useResourceInstance(() => options.resource, [options.resource]);
  const reaction = useReaction(() => options.reaction, [options.reaction]);
  return { resource, reaction, observed: useResource(resource) };
}
export function useBackendBindings(resource: ResourceImplementation<typeof current>, reaction: ReactionImplementation<typeof changes>) {
  const instance = useResourceInstance(resource, null);
  const composed = useSharedResource(() => combineResources({ current: instance, same: instance.observations$ }).pipe(
    mapResource(value => value.current.revision + value.same.revision), switchResource(() => instance),
    mapResource(value => value.revision.toFixed())), [instance]);
  const observed = useResource(composed);
  return { resource: instance, composed, observed, borrowed: useResource(observed), selected: useResource(() => instance, [instance]),
    reaction: useReaction(reaction, null), declarations: [
      Context({ name: 'current', source: observed, render: value => value }),
      Expose({ surface: backendFacts, value: { views: { current: observed } } }),
    ] };
}
export const useBackendValues = (source: Observable<number>) => useObservable(useShared(source));
export function useBackendRelation(relation: Observable<void>) { useRelation(relation); }
export function useLocalBackend(target: string, changes$: Observable<number>, save: (target: string, value: number) => Promise<number>) {
  const resource = useResourceInstance(() => ({ value: z.string(), read: () => target }), [target]);
  const action = useAction(() => ({ name: 'local_save', description: '保存明确目标',
    scope: z.object({ target: z.string() }), target: { target }, input: z.string().transform(Number), output: z.number(),
    execute: (value, call) => save(call.scope.target, value),
  }), [target, save]);
  useBeforeStep(async signal => { await resource.read(signal); }, [resource]);
  const reaction = useReaction(() => ({ event: z.number(), result: z.string(),
    observe: () => changes$, operator: concatMap, execute: revision => target + ':' + (revision + 1) }), [target, changes$]);
  const activity = useSharedResource(() => combineResources({ current: resource, calls: action.activeInvocations }), [resource, action]);
  return { resource, action, reaction, activity, observed: useResource(activity) };
}
export const localImplementation = (save: (target: string, value: number) => Promise<number>) => implementAction({
  name: 'local_service_save', description: '直接提供局部实现', scope: z.object({ target: z.string() }),
  input: z.number(), output: z.number(), execute: (value, call) => save(call.scope.target, value),
});

const preparation = defineOperation({ kind: 'consumer.preparation', version: 1,
  command: z.object({ target: z.string(), commandId: z.string() }), reference: z.object({ id: z.string() }),
  state: z.enum(['running', 'complete']), settled: state => state === 'complete' ? state : undefined });
export function backendModule(assembly: ApplicationAssembly, owner: OperationOwner<typeof preparation>) {
  const operation: OperationOwner<typeof preparation> = assembly.operation(owner);
  const tools: ReturnType<typeof createNativeTools> = assembly.native(createNativeTools({ service: 'provided' }));
  return { operation, tools, environment: defineEnvironment({ id: 'module', instructions: '发行包模块装配', program }) };
}
export const moduleBackend = (owner: OperationOwner<typeof preparation>, options: RuntimeOptions) => createRuntime(assembly => {
  const module = backendModule(assembly, owner);
  return defineApplication({ name: 'registry-module-consumer', admit: async () => {}, environments: [module.environment] });
}, options);
export const mountedModuleBackend = (context: Parameters<typeof mountApplication>[0], home: string, owner: OperationOwner<typeof preparation>) => mountApplication(context, assembly => {
  const module = backendModule(assembly, owner);
  return defineApplication({ name: 'registry-mounted-module-consumer', admit: async () => {}, environments: [module.environment] });
}, { home });
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
export const publicResource = (scope: Scope, source: ResourceSource<number>) => shareResource(source, { signal: scope.signal });
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
