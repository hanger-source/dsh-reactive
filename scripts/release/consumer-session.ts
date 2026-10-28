import type { ActionInput, CancellationOptions, FileUploadInvocation, FileUploadSource, FileUploadValue, Invocation, Json, ProgramContract, ProgramSessionClient, HumanProgramSessionClient, Surface, SurfaceClient, SurfaceMount, ViewValue } from '@hanger-source/dsh-reactive';
import { combineResources, composeSurface, defineProgram, defineSurface, resourceView, selectSurface, shareResource, z } from '@hanger-source/dsh-reactive';

export function sessionControls<P extends ProgramContract>(session: HumanProgramSessionClient<P>, signal: AbortSignal) {
  return { pause: () => session.pause({ signal }), cancel: () => session.cancel({ signal }),
    status: (requestId: string) => session.requestStatus(requestId, { signal }) };
}

export async function readLocalModule<Root extends Surface, Source extends Surface, Selected extends Surface,
  Options extends CancellationOptions, Async extends boolean, K extends keyof Selected['views'] & string>(
  client: SurfaceClient<Root, Options, Async>, mount: SurfaceMount<Source, Selected>, name: K, options?: Options,
): Promise<ViewValue<Selected['views'][K]>> {
  const local: SurfaceClient<Selected, Options, Async> = client.module(mount);
  return local.readView(name, options);
}

const localSurface = defineSurface({ views: { revision: resourceView(z.number()), internal: resourceView(z.string()) }, actions: {} });
const modules = composeSurface({ record: selectSurface(localSurface, { views: ['revision'] }) });
export const ConsumerModuleProgram = defineProgram({ input: z.null(), surface: modules.surface });
export const readSelectedRevision = (client: SurfaceClient<typeof modules.surface>, signal: AbortSignal): Promise<number> =>
  readLocalModule(client, modules.mounts.record, 'revision', { signal });

export function observePublishedCapabilities<P extends ProgramContract, K extends keyof P['surface']['collections'] & string>(
  session: ProgramSessionClient<P>, collection: K,
) {
  return combineResources({ instances: session.collection(collection).instances, actions: session.actionManifest });
}

// 同一模块仅依赖共享合同，本地、远程和浏览器消费者直接传入原对象。
export function sessionResources<P extends ProgramContract>(session: ProgramSessionClient<P>) {
  return { surface: session.surface, progress: session.progress, human: session.human,
    request: (id: string) => session.request(id) };
}

export function sharedSessionFacts<P extends ProgramContract>(session: ProgramSessionClient<P>, signal: AbortSignal) {
  return shareResource(combineResources({ progress: session.progress, human: session.human }), { signal });
}

// 退等只针对回执；原Invocation、结果Promise和有限活动保持同一身份与owner。
export function invocationReceipt<T, Observed extends Json, Source>(invocation: Invocation<T, Observed, Source>, signal: AbortSignal) {
  return { invocation, source: invocation.source, result: invocation.result,
    wait: (): Promise<T> => invocation.wait({ signal }), activity: invocation.activity, activity$: invocation.activity$ };
}

export function sessionFileUpload<P extends ProgramContract>(session: ProgramSessionClient<P>, data: FileUploadSource,
  options?: CancellationOptions & { readonly name?: string }) {
  const call: FileUploadInvocation = session.uploadFile(data, options);
  return { call, source: call.source, result: call.result, activity: call.activity, activity$: call.activity$,
    wait: (signal: AbortSignal) => invocationReceipt(call, signal).wait() };
}

export async function receiveFileUpload(call: FileUploadInvocation): Promise<FileUploadValue> {
  return await call.result;
}

export async function readSessionView<P extends ProgramContract, K extends keyof P['surface']['views'] & string>(
  session: ProgramSessionClient<P>, name: K, options?: CancellationOptions,
): Promise<ViewValue<P['surface']['views'][K]>> {
  return session.readView(name, options);
}

export async function callPublishedAction<S extends Surface, K extends keyof S['actions'] & string>(
  client: SurfaceClient<S>, name: K, scope: z.input<S['actions'][K]['scope']>, input: ActionInput<S['actions'][K]>, options?: CancellationOptions,
) {
  const binding = await client.resolveAction(name, scope, options);
  return await binding(input, options);
}
