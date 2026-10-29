import { collectionSurface, composeSurface, defineAction, defineOperation, defineProgram, defineSurface, resourceView, z, type OperationContract, type OperationHandle, type Invocation, type Json } from '@hanger-source/dsh-reactive';
import { bindProgramClient, type RemoteProgramSession, type RemoteServiceProgramSession } from '@hanger-source/dsh-reactive-client';
import { createNativeActionViews, defineNativeActionView, bindNativeSession, retainNativeSession,
  nativeHooks, type NativeSession, type NativeActionViewProps } from '@hanger-source/dsh-reactive-native-ui';
import type { SlotCore } from '@deepseek-ai/dsh-client-ui-slots';
import type {} from '@deepseek-ai/dsh-client-ui-renderer/client';
import { getDeveloperSkill } from '@hanger-source/dsh-reactive-skills';
import { ConsumerModuleProgram, sessionResources, sharedSessionFacts, sessionControls, sessionFileUpload, receiveFileUpload, readSessionView, readSelectedRevision, callPublishedAction, invocationReceipt, sessionImage } from './consumer-session.js';

const action = defineAction({ name: 'release_consumer_read', description: '公开 UI 类型消费',
  scope: z.object({ subject: z.string() }).strict(), input: z.null(), output: z.object({ version: z.string() }).strict() });
const program = defineProgram({ name: 'release_ui', input: z.null(), surface: defineSurface({ views: { version: resourceView(z.string()) }, actions: { read: action } }) });
export type Session = RemoteProgramSession<typeof program>;
export const portableSession = (session: Session) => sessionResources(session);
export const nativeImage = (native: NativeSession<typeof program>, attachmentId: Parameters<typeof sessionImage>[1], signal: AbortSignal) =>
  sessionImage(native, attachmentId, signal);
export const portableControls = (session: Session, signal: AbortSignal) => sessionControls(session, signal);
export const portableModuleRevision = (session: RemoteProgramSession<typeof ConsumerModuleProgram>, signal: AbortSignal) => readSelectedRevision(session, signal);
const operation = defineOperation({ kind: 'release-ui-record', version: 1, command: z.string(), reference: z.string(),
  state: z.object({ done: z.boolean() }), settled: state => state.done ? state : undefined });
const moduleSurface = defineSurface({ views: { version: resourceView(z.string()) }, actions: { read: action }, operations: { current: operation } });
const modules = composeSurface({ record: moduleSurface, rows: collectionSurface(moduleSurface) });
const NativeModuleProgram = defineProgram({ name: 'release_ui_modules', input: z.null(), surface: modules.surface });
export async function nativeModuleSurface(native: NativeSession<typeof NativeModuleProgram>, signal: AbortSignal) {
  const record = native.module(modules.mounts.record), rows = native.collection('rows');
  const row = rows.instance('record'), read = await row.resolveAction('read', { subject: 'record' }, { signal });
  return { version: await record.readView('version', { signal }), resource: record.viewResource('version'),
    instances: rows.instances, eligibility: read.eligibility, operation: await record.operations.current({ signal }),
    rootOperation: await native.operations['record.current']({ signal }),
    invocation: (id: string) => row.invocation('read', id, { signal }),
    rootInvocation: (id: string) => native.lookupInvocation('record.read', id, { signal }) };
}
export const portableCall = async (session: Session, signal: AbortSignal) => {
  const invocation = await callPublishedAction(session, 'read', { subject: 'release' }, null);
  return invocationReceipt(invocation, signal);
};
export const portableView = (session: Session): Promise<string> => readSessionView(session, 'version');
export const portableUpload = (session: Session, bytes: Uint8Array, signal: AbortSignal) => sessionFileUpload(session, bytes, { signal });
export const nativeUpload = (native: NativeSession<typeof program>, bytes: Uint8Array, signal: AbortSignal) => {
  const call = native.uploadFile(bytes, { signal });
  return { ...invocationReceipt(call, signal), native: native.invocation(call), receipt: receiveFileUpload(call) };
};
export const nativeClient = (ctx: Parameters<typeof bindProgramClient>[1]) => bindProgramClient(program, ctx);
export const nativeReference = (ctx: Parameters<typeof retainNativeSession>[1], sessionId: string) => retainNativeSession(program, ctx, sessionId);
export function rejectLocalReference(ctx: Parameters<typeof retainNativeSession>[1], sessionId: string) {
  const local = defineProgram({ input: z.null(), surface: program.surface });
  // @ts-expect-error 自行创建远程 SDK 的取得入口要求发布者声明的共享 Program。
  return retainNativeSession(local, ctx, sessionId);
}
export function rejectServiceSession(native: Parameters<typeof bindNativeSession>[1], service: RemoteServiceProgramSession<typeof program>) {
  // @ts-expect-error 服务端口没有人工参与能力，不能作为原生人工会话绑定。
  return bindNativeSession(program, native, service);
}
export const view = defineNativeActionView(action, (_props: NativeActionViewProps<typeof action>) => null);
export const publicEntries = [createNativeActionViews, bindNativeSession, getDeveloperSkill] as const;
export const developerSkill = getDeveloperSkill();
export const publicView = (native: NativeSession<typeof program>, session: Session) => native.resource(session.viewResource('version'));
export const registerNativeResource = (slots: SlotCore, native: NativeSession<typeof program>) => slots.register({
  name: 'root', inject: () => nativeHooks({ current: native.view('version') }),
}, function NativeVersion({ useCurrent }) {
  const current = useCurrent(value => value);
  return current.status === 'ready' ? current.value : current.status;
});
export const publicSessionFacts = (native: NativeSession<typeof program>, session: Session, signal: AbortSignal) =>
  native.resource(sharedSessionFacts(session, signal));
export function operationState<D extends OperationContract>(native: NativeSession<typeof program>, operation: OperationHandle<D>) {
  return native.resource(operation.state);
}
export const humanQuestion = (native: NativeSession<typeof program>, callId: string) => native.question(callId);
export const invocationActivity = <T, O extends Json, Source>(native: NativeSession<typeof program>, invocation: Invocation<T, O, Source>, signal: AbortSignal) => ({
  ...invocationReceipt(invocation, signal), resource: invocation.activity, native: native.invocation(invocation),
});
export const requestProgress = (native: NativeSession<typeof program>, requestId: string) => ({
  session: native.progress, request: native.request(requestId),
});
