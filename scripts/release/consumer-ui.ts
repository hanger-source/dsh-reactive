import { defineAction, defineProgram, defineSurface, resourceView, z, type OperationContract, type OperationHandle, type Invocation, type Json } from '@hanger-source/dsh-reactive';
import { bindProgramClient, type RemoteProgramSession, type RemoteServiceProgramSession } from '@hanger-source/dsh-reactive-client';
import { createNativeActionViews, defineNativeActionView, bindNativeSession, retainNativeSession,
  type NativeSession, type NativeActionViewProps } from '@hanger-source/dsh-reactive-native-ui';
import { getDeveloperSkill } from '@hanger-source/dsh-reactive-skills';
import { ConsumerModuleProgram, sessionResources, sharedSessionFacts, sessionControls, readSessionView, readSelectedRevision, callPublishedAction } from './consumer-session.js';

const action = defineAction({ name: 'release_consumer_read', description: '公开 UI 类型消费',
  scope: z.object({ subject: z.string() }).strict(), input: z.null(), output: z.object({ version: z.string() }).strict() });
const program = defineProgram({ input: z.null(), surface: defineSurface({ views: { version: resourceView(z.string()) }, actions: { read: action } }) });
export type Session = RemoteProgramSession<typeof program>;
export const portableSession = (session: Session) => sessionResources(session);
export const portableControls = (session: Session, signal: AbortSignal) => sessionControls(session, signal);
export const portableModuleRevision = (session: RemoteProgramSession<typeof ConsumerModuleProgram>, signal: AbortSignal) => readSelectedRevision(session, signal);
export const portableCall = (session: Session) => callPublishedAction(session, 'read', { subject: 'release' }, null);
export const portableView = (session: Session): Promise<string> => readSessionView(session, 'version');
export const nativeClient = (ctx: Parameters<typeof bindProgramClient>[1]) => bindProgramClient(program, ctx);
export const nativeReference = (ctx: Parameters<typeof retainNativeSession>[1], sessionId: string) => retainNativeSession(program, ctx, sessionId);
export function rejectServiceSession(native: Parameters<typeof bindNativeSession>[1], service: RemoteServiceProgramSession<typeof program>) {
  // @ts-expect-error 服务端口没有人工参与能力，不能作为原生人工会话绑定。
  return bindNativeSession(program, native, service);
}
export const view = defineNativeActionView(action, (_props: NativeActionViewProps<typeof action>) => null);
export const publicEntries = [createNativeActionViews, bindNativeSession, getDeveloperSkill] as const;
export const developerSkill = getDeveloperSkill();
export const publicView = (native: NativeSession<typeof program>, session: Session) => native.resource(session.viewResource('version'));
export const publicSessionFacts = (native: NativeSession<typeof program>, session: Session, signal: AbortSignal) =>
  native.resource(sharedSessionFacts(session, signal));
export function operationState<D extends OperationContract>(native: NativeSession<typeof program>, operation: OperationHandle<D>) {
  return native.resource(operation.state);
}
export const humanQuestion = (native: NativeSession<typeof program>, callId: string) => native.question(callId);
export const invocationActivity = <T, O extends Json>(native: NativeSession<typeof program>, invocation: Invocation<T, O>) => ({
  resource: invocation.activity, native: native.invocation(invocation),
});
export const requestProgress = (native: NativeSession<typeof program>, requestId: string) => ({
  session: native.progress, request: native.request(requestId),
});
