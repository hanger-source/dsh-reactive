import { defineProgram, defineSurface, resourceView, shareResource, z,
  type Invocation } from '@hanger-source/dsh-reactive';
import type { ResourceSource } from '@hanger-source/dsh-reactive/resources';
import { createProgramClient, bindProgramClient, createServiceProgramClient, bindServiceProgramClient, type ProgramClientOptions } from '@hanger-source/dsh-reactive-client';
export { bindNativeSession, retainNativeSession, createNativeActionViews, nativeSource } from '@hanger-source/dsh-reactive-native-ui';
export { sessionResources, callPublishedAction } from './consumer-session.js';

const date = z.codec(z.iso.datetime(), z.date(), { decode: value => new Date(value), encode: value => value.toISOString() });
export const program = defineProgram({ input: z.null(), surface: defineSurface({ actions: {}, views: { at: resourceView(date) } }) });
export const connect = (options: ProgramClientOptions) => createProgramClient(program, options);
export const bind = (context: Parameters<typeof bindProgramClient>[1]) => bindProgramClient(program, context);
export const connectService = (options: ProgramClientOptions) => createServiceProgramClient(program, options);
export const bindService = (context: Parameters<typeof bindServiceProgramClient>[1]) => bindServiceProgramClient(program, context);
export const composed = (source: ResourceSource<Date>, signal: AbortSignal) => shareResource(source, { signal });
export const invocationActivity = (invocation: Invocation<unknown>) => invocation.activity.read();
