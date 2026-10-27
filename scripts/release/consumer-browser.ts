import { defineProgram, defineSurface, resourceView, shareResource, z,
  type ResourceReadable } from '@hanger-source/dsh-reactive';
import { createProgramClient, bindProgramClient, type ProgramClientOptions } from '@hanger-source/dsh-reactive-client';
export { bindNativeSession, createNativeActionViews, nativeSource } from '@hanger-source/dsh-reactive-native-ui';

const date = z.codec(z.iso.datetime(), z.date(), { decode: value => new Date(value), encode: value => value.toISOString() });
export const program = defineProgram({ input: z.null(), surface: defineSurface({ actions: {}, views: { at: resourceView(date) } }) });
export const connect = (options: ProgramClientOptions) => createProgramClient(program, options);
export const bind = (context: Parameters<typeof bindProgramClient>[1]) => bindProgramClient(program, context);
export const composed = (source: ResourceReadable<Date>, signal: AbortSignal) => shareResource(source.observations$, { signal });
