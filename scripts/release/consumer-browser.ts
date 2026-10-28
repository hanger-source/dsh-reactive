import { defineProgram, defineSurface, resourceView, shareResource, z,
  type Invocation, type Json } from '@hanger-source/dsh-reactive';
import type { ResourceSource } from '@hanger-source/dsh-reactive/resources';
import { createProgramClient, bindProgramClient, createServiceProgramClient, bindServiceProgramClient, type ProgramClientOptions } from '@hanger-source/dsh-reactive-client';
import * as client from '@hanger-source/dsh-reactive-client';
import * as errors from '@hanger-source/dsh-reactive-client/errors';
import * as inbox from '@hanger-source/dsh-reactive-client/inbox';
import * as transport from '@hanger-source/dsh-reactive-client/transport';
import * as types from '@hanger-source/dsh-reactive-client/types';
import type { ProgramAddress } from '@hanger-source/dsh-reactive-client/types';
import * as remote from '@hanger-source/dsh-reactive-client/remote';
export { sessionResources, sessionFileUpload, receiveFileUpload, callPublishedAction } from './consumer-session.js';

const date = z.codec(z.iso.datetime(), z.date(), { decode: value => new Date(value), encode: value => value.toISOString() });
export const program = defineProgram({ name: 'release_browser', input: z.null(), surface: defineSurface({ actions: {}, views: { at: resourceView(date) } }) });
export const connect = (options: ProgramClientOptions) => createProgramClient(program, options);
export const bind = (context: Parameters<typeof bindProgramClient>[1]) => bindProgramClient(program, context);
export const connectService = (options: ProgramClientOptions) => createServiceProgramClient(program, options);
export const bindService = (context: Parameters<typeof bindServiceProgramClient>[1]) => bindServiceProgramClient(program, context);
export const composed = (source: ResourceSource<Date>, signal: AbortSignal) => shareResource(source, { signal });
export const invocationActivity = <T, O extends Json, Source>(invocation: Invocation<T, O, Source>) => invocation.activity.read();
// 同一个实包浏览器产物同时消费所有正式入口，运行消费者验证共享身份及实际字节/Socket行为。
export const clientEntries = { client, errors, inbox, transport, types, remote };
export const address: ProgramAddress = { sessionId: 'release-client-entries', program: { name: program.name, version: program.version } };
