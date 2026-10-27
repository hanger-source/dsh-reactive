import type { Context } from '@deepseek-ai/cordis';
import type { AnyEnvironment } from '@hanger-source/dsh-reactive';
import * as TypertLoader from '@deepseek-ai/dsh-typert-loader';
import { ProgramRemoteService, type ProgramRemoteOptions } from './host.ts';
import '@hanger-source/dsh-reactive-client/errors';

export { ProgramRemoteService } from './host.ts';
export type { ProgramRemoteOptions } from './host.ts';
export { createProgramActionOwner } from './actions.ts';
export type { ProgramActionOwner, ProgramActionOwnerOptions } from './actions.ts';
export * from './carrier.ts';
export * from './gateway.ts';
export type * from '@hanger-source/dsh-reactive-client/types';

/** Gateway 与其可信认证/事件装配由实际宿主提供，插件只贡献本包的业务 API。 */
export const inject: string[] = ['typertGateway', 'remoteAccess', 'typert', 'loader', 'sessionController'];
/** 挂载真实 Service，并通过公开 Loader 登记本包生成的 Host contribution。 */
export async function apply<E extends readonly AnyEnvironment[]>(ctx: Context, options: ProgramRemoteOptions<E>): Promise<void> {
  new ProgramRemoteService(ctx, options);
  await ctx.extend({ baseUrl: import.meta.url }).plugin(TypertLoader, { packages: ['@hanger-source/dsh-reactive-remote'] });
}
