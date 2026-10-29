import type { Context } from '@deepseek-ai/cordis';
import type { AnyEnvironment } from '@hanger-source/dsh-reactive';
import Registry from '@deepseek-ai/dsh-typert-registry';
import * as TypertLoader from '@deepseek-ai/dsh-typert-loader';
import RemoteAccess, { type RemoteAccessService } from '@deepseek-ai/dsh-api-remote-access';
import Gateway from '@deepseek-ai/dsh-api-gateway';
import SessionController from '@deepseek-ai/dsh-api-session-controller';
import DefaultModel from '@deepseek-ai/dsh-agent-default-model';
import type {} from '@deepseek-ai/dsh-credentials-local';
import type {} from '@deepseek-ai/dsh-client-connection';
import type {} from '@deepseek-ai/dsh-attachment-local';
import type {} from '@deepseek-ai/dsh-commands';
import type {} from '@deepseek-ai/dsh-client-file-upload';
import { ProgramRemoteService, type ProgramRemoteOptions } from './host.ts';
import { serveRemoteCarrier, type RemoteCarrier, type RemoteCarrierOptions } from './carrier.ts';
import { registerProgramEvents } from './events.ts';

export interface ProgramGatewayOptions<E extends readonly AnyEnvironment[] = readonly AnyEnvironment[]> extends ProgramRemoteOptions<E> {
  /** local创建本Gateway的Registry；provided明确借用宿主已有Registry，不接管其寿命。 */
  readonly registry?: 'local' | 'provided';
  readonly model: { readonly provider: string; readonly model: string; readonly reasoningEffort?: string };
  readonly carrier: RemoteCarrierOptions;
  /** 应用登记实际 principal policy；框架不授予 owner 或推测业务角色。 */
  access(service: RemoteAccessService): void;
}

/** 在已有 Runtime.native 安装官方 Controller/Gateway 和应用认证的物理 carrier。 */
export async function mountProgramGateway<E extends readonly AnyEnvironment[]>(ctx: Context, options: ProgramGatewayOptions<E>): Promise<RemoteCarrier> {
  const registry = options.registry ?? 'local';
  if (registry !== 'local' && registry !== 'provided') throw new Error('REACTIVE_REMOTE_REGISTRY_MODE_INVALID');
  if (registry === 'provided' && !ctx.get('typert')) throw new Error('REACTIVE_REMOTE_REGISTRY_REQUIRED');
  for (const name of ['attachments', 'commands', 'fileUploads', 'connection', 'credentials'] as const) {
    if (!ctx.get(name)) throw new Error('REACTIVE_RUNTIME_INPUT_SERVICE_REQUIRED:' + name);
  }
  let carrier: RemoteCarrier | undefined;
  const fiber = ctx.plugin(async host => {
    if (registry === 'local') await host.plugin(Registry);
    await host.plugin(RemoteAccess);
    await host.plugin(Gateway);
    await host.plugin(DefaultModel, options.model);
    await host.plugin(SessionController, { nativeOpen: false });
    await host.inject(['remoteAccess', 'typertGateway', 'typert', 'loader', 'connection', 'sessionController'], async services => {
      options.access(services.remoteAccess);
      new ProgramRemoteService(services, options);
      await services.extend({ baseUrl: import.meta.url }).plugin(TypertLoader, { packages: ['@hanger-source/dsh-reactive-remote', '@deepseek-ai/dsh-api-remote-access',
        '@deepseek-ai/dsh-api-session-controller', '@deepseek-ai/dsh-commands', '@deepseek-ai/dsh-client-file-upload'] });
      services.effect(() => registerProgramEvents(services));
      carrier = await serveRemoteCarrier(services, options.carrier);
    });
  });
  try { await fiber; }
  catch (error) { await fiber.dispose(); throw error; }
  if (!carrier) { await fiber.dispose(); throw new Error('REACTIVE_REMOTE_CARRIER_NOT_STARTED'); }
  const active = carrier;
  return { ...active, close: () => fiber.dispose() };
}
