import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import type { Context } from '@deepseek-ai/cordis';
import { resolveImageAttachmentAccess, type LlmAdapter } from '@deepseek-ai/dsh-llm';
import { DeepSeekAdapter, DeepSeekFileStore, DeepSeekUploadIndex, resolveAdapterOptions, type DeepSeekCatalogModel, type DeepSeekConnectionOptions, type Options as DeepSeekProviderOptions } from '@deepseek-ai/dsh-llm-deepseek';
import { getOrCreateAnonymousUserId, ANONYMOUS_USER_ID_FILE_NAME } from '@deepseek-ai/dsh-anonymous-user-id';
import type {} from '@deepseek-ai/dsh-deepseek-llm-api-extensions';
import * as piAiProvider from '@deepseek-ai/dsh-llm-pi-ai';
import type { PiAiProviderProfile } from '@deepseek-ai/dsh-llm-pi-ai';
import { requireActivePlugin } from './native-installation.ts';

export interface RuntimeModel {
  readonly provider: string;
  readonly model: string;
  install(ctx: Context, home: string): Promise<void>;
}
export interface DeepSeekOptions extends Omit<DeepSeekProviderOptions, 'models' | 'defaultContextWindow'> {
  /** 字符串沿用官方目录；对象只覆盖明确声明的能力，容量与模态属于这个实际模型。 */
  model: string | DeepSeekCatalogModel;
  apiKey: string | ((signal?: AbortSignal) => string | Promise<string>);
}

export interface PiAiOptions {
  readonly provider: string;
  readonly model: string;
  /** 原生多协议路由；apiKeyEnv 由宿主凭据服务按每次请求解析，配置中不携带密钥。 */
  readonly profile: PiAiProviderProfile;
}

/** 复用官方多协议适配器、凭据与重试合同，支持应用已有的模型网关。 */
export function piAi(input: PiAiOptions): RuntimeModel {
  if (!input.provider.trim() || !input.model.trim()) throw new Error('REACTIVE_MODEL_REQUIRED');
  const provider = input.provider, model = input.model, profile = structuredClone(input.profile);
  return Object.freeze({ provider, model, async install(ctx: Context) {
    await requireActivePlugin(ctx.plugin(piAiProvider, { providers: { [provider]: profile } }));
  } });
}

/** 使用正式 dsh Messages 适配器；凭据只存在于请求连接快照，不进入应用数据目录。 */
export function deepseek(input: DeepSeekOptions): RuntimeModel {
  const { model: selected, apiKey, ...providerOptions } = input;
  const model = typeof selected === 'string' ? selected : selected.id;
  if (!model.trim()) throw new Error('REACTIVE_MODEL_REQUIRED');
  const defaults = resolveAdapterOptions(providerOptions);
  const known = defaults.models.find(entry => entry.id === model);
  if (known === undefined && typeof selected === 'string') throw new Error('REACTIVE_MODEL_DECLARATION_REQUIRED');
  const declared = typeof selected === 'string' ? undefined : structuredClone(selected);
  if (known === undefined && declared?.contextWindow === undefined) throw new Error('REACTIVE_MODEL_CONTEXT_REQUIRED');
  const resolved = declared === undefined ? defaults : resolveAdapterOptions({ ...providerOptions, models: [
    ...defaults.models.filter(entry => entry.id !== model),
    { ...known, ...declared },
  ] });
  type Connection = DeepSeekConnectionOptions & { credential: string | null };
  return Object.freeze({ provider: 'reactive-deepseek', model,
    async install(ctx: Context, home: string) {
      const userId = getOrCreateAnonymousUserId({ env: { DSH_HOME: home } });
      if (readFileSync(join(home, ANONYMOUS_USER_ID_FILE_NAME), 'utf8').trim() !== userId) throw new Error('REACTIVE_MODEL_ID_NOT_PERSISTED');
      const files = new DeepSeekFileStore({ index: new DeepSeekUploadIndex(join(home, 'llm-deepseek', 'files-v3.json')) });
      const owner = ctx.inject(['llm', 'deepseekLlmApiExtensions'], native => {
        native.llm.registerAdapter(['reactive-deepseek'], new DeepSeekAdapter<Connection>({
          options: () => ({ ...resolved, credential: null }),
          async connect(signal) {
            signal?.throwIfAborted();
            const credential = typeof apiKey === 'function' ? await apiKey(signal) : apiKey;
            signal?.throwIfAborted();
            if (!credential?.trim()) throw new Error('REACTIVE_MODEL_CREDENTIAL_REQUIRED');
            return { ...resolved, credential };
          },
          async resolveAuth(connection) {
            if (!connection.credential) throw new Error('REACTIVE_MODEL_CONNECTION_NOT_PREPARED');
            return { headers: { authorization: 'Bearer ' + connection.credential } };
          },
          resolveUserId: () => userId,
          resolveAttachments: () => native.get('attachments'),
          resolveImageAccess: (attachments, ref) => resolveImageAttachmentAccess(attachments,
            hostPath => native.get('fs')?.processPathFromHostPath(hostPath), ref),
          resolveFiles: () => files,
          prepareExtensions: request => native.deepseekLlmApiExtensions.prepare(request),
          onReplayDegrade: detail => console.warn('reactive.model.replay.invalid', detail),
          onExtensionsOmitted: detail => console.error('reactive.model.extension.omitted', detail),
          onExtensionsUnaccepted: detail => console.error('reactive.model.extension.unaccepted', detail),
        }));
      });
      await requireActivePlugin(owner);
    },
  });
}

/** 原生适配器扩展点，普通业务应用使用配置化模型即可。每个运行时创建独立适配器。 */
export function nativeModel(options: { provider: string; model: string; create(): LlmAdapter }): RuntimeModel {
  if (!options.provider.trim() || !options.model.trim()) throw new Error('REACTIVE_MODEL_REQUIRED');
  return Object.freeze({ provider: options.provider, model: options.model,
    async install(ctx: Context) {
      const owner = ctx.inject(['llm'], native => { native.llm.registerAdapter([options.provider], options.create()); });
      await requireActivePlugin(owner);
    },
  });
}
