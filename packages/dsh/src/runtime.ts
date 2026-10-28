import { mkdir, realpath } from 'node:fs/promises';
import { isAbsolute, join } from 'node:path';
import { Context } from '@deepseek-ai/cordis';
import Loader from '@deepseek-ai/cordis-plugin-loader';
import AgentPresetRegistry from '@deepseek-ai/dsh-agent-preset-registry';
import SessionStore from '@deepseek-ai/dsh-session';
import SessionProjectionRegistry from '@deepseek-ai/dsh-session-projection';
import SessionQuery from '@deepseek-ai/dsh-session-query-sqlite';
import UserQuestions from '@deepseek-ai/dsh-user-questions';
import * as askUserTool from '@deepseek-ai/dsh-tool-ask-user';
import SystemPrompt from '@deepseek-ai/dsh-system-prompt';
import ToolRuntime from '@deepseek-ai/dsh-tools';
import LlmRegistry from '@deepseek-ai/dsh-llm';
import AgentRegistry from '@deepseek-ai/dsh-agent';
import TokenMeter from '@deepseek-ai/dsh-token-meter';
import * as llmRetry from '@deepseek-ai/dsh-llm-retry';
import BasicCompactionEngine, { type BasicCompactionConfig } from '@deepseek-ai/dsh-compaction-basic';
import * as imageOffload from '@deepseek-ai/dsh-compaction-image-offload';
import AgentLoop from '@deepseek-ai/dsh-agent-loop';
import GoalRegistry from '@deepseek-ai/dsh-goal';
import * as goalDriver from '@deepseek-ai/dsh-goal-round-driver';
import * as goalTools from '@deepseek-ai/dsh-tool-goal';
import LocalFileSystem from '@deepseek-ai/dsh-fs-local';
import WorkingDirectory from '@deepseek-ai/dsh-working-directory';
import Skills from '@deepseek-ai/dsh-skill';
import * as skillTool from '@deepseek-ai/dsh-tool-skill';
import SubagentRuntime from '@deepseek-ai/dsh-subagent';
import * as spawnSubagents from '@deepseek-ai/dsh-subagent-spawn-in-process';
import * as forkSubagents from '@deepseek-ai/dsh-subagent-fork-in-process';
import * as persistence from '@deepseek-ai/dsh-session-persistence-jsonl';
import Storage from '@deepseek-ai/dsh-storage';
import * as storageJson from '@deepseek-ai/dsh-storage-json';
import * as storageDomain from '@deepseek-ai/dsh-storage-domain';
import WorkspaceRegistry from '@deepseek-ai/dsh-workspace';
import DeepSeekExtensions from '@deepseek-ai/dsh-deepseek-llm-api-extensions';
import Attachments from '@deepseek-ai/dsh-attachment-local';
import Commands from '@deepseek-ai/dsh-commands';
import LocalCredentials from '@deepseek-ai/dsh-credentials-local';
import * as Connection from '@deepseek-ai/dsh-client-connection';
import FileUploads from '@deepseek-ai/dsh-client-file-upload';
import type { AnyEnvironment } from '@hanger-source/dsh-reactive/kernel';
import { assembleApplication, mountAssembledApplication, type ApplicationFactory } from './applications/mount.ts';
import type { ApplicationHost } from './application.ts';
import InlineRuntime, { type Config as ExecutionLimits } from './execution/inline-runtime.ts';
import type { RuntimeModel } from './models.ts';
import type { NativeInstallation } from './native-installation.ts';
import type { OperationOwnerReference } from '@hanger-source/dsh-reactive/kernel';
import { environmentPresetId } from './programs/presets.ts';

export interface RuntimeOptions {
  home: string;
  actorId?: string;
  serviceActorId?: string;
  operations?: readonly OperationOwnerReference[];
  model: RuntimeModel;
  /** 在应用挂载及任何 Agent 创建前完成；安装失败沿 Runtime 启动失败路径释放。 */
  installNative?: NativeInstallation;
  tools?: 'native' | 'ptc';
  execution?: ExecutionLimits;
  /** 原生压缩策略；容量来自实际模型路由，摘要配额与保留量可逐模型配置。 */
  compaction?: BasicCompactionConfig;
  goalRounds?: number;
  maxParallelToolCalls?: number;
  subagents?: { maxDepth?: number; maxActiveSubagents?: number };
  onError?(event: { sessionId: string; error: unknown }): void;
}
const openHomes = new Set<string>();

/** 独立宿主的正式装配入口；不读取个人 Profile，不改变进程目录或环境。 */
export async function createRuntime<E extends readonly AnyEnvironment[]>(factory: ApplicationFactory<E>, options: RuntimeOptions) {
  if (!isAbsolute(options.home)) throw new Error('REACTIVE_HOME_NOT_ABSOLUTE');
  await mkdir(options.home, { recursive: true, mode: 0o700 });
  const home = await realpath(options.home);
  if (openHomes.has(home)) throw new Error('REACTIVE_HOME_ALREADY_OPEN');
  openHomes.add(home);
  const ctx = new Context();
  let host: ApplicationHost<E> | undefined;
  let assembled: Awaited<ReturnType<typeof assembleApplication<E>>> | undefined;
  try {
    assembled = await assembleApplication(ctx, factory);
    const definition = assembled.definition;
    await ctx.plugin(SessionStore);
    await ctx.plugin(SessionProjectionRegistry);
    await ctx.plugin(Loader);
    const firstEnvironment = definition.environments[0];
    if (!firstEnvironment) throw new Error('REACTIVE_ENVIRONMENT_REQUIRED');
    await ctx.plugin(AgentPresetRegistry, { default: environmentPresetId(definition.name, firstEnvironment.id) });
    await ctx.plugin(SystemPrompt, {});
    await ctx.plugin(InlineRuntime, options.execution ?? {});
    await ctx.plugin(ToolRuntime, { mode: options.tools ?? 'ptc' });
    await ctx.plugin(LlmRegistry);
    await ctx.plugin(AgentRegistry);
    await ctx.plugin(TokenMeter);
    await ctx.plugin(llmRetry);
    await ctx.plugin(BasicCompactionEngine, options.compaction ?? {});
    await ctx.plugin(imageOffload);
    await ctx.plugin(Attachments, { dshHome: home });
    await ctx.plugin(Commands);
    await ctx.plugin(LocalCredentials, { dshHome: home });
    await ctx.plugin(Connection);
    await ctx.plugin(FileUploads);
    await ctx.plugin(persistence, { root: join(home, 'sessions'), compression: 'none' });
    await ctx.plugin(SessionQuery, { path: join(home, 'session-index.sqlite'), openAt: 'first-search' });
    await ctx.plugin(Storage);
    await ctx.plugin(storageJson, { root: join(home, 'state') });
    await ctx.plugin(storageDomain, { backend: 'json' });
    await ctx.plugin(WorkspaceRegistry);
    await ctx.plugin(AgentLoop, { agents: [], maxParallelToolCalls: options.maxParallelToolCalls ?? 10 });
    await ctx.plugin(GoalRegistry, { defaultMaxGoalRounds: options.goalRounds ?? 10 });
    await ctx.plugin(goalDriver);
    await ctx.plugin(goalTools);
    await ctx.plugin(LocalFileSystem, { cwd: home });
    await ctx.plugin(WorkingDirectory, { defaultDirectory: home });
    await ctx.plugin(SubagentRuntime, options.subagents ?? {});
    await ctx.plugin(spawnSubagents, { providerName: 'spawn' });
    await ctx.plugin(forkSubagents, { providerName: 'fork' });
    await ctx.plugin(Skills, {});
    await ctx.plugin(skillTool);
    await ctx.plugin(UserQuestions);
    await ctx.plugin(askUserTool, { mode: 'timed' });
    await ctx.plugin(DeepSeekExtensions);
    ctx.on('agent/error', ({ agent, error }) => {
      const detail = { sessionId: agent.id as string, error };
      if (options.onError) options.onError(detail); else console.error('reactive.agent.failed', detail);
    });
    await options.model.install(ctx, home);
    await options.installNative?.(ctx, home);
    host = await mountAssembledApplication(ctx, assembled, { home, actorId: options.actorId, serviceActorId: options.serviceActorId, operations: options.operations,
      model: { provider: options.model.provider, model: options.model.model } });
  } catch (error) {
    const failures: unknown[] = [error];
    try { await host?.dispose(); } catch (cleanup) { failures.push(cleanup); }
    try { await assembled?.release(); } catch (cleanup) { failures.push(cleanup); }
    try { await ctx.fiber.dispose(); } catch (cleanup) { failures.push(cleanup); }
    openHomes.delete(home);
    if (failures.length > 1) throw new AggregateError(failures, 'REACTIVE_STARTUP_FAILED');
    throw error;
  }
  const application = host;
  let closing: Promise<void> | undefined;
  return { application, home, native: ctx,
    dispose() {
      if (closing) return closing;
      const completion = Promise.withResolvers<void>();
      closing = completion.promise;
      void (async () => {
        try { await application.dispose(); } finally {
          try { await ctx.fiber.dispose(); } finally { openHomes.delete(home); }
        }
      })().then(completion.resolve, completion.reject);
      return closing;
    },
  };
}
