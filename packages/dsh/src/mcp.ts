import * as McpClient from '@deepseek-ai/dsh-mcp-client';
import McpResources from '@deepseek-ai/dsh-mcp-resources';
import type { Observable } from 'rxjs';
import type { ResourceReader } from '@hanger-source/dsh-reactive';
import { requireActivePlugin } from './native-installation.ts';
import { NativeCapabilityOwner, nativeToolNames, type NativeCapability } from './native-capability.ts';

export type { Config as NativeMcpConnection, ReconnectConfig } from '@deepseek-ai/dsh-mcp-client';

export interface NativeMcpOptions {
  /** 首次连接/发现失败阻止启动，不能把未连接实例当可用能力。 */
  readonly connection: McpClient.Config & { readonly failOnStartupError: true };
  /** disabled 不从本实例公开资源工具；宿主已安装服务及其其他声明仍由原生政策管理。 */
  readonly resources: 'disabled' | 'install' | 'provided';
}

export interface NativeMcp extends NativeCapability {
  readonly serverName: string;
  /** 当前连接代际实际注册的工具；断开时为空，供标准 NativeTools 声明消费。 */
  readonly toolNames: ResourceReader<readonly string[]>;
  readonly toolNames$: Observable<readonly string[]>;
  /** 公共资源工具只在正式服务已注册时出现；调用仍须 NativeTools 的原生来源及政策。 */
  readonly resourceToolNames: ResourceReader<readonly string[]>;
  readonly resourceToolNames$: Observable<readonly string[]>;
}

/**
 * 一个正式 MCP 连接的配置、观察及异步释放。原生 client 持有进程/HTTP连接、重连、
 * 工具代际与取消；普通 SDK 不伪造 ToolExecution 请求资源或绕过工具政策。
 */
export function createNativeMcp(options: NativeMcpOptions): NativeMcp {
  const selected = structuredClone(options);
  if (selected.resources !== 'disabled' && selected.resources !== 'install' && selected.resources !== 'provided') {
    throw new Error('REACTIVE_MCP_RESOURCE_SELECTION_INVALID');
  }
  if (selected.connection.failOnStartupError !== true) throw new Error('REACTIVE_MCP_STARTUP_MUST_BE_CONFIRMED');
  if (!/^[A-Za-z0-9_-]{1,32}$/u.test(selected.connection.serverName)) throw new Error('REACTIVE_MCP_SERVER_NAME_INVALID');
  const owner = new NativeCapabilityOwner('reactive-native-mcp', async ctx => {
    if (selected.resources === 'install') await requireActivePlugin(ctx.plugin(McpResources));
    if (selected.resources === 'provided' && !ctx.get('mcpResources')) throw new Error('REACTIVE_MCP_RESOURCES_REQUIRED');
    await requireActivePlugin(ctx.plugin(McpClient, selected.connection));
  });
  const prefix = `mcp__${selected.connection.serverName}__`;
  const resourceNames = new Set(['list_mcp_resources', 'list_mcp_resource_templates', 'read_mcp_resource']);
  const toolNames = nativeToolNames(owner, name => name.startsWith(prefix));
  const resourceToolNames = nativeToolNames(owner, name => selected.resources !== 'disabled' && resourceNames.has(name));
  return Object.freeze({ install: owner.install, dispose: () => owner.dispose(), serverName: selected.connection.serverName,
    toolNames, toolNames$: toolNames.values$,
    resourceToolNames, resourceToolNames$: resourceToolNames.values$,
  });
}
