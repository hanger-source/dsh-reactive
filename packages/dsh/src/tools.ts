import type { ToolSchema } from '@deepseek-ai/dsh-llm';
import type { ToolRuntime } from '@deepseek-ai/dsh-tools';
import { Observable } from 'rxjs';
import type { ResourceReadable } from '@hanger-source/dsh-reactive';
import type { NativeCapability, NativeSessionSource } from './native-capability.ts';
import { createNativeSessionCapability } from './native.ts';

export type { ToolSchema } from '@deepseek-ai/dsh-llm';

export interface NativeToolsOptions {
  /** 使用正式宿主注册表；工具安装、声明与执行政策仍由原 owner 管理。 */
  readonly service: 'provided';
}

/** 当前原生作用域可见 schema；不包含执行闭包或展示回调，不代表某次调用已获批准。 */
export interface NativeToolCatalog extends ResourceReadable<readonly ToolSchema[]> {
  readonly values$: Observable<readonly ToolSchema[]>;
  invalidate(): void;
}

export interface NativeSessionTools {
  /** 注册、撤销、restriction 与 Agent 代际变化都撤下旧快照。PTC 模式仍保留完整可见目录。 */
  readonly catalog: NativeToolCatalog;
}

export interface NativeTools extends NativeCapability {
  forSession(session: NativeSessionSource): NativeSessionTools;
}

/** 读取与 NativeTools 声明使用同一注册表；执行继续由原生调用管线和 guard 负责。 */
export function createNativeTools(options: NativeToolsOptions): NativeTools {
  if (options.service !== 'provided') throw new Error('REACTIVE_NATIVE_TOOLS_SERVICE_SELECTION_INVALID');
  return createNativeSessionCapability<NativeSessionTools>({
    name: 'reactive-native-tools', services: ['tools'],
    setup: function* (ctx) {
      if (!ctx.get('tools')) throw new Error('REACTIVE_NATIVE_TOOLS_REQUIRED');
    },
    session: source => ({ catalog: source.agentResource({
      changes: agent => new Observable<void>(subscriber => agent.ctx.on('tools/change', () => subscriber.next())),
      read: agent => {
        const tools: ToolRuntime | undefined = agent.ctx.get('tools');
        if (!tools) throw new Error('REACTIVE_NATIVE_TOOLS_REQUIRED');
        return tools.schemas(agent);
      },
    }) }),
  });
}
