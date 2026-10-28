import type { ToolSchema } from '@deepseek-ai/dsh-llm';
import type { ToolRuntime } from '@deepseek-ai/dsh-tools';
import { Observable } from 'rxjs';
import type { ResourceReadable } from '@hanger-source/dsh-reactive';
import type { Scope } from '@hanger-source/dsh-reactive/kernel';
import { NativeCapabilityOwner, nativeObservationScope,
  type NativeCapability, type NativeSessionSource } from './native-capability.ts';
import { nativeAgentResource } from './conversations/agent-resource.ts';
import { nativeSessionAccess, type NativeSessionAccess } from './conversations/session-access.ts';

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
  /** 仅释放读取与观察，不撤销工具、restriction、政策或已经受理的执行。 */
  dispose(): void;
}

class SessionTools implements NativeSessionTools {
  readonly catalog: NativeToolCatalog;
  readonly scope: Scope;
  get disposed(): boolean { return this.scope.disposed; }

  constructor(owner: NativeCapabilityOwner, access: NativeSessionAccess) {
    this.scope = nativeObservationScope(owner, access);
    this.catalog = nativeAgentResource(owner, access, this.scope, { service: 'tools',
      changes: agent => new Observable<void>(subscriber => agent.ctx.on('tools/change', () => subscriber.next())),
      read: agent => {
        const tools: ToolRuntime | undefined = agent.ctx.get('tools');
        if (!tools) throw new Error('REACTIVE_NATIVE_TOOLS_REQUIRED');
        return tools.schemas(agent);
      },
    });
  }
  dispose(): void { this.scope.dispose(); }
}

export interface NativeTools extends NativeCapability {
  forSession(session: NativeSessionSource): NativeSessionTools;
}

/** 读取与 NativeTools 声明使用同一注册表；执行继续由原生调用管线和 guard 负责。 */
export function createNativeTools(options: NativeToolsOptions): NativeTools {
  if (options.service !== 'provided') throw new Error('REACTIVE_NATIVE_TOOLS_SERVICE_SELECTION_INVALID');
  const owner = new NativeCapabilityOwner('reactive-native-tools', ['tools'], function* (ctx) {
    if (!ctx.get('tools')) throw new Error('REACTIVE_NATIVE_TOOLS_REQUIRED');
  });
  const sessions = new WeakMap<object, SessionTools>();
  return Object.freeze({ install: owner.install, dispose: () => owner.dispose(),
    forSession(session: NativeSessionSource) {
      const access = nativeSessionAccess(session); owner.session(access, 'tools');
      let current = sessions.get(session);
      if (!current || current.disposed) { current = new SessionTools(owner, access); sessions.set(session, current); }
      return current;
    },
  });
}
