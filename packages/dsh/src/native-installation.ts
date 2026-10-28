import type { Context, Fiber } from '@deepseek-ai/cordis';

/** 基础服务和模型就绪后安装可选原生能力；插件与监听由该 Runtime 的 Context scope 持有。 */
export type NativeInstallation = (ctx: Context, home: string) => void | PromiseLike<void>;

/**
 * 确认这次正式插件安装已经活跃。Cordis会重抛startup错误，但缺少inject服务的
 * PENDING fiber也能结束await；明确要求的能力不能据此宣称可用。
 */
export async function requireActivePlugin(installation: Fiber & PromiseLike<Fiber>): Promise<Fiber> {
  try {
    const owner = await installation;
    // Cordis公开FiberState.ACTIVE；只检查真实fiber，不扫描或复制原生registry。
    if (owner.state !== 2) throw new Error(`REACTIVE_NATIVE_PLUGIN_NOT_ACTIVE: ${owner.name} (${owner.state})`);
    return owner;
  } catch (error) {
    // 嵌入宿主也不能留下后来自动激活的半次安装；只释放这次exact fiber。
    const failures = new Set<unknown>([error]);
    try { await installation.dispose(); } catch (cleanup) { failures.add(cleanup); }
    try { await installation; } catch (cleanup) { failures.add(cleanup); }
    if (failures.size > 1) throw new AggregateError([...failures], 'REACTIVE_NATIVE_PLUGIN_STARTUP_FAILED');
    throw error;
  }
}
