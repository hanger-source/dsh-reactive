import type { Context } from '@deepseek-ai/cordis';
import { isOperationOwner, type ActionContract, type ActionImplementation, type OperationOwnerReference, type ProgramContract } from '@hanger-source/dsh-reactive/kernel';
import { NativeDelegations, type DelegationOptions } from '../delegations.ts';
import type { NativeCapability } from '../native-capability.ts';

// 安装实例只有一个应用owner；失败回滚后也不能复用已经撤销的同一实例。
const assignedCapabilities = new WeakSet<NativeCapability>();

function registerOperationOwner(owners: Map<string, OperationOwnerReference>, owner: OperationOwnerReference): void {
  if (!isOperationOwner(owner)) throw new Error('REACTIVE_OPERATION_OWNER_INVALID');
  const key = JSON.stringify([owner.definition.kind, owner.definition.version]);
  const current = owners.get(key);
  if (current && current !== owner) throw new Error('REACTIVE_OPERATION_OWNER_DUPLICATE');
  owners.set(key, owner);
}

/** @internal 手工宿主依赖与模块依赖遵守同一真实 owner 身份；安装前拒绝合同冲突。 */
export function collectOperationOwners(values: Iterable<OperationOwnerReference>): readonly OperationOwnerReference[] {
  const owners = new Map<string, OperationOwnerReference>();
  for (const owner of values) registerOperationOwner(owners, owner);
  return Object.freeze([...owners.values()]);
}

/** 模块显式选择应用需要的能力与恢复 owner，宿主统一完成安装。 */
export interface ApplicationAssembly {
  /** 将此实例交给应用；失败回滚也会dispose尚未安装的选择，同一对象只安装一次。 */
  native<C extends NativeCapability>(capability: C): C;
  /** 借用实际业务owner供冷恢复；Host不取得业务操作的执行与销毁责任。 */
  operation<O extends OperationOwnerReference>(owner: O): O;
  implementDelegation<D extends ActionContract, P extends ProgramContract>(action: D, options: DelegationOptions<NoInfer<D>, P>): ActionImplementation<D>;
}

/** @internal 仅由正式挂载取得本次装配及其封闭状态。 */
export function createApplicationAssembly(ctx: Context) {
  const native = new NativeDelegations(ctx);
  const capabilities = new Set<NativeCapability>();
  const operations = new Map<string, OperationOwnerReference>();
  let completed: Readonly<{ capabilities: readonly NativeCapability[]; operations: readonly OperationOwnerReference[] }> | undefined;
  const assertOpen = () => {
    if (completed) throw new Error('REACTIVE_APPLICATION_ASSEMBLY_CLOSED');
  };
  const assembly: ApplicationAssembly = Object.freeze({
    native<C extends NativeCapability>(capability: C): C {
      assertOpen();
      if (typeof capability?.install !== 'function' || typeof capability?.dispose !== 'function') {
        throw new Error('REACTIVE_NATIVE_CAPABILITY_INVALID');
      }
      if (!capabilities.has(capability)) {
        if (assignedCapabilities.has(capability)) throw new Error('REACTIVE_NATIVE_CAPABILITY_ALREADY_OWNED');
        assignedCapabilities.add(capability);
      }
      capabilities.add(capability);
      return capability;
    },
    operation<O extends OperationOwnerReference>(owner: O): O {
      assertOpen();
      registerOperationOwner(operations, owner);
      return owner;
    },
    implementDelegation<D extends ActionContract, P extends ProgramContract>(action: D, options: DelegationOptions<NoInfer<D>, P>): ActionImplementation<D> {
      assertOpen();
      return native.implementation(action, options);
    },
  });
  return { assembly, native,
    complete() {
      return completed ??= Object.freeze({ capabilities: Object.freeze([...capabilities]), operations: Object.freeze([...operations.values()]) });
    },
  };
}
