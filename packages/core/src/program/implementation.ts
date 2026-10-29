import { createElement, type ComponentType, type ReactElement } from 'react';
import { assertProgramDefinition, type ProgramContract, type ProgramProps, type ProgramSession, type ProgramWorkspace, type ProgramImplementation, type ModuleImplementations } from './definition.ts';
import type { OperationOwnerReference } from '../operations/contract.ts';
import { captureModuleImplementations, captureOperationOwners, collectProgramDependencies, installedModulesContext, type ProgramDependencies } from './module-implementation.ts';
import { programEnvironment } from './hooks.ts';

type ProgramElementFactory<P extends ProgramContract> =
  (conversation: ProgramSession<P>, workspace: ProgramWorkspace) => ReactElement;
interface ProgramRecord<P extends ProgramContract> {
  readonly element: ProgramElementFactory<P>;
  readonly dependencies: ProgramDependencies;
}
const implementations = new WeakMap<object, unknown>();

export interface ProgramImplementationOptions<P extends ProgramContract, Modules extends ModuleImplementations = {}> {
  readonly Component: ComponentType<ProgramProps<P, NoInfer<Modules>>>;
  readonly modules?: Modules;
  readonly operations?: readonly OperationOwnerReference[];
}

/** 构造宿主实现不会求值组件、连接查询或启动原生执行。 */
export function implementProgram<P extends ProgramContract, const Modules extends ModuleImplementations = {}>(definition: P,
  options: ProgramImplementationOptions<NoInfer<P>, Modules>,
): ProgramImplementation<P> {
  assertProgramDefinition(definition);
  const implementation = Object.freeze({ definition });
  const Component = options.Component;
  const modules = captureModuleImplementations(options.modules);
  const dependencies = collectProgramDependencies(modules, captureOperationOwners(options.operations));
  const element: ProgramElementFactory<P> = (conversation, workspace) =>
    programEnvironment(definition, conversation, workspace,
      createElement(installedModulesContext.Provider, { value: dependencies.modules },
        createElement(Component, { conversation, workspace, modules })));
  implementations.set(implementation, Object.freeze({ element, dependencies }));
  return implementation;
}

/** @internal 只在会话 owner 的组件根创建时连接实际实现与 typed 输入面。 */
export function programElement<P extends ProgramContract>(implementation: ProgramImplementation<P>,
  conversation: ProgramSession<P>, workspace: ProgramWorkspace,
): ReactElement {
  return programRecord(implementation).element(conversation, workspace);
}

/** @internal 静态装配与挂载权限消费同一份已捕获的依赖事实。 */
export function programDependencies(implementation: ProgramImplementation): ProgramDependencies {
  return programRecord(implementation).dependencies;
}

function programRecord<P extends ProgramContract>(implementation: ProgramImplementation<P>): ProgramRecord<P> {
  const record = implementations.get(implementation) as ProgramRecord<P> | undefined;
  if (record === undefined) throw new Error('REACTIVE_PROGRAM_IMPLEMENTATION_INVALID');
  return record;
}
