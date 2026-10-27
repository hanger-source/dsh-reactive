import { createElement, type ComponentType, type ReactElement } from 'react';
import { assertProgramDefinition, type ProgramContract, type ProgramProps, type ProgramSession, type ProgramWorkspace, type ProgramImplementation } from './definition.ts';

type ProgramElementFactory<P extends ProgramContract> =
  (conversation: ProgramSession<P>, workspace: ProgramWorkspace) => ReactElement;
const implementations = new WeakMap<object, unknown>();

/** 构造宿主实现不会求值组件、连接查询或启动原生执行。 */
export function implementProgram<P extends ProgramContract>(definition: P,
  options: NoInfer<{ readonly Component: ComponentType<ProgramProps<P>> }>,
): ProgramImplementation<P> {
  assertProgramDefinition(definition);
  const implementation = Object.freeze({ definition });
  const Component = options.Component;
  const element: ProgramElementFactory<P> = (conversation, workspace) => createElement(Component, { conversation, workspace });
  implementations.set(implementation, element);
  return implementation;
}

/** @internal 只在会话 owner 的组件根创建时连接实际实现与 typed 输入面。 */
export function programElement<P extends ProgramContract>(implementation: ProgramImplementation<P>,
  conversation: ProgramSession<P>, workspace: ProgramWorkspace,
): ReactElement {
  const element = implementations.get(implementation) as ProgramElementFactory<P> | undefined;
  if (!element) throw new Error('REACTIVE_PROGRAM_IMPLEMENTATION_INVALID');
  return element(conversation, workspace);
}
