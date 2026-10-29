import { createContext, createElement, useContext, type ComponentType, type Context, type ReactElement } from 'react';
import { isOperationOwner, type OperationOwnerReference } from '../operations/contract.ts';
import { moduleContribution, type SurfaceMount } from '../surfaces/composition.ts';
import { assertSurfaceBindings, type Surface } from '../surfaces/contract.ts';
import { Module } from './module.ts';
import type { AnyModuleImplementation, ModuleImplementations } from './definition.ts';

export type ModuleComponentProps<Props extends object, Children extends ModuleImplementations = {}> =
  Props & { readonly modules: Children };
export type ModuleMountProps<S extends Surface, Props extends object = {}> =
  Omit<Props, 'mount' | 'modules'> & { readonly mount: SurfaceMount<S, Surface> };
export interface ModuleImplementation<S extends Surface = Surface, Props extends object = {}> extends AnyModuleImplementation {
  readonly surface: S;
  readonly Component: ComponentType<ModuleMountProps<S, Props>>;
}
export interface ModuleImplementationOptions<S extends Surface, Props extends object = {}, Children extends ModuleImplementations = {}> {
  readonly Component: ComponentType<ModuleComponentProps<Props, NoInfer<Children>>>;
  readonly modules?: Children;
  readonly operations?: readonly OperationOwnerReference[];
}
export interface ProgramDependencies {
  readonly modules: readonly AnyModuleImplementation[];
  readonly operations: readonly OperationOwnerReference[];
}
interface ModuleRecord {
  readonly modules: ModuleImplementations;
  readonly operations: readonly OperationOwnerReference[];
}
const implementations = new WeakMap<object, ModuleRecord>();

/** @internal Program 的静态递归图同时就是挂载时的安装事实。 */
export const installedModulesContext: Context<readonly AnyModuleImplementation[] | null> =
  createContext<readonly AnyModuleImplementation[] | null>(null);

/** 签发模块只捕获依赖，不求值业务组件或启动 owner。 */
export function implementModule<S extends Surface, Props extends object = {}, const Children extends ModuleImplementations = {}>(
  surface: S, options: ModuleImplementationOptions<S, Props, Children>,
): ModuleImplementation<S, Props> {
  assertSurfaceBindings(surface, {});
  const modules = captureModuleImplementations(options.modules);
  const operations = captureOperationOwners(options.operations);
  const BusinessComponent = options.Component;
  function Component(props: ModuleMountProps<S, Props>): ReactElement {
    const installed = useContext(installedModulesContext);
    if (installed === null || !installed.includes(implementation)) throw new Error('REACTIVE_MODULE_NOT_INSTALLED');
    const { mount, ...business } = props;
    // 私有 mount 身份核验的是 source；选择后的 selected 只负责公开投射。
    moduleContribution(mount, surface, {});
    const input = { ...business, modules } as ModuleComponentProps<Props, Children>;
    return createElement(Module, { mount }, createElement(BusinessComponent, input));
  }
  const implementation: ModuleImplementation<S, Props> = Object.freeze({ surface, Component });
  implementations.set(implementation, Object.freeze({ modules, operations }));
  return implementation;
}

/** @internal 自有记录快照拒绝伪造模块，不保存调用者的可变依赖容器。 */
export function captureModuleImplementations<M extends ModuleImplementations>(modules: M | undefined): M {
  const source: ModuleImplementations = modules === undefined ? {} : modules;
  if (typeof source !== 'object' || source === null || Array.isArray(source)
    || (Object.getPrototypeOf(source) !== Object.prototype && Object.getPrototypeOf(source) !== null)) {
    throw new Error('REACTIVE_MODULE_DEPENDENCIES_INVALID');
  }
  const captured: Record<string, AnyModuleImplementation> = Object.create(null);
  for (const key of Reflect.ownKeys(source)) {
    if (typeof key !== 'string') throw new Error('REACTIVE_MODULE_DEPENDENCIES_INVALID');
    const module = source[key];
    moduleRecord(module);
    captured[key] = module;
  }
  return Object.freeze(captured) as M;
}

/** @internal owner 身份验证不连接通知来源，也不执行任何业务方法。 */
export function captureOperationOwners(operations: readonly OperationOwnerReference[] | undefined): readonly OperationOwnerReference[] {
  if (operations !== undefined && !Array.isArray(operations)) throw new Error('REACTIVE_OPERATION_OWNERS_INVALID');
  const captured = operations === undefined ? [] : [...operations];
  for (const owner of captured) if (!isOperationOwner(owner)) throw new Error('REACTIVE_OPERATION_OWNER_INVALID');
  return Object.freeze(captured);
}

/** @internal 同一真实模块只安装一次；冲突判断留给最终宿主装配。 */
export function collectProgramDependencies(modules: ModuleImplementations,
  operations: readonly OperationOwnerReference[],
): ProgramDependencies {
  const collected: AnyModuleImplementation[] = [];
  const owners = new Set(operations);
  const visited = new Set<AnyModuleImplementation>();
  const visiting = new Set<AnyModuleImplementation>();
  const visit = (module: AnyModuleImplementation) => {
    const record = moduleRecord(module);
    if (visiting.has(module)) throw new Error('REACTIVE_MODULE_DEPENDENCY_CYCLE');
    if (visited.has(module)) return;
    visiting.add(module);
    collected.push(module);
    for (const owner of record.operations) owners.add(owner);
    for (const child of Object.values(record.modules)) visit(child);
    visiting.delete(module);
    visited.add(module);
  };
  for (const module of Object.values(modules)) visit(module);
  return Object.freeze({ modules: Object.freeze(collected), operations: Object.freeze([...owners]) });
}

function moduleRecord(value: unknown): ModuleRecord {
  const record = typeof value === 'object' && value !== null ? implementations.get(value) : undefined;
  if (record === undefined) throw new Error('REACTIVE_MODULE_IMPLEMENTATION_INVALID');
  return record;
}
