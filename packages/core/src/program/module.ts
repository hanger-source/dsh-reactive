import { createContext, createElement, useContext, useMemo, type Context as ReactContext, type ReactElement, type ReactNode } from 'react';
import { declaration } from './renderer.ts';
import { moduleCollectionInstance, moduleRootSurface, rebaseModuleMount, type SurfaceMount } from '../surfaces/composition.ts';
import type { Surface } from '../surfaces/contract.ts';

/** @internal 作用域只改变声明名称与出口位置，不建立第二个 owner。 */
export const moduleMountContext: ReactContext<SurfaceMount | null> = createContext<SurfaceMount | null>(null);
export interface ModuleDeclaration { readonly kind: 'module'; readonly mount: SurfaceMount; }
const moduleDeclarations = new WeakSet<object>();
// 只分配真实Module代际的短名称，不登记业务key或延长退出实例的寿命。
let dynamicNamespaceSequence = 0;
export interface ModuleProps<S extends Surface = Surface> { readonly mount: SurfaceMount<S, Surface>; readonly children?: ReactNode; }
export function Module<S extends Surface>({ mount, children }: ModuleProps<S>): ReactElement {
  const parent = useContext(moduleMountContext);
  const root = moduleRootSurface(mount), path = JSON.stringify(mount.path);
  // 地址token可以每次重新签发；逻辑目标变化则退出旧名称，重新回到同key也不复活旧代际。
  const effective = useMemo(() => rebaseModuleMount(mount, parent,
    moduleCollectionInstance(mount) ? 'm_i_' + (++dynamicNamespaceSequence).toString(36) + '__' : undefined),
  [root, mount.surface, mount.selected, path, parent]);
  const presence: ModuleDeclaration = Object.freeze({ kind: 'module', mount: effective });
  moduleDeclarations.add(presence);
  return createElement(moduleMountContext.Provider, { value: effective }, declaration(presence), children);
}
/** @internal renderer 擦除后仍只接受真实 Module 边界产生的声明。 */
export function isModuleDeclaration(value: object): value is ModuleDeclaration { return moduleDeclarations.has(value); }
