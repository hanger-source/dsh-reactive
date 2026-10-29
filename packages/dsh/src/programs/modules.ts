import { implementModule, type AnyModuleImplementation, type ModuleImplementation,
  type ModuleImplementationOptions, type ModuleImplementations } from '@hanger-source/dsh-reactive/program';
import type { Surface } from '@hanger-source/dsh-reactive';
import type { NativeCapability } from '../native-capability.ts';

const nativeDependencies = new WeakMap<object, readonly NativeCapability[]>();

/** 模块携带宿主原生依赖；静态装配安装一次，动态挂载只贡献声明与读侧。 */
export interface NativeModuleImplementationOptions<S extends Surface, Props extends object, Children extends ModuleImplementations>
extends ModuleImplementationOptions<S, Props, Children> {
  readonly native: readonly NativeCapability[];
}

export function implementNativeModule<S extends Surface, Props extends object = {}, const Children extends ModuleImplementations = {}>(
  surface: S, options: NativeModuleImplementationOptions<S, Props, Children>,
): ModuleImplementation<S, Props> {
  const capabilities = Object.freeze([...options.native]);
  const implementation = implementModule(surface, options);
  nativeDependencies.set(implementation, capabilities);
  return implementation;
}

/** @internal 普通 core 模块没有宿主依赖；只在应用静态装配时读取。 */
export function moduleNativeDependencies(implementation: AnyModuleImplementation): readonly NativeCapability[] | undefined {
  return nativeDependencies.get(implementation);
}
