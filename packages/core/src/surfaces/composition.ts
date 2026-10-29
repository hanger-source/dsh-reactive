import type { ActionContract } from '../actions/contract.ts';
import type { OperationContract } from '../operations/contract.ts';
import { assertSurfaceBindings, defineSurface, isSurfaceDefinition, defineSurfaceCollection, isSurfaceCollection, surfaceInstanceSlot, surfaceSlotDefinition, prefixSurfaceSlot, surfaceCollectionKeysSlot, type Surface, type SurfaceBindingReferences, type SurfaceCollectionDefinition, type SurfaceSlotKind, type ViewDefinition } from './contract.ts';

const selectionBrand = Symbol('surface-selection');
const mountBrand = Symbol('surface-mount');

/** 选择公开合同的子集；模块仍按原本地合同产生绑定。 */
export interface SurfaceSelection<S extends Surface = Surface, Selected extends Surface = Surface> {
  readonly [selectionBrand]: true;
  readonly source: S;
  readonly surface: Selected;
}
export interface SurfaceTree {
  readonly [name: string]: Surface | SurfaceSelection | SurfaceCollectionDefinition | SurfaceTree;
}
export interface SurfaceCollectionMount<S extends Surface = Surface, Selected extends Surface = S> {
  /** 声明实现使用的原合同；消费端只取得selected。 */
  readonly surface: S;
  readonly selected: Selected;
  readonly path: readonly string[];
  /** 只签发逻辑地址，不保留历史key或建立运行中的模块实例。 */
  instance(key: string): SurfaceMount<S, Selected>;
}
/** 显式组合身份，不从 React key、当前目标或声明顺序推断。 */
export interface SurfaceMount<S extends Surface = Surface, Selected extends Surface = S> {
  readonly [mountBrand]: true;
  /** 原模块保持本地声明合同，公开投射保持同一次选择的合同。 */
  readonly surface: S;
  readonly selected: Selected;
  readonly path: readonly string[];
}

type Intersection<U> = (U extends unknown ? (value: U) => void : never) extends (value: infer I) => void ? I : never;
type Join<P extends string, K extends string> = P extends '' ? K : `${P}.${K}`;
type Prefixed<V, P extends string> = { readonly [K in keyof V & string as `${P}.${K}`]: V[K] };
type Slots<T, Kind extends 'views' | 'actions' | 'operations', P extends string = ''> =
  T extends SurfaceCollectionDefinition ? {}
    : T extends SurfaceSelection<Surface, infer Selected> ? Prefixed<Selected[Kind], P>
    : T extends Surface ? Prefixed<T[Kind], P>
      : T extends SurfaceTree ? Intersection<{
        [K in keyof T & string]: Slots<T[K], Kind, Join<P, K>>;
      }[keyof T & string]> : never;
type Views<T> = Slots<T, 'views'> extends infer V extends Readonly<Record<string, ViewDefinition>> ? V : {};
type Actions<T> = Slots<T, 'actions'> extends infer A extends Readonly<Record<string, ActionContract>> ? A : {};
type Operations<T> = Slots<T, 'operations'> extends infer O extends Readonly<Record<string, OperationContract>> ? O : {};
type CollectionSlots<T, P extends string = ''> = T extends SurfaceCollectionDefinition ? { readonly [K in P]: T }
  : T extends SurfaceSelection<Surface, infer Selected> ? Prefixed<Selected['collections'], P>
    : T extends Surface ? Prefixed<T['collections'], P>
      : T extends SurfaceTree ? Intersection<{ [K in keyof T & string]: CollectionSlots<T[K], Join<P, K>> }[keyof T & string]> : never;
type Collections<T> = CollectionSlots<T> extends infer C extends Readonly<Record<string, SurfaceCollectionDefinition>> ? C : {};
export type ComposedSurface<T extends SurfaceTree> = Surface<Views<T>, Actions<T>, Operations<T>, Collections<T>>;
export type SurfaceMounts<T extends SurfaceTree> = { readonly [K in keyof T]:
  T[K] extends SurfaceCollectionDefinition<infer S, infer Selected> ? SurfaceCollectionMount<S, Selected>
    : T[K] extends SurfaceSelection<infer S, infer Selected> ? SurfaceMount<S, Selected>
    : T[K] extends Surface ? SurfaceMount<T[K]>
      : T[K] extends SurfaceTree ? SurfaceMounts<T[K]> : never;
};
export interface SurfaceComposition<T extends SurfaceTree> {
  readonly surface: ComposedSurface<T>;
  readonly mounts: SurfaceMounts<T>;
}

interface MountInfo {
  readonly root: Surface;
  readonly source: Surface;
  readonly selected: Surface;
  readonly prefix: string;
  /** 动态地址只有在真实Module实例中物化后才取得声明名称。 */
  readonly namespace?: string;
  readonly collection?: { readonly name: string; readonly key: string };
  readonly memberships?: readonly { readonly directory: string; readonly key: string }[];
  readonly mapSlot?: (kind: SurfaceSlotKind, slot: string) => string | undefined;
}
const selections = new WeakSet<object>();
const mounts = new WeakMap<object, MountInfo>();

/** 未列出的类别不公开；选择不改变本地 Action/Operation 的原合同身份。 */
export function selectSurface<S extends Surface,
  const V extends readonly (keyof S['views'] & string)[] = readonly [],
  const A extends readonly (keyof S['actions'] & string)[] = readonly [],
  const O extends readonly (keyof S['operations'] & string)[] = readonly [],
  const C extends readonly (keyof S['collections'] & string)[] = readonly []>(
  source: S, selection: { readonly views?: V; readonly actions?: A; readonly operations?: O; readonly collections?: C },
): SurfaceSelection<S, Surface<Pick<S['views'], V[number]>, Pick<S['actions'], A[number]>, Pick<S['operations'], O[number]>, Pick<S['collections'], C[number]>>> {
  assertSurfaceBindings(source, {});
  for (const key of Reflect.ownKeys(selection)) {
    if (key !== 'views' && key !== 'actions' && key !== 'operations' && key !== 'collections') throw new Error('REACTIVE_SURFACE_SELECTION_KEY_INVALID');
  }
  const pick = <D extends Readonly<Record<string, unknown>>>(definitions: D, keys: readonly string[] | undefined) => {
    const selected: Record<string, unknown> = Object.create(null);
    if (keys !== undefined && !Array.isArray(keys)) throw new Error('REACTIVE_SURFACE_SELECTION_SLOTS_INVALID');
    for (const key of keys ?? []) {
      if (typeof key !== 'string') throw new Error('REACTIVE_SURFACE_SELECTION_SLOT_UNKNOWN');
      if (!Object.hasOwn(definitions, key)) throw new Error('REACTIVE_SURFACE_SELECTION_SLOT_UNKNOWN: ' + key);
      if (Object.hasOwn(selected, key)) throw new Error('REACTIVE_SURFACE_SELECTION_SLOT_DUPLICATE: ' + key);
      selected[key] = definitions[key];
    }
    return selected;
  };
  const surface = defineSurface({
    views: pick(source.views, selection.views) as Pick<S['views'], V[number]>,
    actions: pick(source.actions, selection.actions) as Pick<S['actions'], A[number]>,
    operations: pick(source.operations, selection.operations) as Pick<S['operations'], O[number]>,
    collections: pick(source.collections, selection.collections) as Pick<S['collections'], C[number]>,
  });
  const selected = Object.freeze({ [selectionBrand]: true as const, source, surface });
  selections.add(selected);
  return selected;
}

export function collectionSurface<S extends Surface, Selected extends Surface>(source: SurfaceSelection<S, Selected>): SurfaceCollectionDefinition<S, Selected>;
export function collectionSurface<S extends Surface>(source: S): SurfaceCollectionDefinition<S, S>;
export function collectionSurface(source: Surface | SurfaceSelection): SurfaceCollectionDefinition {
  if (isSurfaceDefinition(source)) return defineSurfaceCollection(source, source);
  if (selections.has(source)) return defineSurfaceCollection(source.source, source.surface);
  throw new Error('REACTIVE_SURFACE_COLLECTION_INVALID');
}

/** 纯合同树生成唯一扁平槽和 mount 身份；SDK、输出槽及 transport 继续使用原 Surface。 */
export function composeSurface<const T extends SurfaceTree>(tree: T): SurfaceComposition<T> {
  const views: Record<string, ViewDefinition> = Object.create(null);
  const actions: Record<string, ActionContract> = Object.create(null);
  const operations: Record<string, OperationContract> = Object.create(null);
  const collections: Record<string, SurfaceCollectionDefinition> = Object.create(null);
  const leaves: { source: Surface; selected: Surface; path: readonly string[]; target: Record<string, unknown>; key: string }[] = [];
  const collectionLeaves: { definition: SurfaceCollectionDefinition; path: readonly string[]; target: Record<string, unknown>; key: string }[] = [];
  const visiting = new Set<object>();
  const collect = (node: SurfaceTree, path: readonly string[]): Record<string, unknown> => {
    if (typeof node !== 'object' || node === null || Array.isArray(node)
      || (Object.getPrototypeOf(node) !== Object.prototype && Object.getPrototypeOf(node) !== null)) {
      throw new Error('REACTIVE_SURFACE_COMPOSITION_TREE_INVALID');
    }
    if (visiting.has(node)) throw new Error('REACTIVE_SURFACE_COMPOSITION_CYCLE');
    visiting.add(node);
    const target: Record<string, unknown> = Object.create(null);
    for (const key of Reflect.ownKeys(node)) {
      if (typeof key !== 'string' || !/^[a-z][a-z0-9_]*$/u.test(key)) throw new Error('REACTIVE_MODULE_PATH_INVALID');
      const value = node[key];
      const next = Object.freeze([...path, key]);
      if (isSurfaceCollection(value)) {
        const name = next.join('.');
        if (Object.hasOwn(collections, name)) throw new Error('REACTIVE_SURFACE_COMPOSITION_SLOT_DUPLICATE: collections:' + name);
        collections[name] = value;
        collectionLeaves.push({ definition: value, path: next, target, key });
        continue;
      }
      let source: Surface;
      let selected: Surface;
      if (isSurfaceDefinition(value)) source = selected = value;
      else if (typeof value === 'object' && value !== null && selections.has(value)) {
        const selection = value as SurfaceSelection;
        source = selection.source; selected = selection.surface;
      } else {
        target[key] = collect(value as SurfaceTree, next);
        continue;
      }
      const prefix = next.join('.') + '.';
      for (const [kind, definitions, destination] of [
        ['views', selected.views, views], ['actions', selected.actions, actions], ['operations', selected.operations, operations],
        ['collections', selected.collections, collections],
      ] as const) {
        for (const slot of Reflect.ownKeys(definitions)) {
          if (typeof slot !== 'string' || !slot || slot.startsWith('$')) throw new Error('REACTIVE_MODULE_SLOT_NAME_INVALID');
          const name = prefix + slot;
          if (Object.hasOwn(destination, name)) throw new Error('REACTIVE_SURFACE_COMPOSITION_SLOT_DUPLICATE: ' + kind + ':' + name);
          Object.assign(destination, { [name]: definitions[slot] });
        }
      }
      leaves.push({ source, selected, path: next, target, key });
    }
    visiting.delete(node);
    return target;
  };
  const instances = collect(tree, []);
  const surface = defineSurface({ views, actions, operations, collections });
  for (const leaf of leaves) {
    const namespace = 'm_' + leaf.path.map(part => part.length + '_' + part).join('_') + '__';
    if (namespace.length >= 80) throw new Error('REACTIVE_MODULE_TOOL_NAME_INVALID');
    const mount = Object.freeze({ [mountBrand]: true as const, surface: leaf.source, selected: leaf.selected, path: leaf.path });
    mounts.set(mount, { root: surface, source: leaf.source, selected: leaf.selected,
      prefix: leaf.path.join('.') + '.', namespace });
    leaf.target[leaf.key] = mount;
  }
  for (const leaf of collectionLeaves) {
    const collection = Object.freeze({ surface: leaf.definition.source, selected: leaf.definition.surface, path: leaf.path,
      instance(key: string): SurfaceMount {
        if (typeof key !== 'string' || !key) throw new Error('REACTIVE_MODULE_INSTANCE_KEY_INVALID');
        const path = Object.freeze([...leaf.path, key]);
        const mount = Object.freeze({ [mountBrand]: true as const, surface: leaf.definition.source, selected: leaf.definition.surface, path });
        mounts.set(mount, { root: surface, source: leaf.definition.source, selected: leaf.definition.surface,
          prefix: '', collection: { name: leaf.path.join('.'), key },
          memberships: [{ directory: surfaceCollectionKeysSlot(leaf.path.join('.')), key }] });
        return mount;
      },
    });
    leaf.target[leaf.key] = collection;
  }
  const freeze = (node: Record<string, unknown>) => {
    for (const value of Object.values(node)) if (typeof value === 'object' && value !== null && !Object.isFrozen(value)) freeze(value as Record<string, unknown>);
    Object.freeze(node);
  };
  freeze(instances);
  // 所有扁平 key 与叶合同均来自同一次受核验的纯树展开。
  return Object.freeze({ surface, mounts: instances }) as SurfaceComposition<T>;
}

/** @internal 声明映射使用精确 token，不允许结构化伪造同名路径。 */
function mountInfo(mount: SurfaceMount): MountInfo {
  const value = typeof mount === 'object' && mount !== null ? mounts.get(mount) : undefined;
  if (!value) throw new Error('REACTIVE_MODULE_MOUNT_INVALID');
  return value;
}
/** @internal 模块与根输出的合同核验。 */
export function moduleRootSurface(mount: SurfaceMount): Surface { return mountInfo(mount).root; }
/** @internal 声明帧的成员事实来自真正挂载点，不从 Expose 或工具字符串猜集合。 */
export function moduleCollectionInstance(mount: SurfaceMount): { readonly name: string; readonly key: string } | undefined { return mountInfo(mount).collection; }
export function moduleCollectionMemberships(mount: SurfaceMount): readonly { readonly directory: string; readonly key: string }[] {
  return mountInfo(mount).memberships ?? [];
}
function mappedSlot(info: MountInfo, kind: SurfaceSlotKind, slot: string): string | undefined {
  if (!surfaceSlotDefinition(info.selected, kind, slot)) return undefined;
  if (info.mapSlot) return info.mapSlot(kind, slot);
  return info.collection ? surfaceInstanceSlot(info.collection.name, info.collection.key, slot) : prefixSurfaceSlot(info.prefix, slot);
}
/** @internal 消费投射复用声明的精确映射；隐藏槽没有可用地址。 */
export function moduleClientSlot(mount: SurfaceMount, kind: SurfaceSlotKind, slot: string): string {
  const name = mappedSlot(mountInfo(mount), kind, slot);
  if (name === undefined) throw new Error('REACTIVE_MODULE_SLOT_NOT_VISIBLE');
  return name;
}
/** @internal 长度前缀是无歧义编码；不通过替换、截断或 React key 猜名字。 */
export function moduleName(mount: SurfaceMount, local: string, tool: boolean): string {
  const namespace = mountInfo(mount).namespace;
  if (namespace === undefined) throw new Error('REACTIVE_MODULE_NAMESPACE_REQUIRED');
  const result = namespace + local;
  if (tool && !/^[a-z][a-z0-9_]{0,79}$/u.test(result)) throw new Error('REACTIVE_MODULE_TOOL_NAME_INVALID');
  return result;
}
/** @internal 先核本地合同，再只公开组合合同明确选择的槽；始终复用原绑定与 Observable。 */
export function moduleContribution(mount: SurfaceMount, source: Surface, value: SurfaceBindingReferences):
  { readonly surface: Surface; readonly value: SurfaceBindingReferences } {
  const info = mountInfo(mount);
  if (source !== info.source) throw new Error('REACTIVE_MODULE_SURFACE_MISMATCH');
  assertSurfaceBindings(source, value);
  const project = <V>(entries: Readonly<Record<string, V>> | undefined, kind: SurfaceSlotKind) =>
    Object.freeze(Object.fromEntries(Object.entries(entries ?? {}).flatMap(([key, entry]) => {
      const name = mappedSlot(info, kind, key);
      return name === undefined ? [] : [[name, entry]];
    })));
  return { surface: info.root, value: Object.freeze({
    views: project(value.views, 'views'), actions: project(value.actions, 'actions'),
    operations: project(value.operations, 'operations'),
  }) };
}

/** @internal 真实Module物化动态声明名称，再将显式子挂载映射到父合同。 */
export function rebaseModuleMount(mount: SurfaceMount, parent: SurfaceMount | null, dynamicNamespace?: string): SurfaceMount {
    const child = mountInfo(mount);
    const namespace = child.collection ? dynamicNamespace : child.namespace;
    if (namespace === undefined) throw new Error('REACTIVE_MODULE_NAMESPACE_REQUIRED');
    if (!parent) {
      if (!child.collection) return mount;
      const materialized = Object.freeze({ ...mount });
      mounts.set(materialized, { ...child, namespace });
      return materialized;
    }
    const outer = mountInfo(parent);
    if (child.root !== outer.source) throw new Error('REACTIVE_MODULE_PARENT_MISMATCH');
    if (outer.namespace === undefined) throw new Error('REACTIVE_MODULE_NAMESPACE_REQUIRED');
    const rebased = Object.freeze({ [mountBrand]: true as const, surface: mount.surface, selected: mount.selected,
      path: Object.freeze([...parent.path, ...mount.path]) });
    mounts.set(rebased, { ...child, root: outer.root, prefix: '', namespace: outer.namespace + namespace,
      collection: outer.collection ?? child.collection,
      memberships: [...outer.memberships ?? [], ...(child.memberships ?? []).flatMap(instance => {
        const directory = mappedSlot(outer, 'views', instance.directory);
        return directory === undefined ? [] : [{ directory, key: instance.key }];
      })],
      mapSlot: (kind, slot) => {
        const inner = mappedSlot(child, kind, slot);
        return inner === undefined ? undefined : mappedSlot(outer, kind, inner);
      },
    });
    return rebased;
}
