import { isObservable, type Observable } from 'rxjs';
import { z } from 'zod';
import { isBoundAction, type ActionContract, type ActionReference, type BoundAction } from '../actions/contract.ts';
import { isOperationBinding, type OperationContract, type OperationBindingReference, type BoundOperation } from '../operations/contract.ts';
import type { Json } from '../conversations/protocol.ts';
import { resourceObservations, type ResourceSource } from '../resources/source.ts';
import { actionManifestSlot, toolActionBindingSchema } from './presentation.ts';

export type ViewMode = 'value' | 'resource';
export interface ViewDefinition<S extends z.ZodType = z.ZodType, M extends ViewMode = ViewMode> {
  readonly schema: S;
  readonly mode: M;
}
export type ViewValue<V extends ViewDefinition> = z.output<V['schema']>;
/** wire 形态由同一 Zod schema 的输入决定，并且必须是 JSON。 */
export type ViewWire<V extends ViewDefinition> = z.input<V['schema']> & Json;
/** 资源来源携带当前有效性；普通值流的 emission 直接代表领域值。 */
export type ViewSource<V extends ViewDefinition> = V extends { readonly mode: 'resource' }
  ? ResourceSource<ViewValue<V>> : Observable<ViewValue<V>>;
type ViewDefinitions = Readonly<Record<string, ViewDefinition>>;
type ActionDefinitions = Readonly<Record<string, ActionContract>>;
type OperationDefinitions = Readonly<Record<string, OperationContract>>;
const surfaceCollectionBrand = Symbol('surface-collection');
export interface SurfaceCollectionDefinition<S extends Surface = Surface, Selected extends Surface = S> {
  readonly [surfaceCollectionBrand]: true;
  readonly source: S;
  readonly surface: Selected;
}
type CollectionDefinitions = Readonly<Record<string, SurfaceCollectionDefinition>>;

export interface Surface<V extends ViewDefinitions = ViewDefinitions, A extends ActionDefinitions = ActionDefinitions,
  O extends OperationDefinitions = OperationDefinitions, C extends CollectionDefinitions = CollectionDefinitions> {
  readonly views: V;
  readonly actions: A;
  readonly operations: O;
  readonly collections: C;
}
/** 消费者读取的领域值流；pending/error 由公开观察面表达，不嵌入领域类型。 */
export type SurfaceViews<S extends Surface> = {
  readonly [K in keyof S['views']]: Observable<ViewValue<S['views'][K]>>;
};
export type SurfaceBindings<S extends Surface> = {
  readonly views: { readonly [K in keyof S['views']]: ViewSource<S['views'][K]> };
  readonly actions: { readonly [K in keyof S['actions']]: BoundAction<S['actions'][K]> };
  readonly operations: { readonly [K in keyof S['operations']]: BoundOperation<S['operations'][K]> };
};
/** 每个模块可贡献部分槽；跨模块重复槽由最终 collector 拒绝。 */
export type PartialSurfaceBindings<S extends Surface> = {
  readonly views?: Partial<SurfaceBindings<S>['views']>;
  readonly actions?: Partial<SurfaceBindings<S>['actions']>;
  readonly operations?: Partial<SurfaceBindings<S>['operations']>;
};
/** 异构声明帧只携带来源和合同身份，不声称保留可调用的泛型输入。 */
export interface SurfaceBindingReferences {
  readonly views?: Readonly<Record<string, ResourceSource<unknown> | Observable<unknown>>>;
  readonly actions?: Readonly<Record<string, ActionReference>>;
  readonly operations?: Readonly<Record<string, OperationBindingReference>>;
}
type ExactKeys<Value, Keys extends PropertyKey> = Value & Record<Exclude<keyof Value, Keys>, never>;
export type ExactSurfaceBindings<S extends Surface, B extends PartialSurfaceBindings<S>> =
  ExactKeys<B, 'views' | 'actions' | 'operations'> & {
    readonly views?: ExactKeys<NonNullable<B['views']>, keyof S['views']>;
    readonly actions?: ExactKeys<NonNullable<B['actions']>, keyof S['actions']>;
    readonly operations?: ExactKeys<NonNullable<B['operations']>, keyof S['operations']>;
  };

const viewDefinitions = new WeakSet<object>();
const surfaces = new WeakSet<object>();
const collections = new WeakSet<object>();
const json = z.json();

/** @internal 组合合同只识别 defineSurface 产生的确切定义。 */
export function isSurfaceDefinition(value: unknown): value is Surface {
  return typeof value === 'object' && value !== null && surfaces.has(value);
}

/** 本地使用 schema output；跨进程复用 Zod encode/decode，不再定义另一套 codec。 */
export function view<S extends z.ZodType>(schema: S): ViewDefinition<S, 'value'> {
  return defineView(schema, 'value');
}

/** schema 仍描述领域值；来源的 pending/error 由资源观察合同单独表达。 */
export function resourceView<S extends z.ZodType>(schema: S): ViewDefinition<S, 'resource'> {
  return defineView(schema, 'resource');
}

function defineView<S extends z.ZodType, M extends ViewMode>(schema: S, mode: M): ViewDefinition<S, M> {
  const definition = Object.freeze({ schema, mode });
  viewDefinitions.add(definition);
  return definition;
}

export function defineSurface<const V extends ViewDefinitions, const A extends ActionDefinitions, const O extends OperationDefinitions = {},
  const C extends CollectionDefinitions = {}>(
  definition: { readonly views: V; readonly actions: A; readonly operations?: O; readonly collections?: C },
): Surface<V, A, O, C> {
  for (const slots of [definition.views, definition.actions, definition.operations ?? {}]) {
    for (const key of Reflect.ownKeys(slots)) if (typeof key !== 'string' || key.startsWith('$')) throw new Error('REACTIVE_SURFACE_SLOT_NAME_INVALID');
  }
  for (const candidate of Object.values(definition.views)) assertView(candidate);
  for (const candidate of Object.values(definition.collections ?? {})) {
    if (!isSurfaceCollection(candidate)) throw new Error('REACTIVE_SURFACE_COLLECTION_INVALID');
  }
  const surface: Surface<V, A, O, C> = Object.freeze({
    views: Object.freeze({ ...definition.views }),
    actions: Object.freeze({ ...definition.actions }),
    operations: Object.freeze({ ...definition.operations }) as O,
    collections: Object.freeze({ ...definition.collections }) as C,
  });
  surfaces.add(surface);
  return surface;
}

/** @internal collectionSurface 在保留原模块合同的同时登记选择后的公开合同。 */
export function defineSurfaceCollection<S extends Surface, Selected extends Surface>(source: S, surface: Selected): SurfaceCollectionDefinition<S, Selected> {
  assertSurfaceBindings(source, {}); assertSurfaceBindings(surface, {});
  const collection = Object.freeze({ [surfaceCollectionBrand]: true as const, source, surface });
  collections.add(collection);
  return collection;
}
export function isSurfaceCollection(value: unknown): value is SurfaceCollectionDefinition {
  return typeof value === 'object' && value !== null && collections.has(value);
}

export type SurfaceSlotKind = 'views' | 'actions' | 'operations';
export interface SurfaceInstanceSlot { readonly collection: string; readonly key: string; readonly slot: string; }
const collectionKeysView = view(z.string().array().readonly());
const actionManifestView = view(toolActionBindingSchema.array().readonly());
/** 成员目录使用同一 view 传输，不把聚合业务事实当成实例路由。 */
export function surfaceCollectionKeysSlot(collection: string): string {
  if (!collection) throw new Error('REACTIVE_SURFACE_INSTANCE_ADDRESS_INVALID');
  return '$' + JSON.stringify([collection]);
}
export function parseSurfaceCollectionKeysSlot(name: string): string | undefined {
  if (!name.startsWith('$')) return undefined;
  let value: unknown;
  try { value = JSON.parse(name.slice(1)); } catch { return undefined; }
  if (!Array.isArray(value) || value.length !== 1 || typeof value[0] !== 'string' || !value[0]
    || '$' + JSON.stringify(value) !== name) return undefined;
  return value[0];
}
export function isSurfaceCollectionKeysSlot(name: string): boolean {
  if (name === actionManifestSlot) return false;
  if (parseSurfaceCollectionKeysSlot(name) !== undefined) return true;
  const address = parseSurfaceInstanceSlot(name);
  return address !== undefined && isSurfaceCollectionKeysSlot(address.slot);
}
/** 地址保留完整业务 key；原生工具的短名不承担 SDK 寻址。 */
export function surfaceInstanceSlot(collection: string, key: string, slot: string): string {
  if (!collection || typeof key !== 'string' || !key || !slot) throw new Error('REACTIVE_SURFACE_INSTANCE_ADDRESS_INVALID');
  return '$' + JSON.stringify([collection, key, slot]);
}
export function parseSurfaceInstanceSlot(name: string): SurfaceInstanceSlot | undefined {
  if (!name.startsWith('$')) return undefined;
  let value: unknown;
  try { value = JSON.parse(name.slice(1)); } catch { throw new Error('REACTIVE_SURFACE_INSTANCE_ADDRESS_INVALID'); }
  if (!Array.isArray(value) || value.length !== 3 || value.some(part => typeof part !== 'string' || !part)
    || '$' + JSON.stringify(value) !== name) throw new Error('REACTIVE_SURFACE_INSTANCE_ADDRESS_INVALID');
  const [collection, key, slot] = value as [string, string, string];
  return { collection, key, slot };
}
/** 一条嵌套地址对应的真实实例链；用于核验每一级实际挂载，不据字符串猜业务权限。 */
export function surfaceSlotInstances(name: string): readonly { readonly directory: string; readonly key: string }[] {
  if (name === actionManifestSlot || parseSurfaceCollectionKeysSlot(name) !== undefined) return [];
  const address = parseSurfaceInstanceSlot(name);
  if (!address) return [];
  return [{ directory: surfaceCollectionKeysSlot(address.collection), key: address.key },
    ...surfaceSlotInstances(address.slot).map(instance => ({
      directory: surfaceInstanceSlot(address.collection, address.key, instance.directory), key: instance.key,
    }))];
}
/** @internal 静态组合也可以包含动态地址；只给 collection 路径加前缀，不改业务 key。 */
export function prefixSurfaceSlot(prefix: string, slot: string): string {
  const collection = parseSurfaceCollectionKeysSlot(slot);
  if (collection !== undefined) return surfaceCollectionKeysSlot(prefix + collection);
  const address = parseSurfaceInstanceSlot(slot);
  return address ? surfaceInstanceSlot(prefix + address.collection, address.key, address.slot) : prefix + slot;
}
/** 已知槽保留原schema类型；动态collection地址仍只查同一叶合同，未知地址不会扩大合同。 */
export function surfaceSlotDefinition<S extends Surface, K extends keyof S['views'] & string>(surface: S, kind: 'views', name: K): S['views'][K] | undefined;
export function surfaceSlotDefinition<S extends Surface, K extends keyof S['actions'] & string>(surface: S, kind: 'actions', name: K): S['actions'][K] | undefined;
export function surfaceSlotDefinition<S extends Surface, K extends keyof S['operations'] & string>(surface: S, kind: 'operations', name: K): S['operations'][K] | undefined;
export function surfaceSlotDefinition(surface: Surface, kind: 'views', name: string): ViewDefinition | undefined;
export function surfaceSlotDefinition(surface: Surface, kind: 'actions', name: string): ActionContract | undefined;
export function surfaceSlotDefinition(surface: Surface, kind: 'operations', name: string): OperationContract | undefined;
export function surfaceSlotDefinition(surface: Surface, kind: SurfaceSlotKind, name: string): ViewDefinition | ActionContract | OperationContract | undefined;
export function surfaceSlotDefinition(surface: Surface, kind: SurfaceSlotKind, name: string): ViewDefinition | ActionContract | OperationContract | undefined {
  if (!isSurfaceDefinition(surface)) throw new Error('REACTIVE_SURFACE_INVALID');
  if (kind === 'views' && name === actionManifestSlot) return actionManifestView;
  if (Object.hasOwn(surface[kind], name)) return surface[kind][name];
  const collection = parseSurfaceCollectionKeysSlot(name);
  if (collection !== undefined) return kind === 'views' && Object.hasOwn(surface.collections, collection) ? collectionKeysView : undefined;
  const address = parseSurfaceInstanceSlot(name);
  if (!address || !Object.hasOwn(surface.collections, address.collection)) return undefined;
  if (address.slot === actionManifestSlot) return undefined;
  return surfaceSlotDefinition(surface.collections[address.collection]!.surface, kind, address.slot);
}

export type CollectionSurface<C extends SurfaceCollectionDefinition> = C['surface'];

export function encodeView<V extends ViewDefinition>(definition: V, value: NoInfer<ViewValue<V>>): ViewWire<V> {
  assertView(definition);
  // encode 自己负责 schema 的反向校验；这里只核验 wire 的 JSON 边界。
  return json.parse(z.encode(definition.schema, value)) as ViewWire<V>;
}

export function decodeView<V extends ViewDefinition>(definition: V, wire: NoInfer<ViewWire<V>>): ViewValue<V> {
  assertView(definition);
  return z.decode(definition.schema, wire) as ViewValue<V>;
}

function assertView(value: unknown): asserts value is ViewDefinition {
  if (typeof value !== 'object' || value === null || !viewDefinitions.has(value)) throw new Error('REACTIVE_VIEW_DEFINITION_INVALID');
}

/** @internal JSX 擦除之后继续按原 Surface 对象与 Action 定义 identity 核验。 */
export function assertSurfaceBindings(surface: Surface, value: unknown): asserts value is SurfaceBindingReferences {
  if (!surfaces.has(surface)) throw new Error('REACTIVE_SURFACE_INVALID');
  if (!isRecord(value)) throw new Error('REACTIVE_SURFACE_BINDINGS_INVALID');
  for (const key of Reflect.ownKeys(value)) if (key !== 'views' && key !== 'actions' && key !== 'operations') throw new Error('REACTIVE_SURFACE_BINDINGS_KEY_INVALID');
  if (value.views !== undefined) {
    if (!isRecord(value.views)) throw new Error('REACTIVE_SURFACE_VIEWS_INVALID');
    for (const key of Reflect.ownKeys(value.views)) {
      if (typeof key !== 'string') throw new Error('REACTIVE_SURFACE_VIEW_UNKNOWN');
      const definition = surfaceSlotDefinition(surface, 'views', key);
      if (!definition) throw new Error('REACTIVE_SURFACE_VIEW_UNKNOWN');
      if (definition.mode === 'resource') resourceObservations(value.views[key] as ResourceSource<unknown>);
      else if (!isObservable(value.views[key])) throw new Error('REACTIVE_SURFACE_VIEW_OBSERVABLE_REQUIRED');
    }
  }
  if (value.actions !== undefined) {
    if (!isRecord(value.actions)) throw new Error('REACTIVE_SURFACE_ACTIONS_INVALID');
    for (const key of Reflect.ownKeys(value.actions)) {
      if (typeof key !== 'string' || !surfaceSlotDefinition(surface, 'actions', key)) throw new Error('REACTIVE_SURFACE_ACTION_UNKNOWN');
      const action = value.actions[key];
      if (!isBoundAction(action) || action.definition !== surfaceSlotDefinition(surface, 'actions', key)) {
        throw new Error('REACTIVE_SURFACE_ACTION_CONTRACT_MISMATCH');
      }
    }
  }
  if (value.operations !== undefined) {
    if (!isRecord(value.operations)) throw new Error('REACTIVE_SURFACE_OPERATIONS_INVALID');
    for (const key of Reflect.ownKeys(value.operations)) {
      if (typeof key !== 'string' || !surfaceSlotDefinition(surface, 'operations', key)) throw new Error('REACTIVE_SURFACE_OPERATION_UNKNOWN');
      const operation = value.operations[key];
      if (!isOperationBinding(operation) || operation.definition !== surfaceSlotDefinition(surface, 'operations', key)) {
        throw new Error('REACTIVE_SURFACE_OPERATION_CONTRACT_MISMATCH');
      }
    }
  }
}

function isRecord(value: unknown): value is Record<PropertyKey, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
