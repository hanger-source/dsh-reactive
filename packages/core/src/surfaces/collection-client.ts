import { defer, type Observable } from 'rxjs';
import type { z } from 'zod';
import type { ActionContract, ActionInput, ActionInvocation, ActionScope } from '../actions/contract.ts';
import type { BoundOperation, OperationContract } from '../operations/contract.ts';
import type { CancellationOptions } from '../conversations/protocol.ts';
import type { Observation } from '../resources/observation.ts';
import type { ResourceReader } from '../resources/reader.ts';
import type { Eligibility } from '../resources/eligibility.ts';
import { moduleClientSlot, moduleRootSurface, type SurfaceMount } from './composition.ts';
import { surfaceCollectionKeysSlot, surfaceInstanceSlot, surfaceSlotDefinition, type Surface, type SurfaceCollectionDefinition, type SurfaceSlotKind, type SurfaceViews, type ViewDefinition, type ViewValue } from './contract.ts';

export type CollectionResult<Async extends boolean, T> = Async extends true ? Promise<T> : T;
export interface CollectionResolvedAction<D extends ActionContract, Options extends CancellationOptions, Async extends boolean> {
  (input: ActionInput<D>, options?: Options): CollectionResult<Async, ActionInvocation<D>>;
  readonly definition: D;
  readonly bindingId: string;
  readonly scope: ActionScope<D>;
  readonly eligibility?: ResourceReader<Eligibility>;
}
/** 根会话与集合成员共用的消费合同；调用时序由实际本地或远程入口决定。 */
export interface SurfaceClient<S extends Surface, Options extends CancellationOptions = CancellationOptions, Async extends boolean = boolean> {
  readonly surface: S;
  readonly views: SurfaceViews<S>;
  viewResource<K extends keyof S['views'] & string>(name: K): ResourceReader<ViewValue<S['views'][K]>>;
  observeView<K extends keyof S['views'] & string>(name: K): Observable<Observation<ViewValue<S['views'][K]>>>;
  readView<K extends keyof S['views'] & string>(name: K, options?: Options): Promise<ViewValue<S['views'][K]>>;
  resolveAction<K extends keyof S['actions'] & string>(name: K, expectedScope: z.input<S['actions'][K]['scope']>, options?: Options):
    CollectionResult<Async, CollectionResolvedAction<S['actions'][K], Options, Async>>;
  /** 查回同一受理者持有的原调用；不依赖当前Expose，也不重新执行。 */
  invocation<K extends keyof S['actions'] & string>(name: K, id: string, options?: Options): CollectionResult<Async, ActionInvocation<S['actions'][K]>>;
  readonly operations: { readonly [K in keyof S['operations']]: (options?: Options) => CollectionResult<Async, BoundOperation<S['operations'][K]>> };
  /** 借用原挂载的局部公开合同，不取得新的执行身份或寿命。 */
  module<Source extends Surface, Selected extends Surface>(mount: SurfaceMount<Source, Selected>): SurfaceClient<Selected, Options, Async>;
  collection<K extends keyof S['collections'] & string>(name: K): SurfaceCollectionClient<S['collections'][K], Options, Async>;
}
export interface SurfaceInstanceClient<S extends Surface, Options extends CancellationOptions = CancellationOptions, Async extends boolean = false>
  extends SurfaceClient<S, Options, Async> {
  readonly key: string;
}
export interface SurfaceCollectionClient<C extends SurfaceCollectionDefinition, Options extends CancellationOptions = CancellationOptions, Async extends boolean = false> {
  /** 同一成员目录的有效性与快照；不注册成员或取得其执行owner。 */
  readonly instances: ResourceReader<readonly string[]>;
  readonly instances$: Observable<readonly string[]>;
  instance(key: string): SurfaceInstanceClient<C['surface'], Options, Async>;
}
/** SDK 只转交已存在的查询和执行入口；它不持有新的执行信号或程序 owner。 */
export interface SurfaceCollectionTransport<Options extends CancellationOptions, Async extends boolean> {
  readonly asynchronous: Async;
  viewResource(name: string, definition: ViewDefinition): ResourceReader<unknown>;
  resolveAction<D extends ActionContract>(name: string, definition: D, expectedScope: z.input<D['scope']>, options?: Options):
    CollectionResult<Async, CollectionResolvedAction<D, Options, Async>>;
  invocation<D extends ActionContract>(name: string, definition: D, id: string, options?: Options): CollectionResult<Async, ActionInvocation<D>>;
  resolveOperation(name: string, definition: OperationContract, options?: Options): CollectionResult<Async, BoundOperation<OperationContract>>;
}

type SurfaceAddress = (kind: SurfaceSlotKind, slot: string) => string;

/** 只变换地址；合同、选项、返回时序及实际资源对象继续由原入口决定。 */
function readdress<Options extends CancellationOptions, Async extends boolean>(
  transport: SurfaceCollectionTransport<Options, Async>, address: SurfaceAddress,
): SurfaceCollectionTransport<Options, Async> {
  return {
    asynchronous: transport.asynchronous,
    viewResource: (slot, definition) => transport.viewResource(address('views', slot), definition),
    resolveAction: (slot, definition, scope, options) => transport.resolveAction(address('actions', slot), definition, scope, options),
    invocation: (slot, definition, id, options) => transport.invocation(address('actions', slot), definition, id, options),
    resolveOperation: (slot, definition, options) => transport.resolveOperation(address('operations', slot), definition, options),
  };
}

/** 静态与集合成员消费同一局部入口；source只用于精确身份核验，不增加可见能力。 */
function createSurfaceClient<S extends Surface, Options extends CancellationOptions, Async extends boolean>(
  source: Surface, surface: S, transport: SurfaceCollectionTransport<Options, Async>,
): SurfaceClient<S, Options, Async> {
  const viewResource = (slot: string): ResourceReader<unknown> => {
    const definition = surfaceSlotDefinition(surface, 'views', slot);
    if (!definition) throw new Error('REACTIVE_VIEW_NOT_FOUND');
    return transport.viewResource(slot, definition);
  };
  const client = {
    surface,
    views: Object.freeze(Object.fromEntries(Object.keys(surface.views).map(slot => [slot, defer(() => viewResource(slot).values$)]))),
    viewResource,
    observeView: (slot: string) => viewResource(slot).observations$,
    readView: (slot: string, options?: Options) => viewResource(slot).read(options?.signal),
    resolveAction<K extends keyof S['actions'] & string>(slot: K, expectedScope: z.input<S['actions'][K]['scope']>, options?: Options) {
      const definition = surfaceSlotDefinition(surface, 'actions', slot);
      if (!definition) throw new Error('REACTIVE_ACTION_SLOT_UNKNOWN');
      return transport.resolveAction(slot, definition, expectedScope, options);
    },
    invocation<K extends keyof S['actions'] & string>(slot: K, id: string, options?: Options) {
      const definition = surfaceSlotDefinition(surface, 'actions', slot);
      if (!definition) throw new Error('REACTIVE_ACTION_SLOT_UNKNOWN');
      return transport.invocation(slot, definition, id, options);
    },
    operations: Object.freeze(Object.fromEntries(Object.entries(surface.operations).map(([slot, definition]) => [slot,
      (options?: Options) => transport.resolveOperation(slot, definition, options)]))),
    module<Source extends Surface, Selected extends Surface>(mount: SurfaceMount<Source, Selected>) {
      return projectModule(source, surface, mount, transport);
    },
    collection: (name: keyof S['collections'] & string) => createSurfaceCollectionClient(surface, name, transport),
  };
  // Action方法已保留K对应的实际D；异构views/operations均由同一surface查得，集中恢复映射形状。
  return Object.freeze(client) as SurfaceClient<S, Options, Async>;
}

function projectModule<Source extends Surface, Selected extends Surface, Options extends CancellationOptions, Async extends boolean>(
  source: Surface, visible: Surface, mount: SurfaceMount<Source, Selected>, transport: SurfaceCollectionTransport<Options, Async>,
): SurfaceClient<Selected, Options, Async> {
  if (moduleRootSurface(mount) !== source) throw new Error('REACTIVE_MODULE_ROOT_MISMATCH');
  const address: SurfaceAddress = (kind, slot) => {
    const name = moduleClientSlot(mount, kind, slot);
    const definition = surfaceSlotDefinition(mount.selected, kind, slot);
    if (!definition || surfaceSlotDefinition(visible, kind, name) !== definition) throw new Error('REACTIVE_MODULE_SLOT_NOT_VISIBLE');
    return name;
  };
  // 返回型承诺整个selected合同；外层选择更窄时拒绝整次投射，不恢复隐藏槽。
  for (const kind of ['views', 'actions', 'operations'] as const) {
    for (const slot of Object.keys(mount.selected[kind])) address(kind, slot);
  }
  for (const collection of Object.keys(mount.selected.collections)) address('views', surfaceCollectionKeysSlot(collection));
  return createSurfaceClient(mount.surface, mount.selected, readdress(transport, address));
}

/** 根SDK按精确mount借出局部合同；不读取资源，也不创建缓存或owner。 */
export function createSurfaceModuleClient<Root extends Surface, Source extends Surface, Selected extends Surface,
  Options extends CancellationOptions, Async extends boolean>(surface: Root, mount: SurfaceMount<Source, Selected>,
  transport: SurfaceCollectionTransport<Options, Async>): SurfaceClient<Selected, Options, Async> {
  return projectModule(surface, surface, mount, transport);
}

/** 根合同决定 collection 与叶合同；业务 key 始终由框架编码，调用仍核原绑定。 */
export function createSurfaceCollectionClient<S extends Surface, K extends keyof S['collections'] & string,
  Options extends CancellationOptions, Async extends boolean>(surface: S, name: K,
  transport: SurfaceCollectionTransport<Options, Async>): SurfaceCollectionClient<S['collections'][K], Options, Async> {
  if (!Object.hasOwn(surface.collections, name)) throw new Error('REACTIVE_SURFACE_COLLECTION_UNKNOWN');
  const definition = surface.collections[name]!;
  const directory = surfaceCollectionKeysSlot(name);
  const directoryDefinition = surfaceSlotDefinition(surface, 'views', directory)!;
  const instances = new Map<string, SurfaceInstanceClient<Surface, Options, Async>>();
  return Object.freeze({
    get instances() { return transport.viewResource(directory, directoryDefinition) as ResourceReader<readonly string[]>; },
    get instances$() { return this.instances.values$; },
    instance(key: string) {
      // 编码同时核验非空 key；不把 key 截断、sanitize 或变成业务 Action input。
      surfaceInstanceSlot(name, key, 'instance');
      let instance = instances.get(key);
      if (!instance) {
        const client = createSurfaceClient(definition.source, definition.surface,
          readdress(transport, (_kind, slot) => surfaceInstanceSlot(name, key, slot)));
        instance = Object.freeze({ key, ...client });
        instances.set(key, instance);
      }
      // 所有槽来自上面同一 selected Surface；transport 保留本地/远程原有返回时序。
      return instance as SurfaceInstanceClient<S['collections'][K]['surface'], Options, Async>;
    },
  });
}
