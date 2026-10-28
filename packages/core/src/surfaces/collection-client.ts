import { defer, type Observable } from 'rxjs';
import type { z } from 'zod';
import type { ActionContract, ActionInput, ActionInvocation, ActionScope } from '../actions/contract.ts';
import type { BoundOperation, OperationContract } from '../operations/contract.ts';
import type { CancellationOptions } from '../conversations/protocol.ts';
import type { Observation } from '../resources/observation.ts';
import type { ResourceReader } from '../resources/reader.ts';
import { surfaceCollectionKeysSlot, surfaceInstanceSlot, surfaceSlotDefinition, type Surface, type SurfaceCollectionDefinition, type SurfaceViews, type ViewDefinition, type ViewValue } from './contract.ts';

export type CollectionResult<Async extends boolean, T> = Async extends true ? Promise<T> : T;
export interface CollectionResolvedAction<D extends ActionContract, Options extends CancellationOptions, Async extends boolean> {
  (input: ActionInput<D>, options?: Options): CollectionResult<Async, ActionInvocation<D>>;
  readonly definition: D;
  readonly bindingId: string;
  readonly scope: ActionScope<D>;
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
  readonly operations: { readonly [K in keyof S['operations']]: (options?: Options) => CollectionResult<Async, BoundOperation<S['operations'][K]>> };
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
  resolveAction(name: string, definition: ActionContract, expectedScope: unknown, options?: Options):
    CollectionResult<Async, CollectionResolvedAction<ActionContract, Options, Async>>;
  resolveOperation(name: string, definition: OperationContract, options?: Options): CollectionResult<Async, BoundOperation<OperationContract>>;
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
        const local = definition.surface;
        const address = (slot: string) => surfaceInstanceSlot(name, key, slot);
        const viewDefinition = (slot: string): ViewDefinition => {
          const found = surfaceSlotDefinition(local, 'views', slot);
          if (!found) throw new Error('REACTIVE_VIEW_NOT_FOUND');
          return found;
        };
        const viewResource = (slot: string): ResourceReader<unknown> => transport.viewResource(address(slot), viewDefinition(slot));
        instance = Object.freeze({ key, surface: local,
          views: Object.freeze(Object.fromEntries(Object.keys(local.views).map(slot => [slot, defer(() => viewResource(slot).values$)]))),
          viewResource,
          observeView: (slot: string) => viewResource(slot).observations$,
          readView: (slot: string, options?: Options) => viewResource(slot).read(options?.signal),
          resolveAction(slot: string, expectedScope: unknown, options?: Options) {
            const action = surfaceSlotDefinition(local, 'actions', slot);
            if (!action) throw new Error('REACTIVE_ACTION_SLOT_UNKNOWN');
            return transport.resolveAction(address(slot), action, expectedScope, options);
          },
          operations: Object.freeze(Object.fromEntries(Object.entries(local.operations).map(([slot, operation]) => [slot,
            (options?: Options) => transport.resolveOperation(address(slot), operation, options)]))),
          collection: (name: string) => createSurfaceCollectionClient<Surface, string, Options, Async>(local, name, {
            asynchronous: transport.asynchronous,
            viewResource: (slot, definition) => transport.viewResource(address(slot), definition),
            resolveAction: (slot, definition, scope, options) => transport.resolveAction(address(slot), definition, scope, options),
            resolveOperation: (slot, definition, options) => transport.resolveOperation(address(slot), definition, options),
          }),
        });
        instances.set(key, instance);
      }
      // 所有槽来自上面同一 selected Surface；transport 保留本地/远程原有返回时序。
      return instance as SurfaceInstanceClient<S['collections'][K]['surface'], Options, Async>;
    },
  });
}
