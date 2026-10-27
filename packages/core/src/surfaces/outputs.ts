import { EmptyError, Subscriber, map, type Observable } from 'rxjs';
import type { ActionReference } from '../actions/contract.ts';
import type { OperationBindingReference } from '../operations/contract.ts';
import type { Json } from '../conversations/protocol.ts';
import { actionManifestSlot, toolActionBindingSchema, type ToolActionBinding } from './presentation.ts';
import { ResourceValue } from '../resources/value.ts';
import { Scope, batch, type Value } from '../reactivity/scope.ts';
import { type Observation } from '../resources/observation.ts';
import type { ProgramFrame } from '../program/frame.ts';
import { assertSurfaceBindings, encodeView, surfaceSlotInstances, isSurfaceCollectionKeysSlot, surfaceCollectionKeysSlot, surfaceSlotDefinition, type Surface, type ViewDefinition, type ViewValue, type ViewWire } from './contract.ts';
import { moduleRootSurface, moduleCollectionMemberships } from './composition.ts';

export type ProgramViews<S extends Surface> = {
  readonly [K in keyof S['views']]: ResourceValue<ViewValue<S['views'][K]>>;
};
export type ProgramWireViews<S extends Surface> = {
  readonly [K in keyof S['views']]: ResourceValue<ViewWire<S['views'][K]>>;
};
export type ProgramActionReferences<S extends Surface> = Readonly<Partial<{
  readonly [K in keyof S['actions']]: ActionReference;
}>>;
export type ProgramOperationReferences<S extends Surface> = Readonly<Partial<{
  readonly [K in keyof S['operations']]: OperationBindingReference;
}>>;
export interface ProgramOutputs<S extends Surface> {
  readonly views: ProgramViews<S>;
  readonly wireViews: ProgramWireViews<S>;
  readonly actions: Value<ProgramActionReferences<S>>;
  readonly operations: Value<ProgramOperationReferences<S>>;
  readonly actionManifest: Value<readonly ToolActionBinding[]>;
  view(name: string): ResourceValue<unknown>;
  wireView(name: string): ResourceValue<Json>;
  collectionKeys(name: string): Value<readonly string[]>;
  actionNames(action: ActionReference): readonly string[];
  /** 在改写声明或启动来源之前校验完整 Surface 合同，不产生订阅及状态更新。 */
  validate(frame: ProgramFrame): void;
  apply(frame: ProgramFrame): void;
  /** 程序终止错误撤下公开 Action 引用，并使所有当前及等待中的 view 读取失败。 */
  fail(error: unknown): void;
}

interface ViewSlot {
  readonly domain: ResourceValue<unknown>;
  readonly wire: ResourceValue<Json>;
  replace(source: Observable<unknown> | undefined, missing?: unknown): void;
  fail(error: unknown): void;
}

/** JSX 擦除后的资源来源仍须满足公开 Observation 合同，不能按业务字段猜状态。 */
function resourceObservation(value: unknown): Observation<unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)
    || !('status' in value) || !('generation' in value)) {
    throw new Error('REACTIVE_VIEW_OBSERVATION_INVALID');
  }
  const generation = value.generation;
  if (typeof generation !== 'number' || !Number.isSafeInteger(generation) || generation < 0) {
    throw new Error('REACTIVE_VIEW_GENERATION_INVALID');
  }
  if (value.status === 'pending') return { status: 'pending', generation };
  if (value.status === 'error' && 'error' in value) return { status: 'error', generation, error: value.error };
  if (value.status === 'ready' && 'value' in value && 'observedAt' in value) {
    const observedAt = value.observedAt;
    if (typeof observedAt !== 'number' || !Number.isFinite(observedAt)) throw new Error('REACTIVE_VIEW_OBSERVED_AT_INVALID');
    return { status: 'ready', generation, value: value.value, observedAt };
  }
  throw new Error('REACTIVE_VIEW_OBSERVATION_INVALID');
}

interface EncodedView { readonly domain: unknown; readonly wire: Json; }

function createSlot(scope: Scope, definition: ViewDefinition): ViewSlot {
  const domain = scope.state<Observation<unknown>>({ status: 'pending', generation: 0 });
  const wire = scope.state<Observation<Json>>({ status: 'pending', generation: 0 });
  let generation = 0;
  let source: Observable<unknown> | undefined;
  let connection: Scope | undefined;
  let failed = false;
  let unavailable: unknown;
  const pending = () => {
    const current = ++generation;
    domain.set({ status: 'pending', generation: current });
    wire.set({ status: 'pending', generation: current });
  };
  const error = (failure: unknown) => {
    const current = ++generation;
    domain.set({ status: 'error', generation: current, error: failure });
    wire.set({ status: 'error', generation: current, error: failure });
  };
  return {
    domain: new ResourceValue(scope, domain),
    wire: new ResourceValue(scope, wire),
    replace(next, missing) {
      if (next === source && missing === unavailable) return;
      const previous = connection;
      connection = undefined; source = next; unavailable = missing;
      pending();
      previous?.dispose();
      if (missing !== undefined) { error(missing); return; }
      if (!next) return;
      const owner = scope.child();
      connection = owner;
      let status: Observation<unknown>['status'] | undefined;
      const active = () => !failed && !scope.disposed && connection === owner;
      const subscription = new Subscriber<Observation<EncodedView>>({
        next(observation) {
          if (!active()) return;
          status = observation.status;
          batch(() => {
            if (observation.status === 'pending') { pending(); return; }
            if (observation.status === 'error') { error(observation.error); return; }
            const current = ++generation;
            domain.set({ status: 'ready', generation: current, value: observation.value.domain, observedAt: observation.observedAt });
            wire.set({ status: 'ready', generation: current, value: observation.value.wire, observedAt: observation.observedAt });
          });
        },
        error(failure: unknown) {
          if (active()) batch(() => error(failure));
          release();
        },
        complete() {
          // 来源结束时没有当前结果，任何现有及未来 read 都不能永久等待。
          if (active() && (status === undefined || status === 'pending')) batch(() => error(new EmptyError()));
          release();
        },
      });
      const release = owner.own(() => subscription.unsubscribe());
      next.pipe(map((value): Observation<EncodedView> => {
        const observation: Observation<unknown> = definition.mode === 'resource' ? resourceObservation(value)
          : { status: 'ready', generation: 0, value, observedAt: Date.now() };
        if (observation.status !== 'ready') return observation;
        // encode 同时核验 Surface 的 output 合同；每次 ready 只编码一次，不再 decode/parse 领域值。
        return { ...observation, value: { domain: observation.value, wire: encodeView(definition, observation.value) } };
      })).subscribe(subscription);
    },
    fail(failure) {
      failed = true;
      error(failure);
      const previous = connection;
      connection = undefined;
      previous?.dispose();
    },
  };
}

/** 程序 owner 持有公开出口；外部订阅只观察，不能决定来源启停或重试。 */
export function createProgramOutputs<const S extends Surface>(scope: Scope, surface: S): ProgramOutputs<S> {
  scope.assertActive();
  assertSurfaceBindings(surface, {});
  for (const key of [...Reflect.ownKeys(surface.views), ...Reflect.ownKeys(surface.actions), ...Reflect.ownKeys(surface.operations)]) {
    if (typeof key !== 'string') throw new Error('REACTIVE_SURFACE_SLOT_NAME_INVALID');
  }
  const slots = new Map(Object.entries(surface.views).map(([name, definition]) => [name, createSlot(scope, definition)]));
  const members = new Map(Object.keys(surface.collections).map(name => [surfaceCollectionKeysSlot(name), scope.state<readonly string[]>(Object.freeze([]))]));
  const missing = new Map<string, Error>();
  const actionManifest = scope.state<readonly ToolActionBinding[]>(Object.freeze([]));
  let mounted: ReadonlyMap<string, ReadonlySet<string>> = new Map();
  let currentSources: Readonly<Record<string, Observable<unknown>>> = {};
  const instanceMissing = (name: string): Error | undefined => {
    if (surfaceSlotInstances(name).every(instance => mounted.get(instance.directory)?.has(instance.key))) return undefined;
    let error = missing.get(name);
    if (!error) { error = new Error('REACTIVE_MODULE_INSTANCE_NOT_FOUND'); missing.set(name, error); }
    return error;
  };
  const memberState = (directory: string) => {
    let state = members.get(directory);
    if (!state) {
      state = scope.state<readonly string[]>(Object.freeze([...(mounted.get(directory) ?? [])].sort()));
      members.set(directory, state);
    }
    return state;
  };
  const slotFor = (name: string): ViewSlot => {
    scope.assertActive();
    if (failure) throw failure.error;
    const definition = surfaceSlotDefinition(surface, 'views', name);
    if (!definition) throw new Error('REACTIVE_VIEW_NOT_FOUND');
    let slot = slots.get(name);
    if (!slot) {
      slot = createSlot(scope, definition); slots.set(name, slot);
      if (name === actionManifestSlot) slot.replace(actionManifest.changes$);
      else if (isSurfaceCollectionKeysSlot(name)) {
        const absent = instanceMissing(name);
        slot.replace(absent ? undefined : memberState(name).changes$, absent);
      }
      else slot.replace(currentSources[name], instanceMissing(name));
    }
    return slot;
  };
  // 所有 key 来自精确 Surface；每个未知 emission 先经过原 definition 的 encodeView 核验。
  const views = Object.freeze(Object.fromEntries([...slots].map(([name, slot]) => [name, slot.domain]))) as ProgramViews<S>;
  const wireViews = Object.freeze(Object.fromEntries([...slots].map(([name, slot]) => [name, slot.wire]))) as ProgramWireViews<S>;
  const actions = scope.state<ProgramActionReferences<S>>(Object.freeze({}));
  const operations = scope.state<ProgramOperationReferences<S>>(Object.freeze({}));
  let failure: { error: unknown } | undefined;
  const validate = (frame: ProgramFrame) => {
    scope.assertActive();
    if (failure) throw failure.error;
    for (const mount of frame.mounts ?? []) {
      if (moduleRootSurface(mount) !== surface) throw new Error('REACTIVE_MODULE_ROOT_MISMATCH');
    }
    for (const contribution of frame.surfaces) {
      if (contribution.surface !== surface) throw new Error('REACTIVE_PROGRAM_SURFACE_MISMATCH');
    }
    if (frame.surfaces.length > 1) throw new Error('REACTIVE_PROGRAM_SURFACE_DUPLICATE');
    const contribution = frame.surfaces[0];
    if (contribution) assertSurfaceBindings(surface, { views: contribution.views, actions: contribution.actions, operations: contribution.operations });
    const instances = new Map<string, Set<string>>();
    for (const mount of frame.mounts ?? []) {
      for (const instance of moduleCollectionMemberships(mount)) {
        if (!surfaceSlotDefinition(surface, 'views', instance.directory)) throw new Error('REACTIVE_SURFACE_COLLECTION_UNKNOWN');
        let keys = instances.get(instance.directory);
        if (!keys) { keys = new Set(); instances.set(instance.directory, keys); }
        keys.add(instance.key);
      }
    }
    for (const category of [contribution?.views, contribution?.actions, contribution?.operations]) {
      for (const name of Object.keys(category ?? {})) {
        if (name === actionManifestSlot || isSurfaceCollectionKeysSlot(name)) throw new Error('REACTIVE_SURFACE_COLLECTION_DIRECTORY_RESERVED');
        if (surfaceSlotInstances(name).some(instance => !instances.get(instance.directory)?.has(instance.key))) throw new Error('REACTIVE_MODULE_INSTANCE_NOT_MOUNTED');
      }
    }
  };
  return Object.freeze({
    views, wireViews, actions, operations, actionManifest, validate,
    view: (name: string) => slotFor(name).domain,
    wireView: (name: string) => slotFor(name).wire,
    collectionKeys(name: string) {
      scope.assertActive();
      const keys = members.get(surfaceCollectionKeysSlot(name));
      if (!keys) throw new Error('REACTIVE_SURFACE_COLLECTION_UNKNOWN');
      return keys;
    },
    actionNames: (action: ActionReference) => Object.freeze(Object.entries(actions.peek()).filter(([, bound]) => bound === action).map(([name]) => name)),
    apply(frame: ProgramFrame) {
      validate(frame);
      const contribution = frame.surfaces[0];
      const nextMounted = new Map<string, Set<string>>();
      for (const mount of frame.mounts ?? []) {
        for (const instance of moduleCollectionMemberships(mount)) {
          let keys = nextMounted.get(instance.directory);
          if (!keys) { keys = new Set(); nextMounted.set(instance.directory, keys); }
          keys.add(instance.key);
        }
      }
      mounted = nextMounted; currentSources = contribution?.views ?? {};
      const nextActions = Object.freeze({ ...contribution?.actions }) as ProgramActionReferences<S>;
      const nextOperations = Object.freeze({ ...contribution?.operations }) as ProgramOperationReferences<S>;
      batch(() => {
        const previous = actions.peek();
        const names = Object.keys(nextActions);
        if (names.length !== Object.keys(previous).length
          || names.some(name => previous[name] !== nextActions[name])) actions.set(nextActions);
        const previousOperations = operations.peek();
        const operationNames = Object.keys(nextOperations);
        if (operationNames.length !== Object.keys(previousOperations).length
          || operationNames.some(name => previousOperations[name] !== nextOperations[name])) operations.set(nextOperations);
        actionManifest.set(Object.freeze(Object.values(frame.offers).map(offer => toolActionBindingSchema.parse({
          toolName: offer.name, action: offer.action.definition.name, bindingId: offer.action.bindingId, scope: offer.action.scope,
          modulePath: offer.modulePath ?? [], enabled: offer.enabled,
          surfaceNames: Object.entries(nextActions).filter(([, action]) => action === offer.action).map(([name]) => name),
        }))));
        for (const [name, state] of members) {
          const keys = [...(mounted.get(name) ?? [])].sort();
          const previous = state.peek();
          if (keys.length !== previous.length || keys.some((key, index) => key !== previous[index])) state.set(Object.freeze(keys));
        }
        for (const name of Object.keys(currentSources)) slotFor(name);
        // 冷来源的 producer 可以同步重入 SDK；启动查询之前，入口必须已经反映当前帧。
        for (const [name, slot] of slots) {
          if (name === actionManifestSlot) continue;
          if (isSurfaceCollectionKeysSlot(name)) {
            const absent = instanceMissing(name);
            slot.replace(absent ? undefined : memberState(name).changes$, absent);
            continue;
          }
          slot.replace(currentSources[name], instanceMissing(name));
        }
      });
    },
    fail(error: unknown) {
      scope.assertActive();
      if (failure) return;
      failure = { error };
      const cleanupErrors: unknown[] = [];
      batch(() => {
        actions.set(Object.freeze({}));
        operations.set(Object.freeze({}));
        actionManifest.set(Object.freeze([]));
        for (const slot of slots.values()) {
          try { slot.fail(error); } catch (cleanupError) { cleanupErrors.push(cleanupError); }
        }
      });
      if (cleanupErrors.length) throw new AggregateError(cleanupErrors, 'REACTIVE_PROGRAM_OUTPUTS_CLEANUP_FAILED');
    },
  });
}
