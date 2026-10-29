import type { ActionReference } from '../actions/contract.ts';
import type { OperationBindingReference } from '../operations/contract.ts';
import type { ContextDeclaration, OfferDeclaration, FollowDeclaration, NativeToolDeclaration, ProgramDeclaration, SkillDeclaration, HumanInputDeclaration } from './declarations.ts';
import type { Frame } from './renderer.ts';
import type { Surface, SurfaceBindingReferences } from '../surfaces/contract.ts';
import { moduleRootSurface, type SurfaceMount } from '../surfaces/composition.ts';
import type { Observation } from '../resources/observation.ts';
import type { Json } from '../conversations/protocol.ts';
import { isObservable } from 'rxjs';

export interface SurfaceContribution extends SurfaceBindingReferences {
  readonly surface: Surface;
}
export interface ProgramFrame {
  readonly revision: number;
  readonly contexts: Readonly<Record<string, ContextDeclaration>>;
  readonly offers: Readonly<Record<string, OfferDeclaration>>;
  readonly follows: Readonly<Record<string, FollowDeclaration>>;
  readonly nativeTools: Readonly<Record<string, NativeToolDeclaration>>;
  readonly skills: readonly SkillDeclaration[];
  readonly surfaces: readonly SurfaceContribution[];
  readonly mounts?: readonly SurfaceMount[];
  readonly humanInput?: HumanInputDeclaration;
}

type MutableContribution = {
  surface: Surface;
  views: Record<string, NonNullable<SurfaceBindingReferences['views']>[string]>;
  actions: Record<string, ActionReference>;
  operations: Record<string, OperationBindingReference>;
};
function entries<T>(): Record<string, T> { return Object.create(null) as Record<string, T>; }
function put<T>(target: Record<string, T>, key: string, value: T, code: string) {
  if (Object.hasOwn(target, key)) throw new Error(code + ': ' + key);
  target[key] = value;
}

/** 合并同一次React commit的声明及会话级依赖；此处不注册原生能力，也不执行资源或业务。 */
export function collectProgramFrame(frame: Frame<ProgramDeclaration>): ProgramFrame {
  const contexts = entries<ContextDeclaration>();
  const offers = entries<OfferDeclaration>();
  const follows = entries<FollowDeclaration>();
  const nativeTools = entries<NativeToolDeclaration>();
  const toolNames = new Set<string>();
  const skills: SkillDeclaration[] = [];
  const skillSources = new Map<string, Set<SkillDeclaration['eligibility']>>();
  const surfaces = new Map<Surface, MutableContribution>();
  const mounts: SurfaceMount[] = [];
  const mounted = new Map<Surface, Set<string>>();
  let humanInput: HumanInputDeclaration | undefined;
  for (const declaration of frame.declarations) {
    switch (declaration.kind) {
      case 'module': {
        const root = moduleRootSurface(declaration.mount);
        const path = JSON.stringify(declaration.mount.path);
        let paths = mounted.get(root);
        if (!paths) { paths = new Set(); mounted.set(root, paths); }
        if (paths.has(path)) throw new Error('REACTIVE_MODULE_MOUNT_DUPLICATE: ' + path);
        paths.add(path); mounts.push(declaration.mount);
        break;
      }
      case 'human-input':
        if (humanInput) {
          if (humanInput.admitResponder !== declaration.admitResponder)
            throw new Error('REACTIVE_HUMAN_INPUT_CONTRIBUTION_CONFLICT');
          break;
        }
        humanInput = declaration;
        break;
      case 'context':
        put(contexts, declaration.name, declaration, 'REACTIVE_CONTEXT_DUPLICATE');
        break;
      case 'offer':
        if (toolNames.has(declaration.name)) throw new Error('REACTIVE_OFFER_DUPLICATE: ' + declaration.name);
        toolNames.add(declaration.name);
        put(offers, declaration.name, declaration, 'REACTIVE_OFFER_DUPLICATE');
        break;
      case 'follow':
        if (toolNames.has(declaration.name)) throw new Error('REACTIVE_OFFER_DUPLICATE: ' + declaration.name);
        toolNames.add(declaration.name);
        put(follows, declaration.name, declaration, 'REACTIVE_FOLLOW_DUPLICATE');
        break;
      case 'native-tool': {
        const previous = nativeTools[declaration.name];
        if (previous) {
          if (previous.enabled !== declaration.enabled || previous.admit !== declaration.admit || previous.eligibility !== declaration.eligibility)
            throw new Error('REACTIVE_NATIVE_TOOL_CONTRIBUTION_CONFLICT: ' + declaration.name);
          break;
        }
        if (toolNames.has(declaration.name)) throw new Error('REACTIVE_OFFER_DUPLICATE: ' + declaration.name);
        toolNames.add(declaration.name);
        put(nativeTools, declaration.name, declaration, 'REACTIVE_NATIVE_TOOL_DUPLICATE');
        break;
      }
      case 'skill': {
        const key = declaration.name === undefined ? 'directory:' + declaration.directory : 'name:' + declaration.name;
        let sources = skillSources.get(key);
        if (!sources) { sources = new Set(); skillSources.set(key, sources); }
        if (sources.has(declaration.eligibility)) break;
        sources.add(declaration.eligibility);
        skills.push(declaration);
        break;
      }
      case 'expose': {
        let contribution = surfaces.get(declaration.surface);
        if (!contribution) {
          contribution = { surface: declaration.surface, views: entries(), actions: entries(), operations: entries() };
          surfaces.set(declaration.surface, contribution);
        }
        for (const [name, source$] of Object.entries(declaration.value.views ?? {}))
          put(contribution.views, name, source$, 'REACTIVE_SURFACE_VIEW_DUPLICATE');
        for (const [name, action] of Object.entries(declaration.value.actions ?? {}))
          put(contribution.actions, name, action, 'REACTIVE_SURFACE_ACTION_DUPLICATE');
        for (const [name, operation] of Object.entries(declaration.value.operations ?? {}))
          put(contribution.operations, name, operation, 'REACTIVE_SURFACE_OPERATION_DUPLICATE');
        break;
      }
    }
  }
  return Object.freeze({ revision: frame.revision, contexts: Object.freeze(contexts), offers: Object.freeze(offers), follows: Object.freeze(follows),
    nativeTools: Object.freeze(nativeTools), skills: Object.freeze(skills), mounts: Object.freeze(mounts), ...humanInput ? { humanInput } : {}, surfaces: Object.freeze([...surfaces.values()].map(contribution =>
      Object.freeze({ surface: contribution.surface, views: Object.freeze(contribution.views),
        actions: Object.freeze(contribution.actions), operations: Object.freeze(contribution.operations) }))) });
}

/** 相同有效声明保留原提交；观察自己的进展不会凭空制造一次装配修订。 */
export function sameProgramFrame(left: ProgramFrame, right: ProgramFrame): boolean {
  const record = <T>(a: Readonly<Record<string, T>> | undefined, b: Readonly<Record<string, T>> | undefined,
    equal: (a: T, b: T) => boolean = Object.is): boolean => {
    const keys = Object.keys(a ?? {});
    return keys.length === Object.keys(b ?? {}).length && keys.every(key => Object.hasOwn(b ?? {}, key) && equal(a![key]!, b![key]!));
  };
  const set = <T>(a: readonly T[], b: readonly T[], equal: (a: T, b: T) => boolean = Object.is): boolean =>
    a.length === b.length && a.every(value => b.some(other => equal(value, other)));
  const observation = <T>(a: Observation<T> | undefined, b: Observation<T> | undefined, equal: (a: T, b: T) => boolean): boolean => {
    if (!a || !b) return a === b;
    if (a.status !== b.status) return false;
    if (a.status === 'pending') return true;
    if (a.status === 'error') return b.status === 'error' && a.error === b.error;
    return b.status === 'ready' && equal(a.value, b.value);
  };
  const text = (value: Json) => typeof value === 'string' ? value : JSON.stringify(value);
  const context = (a: ContextDeclaration, b: ContextDeclaration): boolean => {
    const av = a.observation, bv = b.observation;
    // 静态值和同一ready文本向原生贡献相同内容；观察时间和代次不是prompt输入。
    if ((av === undefined || av.status === 'ready') && (bv === undefined || bv.status === 'ready'))
      return text(av === undefined ? a.value! : av.value) === text(bv === undefined ? b.value! : bv.value);
    return observation(av, bv, (x, y) => text(x) === text(y));
  };
  return record(left.contexts, right.contexts, context)
    && record(left.offers, right.offers, (a, b) => a.action === b.action && a.enabled === b.enabled
      && JSON.stringify(a.modulePath ?? []) === JSON.stringify(b.modulePath ?? [])
      && observation(a.eligibility, b.eligibility, (x, y) => x.allowed === y.allowed && (x.allowed || (!y.allowed && x.reason === y.reason)))
      && (a.follow === undefined || b.follow === undefined ? a.follow === b.follow
        : a.follow.when === b.follow.when && a.follow.mode === b.follow.mode && frameJson(a.follow.scope) === frameJson(b.follow.scope)))
    && record(left.follows, right.follows, (a, b) => a.operation === b.operation && a.description === b.description
      && a.when === b.when && a.mode === b.mode && a.enabled === b.enabled && a.eligibility === b.eligibility
      && frameJson(a.scope) === frameJson(b.scope))
    && record(left.nativeTools, right.nativeTools, (a, b) => a.enabled === b.enabled && a.admit === b.admit && a.eligibility === b.eligibility)
    && set(left.skills, right.skills, (a, b) => a.name === b.name && a.directory === b.directory && a.eligibility === b.eligibility)
    && set(left.surfaces, right.surfaces, (a, b) => a.surface === b.surface
      && record(a.actions, b.actions) && record(a.operations, b.operations)
      && record(a.views, b.views, (x, y) => (isObservable(x) ? x : x.observations$) === (isObservable(y) ? y : y.observations$)))
    // Module以useMemo保留逻辑地址；动态撤后重挂取得不同effective mount，不能合并代际。
    && set(left.mounts ?? [], right.mounts ?? [])
    && left.humanInput?.admitResponder === right.humanInput?.admitResponder;
}

function frameJson(value: Json): string {
  return JSON.stringify(value, (_key, item: unknown) => item !== null && typeof item === 'object' && !Array.isArray(item)
    ? Object.fromEntries(Object.keys(item).sort().map(key => [key, (item as Record<string, unknown>)[key]])) : item);
}
