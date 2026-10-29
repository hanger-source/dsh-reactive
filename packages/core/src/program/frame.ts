import type { ActionReference } from '../actions/contract.ts';
import type { OperationBindingReference } from '../operations/contract.ts';
import type { ContextDeclaration, OfferDeclaration, FollowDeclaration, NativeToolDeclaration, ProgramDeclaration, SkillDeclaration, HumanInputDeclaration } from './declarations.ts';
import type { Frame } from './renderer.ts';
import type { Surface, SurfaceBindingReferences } from '../surfaces/contract.ts';
import { moduleRootSurface, type SurfaceMount } from '../surfaces/composition.ts';

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
  const skillKeys = new Set<string>();
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
          if (previous.enabled !== declaration.enabled || previous.admit !== declaration.admit)
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
        if (skillKeys.has(key)) break;
        skillKeys.add(key);
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
