import { createElement, Fragment, useContext, type ReactElement } from 'react';
import { z } from 'zod';
import { isBoundAction, type ActionContract, type ActionReference, type BoundAction } from '../actions/contract.ts';
import { isOperationBinding, type OperationContract, type OperationBindingReference, type BoundOperation, type OperationFollowMode } from '../operations/contract.ts';
import type { Json } from '../conversations/protocol.ts';
import type { HumanInputPolicy } from '../participation/contract.ts';
import type { NativeToolAdmission } from '../tools/admission.ts';
import type { Observation } from '../resources/observation.ts';
import type { ResourceSource } from '../resources/source.ts';
import { declaration } from './renderer.ts';
import { useResource } from './hooks.ts';
import { assertSurfaceBindings, type ExactSurfaceBindings, type PartialSurfaceBindings, type Surface, type SurfaceBindingReferences } from '../surfaces/contract.ts';
import { isModuleDeclaration, moduleMountContext, type ModuleDeclaration } from './module.ts';
import { moduleContribution, moduleName } from '../surfaces/composition.ts';

export type ContextProps<T = unknown> = { readonly name: string; readonly required?: boolean } & (
  | { readonly value: Json; readonly observation?: never; readonly source?: never; readonly render?: never }
  | { readonly value?: never; readonly observation: Observation<T>; readonly source?: never; readonly render: (value: T) => Json }
  | { readonly value?: never; readonly observation?: never; readonly source: ResourceSource<T>; readonly render: (value: T) => Json }
);
export type ContextDeclaration =
  | { readonly kind: 'context'; readonly name: string; readonly value: Json; readonly observation?: never }
  | { readonly kind: 'context'; readonly name: string; readonly value?: never; readonly observation: Observation<Json> };
export interface OfferProps<D extends ActionContract> {
  /** 省略时使用 Action 定义名；Module 再按确切 mount 添加命名作用域。 */
  readonly name?: string;
  readonly action: BoundAction<D>;
  readonly enabled?: boolean;
  readonly follow?: { readonly when: 'settled' | 'feedback'; readonly scope: Json; readonly mode?: OperationFollowMode };
}
/** 仅在异构声明帧的内部边界擦除 schema，公开 Offer props 仍保留精确合同。 */
export interface OfferDeclaration {
  readonly kind: 'offer'; readonly name: string; readonly action: ActionReference; readonly enabled: boolean;
  readonly modulePath?: readonly string[];
  readonly follow?: { readonly when: 'settled' | 'feedback'; readonly scope: Json; readonly mode: OperationFollowMode };
}
export interface FollowProps<D extends OperationContract> {
  readonly name: string;
  readonly description: string;
  readonly operation: BoundOperation<D>;
  readonly when: [NonNullable<NoInfer<D>['feedback']>] extends [never] ? 'settled' : 'settled' | 'feedback';
  readonly scope: Json;
  readonly enabled?: boolean;
  readonly mode?: OperationFollowMode;
}
/** 声明一个沿原操作登记新等待的能力；求值本身不创建参与或执行命令。 */
export interface FollowDeclaration {
  readonly kind: 'follow'; readonly name: string; readonly description: string;
  readonly operation: OperationBindingReference; readonly when: 'settled' | 'feedback';
  readonly scope: Json; readonly enabled: boolean; readonly mode: OperationFollowMode;
}
export interface ExposeProps<S extends Surface, B extends PartialSurfaceBindings<S> = PartialSurfaceBindings<S>> {
  readonly surface: S;
  readonly value: B & ExactSurfaceBindings<NoInfer<S>, B>;
}
export interface ExposeDeclaration {
  readonly kind: 'expose'; readonly surface: Surface; readonly value: SurfaceBindingReferences;
}
export type SkillProps =
  | { readonly name: string; readonly directory?: never }
  | { readonly name?: never; readonly directory: string };
/** name 选择原生已声明方法；directory 由原生 scoped filesystem provider 提供。 */
export type SkillDeclaration = { readonly kind: 'skill' } & SkillProps;
/** 一个 Program 只声明一项响应参与策略；原生问题属于该根会话。 */
export interface HumanInputProps extends HumanInputPolicy {}
export interface HumanInputDeclaration extends HumanInputPolicy { readonly kind: 'human-input'; }
export interface NativeToolsProps {
  readonly names: readonly string[];
  readonly enabled?: boolean;
  readonly admit?: NativeToolAdmission;
}
export interface NativeToolDeclaration {
  readonly kind: 'native-tool'; readonly name: string; readonly enabled: boolean; readonly admit?: NativeToolAdmission;
}
export type ProgramDeclaration = ContextDeclaration | OfferDeclaration | FollowDeclaration | ExposeDeclaration | SkillDeclaration | HumanInputDeclaration | ModuleDeclaration | NativeToolDeclaration;

// 私有 identity 品牌拒绝手工拼出的 host leaf；声明引用的业务源保持由应用所有。
const declarations = new WeakSet<object>();
const json = z.json();
function leaf(value: ProgramDeclaration): ReactElement {
  const owned = Object.freeze(value);
  declarations.add(owned);
  // 声明工厂仍可在条件表达式中直接调用；只有此内部组件读取 React 命名作用域。
  return createElement(ScopedDeclaration, { value: owned });
}
function ScopedDeclaration({ value }: { readonly value: ProgramDeclaration }): ReactElement {
  const mount = useContext(moduleMountContext);
  if (!mount) return declaration(value);
  let scoped: ProgramDeclaration;
  if (value.kind === 'context') scoped = { ...value, name: moduleName(mount, value.name, false) };
  else if (value.kind === 'offer') scoped = { ...value, name: moduleName(mount, value.name, true), modulePath: mount.path };
  else if (value.kind === 'follow') scoped = { ...value, name: moduleName(mount, value.name, true) };
  else if (value.kind === 'expose') scoped = { kind: 'expose', ...moduleContribution(mount, value.surface, value.value) };
  else scoped = value;
  const owned = Object.freeze(scoped);
  declarations.add(owned);
  return declaration(owned);
}
function name(value: string, code: string) {
  if (typeof value !== 'string' || !value.trim()) throw new Error(code);
}

/** 必需现场保留 pending/error 屏障；显式非必需现场只在 ready 时贡献，不改写资源本身。 */
export function Context<T>(props: ContextProps<T>): ReactElement | null {
  name(props.name, 'REACTIVE_CONTEXT_NAME_REQUIRED');
  if (props.required !== undefined && typeof props.required !== 'boolean') throw new Error('REACTIVE_CONTEXT_REQUIRED_INVALID');
  if (props.source !== undefined) return createElement(ResourceContext<T>, {
    name: props.name, required: props.required, source: props.source, render: props.render,
  });
  if (props.observation === undefined) return leaf({ kind: 'context', name: props.name, value: json.parse(props.value) });
  const observed = props.observation;
  if (!Number.isSafeInteger(observed.generation) || observed.generation < 0) throw new Error('REACTIVE_CONTEXT_GENERATION_INVALID');
  let observation: Observation<Json>;
  if (observed.status === 'pending') {
    if (props.required === false) return null;
    observation = { status: 'pending', generation: observed.generation };
  }
  else if (observed.status === 'error') {
    if (props.required === false) return null;
    observation = { status: 'error', generation: observed.generation, error: observed.error };
  }
  else if (observed.status === 'ready') {
    if (!Number.isFinite(observed.observedAt)) throw new Error('REACTIVE_CONTEXT_OBSERVED_AT_INVALID');
    observation = { status: 'ready', generation: observed.generation, observedAt: observed.observedAt,
      value: json.parse(props.render(observed.value)) };
  } else throw new Error('REACTIVE_CONTEXT_OBSERVATION_INVALID');
  return leaf({ kind: 'context', name: props.name, observation: Object.freeze(observation) });
}

/** 来源观察属于真实组件；工厂函数的不同声明分支不改变调用者的hook次序。 */
function ResourceContext<T>(props: { readonly name: string; readonly required?: boolean;
  readonly source: ResourceSource<T>; readonly render: (value: T) => Json }): ReactElement | null {
  const observed = useResource(props.source);
  return Context({ name: props.name, required: props.required, observation: observed.state, render: props.render });
}

/** 描述已有绑定的可用性，不注册工具、不受理调用。 */
export function Offer<D extends ActionContract>(props: OfferProps<D>): ReactElement {
  if (!isBoundAction(props.action)) throw new Error('REACTIVE_OFFER_ACTION_REQUIRED');
  const localName = props.name ?? props.action.definition.name;
  if (!/^[a-z][a-z0-9_]{0,79}$/u.test(localName)) throw new Error('REACTIVE_OFFER_NAME_INVALID');
  if (props.enabled !== undefined && typeof props.enabled !== 'boolean') throw new Error('REACTIVE_OFFER_ENABLED_INVALID');
  if (props.follow !== undefined && !props.action.definition.operation) throw new Error('REACTIVE_OFFER_OPERATION_REQUIRED');
  const follow = props.follow === undefined ? undefined : Object.freeze(z.object({ when: z.enum(['settled', 'feedback']), scope: json,
    mode: z.enum(['wait', 'background']).default('wait') }).strict().parse(props.follow));
  return leaf({ kind: 'offer', name: localName, action: props.action, enabled: props.enabled ?? true,
    ...follow ? { follow } : {} });
}

/** 工具调用才登记新参与；after 由调用输入明确指出 owner 已签发的反馈节点。 */
export function Follow<D extends OperationContract>(props: FollowProps<D>): ReactElement {
  if (!/^[a-z][a-z0-9_]{0,79}$/u.test(props.name)) throw new Error('REACTIVE_FOLLOW_NAME_INVALID');
  name(props.description, 'REACTIVE_FOLLOW_DESCRIPTION_REQUIRED');
  if (!isOperationBinding(props.operation)) throw new Error('REACTIVE_FOLLOW_OPERATION_REQUIRED');
  if (props.when !== 'settled' && props.when !== 'feedback') throw new Error('REACTIVE_FOLLOW_WHEN_INVALID');
  if (props.when === 'feedback' && props.operation.definition.feedback === undefined) throw new Error('REACTIVE_OPERATION_FEEDBACK_UNSUPPORTED');
  if (props.enabled !== undefined && typeof props.enabled !== 'boolean') throw new Error('REACTIVE_FOLLOW_ENABLED_INVALID');
  if (props.mode !== undefined && props.mode !== 'wait' && props.mode !== 'background') throw new Error('REACTIVE_FOLLOW_MODE_INVALID');
  return leaf({ kind: 'follow', name: props.name, description: props.description, operation: props.operation,
    when: props.when, scope: json.parse(props.scope), enabled: props.enabled ?? true, mode: props.mode ?? 'wait' });
}

/** Surface 是公开合同；多个模块可贡献不同槽，完整帧的槽冲突由 collector 核验。 */
export function Expose<S extends Surface, const B extends PartialSurfaceBindings<NoInfer<S>>>(props: ExposeProps<S, B>): ReactElement {
  const contribution: unknown = props.value;
  assertSurfaceBindings(props.surface, contribution);
  const value = Object.freeze({
    ...contribution.views === undefined ? {} : { views: Object.freeze({ ...contribution.views }) },
    ...contribution.actions === undefined ? {} : { actions: Object.freeze({ ...contribution.actions }) },
    ...contribution.operations === undefined ? {} : { operations: Object.freeze({ ...contribution.operations }) },
  });
  return leaf({ kind: 'expose', surface: props.surface, value });
}

/** 目录存在性和 scope/cwd 下的方法解析由原生装配核验，此处不读文件或创建 provider。 */
export function Skill(props: SkillProps): ReactElement {
  if (props.name !== undefined && props.directory === undefined) {
    name(props.name, 'REACTIVE_SKILL_NAME_REQUIRED');
    return leaf({ kind: 'skill', name: props.name });
  }
  if (props.directory !== undefined && props.name === undefined) {
    name(props.directory, 'REACTIVE_SKILL_DIRECTORY_REQUIRED');
    return leaf({ kind: 'skill', directory: props.directory });
  }
  throw new Error('REACTIVE_SKILL_SOURCE_INVALID');
}

/** 只声明参与权限；连接前台 answerer、claim 和原生晚答由宿主拥有。 */
export function HumanInput(props: HumanInputProps): ReactElement {
  if (typeof props.admitResponder !== 'function') throw new Error('REACTIVE_HUMAN_RESPONDER_ADMISSION_REQUIRED');
  return leaf({ kind: 'human-input', admitResponder: props.admitResponder });
}

/** 组合宿主已经安装的原生能力；空集合不贡献声明，执行与准入仍归原生管线。 */
export function NativeTools(props: NativeToolsProps): ReactElement {
  if (!Array.isArray(props.names)) throw new Error('REACTIVE_NATIVE_TOOLS_REQUIRED');
  if (props.enabled !== undefined && typeof props.enabled !== 'boolean') throw new Error('REACTIVE_NATIVE_TOOL_ENABLED_INVALID');
  if (props.admit !== undefined && typeof props.admit !== 'function') throw new Error('REACTIVE_NATIVE_TOOL_ADMISSION_INVALID');
  const names = new Set<string>();
  const children = props.names.map(tool => {
    if (typeof tool !== 'string' || !/^[A-Za-z][A-Za-z0-9_-]{0,79}$/u.test(tool) || tool === 'run_code') throw new Error('REACTIVE_NATIVE_TOOL_NAME_INVALID');
    if (names.has(tool)) throw new Error('REACTIVE_NATIVE_TOOL_DUPLICATE: ' + tool);
    names.add(tool);
    return createElement(Fragment, { key: tool }, leaf({ kind: 'native-tool', name: tool,
      enabled: props.enabled ?? true, ...props.admit ? { admit: props.admit } : {} }));
  });
  // Fragment 只组合声明，不产生新的原生注册或执行 owner。
  return createElement(Fragment, null, ...children);
}

/** ReactNode 丢失泛型后的正式入口；未经上述构造函数产生的值不能进入程序帧。 */
export function decodeProgramDeclaration(value: unknown): ProgramDeclaration {
  if (typeof value !== 'object' || value === null || (!declarations.has(value) && !isModuleDeclaration(value))) throw new Error('REACTIVE_PROGRAM_DECLARATION_INVALID');
  return value as ProgramDeclaration;
}
