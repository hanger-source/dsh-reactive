import type { z } from 'zod';
import type { Conversation } from '../conversations/conversation.ts';
import type { Workspace } from '../workspaces/context.ts';
import { assertSurfaceBindings, type Surface } from '../surfaces/contract.ts';
import { assertProgramIdentity, programIdentity, SharedProgramRequiredError, type ProgramContractIdentity } from './identity.ts';

/** 静态依赖的纯身份形状；具体组件类型只属于 /program 编程入口。 */
export interface AnyModuleImplementation {
  readonly surface: Surface;
  readonly Component: unknown;
}
export type ModuleImplementations = Readonly<Record<string, AnyModuleImplementation>>;

/** 组件得到对话事实与显式输入，不持有原生 Agent 或声明注册器。 */
export type ProgramSession<P extends ProgramContract> = Pick<Conversation<ProgramParsedInput<P>, ProgramInput<P>>,
  'identity' | 'inputs$' | 'activity$' | 'goal$' | 'waits$' | 'events$' | 'progress' | 'progress$' | 'progressFrames$' | 'request'
  | 'human' | 'questions$' | 'foregroundQuestions$' | 'files' | 'journal' | 'submit'>;

export type ProgramWorkspace = Workspace;
export interface ProgramProps<P extends ProgramContract, Modules extends ModuleImplementations = {}> {
  readonly conversation: ProgramSession<P>;
  readonly workspace: ProgramWorkspace;
  readonly modules: Modules;
}

/** 可跨宿主与消费者共享的纯程序合同，不携带组件或原生执行实现。 */
export interface ProgramContract {
  readonly input: z.ZodType;
  readonly surface: Surface;
}

/** 跨宿主与远程消费者共享的发布者合同；局部 Program 仍可保持无名。 */
export interface SharedProgramContract extends ProgramContract, ProgramContractIdentity {}
/** 本地实际定义保持对象身份核验；显式引用只用于核验发布者声明的共享合同。 */
export type ProgramExpectation = ProgramContract | {
  readonly contract: ProgramContractIdentity;
  readonly input?: never;
  readonly surface?: never;
};

export interface ProgramDefinition<I extends z.ZodType = z.ZodType, S extends Surface = Surface> extends ProgramContract {
  readonly input: I;
  readonly surface: S;
}
export interface SharedProgramDefinition<I extends z.ZodType = z.ZodType, S extends Surface = Surface>
  extends ProgramDefinition<I, S>, ProgramContractIdentity {}

export type ProgramInput<P extends ProgramContract> = z.input<P['input']>;
export type ProgramParsedInput<P extends ProgramContract> = z.output<P['input']>;
export type ProgramSurface<P extends ProgramContract> = P['surface'];

const definitions = new WeakSet<object>();

/** 输入和公开合同是程序定义；组件求值只产生声明，不启动业务调用。 */
export function defineProgram<I extends z.ZodType, const S extends Surface>(
  definition: ProgramDefinition<I, S> & { readonly name: string; readonly version?: number },
): SharedProgramDefinition<I, S>;
export function defineProgram<I extends z.ZodType, const S extends Surface>(
  definition: ProgramDefinition<I, S>,
): ProgramDefinition<I, S>;
export function defineProgram<I extends z.ZodType, const S extends Surface>(
  definition: ProgramDefinition<I, S> & { readonly name?: string; readonly version?: number },
): ProgramDefinition<I, S> {
  assertSurfaceBindings(definition.surface, {});
  let identity: ProgramContractIdentity | undefined;
  if ('name' in definition || 'version' in definition) {
    if (typeof definition.name !== 'string' || !/^[a-z][a-z0-9_]{0,79}$/u.test(definition.name)) {
      throw new Error('REACTIVE_PROGRAM_NAME_INVALID');
    }
    const version = definition.version === undefined ? 1 : definition.version;
    if (!Number.isSafeInteger(version) || version < 1) throw new Error('REACTIVE_PROGRAM_VERSION_INVALID');
    identity = programIdentity({ name: definition.name, version });
  }
  const program = Object.freeze({ ...identity, input: definition.input, surface: definition.surface });
  definitions.add(program);
  return program;
}

/** 消费者与宿主只接受 defineProgram 创建的真实纯合同。 */
export function assertProgramDefinition(value: unknown): asserts value is ProgramContract {
  if (typeof value !== 'object' || value === null || !definitions.has(value)) {
    throw new Error('REACTIVE_PROGRAM_DEFINITION_INVALID');
  }
}

/** 只接受 defineProgram 显式签发的具名定义，不从同形字段或 schema 猜测。 */
export function assertSharedProgramDefinition<P extends ProgramContract>(value: P): asserts value is P & ProgramContractIdentity;
export function assertSharedProgramDefinition(value: unknown): asserts value is SharedProgramContract;
export function assertSharedProgramDefinition(value: unknown): asserts value is SharedProgramContract {
  assertProgramDefinition(value);
  if (!Object.hasOwn(value, 'name') || !Object.hasOwn(value, 'version')) throw new SharedProgramRequiredError();
}

/** @internal 在异步取得目标之前捕获期望；本地定义不复制，显式共享引用取自有快照。 */
export function captureProgramExpectation(expected?: ProgramExpectation): ProgramExpectation | undefined {
  if (expected === undefined) return undefined;
  if ('contract' in expected) {
    if ('input' in expected || 'surface' in expected) throw new Error('REACTIVE_PROGRAM_EXPECTATION_INVALID');
    return Object.freeze({ contract: programIdentity(expected.contract) });
  }
  assertProgramDefinition(expected);
  return expected;
}

/** @internal 宿主在取得或产生会话副作用之前核对真实目标，不能把共享引用当本地定义。 */
export function assertExpectedProgram(actual: ProgramContract, expected?: ProgramExpectation): void {
  if (expected === undefined) return;
  if ('contract' in expected) {
    if ('input' in expected || 'surface' in expected) throw new Error('REACTIVE_PROGRAM_EXPECTATION_INVALID');
    assertSharedProgramDefinition(actual);
    assertProgramIdentity(expected.contract, actual);
  } else {
    assertProgramDefinition(expected);
    if (actual !== expected) throw new Error('REACTIVE_PROGRAM_CONTRACT_MISMATCH');
  }
}

/** 实现只公开原合同；宿主组件由私有身份持有。 */
export interface ProgramImplementation<P extends ProgramContract = ProgramContract> {
  readonly definition: P;
}
