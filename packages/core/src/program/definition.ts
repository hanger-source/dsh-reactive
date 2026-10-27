import type { z } from 'zod';
import type { Conversation, WorkspaceContext } from '../conversations/conversation.ts';
import { assertSurfaceBindings, type Surface } from '../surfaces/contract.ts';

/** 组件得到对话事实与显式输入，不持有原生 Agent 或声明注册器。 */
export type ProgramSession<P extends ProgramContract> = Pick<Conversation<ProgramParsedInput<P>, ProgramInput<P>>,
  'identity' | 'inputs$' | 'activity$' | 'goal$' | 'waits$' | 'events$' | 'progress$' | 'progressFrames$' | 'request'
  | 'questions$' | 'foregroundQuestions$' | 'submit'>;

export type ProgramWorkspace = Pick<WorkspaceContext, 'id' | 'directory' | 'share'>;
export interface ProgramProps<P extends ProgramContract> {
  readonly conversation: ProgramSession<P>;
  readonly workspace: ProgramWorkspace;
}

/** 可跨宿主与消费者共享的纯程序合同，不携带组件或原生执行实现。 */
export interface ProgramContract {
  readonly input: z.ZodType;
  readonly surface: Surface;
}

export interface ProgramDefinition<I extends z.ZodType = z.ZodType, S extends Surface = Surface> extends ProgramContract {
  readonly input: I;
  readonly surface: S;
}

export type ProgramInput<P extends ProgramContract> = z.input<P['input']>;
export type ProgramParsedInput<P extends ProgramContract> = z.output<P['input']>;
export type ProgramSurface<P extends ProgramContract> = P['surface'];

const definitions = new WeakSet<object>();

/** 输入和公开合同是程序定义；组件求值只产生声明，不启动业务调用。 */
export function defineProgram<I extends z.ZodType, const S extends Surface>(
  definition: ProgramDefinition<I, S>,
): ProgramDefinition<I, S> {
  assertSurfaceBindings(definition.surface, {});
  const program = Object.freeze({ input: definition.input, surface: definition.surface });
  definitions.add(program);
  return program;
}

/** 消费者与宿主只接受 defineProgram 创建的真实纯合同。 */
export function assertProgramDefinition(value: unknown): asserts value is ProgramContract {
  if (typeof value !== 'object' || value === null || !definitions.has(value)) {
    throw new Error('REACTIVE_PROGRAM_DEFINITION_INVALID');
  }
}

/** 实现只公开原合同；宿主组件由私有身份持有。 */
export interface ProgramImplementation<P extends ProgramContract = ProgramContract> {
  readonly definition: P;
}
