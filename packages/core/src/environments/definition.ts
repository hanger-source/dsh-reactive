import type { ProgramContract, ProgramImplementation, ProgramInput } from '../program/definition.ts';
import type { SurfaceViews } from '../surfaces/contract.ts';

const environmentDefinition = Symbol('environment-definition');
export interface Environment<P extends ProgramContract = ProgramContract> {
  readonly [environmentDefinition]: true;
  readonly id: string;
  readonly instructions: string;
  readonly skills?: readonly string[];
  readonly program: ProgramImplementation<P>;
}
/** 异构宿主边界；具体环境仍在应用与句柄上保留输入、视图及 Action 合同。 */
export type AnyEnvironment = Environment<any>;
export type EnvironmentInput<E extends AnyEnvironment> = ProgramInput<E['program']['definition']>;
export type EnvironmentViews<E extends AnyEnvironment> = SurfaceViews<E['program']['definition']['surface']>;
export type EnvironmentActions<E extends AnyEnvironment> = E['program']['definition']['surface']['actions'];
export type EnvironmentOperations<E extends AnyEnvironment> = E['program']['definition']['surface']['operations'];
export type EnvironmentAt<E extends AnyEnvironment, Id extends string> = E extends AnyEnvironment ? Id extends E['id'] ? E : never : never;
export function defineEnvironment<const Id extends string, P extends ProgramContract>(
  definition: Omit<Environment<P>, typeof environmentDefinition> & { readonly id: Id },
): Readonly<Environment<P> & { readonly id: Id }> {
  return Object.freeze({ ...definition, [environmentDefinition]: true as const });
}
