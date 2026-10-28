import { z } from 'zod';
import { assertProgramDefinition, assertSharedProgramDefinition, programIdentity, SessionProgramMismatchError,
  type ProgramContract, type ProgramContractIdentity } from '@hanger-source/dsh-reactive/kernel';

/** 持久受理身份；null明确表示本地无名，缺字段不属于此合同。 */
export const sessionProgramBindingSchema = z.object({ name: z.string().min(1), version: z.number().int().positive().safe() }).strict().nullable();

export function captureProgramBinding(definition: ProgramContract): ProgramContractIdentity | null {
  assertProgramDefinition(definition);
  if (!Object.hasOwn(definition, 'name')) return null;
  assertSharedProgramDefinition(definition);
  return programIdentity(definition);
}

export function assertSessionProgram(sessionId: string, recorded: ProgramContractIdentity | null, definition: ProgramContract): void {
  const current = captureProgramBinding(definition);
  if (recorded === null ? current !== null : current === null || recorded.name !== current.name || recorded.version !== current.version) {
    throw new SessionProgramMismatchError(sessionId, recorded, current);
  }
}
