import { programIdentity, type ProgramContractIdentity } from '../program/identity.ts';

/** 持久会话所属Program与当前实现不同；不重新绑定旧会话或推断合同兼容。 */
export class SessionProgramMismatchError extends Error {
  override readonly name = 'SessionProgramMismatchError';
  readonly recorded: ProgramContractIdentity | null;
  readonly current: ProgramContractIdentity | null;
  constructor(readonly sessionId: string, recorded: ProgramContractIdentity | null, current: ProgramContractIdentity | null) {
    super('REACTIVE_SESSION_PROGRAM_MISMATCH');
    this.recorded = recorded === null ? null : programIdentity(recorded);
    this.current = current === null ? null : programIdentity(current);
  }
}
