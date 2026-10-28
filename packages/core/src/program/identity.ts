/** 发布者声明的跨进程 Program 合同身份；不推断 schema 或 codec 等价，不授予会话权限。 */
export interface ProgramContractIdentity {
  readonly name: string;
  readonly version: number;
}

export function programIdentity(definition: ProgramContractIdentity): ProgramContractIdentity {
  return Object.freeze({ name: definition.name, version: definition.version });
}

export class ProgramContractMismatchError extends Error {
  override readonly name = 'ProgramContractMismatchError';
  readonly expected: ProgramContractIdentity;
  readonly actual: ProgramContractIdentity;
  constructor(expected: ProgramContractIdentity, actual: ProgramContractIdentity) {
    super(`Program contract mismatch: expected ${expected.name}@${expected.version}, received ${actual.name}@${actual.version}`);
    this.expected = programIdentity(expected);
    this.actual = programIdentity(actual);
  }
}

/** 跨进程消费需要发布者明确命名；局部 Program 不因此取得共享合同身份。 */
export class SharedProgramRequiredError extends Error {
  override readonly name = 'SharedProgramRequiredError';
  constructor() { super('REACTIVE_SHARED_PROGRAM_REQUIRED'); }
}

export function assertProgramIdentity(expected: ProgramContractIdentity, actual: ProgramContractIdentity): void {
  if (expected.name !== actual.name || expected.version !== actual.version) {
    throw new ProgramContractMismatchError(expected, actual);
  }
}
