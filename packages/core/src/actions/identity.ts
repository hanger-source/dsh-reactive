import type { ActionIdentity } from './contract.ts';

/** 发布者声明的合同身份；不推断 schema 或业务函数等价，不授予执行权限。 */
export function actionIdentity(definition: ActionIdentity): ActionIdentity {
  return Object.freeze({ name: definition.name, version: definition.version });
}

/** 命名合同目录同时容纳同名的多个版本。 */
export function actionIdentityKey(identity: ActionIdentity): string {
  return JSON.stringify([identity.name, identity.version]);
}

export class ActionContractMismatchError extends Error {
  readonly expected: ActionIdentity;
  readonly actual: ActionIdentity;
  constructor(expected: ActionIdentity, actual: ActionIdentity) {
    super(`Action contract mismatch: expected ${expected.name}@${expected.version}, received ${actual.name}@${actual.version}`);
    this.name = 'ActionContractMismatchError';
    this.expected = actionIdentity(expected);
    this.actual = actionIdentity(actual);
  }
}

/** 同一宿主受理身份已经绑定另一份请求；不泄漏原请求的参与者或业务参数。 */
export class ActionInvocationConflictError extends Error {
  override readonly name = 'ActionInvocationConflictError';
  constructor(readonly invocationId: string) { super('REACTIVE_ACTION_INVOCATION_CONFLICT'); }
}

/** 同步受理回调重入时，原Invocation尚未构造完成；原受理继续结算。 */
export class ActionAdmissionInProgressError extends Error {
  override readonly name = 'ActionAdmissionInProgressError';
  constructor(readonly invocationId: string) { super('REACTIVE_ACTION_ADMISSION_IN_PROGRESS'); }
}

export function assertActionIdentity(expected: ActionIdentity, actual: ActionIdentity): void {
  if (expected.name !== actual.name || expected.version !== actual.version) throw new ActionContractMismatchError(expected, actual);
}
