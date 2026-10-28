import type { ActionContract, Invocation, Json, OperationContract, OperationHandle } from '@hanger-source/dsh-reactive';

/** 本进程真实受理记录的只读元数据；调用对象始终是原对象。 */
export interface RetainedActionInvocation {
  readonly name: string;
  readonly definition: ActionContract;
  readonly invocation: Invocation<unknown, Json> & { readonly operation?: Promise<OperationHandle<OperationContract>> };
}

export class InvocationUnavailableError extends Error {
  constructor() { super('REACTIVE_INVOCATION_UNAVAILABLE'); this.name = 'InvocationUnavailableError'; }
}
