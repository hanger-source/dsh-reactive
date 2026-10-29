import type { ActionContract, Invocation, Json, OperationContract, OperationHandle, SubmitOptions } from '@hanger-source/dsh-reactive';

/** 宿主调用关联；显式身份只用于核对同一次请求，不授予执行资格。 */
export interface ActionInvocationOptions extends SubmitOptions {
  /** 显式受理身份须为UUID，与原生工具发生身份保持独立；正文使用原始JSON快照核对。 */
  readonly invocationId?: string;
}

/** 本进程真实受理记录的只读元数据；调用对象始终是原对象。 */
export interface RetainedActionInvocation {
  readonly name: string;
  readonly definition: ActionContract;
  readonly invocation: Invocation<unknown, Json> & { readonly operation?: Promise<OperationHandle<OperationContract>> };
}

export class InvocationUnavailableError extends Error {
  constructor() { super('REACTIVE_INVOCATION_UNAVAILABLE'); this.name = 'InvocationUnavailableError'; }
}
