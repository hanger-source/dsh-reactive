import type { ActionSource } from '../actions/contract.ts';
import type { InputReference, Json } from '../conversations/protocol.ts';

/** 已安装原生工具的真实调用；不把原生执行重新包装成业务 Action。 */
export interface NativeToolCall {
  readonly name: string;
  readonly arguments: Json;
  readonly sessionId: string;
  readonly workspaceId: string;
  readonly directory: string;
  readonly source: Extract<ActionSource, { readonly kind: 'tool' }>;
  readonly inputs: readonly InputReference[];
  readonly signal: AbortSignal;
}

/** 业务宿主补充的执行政策；原生工具及其他原生 guard 仍各自核验。 */
export type NativeToolAdmission = (call: NativeToolCall) => void | PromiseLike<void>;
