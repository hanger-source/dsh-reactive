import { z } from 'zod';
import type { ActionContract, ActionOutput, ActionScope } from '../actions/contract.ts';
import type { Json } from '../conversations/protocol.ts';

/** 运行中的公开能力目录；字符串名称来自真实绑定，无需客户端反解工具 namespace。 */
export const toolActionBindingSchema = z.object({
  toolName: z.string().min(1), action: z.string().min(1), bindingId: z.string().min(1), scope: z.json(),
  modulePath: z.string().array().readonly(), surfaceNames: z.string().array().readonly(), enabled: z.boolean(),
}).strict();
export type ToolActionBinding = z.output<typeof toolActionBindingSchema>;
export const actionManifestSlot = '$actions';

const actionPresentationSchema = z.object({
  version: z.literal(1), action: z.string().min(1), bindingId: z.string().min(1),
  modulePath: z.string().array().readonly(), scope: z.json(), output: z.json(),
  operation: z.object({ kind: z.string(), version: z.number().int(), reference: z.json() }).strict().optional(),
}).strict();

/** 与原生 tool/result、PTC 子调用共同持久保存；不解析模型可见的文本。 */
export type ActionPresentationMetadata = z.output<typeof actionPresentationSchema>;
export interface ActionPresentation<D extends ActionContract> {
  readonly action: string;
  readonly bindingId: string;
  readonly modulePath: readonly string[];
  readonly scope: ActionScope<D>;
  readonly output: ActionOutput<D>;
  readonly operation?: { readonly kind: string; readonly version: number; readonly reference: Json };
}

/** 原生 UI slot 和普通消费者以共享合同读取同一结果；业务组件自行决定呈现。 */
export function readActionPresentation<D extends ActionContract>(definition: D, meta: unknown): ActionPresentation<D> | undefined {
  if (typeof meta !== 'object' || meta === null || !Object.hasOwn(meta, 'reactiveAction')) return undefined;
  const value = actionPresentationSchema.parse(Reflect.get(meta, 'reactiveAction'));
  if (value.action !== definition.name) return undefined;
  const { version: _version, scope, output, ...identity } = value;
  // 这两个值已在真实宿主中完成合同解析；canonical 原生输出是 JSON，不能再次运行单向 transform。
  return { ...identity, scope: scope as ActionScope<D>, output: output as ActionOutput<D> };
}
