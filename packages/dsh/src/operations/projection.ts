import { pluginRecordOf } from '@deepseek-ai/dsh-session';
import type { ProjectionDefinition } from '@deepseek-ai/dsh-session-projection';
import { z } from 'zod';
import { operationFollowLinkSchema, type OperationFollowSnapshot } from '@hanger-source/dsh-reactive/kernel';
import { canonical, operationNotStartedSchema } from '../conversations/records.ts';

export const receiptSchema = z.object({ id: z.string().min(1), reference: z.json() }).strict();
const publisher = { publisherId: z.string().min(1) };
export const intentSchema = z.discriminatedUnion('when', [operationFollowLinkSchema.options[0].extend(publisher), operationFollowLinkSchema.options[1].extend(publisher)]);
export const deliverySchema = z.object({ id: z.string().min(1), actorId: z.string().min(1), notifierId: z.string().min(1), reference: z.json().optional(),
  node: z.object({ id: z.string().min(1), payload: z.json() }).strict().optional(), instruction: z.string().min(1).max(32_000) }).strict();
export const followsSchema = z.object({
  inheritedEventCount: z.number().int().nonnegative(),
  intents: z.record(z.string(), intentSchema),
  receipts: z.record(z.string(), receiptSchema),
  deliveries: z.record(z.string(), deliverySchema),
  notStarted: z.record(z.string(), operationNotStartedSchema),
}).strict();
export type FollowState = z.infer<typeof followsSchema>;
export type FollowIntent = z.infer<typeof intentSchema>;
export type Delivery = z.infer<typeof deliverySchema>;

declare module '@deepseek-ai/dsh-session/types' {
  interface PluginRecordMap {
    'plugin:reactive/operation-follow': FollowIntent;
    'plugin:reactive/operation-receipt': z.infer<typeof receiptSchema>;
    'plugin:reactive/operation-delivery': Delivery;
  }
}
declare module '@deepseek-ai/dsh-session-projection/types' {
  interface SessionProjectionStateMap { reactiveOperations: FollowState; }
}

/** 执行与公开观察共用唯一折叠；这里只保存关联与反馈准备事实，不执行业务任务。 */
export const followsProjection: ProjectionDefinition<'reactiveOperations'> = {
  key: 'reactiveOperations', stateVersion: 4, stateSchema: followsSchema,
  init: (_header, inheritedEventCount) => ({ inheritedEventCount, intents: {}, receipts: {}, deliveries: {}, notStarted: {} }),
  apply(previous, event) {
    if (event.seq < previous.inheritedEventCount) return previous;
    const record = pluginRecordOf(event);
    if (record?.type === 'plugin:reactive/operation-not-started') {
      const closed = operationNotStartedSchema.parse(record.data);
      const intent = Object.hasOwn(previous.intents, closed.id) ? previous.intents[closed.id] : undefined;
      if (!intent) throw new Error('REACTIVE_OPERATION_FOLLOW_NOT_FOUND');
      if (canonical({ id: intent.id, continuation: intent.continuation, wait: intent.wait }) !== canonical(closed))
        throw new Error('REACTIVE_OPERATION_FOLLOW_MISMATCH');
      if (Object.hasOwn(previous.receipts, closed.id)) throw new Error('REACTIVE_OPERATION_ALREADY_STARTED');
      return { ...previous, notStarted: { ...previous.notStarted, [closed.id]: closed } };
    }
    if (record?.type === 'plugin:reactive/operation-follow') {
      const link = intentSchema.parse(record.data);
      const before = Object.hasOwn(previous.intents, link.id) ? previous.intents[link.id] : undefined;
      if (before && canonical(before) !== canonical(link)) throw new Error('REACTIVE_OPERATION_FOLLOW_CONFLICT');
      return { ...previous, intents: { ...previous.intents, [link.id]: link } };
    }
    if (record?.type === 'plugin:reactive/operation-receipt') {
      const receipt = receiptSchema.parse(record.data);
      if (!Object.hasOwn(previous.intents, receipt.id)) throw new Error('REACTIVE_OPERATION_FOLLOW_NOT_FOUND');
      const before = Object.hasOwn(previous.receipts, receipt.id) ? previous.receipts[receipt.id] : undefined;
      if (before && canonical(before) !== canonical(receipt)) throw new Error('REACTIVE_OPERATION_REFERENCE_CONFLICT');
      return { ...previous, receipts: { ...previous.receipts, [receipt.id]: receipt } };
    }
    if (record?.type === 'plugin:reactive/operation-delivery') {
      const delivery = deliverySchema.parse(record.data);
      if (!Object.hasOwn(previous.intents, delivery.id)) throw new Error('REACTIVE_OPERATION_FOLLOW_NOT_FOUND');
      const before = Object.hasOwn(previous.deliveries, delivery.id) ? previous.deliveries[delivery.id] : undefined;
      if (before && canonical(before) !== canonical(delivery)) throw new Error('REACTIVE_OPERATION_DELIVERY_CONFLICT');
      return { ...previous, deliveries: { ...previous.deliveries, [delivery.id]: delivery } };
    }
    return previous;
  },
};

/** 同一日志切面的只读投射；准备正文不代表原生受理，缺回执不代表业务未开始。 */
export function operationFollowSnapshots(state: FollowState): readonly OperationFollowSnapshot[] {
  return Object.freeze(Object.values(state.intents).map(intent => {
    const { publisherId: _publisherId, ...link } = intent;
    const receipt = state.receipts[intent.id], delivery = state.deliveries[intent.id];
    return Object.freeze({ link,
      receipt: receipt === undefined ? null : { reference: receipt.reference },
      preparedDelivery: delivery === undefined ? null : {
        requestId: 'operation:' + intent.id, actorId: delivery.actorId, notifierId: delivery.notifierId,
        ...delivery.reference === undefined ? {} : { reference: delivery.reference },
        ...delivery.node === undefined ? {} : { node: delivery.node },
      },
      notStarted: state.notStarted[intent.id] ?? null,
    });
  }));
}
