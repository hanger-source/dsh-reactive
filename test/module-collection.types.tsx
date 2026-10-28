import { collectionSurface, composeSurface, selectSurface, defineSurface, resourceView, defineAction, defineOperation, defineProgram, z, type CancellationOptions, type ResourceReadable, type SurfaceClient } from '@hanger-source/dsh-reactive';
import { Module } from '@hanger-source/dsh-reactive/program';
import type { ConversationHandle } from '@hanger-source/dsh-reactive-dsh';

const date = z.codec(z.iso.datetime(), z.date(), { decode: value => new Date(value), encode: value => value.toISOString() });
const work = defineOperation({ kind: 'collection.work', version: 1, command: z.object({ merchantId: z.string() }),
  reference: z.object({ operationId: z.string() }), state: z.object({ done: z.boolean(), at: date }),
  settled: state => state.done ? state : undefined });
const record = defineAction({ name: 'record_details', description: '记录商家资料', scope: z.string().transform(merchantId => ({ merchantId })),
  input: z.object({ count: z.string().transform(Number) }), output: z.object({ recorded: z.boolean() }) });
const local = defineSurface({ views: { loadedAt: resourceView(date) }, actions: { record }, operations: { work } });
const merchant = composeSurface({ knowledge: local, reviews: collectionSurface(local) });
const composed = composeSurface({ merchants: collectionSurface(merchant.surface) });
const program = defineProgram({ input: z.null(), surface: composed.surface });
declare const session: ConversationHandle<typeof program>;
declare const options: CancellationOptions;
async function consume(key: string) {
  const instances: ResourceReadable<readonly string[]> = session.collection('merchants').instances;
  const instance = session.collection('merchants').instance(key);
  const knowledge = instance.module(merchant.mounts.knowledge);
  const localDate: Date = await consumeModule(knowledge);
  const rootedDate: Date = await consumeModule(session.module(composed.mounts.merchants.instance(key)).module(merchant.mounts.knowledge));
  const localAction = knowledge.resolveAction('record', key, options);
  const localResult: boolean = (await localAction({ count: '7' }, options).result).recorded;
  const localCommand: string = knowledge.operations.work(options).command.merchantId;
  void localDate; void rootedDate; void localResult; void localCommand;
  // @ts-expect-error 嵌套module恢复局部槽名，不接受父级路径。
  knowledge.readView('knowledge.loadedAt');
  // @ts-expect-error 嵌套module保留输入transform之前的string。
  localAction({ count: 7 });
  const nested: readonly string[] = await instance.collection('reviews').instances.read();
  const resource: ResourceReadable<Date> = instance.viewResource('knowledge.loadedAt');
  const loadedAt: Date = await instance.readView('knowledge.loadedAt', options);
  const reviewDate: Date = await instance.collection('reviews').instance('review-a').readView('loadedAt', options);
  const action = await instance.resolveAction('knowledge.record', key, options);
  const merchantId: string = action.scope.merchantId;
  const invocation = await action({ count: '7' }, options);
  const recorded: boolean = (await invocation.result).recorded;
  const existing = await instance.operations['knowledge.work'](options);
  const operationMerchant: string = existing.command.merchantId;
  void instances; void nested; void resource; void loadedAt; void reviewDate; void merchantId; void recorded; void operationMerchant;
  // @ts-expect-error 资源地址仍由局部合同决定。
  instance.viewResource('unknown');
  // @ts-expect-error collection名仍由稳定纯合同决定。
  session.collection('unknown');
  // @ts-expect-error 动态key不放宽本地slot合同。
  instance.readView('unknown');
  // @ts-expect-error 目标仍按scope原始string输入，不重parse领域output。
  instance.resolveAction('knowledge.record', { merchantId: key });
  // @ts-expect-error 输入保持原始string，业务transform输出number不会泄漏为输入。
  action({ count: 7 });
  // @ts-expect-error collection不扩成任意根Action字符串。
  session.resolveAction('merchants.any.knowledge.record', key);
}
void consume;
function consumeModule(client: SurfaceClient<typeof local>) { return client.readView('loadedAt'); }
const review = composeSurface({ merchants: collectionSurface(selectSurface(merchant.surface, { views: ['knowledge.loadedAt'] })) });
const reviewProgram = defineProgram({ input: z.null(), surface: review.surface });
declare const reviewer: ConversationHandle<typeof reviewProgram>;
const reviewed = reviewer.collection('merchants').instance('merchant-a');
const projectedReview = reviewer.module(review.mounts.merchants.instance('merchant-a'));
// @ts-expect-error collection mount的第二型参保留实际selected，不能暴露source的Action。
projectedReview.resolveAction('knowledge.record', 'merchant-a');
// @ts-expect-error collection mount的selected不恢复Operation。
projectedReview.operations['knowledge.work']();
// @ts-expect-error selection未公开Action。
reviewed.resolveAction('knowledge.record', 'merchant-a');
// @ts-expect-error selection未公开Operation。
reviewed.operations['knowledge.work']();
const mounted = <Module mount={composed.mounts.merchants.instance('merchant-a')}>
  <Module mount={merchant.mounts.knowledge} />
</Module>;
void mounted;
