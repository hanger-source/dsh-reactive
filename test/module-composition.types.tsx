import type { Observable } from 'rxjs';
import { composeSurface, selectSurface, defineSurface, resourceView, defineAction, defineOperation, defineProgram, z, type BoundAction, type CancellationOptions, type Observation, type SurfaceClient } from '@hanger-source/dsh-reactive';
import { Module, Expose, Offer } from '@hanger-source/dsh-reactive/program';
import type { ConversationHandle } from '@hanger-source/dsh-reactive-dsh';

const date = z.codec(z.iso.datetime(), z.date(), { decode: value => new Date(value), encode: value => value.toISOString() });
const work = defineOperation({ kind: 'module-types.work', version: 1,
  command: z.object({ merchantId: z.string(), commandId: z.string() }), reference: z.object({ operationId: z.string() }),
  state: z.object({ done: z.boolean(), at: date }), settled: state => state.done ? state : undefined });
const prepare = defineAction({ name: 'prepare_merchant', description: '准备明确商家',
  scope: z.string().transform(merchantId => ({ merchantId })),
  input: z.object({ amount: z.string().transform(Number) }), output: work.reference,
  operation: { definition: work, command: (_input, target, source) => ({ merchantId: target.merchantId, commandId: source.invocationId }),
    reference: value => value } });
const local = defineSurface({ views: { loadedAt: resourceView(date) }, actions: { prepare }, operations: { work } });
const composed = composeSurface({ a: { preparation: local }, b: { preparation: local } });
const program = defineProgram({ input: z.null(), surface: composed.surface });
declare const session: ConversationHandle<typeof program>;
declare const options: CancellationOptions;

async function consume() {
  const local = session.module(composed.mounts.a.preparation);
  const localDate: Date = await consumeModule(local);
  const localAction = local.resolveAction('prepare', 'merchant-a', options);
  const localScope: string = localAction.scope.merchantId;
  const localOperationId: string = (await localAction({ amount: '7' }).result).operationId;
  const localOperation = local.operations.work(options);
  const localCommand: string = localOperation.command.commandId;
  void localDate; void localScope; void localOperationId; void localCommand;
  // @ts-expect-error 局部SDK不接受根路径或任意字符串。
  local.readView('a.preparation.loadedAt');
  // @ts-expect-error scope保留原始string输入。
  local.resolveAction('prepare', { merchantId: 'merchant-a' });
  // @ts-expect-error 局部Action保持原始input。
  localAction({ amount: 7 });
  const a: Date = await session.readView('a.preparation.loadedAt', options);
  const b: Date = await session.readView('b.preparation.loadedAt', options);
  const prepareA = await session.resolveAction('a.preparation.prepare', 'merchant-a', options);
  const target: string = prepareA.scope.merchantId;
  const invocation = await prepareA({ amount: '7' }, options);
  const operationId: string = (await invocation.result).operationId;
  const operation = await invocation.operation;
  const at: Date = (await operation.settled(options)).at;
  const existing = await session.operations['b.preparation.work'](options);
  const commandId: string = existing.command.commandId;
  void a; void b; void target; void operationId; void at; void commandId;
  // @ts-expect-error 组合后公开的槽保留完整模块路径，不猜测未限定的同名槽。
  session.readView('loadedAt');
  // @ts-expect-error 叶合同未声明的槽不会因组合而变成任意字符串。
  session.resolveAction('a.preparation.unknown', 'merchant-a');
  // @ts-expect-error 期待目标仍使用单向scope合同的原始input，不接受其解析输出。
  session.resolveAction('a.preparation.prepare', { merchantId: 'merchant-a' });
  // @ts-expect-error Action输入保留原始string，不被框架展开成解析后的number。
  prepareA({ amount: 7 });
  // @ts-expect-error 公开读取仍保留Date领域值。
  const wire: string = await session.readView('b.preparation.loadedAt');
  void wire;
}
void consume;
function consumeModule(client: SurfaceClient<typeof local>) { return client.readView('loadedAt'); }

const review = selectSurface(local, { views: ['loadedAt'] });
const reviewComposition = composeSurface({ review });
const reviewProgram = defineProgram({ input: z.null(), surface: reviewComposition.surface });
declare const reviewer: ConversationHandle<typeof reviewProgram>;
async function consumeReview() {
  const local = reviewer.module(reviewComposition.mounts.review);
  const localDate: Date = await local.readView('loadedAt');
  void localDate;
  // @ts-expect-error 挂载selected合同不公开动作，不能恢复source的Action。
  local.resolveAction('prepare', 'merchant-a');
  // @ts-expect-error 挂载selected合同不公开Operation。
  local.operations.work();
  // @ts-expect-error 完整模块消费函数不能接受缺少业务能力的selected合同。
  consumeModule(local);
  const date: Date = await reviewer.readView('review.loadedAt');
  void date;
  // @ts-expect-error 未选择的动作不会出现在只读组合合同中。
  reviewer.resolveAction('review.prepare', 'merchant-a');
  // @ts-expect-error 未选择的操作类别保持不公开。
  reviewer.operations['review.work']();
}
void consumeReview;
// @ts-expect-error selectSurface的key来自同一个本地合同。
selectSurface(local, { actions: ['unknown'] });

declare const action: BoundAction<typeof prepare, true>;
declare const loadedAt$: Observable<Observation<Date>>;
const element = <Module mount={composed.mounts.a.preparation}>
  <Offer action={action} />
  <Expose surface={local} value={{ views: { loadedAt: loadedAt$ }, actions: { prepare: action } }} />
</Module>;
const selected = <Module mount={reviewComposition.mounts.review}>
  <Expose surface={local} value={{ views: { loadedAt: loadedAt$ }, actions: { prepare: action } }} />
</Module>;
void element; void selected;
// @ts-expect-error Surface定义不是框架签发的模块挂载token。
const invalid = <Module mount={local} />;
void invalid;
