import { of, type Observable } from 'rxjs';
import { z } from 'zod';
import { bindAction, defineAction, implementAction, type BoundAction } from '@hanger-source/dsh-reactive';
import { defineOperation, type BoundOperation, type OperationOwner } from '@hanger-source/dsh-reactive';
import { Context, Expose, Offer, Follow, Skill } from '@hanger-source/dsh-reactive/program';
import { useAction, useOperation, useResource } from '@hanger-source/dsh-reactive/program';
import type { Observation } from '@hanger-source/dsh-reactive';
import { decodeView, defineSurface, encodeView, resourceView, view, type SurfaceBindings, type SurfaceViews, type ViewSource } from '@hanger-source/dsh-reactive';

const prepare = defineAction({ name: 'prepare', description: '准备交付', scope: z.object({ merchantId: z.string() }),
  input: z.object({ revision: z.number() }), output: z.object({ ready: z.boolean() }),
});
const other = defineAction({ name: 'other', description: '其他合同', scope: z.null(), input: z.string(), output: z.number() });
const prepareImplementation = implementAction(prepare, { execute: () => ({ ready: true }) });
const prepared = bindAction(prepareImplementation, { merchantId: 'merchant-a' }, { admit() {} });
const foreign = bindAction(implementAction(other, { execute: () => 1 }), null, { admit() {} });
const dateCodec = z.codec(z.iso.datetime(), z.date(), { decode: value => new Date(value), encode: value => value.toISOString() });
const date = view(dateCodec);
const loadedAt = resourceView(dateCodec);
const surface = defineSurface({ views: { merchant: view(z.object({ merchantId: z.string(), revision: z.number() })), observedAt: date, loadedAt },
  actions: { prepare } });
const merchant$ = of({ merchantId: 'merchant-a', revision: 1 });
const loadedAt$: Observable<Observation<Date>> = of({ status: 'ready', generation: 1, value: new Date(), observedAt: 0 });
const bindings: SurfaceBindings<typeof surface> = { views: { merchant: merchant$, observedAt: of(new Date()), loadedAt: loadedAt$ }, actions: { prepare: prepared }, operations: {} };
const typed: Observable<Date> = bindings.views.observedAt;
const resourceSource: ViewSource<typeof loadedAt> = bindings.views.loadedAt;
const publicViews: SurfaceViews<typeof surface> = { merchant: merchant$, observedAt: of(new Date()), loadedAt: of(new Date()) };
const publicResource: Observable<Date> = publicViews.loadedAt;
void typed;
void resourceSource;
void publicResource;
Expose({ surface, value: { views: { merchant: merchant$ } } });
Expose({ surface, value: { views: { observedAt: of(new Date()) }, actions: { prepare: prepared } } });
Expose({ surface, value: { views: { loadedAt: loadedAt$ } } });
Offer({ name: 'prepare_merchant', action: prepared, enabled: true });
Context({ name: 'merchant', value: { merchantId: 'merchant-a' } });
Context({ name: 'time', observation: { status: 'ready', generation: 1, value: new Date(), observedAt: 0 }, render: value => value.toISOString() });
Context({ name: 'resource-time', source: { observations$: loadedAt$ }, render: value => value.toISOString() });
Skill({ name: 'merchant-delivery' });
Skill({ directory: '/application/methods' });

const wire: string = encodeView(date, new Date());
const decoded: Date = decodeView(date, wire);
void decoded;
const resourceWire: string = encodeView(loadedAt, new Date());
const resourceDomain: Date = decodeView(loadedAt, resourceWire);
void resourceDomain;
function ResourceConsumer() {
  const resource = useResource(loadedAt$);
  const state: Observation<Date> = resource.state;
  if (state.status === 'ready') {
    const date: Date = state.value;
    void date;
    // @ts-expect-error state 直接是资源观察，ready.value 不是嵌套 Observation。
    const nested: Observation<Date> = state.value;
    void nested;
  }
  Expose({ surface, value: { views: { loadedAt: resource } } });
  // @ts-expect-error useResource 的来源必须明确携带资源有效性。
  useResource(() => of(new Date()), []);
}
void ResourceConsumer;
function ActionConsumer() {
  const action = useAction(prepareImplementation, () => ({ merchantId: 'merchant-a' }), []);
  const typedAction: BoundAction<typeof prepare, true> = action;
  typedAction({ revision: 8 });
  Expose({ surface, value: { actions: { prepare: typedAction } } });
  // @ts-expect-error useAction 需要真实宿主实现，纯合同不提供执行。
  useAction(prepare, () => ({ merchantId: 'merchant-a' }), []);
  // @ts-expect-error 目标构造不能把纯合同反向扩宽成另一种 scope。
  useAction(prepareImplementation, () => ({ merchantId: 123 }), []);
}
void ActionConsumer;
// @ts-expect-error 本地视图使用 codec output Date，不能反向扩宽成 string。
encodeView(date, '2026-10-26T00:00:00.000Z');
// @ts-expect-error wire 使用 codec input string。
decodeView(date, new Date());
// @ts-expect-error 资源 codec 仍只编码领域值，不编码外层 Observation。
encodeView(loadedAt, { status: 'ready', generation: 1, value: new Date(), observedAt: 0 });
// @ts-expect-error 资源视图必须绑定 Observation 流，不能由普通值流反向扩宽 mode。
Expose({ surface, value: { views: { loadedAt: of(new Date()) } } });
// @ts-expect-error 普通值视图不猜测含 status 的业务值是否是资源。
Expose({ surface, value: { views: { observedAt: loadedAt$ } } });
// @ts-expect-error 资源的 ready 领域类型仍由同一个 codec 决定。
Expose({ surface, value: { views: { loadedAt: of<Observation<string>>({ status: 'ready', generation: 1, value: 'wrong-domain', observedAt: 0 }) } } });
// @ts-expect-error 公开领域值流不暴露绑定侧的 Observation 包装。
const wrongPublic: SurfaceViews<typeof surface> = { merchant: merchant$, observedAt: of(new Date()), loadedAt: loadedAt$ };
void wrongPublic;
// @ts-expect-error Surface 视图输出不可由 Expose 的值反向扩宽。
Expose({ surface, value: { views: { observedAt: of('2026-10-26') } } });
// @ts-expect-error 公开动作槽绑定对应的合同。
Expose({ surface, value: { actions: { prepare: foreign } } });
// @ts-expect-error 不能暴露合同没有声明的视图。
Expose({ surface, value: { views: { unknown: of(1) } } });
const unknownView = { views: { merchant: merchant$, unknown: of(1) } };
// @ts-expect-error 变量中的未知视图槽也必须被拒绝。
Expose({ surface, value: unknownView });
const unknownAction = { actions: { prepare: prepared, unknown: prepared } };
// @ts-expect-error 变量中的未知动作槽也必须被拒绝。
Expose({ surface, value: unknownAction });
// @ts-expect-error 未知贡献类别不能被静默丢弃。
Expose({ surface, value: { views: { merchant: merchant$ }, unexpected: true } });
// @ts-expect-error Skill 的来源必须明确且互斥。
Skill({ name: 'merchant-delivery', directory: '/application/methods' });
// @ts-expect-error 公开 Observation render 不能把 Date 当成另一种事实类型。
Context({ name: 'time', observation: { status: 'ready', generation: 1, value: new Date(), observedAt: 0 }, render: (value: number) => value });

const Preparation = defineOperation({
  kind: 'merchant.preparation', version: 1,
  command: z.object({ merchantId: z.string(), commandId: z.string() }),
  reference: z.object({ operationId: z.string() }),
  state: z.object({ complete: z.boolean() }),
  settled: state => state.complete ? state : undefined,
  feedback: z.object({ observedAt: dateCodec, reason: z.string() }),
});
const operationSurface = defineSurface({ views: {}, actions: {}, operations: { preparation: Preparation } });
declare const preparation: BoundOperation<typeof Preparation>;
declare const preparationOwner: OperationOwner<typeof Preparation>;
Expose({ surface: operationSurface, value: { operations: { preparation } } });
Follow({ name: 'wait_preparation', description: '沿原准备操作等待下一次协作反馈', operation: preparation,
  when: 'feedback', scope: { merchantId: 'merchant-a' } });
function OperationConsumer(hasCommand: boolean) {
  const operation = useOperation(preparationOwner, () => ({ merchantId: 'merchant-a', commandId: 'original-command' }), []);
  const precise: BoundOperation<typeof Preparation> = operation;
  Expose({ surface: operationSurface, value: { operations: { preparation: precise } } });
  const optional = useOperation(preparationOwner,
    () => hasCommand ? { merchantId: 'merchant-a', commandId: 'original-command' } : null, [hasCommand]);
  const nullable: BoundOperation<typeof Preparation> | null = optional;
  if (nullable) Expose({ surface: operationSurface, value: { operations: { preparation: nullable } } });
  // @ts-expect-error 未取得原命令时没有绑定，必须先判断再公开或读取。
  const required: BoundOperation<typeof Preparation> = optional;
  void required;
  // @ts-expect-error 已有操作通过明确 command 绑定，不能用另一个引用形状代替。
  useOperation(preparationOwner, () => ({ operationId: 'existing-operation' }), []);
}
void OperationConsumer;
const NullCommand = defineOperation({ kind: 'nullable-command', version: 1,
  command: z.null(), reference: z.string(), state: z.string(), settled: state => state });
declare const nullCommandOwner: OperationOwner<typeof NullCommand>;
function NullOperationConsumer() {
  const binding = useOperation(nullCommandOwner, () => null, []);
  const nullable: BoundOperation<typeof NullCommand> | null = binding;
  // @ts-expect-error null 始终表示当前没有绑定，合法 null command 也不能推成非空返回。
  const required: BoundOperation<typeof NullCommand> = binding;
  void nullable; void required;
}
void NullOperationConsumer;
async function OperationReader() {
  const read = await preparation.participation('original-participation').readFeedback();
  if (read.status === 'ready') {
    const date: Date = read.node.payload.observedAt;
    void date;
    // @ts-expect-error 本地反馈保留 codec output，不是 wire 字符串。
    const wire: string = read.node.payload.observedAt;
    void wire;
  }
  // @ts-expect-error Operation 只读绑定不能作为 Action 重新执行业务。
  preparation({});
  // @ts-expect-error 普通读取不会取得当前 Goal 的新等待权限。
  preparation.follow({ after: 'old-node' });
}
void OperationReader;
// @ts-expect-error 纯合同没有绑定既有命令，不能直接贡献运行出口。
Expose({ surface: operationSurface, value: { operations: { preparation: Preparation } } });
// @ts-expect-error 不允许公开未声明的操作槽。
Expose({ surface: operationSurface, value: { operations: { unknown: preparation } } });
// @ts-expect-error 业务 Action 和 Operation 参与是不同的能力。
Follow({ name: 'wrong', description: '错误能力', operation: prepared, when: 'feedback', scope: null });
