import { implementProgram } from '@hanger-source/dsh-reactive/program';
import { Fragment } from 'react';
import { concatMap, map, of } from 'rxjs';
import { Context, Expose, Offer, useAction, useObservable, useResource, useResourceInstance, useReaction } from '@hanger-source/dsh-reactive/program';
import { defineAction, implementAction, defineApplication, defineEnvironment, defineProgram, defineSurface, defineResource, implementResource, defineReaction, implementReaction, observeResource, resourceView, z, type CancellationOptions, type SubmitOptions } from '@hanger-source/dsh-reactive';
import type { ApplicationHost } from '@hanger-source/dsh-reactive-dsh';

const save = defineAction({
  name: 'save', description: '保存正式修订',
  scope: z.object({ merchantId: z.string() }).strict(),
  input: z.object({ revision: z.number() }).strict(),
  output: z.object({ accepted: z.boolean() }).strict(),
});
const saveImplementation = implementAction(save, {
  async execute(input, call) {
    input.revision.toFixed();
    call.scope.merchantId.toUpperCase();
    // @ts-expect-error 行为参数保持 input schema 的 output 类型。
    input.revision.toUpperCase();
    // @ts-expect-error 行为目标保持 scope schema，不使用全局最近选择。
    call.scope.unknown;
    return { accepted: true };
  },
});
const surface = defineSurface({
  views: { record: resourceView(z.object({ merchantId: z.string(), revision: z.number() })) },
  actions: { save },
});
function Merchant({ merchantId }: { merchantId: string }) {
  const record = useResource(() => observeResource({ key$: of(merchantId),
    read: id => ({ merchantId: id, revision: 1 }) }), [merchantId]);
  const state = record.state;
  const action = useAction(saveImplementation, () => ({ merchantId }), [merchantId]);
  return <Fragment>
    <Context name="merchant" observation={state} render={value => value} />
    <Offer name="save" action={action} enabled={state.status === 'ready' && state.value.revision > 0} />
    <Expose surface={surface} value={{ views: { record: record.observations$ }, actions: { save: action } }} />
  </Fragment>;
}
const delivery = defineEnvironment({
  id: 'delivery', instructions: '交付',
  program: implementProgram(defineProgram({ input: z.object({ merchantId: z.string() }).strict(), surface }), { Component({ conversation }) {
      const input = useObservable(() => conversation.inputs$, [conversation]);
      if (input.status !== 'ready' || !input.value.length) return null;
      const current = input.value.at(-1)!;
      // @ts-expect-error 输入合同贯穿组件程序。
      current.scope.unknownField;
      return <Merchant key={current.scope.merchantId} merchantId={current.scope.merchantId} />;
    } }),
});
const emptySurface = defineSurface({ views: {}, actions: {} });
const analysis = defineEnvironment({
  id: 'analysis', instructions: '分析',
  program: implementProgram(defineProgram({ input: z.null(), surface: emptySurface }), { Component: () => null }),
});
const transformed = defineEnvironment({
  id: 'transformed', instructions: '解析业务输入',
  program: implementProgram(defineProgram({ input: z.object({ revision: z.string().transform(Number) }).strict(), surface: defineSurface({ views: { revision: resourceView(z.number().nullable()) }, actions: {} }) }), { Component({ conversation }) {
      const submitRaw = () => conversation.submit({ requestId: 'program-raw', content: '读取修订', intent: 'followup', delivery: 'followup' as const, scope: { revision: '7' } });
      // @ts-expect-error 程序投递使用原始输入，不把解析后的 output 再送回输入边界。
      const submitParsed = () => conversation.submit({ requestId: 'program-parsed', content: '读取修订', intent: 'followup', delivery: 'followup' as const, scope: { revision: 7 } });
      void submitRaw; void submitParsed;
      return null;
    } }),
});
const application = defineApplication({ name: 'typed', environments: [delivery, analysis, transformed], admit: async () => {} });

// 此文件由项目 TypeScript 检查；真实 JSX 消费合同不需要启动模型。
async function authoring(host: ApplicationHost<typeof application.environments>, options: CancellationOptions) {
  const workspace = await host.createWorkspace({ directory: '/application/merchant-a', title: '商家A' });
  const session = await host.create('delivery', { workspaceId: workspace.workspaceId });
  const record = await session.readView('record', options);
  record.revision.toFixed();
  // @ts-expect-error 公开视图保持领域 output 类型。
  record.revision.toUpperCase();
  // @ts-expect-error 不存在的槽不会退化为动态字符串或 any。
  session.views.unknownView;
  const saveCurrent = session.resolveAction('save', { merchantId: record.merchantId }, options);
  saveCurrent.scope.merchantId.toUpperCase();
  const invocation = saveCurrent({ revision: 2 }, options);
  (await invocation.result).accepted.valueOf();
  const trustedOptions: SubmitOptions = { ...options, actorId: 'authenticated-operator' };
  await session.submit({ requestId: 'shared-options', content: '读取商家修订', intent: 'question', delivery: 'followup',
    scope: { merchantId: record.merchantId } }, trustedOptions);
  // @ts-expect-error SDK读取统一使用选项对象，不接收裸signal。
  session.readView('record', options.signal);
  // @ts-expect-error 绑定解析统一使用同一取消选项，不接收裸signal。
  session.resolveAction('save', { merchantId: record.merchantId }, options.signal);
  // @ts-expect-error 解析绑定只接受取消选项，不授予调用者身份。
  session.resolveAction('save', { merchantId: record.merchantId }, { actorId: 'operator' });
  // @ts-expect-error Action 输入使用原合同。
  saveCurrent({ revision: '2' });
  // @ts-expect-error 未声明动作不可调用。
  session.resolveAction('unknown', null);
  // @ts-expect-error 解析目标保持纯Action合同的原始scope类型。
  session.resolveAction('save', { merchantId: 123 });
  // @ts-expect-error 动态槽调用退出，不能把名称代理当作已捕获绑定。
  session.actions.save({ revision: 2 });
  // @ts-expect-error 环境输入合同保持 merchantId:string。
  await session.submit({ requestId: 'wrong', content: '核对', intent: 'question', delivery: 'followup' as const, scope: { merchantId: 123 } });
  // @ts-expect-error 工作环境名称由应用定义推导。
  await host.create('unknown', { workspaceId: workspace.workspaceId });
  // @ts-expect-error 工作区选择独立于环境，创建不能省略。
  await host.create('delivery');
  // @ts-expect-error 不保留把第二个字符串当sessionId的旧创建入口。
  await host.create('delivery', 'session-id');
  const analysisSession = await host.create('analysis', { workspaceId: workspace.workspaceId });
  // @ts-expect-error 同一工作区不会让分析环境取得交付环境的Action。
  analysisSession.resolveAction('save', { merchantId: 'merchant-a' });
  const readOnly = await host.open('persisted-id', analysis.program.definition);
  // @ts-expect-error 分析环境没有交付环境的视图。
  readOnly.views.record;
  // @ts-expect-error 分析环境没有交付环境的 Action。
  readOnly.resolveAction('save', { merchantId: 'merchant-a' });
  // @ts-expect-error 不能用类型参数跳过真实环境核验。
  await host.open<'delivery'>('persisted-id');
  const parsed = await host.create('transformed', { workspaceId: workspace.workspaceId, sessionId: 'known-session' });
  const parsedEvents$ = parsed.events$.pipe(map(event => {
    if ('inputs' in event && event.inputs !== undefined) {
      event.inputs[0]?.scope.revision.toFixed();
      // @ts-expect-error 事实事件消费 Zod output:number。
      event.inputs[0]?.scope.revision.trim();
    }
    return event;
  }));
  void parsedEvents$;
  await parsed.submit({ requestId: 'raw-input', content: '读取修订', intent: 'question', delivery: 'followup' as const, scope: { revision: '7' } });
  // @ts-expect-error submit 仍接受 Zod input:string。
  await parsed.submit({ requestId: 'parsed-input', content: '读取修订', intent: 'question', delivery: 'followup' as const, scope: { revision: 7 } });
}
void authoring;

const parsedResource = implementResource(defineResource({ name: 'parsed-resource',
  params: z.object({ key: z.string().transform(Number) }), value: z.string().transform(value => new Date(value)),
}), { read: params => { params.key.toFixed(); return '2026-10-27T00:00:00Z'; } });
const parsedReaction = implementReaction(defineReaction({ name: 'parsed-reaction',
  params: z.object({ key: z.string().transform(Number) }), event: z.string().transform(Number), result: z.string().transform(Number),
}), { observe: () => of('7'), operator: concatMap, execute: (event, context) => String(event + context.params.key) });
function useTypedProgramBindings() {
  const resource = useResourceInstance(parsedResource, { key: '7' });
  resource.params.key.toFixed();
  const read: () => Promise<Date> = () => resource.read();
  const values = resource.values$.pipe(map(value => value.toISOString()));
  const observed = useResource(resource);
  if (observed.state.status === 'ready') observed.state.value.toISOString();
  // @ts-expect-error 资源入口接受参数 schema 的 input，不接受解析后的 number。
  useResourceInstance(parsedResource, { key: 7 });
  // @ts-expect-error 直接观察实现时同样遵循原参数输入类型。
  useResource(parsedResource, { key: 7 });
  // @ts-expect-error 资源值已经由 schema 解析为 Date。
  resource.values$.pipe(map(value => value.trim()));
  const reaction = useReaction(parsedReaction, { key: '7' });
  reaction.params.key.toFixed();
  const start = () => reaction.start().events$.pipe(map(event => event.event + event.result));
  // @ts-expect-error 反应入口接受原参数，不能因调用参数错误扩大实现的合同。
  useReaction(parsedReaction, { key: 7 });
  return { resource, reaction, read, values, start };
}
void useTypedProgramBindings;
