# Program、Action 与模块组合

适用版本：`{{PACKAGE_VERSION}}`。共享合同从 `@hanger-source/dsh-reactive` 导入；React 声明、hooks 和 `implementProgram` 只从 `@hanger-source/dsh-reactive/program` 导入。

## 合同、实现与目标分别固定

```ts
import { defineAction, defineProgram, defineSurface, resourceView, z } from '@hanger-source/dsh-reactive';

export const Target = z.object({ sku: z.string() });
export const Save = defineAction({ name: 'save_record', description: '保存商品备注',
  scope: Target, input: z.object({ note: z.string() }),
  output: z.object({ revision: z.number().int() }) });
export const ItemSurface = defineSurface({
  views: { facts: resourceView(z.object({ revision: z.number().int(), note: z.string() })) },
  actions: { save: Save },
});
export const ItemProgram = defineProgram({ input: Target, surface: ItemSurface });
```

`implementAction(Save, { execute(input, call) { ... } })` 在宿主固定真实服务依赖。`call.scope` 是本次绑定已解析目标；业务不得读取当前页面或当前选择来代替它。宿主逐调用核验来源与权限；Offer.enabled 是当前可用性声明，不能代替权威授权。

在模块中用 `useAction(saveImplementation, () => target, [target.sku])` 取得绑定。target factory 只构造数据，deps 表达身份；未列依赖会错误保留旧目标，不能把每次 render 当新绑定修补它。

显式调用返回唯一 Invocation：`const invocation = save({ note }); await invocation.result`。`activity$`、result 与可选 operation 指向同次调用；多订阅不重执行。`invocations$` 是该绑定新调用的热流，不 replay；`activeInvocations$` 是当前在途集合，适合晚挂载关系。结束观察或卸载不解释为取消已执行的业务。

## 组件只贡献当前声明

一个业务模块通常将同一资源接到三处：

```tsx
const facts = useResource(stockImplementation, target);
const save = useAction(saveImplementation, () => target, [target.sku]);
return <>
  <Context name="facts" required={false} observation={facts.state} render={value => value} />
  <Offer action={save} enabled={facts.state.status === 'ready'} />
  <Expose surface={ItemSurface} value={{ views: { facts: facts.observations$ }, actions: { save } }} />
</>;
```

此片段置于后台组件内，并从 `/program` 导入声明与 hooks；资源 schema 应与 ItemSurface.facts 相符。

Context 默认 required=true：pending/error 阻止模型取得不完整现场。required=false 是显式非必需现场：pending/error 时撤掉该 Context，其他模块可以继续；公开资源仍保真实状态。不要丢弃错误后保留旧 ready。Offer 需按同一真实事实关闭相关行为。

`useRelation(() => standardObservable, deps)` 在提交后持有关系；未处理错误使程序失败。需要显式执行/停止/观察的持续工作用 Reaction。`useBeforeStep` 用于原生步骤前必须完成的检查。渲染函数和数据投影不发命令。

## 框架承担命名与公开映射

```tsx
// pure-contract.ts：纯合同，可由 SDK 导入
export const root = composeSurface({ items: collectionSurface(ItemSurface) });
export const Inventory = defineProgram({ input: Target, surface: root.surface });

// program.tsx：后台实现
return items.map(item =>
  <Module key={item.sku} mount={root.mounts.items.instance(item.sku)}>
    <ItemModule target={{ sku: item.sku }} />
  </Module>);
```

从核心导 `composeSurface`、`collectionSurface`；从 `/program` 导 `Module`。集合由业务 Observable 和 React 子树更新，根合同稳定。业务 key 是完整寻址身份；React key 负责重排身份，不从 key 猜 Surface。SDK 使用 `session.collection('items').instance(sku)`，不拼编码地址或工具名。

静态模块使用 `composeSurface({ preparation: PreparationSurface, knowledge: KnowledgeSurface })` 及对应 mounts。内部模块可有自己的 composition，嵌套 Module 自动 rebase，不导根 composition、不逐槽手写 Outlet/names。局部 `Expose` 必须引用定义时同一 Surface 对象；框架不复制 Action 或重新解析 scope。

Review 等选择性公开用 `selectSurface(source, { views: ['facts'], actions: ['save'] })` 后放进 composition；省略类别不公开。模块仍对自己的原 Surface Expose，框架完成选择映射。移除再加入是新绑定，旧捕获 Action 拒绝新调用；已开始执行沿原 owner 完成。

最后 `implementProgram(Inventory, { Component })` 接宿主组件。业务输入解码结果 `ProgramParsedInput` 和提交的 `ProgramInput` 可以不同；不要把单向 transform 输出重新当 raw parse。远程 View/Action 输出需真实可逆 wire codec，不能承诺任意 Date transform 都可反向 encode。
