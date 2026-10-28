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

`defineAction` 的 `version` 省略时为 1，也可显式指定正安全整数。名称与版本是合同发布者声明的跨进程、原生结果和委托恢复身份；输入、目标、输出或业务语义发生不兼容变化时增加版本，并同步消费同一纯合同。本地绑定仍核对定义对象，不以同名同版本替代对象身份。`actionIdentity`、`assertActionIdentity` 与 `ActionContractMismatchError` 可用于自己的明确合同边界；`actionIdentityKey` 供同名多版本目录使用。它们不推断函数等价、不授予权限、不承担业务幂等。

在模块中用 `useAction(saveImplementation, () => target, [target.sku])` 取得绑定。target factory 只构造数据，deps 表达身份；未列依赖会错误保留旧目标，不能把每次 render 当新绑定修补它。

局部实现可以用 `implementAction({ name, description, scope, input, output, execute })`，或在组件中用 `useAction(() => ({ name, description, scope, target, input, output, execute }), dependencies)` 取得同一个正式 Action。factory 不执行或注册副作用，Program 持有绑定并逐调用提供可信来源；同依赖保留身份，目标切换不重定向旧调用。名称在能力定义处明确写一次，不从 React key 或诊断标签推导。组件撤下拒绝新调用，已受理业务按原 owner 完成；Resource 查询、Reaction 运行和 Action 执行继续各守自己的生命周期。

Operation 代表业务 owner 持有的跨调用事实。局部 Action 若声明 operation，必须同时提供同一合同的 operationOwner；共享 Operation 的 kind、version 和恢复规则不能因使用局部语法而省略。共享 SDK 或原生 UI 需要独立导入的 Action 合同时，继续分开纯合同和业务实现。

显式调用返回唯一 Invocation：`const invocation = save({ note }); await invocation.result`。activity、result 与可选 operation 指向同次调用；`invocation.activity.read()` 读取当前事实，`useResource(invocation.activity)` 让业务 hook 借入它，activity$ 是同一资源的 values$。read 不等待业务完成，业务 pending/running/failed 是可有效读取的事实；资源 error 表达观察失败。返回活动达到 returned/failed 后，远程底层流释放，资源继续随 SDK owner 观察有效性；晚读取保留终态，多订阅不重执行。纯本地事实由 Invocation 持有，不随组件或绑定退出撤销；远程读取随所属 SDK 退出。invocations$ 是该绑定新调用的热流，不 replay；activeInvocations$ 是当前在途集合，适合晚挂载关系。结束观察或卸载不解释为取消已执行的业务。

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

`useBeforeStep(check, dependencies)` 的每次登记有独立身份，即使多个组件使用同一 check 函数，退出一个组件也不撤销其他登记。依赖替换或组件退出会中止旧检查收到的 signal 并结束本次等待，准备过程重新核对当前集合；不配合取消的 Promise 迟到完成或失败都不代表当前检查结果。原生步骤取消、Program 失败或退出同样结束等待，当前有效检查的错误仍传播。检查必须幂等，应使用传入 signal 等待真实依赖，例如 `await resource.read(signal)`；业务动作仍从 Action 或其他显式执行入口发起。

`Skill({ name })` 选择原生方法正文进入步骤上下文。异步准备期间，声明、原生 catalog 或本会话工作目录变化会取消旧查询并准备当前集合，不等待旧 provider 返回。迟到结果不能写回，共享 provider 的注册及其他会话查询保持原有寿命。当前方法缺失、禁止模型调用或读取失败仍拒绝准备；步骤取消或程序退出结束等待，不把查询取消解释为业务命令。

Program 的 Context、Tool 与 Skill 声明安装归实际 Agent 下的正式 Fiber，借用同一安装作用域。关闭 Agent 或 Program 失败会撤下注册、目录监听及准备查询，异步清理由原安装等待；已受理 Action 与业务 Operation 继续按自己的 owner 结算。公开句柄可以保留关闭后的冷 Program 图，恢复时重新安装原生声明。open、service、resume 先核对预期合同，合同不符不求值新程序；原生恢复失败只回滚本次实际取得的句柄，不销毁已有冷观察。

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

消费模块发现事实时，直接使用 collection.instances 与 session.actionManifest 资源，按同一 Observation 语义组合成员、业务资格和当前声明。普通函数、Promise、Observable 与 hooks 复用原查询及 SDK owner；父模块撤下不是一个仍有效的空成员目录，嵌套目录会报告 error。不同槽沿各自查询流到达，不提供跨槽事务快照，也不因为观察目录自动调用行为。

静态模块使用 `composeSurface({ preparation: PreparationSurface, knowledge: KnowledgeSurface })` 及对应 mounts。内部模块可有自己的 composition，嵌套 Module 自动 rebase，不导根 composition、不逐槽手写 Outlet/names。局部 `Expose` 必须引用定义时同一 Surface 对象；框架不复制 Action 或重新解析 scope。

外部消费者调用 `session.module(composition.mounts.preparation)` 取得局部 SurfaceClient，随后使用模块自己的视图、Action 和 Operation 名称；集合成员也支持相同 module 入口。挂载点 surface 是实现原合同，selected 是公开子集，返回客户端只消费 selected。资源与根地址是同一个对象，调用选项、同步或异步返回及 owner 沿用原 SDK；局部对象不增加 release。继续进入嵌套 module 时校验父原合同与父公开选择，隐藏槽不能由子挂载重新公开。

Review 等选择性公开用 `selectSurface(source, { views: ['facts'], actions: ['save'] })` 后放进 composition；省略类别不公开。模块仍对自己的原 Surface Expose，框架完成选择映射。移除再加入是新绑定，旧捕获 Action 拒绝新调用；已开始执行沿原 owner 完成。

最后 `implementProgram(Inventory, { Component })` 接宿主组件。业务输入解码结果 `ProgramParsedInput` 和提交的 `ProgramInput` 可以不同；不要把单向 transform 输出重新当 raw parse。远程 View/Action 输出需真实可逆 wire codec，不能承诺任意 Date transform 都可反向 encode。
