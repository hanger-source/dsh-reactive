# 编程模型

程序以普通 TypeScript 值定义业务行为，以组件组合当前对话环境，以标准 RxJS 表达持续关系。组件的渲染结果是 Context、Offer、Follow、Skill 与公开出口；业务动作和新的协作等待在显式调用时发生。

运行于服务端或 Agent 宿主。应用不需要为这套模型建立前端页面，也不需要在业务模块中持有 Cordis Context 或原生 Agent。

## 定义业务行为

可复用消费者通过 core 的 `ProgramSessionClient<P>` 描述共同会话能力，直接接收本地或远程的人工、服务端口。视图、进展、原请求和人工问题继续返回各自原有的资源对象；同一模块可以用函数、Promise、RxJS 或 hooks 读取它们，框架不为跨宿主复用建立第二个会话。需要回答、前台认领或 Goal 控制的模块明确使用 `HumanProgramSessionClient<P>`。

只需要公开模块出口时使用 `SurfaceClient<S>`，根会话与 `collection(name).instance(key)` 共用视图、动作、Operation 槽和嵌套集合合同。业务代码用 `await client.resolveAction(...)` 和 `await action(...)` 消费两种时序；具体本地类型仍保留同步解析和受理，具体远程类型仍返回 Promise。`ProgramSessionClient` 的 Async 参数只描述这部分 Surface 时序；读取或附着业务 Operation 不会变成一次 Action。

会话的 `requestStatus(requestId, { signal })` 属于 `ProgramSessionClient`，`pause({ signal })` 和 `cancel({ signal })` 属于 `HumanProgramSessionClient`。共同回执类型从 core 导入：ProgramGoalControl 提供 Goal 的 id、revision、phase，本地 pause 仍可返回完整原生 GoalView；ProgramCancelResult 的 canceled:true 表示原生处理已 idle 并完成持久 flush，不表示外部业务 Operation 已取消。ProgramRequestStatus 核对原生日志和投影，冷读取不激活 Agent，不能拿客户端缓存中的缺项证明 absent。取消信号和句柄退出只结束本次回执等待，已取消的调用不启动控制，已经受理的控制继续由原生 owner 结算。服务参与者可读取请求状态，不能控制 Goal 或答复人工问题。带 carrier 重连、分页与游标的 openEvents 继续由远程 SDK 提供。

本地服务通过 `host.service(actorId)` 取得固定身份的应用视图，再用 create、open、resume 或 completeCreation 取得服务会话；省略 actorId 时明确使用宿主配置的 serviceActorId。视图借用同一应用 owner，没有独立执行器或销毁权。逐调用选项不能改写服务 actor，输入、Action、Operation、队列编辑和文件上传保持同一来源。远程服务使用 `createServiceProgramClient` 或 `bindServiceProgramClient`，角色与 actor 仍由 Gateway 的认证映射签发，客户端工厂只选择所需能力，不能授予身份。

`ProgramSession` 是后台组件收到的会话事实与输入接口，`ProgramSessionClient` 是外部消费者的能力接口。两者不互相伪造执行身份。消费者释放观察后，已受理的调用和业务 Operation 仍由原 owner 持有。

公开发现也使用同一资源模型：`session.actionManifest` 是当前 Action 绑定目录，`session.collection(name).instances` 是实际成员 key 目录，嵌套集合提供相同入口。两者可 read、peek、观察 observations$、参与 combineResources 或交给 useResource；actionManifest$/instances$ 是对应值流。它们借用原有查询与 SDK owner，不产生另一张目录表。父模块撤下时嵌套目录进入 error，同地址恢复后继续更新；SDK 退出撤销读侧。目录只描述当前声明，调用仍必须经过原绑定与权限核验。

merchantService 是应用已有的业务 owner，负责权限、修订、真实副作用与正式回执。共享合同可以由宿主、CLI 和远程消费者共同导入；实现只在宿主装配。

```ts
import { defineAction, defineOperation, z } from '@hanger-source/dsh-reactive';

const Preparation = defineOperation({
  kind: 'merchant.preparation',
  version: 1,
  command: z.object({
    merchantId: z.string(), commandId: z.string(),
  }).strict(),
  reference: z.object({
    merchantId: z.string(), operationId: z.string(),
  }).strict(),
  state: z.discriminatedUnion('status', [
    z.object({ status: z.literal('running') }).strict(),
    z.object({
      status: z.literal('awaiting-command'),
      checkpointId: z.string(), reason: z.string(),
    }).strict(),
    z.object({
      status: z.literal('complete'), artifactId: z.string(),
    }).strict(),
    z.object({
      status: z.literal('failed'), reason: z.string(),
    }).strict(),
  ]),
  settled: state =>
    state.status === 'complete' || state.status === 'failed'
      ? state : undefined,
  feedback: z.object({
    kind: z.enum(['observation-required', 'phase-finished', 'settled']),
    detail: z.string(),
  }).strict(),
});

const PrepareMerchant = defineAction({
  name: 'prepare_merchant',
  description: '提交商家的正式准备操作',
  scope: z.object({ merchantId: z.string() }).strict(),
  input: z.object({ expectedRevision: z.number().int() }).strict(),
  output: Preparation.reference,
  operation: {
    definition: Preparation,
    command: (_input, scope, origin) => ({
      merchantId: scope.merchantId,
      commandId: origin.invocationId,
    }),
    reference: receipt => receipt,
  },
  observe: receipt => ({ operationId: receipt.operationId }),
});
```

宿主将合同接到真实服务：

```ts
import { implementAction, implementOperation } from '@hanger-source/dsh-reactive';

const PreparationOwner = implementOperation(Preparation, {
  locate: (command, access) =>
    merchantService.locatePreparation(command, access),
  observe: (reference, access) =>
    merchantService.observePreparation(reference, access),
  registerFollow: (command, link, access) =>
    merchantService.registerPreparationFollow(command, link, access),
  readFeedback: (command, link, access) =>
    merchantService.readPreparationFeedback(command, link, access),
});

const prepareMerchant = implementAction(PrepareMerchant, {
  operationOwner: PreparationOwner,
  execute: (input, call) => merchantService.prepare({
    merchantId: call.scope.merchantId,
    expectedRevision: input.expectedRevision,
    commandId: call.invocationId,
    source: call.source,
    signal: call.signal,
  }),
});
```

defineAction 和 implementAction 都不执行业务；useAction(prepareMerchant, targetFactory, dependencies) 只绑定不可变目标。局部实现可直接写 `implementAction({ name, description, scope, input, output, execute })`，需要共享的纯合同继续单独定义。显式调用时，input 在唯一入口解析，execute 取得 schema output，返回值经过 output schema 解析。模型参数描述转换前输入，结果描述转换后输出；无法投影的 schema 明确拒绝装配。

组件内部可以用 `useAction(() => ({ name, description, scope, target, input, output, execute }), dependencies)` 同时描述局部实现与目标。factory 只构造选项，Program 负责正式绑定和来源接入；同一 owner 和 dependencies 保留同一绑定，目标变化取得新绑定，旧调用保留原目标。Action 名称是明确的能力身份，写在该处一次；框架不根据组件位置生成业务名称。普通函数可以调用返回的同一个 Action，并消费其 Promise、Invocation 资源和 Observable。

局部 Action 的准入与退出沿用正式 Action 机制：组件撤下后不再接受新调用，已受理业务仍归原 owner。Resource 的失效、Reaction 的显式运行与 Action 的副作用各有独立语义，构造和渲染不启动它们。声明 Operation 时，局部选项也必须提供与合同完全匹配的 operationOwner；kind、version、命令与持久恢复责任仍由业务明确声明。

本例要求业务服务真正支持按 commandId 定位受理事实，且同一命令永远指向同一操作。invocationId 提供关联身份，本身不保证业务去重。registerPreparationFollow 必须在业务的可靠设施中保存关联并安排独立通知；普通 Observable 订阅不满足这项合同。

Action 绑定保存已解析目标的自有冻结快照。它不会在执行途中改用页面最新选择。原生工具取得真实 callId、rootCallId、parentCallId、输入关联和适用 Goal；独立参与者调用有自己的身份，不从环境中借用当前 Goal 权限。

### 共同业务资格

绑定可以接收 `eligibility: ResourceSource<ActionEligibility>`，其中 `ActionEligibility` 是 `{ allowed: true } | { allowed: false, reason: string }`。`bindAction` 在绑定选项中传入；`useAction(implementation, target, dependencies, { eligibility })` 使用第四个参数，局部 factory 则在返回选项中提供 `eligibility`。绑定通过 `action.eligibility` 暴露只读 `ResourceReader`，保留资源 Observation 的 pending、ready（允许或拒绝）和 error，不把拒绝压成资源错误。

`Offer` 自动读取绑定的资格；`enabled` 仍是模型声明开关，不是执行授权。`Context` 可传入同一资格源或 `action.eligibility`：拒绝时撤下声明，pending/error 时必需 Context 阻塞准备，`required={false}` 则撤下该声明，其他模块继续。`Expose` 可以稳定暴露 Action 合同，资格变化不要求撤下公开合同。共同前提不能替代对原始输入、真实调用者、目标和事务的逐次 `admit` 核验。

## 组合当前现场

领域模块定义自己的局部 Surface。应用用同一份模块合同组合多个实例，框架产生根 Surface、精确 SDK 路径和带身份的挂载点。

```tsx
import { Context, Expose, Module, Offer, useAction, useResource } from '@hanger-source/dsh-reactive/program';
import { composeSurface, defineSurface, resourceView, selectSurface, z } from '@hanger-source/dsh-reactive';

const Merchant = z.object({
  merchantId: z.string(), revision: z.number(), canPrepare: z.boolean(),
});
const merchantSurface = defineSurface({
  views: { facts: resourceView(Merchant) },
  actions: { prepare: PrepareMerchant },
  operations: { preparation: Preparation },
});

const merchants = composeSurface({
  a: merchantSurface,
  b: merchantSurface,
});
const surface = merchants.surface;

function MerchantWork({ merchantId }: { merchantId: string }) {
  const facts = useResource(() => merchantService.facts(merchantId), [merchantId]);
  const prepare = useAction(
    prepareMerchant, () => ({ merchantId }), [merchantId],
  );
  return <>
    <Context name="facts" source={facts} render={value => value} />
    <Offer action={prepare}
      enabled={facts.state.status === 'ready' && facts.state.value.canPrepare}
      follow={{ when: 'feedback', scope: { merchantId } }} />
    <Expose surface={merchantSurface} value={{
      views: { facts },
      actions: { prepare },
    }} />
  </>;
}

function Merchants() {
  return <>
    <Module mount={merchants.mounts.a}>
      <MerchantWork merchantId="merchant-a" />
    </Module>
    <Module mount={merchants.mounts.b}>
      <MerchantWork merchantId="merchant-b" />
    </Module>
  </>;
}

const review = composeSurface({
  a: selectSurface(merchantSurface, { views: ['facts'] }),
});
```

模块内部只写局部 Context、Follow 名称，Offer 默认使用 Action 合同名。Module 按显式路径隔离原生名字，并将本地 Expose 映射到根出口。SDK 使用 `a.facts`、`a.prepare`、`b.facts` 等精确路径，不重复列根槽、传 names 或编写 Outlet。挂载点不是普通路径字符串；伪造、错误根合同和重复挂载均拒绝。

完整模块也可以自带会话级依赖。多个模块声明相同 name 或精确 directory 的 Skill 时共享贡献；directory 必须是绝对路径，宿主通过 path.normalize 做词法规范化，合并规范化后相同的目录。cwd 仅参与原生命名 Skill 方法的解析。一个模块退出后，其他贡献继续持有这份依赖，最后一项退出才撤下本声明拥有的 provider 与上下文，不反向卸载外部 provider；应用无须在中央重新列出模块依赖。

NativeTools 按真实全局工具名逐项共享，前提是 enabled 完全一致（省略视为 true），且 admit 是同一函数引用或均未提供。复用模块应从装配处借入同一政策函数；分别创建的函数即使代码相同也不等价。参与条件或政策不同明确失败，不自动合并权限。最后一项退出撤下本声明贡献及其政策，原生内建工具仍遵循各自独立的基础可用性规则。与实际提交的 Offer、Follow 同名仍冲突；多个 HumanInput 贡献须共享同一个必需的 admitResponder 函数引用。

外部业务模块用 `session.module(merchants.mounts.a)` 取得局部 SurfaceClient，随后直接 `readView('facts', { signal })`、`resolveAction('prepare', target)`，不接收根槽名或拼接路径。module 只重定位既有入口，视图资源仍是根会话中的同一个对象，调用选项、本地同步或远程异步时序及原 owner 都保留，不产生独立 release。集合成员和局部客户端也支持 module；嵌套挂载必须属于当前父模块的原合同。

`selectSurface` 选择公开类别及槽，未列类别不公开。相同 MerchantWork 可以放到 review.mounts.a 下，只公开事实。公开选择不撤下模型工具，也不代替业务授权。

挂载点的 surface 保留组件实现使用的原合同，selected 表达这次公开子集；`client.module(mount).surface` 是 selected。局部读取继续受父模块已经公开的子集约束，不能换一个更宽的子挂载恢复隐藏的视图、动作或 Operation；错误父合同和不可见槽都明确拒绝。

`ResourceSource<T>` 接受标准 `Observable<Observation<T>>` 或提供同一 `observations$` 的对象，不要求对象另有 read、peek 或 subscribe。资源实例、SDK 资源和 `useResource` 返回的轻量对象都满足这个合同。`useResource(source)` 读取当前状态，`Context source={source}` 通过同一 hook 贡献现场，`Expose` 的 `resourceView` 槽直接借入这个来源。资源对象保留原观察流与原 owner；公开视图按原观察流识别来源，对象与其流之间切换不重连。

`resourceView(Merchant)` 明确消费资源状态：pending 使当前读取失效，error 保留为读取失败，只有 ready 的领域值才编码。普通 `view(schema)` 消费值事件，不按业务字段猜状态；已有单次 Observation 仍可交给 `Context observation`。

跨模块关系直接组合 ResourceSource，两种来源可以混用。`combineResources`、`switchResource` 和 `shareResource` 从对象的同一 observations$ 消费，保留解析后的领域值与原 owner，不调用 read/peek 或建立另一份业务状态。资源算子保留失效语义，不把旧 ready 留在派生现场：

```ts
const prerequisites$ = combineResources({
  preparation: preparation$,
  knowledge: knowledge$,
  reception: reception$,
}).pipe(mapResource(({ preparation, knowledge, reception }) => ({
  canEnableReception:
    preparation.preparation?.status === 'prepared' &&
    knowledge.ready &&
    knowledge.snapshotReasonCode === null &&
    reception.canEnable,
})));
```

已有的 `prerequisites$` 可以直接 `useResource(prerequisites$)`，得到的对象继续传给 Context、其他业务 hook 或 Expose；构造上述管道时则使用 `useResource(() => combineResources(...).pipe(...), dependencies)` 或 `useShared(factory, dependencies)` 保持来源身份。依赖 pending/error 会使同一派生失效，跨源组合不提供后端事务。Offer 的 enabled 决定模型现场，正式权限、来源一致性和修订仍由业务 owner 在执行边界核验。

Context 默认是本步必需现场，pending 会等待，error 会使准备失败。独立模块可以显式声明 `<Context required={false} ... />`：未就绪时撤下该上下文，ready 后再贡献当前值；同源 Surface 仍保留 pending/error，相关 Offer 仍按依赖条件关闭。它允许其他可用模块继续参与，不回放旧值，也不代替错误处理或业务授权。

## 定义程序与工作环境

只在本地使用的 Program 可以保持无名。用于 createProgramClient、bindProgramClient、对应服务工厂或 retainNativeSession 的根 Program 应在纯合同文件中定义 name/version，供宿主和消费者共同导入；下面的定义是共享合同：

```tsx
import { defineApplication, defineEnvironment, defineProgram, z } from '@hanger-source/dsh-reactive';
import { implementProgram, useObservable } from '@hanger-source/dsh-reactive/program';

const program = defineProgram({
  name: 'merchant_delivery', version: 1,
  input: z.object({ merchantId: z.string() }).strict(),
  surface,
});

const implementation = implementProgram(program, {
  Component({ conversation }) {
    const observed = useObservable(() => conversation.inputs$, [conversation]);
    if (observed.status !== 'ready') return null;
    const ids = [...new Set(observed.value.map(input => input.scope.merchantId))];
    if (ids.length > 1) throw new Error('这项工作需要明确的商家范围');
    const merchantId = ids[0];
    return merchantId === undefined
      ? null
      : <Module mount={merchants.mounts.a}>
          <MerchantWork key={merchantId} merchantId={merchantId} />
        </Module>;
  },
});

const application = defineApplication({
  name: 'operations',
  admit: authorizeInput,
  environments: [defineEnvironment({
    id: 'delivery',
    instructions: '根据当前商家事实处理交付要求。',
    skills: ['/application/methods/delivery'],
    program: implementation,
  })],
});
```

实际消费的输入由原生 inbox 决定；一个步骤可以消费多个输入。本例的单商家限制是业务规则，不是框架自动选择最后一条输入。提交使用 input schema 的原始 JSON 输入，组件事实与事件消费解析后的领域输出；单向 transform 可以把 ISO 字符串解析为 Date，原始持久输入不被改写，也不要求反向编码。共享合同不导入组件或宿主服务。

组件 key 管理组件身份，Module 的显式挂载点管理声明命名与公开出口。同一路径重新挂载仍复用合同位置；业务目标变化按 useAction 的依赖更新绑定，旧捕获句柄不重定向。

## 来源、关系与执行边界

显式指定 owner 的 ResourceReader 在来源正常有限完成后仍保留有效性观察；来源连接已经释放，owner 撤销才使既有观察失效并完成。不要对远程 Invocation.activity$ 使用 lastValueFrom 等待业务完成：await invocation.result 取得业务结果，按活动 state 选择 returned/failed 则等待该观察达到终态。activity.read 继续只读取当前有效事实，不承诺与独立 result 回执同时到达。

所有流仍是标准 Observable。使用 map、combineLatest、switchMap、concatMap、mergeMap、catchError 等表达业务关系，不需要框架自定义的流操作符。

已有资源对象或观察流交给 workspace.shareResource(source)，同一个 observations$ 复用同一读侧，并把工作区的读取寿命带进外部组合。普通 workspace.share 继续表达标准流的连接与完成；不要借元素形状猜测资源 owner。mapResource 只做同步投影，thenable 返回进入资源 error；异步读取由 switchResource 与 observeResource 组合。

跨 Scope 派生同时跟踪来源的读取资格，owner 退出即拒绝旧缓存。Value.changes$ 以标准 error 结束；持续 callback 使用 subscribe(accept, onError?) 接收读取失败并观察后续分支恢复。peek 不跟踪业务值，仍受 owner 有效性约束。

- `useShared(source$)` 直接共享已有 Observable，`useShared(factory, dependencies)` memo 构造出的管道；RxJS 持有查询连接与重放值。最后一个查询观察退出时释放来源，已完成或失败的同一来源在 Program 活跃时保留终态。
- `useObservable(source$)` 通过 useSyncExternalStore 读取普通值流的当前 Observation，提交后才订阅；构造来源使用 `useObservable(factory, dependencies)`。退出停止此组件的查询观察。
- `useResource(source)` 直接消费 ResourceSource，或用 `useResource(factory, dependencies)` 选择、构造资源对象或观察流，返回 state 与 observations$。借入对象保留原观察流与 owner；裸观察流沿现有 hook 共享查询。来源错误、空完成或停在 pending 后完成都进入资源 error，业务空值仍是 ready 中的值。
- `useScope(setup, dependencies)` 在 React commit effect 中为组件建立真实子 Scope，并同步调用 setup；setup 可返回清理函数，清理函数可异步。组件退出或依赖变化立即停止该关系，StrictMode 再次 setup 时建立新 Scope，未提交的 render 不产生关系。传入的 Scope 只授予该关系的销毁权，不授予共享 Program 或 Workspace 的销毁权。组件局部持续关系可在 setup 中用 `createReaction(scope, options)` 创建并显式 `start()`，与资源、标准 RxJS 和业务 hook 组合。
- `useResourceInstance(implementation, params)` 取得当前 Program 共享的正式资源实例。普通函数通过它 read、invalidate 或订阅，组件通过 `useResource(instance)` 观察，同一实现与解析参数只持有一个实例；直接 `useResource(implementation, params)` 也连接这个实例。
- `useReaction(implementation, params)` 取得当前 Program 共享的持续反应，`start()` 才开始感知和执行。同实例不能同时 start 两次；默认运行由实例 Scope 持有，组件退出不会停止；在 `useScope` 的 setup 中对已有 Program 或 Workspace 实例调用 `reaction.start({ scope })`，将同一 run 同时交给实例 Scope 和组件 Scope，任一退出即停止。程序失败或退出会停止查询、持续感知及尚未开始的排队工作，已开始的裸 Promise 继续由原 run 持有至实际结算；期间状态为 draining，settled 尚未完成，不能再次 start。events$ 只交付 RxJS 选中的成功结果，失选的迟到失败仍由该 run 报告。
- `start({ scope })` 在 observe、operator 和 execute 之前登记运行，两个 owner 均须处于可用状态；传入实例自身 Scope 时只登记一次。各自的 `closed` 都等待该 run 真正 settled。不要在启动后用 `scope.own` 补登记。hook 不自动启动；同一实例在 stop 后仍 draining 时再次 start 报 `REACTIVE_REACTION_ALREADY_RUNNING`，StrictMode 或依赖变化不会自动重启、延后或复活运行。需要每个组件代际独立运行时，在 setup 中创建新的 `createReaction(scope, options).start()`。业务 execute 不得等待自己 owner 的 `closed`，否则会与 owner 等待 run 形成自等。失败由 run 的 failed 状态和 resolve 的 settled 表达，不改变 Scope.closed 的失败语义。
- 局部资源使用 `useResourceInstance(() => ({ value, read, invalidations? }), dependencies)`，或同一选项的 observe 分支；局部反应使用 `useReaction(() => ({ event, result, observe, operator, execute }), dependencies)`。factory 只纯构造 createResource/createReaction 已有选项，React dependencies 决定组件内身份；同依赖重渲染保留实例，不同组件分别拥有局部身份。Program 统一持有这些正式实例，组件退出不销毁已交给普通函数的对象或停止显式运行。依赖改变取得新对象，旧实例不重定向；需要跨组件共享时，使用现有 implementation 与 params 合同入口。
- `combineResources({ a: a$, b: b$ })` 组合具名资源，任一 error 保留为 error，尚有 pending 则不能产生 ready；全部有效才给出具名领域值。
- `shareResource(source, { signal })` 将 ResourceSource 提供为稳定只读资源，原对象无需先拆 observations$。普通函数、Promise、peek、subscribe、values$、observations$ 与 `useResource(resource)` 共用 RxJS 的一份当前快照，不重复 schema 解析，不新建 Scope。构造不连接；有限来源的最终观察保留至最后消费者退出，之后显式读取再连接。signal 固定该资源的读取寿命，不能通过修改原 options 更换 owner。
- `mapResource(project)` 只派生有效值，失效和错误继续传播；同步投影错误进入同代 error，后续有效代次可以恢复。
- `switchResource(project, { dependencies: value => [value.id, value.revision] })` 根据当前有效依赖连接另一资源。依赖 tuple 按元素 `Object.is` 比较，连续 ready 的依赖相同就保留原查询；只选择实际影响查询的标量或稳定引用，不需要 JSON 编码或自行比较 pending/error。任何上游 pending/error 都立即退订旧查询并清除依赖身份，同一依赖恢复也会重新连接。selector 抛错变为当前 error，后续有效观察仍可恢复；新查询先进入 pending，迟到结果不能回写。tuple 在观察时被捕获，后续修改原数组不会改变已选身份。省略 dependencies 时，每次 ready 选择新来源。它只切换读取，不重发业务命令；共享仍由既有 owner 和 `useShared` 持有。
- `useRelation(source$)` 提交后持有已有 Observable，构造关系使用 `useRelation(factory, dependencies)`；未恢复的 RxJS error 进入程序失败边界。它保留普通流元素，不把业务值或 error Observation 猜成异常。异步组合返回 Promise 或 Observable 进入高阶操作符，不依赖 tap(async ...) 等待。
- `useBeforeStep(check, dependencies)` 登记幂等检查，原生步骤实际等待它的返回值。每次登记有独立身份，相同函数被多个组件使用也各自拥有退出寿命。依赖替换或组件退出会中止旧检查收到的 signal 并结束等待；准备过程重新核对当前检查集合，旧 Promise 迟到完成或失败不再作为当前准备结果。原生步骤取消或 Program 失败同样结束等待，仍有效的检查失败则继续传播。检查应使用信号读取真实依赖并等待其工作完成，不在检查中执行业务命令。
- `workspace.share(source$)` 把明确共享的连接交给工作区；应在订阅阶段连接，而非组件纯求值中启动来源。同一工作区与来源复用一个连接，会话或最后一个消费者退出不停止它；有限来源的终态只在工作区 owner 活跃时保留。owner 退出结束已有观察，之前捕获的流在迟到订阅时直接完成，不重放旧值或旧错误，也不重新连接来源。
- `workspace.resource(implementation, params)` 与 `workspace.reaction(implementation, params)` 把正式资源、反应绑定到工作区，跨该工作区的多个 Environment 和 Session 复用同一实例；参数仍按原合同解析，不另传名字或 Scope。资源首次观察才读取，反应显式 start。应用通过 application.workspace(id) 在没有 Session 时也能取得这个编程接点；组件 props.workspace 是同一对象。
- 局部业务直接调用 `workspace.resource({ value, read, invalidations? })` 或 observe 分支，以及 `workspace.reaction({ event, result, observe, operator, execute })`。选项与 Program 局部入口相同，实例统一归实际 Workspace Scope；每次调用构造独立实例，相同 name 或同一选项引用也不合并。保留返回对象即可组合为普通函数、Promise、Observable 和 hooks 共用的业务能力；在组件纯求值中借用已构造的对象，避免每次渲染重新构造。Session 退出不结束这些实例，Workspace owner 退出撤销读取、持续感知及未受理队列，已受理业务仍由原 owner 结算。

factory 纯构造来源或目标，依赖数组决定其身份。重复求值不应发起业务操作。普通 React state 和 Transition 仍遵循 React 调度；它们不自动获得模型准入或权限事实的同步保证。

React commit 提交完整声明，native prepare 等待当前依赖。每次异步等待后，宿主再次刷新并核验声明修订；变化则重取当前现场。commit 本身不代表资源 ready 或业务完成。

`Skill({ name })` 将选中的原生方法正文纳入步骤上下文。准备期间声明、Skill catalog 或当前会话工作目录变化，会取消本次旧查询并读取当前集合，不等待旧 provider 正文返回；迟到结果不会写入新上下文。取消查询不撤销共享 provider，也不影响其他会话的读取。当前有效方法不存在、不允许模型调用或读取失败仍拒绝准备；步骤取消和程序退出结束本次等待。


## 启动宿主

宿主明确选择服务发布身份、独立数据目录和模型；可复用模块携带自己的静态依赖。`deepseek` 使用官方 Messages 适配器，`piAi` 接入官方多协议 provider profile；业务组件不持有模型适配器。`nativeModel` 留给已有原生适配器的宿主。

```tsx
import { createRuntime, deepseek, implementNativeModule } from '@hanger-source/dsh-reactive-dsh';
import { createNativeTools } from '@hanger-source/dsh-reactive-dsh/tools';
import { implementProgram } from '@hanger-source/dsh-reactive/program';

function createMerchantModule() {
  const tools = createNativeTools({ service: 'provided' });
  return implementNativeModule(merchantSurface, {
    native: [tools], operations: [PreparationOwner],
    Component: MerchantWork,
  });
}

const configuredModel = deepseek({
  model: 'deepseek-flash',
  apiKey: resolveModelCredential,
  maxTokens: 4096,
  retryPolicy: { mode: 'normal', maxRetries: 3 },
});

const runtime = await createRuntime(() => {
  const merchant = createMerchantModule();
  const implementation = implementProgram(program, {
    modules: { merchant },
    Component({ modules }) {
      const Merchant = modules.merchant.Component;
      return <Merchant mount={merchants.mounts.a} merchantId="merchant-1" />;
    },
  });
  return defineApplication({ name: 'operations', admit: authorizeInput,
    environments: [defineEnvironment({ id: 'delivery',
      instructions: '根据当前商家事实处理交付要求。', program: implementation })] });
}, {
  home: '/application/data/operations',
  actorId: authenticatedActorId,
  serviceActorId: 'operations-publisher',
  model: configuredModel,
  compaction: { headroomTokens: 8192, maxTokens: 2048 },
});
```

Program 从注入的 `modules.merchant.Component` 渲染模块并传入业务 props 与 `mount`；模块内部自动取得局部映射。`implementModule(surface, { Component, modules?, operations? })` 可组合普通模块；其组件内部取得静态子模块，外部组件只需业务 props 与 mount。Program 列直接模块，依赖递归捕获；渲染未声明的模块硬失败。应用汇总所有 environment 与 delegation 程序的依赖，同一 owner 或 capability 原对象去重，不同 owner 的 kind/version 冲突在安装前失败。动态退出只撤声明和组件读侧，静态 owner 和 capability 属于 App。模块工厂每 App 创建新的原生能力实例，业务 owner 可显式共享。

构造模块不会启动原生 I/O 或业务执行，应用挂载安装能力并在 Host 退出时等待释放。已有宿主使用 `mountApplication(ctx, factory, { home })` 时，有原生能力才要求显式绝对 home；Runtime 使用自己的归一化 home。`assembly.native`、`assembly.operation` 仍供直接登记，`assembly.implementDelegation` 构造原生委托；`options.operations` 和 `RuntimeOptions.installNative` 继续供宿主选择共享 owner 与底座服务。

serviceActorId 是持久原工作反馈的发布主体，必须由业务权限体系明确授予所需能力。它不替代独立调用者的认证，也不能从业务正文取得。

`resolveModelCredential(signal)` 由可信宿主读取凭据，每次模型连接都可以重新解析；密钥不进入会话正文或上传复用索引。`piAi({ provider, model, profile })` 直接使用官方 `PiAiProviderProfile`，在 profile 中以 `apiKeyEnv` 引用凭据、配置实际协议和模型目录。

`deepseek` 的字符串模型沿用官方目录中的容量、模态和对话更新能力；未知模型字符串明确拒绝，不能把它猜成可看图的模型。自定义部署使用官方 `DeepSeekCatalogModel` 对象，声明实际容量和支持的输入，例如 `model: { id: 'deployment-vision', contextWindow: 32768, inputModalities: ['text', 'image'] }`。已知模型的对象形式只覆盖显式字段，其他能力仍由官方目录提供。容量放在该对象中；`maxTokens` 是请求默认输出配额，模型对象中的同名字段可以提供该模型的配额。

独立 Runtime 同时安装官方重试执行器、token meter、基本压缩器与图片卸载插件；它们由 Runtime 持有，每个会话的计量和持久事实彼此独立。普通重试只处理 provider policy 选中的模型请求失败，不重执已经完成的 Action。`normal` 有重试预算，`always` 持续到成功、取消或 Runtime 释放；取消退避会阻止下一次模型请求。

自动压缩读取实际路由的容量和输出配额，摘要默认使用同一路由，也可以通过 `compaction.summarizationProvider` 与 `summarizationModel` 成对指定。`compaction.maxTokens` 是摘要配额；`headroomTokens`、`retainRatio` 或 `retainTokens` 决定预留和保留预算，后两者互斥，`modelPolicies` 提供精确 provider/model 覆盖。官方默认 headroom 为 65536，较小模型须明确调整，保证容量大于请求输出配额与 headroom 之和。容量未知或预算无效时，原生压缩器记录警告并继续当前对话，不能据插件已安装推定压缩一定生效。`compaction.auto: false` 关闭自动压力压缩与上下文溢出恢复；已有计量、普通请求重试和图片卸载仍保留各自职责。自有 `nativeModel` 适配器应通过官方 `resolveModel()` 提供确切路由的 `context` 与请求默认输出配额。

图片数量、编码字节、请求预览和 Files 生命周期直接使用官方 `DeepSeekOptions` 配置。路由要求卸载时，原生 `image/offload` 选择具体输入 occurrence，后续请求改为带附件身份和可用只读路径的文本；原始附件仍保留。Files 上传复用索引位于该 Runtime 的 `home/llm-deepseek/files-v3.json`，不读取或改写个人 Home。

`createRuntime` 在自己的 home 装配原生附件存储、Commands、LocalCredentials、Connection 和 FileUploads，上传与正文提升使用这些真实服务。Connection 持有认证材料，不因这项装配自动启动 HTTP/WebSocket carrier。已有 dsh 宿主使用 `mountApplication` 时，基础设施层须先提供 `attachments`、`fileUploads` 及它们要求的 agents、commands、connection、credentials；应用挂载不会替已有宿主隐式安装另一份服务或选择个人凭据。

## 调用与观察

```ts
const workspace = await runtime.application.createWorkspace({
  directory: '/application/workspaces/merchant-a',
  title: '商家甲',
});
const conversation = await runtime.application.create('delivery', {
  workspaceId: workspace.workspaceId,
});
const observation = conversation.observeView('a.facts').subscribe(renderMerchant);

await conversation.submit({
  requestId,
  content: '核对当前交付现场',
  intent: 'question',
  delivery: 'followup',
  scope: { merchantId },
}, { actorId: authenticatedActorId });

const prepare = conversation.resolveAction('a.prepare', { merchantId });
const invocation = prepare(
  { expectedRevision: 7 },
  { actorId: authenticatedActorId },
);
invocation.activity$.subscribe(renderInvocation);
const currentActivity = await invocation.activity.read();
const receipt = await invocation.result;
```

工作区目录须由应用明确准备，`createWorkspace` 沿原生 registry 注册已存在的绝对目录，按 canonical path 复用身份。Environment 不绑定目录；同一个 delivery 场景可以用于其他工作区，同一工作区也可以运行应用声明的其他场景。`await runtime.application.describe()` 发现原生工作区及其中通过本应用身份核验的成员。创建恢复引用同时携带 environmentId、workspaceId 与 sessionId，恢复不会重新选目录或补授被移除的成员关系。

本地 `runtime.application.workspace(workspaceId)` 返回 `Workspace` 编程对象，类型也可从 `@hanger-source/dsh-reactive/workspaces` 导入。它与 createWorkspace 返回的可序列化登记信息分别承担作用域与描述责任，不把 Scope 传到远程客户端。原生工作区登记删除后，已有资源和反应撤销，旧对象不能重开来源；其他工作区继续运行。目录及 Session 历史仍由原生删除登记的语义保留。共享来源清理失败在删除已经提交后报告，宿主退出汇总失败并继续释放其余工作区。

`Submission.content` 接受字符串或有序 `PromptContentPart[]`。字符串只归一为一个 text part，保留原文；空白判定不会 trim 正文。普通输入必须显式选择 delivery：followup 在下一轮处理并唤醒，steer 在最近后续步骤处理并唤醒，inject 在最近后续步骤补充上下文，本次投递不请求唤醒。冷会话提交需要加载原生实例，已受理子工作仍可沿原授权恢复并产生模型活动；inject 不暂停或撤销既有 Goal 和业务工作。业务 intent 与投递方式分别保存，goal 输入还须有真实的非空文本目标。

```ts
const upload = conversation.uploadFile(fileBytes, { name: 'merchant-details.pdf' });
const uploaded = await upload.result;
await conversation.submit({
  requestId: 'merchant-details',
  intent: 'followup',
  delivery: 'inject',
  scope: { merchantId },
  content: [
    { type: 'text', text: '保留这份补充资料，等下次工作时一起核对。' },
    { type: 'file', receiptId: uploaded.receiptId },
  ],
});
```

`uploadFile` 接受 `Uint8Array | Blob` 和可选 name、signal，同步返回 `FileUploadInvocation`。它不是 thenable；`await upload.result` 取得官方 `FileUploadValue`，`await upload.wait({ signal })` 只等待本消费者所需的同一次回执。Uint8Array 在调用时复制，之后修改原数组不改变上传；Blob 异步读取被取消后，迟到字节不会再激活会话或开始上传。

方法的 options.signal 约束字节准备与分派前检查，同时作为 wait 的默认信号；分派后取消它或释放 Session、原生 UI 只结束 wait，不改写 result。原生 UI 退出也可以阻止尚未分派的准备。upload.activity 是同源有限 ResourceReader，activity$ 是其值流，可 read、peek、组合 observations$ 或交给 useResource；running 只表示已经分派，不证明存储已受理。调用失败仍是 ready 中的 failed 事实，终态可在 Session 或 UI 退出后晚读。source 为 `{ kind: 'file-upload', sessionId }`，id 只关联本次内存调用，不承担持久恢复或幂等去重。

上传不投递模型输入。receipt 只属于接收它的真实原生 Session 实例，不能用于另一会话；未提交的 receipt 在原 Agent 关闭或进程退出后失效。`release()` 不等于关闭 Agent。客户端不能把持久 FileAttachmentRef 直接塞入 Submission；已有 canonical base64 的正式 transport 使用 Handle 的 `uploadEncodedFile(request, options)`，仍由原生服务验证编码及签发 receipt。远程 SDK 或 Connection 物理退出导致回执无法确认时，result 可以拒绝为 `FileUploadReceiptUnknownError`；这不证明服务器没有文件，框架不自动重传。[上传调用迁移说明](migration-file-upload-invocations.md) 给出完整消费方式。

应用的 `admit` 接受 `AdmittedSubmission`：text 保持原文，图片与文件已经提升为原生持久附件引用。首次投递和模型消费前复核使用这份相同形状，策略不从持久引用猜回图片字节或上传 receipt。scope 仍是原始 JSON，不是 Program input 的领域输出；actor 由可信宿主或远程认证签发。

普通 SDK 与 AI 工具进入同一 Action。SDK 的身份来自可信宿主配置或认证入口；组件内直接调用绑定 Action 时，宿主使用显式配置的 serviceActorId，缺少身份则拒绝。独立调用没有继承当前模型输入或 Goal，不能凭当前环境执行 wait 或 completeGoal。

`resolveAction(name, expectedScope)` 只读取得当前公开的具体绑定，使用原合同解析调用者明确期望的业务目标。返回的可调用值保留 `bindingId`、解析后的只读 `scope` 和纯 `definition`；它不会随组件改选而重定向。目标不符、绑定被撤下或换成另一个实例时明确失败；同一目标重新挂载也形成新的绑定。解析和调用都会冲刷当前声明，异步准入结束、业务 execute 开始之前再复核。已经进入 execute 的调用继续由原 owner 结算。

绑定身份只在当前程序实例中识别实际对象，不是权限凭据或持久引用。进程恢复后重新解析；业务恢复使用原 Operation command/reference。开发者不需要为了防止错投而把 scope 再复制进 Action input。

本地与远程 SDK 统一以 `CancellationOptions` 接收 `{ signal }`，例如 `readView(name, options)`、`resolveAction(name, scope, options)`、`submit(input, options)` 和 `prepare(input, options)`。共享消费者可以对两端统一使用 `await`。本地的可信参与入口还接受 `SubmitOptions.actorId`；远程身份由认证载体决定，调用选项不提供 actor。远程解析和受理等待的取消不会撤销已经受理的业务；直接调用中的信号交由执行 owner 处理，长期 Operation 的取消仍由业务合同决定。

Action Invocation 的 activity 资源提供本次调用当前有效事实，read 不等待业务完成；可直接交给 useResource 或组合 observations$，activity$ 引用同一对象的 values$。本地 pending/running 是已经知道的调用状态；业务 failed 也是 ready 的调用事实，观察来源不可用才进入资源 error。纯本地不可变事实沿原来源有限结束，晚读取仍取得终态，组件和绑定退出不撤销它。远程 Action 来源在 returned/failed 后释放连接，资源的有效性观察仍由 SDK 持有；SDK 退出撤销资源，不重新调用或取消 Host 业务。

Invocation 的 returned 是业务函数返回；原生工具后置策略的最终结果由原生事件表达。observe 是纯投影，原生最终展示可能再次对最终结果投影，不会重新解析或执行业务。

模块可以直接对同一个绑定的调用作出反应，输入和结果保持原合同类型：

```tsx
import { from, mergeMap, tap } from 'rxjs';
import { useRelation } from '@hanger-source/dsh-reactive/program';
import { type BoundAction, type ActionOutput } from '@hanger-source/dsh-reactive';

function PreparationReactions({ prepare, onAccepted }: {
  prepare: BoundAction<typeof PrepareMerchant, true>;
  onAccepted(receipt: ActionOutput<typeof PrepareMerchant>): void;
}) {
  useRelation(() => prepare.invocations$.pipe(
    mergeMap(invocation => from(invocation.result).pipe(tap(onAccepted))),
  ), [prepare, onAccepted]);
  return null;
}
```

`invocations$` 只发出订阅后这个绑定新建立的调用，不重放或执行命令。AI、程序和 SDK 都进入同一绑定，观察者取得该次调用原来的 Invocation。单次失败留在该调用，outer 继续存在；示例没有恢复 inner 的失败，它会按 `useRelation` 的错误边界处理。需要继续处理其他调用时，在 inner 中用标准 `catchError` 表达业务处理。关系组件应属于仍需处理结果的业务模块，不能因为暂时隐藏 Offer 就一起退出。

views 提供领域值事件；observeView 保留当前 pending/ready/error，readView 等待当前领域值。跨进程输出通过 Zod encode 转成 JSON wire 值，codec 可表达 string 与 Date 等双向合同。单向 transform 无法正确编码时拒绝，不临时猜一个编码方案。

句柄 release 只结束该观察者。原生 Agent 活着时，会话程序继续存在；已进入业务 execute 的调用不因声明退出或页面断开而撤销。原生调用信号、显式调用信号及业务 owner 分别决定它们有权取消的工作。

## 按业务工作观察进展

应用定义工作键，并将已经取得的会话作为显式来源。键可以是 JSON 值；同一商户可以有多个工作，同一个工作也可以涉及多个会话。下面约定输入的原始 scope 含 `workId`：

```ts
import { createWorkSelector, type WorkSource } from '@hanger-source/dsh-reactive';

const workSelector = createWorkSelector({
  identify(origin) {
    const scope = origin.scope;
    if (scope === null || typeof scope !== 'object' || Array.isArray(scope) || !('workId' in scope)) return undefined;
    return typeof scope.workId === 'string' ? scope.workId : undefined;
  },
});

function observeWork(workId: string, sessions: readonly WorkSource[], signal: AbortSignal) {
  const work = workSelector.observe(workId, sessions, { signal });
  const subscription = work.state$.subscribe(observation => {
    if (observation.status === 'ready') {
      for (const session of observation.value.sessions) {
        for (const call of session.calls) console.log(call.value, call.matchedInputs);
        for (const condition of session.conditions) console.log(condition);
      }
    }
  });
  return { work, subscription };
}
```

本地和远程、人工和服务会话均可作为 `WorkSource`，来源提供 identity、progress、human、rawEvents$ 与 progressFrames$。来源列表必须非空，sessionId 不得重复。`select(key, snapshots)` 同步纯投影已取得的快照；`observe(key, sources, options?)` 提供共享的 `state`、同一观察的 `state$`、新事件 `events$` 和助手帧 `progressFrames$`。识别使用原始 JSON scope，不执行任何 Program 的输入 transform。选择器不发现或打开会话、不启动 Agent，也不授予其他会话的 Goal 或人工答复权限。

`WorkHandle.sessions` 为每个显式来源提供 WorkSessionHandle，其 identity、state、state$、events$ 和 progressFrames$ 对应该来源；通过 `sessions[i].state.read()` 读取局部 ResourceReader；整体资源只有全部来源 ready 才 ready，某来源退出后其他局部资源仍可继续，不以其他来源兜底，也不授予权限。每个 WorkSessionSnapshot 的 operationFollows 保留匹配事实及 resultRequest；条件包括 operation.not-started、operation.delivery-prepared、continuation.unconsumed 和 goal.round-limit。

每项保留完整 `value` 和解释命中依据的 `matchedInputs`；一次调用或回复可能因多条输入同时属于多个工作。调用保留正式 start 事件序号 `callSequence`，回复保留 `messageId` 和完整 inputs；provider 的 callId 可跨轮复用，不能用它合并不同发生。matchedInputs 只解释观察分组，不裁剪事实或授予续接权限。声明是持久请求事实，可在尚无 messageId 时命中，但它不是已受理的 InputReference。缺少输入关联证据的回复不归组。条件表达暂停、阻塞、等待、待答、调用失败及原续接资格，不能据此判断整个工作忙碌或完成。业务 Operation 阶段与资源错误可用现有 RxJS 资源组合表达。

## 读取原生日志

本地、远程的人工与服务会话，以及后台 Program 的 conversation，共用 `session.journal`。`page({ throughSeq, beforeSeq, maxMessages, turnWindow }, { signal })` 读取包含式原生水位内的消息对齐历史；`follow(window, { signal })` 返回一次独立的异步连接，先交付 `opened` 的 cursor 和 page，之后交付该水位后的原生 entry。这些持久事件与只通知新事实的 `events$` 各有责任，不从 ConversationEvent 重建历史。

```ts
const journal = session.journal;
for await (const frame of journal.follow({ maxMessages: 50 }, { signal })) {
  if (frame.type === 'opened') {
    consumeInitialHistory(frame.page, frame.cursor);
  } else {
    consumeNativeEntry(frame.entry);
  }
}
```

RxJS 消费使用 `defer(() => from(journal.follow(window, { signal })))`，可继续组合或交给 `useObservable`。取消或退出连接只结束本次观察；Session release 撤销该句柄的读取资格，不取消已经受理的模型、Action、子 Agent 或业务 Operation。

历史的消息分页使用正式 Controller.page；开场水位使用冷安全 SessionQuery observation，读取不会 promote Agent。子会话沿 Host 已核验的原生父子地址读取；冷会话挂载后的追加从同一个 Session 的原生有序日志接合。Host 只借用自己 Context 中已经安装的 Controller 与 Query；缺少服务时报告 `SessionJournalUnavailableError`。`mountProgramGateway` 的正式装配提供这两项服务，独立 Runtime 仍按需要安装自己的原生能力。已选提供者退出或被替换会撤销原读侧，不自动换源。

带 carrier 重连、缺口修补和 prepend 的 `openEvents` 由远程 SDK 的官方 RemoteJournalStream 承担，并消费同一个 journal。`openEvents(options, request, { signal })` 的信号只取消这次开场等待；取得的 journal 继续由其自身与原 Session owner 管理。

## 消费会话文件

Program 的 conversation、本地与远程 Session SDK 提供共同 `files` 入口。宿主通过 `HostOptions.files` 或 `RuntimeOptions.files` 借入已安装的 NativeFiles；服务仍由宿主显式安装，不因读取文件启动模型或恢复 Agent。

```ts
const page = session.files.text('notes.txt', { offset: 0, limit: 100 });
const current = await page.read(signal);
const metadata = session.files.metadata('notes.txt');
const contents = session.files.directory('.');
// 同一对象的 observations$ 保留 pending/error/ready，values$ 交给普通 RxJS。
const combined = combineResources({ page, metadata, contents });
```

同一来源、路径与窗口复用资源对象，业务 hooks 直接 `useResource(page)`，派生沿 `mapResource` 或 `combineResources`。显式 `page.invalidate()` 重新取得权威切面，不重发写入。单次查询使用 `readText`、`readBytes`、`stat`、`list`；原始变化使用 `watch`，退出时等待其 `dispose()` 或 `closed`。字节不缓存为共享资源。路径与 `readBytes` 的 baseFile 都限制在实际会话 cwd；越界明确失败。

资源 ready 是最近一次权威读取的切面，更新依赖原生失效通知。文件使用单个父目录观察，每个目录 change 帧重新 stat 目标并触发资源重读，允许重复，兄弟文件活动也可能触发额外重读。通知可能合并或缺失，Linux 上裸 writeFile 恢复也可能没有通知，已知裸写入后须显式 `page.invalidate()`。固定 rc.3 的本地 FS 使用 chokidar，同路径 change 在 50ms 内可能直接被抑制且没有尾通知，因此短窗口连续正文写入后不保证自动追上最终内容。已知写入完成的业务应显式 `page.invalidate()`；需要立即核验真实内容时使用单次查询。框架不增加轮询或另一套文件事实，也不把 watch 当成完整变更日志。

SDK release 撤观察资格，Host 关闭等待本应用读侧的实际 I/O 和 unwatch；这与写文件业务的完成分开。远程读取沿认证 Gateway 核资格，重连先失效再等新 watch ready 重读。可选原生 UI 用 `native.resource(native.files.text(path))` 接正式资源，退出时 `await native.dispose()`；原 SDK 的销毁仍归其 owner。

## 观察一次请求

输入受理后，沿原 requestId 观察实际进展。本地句柄和远程 SDK 使用相同的请求句柄：

```ts
const accepted = await conversation.submit({
  requestId,
  content: '核对当前交付现场',
  intent: 'question',
  delivery: 'followup',
  scope: { merchantId },
});
const request = conversation.request(accepted.requestId);
const stateSubscription = request.state$.subscribe(renderRequest);
const eventSubscription = request.events$.subscribe(onRequestEvent);

// 结束本消费者的观察，已经受理的工作仍归原 owner。
stateSubscription.unsubscribe();
eventSubscription.unsubscribe();
```

state$ 是标准 `Observable<Observation<RequestSnapshot>>`。pending/error 表示读取本身的状态；ready 中的 activity 为 null 表示当前原生事实里没有该请求。source 区分 live 与 durable，execution 是会话整体的运行状态。activity 提供受理、消费、本轮结束、工具和持久回复，attempts 提供与这条请求确切关联的当前流式前缀。

events$ 只交付订阅后发生的相关输入、工具、回复、等待与可确切归属的错误。一个步骤可以共同消费多个输入，这些请求会看到同一项活动；工具还保留 rootCallId 与 parentCallId。请求的 settled 仅表示关联轮次结束，后续 Goal 轮次仍可继续处理原工作。它不会结束观察，也不承诺一条请求独占一份最终答案。

`conversation.progress$` 提供整个会话的活动和活跃 attempt 快照；`progressFrames$` 提供原生 start/chunk/end 增量。公开的 `AssistantStreamRecord` 与 `expandAssistantStream`、`assembleAssistantStream`、`joinAssistantStreamText` 等 reader 直接来自 dsh。按内容需要选择官方 reader：joinAssistantStreamText 拼接 text-delta；需要完整 block 语义时使用 assembleAssistantStream。组件、页面和 CLI 可用标准 RxJS 派生自己的呈现，不需要重新拼请求归属或复制原生流解析。

开场与重连快照不冒充新事件，历史记录从 journal 读取。未提交的流式片段属于当前活跃进程，进程退出后不能从持久会话中恢复；冷观察不会复活草稿或加载 Agent。

## 原生反馈与持久续接

conversation.activity$、goal$、waits$ 提供当前状态；events$ 提供订阅后的新事实。冷打开不会重放历史事件重新执行业务。

Offer.follow 将一次工具调用、原业务命令和适用 Goal 修订连接起来。框架先持久保存意图，等待 owner 的可靠关联 ACK，再持久登记原生等待，之后才进入 execute。原函数返回时保存正式操作引用；进程恢复只定位原命令，不重放 execute。

业务通知使用宿主的可信入口：

```ts
const result = await runtime.application.notifyOperation(
  sessionId, followId, { actorId: authenticatedNotifierId },
);
```

调用方不能自报 ready、Goal 或操作结果。框架沿已保存的关联读取 owner 当前事实。unknown 表示无法确认受理，pending 表示条件尚未满足，二者都不能作为 outbox 的投递完成 ACK。accepted 只确认原生持久受理；它不代表模型已消费或业务目标完成。

settled 判断整个操作的终态，feedback schema 描述需要交回协作过程的业务数据。readFeedback 按确切 link 读取该次登记产生的持久节点 `{ id, payload }`；节点 id 由 owner 签发，payload 使用这份 schema。读取、权限错误抛出，不能包装成业务拒绝。awaiting-command 可以交回反馈，同时 Operation.settled() 仍继续等待。

一次观察读取失败也可以成为真实协作节点，并不说明原任务失败。owner 按 link.id 幂等登记新的观察尝试，保留待交回节点；迟到的旧注册不能替换新观察，也不重新执行业务任务。两次观察即使返回相同正文，仍可以有各自稳定的节点身份。框架不比较正文推断节点，也不要求业务建立一套全局事件游标。

owner 负责业务订阅及可靠通知，不接管原生 Goal、wait 或会话恢复。如果已有不可变业务事件，可按 after 确定首个后继节点，无需再保存一套参与选点状态；可靠通知只需沿原 sessionId 和 followId 请求框架核验，原工作的继续条件由框架处理。

每条关联对应一次独立等待和一次固定投递。同 link 的重复通知确认原投递；后续协作使用新的参与身份。

## 沿已有操作继续协作

`useOperation` 绑定原业务命令，只提供读取。`Follow` 声明一个供 AI 调用的等待能力：工具调用发生时，才沿当前调用的输入和 Goal 修订登记新参与。它不调用 Action.execute，也不把旧回执包装成一个新的业务动作。

```tsx
function PreparationCollaboration({ command, merchantId }: {
  command: z.input<typeof Preparation.command>;
  merchantId: string;
}) {
  const preparation = useOperation(
    PreparationOwner, () => command,
    [command.merchantId, command.commandId],
  );
  return <>
    <Follow name="await_preparation"
      description="沿原准备操作等待下一次反馈。首次 after 为 null；再次等待时传入已处理节点的 id。"
      operation={preparation} when="feedback"
      scope={{ merchantId }} />
    <Expose surface={merchantSurface} value={{ operations: { preparation } }} />
  </>;
}
```

这里 command 来自业务已经保存的命令身份，不能从 operationId 猜测。组件在对应 Module 内使用局部合同，框架把 Follow 名称与 Operation 出口映射到该实例，不需要 Outlet。

AI 处理 F1 后，调用 await_preparation，并明确传入 `{ after: F1.id }`，建立 L2。owner 按 L2 读取后继节点，F2 才有资格交回 L2；旧 L1 通知仍确认原 F1。after 是业务条件，新的 Goal 续接权只来自这次真实工具调用。恢复已登记 L2 不重执行原业务，没有登记 L2 也不会从旧反馈自动生成它。

普通代码与 SDK 消费只读出口：

```ts
const preparation = await conversation.operations['a.preparation']({
  actorId: authenticatedActorId,
});
const location = await preparation.locate();
if (location.status === 'accepted') {
  const operation = await preparation.attach(location.reference);
  operation.observations$.subscribe(renderPreparation);
}
const participation = preparation.participation(participationId);
const feedback = await participation.readFeedback();
```

resolver 捕获当前公开绑定；所得句柄保持原 command。组件随后切换对象或撤下入口，不会把它重定向到另一操作。readFeedback 是已登记参与的单次读取，不登记等待、不承诺等待未来节点。恢复读取可以通过宿主按会话、合同、原 command 和 participationId 定位，无需重新挂载原组件或启动 Agent。普通读取没有当前 Goal 权限。

同一 Goal 修订的多个等待共同约束自动续接；暂停不阻止业务事实更新，新 Goal 修订不继承旧结果的继续权限。恢复重建持久关联，不恢复 JavaScript 调用栈。

## 人工参与

程序用 HumanInput 声明真实响应者准入，问题与回答继续属于 dsh 原生问答服务。多个模块可以贡献 HumanInput，但必须传同一个必需的 `admitResponder` 函数引用；冲突拒绝，最后一项退出撤销资格，不提供默认政策：

```tsx
<HumanInput admitResponder={authorizeResponder} />
```

业务 Action 在真实原生工具调用的 execute 中，可以直接请求人补充信息：

```ts
const response = await call.askHuman([
  { id: 'reception-window', question: '这家商户希望在哪些时段提供接待？' },
], { timeoutSeconds: 30 });

if ('pending' in response) {
  return { status: 'waiting-for-answer', callId: response.callId };
}
return { status: 'answered', answer: response };
```

Action 的 output 合同应明确包含这些业务返回分支。省略第二个参数时，`askHuman(questions)` 等待本次函数中的明确答案，返回 HumanAnswer；设置正整数 timeoutSeconds 时，等待窗口结束可以返回 `{ pending: true, callId }`。pending 表示尚未收到答复；后来回答沿原问题进入对话，不恢复已经返回的函数，也不重执行这个 Action。需要跨重启的人工审批或业务任务仍由业务 owner 保存正式事实。

问题支持 id、question、header、options 和 multiSelect，输入按标准提问工具实际支持的合同验证；detail 与 intent 不能通过该工具入口传入。框架使用真实原生父调用执行并记录 programmatic 子调用，保留工具策略、取消、问题归属与原生迟答。程序不需要持有 Agent 或自己写调用日志。

这项控制能力与 `call.wait`、`call.completeGoal` 一样属于本次执行。它只在 execute 的调用体内有效；保存 call 后再次使用、在准入或输出处理阶段使用都会被拒绝。调用体退出时，未等完的提问会取消并完成清理，再结算父调用。普通 SDK 和组件服务调用没有原生父工具，因此不能凭这份 API 提问或借用当前模型身份；它们仍可观察、接手和回答已经存在的问题。

本地句柄提供 questions$、foregroundQuestions$、answerQuestion(callId, answer, options) 和 attachWait(callId, options)。前者是原生持久问题投影，后者是实际仍存活的前台 continuation。PTC 的前台问题尚未进入持久 open 投影，不能用其中任意一个来源替代另一个。

持久问题、已回答问题和前台问题都携带 `origin`：原会话的 `sessionId`，以及实际调用的 `callId`、`rootCallId`、`parentCallId`、`name` 和完整 `inputs`。`admitResponder` 通过 `admission.question.origin` 取得同一关联。一个步骤可能共同消费多个请求，策略需要核验实际涉及的范围；不能只选其中一条来解释整个问题。

消费者可以直接按原请求组织参与入口：

```ts
import { map } from 'rxjs';

const requestQuestions$ = conversation.foregroundQuestions$.pipe(
  map(questions => questions.filter(question => question.origin.inputs.some(input =>
    input.sourceSessionId === conversation.id && input.requestId === requestId,
  ))),
);
```

`origin.inputs` 保留原始 JSON 范围和每条输入的来源会话，不按当前 Program 再次解析，也不随页面选择或后续输入改变。业务模块和消费者无需监听历史工具事件来反查问题；响应准入仍由根程序的明确策略和业务权限服务决定。这份关联提供事实，不提供 Goal、批准业务操作或扩大范围的权限。

attachWait 的物理连接接手前台等待计时，断开时释放 claim。等待窗口结束后，问题可以进入 continued；迟到回答沿同 callId 恢复原工具调用的请求与业务范围，使原模块的上下文和能力继续生效。响应者分别准入，不能替换原工作身份。冷打开不重建前台 resolver，答复已 continued 的问题会经过当前策略再恢复原生驱动。策略撤下只撤当前参与入口，不取消原问题。

answerQuestion 返回原生前台受理或迟答入箱结果，不承诺对应模型步骤已经完成。人工答复不会自动取得批准业务操作或扩大 Goal 范围的权限。

当前跨进程入口与边界见[外部参与与观察](transport.md)。
