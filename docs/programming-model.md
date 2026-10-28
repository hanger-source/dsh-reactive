# 编程模型

程序以普通 TypeScript 值定义业务行为，以组件组合当前对话环境，以标准 RxJS 表达持续关系。组件的渲染结果是 Context、Offer、Follow、Skill 与公开出口；业务动作和新的协作等待在显式调用时发生。

运行于服务端或 Agent 宿主。应用不需要为这套模型建立前端页面，也不需要在业务模块中持有 Cordis Context 或原生 Agent。

## 定义业务行为

可复用消费者通过 core 的 `ProgramSessionClient<P>` 描述所需会话能力，直接接收本地 `ConversationHandle<P>` 或远程 `RemoteProgramSession<P>`。视图、进展、原请求和人工问题继续返回各自原有的资源对象；同一模块可以用函数、Promise、RxJS 或 hooks 读取它们，框架不为跨宿主复用建立第二个会话。

只需要公开模块出口时使用 `SurfaceClient<S>`，根会话与 `collection(name).instance(key)` 共用视图、动作、Operation 槽和嵌套集合合同。业务代码用 `await client.resolveAction(...)` 和 `await action(...)` 消费两种时序；具体本地类型仍保留同步解析和受理，具体远程类型仍返回 Promise。`ProgramSessionClient` 的 Async 参数只描述这部分 Surface 时序；读取或附着业务 Operation 不会变成一次 Action，原生暂停、取消和 journal 控制仍由具体 SDK 提供。

`ProgramSession` 是后台组件收到的会话事实与输入接口，`ProgramSessionClient` 是外部消费者的能力接口。两者不互相伪造执行身份。消费者释放观察后，已受理的调用和业务 Operation 仍由原 owner 持有。

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
    <Context name="facts" observation={facts.state} render={value => value} />
    <Offer action={prepare}
      enabled={facts.state.status === 'ready' && facts.state.value.canPrepare}
      follow={{ when: 'feedback', scope: { merchantId } }} />
    <Expose surface={merchantSurface} value={{
      views: { facts: facts.observations$ },
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

`selectSurface` 选择公开类别及槽，未列类别不公开。相同 MerchantWork 可以放到 review.mounts.a 下，只公开事实。公开选择不撤下模型工具，也不代替业务授权。

`useResource` 直接读取已有资源 Observation，并返回同一条标准 observations$。`resourceView(Merchant)` 明确消费资源状态：pending 使当前读取失效，error 保留为读取失败，只有 ready 的领域值才编码。普通 `view(schema)` 消费值事件，不按业务字段猜状态。

跨模块关系直接组合资源对象及标准观察流，两种来源可以混用。`combineResources` 和 `switchResource` 从对象的同一 observations$ 消费，保留解析后的领域值与原 owner，不调用 read/peek 或建立另一份状态。资源算子保留失效语义，不把旧 ready 留在派生现场：

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

同一派生资源可以同时用于 Context、Offer 的当前条件与公开资源；依赖 pending/error 会使派生失效。共享连接由 useShared/useResource 管理，跨源组合不提供后端事务。Offer 的 enabled 决定模型现场，正式权限、来源一致性和修订仍由业务 owner 在执行边界核验。

Context 默认是本步必需现场，pending 会等待，error 会使准备失败。独立模块可以显式声明 `<Context required={false} ... />`：未就绪时撤下该上下文，ready 后再贡献当前值；同源 Surface 仍保留 pending/error，相关 Offer 仍按依赖条件关闭。它允许其他可用模块继续参与，不回放旧值，也不代替错误处理或业务授权。

## 定义程序与工作环境

```tsx
import { defineApplication, defineEnvironment, defineProgram, z } from '@hanger-source/dsh-reactive';
import { implementProgram, useObservable } from '@hanger-source/dsh-reactive/program';

const program = defineProgram({
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

所有流仍是标准 Observable。使用 map、combineLatest、switchMap、concatMap、mergeMap、catchError 等表达业务关系，不需要框架自定义的流操作符。

- `useShared(factory, dependencies)` memo 来源并使用 RxJS share/ReplaySubject 共享查询。最后一个查询观察退出时释放来源；已完成或失败的同一来源保留终态。
- `useObservable(factory, dependencies)` 通过 useSyncExternalStore 读取普通来源的当前 Observation，提交后才订阅；退出停止此组件的查询观察。
- `useResource(factory, dependencies)` 共享资源来源，直接返回 state 与 observations$。来源错误、空完成或停在 pending 后完成都进入资源 error；业务空值仍是 ready 中的值。
- `useResourceInstance(implementation, params)` 取得当前 Program 共享的正式资源实例。普通函数通过它 read、invalidate 或订阅，组件通过 `useResource(instance)` 观察，同一实现与解析参数只持有一个实例；直接 `useResource(implementation, params)` 也连接这个实例。
- `useReaction(implementation, params)` 取得当前 Program 共享的持续反应，`start()` 才开始感知和执行。同实例不能同时 start 两次；组件退出不停止程序拥有的运行，业务显式调用 `run.stop()`。程序失败或退出会停止查询、持续感知及尚未开始的排队工作，已受理业务仍由原 owner 结算。
- 局部资源使用 `useResourceInstance(() => ({ value, read, invalidations? }), dependencies)`，或同一选项的 observe 分支；局部反应使用 `useReaction(() => ({ event, result, observe, operator, execute }), dependencies)`。factory 只纯构造 createResource/createReaction 已有选项，React dependencies 决定组件内身份；同依赖重渲染保留实例，不同组件分别拥有局部身份。Program 统一持有这些正式实例，组件退出不销毁已交给普通函数的对象或停止显式运行。依赖改变取得新对象，旧实例不重定向；需要跨组件共享时，使用现有 implementation 与 params 合同入口。
- `combineResources({ a: a$, b: b$ })` 组合具名资源，任一 error 保留为 error，尚有 pending 则不能产生 ready；全部有效才给出具名领域值。
- `shareResource(observations$, { signal })` 将已有或组合后的 Observation 流提供为稳定只读资源。普通函数、Promise、peek、subscribe、values$、observations$ 与 `useResource(resource)` 共用 RxJS 的一份当前快照，不重复 schema 解析，不新建 Scope。构造不连接；有限来源的最终观察保留至最后消费者退出，之后显式读取再连接。signal 固定该资源的读取寿命，不能通过修改原 options 更换 owner。
- `mapResource(project)` 只派生有效值，失效和错误继续传播；同步投影错误进入同代 error，后续有效代次可以恢复。
- `switchResource(project, { dependencies: value => [value.id, value.revision] })` 根据当前有效依赖连接另一资源。依赖 tuple 按元素 `Object.is` 比较，连续 ready 的依赖相同就保留原查询；只选择实际影响查询的标量或稳定引用，不需要 JSON 编码或自行比较 pending/error。任何上游 pending/error 都立即退订旧查询并清除依赖身份，同一依赖恢复也会重新连接。selector 抛错变为当前 error，后续有效观察仍可恢复；新查询先进入 pending，迟到结果不能回写。tuple 在观察时被捕获，后续修改原数组不会改变已选身份。省略 dependencies 时，每次 ready 选择新来源。它只切换读取，不重发业务命令；共享仍由既有 owner 和 `useShared` 持有。
- `useRelation(factory, dependencies)` 提交后建立长期关系，流内未恢复的错误进入程序失败边界。异步组合返回 Promise 或 Observable 进入高阶操作符，不依赖 tap(async ...) 等待。
- `useBeforeStep(check, dependencies)` 登记幂等检查，原生步骤实际等待它的返回值。每次登记有独立身份，相同函数被多个组件使用也各自拥有退出寿命。依赖替换或组件退出会中止旧检查收到的 signal 并结束等待；准备过程重新核对当前检查集合，旧 Promise 迟到完成或失败不再作为当前准备结果。原生步骤取消或 Program 失败同样结束等待，仍有效的检查失败则继续传播。检查应使用信号读取真实依赖并等待其工作完成，不在检查中执行业务命令。
- `workspace.share(source$)` 把明确共享的连接交给工作区；应在订阅阶段连接，而非组件纯求值中启动来源。同一工作区与来源复用一个连接，会话或最后一个消费者退出不停止它；有限来源的终态只在工作区 owner 活跃时保留。owner 退出结束已有观察，之前捕获的流在迟到订阅时直接完成，不重放旧值或旧错误，也不重新连接来源。

factory 纯构造来源或目标，依赖数组决定其身份。重复求值不应发起业务操作。普通 React state 和 Transition 仍遵循 React 调度；它们不自动获得模型准入或权限事实的同步保证。

React commit 提交完整声明，native prepare 等待当前依赖。每次异步等待后，宿主再次刷新并核验声明修订；变化则重取当前现场。commit 本身不代表资源 ready 或业务完成。

`Skill({ name })` 将选中的原生方法正文纳入步骤上下文。准备期间声明、Skill catalog 或当前会话工作目录变化，会取消本次旧查询并读取当前集合，不等待旧 provider 正文返回；迟到结果不会写入新上下文。取消查询不撤销共享 provider，也不影响其他会话的读取。当前有效方法不存在、不允许模型调用或读取失败仍拒绝准备；步骤取消和程序退出结束本次等待。


## 启动宿主

宿主明确登记 Operation owner、服务发布身份、独立数据目录和模型。`deepseek` 使用官方 Messages 适配器，`piAi` 接入官方多协议 provider profile；业务组件不持有模型适配器。`nativeModel` 留给已有原生适配器的宿主。

```ts
import { createRuntime, deepseek } from '@hanger-source/dsh-reactive-dsh';

const configuredModel = deepseek({
  model: 'deepseek-flash',
  apiKey: resolveModelCredential,
  maxTokens: 4096,
  retryPolicy: { mode: 'normal', maxRetries: 3 },
});

const runtime = await createRuntime(() => application, {
  home: '/application/data/operations',
  actorId: authenticatedActorId,
  serviceActorId: 'operations-publisher',
  operations: [PreparationOwner],
  model: configuredModel,
  compaction: { headroomTokens: 8192, maxTokens: 2048 },
});
```

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

`Submission.content` 接受字符串或有序 `PromptContentPart[]`。字符串只归一为一个 text part，保留原文；空白判定不会 trim 正文。普通输入必须显式选择 delivery：followup 在下一轮处理并唤醒，steer 在最近后续步骤处理并唤醒，inject 在最近后续步骤补充上下文，本次投递不请求唤醒。冷会话提交需要加载原生实例，已受理子工作仍可沿原授权恢复并产生模型活动；inject 不暂停或撤销既有 Goal 和业务工作。业务 intent 与投递方式分别保存，goal 输入还须有真实的非空文本目标。

```ts
const uploaded = await conversation.uploadFile(fileBytes, { name: 'merchant-details.pdf' });
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

`uploadFile` 接受 `Uint8Array | Blob` 和可选 name、signal，返回官方 `FileUploadValue`。上传不投递模型输入。receipt 只属于接收它的真实原生 Session 实例，不能用于另一会话；未提交的 receipt 在原 Agent 关闭或进程退出后失效。`release()` 只释放观察句柄，不等于关闭 Agent。客户端不能把持久 FileAttachmentRef 直接塞入 Submission；已有 canonical base64 的正式 transport 使用 Handle 的 `uploadEncodedFile(request, options)`，仍由原生服务验证编码及签发 receipt。

应用的 `admit` 接受 `AdmittedSubmission`：text 保持原文，图片与文件已经提升为原生持久附件引用。首次投递和模型消费前复核使用这份相同形状，策略不从持久引用猜回图片字节或上传 receipt。scope 仍是原始 JSON，不是 Program input 的领域输出；actor 由可信宿主或远程认证签发。

普通 SDK 与 AI 工具进入同一 Action。SDK 的身份来自可信宿主配置或认证入口；组件内直接调用绑定 Action 时，宿主使用显式配置的 serviceActorId，缺少身份则拒绝。独立调用没有继承当前模型输入或 Goal，不能凭当前环境执行 wait 或 completeGoal。

`resolveAction(name, expectedScope)` 只读取得当前公开的具体绑定，使用原合同解析调用者明确期望的业务目标。返回的可调用值保留 `bindingId`、解析后的只读 `scope` 和纯 `definition`；它不会随组件改选而重定向。目标不符、绑定被撤下或换成另一个实例时明确失败；同一目标重新挂载也形成新的绑定。解析和调用都会冲刷当前声明，异步准入结束、业务 execute 开始之前再复核。已经进入 execute 的调用继续由原 owner 结算。

绑定身份只在当前程序实例中识别实际对象，不是权限凭据或持久引用。进程恢复后重新解析；业务恢复使用原 Operation command/reference。开发者不需要为了防止错投而把 scope 再复制进 Action input。

本地与远程 SDK 统一以 `CancellationOptions` 接收 `{ signal }`，例如 `readView(name, options)`、`resolveAction(name, scope, options)`、`submit(input, options)` 和 `prepare(input, options)`。共享消费者可以对两端统一使用 `await`。本地的可信参与入口还接受 `SubmitOptions.actorId`；远程身份由认证载体决定，调用选项不提供 actor。远程解析和受理等待的取消不会撤销已经受理的业务；直接调用中的信号交由执行 owner 处理，长期 Operation 的取消仍由业务合同决定。

Invocation 的 activity 资源提供本次调用当前有效事实，read 不等待业务完成；可直接交给 useResource 或组合 observations$，activity$ 引用同一对象的 values$。本地 pending/running 是已经知道的调用状态；业务 failed 也是 ready 的调用事实，观察来源不可用才进入资源 error。returned/failed 后流结束，晚读取仍取得终态，不重新调用业务。纯本地事实由 Invocation 持有，组件和绑定退出不撤销它；远程读取和订阅受所属 SDK 的寿命限制，SDK 退出不取消 Host 业务。

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

程序用 HumanInput 声明真实响应者准入，问题与回答继续属于 dsh 原生问答服务：

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
