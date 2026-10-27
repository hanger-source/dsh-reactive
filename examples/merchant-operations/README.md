# 商家运营模块 reference

这组源码展示准备、知识、接待和商家命令如何作为可复用业务模块组合。模块和共享合同只使用 `@hanger-source/dsh-reactive` 公开入口、RxJS 与 React，不导入 dsh，也不依赖私有运营包。`live.ts` 提供可运行的本地资料协作入口，`live-owner.ts` 以隔离工作区的文件和 SQLite 拥有业务事实；它们复用同一组四域模块和宿主装配。

`contracts/` 定义本 reference 实际消费的领域投影和纯 Action/Operation 合同，不是 `@travel-merchant/operations-contract` 的替代版本或 wire 兼容层。真实集成在自己的信任边界验证原服务结果，再明确投影到这些必要字段。引用保留原业务身份，不用商家名、页面选择或原生 callId 替代业务操作号。

| 模块 | 稳定业务身份 | 写入与真实结果 |
| --- | --- | --- |
| Preparation | onboardingId；独立 operationId 与 deliveryCommandId | prepare 固定交付身份；每个部署函数独立受理。单命令 settled 不等于整项 prepared，awaiting-command/observation-required 可成为协作反馈。 |
| Knowledge | onboardingId/submissionId；原 taskId/activationId 单独保留 | initialize/retry 的回执不是任务成功；查询异常和未知受理不触发再次 POST。commit 固定当前知识上下文，不表示商家认可或接待已开通。 |
| Reception | onboardingId/operationId；保留原 preparationOperationId | enable/resume 必须沿原开通要求、当前修订及真实准备/知识事实核权。enabled 的回执仍明确 evaluation=not-run。 |
| MerchantCommands | merchantKey subject、sandboxId、commandId；绑定 deliveryIdentity | execute 一次；observe/reconcile 沿原命令读取。unknown 不代表没有执行，completed 也须读取 exitCode/output 判断业务结果。 |

准备命令合同包含当前准备链使用的显式部署函数和 provider 操作。商家命令合同限于已有 Shell、服务、路由与制品发布能力，不复制 Host 输入物化等未被此 reference 消费的完整 DTO。知识初始化明确展示 historical-consultations 及其时间窗口；不使用任意 JSON 字段伪装所有知识任务类型。

## 运行本地资料协作

`runMerchantCollaboration(options)` 使用宿主传入的 `RuntimeModel` 实际推理，不预排工具调用或生成模型回复。runner 不读取个人 Profile，不接收、保存或打印密钥；宿主负责选择实际 provider、模型与凭据来源。这里的业务结果是本地资料准备、索引、知识摘要固定和接待配置文件，不涉及正式运营 Instance、生产部署或客户送达。

在本仓源码中使用 Node.js 24.18 或更高版本，先安装依赖并构建公开入口，再将包含 TSX 模块的 runner 打包为 Node ESM：

```sh
npm ci
npm run build:core
npx --no-install esbuild examples/merchant-operations/live.ts --bundle --platform=node --format=esm --target=node24 --packages=external --outfile=.tmp/merchant-live.mjs
```

在仓库根目录创建调用文件 `run-merchant.mjs`。下面使用 `piAi` 的 `openai` 目录路由；进程宿主事先提供 `MERCHANT_MODEL_ID` 和 `MERCHANT_MODEL_API_KEY`，模型 ID 必须属于所选路由的实际目录。`apiKeyEnv` 只保存凭据变量名，密钥值不进入此文件、runner 参数或报告：

```js
import { piAi } from '@hanger-source/dsh-reactive-dsh';
import { runMerchantCollaboration } from '.tmp/merchant-live.mjs';

const model = process.env.MERCHANT_MODEL_ID;
const directory = process.argv[2];
if (!model || !directory) throw new Error('需要模型 ID 和新的隔离绝对目录');

const runtimeModel = piAi({
  provider: 'openai',
  model,
  profile: { apiKeyEnv: 'MERCHANT_MODEL_API_KEY' },
});

const report = await runMerchantCollaboration({
  directory,
  runtimeModel,
  documents: [
    { name: 'service-hours.md', at: '2026-10-01T09:00:00Z', content: '服务时间为工作日 09:00 至 18:00。' },
    { name: 'booking-notes.md', at: '2026-10-02T10:00:00Z', content: '预订资料需包含出行日期、人数和联系方式。' },
  ],
  maxModelRequests: 20,
  timeoutMs: 600_000,
  onTrace(event) { console.info(event); },
});

console.info({ sessionId: report.sessionId, indexedDocuments: report.indexedDocuments, indexDigest: report.indexDigest });
```

```sh
node run-merchant.mjs /tmp/merchant-collaboration-run-001
```

目标目录必须尚不存在、父目录已经存在。runner 在其中建立 `workspace/`、`dsh/`、方法 Skill 和成功报告 `result.json`，保留本次业务制品与原生会话事实。每次独立运行使用新的目录。`documents` 必须非空，文件名唯一且匹配 `^[a-z0-9][a-z0-9.-]*$`，正文非空，`at` 为 ISO 时间；runner 根据全部资料生成完整索引窗口。

已有宿主可直接传入自己的 `RuntimeModel`，无需使用上述 provider。自有网关使用 `piAi({ provider, model, profile })` 时，应在 `profile` 明确配置实际协议、`baseURL` 和模型目录及能力；凭据仍以 `apiKeyEnv` 引用，由进程宿主提供。默认预算是 20 次模型请求、10 分钟，可通过 `maxModelRequests`、`timeoutMs` 调整，外部取消使用 `signal`，过程观察使用 `onTrace`。超预算、取消、错误或 Goal blocked 会拒绝本次调用，不能作为完成结果使用。

模型自行选择动作，四域服务只在权限、目标、当前修订及前置事实成立时受理。完成判据来自实际 owner：

- 准备写出 `manifest.json`，inspect 逐个读取源文件并核对摘要，之后准备聚合才进入 prepared。
- 知识动作读取完整窗口内的源文件，写出 `index.json` 和 SQLite FTS 内容；commit 独立固定当前索引摘要。
- 接待动作核对准备与已固定摘要，写出 `reception.json`。其中 enabled 仅表示本地文件配置完成，evaluation 保持 not-run。
- 最后命令仅允许 `node verify-reception.mjs`，实际启动 Node 子进程读取并核对配置和索引；不是由模型文字声称命令成功。
- runner 还要求 Goal complete、目标 Skill 正文实际装入模型请求，并独立比对输入资料、源文件、索引、SQLite 内容和摘要，核对六项业务效果及 observer-only 目标未被写入，再生成报告。Skill 声明已装配完整方法，不要求模型再调用工具重复读取。

操作的原 command、稳定反馈节点和通知地址保存在 SQLite 中。runner 将业务变化交给框架的 `notifyOperation`，由同一原工作继续；通知不自动选择下一动作，也不把 Action 回执当成 Operation 完成。此入口创建新隔离业务，不提供重开既有业务数据库的入口。

## 宿主装配

`host.ts` 集中接入真实服务，应用不必为每个场景重复 `implementAction`/`implementOperation`：

```ts
const owners = createMerchantOwners(ownerServices);
const bindings = createMerchantBindings(businessServices, owners, {
  humanInput: responderPolicy,
  requiredSkills: configuredSkillNames,
});
```

这里的 `ownerServices`、`businessServices`、`responderPolicy` 和 Skill catalog 均由真实宿主提供。`MerchantServices` 中的函数消费明确 target、解析后的输入和可信 `BusinessAccess`，返回真实受理引用或实际查询结果；工厂不提供这些函数的假实现。业务 commandId 的 UUID 签发由宿主统一负责，同一原 sessionId/invocationId 的 owner 定位与 execute 必须使用同一规则，不能把任意原生调用字符串直接当成 UUID。

`MerchantOwnerServices` 明确要求每个 owner 提供 locate、observe、registerFollow、readFeedback。已有真实 `OperationOwner` 的宿主也可直接传入 `MerchantOwners`，不必重建 owner。将 `merchantOperationOwners(bindings)` 放入应用宿主已有的 operations 配置；Program/Environment 的装配使用 `program.tsx`，纯 SDK 合同使用 `program-contracts.ts`。

Facts 来源是标准 `Observable<Observation<Facts>>`。它保留 pending/ready/error/generation；失效只能要求重新读取，不能以通知正文替代权威事实。`can*` 只表达当前能力现场，实际写入仍由业务服务重新核验权限、目标和修订。`current*` 必须来自真实保存的原 Operation.command；没有明确关联就给 null，不能从 operationId/submissionId 猜造 sessionId/invocationId。

## 模块与消费

`module-contracts.ts` 为每个领域定义一次局部 Surface。`program-contracts.ts` 组合整棵合同树，框架生成精确 SDK 路径和挂载点：

```ts
const merchant = composeSurface({
  preparation: preparationSurface,
  knowledge: knowledgeSurface,
  reception: receptionSurface,
  commands: commandSurface,
  collaboration: collaborationSurface,
});
const operations = composeSurface({ merchants: collectionSurface(merchant.surface) });
const program = defineProgram({ input: merchantProgramInputSchema, surface: operations.surface });
```

`createMerchantPrograms(bindings, observeTargets)` 从业务 owner 的会话可见对象流取得目标集合，集合增加、重排或移除直接更新组件树。外层使用 `<Module mount={operations.mounts.merchants.instance(onboardingId)}>` 挂完整商家模块；商家模块内部继续使用自己的 `merchant.mounts.knowledge`，框架自动完成嵌套映射。模块内部用局部名字声明 Context/Follow，Offer 默认取 Action 合同名。新增商家不重新创建 Surface，也无需再列一套名字和根出口。

Review 使用 `selectSurface(knowledgeSurface, { views: ['facts'], actions: ['commit'] })` 选择公开合同，复用相同模块正文。未列类别不公开；选择不改变模型工具或业务 owner 的权限。模块不携带应用根 Surface，也不按工具字符串筛选调用结果。

`modules/resources.ts` 用标准 RxJS 和框架资源算子建立真实跨域关系：

- 准备现场的修订和原操作变化，通过 `switchResource` 撤下旧知识读取，再连接业务 owner；pending/error 同样传播。
- 接待来源依赖当前准备与知识，依赖变化取消旧读取并重新获取现场。
- `combineResources(...).pipe(mapResource(...))` 按依赖方向派生：知识前提只消费准备与知识；准备与知识 ready/snapshotReasonCode 再共同约束接待开通。接待不可读不会反过来阻止知识修复。
- 派生进入模型 Context、知识/接待 Offer 和每个实例的 `collaboration.facts`；派生不自动提交 initialize、commit 或 enable。模块 Context 显式 `required:false`，未就绪时撤下其现场及依赖能力，让其他模块继续工作；SDK 仍看到 pending/error。默认必需 Context 的等待语义保持不变。

查询只声明哪些业务事实影响来源，连续有效依赖的比较、失效取消及同依赖恢复由 `switchResource` 处理：

```ts
const knowledge$ = useShared(() => preparation$.pipe(
  switchResource(() => bindings.knowledge.facts({ onboardingId: target.onboardingId }), {
    dependencies: preparation => [preparation.onboardingId, preparation.revision],
  }),
), [preparation$, bindings.knowledge, target.onboardingId]);
```

完整的目标校验与依赖字段见 `resources.ts`。校验通过 `mapResource` 表达，坏现场成为该代 error；后续正确现场可以恢复，未经核验的数据不会作为 ready 继续传播。这里不需要 JSON 编码、Observation 比较器或另一份查询缓存。

`modules/delivery.ts` 的 `useMerchantDelivery` 把这张资源图与四域 Action、可选 Operation、人工问题和会话活动组合在同一个业务 hook 中。组件消费 `work.preparation`、`work.knowledge`、`work.reception`、`work.commands` 并声明 Context、Offer、Follow 和 Expose，不再为尚未出现的 Operation 拆出条件 Work 组件。`useOperation` 的明确 null 表示没有当前绑定；旧 reader 始终保留旧命令。

在途准备会约束知识写入，在途知识或准备会约束接待配置，在途接待配置会约束商家执行；各 Action 不会因自己的在途计数撤销本次准入。`collaboration.activity` 分别公开短调用数、业务长期阶段、来源有效性、关联人工问题和阻塞等待。全会话执行及 Goal 放在 `conversation` 字段，不能当作某个商家的独占工作。

组件组合入口只选择一次 `followMode`。默认 wait 暂缓原工作；background 可靠关联结果并允许 Agent 继续其他工作。结果回来后仍须原 Goal 修订有效、未暂停且没有其他阻塞等待，才可自动继续。只读观察直接用 Operation 流，不登记 Follow。

```ts
const merchant = conversation.collection('merchants').instance(onboardingId);
merchant.observeView('collaboration.activity').subscribe(onActivity);
```

此观察在本地和远程使用同一个合同。页面退出只退订自己的观察；业务操作、原工作反馈与人工问题保持各自的归属。资源错误可以留在该模块的 Observation 中；未处理的 `useRelation` 错误会使 Program 失败并同步释放查询和关系，已受理的 Action 继续由原执行 owner 结算。需要处理某次异步业务失败时，在 RxJS 的对应内层显式处理，不把已终止的外层流当成仍在运行。

每个来源由 `useShared` 共享，多个模块和公开观察不产生多份并行查询。显式跨域重读要求 `facts()` 每次连接实际读取权威现场，不能只回放旧缓存；具体 source identity、授权和修订继续由业务 owner 在写入边界核验。资源组合不提供跨后端事务。

Invocation.result 只表达这次函数的返回；Invocation.operation 提供独立的 OperationHandle。Operation 的 state、settled 和反馈节点各自保留语义。再次 Follow 只建立新参与，不执行原 Action；after 指向已消费的真实节点。只读 SDK 可以沿原 command/reference/participationId 查询，无需启动 Agent。

SDK 通过纯 Program 合同取得视图和 Action 类型：

```ts
const merchants = conversation.collection('merchants');
merchants.instances$.subscribe(showAvailableMerchants);
const merchant = merchants.instance(onboardingId);
const facts = await merchant.readView('knowledge.facts');
const initialize = await merchant.resolveAction('knowledge.initialize', { onboardingId });
const invocation = await initialize({ expectedRevision: facts.revision, request });
const receipt = await invocation.result;
const task = await invocation.operation;
task.observations$.subscribe(showTaskProgress);
```

调用保留 bindingId，实例撤下后旧句柄拒绝新调用；重新挂载同一 key 也不重定向旧句柄。已经受理的业务仍归原 owner。业务输入只保留实际修订和命令参数，不重复塞 scope。完整本地/远程通用消费见 `consumer.ts`。

原生能力由宿主安装并提供真实文件系统或其他业务政策，再通过 `<NativeTools names={['read', 'write', 'edit', 'todo_write']} admit={nativePolicy} />` 组合；声明不会安装这些工具或绕过其 guard。`actionManifest$` 提供真实 Action/模块关联，工具及 PTC 结果可用 `readActionPresentation(Action, event.meta)` 读取，业务 UI 负责卡片内容。

整个 Program 只声明一个真实 HumanInput 响应策略。业务函数可通过 `BusinessAccess.askHuman` 收集交付偏好等缺失资料；这只在真实原生 Action body 内可用，独立 SDK/服务调用不能伪造原生提问或 Goal 权限。响应策略根据 question.origin 审核原调用及原始输入；回答不会扩大原业务批准范围。

## 真实接入边界

这些模块与机械宿主装配是完整的源码消费面；真实运营 owner 尚未接通这份可靠参与合同。现有运营 wait/outbox 不能仅靠字段改名满足副作用前的幂等登记、精确参与的稳定后继反馈读取及可靠通知。这里不提供默认 ACK，也不以临时 Observable 订阅声称可靠续接已经完成。

真实业务集成须在副作用前可靠登记原 command 与通知地址，保留或确定性读取稳定反馈节点，并利用已有事务 outbox 调用 `notifyOperation(sessionId, followId)`。通知不自报 ready、结果或 Goal。框架已经负责原生关联日志、wait、精确 Goal 修订、固定投递、inbox 受理与会话恢复；宿主和四业务模块都不手写这些接线，也不创建第二套持久队列。

当前 dsh SessionController 的原生新建、冷恢复、composer 和队列编辑尚无公开宿主接点，不能直接把原生 UI 接入视作完成。应用 SDK 已有统一 submit/upload/edit 入口；原生 UI 直连仍需上游提供同一受理 owner 的生命周期接点。
