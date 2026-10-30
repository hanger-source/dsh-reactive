# 外部参与与观察

`@hanger-source/dsh-reactive` 提供纯 Program、Action、Operation、Surface 合同与程序 API；`@hanger-source/dsh-reactive-dsh` 提供原生宿主；`@hanger-source/dsh-reactive-remote` 提供已认证 Gateway 与 Host Typert 贡献。Node 与浏览器 SDK 是独立的 `@hanger-source/dsh-reactive-client` 包，只依赖 core 和官方客户端协议、连接及恢复实现，不安装 Gateway 宿主、renderer 或 UI。Gateway 通过 client 的 `/types`、`/errors`、`/transport` 复用同一纯合同，core 不依赖这些扩展。客户端只导入纯合同与 client SDK，不导入宿主组件、业务 handler、凭据或宿主装配。

## 宿主与认证

`mountProgramGateway(runtime.native, options)` 在同一个原生 Context 安装官方 Typert Registry/Loader、RemoteAccess、Gateway 与 SessionController，复用 Connection，登记真实服务和原生事件源，并启动已认证 HTTP/WebSocket carrier。应用提供已有的 `ApplicationHost`、默认模型配置、认证函数与权限 policy。独立 Runtime 已持有附件存储和 FileUploads，Gateway 沿用这份服务，不另选 home 或安装另一份附件 owner。

```ts
import { mountProgramGateway } from '@hanger-source/dsh-reactive-remote';

const gateway = await mountProgramGateway(runtime.native, {
  application: runtime.application,
  model: { provider: 'my-provider', model: 'my-model' },
  participant: principal => ({ participant: 'human', actorId: actorDirectory.actorId(principal) }),
  access: service => registerApplicationPolicies(service),
  carrier: {
    host: '127.0.0.1',
    port: 3081,
    authenticate: request => authenticateRequest(request),
  },
});
```

`actorDirectory`、`registerApplicationPolicies` 与 `authenticateRequest` 属于应用。认证必须同时覆盖 HTTP 和 WebSocket upgrade，核对实际部署的 Host、Origin、会话或凭据，返回可信 `Principal`；参数正文不能选择 principal、actor 或权限。`RemoteAuthenticationError(401 | 403)` 表达认证拒绝。反向代理部署须同时转发 `/api/*` 与官方 stream mux 路径，保持这条认证边界。

应用通过官方 `PrincipalMap` 声明自己的可信身份种类，policy 只为该种类登记实际权限。`participant` 必须判别实际 principal，再映射到业务角色与 actorId；不要假设所有 principal 都有 id。

```ts
declare module '@deepseek-ai/dsh-typert-protocol' {
  interface PrincipalMap {
    'application-user': { readonly kind: 'application-user'; readonly id: string };
  }
}
```

`participant(principal)` 签发输入来源中的角色与 actorId；客户端收到的 `ProgramIdentity.participant` 和 `actorId` 是宿主确认的来源事实。框架不向参与者发放 owner token，也不把普通参与者提升为 owner。每个 Remote endpoint 仍由官方 Gateway 与 RemoteAccess 准入：

| 权限 | 准入对象 |
| --- | --- |
| 工作目录注册、发现入口 | `createWorkspace`、`describe` 的显式 global grant |
| 创建会话、补齐创建 | `create`、`completeCreation` 对请求中精确 Workspace 的 grant |
| 工作区发现结果 | 逐 Workspace 与 Session grant 过滤 |
| 提交输入与上传文件 | `submit`、`uploadFile` 对应 Session 的独立 grant；上传不替代输入的应用准入 |
| 读视图、问题、请求进展与 journal | 对应 Session 的 `view`、`questions`、`progress`、`journal`、`journalPage` 等读取 endpoint 分别授权 |
| 解析、开始、查询、观察 Action | `resolveAction`、`startAction`、`actionInvocation`、`actionResult`、`observeAction` 对应 Session 的独立 grant |
| 答复与 claim | `answerQuestion`、`attachWait` 各自 grant，加当前 HumanInput policy |
| 激活会话、恢复目标、暂停、取消 | `resume`、`resumeGoal`、`pause`、`cancel` 各自 grant；后三项同时要求真实 human 身份 |
| Operation 槽、反馈读取与通知 | `resolveOperation`、`locateOperation`、`observeOperation`、`readOperationFeedback`、`notifyOperation` 分别授权，并由 Host/业务 owner 再核验 |

`describe()` 的 global 入口许可不授予工作区内容。服务从实际 Host 清单取值，只返回调用者可访问的工作区及其可访问成员。程序 journal 的读取许可不要求给消费者发放原生 Controller.follow 权限。

Environment 提供 Program 实现、指令与原生扩展；Workspace 是原生 registry 中已有绝对目录的身份，不属于某个 Environment。同一 Workspace 可以运行不同 Environment，同一 Environment 也可以用于不同 Workspace。会话创建时分别选择两者，冷读继续核对该应用自己的 binding、原生 Workspace 成员与实际目录，不保存另一份 Environment 到 Workspace 的映射。

`await client.createWorkspace({ directory, title? })` 注册宿主机器上已存在的绝对目录，不创建目录、会话或新业务 domain。它返回 `{ workspaceId, directory, title, sessionIds }`，不含 environmentId。注册入口的 global grant 不自动授予返回 Workspace 的内容或创建权；应用须显式授予该 Workspace 上的 `create`、`completeCreation` 等 endpoint。重复注册已有目录也不会公开全部成员：sessionIds 与 describe 共用同一过滤接点，按当前 endpoint 的精确 Workspace 和 Session grant 逐项核对，缺少 Workspace 内容许可时返回空成员。`await client.describe()` 返回相同 WorkspaceInfo 形状，并继续按 Workspace 和 Session grant 过滤。官方 RemoteAccess 的 `bindings` 是同步权限读取，不能在其中调用异步 `application.describe()`；应用应从自己的授权事实提供明确资源 ID。

已有 Cordis 装配可用 `apply(ctx, options)` 安装贡献；需要独立持有 Action owner 时调用 `createProgramActionOwner(ctx, application, options)`。这个宿主工厂要求 `ctx.typertGateway` 已安装，使用同一个官方 Gateway 编码调用结果及活动中的错误。

嵌入宿主先为 `mountApplication` 提供官方 `attachments`、`fileUploads` 及其真实依赖，Gateway 继续复用相同实例。它不为缺失服务补装本地目录或凭据，也不把普通 API principal 当成原生 owner Peer。文件上传走 Program 已授权的 Session endpoint，随后提交该 Session 签发的 receipt。

## 同一份 Program 合同

图片附件使用 `reactivePrograms/readImage` 的独立 Session grant。请求只提供原会话地址及 attachmentId；服务在读取前和交付后复核原主体的正式权限，并借 `handle.attachments` 委托 Controller 核验会话图片引用。SDK 的 `session.attachments.readImage` 返回正规引用与独占 Uint8Array，字节沿现有 binary codec。未安装 Controller 报 `reactive/attachments-unavailable`；不需要向普通消费者授予原生 Controller 权限。

文件消费沿同一 Session 地址进入 `reactivePrograms/fileRead`、`fileReadBytes`、`fileStat`、`fileList` 与 `fileChanges`。这些端点先核共享 Program 与 accessId，再借用 Host 的 `handle.files`；读取前、交付后和每个变化帧发送前复核原认证主体的 action、session resource 及请求条件。只授予需要的 Program 文件端点，不因安装服务给非 owner 授予原生 `workspaceFiles/*`。官方 Gateway 仍能发现其他已登记的原生贡献，不能把框架端点视为全局拦截层；闲置流的条件策略撤销也不具备完整即时推送保证。

文件内容、字节与变化帧使用正式 WorkspaceFiles 类型和唯一 Typert 生成链，字节沿 binary attachment codec。SDK 的 `session.files` 复用原 Session ClientScope；断线撤下旧 ready，重连建立真实 watch 后重读，变化帧不承担断线补偿。服务端真实 unwatch 由原生 owner 排空，客户端退出不冒称收到远端释放确认。未配置能力使用 `reactive/files-unavailable`；范围失败保留正式 `workspace-file/outside-workspace` 及被拒绝的 path，单次文件错误不关闭整个 Session。

Gateway 默认拥有本次 Registry；宿主已为 WorkspaceFiles 等贡献安装 Registry 时，显式使用 `registry: 'provided'` 借用它，缺失时启动失败，不重复安装或猜测服务。

远程消费者使用 `defineProgram({ name, version, input, surface })` 声明共享 Program。省略 version 时为 1；name 使用小写字母开头的小写字母、数字或下划线，最长 80 个字符，version 为正安全整数。纯合同模块由宿主与客户端共同导入：

```ts
import { z, defineAction, defineProgram, defineSurface, view } from '@hanger-source/dsh-reactive';

const date = z.codec(z.iso.datetime(), z.date(), {
  decode: value => new Date(value),
  encode: value => value.toISOString(),
});
const confirm = defineAction({
  name: 'confirm_order', description: '确认明确订单',
  scope: z.object({ orderId: z.string() }),
  input: z.object({ amount: z.string().transform(value => Number(value)).pipe(z.number()) }),
  output: z.object({ receiptId: z.string() }),
});
export const orderProgram = defineProgram({
  name: 'orders', version: 1,
  input: z.object({ orderId: z.string(), asOf: z.iso.datetime().transform(value => new Date(value)) }),
  surface: defineSurface({
    views: { summary: view(z.object({ orderId: z.string(), updatedAt: date })) },
    actions: { confirm },
  }),
});
```

宿主另用 `implementProgram(orderProgram, { Component: OrderProgram })` 提供组件，实现对象只公开其 `definition`。共享合同模块不导入这个实现或组件。客户端将纯 Program 交给 SDK，输入、视图、Action 与 Operation 类型均由该合同推导：

```ts
import { createProgramClient } from '@hanger-source/dsh-reactive-client';
import { orderProgram } from './contracts.js';

const client = await createProgramClient(orderProgram, { baseUrl: location.origin });
const session = await client.open(sessionId);
const subscription = session.views.summary.subscribe(value => showSummary(value));
const controller = new AbortController();
const options = { signal: controller.signal };
const confirm = await session.resolveAction('confirm', { orderId }, options);
const invocation = await confirm({ amount: '7' }, options);
const receipt = await invocation.result;
```

`createProgramClient`、`bindProgramClient`、`createServiceProgramClient` 与 `bindServiceProgramClient` 只接受真实 `defineProgram` 签发的 `SharedProgramContract`。SDK 将 name/version 作为预期身份随 Program 请求发送，Gateway 核对目标环境或会话实际安装的合同；create、completeCreation 与 resume 在创建、补齐或恢复副作用前核验。会话取得回执也携带实际身份，SDK 在建立句柄时复核，随后按已核合同解码。工作区注册和 describe 是应用级发现，不绑定某个 Program。

`ProgramContractMismatchError` 保留 expected/actual，宿主确定拒绝使用 `reactive/program-contract-mismatch`；未具名的本地 Program 不能从远程使用，使用 `SharedProgramRequiredError` 或宿主的 `reactive/shared-program-required` 表达。纯本地 `defineProgram({ input, surface })` 仍有效；本地取得传入实际定义对象时继续核对 exact 对象身份。同名同版本不能替代本地对象、认证或业务权限。

`resolveAction` 发送调用者明确期望的原始 scope，宿主在当前合同下解析并核对当前公开绑定，返回其真实 `bindingId` 与解析后的 JSON scope。客户端不把这个输出再次交给 scope schema，因此字符串到对象等单向 transform 也可以使用，scope 仍须满足 JSON 传输边界。

随后调用携带捕获的 bindingId。槽切换到另一目标、绑定撤下或同目标重新挂载都会使旧句柄明确失败，分别通过 `reactive/action-target-mismatch`、`reactive/action-binding-unavailable` 表达；这类确定拒绝不会归入未知受理。准入等待期间换绑也会在 execute 前被拒绝；已开始的业务不因页面切换或连接断开被取消。解析和业务执行各自授权，bindingId 不能替代权限核验，也不提供跨重启身份。

远程解析使用 Gateway 原生流：开场交付绑定，流持有实际会话句柄。每个 SDK Session 对同一槽保留一个已就绪连接，重复解析完成后移交持有，`session.release()` 等待这些连接释放。它不依赖视图或人工观察权限来延长冷程序寿命。断线期间暂停新调用；重连必须保留同一绑定身份才能恢复，不能把重连取得的新 ID 填进旧函数。失去连续持有以 `connection-lost` 原因明确拒绝，需要新的显式解析。

支持调用方取消的本地与远程 SDK 方法使用 `{ signal }` 选项，类型来自 core 的 `CancellationOptions`。远程身份始终来自认证载体，选项不携带 actor。`resolveAction` 的信号只取消尚未完成的解析等待，就绪连接随后由 Session 持有；远程 Action 调用的信号只取消开始回执的等待，已受理的业务和已取得 Invocation 的结果、观察继续归原 owner。受理尚未确认时仍按未知受理处理，不自动重发。已取消的调用在发送前明确拒绝。

本地 SDK Action 的调用 signal 限制进入业务 execute 之前的准入，不能在业务已经开始后撤回其结果或 Operation 关联。取得 Invocation 后，使用 call.wait({ signal }) 取消某一次结果等待；此方法属于共同 Invocation 合同，模块与集合中的调用及查回同样适用。取消一个 waiter 不终止其他 waiter，不改写 call.result 或 activity，不向 Host 发送执行取消。远程 wait 另受其 SDK 读取 owner 约束，SDK 退出后的原 Host 回执可以在有效 SDK 中按精确地址和 id 查回。本地查回仍交付原 call 对象，首次调用者取消不会成为它之后所有等待的条件。用法见[调用回执等待](migration-invocation-waits.md)。

本地与远程的 `requestStatus(requestId, { signal })` 属于共同的 ProgramSessionClient；`pause({ signal })`、`cancel({ signal })` 和问答控制属于 HumanProgramSessionClient。回执类型 ProgramRequestStatus、ProgramGoalControl 与 ProgramCancelResult 定义在 core。暂停返回原 Goal 的 id、revision、phase，本地具体句柄还保留完整原生 GoalView；canceled:true 只确认原生处理已 idle 并完成持久 flush，不代表外部业务已取消。服务参与者可以读取问题和请求状态，不能调用人工控制。

Gateway 的 participant(principal) 从认证主体映射 `{ participant: 'human' | 'service', actorId }`。该身份不进入请求参数，由宿主在取得会话端口时固定；输入、Action、Operation 与上传沿同一身份准入，Invocation 回执还核对参与者类别和 actor。服务自动化通过 createServiceProgramClient 或 bindServiceProgramClient 使用共同能力，人工端使用 createProgramClient 或 bindProgramClient；各工厂核对宿主返回的身份，服务运行对象不包含回答、前台认领或 Goal 控制方法。即使单独授予了这些 endpoint 的访问 grant，服务身份仍会被服务器拒绝。

固定 rc.3 的原生 `$stream` 与 Journal supervisor 需要 `interactions: 'answer'` 才具备连接代际和恢复循环。服务 SDK 明确复用这一正式传输；该标志不表示业务上的 human 身份，也不授予人工权限。`none` 连接目前不能绑定这套完整 SDK，框架不会降级成无恢复流、伪造连接代际或复制原生重连机制。

这些方法的等待属于调用它们的 Session 句柄。调用级 signal、句柄 release 或 SDK dispose 会结束尚未返回的等待，即使公共 Context 仍存活或载体延迟返回回执；同一 Context 上其他 SDK 继续工作。已取消调用不启动控制，Host 在异步取得真实句柄后、进入读取或原生控制前再次核验请求取消。已经进入暂停或取消的控制由原生 owner 结算，Host 持有其短命句柄直到结算，不因 transport 退出提前释放；客户端退出不回滚控制，也不取消已受理业务。需要确认最终事实时，使用仍有效的句柄观察当前状态。

Node 使用同一入口，提供绝对 HTTP(S) baseUrl，并按应用认证方式提供 `fetch` 和 `createWebSocket`。浏览器默认使用同源 fetch/WebSocket；Cookie 或其他认证载体由应用配置，不能把只有 unary 的请求头认证误当成 socket 认证。

`createProgramClient` 拥有独立的客户端 Context、Connection 和远程合同，dispose 释放这些装配。已存在 dsh 客户端 Context 的原生扩展使用 `await bindProgramClient(program, ctx)`：由应用装配 owner 在该 Context 登记一次 `@hanger-source/dsh-reactive-client/remote` 贡献，SDK 借入现有 Connection 与 `reactivePrograms` 服务。Context 必须具备 `interactions:'answer'`；缺少装配时明确失败。

每个 SDK 安装独立的原生 Fiber，Session 句柄在该 SDK 下拥有自己的 Fiber。SDK 构造等待作用域真正活跃，不交付尚未安装完成的客户端。查询、绑定、journal 和前台 claim 在构造前交给正式 effect；手动退订后尚未完成的异步清理仍由原生 Fiber 持有，Session release 与并发 SDK dispose 都能等待它。框架只汇总这份 owner 的退出错误，不为各能力维护另一套退休队列。一次清理失败不提前结束 owner 的其他清理，全部静止后才向调用者报告失败。

`createAuthenticatedTransport(options)` 返回拥有物理流的 `AuthenticatedTransport`，支持 `dispose()` 与 `Symbol.asyncDispose`。退出同步禁止新 fetch/openStream，等待已启动 socket 的真实 close，以及 uplink 的 next/return 清理，并保留退出故障；来源中不合作的 Promise 仍须由来源结算，不能以超时声称已经静止。已经分派的 HTTP 请求及响应体继续由原请求 signal 与消费者持有，transport 退出不追认撤销远端受理。独立 createProgramClient 自动登记 transport 并在退出时等待其清理回执；手动 installConnection 时，应用将 transport.dispose 登记到该 Connection 的装配 owner，并在退出时显式等待同一 dispose 回执以取得清理失败。bindProgramClient 只借用 Connection，官方流或 Fiber 未交付的物理清理失败由 transport 原 owner 交付，不转移给借用 SDK。

进展流的载体断线继续走官方恢复并等待新 baseline。真正的协议、授权或解析终止错误会结束原 consumer，原 progress、request 与事件入口保留该失败；connection/reset 不将其改回 pending。调用 client.open(sessionId) 取得新的观察 owner，旧句柄可显式 release，业务执行及请求身份不因此重建或重新投递。

取消信号表达立即撤销使用权，原生 Fiber 的异步释放表达实际静止。Session 的视图、进展、只读 Operation 和人工等待归该句柄；已受理 Invocation 的观察归 SDK，业务执行及可靠 Operation 继续归原 owner。一个 SDK 退出不释放借入的公共 Context，也不停止其他 SDK 或原生 Host。core 的 Scope 负责同步响应图，不承担网络清理。

Action 受理 ACK 已确认后，观察作用域退出不能抹掉原 Invocation 的 id、来源与合同。退出后的观察和 result 等待按原取消原因结算；调用方可以在另一个 SDK 使用该 id 恢复原结果，不重新执行 Action。构造观察期间发生重入退出时，已取得的资源仍由原生 effect 释放，已受理身份继续保留。

同一 Context 可为多个 Program 或多个消费者建立独立 SDK。SDK dispose 释放自己的 Session、观察与等待，保留公共 Context 和远程合同；Context 退出会同时关闭其借用 SDK。`bindNativeSession` 核验 SDK 原对象所使用的实际 Connection、Program 与原生 Session generation。原生扩展取得的 SDK 应来自该客户端 Context 的 bindProgramClient；相同会话 ID、相同合同或相同服务器地址不能替代这份连接归属。

需要框架持有整组引用时，使用 `await retainNativeSession(Program, clientContext, sessionId, { signal })`；该入口自行创建 SDK，要求真实具名的 SharedProgramContract。从返回对象的 `session` 消费同一 NativeSession，退出时 `await reference.release()` 或使用 `await using`。调用方提供已安装官方 Client Sessions、Connection、Gateway 和 Program 贡献的客户端根或应用插件 Context；目标 Agent generation 的 Context 不能成为这组引用的 owner，避免会话保留自己的寿命。取得入口先通过官方 sessions.refresh 刷新共享目录，再 retain 精确代际并打开 Program SDK；需要原生目录和该会话的读取许可，不授予新权限或创建 Host 会话。

取得失败或取得期间 signal 取消，会释放本次已取得的原生引用与 SDK；信号只控制取得过程，成功后引用由显式 release 或所属 Context 退出释放。release 关闭本次 UI 观察、原生引用及独立 SDK，其他引用保留当前代际；最后一个原生引用的代际退出继续由官方 Client Sessions 执行。同步 bindNativeSession 仍只拥有 UI 观察，不释放借入对象；两种入口都不取消 Host 已受理的 Action、Operation 或模型工作。

SDK dispose 同时结束在途的 createWorkspace、describe、create、completeCreation、open 和 resume 等回执等待，并向连接传递取消信号；自定义 transport 未配合取消也不会使调用者继续挂起。停止等待不撤销 Host 已受理的登记或执行。create 请求已发出但结果尚未确认时返回 `CreationUnknownError`，保留原 environmentId、workspaceId、sessionId；用另一有效 SDK 沿同一引用显式 completeCreation，不生成新会话或自动重发。

Program name/version 是发布者声明的输入、Surface 和业务语义身份，与 npm 包版本无关。发布者为不兼容变化增加合同 version，并让宿主和消费者使用对应定义；框架不推断 schema、codec 或函数等价，也不按远程环境猜选客户端定义。`programIdentity` 提取身份，`assertProgramIdentity` 核对两份明确身份；这类核对不授予 endpoint、actor 或业务权限。

`session.submit` 的范围类型是 `ProgramInput<typeof orderProgram>`，即 `z.input`。上述合同提交 `asOf` 字符串：

```ts
const request = session.request('inspect-order');
const events = request.events$.subscribe(event => {
  if (event.kind === 'input.entered') {
    for (const input of event.inputs) showInputTime(input.scope.asOf.toISOString());
  }
});
await session.submit({
  requestId: 'inspect-order', content: '核验这份订单的当前状态', intent: 'question', delivery: 'followup',
  scope: { orderId: 'order-7', asOf: '2026-10-27T00:00:07.000Z' },
});
```

客户端只核验提交可表示为 JSON，不提前执行 Program input schema。Host 使用实际环境的合同核验原始 scope，将正文提升为原生持久附件引用后，以 `AdmittedSubmission` 和认证所得的 actor 交给应用 admit。消费前从同一持久正文复核；不会把引用伪装成原始字节或 receipt。原生受理、持久记录与后续事实保留原始 JSON scope。回执只确认受理，输入何时进入模型由后续事实说明。

content 接受字符串或有序 prompt parts，字符串只归一为一个 text part，不 trim。普通提交必须显式选择 delivery：followup 进入下一轮并唤醒；steer 在最近后续步骤参与并唤醒；inject 在最近后续步骤补充现场，本次投递不请求唤醒。已有步骤可能已经 claim 输入，因此 inject 不承诺赶上正在进行的模型请求。冷会话提交需要加载原生实例，已受理子工作仍可沿原授权恢复并产生模型活动；inject 不暂停或撤销既有 Goal 和业务工作。业务 intent 不授予原生 Goal 权限，`intent: 'result'` 必须携原 continuation/wait，禁止提供 delivery，继续由框架核对原工作的续接条件。

```ts
const upload = session.uploadFile(fileBytes, { name: 'order-details.pdf' });
const uploaded = await upload.result;
await session.submit({
  requestId: 'order-details', intent: 'followup', delivery: 'inject',
  scope: { orderId: 'order-7', asOf: '2026-10-27T00:00:07.000Z' },
  content: [
    { type: 'text', text: '下一次核验时使用这份资料。' },
    { type: 'file', receiptId: uploaded.receiptId },
  ],
});
```

`uploadFile` 接受 `Uint8Array | Blob`，在本地和远程均同步返回 `FileUploadInvocation`，不提供 thenable。result 等待同一次上传的官方 `FileUploadValue`，wait(options?) 额外限定本消费者的等待。调用对象的 source 为 `{ kind: 'file-upload', sessionId }`，id 只关联本次内存调用，不能作为跨重启恢复或重传去重的凭据。Program SDK 使用有界 unary endpoint；Gateway wire 只传 `FileUploadValue`，不传 Invocation 或客户端活动流。服务端通过 Handle.uploadEncodedFile 接原生 FileUploads，保留 canonical base64 校验，不通过先解码再编码接受非法输入。

Uint8Array 在调用时复制，Blob 的异步转换在准备阶段响应取消；转换退出后的迟到字节不再触发 HTTP 或激活会话。options.signal 约束准备及分派前检查，也作为 wait 的默认信号；分派后该 signal、Session release 或 UI dispose 只拒绝 wait，已启动 HTTP 的真实回执继续结算 result。UI 退出在分派前仍可取消准备。所属 SDK 或 Connection 物理退出可能使远程 result 拒绝为客户端公开的 `FileUploadReceiptUnknownError`；它保留 sessionId 与 cause，表示未取得正式回执，不证明服务器没有文件，也不能通过调用 id 查回原 receipt。框架不自动重传。

上传活动是本次内存调用的有限事实，activity 与 activity$ 同源，可 read、peek、组合或交给 hooks。running 只表示 SDK 已分派，不证明 Gateway 或存储已受理；失败保留为 ready.value 的 failed 事实，晚读仍能取得终态，不由 Session 或 UI 的观察退出改写。

图片也可以在 content 中提供官方 image part 的 mediaType、canonical base64 data 和可选 name。文件输入只允许 receiptId，不接受客户端自报持久文件引用。receipt 属于实际接收上传的原生 Session 实例，未提交时不能跨 Agent 关闭或进程重启存活；单纯释放 SDK 观察句柄不关闭 Agent。上传本身不投递模型输入。

组件 `inputs$`、本地 typed 句柄的 `events$` 和远程 `events$` 使用同一 Program 合同解析各自的读侧，得到 `ProgramParsedInput`，即 `z.output`。上述事件的 `scope.asOf` 是 Date。`decodeProgramInputs(program, sessionId, inputs)` 与 `decodeProgramEvent(program, event)` 是共享解码接点；每条输入必须带真实 `sourceSessionId`，本会话输入中混入 foreign 来源会明确失败。本地 `create(environmentId, { workspaceId, sessionId? })` 从已装配环境推导 Program；动态 `open` / `resume` 要取得领域类型时，显式传入预期的纯 Program，由 Host 核对定义身份。

远程 Host 直接读取与本地 typed 事件共享事实切面的 `rawEvents$`，传输原始字符串；客户端再以自己的纯合同解析为 Date。原始值始终存在，因此输入可以使用单向 transform，不需要反向执行 schema。宿主准入与本地、远程读侧的解析各有责任，读侧解析不重新提交输入或重复业务调用。

调用 `resolveAction` 返回的函数在网络边界取得 `Promise<Invocation>`。它保留 Host 生成的调用身份、可信 source、`activity$` 和 `result`，有 Operation 合同时还提供 `operation`。`result` 是 Action 回执，不表示业务操作终态。客户端只核验输入可 JSON 表达，将原始 `z.input<I>` 原样投递；唯一的 Action input parse 在 Host。纯合同中没有 execute；宿主通过 `implementAction` 提供实现。

本地与远程 SDK 的 `viewResource(name)` 返回同一槽的稳定只读资源，提供 `peek()`、`read(signal?)`、`subscribe`、`observations$` 和 `values$`，可以直接交给 `useResource` 或原生 UI 的 `resource`。它借用已有 View 状态，读取和订阅随当前 SDK 句柄退出，不创建第二份缓存或来源连接。动态集合的 `instance(key).viewResource(name)` 使用相同入口：资源固定逻辑地址，模块撤下报告 error，同地址重新挂载继续观察新来源；它不具有 Action 的绑定身份。

`session.module(mount)` 将同一 SurfaceClient 重定位到模块的局部槽名；本地、远程与集合成员使用同一入口，返回类型保留 mount.selected、调用选项及原同步/异步时序。mount.surface 仍是实现的原合同，mount.selected 是实际公开子集。局部 viewResource 与根地址取得同一资源对象，不复制缓存或取得新的 owner；继续 module 时核验父原合同及已经公开的子集，不能重新暴露父选择隐藏的槽。局部对象沿原 Session 释放，不单独建立连接或 release。

远程会话的 View 与 human 查询在首个消费者读取或订阅时才连接 Gateway；open、构造资源、取得 views 值流和 peek 不发起这些查询。相同资源的多个消费者共用一次连接；最后消费者退出撤下当前快照并取消流，随后显式读取从新的原生 baseline 开始。会话 release 等待已退出的查询真正释放。View、人工观察与控制各自需要对应 endpoint 的权限，打开会话不隐式要求未消费的读侧权限；释放查询不停止 Program、Goal、Job 或已经受理的 Action。

`views.slot`、`observeView(name)` 和 `readView(name, options?)` 消费同一个读侧对象。前者发出领域值，后两者保留 pending/ready/error 或等待当前可用值。`readView(name, { signal })` 和资源的 `read(signal)` 只取消本次读取。`view(schema)` 接收普通值流；`resourceView(schema)` 明确接收现有 Observation 流。pending 立即撤销旧 ready，error 不伪装成空业务值，同源后续状态可以恢复。Program 持有 source 连接，外部观察者退订不停止业务连接。

视图来源已经是 `z.output` 领域值；Host 在每次 ready emission 调用同一 schema 的 `z.encode` 一次，remote 读取已编码 wire 状态，客户端用 Program Surface 的 `decodeView` 恢复领域值。视图需要从领域值产生 wire，所以示例中的 `updatedAt` 使用双向 Date codec；裸 `z.date()` 或不可反向执行的 transform 无法承担这一方向。输入的单向 transform 则从始终保留的原始 JSON 产生领域值。增加观察者和重连不重复编码已存在的视图 emission，编码或解码失败进入观察错误。

消费者退出时退订自己的 `subscription` 与 `events`。`session.release()` 释放该会话句柄的视图、进展、人工和 journal 连接；`client.dispose()` 释放该客户端全部连接。观察释放只结束观察，已经开始的 Action body 仍由 Host owner 持有。暂停模型、取消原生处理与取消业务任务是各自 owner 的显式行为。

## 请求、流式进展与新事件

会话活动及事件在本地与远程保留同一 operationFollows 持久事实和 operation.follow.changed 直播追加；冷恢复读取当前投影而不重放事件。跨会话工作观察的整体资源要求所有显式来源 ready，每个来源也有独立局部资源；来源退出不使其他局部资源失效，也不改变权限。

`session.request(requestId).state` 与本地句柄提供相同的资源读侧，支持当前 read、peek、observations$ 和 useResource；`state$` 是该对象的完整 Observation 流。其 `events$` 从同一会话事件流选择精确关联的新事实，保留 Program 的领域输入类型。会话级 `progress` 资源提供当前活动与活跃 attempt 前缀，`progress$` 是同源观察，`progressFrames$` 提供原生 start/chunk/end 增量。当前活动、attempt、原始流帧及 wait continuation 内的输入范围保持 JSON，只有本会话 typed 事件与组件输入读侧按 Program.input 解析。

一个 Session 的这些入口共享一条进展连接，首次取得任一进展入口时建立，连接由会话句柄持有到 release。只消费视图或 Action 不额外连接进展，也不要求 progress grant。某一个观察者退出不重启连接或取消工作。

请求状态的 read 只等当前有效事实，不等待业务或 Goal 结束；activity 为 null 表示当前快照没有该请求，不代替 requestStatus 的受理查询。requestStatus 沿 exact 会话访问取得原生 inspection：活跃会话先 flush，冷会话直接读持久日志，并在读取前后核验工作区成员和 Agent 代际，再读取同一原生投影。失败与取消直接拒绝，不解释为 absent，也不回写另一份观察缓存。Request 不取得 Goal 控制权。资源失效会撤下旧 ready，单次读取取消不关闭会话连接；释放句柄后，捕获的进展与请求资源随原 SDK 寿命退出。原生 UI 的 progress 和 request(id).state 借入这些来源，UI 释放只退出自身观察。

开场与重连交付完整当前快照，之后仅传活动变更、原生流帧与新事件，不逐 token 重传累计前缀。客户端使用 core 的同一累积接点与官方 AssistantStreamAccumulator 物化本地快照；RemoteSnapshotStream 负责代次切换。开场不会伪造 start/chunk；断线期间已经结束的瞬态 attempt 不承诺补回，持久历史从 journal 读取。

execution 表示会话活动，不代表某个请求独占执行。一个步骤共同消费的请求关联同一工具和回复；settled 仅表示关联轮次结束，后续 Goal 轮次仍能处理原工作。模型错误保留可证明的步骤输入，装配前错误不借用上一已完成步骤的来源；Error 使用 Gateway 正式 failure 编解码，不把本地异常序列化成空对象。

`ProgramFailure` 从官方 `RemoteStreamFailure` 派生，保留 `code`、`message` 与 `details`；details 在 Typert wire 边界限定为 canonical JSON 对象或数组。业务通过官方 `RemoteErrorDetailsMap` 声明错误 code 对应的 details，并抛出 `RemoteError`；SDK 在 unary 结果、Invocation activity、视图及 Operation 的 Observation.error、execution.error 事件中保留这些公开字段，读取便利方法据此抛出相同错误。消费者可按 code 判别，状态观察不要求从原生异常文本猜测业务事实。未声明的普通异常继续由官方 `wireStream.failure` / unary Gateway 分类：普通 Error 使用 `gateway/internal`、其 message 和空 details，不公开 cause 或任意自定义属性。

## 命令身份与未知受理

本地和远程 Action Invocation 的 `activity` 都是 `ResourceReader<InvocationActivity>`；`activity$` 直接引用它的 values$，result 继续表达业务返回。`activity.read()` 读取当前事实，可能是业务 pending/running，并不等待业务完成；业务 failed 保持为 ready.value，观察错误保留为资源 error。远程 Action activity 的原生来源在 returned/failed 后释放连接，但资源观察继续归 SDK owner；SDK 有效期间晚读取仍取得终态，SDK 退出会撤销这些终态的可用性。本地不可变调用事实未额外指定读取 owner，沿原事实来源有限完成。等待业务完成使用 result；需要等活动终态时按 state 选择 returned/failed，不以流 EOF 作判断。远程 Action 资源初次等待真实活动帧时保留 pending，不从调用回执猜业务阶段；SDK 退出结束资源读取与订阅，不撤销 Host 已执行工作。原生 UI 的 invocation 入口接受不同 source 类型，直接借用同一资源和 Observation，不另存一份活动状态。

尚未取得终态的活动连接断开时，资源立即进入 pending，重连后从同一调用取得真实当前帧；取消某一次 activity.read 不关闭共享观察，也不重发 Action。已确认的 returned/failed 不因观察流清理失败改写，清理错误保留诊断日志；业务结果、观察可用性和连接退出分别表达。

`create(environmentId, { workspaceId, sessionId? })` 在精确获准的 Workspace 中使用指定 Environment 创建会话；创建目录和注册目录由调用前的应用流程负责。`completeCreation(reference)`、`open(sessionId)`、`resume(sessionId)` 分别表达补齐登记、只读打开与显式激活。只读打开及观察不会创建 Agent 或调用模型。人工会话的 `resumeGoal({ id, revision }, { signal })` 对 live 会话执行原生 CAS，恢复指定目标的续行并返回 ProgramGoalControl；`pause()` 暂停当前原生 Goal，`cancel()` 停止原生推理及待处理输入，外部业务任务仍由业务 owner 管理。

`resumeGoal` 使用独立的 `reactivePrograms/resumeGoal` Session grant，参与者身份始终来自认证映射。服务客户端没有这个方法，直接构造 RPC 请求也不能取得人工控制权。调用开始时捕获 expected 引用；旧引用不会随调用者修改对象而改指新修订。等待取消不回滚已经开始的原生控制，也不自动重试。

原生 Goal 的拒绝通过 `RemoteError` 的 `reactive/native-goal` 保留原始 `details.code`，例如 `GOAL_STALE_REVISION`；驱动不可用通过 `reactive/goal-round-driver-unavailable` 保留 `details.reason`。这些错误沿原生 CAS 与实际依赖核验产生，传输层不推断 expected/actual 或重新执行控制。

```ts
// Workspace 已由应用显式注册并授权；它不由 Environment 隐式产生。
const created = await client.create('order', { workspaceId: authorizedWorkspaceId });
```

`CreationIncompleteError.reference` 表示 Host 明确报告：原创建尚未确认，Workspace 登记、Agent setup 或最终持久回执仍未完成。create、completeCreation、open 和 resume 均保留这个类型及准确 `{ environmentId, workspaceId, sessionId }`；普通读取不提前签发句柄，也不自动续做创建。沿原引用显式调用 `completeCreation`，该入口先核对精确 Workspace 的资源权限，再由 Host 核验真实存储与成员权威。`CreationUnknownError.reference` 保留相同三项身份，表示回执不能证明创建结果，不能据此改选 Workspace 或另建会话。

原生子工作已有明确 command/reference、但创建尚未确认时，宿主抛出 `ChildCreationIncompleteError`。远程统一映射为 `RemoteError`，code 为 `reactive/child-creation-incomplete`，details 为 `{ command, reference }`，保留原 `parentSessionId`、`invocationId` 与 `childSessionId`。Action 开始/结果、Operation 槽准备/读取及显式 resume 都使用同一个映射；原始 cause 不进入 wire。它与网络回执未知保持各自语义，消费者沿原 command/reference 核对既有事实；错误映射不恢复工作、不猜测 messageId，也不重发子输入。

`submit` 的受理回执不表示模型已经消费或业务完成。请求关联记录保存原请求 sourceDigest、已提升的正文和原生消息 digest；重试核对相同 actor、scope、intent、delivery、归一正文与续接信息。已持久的 prepared record 可以在尚未入 inbox 时沿原 requestId 继续受理，使用原持久正文，不重新解析已经失效的 receipt。已入 inbox、已消费或已结算的同请求也不重新提升或投递。declared、accepted、consumed 与 settled 分别来自准备记录、原生 inbox、user/message 与相关轮次结算，不能互相代替。

回执丢失时，`SubmissionAdmissionUnknownError` 保存准确 `sessionId`、`requestId`；调用者用 `requestStatus(requestId)` 查询原生事实，再决定是否用原身份和原内容显式重投。客户端不自动重发，inject 重试也不能获得唤醒资格。网络试验覆盖了 Host 已受理后丢弃 HTTP 回执，再按原 ID 查询与显式重投，模型没有重复处理。

远程 Action 在业务 JSON 校验和预取消检查之后生成 UUID invocationId，并在必填的 `ProgramActionRequest.invocationId` 中发送；每次调用只分派一次，不自动重发。传输抛错、Gateway 内部或取消结果，以及 ACK 的 id、名称或 Action 合同矛盾，由客户端保留为 `ActionAdmissionUnknownError(sessionId, name, invocationId, cause)`。未知不代表业务未执行：使用原会话端口 `session.invocation(actionName, error.invocationId)` 只读查回，不找最近调用或直接重发。请求仍在路上、身份未知或过期、Host 重启时查不到，也不能证明未执行。自行构造 TypedRPC 的调用方必须提供 UUID invocationId 并保持协议版本一致。

ApplicationHost 的实际 Action owner 持有唯一 Invocation 回执索引，Gateway 只编码和观察原调用。`session.invocation(slot, id)`、Module 和嵌套 Collection 的局部 invocation、result 和 activity 查询不再次 execute，也不依赖当前 Expose。查回核原参与者、Session、Program、Action 合同和完整地址；同合同的另一个地址不能取得回执。本地同步返回原 Invocation，远程异步返回同一回执的读取对象。默认保留已完成调用 5 分钟，最多 1000 个；`completedRetentionMs`、`maxCompleted` 配置属于 RuntimeOptions/HostOptions。执行中的调用不因这项保留策略淘汰，断线也不取消 body。过期、未知或服务重启后的进程内记录缺失返回 `reactive/invocation-unavailable`，不会重放 Action；长期业务恢复使用自己的 Operation identity。

## Operation 与人工参与

`await invocation.operation` 从同次 Action 回执及纯 Operation 合同取得只读 core OperationHandle。客户端以原 Invocation 地址核验并观察实际 Operation 句柄，复用 core 的 state parse、共享 Observation 与 `settled()`，不重新取得会话或通过任意 reference 定位。reference/command 按对应 schema input 传输，解析后的值通过同一 schema encode 恢复 wire；身份合同须可逆。

`attachOperation(definition, reference)` 只连接既有事实，`locateOperation(definition, command)` 只查询受理事实；unknown 不是许可重发。查询错误进入 Observation.error，不改写已返回的 Action 回执，也不冒充业务失败终态。有限查询以 ready/error 完成时保留最后事实，空或 pending 完成是错误。

`await session.operations.slot()` 通过正式 resolver 取得当前 Expose 绑定的原 command，再返回只读 BoundOperation。其 locate、attach 与 participation(id).readFeedback 使用已捕获的合同和 command；组件后来切换对象或撤下槽，不会把旧句柄改接另一操作。resolver 仅解析当前入口，不开始观察、业务任务或新的 Goal 等待。

`readOperationFeedback(definition, command, participationId)` 可以独立恢复读取已登记参与，不要求当前槽存在或 Agent 存活。它核对真实持久 link、合同、command 与 owner 的读权，不调用 registerFollow；返回单次 unknown/rejected/pending/ready 快照，ready 的 `{ id, payload }` 通过同一反馈 schema 编解码。payload 可用双向 codec 保留 Date 等领域值。读取及认证错误明确失败，不伪装成业务拒绝。

新的持久等待通过程序声明的 Follow 原生工具显式登记。每次真实调用取得本次工作输入和当前 Goal 修订，以 after 选择 owner 节点之后的反馈；普通 SDK 查询没有这份权限。旧参与固定交回的节点不会被下一轮节点覆盖，再次观察也不重提交原业务命令。

`notifyOperation(followId)` 只请求 Host 根据已持久 follow 和真实业务 owner 再核验事实。通知 actor 来自认证；结果投递 publisher 由可信 Host 固定并经应用 admit 准入，客户端不能自报。返回 `unknown`、`pending` 或 `accepted`（含原生输入回执），accepted 不表示模型已消费。

`questions$`、`foregroundQuestions$` 来自当前 Handle 的真实 Human owner。`answerQuestion(callId, answer, options?)` 委托当前程序的 HumanInput 回答准入，bool 仅说明前台受理或 continued 排队，不是答案持久化或工具结算。`attachWait(callId, { signal })` claim 真实前台等待；退出或取消释放 claim，既不自动回答，也不跨断线自动重新 claim。

本地与远程 SDK 的 `human` 资源提供同一批 questions 和 foreground 事实，远程 `human$` 是其完整 Observation 流。`question(callId, options?)` 只绑定当前快照中已知的原问题并复用对象，返回固定 callId、state、answer 与 attachWait。state 保留 active、foreground 和 settled 的原生形态；前台超时、迟答、冷恢复和取消均不靠 SDK 自建阶段推断。服务来源不能借这些入口作答或控制 Goal。

前台、持久 active 与 settled 问题均携带与本地相同的 origin：原 sessionId、实际工具调用及其完整原始输入集合。客户端可据此把问题交给对应请求或业务模块，无需订阅并反查工具历史。多输入问题保留全部来源，JSON 范围不按客户端 Program 转换；origin 是关联事实，响应许可仍由认证入口与当前 HumanInput 策略核验。

## 官方流与发布边界

unary 使用 Connection 的 `createSharedFetchHandler` 与 `createPeer`；流使用 Gateway 的公开 `wireStream.open` 和官方 stream-protocol。carrier 只负责已认证 physical bytes、大小限制、backpressure、abort 与 uplink，不维护业务历史。客户端通过官方 `RemoteStream`、`RemoteSnapshotStream` 与 `RemoteJournalStream` 完成代次切换、完整基线和可靠 journal 消费。`@hanger-source/dsh-reactive-client/types` 持有精确 wire DTO，`/errors` 持有共享错误合同，`/remote` 持有由官方 Host generator 生成的贡献；它们不依赖宿主 Controller。构建先产出这些纯合同，再生成 Host metadata 与 client 贡献，最后构建 SDK，不手写或猜测远程 schema。

本地与远程会话共用 core 的 `SessionJournal` 合同：`session.journal.page(query, options)` 读取原生消息分页，`session.journal.follow(window, options)` 交付开场 cut/page 与之后的原生 entry。Gateway 转发 Host 的同一读侧，不再单独拥有监听、cut 或分页装配。原生读取借确切 Context 中已安装的 Controller/Query，缺服务报告 `SessionJournalUnavailableError`，不隐装 Controller，也不调用会 promote Agent 的 Controller.follow。

`session.openEvents({ publish, failed }, request, { signal })` 在相同 journal 上提供官方 RemoteJournalStream 的缺口修补、分页、重连及游标顺序；不增加 epoch/sequence 协议或事件重放引擎。第三参数只取消本次开场，已取得的连接仍归自身与原 Session owner，开场失败和清理失败同时发生时保留两者。冷读进展、日志和分页都不创建 Agent 或发起模型调用。

官方 `api-remotes` 的公开 apply 固定包含交互式 waterfall，未提供关闭交互转发的 Config 或 source factory；在没有 owner UI 的部署中它会阻塞当前 HumanInput。装配因此只从公开 `API_REMOTE_FORWARDED_EVENTS` 取实际 `api-session/*` emit 事件及原 resource reader，监听真实 Controller 事实后登记 Remote source。人工交互由 Program 的 HumanInput owner 负责。

Typert contribution 由官方 generator 分析真实 Service 与公开类型后生成。当前发布版分析器要求依赖包出现在 workspace registration inventory；构建脚本使用公开 `WorkspaceCaches.registrations`，从实际 npm manifest、exports 与 d.ts 登记 protocol/util-values，再交官方 analyzer/emitter。诊断不关闭，不手写 endpoint descriptor 或深层导入。

`./client` 发布 Node ESM 与普通 Browser ESM。Browser bundle 保留 core 为共享外部合同模块，使 Program、Surface、Action 与 Operation 合同使用同一实例的品牌；消费者的纯合同模块也应解析到这份 core。单份 core 不构成跨进程合同等价证明。浏览器入口不包含原生 WebUI ModuleLoader 插件；正式应用仍需验证自己的认证、反向代理、模型 provider 与业务 owner。

远程资格观察沿既有资源流传递；资格错误码为 `reactive/eligibility`，与本地 `EligibilityError` 保留同一语义。`operation.follow-unregistered` 是 Follow 未进入登记的持久事实，不表示原 Operation 未开始。
