# @hanger-source/dsh-reactive-remote

将同一份 dsh-reactive Program 合同通过已认证 Gateway 提供给外部消费者。本包是宿主 Typert contributor；Node 与浏览器 SDK 独立安装 `@hanger-source/dsh-reactive-client`。Host endpoint、错误合同和参数 schema 由官方 generator 生成，业务执行继续归 ApplicationHost 与业务 owner。

## 入口

| 入口 | 责任 |
| --- | --- |
| `@hanger-source/dsh-reactive-remote` | Gateway 装配、真实 Action owner、已认证 carrier |
| `@hanger-source/dsh-reactive-client` | 独立 typed Program SDK，Node ESM / Browser ESM |
| `@hanger-source/dsh-reactive-client/types` | 纯 JSON wire 类型 |
| `@hanger-source/dsh-reactive-client/errors`、`/transport` | 公开错误合同与已认证传输接点 |
| `@hanger-source/dsh-reactive-remote/typert` | 官方生成的 Host contribution |
| `@hanger-source/dsh-reactive-client/remote` | 官方生成的 client contribution |

Gateway 依赖 core、dsh 宿主与 client 的公开合同；client 依赖 core 与官方客户端协议、连接和恢复实现，不依赖 Gateway 宿主、renderer 或 UI。`defineProgram({ input, surface })` 创建可共享的纯合同；宿主从 `@hanger-source/dsh-reactive/program` 使用 `implementProgram(program, { Component })` 提供组件。客户端导入纯 Program、Action、Operation 与 Surface 合同，不导入宿主组件、业务 handler、原生 Runtime 或服务端配置。

## 宿主

```ts
import { mountProgramGateway } from '@hanger-source/dsh-reactive-remote';

const gateway = await mountProgramGateway(runtime.native, {
  application: runtime.application,
  model: { provider: 'my-provider', model: 'my-model' },
  actor: principal => actorDirectory.actorId(principal),
  access: service => registerApplicationPolicies(service),
  carrier: { authenticate: request => authenticateRequest(request) },
});
// 停止远程入口，不取消已开始的业务 body。
await gateway.close();
```

三个应用函数分别从已认证 principal 签发 actor、登记实际权限、验证 HTTP 与 WebSocket 请求。参与者不取得 owner token。官方 RemoteAccess 分别检查读取、Action、控制、人工回答、Operation 通知的 endpoint 权限；业务 admit 和当前 HumanInput policy 继续核权。`describe()` 还按真实 Workspace/Session grant 过滤结果。

宿主须按官方 `PrincipalMap` 扩展真实身份种类；`actor` 判别 kind 后映射业务身份。只有 owner 的内建类型不能代替应用认证。

Environment 提供程序实现和指令；Workspace 是独立的原生目录身份，不归某个 Environment 所有。`createWorkspace({ directory, title? })` 只注册宿主上已存在的绝对目录，不创建目录或会话；它需要显式 global grant。`create(environmentId, { workspaceId, sessionId? })` 和 `completeCreation(reference)` 都要求该精确 Workspace 的对应 endpoint grant，注册许可不自动授予创建权。`describe()` 异步返回 `{ workspaceId, directory, title, sessionIds }[]`，不带 environmentId，并按真实资源授权过滤；同步 policy bindings 应读取应用已有的授权事实，不调用异步 describe。

```ts
declare module '@deepseek-ai/dsh-typert-protocol' {
  interface PrincipalMap {
    'application-user': { readonly kind: 'application-user'; readonly id: string };
  }
}
```

已有 Cordis 宿主也可使用本包 `apply(ctx, options)` 登记 contributor，或用 `createAuthenticatedHandler`、`serveRemoteCarrier` 接自己的正式装配。`createSharedFetchHandler`、`createPeer`、`wireStream.open` 均复用官方实现；物理 carrier 不拥有第二套 journal。

需要单独管理 Action owner 时使用 `createProgramActionOwner(ctx, application, options)`；`ctx` 必须已安装官方 Gateway。owner 复用该 Gateway 的 failure 合同，不自行归类普通内部异常。

## 客户端

```ts
import { createProgramClient } from '@hanger-source/dsh-reactive-client';
import { orderProgram } from './contracts.js';

const client = await createProgramClient(orderProgram, { baseUrl: location.origin });
const session = await client.open(sessionId);
const subscription = session.views.summary.subscribe(renderSummary);
const confirm = await session.resolveAction('confirm', { orderId });
const invocation = await confirm({ amount: '7' });
const receipt = await invocation.result;
// 若 confirm 合同声明了 operation：
const operation = await invocation.operation;
const completed = await operation.settled();
subscription.unsubscribe();
await session.release();
await client.dispose();
```

解析先核对期望业务目标，再捕获具体绑定。返回函数不会随页面改选而重定向；撤下或替换时以正式 `reactive/action-binding-unavailable` 拒绝，目标不符以 `reactive/action-target-mismatch` 拒绝。调用仍逐次核权，已进入业务执行的 Invocation 沿原 owner 结算。bindingId 是当前程序实例的身份，不是权限令牌；恢复进程后应重新解析。

Node 传入绝对 HTTP(S) baseUrl，并按认证方式提供 `fetch` / `createWebSocket`。浏览器默认用同源 fetch/WebSocket，应用须同时配置两者的可信认证载体与 Origin 策略。

客户端接受 `defineProgram` 创建的真实合同，输入及公开出口类型都从该合同推导。应用负责为目标环境选择对应合同；构造客户端声明预期合同，不会从远程环境自动推断 schema 或证明两份 schema 等价。无法按所选 schema 解析的读取明确失败，Program 合同不授予任何 endpoint 或业务权限。

Action 输入保持纯合同的 `z.input`，客户端只检查 JSON 后原样投递，由 Host 唯一 parse。返回 `Promise<Invocation>`；回执与 Operation 终态分别等待。Invocation 的 source 来自 Host。断开连接不取消已开始的 body；按原 ID 调用 `session.invocation(slot, id)` 不重 execute。默认完成记录保留 5 分钟、最多 1000 个，过期明确报 `reactive/invocation-unavailable`。Invocation ID 不是跨进程重启的幂等键。

视图使用同一 Surface schema：Host 一次 encode，客户端 decode。`views.slot` 提供领域值，`observeView` 保留 pending/ready/error，`readView` 等当前有效值。`resourceView` 的 pending 撤销旧 ready；错误不会变成空业务值。同一 Program source 的连接不随外部消费者退订而停止。Date 等跨进程值使用可逆 codec。

已声明的 `RemoteError` 在 result、activity、视图、Operation 与执行事件中保留相同的 `code`、`message`、`details`。普通未知异常沿用官方 Gateway 分类和公开范围。宿主的 `ChildCreationIncompleteError` 映射为 `reactive/child-creation-incomplete`，details 保留原 `command` 与 `reference`；它只报告明确持有的未确认事实，不隐式重发或另建子会话。

## 请求进展

```ts
const request = session.request(requestId);
const state = request.state$.subscribe(observation => {
  if (observation.status === 'ready') renderRequest(observation.value);
});
const events = request.events$.subscribe(renderRequestEvent);
// 需要 lossless 原始增量时，读取同一连接的 session.progressFrames$。
state.unsubscribe();
events.unsubscribe();
```

首次取得 `request(id)`、`progress$`、`progressFrames$` 或 `events$` 时，Session 建立一条共享的进展连接。request 句柄固定会话与 requestId；取消单个订阅不会取消该连接，`session.release()` 统一释放。读取需要独立的 `reactivePrograms/progress` Session 权限，不授予 submit、控制或业务执行权限。

`state$` 是 `Observation<RequestSnapshot>`：网络尚未建立或断线时 pending，读取或协议失败时 error，ready 内保留真实 `source`、`execution`、原生 `sequence`、`activity | null` 和当前 attempt。null 表示该快照中没有这个请求；冷快照使用 durable/unavailable，当前 attempts 为空，不激活 Agent。一次 turn 的 settled 不结束 request 观察，也不表示该请求独占一条最终回复。

`session.submit` 接受 Program input 的 `z.input`，客户端只核验 JSON 后原样投递；实际环境负责 input 解析与应用准入。本地与远程 `events$`、`request(id).events$` 使用同一纯 Program 合同和 `decodeProgramEvent`，交付输入的 `z.output` 领域类型。Host 读取与本地 typed 事件共享同一事实切面的 `rawEvents$`，不从解析结果逆推原始值。单向 ISO 字符串 → Date transform 可以直接用于输入；JSON 约束原始提交与传输，不限制读侧领域值。

每条输入的 `sourceSessionId` 保留真实来源；本会话事件的输入不允许混入另一会话的范围，合同解码遇到 foreign 输入会失败。当前 progress 快照、`progressFrames$` 及 wait continuation 中的范围保留原始 JSON，不伪装为已经按当前 Program 解码的领域输入。

开场和重连交付当前 attempt 的官方 compact prefix；之后传原生 start/chunk/end 增量，客户端通过同一 core helper 和官方 accumulator 物化状态。`progressFrames$` 保留 block、usage 等原始 chunk，不能用便利 text/tool 事件重建完整流。`events$` 只交付精确关联的新事实，基线不会变成新 token 或消息；多个输入可以共同归属同一 attempt、工具或回复。断线恢复当前状态，不承诺补回断线期间的瞬态事件。

## 只读 Operation 参与

Surface 的 `operations` 保存纯 Operation 合同，宿主通过 Expose 贡献按原 command 绑定的 `useOperation`。远程 resolver 捕获当前公开绑定，不开始业务、登记等待或取得当前 Goal 的续接权：

```ts
const preparation = await session.operations.preparation();
const location = await preparation.locate();
if (location.status === 'accepted') {
  const operation = preparation.attach(location.reference);
  // operation.observations$ / settled() 继续读取该正式操作。
}
const participation = preparation.participation(registeredParticipationId);
const feedback = await participation.readFeedback();
if (feedback.status === 'ready') {
  showFeedback(feedback.node.id, feedback.node.payload);
}
```

resolver 经 `resolveOperation` endpoint 取当前槽的真实 command。已经取得的 preparation / participation 保留原 command 和参与 ID；组件改选或撤下不使它们改读新对象。已知原关联时，可用 `session.readOperationFeedback(definition, originalCommand, participationId, { signal })` 显式恢复读取，无须恢复原组件或激活模型。

`readFeedback()` 是单次权威快照，返回 unknown、rejected、pending（含正式 reference）或 ready（含稳定节点 ID 与 payload）；不承诺等待未来节点，也没有隐式轮询。查询或权限错误仍抛错，不包装成业务 rejected。Host 读取精确的持久 link 并核验合同、command 与当前访问权，读取不调用 registerFollow。

Operation 的可选 `feedback` 是共享 Zod schema。owner 返回 schema input，core 解析领域 payload，Host 用同一 schema encode，客户端 decode；反馈也可包含通过 codec 表达的 Date。`after` 是业务 owner 的节点 ID，不是客户端自行排序的游标。AI 通过独立 Follow 原生工具显式登记下一参与，其新 Goal 续接来自本次真实工具调用；普通 SDK 只读入口不登记新等待。旧参与的已投递节点保持固定，后续节点由新的参与交回。

## 会话、人工与恢复

- `createWorkspace` 显式注册目录；`create(environmentId, { workspaceId, sessionId? })`、`completeCreation`、`open`、`resume` 分别创建会话、补齐部分创建、只读打开与显式激活。`pause` / `cancel` 控制原生处理，不取消外部业务操作。
- 创建明确部分成功抛 `CreationIncompleteError.reference`，回执未知抛 `CreationUnknownError.reference`；引用均含 environmentId、workspaceId、sessionId，沿原引用显式核对或完成。
- `submit` 回执未知抛 `SubmissionAdmissionUnknownError(sessionId, requestId)`，用 `requestStatus` 查原事实；客户端不自动重发。Action 未知受理抛 `ActionAdmissionUnknownError`，由业务 Operation command/locate 核对，不自动再次 execute。
- `attachOperation` / `locateOperation` 复用 core 只读 Operation owner 的 codec、共享观察与 settled；`notifyOperation` 委托 Host 读取持久 follow 并再次核权，不能自报业务完成。
- `questions$`、`foregroundQuestions$`、`answerQuestion`、`attachWait` 委托真实 Human owner。回答 bool 不是工具结算；退出 claim 不会自动答复。
- `openEvents({ publish, failed })` 使用只读的 `reactivePrograms/journal` / `journalPage` Session 权限。Host 先监听真实 Session 追加，再取得原生 opening cut 并委托 Controller.page 读取；不调用会后台激活 Agent 的 Controller.follow。消费者仍收到官方 RemoteJournalStream 的完整 journal change，缺口、分页、游标和重连由 Gateway 处理。

## 构建与证据

在仓库根使用 Node 24：

```sh
npm run build
npm run test:remote
npm run browser:remote
```

构建先编译 client 的纯合同，再由 Gateway 的 `generate` 使用官方 WorkspaceAnalyzer/FaceModelEmitter 生成 Host 与远程贡献，远程贡献正式写入 client 包，最后构建 Node/Browser SDK。公开 registration inventory 来自实际 npm manifest、public exports 与 d.ts；没有手写 descriptor、deep import 或虚假 client marker。Browser ESM bundle 只把 core 保留为共享外部模块，避免重复合同品牌。journal wire 类型属于 client 的纯数据合同，实际分页仍由宿主的官方 Controller 完成，不为类型依赖把 Controller 的原生 peer 树安装到客户端。

真实 HTTP/WebSocket 场景覆盖非 owner 读写分权、原始 Action input 只 parse 一次、断线后的原 Invocation 查询、resource 状态、Date codec、Operation、人工回答、原生 journal、受理后丢 ACK、创建介质拒写及控制命令。请求场景用同一步的两份原始 ISO 输入，核对持久记录及 wire 保留字符串，本地与远程事件分别得到 Date，actor 与来源会话保持真实。Operation 参与场景使用真实 SQLite 关联与原生 Follow，核对后续节点、槽替换/撤下及冷读取；读取与 notify 分别授权。浏览器场景分别检查 Action/Operation 和纯 Program 请求，使用普通 Browser ESM，不代表原生 WebUI ModuleLoader 插件或业务部署验收。

完整认证、失败与发布边界见 core 仓库的 `docs/transport.md`。
