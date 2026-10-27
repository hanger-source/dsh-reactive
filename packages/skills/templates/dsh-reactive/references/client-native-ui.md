# 客户端、输入与可选原生 UI

适用版本：`{{PACKAGE_VERSION}}`。

## 用同一纯 Program 消费

`session.requestStatus(requestId)`、`pause()`、`cancel()` 的回执等待归该 Session 句柄；release 或所属 SDK dispose 后立即拒绝尚未返回的等待，公共 Context 与其他 SDK 保持原寿命。Host 在取得句柄后、进入实际读取或控制前重新检查请求取消。取消客户端等待不等于撤销已经进入原生控制的工作，也不回滚已受理业务；用另一个有效句柄观察当前事实，不因回执未知自动重发控制。

远程 SDK 从 `@hanger-source/dsh-reactive-client` 导入，不是 Gateway 包的 `/client`；`@hanger-source/dsh-reactive-remote` 只装在认证 Gateway 宿主。纯 Program 文件不能导后台组件/宿主，从而让 CLI、浏览器、UI 共用定义。

```ts
import { createProgramClient } from '@hanger-source/dsh-reactive-client';
import type { CancellationOptions } from '@hanger-source/dsh-reactive';

// InventoryProgram 来自应用的纯合同文件；认证 transport 由应用提供。
const client = await createProgramClient(InventoryProgram, authenticatedTransport);
const session = await client.open(sessionId);
const options: CancellationOptions = { signal };
const facts = await session.readView('facts', options);
const save = await session.resolveAction('save', { sku }, options);
const invocation = await save({ note: '交付前确认包装' }, options);
const receipt = await invocation.result;
```

local resolveAction 同步，remote 异步；统一代码 `await` 两者。预期 scope 是调用者持有的**原始**目标，用同 schema 与当前绑定核验；不要把 scope 重复进业务 input，不按当前页面重定向。捕获后保 bindingId，撤下/替换/重挂硬拒旧句柄。接收 ActionAdmissionUnknownError 时不自动重发；定位已有 invocation/Operation 或按应用幂等合同处理。

创建尚未完成时，create、completeCreation、open、resume 都可能返回 `CreationIncompleteError`，reference 保留原 environmentId、workspaceId、sessionId。原生已有成员或内存中已有完成事件仍不能证明创建已 ACK。沿原引用显式 completeCreation，不把 open/resume 写成隐式创建恢复，也不换一个会话重试。

client.dispose 或借入的 Context 退出时，在途的 createWorkspace、describe、create、completeCreation、open、resume 回执等待都会结算。SDK 向 transport 传递取消信号，也独立结束自己的等待；这不撤销 Host 已受理的工作。create 已发出而尚未确认时返回 CreationUnknownError，保留原创建引用，之后用有效 SDK 显式 completeCreation。已经退出的 SDK 拒绝新调用；借用 SDK 退出不关闭公共 Context 或其他 SDK。

动态集合：`session.collection('items').instances$` 观察实际成员；`collection.instance(key).readView('facts', options)`、`.resolveAction('save', rawScope, options)`、`.operations` 消费同一局部合同。不要维护第二张 key→工具名表或解析命名 namespace。

`session.viewResource('facts')` 与动态实例的 `viewResource('facts')` 返回稳定只读资源。直接使用 `resource.read(signal)`、`peek()`、`subscribe`、`values$`、`observations$`，或将同一对象交给 `useResource(resource)`、`nativeSession.resource(resource)`；不要再复制 BehaviorSubject 或缓存。SDK release 结束该对象的读取与观察，取消某一次 read 不影响其他消费者或原 Program。动态资源固定逻辑地址，模块撤下报告 error，同地址重挂继续更新；这不改变 Action 旧绑定必须拒绝的规则。

readView 返回当前有效业务值，observeView 保完整 Observation；这些入口共用 viewResource 的状态，views 是值流，不能用它声称失效仍有效。progress 和 request(id).state 是同源资源，可 read、组合 observations$ 或交给 useResource；progress$/state$ 直接引用相应资源的观察，events$ 只交付已确认关联的新事实。一个模型步骤可消费整组输入，不能承诺每请求独占一轮答案，也不能把 read 当前快照当作等待业务或 Goal 完成。

Program 的 conversation.progress 与 conversation.request(id).state、SDK 和原生 UI 的对应入口都借入原进展 owner。首次显式取得 SDK 进展入口才建立远程连接；UI 绑定本身不要求 progress grant。原生 UI 的 native.progress 与 native.request(id).state 保留 pending/error，native.request(id).events$ 保留原事件与当前 UI 的观察寿命，不另存模型输出或按消息正文猜请求。

## 输入保 intent 与 delivery 独立

```ts
await session.submit({ requestId: stableRequestId, intent: 'question',
  scope: { sku }, content: '请确认当前库存', delivery: 'followup' }, options);
```

普通 Submission 必填 content 和 delivery；旧 instruction 已退出。content 是 string 或有序 PromptContentPart[]，string 保原文，仅归一 text。

- followup：原生 next-turn，唤醒；不保证每条输入独占模型 turn。
- steer：原生 nearest-step，唤醒。
- inject：原生 nearest-step，不唤醒；明确写入待消费现场。

intent 描述 question/goal/followup，不能代替 delivery；goal 必须有实际非空文字作为目标。result 分支禁止 delivery，只由可信关联核验沿原 Goal 续接，不给调用者自报 Goal 权限。

附件先 `const upload = await session.uploadFile(bytesOrBlob, { name, signal })`，再将其 receiptId 放入 `{ type:'file', receiptId }` 正文。receipt 绑定 exact 会话实例，未提交而关闭会失效，不接任意持久 file ref。首次提升后的正文持久保存；同 requestId、同请求内容可复用 prepared 事实，重试不能先重新消费已退役 receipt。正文、scope、来源或 delivery 不同不能重用原幂等身份。

`editQueuedInput(requestId, content, options)` 只编辑同操作者尚未消费的原请求，保初次目标/来源/顺序，不唤醒模型。`InputEditAdmissionUnknownError` 不触发自动重投。

共享取消对象统一 `{ signal }`，不传裸 signal。remote 不允许 actorId，认证端口决定真实身份；local SubmitOptions 的 actor 仅供可信宿主。取消某次读/解析/受理等待，不意味着取消已经执行的业务。OperationHandle.settled 和各次观察有各自读取寿命；明确业务 cancel 沿 owner。

## 可选原生 UI 借入已有对象

安装 `@hanger-source/dsh-reactive-native-ui` 只用于 dsh 原生客户端扩展，不加载后台 renderer。原生 SlotCore、UiSession、UiConversation 仍拥有 slots、节点与会话状态，应用提供视觉组件。

`bindNativeSession(Program, exactSessionBinding, remoteSession)` 同时核真实 Program 对象、SDK definition、实际 Connection 与 exact native 代际。原生扩展在其客户端 Context 使用 `bindProgramClient(Program, ctx)`，再通过该 SDK 的 open 取得 Session。Context 的装配 owner 负责登记一次 Program 远程贡献：

```ts
import { bindProgramClient } from '@hanger-source/dsh-reactive-client';
import programRemotes from '@hanger-source/dsh-reactive-client/remote';

// 原生客户端装配阶段，ctx 已安装 Connection 与 Gateway。
await ctx.remote.$mount(programRemotes);
const client = bindProgramClient(InventoryProgram, ctx);
const sdk = await client.open(exactSessionBinding.sessionId);
const native = bindNativeSession(InventoryProgram, exactSessionBinding, sdk);
```

该 Context 必须允许 `interactions:'answer'`。多个 SDK 可以借用同一装配；SDK dispose 只退出自己的会话和观察，Context 退出关闭全部借用 SDK。独立 createProgramClient 拥有另一份 Connection，适用于独立客户端；原生 UI 按实际连接归属绑定，不能用相同 URL、sessionId 或复制出来的同形句柄代替。

需要整体取得并拥有引用时，使用 `const reference = await retainNativeSession(Program, clientContext, sessionId, { signal })`，消费 `reference.session`，退出时 `await reference.release()`，也支持 await using。Context 由客户端根或应用插件拥有，须已有官方 Client Sessions 与上述 Program 远程贡献；不要用目标 Agent generation 的 Context 保留其自身寿命。框架通过官方目录刷新、精确代际 retain 和 SDK open 完成装配，权限仍由原生目录及 Program 读取入口分别核验。取得失败或取消释放部分成果，成功后的 signal 不释放已返回引用；显式 release 或所属 Context 退出释放本次 UI、SDK 和原生引用，不影响其他引用或 Host 工作。已有 binding 和 SDK 时用 bindNativeSession，原对象仍由调用者释放。

- `nativeSource` 直接接已有 peek/subscribe；`observeNativeResource` 保 pending/error；NativeSession.resource/operation/invocation/view 将同一个对象交给原生观察。
- Invocation 的 activity 资源可直接 read、peek 或交给业务 hook；activity$ 是其 values$，returned/failed 后有限结束，result 独立等待业务完成。native.invocation(invocation) 直接读取同一个 Observation，不在 UI 重新物化活动。业务 failed 是 ready.value 的调用事实，SDK 失效或观察错误不会伪装成业务失败。
- 未终结的活动连接断开时，资源立即 pending，重连读取原调用的当前事实；取消单次 activity.read 只退出本次等待，不重发 Action 或关闭共享观察。已确认终态不被流清理错误撤销，清理失败保留日志。
- `nativeHooks`、`nativeSessionInject` 复用原生 InjectFace；注入的 `program` 不覆盖原生 props.session。`provideNativeSources` 交原 UiSession.provide 持有注册/更新/退出，不再建 registry。
- `registerNativeConversation` 组合原节点/视图/分组/渲染注册，部分失败回滚整组；所有解析与 replay 留给原 UiConversation。
- `createNativeActionViews` 与 `readActionPresentation` 读取真实调用 metadata，不用当前 Action scope 解释历史结果，也不反向 encode 单向 transform。

NativeSession.dispose 退出 UI 观察/贡献，不 release 借入 SDK/SessionReference，不取消已受理工作。代际变化或 SDK.release 后旧参与端口硬拒。人工回答走绑定的纯 SDK 与原 question.origin，不接 Controller 当前页的默认路径冒充准入。

`nativeSession.human` 保留 SDK 的完整人工快照；questions 和 foregroundQuestions 同样传递 pending/error，不在断线时继续展示旧 ready。`await nativeSession.question(callId, options)` 返回固定问题的原生 UI 绑定，state 是原生 HostObservable，answer 与 attachWait 继续核验这个 UI generation，并委托原 SDK；绑定不创建新问题、不自动 claim 或代答。

dsh rc.3 SessionController 缺 Program 创建/prompt/队列执行前接点，不能声称其默认输入框已贯通框架准入；这不妨碍 typed slots、节点、资源与明确 SDK 参与组合。后台 React 19 renderer 与原生 React 18 UI 需独立编译、独立部署。
