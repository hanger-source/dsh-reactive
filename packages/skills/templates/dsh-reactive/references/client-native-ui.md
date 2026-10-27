# 客户端、输入与可选原生 UI

适用版本：`{{PACKAGE_VERSION}}`。

## 用同一纯 Program 消费

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

动态集合：`session.collection('items').instances$` 观察实际成员；`collection.instance(key).readView('facts', options)`、`.resolveAction('save', rawScope, options)`、`.operations` 消费同一局部合同。不要维护第二张 key→工具名表或解析命名 namespace。

`session.viewResource('facts')` 与动态实例的 `viewResource('facts')` 返回稳定只读资源。直接使用 `resource.read(signal)`、`peek()`、`subscribe`、`values$`、`observations$`，或将同一对象交给 `useResource(resource)`、`nativeSession.resource(resource)`；不要再复制 BehaviorSubject 或缓存。SDK release 结束该对象的读取与观察，取消某一次 read 不影响其他消费者或原 Program。动态资源固定逻辑地址，模块撤下报告 error，同地址重挂继续更新；这不改变 Action 旧绑定必须拒绝的规则。

readView 返回当前有效业务值，observeView 保完整 Observation；这些入口共用 viewResource 的状态，views 是值流，不能用它声称失效仍有效。progress$/request(id)/events$ 观察受理、实际模型消费、工具和回复；一个模型步骤可消费整组输入，不能承诺每请求独占一轮答案。

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

`bindNativeSession(Program, exactSessionBinding, remoteSession)` 同时核真实 Program 对象、SDK definition 和 exact native 代际。SDK必须来自同一 Program；不能只按相同 sessionId 或当前页面猜归属。

- `nativeSource` 直接接已有 peek/subscribe；`observeNativeResource` 保 pending/error；NativeSession.resource/operation/invocation/view 将同一个对象交给原生观察。
- `nativeHooks`、`nativeSessionInject` 复用原生 InjectFace；注入的 `program` 不覆盖原生 props.session。`provideNativeSources` 交原 UiSession.provide 持有注册/更新/退出，不再建 registry。
- `registerNativeConversation` 组合原节点/视图/分组/渲染注册，部分失败回滚整组；所有解析与 replay 留给原 UiConversation。
- `createNativeActionViews` 与 `readActionPresentation` 读取真实调用 metadata，不用当前 Action scope 解释历史结果，也不反向 encode 单向 transform。

NativeSession.dispose 退出 UI 观察/贡献，不 release 借入 SDK/SessionReference，不取消已受理工作。代际变化或 SDK.release 后旧参与端口硬拒。人工回答走绑定的纯 SDK 与原 question.origin，不接 Controller 当前页的默认路径冒充准入。

dsh rc.3 SessionController 缺 Program 创建/prompt/队列执行前接点，不能声称其默认输入框已贯通框架准入；这不妨碍 typed slots、节点、资源与明确 SDK 参与组合。后台 React 19 renderer 与原生 React 18 UI 需独立编译、独立部署。
