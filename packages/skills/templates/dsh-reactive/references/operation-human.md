# 长操作、可靠反馈与人工参与

适用版本：`{{PACKAGE_VERSION}}`。

## 一次调用与长期业务不同寿命

`defineOperation({ kind, version, command, reference, state, settled, feedback? })` 定义纯合同。command 是业务命令的稳定恢复身份，reference 是正式受理后的定位身份；不能用当前最新任务或调用次数替代。

`implementOperation(definition, { locate, observe, registerFollow?, readFeedback? })` 接真实 owner：

- locate 区分 accepted / rejected / unknown；unknown 不能被当作未执行而自动重发。
- observe 发完整 Observation；查询错误不伪装成业务 rejected/cancelled。
- settled(state) 返回领域终态或 undefined。阶段反馈不等于终态，业务失败/取消也可以是明确的领域终态。
- 可靠 Follow 需要 registerFollow 在副作用前持久保存精确 link，并用业务已有投递设施可靠通知。readFeedback 必须按该 link 和 after 读**已选定节点**，不能每次返回最新节点。owner 缺这两项能力时，只提供定位/观察，不能假 ACK 宣称可靠续接。

Action 的 `operation: { definition, command(input, scope, origin), reference(receipt) }` 将本次调用接到同一 Operation；实现同时传 `operationOwner`。origin 保真实 invocation/session/source，不是业务 UUID 签发器。需要业务幂等键时由业务约定正式身份。

显式调用后，`invocation.result` 是该次 Action 返回，`await invocation.operation` 是同一业务的 OperationHandle。`handle.state` 是稳定只读资源，可直接使用 read、peek、subscribe、values$、observations$，交给 useResource 或原生 UI；`handle.observations$` 是该资源的同一观察。`state.read(signal)` 取得当前有效业务状态，`handle.settled({ signal })` 另行等权威终态，业务失败/取消也作为领域终态值交付，查询 error/abort 才拒绝等待。所有消费者共享一次观察与 state schema 解析，退出不取消后台业务。来源结束仍没有业务终态时进入查询 error，settled 不永远等待；已有最终观察保留至最后消费者退出，之后显式读取重新查询原操作，不再次 execute 或登记 follow。

已有命令使用 `useOperation(owner, () => commandOrNull, deps)`；null 明确尚无绑定，不创建查询或等待。SDK 可通过 Surface.operations、attachOperation、locateOperation、readOperationFeedback 找回已有业务。不要为了观察重新调用启动 Action。

## Follow 参与由原生 Goal 精确关联

```tsx
<Offer action={start} follow={{ when: 'feedback', scope: target, mode: 'background' }} />
{existing && <Follow name="observe_delivery" description="参与原交付操作"
  operation={existing} when="feedback" scope={target} mode="wait" />}
```

这些声明不立即执行。真实原生调用时框架才登记参与，保原 Goal id/revision、整组来源输入、调用和选定反馈节点。mode 默认 wait，暂缓原工作；background 登记可靠结果关联但让当前工作继续；纯观察无需 Follow。其他等待、Goal 暂停/修订仍约束结果续接，不能用 background 越权唤醒另一 Goal。

不要由业务拼 `intent:'result'`、Continuation 或 WaitReference 声称持有 Goal 权限。普通人工/服务输入与工具来源不互换；子程序本地输入不冒充父请求，父 causes 保每条 sourceSessionId。

## 人工补充不是正式授权

Program 根声明一项 `<HumanInput admitResponder={policy} />`。policy 按 `question.origin` 的 session、actual tool call、原始 inputs 与真实 actor 核权，不能按当前页面选择或 SDK 自报用户判断。问题观察是独立快照，修改它不会改变权威关联。

回答必须恰好覆盖原问题的所有题目，每个 id 只出现一次。框架在首次异步操作前保存解析后的答案；冷晚答先验证完整批次再准入和激活，准入期间修改原对象不会改变提交内容。响应者准入和前台 claim 的等待可取消；调用者、SDK 或 Program 退出后，迟到准入结果不会继续答题。不配合取消的政策函数不会阻止当前消费者退等，政策函数内部的外部副作用仍由其自身负责。

Program 的 conversation、本地和远程 SDK 提供 `human` 只读资源，questions 与 foreground 来自同一个快照，可直接 read、组合 observations$ 或交给 useResource。人工 SDK 的 `const question = await session.question(callId, options)` 绑定该会话里已知的原问题；同一 SDK 重复取得同一个对象。`question.state` 保留原 active、foreground、settled 事实，`question.answer(answer, options)` 与 `question.attachWait(options)` 委托原响应准入和真实前台 claim，不再重复传 callId。只读绑定不创建问题、计时器或批准权；三个事实均缺失时不能推断业务取消或完成。读取与退订不回答问题、不停止原问答。

限时问题对象可从前台持续观察到 continued 和正式 settled，也可在冷 SDK 上取得迟答对象；迟答是否需要恢复实际 Agent 仍由原会话入口决定。answer 返回的 bool 只表示前台受理或 continued 排队，不能代替持久答复和工具结算。服务端口的运行时拒绝 question、answerQuestion、attachWait、pause 和 cancel，不能靠绕过 TypeScript 声明借用人的身份。

原生工具中的 Action 可以主动问人：

```ts
const answer = await call.askHuman([{ id: 'delivery-mode', question: '如何接收交付通知？',
  options: [{ label: '站内' }, { label: '邮件' }] }]);
```

不带 options 等本次答案；`{ timeoutSeconds: positiveInteger }` 返回 HumanAnswer 或 `{ pending:true, callId }`。超时后 JS 不会恢复，晚答回原会话，不能重跑后半段副作用。需要跨重启持久审核的业务仍是正式 Operation，不能把问答等同批准权。

askHuman 只接受 id/question/header/options/multiSelect；detail/intent 明确不支持。仅真实 native/PTC 工具执行来源可用，SDK/service Action 无此来源会拒绝且不启动 Agent。call.wait、completeGoal、askHuman 仅在 execute 调用体活跃时可用；保存 call 在返回后调用会拒绝。

调用体结束会取消并 drain 未完成子问，保原 return/throw；不要通过 fire-and-forget 让问答逃离父调用。SDK/原生 UI 回答沿 `answerQuestion(callId, answer, options)`，前台等待通过 `attachWait`；退出读侧不等于取消业务。
