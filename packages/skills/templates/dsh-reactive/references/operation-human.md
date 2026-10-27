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

显式调用后，`invocation.result` 是该次 Action 返回，`await invocation.operation` 是同一业务的 OperationHandle。`handle.observations$` 只读，`handle.settled({ signal })` 等权威终态；等待退出不取消后台业务。

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

原生工具中的 Action 可以主动问人：

```ts
const answer = await call.askHuman([{ id: 'delivery-mode', question: '如何接收交付通知？',
  options: [{ label: '站内' }, { label: '邮件' }] }]);
```

不带 options 等本次答案；`{ timeoutSeconds: positiveInteger }` 返回 HumanAnswer 或 `{ pending:true, callId }`。超时后 JS 不会恢复，晚答回原会话，不能重跑后半段副作用。需要跨重启持久审核的业务仍是正式 Operation，不能把问答等同批准权。

askHuman 只接受 id/question/header/options/multiSelect；detail/intent 明确不支持。仅真实 native/PTC 工具执行来源可用，SDK/service Action 无此来源会拒绝且不启动 Agent。call.wait、completeGoal、askHuman 仅在 execute 调用体活跃时可用；保存 call 在返回后调用会拒绝。

调用体结束会取消并 drain 未完成子问，保原 return/throw；不要通过 fire-and-forget 让问答逃离父调用。SDK/原生 UI 回答沿 `answerQuestion(callId, answer, options)`，前台等待通过 `attachWait`；退出读侧不等于取消业务。
