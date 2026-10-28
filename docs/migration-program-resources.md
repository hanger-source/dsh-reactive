# Program 资源与调用回执

`useSharedResource(source)` 将已有或派生的 ResourceSource 接为 Program 拥有的 ResourceReader；现场构造来源使用 `useSharedResource(factory, dependencies)`。它对应 `shareResource` 和 Workspace.shareResource，不要求重新定义业务资源合同、传名称或另建 Scope。factory 只构造来源，RxJS 继续组织派生和组合。

同一个 Program 按原 observations$ 共享读侧。构造不订阅，`read`、`peek`、`subscribe`、values$、observations$ 与 `useResource(reader)` 消费同一事实。最后消费者退出释放来源连接；Program 失败或退出使本读侧失效，并保留原退出原因。借入资源的业务 owner 不转移，其他消费者继续按自己的资格工作。

`useResource` 保留当前 React 提交的快照语义。需要把完整派生能力交给普通函数时，返回 ResourceReader；组件再按需用 useResource 读取快照。定义本地资源与参数化资源继续用 useResourceInstance，其局部 factory 返回资源实现选项，不猜测 factory 的返回类型。

Action 的 `activeInvocations` 是当前在途集合的 ResourceReader，activeInvocations$ 是其 values$；invocations$ 仍只交付新发生的调用。ReactionRun 的 `state` 是运行事实的 ResourceReader，state$ 是其 values$。业务 pending、draining 或 failed 仍是 ready 中可读取的事实；资源 error 表示读侧失败。`read()` 不等待业务完成，调用结果继续等 Invocation.result，反应结算继续等 run.settled。

这些有限运行事实由实际绑定或 run 持有。组件卸载、绑定 release 或 Reaction.stop 不会把已经开始的业务伪装为完成；活动集合和反应状态在实际结算后完成，晚读取保留终态。同步事件回调里的 read 与 peek 也读取同一个当前事实。

根 Session、局部 Module 与嵌套 Collection 都提供 `invocation(name, id, options?)`。本地同步返回原 Invocation，远程异步取得原回执的读取对象；名称按该 Surface 的局部动作槽解释，模块和集合负责精确投射地址。查回核验原参与者、Session、Program、Action 合同及实际地址，不依赖当前 Expose，不重新 execute。同合同的另一个地址不能取得该调用。

高层 SDK 的 Action 解析与查回通过共同错误 codec 保留 `ActionContractMismatchError` 及其 expected、actual 身份；底层 generated reply 继续返回正式 wire code 和 details。

回执保留属于 ApplicationHost 的实际 Action owner。独立 Host 或固定 actor 的 Service 可用 `readInvocation(sessionId, id, expectedProgram, options?)` 读取冻结的原 name、definition 和 invocation；它不打开、恢复或重新装配 Session。旧 Session 句柄仍遵守自己的借用资格，已有 Invocation 的结果与活动保留原执行 owner。

`completedRetentionMs`、`maxCompleted` 配置放在 createRuntime 的 RuntimeOptions 或嵌入宿主的 HostOptions，移除 Gateway 上的同名配置。默认保留完成调用五分钟、最多一千个，执行中的调用不因此淘汰。Gateway 只编码和观察 Host 的原回执；过期或进程重启后的缺失明确失败，持久业务恢复仍使用 Operation 的身份。
