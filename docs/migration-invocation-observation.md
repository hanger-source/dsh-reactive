# 业务调用与原生发生的关联

同一个 Action 的 Invocation 表达业务函数的调用，ToolActivity 表达原生工具管线的发生。业务函数已经返回后，原生后置策略仍可能拒绝交付或变换内容；两项结果分别观察，业务 returned 与工具 failed 可以同时成立。

同一会话的 `progress.activity.calls`、请求的 tools 和 Work 的 calls 都提供 `invocationId: string | null`。它来自真实 Invocation 创建后的原生关联事实，与 `Invocation.id` 直接相等。provider 重复使用 callId、rootCallId 或 PTC 子标签时，仍可精确连接，不解析不透明 ID，也不按标签选择最近调用。

原生 `tool.started` 先说明工具发生，`tool.invocation` 随后说明本次工具创建了哪个业务调用，`tool.settled` 提供原生终态及同一关联。关联不证明业务准入、执行或成功；输入解析失败的 Invocation 也可能已经建立关联。`invocationId === null` 只表示缺少关联证据，不能推断没有发生业务；例如 PTC 根工具可以调用具有独立 Invocation 的子工具。

`OperationFollowSnapshot.callSequence` 给出参与登记所属的原生调用，缺证时为 null。通过相同序号连接 ToolActivity，再由 invocationId 取得业务调用身份。显式 Follow 工具只参与原操作，可以有 callSequence 而没有 Action Invocation；普通 SDK 调用可以有 Invocation，而没有原生工具发生。

`operation.follow.changed` 的 `call-associated` 描述来源关联。intent 已追加、来源尚未追加时，观察可先取得缺证快照；关联追加后更新同一个对象的事实。关联不解除等待、不签发续接或业务权限，也不改变 owner 的登记、ACK 与恢复合同。

宿主在完整 Invocation 对象构造后、业务准备和执行前登记关联。登记失败通过 Invocation 的 `registration` 阶段结算，输入解析、应用准入和 handler 均未开始，不留下在途活动。关联记录使用原生 append 与既有 checkpoint，不对普通 Action 另许跨进程 exactly once 或新增事务落盘承诺。

冷读取恢复的是原生发生和关联，不重建 Promise 或可执行 Invocation，不重发业务命令。没有关联记录的历史事实保留 null，不从旧 ID、名称或当前绑定推断。需要长期恢复业务结果时，继续使用 Operation owner 的原 command 和正式引用。

## 固定 rc.3 的 PTC 读取边界

dsh `0.2.1-rc.3` 的 PTC bridge 为每次 run_code 重置子调用计数，并使用根 callId 生成子 ID；正式持久日志读取器要求子 ID 在整份日志中唯一。重复根标签并再次调用工具时，运行内观察能够按 callSequence 区分，正式冷读取却会报告 `PTC dispatch repeats subCallId`。不同根标签的 PTC 和重复标签的普通原生调用均有实际冷恢复验证。

当前公开 bridge/runtime 接口没有按真实发生签发子 ID 的接点，执行 wrapper 只能修改 signal，日志内容 hook 不能修改身份。该情况需要上游提供统一的子 ID 签发接点或调整正式读取协议；框架保留这项具体边界，不修改私有调用身份或绕过日志校验。
