# 调用回执等待

Action、原生写入及文件上传共用 Invocation 的 result、activity 与 wait。显式调用开始一次执行，读取和退等不重新执行。

```ts
const call = await save(input, { signal: admissionSignal });
const receipt = await call.wait({ signal: receiptWaitSignal });
```

本地 SDK 的 admissionSignal 限制进入 execute 之前的准入和持久准备；远程调用的 signal 限制开始回执的等待，尚未确认受理仍保留未知受理语义。业务开始后，二者都不因调用者退等而撤回原 Host 回执或 Operation 关联。需要退出结果等待时，对已经取得的 call 使用 wait；不要用真正的 execution.signal 代替等待信号。

每次 wait 只借原 result。取消一个等待者不影响其他等待者、activity 或 operation，取消原因按原对象交付。纯本地 Action 的 wait 不捕获首次 SDK caller 的信号；之后按原地址查回的仍是同一个 call，可以重新等待原回执。原生和上传继续使用各自 SDK 的默认等待寿命；远程 Invocation 继续归其 SDK 读取 owner，SDK 退出可以结束读取，但不证明 Host 业务失败。

已声明 Operation 映射时，call.operation 保留同一次真实结果的关联，取得句柄后用 operation.settled({ signal }) 等待权威业务终态。显式 ActionExecutionContext.signal 和原生工具 signal 继续拥有原执行边界的取消权限；这项权限不会因为增加 wait 而改变。

自建 Invocation 实现需提供 wait(options?: CancellationOptions):Promise<Output>，通过现有 abortable 读取原 result 并尊重自己的读取 owner；不创建第二份业务状态或结果 Promise。普通消费者继续使用框架签发的对象，泛型函数可直接接收 Invocation<Output, Observed, Source>。
