# SDK 模块与会话能力

可复用消费函数接收局部 `SurfaceClient`。应用通过 `session.module(composition.mounts.business)` 或 `collection.instance(key).module(localComposition.mounts.business)` 取得它，无需重写根槽名。`viewResource` 返回根 SDK 的同一个 Reader，Action 和 Operation 保留原绑定、权限及调用时序。

`SurfaceMount.surface` 是模块实现使用的完整源合同，`selected` 是公开合同。投射结果按 selected 定型；外层选择更窄时拒绝整次投射，不能恢复隐藏槽。挂载点必须由当前源合同的组合签发，同形对象和其他根的挂载点均不能代替它。模块 SDK 借用原句柄的寿命，没有独立 release，也没有整个会话的人工控制权限。

`ProgramSessionClient` 提供 `requestStatus(requestId, options)`，人工控制由 `HumanProgramSessionClient` 提供 `pause(options)` 与 `cancel(options)`。`options.signal` 只取消回执等待，预取消不开始控制；已进入原生 Goal 或 Agent 的控制继续结算。requestStatus 核对原生日志及持久投影，不能用客户端缓存缺项推断请求未受理。

本地 cancel 返回 `Promise<ProgramCancelResult>`。显式声明 `Promise<void>` 的消费函数应调整为共同回执，或在自身函数内 await 后不返回该值。`canceled: true` 表示原生处理已静止并完成 flush，不表示外部业务 Operation 已取消。本地暂停可保留更丰富的原生 GoalView；共同消费只依赖 id、revision 和 phase。

控制与请求状态类型从 `@hanger-source/dsh-reactive` 导入；client 包继续重导出同一份合同。自定义 SurfaceClient 和 ProgramSessionClient 实现需要提供相应能力，并将执行、身份及释放交给实际 owner。服务参与端口保留只读请求核对，不提供人工控制。远程 openEvents 仍使用正式 carrier 的 journal 合同，不需要为本地模块复制传输恢复机制。

宿主与句柄通过 createRuntime、mountApplication、create、open 或 resume 取得。标记 internal 的 create、drain 和装配函数不属于发行声明；公开消费不需要持久投影 Registry 或宿主内部类型。框架包之间使用的 kernel 合同仍通过独立正式出口提供。
