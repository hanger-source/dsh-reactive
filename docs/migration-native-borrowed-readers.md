# 原生会话能力的借用与释放

`capability.forSession(source)` 按同一个真实 Session 或 Program conversation 返回同一个共享业务对象。资源、固定目标和原生控制沿原合同工作；返回对象不提供 `dispose()`、Scope 或内部服务。

这条规则适用于 Jobs、Schedule、Team、Clock、Tools、Subagents、Skills、Approval、Files、Journal 和 Attachments。MCP 的工具与资源目录继续归安装能力所有。

## 消费者退出

结束自己的订阅，或取消本次读取、等待的 signal。其他消费者仍可使用同一对象。组件从 Program conversation 取得能力时，退出组件只释放组件拥有的观察，Program 来源的读取资格继续由 Program 管理。

需要独立读取寿命时，先取得自己拥有的 Session 句柄：

```ts
const source = await host.open(session.id);
const files = capability.forSession(source);
try {
  const content = await files.readText('report.txt');
  consume(content);
} finally {
  source.release();
}
```

`source.release()` 同步撤销该来源的读取资格，不结束另一个 Session 句柄或 Program 来源的观察。需要继续使用同一会话时显式取得新的句柄，不复活已经退出的来源。

原先对 `forSession` 返回对象调用 `dispose()` 的位置，应根据实际归属改为退出本次订阅、取消本次等待，或释放自己取得的 Session 句柄。不要释放从其他模块借来的句柄，也不要用能力安装的退出替代一个消费者退出。

## 实际清理与执行结算

`capability.dispose()` 仍由安装 owner 调用，结束该能力安装范围的共享读侧。Host、Runtime 和能力 owner 的异步退出等待已经开始的真实清理。

Files、Journal 和 Attachments 在取得读侧时直接向真实来源登记内部清理。某个 Session 已从 Host 的活跃集合移除时，其在途读取、原生 observation 和 unwatch 仍在原清理链中，Host 退出继续等待它们。停止一次等待不证明底层 I/O 已经结束。

`files.watch(path)` 每次显式创建的 `FileWatch` 由调用者持有，保留自己的 `dispose()` 和 `closed`。它与共享的 `forSession` 业务对象具有不同归属。

Job 的 `stop/collect/remove`、Schedule 的 `update/delete`、Team 的任务操作和 `interrupt` 均保留各自业务含义。退出观察不调用这些控制。已返回的 NativeInvocation 继续保存同一次原生结算，已受理操作不会因消费者退出自动回滚或重试。
