# 原生调用与独立等待

Team 的 createTask、updateTask、task.update 和 Schedule 的 task.update、task.delete 同步返回 NativeInvocation。直接 `await task.update(request)` 不会等待写入，应保存调用对象并读取 result：

```ts
const call = task.update(request);
const result = await call.result;
```

需要取消本消费者的等待时使用 `await call.wait({ signal })`。调用本身只开始一次；取消等待、退出 SDK 或再次观察不重新执行、撤回或改变原生结果。方法的 options.signal 保留执行前取消并作为默认等待信号；Schedule 仍由原生服务在 FIFO 和存储准备后核验它，Team 不凭等待取消撤回已交给原生的事务。

来源、目标保护、已退出 SDK 和预取消在调用前同步检查，失败会直接抛出并且不开始调用。实际原生方法的成功或拒绝沿 call.result 结算；Schema/CAS/权限拒绝、存储错误以及写入后的 flush 错误都保留原语义，失败不能证明没有写入。框架不自动重试。

call.activity 是同源 ResourceReader，activity$ 是其值流，可以 read、peek、组合 observations$ 或交给 useResource。running 只说明原生方法已经开始调用，不证明 FIFO、CAS、持久化或业务完成。失败是 ready 中的 failed 调用事实，终态不随 SDK 或能力观察退出而改写；晚订阅仍可取得同一次终态。等待完整结果继续使用 result，不能依赖活动流的完成来取得返回值。

调用来源使用 kind: native、实际 sessionId、capability 与 operation，不冒充 Action、Tool 或人工身份。id 只关联这一次内存调用，不是可跨重启查询的业务命令号；持久恢复仍按原生任务身份、修订与记录处理。

能力 dispose 等待自己已经开始的调用结算，再释放本实例安装范围；调用拒绝保留在对应 result 与活动中，不作为清理失败。provided 服务的寿命由外部宿主决定，框架不接管其退出顺序。只退出会话 SDK 不会销毁能力 owner 或原生工作。

Team.roster 与 roster$ 表达原生实时成员运行状态；state、projection$、members$ 继续表达持久成员阶段。需要派生成员是否可接任务的业务条件时使用 roster，并与任务资源一起组合；冷会话 roster 保持 pending，不因读取恢复 Agent 或创建成员。
