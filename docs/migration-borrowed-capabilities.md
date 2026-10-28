# 借用能力与 owner 权限

工作区、对话、值、资源实例与反应绑定不再公开所属 Scope。原来通过 `value.scope`、`resource.scope`、`binding.scope` 或 `conversation.scope` 退出共享 owner 的代码，应把 Scope 保留在实际创建者处；消费函数只接收业务对象。普通应用从 Host 或组件取得 Workspace，退出自己的订阅、读取等待或 ReactionRun，不销毁工作区或其他会话共享的资源。ReactionRun.stop 继续只停止该次运行。

公共工作区类型统一使用 `Workspace`，从 `@hanger-source/dsh-reactive` 或 `/workspaces` 导入；`WorkspaceContext` 退出公共出口。它提供 share、resource、reaction 与身份，不携带销毁能力。

底层宿主从 `@hanger-source/dsh-reactive/kernel` 使用 `createWorkspace(scope, identity)`，替换 `createWorkspaceContext`。调用者显式创建、保存并最终 dispose 该 Scope。已有宿主取得的 Workspace 无须重新包装，内核装配也不能从公开 Workspace 反查 Scope。

内核 `Conversation` 构造改为 `new Conversation(scope, identity, workspace, program, initial)`。Scope 是首个显式借入参数，Conversation 不自行从工作区创建子 Scope，也不返回它。实际会话 owner 创建自己的子 Scope，传给 Conversation 与 Program 装配，并在该会话退出时释放；不能把工作区共享 Scope 当作每个会话的退出对象。

Jobs 的 `job(id)`、Schedule 与 Team 的 `task(id)` 同步返回固定目标。Team、Schedule 的调用不再声明 `Promise<NativeTeamTask>` 或 `Promise<NativeScheduleTask>`，也不接收构造时的取消选项；把 signal 放到 read、history、update 或 delete 等实际操作中。它们的 state 与 Job.output 可以直接组合，构造不会读取原生服务或证明目标存在。

Team、Schedule 的不存在目标通过资源或 read 返回 undefined，不能再依赖 task(id) 拒绝来查询存在性。需要 Team 墓碑时继续调用真实活跃成员的 getTask。Job 未知或外来目标仍由原生读取、等待或控制入口拒绝。任何目标的构造都不替代归属、成员、权限或 CAS 核验。

Schedule task.update/delete 的调用取消与 SDK 退出结束本次回执等待，预取消不启动操作；原生事务已经受理后继续持久化。等待取消不证明修改失败，不自动重发；应沿原目标读取权威修订或历史后判断结果。读取取消、观察退出和业务取消保留各自含义。
