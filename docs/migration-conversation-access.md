# 会话成员资格与读取寿命

Workspace 成员资格属于原生持久登记。`detachSession` 撤销该 Session 已签发的读取与准入资格，工作区删除撤销其全部 Session；它们不删除 Session 日志，也不取消已受理 Action、Job 或业务 Operation。

旧本地句柄、Program 来源、原生 SDK、远程资源及人工控制单向失效。仍在观察的资源交付 `ConversationAccessRevokedError`；该错误从 core 根入口导入，包含 `sessionId`、`workspaceId` 和 `reason`。成员移除为 `membership-removed`，工作区删除为 `workspace-deleted`，请求携带的旧资格与当前签发不符为 `access-expired`。普通业务资源错误不撤销整个 Session。

重新加入后显式 `open` 或 `resume` 取得新句柄。旧对象不能复活。仍驻留的同一个原生 Agent 只重新装配 Program 图，不重启模型循环；冷 Agent 继续遵守 `open` 不加载、`resume` 显式驱动的边界。工作区共享资源及其他成员会话保留各自寿命。

本地 `ConversationHandle.signal` 表示这个借用与成员资格的退出，`accessId` 保留其签发身份；两者在退出后仍可读取。它们不是业务执行的取消信号。`Invocation.result`、原调用活动、上传 result 及独立 Operation 回执继续沿原 owner 结算；停止读取不表示业务未执行。

远程 SDK 自动保存和传递 `accessId`。自定义协议消费者须从 create、completeCreation、open 或 resume 的身份回执取得它；后续会话读取、控制及 Operation 查询使用必填 `ProgramSessionAddress`。首次取得仍使用 `ProgramAddress`。缺少资格字段的旧会话请求直接拒绝，不隐式重新取得资格。认证、参与角色和共享 Program 核验仍独立成立，`accessId` 本身不授予权限。

原 Invocation 派生的 Operation 观察使用该 Invocation 的精确地址。宿主从原受理记录取得实际 Operation 句柄，核验参与者、Program、Operation 合同与引用，再观察同一个业务 owner；这条链不重新取得会话资格。任意 command 或 reference 本身不是原调用回执，不能借此绕过会话查询资格。

资格引用由工作区签发，冷 Program 图回收不会更换它；真实成员撤销和工作区退出会使它失效。SDK 无需额外订阅人工、进展或模型事件来检测未观察期间的移除与重新加入。跨 Host 重建后，旧资格失效时应显式重新取得 Session。

输入准入、原生步骤准备及人工响应在等待和最终副作用前复核资格；不合作的迟到策略不能继续激活会话或创建 Goal。已经 send、replace 或提交的输入仍完成原持久回执，消费者取消等待不覆盖实际受理事实。
