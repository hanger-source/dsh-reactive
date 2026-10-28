# 2.0 资源有效性与原生依赖

2.0 改变资源 owner 撤销时的公开观察行为，并将原生能力的必需服务纳入正式 Fiber 依赖。SDK 的异步退出方式沿用 1.0 的合同。

## 资源观察

1.x 的 owner 退出只完成观察，外部仍活跃的 hook 或有限值缓存可能继续保留旧 ready。2.0 在仍活跃的 observations$ 上先交付 error Observation，再正常完成；error 直接使用原 signal.reason。退出后的新订阅只取得撤销观察，不回放旧 ready、旧查询错误或重连来源。

消费 observations$ 时必须处理 pending、ready、error，不能将每次 next 都当作业务成功。需要有效性参与 Context、工具条件或公开资源时，继续使用资源对象或 observations$，不将 values$ 的有限完成当作业务资格仍然有效。values$ 在自身 owner 退出时正常完成，活跃 owner 内的查询错误仍沿值流报错。

peek、read 和回调式 subscribe 拒绝已退出 owner；父子 Scope 使用同一个父撤销原因。不要依赖每次读取生成不同的 ScopeDisposedError 实例。普通有限 Observable 的完成语义保持不变；已经结束的订阅不会再次发出撤销。需要在整个借用期间继续接收有效性变化时，使用受管理资源或 shareResource(source$, { signal })。

## 原生服务依赖

Team、Schedule、Files、Skills、Tools、Jobs、MCP、Clock 和 Approval 按各自实际选择声明必需服务。未选择 Jobs 工具时，其 toolNames 明确为空，不要求 ToolRuntime；选择后的服务退出则结束原能力。

provided 服务卸载后，旧能力和其所有 SDK 结束，不再自动借用随后安装的同名服务。替换 provider 时创建并安装新的能力实例，并明确把新对象交给消费者。需要展示旧资源退出状态的 React 组件可按 conversation 身份保留原对象，再用 useResource 观察；每次 render 都重新调用已退出能力的 forSession 会使程序明确失败。

撤销读取不等于停止已受理 Action、业务 Operation 或后台 Job。能力自行安装的服务仍按原生 Fiber 执行 drain，借入服务由外部 owner 释放。dispose 继续等待本实例安装范围的实际清理，清理失败不会提前结束等待。
