# 原生资源与客户端边界

## 工作区共享

已有资源对象或其 observations$ 使用 workspace.shareResource(source)。同一个原流取得相同 reader，构造不连接；read、peek、Observable 和 hooks 消费同一份事实。工作区删除或退出向仍活跃的外部消费者交付原原因的 error 观察，旧 reader 不复活。

workspace.share(source$) 继续共享普通事件流，owner 退出表现为标准 complete。普通流不能仅凭元素形状推断其资源 owner；需要保留资源读取资格时，选择 shareResource。

工作区共享来源的清理失败归工作区退出链汇总，资源观察保留原撤销原因。直接使用 shareResource 或 ResourceReader 时，onCleanupError 接收来源释放异常；消费者回调失败仍由 onObserverError 处理。未提供 onCleanupError 的显式退订继续同步抛错，信号撤销或 read 内部释放则报告清理失败并结算原读取结果。

mapResource(project) 负责同步派生。thenable 返回进入该代 error，已产生的拒绝会被观察。异步依赖继续通过 switchResource 选择 observeResource 的查询；没有第二套异步状态或生命周期。

## 原生控制

Job 收到终态并取得需要的输出后，显式调用 job.remove() 移除可见记录。移除不是 stop，也不消费模型输出游标。运行中、未知和外会话目标由正式注册表拒绝；列表跟随 removed 事件，旧固定目标的 state/output 交付原生缺席错误。

Schedule update/delete 在原生 FIFO 中保留实际调用。SDK dispose 或来源句柄 release 只结束默认 wait 与资源观察，不会取消已经返回 NativeInvocation 的写入。显式 options.signal 仍送入原生服务，可在落盘前取消排队工作；能力 owner 退出会等待已登记调用的实际结算。

Jobs、Schedule、Team 的宿主入口分别位于各能力域的 index；共享词汇从 /jobs/client、/schedule/client、/team/client 导入。这些叶只转导官方公开纯合同，不加载服务、安装工具或提供远程控制。Jobs/Team 的品牌构造值与宿主出口同源；Schedule 的官方 client 叶只有类型。ScheduleId、ScheduleInputError、ScheduleLogError 从宿主 /schedule 取得，目标存在性与成员资格仍由服务核验。JobOutputRead 仍是宿主合同，不复制为浏览器 DTO。

## 客户端构建

共享 wire 类型、错误及 Inbox 位于 client/contracts，只有前置合同编译负责其 Node 产物。认证与连接 transport 属于 SDK 运行构建；Gateway 需要 Inbox 时直接导入 @hanger-source/dsh-reactive-client/inbox。

浏览器根入口及公共子入口由一次 splitting 构建生成并共享 chunk。根入口、/errors、/inbox、/transport 等路径取得同一运行时类与数据合同，core 继续只有一个模块身份。消费者通过 package exports 选择入口，不拼接 lib 下的文件名。
