# 会话参与端口

`ProgramSessionClient` 是人工和服务共同的读取、输入、Surface 与 Operation 合同。只消费资源、请求或业务调用的模块继续使用它；需要 question、answerQuestion、attachWait、pause 或 cancel 的模块改用 `HumanProgramSessionClient`。共同端口可以读取 human 资源，读取问题不取得答复、批准或前台认领权。

本地服务先用 `host.service(actorId)` 取得固定身份的应用视图，再调用 open、create、resume 或 completeCreation。原来一次调用的 `host.service(sessionId, Program)` 改为 `host.service().open(sessionId, Program)`；不传 actorId 时使用宿主明确配置的 serviceActorId。这个视图复用同一工作区和会话取得链，没有独立 owner 或 dispose。服务的逐次调用选项只传取消等本次参数，不能含 actorId，连与固定身份相同的值也会拒绝。

`host.attachOperation`、`host.locateOperation` 和 `host.readOperationFeedback` 不再接收末尾的 participant 参数。原服务调用改为先 `await host.service(actorId).open(sessionId, Program)`，再通过取得的服务句柄调用对应方法，并在使用结束后 release；actor 在取得服务端口时固定，逐次调用只传取消选项。会话句柄的 notifyOperation 同样保留本参与者角色；Host 顶层的 notifyOperation 继续表达明确的后台发布者入口，可靠投递始终沿原持久 publisher。

Gateway 配置使用 `participant(principal)`，返回 `{ participant: 'human' | 'service', actorId }`，替换只返回 actor 字符串的配置。认证主体和参与者映射由宿主提供，客户端请求不带身份选择。输入、队列编辑、上传、Action 和 Operation 使用同一受信端口；Invocation 关联同时核对角色和 actor。部署仍需配置各 endpoint 的 RemoteAccess grant，参与者身份不替代资源授权。

人工客户端使用 createProgramClient/bindProgramClient，服务客户端使用 createServiceProgramClient/bindServiceProgramClient。RemoteServiceProgramSession 的运行对象不包含人工控制方法；即使绕过 SDK 直接调用远程人工 endpoint，服务身份仍会被服务器拒绝。原生人工会话 UI 只绑定人工客户端，通用资源呈现可以独立借用服务资源。

客户端核对宿主实际返回的角色。如果工厂与认证身份不匹配，ProgramParticipantMismatchError.identity 保留已确认的实际身份及会话地址；创建可能已经完成，应换用匹配的工厂打开原会话，不能自动重建、补偿删除或归类为未知受理。

固定 rc.3 的 `$stream`、连接代际与 Journal supervisor 需要 answer 传输。两类 SDK 都明确复用这条正式恢复链；answer 不授予业务 human 权限。none 连接尚不能绑定完整 SDK，框架不提供缺少恢复的替代流，也不自行实现重连。
