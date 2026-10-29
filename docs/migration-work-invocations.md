# 工作调用观察迁移

`Work.select` 和 `Work.observe` 的来源参数为 `{ sessions?, invocations? }`，不接受旧的会话数组。`sessions` 可以为空；已持有调用可以直接作为 invocation 来源，也可以用 `{ invocation, key }` 添加只读工作标签。`observe` 的 invocation 列表可以是静态数组或动态 ResourceSource；`select` 接收 activity Observation 快照。

`identify` 可收到 `{ kind: 'invocation', id, origin }`。使用唯一工作标识归组，scope 相同本身不证明属于同一工作。真实 inputs 完整匹配；显式标签允许一个调用属于多个工作，不改变 inputs。快照的 invocations 保存 id、origin、activity Observation、matchedInputs 和 keys。

来源按 `(sessionId, bindingId, id)` 去重，origin 冲突会硬失败。每个 Work 观察连接固定使用首个命中的 activity Reader；重排、重发或增加标签不会切换，失效也不回退。只有显式移除已选来源才可重新选择。`activity.status === 'error'` 表示该行观察不可用；`activity.status === 'ready'` 且 `activity.value.state === 'failed'` 才表示调用业务失败。动态成员来源 pending 或 error 使整体观察失效，不视为空数组。

Work 只订阅 activity，不读取 result、wait 或 operation，不取消业务、不自动查询 Operation，也不打开会话、创建 Goal、follow 或控制业务。Operation 阶段仍由领域资源提供。

独立调用进入 `state` 与 `state$`；`events$` 和 `progressFrames$` 仍来自显式提供的会话，不合成调用日志。该观察只覆盖已经提供的调用，Host 的有限保留表按原 actor、participant、session 和 Program 核验 lookup；它不负责发现未持有的历史调用。

Action Invocation 的不可变 origin 在输入解析及准入前捕获，包含 sessionId、action 的 name 和 version、bindingId、scope、source、inputs；输入或准入失败仍保留。origin 不包含原始 ActionInput，本地非 JSON 输入不受限制。通用 native 和 fileUpload Invocation 不增加 Action 字段。

Wire Start 使用唯一的 `{ id, name, origin }`，SDK 与 Gateway 须使用匹配版本。ACK 继续校验原 id、地址、合同、session、binding 和 target；lookup 保持原 binding。Host 原有的有限内存 Invocation 表及 actor、session、participant、Program lookup 权限不变；没有全局枚举、新账本或重启后重执。
