# 共同资格迁移

资源层公开 `Eligibility` 和 `EligibilityError`，替代 `ActionEligibility` 和 `ActionEligibilityError`；没有旧名称别名。错误的能力字段为 `capabilityName`，远程错误码为 `reactive/eligibility`。资格持有与释放由框架负责。

Action、Context 和独立 Follow 可绑定同一资格来源；读取资格不授予执行权。Follow 的资格撤销和未进入登记事实见 [运行语义](runtime-semantics.md)。

Follow 的进程内准入不会跨崩溃保存：仅有持久 intent 不能证明 owner 未被调用，冷恢复按当前 Application 与业务 owner 对原关联的授权幂等重建。只有持久 unregistered 证明该参与被拒绝且未进入登记。未登记结案 checkpoint 失败影响目标 link 和原 Goal 组，不封锁同一 Session 的其他模块。

NativeTools 与 Skill 均可通过 `eligibility?: ResourceSource<Eligibility>` 借用共同来源，包括经 Program `shareResource` 共享的同一 reader；借用不转移资源 owner。未提供资格时保持原有行为。

NativeTools 仅在 enabled 且当前 reader 为 ready.allowed 时可见；pending、error、denied 均撤下，不保留上次 allowed。每次异步 admit 仍核验真实来源、参数和原始输入。preExecute 在 admit 和后续 Approval 期间持有资格，任意瞬态撤销永久拒绝该次调用；恢复只允许新调用。正式 guard 最后复核并释放，已进入原生 execute 的工作沿原 owner 结算。同名工具贡献须共享完全相同的 enabled、admit 函数和 reader 身份，不合并权限。独立 SDK 和原生服务不受模型 NativeTools 声明反向限制。

Skill 资格只决定 Program 方法正文（name）或目录 provider（directory）的当前贡献，不是 Registry 安全授权或 skill 工具许可。pending、error、denied 局部撤贡献，不等待，也不使整个 Program 失败；需要阻塞时使用 required Context。同目标同 reader 在 frame 内去重，不同 reader 保留；同名或规范化绝对目录只要有有效贡献就保留接点，最后有效贡献退出才撤下，外部 provider 不卸载。ProgramSkills 拥有持久订阅，直接从 reader 派生有效集合，不建立第二份资格缓存。资格变化修订会取消旧准备；每轮每目标 pin 一个有效贡献，失效则废弃该轮，由外层 flush 重新收集并可选择其他贡献。provider 的真实释放等待原 Fiber 静止。
