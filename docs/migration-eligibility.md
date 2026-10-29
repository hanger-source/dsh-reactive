# 共同资格迁移

资源层公开 `Eligibility` 和 `EligibilityError`，替代 `ActionEligibility` 和 `ActionEligibilityError`；没有旧名称别名。错误的能力字段为 `capabilityName`，远程错误码为 `reactive/eligibility`。资格持有与释放由框架负责。

Action、Context 和独立 Follow 可绑定同一资格来源；读取资格不授予执行权。Follow 的资格撤销和未进入登记事实见 [运行语义](runtime-semantics.md)。

Follow 的进程内准入不会跨崩溃保存：仅有持久 intent 不能证明 owner 未被调用，冷恢复按当前 Application 与业务 owner 对原关联的授权幂等重建。只有持久 unregistered 证明该参与被拒绝且未进入登记。未登记结案 checkpoint 失败影响目标 link 和原 Goal 组，不封锁同一 Session 的其他模块。
