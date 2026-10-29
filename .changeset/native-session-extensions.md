---
"@hanger-source/dsh-reactive-dsh": minor
---

公开 `createNativeSessionCapability` 与 `/native` 宿主扩展入口，统一复用安装 owner、全部必需服务的提供者核验、exact Session 来源、共享冷资源、Agent 代际与 NativeInvocation。扩展作者不必深层导入权限和 Scope，也不必自行维护来源缓存、清理或调用状态。

Tools 与 Schedule 使用同一公共工厂；异步清理继续进入真实来源与 Host 的退休图，已开始的原生调用由原安装 owner 等待结算。构造期拒绝实际观察和调用，旧来源及已退出 provider 不会复活。

提供者身份同时覆盖正式 `provide` 的普通对象／函数与 Cordis trace 代理，拒绝将两个缺少 original 标记的不同提供者视作同一服务；配置用的 primitive 值不能充当服务身份。
