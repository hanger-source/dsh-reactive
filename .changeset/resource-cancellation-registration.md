---
"@hanger-source/dsh-reactive": patch
---

在 Promise 读取及 Resource 的观察、失效工厂进入业务代码前登记取消，修复读取工厂尚未返回时触发失效、旧读取未及时收到 abort 的窗口。工厂返回前释放 Scope 时，提供者也能立即通过原 signal 阻止后续副作用。保留原有共享、参数身份和迟到结果隔离，不增加查询生命周期或重试策略。
