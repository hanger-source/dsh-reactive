---
"@hanger-source/dsh-reactive-dsh": minor
---

原生能力的 `forSession(source)` 统一返回稳定的共享借用面，仅提供本域资源和明确的控制方法，不再公开共享读侧的 `dispose()`、Scope 或内部服务。组件和普通消费者退出自己的订阅或取消本次等待；需要独立读取寿命时持有独立 Session 句柄，并释放该句柄。安装能力继续由真正的 NativeCapability owner 释放。

Files、Journal、Attachments 将真实读取与监听清理直接登记到 Host 签发的原来源，Host 仍等待已退出和已收集 Session 的实际 I/O、unwatch 及观察释放；已受理原生调用继续按原合同结算。
