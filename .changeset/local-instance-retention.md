---
"@hanger-source/dsh-reactive": patch
---

局部 Resource 与 Reaction 直接构造运行实例，不再登记只用于正式实现及参数复用的绑定缓存。资源仅在持有实际来源连接时登记 Scope 清理，最后观察者退出即释放登记，避免长期 Program 或 Workspace 留住已经丢弃的局部实例。仍被调用者持有的对象保留原 owner、目标和读取资格；显式启动的反应及已开始业务继续按原有生命周期结算。
