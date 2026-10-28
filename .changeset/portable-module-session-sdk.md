---
"@hanger-source/dsh-reactive": major
"@hanger-source/dsh-reactive-dsh": major
"@hanger-source/dsh-reactive-client": major
"@hanger-source/dsh-reactive-skills": major
---

根会话与集合成员通过 module(mount) 借出按局部 Surface 编程的 SDK，保留 source/selected 合同、原始资源对象、参数类型和本地/远程调用时序；模块不再要求消费函数知道根路径。挂载与父选择共同限制可见性，投射不创建缓存、业务 owner 或控制权限。

ProgramSessionClient 统一暂停、取消和权威请求核对；控制回执与请求状态类型归核心合同，client 重导出同一类型。调用级 signal 及句柄退出只结束本次等待，已经进入原生控制的工作继续完成。服务身份可读请求状态但不能取得人工控制。远程 journal 保留正式 carrier 的专有合同。

本地 cancel 现在返回 ProgramCancelResult，而非 Promise<void>；自定义 SurfaceClient 和 ProgramSessionClient 实现需提供同一模块与会话能力。迁移方式见 docs/migration-sdk-modules.md。同版本开发 Skill、本地与远程直接消费者、实际 Gateway 和三环境包消费入口同步此合同。

dsh 发行声明排除已标记 internal 的构造及装配入口，公开观察类型直接来自核心合同；本地 SDK 不再因内部宿主类型牵入持久投影 Registry 的实现声明。运行时装配继续使用同一实现，跨包内核合同保持独立出口。
