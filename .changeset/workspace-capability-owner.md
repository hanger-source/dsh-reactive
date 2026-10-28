---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-skills": minor
---

Workspace提供直接的resource与reaction绑定入口，复用既有参数身份、Resource、Reaction和Scope；本地application.workspace(id)与组件props.workspace取得同一对象，在没有Session时也可构造、读取和显式运行。工作区编程合同及内核装配归core/workspaces，公共类型从根入口与/workspaces导出。跨环境、会话共享，同实现与目标在不同工作区隔离。正式工作区登记删除或宿主退出结束其owner，撤销旧资源与排队关系，保留已受理业务；清理重入不能复活已经删除的owner。共享连接直接释放来源后结束观察，使teardown失败返回Scope，删除后的清理失败保留到宿主退出汇总，其他工作区仍完整释放。同步开发指导和仓外消费入口。
