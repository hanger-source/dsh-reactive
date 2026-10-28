---
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-skills": minor
---

应用模块通过 ApplicationAssembly.native 与 operation 一次选择原生能力及恢复 owner，不再要求外围重列安装和恢复清单。独立 Runtime 与嵌入 mountApplication 共用应用安装 Fiber；嵌入宿主显式提供能力安装 home，factory 返回后封闭装配，同一实际对象只登记一次，业务 owner 保留原执行责任。

Host 的业务排空与安装退出分别由原 owner 负责，公开 dispose 与父作用域退出都按排空、观察装配、原生能力的顺序等待实际清理，退出后撤下应用监听。启动失败释放已经选择的能力并保留原始失败，借入服务及同宿主其他应用继续使用。同版本开发指导与仓外消费入口同步此合同。
