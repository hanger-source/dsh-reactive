---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": patch
"@hanger-source/dsh-reactive-skills": minor
---

Workspace 的 resource 与 reaction 直接接受与 Program 相同的局部业务选项，由实际工作区持有既有资源和反应实例。每次局部构造保留独立身份，显式共享返回对象即可在普通函数、Promise、RxJS 和 Program 中复用；正式实现与参数入口继续稳定绑定。构造不读取或执行，Session 退出不结束工作区实例，登记撤销停止读取与未受理队列，已受理业务仍由原 owner 结算。

Team 创建与更新在调用取消或 SDK 退出时及时结束回执等待，预取消不进入原生事务；已进入的原生 journal 与 CAS 继续持久化，不以取消表示回滚或自动重试。更新同版本开发指导及仓外消费者。
