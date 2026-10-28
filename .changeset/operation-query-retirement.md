---
"@hanger-source/dsh-reactive-client": patch
"@hanger-source/dsh-reactive-skills": patch
---

远程 Operation 观察使用原生 baseline/delta 流。事件连接恢复时，Operation、View、人工资源与执行进展统一失效并通过原生 restart 重新取得 baseline，避免查询流仍存活却只清空快照后永久 pending。Session release 与客户端 dispose 等待自己拥有的 Operation 原生流完成异步清理，包括已退订但尚未退出的查询；状态解析和共享仍归同一 core ResourceReader。

Session 显式查询与已受理 Invocation 的 Operation 观察保持不同寿命，原 Session 退出不会撤销后者。客户端退出也等待 Invocation 活动流真正退出，而不只发出取消。清理等待只证明本地原生迭代器和回调静止，不增加远端关闭 ACK 或取消业务操作。
