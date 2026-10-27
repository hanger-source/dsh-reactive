---
"@hanger-source/dsh-reactive-client": patch
"@hanger-source/dsh-reactive-skills": patch
---

远程 View 与 human 读侧只在实际消费者读取或订阅时连接，共享正式资源快照，最后消费者退出取消查询并撤下旧值。会话释放等待退休查询完成，不再在打开会话时隐式请求全部 View 与人工观察权限。原生 Gateway 仍负责传输代际和恢复，业务执行与 Invocation 的寿命保持独立。
