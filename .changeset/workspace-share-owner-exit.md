---
"@hanger-source/dsh-reactive": patch
---

修复工作区共享来源在 owner 退出后仍向迟到订阅者重放旧值或旧错误的问题。活跃 owner 内继续共享唯一来源并保留有限来源终态；退出时释放可重放对象，结束现有观察，complete/teardown 中重入或之后捕获的旧流都直接完成且不重连。Workspace 会话与工作区自身寿命保持独立，同版本开发指导说明此边界。
