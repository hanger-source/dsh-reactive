---
"@hanger-source/dsh-reactive": major
"@hanger-source/dsh-reactive-dsh": major
"@hanger-source/dsh-reactive-skills": major
---

将借用能力与拥有作用域的权限分开。Resource、Reaction、Value、Workspace 和 Conversation 的公开对象不再返回所属 Scope；工作区和会话的实际内核记录显式拥有 Scope，继续负责资源、程序、观察和失败清理。公共 WorkspaceContext 类型退出，业务使用 Workspace；内核构造改为显式传入由调用者拥有的 Scope。

Team.task、Schedule.task 与 Jobs.job 同步绑定固定来源和目标，不因构造对象读取领域存储；存在性、成员关系和权限继续由真实读取、观察及原生控制边界裁决。Schedule 修改和删除在取消或 SDK 退出时及时结束消费者等待，原生已受理的事务继续结算。

同版本开发 Skill、迁移说明和仓外消费者一起更新。Skill 构建复用工作区归档机制撤下过期生成文件，避免模板退出后仍将旧 API 指导装入产物。
