---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-client": minor
"@hanger-source/dsh-reactive-remote": minor
"@hanger-source/dsh-reactive-skills": minor
---

将工作区成员资格贯通到 Session、Program、本地与远程 SDK、原生资源和人工控制。成员撤销或工作区删除结束旧读取与准入资格，已受理调用、持久回执及独立业务 Operation 继续由原 owner 结算。

会话取得回执携带 accessId，后续会话请求在装配前核验原资格；无观察的旧 SDK 也不能跨过移除再加入继续使用。显式重新取得保留原生 Agent，并建立新的 Program 图与读取身份。提供 ConversationAccessRevokedError 和只读 signal，开发指导及迁移说明同步更新。
