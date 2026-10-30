---
"@hanger-source/dsh-reactive": patch
"@hanger-source/dsh-reactive-skills": patch
---

修复 Work 借用调用活动或会话状态时，实际来源的释放错误被完成通知吞掉的问题。两类资源共用直接退订的借用连接，保留来源自身清理 owner、并列释放、名单恢复和自然完成的有效结果语义。
