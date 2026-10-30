---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-client": minor
"@hanger-source/dsh-reactive-remote": minor
"@hanger-source/dsh-reactive-skills": minor
---

新增人工端 resumeGoal(expected, options)，通过精确 GoalReference 执行原生恢复、续行启用和持久回执。本地与远程共享合同，服务参与者不能取得此控制；会话激活继续独立表达。

正数 Goal round 沿官方 Driver 的真实 reservation 与步骤准入关联已有输入和 GoalRef，支持恢复后的模型继续工作，不重发旧业务命令或反馈。远程错误保留原生 Goal code 与 Driver 依赖失败原因。
