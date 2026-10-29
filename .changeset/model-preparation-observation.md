---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-client": minor
"@hanger-source/dsh-reactive-remote": minor
"@hanger-source/dsh-reactive-native-ui": minor
"@hanger-source/dsh-reactive-skills": minor
---

在 progress 中提供同源的 ModelPreparation，观察当前模型准备实际等待的输入、委托、步骤检查、Context、Skill 与原生装配。请求和 Work 按完整原输入关联同一准备事实；本地、远程 SDK 与原生 UI 消费相同合同。

完整有效声明相同时保留原绑定和修订，程序可以观察自己的进展而不重复装配。并列 Context 重收集先结束原批次读取；Program 失败撤销原生绑定时保留实际错误原因。

ConversationProgressSnapshot 和 RequestSnapshot 的自有来源需要提供 preparation（无当前准备时为 null）。准备观察不持久化、不参与调度、不签发权限；取消、结束及 Agent 退出撤下旧等待，冷会话不复活历史准备。
