---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-client": minor
"@hanger-source/dsh-reactive-remote": minor
"@hanger-source/dsh-reactive-native-ui": minor
"@hanger-source/dsh-reactive-skills": minor
---

以原生 turn/end:error 提供同源 ExecutionFailure，贯通会话、请求、Work 与远程 SDK，保留真实输入和未入历史消息的失败因果。自定义 ConversationActivity 与 RequestActivity 来源需要提供 failures。

晚答失败后沿原消息 ID 与内容交还正式 inbox，实际 Agent 激活也核验并恢复进程中断留下的交接；只读打开不修改队列。保留等待原生 flush，不重答、不自动重试或恢复 Goal，取消和已消费消息不复活。
