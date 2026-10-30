---
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-skills": patch
---

委托的恢复准入提供独立冻结的 recovery，包含真实子工作引用、初始消息的消费状态及当前 Goal 快照。业务政策可以核验当前目标，授权后的异步等待不能借用被编辑的新修订；未消费消息沿原身份恢复，已消费工作不重新提交。

存活子工作的回执修复明确保留 Goal 尚未创建的事实，核验授权现场后补齐原工作。原生委托指导与开发 Skill 使用局部 Module、Surface 和类型化 SDK，统一说明恢复授权、原消息与独立子 Goal 的责任。

宿主退出等待已开始的子会话观察和恢复工作，正常取消保留原 owner 身份，混合的真实失败继续上报。观察随精确原生作用域结束，在持久句柄关闭前完成实际读取与 flush。
