---
"@hanger-source/dsh-reactive": patch
"@hanger-source/dsh-reactive-dsh": patch
"@hanger-source/dsh-reactive-client": patch
"@hanger-source/dsh-reactive-skills": patch
---

输入提交、队列编辑和上传的消费者等待与实际受理分开。并发提交共享同一请求事务，每个调用者可以独立取消等待；共享登记只随实际事务结算退出。Program 来源与句柄退出结束各自等待，已受理输入和编辑仍由原生 owner 完成持久回执，迟到结算失败保留在原会话错误链。

本地与远程入口统一及时结束回执及 Blob 转换等待，转换取消后不启动上传；保留执行前取消、身份和正文冲突核验。远程提交与编辑只在请求已启动且回执无法确认时报告受理未知，明确业务拒绝和本地格式错误仍保留原错误，不自动重试或撤回已受理工作。
