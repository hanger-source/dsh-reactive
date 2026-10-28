---
"@hanger-source/dsh-reactive-dsh": patch
"@hanger-source/dsh-reactive-skills": patch
---

人工响应在冷读取和异步准入期间及时响应调用者、来源及 Program 取消，迟到准入不能继续激活或提交答案。冷晚答先验证完整题目批次，准入、恢复和受理消费同一份自有答案快照。

批准安装通过官方 Approval.request 传递所属取消信号，自有和 provided 回答者的尚未决策请求在安装退出时沿原生取消路径结算，迟到批准不授予新 grant。保留原生审计与已决策执行的责任，同版本开发 Skill 说明这些边界。
