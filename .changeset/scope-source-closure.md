---
"@hanger-source/dsh-reactive": patch
"@hanger-source/dsh-reactive-skills": patch
---

修复 Scope 持有的来源在订阅装配中退出或自然结束时，迟到清理失败未进入关闭回执的问题。即时共享、Scope.observe、Program 共享查询和持续关系共用真实订阅登记；Program 与 Workspace 的共享资源先登记原始来源，再标准化 Observation。保留立即撤销、稳定来源退出时同步抛错和原来源终态，读取者提前释放的失败由原 owner.closed 保存。
