---
"@hanger-source/dsh-reactive-dsh": patch
---

Subagent Operation 的每次观察连接在最后退订时撤销本次读取资格，及时退出不合作的授权等待，迟到授权或日志回执不再启动后续持久读取。已受理的 Action、子工作执行及 Operation 业务终态继续由原 owner 负责。
