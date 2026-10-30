---
"@hanger-source/dsh-reactive-client": patch
"@hanger-source/dsh-reactive-remote": patch
---

客户端生成声明不再附带指向仓内 Host 源码的无效 declaration map，仓外消费直接使用包内的公开声明。沿用官方生成器和既有产物任务，只归档未发布的映射并移除尾部引用；声明正文、运行代码和协议元数据保持不变。
