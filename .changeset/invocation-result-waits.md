---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-client": minor
"@hanger-source/dsh-reactive-skills": minor
---

Invocation 统一提供 wait({ signal })，Action、原生写入和上传可以通过同一个合同消费。每次等待只借原 result，取消不改写 activity、结果或 Operation 关联；各签发者的读取 owner 继续适用。

本地 SDK Action 的调用者 signal 与真正执行 signal 分开：前者限制进入 execute 之前的准入和持久准备，业务开始后不再把真实成功改成 failed 或丢失 Operation 回执。真正的 Host、工具与显式 execution 取消语义保持；本地按原地址查回仍返回原对象，首次调用者取消不会成为永久等待条件。

远程 Invocation 的 wait 仅限定本 SDK 的等待，不向 Host 发送执行取消。公共类型、泛型仓外消费者、开发 Skill 与调用回执等待指导同步更新。自建 Invocation 实现需补 wait 方法，现有普通消费者使用框架签发对象即可。
