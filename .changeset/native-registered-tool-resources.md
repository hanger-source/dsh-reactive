---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-skills": minor
---

Jobs 与 MCP 的 toolNames、MCP 的 resourceToolNames 提供正式 ResourceReader，普通函数、Promise、Observable 与 hooks 共用实际工具注册事实。已有值流直接取自资源，最后消费者退出只释放查询，能力退出结束旧读取，原生连接、执行及准入仍由原 owner 负责。

NativeTools 接受显式空名称集合并贡献零个声明，使异步准备或全部工具撤销能直接组合；无效名称、重复项和准入配置继续拒绝。
