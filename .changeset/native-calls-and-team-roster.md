---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-skills": minor
---

Team 的运行态 roster 与持久成员投影分开，通过正式原生成员和 Agent 状态通知提供同源资源，可与任务板、其他资源、RxJS 和 hooks 共同组合。读取冷会话不创建 Agent，旧来源及 provider 撤销仍沿实际 owner。

Team 与 Schedule 写入同步返回 NativeInvocation，复用核心 Invocation 合同并保留真实 native 来源、同次 result、有限活动资源及独立 wait。取消等待或退出 SDK 不丢失原生最终成功、拒绝或迟到存储失败；调用拒绝不冒充能力清理失败。直接 Promise 消费迁移为 call.result，可取消等待使用 call.wait。能力 owner 等待真实写入结算，正式安装 effect 按实际 disposer 身份释放读依赖和自有 provider，不建立第二个原生执行队列。

同步公共类型出口、仓外消费者、运行语义及同版本开发 Skill。迁移见 docs/migration-native-invocations.md。
