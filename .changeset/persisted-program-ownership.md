---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-client": minor
"@hanger-source/dsh-reactive-remote": minor
"@hanger-source/dsh-reactive-skills": minor
---

Session 持久归属与子委托受理意图保存实际 Program 合同身份。创建补齐、冷读取、恢复及子输入解释先核原身份；共享换约或具名/本地转换抛出公共 SessionProgramMismatchError，远程 SDK 保留同一类型及原身份事实。旧归属或委托记录缺少 program 字段时明确拒绝，不自动迁移。

Application 与 Environment 在定义时取得自己的环境及 Skill 声明快照，异步装配不受调用者后续修改影响，Program 实现对象保持原身份。原生同源审计读取在调用者或借用资格退出时及时结束等待，真实持久 flush 和句柄清理继续由原 owner 完成。
