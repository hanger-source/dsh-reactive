---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-client": minor
"@hanger-source/dsh-reactive-remote": minor
"@hanger-source/dsh-reactive-native-ui": patch
"@hanger-source/dsh-reactive-skills": minor
---

提供 Program 的 useSharedResource、Action.activeInvocations 与 ReactionRun.state，让派生资源及运行事实通过同一对象供 Promise、Observable 与 hooks 消费，并保留真实执行的结算寿命。

统一根、局部 Surface 与嵌套集合的 Invocation 查回。回执保留由实际 Host 调用 owner 负责；completedRetentionMs/maxCompleted 配置移至 RuntimeOptions/HostOptions，Gateway 只传输同一回执，不重执行或依赖当前 Expose。

修复可选原生 Action 展示在同步注册、退出与重入时的贡献归属和释放失败结算。同步开发 Skill 与迁移说明。
