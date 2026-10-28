---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-client": minor
"@hanger-source/dsh-reactive-remote": minor
"@hanger-source/dsh-reactive-native-ui": minor
"@hanger-source/dsh-reactive-skills": minor
---

Action 以名称与正整数版本声明跨进程合同身份，新定义省略 version 时为 1。远程解析与业务受理前拒绝不匹配合同，原 Invocation 回执、原生展示与子程序委托恢复保留原版本，不再用当前同名合同解释历史结果。

迁移时必须同步更新宿主与客户端。自建 wire 调用须提供 contract；原生 manifest 增加 actionVersion，reactiveAction metadata 格式升至 version 2 并增加 actionVersion，委托与子会话记录也要求 actionVersion。缺少这些字段的旧记录不能直接恢复；保留相应旧版本消费者，或由业务明确核验历史合同后进行数据迁移，框架不自动猜测版本。
