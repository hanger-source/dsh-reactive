---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-client": minor
"@hanger-source/dsh-reactive-remote": minor
"@hanger-source/dsh-reactive-native-ui": patch
"@hanger-source/dsh-reactive-skills": patch
---

Action Invocation 携带在输入解析和准入前捕获的不可变 origin，保留原会话、Action、绑定、目标、真实来源与完整输入；本地、远程及 lookup 消费同一归属合同。远程受理回执核验原绑定与目标，矛盾回执仍表示受理未知，不重复执行业务。

Work 通过 `{ sessions?, invocations? }` 显式组合会话与已持有调用，可在无会话的 CLI 中使用，也可用只读工作标签组织共同进展。动态来源保留原 activity 读侧、失效和释放语义，不读取结果、取消业务或授予 Goal 续接权限。旧会话数组参数须迁移，SDK 与 Gateway 须使用匹配版本；详见 docs/migration-work-invocations.md。
