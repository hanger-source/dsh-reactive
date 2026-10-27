---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-skills": minor
---

新增 useResourceInstance 与 useReaction，让业务函数、Observable 和程序 hooks 共用同一程序拥有的资源及反应实例。取得实例不自动读取或执行，参数身份保持稳定；Program 失败时释放查询、感知和排队关系，已受理业务继续由原 owner 结算。开发 Skill 同步说明公共入口和生命周期。
