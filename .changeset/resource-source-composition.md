---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-skills": minor
---

ResourceSource 统一表达资源对象、业务 hook 返回的观察对象和原 Observation 流。资源算子、shareResource、useResource、资源视图及 Context 直接消费同一来源，不要求外围逐层拆开 observations$；普通值视图继续保持值流语义。

useObservable、useShared、useRelation 可直接接收已有 Observable，现场构造来源继续使用 factory 与显式依赖。useResource 按实际选中的来源决定处理，factory 选择已有资源对象仍借用其原观察流及 owner；自建流继续由程序持有查询与共享。公开视图按原观察流身份保留连接，资源对象与其流之间切换不重新查询。同步公共消费者与同版本开发指导，不增加业务状态、执行权或生命周期。
