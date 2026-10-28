---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-skills": minor
---

combineResources 和 switchResource 直接组合 ResourceReadable 对象与 Observation 流；公开 ResourceSource 表达同一来源的两种消费入口。保持领域类型、原观察流、共享与 owner，不额外读取、解析或取得业务执行所有权。
