---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-client": minor
"@hanger-source/dsh-reactive-remote": minor
"@hanger-source/dsh-reactive-skills": minor
---

远程 Action 在分派前固定 UUID 受理身份，受理回执未知错误携带 invocationId，调用者可沿原身份只读查回。TypedRPC startAction 请求必须提供该身份；不自动重发或推断业务未执行。

宿主唯一调用记录在解析与准入前登记身份，相同请求返回原 Invocation，不同请求冲突，同步重入明确报告受理中。普通本地自动身份保留原输入能力；显式身份使用 JSON 原值并受既有进程 owner、保留期和数量限制约束。
