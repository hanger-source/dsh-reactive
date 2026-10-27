# @hanger-source/dsh-reactive

## 0.2.1

### Patch Changes

- ed8c8dc: 明确六包 TypeScript 构建依赖与客户端合同的唯一产物来源，严格编译并执行仓外消费者；以 GitHub OIDC 发布可追溯的同步版本及开发 Skill，并从 npm registry 安装验收。
- 871403f: 明确 Reaction 失败通过 state$ 与 settled 观察，events$ 只交付成功结果；同步应用开发指导。

## 0.2.0

首次公开源码。

- 提供不依赖原生宿主的响应式应用合同：Resource、Reaction、Action、Invocation、Operation、Program、Module 与 Surface。
- 支持参数化资源共享、依赖失效传播、类型化输入、人工参与归属和多轮 Operation 反馈。
