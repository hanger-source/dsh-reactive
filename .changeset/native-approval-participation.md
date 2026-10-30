---
"@hanger-source/dsh-reactive": minor
"@hanger-source/dsh-reactive-dsh": minor
"@hanger-source/dsh-reactive-client": patch
"@hanger-source/dsh-reactive-remote": patch
"@hanger-source/dsh-reactive-native-ui": patch
"@hanger-source/dsh-reactive-skills": minor
---

通过纯合同 approvalSurface 和 createNativeApprovalModule 组合原生审批参与。本地、远程 SDK 与可选原生 UI 使用相同 pending 资源和 answer Action，模块携带原生安装与当前等待关联，业务只提供批准要求和响应者授权。

答复核验真实 human 来源、会话与目标，异步授权后复核当前等待，竞争只交付一次；取消、模块卸载和观察退出保留各自语义。submitted 仅证明答复交给原生等待者，政策、审计、一次性授权与工具执行继续由原生服务决定。不恢复持久 pending、不合成原生审计身份，也不新增远程协议或审批引擎。
