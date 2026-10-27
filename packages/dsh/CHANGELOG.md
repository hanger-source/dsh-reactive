# @hanger-source/dsh-reactive-dsh

## 0.3.0

### Minor Changes

- 1448bec: 原生 Jobs、Schedule、Team、Files、Approval 和 Clock 接受宿主核验的 Program conversation，普通函数与业务 hooks 共用会话能力实例。原生观察延迟到首次订阅，Program 失败或退出只释放其来源观察，不取消已受理工作。根会话在 Program 构造前持久完成工作区归属，创建 ACK 前不签发外部句柄；本地与远程 open/resume 保留同一创建引用，沿原入口完成登记。恢复继续核验成员撤销；开发 Skill 同步说明来源与生命周期。

### Patch Changes

- Updated dependencies [f713b74]
- Updated dependencies [e4273f8]
  - @hanger-source/dsh-reactive@0.3.0

## 0.2.2

### Patch Changes

- Updated dependencies [5f53764]
  - @hanger-source/dsh-reactive@0.2.2

## 0.2.1

### Patch Changes

- ed8c8dc: 明确六包 TypeScript 构建依赖与客户端合同的唯一产物来源，严格编译并执行仓外消费者；以 GitHub OIDC 发布可追溯的同步版本及开发 Skill，并从 npm registry 安装验收。
- Updated dependencies [ed8c8dc]
- Updated dependencies [871403f]
  - @hanger-source/dsh-reactive@0.2.1

## 0.2.0

首次公开源码。

- 提供原生 dsh 宿主、多工作区与会话、统一输入和文件上传、工具来源准入、人工问询及可靠 Operation 等待。
- 提供可选 Jobs、MCP、Approval、时间、现存 Schedule 管理、Team 任务板和工作区文件 SDK。
- Schedule 创建、Team spawn/send 与原生 Controller 输入前门仍受 dsh rc.3 公开接点限制。
