# @hanger-source/dsh-reactive-skills

## 0.3.0

### Minor Changes

- 1448bec: 原生 Jobs、Schedule、Team、Files、Approval 和 Clock 接受宿主核验的 Program conversation，普通函数与业务 hooks 共用会话能力实例。原生观察延迟到首次订阅，Program 失败或退出只释放其来源观察，不取消已受理工作。根会话在 Program 构造前持久完成工作区归属，创建 ACK 前不签发外部句柄；本地与远程 open/resume 保留同一创建引用，沿原入口完成登记。恢复继续核验成员撤销；开发 Skill 同步说明来源与生命周期。
- f713b74: 新增 useResourceInstance 与 useReaction，让业务函数、Observable 和程序 hooks 共用同一程序拥有的资源及反应实例。取得实例不自动读取或执行，参数身份保持稳定；Program 失败时释放查询、感知和排队关系，已受理业务继续由原 owner 结算。开发 Skill 同步说明公共入口和生命周期。

### Patch Changes

- Updated dependencies [1448bec]
- Updated dependencies [f713b74]
- Updated dependencies [e4273f8]
  - @hanger-source/dsh-reactive-dsh@0.3.0
  - @hanger-source/dsh-reactive-client@0.3.0
  - @hanger-source/dsh-reactive-remote@0.3.0
  - @hanger-source/dsh-reactive@0.3.0
  - @hanger-source/dsh-reactive-native-ui@0.3.0

## 0.2.2

### Patch Changes

- Updated dependencies [5f53764]
  - @hanger-source/dsh-reactive@0.2.2
  - @hanger-source/dsh-reactive-client@0.2.2
  - @hanger-source/dsh-reactive-dsh@0.2.2
  - @hanger-source/dsh-reactive-native-ui@0.2.2
  - @hanger-source/dsh-reactive-remote@0.2.2

## 0.2.1

### Patch Changes

- ed8c8dc: 明确六包 TypeScript 构建依赖与客户端合同的唯一产物来源，严格编译并执行仓外消费者；以 GitHub OIDC 发布可追溯的同步版本及开发 Skill，并从 npm registry 安装验收。
- 871403f: 明确 Reaction 失败通过 state$ 与 settled 观察，events$ 只交付成功结果；同步应用开发指导。
- Updated dependencies [ed8c8dc]
- Updated dependencies [871403f]
  - @hanger-source/dsh-reactive@0.2.1
  - @hanger-source/dsh-reactive-dsh@0.2.1
  - @hanger-source/dsh-reactive-client@0.2.1
  - @hanger-source/dsh-reactive-remote@0.2.1
  - @hanger-source/dsh-reactive-native-ui@0.2.1

## 0.2.0

首次公开源码。

- 提供与框架版本对应的开发者 Skill 及五份分域 references。
- 通过 getDeveloperSkill() 和 dsh-reactive-skill 定位安装内容，不复制到全局目录；五个框架包使用同版可选 peer。
